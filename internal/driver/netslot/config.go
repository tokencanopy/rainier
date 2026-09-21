// internal/driver/netslot/config.go
//
// Package netslot is the microVM host's per-slot network allocator: one
// network namespace, one veth pair, one TAP device and one /30 of addresses
// per concurrently-running session, recycled on release (ADR-0003 §5.2).
//
// Nothing in this package touches the host directly. Every namespace, link,
// address, route and firewall operation goes through the Host interface in
// host.go, which has a Linux implementation (host_linux.go) and a recording
// fake (fake.go). That is what lets the microVM driver's tests run on a
// machine with no CAP_NET_ADMIN — which is every developer machine and all of
// CI — and what leaves room for a privileged helper to own the implementation
// later without the pool changing shape.
//
// Parts of this package are derived from E2B's Apache-2.0 runtime. See
// PROVENANCE.md for the upstream commit, what was taken and what changed.
package netslot

import (
	"fmt"
	"net/netip"
)

const (
	// DefaultGuestCIDR is the host-local range the guest link addresses are
	// carved from, one /30 per slot: <base>.0 network, <base>.1 gateway (the
	// TAP device inside the namespace), <base>.2 guest, <base>.3 broadcast.
	//
	// It replaces the single hard-coded 172.18.0.2/172.18.0.1 pair every
	// microVM used to boot with, which collided the moment a host ran the two
	// concurrent sessions ADR-0003 §5.1 sizes for.
	DefaultGuestCIDR = "10.201.0.0/16"

	// DefaultUplinkCIDR is the range the veth pair addresses are carved from,
	// one /30 per slot: <base>.1 on the host side, <base>.2 on the namespace
	// side. It is a SECOND range rather than more of the first so that the
	// firewall can deny a whole range and mean "every other slot's link",
	// without having to enumerate sixteen thousand /30s.
	DefaultUplinkCIDR = "10.202.0.0/16"

	// DefaultNamePrefix prefixes every host object this package creates. It
	// is what Reclaim recognises as ours, so it must not be something another
	// program on the host would plausibly use.
	DefaultNamePrefix = "rnr"

	// addressesPerSlot is the /30: four addresses, two of them usable.
	addressesPerSlot = 4

	// slotPrefixBits is the /30 itself, written once.
	slotPrefixBits = 30
)

// Config is the host's slot envelope.
type Config struct {
	// GuestCIDR and UplinkCIDR are the two host-local ranges slots are carved
	// from. Empty means the defaults above.
	GuestCIDR  string
	UplinkCIDR string

	// Slots is how many slots this host may hold at once. It is the driver's
	// own capacity number, not a property of the ranges: a /16 carries 16383
	// /30s and no host runs that many microVMs.
	Slots int

	// NamePrefix prefixes the namespace, veth and TAP names. Empty means
	// DefaultNamePrefix.
	NamePrefix string

	// NetnsDir is where named network namespaces live. Empty means
	// "/var/run/netns", which is where `ip netns` puts them and therefore
	// where Reclaim looks for leftovers from a previous runnerd.
	NetnsDir string

	// ProxyAddr and ProxyPort are the ONE host-side destination a guest may
	// reach: the egress proxy runnerd was started with (ADR-0003 §4.3). A
	// zero value means no host-side destination is allowed at all, which is a
	// legal configuration and not a reason to leave the firewall off.
	ProxyAddr string
	ProxyPort int

	// ControlPlaneCIDRs are the regional control-plane ranges ADR-0003 §4.3
	// requires a guest to be unable to reach. They are configuration rather
	// than a constant because they are a property of the deployment, and a
	// host that was given none denies nothing extra rather than guessing.
	ControlPlaneCIDRs []string
}

// resolved is a Config with its defaults filled in and its ranges parsed.
type resolved struct {
	guest      netip.Prefix
	uplink     netip.Prefix
	slots      int
	prefix     string
	netnsDir   string
	proxyAddr  netip.Addr
	proxyPort  int
	hasProxy   bool
	controlNet []netip.Prefix
}

func (c Config) resolve() (resolved, error) {
	var r resolved

	guestCIDR := c.GuestCIDR
	if guestCIDR == "" {
		guestCIDR = DefaultGuestCIDR
	}
	uplinkCIDR := c.UplinkCIDR
	if uplinkCIDR == "" {
		uplinkCIDR = DefaultUplinkCIDR
	}

	var err error
	if r.guest, err = parseHostRange("guest range", guestCIDR); err != nil {
		return resolved{}, err
	}
	if r.uplink, err = parseHostRange("uplink range", uplinkCIDR); err != nil {
		return resolved{}, err
	}
	if r.guest.Overlaps(r.uplink) {
		return resolved{}, fmt.Errorf("netslot: the guest range %s and the uplink range %s overlap; a slot's guest link and its uplink must be separable, because the firewall denies one range wholesale to deny every neighbour", r.guest, r.uplink)
	}

	r.slots = c.Slots
	if r.slots <= 0 {
		return resolved{}, fmt.Errorf("netslot: a slot count is required")
	}
	if max := maxSlots(r.guest); r.slots > max {
		return resolved{}, fmt.Errorf("netslot: %d slots do not fit in the guest range %s, which carries %d /%d blocks", r.slots, r.guest, max, slotPrefixBits)
	}
	if max := maxSlots(r.uplink); r.slots > max {
		return resolved{}, fmt.Errorf("netslot: %d slots do not fit in the uplink range %s, which carries %d /%d blocks", r.slots, r.uplink, max, slotPrefixBits)
	}

	r.prefix = c.NamePrefix
	if r.prefix == "" {
		r.prefix = DefaultNamePrefix
	}
	if err := checkNamePrefix(r.prefix, r.slots); err != nil {
		return resolved{}, err
	}

	r.netnsDir = c.NetnsDir
	if r.netnsDir == "" {
		r.netnsDir = DefaultNetnsDir
	}

	if c.ProxyAddr != "" {
		addr, err := netip.ParseAddr(c.ProxyAddr)
		if err != nil {
			return resolved{}, fmt.Errorf("netslot: egress proxy address %q is not an IP address: the per-slot firewall allows exactly one host-side destination and it is named by address, not by name, because a guest must not be able to move it by answering a DNS query: %w", c.ProxyAddr, err)
		}
		if !addr.Is4() {
			return resolved{}, fmt.Errorf("netslot: egress proxy address %q is not IPv4; the guest link is IPv4 and the ruleset drops IPv6 from the guest outright", c.ProxyAddr)
		}
		if c.ProxyPort <= 0 || c.ProxyPort > 65535 {
			return resolved{}, fmt.Errorf("netslot: egress proxy address %s was given without a usable port (%d); an allow rule with no port would open every port on the host that answers at that address", c.ProxyAddr, c.ProxyPort)
		}
		r.proxyAddr, r.proxyPort, r.hasProxy = addr, c.ProxyPort, true
	}

	for _, cidr := range c.ControlPlaneCIDRs {
		p, err := netip.ParsePrefix(cidr)
		if err != nil {
			return resolved{}, fmt.Errorf("netslot: control-plane range %q: %w", cidr, err)
		}
		if !p.Addr().Is4() {
			return resolved{}, fmt.Errorf("netslot: control-plane range %q is not IPv4", cidr)
		}
		r.controlNet = append(r.controlNet, p.Masked())
	}

	return r, nil
}

// parseHostRange parses one of the two configurable ranges. It insists on a
// masked IPv4 prefix: "10.201.0.5/16" names a range whose first address is
// not the one the caller wrote, and silently correcting it would make the
// rendered firewall disagree with the operator's configuration file.
func parseHostRange(what, cidr string) (netip.Prefix, error) {
	p, err := netip.ParsePrefix(cidr)
	if err != nil {
		return netip.Prefix{}, fmt.Errorf("netslot: %s %q: %w", what, cidr, err)
	}
	if !p.Addr().Is4() {
		return netip.Prefix{}, fmt.Errorf("netslot: %s %q is not IPv4", what, cidr)
	}
	if p.Masked() != p {
		return netip.Prefix{}, fmt.Errorf("netslot: %s %q has host bits set; write %s", what, cidr, p.Masked())
	}
	if p.Bits() > slotPrefixBits-1 {
		return netip.Prefix{}, fmt.Errorf("netslot: %s %q is too small to carry even one /%d slot block and the host's own network address", what, cidr, slotPrefixBits)
	}
	return p, nil
}

// maxSlots is how many /30s a range carries, minus block zero.
//
// Block zero is deliberately never handed out: its network address is the
// range's own, and a slot numbered 0 would make every "which slot is this"
// answer ambiguous with "none". Slot indexes therefore start at 1, as E2B's
// do.
func maxSlots(p netip.Prefix) int {
	spare := slotPrefixBits - p.Bits()
	if spare <= 0 {
		return 0
	}
	if spare >= 31 {
		// Not reachable for an IPv4 prefix (bits >= 0), but the shift below
		// is only defined because of this guard.
		return 1<<31 - 1
	}
	return (1 << spare) - 1
}

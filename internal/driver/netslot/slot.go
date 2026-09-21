// internal/driver/netslot/slot.go
//
// Portions of this file are derived from E2B's orchestrator:
//
//	https://github.com/e2b-dev/infra
//	packages/orchestrator/pkg/sandbox/network/slot.go
//	commit 926fbd6da6c8bf825d6e62674755ab482b1bba26, read 2026-09-21
//
// Copyright 2023 FoundryLabs, Inc.
// Copyright 2026 Rainier Maintainers
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// What was taken: the idea that one integer index determines every name and
// every address a sandbox's network needs, so that a crashed host can be
// reconciled from the names alone. What changed: the addressing follows
// ADR-0003 §5.2 (a /30 per slot from a configurable host-local range) rather
// than E2B's /32-plus-/31-plus-shared-169.254 layout, the MAC is derived from
// the index instead of being one constant for every sandbox, and the Slot is
// a value with no host operations on it. See PROVENANCE.md.
package netslot

import (
	"fmt"
	"net/netip"
	"regexp"
	"strconv"
	"strings"
)

// Slot is one session's whole network identity on this host.
//
// Every field is derived from Index, and that is the point: a runnerd that
// died holding slots can rebuild all of it from the namespace names the
// kernel still has, which is what Pool.Reclaim does.
type Slot struct {
	// Index is 1-based. Zero is never allocated; see maxSlots.
	Index int

	// Key is who asked for the slot — the driver's instance id, or a
	// reclaim marker. It is bookkeeping only and never reaches the host.
	Key string

	// Netns is the named network namespace holding the TAP device and the
	// namespace end of the veth pair.
	Netns string

	// VethHost and VethNS are the two ends of the uplink pair: VethHost
	// stays in the host namespace, VethNS is moved into Netns.
	VethHost string
	VethNS   string

	// Tap is the TAP device Firecracker is handed, inside Netns.
	Tap string

	// GuestIP is the address the guest kernel is configured with on eth0;
	// GatewayIP is the TAP device's address and the guest's default route.
	// Both come from the slot's /30 in the guest range.
	GuestIP   netip.Addr
	GatewayIP netip.Addr
	GuestNet  netip.Prefix

	// VethHostIP and VethNSIP are the slot's /30 in the uplink range.
	VethHostIP netip.Addr
	VethNSIP   netip.Addr
	UplinkNet  netip.Prefix

	// MAC is the guest NIC's hardware address, derived from Index so a
	// capture on the host names the slot it came from. TapMAC is the host
	// end of the same link — the TAP device's own address — and is a
	// DIFFERENT value on purpose: two interfaces on one segment sharing a
	// hardware address is a broken segment, not a saving.
	MAC    string
	TapMAC string
}

// slotBlock returns the idx'th /30 of p.
func slotBlock(p netip.Prefix, idx int) (netip.Prefix, error) {
	base := p.Addr().As4()
	offset := uint32(idx) * addressesPerSlot
	word := uint32(base[0])<<24 | uint32(base[1])<<16 | uint32(base[2])<<8 | uint32(base[3])
	start := word + offset
	if start < word {
		return netip.Prefix{}, fmt.Errorf("netslot: slot %d overflows the range %s", idx, p)
	}
	addr := netip.AddrFrom4([4]byte{byte(start >> 24), byte(start >> 16), byte(start >> 8), byte(start)})
	block := netip.PrefixFrom(addr, slotPrefixBits)
	if !p.Contains(addr) || !p.Contains(lastOf(block)) {
		return netip.Prefix{}, fmt.Errorf("netslot: slot %d does not fit in the range %s", idx, p)
	}
	return block, nil
}

// lastOf is the broadcast address of a /30.
func lastOf(block netip.Prefix) netip.Addr {
	a := block.Addr().As4()
	word := uint32(a[0])<<24 | uint32(a[1])<<16 | uint32(a[2])<<8 | uint32(a[3])
	word += addressesPerSlot - 1
	return netip.AddrFrom4([4]byte{byte(word >> 24), byte(word >> 16), byte(word >> 8), byte(word)})
}

// nth is block's base address plus n.
func nth(block netip.Prefix, n uint32) netip.Addr {
	a := block.Addr().As4()
	word := uint32(a[0])<<24 | uint32(a[1])<<16 | uint32(a[2])<<8 | uint32(a[3])
	word += n
	return netip.AddrFrom4([4]byte{byte(word >> 24), byte(word >> 16), byte(word >> 8), byte(word)})
}

// newSlot derives every name and address for idx.
func (r resolved) newSlot(idx int, key string) (*Slot, error) {
	if idx < 1 || idx > r.slots {
		return nil, fmt.Errorf("netslot: slot index %d is outside [1, %d]", idx, r.slots)
	}
	guestBlock, err := slotBlock(r.guest, idx)
	if err != nil {
		return nil, err
	}
	uplinkBlock, err := slotBlock(r.uplink, idx)
	if err != nil {
		return nil, err
	}
	return &Slot{
		Index:      idx,
		Key:        key,
		Netns:      r.netnsName(idx),
		VethHost:   fmt.Sprintf("%s-vh-%d", r.prefix, idx),
		VethNS:     fmt.Sprintf("%s-vp-%d", r.prefix, idx),
		Tap:        fmt.Sprintf("%s-tap-%d", r.prefix, idx),
		GatewayIP:  nth(guestBlock, 1),
		GuestIP:    nth(guestBlock, 2),
		GuestNet:   guestBlock,
		VethHostIP: nth(uplinkBlock, 1),
		VethNSIP:   nth(uplinkBlock, 2),
		UplinkNet:  uplinkBlock,
		MAC:        slotMAC(0x00, idx),
		TapMAC:     slotMAC(0x01, idx),
	}, nil
}

func (r resolved) netnsName(idx int) string {
	return fmt.Sprintf("%s-ns-%d", r.prefix, idx)
}

// netnsIndex is netnsName's inverse, and the reason Reclaim can work from
// nothing but a directory listing. A name that is not ours, or names an index
// outside this host's envelope, is not claimed.
func (r resolved) netnsIndex(name string) (int, bool) {
	rest, ok := strings.CutPrefix(name, r.prefix+"-ns-")
	if !ok || rest == "" {
		return 0, false
	}
	idx, err := strconv.Atoi(rest)
	if err != nil || idx < 1 || idx > r.slots {
		return 0, false
	}
	return idx, true
}

// slotMAC derives the guest NIC address from the slot index.
//
// 02 is the locally-administered unicast prefix, so this can never collide
// with a real vendor's assignment. E2B hands every sandbox the same MAC —
// which is safe there, because each TAP is alone in its own namespace — but a
// per-slot address costs nothing and makes a capture on the host self-
// describing.
func slotMAC(side byte, idx int) string {
	return fmt.Sprintf("02:fc:00:%02x:%02x:%02x", side, byte(idx>>8), byte(idx))
}

// GuestCIDR is the guest's address with the slot's prefix length, in the form
// `ip addr add` wants.
func (s *Slot) GuestCIDR() string { return fmt.Sprintf("%s/%d", s.GuestIP, slotPrefixBits) }

// GatewayCIDR is the TAP device's address, likewise.
func (s *Slot) GatewayCIDR() string { return fmt.Sprintf("%s/%d", s.GatewayIP, slotPrefixBits) }

// VethHostCIDR and VethNSCIDR are the two ends of the uplink pair.
func (s *Slot) VethHostCIDR() string { return fmt.Sprintf("%s/%d", s.VethHostIP, slotPrefixBits) }
func (s *Slot) VethNSCIDR() string   { return fmt.Sprintf("%s/%d", s.VethNSIP, slotPrefixBits) }

// GuestNetmask is the dotted netmask the guest kernel's `ip=` boot argument
// wants. It is not the prefix length: the kernel's built-in configuration
// predates CIDR notation and will not parse "/30".
func (s *Slot) GuestNetmask() string {
	bits := s.GuestNet.Bits()
	mask := ^uint32(0) << (32 - bits)
	return fmt.Sprintf("%d.%d.%d.%d", byte(mask>>24), byte(mask>>16), byte(mask>>8), byte(mask))
}

// ifaceName is the kernel's limit on an interface name: IFNAMSIZ is 16 bytes
// including the terminator.
const ifaceNameMax = 15

var namePrefixRE = regexp.MustCompile(`^[a-z][a-z0-9]{0,7}$`)

// checkNamePrefix refuses a prefix that would produce an interface name the
// kernel cannot hold, at the top slot index rather than at the first — the
// failure would otherwise arrive on a busy host, hours in, as a create that
// cannot allocate a slot.
func checkNamePrefix(prefix string, slots int) error {
	if !namePrefixRE.MatchString(prefix) {
		return fmt.Errorf("netslot: name prefix %q must be 1 to 8 lowercase letters and digits starting with a letter; it names host network devices", prefix)
	}
	longest := fmt.Sprintf("%s-tap-%d", prefix, slots)
	if len(longest) > ifaceNameMax {
		return fmt.Errorf("netslot: name prefix %q with %d slots would need the interface name %q, over the kernel's %d-byte limit; use a shorter prefix", prefix, slots, longest, ifaceNameMax)
	}
	return nil
}

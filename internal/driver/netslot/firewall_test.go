// internal/driver/netslot/firewall_test.go
package netslot

import (
	"context"
	"errors"
	"flag"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var updateGolden = flag.Bool("update", false, "rewrite the golden nftables ruleset in testdata")

// goldenConfig is the deployment the golden file describes: two ranges, a
// proxy, and two control-plane ranges that are NOT inside RFC1918, so the
// golden shows what a configured range actually does to the deny set.
func goldenConfig() Config {
	return Config{
		GuestCIDR:         "10.201.0.0/16",
		UplinkCIDR:        "10.202.0.0/16",
		Slots:             8,
		NamePrefix:        "rnr",
		ProxyAddr:         "10.44.0.9",
		ProxyPort:         3128,
		ControlPlaneCIDRs: []string{"203.0.113.0/24", "198.51.100.0/24"},
	}
}

func goldenRuleset(t *testing.T) string {
	t.Helper()
	p, err := New(goldenConfig(), NewFakeHost())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	slot, err := p.SlotFor(3, "golden")
	if err != nil {
		t.Fatalf("SlotFor: %v", err)
	}
	rs, err := p.Ruleset(slot)
	if err != nil {
		t.Fatalf("Ruleset: %v", err)
	}
	return rs
}

// TestRulesetMatchesTheGolden pins the exact bytes the kernel is asked for.
//
// A firewall that is assembled from an expression tree can only be reviewed
// by reading the code that assembles it. Rendering it as text and pinning the
// result means a change to the boundary shows up in a diff as the boundary,
// which is the whole reason this is a template and not a library call.
//
// Regenerate with: go test ./internal/driver/netslot -run Golden -update
func TestRulesetMatchesTheGolden(t *testing.T) {
	got := goldenRuleset(t)
	path := filepath.Join("testdata", "slot-3.nft")

	if *updateGolden {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		t.Log("golden updated")
		return
	}

	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	if got != string(want) {
		t.Fatalf("rendered ruleset does not match %s.\n--- got ---\n%s\n--- want ---\n%s", path, got, want)
	}
}

// TestTheProxyIsTheOnlyAllowedDestination is ADR-0003 §4.3's "exactly one
// allowed host-side destination", asserted against the rendered rules rather
// than against the code that renders them.
func TestTheProxyIsTheOnlyAllowedDestination(t *testing.T) {
	rs := goldenRuleset(t)

	var accepts []string
	for _, line := range strings.Split(rs, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasSuffix(line, " accept") {
			continue
		}
		accepts = append(accepts, line)
	}
	if len(accepts) != 2 {
		t.Fatalf("accept rules = %v, want exactly two: return traffic and the proxy", accepts)
	}
	if accepts[0] != "ct state established accept" {
		t.Fatalf("first accept = %q, want the established rule", accepts[0])
	}
	// ESTABLISHED and not RELATED: a RELATED match covers conntrack helper
	// expectations, which a guest talking to the allowed proxy could use to
	// conjure an accept for some other address ahead of the deny set.
	if strings.Contains(rs, "related") {
		t.Fatalf("the ruleset accepts RELATED traffic from the guest:\n%s", rs)
	}
	if accepts[1] != "ip daddr 10.44.0.9 tcp dport 3128 accept" {
		t.Fatalf("second accept = %q, want the egress proxy and nothing else", accepts[1])
	}
}

// TestNoProxyMeansNoHostSideDestinationAtAll: a runner started without a
// proxy gets a ruleset with no address exception, not a ruleset with a
// default one.
func TestNoProxyMeansNoHostSideDestinationAtAll(t *testing.T) {
	cfg := goldenConfig()
	cfg.ProxyAddr, cfg.ProxyPort = "", 0
	p, err := New(cfg, NewFakeHost())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	slot, err := p.SlotFor(3, "no-proxy")
	if err != nil {
		t.Fatalf("SlotFor: %v", err)
	}
	rs, err := p.Ruleset(slot)
	if err != nil {
		t.Fatalf("Ruleset: %v", err)
	}
	if strings.Contains(rs, "daddr") && strings.Contains(rs, "accept") {
		for _, line := range strings.Split(rs, "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "ip daddr") && strings.HasSuffix(line, "accept") {
				t.Fatalf("a proxy-less runner rendered an address exception: %q", line)
			}
		}
	}
}

// TestGuestIPv6IsDroppedBeforeAnythingIsAccepted.
//
// The deny set is an ipv4_addr interval set, so it has nothing to say about
// an IPv6 destination: the metadata service, a neighbour and the control
// plane are all reachable over v6 by a guest that configures itself one,
// whatever this ruleset says about v4. The guest link is configured IPv4-only
// by the boot arguments, so the whole family is dropped — and it has to be
// dropped ABOVE the accepts, or the proxy rule and the established rule would
// let v6 through ahead of it.
//
// The golden file pins these bytes, but only as bytes: a change that moved
// this rule below the accepts would be a golden diff someone could wave
// through. This says what the position is FOR.
func TestGuestIPv6IsDroppedBeforeAnythingIsAccepted(t *testing.T) {
	rs := goldenRuleset(t)

	ipv6, firstAccept, guestReturn := -1, -1, -1
	for i, line := range strings.Split(rs, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.Contains(line, "nfproto ipv6"):
			if !strings.HasSuffix(line, "drop") {
				t.Fatalf("the IPv6 rule is %q, and the deny set is IPv4-only: anything but a drop is a family with no boundary at all", line)
			}
			if ipv6 != -1 {
				t.Fatalf("two IPv6 rules in one chain:\n%s", rs)
			}
			ipv6 = i
		case strings.HasSuffix(line, "accept") && firstAccept == -1:
			firstAccept = i
		case strings.HasPrefix(line, "iifname !="):
			guestReturn = i
		}
	}
	if ipv6 == -1 {
		t.Fatalf("nothing in the ruleset drops IPv6 from the guest:\n%s", rs)
	}
	if guestReturn == -1 || ipv6 < guestReturn {
		t.Fatalf("the IPv6 drop is above the interface match, so it drops traffic that is not the guest's:\n%s", rs)
	}
	if firstAccept != -1 && ipv6 > firstAccept {
		t.Fatalf("the IPv6 drop is below an accept, so a v6 packet can be accepted before the family is dropped:\n%s", rs)
	}
}

// TestNeighbourSlotsAndTheHostAreDenied walks the rendered deny set and asks
// it about concrete addresses: the guest next door, its gateway, the uplink
// veths, the metadata service, the control plane, and this slot's own
// gateway. All of them must fall inside a denied range.
func TestNeighbourSlotsAndTheHostAreDenied(t *testing.T) {
	cfg := goldenConfig()
	p, err := New(cfg, NewFakeHost())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	mine, err := p.SlotFor(3, "mine")
	if err != nil {
		t.Fatalf("SlotFor: %v", err)
	}
	neighbour, err := p.SlotFor(4, "neighbour")
	if err != nil {
		t.Fatalf("SlotFor: %v", err)
	}
	rs, err := p.Ruleset(mine)
	if err != nil {
		t.Fatalf("Ruleset: %v", err)
	}
	denied := deniedPrefixes(t, rs)

	for what, addr := range map[string]string{
		"the neighbouring slot's guest":   neighbour.GuestIP.String(),
		"the neighbouring slot's gateway": neighbour.GatewayIP.String(),
		"the neighbouring slot's uplink":  neighbour.VethNSIP.String(),
		"this slot's own gateway":         mine.GatewayIP.String(),
		"this slot's host-side veth":      mine.VethHostIP.String(),
		"the cloud metadata service":      "169.254.169.254",
		"link-local":                      "169.254.1.1",
		"a control-plane address":         "203.0.113.17",
		"another control-plane address":   "198.51.100.4",
		"an RFC1918 host":                 "192.168.7.7",
		"loopback":                        "127.0.0.1",
	} {
		ip := netip.MustParseAddr(addr)
		if !anyContains(denied, ip) {
			t.Errorf("%s (%s) is not inside any denied range; the ruleset lets a guest reach it", what, addr)
		}
	}

	// And the boundary is not simply "deny everything": a public address is
	// left to egressd's allowlist, which is where egress policy belongs.
	if public := netip.MustParseAddr("93.184.216.34"); anyContains(denied, public) {
		t.Errorf("the public address %s is denied by the slot ruleset; egress policy belongs to egressd, not here", public)
	}
	// The proxy IS inside a denied range (it is on the host's own network) —
	// which is exactly why the accept rule has to come first.
	if !anyContains(denied, netip.MustParseAddr(cfg.ProxyAddr)) {
		t.Errorf("the proxy %s is not inside a denied range, so the ordering of the accept rule is untested here", cfg.ProxyAddr)
	}
	proxyLine := strings.Index(rs, "ip daddr "+cfg.ProxyAddr)
	denyLine := strings.Index(rs, "ip daddr @denied4 drop")
	if proxyLine < 0 || denyLine < 0 || proxyLine > denyLine {
		t.Fatalf("the proxy accept must come before the deny set; got positions %d and %d", proxyLine, denyLine)
	}
}

// TestSlotRangesOutsideTheConstantsAreNamedExplicitly is the test of the one
// line that puts THIS host's slot ranges in the deny set.
//
// The ranges have to be outside every constant in alwaysDenied for the test
// to mean anything: pick 10.201.0.0/16, or anything else inside 10/8, and
// dedupeRanges drops it as already covered, the neighbour is denied by the
// constant, and deleting the line under test leaves the assertion green.
// 192.0.2.0/24 and 203.0.113.0/24 are documentation ranges covered by none
// of the constants, so the only thing that can deny the neighbour here is
// the line this test is for.
func TestSlotRangesOutsideTheConstantsAreNamedExplicitly(t *testing.T) {
	cfg := Config{
		GuestCIDR:  "192.0.2.0/24",
		UplinkCIDR: "203.0.113.0/24",
		Slots:      4,
		NamePrefix: "rnr",
	}
	p, err := New(cfg, NewFakeHost())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	mine, _ := p.SlotFor(1, "mine")
	neighbour, _ := p.SlotFor(2, "neighbour")
	rs, err := p.Ruleset(mine)
	if err != nil {
		t.Fatalf("Ruleset: %v", err)
	}
	denied := deniedPrefixes(t, rs)

	// The ranges themselves, by name.
	var sawGuest, sawUplink bool
	for _, p := range denied {
		switch p.String() {
		case "192.0.2.0/24":
			sawGuest = true
		case "203.0.113.0/24":
			sawUplink = true
		}
	}
	if !sawGuest || !sawUplink {
		t.Fatalf("this host's own slot ranges are not in the deny set: %v", denied)
	}
	// And no constant would have covered them, so the line above is the only
	// thing denying the neighbour.
	for _, constant := range alwaysDenied {
		if netip.MustParsePrefix(constant).Contains(neighbour.GuestIP) {
			t.Fatalf("the neighbour at %s is inside the constant %s, so this test would pass without the slot ranges", neighbour.GuestIP, constant)
		}
	}
	if !anyContains(denied, neighbour.GuestIP) {
		t.Fatalf("the neighbour at %s is reachable; denied ranges were %v", neighbour.GuestIP, denied)
	}
}

// TestTheProxyMayNotBeSomewhereTheFirewallDenies. The proxy's accept rule
// sits above every drop, so its address is the one value in this
// configuration that can turn a control off by being set to the thing the
// control exists to block.
func TestTheProxyMayNotBeSomewhereTheFirewallDenies(t *testing.T) {
	cases := []struct {
		name string
		addr string
		want string
	}{
		{"the metadata service", "169.254.169.254", "link-local"},
		{"link-local", "169.254.1.1", "link-local"},
		{"loopback", "127.0.0.1", "loopback"},
		{"multicast", "239.1.2.3", "multicast"},
		{"this host's guest range", "10.201.0.9", "guest slot range"},
		{"this host's uplink range", "10.202.0.9", "uplink slot range"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := goldenConfig()
			cfg.ProxyAddr = tc.addr
			_, err := New(cfg, NewFakeHost())
			if err == nil {
				t.Fatalf("a runner was configured with its egress proxy at %s", tc.addr)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want it to name %q", err, tc.want)
			}
		})
	}

	// And the ordinary case still works: a proxy on the host's RFC1918
	// network is exactly what the accept-above-drop ordering is for.
	cfg := goldenConfig()
	cfg.ProxyAddr = "10.44.0.9"
	if _, err := New(cfg, NewFakeHost()); err != nil {
		t.Fatalf("a proxy on the host's private network was refused: %v", err)
	}
}

// TestRulesetIsNamespacedPerSlot: two slots do not share a table, so
// removing one does not disarm the other.
func TestRulesetIsNamespacedPerSlot(t *testing.T) {
	p, err := New(goldenConfig(), NewFakeHost())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	one, _ := p.SlotFor(1, "one")
	two, _ := p.SlotFor(2, "two")
	rsOne, _ := p.Ruleset(one)
	rsTwo, _ := p.Ruleset(two)
	if !strings.Contains(rsOne, "table inet rnr-slot-1 {") || !strings.Contains(rsTwo, "table inet rnr-slot-2 {") {
		t.Fatalf("slot tables are not per-slot:\n%s\n%s", rsOne, rsTwo)
	}
	if !strings.Contains(rsOne, `iifname != "rnr-tap-1" return`) {
		t.Fatalf("slot 1's chain does not match slot 1's tap:\n%s", rsOne)
	}
}

// TestFirewallIsAppliedOnAllocateAndRemovedOnRelease.
func TestFirewallIsAppliedOnAllocateAndRemovedOnRelease(t *testing.T) {
	host := NewFakeHost()
	p, err := New(goldenConfig(), host)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	slot, err := p.Allocate(ctx, "mvm-1")
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	applied, ok := host.Ruleset(slot.Netns)
	if !ok {
		t.Fatal("no ruleset was applied in the slot's namespace")
	}
	if !strings.Contains(applied, `iifname != "`+slot.Tap+`" return`) {
		t.Fatalf("the applied ruleset is not this slot's:\n%s", applied)
	}

	if err := p.Release(ctx, slot); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if _, ok := host.Ruleset(slot.Netns); ok {
		t.Fatal("the slot's ruleset survived its release")
	}
}

// TestAFirewallThatWillNotApplyFailsTheAllocation. A slot whose ruleset did
// not take is a guest with no boundary, and handing it out would be exactly
// the failure ADR-0003 §4.3 exists to prevent.
func TestAFirewallThatWillNotApplyFailsTheAllocation(t *testing.T) {
	host := NewFakeHost()
	p, err := New(goldenConfig(), host)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	host.FailOn("nft-apply", errors.New("nft: Operation not permitted"))

	if _, err := p.Allocate(context.Background(), "mvm-1"); err == nil {
		t.Fatal("Allocate handed out a slot whose firewall did not apply")
	}
	if got := host.Namespaces(); len(got) != 0 {
		t.Fatalf("namespaces after a failed firewall = %v, want none", got)
	}
}

// deniedPrefixes reads the prefixes out of a rendered ruleset's deny set,
// parsing only what nft itself would: the `elements = { ... }` block.
func deniedPrefixes(t *testing.T, ruleset string) []netip.Prefix {
	t.Helper()
	start := strings.Index(ruleset, "elements = {")
	if start < 0 {
		t.Fatalf("no deny set in the rendered ruleset:\n%s", ruleset)
	}
	rest := ruleset[start+len("elements = {"):]
	end := strings.Index(rest, "}")
	if end < 0 {
		t.Fatalf("unterminated deny set in the rendered ruleset:\n%s", ruleset)
	}
	var out []netip.Prefix
	for _, field := range strings.Split(rest[:end], ",") {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		p, err := netip.ParsePrefix(field)
		if err != nil {
			t.Fatalf("deny set element %q is not a prefix: %v", field, err)
		}
		out = append(out, p)
	}
	if len(out) == 0 {
		t.Fatal("the deny set is empty")
	}
	return out
}

func anyContains(prefixes []netip.Prefix, addr netip.Addr) bool {
	for _, p := range prefixes {
		if p.Contains(addr) {
			return true
		}
	}
	return false
}

// TestFirewallReadsBackWhatTheHostWasGiven. Ruleset is what the renderer
// produced; Firewall is what the host has. The distinction is the whole reason
// the read exists (ADR-0003 §4.3 asserts a property of the machine, not of a
// template), so this asserts that the read goes to the host: the ruleset comes
// back naming this slot's TAP and its table, and the host recorded a read.
func TestFirewallReadsBackWhatTheHostWasGiven(t *testing.T) {
	host := NewFakeHost()
	p, err := New(goldenConfig(), host)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	slot, err := p.Allocate(ctx, "mvm-1")
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	got, err := p.Firewall(ctx, slot)
	if err != nil {
		t.Fatalf("Firewall: %v", err)
	}
	if !strings.Contains(got, `iifname != "`+slot.Tap+`" return`) {
		t.Fatalf("the ruleset read back is not this slot's:\n%s", got)
	}
	if !strings.Contains(got, "table inet "+p.TableName(slot)+" {") {
		t.Fatalf("the ruleset read back is not in this slot's table %q:\n%s", p.TableName(slot), got)
	}
	// The named metadata drop is the one control a reader has to be able to
	// point at in a dump; see firewall.go's metadataAddress.
	if !strings.Contains(got, "ip daddr "+metadataAddress+" drop") {
		t.Fatalf("the ruleset read back does not name the metadata drop:\n%s", got)
	}

	ops := host.Ops()
	var read *Op
	for i := range ops {
		if ops[i].Verb == "nft-list" {
			read = &ops[i]
		}
	}
	if read == nil {
		t.Fatal("Firewall answered without asking the host; a read that does not reach the machine evidences nothing")
	}
	if read.Netns != slot.Netns || len(read.Args) != 1 || read.Args[0] != p.TableName(slot) {
		t.Fatalf("Firewall asked the host for %v in %q, want table %q in %q", read.Args, read.Netns, p.TableName(slot), slot.Netns)
	}
}

// TestFirewallReportsAnAbsentTableAsSuch. A live slot whose table has gone is
// the finding this read exists to make, and "no rules" must not read the same
// as "an empty ruleset".
//
// The namespace is left standing on purpose: that is what makes this the
// security finding rather than the teardown below.
func TestFirewallReportsAnAbsentTableAsSuch(t *testing.T) {
	host := NewFakeHost()
	p, err := New(goldenConfig(), host)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	slot, err := p.Allocate(ctx, "mvm-1")
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if err := host.DeleteNftTable(ctx, slot.Netns, p.TableName(slot)); err != nil {
		t.Fatalf("taking the slot's table away underneath it: %v", err)
	}
	got, err := p.Firewall(ctx, slot)
	if !errors.Is(err, ErrNoNftTable) {
		t.Fatalf("Firewall of a live slot with no table = %q, %v; want ErrNoNftTable", got, err)
	}
	if got != "" {
		t.Fatalf("Firewall returned a ruleset for a table that is gone: %q", got)
	}
}

// TestFirewallDistinguishesAGoneNamespaceFromAnAbsentTable. `ip netns exec`
// against a namespace that does not exist fails before nft is reached, so a
// released slot answers a question that could not be asked rather than one
// about a table.
//
// Told apart because the two findings are opposite. A live guest behind no
// rules is the thing ADR-0003 §4.3's check exists to catch; a torn-down slot
// behind no rules is teardown having worked. One sentinel for both would make
// a teardown assertion pass for a slot whose namespace was still up with its
// firewall stripped off.
func TestFirewallDistinguishesAGoneNamespaceFromAnAbsentTable(t *testing.T) {
	host := NewFakeHost()
	p, err := New(goldenConfig(), host)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	slot, err := p.Allocate(ctx, "mvm-1")
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if err := p.Release(ctx, slot); err != nil {
		t.Fatalf("Release: %v", err)
	}
	got, err := p.Firewall(ctx, slot)
	if !errors.Is(err, ErrNoNetns) {
		t.Fatalf("Firewall of a released slot = %q, %v; want ErrNoNetns", got, err)
	}
	if errors.Is(err, ErrNoNftTable) {
		t.Fatalf("a gone namespace also reports ErrNoNftTable (%v), so a teardown assertion cannot tell it from a live slot whose firewall was stripped", err)
	}
	if got != "" {
		t.Fatalf("Firewall returned a ruleset from a namespace that is gone: %q", got)
	}
}

// TestFirewallRefusesAHostThatCannotRead. An honest refusal rather than an
// empty string: a caller that read "" as "this guest is behind no rules" would
// report a working host broken, and one that read it as "fine" would miss the
// case the read exists for.
func TestFirewallRefusesAHostThatCannotRead(t *testing.T) {
	p, err := New(goldenConfig(), &nftBlindHost{inner: NewFakeHost()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	slot, err := p.Allocate(ctx, "mvm-1")
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	got, err := p.Firewall(ctx, slot)
	if err == nil {
		t.Fatalf("Firewall on a host with no read half returned %q and no error", got)
	}
	if got != "" {
		t.Fatalf("Firewall returned %q beside its refusal", got)
	}
	if !strings.Contains(err.Error(), "cannot read a ruleset back") {
		t.Fatalf("the refusal = %q, want it to name what the host cannot do", err)
	}
}

// nftBlindHost is a Host with no read half — every method forwarded to a
// FakeHost, and ListNft deliberately not among them. It is written out rather
// than embedded because embedding *FakeHost would promote ListNft and make
// this type the thing it exists not to be.
type nftBlindHost struct{ inner *FakeHost }

var _ Host = (*nftBlindHost)(nil)

func (h *nftBlindHost) AddNetns(ctx context.Context, name string) error {
	return h.inner.AddNetns(ctx, name)
}
func (h *nftBlindHost) DelNetns(ctx context.Context, name string) error {
	return h.inner.DelNetns(ctx, name)
}
func (h *nftBlindHost) ListNetns(ctx context.Context) ([]string, error) {
	return h.inner.ListNetns(ctx)
}
func (h *nftBlindHost) AddVeth(ctx context.Context, hostName, peerName, peerNetns string) error {
	return h.inner.AddVeth(ctx, hostName, peerName, peerNetns)
}
func (h *nftBlindHost) DelLink(ctx context.Context, netns, name string) error {
	return h.inner.DelLink(ctx, netns, name)
}
func (h *nftBlindHost) AddTap(ctx context.Context, netns, name, mac string) error {
	return h.inner.AddTap(ctx, netns, name, mac)
}
func (h *nftBlindHost) AddAddr(ctx context.Context, netns, link, cidr string) error {
	return h.inner.AddAddr(ctx, netns, link, cidr)
}
func (h *nftBlindHost) LinkUp(ctx context.Context, netns, link string) error {
	return h.inner.LinkUp(ctx, netns, link)
}
func (h *nftBlindHost) AddRoute(ctx context.Context, netns, dst, via string) error {
	return h.inner.AddRoute(ctx, netns, dst, via)
}
func (h *nftBlindHost) DelRoute(ctx context.Context, netns, dst, via string) error {
	return h.inner.DelRoute(ctx, netns, dst, via)
}
func (h *nftBlindHost) SetSysctl(ctx context.Context, netns, key, value string) error {
	return h.inner.SetSysctl(ctx, netns, key, value)
}
func (h *nftBlindHost) ApplyNft(ctx context.Context, netns, ruleset string) error {
	return h.inner.ApplyNft(ctx, netns, ruleset)
}
func (h *nftBlindHost) DeleteNftTable(ctx context.Context, netns, table string) error {
	return h.inner.DeleteNftTable(ctx, netns, table)
}

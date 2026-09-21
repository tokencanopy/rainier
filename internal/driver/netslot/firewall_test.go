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
	if accepts[0] != "ct state established,related accept" {
		t.Fatalf("first accept = %q, want the established/related rule", accepts[0])
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
	for what, addr := range map[string]string{
		"a public address": "93.184.216.34",
		"the egress proxy": cfg.ProxyAddr,
	} {
		ip := netip.MustParseAddr(addr)
		if what == "a public address" && anyContains(denied, ip) {
			t.Errorf("%s (%s) is denied by the slot ruleset; egress policy belongs to egressd, not here", what, addr)
		}
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

// TestSlotRangesOutsideRFC1918AreNamedExplicitly: the deny set collapses
// ranges that are already covered, so an operator who puts the slots inside
// 10/8 sees one entry — but one who puts them somewhere public-ish must see
// them named, or neighbours would be reachable.
func TestSlotRangesOutsideRFC1918AreNamedExplicitly(t *testing.T) {
	cfg := Config{
		GuestCIDR:  "100.120.0.0/16",
		UplinkCIDR: "100.121.0.0/16",
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
	if !anyContains(denied, neighbour.GuestIP) {
		t.Fatalf("the neighbour at %s is reachable; denied ranges were %v", neighbour.GuestIP, denied)
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

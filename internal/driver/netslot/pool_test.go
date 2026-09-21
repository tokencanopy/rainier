// internal/driver/netslot/pool_test.go
package netslot

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
)

func testConfig(slots int) Config {
	return Config{
		GuestCIDR:  "10.201.0.0/16",
		UplinkCIDR: "10.202.0.0/16",
		Slots:      slots,
		NamePrefix: "rnr",
		NetnsDir:   "/var/run/netns",
	}
}

func newTestPool(t *testing.T, slots int) (*Pool, *FakeHost) {
	t.Helper()
	host := NewFakeHost()
	p, err := New(testConfig(slots), host)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p, host
}

// TestTwoConcurrentAllocationsAreDisjoint is the property the hard-coded
// 172.18.0.2 did not have: ADR-0003 §5.1 sizes a host for four to six
// sessions, and two of them must not be the same guest.
func TestTwoConcurrentAllocationsAreDisjoint(t *testing.T) {
	p, host := newTestPool(t, 8)

	const n = 6
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		slots []*Slot
		errs  []error
	)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := p.Allocate(context.Background(), fmt.Sprintf("mvm-%d", i))
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			slots = append(slots, s)
		}()
	}
	wg.Wait()
	if len(errs) > 0 {
		t.Fatalf("concurrent Allocate: %v", errors.Join(errs...))
	}
	if len(slots) != n {
		t.Fatalf("got %d slots, want %d", len(slots), n)
	}

	seen := map[string]string{}
	for _, s := range slots {
		for what, value := range map[string]string{
			"guest ip":   s.GuestIP.String(),
			"gateway ip": s.GatewayIP.String(),
			"guest mac":  s.MAC,
			"tap mac":    s.TapMAC,
			"netns":      s.Netns,
			"tap":        s.Tap,
			"veth host":  s.VethHost,
			"veth ns":    s.VethNS,
			"veth ip":    s.VethHostIP.String(),
		} {
			key := what + "=" + value
			if prev, dup := seen[key]; dup {
				t.Fatalf("slot %d and %s share a %s (%s)", s.Index, prev, what, value)
			}
			seen[key] = fmt.Sprintf("slot %d", s.Index)
		}
	}

	if got := len(host.Namespaces()); got != n {
		t.Fatalf("host holds %d namespaces, want %d", got, n)
	}
}

// TestAllocateBuildsTheWholeSlotInOrder is what the recording fake is for:
// the namespace exists before anything is put in it, and the guest's link is
// addressed and up before the driver above is told the slot is ready.
func TestAllocateBuildsTheWholeSlotInOrder(t *testing.T) {
	p, host := newTestPool(t, 4)
	slot, err := p.Allocate(context.Background(), "mvm-1")
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}

	want := []Op{
		{Verb: "netns-add", Args: []string{"rnr-ns-1"}},
		{Verb: "veth-add", Args: []string{"rnr-vh-1", "rnr-vp-1", "rnr-ns-1"}},
		{Verb: "addr-add", Args: []string{"rnr-vh-1", "10.202.0.5/30"}},
		{Verb: "link-up", Args: []string{"rnr-vh-1"}},
		{Verb: "addr-add", Netns: "rnr-ns-1", Args: []string{"rnr-vp-1", "10.202.0.6/30"}},
		{Verb: "link-up", Netns: "rnr-ns-1", Args: []string{"rnr-vp-1"}},
		{Verb: "link-up", Netns: "rnr-ns-1", Args: []string{"lo"}},
		{Verb: "tap-add", Netns: "rnr-ns-1", Args: []string{"rnr-tap-1", "02:fc:00:01:00:01"}},
		{Verb: "addr-add", Netns: "rnr-ns-1", Args: []string{"rnr-tap-1", "10.201.0.5/30"}},
		{Verb: "link-up", Netns: "rnr-ns-1", Args: []string{"rnr-tap-1"}},
		{Verb: "sysctl", Netns: "rnr-ns-1", Args: []string{"net.ipv4.ip_forward", "1"}},
		{Verb: "route-add", Netns: "rnr-ns-1", Args: []string{"default", "10.202.0.5"}},
		{Verb: "route-add", Args: []string{"10.201.0.4/30", "10.202.0.6"}},
		{Verb: "nft-apply", Netns: "rnr-ns-1", Args: []string{"<ruleset>"}},
	}
	assertOps(t, host.Ops(), want)

	if slot.GuestIP.String() != "10.201.0.6" || slot.GatewayIP.String() != "10.201.0.5" {
		t.Fatalf("slot 1 addresses = guest %s gateway %s, want 10.201.0.6 / 10.201.0.5", slot.GuestIP, slot.GatewayIP)
	}
	if slot.MAC != "02:fc:00:00:00:01" {
		t.Fatalf("slot 1 guest MAC = %s", slot.MAC)
	}
	if slot.GuestNetmask() != "255.255.255.252" {
		t.Fatalf("slot 1 netmask = %s, want 255.255.255.252", slot.GuestNetmask())
	}
}

// TestReleaseReturnsTheSlotToThePool: the index comes back, and the next
// create gets the same addresses rather than a fresh pair that slowly walks
// the host out of its range.
func TestReleaseReturnsTheSlotToThePool(t *testing.T) {
	p, host := newTestPool(t, 2)
	ctx := context.Background()

	first, err := p.Allocate(ctx, "mvm-1")
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if err := p.Release(ctx, first); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if got := host.Namespaces(); len(got) != 0 {
		t.Fatalf("namespaces after release = %v, want none", got)
	}
	if got := host.Links(); len(got) != 0 {
		t.Fatalf("links after release = %v, want none", got)
	}

	again, err := p.Allocate(ctx, "mvm-2")
	if err != nil {
		t.Fatalf("second Allocate: %v", err)
	}
	if again.Index != first.Index {
		t.Fatalf("second Allocate took index %d, want the released %d", again.Index, first.Index)
	}
	if again.GuestIP != first.GuestIP {
		t.Fatalf("recycled slot addresses %s, want %s", again.GuestIP, first.GuestIP)
	}
}

// TestTeardownDeletesTheNamespaceLast is the invariant Reclaim rests on: the
// namespace is the only record a leftover slot exists, so it must outlive
// every piece of host-side state keyed to its index.
func TestTeardownDeletesTheNamespaceLast(t *testing.T) {
	p, host := newTestPool(t, 2)
	ctx := context.Background()
	slot, err := p.Allocate(ctx, "mvm-1")
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	before := len(host.Ops())
	if err := p.Release(ctx, slot); err != nil {
		t.Fatalf("Release: %v", err)
	}
	teardown := host.Ops()[before:]
	assertOps(t, teardown, []Op{
		{Verb: "nft-del-table", Netns: "rnr-ns-1", Args: []string{"rnr-slot-1"}},
		{Verb: "route-del", Args: []string{"10.201.0.4/30", "10.202.0.6"}},
		{Verb: "link-del", Args: []string{"rnr-vh-1"}},
		{Verb: "netns-del", Args: []string{"rnr-ns-1"}},
	})
}

// TestReleaseKeepsTheAnchorWhenTeardownFails: a failed teardown must not
// delete the namespace, because the namespace is how the next attempt finds
// the state that is still there.
func TestReleaseKeepsTheAnchorWhenTeardownFails(t *testing.T) {
	p, host := newTestPool(t, 1)
	ctx := context.Background()
	slot, err := p.Allocate(ctx, "mvm-1")
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}

	host.FailOn("link-del", errors.New("device busy"))
	if err := p.Release(ctx, slot); err == nil {
		t.Fatal("Release succeeded with a failing link teardown")
	}
	if got := host.Namespaces(); len(got) != 1 || got[0] != "rnr-ns-1" {
		t.Fatalf("namespaces after a failed release = %v, want the anchor rnr-ns-1 kept", got)
	}

	// The index is leaked, not free: the only slot on this host is the one
	// whose teardown failed, so the next Allocate must finish that teardown
	// before it builds. With the teardown still failing, it must refuse.
	if _, err := p.Allocate(ctx, "mvm-2"); err == nil {
		t.Fatal("Allocate reused a leaked index whose teardown still fails")
	}

	// Once the host is healthy again the index heals in-process, without a
	// restart.
	host.FailOn("link-del", nil)
	healed, err := p.Allocate(ctx, "mvm-3")
	if err != nil {
		t.Fatalf("Allocate after the host recovered: %v", err)
	}
	if healed.Index != 1 {
		t.Fatalf("healed slot index = %d, want 1", healed.Index)
	}
}

// TestAllocateCleansUpAfterAFailedSetup: a create that dies half-way leaves
// no namespace behind and does not consume the index.
func TestAllocateCleansUpAfterAFailedSetup(t *testing.T) {
	p, host := newTestPool(t, 1)
	host.FailOn("tap-add", errors.New("no CAP_NET_ADMIN"))

	if _, err := p.Allocate(context.Background(), "mvm-1"); err == nil {
		t.Fatal("Allocate succeeded with a failing tap-add")
	}
	if got := host.Namespaces(); len(got) != 0 {
		t.Fatalf("namespaces after a failed allocate = %v, want none", got)
	}

	host.FailOn("tap-add", nil)
	if _, err := p.Allocate(context.Background(), "mvm-2"); err != nil {
		t.Fatalf("the index was not returned to the pool: %v", err)
	}
}

// TestReclaimFreesOrphansFromAPreviousRun simulates the crash: namespaces on
// the host that no live pool holds.
func TestReclaimFreesOrphansFromAPreviousRun(t *testing.T) {
	host := NewFakeHost()
	host.Preexisting("rnr-ns-1", "rnr-ns-2", "docker0-ns", "rnr-ns-99999")

	p, err := New(testConfig(4), host)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Slot 2 belongs to a session that outlived its runnerd; the driver
	// above re-associates it before reclaim runs.
	if _, err := p.Adopt(2, "mvm-7"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	n, err := p.Reclaim(context.Background())
	// rnr-ns-99999 is index-shaped and carries this runner's prefix, but its
	// /30 does not fit the configured range — a leftover from a run with
	// different addressing. Reclaim reports it and leaves it alone rather
	// than deleting the one handle anything has on the rest of its state.
	if err == nil || !strings.Contains(err.Error(), "rnr-ns-99999") {
		t.Fatalf("Reclaim = %v, want it to report rnr-ns-99999 as not reconcilable", err)
	}
	if n != 1 {
		t.Fatalf("Reclaim freed %d slots, want 1 (slot 1; slot 2 is held, the others it cannot name)", n)
	}
	got := host.Namespaces()
	want := []string{"docker0-ns", "rnr-ns-2", "rnr-ns-99999"}
	if len(got) != len(want) {
		t.Fatalf("namespaces after reclaim = %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("namespaces after reclaim = %v, want %v", got, want)
		}
	}
}

// TestReclaimLeavesNamespacesThatAreNotOursAlone: a host may run other
// things, and a name that is not ours is never allocated and never deleted.
func TestReclaimLeavesNamespacesThatAreNotOursAlone(t *testing.T) {
	host := NewFakeHost()
	host.Preexisting("cni-abc", "rnr-ns-x")

	p, err := New(testConfig(2), host)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	n, err := p.Reclaim(context.Background())
	if err != nil {
		t.Fatalf("Reclaim: %v", err)
	}
	if n != 0 {
		t.Fatalf("Reclaim touched %d namespaces that are not ours, want 0", n)
	}
	if got := len(host.Namespaces()); got != 2 {
		t.Fatalf("namespaces after reclaim = %d, want 2", got)
	}
}

// TestReclaimCleansUpAboveTheCurrentSlotCount is the operator who shrank
// --slots. The allocator would never hand out index 9 on a four-slot host,
// so if reclaim refused to recognise the name too, the namespaces the resize
// orphaned would be orphaned for good — in this run and every later one.
func TestReclaimCleansUpAboveTheCurrentSlotCount(t *testing.T) {
	host := NewFakeHost()
	host.Preexisting("rnr-ns-2", "rnr-ns-9")

	p, err := New(testConfig(4), host)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	n, err := p.Reclaim(context.Background())
	if err != nil {
		t.Fatalf("Reclaim: %v", err)
	}
	if n != 2 {
		t.Fatalf("Reclaim freed %d slots, want 2 (including the one above the envelope)", n)
	}
	if got := host.Namespaces(); len(got) != 0 {
		t.Fatalf("namespaces after reclaim = %v, want none", got)
	}

	// Reclaiming an index does not make it allocatable: the envelope is
	// still four.
	if _, err := p.SlotFor(9, "x"); err == nil {
		t.Fatal("an index above the slot count was handed out")
	}
}

// TestANamespaceSomebodyElseCreatedRetiresItsIndex. Something else on the
// host using our naming scheme is the one case where deleting a namespace
// would be deleting another program's, so that index is retired and the next
// one is used instead — and the allocation still succeeds.
func TestANamespaceSomebodyElseCreatedRetiresItsIndex(t *testing.T) {
	host := NewFakeHost()
	p, err := New(testConfig(4), host)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Planted AFTER construction, so reclaim is not what deals with it: this
	// is a name that appeared while the runner was up.
	host.Preexisting("rnr-ns-1")

	slot, err := p.Allocate(context.Background(), "mvm-1")
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if slot.Index != 2 {
		t.Fatalf("the allocation took index %d, want 2 — index 1 is somebody else's", slot.Index)
	}
	if !slices.Contains(host.Namespaces(), "rnr-ns-1") {
		t.Fatal("the namespace this runner did not create was deleted")
	}
	// And it stays retired: nothing hands it out later, and nothing adopts
	// it either.
	if _, err := p.Adopt(1, "mvm-x"); err == nil {
		t.Fatal("a retired index was adopted")
	}
}

// TestATransientCreateFailureDoesNotRetireTheIndex is the other half, and
// the one that matters operationally: a runner that lost CAP_NET_ADMIN for a
// minute, or a create whose context was cancelled, must not come back with a
// permanently smaller host. The index is LEAKED, which is recoverable,
// rather than retired, which is not.
func TestATransientCreateFailureDoesNotRetireTheIndex(t *testing.T) {
	host := NewFakeHost()
	p, err := New(testConfig(2), host)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	host.FailOn("netns-add", errors.New("operation not permitted"))
	for range 4 {
		if _, err := p.Allocate(ctx, "mvm-x"); err == nil {
			t.Fatal("Allocate succeeded with a failing netns-add")
		}
	}

	host.FailOn("netns-add", nil)
	for i := range 2 {
		if _, err := p.Allocate(ctx, fmt.Sprintf("mvm-%d", i)); err != nil {
			t.Fatalf("the host recovered but slot %d was still unavailable: %v", i, err)
		}
	}
}

// TestAdoptRefusesADoubleClaim: two sessions on one slot is two guests on one
// address.
func TestAdoptRefusesADoubleClaim(t *testing.T) {
	p, _ := newTestPool(t, 4)
	if _, err := p.Adopt(3, "mvm-1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	if _, err := p.Adopt(3, "mvm-2"); err == nil {
		t.Fatal("Adopt claimed a slot that is already held")
	}
	if _, err := p.Adopt(0, "mvm-3"); err == nil {
		t.Fatal("Adopt claimed index 0, which is never allocated")
	}
	if _, err := p.Adopt(5, "mvm-4"); err == nil {
		t.Fatal("Adopt claimed an index outside the host's envelope")
	}
}

// TestPoolExhaustionIsACapacityAnswer, not a panic and not a duplicate slot.
func TestPoolExhaustionIsACapacityAnswer(t *testing.T) {
	p, _ := newTestPool(t, 2)
	ctx := context.Background()
	for i := range 2 {
		if _, err := p.Allocate(ctx, fmt.Sprintf("mvm-%d", i)); err != nil {
			t.Fatalf("Allocate %d: %v", i, err)
		}
	}
	_, err := p.Allocate(ctx, "mvm-over")
	if !errors.Is(err, ErrNoSlots) {
		t.Fatalf("third Allocate on a two-slot host = %v, want ErrNoSlots", err)
	}
}

func TestConfigRefusesWhatItCannotName(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{"no slots", Config{Slots: 0}, "slot count is required"},
		{"host bits set", Config{Slots: 1, GuestCIDR: "10.201.0.5/16"}, "host bits"},
		{"overlapping ranges", Config{Slots: 1, GuestCIDR: "10.201.0.0/16", UplinkCIDR: "10.201.0.0/16"}, "overlap"},
		{"too many slots", Config{Slots: 100, GuestCIDR: "10.201.0.0/24", UplinkCIDR: "10.202.0.0/24"}, "do not fit"},
		{"ipv6 guest range", Config{Slots: 1, GuestCIDR: "fd00::/64"}, "not IPv4"},
		{"proxy with no port", Config{Slots: 1, ProxyAddr: "10.44.0.9"}, "without a usable port"},
		{"proxy that is a name", Config{Slots: 1, ProxyAddr: "egressd", ProxyPort: 3128}, "not an IP address"},
		{"prefix too long", Config{Slots: 9999, NamePrefix: "rainierx"}, "over the kernel's"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(tc.cfg, NewFakeHost())
			if err == nil {
				t.Fatalf("New(%+v) succeeded", tc.cfg)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want it to name %q", err, tc.want)
			}
		})
	}
}

func TestNewRefusesWithoutAHost(t *testing.T) {
	if _, err := New(testConfig(1), nil); err == nil {
		t.Fatal("New succeeded with no Host; there is no implementation that pretends to build a network")
	}
}

func assertOps(t *testing.T, got, want []Op) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("recorded %d operations, want %d:\ngot:  %s\nwant: %s", len(got), len(want), opLines(got), opLines(want))
	}
	for i := range got {
		if got[i].String() != want[i].String() {
			t.Fatalf("operation %d = %q, want %q\ngot:  %s\nwant: %s", i, got[i], want[i], opLines(got), opLines(want))
		}
	}
}

func opLines(ops []Op) string {
	var b strings.Builder
	for _, op := range ops {
		b.WriteString("\n  ")
		b.WriteString(op.String())
	}
	return b.String()
}

// internal/driver/microvm_netslot_test.go
//
// What the driver does with a network slot: one per running session, given
// back when the session stops occupying the host, and re-associated rather
// than rebuilt when a session outlives its runnerd.
package driver

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/tokencanopy/rainier/internal/driver/netslot"
)

// liveGuestAddresses is every address the driver currently has a live record
// for, sorted. It reads the driver's own map because that is the thing under
// test: two records naming one address is the failure this whole package
// exists to prevent.
func liveGuestAddresses(m *Microvm) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for _, inst := range m.instances {
		if inst.Cfg.GuestIP != "" {
			out = append(out, inst.Cfg.GuestIP)
		}
	}
	slices.Sort(out)
	return out
}

// instanceConfig is the recorded configuration for one instance.
func instanceConfig(m *Microvm, id string) (VMMConfig, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	inst, ok := m.instances[id]
	if !ok {
		return VMMConfig{}, false
	}
	return inst.Cfg, true
}

// TestMicrovmGivesEachSessionItsOwnAddress is the property the hard-coded
// 172.18.0.2 did not have. Two sessions on one host is the ordinary case
// (ADR-0003 §5.1 sizes for four to six), and two guests on one address is a
// broken host, not a busy one.
func TestMicrovmGivesEachSessionItsOwnAddress(t *testing.T) {
	m, sim, _ := testMicrovmNet(t, MicrovmOpts{TotalSlots: 4})
	ctx := context.Background()

	a, err := m.Create(ctx, Spec{SessionID: "alpha"})
	if err != nil {
		t.Fatalf("Create alpha: %v", err)
	}
	b, err := m.Create(ctx, Spec{SessionID: "beta"})
	if err != nil {
		t.Fatalf("Create beta: %v", err)
	}

	cfgA, okA := sim.Config(a.ID)
	cfgB, okB := sim.Config(b.ID)
	if !okA || !okB {
		t.Fatal("the engine was not handed a configuration for both sessions")
	}

	for _, pair := range [][2]string{
		{cfgA.GuestIP, cfgB.GuestIP},
		{cfgA.GatewayIP, cfgB.GatewayIP},
		{cfgA.GuestMAC, cfgB.GuestMAC},
		{cfgA.TapDevice, cfgB.TapDevice},
		{cfgA.Netns, cfgB.Netns},
	} {
		if pair[0] == "" || pair[1] == "" {
			t.Fatalf("a session was configured with an empty network identity: %q / %q", pair[0], pair[1])
		}
		if pair[0] == pair[1] {
			t.Fatalf("both sessions were given %q", pair[0])
		}
	}
	if cfgA.SlotIndex == cfgB.SlotIndex {
		t.Fatalf("both sessions took slot %d", cfgA.SlotIndex)
	}

	// The kernel command line carries the slot's address, not a constant.
	args := bootArgs(cfgA)
	want := "ip=" + cfgA.GuestIP + "::" + cfgA.GatewayIP + ":" + cfgA.GuestNetmask + ":guest:eth0:off"
	if !strings.Contains(args, want) {
		t.Fatalf("boot args = %q, want them to carry %q", args, want)
	}
	if strings.Contains(args, "172.18.0.") {
		t.Fatalf("boot args still carry the old hard-coded address: %q", args)
	}
}

// TestMicrovmBootArgsWithoutASlotCarryNoAddress: a configuration with no slot
// gets no `ip=` rather than a default that would put the guest on someone
// else's /30.
func TestMicrovmBootArgsWithoutASlotCarryNoAddress(t *testing.T) {
	if got := bootArgs(VMMConfig{}); strings.Contains(got, "ip=") {
		t.Fatalf("boot args for a slotless configuration = %q, want no ip= at all", got)
	}
}

// TestMicrovmColdSuspendReturnsTheSlot: a cold-parked session is not
// occupying this host, so it must not hold a /30 and a namespace for a
// dormant window nobody bounded.
func TestMicrovmColdSuspendReturnsTheSlot(t *testing.T) {
	m, sim, net := testMicrovmNet(t, MicrovmOpts{TotalSlots: 1})
	m.SetHost(&stubMicrovmHost{})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{SessionID: "alpha"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if got := len(net.Namespaces()); got != 1 {
		t.Fatalf("namespaces after create = %d, want 1", got)
	}

	if err := m.Suspend(ctx, h.ID, false); err != nil {
		t.Fatalf("cold Suspend: %v", err)
	}
	if got := net.Namespaces(); len(got) != 0 {
		t.Fatalf("namespaces after a cold suspend = %v, want none", got)
	}

	// The one slot on this host is free, so another session can have it.
	other, err := m.Create(ctx, Spec{SessionID: "beta"})
	if err != nil {
		t.Fatalf("Create beta after alpha parked: %v", err)
	}
	otherCfg, _ := sim.Config(other.ID)
	if otherCfg.SlotIndex != 1 {
		t.Fatalf("beta took slot %d, want the recycled 1", otherCfg.SlotIndex)
	}
}

// TestMicrovmColdResumeTakesAFreshSlot: the slot a session had was given
// back, and may be someone else's by now.
func TestMicrovmColdResumeTakesAFreshSlot(t *testing.T) {
	m, sim, net := testMicrovmNet(t, MicrovmOpts{TotalSlots: 4})
	m.SetHost(&stubMicrovmHost{})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{SessionID: "alpha"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := m.Suspend(ctx, h.ID, false); err != nil {
		t.Fatalf("cold Suspend: %v", err)
	}
	// Someone else takes the freed index while alpha is dormant.
	if _, err := m.Create(ctx, Spec{SessionID: "beta"}); err != nil {
		t.Fatalf("Create beta: %v", err)
	}

	restarted, err := m.Resume(ctx, h.ID)
	if err != nil {
		t.Fatalf("cold Resume: %v", err)
	}
	if !restarted {
		t.Fatal("a cold resume reported no restart")
	}
	cfg, _ := sim.Config(h.ID)
	if cfg.SlotIndex == 0 {
		t.Fatal("the resumed session was launched with no network slot")
	}
	if got := len(net.Namespaces()); got != 2 {
		t.Fatalf("namespaces after the resume = %d, want 2 (alpha and beta)", got)
	}

	// Two live sessions, two distinct addresses.
	addrs := liveGuestAddresses(m)
	if len(addrs) != 2 {
		t.Fatalf("live guest addresses = %v, want 2", addrs)
	}
	if addrs[0] == addrs[1] {
		t.Fatalf("both live sessions are on %s", addrs[0])
	}
}

// TestMicrovmSessionsAreBehindTheirOwnFirewall: the ruleset the host was
// given names the session's own TAP and the proxy this runner was started
// with, and it is gone when the session is.
func TestMicrovmSessionsAreBehindTheirOwnFirewall(t *testing.T) {
	m, sim, net := testMicrovmNet(t, MicrovmOpts{
		TotalSlots:        4,
		EgressProxyAddr:   "10.44.0.9",
		EgressProxyPort:   3128,
		ControlPlaneCIDRs: []string{"203.0.113.0/24"},
	})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{SessionID: "alpha"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	cfg, _ := sim.Config(h.ID)

	rules, ok := net.Ruleset(cfg.Netns)
	if !ok {
		t.Fatalf("no firewall was installed in %s", cfg.Netns)
	}
	for _, want := range []string{
		`iifname != "` + cfg.TapDevice + `" return`,
		"ip daddr 10.44.0.9 tcp dport 3128 accept",
		"ip daddr 169.254.169.254 drop",
		"203.0.113.0/24",
	} {
		if !strings.Contains(rules, want) {
			t.Fatalf("the session's ruleset does not carry %q:\n%s", want, rules)
		}
	}

	if err := m.Destroy(ctx, h.ID); err != nil {
		t.Fatalf("Destroy: %v", err)
	}
	if _, ok := net.Ruleset(cfg.Netns); ok {
		t.Fatal("the session's ruleset outlived the session")
	}
}

// TestMicrovmResumeOntoAVanishedRecordLeavesNothingRunning is the race
// between a cold resume and a destroy: the resume launches a VM, and by the
// time it goes to record it there is no record left.
//
// Before, the VM stayed running with nothing on this host naming it — a
// jailed Firecracker, a jail directory, a uid and a network slot — and the
// resume's own cleanup then took its namespace away underneath it. Now it is
// stopped first, and only then does the slot go back.
func TestMicrovmResumeOntoAVanishedRecordLeavesNothingRunning(t *testing.T) {
	engine := &parkedLaunchEngine{
		SimulatedEngine: NewSimulatedEngine(),
		entered:         make(chan struct{}, 1),
		release:         make(chan struct{}),
	}
	m, _, net := testMicrovmNet(t, MicrovmOpts{TotalSlots: 2, Engine: engine})
	m.SetHost(&stubMicrovmHost{})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{SessionID: "alpha"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := m.Suspend(ctx, h.ID, false); err != nil {
		t.Fatalf("cold Suspend: %v", err)
	}

	engine.park()
	done := make(chan error, 1)
	go func() {
		_, err := m.Resume(ctx, h.ID)
		done <- err
	}()

	<-engine.entered
	// The destroy that raced it: the record is gone while the resume is
	// still inside the hypervisor.
	m.mu.Lock()
	delete(m.instances, h.ID)
	m.mu.Unlock()
	close(engine.release)

	if err := <-done; err == nil {
		t.Fatal("a resume onto a record that no longer exists reported success")
	}

	if st, _ := engine.State(ctx, h.ID); st == VMMStateRunning {
		t.Error("the VM the resume started is still running with nothing naming it")
	}
	if got := net.Namespaces(); len(got) != 0 {
		t.Errorf("namespaces after the race = %v, want none", got)
	}
}

// TestMicrovmDestroyReturnsTheSlot.
func TestMicrovmDestroyReturnsTheSlot(t *testing.T) {
	m, _, net := testMicrovmNet(t, MicrovmOpts{TotalSlots: 2})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{SessionID: "alpha"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := m.Destroy(ctx, h.ID); err != nil {
		t.Fatalf("Destroy: %v", err)
	}
	if got := net.Namespaces(); len(got) != 0 {
		t.Fatalf("namespaces after destroy = %v, want none", got)
	}
	if got := net.Links(); len(got) != 0 {
		t.Fatalf("links after destroy = %v, want none", got)
	}
}

// TestMicrovmFailedLaunchLeavesNoSlotBehind is the leak the undo stack is
// there for: a create that dies at the hypervisor must not leave a namespace
// and a /30 allocated to a session that does not exist.
func TestMicrovmFailedLaunchLeavesNoSlotBehind(t *testing.T) {
	m, sim, net := testMicrovmNet(t, MicrovmOpts{TotalSlots: 2})
	sim.FailLaunch(errors.New("the VMM refused to start"))
	ctx := context.Background()

	if _, err := m.Create(ctx, Spec{SessionID: "alpha"}); err == nil {
		t.Fatal("Create succeeded with a failing engine")
	}
	if got := net.Namespaces(); len(got) != 0 {
		t.Fatalf("namespaces after a failed create = %v, want none", got)
	}

	// And the slot is back in the pool, not merely torn down.
	sim.FailLaunch(nil)
	h, err := m.Create(ctx, Spec{SessionID: "beta"})
	if err != nil {
		t.Fatalf("Create after the engine recovered: %v", err)
	}
	cfg, _ := sim.Config(h.ID)
	if cfg.SlotIndex != 1 {
		t.Fatalf("the recovered create took slot %d, want the returned 1", cfg.SlotIndex)
	}
}

// TestMicrovmRecoveryReassociatesLiveSlotsAndReclaimsTheRest is the crash
// case: a runnerd dies holding two sessions and a third session's namespace
// that nothing remembers. The live one keeps its network; the orphan's is
// torn down.
func TestMicrovmRecoveryReassociatesLiveSlotsAndReclaimsTheRest(t *testing.T) {
	stateDir := shortTempDir(t)
	net := netslot.NewFakeHost()
	ctx := context.Background()

	first, sim, _ := testMicrovmNet(t, MicrovmOpts{TotalSlots: 4, StateDir: stateDir, Net: net})
	live, err := first.Create(ctx, Spec{SessionID: "alpha"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	liveCfg, _ := sim.Config(live.ID)

	// A namespace nobody has a record for: a session whose instance metadata
	// was removed, or a create that died between the slot and the record.
	net.Preexisting("rnr-ns-4")

	// A second driver over the same state directory, as a restarted runnerd
	// would be. The simulated engine is rebuilt from the same directory so
	// the recovered record still reads as running.
	second, _, _ := testMicrovmNet(t, MicrovmOpts{TotalSlots: 4, StateDir: stateDir, Net: net})

	if got := net.Namespaces(); len(got) != 1 || got[0] != liveCfg.Netns {
		t.Fatalf("namespaces after recovery = %v, want only the live session's %s", got, liveCfg.Netns)
	}

	// The live session's index is still held: a new create must not get it.
	fresh, err := second.Create(ctx, Spec{SessionID: "beta"})
	if err != nil {
		t.Fatalf("Create on the recovered driver: %v", err)
	}
	freshCfg, ok := instanceConfig(second, fresh.ID)
	if !ok {
		t.Fatal("no record for the new session")
	}
	if freshCfg.SlotIndex == liveCfg.SlotIndex {
		t.Fatalf("the new session took slot %d, which the recovered session still holds", freshCfg.SlotIndex)
	}
}

// TestMicrovmRecoveryDropsAColdSessionsSlotClaim: a cold-parked session has
// no network, and a record that still named one would keep an index off the
// pool for nothing.
func TestMicrovmRecoveryDropsAColdSessionsSlotClaim(t *testing.T) {
	stateDir := shortTempDir(t)
	net := netslot.NewFakeHost()
	ctx := context.Background()

	first, _, _ := testMicrovmNet(t, MicrovmOpts{TotalSlots: 1, StateDir: stateDir, Net: net})
	first.SetHost(&stubMicrovmHost{})
	h, err := first.Create(ctx, Spec{SessionID: "alpha"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := first.Suspend(ctx, h.ID, false); err != nil {
		t.Fatalf("cold Suspend: %v", err)
	}

	second, _, _ := testMicrovmNet(t, MicrovmOpts{TotalSlots: 1, StateDir: stateDir, Net: net})
	if _, err := second.Create(ctx, Spec{SessionID: "beta"}); err != nil {
		t.Fatalf("Create after recovery: %v; the parked session's slot claim was never dropped", err)
	}
}

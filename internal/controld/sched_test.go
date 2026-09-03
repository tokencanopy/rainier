// internal/controld/sched_test.go
//
// sched.go is gone: placement, the FIFO drain, the create dispatch, and the
// spec it builds are controlapp.FleetService's now. What survives here is the
// half those tests could only ever prove from the outside — that controld's
// adapters hand the service a fleet it can actually place on, over real
// websockets, with an explicit runner pin spelled as the capability Task 1
// encodes (placement:<name>) and a snapshot's affinity answered by the
// installation's checkpoint locator.
//
// Every test that called createSpec, dispatchCreate, pickRunner,
// pickForSession, or drainQueue directly is either covered by a named
// controlapp test or rewritten against the adapter that replaced it; the
// task's report lists each by name.
package controld

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// cmdRecorder collects session ids in the order fakeRunner receives "create"
// commands for them — the evidence the FIFO/placement tests assert on.
type cmdRecorder struct {
	mu  sync.Mutex
	ids []string
}

func (r *cmdRecorder) add(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ids = append(r.ids, id)
}

func (r *cmdRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string{}, r.ids...)
}

func (r *cmdRecorder) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.ids)
}

// sameSet reports whether a and b contain the same elements, ignoring
// order and duplicates.
func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]bool, len(a))
	for _, s := range a {
		set[s] = true
	}
	for _, s := range b {
		if !set[s] {
			return false
		}
	}
	return true
}

// ackCreate replies to a dispatched command using fakeRunner's own locked
// writer, without going through fakeRunner.reply — that helper calls
// t.Fatalf, which only the goroutine running the test may do (here we're
// answering from a background goroutine that outlives any single assertion).
func ackCreate(t *testing.T, f *fakeRunner, cmd runner.ToRunner, ok bool, detail string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	f.wmu.Lock()
	defer f.wmu.Unlock()
	m := runner.FromRunner{Type: "result", ReqID: cmd.ReqID, OK: ok, Detail: detail, Used: f.used, Total: f.total}
	if err := wsjson.Write(ctx, f.c, m); err != nil {
		t.Logf("ackCreate: write: %v", err)
	}
}

// autoAckCreates answers every "create" fakeRunner receives with ok:true,
// recording each session id in arrival order, until the test ends. It
// deliberately leaves the runner's reported capacity untouched — a create
// ack alone must never look like it grew Used; only an explicit event does
// that in these tests, mirroring the "creating rows aren't in the runner's
// docker count yet" gap the scheduler's free-capacity formula compensates
// for.
func autoAckCreates(t *testing.T, f *fakeRunner) *cmdRecorder {
	t.Helper()
	rec := &cmdRecorder{}
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	go func() {
		for {
			select {
			case <-done:
				return
			case cmd := <-f.cmds:
				if cmd.Type == "create" {
					rec.add(cmd.Session)
					ackCreate(t, f, cmd, true, "")
				}
			}
		}
	}()
	return rec
}

// seedQueued seeds a queued session with a distinct, deterministic
// CreatedAt so OldestQueued's FIFO ordering doesn't depend on wall-clock
// resolution between seeds.
func seedQueued(t *testing.T, st MemStore, id string, offset int) control.Session {
	t.Helper()
	base := time.Now().Add(-time.Hour)
	return seedSession(t, st, control.Session{
		ID:        control.SessionID(id),
		State:     control.StateQueued,
		Name:      id,
		Spec:      control.PortableSpec{Image: "img:latest"},
		CreatedAt: base.Add(time.Duration(offset) * time.Second),
	})
}

// startRun starts the scheduler loop and arranges for it to stop when the
// test ends.
func startRun(t *testing.T, s *Server) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go s.Run(ctx)
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

// TestPickRunner and TestPickForSession are deleted, not replaced: the
// functions they exercised are controlapp's now, and its own tests cover the
// same table.
//
//   - TestPickRunner (max free wins / lexicographic tie / zero free / empty /
//     negative free) -> controlapp TestPickRunnerCapacityAndTiebreak.
//   - TestPickForSession's pin rows (a pin wins over more capacity, a pin to a
//     full or absent runner places nothing, the pin outranks the snapshot
//     holder) -> controlapp TestSchedulerCapabilityFiltering, which is the same
//     rule spelled as a capability requirement, plus the two integration tests
//     below that drive it through a real fleet.
//   - TestPickForSession's snapshot rows (the holder wins over more capacity,
//     and wins a lexicographic tie) -> controlapp TestPlacementPrefersTheSnapshotHolder,
//     driven end to end by TestCacheTiebreakPrefersTheSnapshotHolder below.
//   - TestPickForSession's two FALLBACK rows ("a full snapshot holder falls
//     back to the normal pick", "a disconnected snapshot holder falls back to
//     the normal pick") are back under deviation D17, which restores what O8's
//     D3 traded away: controlapp TestPlacementFallsBackWhenTheHolderIsFull and
//     the two api_test.go cases that assert the fallback boots the plain image
//     with its setup on the runner that has room.

// TestPlacementPinPlacesOnThePinnedRunner drives the pin through the real
// scheduler: vm1 has strictly more free capacity, so the session landing on
// vm2 can only be the environment's placement.
func TestPlacementPinPlacesOnThePinnedRunner(t *testing.T) {
	s, st, ts := newTestControld(t)
	f1 := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
	f2 := joinRunner(t, s, ts, runnerScript{Name: "vm2", Total: 1})
	autoAckCreates(t, f1)
	autoAckCreates(t, f2)

	env := seedEnv(t, st, control.Environment{Name: "pinned", Image: "img:1", Requirements: control.Requirements{Capabilities: []string{placementCapabilityPrefix + "vm2"}}})
	seedSession(t, st, control.Session{ID: "sess_pinned", State: control.StateQueued, Name: "pinned1",
		EnvironmentID: env.ID, Spec: control.PortableSpec{Image: env.Image}, CreatedAt: time.Now().Add(-time.Hour)})

	startRun(t, s)

	eventually(t, 3*time.Second, func() error {
		got := getSession(t, st, "sess_pinned")
		if got.State != control.StateCreating || got.RunnerID != "vm2" {
			return fmt.Errorf("session = %q on %q, want creating on vm2 (the pin)", got.State, got.RunnerID)
		}
		return nil
	})
}

// TestPlacementPinQueuesWhenTheRunnerHasNoRoom pins both halves of the
// blocked-pin rule: the pinned session waits (it is never placed elsewhere),
// and — the part a naive "stop the pass" scheduler gets wrong — a younger
// unpinned session behind it is still placed.
func TestPlacementPinQueuesWhenTheRunnerHasNoRoom(t *testing.T) {
	t.Run("pinned to a runner with no free slot", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f1 := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		// vm2 announces itself full, so the pin has nowhere to land.
		joinRunner(t, s, ts, runnerScript{Name: "vm2", Used: 1, Total: 1})
		rec := autoAckCreates(t, f1)

		env := seedEnv(t, st, control.Environment{Name: "pinned", Image: "img:1", Requirements: control.Requirements{Capabilities: []string{placementCapabilityPrefix + "vm2"}}})
		seedSession(t, st, control.Session{ID: "sess_blocked", State: control.StateQueued, Name: "blocked",
			EnvironmentID: env.ID, Spec: control.PortableSpec{Image: env.Image}, CreatedAt: time.Now().Add(-time.Hour)})
		seedQueued(t, st, "sess_behind", 1)

		startRun(t, s)

		// The unpinned session behind it places on vm1...
		wantState(t, st, "sess_behind", control.StateCreating)
		if got := rec.snapshot(); !sameSet(got, []string{"sess_behind"}) {
			t.Fatalf("vm1 received creates for %v, want only sess_behind", got)
		}
		// ...while the pinned one stays queued and unplaced.
		got := getSession(t, st, "sess_blocked")
		if got.State != control.StateQueued || got.RunnerID != "" {
			t.Fatalf("pinned session = %q on %q, want still queued and unplaced", got.State, got.RunnerID)
		}
	})

	t.Run("pinned to a runner that never connected", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		rec := autoAckCreates(t, f)

		env := seedEnv(t, st, control.Environment{Name: "hardware", Image: "img:1", Requirements: control.Requirements{Capabilities: []string{placementCapabilityPrefix + "rainier-gpu"}}})
		seedSession(t, st, control.Session{ID: "sess_hw", State: control.StateQueued, Name: "hw",
			EnvironmentID: env.ID, Spec: control.PortableSpec{Image: env.Image}, CreatedAt: time.Now().Add(-time.Hour)})
		seedQueued(t, st, "sess_any", 1)

		startRun(t, s)

		wantState(t, st, "sess_any", control.StateCreating)
		if got := rec.snapshot(); !sameSet(got, []string{"sess_any"}) {
			t.Fatalf("vm1 received creates for %v, want only sess_any", got)
		}
		if got := getSession(t, st, "sess_hw"); got.State != control.StateQueued || got.RunnerID != "" {
			t.Fatalf("pinned session = %q on %q, want still queued and unplaced", got.State, got.RunnerID)
		}
	})
}

// TestCacheTiebreakPrefersTheSnapshotHolder drives the holder preference
// through the real scheduler and this installation's checkpoint locator: vm1
// has more free capacity, but the environment's snapshot exists only in vm2's
// local image store, so the session goes there and resolves to the ref.
func TestCacheTiebreakPrefersTheSnapshotHolder(t *testing.T) {
	s, st, ts := newTestControld(t)
	f1 := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
	f2 := joinRunner(t, s, ts, runnerScript{Name: "vm2", Total: 1})
	autoAckCreates(t, f1)
	autoAckCreates(t, f2)

	env := seedEnv(t, st, control.Environment{Name: "cached", Image: "img:1", Setup: "echo hi"})
	const ref = "rainier-env:cached-0123456789ab"
	env = cacheEnvSnapshot(t, st, env, ref, "vm2")

	// Stored as a create stores it now (D16): the environment's own image.
	seedSession(t, st, control.Session{ID: "sess_cached", State: control.StateQueued, Name: "cached1",
		EnvironmentID: env.ID, Spec: control.PortableSpec{Image: env.Image}, CreatedAt: time.Now().Add(-time.Hour)})

	startRun(t, s)

	eventually(t, 3*time.Second, func() error {
		got := getSession(t, st, "sess_cached")
		if got.State != control.StateCreating || got.RunnerID != "vm2" {
			return fmt.Errorf("session = %q on %q, want creating on vm2 (the snapshot holder)", got.State, got.RunnerID)
		}
		if got.Spec.Image != ref {
			return fmt.Errorf("resolved image = %q, want the snapshot %q the holder has", got.Spec.Image, ref)
		}
		return nil
	})
}

// TestSchedulerFIFOPlacementAndCapacityFrees is the mandated scheduler flow
// test: a 2-slot runner, 3 queued rows, exactly the oldest 2 dispatched and
// left `creating`, the third staying queued until a slot actually frees —
// which a `running` event must NOT trigger (Used and the creating-count
// subtraction move together), but a `dead` event must.
func TestSchedulerFIFOPlacementAndCapacityFrees(t *testing.T) {
	s, st, ts := newTestControld(t)
	f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 2,
		Sessions: []runner.SessionInfo{{ID: ghostSession, State: "running"}}})
	waitConnected(t, s, "vm1")
	awaitReconciled(t, f)

	rec := autoAckCreates(t, f)

	seedQueued(t, st, "sess_q1", 0)
	seedQueued(t, st, "sess_q2", 1)
	seedQueued(t, st, "sess_q3", 2)

	startRun(t, s)

	eventually(t, 2*time.Second, func() error {
		if n := rec.len(); n != 2 {
			return fmt.Errorf("dispatched %d creates so far, want 2", n)
		}
		return nil
	})
	// Placement (the queued->creating transition) is strictly sequential and
	// FIFO; dispatch to the wire that follows it is concurrent, so the two
	// commands can arrive in either order — what's pinned is *which* two
	// sessions were chosen (the oldest), not the wire order they land in.
	if got := rec.snapshot(); len(got) != 2 || !sameSet(got, []string{"sess_q1", "sess_q2"}) {
		t.Fatalf("dispatched = %v, want exactly {sess_q1, sess_q2} (the oldest two)", got)
	}
	wantState(t, st, "sess_q1", control.StateCreating)
	wantState(t, st, "sess_q2", control.StateCreating)

	// No capacity left: sess_q3 waits, and keeps waiting through a mere
	// "running" event — Used catching up to 1 is exactly offset by the
	// creating-count dropping to 1, so no net capacity appears.
	f.setCapacity(1, 2)
	f.event(t, "sess_q1", "running")
	wantState(t, st, "sess_q1", control.StateRunning)
	time.Sleep(150 * time.Millisecond)
	if got := getSession(t, st, "sess_q3"); got.State != control.StateQueued {
		t.Fatalf("sess_q3 state = %q, want still queued (a running event must not free a slot)", got.State)
	}
	if n := rec.len(); n != 2 {
		t.Fatalf("dispatched %d creates, want still 2", n)
	}

	// A slot only frees once the session actually terminates.
	f.setCapacity(0, 2)
	f.event(t, "sess_q1", "dead")
	wantState(t, st, "sess_q1", control.StateDead)

	eventually(t, 2*time.Second, func() error {
		got := getSession(t, st, "sess_q3")
		if got.State != control.StateCreating {
			return fmt.Errorf("sess_q3 state = %q, want creating", got.State)
		}
		if got.RunnerID != "vm1" {
			return fmt.Errorf("sess_q3 runner = %q, want vm1", got.RunnerID)
		}
		return nil
	})

	// A running event frees no slot, but it does clear freeCapacity's
	// double-count: while a row is `creating` AND its container is already in
	// the runner's reported Used, one slot of real headroom is invisible.
	// vm1 now reports used 0/2 with sess_q2 and sess_q3 both `creating`, so
	// free reads 0 and a fourth session waits — until sess_q2 goes running,
	// which drops the creating count to 1 and must WAKE the scheduler. The
	// 2s bound is the assertion: the 10s safety tick would also get there
	// eventually (the burst e2e measured 20s of exactly that), so anything
	// inside this bound is the wake doing its job.
	seedQueued(t, st, "sess_q4", 3)
	time.Sleep(150 * time.Millisecond)
	if got := getSession(t, st, "sess_q4"); got.State != control.StateQueued {
		t.Fatalf("sess_q4 state = %q, want queued (no headroom until a creating row clears)", got.State)
	}

	f.event(t, "sess_q2", "running")
	eventually(t, 2*time.Second, func() error {
		got := getSession(t, st, "sess_q4")
		if got.State != control.StateCreating || got.RunnerID != "vm1" {
			return fmt.Errorf("sess_q4 = %q on %q, want creating on vm1 promptly after the running event", got.State, got.RunnerID)
		}
		return nil
	})
}

// TestCreateDispatchFailureRequeues covers the dispatch-failure paths, which
// differ in exactly one thing — whether the create was ever delivered:
// ok:false fails the session outright; a connection that dies requeues it
// (with placement cleared); and a timeout on a still-live connection does
// NEITHER, because the runner has the command and may be executing it.
func TestCreateDispatchFailureRequeues(t *testing.T) {
	t.Run("ok false fails the session with the runner's detail", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 2,
			Sessions: []runner.SessionInfo{{ID: ghostSession, State: "running"}}})
		waitConnected(t, s, "vm1")
		awaitReconciled(t, f)

		id := "sess_boom"
		seedQueued(t, st, id, 0)

		startRun(t, s)

		cmd := f.nextCmd(t)
		if cmd.Type != "create" || cmd.Session != id {
			t.Fatalf("got %+v, want create of %s", cmd, id)
		}
		ackCreate(t, f, cmd, false, "boom")

		got := wantState(t, st, id, control.StateFailed)
		if got.Error != "boom" {
			t.Fatalf("error = %q, want %q", got.Error, "boom")
		}
	})

	// The two subtests that called dispatchCreate directly are deleted with
	// it: controlapp owns the uncertain-delivery rule now and pins both halves
	// in TestDispatchCreateFailureAndUncertainDelivery — "connection death
	// requeues with runner cleared" and "timeout on a live connection leaves
	// the row creating", under those exact names, driving its own
	// dispatchCreate the same way these did.
}

// pinFailStore fails every SetSessionSetupHash, so a create whose setup
// provenance cannot be recorded can be observed end to end.
type pinFailStore struct {
	MemStore
	err error
}

func (p *pinFailStore) Sessions() control.SessionRepository {
	return pinFailSessions{SessionRepository: p.MemStore.Sessions(), owner: p}
}

type pinFailSessions struct {
	control.SessionRepository
	owner *pinFailStore
}

func (p pinFailSessions) SetSessionSetupHash(ctx context.Context, ws control.WorkspaceID, id control.SessionID, hash string) error {
	return p.owner.err
}

// TestSetupPinIsWrittenBeforeTheCreate pins both halves of the provenance
// write: a create carrying a setup script records the hash of exactly that
// script BEFORE the command goes out, and a create that cannot record it fails
// the session instead of running an unattributable setup.
//
// The three cases used to call dispatchCreate directly. They now go through
// the real scheduler (Run -> the fleet service's drain -> its dispatch -> this
// adapter's transport and store), because that is what the deletion left worth
// asserting here: the pin is written by controlapp, but the hash it writes has
// to be the one THIS store's SetupHash produces, or a cached snapshot is never
// reused. Nothing else in either package pins that equality end to end.
func TestSetupPinIsWrittenBeforeTheCreate(t *testing.T) {
	t.Run("recorded before the command is sent", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})

		env := seedEnv(t, st, control.Environment{Name: "dev", Image: "img:1", Setup: "make deps"})
		seedSession(t, st, control.Session{ID: "sess_pin", State: control.StateQueued, Name: "pin",
			EnvironmentID: env.ID, Spec: control.PortableSpec{Image: env.Image}, CreatedAt: time.Now().Add(-time.Hour)})

		startRun(t, s)

		cmd := nextCreate(t, f)
		// The command is out, so the pin — written first — is already there.
		got := getSession(t, st, "sess_pin")
		if want := SetupHash(env.Image, env.Setup); got.SetupHash != want {
			t.Fatalf("setup hash = %q, want %q recorded before the create went out", got.SetupHash, want)
		}
		if cmd.Spec == nil || cmd.Spec.Setup != env.Setup {
			t.Fatalf("dispatched spec = %+v, want the environment's setup", cmd.Spec)
		}
		f.reply(t, cmd, true, "")
	})

	t.Run("a scratch create pins nothing", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		seedQueued(t, st, "sess_nopin", 0)

		startRun(t, s)

		cmd := nextCreate(t, f)
		f.reply(t, cmd, true, "")
		if got := getSession(t, st, "sess_nopin"); got.SetupHash != "" {
			t.Fatalf("setup hash = %q, want empty — no script was dispatched", got.SetupHash)
		}
	})

	t.Run("a pin that cannot be written fails the session and sends nothing", func(t *testing.T) {
		store := &pinFailStore{MemStore: NewMemStore(), err: errors.New("store down")}
		s, ts := newTestControldOver(t, store)
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})

		env := seedEnv(t, store, control.Environment{Name: "dev", Image: "img:1", Setup: "make deps"})
		seedSession(t, store, control.Session{ID: "sess_pinfail", State: control.StateQueued, Name: "pinfail",
			EnvironmentID: env.ID, Spec: control.PortableSpec{Image: env.Image}, CreatedAt: time.Now().Add(-time.Hour)})

		startRun(t, s)

		got := wantState(t, store, "sess_pinfail", control.StateFailed)
		if got.Error != "could not record the setup this session runs" {
			t.Fatalf("error = %q, want the provenance failure", got.Error)
		}
		wantNothingQueued(t, s, f) // no create was ever sent
	})
}

// TestDrainQueueStopsWhenNoRunnerHasCapacity is deleted: drainQueue is
// controlapp's drainPool now, and controlapp/scheduler_test.go carries the
// same test under the same name over the service's own fixture.

// ---------------------------------------------------------------------------
// createSpec is gone; launchMaterial (adapt_launch.go) is what resolves the
// same material at dispatch. Three of TestCreateSpecExpandsRepos's four cases
// are already covered by name:
//
//   - "a connector added after the create still gets the git egress" ->
//     TestLaunchMaterialResolvesReposAttributionAndSecrets (the resolver
//     answers gitEgressHosts) plus controlapp TestDispatchCreateUnionsMaterialEgress
//     (the union onto the session's own allowlist, deviation D6).
//   - "the session's stored override wins over the environment" and "an
//     explicit empty override clones nothing even under a connector" ->
//     TestLaunchMaterialSessionReposOverrideConnectors, which pins both halves
//     of the nil-vs-empty rule.
//
// The fourth has no counterpart, so it is rewritten against the adapter here.
// ---------------------------------------------------------------------------

// TestLaunchMaterialMissingOwnerFailsTheCreate is the old
// TestCreateSpecExpandsRepos/"an owner whose row is gone fails the create
// rather than cloning anonymously", rewritten against launchMaterial.
// Attribution is not decoration: a commit chain with no author is a mess to
// untangle after the fact, and the create is cheap to redo. The failure may
// not name the owner it could not read.
func TestLaunchMaterialMissingOwnerFailsTheCreate(t *testing.T) {
	st := NewMemStore()
	env := launchTestEnv(nil, githubConnectorRaw(t, "acme/app", "main"))
	row := control.Session{
		ID: "sess_example", WorkspaceID: installWorkspace, CreatorID: "usr_gone",
		Name: "work", EnvironmentID: "env_example",
	}

	m, err := (launchMaterial{st: st, key: testSecretsKey}).ResolveLaunchMaterial(context.Background(), row, &env)
	if err == nil {
		t.Fatalf("ResolveLaunchMaterial = %+v, want a failure", m)
	}
	if strings.Contains(err.Error(), "usr_gone") {
		t.Fatalf("failure reason %q leaks internal identifiers", err)
	}
	if m.GitAuthorName != "" || m.Repos != nil {
		t.Fatalf("a failed resolve must return no material, got %+v", m)
	}
}

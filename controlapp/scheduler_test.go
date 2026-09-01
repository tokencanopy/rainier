package controlapp

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// ---------------------------------------------------------------------------
// scheduler helpers
// ---------------------------------------------------------------------------

func fleetEventually(t *testing.T, timeout time.Duration, fn func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if err := fn(); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal(fn())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// fleetRunFixture starts the scheduler loop and stops it when the test ends.
func fleetRunFixture(t *testing.T, fx *fleetFixture) {
	t.Helper()
	fleetCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = fx.service.Run(fleetCtx) }()
	t.Cleanup(func() { cancel(); <-done })
}

func fleetMapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func fleetScratchQueued(id control.SessionID, offset int) control.Session {
	return control.Session{
		ID: id, WorkspaceID: "ws_example", State: control.StateQueued,
		PoolID: "pool_example", Spec: control.PortableSpec{Image: "img:latest"},
		CreatedAt: time.Now().Add(-time.Hour).Add(time.Duration(offset) * time.Second),
	}
}

func fleetSeededRunner(id control.RunnerID, total, used int, connected bool) control.Runner {
	return control.Runner{
		ID: id, PoolID: "pool_example", CapacityTotal: total, CapacityUsed: used,
		Connected: connected, Generation: 1,
	}
}

// ---------------------------------------------------------------------------
// Task 4 step 1: the sensitive-material seam
// ---------------------------------------------------------------------------

type fleetSelfHostResolver struct{}

func (fleetSelfHostResolver) ResolveLaunchMaterial(context.Context, control.Session, *control.Environment) (LaunchMaterial, error) {
	return LaunchMaterial{
		Repos:          []runner.RepoSpec{{Owner: "acme", Name: "app", BaseBranch: "main", SessionBranch: "rainier/work", Dir: "app"}},
		GitAuthorName:  "alice",
		GitAuthorEmail: "alice@example.com",
		Environment:    map[string]string{"GITHUB_TOKEN": "sh-secret-1"},
	}, nil
}

type fleetCloudResolver struct{}

func (fleetCloudResolver) ResolveLaunchMaterial(context.Context, control.Session, *control.Environment) (LaunchMaterial, error) {
	return LaunchMaterial{
		Repos:          []runner.RepoSpec{{Owner: "cloudco", Name: "svc", BaseBranch: "develop", SessionBranch: "rainier/work", Dir: "svc"}},
		GitAuthorName:  "bob",
		GitAuthorEmail: "bob@example.com",
		Environment:    map[string]string{"CLOUD_CRED": "cloud-secret-2"},
	}, nil
}

type fleetFailingResolver struct{}

func (fleetFailingResolver) ResolveLaunchMaterial(context.Context, control.Session, *control.Environment) (LaunchMaterial, error) {
	// The error string carries no value: only a fixed sentence.
	return LaunchMaterial{}, errors.New("launch material unavailable")
}

func TestLaunchMaterialResolverIsASeam(t *testing.T) {
	tests := []struct {
		name       string
		resolver   LaunchMaterialResolver
		wantRepos  []runner.RepoSpec
		wantAuthor string
		wantEmail  string
		wantEnv    map[string]string
	}{
		{
			name:       "self-host shaped",
			resolver:   fleetSelfHostResolver{},
			wantRepos:  []runner.RepoSpec{{Owner: "acme", Name: "app", BaseBranch: "main", SessionBranch: "rainier/work", Dir: "app"}},
			wantAuthor: "alice", wantEmail: "alice@example.com",
			wantEnv: map[string]string{"GITHUB_TOKEN": "sh-secret-1"},
		},
		{
			name:       "cloud shaped",
			resolver:   fleetCloudResolver{},
			wantRepos:  []runner.RepoSpec{{Owner: "cloudco", Name: "svc", BaseBranch: "develop", SessionBranch: "rainier/work", Dir: "svc"}},
			wantAuthor: "bob", wantEmail: "bob@example.com",
			wantEnv: map[string]string{"CLOUD_CRED": "cloud-secret-2"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFleetFixtureWithResolver(t, tc.resolver)
			fx.st.seedRunner(fleetSeededRunner("vm1", 4, 0, true))
			fx.st.seedSession(fleetScratchQueued("sess_x", 0))
			fleetRunFixture(t, fx)
			fx.service.Wake("pool_example")

			fleetEventually(t, 2*time.Second, func() error {
				cmds := fx.transport.dispatchedCommands()
				if len(cmds) != 1 {
					return fmt.Errorf("dispatched %d commands, want 1", len(cmds))
				}
				spec := cmds[0].Spec
				if spec == nil {
					return fmt.Errorf("spec is nil")
				}
				if !slices.Equal(spec.Repos, tc.wantRepos) {
					return fmt.Errorf("repos = %+v, want %+v", spec.Repos, tc.wantRepos)
				}
				if spec.GitAuthorName != tc.wantAuthor || spec.GitAuthorEmail != tc.wantEmail {
					return fmt.Errorf("author = %q <%s>, want %q <%s>", spec.GitAuthorName, spec.GitAuthorEmail, tc.wantAuthor, tc.wantEmail)
				}
				if !fleetMapsEqual(spec.Env, tc.wantEnv) {
					return fmt.Errorf("env = %v, want %v", spec.Env, tc.wantEnv)
				}
				return nil
			})
		})
	}
}

func TestDispatchCreateMaterialFailureFailsSafely(t *testing.T) {
	fx := newFleetFixtureWithResolver(t, fleetFailingResolver{})
	fx.st.seedRunner(fleetSeededRunner("vm1", 4, 0, true))
	fx.st.seedSession(fleetScratchQueued("sess_x", 0))
	fleetRunFixture(t, fx)
	fx.service.Wake("pool_example")

	fleetEventually(t, 2*time.Second, func() error {
		row := fleetGetSessionState(t, fx, "ws_example", "sess_x")
		if row.State != control.StateFailed {
			return fmt.Errorf("state = %q, want failed", row.State)
		}
		return nil
	})
	row := fleetGetSessionState(t, fx, "ws_example", "sess_x")
	if row.Error == "" || row.Error == "launch material unavailable" {
		t.Fatalf("error = %q, want a bounded safe reason that is not the resolver's own text", row.Error)
	}
	if len(fx.transport.dispatchedCommands()) != 0 {
		t.Fatal("nothing may be dispatched when material resolution fails")
	}
}

// ---------------------------------------------------------------------------
// Task 4 step 2: capacity and selection through the scheduler
// ---------------------------------------------------------------------------

func TestPickRunnerCapacityAndTiebreak(t *testing.T) {
	t.Run("greatest free capacity wins", func(t *testing.T) {
		fx := newFleetFixture(t)
		fx.st.seedRunner(fleetSeededRunner("vm1", 5, 3, true))
		fx.st.seedRunner(fleetSeededRunner("vm2", 5, 0, true))
		fx.st.seedRunner(fleetSeededRunner("vm3", 3, 2, true))
		fx.st.seedSession(fleetScratchQueued("sess_x", 0))
		fleetRunFixture(t, fx)
		fx.service.Wake("pool_example")
		fleetEventually(t, 2*time.Second, func() error {
			row := fleetGetSessionState(t, fx, "ws_example", "sess_x")
			if row.State != control.StateCreating || row.RunnerID != "vm2" {
				return fmt.Errorf("session = %q on %q, want creating on vm2", row.State, row.RunnerID)
			}
			return nil
		})
	})

	t.Run("tie breaks to ascending runner id", func(t *testing.T) {
		fx := newFleetFixture(t)
		fx.st.seedRunner(fleetSeededRunner("vm2", 4, 1, true))
		fx.st.seedRunner(fleetSeededRunner("vm1", 4, 1, true))
		fx.st.seedSession(fleetScratchQueued("sess_x", 0))
		fleetRunFixture(t, fx)
		fx.service.Wake("pool_example")
		fleetEventually(t, 2*time.Second, func() error {
			row := fleetGetSessionState(t, fx, "ws_example", "sess_x")
			if row.State != control.StateCreating || row.RunnerID != "vm1" {
				return fmt.Errorf("session = %q on %q, want creating on vm1 (ascending tie)", row.State, row.RunnerID)
			}
			return nil
		})
	})

	t.Run("disconnected runners are ineligible", func(t *testing.T) {
		fx := newFleetFixture(t)
		fx.st.seedRunner(fleetSeededRunner("vm_big", 100, 0, false))
		fx.st.seedRunner(fleetSeededRunner("vm_small", 2, 0, true))
		fx.st.seedSession(fleetScratchQueued("sess_x", 0))
		fleetRunFixture(t, fx)
		fx.service.Wake("pool_example")
		fleetEventually(t, 2*time.Second, func() error {
			row := fleetGetSessionState(t, fx, "ws_example", "sess_x")
			if row.State != control.StateCreating || row.RunnerID != "vm_small" {
				return fmt.Errorf("session = %q on %q, want creating on vm_small (connected only)", row.State, row.RunnerID)
			}
			return nil
		})
	})

	t.Run("no free capacity places nothing", func(t *testing.T) {
		fx := newFleetFixture(t)
		fx.st.seedRunner(fleetSeededRunner("vm_full", 2, 2, true))
		fx.st.seedRunner(fleetSeededRunner("vm_over", 2, 3, true)) // over-committed
		fx.st.seedSession(fleetScratchQueued("sess_x", 0))
		fleetRunFixture(t, fx)
		fx.service.Wake("pool_example")
		time.Sleep(100 * time.Millisecond)
		row := fleetGetSessionState(t, fx, "ws_example", "sess_x")
		if row.State != control.StateQueued || row.RunnerID != "" {
			t.Fatalf("session = %q on %q, want still queued and unplaced", row.State, row.RunnerID)
		}
	})
}

func TestSchedulerCapabilityFiltering(t *testing.T) {
	fx := newFleetFixture(t)
	fx.st.seedRunner(control.Runner{ID: "vm_gpu", PoolID: "pool_example", CapacityTotal: 4, Connected: true, Generation: 1, Capabilities: []string{"gpu"}})
	fx.st.seedRunner(control.Runner{ID: "vm_cpu", PoolID: "pool_example", CapacityTotal: 4, Connected: true, Generation: 1, Capabilities: []string{"cpu"}})
	fx.st.seedEnv(control.Environment{
		ID: "env_gpu", WorkspaceID: "ws_example", Name: "gpu", Image: "img:latest",
		Requirements: control.Requirements{Capabilities: []string{"gpu"}},
	})
	fx.st.seedSession(control.Session{
		ID: "sess_gpu", WorkspaceID: "ws_example", State: control.StateQueued,
		PoolID: "pool_example", EnvironmentID: "env_gpu",
		Spec:      control.PortableSpec{Image: "img:latest"},
		CreatedAt: time.Now().Add(-time.Hour),
	})
	fx.st.seedSession(fleetScratchQueued("sess_any", 1))

	fleetRunFixture(t, fx)
	fx.service.Wake("pool_example")

	fleetEventually(t, 2*time.Second, func() error {
		row := fleetGetSessionState(t, fx, "ws_example", "sess_gpu")
		if row.State != control.StateCreating || row.RunnerID != "vm_gpu" {
			return fmt.Errorf("gpu session = %q on %q, want creating on vm_gpu", row.State, row.RunnerID)
		}
		return nil
	})
	// The blocked gpu session does not block the later compatible session.
	fleetEventually(t, 2*time.Second, func() error {
		row := fleetGetSessionState(t, fx, "ws_example", "sess_any")
		if row.State != control.StateCreating || row.RunnerID == "" {
			return fmt.Errorf("plain session = %q on %q, want creating somewhere", row.State, row.RunnerID)
		}
		return nil
	})
}

func TestSchedulerFIFOPlacementAndCapacityFrees(t *testing.T) {
	fx := newFleetFixture(t)
	fx.st.seedRunner(fleetSeededRunner("vm1", 2, 0, true))
	fx.st.seedSession(fleetScratchQueued("sess_q1", 0))
	fx.st.seedSession(fleetScratchQueued("sess_q2", 1))
	fx.st.seedSession(fleetScratchQueued("sess_q3", 2))

	fleetRunFixture(t, fx)
	fx.service.Wake("pool_example")

	fleetEventually(t, 2*time.Second, func() error {
		if n := len(fx.transport.dispatchedCommands()); n != 2 {
			return fmt.Errorf("dispatched %d creates, want 2", n)
		}
		return nil
	})
	dispatched := fx.transport.dispatchedCommands()
	got := make([]string, len(dispatched))
	for i, c := range dispatched {
		got[i] = c.Session
	}
	if !fleetSameSet(got, []string{"sess_q1", "sess_q2"}) {
		t.Fatalf("dispatched = %v, want exactly {sess_q1, sess_q2}", got)
	}
	if row := fleetGetSessionState(t, fx, "ws_example", "sess_q1"); row.State != control.StateCreating {
		t.Fatalf("sess_q1 = %q, want creating", row.State)
	}
	if row := fleetGetSessionState(t, fx, "ws_example", "sess_q2"); row.State != control.StateCreating {
		t.Fatalf("sess_q2 = %q, want creating", row.State)
	}

	// A running event must not free a slot: Used catches up exactly as the
	// creating count drops.
	fx.st.mu.Lock()
	vm1 := fx.st.runners["pool_example"]["vm1"]
	vm1.CapacityUsed = 1
	fx.st.runners["pool_example"]["vm1"] = vm1
	fx.st.mu.Unlock()
	if err := fx.service.ApplyRunnerEvent(fleetCtx, control.RunnerEvent{
		WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "vm1",
		Generation: 1, SessionID: "sess_q1", State: control.StateRunning,
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	if row := fleetGetSessionState(t, fx, "ws_example", "sess_q3"); row.State != control.StateQueued {
		t.Fatalf("sess_q3 = %q, want still queued (running event must not free a slot)", row.State)
	}
	if n := len(fx.transport.dispatchedCommands()); n != 2 {
		t.Fatalf("dispatched %d creates, want still 2", n)
	}

	// A dead event frees a slot and wakes the scheduler.
	fx.st.mu.Lock()
	vm1 = fx.st.runners["pool_example"]["vm1"]
	vm1.CapacityUsed = 0
	fx.st.runners["pool_example"]["vm1"] = vm1
	fx.st.mu.Unlock()
	if err := fx.service.ApplyRunnerEvent(fleetCtx, control.RunnerEvent{
		WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "vm1",
		Generation: 1, SessionID: "sess_q1", State: control.StateDead,
	}); err != nil {
		t.Fatal(err)
	}
	fleetEventually(t, 2*time.Second, func() error {
		row := fleetGetSessionState(t, fx, "ws_example", "sess_q3")
		if row.State != control.StateCreating || row.RunnerID != "vm1" {
			return fmt.Errorf("sess_q3 = %q on %q, want creating on vm1", row.State, row.RunnerID)
		}
		return nil
	})

	// A running event on a creating row clears the double-count and lets a
	// fourth session place.
	fx.st.seedSession(fleetScratchQueued("sess_q4", 3))
	fx.service.Wake("pool_example")
	time.Sleep(150 * time.Millisecond)
	if row := fleetGetSessionState(t, fx, "ws_example", "sess_q4"); row.State != control.StateQueued {
		t.Fatalf("sess_q4 = %q, want queued (no headroom until a creating row clears)", row.State)
	}
	if err := fx.service.ApplyRunnerEvent(fleetCtx, control.RunnerEvent{
		WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "vm1",
		Generation: 1, SessionID: "sess_q2", State: control.StateRunning,
	}); err != nil {
		t.Fatal(err)
	}
	fleetEventually(t, 2*time.Second, func() error {
		row := fleetGetSessionState(t, fx, "ws_example", "sess_q4")
		if row.State != control.StateCreating || row.RunnerID != "vm1" {
			return fmt.Errorf("sess_q4 = %q on %q, want creating on vm1", row.State, row.RunnerID)
		}
		return nil
	})
}

func TestDispatchCreateFailureAndUncertainDelivery(t *testing.T) {
	t.Run("ok false fails the session with the runner detail", func(t *testing.T) {
		fx := newFleetFixture(t)
		fx.st.seedRunner(fleetSeededRunner("vm1", 2, 0, true))
		fx.st.seedSession(fleetScratchQueued("sess_boom", 0))
		fx.transport.dispatchReplies = []runner.FromRunner{{OK: false, Detail: "boom"}}

		fleetRunFixture(t, fx)
		fx.service.Wake("pool_example")
		fleetEventually(t, 2*time.Second, func() error {
			row := fleetGetSessionState(t, fx, "ws_example", "sess_boom")
			if row.State != control.StateFailed {
				return fmt.Errorf("state = %q, want failed", row.State)
			}
			return nil
		})
		row := fleetGetSessionState(t, fx, "ws_example", "sess_boom")
		if row.Error != "boom" {
			t.Fatalf("error = %q, want %q", row.Error, "boom")
		}
	})

	// These two drive dispatchCreate directly rather than through Run: a
	// requeue also wakes the scheduler, and with only one still-connected
	// runner the loop would immediately re-place the row, racing any poll
	// that tries to observe the queued/creating outcome. Calling the dispatch
	// once pins the outcome itself.
	t.Run("connection death requeues with runner cleared", func(t *testing.T) {
		fx := newFleetFixture(t)
		fx.st.seedRunner(fleetSeededRunner("vm1", 2, 0, true))
		row := control.Session{
			ID: "sess_conn", WorkspaceID: "ws_example", State: control.StateCreating,
			PoolID: "pool_example", RunnerID: "vm1",
			Spec: control.PortableSpec{Image: "img:latest"},
		}
		fx.st.seedSession(row)
		fx.transport.dispatchErr = errors.New("connection closed")
		fx.transport.connectedOverrides["pool_example/vm1"] = false

		fx.service.dispatchCreate(context.Background(), "pool_example", row, "vm1", nil)

		got := fleetGetSessionState(t, fx, "ws_example", "sess_conn")
		if got.State != control.StateQueued || got.RunnerID != "" {
			t.Fatalf("session = %q on %q, want queued with runner cleared", got.State, got.RunnerID)
		}
	})

	t.Run("timeout on a live connection leaves the row creating", func(t *testing.T) {
		fx := newFleetFixture(t)
		fx.st.seedRunner(fleetSeededRunner("vm1", 2, 0, true))
		row := control.Session{
			ID: "sess_timeout", WorkspaceID: "ws_example", State: control.StateCreating,
			PoolID: "pool_example", RunnerID: "vm1",
			Spec: control.PortableSpec{Image: "img:latest"},
		}
		fx.st.seedSession(row)
		fx.transport.dispatchErr = errors.New("no result before timeout")
		// Connected stays true: the command was delivered.

		fx.service.dispatchCreate(context.Background(), "pool_example", row, "vm1", nil)

		got := fleetGetSessionState(t, fx, "ws_example", "sess_timeout")
		if got.State != control.StateCreating || got.RunnerID != "vm1" {
			t.Fatalf("session = %q on %q, want still creating on vm1", got.State, got.RunnerID)
		}
	})
}

func TestDrainQueueStopsWhenNoRunnerHasCapacity(t *testing.T) {
	fx := newFleetFixture(t)
	fx.st.seedSession(fleetScratchQueued("sess_stuck", 0))
	fx.service.drainPool(context.Background(), "pool_example")

	got := fleetGetSessionState(t, fx, "ws_example", "sess_stuck")
	if got.State != control.StateQueued {
		t.Fatalf("state = %q, want still queued (no connected runner)", got.State)
	}
}

func TestWakeNeverBlocks(t *testing.T) {
	fx := newFleetFixture(t)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 64; i++ {
			fx.service.Wake(control.PoolID(fmt.Sprintf("pool_%d", i)))
		}
		for i := 0; i < 64; i++ {
			fx.service.Wake("pool_0")
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Wake blocked")
	}
	known := fx.service.knownPools()
	if len(known) != 64 {
		t.Fatalf("knownPools has %d pools, want 64", len(known))
	}
}

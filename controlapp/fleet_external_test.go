package controlapp_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/controlapp"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// var _ control.Fleet = (*controlapp.FleetService)(nil) pins at compile time
// that the deep FleetService module satisfies the frozen caller-facing
// contract, from outside the package.
var _ control.Fleet = (*controlapp.FleetService)(nil)

func externalFleetOptions() controlapp.FleetOptions {
	return controlapp.FleetOptions{
		Authorizer:     extAuthorizer{},
		Sessions:       extSessions{},
		Environments:   extEnvironments{},
		Fleet:          extFleet{},
		Pools:          extPools{},
		Transport:      extTransport{},
		Events:         extEvents{},
		Clock:          extClock{},
		IDs:            extIDs{},
		SafetyInterval: time.Second,
		LaunchMaterial: extResolver{},
	}
}

func constructFleet(t *testing.T) control.Fleet {
	t.Helper()
	svc, err := controlapp.NewFleetService(externalFleetOptions())
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func TestFleetApplicationSeam(t *testing.T) {
	fleet := constructFleet(t)
	svc := fleet.(*controlapp.FleetService)

	// Registration through the frozen interface.
	res, err := fleet.RegisterRunner(context.Background(), control.RunnerRegistration{
		WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
		Generation: 1, CapacityTotal: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Accepted {
		t.Fatalf("registration refused: %+v", res)
	}

	// Scoped listing.
	page, err := fleet.ListRunners(context.Background(), control.Scope{
		WorkspaceID: "ws_example",
		Actor:       control.Actor{ID: "act_example", Kind: control.ActorService},
		Placement: control.PlacementScope{
			ProductRegion: "us", HomeCell: "cell-1", Mode: control.ExecutionDedicated,
		},
	}, control.RunnerQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Runners) != 1 {
		t.Fatalf("listed %d runners, want 1", len(page.Runners))
	}

	// Reconciliation.
	rec, err := fleet.ReconcileRunner(context.Background(), control.RunnerSnapshot{
		WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
		Generation: 1, CapacityTotal: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if rec.Fenced {
		t.Fatalf("snapshot fenced: %+v", rec)
	}

	// Event application.
	if err := fleet.ApplyRunnerEvent(context.Background(), control.RunnerEvent{
		WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
		Generation: 1, SessionID: "sess_example", State: control.StateRunning,
	}); err != nil {
		t.Fatal(err)
	}

	// Run in an already-canceled context returns context.Canceled and leaks
	// nothing: the loop observes cancellation and returns immediately.
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := svc.Run(cctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled", err)
	}
}

// ---------------------------------------------------------------------------
// external fakes: each port implemented without any internal/ import.
// ---------------------------------------------------------------------------

type extAuthorizer struct{}

func (extAuthorizer) Authorize(context.Context, control.Scope, control.Action, control.Resource) error {
	return nil
}

type extSessions struct{}

func (extSessions) CreateSession(context.Context, control.WorkspaceID, control.Session) (control.Session, error) {
	return control.Session{}, control.ErrUnsupported
}
func (extSessions) GetSession(_ context.Context, _ control.WorkspaceID, _ control.SessionID) (control.Session, error) {
	return control.Session{
		ID: "sess_example", WorkspaceID: "ws_example", State: control.StateCreating,
		PoolID: "pool_example", RunnerID: "runner_example",
	}, nil
}
func (extSessions) SessionByIDem(context.Context, control.WorkspaceID, control.ActorID, string) (control.Session, error) {
	return control.Session{}, control.ErrNotFound
}
func (extSessions) ListSessions(context.Context, control.WorkspaceID, control.SessionQuery) ([]control.Session, string, error) {
	return nil, "", nil
}
func (extSessions) Transition(context.Context, control.WorkspaceID, control.SessionID, []control.SessionState, control.SessionState, control.TransitionOpts) error {
	return nil
}
func (extSessions) SetSessionSetupHash(context.Context, control.WorkspaceID, control.SessionID, string) error {
	return nil
}
func (extSessions) SetChildExitCode(context.Context, control.WorkspaceID, control.SessionID, int) error {
	return nil
}

type extEnvironments struct{}

func (extEnvironments) CreateEnvironment(context.Context, control.WorkspaceID, control.Environment) (control.Environment, error) {
	return control.Environment{}, control.ErrUnsupported
}
func (extEnvironments) GetEnvironment(_ context.Context, _ control.WorkspaceID, _ control.EnvironmentID) (control.Environment, error) {
	return control.Environment{ID: "env_example", WorkspaceID: "ws_example", Name: "env", Image: "img:latest"}, nil
}
func (extEnvironments) ListEnvironments(context.Context, control.WorkspaceID, control.EnvironmentQuery) ([]control.Environment, string, error) {
	return nil, "", nil
}
func (extEnvironments) UpdateEnvironment(context.Context, control.WorkspaceID, control.Environment) (control.Environment, error) {
	return control.Environment{}, control.ErrUnsupported
}
func (extEnvironments) DeleteEnvironment(context.Context, control.WorkspaceID, control.EnvironmentID) error {
	return control.ErrUnsupported
}
func (extEnvironments) CountSessionsByEnvironment(context.Context, control.WorkspaceID, control.EnvironmentID, []control.SessionState) (int, error) {
	return 0, nil
}
func (extEnvironments) SetEnvironmentSnapshot(context.Context, control.WorkspaceID, control.EnvironmentID, string, string, control.RunnerID) error {
	return control.ErrUnsupported
}

type extFleet struct{}

func (extFleet) UpsertRunner(context.Context, control.PoolID, control.Runner) error { return nil }
func (extFleet) SetRunnerConnected(context.Context, control.PoolID, control.RunnerID, bool) error {
	return nil
}
func (extFleet) ListRunners(_ context.Context, _ control.PoolID) ([]control.Runner, error) {
	return []control.Runner{{ID: "runner_example", PoolID: "pool_example", Generation: 1, CapacityTotal: 4, Connected: true}}, nil
}
func (extFleet) SessionsOnRunner(context.Context, control.PoolID, control.RunnerID, []control.SessionState) ([]control.Session, error) {
	return nil, nil
}
func (extFleet) OldestQueued(context.Context, control.PoolID) ([]control.Session, error) {
	return nil, nil
}

type extPools struct{}

func (extPools) EligiblePools(context.Context, control.Scope, control.Requirements) ([]control.Pool, error) {
	return []control.Pool{{ID: "pool_example", CapacityTotal: 4}}, nil
}

type extTransport struct{}

func (extTransport) Dispatch(_ context.Context, _ control.PoolID, _ control.RunnerID, _ runner.ToRunner) (runner.FromRunner, error) {
	return runner.FromRunner{OK: true}, nil
}
func (extTransport) Connected(control.PoolID, control.RunnerID) bool { return true }

type extEvents struct{}

func (extEvents) Record(context.Context, control.Event) error { return nil }

type extClock struct{}

func (extClock) Now() time.Time { return time.Unix(1_700_000_000, 0) }

type extIDs struct{}

func (extIDs) NewSessionID() control.SessionID         { return "sess_example" }
func (extIDs) NewEnvironmentID() control.EnvironmentID { return "env_example" }
func (extIDs) NewEventID() control.EventID             { return "evt_example" }

type extResolver struct{}

func (extResolver) ResolveLaunchMaterial(context.Context, control.Session, *control.Environment) (controlapp.LaunchMaterial, error) {
	return controlapp.LaunchMaterial{}, nil
}

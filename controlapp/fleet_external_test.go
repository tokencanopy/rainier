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

func fleetExternalOptions() controlapp.FleetOptions {
	return controlapp.FleetOptions{
		Authorizer:     fleetExtAuthorizer{},
		Sessions:       fleetExtSessions{},
		Environments:   fleetExtEnvironments{},
		Fleet:          fleetExtFleet{},
		Pools:          fleetExtPools{},
		Transport:      fleetExtTransport{},
		Events:         fleetExtEvents{},
		Clock:          fleetExtClock{},
		IDs:            fleetExtIDs{},
		SafetyInterval: time.Second,
		LaunchMaterial: fleetExtResolver{},
	}
}

func fleetConstructService(t *testing.T) control.Fleet {
	t.Helper()
	svc, err := controlapp.NewFleetService(fleetExternalOptions())
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func TestFleetApplicationSeam(t *testing.T) {
	fleet := fleetConstructService(t)
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

type fleetExtAuthorizer struct{}

func (fleetExtAuthorizer) Authorize(context.Context, control.Scope, control.Action, control.Resource) error {
	return nil
}

type fleetExtSessions struct{}

func (fleetExtSessions) CreateSession(context.Context, control.WorkspaceID, control.Session) (control.Session, error) {
	return control.Session{}, control.ErrUnsupported
}
func (fleetExtSessions) GetSession(_ context.Context, _ control.WorkspaceID, _ control.SessionID) (control.Session, error) {
	return control.Session{
		ID: "sess_example", WorkspaceID: "ws_example", State: control.StateCreating,
		PoolID: "pool_example", RunnerID: "runner_example",
	}, nil
}
func (fleetExtSessions) SessionByIDem(context.Context, control.WorkspaceID, control.ActorID, string) (control.Session, error) {
	return control.Session{}, control.ErrNotFound
}
func (fleetExtSessions) ListSessions(context.Context, control.WorkspaceID, control.SessionQuery) ([]control.Session, string, error) {
	return nil, "", nil
}
func (fleetExtSessions) Transition(context.Context, control.WorkspaceID, control.SessionID, []control.SessionState, control.SessionState, control.TransitionOpts) error {
	return nil
}
func (fleetExtSessions) SetSessionSetupHash(context.Context, control.WorkspaceID, control.SessionID, string) error {
	return nil
}
func (fleetExtSessions) SetChildExitCode(context.Context, control.WorkspaceID, control.SessionID, int) error {
	return nil
}
func (fleetExtSessions) NextControllerGeneration(context.Context, control.WorkspaceID, control.SessionID) (uint64, error) {
	return 1, nil
}

type fleetExtEnvironments struct{}

func (fleetExtEnvironments) CreateEnvironment(context.Context, control.WorkspaceID, control.Environment) (control.Environment, error) {
	return control.Environment{}, control.ErrUnsupported
}
func (fleetExtEnvironments) GetEnvironment(_ context.Context, _ control.WorkspaceID, _ control.EnvironmentID) (control.Environment, error) {
	return control.Environment{ID: "env_example", WorkspaceID: "ws_example", Name: "env", Image: "img:latest"}, nil
}
func (fleetExtEnvironments) ListEnvironments(context.Context, control.WorkspaceID, control.EnvironmentQuery) ([]control.Environment, string, error) {
	return nil, "", nil
}
func (fleetExtEnvironments) UpdateEnvironment(context.Context, control.WorkspaceID, control.Environment) (control.Environment, error) {
	return control.Environment{}, control.ErrUnsupported
}
func (fleetExtEnvironments) DeleteEnvironment(context.Context, control.WorkspaceID, control.EnvironmentID) error {
	return control.ErrUnsupported
}
func (fleetExtEnvironments) CountSessionsByEnvironment(context.Context, control.WorkspaceID, control.EnvironmentID, []control.SessionState) (int, error) {
	return 0, nil
}
func (fleetExtEnvironments) SetEnvironmentSnapshot(context.Context, control.WorkspaceID, control.EnvironmentID, string, string, control.RunnerID) error {
	return control.ErrUnsupported
}

type fleetExtFleet struct{}

func (fleetExtFleet) UpsertRunner(context.Context, control.PoolID, control.Runner) error { return nil }
func (fleetExtFleet) SetRunnerConnected(context.Context, control.PoolID, control.RunnerID, bool) error {
	return nil
}
func (fleetExtFleet) ListRunners(_ context.Context, _ control.PoolID) ([]control.Runner, error) {
	return []control.Runner{{ID: "runner_example", PoolID: "pool_example", Generation: 1, CapacityTotal: 4, Connected: true}}, nil
}
func (fleetExtFleet) SessionsOnRunner(context.Context, control.PoolID, control.RunnerID, []control.SessionState) ([]control.Session, error) {
	return nil, nil
}
func (fleetExtFleet) OldestQueued(context.Context, control.PoolID) ([]control.Session, error) {
	return nil, nil
}

type fleetExtPools struct{}

func (fleetExtPools) EligiblePools(context.Context, control.Scope, control.Requirements) ([]control.Pool, error) {
	return []control.Pool{{ID: "pool_example", CapacityTotal: 4}}, nil
}

type fleetExtTransport struct{}

func (fleetExtTransport) Dispatch(_ context.Context, _ control.PoolID, _ control.RunnerID, _ runner.ToRunner) (runner.FromRunner, error) {
	return runner.FromRunner{OK: true}, nil
}
func (fleetExtTransport) Connected(control.PoolID, control.RunnerID) bool { return true }

type fleetExtEvents struct{}

func (fleetExtEvents) Record(context.Context, control.Event) error { return nil }

type fleetExtClock struct{}

func (fleetExtClock) Now() time.Time { return time.Unix(1_700_000_000, 0) }

type fleetExtIDs struct{}

func (fleetExtIDs) NewSessionID() control.SessionID         { return "sess_example" }
func (fleetExtIDs) NewEnvironmentID() control.EnvironmentID { return "env_example" }
func (fleetExtIDs) NewEventID() control.EventID             { return "evt_example" }

type fleetExtResolver struct{}

func (fleetExtResolver) ResolveLaunchMaterial(context.Context, control.Session, *control.Environment) (controlapp.LaunchMaterial, error) {
	return controlapp.LaunchMaterial{}, nil
}

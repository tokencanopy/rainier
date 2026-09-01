package controlapp_test

import (
	"context"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/controlapp"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// This external package proves a separate module (exactly as a Rainier Cloud
// module would) can construct and call both deep modules behind the frozen
// control interfaces without importing any Rainier internal package.

var (
	_ control.Sessions     = mustSessions()
	_ control.Environments = mustEnvironments()
)

var extNow = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func extScope() control.Scope {
	return control.Scope{
		WorkspaceID: "ws_example",
		Actor:       control.Actor{ID: "act_example", Kind: control.ActorUser},
		Placement: control.PlacementScope{
			ProductRegion: "us",
			HomeCell:      "cell-1",
			Mode:          control.ExecutionDedicated,
		},
	}
}

func mustSessions() control.Sessions {
	svc, err := controlapp.NewSessionService(controlapp.SessionOptions{
		Authorizer:   allowAllAuthorizer{},
		Sessions:     extSessionRepo{},
		Environments: extEnvironmentRepo{},
		Pools:        extPoolResolver{},
		Events:       extEventRecorder{},
		Clock:        extClock{now: extNow},
		IDs:          extIDs{},
		Wake:         func(control.PoolID) {},
		Fleet:        extFleet{},
		Transport:    extTransport{},
	})
	if err != nil {
		panic(err)
	}
	return svc
}

func mustEnvironments() control.Environments {
	svc, err := controlapp.NewEnvironmentService(controlapp.EnvironmentOptions{
		Authorizer:   allowAllAuthorizer{},
		Environments: extEnvironmentRepo{},
		Events:       extEventRecorder{},
		Clock:        extClock{now: extNow},
		IDs:          extIDs{},
	})
	if err != nil {
		panic(err)
	}
	return svc
}

type allowAllAuthorizer struct{}

func (allowAllAuthorizer) Authorize(context.Context, control.Scope, control.Action, control.Resource) error {
	return nil
}

type extSessionRepo struct{}

func (extSessionRepo) CreateSession(ctx context.Context, ws control.WorkspaceID, s control.Session) (control.Session, error) {
	return s, nil
}
func (extSessionRepo) GetSession(context.Context, control.WorkspaceID, control.SessionID) (control.Session, error) {
	return control.Session{}, control.ErrNotFound
}
func (extSessionRepo) SessionByIDem(context.Context, control.WorkspaceID, control.ActorID, string) (control.Session, error) {
	return control.Session{}, control.ErrNotFound
}
func (extSessionRepo) ListSessions(context.Context, control.WorkspaceID, control.SessionQuery) ([]control.Session, string, error) {
	return nil, "", nil
}
func (extSessionRepo) Transition(context.Context, control.WorkspaceID, control.SessionID, []control.SessionState, control.SessionState, control.TransitionOpts) error {
	return nil
}
func (extSessionRepo) SetSessionSetupHash(context.Context, control.WorkspaceID, control.SessionID, string) error {
	return nil
}
func (extSessionRepo) SetChildExitCode(context.Context, control.WorkspaceID, control.SessionID, int) error {
	return nil
}

type extEnvironmentRepo struct{}

func (extEnvironmentRepo) CreateEnvironment(ctx context.Context, ws control.WorkspaceID, e control.Environment) (control.Environment, error) {
	return e, nil
}
func (extEnvironmentRepo) GetEnvironment(context.Context, control.WorkspaceID, control.EnvironmentID) (control.Environment, error) {
	return control.Environment{}, control.ErrNotFound
}
func (extEnvironmentRepo) ListEnvironments(context.Context, control.WorkspaceID, control.EnvironmentQuery) ([]control.Environment, string, error) {
	return nil, "", nil
}
func (extEnvironmentRepo) UpdateEnvironment(context.Context, control.WorkspaceID, control.Environment) (control.Environment, error) {
	return control.Environment{}, control.ErrNotFound
}
func (extEnvironmentRepo) DeleteEnvironment(context.Context, control.WorkspaceID, control.EnvironmentID) error {
	return nil
}
func (extEnvironmentRepo) CountSessionsByEnvironment(context.Context, control.WorkspaceID, control.EnvironmentID, []control.SessionState) (int, error) {
	return 0, nil
}
func (extEnvironmentRepo) SetEnvironmentSnapshot(context.Context, control.WorkspaceID, control.EnvironmentID, string, string, control.RunnerID) error {
	return nil
}

type extPoolResolver struct{}

func (extPoolResolver) EligiblePools(context.Context, control.Scope, control.Requirements) ([]control.Pool, error) {
	return []control.Pool{{ID: "pool_example", CapacityTotal: 2, CapacityUsed: 0}}, nil
}

type extEventRecorder struct{}

func (extEventRecorder) Record(context.Context, control.Event) error { return nil }

type extClock struct {
	now time.Time
}

func (c extClock) Now() time.Time { return c.now }

type extIDs struct{}

func (extIDs) NewSessionID() control.SessionID         { return "sess_example" }
func (extIDs) NewEnvironmentID() control.EnvironmentID { return "env_example" }
func (extIDs) NewEventID() control.EventID             { return "evt_example" }

type extFleet struct{}

func (extFleet) UpsertRunner(context.Context, control.PoolID, control.Runner) error { return nil }
func (extFleet) SetRunnerConnected(context.Context, control.PoolID, control.RunnerID, bool) error {
	return nil
}
func (extFleet) ListRunners(context.Context, control.PoolID) ([]control.Runner, error) {
	return nil, nil
}
func (extFleet) SessionsOnRunner(context.Context, control.PoolID, control.RunnerID, []control.SessionState) ([]control.Session, error) {
	return nil, nil
}
func (extFleet) OldestQueued(context.Context, control.PoolID) ([]control.Session, error) {
	return nil, nil
}

type extTransport struct{}

func (extTransport) Dispatch(context.Context, control.PoolID, control.RunnerID, runner.ToRunner) (runner.FromRunner, error) {
	return runner.FromRunner{OK: true}, nil
}
func (extTransport) Connected(control.PoolID, control.RunnerID) bool { return true }

func TestExternalPackageConsumesTheSeam(t *testing.T) {
	ctx := context.Background()

	sess, err := mustSessions().CreateSession(ctx, extScope(), control.CreateSession{
		Name: "scratch",
		Spec: control.PortableSpec{Image: "registry.example.invalid/agent@sha256:0000", Cmd: []string{"bash"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if sess.ID != "sess_example" || sess.State != control.StateQueued || sess.WorkspaceID != "ws_example" || sess.CreatorID != "act_example" {
		t.Fatalf("session = %+v", sess)
	}

	env, err := mustEnvironments().CreateEnvironment(ctx, extScope(), control.CreateEnvironment{
		Name: "standard", Image: "registry.example.invalid/rainier@sha256:0000", Setup: "make bootstrap",
	})
	if err != nil {
		t.Fatal(err)
	}
	if env.ID != "env_example" || env.SetupHash == "" || env.WorkspaceID != "ws_example" {
		t.Fatalf("environment = %+v", env)
	}
}

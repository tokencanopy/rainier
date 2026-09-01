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
	_ control.Sessions     = sessionMustSessions()
	_ control.Environments = sessionMustEnvironments()
)

var sessionExtNow = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func sessionExtScope() control.Scope {
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

func sessionMustSessions() control.Sessions {
	svc, err := controlapp.NewSessionService(controlapp.SessionOptions{
		Authorizer:   sessionAllowAllAuthorizer{},
		Sessions:     sessionExtSessionRepo{},
		Environments: sessionExtEnvironmentRepo{},
		Pools:        sessionExtPoolResolver{},
		Events:       sessionExtEventRecorder{},
		Clock:        sessionExtClock{now: sessionExtNow},
		IDs:          sessionExtIDs{},
		Wake:         func(control.PoolID) {},
		Fleet:        sessionExtFleet{},
		Transport:    sessionExtTransport{},
	})
	if err != nil {
		panic(err)
	}
	return svc
}

func sessionMustEnvironments() control.Environments {
	svc, err := controlapp.NewEnvironmentService(controlapp.EnvironmentOptions{
		Authorizer:   sessionAllowAllAuthorizer{},
		Environments: sessionExtEnvironmentRepo{},
		Events:       sessionExtEventRecorder{},
		Clock:        sessionExtClock{now: sessionExtNow},
		IDs:          sessionExtIDs{},
	})
	if err != nil {
		panic(err)
	}
	return svc
}

type sessionAllowAllAuthorizer struct{}

func (sessionAllowAllAuthorizer) Authorize(context.Context, control.Scope, control.Action, control.Resource) error {
	return nil
}

type sessionExtSessionRepo struct{}

func (sessionExtSessionRepo) CreateSession(ctx context.Context, ws control.WorkspaceID, s control.Session) (control.Session, error) {
	return s, nil
}
func (sessionExtSessionRepo) GetSession(context.Context, control.WorkspaceID, control.SessionID) (control.Session, error) {
	return control.Session{}, control.ErrNotFound
}
func (sessionExtSessionRepo) SessionByIDem(context.Context, control.WorkspaceID, control.ActorID, string) (control.Session, error) {
	return control.Session{}, control.ErrNotFound
}
func (sessionExtSessionRepo) ListSessions(context.Context, control.WorkspaceID, control.SessionQuery) ([]control.Session, string, error) {
	return nil, "", nil
}
func (sessionExtSessionRepo) Transition(context.Context, control.WorkspaceID, control.SessionID, []control.SessionState, control.SessionState, control.TransitionOpts) error {
	return nil
}
func (sessionExtSessionRepo) SetSessionSetupHash(context.Context, control.WorkspaceID, control.SessionID, string) error {
	return nil
}
func (sessionExtSessionRepo) SetChildExitCode(context.Context, control.WorkspaceID, control.SessionID, int) error {
	return nil
}

type sessionExtEnvironmentRepo struct{}

func (sessionExtEnvironmentRepo) CreateEnvironment(ctx context.Context, ws control.WorkspaceID, e control.Environment) (control.Environment, error) {
	return e, nil
}
func (sessionExtEnvironmentRepo) GetEnvironment(context.Context, control.WorkspaceID, control.EnvironmentID) (control.Environment, error) {
	return control.Environment{}, control.ErrNotFound
}
func (sessionExtEnvironmentRepo) ListEnvironments(context.Context, control.WorkspaceID, control.EnvironmentQuery) ([]control.Environment, string, error) {
	return nil, "", nil
}
func (sessionExtEnvironmentRepo) UpdateEnvironment(context.Context, control.WorkspaceID, control.Environment) (control.Environment, error) {
	return control.Environment{}, control.ErrNotFound
}
func (sessionExtEnvironmentRepo) DeleteEnvironment(context.Context, control.WorkspaceID, control.EnvironmentID) error {
	return nil
}
func (sessionExtEnvironmentRepo) CountSessionsByEnvironment(context.Context, control.WorkspaceID, control.EnvironmentID, []control.SessionState) (int, error) {
	return 0, nil
}
func (sessionExtEnvironmentRepo) SetEnvironmentSnapshot(context.Context, control.WorkspaceID, control.EnvironmentID, string, string, control.RunnerID) error {
	return nil
}

type sessionExtPoolResolver struct{}

func (sessionExtPoolResolver) EligiblePools(context.Context, control.Scope, control.Requirements) ([]control.Pool, error) {
	return []control.Pool{{ID: "pool_example", CapacityTotal: 2, CapacityUsed: 0}}, nil
}

type sessionExtEventRecorder struct{}

func (sessionExtEventRecorder) Record(context.Context, control.Event) error { return nil }

type sessionExtClock struct {
	now time.Time
}

func (c sessionExtClock) Now() time.Time { return c.now }

type sessionExtIDs struct{}

func (sessionExtIDs) NewSessionID() control.SessionID         { return "sess_example" }
func (sessionExtIDs) NewEnvironmentID() control.EnvironmentID { return "env_example" }
func (sessionExtIDs) NewEventID() control.EventID             { return "evt_example" }

type sessionExtFleet struct{}

func (sessionExtFleet) UpsertRunner(context.Context, control.PoolID, control.Runner) error {
	return nil
}
func (sessionExtFleet) SetRunnerConnected(context.Context, control.PoolID, control.RunnerID, bool) error {
	return nil
}
func (sessionExtFleet) ListRunners(context.Context, control.PoolID) ([]control.Runner, error) {
	return nil, nil
}
func (sessionExtFleet) SessionsOnRunner(context.Context, control.PoolID, control.RunnerID, []control.SessionState) ([]control.Session, error) {
	return nil, nil
}
func (sessionExtFleet) OldestQueued(context.Context, control.PoolID) ([]control.Session, error) {
	return nil, nil
}

type sessionExtTransport struct{}

func (sessionExtTransport) Dispatch(context.Context, control.PoolID, control.RunnerID, runner.ToRunner) (runner.FromRunner, error) {
	return runner.FromRunner{OK: true}, nil
}
func (sessionExtTransport) Connected(control.PoolID, control.RunnerID) bool { return true }

func TestExternalPackageConsumesTheSeam(t *testing.T) {
	ctx := context.Background()

	sess, err := sessionMustSessions().CreateSession(ctx, sessionExtScope(), control.CreateSession{
		Name: "scratch",
		Spec: control.PortableSpec{Image: "registry.example.invalid/agent@sha256:0000", Cmd: []string{"bash"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if sess.ID != "sess_example" || sess.State != control.StateQueued || sess.WorkspaceID != "ws_example" || sess.CreatorID != "act_example" {
		t.Fatalf("session = %+v", sess)
	}

	env, err := sessionMustEnvironments().CreateEnvironment(ctx, sessionExtScope(), control.CreateEnvironment{
		Name: "standard", Image: "registry.example.invalid/rainier@sha256:0000", Setup: "make bootstrap",
	})
	if err != nil {
		t.Fatal(err)
	}
	if env.ID != "env_example" || env.SetupHash == "" || env.WorkspaceID != "ws_example" {
		t.Fatalf("environment = %+v", env)
	}
}

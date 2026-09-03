package controlapp

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/terminal"
	"github.com/tokencanopy/rainier/protocol/workspace"
)

// The atomicity contract of every command in this package: the mutation a
// command makes and the event describing it happen inside ONE unit of work,
// and the scheduler learns about the command only after that unit commits. A
// unit that cannot commit fails the command with control.ErrUnavailable and
// wakes nobody — the alternative is a scheduler chasing a session that does
// not exist, or a billing history missing the row it bills for.
//
// The stubs here prove it the only way a repository can: by reporting the
// unit depth the context handed them carried. Depth 1 means "inside the
// command's own unit"; depth 0 means "beside it".

// depthKey is how countingUOW tells a stub which unit depth it was called at.
// The context is the whole of what a repository sees of a unit — no
// transaction, connection, or driver type crosses control.UnitOfWork — so it
// is also the whole of what a test can observe.
type depthKey struct{}

// unitDepth reports the unit depth ctx carries, zero when it carries none.
func unitDepth(ctx context.Context) int {
	d, _ := ctx.Value(depthKey{}).(int)
	return d
}

// countingUOW counts open units and refuses to commit when told to. Every
// repository stub in this file reports the unit depth it was called at.
type countingUOW struct {
	depth, runs int
	failCommit  bool
}

func (u *countingUOW) Run(ctx context.Context, fn func(context.Context) error) error {
	u.runs++
	u.depth++
	defer func() { u.depth-- }()
	if err := fn(context.WithValue(ctx, depthKey{}, u.depth)); err != nil {
		return err
	}
	if u.failCommit {
		return control.ErrUnavailable
	}
	return nil
}

var _ control.UnitOfWork = (*countingUOW)(nil)

// ---------------------------------------------------------------------------
// depth-recording stubs
// ---------------------------------------------------------------------------

// uowSessionRepo is the package's session repository stub with one addition:
// every mutating method records the unit depth it ran at.
type uowSessionRepo struct {
	*sessionStubSessionRepo
	createDepth     int
	transitionDepth int
}

func (r *uowSessionRepo) CreateSession(ctx context.Context, ws control.WorkspaceID, s control.Session) (control.Session, error) {
	r.createDepth = unitDepth(ctx)
	return r.sessionStubSessionRepo.CreateSession(ctx, ws, s)
}

func (r *uowSessionRepo) Transition(ctx context.Context, ws control.WorkspaceID, id control.SessionID,
	from []control.SessionState, to control.SessionState, opts control.TransitionOpts) error {
	r.transitionDepth = unitDepth(ctx)
	return r.sessionStubSessionRepo.Transition(ctx, ws, id, from, to, opts)
}

// uowEnvRepo is the environment repository stub reporting write depth.
type uowEnvRepo struct {
	*sessionStubEnvironmentRepo
	createDepth, updateDepth, deleteDepth int
}

func (r *uowEnvRepo) CreateEnvironment(ctx context.Context, ws control.WorkspaceID, e control.Environment) (control.Environment, error) {
	r.createDepth = unitDepth(ctx)
	return r.sessionStubEnvironmentRepo.CreateEnvironment(ctx, ws, e)
}

func (r *uowEnvRepo) UpdateEnvironment(ctx context.Context, ws control.WorkspaceID, e control.Environment) (control.Environment, error) {
	r.updateDepth = unitDepth(ctx)
	return r.sessionStubEnvironmentRepo.UpdateEnvironment(ctx, ws, e)
}

func (r *uowEnvRepo) DeleteEnvironment(ctx context.Context, ws control.WorkspaceID, id control.EnvironmentID) error {
	r.deleteDepth = unitDepth(ctx)
	return r.sessionStubEnvironmentRepo.DeleteEnvironment(ctx, ws, id)
}

// uowRecorder is the event recorder reporting the depth each record ran at.
type uowRecorder struct {
	*sessionStubEventRecorder
	recordDepth int
}

func (r *uowRecorder) Record(ctx context.Context, e control.Event) error {
	r.recordDepth = unitDepth(ctx)
	return r.sessionStubEventRecorder.Record(ctx, e)
}

// last returns the most recently recorded event.
func (r *uowRecorder) last(t *testing.T) control.Event {
	t.Helper()
	if len(r.events) == 0 {
		t.Fatal("no event recorded")
	}
	return r.events[len(r.events)-1]
}

// ---------------------------------------------------------------------------
// session fixture
// ---------------------------------------------------------------------------

type uowSessionFixture struct {
	svc       *SessionService
	uow       *countingUOW
	repo      *uowSessionRepo
	rec       *uowRecorder
	transport *sessionStubTransport
	fleet     *sessionStubFleet
	woke      int
}

func newUOWSessionFixture(t *testing.T) *uowSessionFixture {
	t.Helper()
	fx := &uowSessionFixture{
		uow:       &countingUOW{},
		repo:      &uowSessionRepo{sessionStubSessionRepo: newSessionStubSessionRepo(nil)},
		rec:       &uowRecorder{sessionStubEventRecorder: &sessionStubEventRecorder{}},
		transport: &sessionStubTransport{res: runner.FromRunner{OK: true, Detail: "ckpt_example"}},
		fleet:     &sessionStubFleet{},
	}
	svc, err := NewSessionService(SessionOptions{
		Authorizer:   &sessionStubAuthorizer{},
		Sessions:     fx.repo,
		Environments: newSessionStubEnvironmentRepo(nil),
		Pools:        &sessionStubPoolResolver{pools: []control.Pool{{ID: "pool_a", CapacityTotal: 8}}},
		Events:       fx.rec,
		Clock:        sessionStubClock{now: sessionFixedNow},
		IDs:          &sessionStubIDs{sessionID: "sess_example", envID: "env_example", eventID: "evt_example"},
		Wake:         func(control.PoolID) { fx.woke++ },
		Fleet:        fx.fleet,
		Transport:    fx.transport,
		UnitOfWork:   fx.uow,
	})
	if err != nil {
		t.Fatalf("NewSessionService: %v", err)
	}
	fx.svc = svc
	return fx
}

// uowPlacedSession is a session already on a runner, the shape every
// lifecycle command below starts from.
func uowPlacedSession(id control.SessionID, state control.SessionState) control.Session {
	return control.Session{
		ID:                  id,
		WorkspaceID:         "ws_example",
		CreatorID:           "act_example",
		State:               state,
		PoolID:              "pool_a",
		RunnerID:            "runner_a",
		PlacementGeneration: 3,
	}
}

var uowCtx = context.Background()

// ---------------------------------------------------------------------------
// SessionService
// ---------------------------------------------------------------------------

// TestCreateSessionCommitsRowAndEventTogether: the row write and the event
// record happen at depth 1 of one unit, and a unit that cannot commit fails
// the create with ErrUnavailable without waking the scheduler.
func TestCreateSessionCommitsRowAndEventTogether(t *testing.T) {
	fx := newUOWSessionFixture(t)
	got, err := fx.svc.CreateSession(uowCtx, sessionTestScope(), control.CreateSession{Name: "investigate"})
	if err != nil {
		t.Fatal(err)
	}
	if fx.uow.runs != 1 || fx.repo.createDepth != 1 || fx.rec.recordDepth != 1 || fx.woke != 1 {
		t.Fatalf("runs %d, create at depth %d, record at depth %d, woke %d; want 1/1/1/1",
			fx.uow.runs, fx.repo.createDepth, fx.rec.recordDepth, fx.woke)
	}
	// A create opens the session's first placement generation, and the event
	// carries the generation the STORED row has.
	if ev := fx.rec.last(t); ev.PlacementGeneration != got.PlacementGeneration || ev.PlacementGeneration != 1 {
		t.Fatalf("event placement generation = %d, want %d (the stored row's)",
			ev.PlacementGeneration, got.PlacementGeneration)
	}

	fx.uow.failCommit = true
	if _, err := fx.svc.CreateSession(uowCtx, sessionTestScope(), control.CreateSession{Name: "again"}); !errors.Is(err, control.ErrUnavailable) {
		t.Fatalf("uncommittable create: err = %v, want ErrUnavailable", err)
	}
	if fx.woke != 1 {
		t.Fatal("scheduler woken for a create that did not commit")
	}
}

// TestDeleteSessionCommitsTransitionAndEventTogether: the cancel and its
// event are one unit; an uncommittable unit is ErrUnavailable and no wake.
func TestDeleteSessionCommitsTransitionAndEventTogether(t *testing.T) {
	fx := newUOWSessionFixture(t)
	fx.repo.put(uowPlacedSession("sess_one", control.StateQueued))
	if err := fx.svc.DeleteSession(uowCtx, sessionTestScope(), control.DeleteSession{ID: "sess_one"}); err != nil {
		t.Fatal(err)
	}
	if fx.uow.runs != 1 || fx.repo.transitionDepth != 1 || fx.rec.recordDepth != 1 || fx.woke != 1 {
		t.Fatalf("runs %d, transition at depth %d, record at depth %d, woke %d; want 1/1/1/1",
			fx.uow.runs, fx.repo.transitionDepth, fx.rec.recordDepth, fx.woke)
	}
	if ev := fx.rec.last(t); ev.PlacementGeneration != 3 {
		t.Fatalf("event placement generation = %d, want 3 (the row's, unchanged by a cancel)", ev.PlacementGeneration)
	}

	fx.uow.failCommit = true
	fx.repo.put(uowPlacedSession("sess_two", control.StateQueued))
	if err := fx.svc.DeleteSession(uowCtx, sessionTestScope(), control.DeleteSession{ID: "sess_two"}); !errors.Is(err, control.ErrUnavailable) {
		t.Fatalf("uncommittable delete: err = %v, want ErrUnavailable", err)
	}
	if fx.woke != 1 {
		t.Fatal("scheduler woken for a delete that did not commit")
	}
}

// TestDeleteRunningSessionCommitsDestroyAndEventTogether: the destroy the
// runner already accepted is a transport call and stays outside the unit; the
// store transition and the event are inside it.
func TestDeleteRunningSessionCommitsDestroyAndEventTogether(t *testing.T) {
	fx := newUOWSessionFixture(t)
	fx.repo.put(uowPlacedSession("sess_one", control.StateRunning))
	if err := fx.svc.DeleteSession(uowCtx, sessionTestScope(), control.DeleteSession{ID: "sess_one"}); err != nil {
		t.Fatal(err)
	}
	if fx.uow.runs != 1 || fx.repo.transitionDepth != 1 || fx.rec.recordDepth != 1 || fx.woke != 1 {
		t.Fatalf("runs %d, transition at depth %d, record at depth %d, woke %d; want 1/1/1/1",
			fx.uow.runs, fx.repo.transitionDepth, fx.rec.recordDepth, fx.woke)
	}
	if !fx.transport.dispatchedType("destroy") {
		t.Fatal("the runner never got the destroy")
	}
}

// TestSuspendSessionCommitsTransitionAndEventTogether.
func TestSuspendSessionCommitsTransitionAndEventTogether(t *testing.T) {
	fx := newUOWSessionFixture(t)
	fx.repo.put(uowPlacedSession("sess_one", control.StateRunning))
	if _, err := fx.svc.SuspendSession(uowCtx, sessionTestScope(), control.SuspendSession{ID: "sess_one", Warm: true}); err != nil {
		t.Fatal(err)
	}
	if fx.uow.runs != 1 || fx.repo.transitionDepth != 1 || fx.rec.recordDepth != 1 || fx.woke != 1 {
		t.Fatalf("runs %d, transition at depth %d, record at depth %d, woke %d; want 1/1/1/1",
			fx.uow.runs, fx.repo.transitionDepth, fx.rec.recordDepth, fx.woke)
	}
	if ev := fx.rec.last(t); ev.PlacementGeneration != 3 {
		t.Fatalf("event placement generation = %d, want 3 (a suspend names no runner)", ev.PlacementGeneration)
	}

	fx.uow.failCommit = true
	fx.repo.put(uowPlacedSession("sess_two", control.StateRunning))
	if _, err := fx.svc.SuspendSession(uowCtx, sessionTestScope(), control.SuspendSession{ID: "sess_two", Warm: true}); !errors.Is(err, control.ErrUnavailable) {
		t.Fatalf("uncommittable suspend: err = %v, want ErrUnavailable", err)
	}
	if fx.woke != 1 {
		t.Fatal("scheduler woken for a suspend that did not commit")
	}
}

// TestResumeSessionCommitsTransitionAndEventTogether: a cold resume's
// transition names the runner, so the repository opens a NEW placement
// generation — and the event carries the generation the row has after the
// mutation, which only a re-read inside the unit knows.
func TestResumeSessionCommitsTransitionAndEventTogether(t *testing.T) {
	fx := newUOWSessionFixture(t)
	fx.fleet.runners = []control.Runner{{ID: "runner_a", PoolID: "pool_a", CapacityTotal: 4}}
	fx.repo.put(uowPlacedSession("sess_one", control.StateSuspendedCold))
	if _, err := fx.svc.ResumeSession(uowCtx, sessionTestScope(), control.ResumeSession{ID: "sess_one"}); err != nil {
		t.Fatal(err)
	}
	if fx.uow.runs != 1 || fx.repo.transitionDepth != 1 || fx.rec.recordDepth != 1 || fx.woke != 1 {
		t.Fatalf("runs %d, transition at depth %d, record at depth %d, woke %d; want 1/1/1/1",
			fx.uow.runs, fx.repo.transitionDepth, fx.rec.recordDepth, fx.woke)
	}
	if ev := fx.rec.last(t); ev.PlacementGeneration != 4 {
		t.Fatalf("event placement generation = %d, want 4 (the generation the cold resume opened)", ev.PlacementGeneration)
	}

	fx.uow.failCommit = true
	fx.repo.put(uowPlacedSession("sess_two", control.StateSuspendedWarm))
	if _, err := fx.svc.ResumeSession(uowCtx, sessionTestScope(), control.ResumeSession{ID: "sess_two"}); !errors.Is(err, control.ErrUnavailable) {
		t.Fatalf("uncommittable resume: err = %v, want ErrUnavailable", err)
	}
	if fx.woke != 1 {
		t.Fatal("scheduler woken for a resume that did not commit")
	}
}

// TestSnapshotSessionRecordsInsideOneUnit: a snapshot's only write is its
// event, and it still commits as a unit — an uncommittable one is
// ErrUnavailable rather than a checkpoint nobody recorded.
func TestSnapshotSessionRecordsInsideOneUnit(t *testing.T) {
	fx := newUOWSessionFixture(t)
	fx.repo.put(uowPlacedSession("sess_one", control.StateRunning))
	if _, err := fx.svc.SnapshotSession(uowCtx, sessionTestScope(), control.SnapshotSession{ID: "sess_one"}); err != nil {
		t.Fatal(err)
	}
	if fx.uow.runs != 1 || fx.rec.recordDepth != 1 {
		t.Fatalf("runs %d, record at depth %d; want 1/1", fx.uow.runs, fx.rec.recordDepth)
	}
	if ev := fx.rec.last(t); ev.PlacementGeneration != 3 {
		t.Fatalf("event placement generation = %d, want 3", ev.PlacementGeneration)
	}

	fx.uow.failCommit = true
	if _, err := fx.svc.SnapshotSession(uowCtx, sessionTestScope(), control.SnapshotSession{ID: "sess_one"}); !errors.Is(err, control.ErrUnavailable) {
		t.Fatalf("uncommittable snapshot: err = %v, want ErrUnavailable", err)
	}
}

// ---------------------------------------------------------------------------
// EnvironmentService
// ---------------------------------------------------------------------------

type uowEnvFixture struct {
	svc  *EnvironmentService
	uow  *countingUOW
	repo *uowEnvRepo
	rec  *uowRecorder
}

func newUOWEnvFixture(t *testing.T) *uowEnvFixture {
	t.Helper()
	fx := &uowEnvFixture{
		uow:  &countingUOW{},
		repo: &uowEnvRepo{sessionStubEnvironmentRepo: newSessionStubEnvironmentRepo(nil)},
		rec:  &uowRecorder{sessionStubEventRecorder: &sessionStubEventRecorder{}},
	}
	svc, err := NewEnvironmentService(EnvironmentOptions{
		Authorizer:   &sessionStubAuthorizer{},
		Environments: fx.repo,
		Events:       fx.rec,
		Clock:        sessionStubClock{now: sessionFixedNow},
		IDs:          &sessionStubIDs{sessionID: "sess_example", envID: "env_example", eventID: "evt_example"},
		UnitOfWork:   fx.uow,
	})
	if err != nil {
		t.Fatalf("NewEnvironmentService: %v", err)
	}
	fx.svc = svc
	return fx
}

// TestEnvironmentCommandsCommitWriteAndEventTogether: create, update, and
// delete each write and record inside one unit, and an event about an
// environment carries no placement generation — it is about no session.
func TestEnvironmentCommandsCommitWriteAndEventTogether(t *testing.T) {
	scope := sessionTestScope()
	create := control.CreateEnvironment{Name: "dev", Image: "registry.example.invalid/base@sha256:0000"}

	t.Run("create", func(t *testing.T) {
		fx := newUOWEnvFixture(t)
		if _, err := fx.svc.CreateEnvironment(uowCtx, scope, create); err != nil {
			t.Fatal(err)
		}
		if fx.uow.runs != 1 || fx.repo.createDepth != 1 || fx.rec.recordDepth != 1 {
			t.Fatalf("runs %d, create at depth %d, record at depth %d; want 1/1/1",
				fx.uow.runs, fx.repo.createDepth, fx.rec.recordDepth)
		}
		if ev := fx.rec.last(t); ev.PlacementGeneration != 0 {
			t.Fatalf("environment event placement generation = %d, want 0", ev.PlacementGeneration)
		}
		fx.uow.failCommit = true
		if _, err := fx.svc.CreateEnvironment(uowCtx, scope, create); !errors.Is(err, control.ErrUnavailable) {
			t.Fatalf("uncommittable create: err = %v, want ErrUnavailable", err)
		}
	})

	t.Run("update", func(t *testing.T) {
		fx := newUOWEnvFixture(t)
		if _, err := fx.svc.CreateEnvironment(uowCtx, scope, create); err != nil {
			t.Fatal(err)
		}
		name := "dev2"
		if _, err := fx.svc.UpdateEnvironment(uowCtx, scope, control.UpdateEnvironment{ID: "env_example", Name: &name}); err != nil {
			t.Fatal(err)
		}
		if fx.uow.runs != 2 || fx.repo.updateDepth != 1 || fx.rec.recordDepth != 1 {
			t.Fatalf("runs %d, update at depth %d, record at depth %d; want 2/1/1",
				fx.uow.runs, fx.repo.updateDepth, fx.rec.recordDepth)
		}
		fx.uow.failCommit = true
		if _, err := fx.svc.UpdateEnvironment(uowCtx, scope, control.UpdateEnvironment{ID: "env_example", Name: &name}); !errors.Is(err, control.ErrUnavailable) {
			t.Fatalf("uncommittable update: err = %v, want ErrUnavailable", err)
		}
	})

	t.Run("delete", func(t *testing.T) {
		fx := newUOWEnvFixture(t)
		if _, err := fx.svc.CreateEnvironment(uowCtx, scope, create); err != nil {
			t.Fatal(err)
		}
		if err := fx.svc.DeleteEnvironment(uowCtx, scope, control.DeleteEnvironment{ID: "env_example"}); err != nil {
			t.Fatal(err)
		}
		if fx.uow.runs != 2 || fx.repo.deleteDepth != 1 || fx.rec.recordDepth != 1 {
			t.Fatalf("runs %d, delete at depth %d, record at depth %d; want 2/1/1",
				fx.uow.runs, fx.repo.deleteDepth, fx.rec.recordDepth)
		}
	})
}

// ---------------------------------------------------------------------------
// FleetService.ApplyRunnerEvent
// ---------------------------------------------------------------------------

// uowFleetSessions is the fleet fixture's session repository reporting the
// depth its two mutating methods ran at.
type uowFleetSessions struct {
	*fleetFakeSessions
	transitionDepth, exitCodeDepth int
}

func (f *uowFleetSessions) Transition(ctx context.Context, ws control.WorkspaceID, id control.SessionID,
	from []control.SessionState, to control.SessionState, opts control.TransitionOpts) error {
	f.transitionDepth = unitDepth(ctx)
	return f.fleetFakeSessions.Transition(ctx, ws, id, from, to, opts)
}

func (f *uowFleetSessions) SetChildExitCode(ctx context.Context, ws control.WorkspaceID, id control.SessionID, code int) error {
	f.exitCodeDepth = unitDepth(ctx)
	return f.fleetFakeSessions.SetChildExitCode(ctx, ws, id, code)
}

// uowFleetEvents is the fleet fixture's recorder reporting record depth.
type uowFleetEvents struct {
	*fleetFakeEvents
	recordDepth int
}

func (f *uowFleetEvents) Record(ctx context.Context, e control.Event) error {
	f.recordDepth = unitDepth(ctx)
	return f.fleetFakeEvents.Record(ctx, e)
}

type uowFleetFixture struct {
	svc      *FleetService
	uow      *countingUOW
	sessions *uowFleetSessions
	events   *uowFleetEvents
	st       *fleetFakeStore
}

func newUOWFleetFixture(t *testing.T) *uowFleetFixture {
	t.Helper()
	st := newFleetFakeStore()
	fx := &uowFleetFixture{
		uow:      &countingUOW{},
		sessions: &uowFleetSessions{fleetFakeSessions: &fleetFakeSessions{st: st}},
		events:   &uowFleetEvents{fleetFakeEvents: &fleetFakeEvents{}},
		st:       st,
	}
	svc, err := NewFleetService(FleetOptions{
		Authorizer:     &fleetFakeAuthorizer{},
		Sessions:       fx.sessions,
		Environments:   &fleetFakeEnvironments{st: st},
		Fleet:          &fleetFakeFleet{st: st},
		Pools:          &fleetFakePools{},
		Transport:      newFleetFakeTransport(),
		Events:         fx.events,
		Clock:          &fleetFakeClock{now: sessionFixedNow},
		IDs:            &fleetFakeIDs{},
		SafetyInterval: time.Second,
		LaunchMaterial: &fleetFakeResolver{},
		UnitOfWork:     fx.uow,
		Checkpoints:    locatorStub{},
	})
	if err != nil {
		t.Fatalf("NewFleetService: %v", err)
	}
	fx.svc = svc
	return fx
}

// TestApplyRunnerEventCommitsTransitionAndEventTogether: the state change a
// runner reports and the fact recording it are one unit, the pool is woken
// only after it commits, and an uncommittable unit is ErrUnavailable.
func TestApplyRunnerEventCommitsTransitionAndEventTogether(t *testing.T) {
	fx := newUOWFleetFixture(t)
	fx.st.seedRunner(fleetEventRunner(1))
	fx.st.seedSession(control.Session{
		ID: "sess_example", WorkspaceID: "ws_example", State: control.StateCreating,
		PoolID: "pool_example", RunnerID: "runner_example", PlacementGeneration: 5,
	})
	ev := control.RunnerEvent{
		WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
		Generation: 1, SessionID: "sess_example", State: control.StateRunning,
	}
	if err := fx.svc.ApplyRunnerEvent(uowCtx, ev); err != nil {
		t.Fatal(err)
	}
	if fx.uow.runs != 1 || fx.sessions.transitionDepth != 1 || fx.events.recordDepth != 1 {
		t.Fatalf("runs %d, transition at depth %d, record at depth %d; want 1/1/1",
			fx.uow.runs, fx.sessions.transitionDepth, fx.events.recordDepth)
	}
	if woke := fleetWakePools(fx.svc); len(woke) != 1 || woke[0] != "pool_example" {
		t.Fatalf("woke %v, want one wake of pool_example after the commit", woke)
	}
	recorded := fx.events.recorded()
	if len(recorded) != 1 || recorded[0].PlacementGeneration != 5 {
		t.Fatalf("recorded %+v, want one event at placement generation 5", recorded)
	}

	// An uncommittable unit: the runner's report is ErrUnavailable and the
	// scheduler is not woken for a state change that did not commit.
	fx.uow.failCommit = true
	fx.st.seedSession(control.Session{
		ID: "sess_other", WorkspaceID: "ws_example", State: control.StateCreating,
		PoolID: "pool_example", RunnerID: "runner_example", PlacementGeneration: 5,
	})
	ev.SessionID = "sess_other"
	if err := fx.svc.ApplyRunnerEvent(uowCtx, ev); !errors.Is(err, control.ErrUnavailable) {
		t.Fatalf("uncommittable runner event: err = %v, want ErrUnavailable", err)
	}
	if woke := fleetWakePools(fx.svc); len(woke) != 0 {
		t.Fatalf("woke %v for a runner event that did not commit", woke)
	}
}

// TestApplyRunnerEventCommitsChildExitAndEventTogether: the child exit code
// and its event are the same unit, and the idempotent replay of an identical
// exit still opens no unit and returns nil.
func TestApplyRunnerEventCommitsChildExitAndEventTogether(t *testing.T) {
	fx := newUOWFleetFixture(t)
	fx.st.seedRunner(fleetEventRunner(1))
	fx.st.seedSession(control.Session{
		ID: "sess_example", WorkspaceID: "ws_example", State: control.StateRunning,
		PoolID: "pool_example", RunnerID: "runner_example", PlacementGeneration: 2,
	})
	code := 7
	ev := control.RunnerEvent{
		WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
		Generation: 1, SessionID: "sess_example", State: control.StateRunning, ChildExitCode: &code,
	}
	if err := fx.svc.ApplyRunnerEvent(uowCtx, ev); err != nil {
		t.Fatal(err)
	}
	if fx.uow.runs != 1 || fx.sessions.exitCodeDepth != 1 || fx.events.recordDepth != 1 {
		t.Fatalf("runs %d, child exit at depth %d, record at depth %d; want 1/1/1",
			fx.uow.runs, fx.sessions.exitCodeDepth, fx.events.recordDepth)
	}

	// The same report again is idempotent success: no unit, no second event.
	fx.uow.failCommit = true
	if err := fx.svc.ApplyRunnerEvent(uowCtx, ev); err != nil {
		t.Fatalf("identical replay: err = %v, want nil", err)
	}
	if fx.uow.runs != 1 || len(fx.events.recorded()) != 1 {
		t.Fatalf("runs %d, %d events; want 1/1 — an already-applied report writes nothing",
			fx.uow.runs, len(fx.events.recorded()))
	}
}

// TestApplyRunnerEventIdenticalStateOpensNoUnit: a report of the state the
// row already has is idempotent success with no unit and no event.
func TestApplyRunnerEventIdenticalStateOpensNoUnit(t *testing.T) {
	fx := newUOWFleetFixture(t)
	fx.st.seedRunner(fleetEventRunner(1))
	fx.st.seedSession(control.Session{
		ID: "sess_example", WorkspaceID: "ws_example", State: control.StateRunning,
		PoolID: "pool_example", RunnerID: "runner_example",
	})
	fx.uow.failCommit = true
	err := fx.svc.ApplyRunnerEvent(uowCtx, control.RunnerEvent{
		WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
		Generation: 1, SessionID: "sess_example", State: control.StateRunning,
	})
	if err != nil {
		t.Fatalf("already-applied event: err = %v, want nil", err)
	}
	if fx.uow.runs != 0 || len(fx.events.recorded()) != 0 {
		t.Fatalf("runs %d, %d events; want 0/0", fx.uow.runs, len(fx.events.recorded()))
	}
}

// ---------------------------------------------------------------------------
// AttachmentService
// ---------------------------------------------------------------------------

// uowAttachEvents is the attachment recorder reporting record depth.
type uowAttachEvents struct {
	*attachmentFakeEvents
	recordDepth int
}

func (f *uowAttachEvents) Record(ctx context.Context, e control.Event) error {
	f.recordDepth = unitDepth(ctx)
	return f.attachmentFakeEvents.Record(ctx, e)
}

type uowAttachFixture struct {
	svc       *AttachmentService
	uow       *countingUOW
	events    *uowAttachEvents
	transport *attachmentFakeTransport
	broker    *attachmentFakeBroker
}

func newUOWAttachFixture(t *testing.T) *uowAttachFixture {
	t.Helper()
	fx := &uowAttachFixture{
		uow:       &countingUOW{},
		events:    &uowAttachEvents{attachmentFakeEvents: &attachmentFakeEvents{}},
		transport: &attachmentFakeTransport{},
		broker:    &attachmentFakeBroker{},
	}
	svc, err := NewAttachmentService(AttachmentOptions{
		Authorizer: &attachmentFakeAuthorizer{},
		Policy:     &attachmentFakePolicy{},
		Sessions:   &attachmentFakeSessions{found: true, row: attachmentRunningSession()},
		Transport:  fx.transport,
		Broker:     fx.broker,
		Events:     fx.events,
		Clock:      attachmentFakeClock(func() time.Time { return sessionFixedNow }),
		IDs:        &attachmentFakeIDs{eventID: "evt_example"},
		UnitOfWork: fx.uow,
	})
	if err != nil {
		t.Fatalf("NewAttachmentService: %v", err)
	}
	fx.svc = svc
	return fx
}

// TestAttachTerminalRecordsInsideOneUnit: an attach's only write is its
// event; it commits as a unit, and a unit that cannot commit is
// ErrUnavailable.
func TestAttachTerminalRecordsInsideOneUnit(t *testing.T) {
	fx := newUOWAttachFixture(t)
	err := fx.svc.AttachTerminal(uowCtx, attachmentTestScope(), control.AttachTerminal{
		SessionID: "sess_example", Since: terminal.SinceAll, Mode: control.AttachmentViewer,
	}, &attachmentRecordingTerminalStream{})
	if err != nil {
		t.Fatal(err)
	}
	if fx.uow.runs != 1 || fx.events.recordDepth != 1 {
		t.Fatalf("runs %d, record at depth %d; want 1/1", fx.uow.runs, fx.events.recordDepth)
	}
	got := fx.events.snapshot()
	if len(got) != 1 || got[0].PlacementGeneration != 7 {
		t.Fatalf("recorded %+v, want one event at the session's placement generation 7", got)
	}

	fx.uow.failCommit = true
	err = fx.svc.AttachTerminal(uowCtx, attachmentTestScope(), control.AttachTerminal{
		SessionID: "sess_example", Since: terminal.SinceAll, Mode: control.AttachmentViewer,
	}, &attachmentRecordingTerminalStream{})
	if !errors.Is(err, control.ErrUnavailable) {
		t.Fatalf("uncommittable attach: err = %v, want ErrUnavailable", err)
	}
}

// TestWorkspaceRPCsRecordInsideOneUnit: push and pull each record their one
// event inside a unit; the bounded transfer that precedes it is transport,
// not a store write, and stays outside.
func TestWorkspaceRPCsRecordInsideOneUnit(t *testing.T) {
	t.Run("push", func(t *testing.T) {
		fx := newUOWAttachFixture(t)
		fx.transport.replyFn = defaultPushAck
		if err := fx.svc.PushWorkspace(uowCtx, attachmentTestScope(), control.PushWorkspace{
			SessionID: "sess_example", Path: "dst", Body: strings.NewReader("hello"),
		}); err != nil {
			t.Fatal(err)
		}
		if fx.uow.runs != 1 || fx.events.recordDepth != 1 {
			t.Fatalf("runs %d, record at depth %d; want 1/1", fx.uow.runs, fx.events.recordDepth)
		}
		if got := fx.events.snapshot(); len(got) != 1 || got[0].PlacementGeneration != 7 {
			t.Fatalf("recorded %+v, want one event at placement generation 7", got)
		}
		fx.uow.failCommit = true
		if err := fx.svc.PushWorkspace(uowCtx, attachmentTestScope(), control.PushWorkspace{
			SessionID: "sess_example", Path: "dst", Body: strings.NewReader("hello"),
		}); !errors.Is(err, control.ErrUnavailable) {
			t.Fatalf("uncommittable push: err = %v, want ErrUnavailable", err)
		}
	})

	t.Run("pull", func(t *testing.T) {
		fx := newUOWAttachFixture(t)
		fx.transport.replyFn = servePull([]workspace.PullChunk{{Seq: 0, Data: []byte("hello"), Done: true}})
		var sink strings.Builder
		if err := fx.svc.PullWorkspace(uowCtx, attachmentTestScope(), control.PullWorkspace{
			SessionID: "sess_example", Path: "src", Body: &sink,
		}); err != nil {
			t.Fatal(err)
		}
		if fx.uow.runs != 1 || fx.events.recordDepth != 1 {
			t.Fatalf("runs %d, record at depth %d; want 1/1", fx.uow.runs, fx.events.recordDepth)
		}
		fx.uow.failCommit = true
		fx.transport.replyFn = servePull([]workspace.PullChunk{{Seq: 0, Data: []byte("hello"), Done: true}})
		if err := fx.svc.PullWorkspace(uowCtx, attachmentTestScope(), control.PullWorkspace{
			SessionID: "sess_example", Path: "src", Body: &sink,
		}); !errors.Is(err, control.ErrUnavailable) {
			t.Fatalf("uncommittable pull: err = %v, want ErrUnavailable", err)
		}
	})
}

package controlapp

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// attachmentTerminalPort is the single-method contract AttachmentService must
// satisfy for terminal attach; the full control.Attachments assertion lives in
// the external-package test once all four methods exist.
type attachmentTerminalPort interface {
	AttachTerminal(context.Context, control.Scope, control.AttachTerminal, control.TerminalStream) error
}

var _ attachmentTerminalPort = (*AttachmentService)(nil)

func TestAttachAuthorizesBeforeBroker(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.auth.err = control.ErrDenied
	err := fx.svc.AttachTerminal(context.Background(), attachmentTestScope(), control.AttachTerminal{
		SessionID: "sess_example", Since: terminal.SinceAll,
		Mode: control.AttachmentViewer,
	}, &attachmentRecordingTerminalStream{})
	if !errors.Is(err, control.ErrDenied) {
		t.Fatalf("got %v", err)
	}
	if fx.broker.calls != 0 {
		t.Fatal("denied attach reached broker")
	}
}

func TestNewAttachmentServiceRejectsMissingDependency(t *testing.T) {
	base := AttachmentOptions{
		Authorizer: &attachmentFakeAuthorizer{},
		Policy:     &attachmentFakePolicy{},
		Sessions:   &attachmentFakeSessions{},
		Transport:  &attachmentFakeTransport{},
		Broker:     &attachmentFakeBroker{},
		Events:     &attachmentFakeEvents{},
		Clock:      attachmentFakeClock(func() time.Time { return time.Unix(0, 0) }),
		IDs:        attachmentFakeIDs{eventID: "evt_example"},
		UnitOfWork: directUOW{},
	}
	tests := []struct {
		name   string
		nilify func(*AttachmentOptions)
	}{
		{"authorizer", func(o *AttachmentOptions) { o.Authorizer = nil }},
		{"policy", func(o *AttachmentOptions) { o.Policy = nil }},
		{"sessions", func(o *AttachmentOptions) { o.Sessions = nil }},
		{"transport", func(o *AttachmentOptions) { o.Transport = nil }},
		{"broker", func(o *AttachmentOptions) { o.Broker = nil }},
		{"events", func(o *AttachmentOptions) { o.Events = nil }},
		{"clock", func(o *AttachmentOptions) { o.Clock = nil }},
		{"ids", func(o *AttachmentOptions) { o.IDs = nil }},
		{"unit of work", func(o *AttachmentOptions) { o.UnitOfWork = nil }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := base
			tt.nilify(&opts)
			if _, err := NewAttachmentService(opts); !errors.Is(err, control.ErrInvalid) {
				t.Fatalf("got %v, want ErrInvalid", err)
			}
		})
	}
	if _, err := NewAttachmentService(base); err != nil {
		t.Fatalf("complete options rejected: %v", err)
	}
}

func TestAttachTerminalRejections(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*attachmentFixture, *control.Scope, *control.AttachTerminal)
		want   error
	}{
		{
			name:   "invalid scope",
			mutate: func(_ *attachmentFixture, sc *control.Scope, _ *control.AttachTerminal) { sc.WorkspaceID = "" },
			want:   control.ErrInvalid,
		},
		{
			name: "nil stream",
			mutate: func(_ *attachmentFixture, _ *control.Scope, cmd *control.AttachTerminal) {
				cmd.Mode = control.AttachmentViewer
			},
			want: control.ErrInvalid,
		},
		{
			name: "unknown mode",
			mutate: func(_ *attachmentFixture, _ *control.Scope, cmd *control.AttachTerminal) {
				cmd.Mode = control.AttachmentMode("admin")
			},
			want: control.ErrInvalid,
		},
		{
			name: "cross-workspace not found",
			mutate: func(fx *attachmentFixture, _ *control.Scope, _ *control.AttachTerminal) {
				fx.sessions.found = false
			},
			want: control.ErrNotFound,
		},
		{
			name: "queued session",
			mutate: func(fx *attachmentFixture, _ *control.Scope, _ *control.AttachTerminal) {
				fx.sessions.row.State = control.StateQueued
			},
			want: control.ErrConflict,
		},
		{
			name: "suspended session",
			mutate: func(fx *attachmentFixture, _ *control.Scope, _ *control.AttachTerminal) {
				fx.sessions.row.State = control.StateSuspendedWarm
			},
			want: control.ErrConflict,
		},
		{
			name: "dead session",
			mutate: func(fx *attachmentFixture, _ *control.Scope, _ *control.AttachTerminal) {
				fx.sessions.row.State = control.StateDead
			},
			want: control.ErrConflict,
		},
		{
			name: "failed session with disconnected runner",
			mutate: func(fx *attachmentFixture, _ *control.Scope, _ *control.AttachTerminal) {
				fx.sessions.row.State = control.StateFailed
				fx.transport.connected = false
			},
			want: control.ErrConflict,
		},
		{
			name: "authorizer denied",
			mutate: func(fx *attachmentFixture, _ *control.Scope, _ *control.AttachTerminal) {
				fx.auth.err = control.ErrDenied
			},
			want: control.ErrDenied,
		},
		{
			name: "policy denied",
			mutate: func(fx *attachmentFixture, _ *control.Scope, _ *control.AttachTerminal) {
				fx.policy.err = control.ErrDenied
			},
			want: control.ErrDenied,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fx := newAttachmentFixture(t)
			fx.sessions.found = true
			fx.sessions.row = attachmentRunningSession()
			scope := attachmentTestScope()
			cmd := control.AttachTerminal{SessionID: "sess_example", Since: terminal.SinceAll, Mode: control.AttachmentViewer}
			var stream control.TerminalStream = &attachmentRecordingTerminalStream{}
			if tt.name == "nil stream" {
				stream = nil
			}
			tt.mutate(fx, &scope, &cmd)
			err := fx.svc.AttachTerminal(context.Background(), scope, cmd, stream)
			if !errors.Is(err, tt.want) {
				t.Fatalf("got %v, want %v", err, tt.want)
			}
			if fx.broker.calls != 0 {
				t.Fatalf("rejected attach reached broker %d times", fx.broker.calls)
			}
		})
	}
}

func TestAttachTerminalRunning(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.sessions.found = true
	fx.sessions.row = attachmentRunningSession()
	stream := &attachmentRecordingTerminalStream{}
	if err := fx.svc.AttachTerminal(context.Background(), attachmentTestScope(), control.AttachTerminal{
		SessionID: "sess_example", Since: terminal.SinceAll, Mode: control.AttachmentViewer,
	}, stream); err != nil {
		t.Fatalf("attach: %v", err)
	}
	if fx.broker.calls != 1 {
		t.Fatalf("broker called %d times, want 1", fx.broker.calls)
	}
	target := fx.broker.lastTarget
	if target.WorkspaceID != "ws_example" || target.SessionID != "sess_example" ||
		target.PoolID != "pool_example" || target.RunnerID != "runner_example" ||
		target.PlacementGeneration != 7 {
		t.Fatalf("target = %+v", target)
	}
	if stream.closed {
		t.Fatal("module closed a broker-owned stream")
	}
}

func TestAttachTerminalFailedButConnected(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.sessions.found = true
	fx.sessions.row = attachmentRunningSession()
	fx.sessions.row.State = control.StateFailed
	fx.transport.connected = true
	if err := fx.svc.AttachTerminal(context.Background(), attachmentTestScope(), control.AttachTerminal{
		SessionID: "sess_example", Mode: control.AttachmentViewer,
	}, &attachmentRecordingTerminalStream{}); err != nil {
		t.Fatalf("failed-but-connected attach: %v", err)
	}
	if fx.broker.calls != 1 {
		t.Fatalf("broker called %d times, want 1", fx.broker.calls)
	}
}

func TestAttachControllerGenerations(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.sessions.found = true
	fx.sessions.row = attachmentRunningSession()
	ctx := context.Background()
	scope := attachmentTestScope()

	attach := func(mode control.AttachmentMode) {
		t.Helper()
		if err := fx.svc.AttachTerminal(ctx, scope, control.AttachTerminal{
			SessionID: "sess_example", Mode: mode,
		}, &attachmentRecordingTerminalStream{}); err != nil {
			t.Fatalf("%s attach: %v", mode, err)
		}
	}

	attach(control.AttachmentViewer)
	if got := fx.broker.lastTarget.ControllerGeneration; got != 0 {
		t.Fatalf("first viewer generation = %d, want 0", got)
	}
	attach(control.AttachmentController)
	if got := fx.broker.lastTarget.ControllerGeneration; got != 1 {
		t.Fatalf("first controller generation = %d, want 1", got)
	}
	attach(control.AttachmentController)
	if got := fx.broker.lastTarget.ControllerGeneration; got != 2 {
		t.Fatalf("second controller generation = %d, want 2", got)
	}
	attach(control.AttachmentViewer)
	if got := fx.broker.lastTarget.ControllerGeneration; got != 2 {
		t.Fatalf("viewer after controllers generation = %d, want 2", got)
	}
}

// TestControllerGenerationIsTheRepositorys pins the lease's home: a viewer
// attaches under the row's current generation, a controller asks the
// repository for the next one, and the service keeps no generation of its
// own.
func TestControllerGenerationIsTheRepositorys(t *testing.T) {
	fx := newAttachmentFixture(t)
	row := attachmentRunningSession()
	row.ControllerGeneration = 4
	fx.sessions.found, fx.sessions.row = true, row
	ctx := context.Background()
	scope := attachmentTestScope()

	if err := fx.svc.AttachTerminal(ctx, scope, control.AttachTerminal{
		SessionID: "sess_example", Mode: control.AttachmentViewer,
	}, &attachmentRecordingTerminalStream{}); err != nil {
		t.Fatal(err)
	}
	if got := fx.broker.lastTarget.ControllerGeneration; got != 4 {
		t.Fatalf("viewer generation = %d, want the row's 4", got)
	}
	if fx.sessions.nextCalls != 0 {
		t.Fatalf("a viewer asked the repository %d times, want 0", fx.sessions.nextCalls)
	}

	if err := fx.svc.AttachTerminal(ctx, scope, control.AttachTerminal{
		SessionID: "sess_example", Mode: control.AttachmentController,
	}, &attachmentRecordingTerminalStream{}); err != nil {
		t.Fatal(err)
	}
	if got := fx.broker.lastTarget.ControllerGeneration; got != 5 || fx.sessions.nextCalls != 1 {
		t.Fatalf("controller generation = %d (repo calls %d), want 5 from one NextControllerGeneration",
			got, fx.sessions.nextCalls)
	}
}

func TestAttachConcurrentControllersDistinct(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.sessions.found = true
	fx.sessions.row = attachmentRunningSession()
	ctx := context.Background()
	scope := attachmentTestScope()

	const n = 8
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fx.svc.AttachTerminal(ctx, scope, control.AttachTerminal{
				SessionID: "sess_example", Mode: control.AttachmentController,
			}, &attachmentRecordingTerminalStream{}); err != nil {
				t.Errorf("controller attach: %v", err)
			}
		}()
	}
	wg.Wait()

	gens := fx.broker.targetGenerations()
	if len(gens) != n {
		t.Fatalf("broker saw %d attaches, want %d", len(gens), n)
	}
	seen := map[uint64]bool{}
	for _, g := range gens {
		if g == 0 || g > n {
			t.Fatalf("generation %d out of range [1,%d]", g, n)
		}
		if seen[g] {
			t.Fatalf("duplicate generation %d", g)
		}
		seen[g] = true
	}
}

func TestAttachBrokerErrorClosesStream(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.sessions.found = true
	fx.sessions.row = attachmentRunningSession()
	fx.broker.err = errors.New("synthetic broker failure")
	stream := &attachmentRecordingTerminalStream{}
	err := fx.svc.AttachTerminal(context.Background(), attachmentTestScope(), control.AttachTerminal{
		SessionID: "sess_example", Mode: control.AttachmentViewer,
	}, stream)
	if !errors.Is(err, control.ErrUnavailable) {
		t.Fatalf("got %v, want ErrUnavailable", err)
	}
	if !stream.closed {
		t.Fatal("stream was not closed")
	}
	if !errors.Is(stream.closeErr, control.ErrUnavailable) {
		t.Fatalf("stream close error = %v, want ErrUnavailable", stream.closeErr)
	}
}

func TestAttachRecordsEventAndNeverReadsStream(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.sessions.found = true
	fx.sessions.row = attachmentRunningSession()
	stream := &attachmentRecordingTerminalStream{}
	if err := fx.svc.AttachTerminal(context.Background(), attachmentTestScope(), control.AttachTerminal{
		SessionID: "sess_example", Mode: control.AttachmentViewer,
	}, stream); err != nil {
		t.Fatalf("attach: %v", err)
	}
	if stream.recvCalls != 0 || stream.sendCalls != 0 {
		t.Fatalf("module read terminal messages: recv=%d send=%d", stream.recvCalls, stream.sendCalls)
	}
	evs := fx.events.snapshot()
	if len(evs) != 1 {
		t.Fatalf("recorded %d events, want 1", len(evs))
	}
	ev := evs[0]
	if ev.Action != control.ActionAttach || ev.Resource.Kind != control.ResourceSession ||
		ev.Resource.ID != "sess_example" || ev.WorkspaceID != "ws_example" {
		t.Fatalf("event = %+v", ev)
	}
}

func TestAttachRecorderFailureReturnsUnavailable(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.sessions.found = true
	fx.sessions.row = attachmentRunningSession()
	fx.events.err = errors.New("synthetic recorder failure")
	err := fx.svc.AttachTerminal(context.Background(), attachmentTestScope(), control.AttachTerminal{
		SessionID: "sess_example", Mode: control.AttachmentViewer,
	}, &attachmentRecordingTerminalStream{})
	if err != control.ErrUnavailable {
		t.Fatalf("got %v, want the closed ErrUnavailable sentinel", err)
	}
	if fx.broker.calls != 1 {
		t.Fatalf("broker called %d times, want 1", fx.broker.calls)
	}
	if fx.events.calls != 1 {
		t.Fatalf("recorder called %d times, want 1", fx.events.calls)
	}
}

func TestAttachEmptyEventIDPreventsBroker(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.sessions.found = true
	fx.sessions.row = attachmentRunningSession()
	fx.ids.eventID = ""
	err := fx.svc.AttachTerminal(context.Background(), attachmentTestScope(), control.AttachTerminal{
		SessionID: "sess_example", Mode: control.AttachmentViewer,
	}, &attachmentRecordingTerminalStream{})
	if !errors.Is(err, control.ErrUnavailable) {
		t.Fatalf("got %v, want ErrUnavailable", err)
	}
	if fx.broker.calls != 0 {
		t.Fatalf("empty event ID attach reached broker %d times", fx.broker.calls)
	}
}

func TestAttachPolicyViewGrant(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.sessions.found = true
	fx.sessions.row = attachmentRunningSession()
	fx.policy.deny = map[control.AttachmentMode]bool{control.AttachmentController: true}
	ctx := context.Background()
	scope := attachmentTestScope()

	if err := fx.svc.AttachTerminal(ctx, scope, control.AttachTerminal{
		SessionID: "sess_example", Mode: control.AttachmentViewer,
	}, &attachmentRecordingTerminalStream{}); err != nil {
		t.Fatalf("viewer under view grant: %v", err)
	}
	err := fx.svc.AttachTerminal(ctx, scope, control.AttachTerminal{
		SessionID: "sess_example", Mode: control.AttachmentController,
	}, &attachmentRecordingTerminalStream{})
	if !errors.Is(err, control.ErrDenied) {
		t.Fatalf("controller under view grant = %v, want ErrDenied", err)
	}
}

func TestAttachPolicyControlGrant(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.sessions.found = true
	fx.sessions.row = attachmentRunningSession()
	ctx := context.Background()
	scope := attachmentTestScope()

	if err := fx.svc.AttachTerminal(ctx, scope, control.AttachTerminal{
		SessionID: "sess_example", Mode: control.AttachmentViewer,
	}, &attachmentRecordingTerminalStream{}); err != nil {
		t.Fatalf("viewer under control grant: %v", err)
	}
	if err := fx.svc.AttachTerminal(ctx, scope, control.AttachTerminal{
		SessionID: "sess_example", Mode: control.AttachmentController,
	}, &attachmentRecordingTerminalStream{}); err != nil {
		t.Fatalf("controller under control grant: %v", err)
	}
}

func TestAttachActionDenialPrecedesPolicy(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.sessions.found = true
	fx.sessions.row = attachmentRunningSession()
	fx.auth.err = control.ErrDenied
	err := fx.svc.AttachTerminal(context.Background(), attachmentTestScope(), control.AttachTerminal{
		SessionID: "sess_example", Mode: control.AttachmentController,
	}, &attachmentRecordingTerminalStream{})
	if !errors.Is(err, control.ErrDenied) {
		t.Fatalf("got %v, want ErrDenied", err)
	}
	if fx.policy.calls != 0 {
		t.Fatalf("policy ran %d times after action denial", fx.policy.calls)
	}
	if fx.broker.calls != 0 {
		t.Fatal("denied attach reached broker")
	}
}

// ---------------------------------------------------------------------------
// shared fakes
// ---------------------------------------------------------------------------

func attachmentTestScope() control.Scope {
	return control.Scope{
		WorkspaceID: "ws_example",
		Actor:       control.Actor{ID: "act_example", Kind: control.ActorUser},
		Placement: control.PlacementScope{
			ProductRegion: "us", HomeCell: "cell-1", Mode: control.ExecutionDedicated,
		},
	}
}

func attachmentRunningSession() control.Session {
	return control.Session{
		ID:                  "sess_example",
		WorkspaceID:         "ws_example",
		CreatorID:           "act_creator",
		State:               control.StateRunning,
		PoolID:              "pool_example",
		RunnerID:            "runner_example",
		PlacementGeneration: 7,
		Spec: control.PortableSpec{
			Image:       "img_example",
			Cmd:         []string{"make"},
			EgressAllow: []string{"example.com"},
			Repos:       []control.RepoRef{{Repo: "acme/app", BaseBranch: "main"}},
		},
	}
}

type attachmentFixture struct {
	// now is the fake clock every lease in these tests is measured against;
	// a test moves it rather than sleeping, which is the only way a 30s lease
	// is worth testing at all.
	now       time.Time
	svc       *AttachmentService
	auth      *attachmentFakeAuthorizer
	policy    *attachmentFakePolicy
	sessions  *attachmentFakeSessions
	transport *attachmentFakeTransport
	broker    *attachmentFakeBroker
	events    *attachmentFakeEvents
	ids       *attachmentFakeIDs
}

func newAttachmentFixture(t *testing.T) *attachmentFixture {
	t.Helper()
	fx := &attachmentFixture{
		now:       time.Unix(1_700_000_000, 0).UTC(),
		auth:      &attachmentFakeAuthorizer{},
		policy:    &attachmentFakePolicy{},
		sessions:  &attachmentFakeSessions{found: true, row: attachmentRunningSession()},
		transport: &attachmentFakeTransport{},
		broker:    &attachmentFakeBroker{},
		events:    &attachmentFakeEvents{},
		ids:       &attachmentFakeIDs{eventID: "evt_example"},
	}
	svc, err := NewAttachmentService(AttachmentOptions{
		Authorizer: fx.auth,
		Policy:     fx.policy,
		Sessions:   fx.sessions,
		Transport:  fx.transport,
		Broker:     fx.broker,
		Events:     fx.events,
		Clock:      attachmentFakeClock(func() time.Time { return fx.now }),
		IDs:        fx.ids,
		UnitOfWork: directUOW{},
	})
	if err != nil {
		t.Fatalf("NewAttachmentService: %v", err)
	}
	fx.svc = svc
	return fx
}

type attachmentFakeAuthorizer struct {
	mu           sync.Mutex
	err          error
	calls        int
	lastScope    control.Scope
	lastAction   control.Action
	lastResource control.Resource
}

func (f *attachmentFakeAuthorizer) Authorize(_ context.Context, sc control.Scope, a control.Action, r control.Resource) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastScope, f.lastAction, f.lastResource = sc, a, r
	return f.err
}

type attachmentFakePolicy struct {
	mu       sync.Mutex
	err      error
	deny     map[control.AttachmentMode]bool
	calls    int
	lastMode control.AttachmentMode
	asked    []control.AttachmentMode
}

func (f *attachmentFakePolicy) AuthorizeAttachment(_ context.Context, _ control.Scope, _ control.Resource, mode control.AttachmentMode) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastMode = mode
	f.asked = append(f.asked, mode)
	if f.deny != nil && f.deny[mode] {
		return control.ErrDenied
	}
	return f.err
}

// modes is every mode this policy was asked about, in order. The FIRST is the
// one an attach was admitted on, which is the question the edge's
// pre-upgrade check asks too; a later one is a privilege asked for
// separately.
func (f *attachmentFakePolicy) modes() []control.AttachmentMode {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]control.AttachmentMode(nil), f.asked...)
}

type attachmentFakeSessions struct {
	mu         sync.Mutex
	found      bool
	row        control.Session
	err        error
	calls      int
	nextCalls  int
	casCalls   int
	renewCalls int
	// renewErr, when set, is what every lease renew answers, so a test can
	// stage the two ways a claim's first renew can fail.
	renewErr error
	lastWS   control.WorkspaceID
	lastID   control.SessionID
}

func (f *attachmentFakeSessions) GetSession(_ context.Context, ws control.WorkspaceID, id control.SessionID) (control.Session, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastWS, f.lastID = ws, id
	if f.err != nil {
		return control.Session{}, f.err
	}
	if !f.found {
		return control.Session{}, control.ErrNotFound
	}
	return f.row, nil
}

func (f *attachmentFakeSessions) CreateSession(context.Context, control.WorkspaceID, control.Session) (control.Session, error) {
	return control.Session{}, nil
}
func (f *attachmentFakeSessions) SessionByIDem(context.Context, control.WorkspaceID, control.ActorID, string) (control.Session, error) {
	return control.Session{}, nil
}
func (f *attachmentFakeSessions) ListSessions(context.Context, control.WorkspaceID, control.SessionQuery) ([]control.Session, string, error) {
	return nil, "", nil
}
func (f *attachmentFakeSessions) Transition(context.Context, control.WorkspaceID, control.SessionID, []control.SessionState, control.SessionState, control.TransitionOpts) error {
	return nil
}
func (f *attachmentFakeSessions) SetSessionSetupHash(context.Context, control.WorkspaceID, control.SessionID, string) error {
	return nil
}

// compareAndAdvanceCalls is how many conditional claims reached the store.
func (f *attachmentFakeSessions) compareAndAdvanceCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.casCalls
}

// NextControllerGeneration is the repository's lease: it advances the stored
// row's generation, vacates the lease as the port requires — this is the
// unconditional take-over, and it displaces whoever held control — and
// returns the new value, counting the calls so a test can assert that only a
// controller attach asks for one.
func (f *attachmentFakeSessions) NextControllerGeneration(_ context.Context, ws control.WorkspaceID, id control.SessionID) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextCalls++
	if !f.found {
		return 0, control.ErrNotFound
	}
	f.row.ControllerGeneration++
	f.row.ControllerHolder = ""
	f.row.ControllerLeaseExpiresAt = time.Time{}
	return f.row.ControllerGeneration, nil
}

func (f *attachmentFakeSessions) SetChildExitCode(context.Context, control.WorkspaceID, control.SessionID, int) error {
	return nil
}

type attachmentFakeTransport struct {
	mu             sync.Mutex
	connected      bool
	dispatchErr    error
	replyFn        func(runner.ToRunner) runner.FromRunner
	replies        []runner.FromRunner
	got            []runner.ToRunner
	gotPool        []control.PoolID
	gotRunner      []control.RunnerID
	connectedCalls int
}

func (f *attachmentFakeTransport) Connected(control.PoolID, control.RunnerID) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.connectedCalls++
	return f.connected
}

func (f *attachmentFakeTransport) Dispatch(ctx context.Context, pool control.PoolID, rid control.RunnerID, m runner.ToRunner) (runner.FromRunner, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got = append(f.got, m)
	f.gotPool = append(f.gotPool, pool)
	f.gotRunner = append(f.gotRunner, rid)
	if f.dispatchErr != nil {
		return runner.FromRunner{}, f.dispatchErr
	}
	if err := ctx.Err(); err != nil {
		return runner.FromRunner{}, err
	}
	if f.replyFn != nil {
		return f.replyFn(m), nil
	}
	if len(f.replies) == 0 {
		return runner.FromRunner{}, control.ErrUnavailable
	}
	r := f.replies[0]
	f.replies = f.replies[1:]
	return r, nil
}

func (f *attachmentFakeTransport) dispatched() []runner.ToRunner {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]runner.ToRunner(nil), f.got...)
}

type attachmentFakeBroker struct {
	mu         sync.Mutex
	err        error
	calls      int
	lastTarget control.AttachTarget
	targets    []control.AttachTarget
}

func (f *attachmentFakeBroker) Attach(_ context.Context, target control.AttachTarget, _ control.TerminalStream) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastTarget = target
	f.targets = append(f.targets, target)
	return f.err
}

// target is the last binding the service handed this broker.
func (f *attachmentFakeBroker) target() control.AttachTarget {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastTarget
}

func (f *attachmentFakeBroker) targetGenerations() []uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]uint64, 0, len(f.targets))
	for _, tg := range f.targets {
		out = append(out, tg.ControllerGeneration)
	}
	return out
}

type attachmentFakeEvents struct {
	mu    sync.Mutex
	got   []control.Event
	calls int
	err   error
}

func (f *attachmentFakeEvents) Record(_ context.Context, e control.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return f.err
	}
	f.got = append(f.got, e)
	return nil
}

func (f *attachmentFakeEvents) snapshot() []control.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]control.Event(nil), f.got...)
}

type attachmentFakeClock func() time.Time

func (f attachmentFakeClock) Now() time.Time { return f() }

type attachmentFakeIDs struct{ eventID control.EventID }

func (f attachmentFakeIDs) NewSessionID() control.SessionID         { return "sess_example" }
func (f attachmentFakeIDs) NewEnvironmentID() control.EnvironmentID { return "env_example" }
func (f attachmentFakeIDs) NewEventID() control.EventID             { return f.eventID }

type attachmentRecordingTerminalStream struct {
	mu        sync.Mutex
	closed    bool
	closeErr  error
	recvCalls int
	sendCalls int
}

func (r *attachmentRecordingTerminalStream) Receive(context.Context) (terminal.ClientMessage, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recvCalls++
	return terminal.ClientMessage{}, nil
}

func (r *attachmentRecordingTerminalStream) Send(context.Context, terminal.ServerMessage) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sendCalls++
	return nil
}

func (r *attachmentRecordingTerminalStream) Close(err error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	r.closeErr = err
	return nil
}

// CompareAndAdvanceControllerGeneration is the conditional grant: it advances
// the row only from the exact generation the caller expected, so an attach
// test can stage the race the contract is about.
func (f *attachmentFakeSessions) CompareAndAdvanceControllerGeneration(_ context.Context, ws control.WorkspaceID,
	id control.SessionID, expected uint64) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.casCalls++
	if !f.found || f.row.ControllerGeneration != expected {
		return 0, control.ErrStale
	}
	f.row.ControllerGeneration++
	f.row.ControllerHolder = ""
	f.row.ControllerLeaseExpiresAt = time.Time{}
	return f.row.ControllerGeneration, nil
}

func (f *attachmentFakeSessions) RenewControllerLease(_ context.Context, ws control.WorkspaceID,
	id control.SessionID, l control.ControllerLease) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renewCalls++
	if f.renewErr != nil {
		return f.renewErr
	}
	if !f.found || f.row.ControllerGeneration != l.Generation {
		return control.ErrStale
	}
	if f.row.ControllerHolder != "" && f.row.ControllerHolder != l.Holder {
		return control.ErrStale
	}
	f.row.ControllerHolder = l.Holder
	f.row.ControllerLeaseExpiresAt = l.ExpiresAt
	return nil
}

// ---------------------------------------------------------------------------
// conditional controller ownership
// ---------------------------------------------------------------------------

// attachNegotiated runs one negotiated attach and returns the target the
// broker was handed, which is the whole of what the application granted.
func attachNegotiated(t *testing.T, fx *attachmentFixture, cmd control.AttachTerminal) control.AttachTarget {
	t.Helper()
	cmd.SessionID = "sess_example"
	cmd.Negotiated = true
	if err := fx.svc.AttachTerminal(context.Background(), attachmentTestScope(), cmd,
		&attachmentRecordingTerminalStream{}); err != nil {
		t.Fatalf("attach: %v", err)
	}
	fx.broker.mu.Lock()
	defer fx.broker.mu.Unlock()
	return fx.broker.lastTarget
}

// TestJourney1IdleSessionAttachClaimsControl is step 1: a laptop attaching to
// a session nobody is using becomes the controller at generation 1, with no
// flag, no prompt and no second round trip.
func TestJourney1IdleSessionAttachClaimsControl(t *testing.T) {
	fx := newAttachmentFixture(t)
	target := attachNegotiated(t, fx, control.AttachTerminal{Mode: control.AttachmentController})
	if target.Mode != control.AttachmentController {
		t.Fatalf("mode = %q, want controller", target.Mode)
	}
	if target.ControllerGeneration != 1 {
		t.Fatalf("generation = %d, want 1", target.ControllerGeneration)
	}
	if target.Controller == nil {
		t.Fatal("a negotiated attach was handed no lease keeper")
	}
	// It claimed conditionally, from the generation it read, and never
	// through the unconditional grant.
	fx.sessions.mu.Lock()
	defer fx.sessions.mu.Unlock()
	if fx.sessions.casCalls != 1 || fx.sessions.nextCalls != 0 {
		t.Fatalf("cas calls = %d, unconditional calls = %d; want 1 and 0",
			fx.sessions.casCalls, fx.sessions.nextCalls)
	}
	if fx.sessions.row.ControllerHolder == "" {
		t.Fatal("the winner took the generation but never took the lease")
	}
	if !fx.sessions.row.ControllerLeaseExpiresAt.Equal(fx.now.Add(control.ControllerLeaseTTL)) {
		t.Fatalf("lease expiry = %v, want now + %s", fx.sessions.row.ControllerLeaseExpiresAt, control.ControllerLeaseTTL)
	}
}

// TestJourney2SecondAttachUnderALiveLeaseIsAViewer is step 2: the phone gets
// the screen and the output, and it is told which generation is in force so
// it can decide to take control from it.
func TestJourney2SecondAttachUnderALiveLeaseIsAViewer(t *testing.T) {
	fx := newAttachmentFixture(t)
	first := attachNegotiated(t, fx, control.AttachTerminal{Mode: control.AttachmentController})
	second := attachNegotiated(t, fx, control.AttachTerminal{Mode: control.AttachmentController})

	if second.Mode != control.AttachmentViewer {
		t.Fatalf("the second attach's mode = %q, want viewer", second.Mode)
	}
	if second.ControllerGeneration != first.ControllerGeneration {
		t.Fatalf("the viewer was told generation %d, want the live one (%d)",
			second.ControllerGeneration, first.ControllerGeneration)
	}
	if second.Controller == nil {
		t.Fatal("a viewer was handed no keeper, so it can never take control")
	}
	fx.sessions.mu.Lock()
	defer fx.sessions.mu.Unlock()
	if fx.sessions.casCalls != 1 {
		t.Fatalf("cas calls = %d; a viewer attach must not even attempt a claim", fx.sessions.casCalls)
	}
}

// TestJourney5ACrashedControllersLeaseExpires is step 5's second half: a
// client that died without releasing holds control for the lease TTL and no
// longer, so the next attach claims with no click. The clock moves; nothing
// sleeps, and nothing sweeps.
func TestJourney5ACrashedControllersLeaseExpires(t *testing.T) {
	fx := newAttachmentFixture(t)
	attachNegotiated(t, fx, control.AttachTerminal{Mode: control.AttachmentController})

	// One second before the lease runs out, the dead device still holds it.
	fx.now = fx.now.Add(control.ControllerLeaseTTL - time.Second)
	if got := attachNegotiated(t, fx, control.AttachTerminal{Mode: control.AttachmentController}); got.Mode != control.AttachmentViewer {
		t.Fatalf("mode inside the lease = %q, want viewer", got.Mode)
	}
	// One second after, it does not.
	fx.now = fx.now.Add(2 * time.Second)
	got := attachNegotiated(t, fx, control.AttachTerminal{Mode: control.AttachmentController})
	if got.Mode != control.AttachmentController {
		t.Fatalf("mode after the lease expired = %q, want controller", got.Mode)
	}
	if got.ControllerGeneration != 2 {
		t.Fatalf("generation = %d, want 2", got.ControllerGeneration)
	}
}

// TestJourney5ReleasingAdvancesAndFrees is step 5's first half: a clean
// release frees control immediately AND advances the generation, so the
// departing controller's already-sent bytes are fenced and the next attach
// claims without waiting out anything.
func TestJourney5ReleasingAdvancesAndFrees(t *testing.T) {
	fx := newAttachmentFixture(t)
	target := attachNegotiated(t, fx, control.AttachTerminal{Mode: control.AttachmentController})
	if err := target.Controller.Release(context.Background(), target.ControllerGeneration); err != nil {
		t.Fatalf("release: %v", err)
	}
	fx.sessions.mu.Lock()
	row := fx.sessions.row
	fx.sessions.mu.Unlock()
	if row.ControllerGeneration != 2 {
		t.Fatalf("generation after release = %d, want 2 — a release that does not advance leaves the leaver's bytes executable", row.ControllerGeneration)
	}
	if row.ControllerHolder != "" {
		t.Fatalf("holder after release = %q, want vacant", row.ControllerHolder)
	}
	// And with no waiting: the very next attach is the controller.
	next := attachNegotiated(t, fx, control.AttachTerminal{Mode: control.AttachmentController})
	if next.Mode != control.AttachmentController || next.ControllerGeneration != 3 {
		t.Fatalf("the attach after a release = %q at %d, want controller at 3", next.Mode, next.ControllerGeneration)
	}
	// Releasing a generation somebody else has already taken is not an
	// error: it is the state the caller was asking for.
	if err := target.Controller.Release(context.Background(), 1); err != nil {
		t.Fatalf("a stale release: %v", err)
	}
}

// TestJourney6ReconnectIsConditional is step 6: presenting a generation is
// how a returning controller stays honest. Still holding it resumes control;
// having been superseded comes back a viewer, at the generation that actually
// exists now — never an auto-claim.
func TestJourney6ReconnectIsConditional(t *testing.T) {
	t.Run("still holds it", func(t *testing.T) {
		fx := newAttachmentFixture(t)
		first := attachNegotiated(t, fx, control.AttachTerminal{Mode: control.AttachmentController})
		back := attachNegotiated(t, fx, control.AttachTerminal{
			Mode: control.AttachmentController, ExpectedGeneration: first.ControllerGeneration})
		if back.Mode != control.AttachmentController {
			t.Fatalf("mode = %q, want controller", back.Mode)
		}
		if back.ControllerGeneration != 2 {
			t.Fatalf("generation = %d, want 2 — a resumed stream is a new binding", back.ControllerGeneration)
		}
	})
	t.Run("was superseded", func(t *testing.T) {
		fx := newAttachmentFixture(t)
		first := attachNegotiated(t, fx, control.AttachTerminal{Mode: control.AttachmentController})
		// Somebody else took control while this device was away.
		if _, err := first.Controller.Claim(context.Background(), first.ControllerGeneration); err != nil {
			t.Fatalf("the take-over: %v", err)
		}
		back := attachNegotiated(t, fx, control.AttachTerminal{
			Mode: control.AttachmentController, ExpectedGeneration: first.ControllerGeneration})
		if back.Mode != control.AttachmentViewer {
			t.Fatalf("mode = %q, want viewer — a reconnect must never take control back on its own", back.Mode)
		}
		if back.ControllerGeneration != 2 {
			t.Fatalf("the returning device was told generation %d, want the current 2", back.ControllerGeneration)
		}
	})
}

// TestNegotiatedViewerNeverClaims pins --view: whatever the session's state,
// an attach that asked to view does not touch the generation.
func TestNegotiatedViewerNeverClaims(t *testing.T) {
	fx := newAttachmentFixture(t)
	got := attachNegotiated(t, fx, control.AttachTerminal{Mode: control.AttachmentViewer})
	if got.Mode != control.AttachmentViewer || got.ControllerGeneration != 0 {
		t.Fatalf("mode %q at generation %d, want viewer at 0", got.Mode, got.ControllerGeneration)
	}
	fx.sessions.mu.Lock()
	defer fx.sessions.mu.Unlock()
	if fx.sessions.casCalls != 0 || fx.sessions.nextCalls != 0 {
		t.Fatalf("a --view attach claimed: cas %d, unconditional %d", fx.sessions.casCalls, fx.sessions.nextCalls)
	}
}

// TestLegacyAttachIsRecordedAsATakeOver is the old client + new plane
// pairing, at the layer that decides it. A client that cannot be told it is a
// viewer is admitted as the controller unconditionally — and that advance is
// exactly what fences and notifies a negotiated client attached at the same
// time, which is the only safe way to admit it.
func TestLegacyAttachIsRecordedAsATakeOver(t *testing.T) {
	fx := newAttachmentFixture(t)
	negotiated := attachNegotiated(t, fx, control.AttachTerminal{Mode: control.AttachmentController})

	if err := fx.svc.AttachTerminal(context.Background(), attachmentTestScope(), control.AttachTerminal{
		SessionID: "sess_example", Mode: control.AttachmentController, // Negotiated deliberately false
	}, &attachmentRecordingTerminalStream{}); err != nil {
		t.Fatalf("legacy attach: %v", err)
	}
	fx.broker.mu.Lock()
	legacy := fx.broker.lastTarget
	fx.broker.mu.Unlock()

	if legacy.Mode != control.AttachmentController {
		t.Fatalf("a legacy attach's mode = %q, want controller", legacy.Mode)
	}
	if legacy.Controller != nil {
		t.Fatal("a legacy attach was handed a keeper; it has no way to use one")
	}
	if legacy.ControllerGeneration <= negotiated.ControllerGeneration {
		t.Fatalf("a legacy attach did not advance the generation: %d, want more than %d",
			legacy.ControllerGeneration, negotiated.ControllerGeneration)
	}
	// The negotiated client that was the controller a moment ago now finds
	// its heartbeat refused, which is how it learns and how it is fenced.
	if err := negotiated.Controller.Renew(context.Background(), negotiated.ControllerGeneration); !errors.Is(err, control.ErrStale) {
		t.Fatalf("the displaced controller's heartbeat: err = %v, want ErrStale", err)
	}
}

// TestTwoDevicesRacingFromOneGenerationHaveOneWinner is step 3 at the
// application layer: the repository decides it, and the service must not
// soften the answer into two controllers.
func TestTwoDevicesRacingFromOneGenerationHaveOneWinner(t *testing.T) {
	fx := newAttachmentFixture(t)
	seed := attachNegotiated(t, fx, control.AttachTerminal{Mode: control.AttachmentController})
	from := seed.ControllerGeneration

	var wg sync.WaitGroup
	results := make([]error, 2)
	for i := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, results[i] = seed.Controller.Claim(context.Background(), from)
		}()
	}
	wg.Wait()

	winners := 0
	for i, err := range results {
		switch {
		case err == nil:
			winners++
		case errors.Is(err, control.ErrStale):
		default:
			t.Fatalf("claim %d: %v", i, err)
		}
	}
	if winners != 1 {
		t.Fatalf("%d of 2 claims from generation %d won; want exactly 1", winners, from)
	}
}

// TestJourney6ReconnectResumesAfterItsOwnReleaseFreedTheLease is the case a
// dropped connection actually produces, and the one the "still holds it"
// branch above cannot reach: a controller whose attach ended released on its
// way out, so by the time it dials back the generation has already moved past
// the one it is presenting and NOBODY holds control.
//
// Coming back a viewer there would be a false statement — there is no other
// device — and would cost the zero-click property to one lost packet.
func TestJourney6ReconnectResumesAfterItsOwnReleaseFreedTheLease(t *testing.T) {
	fx := newAttachmentFixture(t)
	first := attachNegotiated(t, fx, control.AttachTerminal{Mode: control.AttachmentController})
	// What the plane does when an attach ends, however it ended.
	if err := first.Controller.Release(context.Background(), first.ControllerGeneration); err != nil {
		t.Fatalf("release: %v", err)
	}

	back := attachNegotiated(t, fx, control.AttachTerminal{
		Mode: control.AttachmentController, ExpectedGeneration: first.ControllerGeneration})
	if back.Mode != control.AttachmentController {
		t.Fatalf("mode = %q, want controller — nobody else had it", back.Mode)
	}
	if back.ControllerGeneration != 3 {
		t.Fatalf("generation = %d, want 3", back.ControllerGeneration)
	}
}

// TestAClaimWhoseFirstRenewIsStaleIsALostClaim pins the difference between
// the two ways a claim's lease write can fail. ErrStale means the generation
// this claim just won has already been advanced past: the claim is a moment
// old and already lost, and answering it with success would tell two devices
// at once that they have control.
func TestAClaimWhoseFirstRenewIsStaleIsALostClaim(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.sessions.renewErr = control.ErrStale

	got := attachNegotiated(t, fx, control.AttachTerminal{Mode: control.AttachmentController})
	if got.Mode != control.AttachmentViewer {
		t.Fatalf("mode = %q, want viewer — the generation moved out from under the claim", got.Mode)
	}
}

// TestAClaimSurvivesAStoreThatIsBrieflyUnusable is the other half. A renew
// that fails for any reason OTHER than staleness failed at a generation that
// is still this attach's, and the generation is the authority while the lease
// is only the hint — so the claim stands and the heartbeat installs the lease
// on its next pass. Failing here would advance the generation, displace
// whoever had control, and then hand control to nobody.
func TestAClaimSurvivesAStoreThatIsBrieflyUnusable(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.sessions.renewErr = control.ErrUnavailable

	got := attachNegotiated(t, fx, control.AttachTerminal{Mode: control.AttachmentController})
	if got.Mode != control.AttachmentController {
		t.Fatalf("mode = %q, want controller", got.Mode)
	}
	if got.ControllerGeneration != 1 {
		t.Fatalf("generation = %d, want 1", got.ControllerGeneration)
	}
}

// TestAnAttachAsksTheHostsPolicyOnceAtTheDoor pins the cost of the seam. The
// attach-time grant of a negotiated controller attach is the one claim whose
// policy question was answered microseconds earlier, on the very same mode,
// so it is not asked again — a host whose policy is a network call or an
// audited decision pays once per attach, and a backend that blinks between
// two identical calls cannot turn a dependency outage into "not authorized".
//
// The claims that follow on the client's own stream ARE asked, every time,
// which is what honours a grant revoked mid-attach.
func TestAnAttachAsksTheHostsPolicyOnceAtTheDoor(t *testing.T) {
	fx := newAttachmentFixture(t)
	if err := fx.svc.AttachTerminal(context.Background(), attachmentTestScope(), control.AttachTerminal{
		SessionID: "sess_example", Mode: control.AttachmentController, Negotiated: true,
	}, &attachmentRecordingTerminalStream{}); err != nil {
		t.Fatalf("a negotiated control attach: %v", err)
	}
	if got := fx.policy.modes(); len(got) != 1 || got[0] != control.AttachmentController {
		t.Fatalf("the policy was asked %v; want exactly one controller question", got)
	}

	// And the mid-attach claim asks for itself.
	target := fx.broker.target()
	if _, err := target.Controller.Claim(context.Background(), target.ControllerGeneration); err != nil {
		t.Fatalf("a mid-attach claim: %v", err)
	}
	if got := fx.policy.modes(); len(got) != 2 {
		t.Fatalf("the policy was asked %v; a mid-attach claim must ask for itself", got)
	}
}

// TestAViewOnlyPrincipalWatchesAndMayNotClaim is the mode-aware attachment
// policy from both sides at once, which is the whole reason that seam exists:
// a host that grants viewing without granting driving — the Cloud
// collaboration policy — must be able to admit `rainier attach --view`, and
// must never have that viewer become a controller.
//
// Authorizing every negotiated attach as a controller made the first half
// impossible. It locked a view-only principal out of a session it may
// perfectly well watch, and it left the edge's pre-upgrade check (which asks
// about the mode the client asked for) asking a different question from the
// service — so such a caller would get a 101 upgrade and then a
// policy-violation close instead of a clean 403.
//
// An attach is therefore authorized for the mode it OPENS in, and the
// privilege it might REACH is asked for separately: once here, to decide what
// the plane is told, and again live on the claim itself.
func TestAViewOnlyPrincipalWatchesAndMayNotClaim(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.policy.deny = map[control.AttachmentMode]bool{control.AttachmentController: true}

	if err := fx.svc.AttachTerminal(context.Background(), attachmentTestScope(), control.AttachTerminal{
		SessionID: "sess_example", Mode: control.AttachmentViewer, Negotiated: true,
	}, &attachmentRecordingTerminalStream{}); err != nil {
		t.Fatalf("a negotiated view attach under a controller-denying policy: %v", err)
	}
	if modes := fx.policy.modes(); len(modes) == 0 || modes[0] != control.AttachmentViewer {
		t.Fatalf("the attach was admitted on %v, want the viewer mode it opens in", modes)
	}
	target := fx.broker.target()
	switch {
	case target.Mode != control.AttachmentViewer:
		t.Fatalf("granted mode = %q, want viewer", target.Mode)
	case !target.Negotiated:
		t.Fatal("a negotiated attach was not marked negotiated; its client would be told nothing")
	case target.MayClaim:
		t.Fatal("a principal the policy refuses the controller was handed the right to claim")
	case target.Controller == nil:
		t.Fatal("a negotiated viewer got no keeper, so it cannot read its own generation")
	}

	// And the claim path refuses it live, whatever the plane does with the
	// flag: the store is never reached, so nobody is displaced on the way to
	// finding out.
	before := fx.sessions.compareAndAdvanceCalls()
	if _, err := target.Controller.Claim(context.Background(), 0); !errors.Is(err, control.ErrDenied) {
		t.Fatalf("a view-only principal's mid-attach claim: err = %v, want ErrDenied", err)
	}
	if got := fx.sessions.compareAndAdvanceCalls(); got != before {
		t.Fatalf("a refused claim still advanced the generation %d time(s)", got-before)
	}

	// A negotiated CONTROLLER attach under the same policy is still refused
	// at the door, which is where `mode=control` is answered.
	if err := fx.svc.AttachTerminal(context.Background(), attachmentTestScope(), control.AttachTerminal{
		SessionID: "sess_example", Mode: control.AttachmentController, Negotiated: true,
	}, &attachmentRecordingTerminalStream{}); !errors.Is(err, control.ErrDenied) {
		t.Fatalf("a negotiated control attach under a controller-denying policy: err = %v, want ErrDenied", err)
	}

	// An UNNEGOTIATED viewer is authorized as what it is and may not claim:
	// it has no way to send one.
	fx.policy.deny = nil
	if err := fx.svc.AttachTerminal(context.Background(), attachmentTestScope(), control.AttachTerminal{
		SessionID: "sess_example", Mode: control.AttachmentViewer,
	}, &attachmentRecordingTerminalStream{}); err != nil {
		t.Fatalf("an unnegotiated view attach: %v", err)
	}
	if got := fx.policy.authorizedMode(); got != control.AttachmentViewer {
		t.Fatalf("an unnegotiated viewer was authorized as %q, want viewer", got)
	}
	if tg := fx.broker.target(); tg.Negotiated || tg.MayClaim {
		t.Fatalf("an unnegotiated attach was marked negotiated=%v mayClaim=%v", tg.Negotiated, tg.MayClaim)
	}

	// A permitted negotiated controller attach carries both facts.
	if err := fx.svc.AttachTerminal(context.Background(), attachmentTestScope(), control.AttachTerminal{
		SessionID: "sess_example", Mode: control.AttachmentController, Negotiated: true,
	}, &attachmentRecordingTerminalStream{}); err != nil {
		t.Fatalf("a permitted negotiated control attach: %v", err)
	}
	if tg := fx.broker.target(); !tg.Negotiated || !tg.MayClaim {
		t.Fatalf("a permitted controller attach: negotiated=%v mayClaim=%v, want both true", tg.Negotiated, tg.MayClaim)
	}
}

// authorizedMode is the mode the last attach was actually authorized for.
func (f *attachmentFakePolicy) authorizedMode() control.AttachmentMode {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastMode
}

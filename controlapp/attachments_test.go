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
		Clock:      attachmentFakeClock(func() time.Time { return time.Unix(0, 0) }),
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
}

func (f *attachmentFakePolicy) AuthorizeAttachment(_ context.Context, _ control.Scope, _ control.Resource, mode control.AttachmentMode) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastMode = mode
	if f.deny != nil && f.deny[mode] {
		return control.ErrDenied
	}
	return f.err
}

type attachmentFakeSessions struct {
	mu        sync.Mutex
	found     bool
	row       control.Session
	err       error
	calls     int
	nextCalls int
	lastWS    control.WorkspaceID
	lastID    control.SessionID
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

// NextControllerGeneration is the repository's lease: it advances the stored
// row's generation and returns the new value, counting the calls so a test can
// assert that only a controller attach asks for one.
func (f *attachmentFakeSessions) NextControllerGeneration(_ context.Context, ws control.WorkspaceID, id control.SessionID) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextCalls++
	if !f.found {
		return 0, control.ErrNotFound
	}
	f.row.ControllerGeneration++
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

package controlapp

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// ---------------------------------------------------------------------------
// shared fake store
//
// One store backs all of the port fakes so the repository fakes stay mutually
// coherent: a Transition is immediately visible to SessionsOnRunner and
// OldestQueued, exactly as a real persistence layer would make it.
// ---------------------------------------------------------------------------

type fakeStore struct {
	mu sync.Mutex

	sessions map[control.WorkspaceID]map[control.SessionID]control.Session
	runners  map[control.PoolID]map[control.RunnerID]control.Runner
	envs     map[control.WorkspaceID]map[control.EnvironmentID]control.Environment

	// call logs
	upsertCalls           int
	listRunnersCalls      int
	sessionsOnRunnerCalls int
	oldestQueuedCalls     int
	getSessionCalls       int
	transitionCalls       int
	setupHashCalls        int
	childExitCalls        int

	// injected failures
	upsertErr      error
	listRunnersErr error
	getSessionErr  error
	transitionErr  error
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		sessions: map[control.WorkspaceID]map[control.SessionID]control.Session{},
		runners:  map[control.PoolID]map[control.RunnerID]control.Runner{},
		envs:     map[control.WorkspaceID]map[control.EnvironmentID]control.Environment{},
	}
}

func containsState(states []control.SessionState, s control.SessionState) bool {
	for _, want := range states {
		if want == s {
			return true
		}
	}
	return false
}

func (st *fakeStore) seedSession(s control.Session) {
	st.mu.Lock()
	defer st.mu.Unlock()
	m := st.sessions[s.WorkspaceID]
	if m == nil {
		m = map[control.SessionID]control.Session{}
		st.sessions[s.WorkspaceID] = m
	}
	m[s.ID] = s
}

func (st *fakeStore) seedRunner(r control.Runner) {
	st.mu.Lock()
	defer st.mu.Unlock()
	m := st.runners[r.PoolID]
	if m == nil {
		m = map[control.RunnerID]control.Runner{}
		st.runners[r.PoolID] = m
	}
	m[r.ID] = r
}

func (st *fakeStore) seedEnv(e control.Environment) {
	st.mu.Lock()
	defer st.mu.Unlock()
	m := st.envs[e.WorkspaceID]
	if m == nil {
		m = map[control.EnvironmentID]control.Environment{}
		st.envs[e.WorkspaceID] = m
	}
	m[e.ID] = e
}

func (st *fakeStore) getSession(ws control.WorkspaceID, id control.SessionID) (control.Session, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.getSessionCalls++
	if st.getSessionErr != nil {
		return control.Session{}, st.getSessionErr
	}
	m := st.sessions[ws]
	s, ok := m[id]
	if !ok {
		return control.Session{}, control.ErrNotFound
	}
	return s, nil
}

func (st *fakeStore) transition(ws control.WorkspaceID, id control.SessionID, from []control.SessionState, to control.SessionState, opts control.TransitionOpts) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.transitionCalls++
	if st.transitionErr != nil {
		return st.transitionErr
	}
	m := st.sessions[ws]
	s, ok := m[id]
	if !ok {
		return control.ErrNotFound
	}
	if !containsState(from, s.State) {
		return control.ErrConflict
	}
	s.State = to
	if opts.RunnerID != nil {
		s.RunnerID = *opts.RunnerID
	}
	if opts.Error != nil {
		s.Error = *opts.Error
	}
	m[id] = s
	return nil
}

func (st *fakeStore) setSessionSetupHash(ws control.WorkspaceID, id control.SessionID, hash string) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.setupHashCalls++
	m := st.sessions[ws]
	s, ok := m[id]
	if !ok {
		return control.ErrNotFound
	}
	s.SetupHash = hash
	m[id] = s
	return nil
}

func (st *fakeStore) setChildExitCode(ws control.WorkspaceID, id control.SessionID, code int) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.childExitCalls++
	m := st.sessions[ws]
	s, ok := m[id]
	if !ok {
		return control.ErrNotFound
	}
	c := code
	s.ChildExitCode = &c
	m[id] = s
	return nil
}

func (st *fakeStore) upsertRunner(pool control.PoolID, r control.Runner) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.upsertCalls++
	if st.upsertErr != nil {
		return st.upsertErr
	}
	m := st.runners[pool]
	if m == nil {
		m = map[control.RunnerID]control.Runner{}
		st.runners[pool] = m
	}
	m[r.ID] = r
	return nil
}

func (st *fakeStore) listRunners(pool control.PoolID) ([]control.Runner, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.listRunnersCalls++
	if st.listRunnersErr != nil {
		return nil, st.listRunnersErr
	}
	m := st.runners[pool]
	out := make([]control.Runner, 0, len(m))
	for _, r := range m {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return string(out[i].ID) < string(out[j].ID) })
	return out, nil
}

func (st *fakeStore) sessionsOnRunner(pool control.PoolID, id control.RunnerID, states []control.SessionState) ([]control.Session, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.sessionsOnRunnerCalls++
	var out []control.Session
	for _, m := range st.sessions {
		for _, s := range m {
			if s.PoolID == pool && s.RunnerID == id && containsState(states, s.State) {
				out = append(out, s)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return string(out[i].ID) < string(out[j].ID) })
	return out, nil
}

func (st *fakeStore) oldestQueued(pool control.PoolID) ([]control.Session, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.oldestQueuedCalls++
	var out []control.Session
	for _, m := range st.sessions {
		for _, s := range m {
			if s.PoolID == pool && s.State == control.StateQueued {
				out = append(out, s)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return string(out[i].ID) < string(out[j].ID)
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out, nil
}

func (st *fakeStore) getEnv(ws control.WorkspaceID, id control.EnvironmentID) (control.Environment, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	m := st.envs[ws]
	e, ok := m[id]
	if !ok {
		return control.Environment{}, control.ErrNotFound
	}
	return e, nil
}

// ---------------------------------------------------------------------------
// port fakes
// ---------------------------------------------------------------------------

type fakeAuthorizer struct {
	mu        sync.Mutex
	calls     int
	deny      error
	actions   []control.Action
	resources []control.Resource
}

func (f *fakeAuthorizer) Authorize(_ context.Context, _ control.Scope, a control.Action, r control.Resource) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.actions = append(f.actions, a)
	f.resources = append(f.resources, r)
	return f.deny
}

type fakeSessions struct{ st *fakeStore }

func (f *fakeSessions) CreateSession(context.Context, control.WorkspaceID, control.Session) (control.Session, error) {
	return control.Session{}, control.ErrUnsupported
}
func (f *fakeSessions) GetSession(_ context.Context, ws control.WorkspaceID, id control.SessionID) (control.Session, error) {
	return f.st.getSession(ws, id)
}
func (f *fakeSessions) SessionByIDem(context.Context, control.WorkspaceID, control.ActorID, string) (control.Session, error) {
	return control.Session{}, control.ErrNotFound
}
func (f *fakeSessions) ListSessions(context.Context, control.WorkspaceID, control.SessionQuery) ([]control.Session, string, error) {
	return nil, "", nil
}
func (f *fakeSessions) Transition(_ context.Context, ws control.WorkspaceID, id control.SessionID, from []control.SessionState, to control.SessionState, opts control.TransitionOpts) error {
	return f.st.transition(ws, id, from, to, opts)
}
func (f *fakeSessions) SetSessionSetupHash(_ context.Context, ws control.WorkspaceID, id control.SessionID, hash string) error {
	return f.st.setSessionSetupHash(ws, id, hash)
}
func (f *fakeSessions) SetChildExitCode(_ context.Context, ws control.WorkspaceID, id control.SessionID, code int) error {
	return f.st.setChildExitCode(ws, id, code)
}

type fakeEnvironments struct{ st *fakeStore }

func (f *fakeEnvironments) CreateEnvironment(context.Context, control.WorkspaceID, control.Environment) (control.Environment, error) {
	return control.Environment{}, control.ErrUnsupported
}
func (f *fakeEnvironments) GetEnvironment(_ context.Context, ws control.WorkspaceID, id control.EnvironmentID) (control.Environment, error) {
	return f.st.getEnv(ws, id)
}
func (f *fakeEnvironments) ListEnvironments(context.Context, control.WorkspaceID, control.EnvironmentQuery) ([]control.Environment, string, error) {
	return nil, "", nil
}
func (f *fakeEnvironments) UpdateEnvironment(context.Context, control.WorkspaceID, control.Environment) (control.Environment, error) {
	return control.Environment{}, control.ErrUnsupported
}
func (f *fakeEnvironments) DeleteEnvironment(context.Context, control.WorkspaceID, control.EnvironmentID) error {
	return control.ErrUnsupported
}
func (f *fakeEnvironments) CountSessionsByEnvironment(context.Context, control.WorkspaceID, control.EnvironmentID, []control.SessionState) (int, error) {
	return 0, nil
}
func (f *fakeEnvironments) SetEnvironmentSnapshot(context.Context, control.WorkspaceID, control.EnvironmentID, string, string, control.RunnerID) error {
	return control.ErrUnsupported
}

type fakeFleet struct{ st *fakeStore }

func (f *fakeFleet) UpsertRunner(_ context.Context, pool control.PoolID, r control.Runner) error {
	return f.st.upsertRunner(pool, r)
}
func (f *fakeFleet) SetRunnerConnected(_ context.Context, pool control.PoolID, id control.RunnerID, connected bool) error {
	f.st.mu.Lock()
	defer f.st.mu.Unlock()
	m := f.st.runners[pool]
	r, ok := m[id]
	if !ok {
		return control.ErrNotFound
	}
	r.Connected = connected
	m[id] = r
	return nil
}
func (f *fakeFleet) ListRunners(_ context.Context, pool control.PoolID) ([]control.Runner, error) {
	return f.st.listRunners(pool)
}
func (f *fakeFleet) SessionsOnRunner(_ context.Context, pool control.PoolID, id control.RunnerID, states []control.SessionState) ([]control.Session, error) {
	return f.st.sessionsOnRunner(pool, id, states)
}
func (f *fakeFleet) OldestQueued(_ context.Context, pool control.PoolID) ([]control.Session, error) {
	return f.st.oldestQueued(pool)
}

type fakePools struct {
	mu    sync.Mutex
	calls int
	deny  error
	pools []control.Pool
}

func (f *fakePools) EligiblePools(_ context.Context, _ control.Scope, _ control.Requirements) ([]control.Pool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.deny != nil {
		return nil, f.deny
	}
	return slices.Clone(f.pools), nil
}

type fakeTransport struct {
	mu                 sync.Mutex
	dispatched         []runner.ToRunner
	dispatchErr        error
	dispatchReplies    []runner.FromRunner
	connectedDefault   bool
	connectedOverrides map[string]bool
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{connectedDefault: true, connectedOverrides: map[string]bool{}}
}

func poolRunnerKey(pool control.PoolID, id control.RunnerID) string {
	return string(pool) + "/" + string(id)
}

func (f *fakeTransport) Dispatch(_ context.Context, _ control.PoolID, _ control.RunnerID, m runner.ToRunner) (runner.FromRunner, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dispatched = append(f.dispatched, m)
	if f.dispatchErr != nil {
		return runner.FromRunner{}, f.dispatchErr
	}
	if len(f.dispatchReplies) > 0 {
		r := f.dispatchReplies[0]
		f.dispatchReplies = f.dispatchReplies[1:]
		return r, nil
	}
	return runner.FromRunner{OK: true}, nil
}

func (f *fakeTransport) Connected(pool control.PoolID, id control.RunnerID) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if v, ok := f.connectedOverrides[poolRunnerKey(pool, id)]; ok {
		return v
	}
	return f.connectedDefault
}

func (f *fakeTransport) dispatchedCommands() []runner.ToRunner {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.dispatched)
}

type fakeEvents struct {
	mu     sync.Mutex
	events []control.Event
}

func (f *fakeEvents) Record(_ context.Context, e control.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, e)
	return nil
}

func (f *fakeEvents) recorded() []control.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.events)
}

type fakeClock struct{ now time.Time }

func (f *fakeClock) Now() time.Time { return f.now }

type fakeIDs struct {
	mu sync.Mutex
	n  int
}

func (f *fakeIDs) NewSessionID() control.SessionID {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n++
	return control.SessionID(fmt.Sprintf("sess_%d", f.n))
}
func (f *fakeIDs) NewEnvironmentID() control.EnvironmentID {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n++
	return control.EnvironmentID(fmt.Sprintf("env_%d", f.n))
}
func (f *fakeIDs) NewEventID() control.EventID {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n++
	return control.EventID(fmt.Sprintf("evt_%d", f.n))
}

// ---------------------------------------------------------------------------
// fixture
// ---------------------------------------------------------------------------

type fleetFixture struct {
	service   *FleetService
	auth      *fakeAuthorizer
	sessions  *fakeSessions
	envs      *fakeEnvironments
	fleet     *fakeFleet
	pools     *fakePools
	transport *fakeTransport
	events    *fakeEvents
	clock     *fakeClock
	ids       *fakeIDs
	st        *fakeStore
}

func newFleetFixture(t *testing.T) *fleetFixture {
	t.Helper()
	st := newFakeStore()
	auth := &fakeAuthorizer{}
	sessions := &fakeSessions{st: st}
	envs := &fakeEnvironments{st: st}
	fleet := &fakeFleet{st: st}
	pools := &fakePools{}
	transport := newFakeTransport()
	events := &fakeEvents{}
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	ids := &fakeIDs{}
	svc, err := NewFleetService(FleetOptions{
		Authorizer:     auth,
		Sessions:       sessions,
		Environments:   envs,
		Fleet:          fleet,
		Pools:          pools,
		Transport:      transport,
		Events:         events,
		Clock:          clock,
		IDs:            ids,
		SafetyInterval: time.Second,
	})
	if err != nil {
		t.Fatalf("NewFleetService: %v", err)
	}
	return &fleetFixture{
		service: svc, auth: auth, sessions: sessions, envs: envs, fleet: fleet,
		pools: pools, transport: transport, events: events, clock: clock, ids: ids, st: st,
	}
}

// wakePools drains every currently-buffered wake and returns the pool IDs in
// arrival order. It never blocks.
func wakePools(svc *FleetService) []control.PoolID {
	var out []control.PoolID
	for {
		select {
		case p := <-svc.wake:
			out = append(out, p)
		default:
			return out
		}
	}
}

func validScope() control.Scope {
	return control.Scope{
		WorkspaceID: "ws_example",
		Actor:       control.Actor{ID: "act_example", Kind: control.ActorService},
		Placement: control.PlacementScope{
			ProductRegion: "us", HomeCell: "cell-1", Mode: control.ExecutionDedicated,
		},
	}
}

var ctx = context.Background()

// ---------------------------------------------------------------------------
// Task 1: constructor + registration + listing
// ---------------------------------------------------------------------------

type runnerRegistrationAndListing interface {
	RegisterRunner(context.Context, control.RunnerRegistration) (control.RunnerRegistrationResult, error)
	ListRunners(context.Context, control.Scope, control.RunnerQuery) (control.RunnerPage, error)
}

var _ runnerRegistrationAndListing = (*FleetService)(nil)

func TestNewFleetServiceRequiresEveryPort(t *testing.T) {
	base := func() FleetOptions {
		st := newFakeStore()
		return FleetOptions{
			Authorizer:     &fakeAuthorizer{},
			Sessions:       &fakeSessions{st: st},
			Environments:   &fakeEnvironments{st: st},
			Fleet:          &fakeFleet{st: st},
			Pools:          &fakePools{},
			Transport:      newFakeTransport(),
			Events:         &fakeEvents{},
			Clock:          &fakeClock{now: time.Now()},
			IDs:            &fakeIDs{},
			SafetyInterval: time.Second,
		}
	}
	if _, err := NewFleetService(base()); err != nil {
		t.Fatalf("complete options rejected: %v", err)
	}
	for name, zero := range map[string]func(*FleetOptions){
		"authorizer":   func(o *FleetOptions) { o.Authorizer = nil },
		"sessions":     func(o *FleetOptions) { o.Sessions = nil },
		"environments": func(o *FleetOptions) { o.Environments = nil },
		"fleet":        func(o *FleetOptions) { o.Fleet = nil },
		"pools":        func(o *FleetOptions) { o.Pools = nil },
		"transport":    func(o *FleetOptions) { o.Transport = nil },
		"events":       func(o *FleetOptions) { o.Events = nil },
		"clock":        func(o *FleetOptions) { o.Clock = nil },
		"ids":          func(o *FleetOptions) { o.IDs = nil },
	} {
		o := base()
		zero(&o)
		if _, err := NewFleetService(o); !errors.Is(err, control.ErrInvalid) {
			t.Errorf("missing %s: got %v, want ErrInvalid", name, err)
		}
	}
	o := base()
	o.SafetyInterval = 0
	if _, err := NewFleetService(o); !errors.Is(err, control.ErrInvalid) {
		t.Errorf("zero safety interval: got %v, want ErrInvalid", err)
	}
	o = base()
	o.SafetyInterval = -time.Second
	if _, err := NewFleetService(o); !errors.Is(err, control.ErrInvalid) {
		t.Errorf("negative safety interval: got %v, want ErrInvalid", err)
	}
}

func TestRegisterRunnerRejectsStaleGeneration(t *testing.T) {
	fx := newFleetFixture(t)
	fx.st.seedRunner(control.Runner{
		ID: "runner_example", PoolID: "pool_example", Generation: 8,
		CapacityTotal: 4, Connected: true,
	})
	got, err := fx.service.RegisterRunner(ctx, control.RunnerRegistration{
		WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
		Generation: 7, CapacityTotal: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Accepted || got.Generation != 8 {
		t.Fatalf("result = %+v", got)
	}
	if fx.st.upsertCalls != 0 {
		t.Fatal("stale registration mutated the store")
	}
	if len(wakePools(fx.service)) != 0 {
		t.Fatal("stale registration woke the pool")
	}
}

func TestRegisterRunnerValidation(t *testing.T) {
	valid := control.RunnerRegistration{
		WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
		Generation: 1, CapacityTotal: 4,
	}
	cases := map[string]func(*control.RunnerRegistration){
		"empty workspace":      func(r *control.RunnerRegistration) { r.WorkspaceID = "" },
		"empty pool":           func(r *control.RunnerRegistration) { r.PoolID = "" },
		"empty runner":         func(r *control.RunnerRegistration) { r.RunnerID = "" },
		"zero generation":      func(r *control.RunnerRegistration) { r.Generation = 0 },
		"negative used":        func(r *control.RunnerRegistration) { r.CapacityUsed = -1 },
		"negative total":       func(r *control.RunnerRegistration) { r.CapacityTotal = -1 },
		"over total":           func(r *control.RunnerRegistration) { r.CapacityUsed = 5 },
		"duplicate capability": func(r *control.RunnerRegistration) { r.Capabilities = []string{"gpu", "gpu"} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			fx := newFleetFixture(t)
			r := valid
			mutate(&r)
			if _, err := fx.service.RegisterRunner(ctx, r); !errors.Is(err, control.ErrInvalid) {
				t.Fatalf("got %v, want ErrInvalid", err)
			}
			if fx.st.upsertCalls != 0 {
				t.Fatal("invalid registration mutated the store")
			}
		})
	}
}

func TestRegisterRunnerIdempotentReconnectAndReplacement(t *testing.T) {
	t.Run("same generation reconnects idempotently", func(t *testing.T) {
		fx := newFleetFixture(t)
		fx.st.seedRunner(control.Runner{
			ID: "runner_example", PoolID: "pool_example", Generation: 3,
			CapacityTotal: 4, Connected: false,
		})
		got, err := fx.service.RegisterRunner(ctx, control.RunnerRegistration{
			WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
			Generation: 3, CapacityUsed: 1, CapacityTotal: 4, Capabilities: []string{"gpu"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if !got.Accepted || got.Generation != 3 {
			t.Fatalf("result = %+v, want accepted generation 3", got)
		}
		r, ok := fx.st.runners["pool_example"]["runner_example"]
		if !ok {
			t.Fatal("runner not stored")
		}
		if !r.Connected || r.Generation != 3 || r.CapacityUsed != 1 {
			t.Fatalf("stored runner = %+v", r)
		}
		if len(wakePools(fx.service)) != 1 {
			t.Fatal("accepted reconnect did not wake the pool")
		}
	})

	t.Run("newer generation replaces", func(t *testing.T) {
		fx := newFleetFixture(t)
		fx.st.seedRunner(control.Runner{
			ID: "runner_example", PoolID: "pool_example", Generation: 3,
			CapacityTotal: 4, Connected: true, Capabilities: []string{"gpu"},
		})
		got, err := fx.service.RegisterRunner(ctx, control.RunnerRegistration{
			WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
			Generation: 4, CapacityTotal: 8, Capabilities: []string{"gpu", "arm"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if !got.Accepted || got.Generation != 4 {
			t.Fatalf("result = %+v, want accepted generation 4", got)
		}
		r, ok := fx.st.runners["pool_example"]["runner_example"]
		if !ok {
			t.Fatal("runner not stored")
		}
		if r.Generation != 4 || r.CapacityTotal != 8 {
			t.Fatalf("stored runner = %+v", r)
		}
	})
}

func TestRegisterRunnerAdapterStale(t *testing.T) {
	fx := newFleetFixture(t)
	fx.st.upsertErr = control.ErrStale
	got, err := fx.service.RegisterRunner(ctx, control.RunnerRegistration{
		WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
		Generation: 1, CapacityTotal: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Accepted {
		t.Fatalf("result = %+v, want refused", got)
	}
	if fx.st.upsertCalls != 1 {
		t.Fatalf("upsertCalls = %d, want 1 attempt", fx.st.upsertCalls)
	}
	if len(wakePools(fx.service)) != 0 {
		t.Fatal("refused registration woke the pool")
	}
}

func TestRegisterRunnerCopiesCapabilities(t *testing.T) {
	fx := newFleetFixture(t)
	caps := []string{"gpu", "arm"}
	got, err := fx.service.RegisterRunner(ctx, control.RunnerRegistration{
		WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
		Generation: 1, CapacityTotal: 4, Capabilities: caps,
	})
	if err != nil || !got.Accepted {
		t.Fatalf("registration = %+v, %v", got, err)
	}
	// Mutating the caller's slice after the call must not affect the stored
	// runner.
	caps[0] = "nope"
	r := fx.st.runners["pool_example"]["runner_example"]
	if !slices.Equal(r.Capabilities, []string{"gpu", "arm"}) {
		t.Fatalf("stored capabilities = %v, want [gpu arm]", r.Capabilities)
	}
}

func TestListRunnersAuthorizesBeforeReads(t *testing.T) {
	fx := newFleetFixture(t)
	fx.auth.deny = control.ErrDenied
	fx.pools.pools = []control.Pool{{ID: "pool_example"}}
	if _, err := fx.service.ListRunners(ctx, validScope(), control.RunnerQuery{}); !errors.Is(err, control.ErrDenied) {
		t.Fatalf("got %v, want ErrDenied", err)
	}
	if fx.pools.calls != 0 {
		t.Fatal("EligiblePools was called despite denial")
	}
	if fx.st.listRunnersCalls != 0 {
		t.Fatal("fleet ListRunners was called despite denial")
	}
}

func TestListRunnersMergesSortsAndPaginates(t *testing.T) {
	fx := newFleetFixture(t)
	fx.pools.pools = []control.Pool{{ID: "pool_b"}, {ID: "pool_a"}}
	// Deliberately out of order across pools: the result must be sorted by
	// (RunnerID, PoolID).
	fx.st.seedRunner(control.Runner{ID: "runner_z", PoolID: "pool_b", Generation: 1})
	fx.st.seedRunner(control.Runner{ID: "runner_a", PoolID: "pool_b", Generation: 1})
	fx.st.seedRunner(control.Runner{ID: "runner_a", PoolID: "pool_a", Generation: 1})
	fx.st.seedRunner(control.Runner{ID: "runner_m", PoolID: "pool_a", Generation: 1})

	page, err := fx.service.ListRunners(ctx, validScope(), control.RunnerQuery{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Runners) != 2 {
		t.Fatalf("page has %d runners, want 2", len(page.Runners))
	}
	want := []control.Runner{
		{ID: "runner_a", PoolID: "pool_a", Generation: 1},
		{ID: "runner_a", PoolID: "pool_b", Generation: 1},
	}
	for i := range want {
		if page.Runners[i].ID != want[i].ID || page.Runners[i].PoolID != want[i].PoolID {
			t.Fatalf("page[%d] = %s/%s, want %s/%s", i, page.Runners[i].ID, page.Runners[i].PoolID, want[i].ID, want[i].PoolID)
		}
	}
	if page.NextCursor == "" {
		t.Fatal("expected a next cursor")
	}

	page2, err := fx.service.ListRunners(ctx, validScope(), control.RunnerQuery{Limit: 2, Cursor: page.NextCursor})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.Runners) != 2 {
		t.Fatalf("page2 has %d runners, want 2", len(page2.Runners))
	}
	want2 := []control.Runner{
		{ID: "runner_m", PoolID: "pool_a", Generation: 1},
		{ID: "runner_z", PoolID: "pool_b", Generation: 1},
	}
	for i := range want2 {
		if page2.Runners[i].ID != want2[i].ID || page2.Runners[i].PoolID != want2[i].PoolID {
			t.Fatalf("page2[%d] = %s/%s, want %s/%s", i, page2.Runners[i].ID, page2.Runners[i].PoolID, want2[i].ID, want2[i].PoolID)
		}
	}
	if page2.NextCursor != "" {
		t.Fatalf("last page next cursor = %q, want empty", page2.NextCursor)
	}
}

func TestListRunnersValidation(t *testing.T) {
	fx := newFleetFixture(t)
	fx.pools.pools = []control.Pool{{ID: "pool_example"}}

	if _, err := fx.service.ListRunners(ctx, validScope(), control.RunnerQuery{Limit: -1}); !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("negative limit: got %v, want ErrInvalid", err)
	}
	if _, err := fx.service.ListRunners(ctx, validScope(), control.RunnerQuery{Cursor: "not base64!!"}); !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("malformed cursor: got %v, want ErrInvalid", err)
	}
	if _, err := fx.service.ListRunners(ctx, validScope(), control.RunnerQuery{Cursor: "c2VjcmV0"}); !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("non-JSON cursor: got %v, want ErrInvalid", err)
	}

	// An empty result is [], never nil.
	page, err := fx.service.ListRunners(ctx, validScope(), control.RunnerQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if page.Runners == nil {
		t.Fatal("empty result is nil, want []")
	}
	if len(page.Runners) != 0 {
		t.Fatalf("empty result has %d runners", len(page.Runners))
	}
}

func TestListRunnersCopiesCapabilities(t *testing.T) {
	fx := newFleetFixture(t)
	fx.pools.pools = []control.Pool{{ID: "pool_example"}}
	fx.st.seedRunner(control.Runner{ID: "runner_example", PoolID: "pool_example", Generation: 1, Capabilities: []string{"gpu", "arm"}})

	page, err := fx.service.ListRunners(ctx, validScope(), control.RunnerQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Runners) != 1 {
		t.Fatalf("page has %d runners, want 1", len(page.Runners))
	}
	page.Runners[0].Capabilities[0] = "nope"
	stored := fx.st.runners["pool_example"]["runner_example"]
	if !slices.Equal(stored.Capabilities, []string{"gpu", "arm"}) {
		t.Fatalf("stored capabilities = %v, want [gpu arm]", stored.Capabilities)
	}
}

// ---------------------------------------------------------------------------
// Task 2: generation-fenced reconciliation
// ---------------------------------------------------------------------------

func reconcileRunnerRow() control.Runner {
	return control.Runner{ID: "runner_example", PoolID: "pool_example", Generation: 1, CapacityTotal: 4, Connected: true}
}

func getSessionState(t *testing.T, fx *fleetFixture, ws control.WorkspaceID, id control.SessionID) control.Session {
	t.Helper()
	s, err := fx.st.getSession(ws, id)
	if err != nil {
		t.Fatalf("get session %s: %v", id, err)
	}
	return s
}

func TestReconcileRunnerMatrix(t *testing.T) {
	tests := []struct {
		name        string
		stored      control.SessionState
		reported    *control.RunnerSession
		want        control.SessionState
		wantDestroy bool
	}{
		{"creating adopted running", control.StateCreating, &control.RunnerSession{SessionID: "sess_example", State: control.StateRunning}, control.StateRunning, false},
		{"running missing becomes dead", control.StateRunning, nil, control.StateDead, false},
		{"creating missing requeues", control.StateCreating, nil, control.StateQueued, false},
		{"terminal announced is orphan", control.StateDestroyed, &control.RunnerSession{SessionID: "sess_example", State: control.StateRunning}, control.StateDestroyed, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFleetFixture(t)
			fx.st.seedRunner(reconcileRunnerRow())
			fx.st.seedSession(control.Session{
				ID: "sess_example", WorkspaceID: "ws_example",
				State: tc.stored, PoolID: "pool_example", RunnerID: "runner_example",
			})

			snap := control.RunnerSnapshot{
				WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
				Generation: 1, CapacityTotal: 4,
			}
			if tc.reported != nil {
				snap.Sessions = []control.RunnerSession{*tc.reported}
			}

			got, err := fx.service.ReconcileRunner(ctx, snap)
			if err != nil {
				t.Fatal(err)
			}
			if got.Fenced {
				t.Fatalf("fenced unexpectedly: %+v", got)
			}
			if got.Generation != 1 {
				t.Fatalf("generation = %d, want 1", got.Generation)
			}
			if tc.wantDestroy {
				if len(got.Destroy) != 1 || got.Destroy[0] != "sess_example" {
					t.Fatalf("destroy = %v, want [sess_example]", got.Destroy)
				}
			} else if len(got.Destroy) != 0 {
				t.Fatalf("destroy = %v, want empty", got.Destroy)
			}
			row := getSessionState(t, fx, "ws_example", "sess_example")
			if row.State != tc.want {
				t.Fatalf("state = %q, want %q", row.State, tc.want)
			}
			if tc.stored == control.StateCreating && tc.reported == nil && row.RunnerID != "" {
				t.Fatalf("requeued session still carries runner %q", row.RunnerID)
			}
		})
	}
}

func TestReconcileRunnerFencesLowerGeneration(t *testing.T) {
	fx := newFleetFixture(t)
	fx.st.seedRunner(control.Runner{
		ID: "runner_example", PoolID: "pool_example", Generation: 8, CapacityTotal: 4, Connected: true,
	})
	// A live session exists, but the fence must return before reading it.
	fx.st.seedSession(control.Session{
		ID: "sess_example", WorkspaceID: "ws_example", State: control.StateCreating,
		PoolID: "pool_example", RunnerID: "runner_example",
	})

	got, err := fx.service.ReconcileRunner(ctx, control.RunnerSnapshot{
		WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
		Generation: 7, CapacityTotal: 4,
		Sessions: []control.RunnerSession{{SessionID: "sess_example", State: control.StateRunning}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !got.Fenced || got.Generation != 8 {
		t.Fatalf("result = %+v, want fenced at generation 8", got)
	}
	if fx.st.sessionsOnRunnerCalls != 0 || fx.st.getSessionCalls != 0 || fx.st.transitionCalls != 0 {
		t.Fatal("fenced snapshot read or mutated sessions")
	}
}

func TestReconcileRunnerNewerGenerationUpsertsThenReconciles(t *testing.T) {
	fx := newFleetFixture(t)
	fx.st.seedRunner(control.Runner{
		ID: "runner_example", PoolID: "pool_example", Generation: 3, CapacityTotal: 4, Connected: true,
	})
	fx.st.seedSession(control.Session{
		ID: "sess_example", WorkspaceID: "ws_example", State: control.StateRunning,
		PoolID: "pool_example", RunnerID: "runner_example",
	})

	got, err := fx.service.ReconcileRunner(ctx, control.RunnerSnapshot{
		WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
		Generation: 4, CapacityUsed: 2, CapacityTotal: 8,
		Sessions: []control.RunnerSession{{SessionID: "sess_example", State: control.StateSuspendedWarm}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Fenced || got.Generation != 4 {
		t.Fatalf("result = %+v, want unfenced at generation 4", got)
	}
	stored := fx.st.runners["pool_example"]["runner_example"]
	if stored.Generation != 4 || stored.CapacityTotal != 8 || stored.CapacityUsed != 2 {
		t.Fatalf("stored runner = %+v, want generation 4 capacity 2/8", stored)
	}
	row := getSessionState(t, fx, "ws_example", "sess_example")
	if row.State != control.StateSuspendedWarm {
		t.Fatalf("session = %q, want suspended_warm after adoption", row.State)
	}
}

func TestReconcileRunnerOrphans(t *testing.T) {
	t.Run("unknown announced session is destroyed", func(t *testing.T) {
		fx := newFleetFixture(t)
		fx.st.seedRunner(reconcileRunnerRow())
		got, err := fx.service.ReconcileRunner(ctx, control.RunnerSnapshot{
			WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
			Generation: 1, CapacityTotal: 4,
			Sessions: []control.RunnerSession{{SessionID: "sess_ghost", State: control.StateRunning}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Destroy) != 1 || got.Destroy[0] != "sess_ghost" {
			t.Fatalf("destroy = %v, want [sess_ghost]", got.Destroy)
		}
	})

	t.Run("duplicate announced ids are ErrInvalid", func(t *testing.T) {
		fx := newFleetFixture(t)
		fx.st.seedRunner(reconcileRunnerRow())
		_, err := fx.service.ReconcileRunner(ctx, control.RunnerSnapshot{
			WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
			Generation: 1, CapacityTotal: 4,
			Sessions: []control.RunnerSession{
				{SessionID: "sess_x", State: control.StateRunning},
				{SessionID: "sess_x", State: control.StateRunning},
			},
		})
		if !errors.Is(err, control.ErrInvalid) {
			t.Fatalf("got %v, want ErrInvalid", err)
		}
		if fx.st.transitionCalls != 0 {
			t.Fatal("duplicate snapshot mutated sessions")
		}
	})

	t.Run("a session from another workspace is destroyed without mutation", func(t *testing.T) {
		fx := newFleetFixture(t)
		fx.st.seedRunner(reconcileRunnerRow())
		fx.st.seedSession(control.Session{
			ID: "sess_other", WorkspaceID: "ws_other", State: control.StateRunning,
			PoolID: "pool_example", RunnerID: "runner_example",
		})
		got, err := fx.service.ReconcileRunner(ctx, control.RunnerSnapshot{
			WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
			Generation: 1, CapacityTotal: 4,
			Sessions: []control.RunnerSession{{SessionID: "sess_other", State: control.StateRunning}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Destroy) != 1 || got.Destroy[0] != "sess_other" {
			t.Fatalf("destroy = %v, want [sess_other]", got.Destroy)
		}
		other := getSessionState(t, fx, "ws_other", "sess_other")
		if other.State != control.StateRunning {
			t.Fatalf("other workspace's session mutated to %q", other.State)
		}
	})

	t.Run("mismatched pool or runner is destroyed without mutation", func(t *testing.T) {
		fx := newFleetFixture(t)
		fx.st.seedRunner(reconcileRunnerRow())
		fx.st.seedSession(control.Session{
			ID: "sess_dup", WorkspaceID: "ws_example", State: control.StateRunning,
			PoolID: "pool_other", RunnerID: "runner_other",
		})
		got, err := fx.service.ReconcileRunner(ctx, control.RunnerSnapshot{
			WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
			Generation: 1, CapacityTotal: 4,
			Sessions: []control.RunnerSession{{SessionID: "sess_dup", State: control.StateRunning}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Destroy) != 1 || got.Destroy[0] != "sess_dup" {
			t.Fatalf("destroy = %v, want [sess_dup]", got.Destroy)
		}
		dup := getSessionState(t, fx, "ws_example", "sess_dup")
		if dup.State != control.StateRunning || dup.RunnerID != "runner_other" {
			t.Fatalf("mismatched session mutated to %q on %q", dup.State, dup.RunnerID)
		}
	})

	t.Run("destroy output is deterministic and sorted", func(t *testing.T) {
		fx := newFleetFixture(t)
		fx.st.seedRunner(reconcileRunnerRow())
		got, err := fx.service.ReconcileRunner(ctx, control.RunnerSnapshot{
			WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
			Generation: 1, CapacityTotal: 4,
			Sessions: []control.RunnerSession{
				{SessionID: "sess_z", State: control.StateRunning},
				{SessionID: "sess_a", State: control.StateRunning},
				{SessionID: "sess_m", State: control.StateRunning},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		want := []control.SessionID{"sess_a", "sess_m", "sess_z"}
		if !slices.Equal(got.Destroy, want) {
			t.Fatalf("destroy = %v, want %v", got.Destroy, want)
		}

		// Idempotent: the same snapshot yields the same destroy list with no
		// additional state mutation.
		again, err := fx.service.ReconcileRunner(ctx, control.RunnerSnapshot{
			WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
			Generation: 1, CapacityTotal: 4,
			Sessions: []control.RunnerSession{
				{SessionID: "sess_z", State: control.StateRunning},
				{SessionID: "sess_a", State: control.StateRunning},
				{SessionID: "sess_m", State: control.StateRunning},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(again.Destroy, want) {
			t.Fatalf("second destroy = %v, want %v", again.Destroy, want)
		}
	})
}

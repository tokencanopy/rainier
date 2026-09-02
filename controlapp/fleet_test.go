package controlapp

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
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

type fleetFakeStore struct {
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

	// staleBump simulates a concurrent higher-generation write landing
	// between a pre-read and an upsert: when upsertErr is ErrStale and
	// staleBump is non-zero, the runner's stored generation is advanced to
	// staleBump before the stale refusal is returned.
	staleBump uint64
}

func newFleetFakeStore() *fleetFakeStore {
	return &fleetFakeStore{
		sessions: map[control.WorkspaceID]map[control.SessionID]control.Session{},
		runners:  map[control.PoolID]map[control.RunnerID]control.Runner{},
		envs:     map[control.WorkspaceID]map[control.EnvironmentID]control.Environment{},
	}
}

func fleetContainsState(states []control.SessionState, s control.SessionState) bool {
	for _, want := range states {
		if want == s {
			return true
		}
	}
	return false
}

// fleetSameSet reports whether a and b contain the same elements, ignoring order.
func fleetSameSet(a, b []string) bool {
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

func (st *fleetFakeStore) seedSession(s control.Session) {
	st.mu.Lock()
	defer st.mu.Unlock()
	m := st.sessions[s.WorkspaceID]
	if m == nil {
		m = map[control.SessionID]control.Session{}
		st.sessions[s.WorkspaceID] = m
	}
	m[s.ID] = s
}

func (st *fleetFakeStore) seedRunner(r control.Runner) {
	st.mu.Lock()
	defer st.mu.Unlock()
	m := st.runners[r.PoolID]
	if m == nil {
		m = map[control.RunnerID]control.Runner{}
		st.runners[r.PoolID] = m
	}
	m[r.ID] = r
}

func (st *fleetFakeStore) seedEnv(e control.Environment) {
	st.mu.Lock()
	defer st.mu.Unlock()
	m := st.envs[e.WorkspaceID]
	if m == nil {
		m = map[control.EnvironmentID]control.Environment{}
		st.envs[e.WorkspaceID] = m
	}
	m[e.ID] = e
}

func (st *fleetFakeStore) getSession(ws control.WorkspaceID, id control.SessionID) (control.Session, error) {
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

func (st *fleetFakeStore) transition(ws control.WorkspaceID, id control.SessionID, from []control.SessionState, to control.SessionState, opts control.TransitionOpts) error {
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
	if !fleetContainsState(from, s.State) {
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

func (st *fleetFakeStore) setSessionSetupHash(ws control.WorkspaceID, id control.SessionID, hash string) error {
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

func (st *fleetFakeStore) setChildExitCode(ws control.WorkspaceID, id control.SessionID, code int) error {
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

func (st *fleetFakeStore) upsertRunner(pool control.PoolID, r control.Runner) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.upsertCalls++
	if st.upsertErr != nil {
		if errors.Is(st.upsertErr, control.ErrStale) && st.staleBump > 0 {
			if m := st.runners[pool]; m != nil {
				if cur, ok := m[r.ID]; ok {
					cur.Generation = st.staleBump
					m[r.ID] = cur
				}
			}
		}
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

func (st *fleetFakeStore) listRunners(pool control.PoolID) ([]control.Runner, error) {
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

func (st *fleetFakeStore) sessionsOnRunner(pool control.PoolID, id control.RunnerID, states []control.SessionState) ([]control.Session, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.sessionsOnRunnerCalls++
	var out []control.Session
	for _, m := range st.sessions {
		for _, s := range m {
			if s.PoolID == pool && s.RunnerID == id && fleetContainsState(states, s.State) {
				out = append(out, s)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return string(out[i].ID) < string(out[j].ID) })
	return out, nil
}

func (st *fleetFakeStore) oldestQueued(pool control.PoolID) ([]control.Session, error) {
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

func (st *fleetFakeStore) getEnv(ws control.WorkspaceID, id control.EnvironmentID) (control.Environment, error) {
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

type fleetFakeAuthorizer struct {
	mu        sync.Mutex
	calls     int
	deny      error
	actions   []control.Action
	resources []control.Resource
}

func (f *fleetFakeAuthorizer) Authorize(_ context.Context, _ control.Scope, a control.Action, r control.Resource) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.actions = append(f.actions, a)
	f.resources = append(f.resources, r)
	return f.deny
}

type fleetFakeSessions struct{ st *fleetFakeStore }

func (f *fleetFakeSessions) CreateSession(context.Context, control.WorkspaceID, control.Session) (control.Session, error) {
	return control.Session{}, control.ErrUnsupported
}
func (f *fleetFakeSessions) GetSession(_ context.Context, ws control.WorkspaceID, id control.SessionID) (control.Session, error) {
	return f.st.getSession(ws, id)
}
func (f *fleetFakeSessions) SessionByIDem(context.Context, control.WorkspaceID, control.ActorID, string) (control.Session, error) {
	return control.Session{}, control.ErrNotFound
}
func (f *fleetFakeSessions) ListSessions(context.Context, control.WorkspaceID, control.SessionQuery) ([]control.Session, string, error) {
	return nil, "", nil
}
func (f *fleetFakeSessions) Transition(_ context.Context, ws control.WorkspaceID, id control.SessionID, from []control.SessionState, to control.SessionState, opts control.TransitionOpts) error {
	return f.st.transition(ws, id, from, to, opts)
}
func (f *fleetFakeSessions) SetSessionSetupHash(_ context.Context, ws control.WorkspaceID, id control.SessionID, hash string) error {
	return f.st.setSessionSetupHash(ws, id, hash)
}
func (f *fleetFakeSessions) SetChildExitCode(_ context.Context, ws control.WorkspaceID, id control.SessionID, code int) error {
	return f.st.setChildExitCode(ws, id, code)
}

type fleetFakeEnvironments struct{ st *fleetFakeStore }

func (f *fleetFakeEnvironments) CreateEnvironment(context.Context, control.WorkspaceID, control.Environment) (control.Environment, error) {
	return control.Environment{}, control.ErrUnsupported
}
func (f *fleetFakeEnvironments) GetEnvironment(_ context.Context, ws control.WorkspaceID, id control.EnvironmentID) (control.Environment, error) {
	return f.st.getEnv(ws, id)
}
func (f *fleetFakeEnvironments) ListEnvironments(context.Context, control.WorkspaceID, control.EnvironmentQuery) ([]control.Environment, string, error) {
	return nil, "", nil
}
func (f *fleetFakeEnvironments) UpdateEnvironment(context.Context, control.WorkspaceID, control.Environment) (control.Environment, error) {
	return control.Environment{}, control.ErrUnsupported
}
func (f *fleetFakeEnvironments) DeleteEnvironment(context.Context, control.WorkspaceID, control.EnvironmentID) error {
	return control.ErrUnsupported
}
func (f *fleetFakeEnvironments) CountSessionsByEnvironment(context.Context, control.WorkspaceID, control.EnvironmentID, []control.SessionState) (int, error) {
	return 0, nil
}
func (f *fleetFakeEnvironments) SetEnvironmentSnapshot(context.Context, control.WorkspaceID, control.EnvironmentID, string, string, control.RunnerID) error {
	return control.ErrUnsupported
}

type fleetFakeFleet struct{ st *fleetFakeStore }

func (f *fleetFakeFleet) UpsertRunner(_ context.Context, pool control.PoolID, r control.Runner) error {
	return f.st.upsertRunner(pool, r)
}
func (f *fleetFakeFleet) SetRunnerConnected(_ context.Context, pool control.PoolID, id control.RunnerID, connected bool) error {
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
func (f *fleetFakeFleet) ListRunners(_ context.Context, pool control.PoolID) ([]control.Runner, error) {
	return f.st.listRunners(pool)
}
func (f *fleetFakeFleet) SessionsOnRunner(_ context.Context, pool control.PoolID, id control.RunnerID, states []control.SessionState) ([]control.Session, error) {
	return f.st.sessionsOnRunner(pool, id, states)
}
func (f *fleetFakeFleet) OldestQueued(_ context.Context, pool control.PoolID) ([]control.Session, error) {
	return f.st.oldestQueued(pool)
}

type fleetFakePools struct {
	mu    sync.Mutex
	calls int
	deny  error
	pools []control.Pool
}

func (f *fleetFakePools) EligiblePools(_ context.Context, _ control.Scope, _ control.Requirements) ([]control.Pool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.deny != nil {
		return nil, f.deny
	}
	return slices.Clone(f.pools), nil
}

type fleetFakeTransport struct {
	mu                 sync.Mutex
	dispatched         []runner.ToRunner
	dispatchErr        error
	dispatchReplies    []runner.FromRunner
	connectedDefault   bool
	connectedOverrides map[string]bool
}

func newFleetFakeTransport() *fleetFakeTransport {
	return &fleetFakeTransport{connectedDefault: true, connectedOverrides: map[string]bool{}}
}

func fleetPoolRunnerKey(pool control.PoolID, id control.RunnerID) string {
	return string(pool) + "/" + string(id)
}

func (f *fleetFakeTransport) Dispatch(_ context.Context, _ control.PoolID, _ control.RunnerID, m runner.ToRunner) (runner.FromRunner, error) {
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

func (f *fleetFakeTransport) Connected(pool control.PoolID, id control.RunnerID) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if v, ok := f.connectedOverrides[fleetPoolRunnerKey(pool, id)]; ok {
		return v
	}
	return f.connectedDefault
}

func (f *fleetFakeTransport) dispatchedCommands() []runner.ToRunner {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.dispatched)
}

type fleetFakeEvents struct {
	mu        sync.Mutex
	events    []control.Event
	recordErr error
}

func (f *fleetFakeEvents) Record(_ context.Context, e control.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.recordErr != nil {
		return f.recordErr
	}
	f.events = append(f.events, e)
	return nil
}

func (f *fleetFakeEvents) recorded() []control.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.events)
}

type fleetFakeClock struct{ now time.Time }

func (f *fleetFakeClock) Now() time.Time { return f.now }

type fleetFakeIDs struct {
	mu           sync.Mutex
	n            int
	emptyEventID bool
}

func (f *fleetFakeIDs) NewSessionID() control.SessionID {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n++
	return control.SessionID(fmt.Sprintf("sess_%d", f.n))
}
func (f *fleetFakeIDs) NewEnvironmentID() control.EnvironmentID {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n++
	return control.EnvironmentID(fmt.Sprintf("env_%d", f.n))
}
func (f *fleetFakeIDs) NewEventID() control.EventID {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n++
	if f.emptyEventID {
		return ""
	}
	return control.EventID(fmt.Sprintf("evt_%d", f.n))
}

type fleetFakeResolver struct {
	mu       sync.Mutex
	calls    int
	material LaunchMaterial
	err      error
}

func (f *fleetFakeResolver) ResolveLaunchMaterial(_ context.Context, _ control.Session, _ *control.Environment) (LaunchMaterial, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return LaunchMaterial{}, f.err
	}
	return f.material, nil
}

// ---------------------------------------------------------------------------
// fixture
// ---------------------------------------------------------------------------

type fleetFixture struct {
	service   *FleetService
	auth      *fleetFakeAuthorizer
	sessions  *fleetFakeSessions
	envs      *fleetFakeEnvironments
	fleet     *fleetFakeFleet
	pools     *fleetFakePools
	transport *fleetFakeTransport
	events    *fleetFakeEvents
	clock     *fleetFakeClock
	ids       *fleetFakeIDs
	st        *fleetFakeStore
	resolver  *fleetFakeResolver
}

func newFleetFixtureWithResolver(t *testing.T, resolver LaunchMaterialResolver) *fleetFixture {
	t.Helper()
	st := newFleetFakeStore()
	auth := &fleetFakeAuthorizer{}
	sessions := &fleetFakeSessions{st: st}
	envs := &fleetFakeEnvironments{st: st}
	fleet := &fleetFakeFleet{st: st}
	pools := &fleetFakePools{}
	transport := newFleetFakeTransport()
	events := &fleetFakeEvents{}
	clock := &fleetFakeClock{now: time.Unix(1_700_000_000, 0)}
	ids := &fleetFakeIDs{}
	r := resolver
	if r == nil {
		r = &fleetFakeResolver{}
	}
	fr, ok := r.(*fleetFakeResolver)
	if !ok {
		fr = &fleetFakeResolver{}
	}
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
		LaunchMaterial: r,
	})
	if err != nil {
		t.Fatalf("NewFleetService: %v", err)
	}
	return &fleetFixture{
		service: svc, auth: auth, sessions: sessions, envs: envs, fleet: fleet,
		pools: pools, transport: transport, events: events, clock: clock, ids: ids, st: st,
		resolver: fr,
	}
}

func newFleetFixture(t *testing.T) *fleetFixture {
	t.Helper()
	return newFleetFixtureWithResolver(t, nil)
}

// fleetWakePools drains every currently-buffered wake and returns the pool IDs in
// arrival order. It never blocks.
func fleetWakePools(svc *FleetService) []control.PoolID {
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

func fleetValidScope() control.Scope {
	return control.Scope{
		WorkspaceID: "ws_example",
		Actor:       control.Actor{ID: "act_example", Kind: control.ActorService},
		Placement: control.PlacementScope{
			ProductRegion: "us", HomeCell: "cell-1", Mode: control.ExecutionDedicated,
		},
	}
}

var fleetCtx = context.Background()

// ---------------------------------------------------------------------------
// Task 1: constructor + registration + listing
// ---------------------------------------------------------------------------

type fleetRunnerRegistrationAndListing interface {
	RegisterRunner(context.Context, control.RunnerRegistration) (control.RunnerRegistrationResult, error)
	ListRunners(context.Context, control.Scope, control.RunnerQuery) (control.RunnerPage, error)
}

var _ fleetRunnerRegistrationAndListing = (*FleetService)(nil)

func TestNewFleetServiceRequiresEveryPort(t *testing.T) {
	base := func() FleetOptions {
		st := newFleetFakeStore()
		return FleetOptions{
			Authorizer:     &fleetFakeAuthorizer{},
			Sessions:       &fleetFakeSessions{st: st},
			Environments:   &fleetFakeEnvironments{st: st},
			Fleet:          &fleetFakeFleet{st: st},
			Pools:          &fleetFakePools{},
			Transport:      newFleetFakeTransport(),
			Events:         &fleetFakeEvents{},
			Clock:          &fleetFakeClock{now: time.Now()},
			IDs:            &fleetFakeIDs{},
			SafetyInterval: time.Second,
			LaunchMaterial: &fleetFakeResolver{},
		}
	}
	if _, err := NewFleetService(base()); err != nil {
		t.Fatalf("complete options rejected: %v", err)
	}
	for name, zero := range map[string]func(*FleetOptions){
		"authorizer":      func(o *FleetOptions) { o.Authorizer = nil },
		"sessions":        func(o *FleetOptions) { o.Sessions = nil },
		"environments":    func(o *FleetOptions) { o.Environments = nil },
		"fleet":           func(o *FleetOptions) { o.Fleet = nil },
		"pools":           func(o *FleetOptions) { o.Pools = nil },
		"transport":       func(o *FleetOptions) { o.Transport = nil },
		"events":          func(o *FleetOptions) { o.Events = nil },
		"clock":           func(o *FleetOptions) { o.Clock = nil },
		"ids":             func(o *FleetOptions) { o.IDs = nil },
		"launch material": func(o *FleetOptions) { o.LaunchMaterial = nil },
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
	got, err := fx.service.RegisterRunner(fleetCtx, control.RunnerRegistration{
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
	if len(fleetWakePools(fx.service)) != 0 {
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
			if _, err := fx.service.RegisterRunner(fleetCtx, r); !errors.Is(err, control.ErrInvalid) {
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
		got, err := fx.service.RegisterRunner(fleetCtx, control.RunnerRegistration{
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
		if len(fleetWakePools(fx.service)) != 1 {
			t.Fatal("accepted reconnect did not wake the pool")
		}
	})

	t.Run("newer generation replaces", func(t *testing.T) {
		fx := newFleetFixture(t)
		fx.st.seedRunner(control.Runner{
			ID: "runner_example", PoolID: "pool_example", Generation: 3,
			CapacityTotal: 4, Connected: true, Capabilities: []string{"gpu"},
		})
		got, err := fx.service.RegisterRunner(fleetCtx, control.RunnerRegistration{
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
	got, err := fx.service.RegisterRunner(fleetCtx, control.RunnerRegistration{
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
	if len(fleetWakePools(fx.service)) != 0 {
		t.Fatal("refused registration woke the pool")
	}
}

func TestRegisterRunnerAdapterStaleReportsAuthoritativeGeneration(t *testing.T) {
	fx := newFleetFixture(t)
	// A concurrent write advances the store between the service's pre-read and
	// its upsert; the adapter reports ErrStale and the service must re-read and
	// return the store-authoritative generation, never the caller's own.
	fx.st.seedRunner(control.Runner{
		ID: "runner_example", PoolID: "pool_example", Generation: 1,
		CapacityTotal: 4, Connected: true,
	})
	fx.st.upsertErr = control.ErrStale
	fx.st.staleBump = 9
	got, err := fx.service.RegisterRunner(fleetCtx, control.RunnerRegistration{
		WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
		Generation: 1, CapacityTotal: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Accepted {
		t.Fatalf("result = %+v, want refused", got)
	}
	if got.Generation != 9 {
		t.Fatalf("generation = %d, want the authoritative 9", got.Generation)
	}
	if fx.st.upsertCalls != 1 {
		t.Fatalf("upsertCalls = %d, want 1 attempt", fx.st.upsertCalls)
	}
	if len(fleetWakePools(fx.service)) != 0 {
		t.Fatal("refused registration woke the pool")
	}
}

func TestRegisterRunnerCopiesCapabilities(t *testing.T) {
	fx := newFleetFixture(t)
	caps := []string{"gpu", "arm"}
	got, err := fx.service.RegisterRunner(fleetCtx, control.RunnerRegistration{
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
	if _, err := fx.service.ListRunners(fleetCtx, fleetValidScope(), control.RunnerQuery{}); !errors.Is(err, control.ErrDenied) {
		t.Fatalf("got %v, want ErrDenied", err)
	}
	if fx.pools.calls != 0 {
		t.Fatal("EligiblePools was called despite denial")
	}
	if fx.st.listRunnersCalls != 0 {
		t.Fatal("fleet ListRunners was called despite denial")
	}
}

func TestListRunnersRejectsInvalidScopeBeforeAnyPort(t *testing.T) {
	fx := newFleetFixture(t)
	fx.pools.pools = []control.Pool{{ID: "pool_example"}}
	if _, err := fx.service.ListRunners(fleetCtx, control.Scope{}, control.RunnerQuery{}); !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("got %v, want ErrInvalid", err)
	}
	if fx.auth.calls != 0 {
		t.Fatal("Authorizer was called for an invalid scope")
	}
	if fx.pools.calls != 0 {
		t.Fatal("EligiblePools was called for an invalid scope")
	}
	if fx.st.listRunnersCalls != 0 {
		t.Fatal("fleet ListRunners was called for an invalid scope")
	}
}

func TestListRunnersNormalizesAuthorizerRefusal(t *testing.T) {
	fx := newFleetFixture(t)
	fx.auth.deny = errors.New("internal policy refused")
	fx.pools.pools = []control.Pool{{ID: "pool_example"}}
	if _, err := fx.service.ListRunners(fleetCtx, fleetValidScope(), control.RunnerQuery{}); !errors.Is(err, control.ErrDenied) {
		t.Fatalf("got %v, want ErrDenied", err)
	}
	if fx.pools.calls != 0 {
		t.Fatal("EligiblePools was called despite refusal")
	}
	if fx.st.listRunnersCalls != 0 {
		t.Fatal("fleet ListRunners was called despite refusal")
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

	page, err := fx.service.ListRunners(fleetCtx, fleetValidScope(), control.RunnerQuery{Limit: 2})
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

	page2, err := fx.service.ListRunners(fleetCtx, fleetValidScope(), control.RunnerQuery{Limit: 2, Cursor: page.NextCursor})
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

	if _, err := fx.service.ListRunners(fleetCtx, fleetValidScope(), control.RunnerQuery{Limit: -1}); !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("negative limit: got %v, want ErrInvalid", err)
	}
	if _, err := fx.service.ListRunners(fleetCtx, fleetValidScope(), control.RunnerQuery{Cursor: "not base64!!"}); !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("malformed cursor: got %v, want ErrInvalid", err)
	}
	if _, err := fx.service.ListRunners(fleetCtx, fleetValidScope(), control.RunnerQuery{Cursor: "c2VjcmV0"}); !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("non-JSON cursor: got %v, want ErrInvalid", err)
	}

	// An empty result is [], never nil.
	page, err := fx.service.ListRunners(fleetCtx, fleetValidScope(), control.RunnerQuery{})
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

	page, err := fx.service.ListRunners(fleetCtx, fleetValidScope(), control.RunnerQuery{})
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

func fleetReconcileRunnerRow() control.Runner {
	return control.Runner{ID: "runner_example", PoolID: "pool_example", Generation: 1, CapacityTotal: 4, Connected: true}
}

func fleetGetSessionState(t *testing.T, fx *fleetFixture, ws control.WorkspaceID, id control.SessionID) control.Session {
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
			fx.st.seedRunner(fleetReconcileRunnerRow())
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

			got, err := fx.service.ReconcileRunner(fleetCtx, snap)
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
			row := fleetGetSessionState(t, fx, "ws_example", "sess_example")
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

	got, err := fx.service.ReconcileRunner(fleetCtx, control.RunnerSnapshot{
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

	got, err := fx.service.ReconcileRunner(fleetCtx, control.RunnerSnapshot{
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
	row := fleetGetSessionState(t, fx, "ws_example", "sess_example")
	if row.State != control.StateSuspendedWarm {
		t.Fatalf("session = %q, want suspended_warm after adoption", row.State)
	}
}

func TestReconcileRunnerSameGenerationRefreshesAuthority(t *testing.T) {
	fx := newFleetFixture(t)
	fx.st.seedRunner(control.Runner{
		ID: "runner_example", PoolID: "pool_example", Generation: 5,
		CapacityUsed: 1, CapacityTotal: 4, Connected: false,
		Capabilities: []string{"gpu"}, LastSeenAt: time.Unix(1, 0),
	})
	// Advance the clock so a refreshed LastSeenAt is observable.
	fx.clock.now = time.Unix(1_800_000_000, 0)

	got, err := fx.service.ReconcileRunner(fleetCtx, control.RunnerSnapshot{
		WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
		Generation: 5, CapacityUsed: 2, CapacityTotal: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Fenced || got.Generation != 5 {
		t.Fatalf("result = %+v, want unfenced at generation 5", got)
	}
	stored := fx.st.runners["pool_example"]["runner_example"]
	if stored.Generation != 5 {
		t.Fatalf("generation = %d, want 5", stored.Generation)
	}
	if stored.CapacityUsed != 2 || stored.CapacityTotal != 8 {
		t.Fatalf("capacity = %d/%d, want 2/8", stored.CapacityUsed, stored.CapacityTotal)
	}
	if !stored.Connected {
		t.Fatal("connected was not refreshed to true")
	}
	if !stored.LastSeenAt.Equal(time.Unix(1_800_000_000, 0)) {
		t.Fatalf("LastSeenAt = %v, want refreshed to the clock", stored.LastSeenAt)
	}
	if !slices.Equal(stored.Capabilities, []string{"gpu"}) {
		t.Fatalf("capabilities = %v, want [gpu] preserved", stored.Capabilities)
	}
}

func TestReconcileRunnerOrphans(t *testing.T) {
	t.Run("unknown announced session is destroyed", func(t *testing.T) {
		fx := newFleetFixture(t)
		fx.st.seedRunner(fleetReconcileRunnerRow())
		got, err := fx.service.ReconcileRunner(fleetCtx, control.RunnerSnapshot{
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
		fx.st.seedRunner(fleetReconcileRunnerRow())
		_, err := fx.service.ReconcileRunner(fleetCtx, control.RunnerSnapshot{
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
		fx.st.seedRunner(fleetReconcileRunnerRow())
		fx.st.seedSession(control.Session{
			ID: "sess_other", WorkspaceID: "ws_other", State: control.StateRunning,
			PoolID: "pool_example", RunnerID: "runner_example",
		})
		got, err := fx.service.ReconcileRunner(fleetCtx, control.RunnerSnapshot{
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
		other := fleetGetSessionState(t, fx, "ws_other", "sess_other")
		if other.State != control.StateRunning {
			t.Fatalf("other workspace's session mutated to %q", other.State)
		}
	})

	t.Run("mismatched pool or runner is destroyed without mutation", func(t *testing.T) {
		fx := newFleetFixture(t)
		fx.st.seedRunner(fleetReconcileRunnerRow())
		fx.st.seedSession(control.Session{
			ID: "sess_dup", WorkspaceID: "ws_example", State: control.StateRunning,
			PoolID: "pool_other", RunnerID: "runner_other",
		})
		got, err := fx.service.ReconcileRunner(fleetCtx, control.RunnerSnapshot{
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
		dup := fleetGetSessionState(t, fx, "ws_example", "sess_dup")
		if dup.State != control.StateRunning || dup.RunnerID != "runner_other" {
			t.Fatalf("mismatched session mutated to %q on %q", dup.State, dup.RunnerID)
		}
	})

	t.Run("destroy output is deterministic and sorted", func(t *testing.T) {
		fx := newFleetFixture(t)
		fx.st.seedRunner(fleetReconcileRunnerRow())
		got, err := fx.service.ReconcileRunner(fleetCtx, control.RunnerSnapshot{
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
		again, err := fx.service.ReconcileRunner(fleetCtx, control.RunnerSnapshot{
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

// ---------------------------------------------------------------------------
// Task 3: stale-safe runner events
// ---------------------------------------------------------------------------

func fleetEventRunner(gen uint64) control.Runner {
	return control.Runner{ID: "runner_example", PoolID: "pool_example", Generation: gen, CapacityTotal: 4, Connected: true}
}

func TestApplyRunnerEventLifecycle(t *testing.T) {
	code := 7
	tests := []struct {
		name           string
		from           control.SessionState
		event          control.RunnerEvent
		wantState      control.SessionState
		wantError      string
		wantTransition bool
	}{
		{
			name:           "running",
			from:           control.StateCreating,
			event:          control.RunnerEvent{State: control.StateRunning},
			wantState:      control.StateRunning,
			wantTransition: true,
		},
		{
			name:           "suspended warm",
			from:           control.StateRunning,
			event:          control.RunnerEvent{State: control.StateSuspendedWarm},
			wantState:      control.StateSuspendedWarm,
			wantTransition: true,
		},
		{
			name:           "suspended cold",
			from:           control.StateRunning,
			event:          control.RunnerEvent{State: control.StateSuspendedCold},
			wantState:      control.StateSuspendedCold,
			wantTransition: true,
		},
		{
			name:           "failed carries the runner detail",
			from:           control.StateCreating,
			event:          control.RunnerEvent{State: control.StateFailed, Detail: "rc 1: boom"},
			wantState:      control.StateFailed,
			wantError:      "rc 1: boom",
			wantTransition: true,
		},
		{
			name:           "dead records a safe reason",
			from:           control.StateRunning,
			event:          control.RunnerEvent{State: control.StateDead},
			wantState:      control.StateDead,
			wantError:      "runner reported dead",
			wantTransition: true,
		},
		{
			name:           "child exit does not transition",
			from:           control.StateRunning,
			event:          control.RunnerEvent{State: control.StateRunning, ChildExitCode: &code},
			wantState:      control.StateRunning,
			wantTransition: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFleetFixture(t)
			fx.st.seedRunner(fleetEventRunner(1))
			fx.st.seedSession(control.Session{
				ID: "sess_example", WorkspaceID: "ws_example", State: tc.from,
				PoolID: "pool_example", RunnerID: "runner_example",
			})

			ev := tc.event
			ev.WorkspaceID = "ws_example"
			ev.PoolID = "pool_example"
			ev.RunnerID = "runner_example"
			ev.Generation = 1
			ev.SessionID = "sess_example"

			if err := fx.service.ApplyRunnerEvent(fleetCtx, ev); err != nil {
				t.Fatal(err)
			}
			row := fleetGetSessionState(t, fx, "ws_example", "sess_example")
			if row.State != tc.wantState {
				t.Fatalf("state = %q, want %q", row.State, tc.wantState)
			}
			if row.Error != tc.wantError {
				t.Fatalf("error = %q, want %q", row.Error, tc.wantError)
			}
			if tc.wantTransition {
				if fx.st.transitionCalls == 0 {
					t.Fatal("expected a transition")
				}
			} else if fx.st.transitionCalls != 0 {
				t.Fatal("child exit must not transition")
			}
			if tc.event.ChildExitCode != nil {
				if fx.st.childExitCalls != 1 {
					t.Fatalf("childExitCalls = %d, want 1", fx.st.childExitCalls)
				}
				if row.ChildExitCode == nil || *row.ChildExitCode != 7 {
					t.Fatalf("child exit code = %v, want 7", row.ChildExitCode)
				}
			}
			if len(fx.events.recorded()) != 1 {
				t.Fatalf("recorded %d events, want 1", len(fx.events.recorded()))
			}
		})
	}
}

func TestApplyRunnerEventRejectsStale(t *testing.T) {
	// The exact stale-generation case from the plan: the event names a runner
	// whose authoritative generation has moved on.
	t.Run("stale generation has no effects", func(t *testing.T) {
		fx := newFleetFixture(t)
		fx.st.seedRunner(control.Runner{ID: "runner_old", PoolID: "pool_example", Generation: 7, CapacityTotal: 4, Connected: true})
		fx.st.seedSession(control.Session{
			ID: "sess_example", WorkspaceID: "ws_example", State: control.StateCreating,
			PoolID: "pool_example", RunnerID: "runner_old",
		})
		event := control.RunnerEvent{
			WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_old",
			Generation: 6, SessionID: "sess_example", State: control.StateRunning,
		}
		if err := fx.service.ApplyRunnerEvent(fleetCtx, event); !errors.Is(err, control.ErrStale) {
			t.Fatalf("got %v, want ErrStale", err)
		}
		if fx.st.transitionCalls != 0 || len(fx.events.recorded()) != 0 {
			t.Fatal("stale event had effects")
		}
	})

	cases := map[string]func(*fleetFixture){
		"wrong workspace": func(fx *fleetFixture) {
			fx.st.seedSession(control.Session{
				ID: "sess_example", WorkspaceID: "ws_other", State: control.StateCreating,
				PoolID: "pool_example", RunnerID: "runner_example",
			})
		},
		"wrong pool": func(fx *fleetFixture) {
			fx.st.seedSession(control.Session{
				ID: "sess_example", WorkspaceID: "ws_example", State: control.StateCreating,
				PoolID: "pool_other", RunnerID: "runner_example",
			})
		},
		"wrong runner": func(fx *fleetFixture) {
			fx.st.seedSession(control.Session{
				ID: "sess_example", WorkspaceID: "ws_example", State: control.StateCreating,
				PoolID: "pool_example", RunnerID: "runner_other",
			})
		},
		"unknown session": func(fx *fleetFixture) {},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			fx := newFleetFixture(t)
			fx.st.seedRunner(fleetEventRunner(1))
			mutate(fx)
			event := control.RunnerEvent{
				WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
				Generation: 1, SessionID: "sess_example", State: control.StateRunning,
			}
			if err := fx.service.ApplyRunnerEvent(fleetCtx, event); !errors.Is(err, control.ErrStale) {
				t.Fatalf("got %v, want ErrStale", err)
			}
			if fx.st.transitionCalls != 0 || len(fx.events.recorded()) != 0 {
				t.Fatal("mismatched event had effects")
			}
		})
	}
}

func TestApplyRunnerEventDuplicateTerminalIsSuccess(t *testing.T) {
	fx := newFleetFixture(t)
	fx.st.seedRunner(fleetEventRunner(1))
	fx.st.seedSession(control.Session{
		ID: "sess_example", WorkspaceID: "ws_example", State: control.StateRunning,
		PoolID: "pool_example", RunnerID: "runner_example",
	})
	event := control.RunnerEvent{
		WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
		Generation: 1, SessionID: "sess_example", State: control.StateDead,
	}
	if err := fx.service.ApplyRunnerEvent(fleetCtx, event); err != nil {
		t.Fatal(err)
	}
	if err := fx.service.ApplyRunnerEvent(fleetCtx, event); err != nil {
		t.Fatalf("duplicate terminal event: got %v, want nil (idempotent success)", err)
	}
	if fx.st.transitionCalls != 1 {
		t.Fatalf("transitionCalls = %d, want 1", fx.st.transitionCalls)
	}
	if len(fx.events.recorded()) != 1 {
		t.Fatalf("recorded %d events, want 1", len(fx.events.recorded()))
	}
}

func TestApplyRunnerEventInvalidInput(t *testing.T) {
	fx := newFleetFixture(t)
	fx.st.seedRunner(fleetEventRunner(1))
	fx.st.seedSession(control.Session{
		ID: "sess_example", WorkspaceID: "ws_example", State: control.StateCreating,
		PoolID: "pool_example", RunnerID: "runner_example",
	})
	base := control.RunnerEvent{
		WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
		Generation: 1, SessionID: "sess_example", State: control.StateRunning,
	}
	cases := map[string]func(*control.RunnerEvent){
		"invalid state":   func(e *control.RunnerEvent) { e.State = control.StateQueued },
		"overlong detail": func(e *control.RunnerEvent) { e.Detail = string(make([]byte, 2049)) },
		"empty session":   func(e *control.RunnerEvent) { e.SessionID = "" },
		"zero generation": func(e *control.RunnerEvent) { e.Generation = 0 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			fx := newFleetFixture(t)
			fx.st.seedRunner(fleetEventRunner(1))
			fx.st.seedSession(control.Session{
				ID: "sess_example", WorkspaceID: "ws_example", State: control.StateCreating,
				PoolID: "pool_example", RunnerID: "runner_example",
			})
			e := base
			mutate(&e)
			if err := fx.service.ApplyRunnerEvent(fleetCtx, e); !errors.Is(err, control.ErrInvalid) {
				t.Fatalf("got %v, want ErrInvalid", err)
			}
			if fx.st.transitionCalls != 0 || len(fx.events.recorded()) != 0 {
				t.Fatal("invalid event had effects")
			}
		})
	}
}

func TestApplyRunnerEventDetailNeverEntersEvent(t *testing.T) {
	fx := newFleetFixture(t)
	fx.st.seedRunner(fleetEventRunner(1))
	fx.st.seedSession(control.Session{
		ID: "sess_example", WorkspaceID: "ws_example", State: control.StateCreating,
		PoolID: "pool_example", RunnerID: "runner_example",
	})
	const detail = "SENSITIVE_SECRET_TAIL_DO_NOT_LOG"
	event := control.RunnerEvent{
		WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
		Generation: 1, SessionID: "sess_example", State: control.StateFailed, Detail: detail,
	}
	if err := fx.service.ApplyRunnerEvent(fleetCtx, event); err != nil {
		t.Fatal(err)
	}
	row := fleetGetSessionState(t, fx, "ws_example", "sess_example")
	if row.Error != detail {
		t.Fatalf("session error = %q, want the bounded detail in the row", row.Error)
	}
	recorded := fx.events.recorded()
	if len(recorded) != 1 {
		t.Fatalf("recorded %d events, want 1", len(recorded))
	}
	if recorded[0].Resource.ID != "sess_example" {
		t.Fatalf("event resource = %+v, want session sess_example", recorded[0].Resource)
	}
	if got := fmt.Sprintf("%+v", recorded[0]); strings.Contains(got, detail) {
		t.Fatal("runner detail leaked into the recorded control.Event")
	}
}

func TestApplyRunnerEventChildExitIdempotentAndConflict(t *testing.T) {
	t.Run("duplicate identical child exit is idempotent", func(t *testing.T) {
		fx := newFleetFixture(t)
		fx.st.seedRunner(fleetEventRunner(1))
		code := 7
		fx.st.seedSession(control.Session{
			ID: "sess_example", WorkspaceID: "ws_example", State: control.StateRunning,
			PoolID: "pool_example", RunnerID: "runner_example", ChildExitCode: &code,
		})
		event := control.RunnerEvent{
			WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
			Generation: 1, SessionID: "sess_example", State: control.StateRunning,
			ChildExitCode: &code,
		}
		if err := fx.service.ApplyRunnerEvent(fleetCtx, event); err != nil {
			t.Fatalf("duplicate child exit: got %v, want nil (idempotent)", err)
		}
		if fx.st.childExitCalls != 0 {
			t.Fatalf("childExitCalls = %d, want 0 (idempotent)", fx.st.childExitCalls)
		}
		if len(fx.events.recorded()) != 0 {
			t.Fatalf("recorded %d events, want 0 (idempotent)", len(fx.events.recorded()))
		}
	})

	t.Run("different exit code conflicts", func(t *testing.T) {
		fx := newFleetFixture(t)
		fx.st.seedRunner(fleetEventRunner(1))
		stored := 7
		fx.st.seedSession(control.Session{
			ID: "sess_example", WorkspaceID: "ws_example", State: control.StateRunning,
			PoolID: "pool_example", RunnerID: "runner_example", ChildExitCode: &stored,
		})
		other := 9
		event := control.RunnerEvent{
			WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
			Generation: 1, SessionID: "sess_example", State: control.StateRunning,
			ChildExitCode: &other,
		}
		if err := fx.service.ApplyRunnerEvent(fleetCtx, event); !errors.Is(err, control.ErrConflict) {
			t.Fatalf("different exit code: got %v, want ErrConflict", err)
		}
		if fx.st.childExitCalls != 0 {
			t.Fatalf("childExitCalls = %d, want 0 (conflict)", fx.st.childExitCalls)
		}
		if len(fx.events.recorded()) != 0 {
			t.Fatalf("recorded %d events, want 0 (conflict)", len(fx.events.recorded()))
		}
		row := fleetGetSessionState(t, fx, "ws_example", "sess_example")
		if row.ChildExitCode == nil || *row.ChildExitCode != 7 {
			t.Fatalf("child exit code = %v, want unchanged 7", row.ChildExitCode)
		}
	})
}

// TestApplyRunnerEventEmptyEventIDRejectedBeforeMutation pins that an ID
// generator that cannot answer fails the call before any mutation, and that it
// fails as ErrUnavailable: the runner's event is well-formed, so a runner
// adapter must be free to retry rather than be told its report was malformed.
func TestApplyRunnerEventEmptyEventIDRejectedBeforeMutation(t *testing.T) {
	fx := newFleetFixture(t)
	fx.st.seedRunner(fleetEventRunner(1))
	fx.st.seedSession(control.Session{
		ID: "sess_example", WorkspaceID: "ws_example", State: control.StateCreating,
		PoolID: "pool_example", RunnerID: "runner_example",
	})
	fx.ids.emptyEventID = true
	event := control.RunnerEvent{
		WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
		Generation: 1, SessionID: "sess_example", State: control.StateRunning,
	}
	if err := fx.service.ApplyRunnerEvent(fleetCtx, event); !errors.Is(err, control.ErrUnavailable) {
		t.Fatalf("got %v, want ErrUnavailable", err)
	}
	if fx.st.transitionCalls != 0 {
		t.Fatalf("transitionCalls = %d, want 0 (no mutation)", fx.st.transitionCalls)
	}
	if len(fx.events.recorded()) != 0 {
		t.Fatalf("recorded %d events, want 0", len(fx.events.recorded()))
	}
}

func TestApplyRunnerEventRecordFailureIsUnavailable(t *testing.T) {
	fx := newFleetFixture(t)
	fx.st.seedRunner(fleetEventRunner(1))
	fx.st.seedSession(control.Session{
		ID: "sess_example", WorkspaceID: "ws_example", State: control.StateCreating,
		PoolID: "pool_example", RunnerID: "runner_example",
	})
	fx.events.recordErr = errors.New("outbox unavailable")
	event := control.RunnerEvent{
		WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
		Generation: 1, SessionID: "sess_example", State: control.StateRunning,
	}
	if err := fx.service.ApplyRunnerEvent(fleetCtx, event); !errors.Is(err, control.ErrUnavailable) {
		t.Fatalf("got %v, want ErrUnavailable", err)
	}
	// The accepted mutation still landed; only the event record failed.
	row := fleetGetSessionState(t, fx, "ws_example", "sess_example")
	if row.State != control.StateRunning {
		t.Fatalf("state = %q, want running (mutation persisted)", row.State)
	}
	if len(fx.events.recorded()) != 0 {
		t.Fatalf("recorded %d events, want 0", len(fx.events.recorded()))
	}
}

// ---------------------------------------------------------------------------
// closed-sentinel error model
// ---------------------------------------------------------------------------

// fleetAdapterErr stands in for the error a real persistence or transport
// adapter produces. Its text is exactly what must never leave the service: a
// driver prefix, a table name, and a host and user from a connection string.
var fleetAdapterErr = errors.New(`pq: relation "runners" does not exist (host=db.internal.invalid user=rainier)`)

// fleetControlSentinels is the closed error set control/errors.go defines.
var fleetControlSentinels = []error{
	control.ErrInvalid, control.ErrDenied, control.ErrNotFound, control.ErrConflict,
	control.ErrStale, control.ErrUnavailable, control.ErrUnsupported,
}

func fleetIsControlSentinel(err error) bool {
	for _, s := range fleetControlSentinels {
		if errors.Is(err, s) {
			return true
		}
	}
	return false
}

// TestFleetPortErrorsNeverLeaveTheService pins the frozen contract's error
// model on every control.Fleet method: an adapter failure is reported through
// one of the seven closed sentinels, and the adapter's own text — which may
// name a table, a host, or a credential — never reaches the caller.
func TestFleetPortErrorsNeverLeaveTheService(t *testing.T) {
	cases := []struct {
		name   string
		inject func(fx *fleetFixture)
		call   func(fx *fleetFixture) error
	}{
		{
			name:   "RegisterRunner/ListRunners",
			inject: func(fx *fleetFixture) { fx.st.listRunnersErr = fleetAdapterErr },
			call: func(fx *fleetFixture) error {
				_, err := fx.service.RegisterRunner(fleetCtx, control.RunnerRegistration{
					WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
					Generation: 1, CapacityTotal: 4,
				})
				return err
			},
		},
		{
			name:   "RegisterRunner/UpsertRunner",
			inject: func(fx *fleetFixture) { fx.st.upsertErr = fleetAdapterErr },
			call: func(fx *fleetFixture) error {
				_, err := fx.service.RegisterRunner(fleetCtx, control.RunnerRegistration{
					WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
					Generation: 1, CapacityTotal: 4,
				})
				return err
			},
		},
		{
			name:   "ReconcileRunner/ListRunners",
			inject: func(fx *fleetFixture) { fx.st.listRunnersErr = fleetAdapterErr },
			call: func(fx *fleetFixture) error {
				_, err := fx.service.ReconcileRunner(fleetCtx, fleetSnapshotFor(1))
				return err
			},
		},
		{
			name:   "ReconcileRunner/UpsertRunner",
			inject: func(fx *fleetFixture) { fx.st.upsertErr = fleetAdapterErr },
			call: func(fx *fleetFixture) error {
				_, err := fx.service.ReconcileRunner(fleetCtx, fleetSnapshotFor(1))
				return err
			},
		},
		{
			name: "ReconcileRunner/Transition",
			inject: func(fx *fleetFixture) {
				fx.st.seedRunner(fleetEventRunner(1))
				fx.st.seedSession(control.Session{
					ID: "sess_example", WorkspaceID: "ws_example", State: control.StateRunning,
					PoolID: "pool_example", RunnerID: "runner_example",
				})
				fx.st.transitionErr = fleetAdapterErr
			},
			call: func(fx *fleetFixture) error {
				// The snapshot reports no sessions, so the stored running row
				// is lost at announce and must be transitioned to dead.
				_, err := fx.service.ReconcileRunner(fleetCtx, fleetSnapshotFor(1))
				return err
			},
		},
		{
			name:   "ListRunners/EligiblePools",
			inject: func(fx *fleetFixture) { fx.pools.deny = fleetAdapterErr },
			call: func(fx *fleetFixture) error {
				_, err := fx.service.ListRunners(fleetCtx, fleetValidScope(), control.RunnerQuery{})
				return err
			},
		},
		{
			name: "ListRunners/fleet.ListRunners",
			inject: func(fx *fleetFixture) {
				fx.pools.pools = []control.Pool{{ID: "pool_example", CapacityTotal: 4}}
				fx.st.listRunnersErr = fleetAdapterErr
			},
			call: func(fx *fleetFixture) error {
				_, err := fx.service.ListRunners(fleetCtx, fleetValidScope(), control.RunnerQuery{})
				return err
			},
		},
		{
			name:   "ApplyRunnerEvent/GetSession",
			inject: func(fx *fleetFixture) { fx.st.getSessionErr = fleetAdapterErr },
			call: func(fx *fleetFixture) error {
				return fx.service.ApplyRunnerEvent(fleetCtx, fleetEventFor(1, control.StateRunning))
			},
		},
		{
			name: "ApplyRunnerEvent/ListRunners",
			inject: func(fx *fleetFixture) {
				fx.st.seedSession(control.Session{
					ID: "sess_example", WorkspaceID: "ws_example", State: control.StateCreating,
					PoolID: "pool_example", RunnerID: "runner_example",
				})
				fx.st.listRunnersErr = fleetAdapterErr
			},
			call: func(fx *fleetFixture) error {
				return fx.service.ApplyRunnerEvent(fleetCtx, fleetEventFor(1, control.StateRunning))
			},
		},
		{
			name: "ApplyRunnerEvent/Transition",
			inject: func(fx *fleetFixture) {
				fx.st.seedRunner(fleetEventRunner(1))
				fx.st.seedSession(control.Session{
					ID: "sess_example", WorkspaceID: "ws_example", State: control.StateCreating,
					PoolID: "pool_example", RunnerID: "runner_example",
				})
				fx.st.transitionErr = fleetAdapterErr
			},
			call: func(fx *fleetFixture) error {
				return fx.service.ApplyRunnerEvent(fleetCtx, fleetEventFor(1, control.StateRunning))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFleetFixture(t)
			tc.inject(fx)
			err := tc.call(fx)
			if err == nil {
				t.Fatal("injected adapter failure produced no error")
			}
			if !fleetIsControlSentinel(err) {
				t.Fatalf("error is not a closed control sentinel: %v", err)
			}
			if strings.Contains(err.Error(), "db.internal.invalid") ||
				strings.Contains(err.Error(), "relation") {
				t.Fatalf("adapter text escaped the service: %v", err)
			}
		})
	}
}

// fleetSnapshotFor builds a well-formed snapshot at generation gen reporting
// no sessions.
func fleetSnapshotFor(gen uint64) control.RunnerSnapshot {
	return control.RunnerSnapshot{
		WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
		Generation: gen, CapacityUsed: 0, CapacityTotal: 4,
	}
}

// fleetEventFor builds a well-formed lifecycle event at generation gen.
func fleetEventFor(gen uint64, state control.SessionState) control.RunnerEvent {
	return control.RunnerEvent{
		WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
		Generation: gen, SessionID: "sess_example", State: state,
	}
}

// TestReconcileRunnerFencedByConcurrentWriteOnEveryPath pins that all three
// accepted reconcile paths — a newer generation, the same generation, and an
// unknown runner — answer a lost race to a concurrent higher-generation write
// the same way: the store-authoritative generation with Fenced set, so the
// runner has something to resync to, and no error the caller cannot act on.
func TestReconcileRunnerFencedByConcurrentWriteOnEveryPath(t *testing.T) {
	cases := []struct {
		name        string
		storedGen   uint64
		snapshotGen uint64
	}{
		{name: "newer generation", storedGen: 1, snapshotGen: 2},
		{name: "same generation", storedGen: 1, snapshotGen: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFleetFixture(t)
			fx.st.seedRunner(control.Runner{
				ID: "runner_example", PoolID: "pool_example",
				Generation: tc.storedGen, CapacityTotal: 4,
			})
			fx.st.upsertErr = control.ErrStale
			fx.st.staleBump = 9

			got, err := fx.service.ReconcileRunner(fleetCtx, fleetSnapshotFor(tc.snapshotGen))
			if err != nil {
				t.Fatalf("a lost race is an answer, not an error: %v", err)
			}
			if !got.Fenced {
				t.Fatalf("result = %+v, want Fenced", got)
			}
			if got.Generation != 9 {
				t.Fatalf("Generation = %d, want the store-authoritative 9", got.Generation)
			}
			if len(got.Destroy) != 0 {
				t.Fatalf("Destroy = %v, want none on a fenced snapshot", got.Destroy)
			}
		})
	}
}

// TestReconcileAgreeingAnnounceTouchesTheRow pins that a snapshot which agrees
// with the store still makes the same-state transition: it is the one write
// that records the session as demonstrably alive at this announce.
func TestReconcileAgreeingAnnounceTouchesTheRow(t *testing.T) {
	fx := newFleetFixture(t)
	fx.st.seedRunner(fleetEventRunner(1))
	fx.st.seedSession(control.Session{
		ID: "sess_example", WorkspaceID: "ws_example", State: control.StateRunning,
		PoolID: "pool_example", RunnerID: "runner_example",
	})
	snap := control.RunnerSnapshot{
		WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
		Generation: 1, CapacityTotal: 4,
		Sessions: []control.RunnerSession{{SessionID: "sess_example", State: control.StateRunning}},
	}
	res, err := fx.service.ReconcileRunner(fleetCtx, snap)
	if err != nil || len(res.Destroy) != 0 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if fx.st.transitionCalls != 1 {
		t.Fatalf("transitionCalls = %d, want 1 (the same-state touch)", fx.st.transitionCalls)
	}
	if row := fleetGetSessionState(t, fx, "ws_example", "sess_example"); row.State != control.StateRunning || row.RunnerID != "runner_example" {
		t.Fatalf("row = %+v, want unchanged", row)
	}
}

// TestReconcileAdoptsAnUnplacedLiveRow pins that a live row the store has
// placed nowhere is adopted onto the runner announcing it, in the announced
// state, rather than destroyed.
func TestReconcileAdoptsAnUnplacedLiveRow(t *testing.T) {
	fx := newFleetFixture(t)
	fx.st.seedRunner(fleetEventRunner(1))
	fx.st.seedSession(control.Session{
		ID: "sess_unplaced", WorkspaceID: "ws_example", State: control.StateQueued,
		PoolID: "pool_example", RunnerID: "",
	})
	snap := control.RunnerSnapshot{
		WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
		Generation: 1, CapacityTotal: 4,
		Sessions: []control.RunnerSession{{SessionID: "sess_unplaced", State: control.StateSuspendedWarm}},
	}
	res, err := fx.service.ReconcileRunner(fleetCtx, snap)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Destroy) != 0 {
		t.Fatalf("an unplaced live row was destroyed: %v", res.Destroy)
	}
	row := fleetGetSessionState(t, fx, "ws_example", "sess_unplaced")
	if row.State != control.StateSuspendedWarm || row.RunnerID != "runner_example" {
		t.Fatalf("row = %+v, want adopted as suspended_warm on runner_example", row)
	}

	// A row placed on a different runner is still a duplicate to destroy.
	fx.st.seedSession(control.Session{
		ID: "sess_elsewhere", WorkspaceID: "ws_example", State: control.StateRunning,
		PoolID: "pool_example", RunnerID: "runner_other",
	})
	snap.Sessions = []control.RunnerSession{{SessionID: "sess_elsewhere", State: control.StateRunning}}
	res, err = fx.service.ReconcileRunner(fleetCtx, snap)
	if err != nil || len(res.Destroy) != 1 || res.Destroy[0] != "sess_elsewhere" {
		t.Fatalf("duplicate: res=%+v err=%v", res, err)
	}
}

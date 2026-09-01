package controlapp

import (
	"context"
	"errors"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
)

var fixedNow = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// ---------------------------------------------------------------------------
// call log — proves authorization and durability order across fakes.
// ---------------------------------------------------------------------------

type callLog struct {
	steps []string
}

func (c *callLog) add(step string) {
	if c == nil {
		return
	}
	c.steps = append(c.steps, step)
}

func (c *callLog) snapshot() []string {
	if c == nil {
		return nil
	}
	return slices.Clone(c.steps)
}

func (c *callLog) has(step string) bool {
	for _, s := range c.snapshot() {
		if s == step {
			return true
		}
	}
	return false
}

func (c *callLog) hasPrefix(prefix string) bool {
	for _, s := range c.snapshot() {
		if strings.HasPrefix(s, prefix) {
			return true
		}
	}
	return false
}

// ordered reports that every want step appears in the given order (not
// necessarily adjacent to one another).
func ordered(t *testing.T, steps []string, want ...string) {
	t.Helper()
	pos := -1
	for _, w := range want {
		found := -1
		for i := pos + 1; i < len(steps); i++ {
			if steps[i] == w {
				found = i
				break
			}
		}
		if found == -1 {
			t.Fatalf("step %q not found in order in %v", w, steps)
		}
		pos = found
	}
}

func testScope() control.Scope {
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

// ---------------------------------------------------------------------------
// fakes
// ---------------------------------------------------------------------------

type stubClock struct {
	now time.Time
}

func (c stubClock) Now() time.Time { return c.now }

type stubIDs struct {
	log       *callLog
	sessionID control.SessionID
	envID     control.EnvironmentID
	eventID   control.EventID
}

func (g *stubIDs) NewSessionID() control.SessionID {
	g.log.add("id:session")
	return g.sessionID
}

func (g *stubIDs) NewEnvironmentID() control.EnvironmentID {
	g.log.add("id:environment")
	return g.envID
}

func (g *stubIDs) NewEventID() control.EventID {
	g.log.add("id:event")
	return g.eventID
}

type stubAuthorizer struct {
	log *callLog
	err error
}

func (a *stubAuthorizer) Authorize(ctx context.Context, sc control.Scope, act control.Action, r control.Resource) error {
	a.log.add("auth:" + string(act) + ":" + string(r.Kind))
	return a.err
}

type stubSessionRepo struct {
	log *callLog

	rows map[control.SessionID]control.Session
	idem map[string]control.Session

	createErr     error
	idemErr       error
	getErr        error
	listErr       error
	transitionErr error

	lastListWS    control.WorkspaceID
	lastListQuery control.SessionQuery
}

func newStubSessionRepo(log *callLog) *stubSessionRepo {
	return &stubSessionRepo{
		log:  log,
		rows: map[control.SessionID]control.Session{},
		idem: map[string]control.Session{},
	}
}

func (r *stubSessionRepo) put(s control.Session) {
	r.rows[s.ID] = s
	if s.IdempotencyKey != "" {
		r.idem[idemKey(s.WorkspaceID, s.CreatorID, s.IdempotencyKey)] = s
	}
}

func idemKey(ws control.WorkspaceID, creator control.ActorID, key string) string {
	return string(ws) + "\x00" + string(creator) + "\x00" + key
}

func (r *stubSessionRepo) CreateSession(ctx context.Context, ws control.WorkspaceID, s control.Session) (control.Session, error) {
	r.log.add("sessions:create")
	if r.createErr != nil {
		return control.Session{}, r.createErr
	}
	r.put(s)
	return s, nil
}

func (r *stubSessionRepo) SessionByIDem(ctx context.Context, ws control.WorkspaceID, creator control.ActorID, key string) (control.Session, error) {
	r.log.add("sessions:byidem")
	if r.idemErr != nil {
		return control.Session{}, r.idemErr
	}
	s, ok := r.idem[idemKey(ws, creator, key)]
	if !ok {
		return control.Session{}, control.ErrNotFound
	}
	return s, nil
}

func (r *stubSessionRepo) GetSession(ctx context.Context, ws control.WorkspaceID, id control.SessionID) (control.Session, error) {
	r.log.add("sessions:get")
	if r.getErr != nil {
		return control.Session{}, r.getErr
	}
	s, ok := r.rows[id]
	if !ok || s.WorkspaceID != ws {
		return control.Session{}, control.ErrNotFound
	}
	return s, nil
}

func (r *stubSessionRepo) ListSessions(ctx context.Context, ws control.WorkspaceID, q control.SessionQuery) ([]control.Session, string, error) {
	r.log.add("sessions:list")
	r.lastListWS = ws
	r.lastListQuery = q
	if r.listErr != nil {
		return nil, "", r.listErr
	}
	rows := make([]control.Session, 0, len(r.rows))
	for _, s := range r.rows {
		if s.WorkspaceID != ws {
			continue
		}
		rows = append(rows, s)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].CreatedAt.Equal(rows[j].CreatedAt) {
			return rows[i].ID < rows[j].ID
		}
		return rows[i].CreatedAt.Before(rows[j].CreatedAt)
	})
	return rows, "", nil
}

func (r *stubSessionRepo) Transition(ctx context.Context, ws control.WorkspaceID, id control.SessionID, from []control.SessionState, to control.SessionState, opts control.TransitionOpts) error {
	r.log.add("sessions:transition:" + string(to))
	if r.transitionErr != nil {
		return r.transitionErr
	}
	s, ok := r.rows[id]
	if !ok || s.WorkspaceID != ws {
		return control.ErrNotFound
	}
	if !slices.Contains(from, s.State) {
		return control.ErrConflict
	}
	s.State = to
	if opts.RunnerID != nil {
		s.RunnerID = *opts.RunnerID
	}
	if opts.Error != nil {
		s.Error = *opts.Error
	}
	r.rows[id] = s
	return nil
}

func (r *stubSessionRepo) SetSessionSetupHash(ctx context.Context, ws control.WorkspaceID, id control.SessionID, hash string) error {
	r.log.add("sessions:set-setup-hash")
	return nil
}

func (r *stubSessionRepo) SetChildExitCode(ctx context.Context, ws control.WorkspaceID, id control.SessionID, code int) error {
	r.log.add("sessions:set-child-exit-code")
	return nil
}

type stubEnvironmentRepo struct {
	log *callLog

	rows map[control.EnvironmentID]control.Environment

	createErr error
	getErr    error
	listErr   error
	updateErr error
	deleteErr error
	countErr  error

	liveSessionCount int
	lastCountStates  []control.SessionState
}

func newStubEnvironmentRepo(log *callLog) *stubEnvironmentRepo {
	return &stubEnvironmentRepo{
		log:  log,
		rows: map[control.EnvironmentID]control.Environment{},
	}
}

func (r *stubEnvironmentRepo) put(e control.Environment) {
	r.rows[e.ID] = e
}

func (r *stubEnvironmentRepo) CreateEnvironment(ctx context.Context, ws control.WorkspaceID, e control.Environment) (control.Environment, error) {
	r.log.add("environments:create")
	if r.createErr != nil {
		return control.Environment{}, r.createErr
	}
	r.put(e)
	return e, nil
}

func (r *stubEnvironmentRepo) GetEnvironment(ctx context.Context, ws control.WorkspaceID, id control.EnvironmentID) (control.Environment, error) {
	r.log.add("environments:get")
	if r.getErr != nil {
		return control.Environment{}, r.getErr
	}
	e, ok := r.rows[id]
	if !ok || e.WorkspaceID != ws {
		return control.Environment{}, control.ErrNotFound
	}
	return e, nil
}

func (r *stubEnvironmentRepo) ListEnvironments(ctx context.Context, ws control.WorkspaceID, q control.EnvironmentQuery) ([]control.Environment, string, error) {
	r.log.add("environments:list")
	if r.listErr != nil {
		return nil, "", r.listErr
	}
	rows := make([]control.Environment, 0, len(r.rows))
	for _, e := range r.rows {
		if e.WorkspaceID != ws {
			continue
		}
		rows = append(rows, e)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Name == rows[j].Name {
			return rows[i].ID < rows[j].ID
		}
		return rows[i].Name < rows[j].Name
	})
	return rows, "", nil
}

func (r *stubEnvironmentRepo) UpdateEnvironment(ctx context.Context, ws control.WorkspaceID, e control.Environment) (control.Environment, error) {
	r.log.add("environments:update")
	if r.updateErr != nil {
		return control.Environment{}, r.updateErr
	}
	if cur, ok := r.rows[e.ID]; !ok || cur.WorkspaceID != ws {
		return control.Environment{}, control.ErrNotFound
	}
	r.put(e)
	return e, nil
}

func (r *stubEnvironmentRepo) DeleteEnvironment(ctx context.Context, ws control.WorkspaceID, id control.EnvironmentID) error {
	r.log.add("environments:delete")
	if r.deleteErr != nil {
		return r.deleteErr
	}
	e, ok := r.rows[id]
	if !ok || e.WorkspaceID != ws {
		return control.ErrNotFound
	}
	delete(r.rows, id)
	return nil
}

func (r *stubEnvironmentRepo) CountSessionsByEnvironment(ctx context.Context, ws control.WorkspaceID, envID control.EnvironmentID, states []control.SessionState) (int, error) {
	r.log.add("environments:count")
	r.lastCountStates = states
	if r.countErr != nil {
		return 0, r.countErr
	}
	return r.liveSessionCount, nil
}

func (r *stubEnvironmentRepo) SetEnvironmentSnapshot(ctx context.Context, ws control.WorkspaceID, envID control.EnvironmentID, expectHash, ref string, runnerID control.RunnerID) error {
	r.log.add("environments:set-snapshot")
	return nil
}

type stubPoolResolver struct {
	log *callLog

	pools           []control.Pool
	err             error
	gotRequirements control.Requirements
}

func (p *stubPoolResolver) EligiblePools(ctx context.Context, sc control.Scope, req control.Requirements) ([]control.Pool, error) {
	p.log.add("pools:eligible")
	if p.err != nil {
		return nil, p.err
	}
	p.gotRequirements = req
	return p.pools, nil
}

type stubEventRecorder struct {
	log    *callLog
	err    error
	events []control.Event
}

func (r *stubEventRecorder) Record(ctx context.Context, e control.Event) error {
	r.log.add("events:record")
	if r.err != nil {
		return r.err
	}
	r.events = append(r.events, e)
	return nil
}

type stubTransport struct {
	log          *callLog
	res          runner.FromRunner
	err          error
	connectedMap map[string]bool
	dispatched   []runner.ToRunner
}

func (t *stubTransport) Dispatch(ctx context.Context, pool control.PoolID, id control.RunnerID, m runner.ToRunner) (runner.FromRunner, error) {
	t.log.add("transport:dispatch:" + m.Type)
	if t.err != nil {
		return runner.FromRunner{}, t.err
	}
	t.dispatched = append(t.dispatched, m)
	return t.res, nil
}

func (t *stubTransport) Connected(pool control.PoolID, id control.RunnerID) bool {
	t.log.add("transport:connected:" + string(id))
	if t.connectedMap == nil {
		return true
	}
	return t.connectedMap[string(pool)+"\x00"+string(id)]
}

func (t *stubTransport) dispatchedType(typ string) bool {
	for _, m := range t.dispatched {
		if m.Type == typ {
			return true
		}
	}
	return false
}

type stubFleet struct {
	log *callLog

	runners []control.Runner
	listErr error

	creatingOnRunner    map[string][]control.Session
	sessionsOnRunnerErr error
}

func (f *stubFleet) UpsertRunner(ctx context.Context, pool control.PoolID, r control.Runner) error {
	f.log.add("fleet:upsert")
	return nil
}

func (f *stubFleet) SetRunnerConnected(ctx context.Context, pool control.PoolID, id control.RunnerID, connected bool) error {
	f.log.add("fleet:set-connected")
	return nil
}

func (f *stubFleet) ListRunners(ctx context.Context, pool control.PoolID) ([]control.Runner, error) {
	f.log.add("fleet:list-runners")
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.runners, nil
}

func (f *stubFleet) SessionsOnRunner(ctx context.Context, pool control.PoolID, id control.RunnerID, states []control.SessionState) ([]control.Session, error) {
	f.log.add("fleet:sessions-on-runner")
	if f.sessionsOnRunnerErr != nil {
		return nil, f.sessionsOnRunnerErr
	}
	return f.creatingOnRunner[string(pool)+"\x00"+string(id)], nil
}

func (f *stubFleet) OldestQueued(ctx context.Context, pool control.PoolID) ([]control.Session, error) {
	f.log.add("fleet:oldest-queued")
	return nil, nil
}

// ---------------------------------------------------------------------------
// constructor
// ---------------------------------------------------------------------------

type sessionCreationAndQueries interface {
	CreateSession(context.Context, control.Scope, control.CreateSession) (control.Session, error)
	GetSession(context.Context, control.Scope, control.SessionID) (control.Session, error)
	ListSessions(context.Context, control.Scope, control.SessionQuery) (control.SessionPage, error)
}

var _ sessionCreationAndQueries = (*SessionService)(nil)
var _ control.Sessions = (*SessionService)(nil)

func validSessionOptions() SessionOptions {
	return SessionOptions{
		Authorizer:   &stubAuthorizer{},
		Sessions:     newStubSessionRepo(nil),
		Environments: newStubEnvironmentRepo(nil),
		Pools:        &stubPoolResolver{},
		Events:       &stubEventRecorder{},
		Clock:        stubClock{now: fixedNow},
		IDs:          &stubIDs{sessionID: "sess_example", envID: "env_example", eventID: "evt_example"},
		Wake:         func(control.PoolID) {},
		Fleet:        &stubFleet{},
		Transport:    &stubTransport{},
	}
}

func TestNewSessionServiceRequiresEveryDependency(t *testing.T) {
	if _, err := NewSessionService(validSessionOptions()); err != nil {
		t.Fatalf("NewSessionService(valid): %v", err)
	}
	tests := []struct {
		name string
		mut  func(*SessionOptions)
	}{
		{"authorizer", func(o *SessionOptions) { o.Authorizer = nil }},
		{"sessions", func(o *SessionOptions) { o.Sessions = nil }},
		{"environments", func(o *SessionOptions) { o.Environments = nil }},
		{"pools", func(o *SessionOptions) { o.Pools = nil }},
		{"events", func(o *SessionOptions) { o.Events = nil }},
		{"clock", func(o *SessionOptions) { o.Clock = nil }},
		{"ids", func(o *SessionOptions) { o.IDs = nil }},
		{"wake", func(o *SessionOptions) { o.Wake = nil }},
		{"fleet", func(o *SessionOptions) { o.Fleet = nil }},
		{"transport", func(o *SessionOptions) { o.Transport = nil }},
	}
	for _, tt := range tests {
		o := validSessionOptions()
		tt.mut(&o)
		if _, err := NewSessionService(o); !errors.Is(err, control.ErrInvalid) {
			t.Fatalf("missing %s: got %v, want ErrInvalid", tt.name, err)
		}
	}
}

// sessionFixture wires every dependency with a shared call log so tests can
// prove authorization and durability order.
type sessionFixture struct {
	svc       *SessionService
	repo      *stubSessionRepo
	envRepo   *stubEnvironmentRepo
	pools     *stubPoolResolver
	events    *stubEventRecorder
	auth      *stubAuthorizer
	transport *stubTransport
	fleet     *stubFleet
	log       *callLog
}

func newSessionFixtureFull(t *testing.T) *sessionFixture {
	t.Helper()
	log := &callLog{}
	repo := newStubSessionRepo(log)
	envRepo := newStubEnvironmentRepo(log)
	pools := &stubPoolResolver{log: log}
	events := &stubEventRecorder{log: log}
	auth := &stubAuthorizer{log: log}
	transport := &stubTransport{log: log, res: runner.FromRunner{OK: true}}
	fleet := &stubFleet{log: log}
	svc, err := NewSessionService(SessionOptions{
		Authorizer:   auth,
		Sessions:     repo,
		Environments: envRepo,
		Pools:        pools,
		Events:       events,
		Clock:        stubClock{now: fixedNow},
		IDs:          &stubIDs{log: log, sessionID: "sess_example", envID: "env_example", eventID: "evt_example"},
		Wake:         func(p control.PoolID) { log.add("wake:" + string(p)) },
		Fleet:        fleet,
		Transport:    transport,
	})
	if err != nil {
		t.Fatalf("NewSessionService: %v", err)
	}
	return &sessionFixture{
		svc: svc, repo: repo, envRepo: envRepo, pools: pools, events: events,
		auth: auth, transport: transport, fleet: fleet, log: log,
	}
}

// newSessionFixture keeps the pre-lifecycle call shape for Task 1 tests.
func newSessionFixture(t *testing.T) (*SessionService, *stubSessionRepo, *stubEnvironmentRepo, *stubPoolResolver, *stubEventRecorder, *stubAuthorizer, *callLog) {
	f := newSessionFixtureFull(t)
	return f.svc, f.repo, f.envRepo, f.pools, f.events, f.auth, f.log
}

func exampleEnvironment() control.Environment {
	return control.Environment{
		ID:           "env_example",
		WorkspaceID:  "ws_example",
		Name:         "standard",
		Image:        "registry.example.invalid/rainier@sha256:0000",
		Setup:        "make bootstrap",
		SetupHash:    "sha256_setup_example",
		EgressAllow:  []string{"example.com"},
		Requirements: control.Requirements{Capabilities: []string{"gpu"}, MinCPU: 2},
		Snapshot:     control.Checkpoint{},
		SnapshotHash: "",
		CreatedAt:    fixedNow,
		UpdatedAt:    fixedNow,
	}
}

// ---------------------------------------------------------------------------
// CreateSession
// ---------------------------------------------------------------------------

func TestCreateSessionCore(t *testing.T) {
	svc, _, envRepo, pools, _, _, _ := newSessionFixture(t)
	envRepo.put(exampleEnvironment())
	pools.pools = []control.Pool{
		{ID: "pool_b", CapacityTotal: 5, CapacityUsed: 0},
		{ID: "pool_a", CapacityTotal: 10, CapacityUsed: 2},
	}

	got, err := svc.CreateSession(context.Background(), testScope(), control.CreateSession{
		Name: "investigate", EnvironmentID: "env_example",
		Repos: []control.RepoRef{}, IdempotencyKey: "idem_example",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.WorkspaceID != "ws_example" || got.CreatorID != "act_example" ||
		got.State != control.StateQueued || got.PoolID != "pool_a" ||
		got.PlacementGeneration != 1 {
		t.Fatalf("created session = %+v", got)
	}
	if got.Spec.Repos == nil || len(got.Spec.Repos) != 0 {
		t.Fatalf("explicit empty repos lost: %#v", got.Spec.Repos)
	}
	if got.ID != "sess_example" || got.EnvironmentID != "env_example" {
		t.Fatalf("identity = %+v", got)
	}
	if got.Spec.Image != "registry.example.invalid/rainier@sha256:0000" {
		t.Fatalf("resolved image = %q", got.Spec.Image)
	}
	if !slices.Equal(got.Spec.EgressAllow, []string{"example.com"}) {
		t.Fatalf("resolved egress = %#v", got.Spec.EgressAllow)
	}
	if got.SetupHash != "" {
		t.Fatalf("setup hash = %q, want empty (dispatch is the Fleet lane's job)", got.SetupHash)
	}
	if !got.CreatedAt.Equal(fixedNow) || !got.UpdatedAt.Equal(fixedNow) || !got.LastEventAt.Equal(fixedNow) {
		t.Fatalf("timestamps = %v/%v/%v, want fixed now", got.CreatedAt, got.UpdatedAt, got.LastEventAt)
	}
}

func TestCreateSessionInvalidInput(t *testing.T) {
	svc, repo, envRepo, _, _, _, log := newSessionFixture(t)
	envRepo.put(exampleEnvironment())
	ctx := context.Background()

	// Invalid scope touches no port.
	if _, err := svc.CreateSession(ctx, control.Scope{}, control.CreateSession{Name: "x", EnvironmentID: "env_example"}); !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("invalid scope: got %v, want ErrInvalid", err)
	}
	if len(log.snapshot()) != 0 {
		t.Fatalf("invalid scope touched ports: %v", log.snapshot())
	}

	// Contradictory environment/scratch input touches no port.
	bad := control.CreateSession{Name: "x", EnvironmentID: "env_example", Spec: control.PortableSpec{Image: "img.example.invalid"}}
	if _, err := svc.CreateSession(ctx, testScope(), bad); !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("contradictory create: got %v, want ErrInvalid", err)
	}
	if len(log.snapshot()) != 0 {
		t.Fatalf("contradictory create touched ports: %v", log.snapshot())
	}
	_ = repo
}

func TestCreateSessionAuthorizesBeforeLookupAndStorage(t *testing.T) {
	svc, repo, envRepo, pools, _, auth, log := newSessionFixture(t)
	envRepo.put(exampleEnvironment())
	pools.pools = []control.Pool{{ID: "pool_a", CapacityTotal: 1, CapacityUsed: 0}}
	auth.err = control.ErrDenied

	_, err := svc.CreateSession(context.Background(), testScope(), control.CreateSession{
		Name: "investigate", EnvironmentID: "env_example", IdempotencyKey: "idem_example",
	})
	if !errors.Is(err, control.ErrDenied) {
		t.Fatalf("got %v, want ErrDenied", err)
	}
	for _, forbidden := range []string{"sessions:byidem", "environments:get", "pools:eligible", "sessions:create", "events:record"} {
		if log.has(forbidden) {
			t.Fatalf("denied create reached %q: %v", forbidden, log.snapshot())
		}
	}
	_ = repo
}

func TestCreateSessionIdempotentReplay(t *testing.T) {
	svc, repo, envRepo, pools, events, _, log := newSessionFixture(t)
	envRepo.put(exampleEnvironment())
	pools.pools = []control.Pool{{ID: "pool_a", CapacityTotal: 1, CapacityUsed: 0}}

	existing := control.Session{
		ID: "sess_existing", WorkspaceID: "ws_example", CreatorID: "act_example",
		Name: "investigate", State: control.StateQueued, EnvironmentID: "env_example",
		Spec: control.PortableSpec{Image: "registry.example.invalid/rainier@sha256:0000",
			EgressAllow: []string{"example.com"}, Repos: []control.RepoRef{}},
		IdempotencyKey: "idem_example", CreatedAt: fixedNow, UpdatedAt: fixedNow, LastEventAt: fixedNow,
	}
	repo.idem[idemKey("ws_example", "act_example", "idem_example")] = existing

	got, err := svc.CreateSession(context.Background(), testScope(), control.CreateSession{
		Name: "investigate", EnvironmentID: "env_example", IdempotencyKey: "idem_example",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "sess_existing" {
		t.Fatalf("replay returned %+v, want existing row", got)
	}
	if log.has("id:session") {
		t.Fatalf("replay minted a new id: %v", log.snapshot())
	}
	if log.has("sessions:create") || log.has("events:record") {
		t.Fatalf("replay stored or recorded again: %v", log.snapshot())
	}
	if log.has("wake:pool_a") || log.has("wake:") {
		t.Fatalf("replay woke the scheduler: %v", log.snapshot())
	}
	_ = events
}

func TestCreateSessionEnvironmentNotFound(t *testing.T) {
	svc, repo, envRepo, pools, _, _, log := newSessionFixture(t)
	pools.pools = []control.Pool{{ID: "pool_a", CapacityTotal: 1, CapacityUsed: 0}}

	_, err := svc.CreateSession(context.Background(), testScope(), control.CreateSession{
		Name: "investigate", EnvironmentID: "env_missing",
	})
	if !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
	if log.has("sessions:create") {
		t.Fatalf("missing environment still created a session: %v", log.snapshot())
	}
	_ = repo
	_ = envRepo
}

func TestCreateSessionReposOptionality(t *testing.T) {
	svc, _, envRepo, pools, _, _, _ := newSessionFixture(t)
	envRepo.put(exampleEnvironment())
	pools.pools = []control.Pool{{ID: "pool_a", CapacityTotal: 1, CapacityUsed: 0}}

	nilRepos, err := svc.CreateSession(context.Background(), testScope(), control.CreateSession{
		Name: "a", EnvironmentID: "env_example",
	})
	if err != nil {
		t.Fatal(err)
	}
	if nilRepos.Spec.Repos != nil {
		t.Fatalf("nil repos became %#v", nilRepos.Spec.Repos)
	}

	emptyRepos, err := svc.CreateSession(context.Background(), testScope(), control.CreateSession{
		Name: "b", EnvironmentID: "env_example", Repos: []control.RepoRef{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if emptyRepos.Spec.Repos == nil || len(emptyRepos.Spec.Repos) != 0 {
		t.Fatalf("explicit empty repos lost: %#v", emptyRepos.Spec.Repos)
	}
}

func TestCreateSessionResolvesEnvironmentWithoutAliasing(t *testing.T) {
	svc, repo, envRepo, pools, _, _, _ := newSessionFixture(t)
	egress := []string{"example.com"}
	env := exampleEnvironment()
	env.EgressAllow = egress
	env.Snapshot = control.Checkpoint{Ref: "registry.example.invalid/rainier@sha256:cafe", Format: "rainier-runner-v0", Capabilities: []string{"workspace"}}
	env.SnapshotHash = env.SetupHash // current snapshot
	envRepo.put(env)
	pools.pools = []control.Pool{{ID: "pool_a", CapacityTotal: 1, CapacityUsed: 0}}

	got, err := svc.CreateSession(context.Background(), testScope(), control.CreateSession{
		Name: "investigate", EnvironmentID: "env_example", Repos: nil,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Spec.Image != "registry.example.invalid/rainier@sha256:cafe" {
		t.Fatalf("current snapshot not resolved: %q", got.Spec.Image)
	}
	if got.Spec.Repos != nil {
		t.Fatalf("nil repos lost: %#v", got.Spec.Repos)
	}
	if !slices.Equal(got.Spec.EgressAllow, []string{"example.com"}) {
		t.Fatalf("egress = %#v", got.Spec.EgressAllow)
	}

	// Mutating the environment's own slice must not change the stored session.
	egress[0] = "evil.example"
	stored := repo.rows[got.ID]
	if stored.Spec.EgressAllow[0] != "example.com" {
		t.Fatalf("stored session aliased the environment's egress: %#v", stored.Spec.EgressAllow)
	}
}

func TestCreateSessionScratchRequirementsAreZero(t *testing.T) {
	svc, _, _, pools, _, _, _ := newSessionFixture(t)
	pools.pools = []control.Pool{{ID: "pool_a", CapacityTotal: 1, CapacityUsed: 0}}

	got, err := svc.CreateSession(context.Background(), testScope(), control.CreateSession{
		Name: "scratch", Spec: control.PortableSpec{
			Image: "registry.example.invalid/agent@sha256:0000", Cmd: []string{"bash"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Spec.Image != "registry.example.invalid/agent@sha256:0000" {
		t.Fatalf("scratch image = %q", got.Spec.Image)
	}
	if !slices.Equal(got.Spec.Cmd, []string{"bash"}) {
		t.Fatalf("scratch cmd = %#v", got.Spec.Cmd)
	}
	if got.EnvironmentID != "" {
		t.Fatalf("scratch environment id = %q", got.EnvironmentID)
	}
	if pools.gotRequirements.Capabilities != nil || pools.gotRequirements.MinCPU != 0 ||
		pools.gotRequirements.MinMemoryBytes != 0 || pools.gotRequirements.MinDiskBytes != 0 {
		t.Fatalf("scratch requirements = %+v, want zero", pools.gotRequirements)
	}
}

func TestCreateSessionPoolSelection(t *testing.T) {
	svc, _, envRepo, pools, _, _, _ := newSessionFixture(t)
	envRepo.put(exampleEnvironment())
	ctx := context.Background()

	tests := []struct {
		name    string
		pools   []control.Pool
		want    control.PoolID
		wantErr error
	}{
		{"greatest free", []control.Pool{
			{ID: "pool_small", CapacityTotal: 3, CapacityUsed: 1},
			{ID: "pool_big", CapacityTotal: 20, CapacityUsed: 2},
		}, "pool_big", nil},
		{"tie broken by ascending id", []control.Pool{
			{ID: "pool_z", CapacityTotal: 10, CapacityUsed: 5},
			{ID: "pool_a", CapacityTotal: 10, CapacityUsed: 5},
		}, "pool_a", nil},
		{"no positive capacity", []control.Pool{
			{ID: "pool_full", CapacityTotal: 2, CapacityUsed: 2},
			{ID: "pool_over", CapacityTotal: 1, CapacityUsed: 3},
		}, "", control.ErrUnavailable},
		{"no pools", nil, "", control.ErrUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pools.pools = tt.pools
			got, err := svc.CreateSession(ctx, testScope(), control.CreateSession{Name: "investigate", EnvironmentID: "env_example"})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("got %v, want %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if got.PoolID != tt.want {
				t.Fatalf("pool = %q, want %q", got.PoolID, tt.want)
			}
		})
	}
}

func TestCreateSessionOrdering(t *testing.T) {
	svc, _, envRepo, pools, _, _, log := newSessionFixture(t)
	envRepo.put(exampleEnvironment())
	pools.pools = []control.Pool{{ID: "pool_a", CapacityTotal: 1, CapacityUsed: 0}}

	if _, err := svc.CreateSession(context.Background(), testScope(), control.CreateSession{
		Name: "investigate", EnvironmentID: "env_example",
	}); err != nil {
		t.Fatal(err)
	}
	ordered(t, log.snapshot(),
		"auth:create:session", "environments:get", "pools:eligible",
		"id:session", "sessions:create", "id:event", "events:record", "wake:pool_a")
}

// ---------------------------------------------------------------------------
// GetSession / ListSessions
// ---------------------------------------------------------------------------

func TestGetSessionScopedAndCopied(t *testing.T) {
	svc, repo, _, _, _, auth, _ := newSessionFixture(t)
	repo.put(control.Session{
		ID: "sess_example", WorkspaceID: "ws_example", CreatorID: "act_example",
		Name: "investigate", State: control.StateRunning,
		Spec: control.PortableSpec{Repos: []control.RepoRef{{Repo: "acme/app"}}},
	})

	got, err := svc.GetSession(context.Background(), testScope(), "sess_example")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "sess_example" {
		t.Fatalf("got %+v", got)
	}
	// Mutating the returned copy must not touch the stored row.
	got.Spec.Repos[0].Repo = "evil/thing"
	stored := repo.rows["sess_example"]
	if stored.Spec.Repos[0].Repo != "acme/app" {
		t.Fatalf("returned session aliased the stored row: %#v", stored.Spec.Repos)
	}

	// A denied caller receives no row.
	auth.err = control.ErrDenied
	if _, err := svc.GetSession(context.Background(), testScope(), "sess_example"); !errors.Is(err, control.ErrDenied) {
		t.Fatalf("denied get: got %v, want ErrDenied", err)
	}

	// A cross-workspace miss is ErrNotFound, not ErrDenied or ErrInvalid.
	other := testScope()
	other.WorkspaceID = "ws_other"
	auth.err = nil
	if _, err := svc.GetSession(context.Background(), other, "sess_example"); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("cross-workspace get: got %v, want ErrNotFound", err)
	}
}

func TestListSessionsAuthorizesBeforeRepository(t *testing.T) {
	svc, repo, _, _, _, auth, log := newSessionFixture(t)
	repo.put(control.Session{
		ID: "sess_example", WorkspaceID: "ws_example", CreatorID: "act_example",
		State: control.StateQueued, CreatedAt: fixedNow,
	})
	auth.err = control.ErrDenied

	page, err := svc.ListSessions(context.Background(), testScope(), control.SessionQuery{Limit: 10})
	if !errors.Is(err, control.ErrDenied) {
		t.Fatalf("got %v, want ErrDenied", err)
	}
	if page.Sessions != nil {
		t.Fatalf("denied list returned a page: %+v", page)
	}
	if log.has("sessions:list") {
		t.Fatalf("denied list reached the repository: %v", log.snapshot())
	}
}

func TestListSessionsPassthroughAndCopies(t *testing.T) {
	svc, repo, _, _, _, _, _ := newSessionFixture(t)
	repo.put(control.Session{
		ID: "sess_example", WorkspaceID: "ws_example", CreatorID: "act_example",
		State: control.StateQueued, CreatedAt: fixedNow,
		Spec: control.PortableSpec{Repos: []control.RepoRef{{Repo: "acme/app"}}},
	})

	// Invalid limits/cursors are the repository's concern: they pass through.
	q := control.SessionQuery{Limit: -5, Cursor: "bogus_cursor", IncludeTerminal: true}
	page, err := svc.ListSessions(context.Background(), testScope(), q)
	if err != nil {
		t.Fatal(err)
	}
	if repo.lastListWS != "ws_example" {
		t.Fatalf("list workspace = %q", repo.lastListWS)
	}
	if repo.lastListQuery.Limit != -5 || repo.lastListQuery.Cursor != "bogus_cursor" || !repo.lastListQuery.IncludeTerminal {
		t.Fatalf("list query not passed through: %+v", repo.lastListQuery)
	}
	if len(page.Sessions) != 1 || page.Sessions[0].ID != "sess_example" {
		t.Fatalf("page = %+v", page)
	}

	// The returned page cannot mutate the stored row.
	page.Sessions[0].Spec.Repos[0].Repo = "evil/thing"
	if repo.rows["sess_example"].Spec.Repos[0].Repo != "acme/app" {
		t.Fatalf("page aliased the stored row")
	}

	// An empty page is a non-nil empty slice.
	repo.rows = map[control.SessionID]control.Session{}
	empty, err := svc.ListSessions(context.Background(), testScope(), control.SessionQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if empty.Sessions == nil || len(empty.Sessions) != 0 {
		t.Fatalf("empty page = %#v, want non-nil empty", empty.Sessions)
	}
}

// ---------------------------------------------------------------------------
// guarded lifecycle
// ---------------------------------------------------------------------------

func sessionInState(state control.SessionState) control.Session {
	row := control.Session{
		ID:                  "sess_example",
		WorkspaceID:         "ws_example",
		CreatorID:           "act_example",
		Name:                "investigate",
		State:               state,
		EnvironmentID:       "env_example",
		PoolID:              "pool_a",
		RunnerID:            "runner_a",
		PlacementGeneration: 1,
		CreatedAt:           fixedNow,
		UpdatedAt:           fixedNow,
		LastEventAt:         fixedNow,
	}
	if state == control.StateQueued {
		row.RunnerID = ""
	}
	return row
}

func TestDeleteSessionStateTable(t *testing.T) {
	tests := []struct {
		name        string
		state       control.SessionState
		wantTo      control.SessionState
		wantCommand string
		wantErr     error
	}{
		{"delete queued", control.StateQueued, control.StateCanceled, "", nil},
		{"delete creating", control.StateCreating, "", "", control.ErrConflict},
		{"delete running", control.StateRunning, control.StateDestroyed, "destroy", nil},
		{"delete failed", control.StateFailed, control.StateDestroyed, "destroy", nil},
		{"delete destroyed idempotent", control.StateDestroyed, control.StateDestroyed, "", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newSessionFixtureFull(t)
			f.repo.put(sessionInState(tt.state))

			err := f.svc.DeleteSession(context.Background(), testScope(), control.DeleteSession{ID: "sess_example"})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("got %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				if f.transport.dispatchedType("destroy") {
					t.Fatalf("conflicting delete still dispatched destroy: %+v", f.transport.dispatched)
				}
				return
			}

			got, err := f.repo.GetSession(context.Background(), "ws_example", "sess_example")
			if err != nil {
				t.Fatal(err)
			}
			if got.State != tt.wantTo {
				t.Fatalf("state = %q, want %q", got.State, tt.wantTo)
			}
			if tt.wantCommand == "" {
				if f.transport.dispatchedType("destroy") {
					t.Fatalf("delete dispatched destroy but none was expected")
				}
			} else if !f.transport.dispatchedType(tt.wantCommand) {
				t.Fatalf("delete did not dispatch %q", tt.wantCommand)
			}
		})
	}
}

func TestSuspendSession(t *testing.T) {
	tests := []struct {
		name    string
		state   control.SessionState
		warm    bool
		wantTo  control.SessionState
		wantErr error
	}{
		{"warm from running", control.StateRunning, true, control.StateSuspendedWarm, nil},
		{"cold from running", control.StateRunning, false, control.StateSuspendedCold, nil},
		{"not running", control.StateQueued, true, "", control.ErrConflict},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newSessionFixtureFull(t)
			f.repo.put(sessionInState(tt.state))

			got, err := f.svc.SuspendSession(context.Background(), testScope(), control.SuspendSession{ID: "sess_example", Warm: tt.warm})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("got %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}

			var suspendMsg runner.ToRunner
			for _, m := range f.transport.dispatched {
				if m.Type == "suspend" {
					suspendMsg = m
					break
				}
			}
			if suspendMsg.Type == "" {
				t.Fatalf("suspend did not dispatch suspend")
			}
			if suspendMsg.Session != "sess_example" || suspendMsg.Warm != tt.warm {
				t.Fatalf("suspend message = %+v, want session sess_example warm=%v", suspendMsg, tt.warm)
			}
			if got.State != tt.wantTo {
				t.Fatalf("returned state = %q, want %q", got.State, tt.wantTo)
			}
			row, _ := f.repo.GetSession(context.Background(), "ws_example", "sess_example")
			if row.State != tt.wantTo {
				t.Fatalf("stored state = %q, want %q", row.State, tt.wantTo)
			}
			ordered(t, f.log.snapshot(),
				"auth:suspend:session",
				"transport:dispatch:suspend",
				"sessions:transition:"+string(tt.wantTo),
				"events:record",
				"wake:pool_a")
		})
	}
}

func TestResumeSession(t *testing.T) {
	tests := []struct {
		name    string
		state   control.SessionState
		wantErr error
	}{
		{"warm", control.StateSuspendedWarm, nil},
		{"cold", control.StateSuspendedCold, nil},
		{"not suspended", control.StateRunning, control.ErrConflict},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newSessionFixtureFull(t)
			f.fleet.runners = []control.Runner{{ID: "runner_a", PoolID: "pool_a", CapacityTotal: 4, CapacityUsed: 1, Connected: true}}
			f.fleet.creatingOnRunner = map[string][]control.Session{}
			f.repo.put(sessionInState(tt.state))

			got, err := f.svc.ResumeSession(context.Background(), testScope(), control.ResumeSession{ID: "sess_example"})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("got %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			if !f.transport.dispatchedType("resume") {
				t.Fatalf("resume did not dispatch resume")
			}
			if got.State != control.StateRunning {
				t.Fatalf("returned state = %q, want running", got.State)
			}
			row, _ := f.repo.GetSession(context.Background(), "ws_example", "sess_example")
			if row.State != control.StateRunning {
				t.Fatalf("stored state = %q, want running", row.State)
			}
		})
	}
}

func TestResumeColdRequiresFreeCapacity(t *testing.T) {
	ctx := context.Background()

	t.Run("no free slot", func(t *testing.T) {
		f := newSessionFixtureFull(t)
		f.fleet.runners = []control.Runner{{ID: "runner_a", PoolID: "pool_a", CapacityTotal: 2, CapacityUsed: 1, Connected: true}}
		// two creating sessions: free = 2 - 1 - 2 = -1, so no slot.
		f.fleet.creatingOnRunner = map[string][]control.Session{
			"pool_a\x00runner_a": {{}, {}},
		}
		f.repo.put(sessionInState(control.StateSuspendedCold))

		if _, err := f.svc.ResumeSession(ctx, testScope(), control.ResumeSession{ID: "sess_example"}); !errors.Is(err, control.ErrConflict) {
			t.Fatalf("got %v, want ErrConflict", err)
		}
		if f.transport.dispatchedType("resume") {
			t.Fatalf("cold resume with no slot still dispatched resume")
		}
	})

	t.Run("free slot", func(t *testing.T) {
		f := newSessionFixtureFull(t)
		f.fleet.runners = []control.Runner{{ID: "runner_a", PoolID: "pool_a", CapacityTotal: 4, CapacityUsed: 1, Connected: true}}
		f.fleet.creatingOnRunner = map[string][]control.Session{
			"pool_a\x00runner_a": {{}},
		}
		f.repo.put(sessionInState(control.StateSuspendedCold))

		got, err := f.svc.ResumeSession(ctx, testScope(), control.ResumeSession{ID: "sess_example"})
		if err != nil {
			t.Fatal(err)
		}
		if got.State != control.StateRunning {
			t.Fatalf("state = %q, want running", got.State)
		}
	})
}

func TestSnapshotSession(t *testing.T) {
	ctx := context.Background()

	for _, state := range []control.SessionState{control.StateRunning, control.StateSuspendedWarm, control.StateSuspendedCold} {
		t.Run(string(state), func(t *testing.T) {
			f := newSessionFixtureFull(t)
			f.transport.res = runner.FromRunner{OK: true, Detail: "snap_ref_example"}
			f.repo.put(sessionInState(state))

			got, err := f.svc.SnapshotSession(ctx, testScope(), control.SnapshotSession{ID: "sess_example"})
			if err != nil {
				t.Fatal(err)
			}
			if got.Ref != "snap_ref_example" || got.Format != "rainier-runner-v0" || !slices.Equal(got.Capabilities, []string{"workspace"}) {
				t.Fatalf("checkpoint = %+v", got)
			}
			if len(f.events.events) != 1 || f.events.events[0].Action != control.ActionSnapshot {
				t.Fatalf("events = %+v", f.events.events)
			}
		})
	}

	t.Run("conflicting state", func(t *testing.T) {
		f := newSessionFixtureFull(t)
		f.repo.put(sessionInState(control.StateQueued))
		if _, err := f.svc.SnapshotSession(ctx, testScope(), control.SnapshotSession{ID: "sess_example"}); !errors.Is(err, control.ErrConflict) {
			t.Fatalf("got %v, want ErrConflict", err)
		}
	})

	t.Run("empty detail is unavailable", func(t *testing.T) {
		f := newSessionFixtureFull(t)
		f.transport.res = runner.FromRunner{OK: true, Detail: ""}
		f.repo.put(sessionInState(control.StateRunning))
		if _, err := f.svc.SnapshotSession(ctx, testScope(), control.SnapshotSession{ID: "sess_example"}); !errors.Is(err, control.ErrUnavailable) {
			t.Fatalf("got %v, want ErrUnavailable", err)
		}
	})

	t.Run("runner failure detail never reaches the error", func(t *testing.T) {
		f := newSessionFixtureFull(t)
		f.transport.res = runner.FromRunner{OK: false, Detail: "secret runner detail"}
		f.repo.put(sessionInState(control.StateRunning))
		_, err := f.svc.SnapshotSession(ctx, testScope(), control.SnapshotSession{ID: "sess_example"})
		if !errors.Is(err, control.ErrUnavailable) {
			t.Fatalf("got %v, want ErrUnavailable", err)
		}
		if strings.Contains(err.Error(), "secret runner detail") {
			t.Fatalf("runner detail leaked: %v", err)
		}
	})
}

func TestLifecycleDenialPreventsDispatchAndTransition(t *testing.T) {
	ctx := context.Background()
	f := newSessionFixtureFull(t)
	f.auth.err = control.ErrDenied
	f.repo.put(sessionInState(control.StateRunning))

	if err := f.svc.DeleteSession(ctx, testScope(), control.DeleteSession{ID: "sess_example"}); !errors.Is(err, control.ErrDenied) {
		t.Fatalf("delete: got %v, want ErrDenied", err)
	}
	if _, err := f.svc.SuspendSession(ctx, testScope(), control.SuspendSession{ID: "sess_example", Warm: true}); !errors.Is(err, control.ErrDenied) {
		t.Fatalf("suspend: got %v, want ErrDenied", err)
	}
	if _, err := f.svc.ResumeSession(ctx, testScope(), control.ResumeSession{ID: "sess_example"}); !errors.Is(err, control.ErrDenied) {
		t.Fatalf("resume: got %v, want ErrDenied", err)
	}
	if _, err := f.svc.SnapshotSession(ctx, testScope(), control.SnapshotSession{ID: "sess_example"}); !errors.Is(err, control.ErrDenied) {
		t.Fatalf("snapshot: got %v, want ErrDenied", err)
	}

	for _, forbidden := range []string{"transport:dispatch", "sessions:transition", "events:record", "fleet:list-runners"} {
		if f.log.hasPrefix(forbidden) {
			t.Fatalf("denied lifecycle reached %q: %v", forbidden, f.log.snapshot())
		}
	}
}

func TestLifecycleRunnerUnavailable(t *testing.T) {
	f := newSessionFixtureFull(t)
	f.transport.connectedMap = map[string]bool{}
	f.repo.put(sessionInState(control.StateRunning))

	if err := f.svc.DeleteSession(context.Background(), testScope(), control.DeleteSession{ID: "sess_example"}); !errors.Is(err, control.ErrUnavailable) {
		t.Fatalf("got %v, want ErrUnavailable", err)
	}
	if f.log.hasPrefix("sessions:transition") {
		t.Fatalf("unavailable runner still transitioned: %v", f.log.snapshot())
	}
}

func TestSuspendConflictAfterDispatchRereadsAuthoritative(t *testing.T) {
	f := newSessionFixtureFull(t)
	f.repo.put(sessionInState(control.StateRunning))
	f.repo.transitionErr = control.ErrConflict

	got, err := f.svc.SuspendSession(context.Background(), testScope(), control.SuspendSession{ID: "sess_example", Warm: true})
	if err != nil {
		t.Fatalf("suspend after dispatched side effect: %v", err)
	}
	if got.State != control.StateRunning {
		t.Fatalf("authoritative state = %q, want running (the row never committed)", got.State)
	}
	if f.log.hasPrefix("wake:pool_a") {
		// waking is fine; nothing asserted here beyond the authoritative read.
	}
}

func TestResumeConflictAfterDispatchRereadsAuthoritative(t *testing.T) {
	f := newSessionFixtureFull(t)
	f.repo.put(sessionInState(control.StateSuspendedWarm))
	f.repo.transitionErr = control.ErrConflict

	got, err := f.svc.ResumeSession(context.Background(), testScope(), control.ResumeSession{ID: "sess_example"})
	if err != nil {
		t.Fatalf("resume after dispatched side effect: %v", err)
	}
	if got.State != control.StateSuspendedWarm {
		t.Fatalf("authoritative state = %q, want suspended_warm", got.State)
	}
}

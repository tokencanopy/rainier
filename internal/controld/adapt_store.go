package controld

import (
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/tokencanopy/rainier/control"
)

// The store adapters. Store (store.go) is single-tenant and predates the
// control contract: it has no workspace, no pool, no placement generation,
// and no capability list. These three types are the whole translation —
// every method is a workspace or pool check, a store call, a sentinel
// mapping, and a conversion, and nothing here makes a lifecycle decision.
var (
	_ control.SessionRepository     = storeSessions{}
	_ control.EnvironmentRepository = storeEnvironments{}
	_ control.FleetRepository       = (*storeFleet)(nil)
	_ control.PoolResolver          = installationPools{}
	_ control.EventRecorder         = logRecorder{}
	_ control.Clock                 = systemClock{}
	_ control.IDGenerator           = idGenerator{}
)

// storeErr maps a Store error onto the control contract's closed sentinel
// set. It returns the bare sentinel and never wraps: a store's own message
// may carry SQL, a DSN, or a row's contents, and none of that may reach a
// control error (control/errors.go). A store that cannot answer is a
// dependency that is temporarily unusable, which is ErrUnavailable.
func storeErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrNotFound):
		return control.ErrNotFound
	case errors.Is(err, ErrConflict):
		return control.ErrConflict
	default:
		return control.ErrUnavailable
	}
}

// ---------------------------------------------------------------------------
// runner generations
// ---------------------------------------------------------------------------

// runnerGenerations hands out the monotonic placement generation a runner's
// connection acts under. In O8 it is process-local: a controld restart starts
// every runner over at 1, which is sound because the connections restart with
// it. O9 persists it.
type runnerGenerations struct {
	mu  sync.Mutex
	cur map[string]uint64
}

// next opens a new generation for name and returns it: 1 for the first
// connection, 2 for the next, and so on.
func (g *runnerGenerations) next(name string) uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.cur == nil {
		g.cur = make(map[string]uint64)
	}
	g.cur[name]++
	return g.cur[name]
}

// current reports name's authoritative generation, 0 when it has never
// connected to this process.
func (g *runnerGenerations) current(name string) uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.cur[name]
}

// ---------------------------------------------------------------------------
// controller leases
// ---------------------------------------------------------------------------

// controllerLeases is the process-local controller generation table the
// self-hosted adapter answers NextControllerGeneration from until the store
// persists it (Task 2 of the workspace-scope plan). Keyed by session; a
// restart starts every session over at 0, which is sound because every
// attachment dies with the process.
type controllerLeases struct {
	mu  sync.Mutex
	cur map[control.SessionID]uint64
}

// next opens a new controller generation for id and returns it: 1 for the
// first controller, 2 for the next, and so on.
func (l *controllerLeases) next(id control.SessionID) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cur == nil {
		l.cur = make(map[control.SessionID]uint64)
	}
	l.cur[id]++
	return l.cur[id]
}

// current reports id's controller generation, 0 when no controller has
// attached to it in this process.
func (l *controllerLeases) current(id control.SessionID) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.cur[id]
}

// ---------------------------------------------------------------------------
// sessions
// ---------------------------------------------------------------------------

// storeSessions is control.SessionRepository over Store, plus the
// process-local controller lease table the store has no column for.
type storeSessions struct {
	st     Store
	leases *controllerLeases
}

// generation reports a session's controller generation, 0 when no lease
// table is wired (nothing has taken control in this process).
func (r storeSessions) generation(id control.SessionID) uint64 {
	if r.leases == nil {
		return 0
	}
	return r.leases.current(id)
}

func (r storeSessions) CreateSession(ctx context.Context, ws control.WorkspaceID, s control.Session) (control.Session, error) {
	if ws != installWorkspace {
		return control.Session{}, control.ErrNotFound
	}
	row, err := r.st.CreateSession(ctx, sessionFromControl(s))
	if errors.Is(err, ErrIdemReplay) {
		// The store refuses a replayed key; the contract answers it with the
		// row the key already created, so the caller sees its own earlier
		// answer rather than an error it cannot act on.
		row, err = r.st.SessionByIdem(ctx, string(s.CreatorID), s.IdempotencyKey)
	}
	if err != nil {
		return control.Session{}, storeErr(err)
	}
	return sessionToControl(row), nil
}

func (r storeSessions) GetSession(ctx context.Context, ws control.WorkspaceID, id control.SessionID) (control.Session, error) {
	if ws != installWorkspace {
		return control.Session{}, control.ErrNotFound
	}
	row, err := r.st.GetSession(ctx, string(id))
	if err != nil {
		return control.Session{}, storeErr(err)
	}
	c := sessionToControl(row)
	c.ControllerGeneration = r.generation(id)
	return c, nil
}

func (r storeSessions) SessionByIDem(ctx context.Context, ws control.WorkspaceID, creator control.ActorID, key string) (control.Session, error) {
	if ws != installWorkspace {
		return control.Session{}, control.ErrNotFound
	}
	row, err := r.st.SessionByIdem(ctx, string(creator), key)
	if err != nil {
		return control.Session{}, storeErr(err)
	}
	return sessionToControl(row), nil
}

func (r storeSessions) ListSessions(ctx context.Context, ws control.WorkspaceID, q control.SessionQuery) ([]control.Session, string, error) {
	if ws != installWorkspace {
		return nil, "", control.ErrNotFound
	}
	// control.SessionQuery carries no state, name, or runner filter (D5);
	// the handler applies those to the page the service returns.
	rows, next, err := r.st.ListSessions(ctx, SessionQuery{
		IncludeTerminal: q.IncludeTerminal,
		Limit:           q.Limit,
		Cursor:          q.Cursor,
	})
	if err != nil {
		if q.Cursor != "" {
			// The only per-request failure this query has is a cursor the
			// store cannot decode, and that is the caller's mistake rather
			// than a store that cannot answer.
			return nil, "", control.ErrInvalid
		}
		return nil, "", storeErr(err)
	}
	out := sessionsToControl(rows)
	for i := range out {
		out[i].ControllerGeneration = r.generation(out[i].ID)
	}
	return out, next, nil
}

func (r storeSessions) Transition(ctx context.Context, ws control.WorkspaceID, id control.SessionID, from []control.SessionState, to control.SessionState, opts control.TransitionOpts) error {
	if ws != installWorkspace {
		return control.ErrNotFound
	}
	sopts := TransitionOpts{Error: opts.Error}
	if opts.RunnerID != nil {
		name := string(*opts.RunnerID)
		sopts.Runner = &name
	}
	return storeErr(r.st.Transition(ctx, string(id), statesFromControl(from), SessionState(to), sopts))
}

func (r storeSessions) SetSessionSetupHash(ctx context.Context, ws control.WorkspaceID, id control.SessionID, hash string) error {
	if ws != installWorkspace {
		return control.ErrNotFound
	}
	return storeErr(r.st.SetSessionSetupHash(ctx, string(id), hash))
}

func (r storeSessions) SetChildExitCode(ctx context.Context, ws control.WorkspaceID, id control.SessionID, code int) error {
	if ws != installWorkspace {
		return control.ErrNotFound
	}
	return storeErr(r.st.SetChildExitCode(ctx, string(id), code))
}

// NextControllerGeneration advances id's controller generation in the
// process-local table, after confirming through the store that the session
// exists: the table is not authority over a session's existence, the store
// is, and a generation for a row that is gone would be authority over
// nothing.
func (r storeSessions) NextControllerGeneration(ctx context.Context, ws control.WorkspaceID, id control.SessionID) (uint64, error) {
	if ws != installWorkspace {
		return 0, control.ErrNotFound
	}
	if _, err := r.st.GetSession(ctx, string(id)); err != nil {
		return 0, storeErr(err)
	}
	if r.leases == nil {
		// A repository composed without a lease table cannot hand out
		// authority; refusing is the only safe answer.
		return 0, control.ErrUnavailable
	}
	return r.leases.next(id), nil
}

// ---------------------------------------------------------------------------
// environments
// ---------------------------------------------------------------------------

// storeEnvironments is control.EnvironmentRepository over Store.
type storeEnvironments struct{ st Store }

func (r storeEnvironments) CreateEnvironment(ctx context.Context, ws control.WorkspaceID, e control.Environment) (control.Environment, error) {
	if ws != installWorkspace {
		return control.Environment{}, control.ErrNotFound
	}
	row, err := r.st.CreateEnvironment(ctx, environmentFromControl(e))
	if err != nil {
		return control.Environment{}, storeErr(err)
	}
	return environmentToControl(row), nil
}

func (r storeEnvironments) GetEnvironment(ctx context.Context, ws control.WorkspaceID, id control.EnvironmentID) (control.Environment, error) {
	if ws != installWorkspace {
		return control.Environment{}, control.ErrNotFound
	}
	row, err := r.st.GetEnvironment(ctx, string(id))
	if err != nil {
		return control.Environment{}, storeErr(err)
	}
	return environmentToControl(row), nil
}

// ListEnvironments returns the whole table and no cursor: there are few
// environments per installation, so the store's listing is already the only
// page there is (store.go, ListEnvironments).
func (r storeEnvironments) ListEnvironments(ctx context.Context, ws control.WorkspaceID, q control.EnvironmentQuery) ([]control.Environment, string, error) {
	if ws != installWorkspace {
		return nil, "", control.ErrNotFound
	}
	rows, err := r.st.ListEnvironments(ctx)
	if err != nil {
		return nil, "", storeErr(err)
	}
	out := make([]control.Environment, len(rows))
	for i, row := range rows {
		out[i] = environmentToControl(row)
	}
	return out, "", nil
}

// UpdateEnvironment re-reads the row it is about to replace and carries the
// three snapshot columns forward. environmentFromControl deliberately leaves
// them empty, so without this an update would ask the store to blank a cache
// the store alone owns.
func (r storeEnvironments) UpdateEnvironment(ctx context.Context, ws control.WorkspaceID, e control.Environment) (control.Environment, error) {
	if ws != installWorkspace {
		return control.Environment{}, control.ErrNotFound
	}
	cur, err := r.st.GetEnvironment(ctx, string(e.ID))
	if err != nil {
		return control.Environment{}, storeErr(err)
	}
	upd := environmentFromControl(e)
	upd.SnapshotRef, upd.SnapshotRunner, upd.SnapshotHash = cur.SnapshotRef, cur.SnapshotRunner, cur.SnapshotHash
	row, err := r.st.UpdateEnvironment(ctx, upd)
	if err != nil {
		return control.Environment{}, storeErr(err)
	}
	return environmentToControl(row), nil
}

func (r storeEnvironments) DeleteEnvironment(ctx context.Context, ws control.WorkspaceID, id control.EnvironmentID) error {
	if ws != installWorkspace {
		return control.ErrNotFound
	}
	return storeErr(r.st.DeleteEnvironment(ctx, string(id)))
}

func (r storeEnvironments) CountSessionsByEnvironment(ctx context.Context, ws control.WorkspaceID, envID control.EnvironmentID, states []control.SessionState) (int, error) {
	if ws != installWorkspace {
		return 0, control.ErrNotFound
	}
	n, err := r.st.CountSessionsByEnvironment(ctx, string(envID), statesFromControl(states))
	if err != nil {
		return 0, storeErr(err)
	}
	return n, nil
}

// SetEnvironmentSnapshot is the one method that does not map the store's
// sentinels straight through. The store reports both a superseded setup hash
// and a vanished environment as ErrConflict (store.go), and the contract
// names a snapshot built from edited setup stale, not conflicting — a
// caller's answer to stale is "rebuild", not "retry".
func (r storeEnvironments) SetEnvironmentSnapshot(ctx context.Context, ws control.WorkspaceID, envID control.EnvironmentID, expectHash, ref string, runnerID control.RunnerID) error {
	if ws != installWorkspace {
		return control.ErrNotFound
	}
	err := r.st.SetEnvironmentSnapshot(ctx, string(envID), expectHash, ref, string(runnerID))
	if errors.Is(err, ErrConflict) {
		return control.ErrStale
	}
	return storeErr(err)
}

// ---------------------------------------------------------------------------
// fleet
// ---------------------------------------------------------------------------

// storeFleet is control.FleetRepository over Store, plus the process-local
// generation table the store has no column for.
type storeFleet struct {
	st   Store
	gens *runnerGenerations
}

// UpsertRunner writes the four columns the store has. Generation and
// Capabilities are deliberately dropped: neither has a column in O8, the
// generation is process-local, and the capabilities are synthesized from the
// name on the way out. There is therefore no generation for a concurrent
// write to lose a race on, and this never reports ErrStale.
func (f *storeFleet) UpsertRunner(ctx context.Context, pool control.PoolID, r control.Runner) error {
	if pool != installPool {
		return control.ErrNotFound
	}
	return storeErr(f.st.UpsertRunner(ctx, Runner{
		Name:          string(r.ID),
		CapacityUsed:  r.CapacityUsed,
		CapacityTotal: r.CapacityTotal,
		Connected:     r.Connected,
		LastSeenAt:    r.LastSeenAt,
	}))
}

func (f *storeFleet) SetRunnerConnected(ctx context.Context, pool control.PoolID, id control.RunnerID, connected bool) error {
	if pool != installPool {
		return control.ErrNotFound
	}
	return storeErr(f.st.SetRunnerConnected(ctx, string(id), connected))
}

// ListRunners attaches each runner's current generation and its two
// synthesized capabilities, in stable name order.
func (f *storeFleet) ListRunners(ctx context.Context, pool control.PoolID) ([]control.Runner, error) {
	if pool != installPool {
		return nil, control.ErrNotFound
	}
	rows, err := f.st.ListRunners(ctx)
	if err != nil {
		return nil, storeErr(err)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })
	out := make([]control.Runner, len(rows))
	for i, row := range rows {
		out[i] = runnerToControl(row, f.generation(row.Name))
	}
	return out, nil
}

func (f *storeFleet) SessionsOnRunner(ctx context.Context, pool control.PoolID, id control.RunnerID, states []control.SessionState) ([]control.Session, error) {
	if pool != installPool {
		return nil, control.ErrNotFound
	}
	rows, err := f.st.SessionsOnRunner(ctx, string(id), statesFromControl(states))
	if err != nil {
		return nil, storeErr(err)
	}
	return sessionsToControl(rows), nil
}

func (f *storeFleet) OldestQueued(ctx context.Context, pool control.PoolID) ([]control.Session, error) {
	if pool != installPool {
		return nil, control.ErrNotFound
	}
	rows, err := f.st.OldestQueued(ctx)
	if err != nil {
		return nil, storeErr(err)
	}
	return sessionsToControl(rows), nil
}

// generation reports a runner's current generation, 0 when no generation
// table is wired (nothing has connected to this process).
func (f *storeFleet) generation(name string) uint64 {
	if f.gens == nil {
		return 0
	}
	return f.gens.current(name)
}

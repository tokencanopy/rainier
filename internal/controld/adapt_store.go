package controld

import (
	"context"
	"errors"
	"slices"
	"sort"
	"strings"
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

// snapshotCheckpointFormat is the format every self-hosted environment
// snapshot has: a runner-built container image reference. control.Checkpoint
// carries the format so a later provider can add a second one without
// changing the field.
const snapshotCheckpointFormat = "rainier-runner-v0"

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
// conversions
// ---------------------------------------------------------------------------

// sessionToControl lifts a store row into the control model. Three fields
// have no column and are supplied by the installation's identity: the
// workspace, the pool (a queued session is queued *in* the installation
// pool, so it is set whether or not the row is placed), and the placement
// generation, which is 1 for every row until O9 persists it.
func sessionToControl(s Session) control.Session {
	return control.Session{
		ID:            control.SessionID(s.ID),
		WorkspaceID:   installWorkspace,
		CreatorID:     control.ActorID(s.OwnerID),
		Name:          s.Name,
		State:         control.SessionState(s.State),
		EnvironmentID: control.EnvironmentID(s.EnvironmentID),
		Spec: control.PortableSpec{
			Image:       s.effectiveImage(),
			Cmd:         slices.Clone(s.Cmd),
			EgressAllow: slices.Clone(s.EgressAllow),
			Repos:       reposToControl(s.Repos),
		},
		SetupHash:           s.SetupHash,
		PoolID:              installPool,
		RunnerID:            control.RunnerID(s.Runner),
		PlacementGeneration: 1,
		IdempotencyKey:      s.IdempotencyKey,
		ChildExitCode:       s.ChildExitCode,
		Error:               s.Error,
		CreatedAt:           s.CreatedAt,
		UpdatedAt:           s.UpdatedAt,
		LastEventAt:         s.LastEventAt,
	}
}

// sessionFromControl lowers a control session back onto the store's columns.
// The image lands in whichever of the two image columns the row's kind calls
// for: a session started from an environment has its image *resolved* (the
// environment's image, or its cached snapshot), while a scratch session has
// only the caller's own (store.go, Session.effectiveImage).
func sessionFromControl(c control.Session) Session {
	s := Session{
		ID:             string(c.ID),
		OwnerID:        string(c.CreatorID),
		Name:           c.Name,
		Cmd:            slices.Clone(c.Spec.Cmd),
		EgressAllow:    slices.Clone(c.Spec.EgressAllow),
		State:          SessionState(c.State),
		Runner:         string(c.RunnerID),
		IdempotencyKey: c.IdempotencyKey,
		Error:          c.Error,
		EnvironmentID:  string(c.EnvironmentID),
		SetupHash:      c.SetupHash,
		Repos:          reposFromControl(c.Spec.Repos),
		ChildExitCode:  c.ChildExitCode,
		CreatedAt:      c.CreatedAt,
		UpdatedAt:      c.UpdatedAt,
		LastEventAt:    c.LastEventAt,
	}
	if c.EnvironmentID != "" {
		s.ResolvedImage = c.Spec.Image
	} else {
		s.Image = c.Spec.Image
	}
	return s
}

// sessionsToControl converts a whole page or listing.
func sessionsToControl(rows []Session) []control.Session {
	if rows == nil {
		return nil
	}
	out := make([]control.Session, len(rows))
	for i, row := range rows {
		out[i] = sessionToControl(row)
	}
	return out
}

// reposToControl and reposFromControl preserve the nil-vs-empty distinction
// the override depends on: nil means "inherit the environment's connectors",
// an empty slice means "clone nothing" (store.go, Session.Repos).
func reposToControl(in []RepoRef) []control.RepoRef {
	if in == nil {
		return nil
	}
	out := make([]control.RepoRef, len(in))
	for i, r := range in {
		out[i] = control.RepoRef{Repo: r.Repo, BaseBranch: r.BaseBranch}
	}
	return out
}

func reposFromControl(in []control.RepoRef) []RepoRef {
	if in == nil {
		return nil
	}
	out := make([]RepoRef, len(in))
	for i, r := range in {
		out[i] = RepoRef{Repo: r.Repo, BaseBranch: r.BaseBranch}
	}
	return out
}

// environmentToControl lifts an environment row. control.Environment names no
// runner, so the two things that pin one — the operator's explicit placement
// and the affinity a cached snapshot has to the runner that built it — become
// portable capabilities the pool resolver can match. The snapshot pin is
// emitted only while the snapshot is still current: a snapshot built from
// setup that has since been edited must not hold a session to one runner.
func environmentToControl(e Environment) control.Environment {
	c := control.Environment{
		ID:              control.EnvironmentID(e.ID),
		WorkspaceID:     installWorkspace,
		Name:            e.Name,
		Image:           e.Image,
		Setup:           e.Setup,
		SetupHash:       e.SetupHash,
		Init:            e.Init,
		InitTimeoutSec:  e.InitTimeoutSec,
		EgressAllow:     slices.Clone(e.EgressAllow),
		SecretRefs:      slices.Clone(e.SecretRefs),
		Connectors:      connectorsToControl(e.Connectors),
		SetupTimeoutSec: e.SetupTimeoutSec,
		SnapshotHash:    e.SnapshotHash,
		CreatedAt:       e.CreatedAt,
		UpdatedAt:       e.UpdatedAt,
	}
	if e.SnapshotRef != "" {
		c.Snapshot = control.Checkpoint{
			Ref:          e.SnapshotRef,
			Format:       snapshotCheckpointFormat,
			Capabilities: []string{"workspace"},
		}
	}
	var caps []string
	if e.Placement != "" {
		caps = append(caps, placementCapabilityPrefix+e.Placement)
	}
	if e.SnapshotRef != "" && e.SnapshotRunner != "" && e.SnapshotHash == e.SetupHash {
		caps = append(caps, snapshotCapabilityPrefix+e.SnapshotRunner)
	}
	c.Requirements.Capabilities = caps
	return c
}

// environmentFromControl lowers an environment back onto the store's columns.
// It never writes the three snapshot columns: those are the store's, written
// only by SetEnvironmentSnapshot, so a snapshot built from a superseded setup
// hash stays visibly stale instead of being silently adopted or dropped
// (store.go, Environment). The placement and snapshot capabilities are
// likewise dropped rather than written back — placement is recovered into its
// own column, and O8 has no column for any other requirement.
func environmentFromControl(c control.Environment) Environment {
	return Environment{
		ID:              string(c.ID),
		Name:            c.Name,
		Image:           c.Image,
		Setup:           c.Setup,
		SetupHash:       c.SetupHash,
		Init:            c.Init,
		InitTimeoutSec:  c.InitTimeoutSec,
		EgressAllow:     slices.Clone(c.EgressAllow),
		SecretRefs:      slices.Clone(c.SecretRefs),
		Connectors:      connectorsFromControl(c.Connectors),
		Placement:       capabilityValue(c.Requirements.Capabilities, placementCapabilityPrefix),
		SetupTimeoutSec: c.SetupTimeoutSec,
		CreatedAt:       c.CreatedAt,
		UpdatedAt:       c.UpdatedAt,
	}
}

func connectorsToControl(in []Connector) []control.Connector {
	if in == nil {
		return nil
	}
	out := make([]control.Connector, len(in))
	for i, c := range in {
		out[i] = control.Connector{Type: c.Type, Raw: slices.Clone(c.Raw)}
	}
	return out
}

func connectorsFromControl(in []control.Connector) []Connector {
	if in == nil {
		return nil
	}
	out := make([]Connector, len(in))
	for i, c := range in {
		out[i] = Connector{Type: c.Type, Raw: slices.Clone(c.Raw)}
	}
	return out
}

// capabilityValue returns the tail of the first capability carrying prefix,
// or "" when none does.
func capabilityValue(caps []string, prefix string) string {
	for _, c := range caps {
		if after, ok := strings.CutPrefix(c, prefix); ok {
			return after
		}
	}
	return ""
}

// runnerToControl lifts a runner row. The generation is process-local in O8
// (runnerGenerations), and the two capabilities are synthesized from the name
// so a session pinned to this runner by an environment's placement, or held
// to it by a snapshot it built, matches it.
func runnerToControl(r Runner, gen uint64) control.Runner {
	return control.Runner{
		ID:            control.RunnerID(r.Name),
		PoolID:        installPool,
		CapacityUsed:  r.CapacityUsed,
		CapacityTotal: r.CapacityTotal,
		Connected:     r.Connected,
		Generation:    gen,
		Capabilities:  runnerCapabilities(r.Name),
		LastSeenAt:    r.LastSeenAt,
	}
}

// runnerCapabilities is the pair every runner advertises for its own name.
func runnerCapabilities(name string) []string {
	return []string{placementCapabilityPrefix + name, snapshotCapabilityPrefix + name}
}

// statesFromControl converts a from-list or state filter.
func statesFromControl(in []control.SessionState) []SessionState {
	if in == nil {
		return nil
	}
	out := make([]SessionState, len(in))
	for i, s := range in {
		out[i] = SessionState(s)
	}
	return out
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
// sessions
// ---------------------------------------------------------------------------

// storeSessions is control.SessionRepository over Store.
type storeSessions struct{ st Store }

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
	return sessionToControl(row), nil
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
	return sessionsToControl(rows), next, nil
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

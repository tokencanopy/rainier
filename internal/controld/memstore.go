// internal/controld/memstore.go
package controld

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tokencanopy/rainier/control"
)

// memStore is an in-memory MemStore. Its tenant rows are kept in the control
// model and keyed by the scope that owns them — a session and an environment
// by workspace, a runner by pool — so the three repository ports are views
// over the rows themselves rather than translations of somebody else's
// shape.
//
// One mutex guards every map, and no live pointer ever crosses the lock
// boundary: every method returns value copies, the same discipline as
// internal/runnerd/registry.go's list(). It exists for tests and local
// development; controld's production deployment uses the Postgres-backed
// store instead.
type memStore struct {
	mu sync.Mutex

	workspaces   map[control.WorkspaceID]struct{}
	sessions     map[sessionKey]*control.Session
	environments map[environmentKey]*control.Environment
	// snapshots names, per environment, the runner that built the cached
	// snapshot. It is the store's own index rather than a field on the row
	// because control.Environment names no runner: the affinity leaves here
	// as a portable capability, and only while the cache is still current.
	snapshots map[environmentKey]control.RunnerID
	runners   map[runnerKey]*control.Runner

	// Identity and the vault are the host's, not a tenant's: users and
	// credentials are the installation's operators, and the OSS vault is
	// installation-wide. None of the four is workspace-keyed.
	users         map[string]*User  // by user id
	usersByGitHub map[int64]string  // github id -> user id
	tokens        map[string]string // token hash -> user id
	secrets       map[string]*secretRow
	credentials   map[credKey]*Credential
}

var _ MemStore = (*memStore)(nil)

// sessionKey, environmentKey, and runnerKey are the composite keys the
// contract requires: an id is never authority on its own, so a row is only
// ever reachable through the scope that owns it.
type sessionKey struct {
	ws control.WorkspaceID
	id control.SessionID
}

type environmentKey struct {
	ws control.WorkspaceID
	id control.EnvironmentID
}

type runnerKey struct {
	pool control.PoolID
	id   control.RunnerID
}

// secretRow is one stored secret: the sealed bytes plus the metadata
// ListSecrets is allowed to hand out.
type secretRow struct {
	meta       SecretMeta
	ciphertext []byte
	nonce      []byte
}

// credKey is the credentials table's composite primary key, spelled as a Go
// map key.
type credKey struct {
	userID   string
	provider string
}

// NewMemStore returns a fresh in-memory store with the installation
// workspace already provisioned, so a store from this source is usable
// before New re-asserts it.
func NewMemStore() MemStore {
	return &memStore{
		workspaces:    map[control.WorkspaceID]struct{}{installWorkspace: {}},
		sessions:      map[sessionKey]*control.Session{},
		environments:  map[environmentKey]*control.Environment{},
		snapshots:     map[environmentKey]control.RunnerID{},
		runners:       map[runnerKey]*control.Runner{},
		users:         map[string]*User{},
		usersByGitHub: map[int64]string{},
		tokens:        map[string]string{},
		secrets:       map[string]*secretRow{},
		credentials:   map[credKey]*Credential{},
	}
}

// Sessions, Environments, and Fleet are the three control repository ports,
// each a view over this store's own rows.
func (m *memStore) Sessions() control.SessionRepository { return memSessions{m} }

func (m *memStore) Environments() control.EnvironmentRepository { return memEnvironments{m} }

func (m *memStore) Fleet() control.FleetRepository { return memFleet{m} }

var (
	_ control.SessionRepository     = memSessions{}
	_ control.EnvironmentRepository = memEnvironments{}
	_ control.FleetRepository       = memFleet{}
)

// ---------------------------------------------------------------------------
// clones
// ---------------------------------------------------------------------------

// cloneControlSession returns a deep copy of s. A plain struct copy would
// still share Cmd's, EgressAllow's and Repos's backing arrays — and, worse,
// the ChildExitCode pointer — with the map's own row, so a caller could
// write straight through into the store.
//
// slices.Clone preserves the nil/empty distinction Repos depends on (a nil
// clones to nil, an empty slice to an empty one), which is what keeps "no
// override" and "explicitly no repositories" different answers here as well
// as in Postgres.
func cloneControlSession(s control.Session) control.Session {
	cp := s
	cp.Spec.Cmd = slices.Clone(s.Spec.Cmd)
	cp.Spec.EgressAllow = slices.Clone(s.Spec.EgressAllow)
	cp.Spec.Repos = slices.Clone(s.Spec.Repos)
	if s.ChildExitCode != nil {
		code := *s.ChildExitCode
		cp.ChildExitCode = &code
	}
	return cp
}

// cloneControlEnvironment returns a deep copy of e: every slice, including
// the connectors' raw bytes and both capability lists, is reallocated.
func cloneControlEnvironment(e control.Environment) control.Environment {
	cp := e
	cp.EgressAllow = slices.Clone(e.EgressAllow)
	cp.SecretRefs = slices.Clone(e.SecretRefs)
	cp.Requirements.Capabilities = slices.Clone(e.Requirements.Capabilities)
	cp.Snapshot.Capabilities = slices.Clone(e.Snapshot.Capabilities)
	if e.Connectors != nil {
		cp.Connectors = make([]control.Connector, len(e.Connectors))
		for i, c := range e.Connectors {
			c.Raw = bytes.Clone(c.Raw)
			cp.Connectors[i] = c
		}
	}
	return cp
}

// cloneControlRunner returns a deep copy of r.
func cloneControlRunner(r control.Runner) control.Runner {
	cp := r
	cp.Capabilities = slices.Clone(r.Capabilities)
	return cp
}

// ---------------------------------------------------------------------------
// sessions (control.SessionRepository)
// ---------------------------------------------------------------------------

// memSessions is control.SessionRepository over the store's session rows.
// Every method is the same four steps: check the scope, take the lock, find
// the row, and hand back a clone.
type memSessions struct{ m *memStore }

func (r memSessions) CreateSession(ctx context.Context, ws control.WorkspaceID, s control.Session) (control.Session, error) {
	if ws == "" {
		return control.Session{}, control.ErrInvalid
	}
	m := r.m
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.workspaces[ws]; !ok {
		return control.Session{}, control.ErrNotFound
	}

	// The idempotency check runs first and answers with the row the key
	// already created: a replay is the caller seeing its own earlier answer,
	// not a name collision with itself.
	if s.IdempotencyKey != "" {
		if existing := m.sessionByIdem(ws, s.CreatorID, s.IdempotencyKey); existing != nil {
			return cloneControlSession(*existing), nil
		}
	}
	if s.Name != "" {
		for key, existing := range m.sessions {
			if key.ws == ws && existing.CreatorID == s.CreatorID && existing.Name == s.Name && !existing.State.Terminal() {
				return control.Session{}, control.ErrConflict
			}
		}
	}

	now := time.Now()
	if s.CreatedAt.IsZero() {
		s.CreatedAt = now
	}
	if s.UpdatedAt.IsZero() {
		s.UpdatedAt = now
	}
	if s.LastEventAt.IsZero() {
		s.LastEventAt = now
	}
	s.WorkspaceID = ws
	// Three fields are the row's own history, never the caller's: a create
	// opens the first placement generation, no controller has attached yet,
	// and nothing has exited.
	if s.PlacementGeneration < 1 {
		s.PlacementGeneration = 1
	}
	s.ControllerGeneration = 0
	s.ChildExitCode = nil

	cp := cloneControlSession(s)
	m.sessions[sessionKey{ws, s.ID}] = &cp
	return cloneControlSession(cp), nil
}

func (r memSessions) GetSession(ctx context.Context, ws control.WorkspaceID, id control.SessionID) (control.Session, error) {
	if ws == "" {
		return control.Session{}, control.ErrInvalid
	}
	m := r.m
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[sessionKey{ws, id}]
	if !ok {
		return control.Session{}, control.ErrNotFound
	}
	return cloneControlSession(*s), nil
}

func (r memSessions) SessionByIDem(ctx context.Context, ws control.WorkspaceID, creator control.ActorID, key string) (control.Session, error) {
	if ws == "" {
		return control.Session{}, control.ErrInvalid
	}
	if key == "" {
		return control.Session{}, control.ErrNotFound
	}
	m := r.m
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.sessionByIdem(ws, creator, key)
	if s == nil {
		return control.Session{}, control.ErrNotFound
	}
	return cloneControlSession(*s), nil
}

func (r memSessions) ListSessions(ctx context.Context, ws control.WorkspaceID, q control.SessionQuery) ([]control.Session, string, error) {
	if ws == "" {
		return nil, "", control.ErrInvalid
	}
	m := r.m
	m.mu.Lock()
	rows := make([]control.Session, 0, len(m.sessions))
	for key, s := range m.sessions {
		if key.ws != ws {
			continue
		}
		if !q.IncludeTerminal && s.State.Terminal() {
			continue
		}
		rows = append(rows, cloneControlSession(*s))
	}
	m.mu.Unlock()
	sortSessionsNewestFirst(rows)

	start := 0
	if q.Cursor != "" {
		curNano, curID, err := decodeCursor(q.Cursor)
		if err != nil {
			return nil, "", control.ErrInvalid
		}
		start = len(rows) // a cursor past every row is an empty page
		for i, s := range rows {
			nano := s.CreatedAt.UnixNano()
			if nano < curNano || (nano == curNano && string(s.ID) < curID) {
				start = i
				break
			}
		}
	}
	page, next := pageOf(rows[start:], q.Limit, func(s control.Session) string {
		return encodeCursor(s.CreatedAt, string(s.ID))
	})
	return page, next, nil
}

func (r memSessions) Transition(ctx context.Context, ws control.WorkspaceID, id control.SessionID, from []control.SessionState, to control.SessionState, opts control.TransitionOpts) error {
	if ws == "" {
		return control.ErrInvalid
	}
	m := r.m
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[sessionKey{ws, id}]
	if !ok {
		return control.ErrNotFound
	}
	if !slices.Contains(from, s.State) {
		return control.ErrConflict
	}
	s.State = to
	if opts.RunnerID != nil {
		s.RunnerID = *opts.RunnerID
		// A generation is one sandbox on one runner, so it opens exactly when
		// a transition names a runner to run on. Clearing the runner ends a
		// placement; it does not open one.
		if *opts.RunnerID != "" {
			s.PlacementGeneration++
		}
	}
	if opts.Error != nil {
		s.Error = *opts.Error
	}
	now := time.Now()
	s.UpdatedAt = now
	s.LastEventAt = now
	return nil
}

func (r memSessions) SetSessionSetupHash(ctx context.Context, ws control.WorkspaceID, id control.SessionID, hash string) error {
	if ws == "" {
		return control.ErrInvalid
	}
	m := r.m
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[sessionKey{ws, id}]
	if !ok {
		return control.ErrNotFound
	}
	s.SetupHash = hash
	s.UpdatedAt = time.Now()
	return nil
}

func (r memSessions) SetChildExitCode(ctx context.Context, ws control.WorkspaceID, id control.SessionID, code int) error {
	if ws == "" {
		return control.ErrInvalid
	}
	m := r.m
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[sessionKey{ws, id}]
	if !ok {
		return control.ErrNotFound
	}
	// A fresh pointer, never the caller's: the row must not alias anything
	// outside the store.
	s.ChildExitCode = &code
	s.UpdatedAt = time.Now()
	return nil
}

// NextControllerGeneration advances the row's own controller counter. The
// lease is the repository's — durable and shared by every replica — so two
// controllers can never be handed the same authority, whatever process they
// attached through.
func (r memSessions) NextControllerGeneration(ctx context.Context, ws control.WorkspaceID, id control.SessionID) (uint64, error) {
	if ws == "" {
		return 0, control.ErrInvalid
	}
	m := r.m
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[sessionKey{ws, id}]
	if !ok {
		return 0, control.ErrNotFound
	}
	s.ControllerGeneration++
	s.UpdatedAt = time.Now()
	return s.ControllerGeneration, nil
}

// sessionByIdem finds the session creator already created under key in ws.
// The caller holds the lock.
func (m *memStore) sessionByIdem(ws control.WorkspaceID, creator control.ActorID, key string) *control.Session {
	if key == "" {
		return nil
	}
	for k, s := range m.sessions {
		if k.ws == ws && s.CreatorID == creator && s.IdempotencyKey == key {
			return s
		}
	}
	return nil
}

// sortSessionsNewestFirst is the listing order every page comes back in:
// created_at descending, id descending as the tiebreak, so a cursor names
// exactly one position.
func sortSessionsNewestFirst(rows []control.Session) {
	sort.Slice(rows, func(i, j int) bool {
		if !rows[i].CreatedAt.Equal(rows[j].CreatedAt) {
			return rows[i].CreatedAt.After(rows[j].CreatedAt)
		}
		return rows[i].ID > rows[j].ID
	})
}

// pageOf cuts one page out of an already-ordered, already-positioned slice
// and mints the cursor that resumes after it. A limit the rest exactly fills
// still yields a cursor: the store cannot know it is the last page without
// reading one row further, and an extra empty page is cheaper than a dropped
// row.
func pageOf[T any](rest []T, limit int, cursor func(T) string) ([]T, string) {
	end, full := len(rest), false
	if limit > 0 && limit <= len(rest) {
		end, full = limit, true
	}
	page := append([]T{}, rest[:end]...)
	if !full {
		return page, ""
	}
	return page, cursor(page[len(page)-1])
}

// ---------------------------------------------------------------------------
// environments (control.EnvironmentRepository)
// ---------------------------------------------------------------------------

// memEnvironments is control.EnvironmentRepository over the store's
// environment rows.
type memEnvironments struct{ m *memStore }

func (r memEnvironments) CreateEnvironment(ctx context.Context, ws control.WorkspaceID, e control.Environment) (control.Environment, error) {
	if ws == "" {
		return control.Environment{}, control.ErrInvalid
	}
	m := r.m
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.workspaces[ws]; !ok {
		return control.Environment{}, control.ErrNotFound
	}
	key := environmentKey{ws, e.ID}
	if _, ok := m.environments[key]; ok {
		return control.Environment{}, control.ErrConflict
	}
	if m.environmentByName(ws, e.Name) != nil {
		return control.Environment{}, control.ErrConflict
	}

	now := time.Now()
	if e.CreatedAt.IsZero() {
		e.CreatedAt = now
	}
	if e.UpdatedAt.IsZero() {
		e.UpdatedAt = now
	}
	e.WorkspaceID = ws
	e.Requirements.Capabilities = StripSnapshotCapabilities(e.Requirements.Capabilities)
	// A brand-new environment has no cached snapshot, whatever the caller
	// passed — only SetEnvironmentSnapshot writes these.
	e.Snapshot, e.SnapshotHash = control.Checkpoint{}, ""

	cp := cloneControlEnvironment(e)
	m.environments[key] = &cp
	delete(m.snapshots, key)
	return m.environmentView(key, cp), nil
}

func (r memEnvironments) GetEnvironment(ctx context.Context, ws control.WorkspaceID, id control.EnvironmentID) (control.Environment, error) {
	if ws == "" {
		return control.Environment{}, control.ErrInvalid
	}
	m := r.m
	m.mu.Lock()
	defer m.mu.Unlock()
	key := environmentKey{ws, id}
	e, ok := m.environments[key]
	if !ok {
		return control.Environment{}, control.ErrNotFound
	}
	return m.environmentView(key, *e), nil
}

func (r memEnvironments) ListEnvironments(ctx context.Context, ws control.WorkspaceID, q control.EnvironmentQuery) ([]control.Environment, string, error) {
	if ws == "" {
		return nil, "", control.ErrInvalid
	}
	m := r.m
	m.mu.Lock()
	rows := make([]control.Environment, 0, len(m.environments))
	for key, e := range m.environments {
		if key.ws != ws {
			continue
		}
		rows = append(rows, m.environmentView(key, *e))
	}
	m.mu.Unlock()

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Name != rows[j].Name {
			return rows[i].Name < rows[j].Name
		}
		return rows[i].ID < rows[j].ID
	})

	start := 0
	if q.Cursor != "" {
		curName, curID, err := decodeEnvCursor(q.Cursor)
		if err != nil {
			return nil, "", control.ErrInvalid
		}
		start = len(rows)
		for i, e := range rows {
			if e.Name > curName || (e.Name == curName && e.ID > curID) {
				start = i
				break
			}
		}
	}
	page, next := pageOf(rows[start:], q.Limit, func(e control.Environment) string {
		return encodeEnvCursor(e.Name, e.ID)
	})
	return page, next, nil
}

// UpdateEnvironment replaces the caller-owned columns and carries the cache
// forward untouched. The snapshot is the store's: an update may move the
// setup hash — that is how a cache goes stale — but it may not write, clear,
// or hijack the cache itself.
func (r memEnvironments) UpdateEnvironment(ctx context.Context, ws control.WorkspaceID, e control.Environment) (control.Environment, error) {
	if ws == "" {
		return control.Environment{}, control.ErrInvalid
	}
	m := r.m
	m.mu.Lock()
	defer m.mu.Unlock()
	key := environmentKey{ws, e.ID}
	cur, ok := m.environments[key]
	if !ok {
		return control.Environment{}, control.ErrNotFound
	}
	if held := m.environmentByName(ws, e.Name); held != nil && held.ID != e.ID {
		return control.Environment{}, control.ErrConflict
	}

	upd := cloneControlEnvironment(e)
	upd.WorkspaceID = ws
	upd.CreatedAt = cur.CreatedAt
	upd.Snapshot = cloneControlEnvironment(*cur).Snapshot
	upd.SnapshotHash = cur.SnapshotHash
	upd.Requirements.Capabilities = StripSnapshotCapabilities(upd.Requirements.Capabilities)
	upd.UpdatedAt = time.Now()

	m.environments[key] = &upd
	return m.environmentView(key, upd), nil
}

func (r memEnvironments) DeleteEnvironment(ctx context.Context, ws control.WorkspaceID, id control.EnvironmentID) error {
	if ws == "" {
		return control.ErrInvalid
	}
	m := r.m
	m.mu.Lock()
	defer m.mu.Unlock()
	key := environmentKey{ws, id}
	if _, ok := m.environments[key]; !ok {
		return control.ErrNotFound
	}
	delete(m.environments, key)
	delete(m.snapshots, key)
	return nil
}

func (r memEnvironments) CountSessionsByEnvironment(ctx context.Context, ws control.WorkspaceID, envID control.EnvironmentID, states []control.SessionState) (int, error) {
	if ws == "" {
		return 0, control.ErrInvalid
	}
	m := r.m
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for key, s := range m.sessions {
		if key.ws != ws || s.EnvironmentID != envID {
			continue
		}
		if len(states) > 0 && !slices.Contains(states, s.State) {
			continue
		}
		n++
	}
	return n, nil
}

// SetEnvironmentSnapshot is a compare-and-set on the setup hash: a snapshot
// built from setup that has since been edited must not land, and neither
// must one for an environment that is gone.
func (r memEnvironments) SetEnvironmentSnapshot(ctx context.Context, ws control.WorkspaceID, envID control.EnvironmentID, expectHash, ref string, runnerID control.RunnerID) error {
	if ws == "" {
		return control.ErrInvalid
	}
	m := r.m
	m.mu.Lock()
	defer m.mu.Unlock()
	key := environmentKey{ws, envID}
	e, ok := m.environments[key]
	if !ok {
		return control.ErrNotFound
	}
	if e.SetupHash != expectHash {
		return control.ErrStale
	}
	e.Snapshot = SnapshotCheckpoint(ref)
	e.SnapshotHash = expectHash
	e.UpdatedAt = time.Now()
	m.snapshots[key] = runnerID
	return nil
}

// environmentView is the row as callers see it: a clone, plus the affinity
// capability the cached snapshot lends the environment while it is still
// current. A snapshot whose hash no longer matches the environment's is
// stale, and a stale snapshot must not hold a session to one runner. The
// caller holds the lock.
func (m *memStore) environmentView(key environmentKey, e control.Environment) control.Environment {
	c := cloneControlEnvironment(e)
	holder := m.snapshots[key]
	if c.Snapshot.Ref != "" && holder != "" && c.SnapshotHash == c.SetupHash {
		c.Requirements.Capabilities = append(c.Requirements.Capabilities, SnapshotCapability(holder))
	}
	return c
}

// environmentByName finds ws's environment called name, or nil. The caller
// holds the lock.
func (m *memStore) environmentByName(ws control.WorkspaceID, name string) *control.Environment {
	if name == "" {
		return nil
	}
	for key, e := range m.environments {
		if key.ws == ws && e.Name == name {
			return e
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// fleet (control.FleetRepository)
// ---------------------------------------------------------------------------

// memFleet is control.FleetRepository over the store's runner rows and the
// pool half of its session rows.
type memFleet struct{ m *memStore }

// UpsertRunner fences on the stored generation: a write from a superseded
// connection changes nothing at all, rather than half-overwriting the
// current one's view of its own capacity.
func (r memFleet) UpsertRunner(ctx context.Context, pool control.PoolID, run control.Runner) error {
	if pool == "" {
		return control.ErrInvalid
	}
	m := r.m
	m.mu.Lock()
	defer m.mu.Unlock()
	key := runnerKey{pool, run.ID}
	if cur, ok := m.runners[key]; ok && cur.Generation > run.Generation {
		return control.ErrStale
	}
	cp := cloneControlRunner(run)
	cp.PoolID = pool
	m.runners[key] = &cp
	return nil
}

func (r memFleet) SetRunnerConnected(ctx context.Context, pool control.PoolID, id control.RunnerID, connected bool) error {
	if pool == "" {
		return control.ErrInvalid
	}
	m := r.m
	m.mu.Lock()
	defer m.mu.Unlock()
	run, ok := m.runners[runnerKey{pool, id}]
	if !ok {
		return control.ErrNotFound
	}
	run.Connected = connected
	run.LastSeenAt = time.Now()
	return nil
}

func (r memFleet) ListRunners(ctx context.Context, pool control.PoolID) ([]control.Runner, error) {
	if pool == "" {
		return nil, control.ErrInvalid
	}
	m := r.m
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]control.Runner, 0, len(m.runners))
	for key, run := range m.runners {
		if key.pool != pool {
			continue
		}
		out = append(out, cloneControlRunner(*run))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (r memFleet) SessionsOnRunner(ctx context.Context, pool control.PoolID, id control.RunnerID, states []control.SessionState) ([]control.Session, error) {
	if pool == "" {
		return nil, control.ErrInvalid
	}
	m := r.m
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]control.Session, 0)
	for _, s := range m.sessions {
		if s.PoolID != pool || s.RunnerID != id {
			continue
		}
		if len(states) > 0 && !slices.Contains(states, s.State) {
			continue
		}
		out = append(out, cloneControlSession(*s))
	}
	return out, nil
}

func (r memFleet) OldestQueued(ctx context.Context, pool control.PoolID) ([]control.Session, error) {
	if pool == "" {
		return nil, control.ErrInvalid
	}
	m := r.m
	m.mu.Lock()
	out := make([]control.Session, 0)
	for _, s := range m.sessions {
		if s.PoolID != pool || s.State != control.StateQueued {
			continue
		}
		out = append(out, cloneControlSession(*s))
	}
	m.mu.Unlock()

	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}

// ---------------------------------------------------------------------------
// host lookups
// ---------------------------------------------------------------------------

// EnsureWorkspace makes ws exist. It is a statement of fact rather than a
// create: New calls it on every start for the installation workspace.
func (m *memStore) EnsureWorkspace(ctx context.Context, ws control.WorkspaceID) error {
	if ws == "" {
		return control.ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workspaces[ws] = struct{}{}
	return nil
}

func (m *memStore) EnvironmentByName(ctx context.Context, ws control.WorkspaceID, name string) (control.EnvironmentID, error) {
	if ws == "" {
		return "", control.ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.environmentByName(ws, name)
	if e == nil {
		return "", control.ErrNotFound
	}
	return e.ID, nil
}

func (m *memStore) SnapshotRunner(ctx context.Context, ws control.WorkspaceID, id control.EnvironmentID) (control.RunnerID, error) {
	if ws == "" {
		return "", control.ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := environmentKey{ws, id}
	if _, ok := m.environments[key]; !ok {
		return "", control.ErrNotFound
	}
	return m.snapshots[key], nil
}

func (m *memStore) NextRunnerGeneration(ctx context.Context, pool control.PoolID, id control.RunnerID) (uint64, error) {
	if pool == "" || id == "" {
		return 0, control.ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := runnerKey{pool, id}
	run, ok := m.runners[key]
	if !ok {
		run = &control.Runner{ID: id, PoolID: pool}
		m.runners[key] = run
	}
	run.Generation++
	return run.Generation, nil
}

// ---------------------------------------------------------------------------
// identity and the vault
// ---------------------------------------------------------------------------

func (m *memStore) UpsertUser(ctx context.Context, githubID int64, login, role string) (User, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if id, ok := m.usersByGitHub[githubID]; ok {
		u := m.users[id]
		u.Login = login
		u.Role = role
		return *u, nil
	}
	u := &User{
		ID:        NewUserID(),
		GitHubID:  githubID,
		Login:     login,
		Role:      role,
		CreatedAt: time.Now(),
	}
	m.users[u.ID] = u
	m.usersByGitHub[githubID] = u.ID
	return *u, nil
}

func (m *memStore) InsertToken(ctx context.Context, userID, tokenHash string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tokens[tokenHash] = userID
	return nil
}

func (m *memStore) GetUser(ctx context.Context, id string) (User, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	u, ok := m.users[id]
	if !ok {
		return User{}, control.ErrNotFound
	}
	return *u, nil
}

func (m *memStore) UserByToken(ctx context.Context, tokenHash string) (User, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	userID, ok := m.tokens[tokenHash]
	if !ok {
		return User{}, control.ErrNotFound
	}
	u, ok := m.users[userID]
	if !ok {
		return User{}, control.ErrNotFound
	}
	return *u, nil
}

func (m *memStore) PutSecret(ctx context.Context, name string, ciphertext, nonce []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	row, ok := m.secrets[name]
	if !ok {
		row = &secretRow{meta: SecretMeta{Name: name, CreatedAt: now}}
		m.secrets[name] = row
	}
	row.ciphertext = bytes.Clone(ciphertext)
	row.nonce = bytes.Clone(nonce)
	row.meta.UpdatedAt = now
	return nil
}

func (m *memStore) ListSecrets(ctx context.Context) ([]SecretMeta, error) {
	m.mu.Lock()
	out := make([]SecretMeta, 0, len(m.secrets))
	for _, row := range m.secrets {
		out = append(out, row.meta)
	}
	m.mu.Unlock()

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (m *memStore) GetSecret(ctx context.Context, name string) ([]byte, []byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	row, ok := m.secrets[name]
	if !ok {
		return nil, nil, control.ErrNotFound
	}
	return bytes.Clone(row.ciphertext), bytes.Clone(row.nonce), nil
}

func (m *memStore) DeleteSecret(ctx context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.secrets[name]; !ok {
		return control.ErrNotFound
	}
	delete(m.secrets, name)
	return nil
}

// cloneCredential returns a deep copy of c: the four sealed byte slices and
// the ExpiresAt pointer are all reallocated, so nothing a caller holds can
// reach back into the store's row (or, worse, be mutated underneath another
// caller mid-mint).
func cloneCredential(c Credential) Credential {
	cp := c
	cp.Ciphertext = bytes.Clone(c.Ciphertext)
	cp.Nonce = bytes.Clone(c.Nonce)
	cp.RefreshCiphertext = bytes.Clone(c.RefreshCiphertext)
	cp.RefreshNonce = bytes.Clone(c.RefreshNonce)
	if c.ExpiresAt != nil {
		exp := *c.ExpiresAt
		cp.ExpiresAt = &exp
	}
	return cp
}

func (m *memStore) UpsertCredential(ctx context.Context, c Credential) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	if c.Status == "" {
		c.Status = CredentialValid
	}
	if c.ObtainedAt.IsZero() {
		c.ObtainedAt = now
	}
	if c.LastVerifiedAt.IsZero() {
		c.LastVerifiedAt = now
	}
	if c.LastUsedAt.IsZero() {
		c.LastUsedAt = now
	}
	if c.UpdatedAt.IsZero() {
		c.UpdatedAt = now
	}

	// A whole-row replace: an upsert is a fresh login, so nothing survives
	// from the credential it supersedes.
	cp := cloneCredential(c)
	m.credentials[credKey{c.UserID, c.Provider}] = &cp
	return nil
}

func (m *memStore) GetCredential(ctx context.Context, userID, provider string) (Credential, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.credentials[credKey{userID, provider}]
	if !ok {
		return Credential{}, control.ErrNotFound
	}
	return cloneCredential(*c), nil
}

func (m *memStore) SetCredentialStatus(ctx context.Context, userID, provider, status string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.credentials[credKey{userID, provider}]
	if !ok {
		return control.ErrNotFound
	}
	c.Status = status
	c.UpdatedAt = time.Now()
	return nil
}

func (m *memStore) TouchCredentialUsed(ctx context.Context, userID, provider string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.credentials[credKey{userID, provider}]
	if !ok {
		return control.ErrNotFound
	}
	// last_used_at only: updated_at is the edit clock, and a mint is a read.
	c.LastUsedAt = time.Now()
	return nil
}

func (m *memStore) ListCredentials(ctx context.Context, userID string) ([]Credential, error) {
	m.mu.Lock()
	out := make([]Credential, 0)
	for key, c := range m.credentials {
		if key.userID != userID {
			continue
		}
		out = append(out, cloneCredential(*c))
	}
	m.mu.Unlock()

	sort.Slice(out, func(i, j int) bool { return out[i].Provider < out[j].Provider })
	return out, nil
}

// ---------------------------------------------------------------------------
// cursors
// ---------------------------------------------------------------------------

// encodeCursor and decodeCursor implement the session listing's opaque page
// cursor: base64 raw-URL encoding of "<created_at unixnano>|<id>". Rows are
// ordered created_at desc, id desc, so "the row after the cursor" is the
// first row strictly less than (curNano, curID) under that same ordering.
func encodeCursor(createdAt time.Time, id string) string {
	raw := fmt.Sprintf("%d|%s", createdAt.UnixNano(), id)
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeCursor(cursor string) (createdAtNano int64, id string, err error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, "", fmt.Errorf("controld: invalid cursor: %w", err)
	}
	nanoStr, id, ok := strings.Cut(string(raw), "|")
	if !ok {
		return 0, "", fmt.Errorf("controld: invalid cursor: %q", cursor)
	}
	nano, err := strconv.ParseInt(nanoStr, 10, 64)
	if err != nil {
		return 0, "", fmt.Errorf("controld: invalid cursor: %w", err)
	}
	return nano, id, nil
}

// encodeEnvCursor and decodeEnvCursor are the environment listing's cursor:
// base64 raw-URL encoding of "<id>|<name>". Rows are ordered (name, id)
// ascending; the id leads the encoding because an environment id never
// contains a "|" and a name may, so the split stays unambiguous.
func encodeEnvCursor(name string, id control.EnvironmentID) string {
	return base64.RawURLEncoding.EncodeToString([]byte(string(id) + "|" + name))
}

func decodeEnvCursor(cursor string) (name string, id control.EnvironmentID, err error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return "", "", fmt.Errorf("controld: invalid cursor: %w", err)
	}
	rawID, rawName, ok := strings.Cut(string(raw), "|")
	if !ok {
		return "", "", fmt.Errorf("controld: invalid cursor: %q", cursor)
	}
	return rawName, control.EnvironmentID(rawID), nil
}

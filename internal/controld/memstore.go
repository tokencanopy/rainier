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
)

// memStore is an in-memory Store. It holds one mutex guarding seven maps and
// never leaks a live pointer across the lock boundary — every method
// returns value copies, same discipline as internal/runnerd/registry.go's
// list(). It exists for tests and local development; controld's production
// deployment uses the Postgres-backed Store instead.
type memStore struct {
	mu sync.Mutex

	users         map[string]*User  // by user id
	usersByGitHub map[int64]string  // github id -> user id
	tokens        map[string]string // token hash -> user id
	sessions      map[string]*Session
	runners       map[string]*Runner
	environments  map[string]*Environment
	secrets       map[string]*secretRow // by secret name
	credentials   map[credKey]*Credential
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

// NewMemStore returns a fresh in-memory Store.
func NewMemStore() Store {
	return &memStore{
		users:         map[string]*User{},
		usersByGitHub: map[int64]string{},
		tokens:        map[string]string{},
		sessions:      map[string]*Session{},
		runners:       map[string]*Runner{},
		environments:  map[string]*Environment{},
		secrets:       map[string]*secretRow{},
		credentials:   map[credKey]*Credential{},
	}
}

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
		return User{}, ErrNotFound
	}
	return *u, nil
}

func (m *memStore) UserByToken(ctx context.Context, tokenHash string) (User, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	userID, ok := m.tokens[tokenHash]
	if !ok {
		return User{}, ErrNotFound
	}
	u, ok := m.users[userID]
	if !ok {
		return User{}, ErrNotFound
	}
	return *u, nil
}

// cloneSession returns a deep copy of s. A plain struct copy would still
// share Cmd's, EgressAllow's and Repos's backing arrays — and, worse, the
// ChildExitCode pointer — with the map's own row, so a caller could write
// straight through into the store. Every session that crosses the mutex is
// cloned instead, same discipline as cloneEnvironment.
//
// slices.Clone preserves the nil/empty distinction Repos depends on (a nil
// clones to nil, an empty slice to an empty one), which is what keeps "no
// override" and "explicitly no repositories" different answers here as well
// as in Postgres.
func cloneSession(s Session) Session {
	cp := s
	cp.Cmd = slices.Clone(s.Cmd)
	cp.EgressAllow = slices.Clone(s.EgressAllow)
	cp.Repos = slices.Clone(s.Repos)
	if s.ChildExitCode != nil {
		code := *s.ChildExitCode
		cp.ChildExitCode = &code
	}
	return cp
}

func (m *memStore) CreateSession(ctx context.Context, s Session) (Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if s.IdempotencyKey != "" {
		for _, existing := range m.sessions {
			if existing.OwnerID == s.OwnerID && existing.IdempotencyKey == s.IdempotencyKey {
				return Session{}, ErrIdemReplay
			}
		}
	}
	if s.Name != "" {
		for _, existing := range m.sessions {
			if existing.OwnerID == s.OwnerID && existing.Name == s.Name && !existing.State.Terminal() {
				return Session{}, ErrConflict
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
	// child_exit_code is the store's, not the caller's: nothing has exited at
	// create time, and only SetChildExitCode ever writes it.
	s.ChildExitCode = nil
	cp := cloneSession(s)
	m.sessions[s.ID] = &cp
	return cloneSession(cp), nil
}

func (m *memStore) GetSession(ctx context.Context, id string) (Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[id]
	if !ok {
		return Session{}, ErrNotFound
	}
	return cloneSession(*s), nil
}

func (m *memStore) SessionByIdem(ctx context.Context, ownerID, key string) (Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if key == "" {
		return Session{}, ErrNotFound
	}
	for _, s := range m.sessions {
		if s.OwnerID == ownerID && s.IdempotencyKey == key {
			return cloneSession(*s), nil
		}
	}
	return Session{}, ErrNotFound
}

func (m *memStore) SessionByName(ctx context.Context, ownerID, name string) (Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if name == "" {
		return Session{}, ErrNotFound
	}
	for _, s := range m.sessions {
		if s.OwnerID == ownerID && s.Name == name && !s.State.Terminal() {
			return cloneSession(*s), nil
		}
	}
	return Session{}, ErrNotFound
}

func (m *memStore) ListSessions(ctx context.Context, q SessionQuery) ([]Session, string, error) {
	m.mu.Lock()
	all := make([]Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		all = append(all, cloneSession(*s))
	}
	m.mu.Unlock()

	sort.Slice(all, func(i, j int) bool {
		if !all[i].CreatedAt.Equal(all[j].CreatedAt) {
			return all[i].CreatedAt.After(all[j].CreatedAt)
		}
		return all[i].ID > all[j].ID
	})

	var stateSet map[SessionState]bool
	if len(q.States) > 0 {
		stateSet = make(map[SessionState]bool, len(q.States))
		for _, st := range q.States {
			stateSet[st] = true
		}
	}

	filtered := make([]Session, 0, len(all))
	for _, s := range all {
		if !q.IncludeTerminal && s.State.Terminal() {
			continue
		}
		if stateSet != nil && !stateSet[s.State] {
			continue
		}
		if q.Runner != "" && s.Runner != q.Runner {
			continue
		}
		filtered = append(filtered, s)
	}

	start := 0
	if q.Cursor != "" {
		curNano, curID, err := decodeCursor(q.Cursor)
		if err != nil {
			return nil, "", err
		}
		start = len(filtered) // cursor past every row -> empty page
		for i, s := range filtered {
			nano := s.CreatedAt.UnixNano()
			if nano < curNano || (nano == curNano && s.ID < curID) {
				start = i
				break
			}
		}
	}

	rest := filtered[start:]
	end := len(rest)
	full := false
	if q.Limit > 0 && q.Limit <= len(rest) {
		end = q.Limit
		full = true
	}
	page := append([]Session{}, rest[:end]...)

	next := ""
	if full {
		last := page[len(page)-1]
		next = encodeCursor(last.CreatedAt, last.ID)
	}
	return page, next, nil
}

func (m *memStore) SessionsOnRunner(ctx context.Context, runner string, states []SessionState) ([]Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var stateSet map[SessionState]bool
	if len(states) > 0 {
		stateSet = make(map[SessionState]bool, len(states))
		for _, st := range states {
			stateSet[st] = true
		}
	}

	out := make([]Session, 0)
	for _, s := range m.sessions {
		if s.Runner != runner {
			continue
		}
		if stateSet != nil && !stateSet[s.State] {
			continue
		}
		out = append(out, cloneSession(*s))
	}
	return out, nil
}

func (m *memStore) OldestQueued(ctx context.Context) ([]Session, error) {
	m.mu.Lock()
	out := make([]Session, 0)
	for _, s := range m.sessions {
		if s.State == StateQueued {
			out = append(out, cloneSession(*s))
		}
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

func (m *memStore) Transition(ctx context.Context, id string, from []SessionState, to SessionState, opts TransitionOpts) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[id]
	if !ok {
		return ErrNotFound
	}
	if !slices.Contains(from, s.State) {
		return ErrConflict
	}
	s.State = to
	if opts.Runner != nil {
		s.Runner = *opts.Runner
	}
	if opts.Error != nil {
		s.Error = *opts.Error
	}
	now := time.Now()
	s.UpdatedAt = now
	s.LastEventAt = now
	return nil
}

func (m *memStore) SetSessionSetupHash(ctx context.Context, id, hash string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[id]
	if !ok {
		return ErrNotFound
	}
	s.SetupHash = hash
	s.UpdatedAt = time.Now()
	return nil
}

func (m *memStore) SetChildExitCode(ctx context.Context, id string, code int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[id]
	if !ok {
		return ErrNotFound
	}
	// A fresh pointer, never the caller's: the row must not alias anything
	// outside the store.
	s.ChildExitCode = &code
	s.UpdatedAt = time.Now()
	return nil
}

func (m *memStore) UpsertRunner(ctx context.Context, r Runner) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := r
	m.runners[r.Name] = &cp
	return nil
}

func (m *memStore) SetRunnerConnected(ctx context.Context, name string, connected bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.runners[name]
	if !ok {
		return ErrNotFound
	}
	r.Connected = connected
	r.LastSeenAt = time.Now()
	return nil
}

func (m *memStore) ListRunners(ctx context.Context) ([]Runner, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Runner, 0, len(m.runners))
	for _, r := range m.runners {
		out = append(out, *r)
	}
	return out, nil
}

// cloneEnvironment returns a deep copy of e. A plain struct copy would still
// share the slice backing arrays with the map's own row, so a caller who
// appended to a returned EgressAllow could reach back into the store; every
// value that crosses the mutex is cloned instead.
func cloneEnvironment(e Environment) Environment {
	cp := e
	cp.EgressAllow = slices.Clone(e.EgressAllow)
	cp.SecretRefs = slices.Clone(e.SecretRefs)
	if e.Connectors != nil {
		cp.Connectors = make([]Connector, len(e.Connectors))
		for i, c := range e.Connectors {
			c.Raw = bytes.Clone(c.Raw)
			cp.Connectors[i] = c
		}
	}
	return cp
}

func (m *memStore) CreateEnvironment(ctx context.Context, e Environment) (Environment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.environments[e.ID]; ok {
		return Environment{}, ErrConflict
	}
	for _, existing := range m.environments {
		if existing.Name == e.Name {
			return Environment{}, ErrConflict
		}
	}

	now := time.Now()
	if e.CreatedAt.IsZero() {
		e.CreatedAt = now
	}
	if e.UpdatedAt.IsZero() {
		e.UpdatedAt = now
	}
	e.SetupHash = SetupHash(e.Image, e.Setup)
	// A brand-new environment has no cached snapshot, whatever the caller
	// passed — only SetEnvironmentSnapshot writes these.
	e.SnapshotRef, e.SnapshotRunner, e.SnapshotHash = "", "", ""

	cp := cloneEnvironment(e)
	m.environments[e.ID] = &cp
	return cloneEnvironment(cp), nil
}

func (m *memStore) GetEnvironment(ctx context.Context, id string) (Environment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.environments[id]
	if !ok {
		return Environment{}, ErrNotFound
	}
	return cloneEnvironment(*e), nil
}

func (m *memStore) GetEnvironmentByName(ctx context.Context, name string) (Environment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range m.environments {
		if e.Name == name {
			return cloneEnvironment(*e), nil
		}
	}
	return Environment{}, ErrNotFound
}

func (m *memStore) ListEnvironments(ctx context.Context) ([]Environment, error) {
	m.mu.Lock()
	out := make([]Environment, 0, len(m.environments))
	for _, e := range m.environments {
		out = append(out, cloneEnvironment(*e))
	}
	m.mu.Unlock()

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (m *memStore) UpdateEnvironment(ctx context.Context, e Environment) (Environment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cur, ok := m.environments[e.ID]
	if !ok {
		return Environment{}, ErrNotFound
	}
	for id, existing := range m.environments {
		if id != e.ID && existing.Name == e.Name {
			return Environment{}, ErrConflict
		}
	}

	upd := cloneEnvironment(e)
	upd.CreatedAt = cur.CreatedAt
	// The snapshot columns are the store's: an update that moves SetupHash
	// leaves the old snapshot in place, visibly stale, for the caching path
	// to notice and rebuild.
	upd.SnapshotRef, upd.SnapshotRunner, upd.SnapshotHash = cur.SnapshotRef, cur.SnapshotRunner, cur.SnapshotHash
	upd.SetupHash = SetupHash(upd.Image, upd.Setup)
	upd.UpdatedAt = time.Now()

	m.environments[e.ID] = &upd
	return cloneEnvironment(upd), nil
}

func (m *memStore) DeleteEnvironment(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.environments[id]; !ok {
		return ErrNotFound
	}
	delete(m.environments, id)
	return nil
}

func (m *memStore) CountSessionsByEnvironment(ctx context.Context, envID string, states []SessionState) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var stateSet map[SessionState]bool
	if len(states) > 0 {
		stateSet = make(map[SessionState]bool, len(states))
		for _, st := range states {
			stateSet[st] = true
		}
	}

	n := 0
	for _, s := range m.sessions {
		if s.EnvironmentID != envID {
			continue
		}
		if stateSet != nil && !stateSet[s.State] {
			continue
		}
		n++
	}
	return n, nil
}

func (m *memStore) SetEnvironmentSnapshot(ctx context.Context, envID, expectHash, ref, runner string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Compare-and-set: the environment must still be the one this snapshot
	// was built from. A missing environment fails the same way — there is
	// nothing left for the snapshot to belong to.
	e, ok := m.environments[envID]
	if !ok || e.SetupHash != expectHash {
		return ErrConflict
	}
	e.SnapshotRef, e.SnapshotRunner, e.SnapshotHash = ref, runner, expectHash
	e.UpdatedAt = time.Now()
	return nil
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
		return nil, nil, ErrNotFound
	}
	return bytes.Clone(row.ciphertext), bytes.Clone(row.nonce), nil
}

func (m *memStore) DeleteSecret(ctx context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.secrets[name]; !ok {
		return ErrNotFound
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
		return Credential{}, ErrNotFound
	}
	return cloneCredential(*c), nil
}

func (m *memStore) SetCredentialStatus(ctx context.Context, userID, provider, status string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.credentials[credKey{userID, provider}]
	if !ok {
		return ErrNotFound
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
		return ErrNotFound
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

// encodeCursor and decodeCursor implement ListSessions's opaque page
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

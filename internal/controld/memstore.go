// internal/controld/memstore.go
package controld

import (
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

// memStore is an in-memory Store. It holds one mutex guarding four maps and
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
}

// NewMemStore returns a fresh in-memory Store.
func NewMemStore() Store {
	return &memStore{
		users:         map[string]*User{},
		usersByGitHub: map[int64]string{},
		tokens:        map[string]string{},
		sessions:      map[string]*Session{},
		runners:       map[string]*Runner{},
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
	cp := s
	m.sessions[s.ID] = &cp
	return cp, nil
}

func (m *memStore) GetSession(ctx context.Context, id string) (Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[id]
	if !ok {
		return Session{}, ErrNotFound
	}
	return *s, nil
}

func (m *memStore) SessionByIdem(ctx context.Context, ownerID, key string) (Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if key == "" {
		return Session{}, ErrNotFound
	}
	for _, s := range m.sessions {
		if s.OwnerID == ownerID && s.IdempotencyKey == key {
			return *s, nil
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
			return *s, nil
		}
	}
	return Session{}, ErrNotFound
}

func (m *memStore) ListSessions(ctx context.Context, q SessionQuery) ([]Session, string, error) {
	m.mu.Lock()
	all := make([]Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		all = append(all, *s)
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
		out = append(out, *s)
	}
	return out, nil
}

func (m *memStore) OldestQueued(ctx context.Context) ([]Session, error) {
	m.mu.Lock()
	out := make([]Session, 0)
	for _, s := range m.sessions {
		if s.State == StateQueued {
			out = append(out, *s)
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

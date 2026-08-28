// Package controld defines controld's domain types and its Store
// persistence interface. internal/controld/memstore.go and pgstore (a later
// task) both implement Store, and both must pass the contract suite in
// internal/controld/storetest unchanged — that suite, not this file's
// comments, is the source of truth for exact semantics.
package controld

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"
)

// SessionState is a Session's lifecycle state.
type SessionState string

const (
	StateQueued        SessionState = "queued"
	StateCreating      SessionState = "creating"
	StateRunning       SessionState = "running"
	StateSuspendedWarm SessionState = "suspended_warm"
	StateSuspendedCold SessionState = "suspended_cold"
	StateCanceled      SessionState = "canceled"
	StateFailed        SessionState = "failed"
	StateDead          SessionState = "dead"
	StateDestroyed     SessionState = "destroyed"
)

// Terminal reports whether s is one of the states a session never leaves:
// canceled, failed, dead, or destroyed.
func (s SessionState) Terminal() bool {
	switch s {
	case StateCanceled, StateFailed, StateDead, StateDestroyed:
		return true
	}
	return false
}

// OccupiesSlot reports whether a session in state s counts against a
// runner's capacity: creating, running, or suspended_warm.
func (s SessionState) OccupiesSlot() bool {
	switch s {
	case StateCreating, StateRunning, StateSuspendedWarm:
		return true
	}
	return false
}

// NonTerminal lists every non-terminal state, in the order a session
// normally progresses through them. Callers pass it as Transition's
// from-list when any live state should be accepted.
var NonTerminal = []SessionState{StateQueued, StateCreating, StateRunning, StateSuspendedWarm, StateSuspendedCold}

// User is an authenticated controld operator, identified by GitHub account.
type User struct {
	ID        string
	GitHubID  int64
	Login     string
	Role      string
	CreatedAt time.Time
}

// Session is one coding-agent run: its identity, placement, and lifecycle
// state.
type Session struct {
	ID             string
	OwnerID        string
	Name           string
	Image          string
	Cmd            []string
	EgressAllow    []string
	State          SessionState
	Runner         string // runner name; "" until placed
	IdempotencyKey string // "" = none
	Error          string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	LastEventAt    time.Time
}

// Runner is a registered runnerd fleet member and its current capacity.
type Runner struct {
	Name          string
	CapacityUsed  int
	CapacityTotal int
	Connected     bool
	LastSeenAt    time.Time
}

var (
	// ErrNotFound is returned when a lookup by id, token, name, or key
	// finds nothing.
	ErrNotFound = errors.New("not found")
	// ErrConflict is returned when a guarded Transition's from-list
	// doesn't contain the row's current state, or CreateSession's name
	// is already held by another non-terminal session of the same owner.
	ErrConflict = errors.New("conflict")
	// ErrIdemReplay is returned by CreateSession when the owner has
	// already used the given idempotency key.
	ErrIdemReplay = errors.New("idempotent replay")
)

// TransitionOpts carries the columns Transition may update alongside state.
// A nil field leaves that column unchanged.
type TransitionOpts struct {
	Runner *string
	Error  *string
}

// SessionQuery filters and paginates ListSessions.
type SessionQuery struct {
	States          []SessionState
	Runner          string
	IncludeTerminal bool
	Limit           int
	Cursor          string
}

// Store is controld's persistence interface. memstore (this package) and
// pgstore (a later task) both implement it; storetest.RunContract pins the
// semantics both must satisfy identically.
type Store interface {
	UpsertUser(ctx context.Context, githubID int64, login, role string) (User, error)
	InsertToken(ctx context.Context, userID, tokenHash string) error
	UserByToken(ctx context.Context, tokenHash string) (User, error) // touches last_used_at; ErrNotFound

	CreateSession(ctx context.Context, s Session) (Session, error) // ErrConflict (name), ErrIdemReplay (idem key)
	GetSession(ctx context.Context, id string) (Session, error)
	SessionByIdem(ctx context.Context, ownerID, key string) (Session, error)
	SessionByName(ctx context.Context, ownerID, name string) (Session, error) // non-terminal only
	ListSessions(ctx context.Context, q SessionQuery) (rows []Session, next string, err error)
	SessionsOnRunner(ctx context.Context, runner string, states []SessionState) ([]Session, error)
	OldestQueued(ctx context.Context) ([]Session, error) // created_at asc
	// Transition returns ErrConflict if the session's current state isn't
	// in from, ErrNotFound if id doesn't exist. On success it also bumps
	// updated_at and last_event_at.
	Transition(ctx context.Context, id string, from []SessionState, to SessionState, opts TransitionOpts) error

	UpsertRunner(ctx context.Context, r Runner) error // by Name; sets connected/capacity/last_seen
	SetRunnerConnected(ctx context.Context, name string, connected bool) error
	ListRunners(ctx context.Context) ([]Runner, error)
}

// randHex returns n random bytes, hex-encoded (2n hex characters), sourced
// from crypto/rand.
func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand failing means the OS entropy source is broken; there
		// is no sane recovery, and every caller here needs an unguessable
		// id or token, not a zero-value fallback.
		panic("controld: crypto/rand: " + err.Error())
	}
	return hex.EncodeToString(b)
}

// NewSessionID returns a fresh session id: "sess_" + 32 lowercase hex chars
// (16 random bytes).
func NewSessionID() string { return "sess_" + randHex(16) }

// NewUserID returns a fresh user id: "usr_" + 32 lowercase hex chars (16
// random bytes).
func NewUserID() string { return "usr_" + randHex(16) }

// NewToken returns a fresh bearer token ("rnr_" + 64 lowercase hex chars,
// 32 random bytes) and the hex-encoded SHA-256 hash that InsertToken should
// store in its place — the raw token is never persisted.
func NewToken() (token, hash string) {
	tok := "rnr_" + randHex(32)
	return tok, HashToken(tok)
}

// HashToken returns the hex-encoded SHA-256 hash of tok, as stored by
// InsertToken and looked up by UserByToken.
func HashToken(tok string) string {
	h := sha256.Sum256([]byte(tok))
	return hex.EncodeToString(h[:])
}

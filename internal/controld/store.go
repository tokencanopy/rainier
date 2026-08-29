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
	"encoding/json"
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
	EnvironmentID  string // environment this session came from; "" for scratch
	ResolvedImage  string // image actually dispatched (snapshot or env image); "" for scratch
	// SetupHash identifies the build inputs of the setup script this session
	// was actually dispatched with: SetupHash(resolved image, script), pinned
	// by the create dispatch and "" when no script was sent. It is the
	// session's provenance, and the only way controld can tell, when the setup
	// finishes, whether the container it is about to snapshot was built from
	// the environment as it stands NOW — the environment may have been edited
	// while the script ran, and the row carries no other trace of which script
	// that was.
	SetupHash   string
	CreatedAt   time.Time
	UpdatedAt   time.Time
	LastEventAt time.Time
}

// effectiveImage is the image this session actually runs: the one resolution
// settled on for a session started from an environment (its image, or the
// environment's cached snapshot), and the caller's own Image for a scratch
// session, which has no ResolvedImage at all. Every place that needs "which
// image is this" — the dispatched Spec, the client-facing view — asks here,
// so the two can never disagree.
func (s Session) effectiveImage() string {
	if s.ResolvedImage != "" {
		return s.ResolvedImage
	}
	return s.Image
}

// Connector is one entry of an Environment's connectors array: the "type"
// member decoded out, plus the whole original object kept verbatim in Raw.
// The store treats a connector as opaque — it persists Raw as-is and decodes
// Type back out of it; the API layer is what validates a connector's shape
// per type.
type Connector struct {
	Type string          `json:"type"`
	Raw  json.RawMessage `json:"-"` // full original object, stored verbatim
}

// Environment is a named, reusable template a session starts from: the image
// and setup script that define its filesystem, plus the egress, secrets, and
// connectors it may use.
//
// Four fields belong to the store, not to callers. SetupHash is recomputed
// from Image and Setup on every create and update — whatever a caller puts
// there is ignored. The three Snapshot fields are written only by
// SetEnvironmentSnapshot: create leaves them empty and update leaves them
// exactly as they were, so a snapshot built from a superseded SetupHash
// stays visibly stale (SnapshotHash != SetupHash) instead of being silently
// adopted or silently dropped.
type Environment struct {
	ID              string
	Name            string
	Image           string
	Setup           string
	SetupHash       string // sha256(image+"\x00"+setup); store-maintained
	EgressAllow     []string
	SecretRefs      []string
	Connectors      []Connector
	Placement       string // runner name, or "" for any runner
	SetupTimeoutSec int
	SnapshotRef     string // "" until cached
	SnapshotRunner  string // runner that built the cache
	SnapshotHash    string // setup_hash the snapshot was built from
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// SecretMeta is everything about a stored secret except the secret: its name
// and timestamps. Listing secrets returns these, so no listing path can leak
// ciphertext — only GetSecret hands the bytes back.
type SecretMeta struct {
	Name      string
	CreatedAt time.Time
	UpdatedAt time.Time
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
	// SetSessionSetupHash records the identity of the setup script a session
	// was dispatched with (see Session.SetupHash). It is unguarded — the
	// create dispatch writes it once, before the command goes out — and
	// returns ErrNotFound if id doesn't exist.
	SetSessionSetupHash(ctx context.Context, id, hash string) error

	UpsertRunner(ctx context.Context, r Runner) error // by Name; sets connected/capacity/last_seen
	SetRunnerConnected(ctx context.Context, name string, connected bool) error
	ListRunners(ctx context.Context) ([]Runner, error)

	// CreateEnvironment stores e with a freshly computed SetupHash and no
	// snapshot, and returns the stored row. ErrConflict if the name is
	// already held.
	CreateEnvironment(ctx context.Context, e Environment) (Environment, error)
	GetEnvironment(ctx context.Context, id string) (Environment, error)
	GetEnvironmentByName(ctx context.Context, name string) (Environment, error)
	// ListEnvironments returns every environment, name ascending. There are
	// few environments per deployment, so this page is the whole table.
	ListEnvironments(ctx context.Context) ([]Environment, error)
	// UpdateEnvironment replaces the mutable columns of the environment with
	// e.ID, recomputes SetupHash, and leaves created_at and the snapshot
	// columns alone. ErrNotFound if no such id, ErrConflict if e.Name is
	// held by another environment.
	UpdateEnvironment(ctx context.Context, e Environment) (Environment, error)
	DeleteEnvironment(ctx context.Context, id string) error // ErrNotFound
	// CountSessionsByEnvironment counts sessions on envID whose state is in
	// states; an empty states counts every session on the environment.
	CountSessionsByEnvironment(ctx context.Context, envID string, states []SessionState) (int, error)
	// SetEnvironmentSnapshot records a built snapshot against the
	// environment, but only while its SetupHash is still expectHash. A
	// snapshot built from setup that has since been edited must not land, so
	// a hash mismatch — like an environment that no longer exists — returns
	// ErrConflict and changes nothing.
	SetEnvironmentSnapshot(ctx context.Context, envID, expectHash, ref, runner string) error

	// PutSecret stores (or replaces) the sealed value of name. Sealing
	// happens above the store: ciphertext and nonce are opaque bytes here.
	PutSecret(ctx context.Context, name string, ciphertext, nonce []byte) error
	ListSecrets(ctx context.Context) ([]SecretMeta, error) // name asc; never ciphertext
	GetSecret(ctx context.Context, name string) (ciphertext, nonce []byte, err error)
	DeleteSecret(ctx context.Context, name string) error // ErrNotFound
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

// NewEnvironmentID returns a fresh environment id: "env_" + 32 lowercase hex
// chars (16 random bytes).
func NewEnvironmentID() string { return "env_" + randHex(16) }

// SetupHash returns the identity of an environment's build inputs: the
// hex-encoded SHA-256 of image + "\x00" + setup. The NUL separator keeps the
// pair unambiguous, so no image/setup split can collide with another. A
// snapshot is a cache keyed by this hash: it is usable exactly while the
// environment's SetupHash still equals the hash it was built from.
func SetupHash(image, setup string) string {
	h := sha256.Sum256([]byte(image + "\x00" + setup))
	return hex.EncodeToString(h[:])
}

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

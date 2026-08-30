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
	SetupHash string
	// Repos is the session's own `repos` override: the repositories it clones
	// INSTEAD of the ones its environment's github connectors declare. Three
	// states, all distinct and all load-bearing:
	//
	//   - nil: the caller named none, so the environment's connectors decide
	//     (a scratch session with no override clones nothing).
	//   - empty but non-nil: the caller explicitly asked for no clone at all,
	//     which an environment's connectors must not override.
	//   - populated: exactly these, in this order.
	//
	// It is recorded on the row rather than resolved into the Spec at create
	// time because the create only QUEUES the session: the dispatch that turns
	// this into clone instructions happens later, from a row read back out of
	// the store, possibly after a restart and possibly on another replica. The
	// expansion itself (branch names, directories) still happens at dispatch,
	// like every other thing the Spec carries.
	Repos []RepoRef
	// ChildExitCode is the exit status of the session's agent process, once
	// it has exited. It is a pointer because "the child has not exited" and
	// "the child exited 0" are different facts and a plain int cannot tell
	// them apart — a session whose agent finished cleanly still has a live
	// container and a shell the operator can attach to. Only
	// SetChildExitCode writes it; create always leaves it nil.
	ChildExitCode *int
	CreatedAt     time.Time
	UpdatedAt     time.Time
	LastEventAt   time.Time
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

// RepoRef is one entry of a session's `repos` override, in the spelling the
// client sent: "owner/name" plus the branch to clone from. It is deliberately
// the REQUEST's shape and not the resolved one — the session branch and the
// directory a repository lands in are derived at dispatch from the session
// itself, so storing them here would be storing a copy of a derivation.
//
// An empty BaseBranch means the default branch; the API rejects an explicitly
// empty one, so "" here can only ever mean "unset".
type RepoRef struct {
	Repo       string `json:"repo"`
	BaseBranch string `json:"base_branch,omitempty"`
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
//
// Init and InitTimeoutSec are deliberately absent from SetupHash. Setup
// builds the filesystem once and is cached as a snapshot; init runs on every
// session boot, after the code is in place, so editing it changes nothing
// about the image the snapshot holds. Folding init into the hash would throw
// away a whole team's cache for a change that cannot affect it.
type Environment struct {
	ID              string
	Name            string
	Image           string
	Setup           string
	SetupHash       string // sha256(image+"\x00"+setup); store-maintained
	Init            string // per-boot hook, run after setup and clone
	InitTimeoutSec  int
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

// Credential status vocabulary. A credential is either usable as far as
// anyone knows (CredentialValid) or known to have been rejected by the
// provider (CredentialNeedsRefresh) — there is no third state, because the
// vault never calls the provider to find out: it mints optimistically and
// only ever learns from an observed failure.
const (
	CredentialValid        = "valid"
	CredentialNeedsRefresh = "needs_refresh"
)

// Credential is one user's sealed access credential for one provider, keyed
// by (UserID, Provider).
//
// The store never interprets the sealed bytes: Ciphertext/Nonce (and the
// refresh pair, unused in v0) are opaque here exactly as a secret's are, and
// sealing happens above the store. No method on this type, and no error any
// store returns about one, may carry a credential value — a stack trace or a
// log line naming the token would defeat the vault entirely.
type Credential struct {
	UserID, Provider                string
	Ciphertext, Nonce               []byte // sealed access token
	RefreshCiphertext, RefreshNonce []byte // nullable; unused in v0
	Status                          string // CredentialValid | CredentialNeedsRefresh
	Scopes                          string // informational; what the provider said it granted
	ObtainedAt                      time.Time
	ExpiresAt                       *time.Time // nil when the provider named no expiry
	LastVerifiedAt                  time.Time
	LastUsedAt                      time.Time
	UpdatedAt                       time.Time
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
	// GetUser looks a user up by id, for the paths that hold a row rather
	// than a request: the create dispatch turns a session's owner_id into the
	// GitHub login and numeric id its commits are attributed to, long after
	// the bearer token that created it is out of scope. ErrNotFound.
	GetUser(ctx context.Context, id string) (User, error)

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
	// SetChildExitCode records the exit status of a session's agent process
	// (see Session.ChildExitCode). Like SetSessionSetupHash it is unguarded
	// and state-agnostic — the child exiting is an observation, not a
	// lifecycle transition — and returns ErrNotFound if id doesn't exist.
	SetChildExitCode(ctx context.Context, id string, code int) error

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
	//
	// envID must be a REAL environment id. A scratch session carries
	// environment_id "", so passing "" counts every scratch session in the
	// fleet — a number that means nothing here and, used as a delete guard,
	// would refuse a deletion because of sessions that belong to no
	// environment at all. Callers resolve their ref to an id first (see
	// handleDeleteEnvironment).
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

	// UpsertCredential stores (or wholly replaces) the credential for
	// (c.UserID, c.Provider). A re-upsert is a fresh login — new bytes, new
	// scopes, new clocks — so it restamps every zero timestamp with now and
	// keeps nothing from the row it replaces. An empty Status stores
	// CredentialValid, matching the column's own default.
	UpsertCredential(ctx context.Context, c Credential) error
	GetCredential(ctx context.Context, userID, provider string) (Credential, error) // ErrNotFound
	// SetCredentialStatus flips a credential's status (and bumps updated_at),
	// leaving its value, scopes, and other clocks alone. ErrNotFound if the
	// user has no credential for that provider.
	SetCredentialStatus(ctx context.Context, userID, provider, status string) error
	// TouchCredentialUsed stamps last_used_at. It is a read-path write, so it
	// deliberately leaves updated_at alone — that clock belongs to edits.
	// ErrNotFound if there is no such credential.
	TouchCredentialUsed(ctx context.Context, userID, provider string) error
	// ListCredentials returns userID's credentials, provider ascending, and
	// never another user's. The rows carry sealed bytes like any other read;
	// stripping them for a client-facing view is the caller's job.
	ListCredentials(ctx context.Context, userID string) ([]Credential, error)
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

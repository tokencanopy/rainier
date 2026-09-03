// Package controld defines controld's host-owned domain types and its Store
// persistence interface: the host's own persistence (identity, the vault, and
// the lookups the control ports lack) and the three control repository ports.
// Sessions, environments, and runners are control's types, not this package's.
// internal/controld/memstore.go and pgstore both implement Store, and both
// must pass controlapp/repotest and internal/controld/storetest unchanged —
// those suites, not this file's comments, are the source of truth for exact
// semantics.
package controld

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"

	"github.com/tokencanopy/rainier/control"
)

// User is an authenticated controld operator, identified by GitHub account.
type User struct {
	ID        string
	GitHubID  int64
	Login     string
	Role      string
	CreatedAt time.Time
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

// Store is controld's persistence interface: the host's own persistence
// (HostStore) and the three control repository ports (Repositories). There is
// no third surface — sessions, environments, and runners exist once, as
// control types, and every caller reaches them through the ports.
// memstore (this package) and pgstore both implement it; storetest.RunHost
// pins the host half's semantics, controlapp/repotest the ports'.
type Store interface {
	HostStore
	Repositories
}

// HostStore is the persistence the self-hosted host owns beside the control
// repositories: identity (users, bearer tokens), the vault (secrets,
// credentials), and four lookups the control ports deliberately have no
// method for. Like the ports, every method here answers with the control
// sentinel set — control.ErrNotFound for a lookup that finds nothing,
// control.ErrConflict for a name already held — and never leaks SQL, a DSN,
// or a row's contents in an error.
type HostStore interface {
	// EnsureWorkspace makes ws exist; idempotent. New calls it for the
	// installation workspace, the repository contract suite for its two.
	EnsureWorkspace(ctx context.Context, ws control.WorkspaceID) error

	UpsertUser(ctx context.Context, githubID int64, login, role string) (User, error)
	InsertToken(ctx context.Context, userID, tokenHash string) error
	UserByToken(ctx context.Context, tokenHash string) (User, error)
	GetUser(ctx context.Context, id string) (User, error)

	PutSecret(ctx context.Context, name string, ciphertext, nonce []byte) error
	ListSecrets(ctx context.Context) ([]SecretMeta, error)
	GetSecret(ctx context.Context, name string) (ciphertext, nonce []byte, err error)
	DeleteSecret(ctx context.Context, name string) error

	UpsertCredential(ctx context.Context, c Credential) error
	GetCredential(ctx context.Context, userID, provider string) (Credential, error)
	SetCredentialStatus(ctx context.Context, userID, provider, status string) error
	TouchCredentialUsed(ctx context.Context, userID, provider string) error
	ListCredentials(ctx context.Context, userID string) ([]Credential, error)

	// EnvironmentByName resolves a name inside ws to the id the service is
	// then asked for. The name index is a locator, never authority: the
	// caller still fetches through the service, which authorizes.
	EnvironmentByName(ctx context.Context, ws control.WorkspaceID, name string) (control.EnvironmentID, error)
	// SnapshotRunner names the runner that built id's cached snapshot, ""
	// when there is none — stale or not, because the wire has always shown
	// the column. It decides nothing.
	SnapshotRunner(ctx context.Context, ws control.WorkspaceID, id control.EnvironmentID) (control.RunnerID, error)
	// NextRunnerGeneration opens a new generation for id in pool and returns
	// it: 1 for a runner never seen, else one more than stored. It is the
	// only writer of the generation the fleet repository fences on.
	NextRunnerGeneration(ctx context.Context, pool control.PoolID, id control.RunnerID) (uint64, error)
}

// Repositories is the three control repository ports a store offers over its
// own rows. Each accessor is a view, not a copy: the sessions Sessions()
// creates are the sessions Fleet() places, which is what makes one store
// satisfy the whole repository contract (controlapp/repotest).
type Repositories interface {
	Sessions() control.SessionRepository
	Environments() control.EnvironmentRepository
	Fleet() control.FleetRepository
}

// MemStore is the shape the in-memory store has: Store itself, the union of
// the host's own persistence and the three control repositories. It stays as
// its own name because every test helper is written in terms of it.
type MemStore interface {
	Store
}

// snapshotCheckpointFormat is the format every self-hosted environment
// snapshot has: a runner-built container image reference. control.Checkpoint
// carries the format so a later provider can add a second one without
// changing the field.
const snapshotCheckpointFormat = "rainier-runner-v0"

// snapshotCapabilityPrefix is the self-hosted spelling of a snapshot's
// affinity to the runner that built it. It lives beside the helpers that
// write and strip it rather than beside the scope constants, because the
// stores are what put it on an environment and take it back off.
const snapshotCapabilityPrefix = "snapshot:"

// SnapshotCheckpoint is the control spelling of a self-hosted environment
// snapshot: a runner-built image ref, the one format this build has.
func SnapshotCheckpoint(ref string) control.Checkpoint {
	return control.Checkpoint{Ref: ref, Format: snapshotCheckpointFormat, Capabilities: []string{"workspace"}}
}

// SnapshotCapability is the self-hosted spelling of a CURRENT snapshot's
// affinity to the runner that built it: appended to an environment's
// requirements on the way out of a store, stripped on the way in. A later
// plan replaces it with a portable checkpoint locator and deletes it.
func SnapshotCapability(holder control.RunnerID) string {
	return snapshotCapabilityPrefix + string(holder)
}

// StripSnapshotCapabilities returns caps without any snapshot affinity. A
// store owns that capability the way it owns the cache it describes, so a
// caller's copy of it — read back out and written straight in again — is
// dropped rather than persisted.
func StripSnapshotCapabilities(caps []string) []string {
	if caps == nil {
		return nil
	}
	out := make([]string, 0, len(caps))
	for _, c := range caps {
		if strings.HasPrefix(c, snapshotCapabilityPrefix) {
			continue
		}
		out = append(out, c)
	}
	return out
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

package control

import (
	"context"
	"time"
)

// SessionState is a session's lifecycle state. The vocabulary and the
// Terminal/OccupiesSlot predicates are carried byte-for-byte from the
// internal/controld definition so the extracted application service cannot
// drift from today's behavior; the extraction lanes remove the internal copy
// after this freeze.
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

// Terminal reports whether s is out of the schedulable lifecycle and hidden
// from the default list: canceled, failed, dead, or destroyed.
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
// normally progresses through them. Callers pass it as the from-list of a
// guarded transition when any live state should be accepted.
var NonTerminal = []SessionState{StateQueued, StateCreating, StateRunning, StateSuspendedWarm, StateSuspendedCold}

// TransitionOpts carries the columns a guarded session transition may update
// alongside state. A nil field leaves that column unchanged.
type TransitionOpts struct {
	RunnerID *RunnerID
	Error    *string
}

// RepoRef is one entry of a session's repository override, in the spelling
// the caller sent: "owner/name" plus the branch to clone from. It is the
// request shape, not the resolved clone instruction (that lives in the
// public runner protocol). An empty BaseBranch means the default branch.
type RepoRef struct {
	Repo       string
	BaseBranch string
}

// PortableSpec is the resolved, provider-neutral execution description of a
// session: what image to boot, what command to run, which egress hosts it
// may reach, and which repositories to clone. It names no VM shape, no size,
// and no provider resource.
type PortableSpec struct {
	Image       string
	Cmd         []string
	EgressAllow []string
	Repos       []RepoRef
}

// isZero reports whether s carries no execution description at all.
func (s PortableSpec) isZero() bool {
	return s.Image == "" && s.Cmd == nil && s.EgressAllow == nil && s.Repos == nil
}

// Session is one coding-agent run as the application service sees it: its
// identity, workspace, creator, lifecycle state, resolved portable execution
// spec, selected pool and runner, monotonic placement generation, idempotency
// key, exit code, and timestamps. It carries no role, email, provider,
// native machine shape, charge, or credential.
type Session struct {
	ID            SessionID
	WorkspaceID   WorkspaceID
	CreatorID     ActorID
	Name          string
	State         SessionState
	EnvironmentID EnvironmentID // "" for a scratch session
	Spec          PortableSpec  // the resolved execution description
	SetupHash     string        // identity of the setup script actually dispatched; "" when none
	PoolID        PoolID
	RunnerID      RunnerID
	// PlacementGeneration is the monotonic generation of this session's
	// current placement. Later hosted fencing rejects a runner event whose
	// generation does not match, without replacing the event shape.
	PlacementGeneration uint64
	IdempotencyKey      string
	ChildExitCode       *int
	Error               string
	CreatedAt           time.Time
	UpdatedAt           time.Time
	LastEventAt         time.Time
}

// CreateSession is the command for CreateSession. It distinguishes a session
// that starts from an environment (EnvironmentID set) from a scratch session
// (Spec set). Exactly one may be set: a session that names an environment and
// a scratch spec together is contradictory and refused.
//
// Repos overrides the repositories the environment's connectors declare.
// Its nil-vs-empty distinction is load-bearing: nil means "the caller said
// nothing, inherit the environment's connectors"; a non-nil empty slice means
// "clone nothing"; a non-empty slice means "clone exactly these, in order".
//
// It does not accept CPU, memory, a VM shape, or a user-selected size:
// environment defaults and host policy derive Requirements, so an ordinary
// session is created without any sizing choice.
type CreateSession struct {
	Name           string
	EnvironmentID  EnvironmentID
	Spec           PortableSpec
	Repos          []RepoRef
	IdempotencyKey string
}

// Validate reports whether c is a coherent create. It returns ErrInvalid when
// an environment and a scratch spec are both set.
func (c CreateSession) Validate() error {
	if c.EnvironmentID != "" && !c.Spec.isZero() {
		return ErrInvalid
	}
	return nil
}

// SessionQuery filters and paginates ListSessions. Limit is capped by the
// implementation; Cursor is opaque to callers; rows come back in stable
// (created_at, id) order; and IncludeTerminal admits the terminal states that
// the default list hides. There is no provider filter: a query cannot name a
// provider, machine type, zone, or native resource.
type SessionQuery struct {
	IncludeTerminal bool
	Limit           int
	Cursor          string
}

// SessionPage is one page of ListSessions. NextCursor is empty on the last
// page.
type SessionPage struct {
	Sessions   []Session
	NextCursor string
}

// DeleteSession is the command for DeleteSession.
type DeleteSession struct {
	ID SessionID
}

// SuspendSession is the command for SuspendSession. Warm selects a warm
// suspend (container paused in place) over a cold one (container stopped,
// state checkpointed).
type SuspendSession struct {
	ID   SessionID
	Warm bool
}

// ResumeSession is the command for ResumeSession.
type ResumeSession struct {
	ID SessionID
}

// SnapshotSession is the command for SnapshotSession.
type SnapshotSession struct {
	ID SessionID
}

// Checkpoint is a portable opaque reference to a session snapshot, plus its
// format and capability metadata. The provider object or image reference
// behind Ref stays inside the checkpoint adapter; this type never names it.
type Checkpoint struct {
	Ref          string
	Format       string
	Capabilities []string
}

// Sessions is the session half of the caller-facing application contract.
// Every method receives a host-created Scope; authorization is an Authorizer
// call made before any state disclosure or side effect.
type Sessions interface {
	CreateSession(context.Context, Scope, CreateSession) (Session, error)
	GetSession(context.Context, Scope, SessionID) (Session, error)
	ListSessions(context.Context, Scope, SessionQuery) (SessionPage, error)
	DeleteSession(context.Context, Scope, DeleteSession) error
	SuspendSession(context.Context, Scope, SuspendSession) (Session, error)
	ResumeSession(context.Context, Scope, ResumeSession) (Session, error)
	SnapshotSession(context.Context, Scope, SnapshotSession) (Checkpoint, error)
}

// Application is the whole caller-facing application contract. It embeds the
// four operation-oriented interfaces and nothing else, so a consumer depends
// on the behaviors it uses rather than on a concrete service type.
type Application interface {
	Sessions
	Environments
	Fleet
	Attachments
}

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
//
// On a CreateSession it is the session's own description, layered over the
// environment the create names, if any. An environment is a template and a
// session is an instance of it: every field that is set overrides the
// environment's, and every field that is unset is inherited. Unset is "" for
// Image and nil for a list; an explicitly empty list means "none".
//
//	Image        unset: the environment's image — its current snapshot when
//	             one is valid — or, on a scratch session, the host's default
//	             image. Set: this image, and no snapshot reuse for this
//	             session, because a snapshot is built from the environment's
//	             own image and setup.
//	Cmd          unset: the image's default command. Set: this command. An
//	             environment carries no command; this is how a session from
//	             one says what to run.
//	EgressAllow  unset: the environment's list. Set: the environment's list
//	             extended by these hosts, in order, without duplicates. An
//	             environment's egress is what it needs to work; a session
//	             adds to it and never silently removes from it.
//	Repos        the repository override; see CreateSession.Repos.
type PortableSpec struct {
	Image       string
	Cmd         []string
	EgressAllow []string
	Repos       []RepoRef
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
	// generation does not match, without replacing the event shape. A
	// repository advances it by one on every Transition whose RunnerID option
	// names a runner, and leaves it unchanged otherwise. A generation is one
	// sandbox on one runner: the scheduler's placement, reconcile's adoption,
	// and a cold resume — which names the row's own runner, because it starts
	// a new sandbox there — each open one; a warm resume unpauses the sandbox
	// it has and opens none.
	PlacementGeneration uint64
	// ControllerGeneration is the monotonic generation of this session's
	// current terminal controller: 0 until a controller has attached, then
	// the value NextControllerGeneration last returned. A viewer attaches
	// under the current value; a controller advances it. Stale input is
	// fenced against it by the attachment plane.
	ControllerGeneration uint64
	IdempotencyKey       string
	ChildExitCode        *int
	Error                string
	CreatedAt            time.Time
	UpdatedAt            time.Time
	LastEventAt          time.Time
}

// CreateSession is the command for CreateSession. EnvironmentID names the
// environment the session starts from, or is empty for a scratch session;
// Spec is the session's own execution description, layered over that
// environment by the rules on PortableSpec. No combination of the two is
// contradictory: a session that names an environment may still choose its
// command, override its image, or reach more hosts, and a scratch session
// that names no image asks the host for its default one. A host that wants
// to forbid an override, or require an image, does so in its own policy —
// this contract says what is possible, not what a given host allows.
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

// Validate reports whether c is well formed. It returns ErrInvalid for a
// repository reference that names no repository. No combination of
// EnvironmentID and Spec is refused, because none is contradictory (see
// PortableSpec).
func (c CreateSession) Validate() error {
	for _, r := range c.Repos {
		if r.Repo == "" {
			return ErrInvalid
		}
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

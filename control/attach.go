package control

import (
	"context"
	"io"

	"github.com/tokencanopy/rainier/protocol/terminal"
	"github.com/tokencanopy/rainier/protocol/workspace"
)

// AttachmentMode names the intent of an attach: a viewer reads a session's
// terminal; a controller also writes to it. Controller authorization and the
// monotonic controller-lease generation are application facts — the
// AttachmentBroker receives the granted generation out of band rather than
// changing the terminal protocol.
type AttachmentMode string

const (
	AttachmentViewer     AttachmentMode = "viewer"
	AttachmentController AttachmentMode = "controller"
)

// InputPolicy names who may type when several terminals are attached to one
// session. It is the HOST's choice, made once where the application is
// composed, and it is not negotiated with any client: a client reads what it
// is told about its own attach and never asks for a policy.
//
//	PolicyShared     every attach that asked to type may type, at the
//	                 session's current generation. Nothing is claimed, no
//	                 generation is advanced on an attach, and no peer is ever
//	                 displaced by another attach's arrival.
//	PolicyExclusive  at most one attached terminal is the controller, taken
//	                 by compare-and-advance on the generation and held under
//	                 a lease. This is the model conditional ownership
//	                 shipped, unchanged.
//
// The empty value means PolicyShared, wherever one appears: shared typing is
// the product default for both composers — the hosted cell gateway and the
// self-hosted controld binary — and a default that every composer has to
// restate is a default in name only. A host that wants exclusive ownership
// names it.
//
// It is deliberately not called AttachmentPolicy. That name belongs to the
// mode-aware AUTHORIZATION seam an application composes ("may this principal
// drive?"), and two types a reader has to disambiguate by package is worse
// than one named for what it decides.
type InputPolicy string

const (
	PolicyShared    InputPolicy = "shared"
	PolicyExclusive InputPolicy = "exclusive"
)

// Shared reports whether p admits several typers, reading the empty value as
// PolicyShared. Every consumer asks this rather than comparing to a constant,
// so the default lives in exactly one place.
func (p InputPolicy) Shared() bool { return p != PolicyExclusive }

// Valid reports whether p is a policy this contract defines. The empty value
// is valid — it is the default — and anything else is a composition error
// worth refusing where the application is built rather than at the first
// attach.
func (p InputPolicy) Valid() bool {
	switch p {
	case "", PolicyShared, PolicyExclusive:
		return true
	}
	return false
}

// Resolved returns p with the empty value spelled out, for a consumer that
// carries the policy onward — an AttachTarget, a status view — rather than
// only asking Shared about it.
func (p InputPolicy) Resolved() InputPolicy {
	if p == "" {
		return PolicyShared
	}
	return p
}

// AttachTerminal is the command for AttachTerminal. Since is the attach
// cursor (terminal.SinceAll for the whole log, 0 for a snapshot of the
// current screen). Mode distinguishes viewer from controller intent.
//
// Negotiated and ExpectedGeneration are the conditional half, and both are
// optional: a command that sets neither has exactly the semantics it had
// before they existed, which is what a client that negotiates nothing sends.
type AttachTerminal struct {
	SessionID SessionID
	Since     uint64
	Mode      AttachmentMode
	// Negotiated reports that this client understands conditional ownership.
	// It changes what CONTROLLER intent means. An unnegotiated controller
	// attach is admitted unconditionally, because a client that cannot be
	// told it is a viewer cannot be made one without breaking it; it is
	// therefore recorded as a take-over, so a negotiated client attached at
	// the same time is fenced and told. A negotiated controller attach claims
	// only when control is actually free, and is admitted as a viewer when it
	// is not.
	Negotiated bool
	// ExpectedGeneration is the generation this client last held, and turns
	// the claim into a conditional one: take control only while the session
	// is still at this generation, and become a viewer otherwise. It is what
	// a reconnecting controller presents, so that a device superseded while
	// it was away comes back honestly instead of taking control from whoever
	// now has it. Zero means "I hold no generation".
	ExpectedGeneration uint64
}

// AttachTarget is the fully resolved binding an AttachmentBroker needs to
// splice a stream to a session: the workspace, session, pool, runner, and the
// placement and controller generations the application granted.
type AttachTarget struct {
	WorkspaceID          WorkspaceID
	SessionID            SessionID
	PoolID               PoolID
	RunnerID             RunnerID
	PlacementGeneration  uint64
	ControllerGeneration uint64
	// Mode is the mode the application actually GRANTED, which is not always
	// the one the command asked for: a controller attach arriving while
	// somebody else holds a live lease is granted AttachmentViewer.
	Mode AttachmentMode
	// Negotiated reports that this client advertised conditional ownership
	// and can therefore be TOLD things: `attached`, `stale`,
	// `control_changed`. A broker sends none of them to a client that
	// negotiated nothing, because it cannot decode them.
	Negotiated bool
	// MayClaim reports that this client may TAKE control mid-attach — what
	// the take-control key does.
	//
	// It is a fact of its own, and deliberately not inferred from Negotiated
	// or from Controller being non-nil, because a host policy can grant
	// viewing without granting driving. Such a client must still be told its
	// mode and its generation, and must still never be admitted a
	// controller; one boolean cannot say both. A broker refuses its claims
	// without leaving the replica; the keeper's own Claim asks the policy
	// again, and that answer is the authority.
	MayClaim bool
	// Policy is the attachment policy this attach was granted under, so a
	// broker fences and announces under the same rule the application
	// granted by. It is the application's value and not a second knob: a
	// plane with a policy of its own is a value a composer has to keep equal
	// to this one, and the failure when they drift is the worst one available
	// — an application that advances the generation on attach while the plane
	// declines to tell the peer it displaced.
	//
	// The empty value reads as PolicyShared, like every other InputPolicy.
	Policy InputPolicy
	// Controller is the live half of the lease — claim, renew, release —
	// bound by the application to this workspace, session and attach. A
	// broker drives a handoff through it and is never handed a repository.
	//
	// It is nil when the attach was not negotiated: there is no lease for a
	// client that cannot be told about one. A negotiated attach carries one
	// whether or not MayClaim is set, because a viewer reads its own
	// generation through it.
	//
	// Two invariants tie the three fields together, and an application that
	// breaks either is describing an attach that cannot exist: Controller is
	// non-nil exactly when Negotiated is set, and MayClaim implies
	// Negotiated. A broker that is handed one anyway resolves it in the
	// direction that costs nobody anything — a keeper without the flag is a
	// composer written before the flag existed and still means what a keeper
	// has always meant, while a claim without a keeper has nothing to
	// exercise and is refused.
	Controller ControllerLeaseKeeper
}

// TerminalStream is the transport adapter over complete terminal protocol
// messages — not a socket. The application authorizes before calling the
// attachment broker and does not log or persist stream messages.
type TerminalStream interface {
	Receive(context.Context) (terminal.ClientMessage, error)
	Send(context.Context, terminal.ServerMessage) error
	Close(error) error
}

// WorkspaceDiff is the command for WorkspaceDiff: the session's per-repository
// diff, straight from the sandbox, bounded by the public workspace contract.
type WorkspaceDiff struct {
	SessionID SessionID
}

// PushWorkspace is the command for PushWorkspace: stream the gzipped tar
// archive at Body into Path inside the session's workspace. Body is bounded
// by the public workspace limits; no second archive or path type is
// introduced.
type PushWorkspace struct {
	SessionID SessionID
	Path      string
	Body      io.Reader
}

// PullWorkspace is the command for PullWorkspace: stream the gzipped tar
// archive of Path out of the session's workspace into Body.
type PullWorkspace struct {
	SessionID SessionID
	Path      string
	Body      io.Writer
}

// Attachments is the attach/workspace half of the caller-facing application
// contract. Terminal and workspace bytes use the public protocol packages;
// this interface references them and never duplicates their message structs.
type Attachments interface {
	AttachTerminal(context.Context, Scope, AttachTerminal, TerminalStream) error
	WorkspaceDiff(context.Context, Scope, WorkspaceDiff) (workspace.DiffAnswer, error)
	PushWorkspace(context.Context, Scope, PushWorkspace) error
	PullWorkspace(context.Context, Scope, PullWorkspace) error
}

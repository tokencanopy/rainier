package controlapp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"slices"
	"sync/atomic"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/workspace"
)

// AttachmentOptions carries the host-supplied dependencies the attachment
// service needs. Each is a narrow control port; there is no catch-all host or
// backend store. A nil Policy is rejected: the mode-aware attachment policy is
// a real seam the self-hosted and Cloud composers map to their own current
// collaboration rules.
type AttachmentOptions struct {
	Authorizer control.Authorizer
	Policy     AttachmentPolicy
	Sessions   control.SessionRepository
	Transport  control.RunnerTransport
	Broker     control.AttachmentBroker
	Events     control.EventRecorder
	Clock      control.Clock
	IDs        control.IDGenerator
	// UnitOfWork is the host's atomicity: an attachment's event is a write
	// like any other and commits inside one Run. The effect it describes is a
	// transport call rather than a store write, so it stays outside the unit
	// — the unit holds every store write the operation makes, which here is
	// the event alone.
	UnitOfWork control.UnitOfWork
	// ExecBroker is the plane an exec attachment is handed to. It is
	// OPTIONAL, and nil is a host that has composed no exec plane rather
	// than a misconfiguration: an exec against such a host answers
	// control.ErrUnsupported, which its route renders as 501, and every
	// other operation on this service is unaffected.
	//
	// Optional rather than required for a reason that outlives this release:
	// this struct is composed by Rainier Cloud as well as by self-hosted
	// controld, and making a new dependency mandatory would break a host's
	// build the moment it took the update, for a capability it may not want.
	ExecBroker ExecBroker
	// InputPolicy is who may type when several terminals are attached to one
	// session: control.PolicyShared (the default, and the empty value) or
	// control.PolicyExclusive. It is read once, here, and carried on every
	// AttachTarget this service hands its broker, so the application and the
	// plane can never disagree about which rule an attach was granted under.
	//
	// A host taking this update without naming a policy gets SHARED, which is
	// a deliberate change of default rather than an oversight: shared typing
	// is the product decision for both composers, and the exclusive model is
	// intact behind the other value.
	InputPolicy control.InputPolicy
	// MaxTransferBytes bounds one push or pull's compressed bytes. Zero means
	// workspace.MaxBytes; a negative value is control.ErrInvalid. Hosts lower
	// it in tests so the overrun path is exercised without streaming the full
	// limit, and a host relaying transfers through its own memory lowers it in
	// production for the same reason it bounds any other relay.
	MaxTransferBytes int64
}

// AttachmentPolicy is the mode-aware attachment authorization seam. The frozen
// control.ActionAttach value alone cannot distinguish a view grant from a
// control grant, so after the generic Authorizer permits ActionAttach the
// service asks this policy whether the validated mode is also permitted for
// the same authoritative resource. Self-hosted maps it to creator/installation
// policy; Cloud maps it to current session collaboration grants.
//
// The context an implementation receives differs by question. For the attach
// itself (admit, and the may-claim question) it is the attaching request's
// context. For a mid-attach CLAIM it carries the VALUES of the context that
// authorized the attach — the attaching user, never the runner whose dial-back
// call is executing the claim — grafted onto the live call's deadline and
// cancellation. An implementation that resolves its caller from the context
// therefore answers about the same principal for every question on one
// attachment; one that logs a request id from the context will see the
// attach's id on a claim, not the claim's.
type AttachmentPolicy interface {
	AuthorizeAttachment(context.Context, control.Scope, control.Resource, control.AttachmentMode) error
}

// AttachmentService implements control.Attachments — AttachTerminal and the
// three bounded workspace operations — and, when a host composed an exec
// plane, controlapp.Execs beside it. It owns the correlation counter, the
// controller-generation fence, and every authorization/readiness/bound check
// before a byte crosses the broker, runner, reader, or writer seam.
type AttachmentService struct {
	auth        control.Authorizer
	policy      AttachmentPolicy
	sessions    control.SessionRepository
	transport   control.RunnerTransport
	broker      control.AttachmentBroker
	events      control.EventRecorder
	clock       control.Clock
	ids         control.IDGenerator
	uow         control.UnitOfWork
	execBroker  ExecBroker
	policyInput control.InputPolicy
	maxTransfer int64
	rpcSeq      atomic.Uint64
}

// NewAttachmentService builds an AttachmentService, rejecting any missing
// dependency with control.ErrInvalid. It holds no lease table and starts no
// goroutine: the controller generation is the session repository's, so it
// survives a restart and is shared by every replica over the same store.
func NewAttachmentService(opts AttachmentOptions) (*AttachmentService, error) {
	switch {
	case opts.Authorizer == nil,
		opts.Policy == nil,
		opts.Sessions == nil,
		opts.Transport == nil,
		opts.Broker == nil,
		opts.Events == nil,
		opts.Clock == nil,
		opts.IDs == nil,
		opts.UnitOfWork == nil,
		!opts.InputPolicy.Valid(),
		opts.MaxTransferBytes < 0:
		return nil, control.ErrInvalid
	}
	maxTransfer := opts.MaxTransferBytes
	if maxTransfer == 0 {
		maxTransfer = workspace.MaxBytes
	}
	return &AttachmentService{
		auth:        opts.Authorizer,
		policy:      opts.Policy,
		sessions:    opts.Sessions,
		transport:   opts.Transport,
		broker:      opts.Broker,
		events:      opts.Events,
		clock:       opts.Clock,
		ids:         opts.IDs,
		uow:         opts.UnitOfWork,
		execBroker:  opts.ExecBroker,
		policyInput: opts.InputPolicy.Resolved(),
		maxTransfer: maxTransfer,
	}, nil
}

// authorizedSession resolves and authorizes one session for action, returning
// a defensive clone. The workspace-keyed session read happens before the
// authorization decision because the authoritative resource (workspace,
// creator) can only come from the stored row, never from actor-supplied
// values; the read itself discloses nothing, and authorization precedes every
// external effect.
func (s *AttachmentService) authorizedSession(ctx context.Context, scope control.Scope,
	id control.SessionID, action control.Action) (control.Session, error) {
	if err := scope.Validate(); err != nil || id == "" {
		return control.Session{}, control.ErrInvalid
	}
	row, err := s.sessions.GetSession(ctx, scope.WorkspaceID, id)
	if err != nil {
		return control.Session{}, portError(err)
	}
	resource := control.Resource{Kind: control.ResourceSession, WorkspaceID: row.WorkspaceID,
		ID: string(row.ID), CreatorID: row.CreatorID}
	if err := s.auth.Authorize(ctx, scope, action, resource); err != nil {
		return control.Session{}, control.ErrDenied
	}
	return cloneAttachmentSession(row), nil
}

// cloneAttachmentSession copies the command, egress, repositories, and
// child-exit pointer so this lane never aliases repository-owned memory.
func cloneAttachmentSession(row control.Session) control.Session {
	row.Spec.Cmd = slices.Clone(row.Spec.Cmd)
	row.Spec.EgressAllow = slices.Clone(row.Spec.EgressAllow)
	row.Spec.Repos = slices.Clone(row.Spec.Repos)
	if row.ChildExitCode != nil {
		v := *row.ChildExitCode
		row.ChildExitCode = &v
	}
	return row
}

// attachable reports whether a session's state permits an attach. A running
// session is attachable; a failed session is attachable only while it retains
// a non-empty runner and the transport reports that runner connected, which
// preserves setup-failure diagnosis. Every other state is refused.
func (s *AttachmentService) attachable(row control.Session) bool {
	switch row.State {
	case control.StateRunning:
		return true
	case control.StateFailed:
		return row.RunnerID != "" && s.transport.Connected(row.PoolID, row.RunnerID)
	default:
		return false
	}
}

// controllerKeeper is control.ControllerLeaseKeeper bound to one attach: one
// workspace, one session, one opaque holder. It is what a broker drives a
// handoff through, and it is the only thing that crosses that seam — no
// repository, no workspace-wide authority, nothing about any other session.
type controllerKeeper struct {
	sessions control.SessionRepository
	clock    control.Clock
	// policy, scope and resource are what a mid-attach claim is authorized
	// against. The attach itself was authorized for the mode it OPENED in,
	// so taking control later is a privilege that has to be asked for on its
	// own — and asked LIVE rather than cached, so a host that revokes a
	// collaboration grant mid-attach is honoured at the next press of the
	// take-control key.
	policy   AttachmentPolicy
	scope    control.Scope
	resource control.Resource
	// identity is the context that AUTHORIZED this attach, captured where
	// the keeper was built and carried for the attach's whole life. It is
	// the answer to "who is asking" on every policy question this keeper
	// asks later, and it has to be captured because by then there is nobody
	// to ask: a mid-attach claim is handled on the RUNNER's dial-back
	// request, which is authenticated as a runner and carries no principal
	// at all, so a policy that resolves its caller from the context would
	// refuse the session's own creator.
	//
	// Values only, and never the runner's — see authorizing. A nil
	// identity is a keeper built without one, which asks exactly as it did
	// before.
	identity context.Context
	ws       control.WorkspaceID
	id       control.SessionID
	holder   string
}

// authorizing returns the context one of this keeper's policy questions is
// asked on, and the release its caller must run. It carries the VALUES of the
// context that authorized the attach, and the deadline and cancellation of
// the call being made now.
//
// Both halves are deliberate. The values are the authorizing context's alone:
// the live call is the runner's dial-back, and a runner must not be able to
// contribute a value to an authorization decision about a user — not even by
// filling a gap the authorizing context leaves. context.WithoutCancel is what
// severs the chain at capture, so nothing on the runner's side is reachable
// from here at all. The deadline and cancellation are the live call's, because
// a policy that is a network call must die with the claim that asked it, and a
// captured context must not be able to keep one alive after the attach is
// gone.
//
// They are grafted on with the context package's own machinery — a deadline
// and an AfterFunc — rather than by overriding Value on a wrapper around the
// live call. The override is three lines shorter and it hides the cancellation
// key the package looks a parent up by, so every cancelable context a host's
// policy derived would cost a goroutine instead of a registration.
//
// The keeper's STORE calls are not routed through this. They are not
// authorization decisions, they are already fenced by the generation, and a
// host repository that reads a request-scoped value (a transaction handle,
// say) must see the context of the call it is actually serving.
//
// A keeper with no captured identity asks on the caller's own context, which
// is what a composer got before this existed. It is the only nil this
// tolerates: a keeper with no policy is a composition error, not a case.
func (k controllerKeeper) authorizing(ctx context.Context) (context.Context, func()) {
	if k.identity == nil {
		return ctx, func() {}
	}
	base, stopTimer := k.identity, context.CancelFunc(func() {})
	if deadline, ok := ctx.Deadline(); ok {
		base, stopTimer = context.WithDeadline(base, deadline)
	}
	out, cancel := context.WithCancelCause(base)
	stop := context.AfterFunc(ctx, func() { cancel(context.Cause(ctx)) })
	// A call that is ALREADY over is cancelled here rather than left to that
	// callback, which the context package runs on a goroutine of its own: a
	// policy asked microseconds later would otherwise see a context that is
	// not cancelled yet, on behalf of a claim nobody is waiting for.
	if ctx.Err() != nil {
		cancel(context.Cause(ctx))
	}
	return out, func() {
		stop()
		cancel(context.Canceled)
		stopTimer()
	}
}

// Claim advances the generation from expected and takes the lease. The two
// statements are safe in this order because the generation is the authority
// and the lease is only the hint: a renew that lands after somebody else's
// claim fails on the generation fence, so a loser cannot install a lease over
// a winner's authority.
//
// The two ways that renew can fail are not the same answer. ErrStale means
// the generation this claim just won has ALREADY been advanced past — the
// claim is a moment old and already lost — and answering it with success
// would tell two devices at once that they have control. Any other failure is
// the store being briefly unusable at a generation that is still this
// attach's: the generation is the authority and the lease is only the hint,
// so the claim stands and the heartbeat installs the lease on its next pass.
func (k controllerKeeper) Claim(ctx context.Context, expected uint64) (uint64, error) {
	authCtx, release := k.authorizing(ctx)
	err := k.policy.AuthorizeAttachment(authCtx, k.scope, k.resource, control.AttachmentController)
	release()
	if err != nil {
		return 0, control.ErrDenied
	}
	return k.claimAuthorized(ctx, expected)
}

// claimAuthorized is the same claim with the policy question already
// answered, for the one caller that has just asked it: the attach-time grant
// of a negotiated CONTROLLER attach, which the service admitted on exactly
// this mode microseconds earlier.
//
// Asking twice is neither free nor harmless. It doubles a Cloud
// collaboration-grant lookup on the attach path, and a policy backend that
// blinks between the two calls would fail the attach ErrDenied — reporting a
// dependency outage to a user as "not authorized to attach to this session"
// — after the door had already said yes.
func (k controllerKeeper) claimAuthorized(ctx context.Context, expected uint64) (uint64, error) {
	gen, err := k.sessions.CompareAndAdvanceControllerGeneration(ctx, k.ws, k.id, expected)
	if err != nil {
		if errors.Is(err, control.ErrStale) {
			return 0, control.ErrStale
		}
		return 0, portError(err)
	}
	if err := k.Renew(ctx, gen); errors.Is(err, control.ErrStale) {
		return 0, control.ErrStale
	}
	return gen, nil
}

// Renew extends the lease under a generation this attach was granted. Its
// ErrStale is how a controller displaced by an attach on another replica
// finds out: nothing pushed it a message, its own heartbeat simply stopped
// being accepted.
//
// It asks the host's policy NOTHING, and neither do Release or State. That is
// what lets them run on the runner's dial-back context — the heartbeat, a
// release on the way out, the read behind a refusal — and it is asserted by a
// test, so that a policy question added to one of them without routing it
// through authorizing() fails rather than silently refusing every heartbeat.
// A claim is the only privilege here; extending a lease the store already
// granted is not a second decision.
func (k controllerKeeper) Renew(ctx context.Context, generation uint64) error {
	err := k.sessions.RenewControllerLease(ctx, k.ws, k.id, control.ControllerLease{
		Generation: generation,
		Holder:     k.holder,
		ExpiresAt:  k.clock.Now().Add(control.ControllerLeaseTTL),
	})
	if err != nil {
		if errors.Is(err, control.ErrStale) {
			return control.ErrStale
		}
		return portError(err)
	}
	return nil
}

// Release gives up control by ADVANCING the generation, which vacates the
// lease and fences everything this attach had already sent in the same step.
// A release that is already stale is still a release: somebody else holds
// control, which is precisely the state the caller was asking for.
func (k controllerKeeper) Release(ctx context.Context, generation uint64) error {
	if generation == 0 {
		return nil
	}
	if _, err := k.sessions.CompareAndAdvanceControllerGeneration(ctx, k.ws, k.id, generation); err != nil {
		if errors.Is(err, control.ErrStale) {
			return nil
		}
		return portError(err)
	}
	return nil
}

// State reads the row's current generation and whether anybody holds a live
// lease on it. It discloses no holder: "somebody has control" is the whole of
// what a client is told about another client.
func (k controllerKeeper) State(ctx context.Context) (uint64, bool, error) {
	row, err := k.sessions.GetSession(ctx, k.ws, k.id)
	if err != nil {
		return 0, false, portError(err)
	}
	return row.ControllerGeneration, control.ControllerLeaseOf(row).Live(k.clock.Now()), nil
}

// grant decides what one attach actually gets: its mode, the generation it
// holds, and the keeper it runs the rest of its life through.
//
// Under control.PolicyShared there is nothing exclusive to take, so nothing is
// claimed: an attach admitted as a viewer is a viewer at the row's current
// generation, and EVERY other attach — negotiated or not — is a controller at
// that same current generation, with no compare-and-advance and no lease. The
// legacy path is covered by the same rule and deliberately: an unnegotiated
// attach is recorded as a take-over under the exclusive policy because a
// client that cannot be told it is a viewer cannot be made one, and under
// shared there is nobody to displace, so advancing the generation would fence
// the other typers for nothing.
//
// A negotiated attach still carries a keeper under either policy — the
// contract's invariant is that Controller is non-nil exactly when Negotiated
// is set, and a viewer reads its own generation through it. What differs is
// that nothing under the shared policy ever calls Claim, Renew or Release on
// it.
//
// Everything from here down is the EXCLUSIVE rule, unchanged.
//
// An UNNEGOTIATED attach behaves exactly as it did before conditional
// ownership existed. A viewer reads the row's current generation; a
// controller advances it unconditionally. That is deliberate rather than
// grandfathered: a client that cannot be told it is a viewer cannot be made
// one without silently breaking it, so a legacy attach is recorded as a
// take-over, and a negotiated client attached at the same time is fenced and
// told — which is a great deal better than two clients typing into one shell.
//
// A NEGOTIATED attach is conditional. A viewer never claims. A controller
// claims while nobody holds a live lease — that is the zero-click case, and
// it is also what brings a controller back after its own connection dropped,
// because the departing attach's release freed the lease on its way out. A
// controller that PRESENTS the generation it last held claims from it even
// under a live lease: presenting a generation is how a device says "resume
// what I had", and how `--take` says "take it", and the only lease that can
// still be live at a generation nobody has advanced past is the one this
// attach is resuming.
//
// A controller that presents a generation somebody has since advanced past
// gets the ordinary rule: viewer if the new holder's lease is live, and
// otherwise a claim, because control that nobody holds is free whoever asks.
// Every refusal lands the attach in AttachmentViewer under the current
// generation — never an error, because "somebody else is typing" is an
// answer, not a failure.
func (s *AttachmentService) grant(ctx context.Context, scope control.Scope, resource control.Resource,
	row control.Session, cmd control.AttachTerminal) (control.AttachmentMode, uint64, control.ControllerLeaseKeeper, error) {
	shared := s.policyInput.Shared()
	if !cmd.Negotiated {
		if cmd.Mode == control.AttachmentViewer {
			return control.AttachmentViewer, row.ControllerGeneration, nil, nil
		}
		if shared {
			return control.AttachmentController, row.ControllerGeneration, nil, nil
		}
		gen, err := s.sessions.NextControllerGeneration(ctx, row.WorkspaceID, row.ID)
		if err != nil {
			if errors.Is(err, control.ErrNotFound) {
				return "", 0, nil, control.ErrNotFound
			}
			return "", 0, nil, portError(err)
		}
		return control.AttachmentController, gen, nil, nil
	}

	holder, err := newHolderID()
	if err != nil {
		return "", 0, nil, err
	}
	// ctx here is AttachTerminal's — the client's own authorized request,
	// the one place in this module where the context still carries the
	// caller. Capturing it is what lets a claim made later, on the runner's
	// dial-back, be authorized against the person who opened the attach.
	// WithoutCancel because only the values are wanted: the stored context
	// carries no Done channel for anything to wait on and no deadline for
	// anything to inherit.
	keeper := controllerKeeper{sessions: s.sessions, clock: s.clock,
		policy: s.policy, scope: scope, resource: resource,
		identity: context.WithoutCancel(ctx),
		ws:       row.WorkspaceID, id: row.ID, holder: holder}
	if cmd.Mode == control.AttachmentViewer {
		return control.AttachmentViewer, row.ControllerGeneration, keeper, nil
	}
	if shared {
		// A typer at the generation the row already carries. Not a claim:
		// every peer is typing under this same generation, so advancing would
		// fence all of them, and the lease is an exclusivity hint there is
		// nothing here to hint at.
		return control.AttachmentController, row.ControllerGeneration, keeper, nil
	}

	// A live lease held at a generation this attach is not presenting is
	// somebody else typing, and the honest answer is to watch and say who
	// has it. Anything else — a vacant lease, an expired one, or the
	// generation this attach is resuming — is a claim, made from the
	// generation just read so that two attaches racing here still produce
	// exactly one winner.
	if control.ControllerLeaseOf(row).Live(s.clock.Now()) && cmd.ExpectedGeneration != row.ControllerGeneration {
		return control.AttachmentViewer, row.ControllerGeneration, keeper, nil
	}
	gen, err := keeper.claimAuthorized(ctx, row.ControllerGeneration)
	switch {
	case err == nil:
		return control.AttachmentController, gen, keeper, nil
	case errors.Is(err, control.ErrStale):
		return s.viewerAfterStaleClaim(ctx, row, keeper)
	default:
		return "", 0, nil, err
	}
}

// viewerAfterStaleClaim re-reads the row so a refused claimant is told the
// generation that actually exists now, rather than the one it lost a race
// from — which is the value it would have to claim from to try again. A read
// that fails falls back to the generation it already had: the attach is a
// viewer either way, and a viewer's generation is information, not authority.
func (s *AttachmentService) viewerAfterStaleClaim(ctx context.Context, row control.Session,
	keeper control.ControllerLeaseKeeper) (control.AttachmentMode, uint64, control.ControllerLeaseKeeper, error) {
	current, err := s.sessions.GetSession(ctx, row.WorkspaceID, row.ID)
	if err != nil {
		return control.AttachmentViewer, row.ControllerGeneration, keeper, nil
	}
	return control.AttachmentViewer, current.ControllerGeneration, keeper, nil
}

// newHolderID mints the opaque identity one attach holds its lease under: 8
// crypto/rand bytes, hex. It is not a user, device, account or session
// identifier and never leaves the control plane — the only thing derived from
// it that any client can see is the boolean "somebody holds control".
//
// A broken entropy source fails the attach HERE, before anything has been
// claimed. Returning an empty holder and letting the renew refuse it would
// mean the generation had already been advanced — displacing whoever held
// control — on the way to reporting the failure.
func newHolderID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", control.ErrUnavailable
	}
	return hex.EncodeToString(b), nil
}

// admit resolves the mode this attach is actually authorized to open in, or
// refuses it. The policy is asked about the mode the client asked for, exactly
// as before; what is new is what happens when it says no to a NEGOTIATED
// controller.
//
// A plain `rainier attach` asks for control — it is zero-click on one laptop,
// which is the case that has always worked — so a principal a host grants
// viewing and not driving was refused outright at the door, and had to know to
// type `--view`. That is the exact principal mayClaim exists for, and
// `grant`'s own rule is that every refusal lands the attach in
// AttachmentViewer and never in an error: "somebody else is typing" is an
// answer, and so is "you may watch this". The client is told `attached, view`
// before it paints a screen, prints the viewing notice, and its take-control
// key is answered "you are still a viewer" without the store being touched,
// because mayClaim asks the controller question separately and gets the same
// no.
//
// An UNNEGOTIATED attach is still refused: it cannot be told it is a viewer,
// so admitting it as one would leave a terminal that silently does not type.
// Hosts whose policy answers both questions the same way — self-hosted
// Rainier, where a caller who may attach may drive — never reach the second
// call at all.
// It also reports whether it ASKED the policy about the controller and was
// refused, so that the separate privilege question below is not paid for a
// second time: a host's policy can be a network call or an audited decision,
// and an answer this attach already has is not worth asking for twice.
func (s *AttachmentService) admit(ctx context.Context, scope control.Scope,
	resource control.Resource, cmd control.AttachTerminal) (control.AttachmentMode, bool, error) {
	if err := s.policy.AuthorizeAttachment(ctx, scope, resource, cmd.Mode); err == nil {
		return cmd.Mode, false, nil
	}
	if !cmd.Negotiated || cmd.Mode != control.AttachmentController {
		return "", true, control.ErrDenied
	}
	if err := s.policy.AuthorizeAttachment(ctx, scope, resource, control.AttachmentViewer); err != nil {
		return "", true, control.ErrDenied
	}
	return control.AttachmentViewer, true, nil
}

// mayClaim reports whether this attach may take control mid-attach, which is
// a second question from whether it may attach at all. An attach is
// authorized for the mode it OPENS in — a view-only principal is a principal
// the host means to admit, and authorizing every negotiated attach as a
// controller locks it out of a session it may perfectly well watch. The
// privilege it might REACH is asked for separately, here and again in
// controllerKeeper.Claim.
//
// A negotiated controller attach was admitted on this very mode a moment ago,
// so it is not asked again here, and neither is the claim the grant below
// makes on its behalf. One that ASKED for control and was admitted as a
// viewer instead (see admit) is not asked again either: the policy refused it
// the controller a moment ago, and that answer is carried down rather than
// paid for twice. An unnegotiated attach has no way to send a claim at all. What every negotiated attach does cost is one policy call per claim it
// makes afterwards — worth knowing for a host whose policy is a network call
// or an audited decision, and deliberate: a cached answer cannot honour a
// grant revoked mid-attach.
func (s *AttachmentService) mayClaim(ctx context.Context, scope control.Scope,
	resource control.Resource, cmd control.AttachTerminal) bool {
	switch {
	case !cmd.Negotiated:
		return false
	case cmd.Mode == control.AttachmentController:
		return true
	default:
		return s.policy.AuthorizeAttachment(ctx, scope, resource, control.AttachmentController) == nil
	}
}

// AttachTerminal authorizes and fences one terminal attach, then hands the
// stream to the broker. Terminal messages stay opaque: the module never reads,
// logs, or persists a stream message.
func (s *AttachmentService) AttachTerminal(ctx context.Context, scope control.Scope,
	cmd control.AttachTerminal, stream control.TerminalStream) error {
	row, err := s.authorizedSession(ctx, scope, cmd.SessionID, control.ActionAttach)
	if err != nil {
		return err
	}
	switch cmd.Mode {
	case control.AttachmentViewer, control.AttachmentController:
	default:
		return control.ErrInvalid
	}
	if stream == nil {
		return control.ErrInvalid
	}
	resource := control.Resource{Kind: control.ResourceSession, WorkspaceID: row.WorkspaceID,
		ID: string(row.ID), CreatorID: row.CreatorID}
	mode, refusedControl, err := s.admit(ctx, scope, resource, cmd)
	if err != nil {
		return err
	}
	// Everything below asks about the mode this attach was ADMITTED in, which
	// is not always the mode it asked for.
	cmd.Mode = mode
	if !s.attachable(row) {
		return control.ErrConflict
	}
	eventID, err := s.newEvent()
	if err != nil {
		return err
	}
	mayClaim := !refusedControl && s.mayClaim(ctx, scope, resource, cmd)
	mode, generation, keeper, err := s.grant(ctx, scope, resource, row, cmd)
	if err != nil {
		return err
	}
	target := control.AttachTarget{
		WorkspaceID:          row.WorkspaceID,
		SessionID:            row.ID,
		PoolID:               row.PoolID,
		RunnerID:             row.RunnerID,
		PlacementGeneration:  row.PlacementGeneration,
		ControllerGeneration: generation,
		Mode:                 mode,
		Policy:               s.policyInput,
		Negotiated:           cmd.Negotiated,
		MayClaim:             mayClaim,
		Controller:           keeper,
	}
	if err := s.broker.Attach(ctx, target, stream); err != nil {
		_ = stream.Close(control.ErrUnavailable)
		return control.ErrUnavailable
	}
	if err := s.uow.Run(ctx, func(ctx context.Context) error {
		return s.record(ctx, eventID, scope, control.ActionAttach, resource, row.PlacementGeneration)
	}); err != nil {
		return err
	}
	return nil
}

// newEvent mints and validates an event ID before any external effect. An
// empty generated ID fails the operation closed (ErrUnavailable) before a
// terminal, runner, reader, or writer seam is touched, honoring the
// IDGenerator contract that returned IDs are non-empty.
func (s *AttachmentService) newEvent() (control.EventID, error) {
	id := s.ids.NewEventID()
	if id == "" {
		return "", control.ErrUnavailable
	}
	return id, nil
}

// record writes one provider-neutral event for an already-accepted operation
// using an ID minted before the operation ran. Callers run it inside a
// UnitOfWork.Run and hand it that unit's ctx, so the event is the whole of
// what the operation commits — the attach/push/pull effect itself is a
// transport call, not a store write, and no unit can roll a delivered byte
// back. A recorder failure is control.ErrUnavailable. Events and errors never
// carry terminal, workspace, or path content.
//
// placementGeneration is the session's, so an attach or a transfer is
// attributed to exactly one placement of the session it touched.
func (s *AttachmentService) record(ctx context.Context, id control.EventID, scope control.Scope,
	action control.Action, resource control.Resource, placementGeneration uint64) error {
	if err := s.events.Record(ctx, control.Event{
		ID:                  id,
		WorkspaceID:         scope.WorkspaceID,
		ActorID:             scope.Actor.ID,
		Action:              action,
		Resource:            resource,
		At:                  s.clock.Now(),
		PlacementGeneration: placementGeneration,
	}); err != nil {
		return control.ErrUnavailable
	}
	return nil
}

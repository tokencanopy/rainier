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
type AttachmentPolicy interface {
	AuthorizeAttachment(context.Context, control.Scope, control.Resource, control.AttachmentMode) error
}

// AttachmentService implements control.Attachments: AttachTerminal and the
// three bounded workspace operations. It owns the correlation counter, the
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
	ws       control.WorkspaceID
	id       control.SessionID
	holder   string
}

var _ control.ControllerLeaseKeeper = controllerKeeper{}

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
func (s *AttachmentService) grant(ctx context.Context, row control.Session,
	cmd control.AttachTerminal) (control.AttachmentMode, uint64, control.ControllerLeaseKeeper, error) {
	if !cmd.Negotiated {
		if cmd.Mode == control.AttachmentViewer {
			return control.AttachmentViewer, row.ControllerGeneration, nil, nil
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
	keeper := controllerKeeper{sessions: s.sessions, clock: s.clock,
		ws: row.WorkspaceID, id: row.ID, holder: holder}
	if cmd.Mode == control.AttachmentViewer {
		return control.AttachmentViewer, row.ControllerGeneration, keeper, nil
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
	gen, err := keeper.Claim(ctx, row.ControllerGeneration)
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

// authorizedAs is the mode an attach must be authorized for, which is not
// always the mode it asked to open in. A negotiated attach can claim control
// at any moment on its own stream — that is what the take-control key is —
// so it is authorized for the privilege it may REACH, not the one it happens
// to start with. A host whose policy answers differently for a viewer and a
// controller would otherwise find `mode=view` a way around it.
func authorizedAs(cmd control.AttachTerminal) control.AttachmentMode {
	if cmd.Negotiated {
		return control.AttachmentController
	}
	return cmd.Mode
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
	if err := s.policy.AuthorizeAttachment(ctx, scope, resource, authorizedAs(cmd)); err != nil {
		return control.ErrDenied
	}
	if !s.attachable(row) {
		return control.ErrConflict
	}
	eventID, err := s.newEvent()
	if err != nil {
		return err
	}
	mode, generation, keeper, err := s.grant(ctx, row, cmd)
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

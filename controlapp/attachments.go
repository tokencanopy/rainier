package controlapp

import (
	"context"
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

// grantGeneration returns the controller generation a target carries: a
// viewer attaches under the row's current value; a controller asks the
// repository to advance it. The generation is the repository's — durable,
// shared by every replica — and this service keeps none of its own.
func (s *AttachmentService) grantGeneration(ctx context.Context, row control.Session, mode control.AttachmentMode) (uint64, error) {
	if mode == control.AttachmentViewer {
		return row.ControllerGeneration, nil
	}
	gen, err := s.sessions.NextControllerGeneration(ctx, row.WorkspaceID, row.ID)
	if err != nil {
		if errors.Is(err, control.ErrNotFound) {
			return 0, control.ErrNotFound
		}
		return 0, portError(err)
	}
	return gen, nil
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
	if err := s.policy.AuthorizeAttachment(ctx, scope, resource, cmd.Mode); err != nil {
		return control.ErrDenied
	}
	if !s.attachable(row) {
		return control.ErrConflict
	}
	eventID, err := s.newEvent()
	if err != nil {
		return err
	}
	generation, err := s.grantGeneration(ctx, row, cmd.Mode)
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

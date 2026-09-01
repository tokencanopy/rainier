// Package controlapp implements the attach/workspace half of the public
// control application contract: authorized terminal attachment behind a
// fenced controller generation, plus bounded workspace diff/push/pull
// orchestration over the public session RPC.
//
// The package is a service, not a transport. It resolves and authorizes a
// session, grants a fenced controller generation, and delegates terminal
// transport to an AttachmentBroker, while workspace operations share one
// private session-RPC implementation over RunnerTransport. It imports no
// HTTP/WebSocket implementation, SQL, Docker, GitHub or cloud SDK, billing
// package, or internal/ package; it never parses a socket, logs a terminal
// message, persists terminal bytes, or duplicates the terminal protocol.
//
// Terminal and workspace bytes stay in the already-public protocol packages
// (github.com/tokencanopy/rainier/protocol/{terminal,runner,workspace}); this
// package references those contracts and never duplicates their message
// structs.
package controlapp

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/tokencanopy/rainier/control"
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
	auth       control.Authorizer
	policy     AttachmentPolicy
	sessions   control.SessionRepository
	transport  control.RunnerTransport
	broker     control.AttachmentBroker
	events     control.EventRecorder
	clock      control.Clock
	ids        control.IDGenerator
	rpcSeq     atomic.Uint64
	leaseMu    sync.Mutex
	controller map[attachmentLeaseKey]uint64
}

// attachmentLeaseKey names one session's controller lease by its authoritative
// workspace plus session, so two workspaces with colliding session IDs never
// share a generation.
type attachmentLeaseKey struct {
	workspace control.WorkspaceID
	session   control.SessionID
}

// NewAttachmentService builds an AttachmentService, rejecting any missing
// dependency with control.ErrInvalid. It initializes the controller map keyed
// by authoritative workspace plus session and starts no goroutine. The
// in-memory lease store is extraction-only and is replaced by durable
// controller generations in the next sequential scope/generation plan.
func NewAttachmentService(opts AttachmentOptions) (*AttachmentService, error) {
	switch {
	case opts.Authorizer == nil,
		opts.Policy == nil,
		opts.Sessions == nil,
		opts.Transport == nil,
		opts.Broker == nil,
		opts.Events == nil,
		opts.Clock == nil,
		opts.IDs == nil:
		return nil, control.ErrInvalid
	}
	return &AttachmentService{
		auth:       opts.Authorizer,
		policy:     opts.Policy,
		sessions:   opts.Sessions,
		transport:  opts.Transport,
		broker:     opts.Broker,
		events:     opts.Events,
		clock:      opts.Clock,
		ids:        opts.IDs,
		controller: make(map[attachmentLeaseKey]uint64),
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
		return control.Session{}, attachmentPortError(err)
	}
	resource := control.Resource{Kind: control.ResourceSession, WorkspaceID: row.WorkspaceID,
		ID: string(row.ID), CreatorID: row.CreatorID}
	if err := s.auth.Authorize(ctx, scope, action, resource); err != nil {
		return control.Session{}, control.ErrDenied
	}
	return cloneAttachmentSession(row), nil
}

// attachmentPortError normalizes a port error to the closed control sentinel
// vocabulary. Context cancellation and deadline propagation are preserved
// (the caller went away, which is not a dependency failure); every other
// adapter failure maps to control.ErrUnavailable.
func attachmentPortError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	switch {
	case errors.Is(err, control.ErrInvalid):
		return control.ErrInvalid
	case errors.Is(err, control.ErrDenied):
		return control.ErrDenied
	case errors.Is(err, control.ErrNotFound):
		return control.ErrNotFound
	case errors.Is(err, control.ErrConflict):
		return control.ErrConflict
	case errors.Is(err, control.ErrStale):
		return control.ErrStale
	case errors.Is(err, control.ErrUnavailable):
		return control.ErrUnavailable
	case errors.Is(err, control.ErrUnsupported):
		return control.ErrUnsupported
	default:
		return control.ErrUnavailable
	}
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

// grantGeneration returns the controller generation a target carries. A viewer
// reads the current value without incrementing it; a controller increments the
// monotonic generation under the lease mutex, refusing uint64 overflow.
func (s *AttachmentService) grantGeneration(ws control.WorkspaceID, id control.SessionID, mode control.AttachmentMode) (uint64, error) {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	key := attachmentLeaseKey{workspace: ws, session: id}
	cur := s.controller[key]
	if mode == control.AttachmentViewer {
		return cur, nil
	}
	if cur == ^uint64(0) {
		return 0, control.ErrUnavailable
	}
	cur++
	s.controller[key] = cur
	return cur, nil
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
	generation, err := s.grantGeneration(row.WorkspaceID, row.ID, cmd.Mode)
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
	s.record(ctx, scope, control.ActionAttach, resource)
	return nil
}

// record writes one provider-neutral event without terminal or workspace
// content. Recording is best-effort: the operation's outcome does not depend
// on the event outbox, which a later persistence plan makes atomic.
func (s *AttachmentService) record(ctx context.Context, scope control.Scope, action control.Action, resource control.Resource) {
	_ = s.events.Record(ctx, control.Event{
		ID:          s.ids.NewEventID(),
		WorkspaceID: scope.WorkspaceID,
		ActorID:     scope.Actor.ID,
		Action:      action,
		Resource:    resource,
		At:          s.clock.Now(),
	})
}

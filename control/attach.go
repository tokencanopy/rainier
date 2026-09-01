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

// AttachTerminal is the command for AttachTerminal. Since is the attach
// cursor (terminal.SinceAll for the whole log, 0 for a snapshot of the
// current screen). Mode distinguishes viewer from controller intent.
type AttachTerminal struct {
	SessionID SessionID
	Since     uint64
	Mode      AttachmentMode
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

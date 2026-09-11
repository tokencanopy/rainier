package controlapp

import (
	"context"
	"path"
	"strings"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/terminal"
	"github.com/tokencanopy/rainier/protocol/workspace"
)

// This file is the application half of `rainier exec`: who may run a command
// inside a live session, what shape a command has to have before it is
// carried anywhere, and the one audit record it leaves.
//
// It is beside AttachmentService rather than inside control.Attachments
// because that interface is frozen: adding a method to it would break every
// implementation of the contract, in this repository and in Rainier Cloud
// alike, for a capability a host may not have a plane for. Execs is a second,
// smaller interface a host composes when it does — and *AttachmentService
// satisfies both, because exec shares the session lookup, the authorizer, the
// readiness rule, the event recorder and the unit of work with attach, and
// two services over the same session repository would be two answers to the
// same question.

// Execs is the exec half of the caller-facing contract: one command, inside
// one live session's sandbox, streamed to one caller.
type Execs interface {
	ExecCommand(context.Context, control.Scope, ExecCommand, control.TerminalStream) error
}

// ExecBroker is the plane seam an exec is handed to, the mirror of
// control.AttachmentBroker for the second kind of attachment. By the time it
// is called every question of authority, readiness and shape has been
// settled; what is left is the replica's own half — park the client, ask the
// session's runner to dial back carrying the spec, require the sandbox's
// `exec_started`, and splice.
//
// It is declared HERE rather than in control for the same reason Execs is:
// the frozen contract gains nothing from a port a host may not implement, and
// a broker satisfies this interface structurally without importing it.
type ExecBroker interface {
	Exec(ctx context.Context, target control.AttachTarget, spec runner.ExecSpec,
		stream control.TerminalStream) error
}

// ExecCommand is the command for ExecCommand. It is the caller's request
// exactly as typed, before the sandbox has validated any of it: the plane
// checks only the shape, because the filesystem the cwd and the log name is
// the sandbox's and nothing else can ask it anything.
type ExecCommand struct {
	SessionID control.SessionID
	Argv      []string
	Cwd       string
	Env       map[string]string
	TTY       bool
	Cols      int
	Rows      int
	// Detach asks for a process that outlives this caller, with its output in
	// Log. Its lifetime is then the SESSION's — it is killed when the session
	// is suspended, stopped or destroyed, and never by a caller disconnecting.
	Detach bool
	Log    string
}

// spec renders the command onto the wire. It is the one place the two shapes
// are mapped, so a field added to either is a compile error here rather than
// a field that silently stops travelling.
func (c ExecCommand) spec() runner.ExecSpec {
	return runner.ExecSpec{
		Argv: c.Argv, Cwd: c.Cwd, Env: c.Env, TTY: c.TTY, Cols: c.Cols, Rows: c.Rows,
		Detach: c.Detach, LogPath: c.Log,
	}
}

// maxExecArgv bounds the number of arguments and the total size of a command
// the plane will carry. It is not a security boundary — the sandbox runs as
// the same user either way — it is the bound that keeps one request from
// being a way to make this process allocate without limit, and it is far
// above any command a person types or a script generates.
const (
	maxExecArgv      = 4096
	maxExecArgvBytes = 1 << 20
	maxExecEnv       = 256
)

// ExecCommand authorizes and shape-checks one exec, then hands the stream to
// the exec broker and records that it happened.
//
// The authority asked for is the CONTROLLER's, live, once, and never from an
// answer cached at some earlier attach: an exec writes into the sandbox and
// runs arbitrary code there, which is exactly what a controller does. A host
// that grants viewing without granting driving refuses exec, and refuses it
// the same way and for the same reason it refuses the take-control key —
// with control.ErrDenied, and never by silently admitting a reduced form,
// because there is no reduced exec.
//
// The generic Authorizer is asked ActionAttach, deliberately not a new verb;
// see control.ActionExec for why the new word is an audit label only.
//
// Unlike attach, this does NOT wait for a session to reach `running`. Its
// caller is a script that wants an answer now, and holding a request open
// turns a fast failure into a slow one — so a session that is not attachable
// is control.ErrConflict, which the route renders as 409 with the state
// named, rather than attach's friendly wait.
func (s *AttachmentService) ExecCommand(ctx context.Context, scope control.Scope,
	cmd ExecCommand, stream control.TerminalStream) error {
	if stream == nil {
		return control.ErrInvalid
	}
	if s.execBroker == nil {
		// This host composed no exec plane. Saying so is honest and is what
		// the route renders as 501; pretending otherwise would leave a caller
		// waiting on a dial-back nobody will make.
		return control.ErrUnsupported
	}
	row, err := s.authorizedSession(ctx, scope, cmd.SessionID, control.ActionAttach)
	if err != nil {
		return err
	}
	resource := control.Resource{Kind: control.ResourceSession, WorkspaceID: row.WorkspaceID,
		ID: string(row.ID), CreatorID: row.CreatorID}
	// The controller question, live, exactly once.
	if err := s.policy.AuthorizeAttachment(ctx, scope, resource,
		control.AttachmentController); err != nil {
		return control.ErrDenied
	}
	if err := ValidateExec(cmd); err != nil {
		return err
	}
	// RUNNING, and only running. Attach also admits a `failed` session whose
	// runner is still connected, because the whole point of that attach is to
	// read the log that says why it failed — a diagnosis, on a sandbox that
	// is still there. An exec into a failed session is not a diagnosis, it is
	// a command run in a session whose boot chain never finished, whose
	// environment was never built and whose repositories may never have been
	// cloned. It is refused as the conflict it is, with the state named.
	if row.State != control.StateRunning {
		return control.ErrConflict
	}
	eventID, err := s.newEvent()
	if err != nil {
		return err
	}

	// An exec is NOT the terminal. It takes no lease, advances no controller
	// generation, is not displaced by a take-over and cannot displace
	// anybody — so it carries no keeper, is not negotiated, and may not
	// claim. The ControllerGeneration it carries is the session's current
	// one, read and never written, which is what an audit of the row would
	// show at this instant and nothing more.
	target := control.AttachTarget{
		WorkspaceID:          row.WorkspaceID,
		SessionID:            row.ID,
		PoolID:               row.PoolID,
		RunnerID:             row.RunnerID,
		PlacementGeneration:  row.PlacementGeneration,
		ControllerGeneration: row.ControllerGeneration,
		Mode:                 control.AttachmentController,
	}
	// Recorded on ACCEPTANCE, which is HERE — before the stream is spliced
	// and not after it is over.
	//
	// The broker holds an exec for its whole life, the way it holds an
	// attach: a build that runs for an hour returns from it an hour from
	// now. Recording afterwards would leave that hour unaudited, and would
	// lose the record entirely if this replica restarted mid-exec — an audit
	// log that only reports the commands that finished while the plane
	// stayed up is not an audit log. So the record says an exec was
	// authorized and dispatched, which is a true thing that is worth knowing
	// even when the sandbox then refused it: "somebody with the authority to
	// run commands here ran one" is exactly the question being asked.
	//
	// The exit code is deliberately not in it: adding it means a second
	// write, and v0 has one.
	if err := s.uow.Run(ctx, func(ctx context.Context) error {
		return s.recordExec(ctx, eventID, scope, resource, row.PlacementGeneration, cmd.Argv)
	}); err != nil {
		return err
	}
	if err := s.execBroker.Exec(ctx, target, cmd.spec(), stream); err != nil {
		_ = stream.Close(err)
		return control.ErrUnavailable
	}
	return nil
}

// recordExec writes the one event an exec leaves: actor, workspace, session,
// placement generation, timestamp, and the command NAME. Never an argument,
// never the environment — not values, not names — never the cwd, never a
// byte or a length of input or output.
func (s *AttachmentService) recordExec(ctx context.Context, id control.EventID, scope control.Scope,
	resource control.Resource, placementGeneration uint64, argv []string) error {
	if err := s.events.Record(ctx, control.Event{
		ID:                  id,
		WorkspaceID:         scope.WorkspaceID,
		ActorID:             scope.Actor.ID,
		Action:              control.ActionExec,
		Resource:            resource,
		At:                  s.clock.Now(),
		PlacementGeneration: placementGeneration,
		Command:             ExecCommandName(argv),
	}); err != nil {
		return control.ErrUnavailable
	}
	return nil
}

// ExecCommandName is what an exec is audited as: the base name of argv[0],
// capped at 64 bytes, and "?" when it is not printable ASCII or names no
// command at all.
//
// It is exported because the rule belongs to the application rather than to
// one transport: a host writing its own audit record for the same operation
// must produce the same string, or two deployments of Rainier would disagree
// about what an audit log is allowed to say.
func ExecCommandName(argv []string) string {
	if len(argv) == 0 {
		return "?"
	}
	name := path.Base(argv[0])
	const maxName = 64
	if name == "" || name == "." || name == ".." ||
		strings.ContainsRune(name, '/') || len(name) > maxName {
		return "?"
	}
	for i := 0; i < len(name); i++ {
		if name[i] < 0x20 || name[i] > 0x7e {
			return "?"
		}
	}
	return name
}

// ValidateExec is the SHAPE check the plane applies before it carries a
// command anywhere. It is deliberately not the whole check: the sandbox
// re-derives every path against the filesystem it actually has, and re-reads
// every environment name, because the last hop before a syscall trusts
// nobody. This is the hop before it, refusing early what can be refused
// without a filesystem.
//
// It is exported so a host's own route can apply the same rule before it
// upgrades a socket, which is the only place a shape error can still be a
// status code.
func ValidateExec(cmd ExecCommand) error {
	if len(cmd.Argv) == 0 || cmd.Argv[0] == "" || len(cmd.Argv) > maxExecArgv {
		return control.ErrInvalid
	}
	total := 0
	for _, arg := range cmd.Argv {
		if strings.ContainsRune(arg, 0) {
			return control.ErrInvalid
		}
		total += len(arg)
	}
	if total > maxExecArgvBytes {
		return control.ErrInvalid
	}
	if len(cmd.Env) > maxExecEnv {
		return control.ErrInvalid
	}
	for name, value := range cmd.Env {
		if name == "" || strings.ContainsRune(name, 0) ||
			strings.ContainsRune(name, '=') || strings.ContainsRune(value, 0) {
			return control.ErrInvalid
		}
	}
	if cmd.Cwd != "" {
		if err := workspace.ValidatePath(cmd.Cwd); err != nil {
			return control.ErrInvalid
		}
	}
	// --log is required with --detach and refused without it. A log for an
	// attached exec would be a second, silent copy of a stream the caller is
	// already reading; a detached run with nowhere for its output to go is
	// the half-feature the design refused to ship.
	switch {
	case cmd.Detach && cmd.Log == "":
		return control.ErrInvalid
	case !cmd.Detach && cmd.Log != "":
		return control.ErrInvalid
	case cmd.Detach:
		if err := workspace.ValidatePath(cmd.Log); err != nil {
			return control.ErrInvalid
		}
	}
	// A size without a terminal is a size for nothing. It is not refused —
	// a client that sends one is harmless — but a terminal without a size
	// would leave the sandbox opening a pty at 0x0, which no program draws
	// on.
	if cmd.TTY && (cmd.Cols <= 0 || cmd.Rows <= 0) {
		return control.ErrInvalid
	}
	return nil
}

// ExecStartMessage reads the one message an exec attachment must open with
// and turns it into a command. The spec travels in the FIRST MESSAGE rather
// than on the URL, and that is a security decision: a URL is written to the
// access log of every proxy between a caller and a cell, so an argv in a URL
// is an argument in a log file — precisely what the audit rule above says is
// never recorded.
//
// It is exported because every host's route needs it and none of them should
// re-derive it: a route that accepted a second message type here, or that
// forwarded a byte of stdin before the spec arrived, would be a different
// protocol wearing the same name.
func ExecStartMessage(m terminal.ClientMessage, id control.SessionID) (ExecCommand, error) {
	if m.Type != terminal.TypeExecStart || m.Exec == nil {
		return ExecCommand{}, control.ErrInvalid
	}
	e := m.Exec
	return ExecCommand{
		SessionID: id,
		Argv:      e.Argv, Cwd: e.Cwd, Env: e.Env,
		TTY: e.TTY, Cols: e.Cols, Rows: e.Rows,
		Detach: e.Detach, Log: e.LogPath,
	}, nil
}

// internal/relay/exec.go
package relay

import (
	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// This file is the relay's whole knowledge of exec: two interfaces and no
// behaviour. The mux learns that a FrameOpen has a KIND and that one of the
// kinds is answered by something other than the session; everything about
// what an exec IS — the spawn, the env rule, the cwd rule, the process group
// — stays in cmd/sessiond, where the sandbox is.
//
// That split is not tidiness. internal/relay is compiled into runnerd and
// into every test that stands a hub up, and an exec runner drags in a pty, a
// process group and a filesystem. The seam keeps them out.

// Execer is the sandbox's exec runner as the relay needs it: one call that
// opens one exec and hands back the live attachment.
//
// It never returns an error. Every way an exec can be refused — a cwd outside
// the workspace, an env name the rule rejects, one exec too many — is a
// `exec_error` the CALLER has to see and map to an exit code, so a refusal is
// a message on the attachment rather than a failure at this seam. An
// attachment that was refused emits its exec_error and closes, which is
// exactly the shape of one that ran.
//
// A nil Execer is a build that cannot exec at all. serveSession answers an
// exec FrameOpen on one by closing the attachment: no `exec_started`, so the
// plane refuses. (It refuses it as "the sandbox did not answer" rather than
// as "this sandbox predates exec" — the two are different words on purpose,
// and a build compiled without an exec runner is neither of the shapes a
// deployed sandbox has. A sessiond that predates the Kind field takes the
// TERMINAL branch instead and answers a snapshot, which is the case that
// actually happens in a fleet and the one the handshake is written for.)
type Execer interface {
	OpenExec(spec runner.ExecSpec) ExecAttachment
}

// ExecAttachment is one live exec, in the same shape session.Attachment has:
// a channel of server messages the relay forwards, a way to deliver the
// client's messages, and a way to end it.
//
// Msgs is CLOSED when the exec is over — after its exec_exit or its
// exec_error — which is what makes the relay's forwarder goroutine exit and
// send the FrameClose that ends the attachment.
//
// Sends on Msgs BLOCK, and that is the whole backpressure design: the relay's
// forwarder is the only consumer, it writes each message to the conn before
// taking the next, and a sandbox that cannot hand over a chunk stops reading
// the process's pipes, so the process blocks on write(2) and nothing is
// dropped. It is the deliberate opposite of session.trySend's force-detach,
// and the reason is the consumer count: a stalled terminal viewer is one of
// many and must not hold the session, while a stalled exec caller is the only
// reader its process will ever have and dropping its output would silently
// corrupt the one answer it asked for.
type ExecAttachment interface {
	Msgs() <-chan terminal.ServerMessage
	// Client delivers one message from the caller: stdin, exec_stdin_eof,
	// resize, exec_signal. It must not block on the process — the relay's
	// demux loop is shared with every other attachment on this conn.
	Client(m terminal.ClientMessage)
	// Close ends the exec. For an attached exec that means killing the
	// process group; a DETACHED exec is already unattached and outlives this
	// call, because its lifetime is the session's rather than its caller's.
	// It is idempotent: the conn's death and an explicit FrameClose can both
	// reach it.
	Close()
}

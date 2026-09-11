// internal/controld/exec.go
package controld

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"slices"

	"github.com/coder/websocket"

	"github.com/tokencanopy/rainier/attachplane"
	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/controlapp"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// ---------------------------------------------------------------------------
// WS GET /v0/sessions/{id}/exec
// ---------------------------------------------------------------------------

// handleClientExec serves the client half of an exec: one command, inside one
// live session's sandbox, streamed back to this caller.
//
// It is its OWN ROUTE rather than a parameter on attach, and that is the
// whole compatibility design. A query parameter is right for negotiating who
// may type, because a plane that predates it ignores it and degrades to
// today's behaviour. It is wrong for "run this": an old plane handed
// `attach?kind=exec` would open an ordinary controller attachment — a
// take-over, displacing whoever was at the keyboard — and the caller's
// command would never run. An old plane answers 404 on a route it does not
// have, which is the honest answer and the one a CLI can act on.
//
// Every failure that can be reported as HTTP is reported BEFORE the upgrade:
// once the socket is a websocket a status code has nowhere to go. The five
// refusals below are the design's status table, in the order they can be
// answered.
//
// It deliberately does not WAIT for a session to reach `running`. Attach
// waits, because a person who just typed `rainier new` is legitimately a few
// seconds early and a friendly wait beats a retry loop in every client.
// Exec's caller is a script that wants an answer now, so a session that is
// not running is a conflict with the resource's current state — 409, with the
// state named so the caller can decide — rather than a request held open.
func (s *Server) handleClientExec(w http.ResponseWriter, r *http.Request, u User) {
	id := r.PathValue("id")

	if !s.mayExec(w, r, u, id) {
		return
	}

	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return // Accept has already answered the request
	}
	defer c.CloseNow()

	// From here every answer is a close reason or a message. The client's
	// opening exec_start carries the command — never the URL, because a URL
	// is written to the access log of every proxy between here and the
	// caller, and an argv in a URL is an argument in a log file.
	// The EXEC stream, not the attach one: an exec caller that has stopped
	// reading backs up the writer every attachment on this session shares, so
	// it is closed sooner than a person watching a screen would be. See
	// attachplane.ExecClientStream.
	stream := attachplane.ExecClientStream(c)
	ctx := withUser(r.Context(), u)
	first, err := attachplane.ExecFirstMessage(ctx, stream)
	if err != nil {
		_ = stream.Close(err)
		return
	}
	cmd, err := controlapp.ExecStartMessage(first, control.SessionID(id))
	if err != nil {
		_ = stream.Close(attachplane.ErrExecFirstMessage)
		return
	}
	// The service re-checks authority and attachability against the
	// authoritative row, shape-checks the command, hands the stream to the
	// exec broker and records the one event this leaves. Its answer stays
	// authoritative; the pre-upgrade checks above only refuse what it would
	// refuse too.
	if err := s.attachments.ExecCommand(ctx, userScope(u), cmd, stream); err != nil {
		// Tagged as an EXEC's refusal, so the close mapping — which is shared
		// with the terminal attach's — can answer it in exec's words without
		// changing what the same error means on the older path.
		_ = stream.Close(attachplane.ExecFailure(err))
	}
}

// mayExec is the pre-upgrade half of the route: the design's status table,
// answered while a status code still has somewhere to go.
//
// The authority it asks for is the CONTROLLER's, and only that. There is no
// reduced exec — a principal a host grants viewing without driving is refused
// outright, the same way and for the same reason the take-control key refuses
// them — so unlike mayAttach there is no "may they watch instead" second
// question. The generic verb is ActionAttach, deliberately not a new one; see
// control.ActionExec.
func (s *Server) mayExec(w http.ResponseWriter, r *http.Request, u User, id string) bool {
	row, err := s.st.Sessions().GetSession(r.Context(), installWorkspace, control.SessionID(id))
	if err != nil {
		if errors.Is(err, control.ErrNotFound) {
			writeErr(w, http.StatusNotFound, "not_found", "session not found")
			return false
		}
		log.Printf("controld: exec %s: %v", clip(id), err)
		writeErr(w, http.StatusInternalServerError, "internal", "could not exec in session")
		return false
	}
	resource := control.Resource{Kind: control.ResourceSession, WorkspaceID: installWorkspace,
		ID: string(row.ID), CreatorID: row.CreatorID}
	ctx := withUser(r.Context(), u)
	if err := (ownerOrAdmin{}).AuthorizeAttachment(ctx, userScope(u), resource,
		control.AttachmentController); err != nil {
		writeErr(w, http.StatusForbidden, "forbidden", "not authorized to run commands in this session")
		return false
	}
	connected := row.RunnerID != "" && s.runnerConnected(string(row.RunnerID))
	if refusal, refused := execReadiness(row, s.attachments.ExecSupported(), connected,
		func() bool { return s.runnerSupportsExec(r.Context(), row.RunnerID) }); refused {
		refusal.write(w)
		return false
	}
	return true
}

// execRefusal is one row of the design's status table: what to answer, and
// the state to name when the answer is a conflict.
type execRefusal struct {
	status  int
	code    string
	message string
	state   control.SessionState
}

func (e execRefusal) write(w http.ResponseWriter) {
	if e.status == http.StatusConflict {
		// The one answer that carries more than the envelope. "Not running"
		// is only actionable if the caller is told WHICH not-running it is: a
		// suspended session is one `rainier resume` away and a destroyed one
		// is not.
		writeJSON(w, http.StatusConflict, execConflict{
			Error: errorBody{Code: e.code, Message: e.message},
			State: string(e.state),
		})
		return
	}
	writeErr(w, e.status, e.code, e.message)
}

// execReadiness is the readiness half of the status table as a pure decision,
// so every row of it is testable without a live fleet — which the last one,
// "the runner cannot forward an exec", otherwise is not: a runnerd built from
// this tree always announces exec.v1, on purpose.
//
// supportsExec is a function rather than a boolean because it is a store
// read, and a caller refused earlier must not pay for one.
//
// hostExec is whether this host composed an exec plane at all. It is answered
// HERE, pre-upgrade, because controlapp's own ErrUnsupported for a nil exec
// broker is reached only after the socket is a websocket — so on such a host
// the promised 501 became a 1008 close, which is a different thing to a
// caller and breaks the table's own rule that every status is answered before
// the upgrade. Self-hosted controld always composes one; the host that does
// not is the stated extension point.
func execReadiness(row control.Session, hostExec, connected bool,
	supportsExec func() bool) (execRefusal, bool) {
	if !hostExec {
		return execRefusal{status: http.StatusNotImplemented, code: "exec_unsupported",
			message: "this server cannot run commands in sessions"}, true
	}
	// Not running: a conflict with the resource's current state, with the
	// state named. A suspended session is deliberately NOT resumed — resuming
	// costs minutes, can fail, and changes what the caller is billed for, so
	// `rainier exec s -- git status` must never be an expensive surprise. A
	// script that means it writes `rainier resume s && rainier exec s -- …`.
	if row.State != control.StateRunning {
		return execRefusal{status: http.StatusConflict, code: "session_not_running",
			message: fmt.Sprintf("session is %s, not running", row.State),
			state:   row.State}, true
	}
	if !connected {
		// 503 rather than attach's 502: the runner is a dependency that is
		// expected back, and 503 is the code a client retries on. A gateway
		// that turns a 502 into a retry is guessing.
		return execRefusal{status: http.StatusServiceUnavailable,
			code: "runner_unreachable", message: "runner is not connected"}, true
	}
	if !supportsExec() {
		return execRefusal{status: http.StatusNotImplemented, code: "exec_unsupported",
			message: "the runner holding this session cannot run commands in it"}, true
	}
	return execRefusal{}, false
}

// execConflict is the 409's body: the standard envelope with the session's
// state added, because "not running" is only actionable if the caller is told
// WHICH not-running it is — a suspended session is one `rainier resume` away
// and a destroyed one is not.
type execConflict struct {
	Error errorBody `json:"error"`
	State string    `json:"state"`
}

// runnerSupportsExec reports whether the runner holding this session
// announced `exec.v1`.
//
// It is a PRE-CHECK and not the fence, and the difference matters. A runner
// forwards the dial-back, but the sandbox is what runs the command — and a
// session keeps the sessiond it booted with for as long as it lives, so a new
// runner can be holding a session whose sandbox has never heard of exec. The
// authoritative fence is the sandbox's own `exec_started`, which the splice
// requires; this only saves the round trip when the answer is already known,
// and turns it into a status code instead of a close reason.
//
// A store that cannot answer yields "supported", which is the safer way to be
// wrong here: the handshake still refuses, one round trip later, where
// refusing on a failed store read would 501 a perfectly good exec because the
// database blinked.
func (s *Server) runnerSupportsExec(ctx context.Context, id control.RunnerID) bool {
	rows, err := s.st.Fleet().ListRunners(ctx, installPool)
	if err != nil {
		log.Printf("controld: exec: listing runners: %v", err)
		return true
	}
	for _, row := range rows {
		if row.ID == id {
			return slices.Contains(row.Capabilities, runner.CapabilityExecV1)
		}
	}
	// The runner is connected (checked above) but has no row yet: the same
	// reasoning as a failed read.
	return true
}

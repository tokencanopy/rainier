package attachplane

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// This file is the plane's half of `rainier exec`: the second KIND of
// attachment over the same dial-back pairing, the same budgets and the same
// splice shape a terminal attach uses — with three differences, each of which
// is the whole reason it is a second kind rather than a flag on the first.
//
//   - What is on the far end is a process this attachment created and owns,
//     not the session's one pty.
//   - Who may write is its own caller, unconditionally. An exec holds no
//     controller lease, advances no generation, is not displaced by a
//     take-over and cannot displace anybody. Nothing here constructs an
//     ownership, stamps a frame, or starts a heartbeat.
//   - Where the output goes is this caller's socket and nowhere else.
//
// The one thing this file adds that the terminal splice does not have is a
// HANDSHAKE, and it exists for a permanent compatibility case rather than a
// transitional one. A session keeps the sessiond it booted with for as long
// as it lives, so "new plane, old sandbox" is not a mixed-version week — it
// is forever, for every session that predates the roll. An old sessiond reads
// a FrameOpen whose Kind it does not know and opens a TERMINAL attachment,
// answering with a snapshot. So a new plane accepts an exec only from a
// sandbox that said `exec_started`, and no byte of the caller's stdin is
// forwarded before it.

// ExecBroker is the exec seam an application hands a stream to. It is the
// mirror of control.AttachmentBroker for the second kind of attachment, and
// it is declared here rather than in control because the frozen contract
// gains nothing from a port a host may not implement; controlapp declares the
// same method set and a broker satisfies it structurally.
type ExecBroker interface {
	Exec(ctx context.Context, target control.AttachTarget, spec runner.ExecSpec,
		stream control.TerminalStream) error
}

// ExecBroker returns the plane behind that seam, for the application's exec
// service to hand authorized streams to.
func (p *Plane) ExecBroker() ExecBroker { return execBroker{p} }

type execBroker struct{ p *Plane }

var _ ExecBroker = execBroker{}

// The two errors an exec socket can be closed with that an attach cannot.
var (
	// errExecUnsupported is the sandbox refusing, or failing to confirm, the
	// exec — the permanent "old sessiond" case above. The client is also
	// SENT an exec_error{unsupported} before the close, because a close
	// reason is a string a CLI has to pattern-match and a message is a field
	// it can switch on.
	errExecUnsupported = errors.New("controld: the session's sandbox does not support exec")
	// errExecEnded is an exec that ran and is over.
	errExecEnded = errors.New("controld: the exec ended")
)

// Exec performs the pairing for one authorized exec. Every exit closes the
// client stream with a reason it can read: the port's contract is that a
// broker either splices the stream or ends it, never both and never neither.
//
// It does NOT read the client's first message. The exec spec is already in
// hand — the application read it, because the command's NAME is a thing it
// audits and the shape is a thing it refuses — so the socket is silent from
// this point until the sandbox speaks, which is exactly the property the
// handshake below needs.
func (b execBroker) Exec(ctx context.Context, target control.AttachTarget,
	spec runner.ExecSpec, stream control.TerminalStream) error {
	p := b.p

	attachID := randHex(8) // 16 hex characters, crypto/rand
	pa := &pendingAttach{stream: stream, exec: true, done: make(chan struct{})}
	// Park before sending: the runner can dial back the instant it reads the
	// command, and an entry that isn't there yet would be refused.
	if !p.attaches.park(attachID, pa) {
		p.logf("controld: exec %s: attach id %s is already parked; refusing rather than "+
			"overwriting another client's pairing", target.SessionID, attachID)
		_ = stream.Close(errAttachIDCollision)
		return control.ErrUnavailable
	}

	dial := runner.ToRunner{Type: "dial_attach", Session: string(target.SessionID), Attach: &runner.Attach{
		AttachID:  attachID,
		Cols:      spec.Cols,
		Rows:      spec.Rows,
		TargetURL: p.host.BackURL(attachID),
		Kind:      runner.KindExec,
		Exec:      &spec,
		// No Mode and no Generation, ever. An exec attachment holds no
		// binding: the pty fence governs the pty, and an exec has its own.
	}}
	if err := p.host.Send(target.PoolID, target.RunnerID, dial); err != nil {
		// The command never left this process, so no runner can ever claim
		// this entry: take it back and let the caller close the client.
		p.attaches.claim(attachID)
		p.logf("controld: exec %s: %v", target.SessionID, err)
		return control.ErrUnavailable
	}

	expired := make(chan struct{})
	ttl := time.AfterFunc(p.ttl, func() {
		if _, ok := p.attaches.claim(attachID); !ok {
			return // the dial-back got here first; it owns the socket now
		}
		p.logf("controld: exec %s: no dial-back from %s within %s; closing the client",
			target.SessionID, target.RunnerID, p.ttl)
		close(expired)
		_ = stream.Close(errAttachNoDialBack)
		close(pa.done)
	})
	defer ttl.Stop()

	// Hold the exec open for its whole life: the socket now belongs to
	// whoever claims the pairing, and returning would let the handler above
	// run its deferred close on a socket the splice is still using.
	<-pa.done
	select {
	case <-expired:
		return control.ErrUnavailable
	default:
		return nil
	}
}

// ExecFirstMessage reads the one message an exec attachment must open with,
// under the same bound an attach's opening resize has: without it a client
// that connects and says nothing parks a goroutine and a file descriptor
// indefinitely.
//
// It is exported because every host's exec route needs it and none of them
// should re-derive it — a route that accepted a second message type here, or
// that read a byte of stdin before the spec arrived, would be a different
// protocol wearing the same name.
func ExecFirstMessage(ctx context.Context, stream control.TerminalStream) (terminal.ClientMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, attachFirstMsgTimeout)
	defer cancel()
	m, err := stream.Receive(ctx)
	if err != nil {
		// The client's own error text says nothing this end may relay, and
		// the outcome is the same either way: it did not open the way the
		// protocol requires.
		return terminal.ClientMessage{}, ErrExecFirstMessage
	}
	if m.Type != terminal.TypeExecStart || m.Exec == nil {
		return terminal.ClientMessage{}, ErrExecFirstMessage
	}
	return m, nil
}

// ErrExecFirstMessage is a client that did not open with the exec_start the
// protocol requires (or did not open at all before the first-message
// timeout). It is the client's own protocol error, so it closes with a policy
// violation rather than an invitation to retry. Exported because the route
// that reads the first message is the one that has to close on it.
var ErrExecFirstMessage = errors.New("controld: the first exec message must be an exec_start")

// ---------------------------------------------------------------------------
// the splice
// ---------------------------------------------------------------------------

// execSplice pumps one live exec both directions until either side ends, then
// closes both. It is the terminal splice's shape without its subject: there
// is no ownership to interpret, nothing to stamp, and no heartbeat, because
// an exec is not the terminal.
//
// It opens with the handshake. The sandbox has to say `exec_started` within
// one acknowledgement timeout; anything else — including the `snapshot` an
// older sessiond answers a Kind it does not know with — closes the socket as
// unsupported, having forwarded nothing.
func execSplice(ctx context.Context, client control.TerminalStream, sandbox runnerConn,
	ackTimeout time.Duration) {
	started, err := awaitExecStarted(ctx, sandbox, ackTimeout)
	if err != nil {
		// The client is told in its own vocabulary before the socket goes:
		// a close reason is a string a CLI has to pattern-match, a message
		// is a field it can switch on.
		tellCtx, cancel := context.WithTimeout(ctx, ackTimeout)
		_ = client.Send(tellCtx, terminal.ServerMessage{
			Type: terminal.TypeExecError, Reason: terminal.ReasonUnsupported})
		cancel()
		_ = client.Close(errExecUnsupported)
		sandbox.Close()
		return
	}
	if client.Send(ctx, started) != nil {
		sandbox.Close()
		_ = client.Close(errExecEnded)
		return
	}

	done := make(chan struct{}, 2)
	go func() {
		defer func() { done <- struct{}{} }()
		for {
			m, err := client.Receive(ctx)
			if err != nil {
				return
			}
			if !execClientForwardable(m.Type) {
				continue
			}
			// A generation is never read off an exec frame and never written
			// onto one. An exec client that sends a `claim` is dropped above
			// exactly as an attach client's `control` is: there is nothing
			// for it to claim.
			m.Generation = ""
			raw, err := json.Marshal(m)
			if err != nil {
				return
			}
			if sandbox.Write(ctx, raw) != nil {
				return
			}
		}
	}()
	go func() {
		defer func() { done <- struct{}{} }()
		for {
			raw, err := sandbox.Read(ctx)
			if err != nil {
				return
			}
			var m terminal.ServerMessage
			if json.Unmarshal(raw, &m) != nil {
				// A frame that is not a server message is the sandbox half
				// breaking the protocol; ending the exec says so, where
				// dropping it would leave a caller missing output it has no
				// way to notice. Nothing about the frame is logged.
				return
			}
			if !execServerForwardable(m.Type) {
				continue
			}
			// Whatever a sandbox puts here, a client is told nothing about a
			// generation on an exec.
			m.Generation = ""
			if client.Send(ctx, m) != nil {
				return
			}
		}
	}()
	<-done
	_ = client.Close(errExecEnded)
	sandbox.Close()
	<-done // let the second pump exit before returning
}

// awaitExecStarted reads the sandbox's first message under the handshake's
// own budget and requires it to be an `exec_started`.
//
// The budget is short because this is a single small frame on an
// already-open socket: the dial-back has happened, the sandbox has the spec,
// and a sandbox that knows what an exec is answers immediately whether the
// spawn succeeded or was refused. One that does not know answers a snapshot
// just as fast, which is the case this exists to catch.
//
// An `exec_error` is a perfectly good first message — a cwd outside the
// workspace, an env name the rule refuses, one exec too many — and is
// forwarded so the caller learns WHY rather than being told "unsupported"
// for something the sandbox understood completely.
func awaitExecStarted(ctx context.Context, sandbox runnerConn,
	budget time.Duration) (terminal.ServerMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	raw, err := sandbox.Read(ctx)
	if err != nil {
		return terminal.ServerMessage{}, errExecUnsupported
	}
	var m terminal.ServerMessage
	if json.Unmarshal(raw, &m) != nil {
		return terminal.ServerMessage{}, errExecUnsupported
	}
	switch m.Type {
	case terminal.TypeExecStarted, terminal.TypeExecError:
		m.Generation = ""
		return m, nil
	default:
		return terminal.ServerMessage{}, errExecUnsupported
	}
}

// execClientForwardable is the closed set of messages a caller may send into
// an exec. Everything else is dropped here rather than at the sandbox,
// including the whole ownership vocabulary: a `claim` on an exec has nothing
// to claim, and a `control` would be a client naming its own mode and its own
// generation at a pty this attachment must never touch.
//
// `exec_start` is dropped too, and that is the handshake's other half: the
// spec was settled before the socket was upgraded, so a second one is a
// client trying to run a command the application never authorized or audited.
func execClientForwardable(typ string) bool {
	switch typ {
	case "stdin", "resize", terminal.TypeExecStdinEOF, terminal.TypeExecSignal:
		return true
	default:
		return false
	}
}

// execServerForwardable is the mirror: what a sandbox may say on an exec.
// The client-facing ownership vocabulary is dropped for the same reason the
// terminal splice drops it — a client told `attached control 99` by its
// sandbox would believe it holds a lease nobody granted — and a second
// `exec_started` is dropped because the handshake already answered.
func execServerForwardable(typ string) bool {
	switch typ {
	case terminal.TypeExecStdout, terminal.TypeExecStderr,
		terminal.TypeExecExit, terminal.TypeExecError:
		return true
	default:
		return false
	}
}

// internal/relay/session_side.go
package relay

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/tokencanopy/rainier/internal/session"
	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// connWriter is the single-writer discipline for one relay conn. Every frame
// leaving the sessiond side goes through it: the per-attachment forwarder
// goroutines ServeSession starts, and ControlSender.Send. That is not
// belt-and-braces — a WebSocket conn admits exactly one writer at a time, so
// two goroutines calling conn.Write concurrently interleave the bytes of two
// frames and corrupt the stream permanently for the peer. Before the control
// channel existed the discipline was implicit (only ServeSession wrote);
// this type makes it explicit so a second writer can be added safely.
type connWriter struct {
	// sem is that discipline, as a one-slot semaphore rather than a Mutex: a
	// Mutex cannot be acquired with a deadline, and writeWithin's whole job is
	// to bound the wait for this writer.
	sem  chan struct{}
	conn Conn
	ctx  context.Context
	// execBudget is what an exec frame gets (see execWriteBudget). It is a
	// field rather than a constant read at the call site so a test can drive
	// the dropped-consumer path in milliseconds instead of in five seconds,
	// and so it cannot become a package variable two tests write.
	execBudget time.Duration
}

func newConnWriter(ctx context.Context, conn Conn) *connWriter {
	return newConnWriterBudget(ctx, conn, execWriteBudget)
}

func newConnWriterBudget(ctx context.Context, conn Conn, exec time.Duration) *connWriter {
	return &connWriter{conn: conn, ctx: ctx, sem: make(chan struct{}, 1), execBudget: exec}
}

func (w *connWriter) write(f Frame) error {
	b, err := Encode(f)
	if err != nil {
		return err
	}
	select {
	case w.sem <- struct{}{}:
	case <-w.ctx.Done():
		return w.ctx.Err()
	}
	defer func() { <-w.sem }()
	return w.conn.Write(w.ctx, b)
}

// execWriteBudget bounds ONE exec frame's trip onto this conn.
//
// Without it an exec forwarder's write takes the shared writer and uses the
// ServeSession context, which lives as long as the session: a caller draining
// at a couple of kilobytes a second never trips the plane's own twenty-second
// budget (each individual write completes inside it) and holds this writer
// indefinitely, and behind it the agent's terminal output and the session RPC
// wait. The plane's budget is a bound on a caller that takes NOTHING; this is
// the bound on one that takes almost nothing, which is the case that actually
// stalls a session.
//
// Five seconds, against the largest frame this path produces — a 32 KiB
// readChunk is 43,692 wire bytes after two base64 hops — is a floor of about
// 8.7 KB/s. That is far below any link a person runs `rainier exec` over and
// far above the rate a stall is measured at. A caller under it loses its exec
// with a clean "no exit status" (125) and a sentence, never a truncated
// stream, and can run the command again.
const execWriteBudget = 5 * time.Second

// errExecWriteBudget is an exec frame that could not be put on the wire inside
// that budget. It is this exec's consumer being gone in every way that matters.
var errExecWriteBudget = errors.New("relay: the exec's frame missed its write budget")

// writeWithin is write with a deadline of its own, for the one caller that
// must not be able to hold this conn's writer indefinitely.
//
// Two things are bounded and their outcomes differ, which is worth stating
// because only one of them is free:
//
//   - ACQUIRING the writer. Somebody else is mid-write and has been for longer
//     than this budget. Nothing has been written, so the conn is untouched and
//     only this exec is dropped.
//   - The write ITSELF. A WebSocket frame cannot be abandoned half-written, so
//     the transport's answer to an expired write deadline is to close the conn
//     (coder/websocket's setupWriteTimeout). That is the honest bound: the
//     alternative is an unbounded freeze of everything else on the session,
//     and sessiond's dial loop redials within its one-second backoff.
func (w *connWriter) writeWithin(f Frame, d time.Duration) error {
	b, err := Encode(f)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(w.ctx, d)
	defer cancel()
	select {
	case w.sem <- struct{}{}:
	case <-ctx.Done():
		if w.ctx.Err() != nil {
			return w.ctx.Err()
		}
		return errExecWriteBudget
	}
	defer func() { <-w.sem }()
	if err := w.conn.Write(ctx, b); err != nil {
		if ctx.Err() != nil && w.ctx.Err() == nil {
			return errExecWriteBudget
		}
		return err
	}
	return nil
}

// ControlSender emits sessiond-originated control frames upstream over the
// conn its ServeSessionWithControl call is serving: setup outcomes, the
// session-RPC requests this end originates, and the responses to requests
// that arrived on onControl. It is safe for concurrent use with that relay —
// and only with that one, since sharing the conn safely means sharing its
// writer, not just its Conn value.
type ControlSender struct{ w *connWriter }

// Send wraps payload in a FrameControl on AttachID 0 (no attachment owns a
// control event) and writes it. It returns the conn's write error once the
// conn is dead, so a caller learns its event was never delivered instead of
// dropping it silently.
//
// It blocks while another frame is mid-write, which on a wedged (stalled but
// not yet failed) conn means blocking until the context ServeSessionWithControl
// was given is cancelled — the same bound every other write on this conn has.
// Callers that must stay responsive should Send from their own goroutine.
func (c *ControlSender) Send(payload []byte) error {
	return c.w.write(Frame{Type: FrameControl, AttachID: 0, Payload: payload})
}

// ServeSession runs on the sessiond side: it reads Frames off conn (an
// already-established outbound conn to runnerd) and demultiplexes them onto
// s by AttachID — FrameOpen calls s.Attach and starts a per-attachment
// goroutine pumping s's ServerMsgs back as FrameServer; FrameClient carries a
// raw terminal.ClientMessage into s.Stdin/s.SetSize; FrameClose calls s.Detach.
// Returns when conn.Read errors (conn closed).
func ServeSession(ctx context.Context, conn Conn, s *session.Session) error {
	return serveSession(ctx, conn, s, newConnWriter(ctx, conn), nil, nil)
}

// ServeSessionWithControl is ServeSession plus a control channel in both
// directions: it runs the relay on its own goroutine and hands back a
// ControlSender sharing the relay's writer, so outbound control events and
// terminal frames cannot interleave on the conn, while onControl receives the
// control frames arriving the other way — the session-RPC requests runnerd
// sends down, and the responses to requests this end originated.
//
// onControl is wired here, in the constructor, for the same two reasons the
// Hub's is (see NewHubWithControl): assigning it to a field afterwards would
// race the relay goroutine this call starts, and a request arriving on the
// conn's first frame would find no handler installed yet. nil means inbound
// control frames are read and dropped, which is exactly the Plan 4 behaviour
// for a session that never expects to be asked anything.
//
// Each inbound frame is dispatched on its OWN goroutine, so onControl may
// take as long as its method needs — an RPC that shells out to git is the
// point of this channel — without stalling the demux that every attachment on
// this conn shares. Two consequences the handler must live with: frames are
// not ordered against each other once dispatched (correlate by ControlEvent.ID,
// never by arrival), and a handler that never returns leaks its goroutine for
// the life of the process, so anything unbounded inside it needs its own
// timeout. Replies go back through the returned ControlSender, whose Send is
// safe to call from those goroutines.
//
// The returned channel is buffered and receives exactly one value — whatever
// ServeSession returned when the conn died. It is deliberately not closed
// afterwards: a nil second receive would read as "the relay is fine", so
// callers take the value once and treat that as the end of this conn's life.
func ServeSessionWithControl(ctx context.Context, conn Conn, s *session.Session, onControl func(payload []byte)) (*ControlSender, <-chan error) {
	return ServeSessionWithExec(ctx, conn, s, onControl, nil)
}

// ServeSessionWithExec is ServeSessionWithControl plus the second KIND of
// attachment this conn can carry: ex answers the FrameOpens whose Kind is
// runner.KindExec, and the session answers every other one exactly as it
// always has.
//
// ex may be nil, and a nil one is not an error — it is a sandbox that cannot
// exec, which answers an exec open by closing the attachment. That is
// deliberately the same thing an older sessiond does with a Kind it has never
// heard of (it opens a terminal attachment and sends a snapshot): in both
// cases the caller never receives an `exec_started`, and the plane refuses on
// the missing handshake rather than on anything it had to be told.
func ServeSessionWithExec(ctx context.Context, conn Conn, s *session.Session,
	onControl func(payload []byte), ex Execer) (*ControlSender, <-chan error) {
	w := newConnWriter(ctx, conn)
	errc := make(chan error, 1)
	go func() { errc <- serveSession(ctx, conn, s, w, onControl, ex) }()
	return &ControlSender{w: w}, errc
}

func serveSession(ctx context.Context, conn Conn, s *session.Session, w *connWriter,
	onControl func(payload []byte), ex Execer) error {
	var mu sync.Mutex
	atts := map[uint64]*session.Attachment{}
	// execs is the second attachment table, keyed by the same ids. An
	// attachment is in exactly one of the two for its whole life — the kind
	// is decided by the frame that opens it and never changes — so
	// FrameClient and FrameClose route by looking in both, and an exec id is
	// never handed to s.Attach, s.Bind or s.Stdin.
	execs := map[uint64]ExecAttachment{}
	// Every frame this loop and its per-attachment forwarder goroutines emit
	// goes through the shared writer, which a ControlSender may be writing
	// through too — see connWriter.
	write := w.write

	for {
		raw, err := conn.Read(ctx)
		if err != nil {
			// Outbound conn is dead — detach every live attachment so its
			// forwarder goroutine (ranging att.Msgs) exits and the session
			// stops clamping/serving a viewer that can no longer be reached.
			mu.Lock()
			for _, att := range atts {
				s.Detach(att.ID)
			}
			atts = map[uint64]*session.Attachment{}
			// An exec whose conn died has lost the one consumer its output
			// was ever for, so it is closed — which kills its process group
			// unless it was detached, whose lifetime is the session's.
			dead := execs
			execs = map[uint64]ExecAttachment{}
			mu.Unlock()
			for _, e := range dead {
				e.Close()
			}
			return err
		}
		f, err := Decode(raw)
		if err != nil {
			continue
		}
		switch f.Type {
		case FrameOpen:
			// An explicit switch over the KINDS, with a default that refuses.
			// `if Kind == KindExec` was correct for the two kinds that exist,
			// and the next one added would silently open the AGENT's pty with
			// no handshake to save it — which is the one mistake this whole
			// design is built to make impossible.
			switch f.Kind {
			case runner.KindExec:
				// Its own branch, and it never touches session.Session: an
				// exec's output must never reach the emulator, the event log
				// or another viewer's screen, and the cheapest way to
				// guarantee that is for the code path not to have the session
				// in its hands at all.
				if ex == nil || f.Exec == nil {
					write(Frame{Type: FrameClose, AttachID: f.AttachID})
					continue
				}
				mu.Lock()
				_, liveExec := execs[f.AttachID]
				_, liveTerm := atts[f.AttachID]
				mu.Unlock()
				if liveExec || liveTerm {
					// A second open on a live id. Unreachable from this
					// runnerd (Hub.next is monotonic), but serveSession is the
					// sandbox's trust boundary: overwriting the entry would
					// leave the previous process unkilled and its slot held
					// for the life of the session.
					write(Frame{Type: FrameClose, AttachID: f.AttachID})
					continue
				}
				att := ex.OpenExec(*f.Exec)
				mu.Lock()
				execs[f.AttachID] = att
				mu.Unlock()
				go func(id uint64, a ExecAttachment) {
					for msg := range a.Msgs() {
						p, err := json.Marshal(msg)
						if err != nil {
							continue
						}
						// A blocking write, which is the point: it is what
						// stops the sandbox reading the process's pipes while
						// this conn is not draining, so the process blocks on
						// write(2) and no byte of its output is dropped.
						//
						// Blocking, but not forever: this writer is shared
						// with every other attachment on this conn and with
						// the session RPC, so an exec caller that has stopped
						// draining would otherwise stall a person's terminal
						// behind it. See execWriteBudget.
						if w.writeWithin(Frame{Type: FrameServer, AttachID: id, Payload: p},
							w.execBudget) != nil {
							mu.Lock()
							delete(execs, id)
							mu.Unlock()
							a.Close()
							return
						}
					}
					write(Frame{Type: FrameClose, AttachID: id})
					mu.Lock()
					delete(execs, id)
					mu.Unlock()
				}(f.AttachID, att)
				continue
			case runner.KindTerminal:
				// The terminal branch, below. Named rather than defaulted: an
				// older plane sends no kind at all, which is the same thing
				// and is why the empty string is this case too.
			default:
				// A kind this sessiond has never heard of. Refused rather than
				// opened as a terminal, because the plane's own handshake is
				// what turns "refused" into an actionable answer and there is
				// no handshake for a kind nobody here can name.
				write(Frame{Type: FrameClose, AttachID: f.AttachID})
				continue
			}
			// The binding rides the frame that opens the attachment, so it is
			// installed before a byte of screen is queued — no acknowledgement
			// to order against, and nothing for a late frame to slip past. An
			// older plane sends no mode, which leaves the attachment unbound
			// and therefore unconditional, exactly as it is today.
			att, err := s.Attach(f.Since, session.Size{Cols: f.Cols, Rows: f.Rows}, frameBinding(f))
			if err != nil {
				write(Frame{Type: FrameClose, AttachID: f.AttachID})
				continue
			}
			mu.Lock()
			atts[f.AttachID] = att
			mu.Unlock()
			go func(id uint64, a *session.Attachment) {
				for msg := range a.Msgs {
					p, _ := json.Marshal(msg)
					if write(Frame{Type: FrameServer, AttachID: id, Payload: p}) != nil {
						// conn is dead: stop pumping and detach locally so
						// this viewer slot is freed instead of held (and
						// clamping terminal size) forever. The outer loop's
						// own conn-death cleanup above may race this and
						// detach the same id too — s.Detach is idempotent,
						// so that's safe, not a bug.
						mu.Lock()
						delete(atts, id)
						mu.Unlock()
						s.Detach(a.ID)
						return
					}
				}
				write(Frame{Type: FrameClose, AttachID: id})
				mu.Lock()
				delete(atts, id)
				mu.Unlock()
			}(f.AttachID, att)
		case FrameClient:
			var cm terminal.ClientMessage
			if json.Unmarshal(f.Payload, &cm) != nil {
				continue
			}
			mu.Lock()
			att := atts[f.AttachID]
			exc := execs[f.AttachID]
			mu.Unlock()
			if exc != nil {
				// An exec's client messages go to its own process and never
				// to the pty. Nothing here reads a generation off them: a
				// plane does not stamp an exec frame, and an exec that sent a
				// claim would have nothing to claim.
				exc.Client(cm)
				continue
			}
			if att == nil {
				continue
			}
			switch cm.Type {
			case "stdin":
				s.Stdin(att.ID, cm.Generation.Value(), cm.Data)
			case "resize":
				s.SetSize(att.ID, cm.Generation.Value(), session.Size{Cols: cm.Cols, Rows: cm.Rows})
			case terminal.TypeControl:
				// A mid-attach handoff. Install it, then say so: the plane
				// does not tell a taker it has control until this
				// acknowledgement comes back, which is what closes the window
				// where the previous controller's already-sent keystroke
				// could still execute.
				gen := cm.Generation.Value()
				if !s.Bind(att.ID, session.Binding{Bound: true, Mode: cm.Mode, Generation: gen}) {
					continue
				}
				ack, err := json.Marshal(terminal.ServerMessage{
					Type: terminal.TypeControlAck, Mode: cm.Mode, Generation: cm.Generation})
				if err != nil {
					continue
				}
				write(Frame{Type: FrameServer, AttachID: f.AttachID, Payload: ack})
			}
		case FrameClose:
			mu.Lock()
			att := atts[f.AttachID]
			delete(atts, f.AttachID)
			exc := execs[f.AttachID]
			delete(execs, f.AttachID)
			mu.Unlock()
			if att != nil {
				s.Detach(att.ID)
			}
			if exc != nil {
				exc.Close()
			}
		case FrameControl:
			// Its own case, never the attachment demux: a control frame
			// carries AttachID 0, which no attachment ever has. The payload is
			// a fresh slice per frame (Decode unmarshals into one), so handing
			// it to a goroutine aliases nothing the next read will overwrite.
			// Dispatched on that goroutine so a handler doing real work
			// (running a diff, reading files) cannot stall the terminal
			// traffic multiplexed over this same conn — see
			// ServeSessionWithControl for what that costs the handler.
			if onControl != nil {
				go onControl(f.Payload)
			}
		}
	}
}

// frameBinding reads the controller binding off an opening frame. A frame
// with no mode is an older plane's, and leaves the attachment unbound — which
// the session reads as today's unconditional attachment.
func frameBinding(f Frame) session.Binding {
	if f.Mode == "" {
		return session.Binding{}
	}
	return session.Binding{Bound: true, Mode: f.Mode, Generation: f.Gen}
}

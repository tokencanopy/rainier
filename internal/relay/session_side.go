// internal/relay/session_side.go
package relay

import (
	"context"
	"encoding/json"
	"sync"

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
	mu   sync.Mutex
	conn Conn
	ctx  context.Context
}

func newConnWriter(ctx context.Context, conn Conn) *connWriter {
	return &connWriter{conn: conn, ctx: ctx}
}

func (w *connWriter) write(f Frame) error {
	b, err := Encode(f)
	if err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.conn.Write(w.ctx, b)
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
			if f.Kind == runner.KindExec {
				// Its own branch, and it never touches session.Session: an
				// exec's output must never reach the emulator, the event log
				// or another viewer's screen, and the cheapest way to
				// guarantee that is for the code path not to have the session
				// in its hands at all.
				if ex == nil || f.Exec == nil {
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
						if write(Frame{Type: FrameServer, AttachID: id, Payload: p}) != nil {
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

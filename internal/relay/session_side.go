// internal/relay/session_side.go
package relay

import (
	"context"
	"encoding/json"
	"sync"

	"rainier/internal/session"
	"rainier/internal/wire"
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
	if err != nil { return err }
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
// raw wire.ClientMsg into s.Stdin/s.SetSize; FrameClose calls s.Detach.
// Returns when conn.Read errors (conn closed).
func ServeSession(ctx context.Context, conn Conn, s *session.Session) error {
	return serveSession(ctx, conn, s, newConnWriter(ctx, conn), nil)
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
	w := newConnWriter(ctx, conn)
	errc := make(chan error, 1)
	go func() { errc <- serveSession(ctx, conn, s, w, onControl) }()
	return &ControlSender{w: w}, errc
}

func serveSession(ctx context.Context, conn Conn, s *session.Session, w *connWriter, onControl func(payload []byte)) error {
	var mu sync.Mutex
	atts := map[uint64]*session.Attachment{}
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
			for _, att := range atts { s.Detach(att.ID) }
			atts = map[uint64]*session.Attachment{}
			mu.Unlock()
			return err
		}
		f, err := Decode(raw)
		if err != nil { continue }
		switch f.Type {
		case FrameOpen:
			att, err := s.Attach(f.Since, session.Size{Cols: f.Cols, Rows: f.Rows})
			if err != nil { write(Frame{Type: FrameClose, AttachID: f.AttachID}); continue }
			mu.Lock(); atts[f.AttachID] = att; mu.Unlock()
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
						mu.Lock(); delete(atts, id); mu.Unlock()
						s.Detach(a.ID)
						return
					}
				}
				write(Frame{Type: FrameClose, AttachID: id})
				mu.Lock(); delete(atts, id); mu.Unlock()
			}(f.AttachID, att)
		case FrameClient:
			var cm wire.ClientMsg
			if json.Unmarshal(f.Payload, &cm) != nil { continue }
			mu.Lock(); att := atts[f.AttachID]; mu.Unlock()
			if att == nil { continue }
			switch cm.Type {
			case "stdin": s.Stdin(cm.Data)
			case "resize": s.SetSize(att.ID, session.Size{Cols: cm.Cols, Rows: cm.Rows})
			}
		case FrameClose:
			mu.Lock(); att := atts[f.AttachID]; delete(atts, f.AttachID); mu.Unlock()
			if att != nil { s.Detach(att.ID) }
		case FrameControl:
			// Its own case, never the attachment demux: a control frame
			// carries AttachID 0, which no attachment ever has. The payload is
			// a fresh slice per frame (Decode unmarshals into one), so handing
			// it to a goroutine aliases nothing the next read will overwrite.
			// Dispatched on that goroutine so a handler doing real work
			// (running a diff, reading files) cannot stall the terminal
			// traffic multiplexed over this same conn — see
			// ServeSessionWithControl for what that costs the handler.
			if onControl != nil { go onControl(f.Payload) }
		}
	}
}

// internal/relay/runnerd_side.go
package relay

import (
	"context"
	"sync"
)

// Conn is the minimal transport interface relay logic runs against, so it's
// testable with in-memory pipes instead of a real network. A *websocket.Conn
// adapter (read/write text frames) is added in Task 9/10 where the real
// transports are wired.
type Conn interface {
	Read(ctx context.Context) ([]byte, error)
	Write(ctx context.Context, b []byte) error
	Close() error
}

// Hub runs on the runnerd side: one Hub wraps one registered session conn
// (the single outbound WebSocket that session opened to runnerd) and lets
// many clients attach over it, each multiplexed by its own AttachID.
type Hub struct {
	conn    Conn
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.Mutex
	next    uint64
	clients map[uint64]Conn // attachID → client conn
	// onControl is wired by the constructor and never written again — see
	// NewHubWithControl for why it isn't a settable field.
	onControl func(payload []byte)
}

func NewHub(ctx context.Context, sessionConn Conn) *Hub {
	return NewHubWithControl(ctx, sessionConn, nil)
}

// NewHubWithControl is NewHub plus a handler for the session's control
// events — the FrameControls that belong to the session itself (setup
// outcomes today) rather than to any attachment, which is why they carry no
// meaningful AttachID and bypass the client demux in readLoop.
//
// The handler is wired here, in the constructor, rather than assigned to a
// field afterwards, and that is deliberate on two counts. Correctness: the
// constructor starts readLoop, so a later assignment would be a plain write
// racing that goroutine's read under the Go memory model — the same shape
// runnerd's OnEvent had to fix in Plan 3. Delivery: wiring it before the
// goroutine starts means the handler exists from the hub's very first frame,
// so a control event that arrives the instant a session registers cannot land
// in the window before a caller got around to installing a handler.
//
// onControl runs ON readLoop's goroutine, so it must hand off and return —
// the same contract as runnerd's OnEvent — because blocking in it stalls
// every attachment multiplexed over this conn, not just the control channel.
// nil means control frames are read and dropped.
func NewHubWithControl(ctx context.Context, sessionConn Conn, onControl func(payload []byte)) *Hub {
	hctx, cancel := context.WithCancel(ctx)
	h := &Hub{conn: sessionConn, ctx: hctx, cancel: cancel, clients: map[uint64]Conn{}, onControl: onControl}
	go h.readLoop()
	return h
}

// readLoop demultiplexes FrameServer/FrameClose from the session to the right
// client, and hands FrameControl to onControl instead (it belongs to the
// session, not to an attachment). Its deferred cleanup is what makes
// session-conn death cascade to every attached client: once h.conn.Read
// errors for good (conn dead), close every remaining client conn rather than
// leaving each one parked forever with no one left to ever write it another
// byte or a close.
func (h *Hub) readLoop() {
	defer func() {
		h.mu.Lock()
		for id, cl := range h.clients { cl.Close(); delete(h.clients, id) }
		h.mu.Unlock()
	}()
	for {
		raw, err := h.conn.Read(h.ctx)
		if err != nil { h.cancel(); return }
		f, err := Decode(raw)
		if err != nil { continue }
		if f.Type == FrameControl {
			// Handled before the client lookup: a control frame carries
			// AttachID 0, which is never a client id (ids start at 1), so
			// the demux below would drop it as "unknown attachment".
			if h.onControl != nil { h.onControl(f.Payload) }
			continue
		}
		h.mu.Lock(); client := h.clients[f.AttachID]; h.mu.Unlock()
		if client == nil { continue }
		switch f.Type {
		case FrameServer:
			// Forward the wire.ServerMsg payload verbatim to the client.
			if client.Write(h.ctx, f.Payload) != nil {
				h.mu.Lock(); delete(h.clients, f.AttachID); h.mu.Unlock()
				client.Close()
			}
		case FrameClose:
			client.Close()
			h.mu.Lock(); delete(h.clients, f.AttachID); h.mu.Unlock()
		}
	}
}

// AttachClient bridges a client conn to a new attachment over the session
// conn: it opens the attachment (FrameOpen), then pumps client → session as
// FrameClient until the client disconnects, at which point it tells the
// session to close the attachment too. Blocks until the client conn errors.
func (h *Hub) AttachClient(ctx context.Context, client Conn, since uint64, cols, rows int) error {
	h.mu.Lock()
	h.next++
	id := h.next
	h.clients[id] = client
	h.mu.Unlock()

	open, _ := Encode(Frame{Type: FrameOpen, AttachID: id, Since: since, Cols: cols, Rows: rows})
	if err := h.conn.Write(h.ctx, open); err != nil {
		// Session conn is already dead: this client would otherwise stay
		// registered in h.clients (and its caller left hanging with no
		// FrameOpen ever having reached the session) forever.
		h.mu.Lock(); delete(h.clients, id); h.mu.Unlock()
		client.Close()
		return err
	}

	// Pump client → session as FrameClient until the client disconnects.
	for {
		raw, err := client.Read(ctx)
		if err != nil {
			cl, _ := Encode(Frame{Type: FrameClose, AttachID: id})
			h.conn.Write(h.ctx, cl)
			h.mu.Lock(); delete(h.clients, id); h.mu.Unlock()
			return err
		}
		fr, _ := Encode(Frame{Type: FrameClient, AttachID: id, Payload: raw})
		if err := h.conn.Write(h.ctx, fr); err != nil {
			// Same cleanup as the client-read-error branch above, applied to
			// a session-conn write failure instead: readLoop is likely
			// already gone (its own h.conn.Read errored too), so nothing
			// will ever close this client conn or notify it otherwise.
			h.mu.Lock(); delete(h.clients, id); h.mu.Unlock()
			client.Close()
			return err
		}
	}
}

// Done reports when the session conn has died — either readLoop noticed
// h.conn.Read fail on its own (container/network death) and called
// h.cancel(), or a caller invoked Close() directly. Both converge on the
// same h.ctx cancellation, so this is the one signal a caller needs to learn
// "this hub is no longer serving a live session conn" without duplicating
// readLoop's own liveness detection.
func (h *Hub) Done() <-chan struct{} { return h.ctx.Done() }

// Close is idempotent and safe to call more than once, including
// concurrently with itself or with readLoop's own h.cancel() call: cancel
// (from context.WithCancel) tolerates repeat calls by contract, and
// coder/websocket's CloseNow is a best-effort immediate close that likewise
// tolerates being called again on an already-closed conn. Callers relying on
// this: runnerd's /register handler calls it after Done() fires, and
// sessionOp's DELETE branch calls it directly to force that same Done() —
// both can legitimately race to close the same hub.
func (h *Hub) Close() { h.cancel(); h.conn.Close() }

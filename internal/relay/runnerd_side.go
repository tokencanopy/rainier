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
}

func NewHub(ctx context.Context, sessionConn Conn) *Hub {
	hctx, cancel := context.WithCancel(ctx)
	h := &Hub{conn: sessionConn, ctx: hctx, cancel: cancel, clients: map[uint64]Conn{}}
	go h.readLoop()
	return h
}

// readLoop demultiplexes FrameServer/FrameClose from the session to the right client.
func (h *Hub) readLoop() {
	for {
		raw, err := h.conn.Read(h.ctx)
		if err != nil { h.cancel(); return }
		f, err := Decode(raw)
		if err != nil { continue }
		h.mu.Lock(); client := h.clients[f.AttachID]; h.mu.Unlock()
		if client == nil { continue }
		switch f.Type {
		case FrameServer:
			// Forward the wire.ServerMsg payload verbatim to the client.
			client.Write(h.ctx, f.Payload)
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
	if err := h.conn.Write(h.ctx, open); err != nil { return err }

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
		if err := h.conn.Write(h.ctx, fr); err != nil { return err }
	}
}

func (h *Hub) Close() { h.cancel(); h.conn.Close() }

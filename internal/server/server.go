// serve(conn) is deliberately transport-direction-agnostic: Plan 2 reuses it
// verbatim on an outbound-dialed connection (spec portability rule 3).
package server

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/tokencanopy/rainier/internal/session"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

const defaultPingInterval = 15 * time.Second

type handler struct {
	s            *session.Session
	pingInterval time.Duration
}

func New(s *session.Session) http.Handler { return NewWithKeepalive(s, defaultPingInterval) }

func NewWithKeepalive(s *session.Session, pingInterval time.Duration) http.Handler {
	mux := http.NewServeMux()
	h := &handler{s: s, pingInterval: pingInterval}
	mux.HandleFunc("/attach", h.attach)
	return mux
}

func (h *handler) attach(w http.ResponseWriter, r *http.Request) {
	since, _ := strconv.ParseUint(r.URL.Query().Get("since"), 10, 64)
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer c.CloseNow()
	// coder/websocket defaults to a 32KiB/message read limit and closes the
	// connection with StatusMessageTooBig past it. PTY output frames (and
	// therefore their ~1.37x JSON envelope) can exceed that on a single
	// bursty write, which would wedge both the live attach path and replay
	// of that frame from the event log forever. 16MiB is a generous explicit
	// cap, not unlimited (-1); a real protocol-level max-frame size is
	// deferred to Plan 2.
	c.SetReadLimit(16 << 20)
	serve(r.Context(), c, h.s, since, h.pingInterval)
}

func serve(ctx context.Context, c *websocket.Conn, s *session.Session, since uint64, pingInterval time.Duration) {
	// First message must announce viewer size.
	var first terminal.ClientMessage
	if err := wsjson.Read(ctx, c, &first); err != nil || first.Type != "resize" {
		return
	}
	att, err := s.Attach(since, session.Size{Cols: first.Cols, Rows: first.Rows})
	if err != nil {
		return
	}
	defer s.Detach(att.ID)

	// Liveness: a viewer whose transport has died (terminal closed, laptop
	// slept, network vanished) never sends a close frame and never errors
	// out of the reader loop below on its own — it just parks forever,
	// still counted by EffectiveSize, still clamping every other viewer's
	// PTY to its last-known size. A periodic ping/pong round trip is the
	// only way to notice a peer that TCP itself hasn't yet noticed is gone;
	// a failed ping forces the socket closed so the reader's blocked Read
	// unblocks with an error, serve returns, and the deferred Detach above
	// runs — after which applySizeLocked (Plan 1) recomputes EffectiveSize
	// from the survivors and they regrow.
	pingCtx, cancelPing := context.WithCancel(ctx)
	defer cancelPing()
	go func() {
		t := time.NewTicker(pingInterval)
		defer t.Stop()
		for {
			select {
			case <-pingCtx.Done():
				return
			case <-t.C:
				pctx, cancel := context.WithTimeout(pingCtx, pingInterval)
				err := c.Ping(pctx)
				cancel()
				if err != nil {
					c.CloseNow() // triggers reader error → serve returns → Detach
					return
				}
			}
		}
	}()

	// Writer: session → client. att.Msgs closes on detach AND on session exit
	// (including attach-after-exit), so this goroutine always ends on its own.
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		for m := range att.Msgs {
			if wsjson.Write(ctx, c, m) != nil {
				return
			}
		}
		// The writer only drains normally (rather than returning early on a
		// write error) when att.Msgs closed on its own — i.e. the session
		// exited. Close the socket so the reader's blocked wsjson.Read below
		// unblocks with an error and serve returns, instead of leaving the
		// client hanging on a connection nothing will ever write to again.
		c.Close(websocket.StatusNormalClosure, "session exited")
	}()

	// Reader: client → session.
	for {
		var m terminal.ClientMessage
		if err := wsjson.Read(ctx, c, &m); err != nil {
			return
		}
		switch m.Type {
		case "stdin":
			s.Stdin(m.Data)
		case "resize":
			s.SetSize(att.ID, session.Size{Cols: m.Cols, Rows: m.Rows})
		}
	}
}

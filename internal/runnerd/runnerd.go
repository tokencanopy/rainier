// internal/runnerd/runnerd.go
package runnerd

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"rainier/internal/driver"
	"rainier/internal/relay"
	"rainier/internal/wire"
)

type Server struct {
	drv         driver.Driver
	reg         *registry
	dialBase    string // e.g. ws://runnerd:8080 — what sessiond dials to register
	seq         atomic.Int64
	egressAdmin string // http://egressd:3129 (optional)
}

func New(drv driver.Driver, dialBase, egressAdmin string) *Server {
	return &Server{drv: drv, reg: newRegistry(), dialBase: dialBase, egressAdmin: egressAdmin}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/sessions", s.sessions)   // POST create, GET list
	mux.HandleFunc("/sessions/", s.sessionOp) // /sessions/{id}/{op}
	mux.HandleFunc("/register", s.register)   // ws: sessiond dials in
	mux.HandleFunc("/attach", s.attach)       // ws: client attaches
	return mux
}

// newID is called from concurrent POST /sessions handlers (the normal fleet
// operating mode — many callers creating sessions at once), so the counter
// must be a real atomic increment: a plain s.seq++ is a read-modify-write
// with no synchronization, and two concurrent POSTs can both read the same
// value, both increment to the same next value, and mint the same id —
// registry.put then silently overwrites the first session's entry, making
// its driver handle unreachable and its capacity accounting wrong.
func (s *Server) newID() string { return "sess-" + strconv.FormatInt(s.seq.Add(1), 10) }

func (s *Server) sessions(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var body struct {
			Name        string   `json:"name"`
			Image       string   `json:"image"`
			Cmd         []string `json:"cmd"`
			EgressAllow []string `json:"egress_allow"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		id := s.newID()
		spec := driver.Spec{
			Name: body.Name, Image: body.Image, Cmd: body.Cmd,
			SessionID: id, DialURL: s.dialBase + "/register",
			EgressAllow: body.EgressAllow,
		}
		if err := s.pushEgress(id, body.EgressAllow); err != nil {
			http.Error(w, "egress setup: "+err.Error(), http.StatusBadGateway)
			return
		}
		// Register the entry before the driver call, not after: a real
		// container's sessiond can dial /register the instant drv.Create's
		// `docker run -d` returns, often faster than this goroutine reaching
		// a put() that came after it — see registry.setHandle's doc comment
		// for what that race used to do.
		s.reg.put(id, &sessionEntry{id: id, state: "starting", allow: body.EgressAllow})
		h, err := s.drv.Create(r.Context(), spec)
		if err != nil {
			s.reg.remove(id)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		s.reg.setHandle(id, h.ID)
		s.reg.setState(id, "running")
		json.NewEncoder(w).Encode(map[string]string{"session_id": id})
	case http.MethodGet:
		type row struct{ ID, State string }
		var rows []row
		for _, e := range s.reg.list() {
			rows = append(rows, row{e.id, e.state})
		}
		json.NewEncoder(w).Encode(rows)
	default:
		http.Error(w, "method", http.StatusMethodNotAllowed)
	}
}

func (s *Server) sessionOp(w http.ResponseWriter, r *http.Request) {
	// /sessions/{id} (DELETE) or /sessions/{id}/{op} (POST)
	rest := strings.TrimPrefix(r.URL.Path, "/sessions/")
	parts := strings.SplitN(rest, "/", 2)
	id := parts[0]
	e, ok := s.reg.get(id)
	if !ok {
		http.Error(w, "no such session", http.StatusNotFound)
		return
	}
	ctx := r.Context()
	if r.Method == http.MethodDelete {
		// Close the hub (if the session ever registered) before removing the
		// entry: hub.Close() cancels its ctx, which is what unblocks
		// register()'s own `<-hub.Done()` wait so that goroutine cleans up
		// synchronously with this deliberate teardown instead of being left
		// to find out later. register() also calls reg.remove/hub.Close on
		// its own unblock — both are safe, idempotent no-ops the second time.
		if h, ok := s.reg.hub(id); ok {
			h.Close()
		}
		s.drv.Destroy(ctx, e.handle)
		s.reg.remove(id)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	// Every other op below is a mutation (suspend/resume) or driver call
	// (snapshot); only POST may trigger them — a GET on
	// /sessions/{id}/suspend must not be able to execute one.
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	op := ""
	if len(parts) == 2 {
		op = parts[1]
	}
	switch op {
	case "suspend":
		warm := r.URL.Query().Get("warm") != "false"
		if err := s.drv.Suspend(ctx, e.handle, warm); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		// e.state is mutated here from a concurrent request goroutine, so it
		// goes through the registry lock rather than a direct field write —
		// see registry.setState.
		s.reg.setState(id, "suspended")
		w.WriteHeader(http.StatusNoContent)
	case "resume":
		if err := s.drv.Resume(ctx, e.handle); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		s.reg.setState(id, "running")
		w.WriteHeader(http.StatusNoContent)
	case "snapshot":
		snap, err := s.drv.Snapshot(ctx, e.handle)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"ref": snap.Ref})
	default:
		http.Error(w, "unknown op", http.StatusBadRequest)
	}
}

func (s *Server) register(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("session")
	if _, ok := s.reg.get(id); !ok {
		http.Error(w, "unknown session", http.StatusNotFound)
		return
	}
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	c.SetReadLimit(16 << 20)
	hub := relay.NewHub(r.Context(), relay.WSConn(c))
	if !s.reg.setHub(id, hub) {
		// The entry vanished between our existence check above and now — a
		// concurrent DELETE raced this dial-in (session torn down while its
		// container was still booting). No registry entry will ever exist to
		// reap this hub later, so close it now rather than leak its readLoop
		// goroutine and the underlying fd.
		hub.Close()
		return
	}
	log.Printf("session %s registered", id)
	// Block on the hub's own liveness signal, not r.Context(). websocket.Accept
	// hijacks the connection for HTTP/1.1, and net/http only cancels
	// r.Context() when this handler itself returns (conn.serve's deferred
	// w.cancelCtx) or the server's base context is canceled — the stdlib's
	// background-read-based "cancel ctx on peer close" watcher is explicitly
	// stopped by Hijack(), so r.Context() never reflects sessiond's socket
	// actually dying. hub.Done() does: it closes both when hub.readLoop
	// notices h.conn.Read fail (container/network death — readLoop already
	// cancels the hub's own ctx and tears down every attached client) and
	// when sessionOp's DELETE branch calls hub.Close() directly (deliberate
	// teardown). Either way, unblocking here is what lets us actually remove
	// the now-dead entry and let this goroutine (and its fd) go — leaving
	// this on r.Context() instead would leak both per session, forever, on
	// every abrupt death or explicit rm.
	<-hub.Done()
	s.reg.remove(id)
	hub.Close()
}

func (s *Server) attach(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("session")
	since, _ := strconv.ParseUint(r.URL.Query().Get("since"), 10, 64)

	// Wait briefly for the session to register (container may still be
	// booting). Reads hub through the registry lock (registry.hub), not by
	// dereferencing a get()-returned pointer's .hub field directly — the
	// latter would race against register's setHub call on the connection's
	// own goroutine.
	var hub *relay.Hub
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if h, ok := s.reg.hub(id); ok {
			hub = h
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if hub == nil {
		http.Error(w, "session not registered", http.StatusServiceUnavailable)
		return
	}

	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer c.CloseNow()
	c.SetReadLimit(16 << 20)
	// The client speaks wire.ClientMsg/ServerMsg; the hub forwards raw payloads.
	// The relay expects the first client frame to be a resize (like Plan 1 serve);
	// rattach sends it. cols/rows for the FrameOpen come from that first message.
	first, err := readFirstResize(r.Context(), c)
	if err != nil {
		c.CloseNow()
		return
	}
	hub.AttachClient(r.Context(), relay.WSConn(c), since, first.Cols, first.Rows)
}

// readFirstResize reads exactly one wire.ClientMsg off a freshly attached
// client's websocket and requires it to be a "resize" — mirroring Plan 1's
// resize-first contract (internal/server/server.go's serve()) so a client
// relayed through runnerd and one attached directly to sessiond behave
// identically.
//
// Its cols/rows size the FrameOpen sent to the session (which applies them
// via session.Attach), so this message must be consumed here and NOT also
// forwarded as a FrameClient once hub.AttachClient starts pumping: the
// FrameOpen already conveys the size, and re-sending it would double-deliver
// the same resize. Every resize after this first one flows normally, as a
// FrameClient carrying a "resize" ClientMsg.
func readFirstResize(ctx context.Context, c *websocket.Conn) (wire.ClientMsg, error) {
	var m wire.ClientMsg
	if err := wsjson.Read(ctx, c, &m); err != nil {
		return wire.ClientMsg{}, err
	}
	if m.Type != "resize" {
		return wire.ClientMsg{}, fmt.Errorf("first attach message must be resize, got %q", m.Type)
	}
	return m, nil
}

func (s *Server) pushEgress(session string, hosts []string) error {
	if s.egressAdmin == "" {
		return nil // egress optional in unit tests
	}
	q := "session=" + session
	for _, h := range hosts {
		q += "&host=" + h
	}
	resp, err := http.Post(s.egressAdmin+"/allow?"+q, "application/json", nil)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

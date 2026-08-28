// internal/runnerd/runnerd.go
package runnerd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
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
	seq         int
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

func (s *Server) newID() string { s.seq++; return "sess-" + strconv.Itoa(s.seq) }

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
		h, err := s.drv.Create(r.Context(), spec)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		s.reg.put(id, &sessionEntry{id: id, handle: h.ID, state: "running", allow: body.EgressAllow})
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
		s.drv.Destroy(ctx, e.handle)
		s.reg.remove(id)
		w.WriteHeader(http.StatusNoContent)
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
	s.reg.setHub(id, hub)
	// Block until the session conn closes, keeping this handler goroutine
	// (and therefore the hub it owns) alive for the container's lifetime.
	//
	// KNOWN LIMITATION: r.Context() is canceled by net/http only when this
	// handler returns (see conn.serve's deferred w.cancelCtx) or the server's
	// base context is canceled — for a hijacked connection (which
	// websocket.Accept performs under the hood for HTTP/1.1), the stdlib's
	// background-read-based "cancel on peer close" watcher is explicitly
	// stopped by Hijack(). So if the container dies and sessiond's socket
	// drops out from under hub.readLoop (which does notice — it calls
	// h.cancel() on its own derived context and tears down every attached
	// client), this handler itself keeps blocking here: the registry entry
	// is never removed and this goroutine is never reclaimed. Fixing that
	// needs an independent liveness signal (e.g. a Ping loop like server.go's
	// viewer-liveness ping) since relay.Hub exposes no "session conn died"
	// signal to key off directly; left as a v0 gap — the only way this
	// unblocks today is r.Context() actually canceling (server shutdown).
	<-r.Context().Done()
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

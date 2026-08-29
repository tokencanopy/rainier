// internal/controld/attach_test.go
package controld

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"rainier/internal/driver"
	"rainier/internal/relay"
	"rainier/internal/runnerd"
	"rainier/internal/rwire"
	"rainier/internal/wire"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// newAttachControld is newTestControld with one difference the attach plane
// requires: ExternalURL names the httptest listener this Server is actually
// served on, because the runner dials it back (dial_attach's target_url is
// derived from it). That means the listener has to exist before New
// validates the config, so the server is built unstarted and started once the
// handler is known.
func newAttachControld(t *testing.T, opts ...func(*Config)) (*Server, Store, *httptest.Server) {
	t.Helper()
	st := NewMemStore()
	ts := httptest.NewUnstartedServer(nil)
	cfg := Config{
		RunnerToken: testRunnerToken,
		ExternalURL: "http://" + ts.Listener.Addr().String(),
		OpTimeout:   2 * time.Second,
	}
	for _, o := range opts {
		o(&cfg)
	}
	s, err := New(st, cfg)
	if err != nil {
		ts.Listener.Close()
		t.Fatalf("New: %v", err)
	}
	ts.Config.Handler = s.Handler()
	ts.Start()
	// Registered first so it runs LAST (cleanups are LIFO): every socket a
	// test opens against this server is closed before the server itself is.
	t.Cleanup(ts.Close)
	return s, st, ts
}

func wsBase(ts *httptest.Server) string { return "ws" + strings.TrimPrefix(ts.URL, "http") }

// dialAttach performs the client-side attach dial with the given bearer
// (empty ⇒ no Authorization header), returning the HTTP response so tests can
// assert on the pre-upgrade status and error envelope.
func dialAttach(t *testing.T, ts *httptest.Server, id, query, token string) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	hdr := http.Header{}
	if token != "" {
		hdr.Set("Authorization", "Bearer "+token)
	}
	return websocket.Dial(ctx, wsBase(ts)+"/v1/sessions/"+id+"/attach"+query,
		&websocket.DialOptions{HTTPHeader: hdr})
}

// dialAttachBack performs the runner-side dial-back, the same way runnerd's
// agent does.
func dialAttachBack(t *testing.T, ts *httptest.Server, attachID, token string) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	hdr := http.Header{}
	if token != "" {
		hdr.Set("Authorization", "Bearer "+token)
	}
	return websocket.Dial(ctx, wsBase(ts)+"/v1/runners/attach-back?attach_id="+attachID,
		&websocket.DialOptions{HTTPHeader: hdr})
}

// assertErrCode reads the error envelope off a rejected dial's response.
// coder/websocket keeps the first 1KB of a failed handshake's body available,
// which is more than any envelope this API writes.
func assertErrCode(t *testing.T, resp *http.Response, want string) {
	t.Helper()
	if resp == nil {
		t.Fatal("no HTTP response to read an error envelope from")
	}
	defer resp.Body.Close()
	var env errorEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("decoding error envelope: %v", err)
	}
	if env.Error.Code != want {
		t.Fatalf("error code = %q, want %q", env.Error.Code, want)
	}
}

// pendingAttaches reports how many pairings the table currently holds, read
// under the table's own lock. In-package test access rather than a
// production accessor nothing but a test would call.
func pendingAttaches(s *Server) int {
	s.attaches.mu.Lock()
	defer s.attaches.mu.Unlock()
	return len(s.attaches.m)
}

func writeClient(t *testing.T, c *websocket.Conn, m wire.ClientMsg) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := wsjson.Write(ctx, c, m); err != nil {
		t.Fatalf("write client msg %q: %v", m.Type, err)
	}
}

func readServer(t *testing.T, c *websocket.Conn) wire.ServerMsg {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var m wire.ServerMsg
	if err := wsjson.Read(ctx, c, &m); err != nil {
		t.Fatalf("read server msg: %v", err)
	}
	return m
}

// snapshotText is what the scripted sessiond replies with on FrameOpen — a
// stand-in for a real terminal's scrollback replay.
const snapshotText = "attach-snapshot"

// fakeSessiond is a scripted sessiond: it dials runnerd's /register and
// speaks relay frames, answering every FrameOpen with a snapshot and echoing
// every stdin ClientMsg back as an output ServerMsg. Everything a test needs
// to synchronize on arrives on a channel — no sleeps.
type fakeSessiond struct {
	conn    relay.Conn
	opens   chan relay.Frame
	resizes chan wire.ClientMsg
	closes  chan uint64
}

func startFakeSessiond(t *testing.T, ctx context.Context, wsBase, id string) *fakeSessiond {
	t.Helper()
	c, _, err := websocket.Dial(ctx, wsBase+"/register?session="+id, nil)
	if err != nil {
		t.Fatalf("dial runnerd /register: %v", err)
	}
	c.SetReadLimit(16 << 20)
	t.Cleanup(func() { c.CloseNow() })
	fs := &fakeSessiond{
		conn:    relay.WSConn(c),
		opens:   make(chan relay.Frame, 8),
		resizes: make(chan wire.ClientMsg, 8),
		closes:  make(chan uint64, 8),
	}
	go fs.serve(ctx)
	return fs
}

// serve is the fake's single reader and single writer, so its replies need no
// lock of their own.
func (fs *fakeSessiond) serve(ctx context.Context) {
	for {
		raw, err := fs.conn.Read(ctx)
		if err != nil {
			return
		}
		f, err := relay.Decode(raw)
		if err != nil {
			continue
		}
		switch f.Type {
		case relay.FrameOpen:
			fs.opens <- f
			fs.send(ctx, f.AttachID, wire.ServerMsg{
				Type: "snapshot", Seq: 1, Cols: f.Cols, Rows: f.Rows, Data: []byte(snapshotText),
			})
		case relay.FrameClient:
			var m wire.ClientMsg
			if json.Unmarshal(f.Payload, &m) != nil {
				continue
			}
			switch m.Type {
			case "stdin":
				fs.send(ctx, f.AttachID, wire.ServerMsg{Type: "output", Data: m.Data})
			case "resize":
				fs.resizes <- m
			}
		case relay.FrameClose:
			fs.closes <- f.AttachID
		}
	}
}

func (fs *fakeSessiond) send(ctx context.Context, attachID uint64, m wire.ServerMsg) {
	payload, err := json.Marshal(m)
	if err != nil {
		return
	}
	raw, err := relay.Encode(relay.Frame{Type: relay.FrameServer, AttachID: attachID, Payload: payload})
	if err != nil {
		return
	}
	fs.conn.Write(ctx, raw)
}

func (fs *fakeSessiond) nextOpen(t *testing.T) relay.Frame {
	t.Helper()
	select {
	case f := <-fs.opens:
		return f
	case <-time.After(5 * time.Second):
		t.Fatal("no FrameOpen reached the session within 5s")
		return relay.Frame{}
	}
}

func (fs *fakeSessiond) nextClose(t *testing.T) uint64 {
	t.Helper()
	select {
	case id := <-fs.closes:
		return id
	case <-time.After(5 * time.Second):
		t.Fatal("no FrameClose reached the session within 5s")
		return 0
	}
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

// TestAttachEndToEnd is the whole terminal plane in one process: a client's
// websocket at controld, paired with a real runnerd's outbound dial-back,
// spliced onto a relay.Hub over a scripted sessiond. It asserts the three
// things the plane exists to do — the snapshot reaches the client, stdin
// reaches the session and comes back, and closing either end tears the other
// down — plus the resize-first contract (the first resize sizes the
// FrameOpen and is NOT forwarded again as a client frame).
func TestAttachEndToEnd(t *testing.T) {
	s, st, ts := newAttachControld(t)
	_, tok := loginUser(t, st, "alice", "member")

	// A real runnerd: fake driver, its own HTTP surface for the session's
	// /register dial-in, and RunAgent holding the control conn to controld.
	rd := runnerd.New(driver.NewFake(4), "", "")
	rsrv := httptest.NewServer(rd.Handler())
	t.Cleanup(rsrv.Close)
	rbase := "ws" + strings.TrimPrefix(rsrv.URL, "http")

	id := "sess_e2e"
	// Both sides know about the session before the control conn comes up:
	// the announce is truth, so a row the runner doesn't announce would be
	// marked dead and a session controld doesn't have would be destroyed as
	// an orphan.
	seedSession(t, st, Session{ID: id, State: StateRunning, Runner: "vm1"})
	if err := rd.CreateWithID(context.Background(), id, driver.Spec{Image: "img"}, nil); err != nil {
		t.Fatalf("runnerd CreateWithID: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go rd.RunAgent(ctx, runnerd.AgentConfig{ControldURL: wsBase(ts), Token: testRunnerToken, RunnerName: "vm1"})
	waitConnected(t, s, "vm1")

	sd := startFakeSessiond(t, ctx, rbase, id)

	cli, resp, err := dialAttach(t, ts, id, "?since=7", tok)
	if err != nil {
		t.Fatalf("dial attach: %v", err)
	}
	defer cli.CloseNow()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("attach status = %d, want 101", resp.StatusCode)
	}
	cli.SetReadLimit(16 << 20)
	writeClient(t, cli, wire.ClientMsg{Type: "resize", Cols: 120, Rows: 40})

	// The FrameOpen carries what the client asked for, all the way through
	// controld's dial_attach and runnerd's AttachClient.
	open := sd.nextOpen(t)
	if open.Since != 7 || open.Cols != 120 || open.Rows != 40 {
		t.Fatalf("FrameOpen = since:%d cols:%d rows:%d, want since:7 cols:120 rows:40",
			open.Since, open.Cols, open.Rows)
	}

	if m := readServer(t, cli); m.Type != "snapshot" || string(m.Data) != snapshotText {
		t.Fatalf("first server msg = %+v, want a snapshot carrying %q", m, snapshotText)
	}

	writeClient(t, cli, wire.ClientMsg{Type: "stdin", Data: []byte("hi")})
	if m := readServer(t, cli); m.Type != "output" || string(m.Data) != "hi" {
		t.Fatalf("echo = %+v, want output %q", m, "hi")
	}

	// The resize that sized the FrameOpen must not also have been forwarded
	// as a client frame: frames are delivered in order, so the echo above
	// having arrived proves any forwarded resize would already be here.
	select {
	case m := <-sd.resizes:
		t.Fatalf("first resize was forwarded to the session as well: %+v", m)
	default:
	}

	// Closing the client cascades: the splice closes the dial-back socket,
	// runnerd's AttachClient sees its client die, and the session is told
	// the attachment is over.
	cli.CloseNow()
	if got := sd.nextClose(t); got != open.AttachID {
		t.Fatalf("FrameClose for attach %d, want %d", got, open.AttachID)
	}
	eventually(t, 3*time.Second, func() error {
		if n := pendingAttaches(s); n != 0 {
			return fmt.Errorf("pairing table still holds %d entries", n)
		}
		return nil
	})
}

func TestAttachRequiresAuth(t *testing.T) {
	_, st, ts := newAttachControld(t)
	seedSession(t, st, Session{ID: "sess_auth", State: StateRunning, Runner: "vm1"})

	for _, tc := range []struct {
		name  string
		token string
	}{
		{"no bearer", ""},
		{"unknown token", "rnr_not_a_real_token"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, resp, err := dialAttach(t, ts, "sess_auth", "", tc.token)
			if err == nil {
				c.CloseNow()
				t.Fatal("dial succeeded, want rejection before upgrade")
			}
			if resp == nil {
				t.Fatalf("no HTTP response: %v", err)
			}
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", resp.StatusCode)
			}
			assertErrCode(t, resp, "unauthenticated")
		})
	}
}

// TestAttachWrongState pins the pre-upgrade rejections: a session that isn't
// running (and isn't going to be within the wait budget) is a 503
// session_not_ready, a session that doesn't exist is a 404, and a running
// session whose runner is gone is a 502 — all answered as HTTP, before the
// socket is ever upgraded.
func TestAttachWrongState(t *testing.T) {
	const wait = 100 * time.Millisecond
	_, st, ts := newAttachControld(t, func(c *Config) { c.AttachWait = wait })
	_, tok := loginUser(t, st, "alice", "member")

	t.Run("queued waits the budget then 503", func(t *testing.T) {
		seedSession(t, st, Session{ID: "sess_queued", State: StateQueued})
		start := time.Now()
		c, resp, err := dialAttach(t, ts, "sess_queued", "", tok)
		if err == nil {
			c.CloseNow()
			t.Fatal("dial succeeded, want rejection before upgrade")
		}
		if resp == nil {
			t.Fatalf("no HTTP response: %v", err)
		}
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", resp.StatusCode)
		}
		assertErrCode(t, resp, "session_not_ready")
		if elapsed := time.Since(start); elapsed < wait {
			t.Fatalf("answered after %s, want at least the %s wait budget", elapsed, wait)
		}
	})

	t.Run("terminal 503s without waiting", func(t *testing.T) {
		seedSession(t, st, Session{ID: "sess_dead", State: StateDead})
		_, resp, err := dialAttach(t, ts, "sess_dead", "", tok)
		if err == nil {
			t.Fatal("dial succeeded, want rejection before upgrade")
		}
		if resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("resp = %+v (%v), want 503", resp, err)
		}
		assertErrCode(t, resp, "session_not_ready")
	})

	t.Run("unknown session 404s", func(t *testing.T) {
		_, resp, err := dialAttach(t, ts, "sess_nope", "", tok)
		if err == nil {
			t.Fatal("dial succeeded, want rejection before upgrade")
		}
		if resp == nil || resp.StatusCode != http.StatusNotFound {
			t.Fatalf("resp = %+v (%v), want 404", resp, err)
		}
		assertErrCode(t, resp, "not_found")
	})

	t.Run("running on a disconnected runner is 502", func(t *testing.T) {
		seedSession(t, st, Session{ID: "sess_gone_runner", State: StateRunning, Runner: "vm-gone"})
		_, resp, err := dialAttach(t, ts, "sess_gone_runner", "", tok)
		if err == nil {
			t.Fatal("dial succeeded, want rejection before upgrade")
		}
		if resp == nil || resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("resp = %+v (%v), want 502", resp, err)
		}
		assertErrCode(t, resp, "runner_unreachable")
	})
}

// TestAttachBackBogusID pins the dial-back's own guards: an unknown (expired,
// or never-issued) attach_id is refused, a bad runner token is refused, and
// neither takes the server with it.
func TestAttachBackBogusID(t *testing.T) {
	_, _, ts := newAttachControld(t)

	t.Run("unknown attach id", func(t *testing.T) {
		c, resp, err := dialAttachBack(t, ts, "deadbeefdeadbeef", testRunnerToken)
		if err == nil {
			c.CloseNow()
			t.Fatal("dial-back for an unknown attach_id succeeded")
		}
		if resp == nil || resp.StatusCode != http.StatusNotFound {
			t.Fatalf("resp = %+v (%v), want 404", resp, err)
		}
		assertErrCode(t, resp, "not_found")
	})

	t.Run("wrong runner token", func(t *testing.T) {
		c, resp, err := dialAttachBack(t, ts, "deadbeefdeadbeef", "rnr_wrong")
		if err == nil {
			c.CloseNow()
			t.Fatal("dial-back with a wrong token succeeded")
		}
		if resp == nil || resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("resp = %+v (%v), want 401", resp, err)
		}
	})

	// Nothing above should have wedged or killed the server.
	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz after bogus dial-backs: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz = %d after bogus dial-backs, want 200", resp.StatusCode)
	}
}

// TestAttachRequiresResizeFirst pins the other half of the resize-first
// contract: a client whose first frame is not a resize is closed, and no
// pairing is ever created for it — the size the FrameOpen needs comes from
// that message and there is nothing to fall back to.
func TestAttachRequiresResizeFirst(t *testing.T) {
	s, st, ts := newAttachControld(t)
	_, tok := loginUser(t, st, "alice", "member")

	f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
		Sessions: []rwire.SessionInfo{{ID: ghostSession, State: "running"}}})
	waitConnected(t, s, "vm1")
	awaitReconciled(t, f)

	id := "sess_no_resize"
	seedSession(t, st, Session{ID: id, State: StateRunning, Runner: "vm1"})

	cli, _, err := dialAttach(t, ts, id, "", tok)
	if err != nil {
		t.Fatalf("dial attach: %v", err)
	}
	defer cli.CloseNow()
	writeClient(t, cli, wire.ClientMsg{Type: "stdin", Data: []byte("oops")})

	readCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, _, err := cli.Read(readCtx); err == nil {
		t.Fatal("client socket still readable after a non-resize first message; want it closed")
	}
	// The close happens before any dial_attach could be sent, so anything the
	// runner was going to receive is already in the channel by now.
	select {
	case cmd := <-f.cmds:
		t.Fatalf("runner was asked to dial back for a rejected attach: %+v", cmd)
	default:
	}
	if n := pendingAttaches(s); n != 0 {
		t.Fatalf("pairing table holds %d entries after a rejected attach, want 0", n)
	}
}

// TestPairingTTL pins the case the design's TTL exists for: the runner takes
// the dial_attach and never dials back (it died, or the command was lost).
// The parked client must be closed rather than left waiting on a terminal
// that will never speak, and its table entry must go with it. It also pins
// the dial_attach message itself — the only place the wire shape controld
// sends is asserted.
func TestPairingTTL(t *testing.T) {
	const ttl = 250 * time.Millisecond
	s, st, ts := newAttachControld(t, func(c *Config) { c.AttachPairTTL = ttl })
	_, tok := loginUser(t, st, "alice", "member")

	// The ghost's destroy proves reconciliation has finished, so the session
	// seeded after it can't be swept by the announce that ran first.
	f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
		Sessions: []rwire.SessionInfo{{ID: ghostSession, State: "running"}}})
	waitConnected(t, s, "vm1")
	awaitReconciled(t, f)

	id := "sess_ttl"
	seedSession(t, st, Session{ID: id, State: StateRunning, Runner: "vm1"})

	cli, _, err := dialAttach(t, ts, id, "?since=3", tok)
	if err != nil {
		t.Fatalf("dial attach: %v", err)
	}
	defer cli.CloseNow()
	writeClient(t, cli, wire.ClientMsg{Type: "resize", Cols: 100, Rows: 30})

	cmd := f.nextCmd(t)
	if cmd.Type != "dial_attach" || cmd.Session != id || cmd.Attach == nil {
		t.Fatalf("command = %+v, want dial_attach for %s with an attach block", cmd, id)
	}
	at := cmd.Attach
	if len(at.AttachID) != 16 {
		t.Fatalf("attach_id = %q, want 16 hex characters", at.AttachID)
	}
	if _, err := hex.DecodeString(at.AttachID); err != nil {
		t.Fatalf("attach_id = %q, want hex: %v", at.AttachID, err)
	}
	if at.Since != 3 || at.Cols != 100 || at.Rows != 30 {
		t.Fatalf("attach = since:%d cols:%d rows:%d, want since:3 cols:100 rows:30", at.Since, at.Cols, at.Rows)
	}
	want := wsBase(ts) + "/v1/runners/attach-back?attach_id=" + at.AttachID
	if at.TargetURL != want {
		t.Fatalf("target_url = %q, want %q", at.TargetURL, want)
	}

	// The fake runner deliberately drops the command: the TTL is the only
	// thing that can free this client now.
	readCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	if _, _, err := cli.Read(readCtx); err == nil {
		t.Fatal("client socket still readable after the pairing TTL; want it closed")
	}
	if elapsed := time.Since(start); elapsed < ttl/2 {
		t.Fatalf("client closed after %s, want it to have waited out the %s TTL", elapsed, ttl)
	}
	eventually(t, 3*time.Second, func() error {
		if s.attaches.has(at.AttachID) {
			return fmt.Errorf("pairing %s still in the table after its TTL", at.AttachID)
		}
		return nil
	})
}

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

	"github.com/tokencanopy/rainier/internal/driver"
	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/internal/runnerd"
	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/terminal"
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
		SecretsKey:  testSecretsKey,
		// Matching newTestControld's, and for the same reason: no test built on
		// this helper asserts an OpTimeout, so a tight one is only a stopwatch
		// over a websocket round trip. The attach tests that DO measure
		// something measure AttachWait, which they pass explicitly.
		OpTimeout: 10 * time.Second,
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
	return websocket.Dial(ctx, wsBase(ts)+"/v0/sessions/"+id+"/attach"+query,
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
	return websocket.Dial(ctx, wsBase(ts)+"/v0/runners/attach-back?attach_id="+attachID,
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

func writeClient(t *testing.T, c *websocket.Conn, m terminal.ClientMessage) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := wsjson.Write(ctx, c, m); err != nil {
		t.Fatalf("write client msg %q: %v", m.Type, err)
	}
}

func readServer(t *testing.T, c *websocket.Conn) terminal.ServerMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var m terminal.ServerMessage
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
	raw     *websocket.Conn
	conn    relay.Conn
	opens   chan relay.Frame
	resizes chan terminal.ClientMessage
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
		raw:     c,
		conn:    relay.WSConn(c),
		opens:   make(chan relay.Frame, 8),
		resizes: make(chan terminal.ClientMessage, 8),
		closes:  make(chan uint64, 8),
	}
	go fs.serve(ctx)
	return fs
}

// die kills the session conn the way a container dying does: no close
// handshake, just a socket that stops existing.
func (fs *fakeSessiond) die() { fs.raw.CloseNow() }

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
			fs.send(ctx, f.AttachID, terminal.ServerMessage{
				Type: "snapshot", Seq: 1, Cols: f.Cols, Rows: f.Rows, Data: []byte(snapshotText),
			})
		case relay.FrameClient:
			var m terminal.ClientMessage
			if json.Unmarshal(f.Payload, &m) != nil {
				continue
			}
			switch m.Type {
			case "stdin":
				fs.send(ctx, f.AttachID, terminal.ServerMessage{Type: "output", Data: m.Data})
			case "resize":
				fs.resizes <- m
			}
		case relay.FrameClose:
			fs.closes <- f.AttachID
		}
	}
}

func (fs *fakeSessiond) send(ctx context.Context, attachID uint64, m terminal.ServerMessage) {
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
// end-to-end fixture
// ---------------------------------------------------------------------------

// attachFixture is the whole terminal plane wired up in one process:
// controld over a memstore, a real runnerd (fake driver) holding a control
// conn to it, and a scripted sessiond registered for one running session the
// logged-in user owns.
type attachFixture struct {
	s   *Server
	st  Store
	ts  *httptest.Server
	sd  *fakeSessiond
	tok string
	id  string
}

func newAttachFixture(t *testing.T) *attachFixture {
	t.Helper()
	s, st, ts := newAttachControld(t)
	u, tok := loginUser(t, st, "alice", "member")

	// A real runnerd: fake driver, its own HTTP surface for the session's
	// /register dial-in, and RunAgent holding the control conn to controld.
	rd := runnerd.New(driver.NewFake(4), "", "", "")
	rsrv := httptest.NewServer(rd.Handler())
	t.Cleanup(rsrv.Close)
	rbase := "ws" + strings.TrimPrefix(rsrv.URL, "http")

	id := "sess_e2e"
	// Both sides know about the session before the control conn comes up:
	// the announce is truth, so a row the runner doesn't announce would be
	// marked dead and a session controld doesn't have would be destroyed as
	// an orphan.
	seedSession(t, st, Session{ID: id, OwnerID: u.ID, State: StateRunning, Runner: "vm1"})
	if err := rd.CreateWithID(context.Background(), id, driver.Spec{Image: "img"}, nil); err != nil {
		t.Fatalf("runnerd CreateWithID: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go rd.RunAgent(ctx, runnerd.AgentConfig{ControldURL: wsBase(ts), Token: testRunnerToken, RunnerName: "vm1"})
	waitConnected(t, s, "vm1")

	return &attachFixture{s: s, st: st, ts: ts, sd: startFakeSessiond(t, ctx, rbase, id), tok: tok, id: id}
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

// TestAttachEndToEnd is the whole terminal plane in one process: a client's
// websocket at controld, paired with a real runnerd's outbound dial-back,
// spliced onto a relay.Hub over a scripted sessiond. It asserts the three
// things the plane exists to do — the snapshot reaches the client, stdin
// reaches the session and comes back, and closing the client tears the
// session-side attachment down — plus the resize-first contract (the first
// resize sizes the FrameOpen and is NOT forwarded again as a client frame).
func TestAttachEndToEnd(t *testing.T) {
	fx := newAttachFixture(t)
	s, ts, sd := fx.s, fx.ts, fx.sd

	cli, resp, err := dialAttach(t, ts, fx.id, "?since=7", fx.tok)
	if err != nil {
		t.Fatalf("dial attach: %v", err)
	}
	defer cli.CloseNow()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("attach status = %d, want 101", resp.StatusCode)
	}
	cli.SetReadLimit(16 << 20)
	writeClient(t, cli, terminal.ClientMessage{Type: "resize", Cols: 120, Rows: 40})

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

	writeClient(t, cli, terminal.ClientMessage{Type: "stdin", Data: []byte("hi")})
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

// TestAttachSessionDeathCascadesToClient is TestAttachEndToEnd's cascade run
// in the other direction: the container (its sessiond conn) dies mid-attach,
// and the viewer must find out. The chain under test is relay.Hub's readLoop
// closing every attached client → runnerd's dial-back socket dying →
// controld's splice closing the client — three processes' worth of teardown
// that has to complete for a client not to sit on a dead terminal forever.
func TestAttachSessionDeathCascadesToClient(t *testing.T) {
	fx := newAttachFixture(t)

	cli, _, err := dialAttach(t, fx.ts, fx.id, "", fx.tok)
	if err != nil {
		t.Fatalf("dial attach: %v", err)
	}
	defer cli.CloseNow()
	cli.SetReadLimit(16 << 20)
	writeClient(t, cli, terminal.ClientMessage{Type: "resize", Cols: 80, Rows: 24})

	// Wait for the pipe to be demonstrably live before killing it, so a
	// failure below is unambiguously about the cascade, not the plumbing.
	fx.sd.nextOpen(t)
	if m := readServer(t, cli); m.Type != "snapshot" {
		t.Fatalf("first server msg = %+v, want a snapshot", m)
	}

	fx.sd.die()

	readCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, _, err := cli.Read(readCtx); err == nil {
		t.Fatal("client socket still readable after the session conn died; want it closed")
	} else if readCtx.Err() != nil {
		t.Fatalf("client socket never closed after the session conn died: %v", err)
	}
	eventually(t, 3*time.Second, func() error {
		if n := pendingAttaches(fx.s); n != 0 {
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

// TestAttachAuthorization pins §4.4's owner-or-admin rule on the one route
// that carries stdin: a teammate who does not own the session cannot type
// into it, an admin can, and the refusal comes before the bounded wait so an
// unauthorized caller can neither occupy a slot nor time the answer to learn
// anything about a session that isn't theirs.
func TestAttachAuthorization(t *testing.T) {
	// A wait long enough that an authz check hiding behind it would be
	// obvious: the 403 below has to come back immediately.
	s, st, ts := newAttachControld(t, func(c *Config) { c.AttachWait = 30 * time.Second })
	owner, _ := loginUser(t, st, "alice", "member")
	_, otherTok := loginUser(t, st, "bob", "member")
	_, adminTok := loginUser(t, st, "root", "admin")

	f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
		Sessions: []runner.SessionInfo{{ID: ghostSession, State: "running"}}})
	waitConnected(t, s, "vm1")
	awaitReconciled(t, f)

	id := "sess_owned_by_alice"
	seedSession(t, st, Session{ID: id, OwnerID: owner.ID, State: StateRunning, Runner: "vm1"})
	// A session that will never reach `running`: an authorization check
	// placed after the wait would answer this one 503 in 30s instead of 403
	// at once, which is exactly what the timing bound below catches.
	queued := "sess_queued_by_alice"
	seedSession(t, st, Session{ID: queued, OwnerID: owner.ID, State: StateQueued})

	for _, tc := range []struct{ name, session string }{
		{"non-owner member is refused", id},
		{"refused before the wait, not behind it", queued},
	} {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Now()
			c, resp, err := dialAttach(t, ts, tc.session, "", otherTok)
			if err == nil {
				c.CloseNow()
				t.Fatal("a non-owner's attach succeeded")
			}
			if resp == nil || resp.StatusCode != http.StatusForbidden {
				t.Fatalf("resp = %+v (%v), want 403", resp, err)
			}
			assertErrCode(t, resp, "forbidden")
			if elapsed := time.Since(start); elapsed > 5*time.Second {
				t.Fatalf("403 took %s: authorization must not be gated behind the attach wait", elapsed)
			}
			if n := pendingAttaches(s); n != 0 {
				t.Fatalf("pairing table holds %d entries after a refused attach, want 0", n)
			}
		})
	}

	t.Run("admin attaches another user's session", func(t *testing.T) {
		cli, resp, err := dialAttach(t, ts, id, "", adminTok)
		if err != nil {
			t.Fatalf("admin attach: %v", err)
		}
		defer cli.CloseNow()
		if resp.StatusCode != http.StatusSwitchingProtocols {
			t.Fatalf("admin attach status = %d, want 101", resp.StatusCode)
		}
		writeClient(t, cli, terminal.ClientMessage{Type: "resize", Cols: 90, Rows: 20})
		cmd := f.nextCmd(t)
		if cmd.Type != "dial_attach" || cmd.Session != id {
			t.Fatalf("command = %+v, want dial_attach for %s", cmd, id)
		}
	})
}

// TestAttachWrongState pins the pre-upgrade rejections: a session that isn't
// running (and isn't going to be within the wait budget) is a 503
// session_not_ready, a session that doesn't exist is a 404, and a running
// session whose runner is gone is a 502 — all answered as HTTP, before the
// socket is ever upgraded.
func TestAttachWrongState(t *testing.T) {
	const wait = 100 * time.Millisecond
	_, st, ts := newAttachControld(t, func(c *Config) { c.AttachWait = wait })
	u, tok := loginUser(t, st, "alice", "member")

	t.Run("queued waits the budget then 503", func(t *testing.T) {
		seedSession(t, st, Session{ID: "sess_queued", OwnerID: u.ID, State: StateQueued})
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
		seedSession(t, st, Session{ID: "sess_dead", OwnerID: u.ID, State: StateDead})
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
		seedSession(t, st, Session{ID: "sess_gone_runner", OwnerID: u.ID, State: StateRunning, Runner: "vm-gone"})
		_, resp, err := dialAttach(t, ts, "sess_gone_runner", "", tok)
		if err == nil {
			t.Fatal("dial succeeded, want rejection before upgrade")
		}
		if resp == nil || resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("resp = %+v (%v), want 502", resp, err)
		}
		assertErrCode(t, resp, "runner_unreachable")
	})

	t.Run("failed on a disconnected runner is still 503", func(t *testing.T) {
		// The failed-session exemption is not a blanket one: with no control
		// connection there is nothing to send a dial_attach down, so this is
		// the ordinary not-ready answer rather than a socket that upgrades and
		// then dies.
		seedSession(t, st, Session{ID: "sess_failed_gone", OwnerID: u.ID, State: StateFailed, Runner: "vm-gone"})
		_, resp, err := dialAttach(t, ts, "sess_failed_gone", "", tok)
		if err == nil {
			t.Fatal("dial succeeded, want rejection before upgrade")
		}
		if resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("resp = %+v (%v), want 503", resp, err)
		}
		assertErrCode(t, resp, "session_not_ready")
	})

	t.Run("failed with no runner at all is 503", func(t *testing.T) {
		seedSession(t, st, Session{ID: "sess_failed_unplaced", OwnerID: u.ID, State: StateFailed})
		_, resp, err := dialAttach(t, ts, "sess_failed_unplaced", "", tok)
		if err == nil {
			t.Fatal("dial succeeded, want rejection before upgrade")
		}
		if resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("resp = %+v (%v), want 503", resp, err)
		}
		assertErrCode(t, resp, "session_not_ready")
	})
}

// TestAttachToFailedSession is the debugging path a failed setup exists to
// leave behind (design §4.3): the session's error column carries only the last
// 2KB the script printed, while the whole log is still inside the container,
// where sessiond is running and serving viewers. Attach has to reach it — with
// the row terminal, which every other terminal state is refused for.
//
// The session is failed AFTER the runner has announced, which is the order
// production produces (a setup_failed event arrives on a live control conn) and
// the only order that works: an announce carrying a session the store already
// has terminal is an orphan, and reconciliation destroys it (§4.8).
func TestAttachToFailedSession(t *testing.T) {
	fx := newAttachFixture(t)
	ts, sd := fx.ts, fx.sd

	reason := "setup failed: rc 7: E: unable to locate package foo"
	err := fx.st.Transition(context.Background(), fx.id, NonTerminal, StateFailed, TransitionOpts{Error: &reason})
	if err != nil {
		t.Fatalf("failing the session: %v", err)
	}

	// No wait budget is consumed: failed-with-a-connected-runner is as settled
	// an answer as running, and the diagnosis is on the other side of it.
	start := time.Now()
	cli, resp, err := dialAttach(t, ts, fx.id, "?since=0", fx.tok)
	if err != nil {
		t.Fatalf("attach to a failed session: %v", err)
	}
	defer cli.CloseNow()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("attach status = %d, want 101", resp.StatusCode)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("attach to a failed session took %s; it must not sit out the wait budget", elapsed)
	}
	cli.SetReadLimit(16 << 20)
	writeClient(t, cli, terminal.ClientMessage{Type: "resize", Cols: 80, Rows: 24})

	// The dial_attach went out and the splice is live: the session's own
	// FrameOpen arrived, and its reply reaches the client. That reply is the
	// scrollback a user attaches to a failed session to read.
	if open := sd.nextOpen(t); open.Since != 0 {
		t.Fatalf("FrameOpen since = %d, want 0 (the full replay a failure needs)", open.Since)
	}
	if m := readServer(t, cli); m.Type != "snapshot" || string(m.Data) != snapshotText {
		t.Fatalf("first server msg = %+v, want the session's snapshot %q", m, snapshotText)
	}

	// The row is untouched by the attach: reading a failure must not resurrect
	// it, and the slot is still held until an explicit rm.
	after, err := fx.st.GetSession(context.Background(), fx.id)
	if err != nil {
		t.Fatal(err)
	}
	if after.State != StateFailed || after.Error != reason {
		t.Fatalf("session after the attach = %s / %q, want it still failed with its error intact", after.State, after.Error)
	}
}

// TestAttachToDeadSessionIsRefused pins the NARROWNESS of the exemption above,
// which TestAttachWrongState cannot: its terminal case has no runner at all, so
// it would pass just as well against a handler that admitted every terminal
// state on a connected runner.
//
// Here the runner is connected and the container is real, and the answer is
// still 503. `dead` is the runner reporting the container gone, and canceled and
// destroyed never had one — only `failed` leaves a container up with a log
// worth reading, so only `failed` is let through.
func TestAttachToDeadSessionIsRefused(t *testing.T) {
	fx := newAttachFixture(t)

	reason := "runner reported dead"
	err := fx.st.Transition(context.Background(), fx.id, NonTerminal, StateDead, TransitionOpts{Error: &reason})
	if err != nil {
		t.Fatalf("killing the session: %v", err)
	}

	c, resp, err := dialAttach(t, fx.ts, fx.id, "", fx.tok)
	if err == nil {
		c.CloseNow()
		t.Fatal("attach to a dead session succeeded; only `failed` is exempt from the terminal rule")
	}
	if resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("resp = %+v (%v), want 503", resp, err)
	}
	assertErrCode(t, resp, "session_not_ready")
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
	u, tok := loginUser(t, st, "alice", "member")

	f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
		Sessions: []runner.SessionInfo{{ID: ghostSession, State: "running"}}})
	waitConnected(t, s, "vm1")
	awaitReconciled(t, f)

	id := "sess_no_resize"
	seedSession(t, st, Session{ID: id, OwnerID: u.ID, State: StateRunning, Runner: "vm1"})

	cli, _, err := dialAttach(t, ts, id, "", tok)
	if err != nil {
		t.Fatalf("dial attach: %v", err)
	}
	defer cli.CloseNow()
	writeClient(t, cli, terminal.ClientMessage{Type: "stdin", Data: []byte("oops")})

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
	u, tok := loginUser(t, st, "alice", "member")

	// The ghost's destroy proves reconciliation has finished, so the session
	// seeded after it can't be swept by the announce that ran first.
	f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
		Sessions: []runner.SessionInfo{{ID: ghostSession, State: "running"}}})
	waitConnected(t, s, "vm1")
	awaitReconciled(t, f)

	id := "sess_ttl"
	seedSession(t, st, Session{ID: id, OwnerID: u.ID, State: StateRunning, Runner: "vm1"})

	cli, _, err := dialAttach(t, ts, id, "?since=3", tok)
	if err != nil {
		t.Fatalf("dial attach: %v", err)
	}
	defer cli.CloseNow()
	writeClient(t, cli, terminal.ClientMessage{Type: "resize", Cols: 100, Rows: 30})

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
	want := wsBase(ts) + "/v0/runners/attach-back?attach_id=" + at.AttachID
	if at.TargetURL != want {
		t.Fatalf("target_url = %q, want %q", at.TargetURL, want)
	}

	// The fake runner deliberately drops the command: the TTL is the only
	// thing that can free this client now.
	readCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, _, err := cli.Read(readCtx); err == nil {
		t.Fatal("client socket still readable after the pairing TTL; want it closed")
	} else if readCtx.Err() != nil {
		t.Fatalf("client socket never closed after the pairing TTL: %v", err)
	}
	eventually(t, 3*time.Second, func() error {
		if s.attaches.has(at.AttachID) {
			return fmt.Errorf("pairing %s still in the table after its TTL", at.AttachID)
		}
		return nil
	})
}

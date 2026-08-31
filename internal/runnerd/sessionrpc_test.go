// internal/runnerd/sessionrpc_test.go
package runnerd

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/tokencanopy/rainier/internal/driver"
	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// ---------------------------------------------------------------------------
// helpers — a runner with both of its neighbours attached
//
// The session RPC only exists as a path THROUGH runnerd, so these tests stand
// up both ends: a fakeControld the agent dials (agent_test.go) and a websocket
// on /register standing in for the sandbox's sessiond. Everything asserted
// below is what one end sees of something the other end sent.
// ---------------------------------------------------------------------------

// sandbox is a scripted sessiond: the /register conn, speaking relay frames.
type sandbox struct {
	c   *websocket.Conn
	ctx context.Context
}

// dialSandbox creates a session on the runner and registers a conn for it,
// exactly as a container's sessiond does at boot.
func dialSandbox(t *testing.T, rd *Server, srv *httptest.Server) (string, *sandbox) {
	t.Helper()
	id := createSession(t, srv.URL)
	base := strings.Replace(srv.URL, "http", "ws", 1)
	ctx := context.Background()
	c, _, err := websocket.Dial(ctx, base+"/register?session="+id, nil)
	if err != nil {
		t.Fatalf("dial /register: %v", err)
	}
	t.Cleanup(func() { c.CloseNow() })
	waitForHub(t, rd, id)
	return id, &sandbox{c: c, ctx: ctx}
}

// send writes one control event up the session's conn, the way sessiond's
// ControlSender does.
func (s *sandbox) send(t *testing.T, ev relay.ControlEvent) {
	t.Helper()
	p, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	f, err := relay.Encode(relay.Frame{Type: relay.FrameControl, Payload: p})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.c.Write(s.ctx, websocket.MessageText, f); err != nil {
		t.Fatalf("write control frame: %v", err)
	}
}

// sendRaw writes a control frame whose payload is not a well-formed event —
// the shapes runnerd must survive rather than route.
func (s *sandbox) sendRaw(t *testing.T, payload string) {
	t.Helper()
	f, err := relay.Encode(relay.Frame{Type: relay.FrameControl, Payload: []byte(payload)})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.c.Write(s.ctx, websocket.MessageText, f); err != nil {
		t.Fatalf("write control frame: %v", err)
	}
}

// nextControl returns the next control event the runner sent down, failing the
// test if none arrives — a bound, so a routing regression reports instead of
// hanging the suite.
func (s *sandbox) nextControl(t *testing.T) relay.ControlEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(s.ctx, 3*time.Second)
	defer cancel()
	for {
		_, raw, err := s.c.Read(ctx)
		if err != nil {
			t.Fatalf("read from runner: %v", err)
		}
		f, err := relay.Decode(raw)
		if err != nil {
			t.Fatalf("decoding frame %s: %v", raw, err)
		}
		if f.Type != relay.FrameControl {
			continue // terminal traffic; not this test's business
		}
		var ev relay.ControlEvent
		if err := json.Unmarshal(f.Payload, &ev); err != nil {
			t.Fatalf("decoding control payload %s: %v", f.Payload, err)
		}
		return ev
	}
}

// runnerWithControld starts a runner serving its HTTP surface and dialing a
// fake controld, returning both ends' handles.
func runnerWithControld(t *testing.T, opts ...func(*Server)) (*Server, *httptest.Server, *fakeConn) {
	t.Helper()
	rd := New(driver.NewFake(4), "", "", "")
	// Applied before anything serves, which is what makes writing these
	// fields safe: no goroutine of this server's exists yet to read them.
	for _, o := range opts {
		o(rd)
	}
	srv := httptest.NewServer(rd.Handler())
	t.Cleanup(srv.Close)

	fc := newFakeControld(t, testToken)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go rd.RunAgent(ctx, AgentConfig{ControldURL: fc.wsURL(), Token: testToken, RunnerName: "vm1"})

	conn := fc.nextConn(t)
	conn.readAnnounce(t)
	return rd, srv, conn
}

// shortHubWait cuts the wait for a session that is never going to register
// down to something a test can sit through.
func shortHubWait(rd *Server) { rd.hubWait = 100 * time.Millisecond }

// nextSessionReq returns the next session_req the runner sent up, skipping the
// events (a registration's "running", say) that share the same socket.
func nextSessionReq(t *testing.T, conn *fakeConn) runner.FromRunner {
	t.Helper()
	for {
		m := conn.readMsg(t)
		if m.Type != "session_req" {
			continue
		}
		if m.RPC == nil {
			t.Fatalf("session_req for %s carried no envelope: %+v", m.Session, m)
		}
		return m
	}
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

// TestSessionRPCReachesTheSandboxAndAnswersBack is the controld-initiated
// direction end to end through the runner: a session_rpc becomes a "req:<m>"
// control frame in the sandbox, and the sandbox's "resp" becomes a session_req
// carrying the same envelope back up. Ids pass through untouched in both
// directions — a response can only ever match the end that originated its
// request, so there is nothing to remap here.
func TestSessionRPCReachesTheSandboxAndAnswersBack(t *testing.T) {
	rd, srv, conn := runnerWithControld(t)
	id, sb := dialSandbox(t, rd, srv)

	conn.send(t, runner.ToRunner{Type: "session_rpc", Session: id,
		RPC: &runner.RPCEnvelope{ID: 42, Method: "diff", Payload: json.RawMessage(`{"repo":"api"}`)}})

	got := sb.nextControl(t)
	if got.Kind != "req:diff" {
		t.Fatalf("kind = %q, want \"req:diff\"", got.Kind)
	}
	if got.ID != 42 {
		t.Fatalf("id = %d, want the controld-assigned 42", got.ID)
	}
	if string(got.Payload) != `{"repo":"api"}` {
		t.Fatalf("payload = %s, want it forwarded verbatim", got.Payload)
	}

	sb.send(t, relay.ControlEvent{Kind: "resp", ID: 42, OK: true, Payload: json.RawMessage(`{"stat":"1 file changed"}`)})

	up := nextSessionReq(t, conn)
	if up.Session != id {
		t.Fatalf("session = %q, want %q — a response with no session has nowhere to be routed", up.Session, id)
	}
	if up.RPC.ID != 42 || up.RPC.Method != "resp" || !up.RPC.OK {
		t.Fatalf("envelope = %+v, want an ok resp for id 42", up.RPC)
	}
	if string(up.RPC.Payload) != `{"stat":"1 file changed"}` {
		t.Fatalf("payload = %s, want it forwarded verbatim", up.RPC.Payload)
	}
}

// TestSandboxRequestReachesControldAndTheAnswerComesDown is the mirror image:
// the sandbox originates (a credential mint), controld answers, and the answer
// lands back in the sandbox as a "resp" with the sandbox's own id.
func TestSandboxRequestReachesControldAndTheAnswerComesDown(t *testing.T) {
	rd, srv, conn := runnerWithControld(t)
	id, sb := dialSandbox(t, rd, srv)

	sb.send(t, relay.ControlEvent{Kind: "req:mint_git_credential", ID: 3})

	up := nextSessionReq(t, conn)
	if up.Session != id {
		t.Fatalf("session = %q, want %q", up.Session, id)
	}
	if up.RPC.ID != 3 || up.RPC.Method != "mint_git_credential" {
		t.Fatalf("envelope = %+v, want mint_git_credential id 3", up.RPC)
	}

	conn.send(t, runner.ToRunner{Type: "session_rpc", Session: id,
		RPC: &runner.RPCEnvelope{ID: 3, Method: "resp", OK: true, Payload: json.RawMessage(`{"token":"t"}`)}})

	down := sb.nextControl(t)
	if down.Kind != "resp" || down.ID != 3 || !down.OK {
		t.Fatalf("event = %+v, want an ok resp for id 3", down)
	}
	if string(down.Payload) != `{"token":"t"}` {
		t.Fatalf("payload = %s, want it forwarded verbatim", down.Payload)
	}
}

// TestSessionRPCForAnUnregisteredSessionIsRefused: a request the runner cannot
// deliver is answered with a failure rather than swallowed. The initiator is
// holding a pending entry either way; the difference is whether it learns now
// or waits out its whole timeout for an answer that was never coming.
func TestSessionRPCForAnUnregisteredSessionIsRefused(t *testing.T) {
	// No sessiond is ever going to register for this one, so the full
	// ten-second wait would be ten seconds of nothing.
	_, _, conn := runnerWithControld(t, shortHubWait)

	conn.send(t, runner.ToRunner{Type: "session_rpc", Session: "sess-nope",
		RPC: &runner.RPCEnvelope{ID: 8, Method: "diff"}})

	up := nextSessionReq(t, conn)
	if up.RPC.ID != 8 || up.RPC.Method != "resp" || up.RPC.OK {
		t.Fatalf("envelope = %+v, want an ok:false resp for id 8", up.RPC)
	}
	var body struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(up.RPC.Payload, &body); err != nil {
		t.Fatalf("decoding the error payload %s: %v", up.RPC.Payload, err)
	}
	if !strings.Contains(body.Error, "sess-nope") {
		t.Fatalf("error = %q, want it to name the session", body.Error)
	}
}

// TestUndeliverableRPCResponseIsDropped: a RESPONSE that cannot be delivered
// has no one to report to — answering an answer is nonsense — so it is dropped
// with a log. The runner must stay serving afterwards, which the ordinary
// forward at the end proves.
func TestUndeliverableRPCResponseIsDropped(t *testing.T) {
	rd, srv, conn := runnerWithControld(t, shortHubWait)

	conn.send(t, runner.ToRunner{Type: "session_rpc", Session: "sess-nope",
		RPC: &runner.RPCEnvelope{ID: 9, Method: "resp", OK: true}})
	// A session_rpc with no envelope at all is dropped the same way.
	conn.send(t, runner.ToRunner{Type: "session_rpc", Session: "sess-nope"})

	id, sb := dialSandbox(t, rd, srv)
	conn.send(t, runner.ToRunner{Type: "session_rpc", Session: id,
		RPC: &runner.RPCEnvelope{ID: 10, Method: "diff"}})
	if got := sb.nextControl(t); got.Kind != "req:diff" || got.ID != 10 {
		t.Fatalf("event = %+v, want the runner still forwarding after two undeliverable messages", got)
	}
}

// TestMalformedControlFramesDoNotDisturbTheChannel: the frames below arrive
// from inside a container, over the conn that also carries every viewer's
// terminal traffic. None of them names anything routable, so each is logged
// and dropped — and the legitimate request that follows still reaches controld,
// which is the property that matters: a malformed frame must not be able to
// take the session's control channel down with it.
func TestMalformedControlFramesDoNotDisturbTheChannel(t *testing.T) {
	rd, srv, conn := runnerWithControld(t)
	id, sb := dialSandbox(t, rd, srv)

	sb.sendRaw(t, `not json at all`)
	sb.sendRaw(t, `{"kind":"req:","id":1}`)    // a request naming no method
	sb.sendRaw(t, `{"kind":"req:diff"}`)       // a request with no id to answer
	sb.sendRaw(t, `{"kind":"resp","ok":true}`) // a response correlating to nothing
	sb.sendRaw(t, `{"kind":"something else"}`) // not part of the vocabulary at all

	sb.send(t, relay.ControlEvent{Kind: "req:mint_git_credential", ID: 11})
	up := nextSessionReq(t, conn)
	if up.Session != id || up.RPC.ID != 11 {
		t.Fatalf("session_req = %+v, want the good request after five malformed frames", up)
	}
}

// TestSandboxRequestWithNoControldConnectionIsRefusedLocally: a runner whose
// controld connection is down cannot forward a mint, and the sandbox is
// holding a git process open waiting for one. Refusing locally turns a 20s
// hang into an immediate, explainable failure the user can retry.
func TestSandboxRequestWithNoControldConnectionIsRefusedLocally(t *testing.T) {
	rd := New(driver.NewFake(4), "", "", "")
	srv := httptest.NewServer(rd.Handler())
	defer srv.Close()

	_, sb := dialSandbox(t, rd, srv) // no RunAgent: this runner is on its own

	sb.send(t, relay.ControlEvent{Kind: "req:mint_git_credential", ID: 12})
	got := sb.nextControl(t)
	if got.Kind != "resp" || got.ID != 12 || got.OK {
		t.Fatalf("event = %+v, want an ok:false resp for id 12", got)
	}
	if !strings.Contains(string(got.Payload), "controld") {
		t.Fatalf("payload = %s, want it to say the runner has no controld connection", got.Payload)
	}
}

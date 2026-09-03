package attachplane

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// These tests drive the plane through its two public halves — the broker the
// application service calls and the dial-back handler the runner dials — over
// a fake host that stands in for everything a controld (or a hosted cell)
// supplies: the runner's credential, the way a command reaches it, and this
// replica's own dial-back URL. Nothing here seeds a session or logs anybody
// in: by the time the broker is called, every question of authority and
// readiness has been settled above it.

// testRunnerToken is the synthetic fleet bearer the fake host accepts. It is
// a fixture, not a credential.
const testRunnerToken = "rnr_test_token"

// The plane's own runner conn is deliberately internal/relay.Conn's method
// set: the runner side of this hop is a relay.Conn everywhere else in the
// fleet (runnerd's agent hands one to its hub), and it must stay usable here
// without a conversion — which is what lets the plane keep relay, and the
// pty and terminal emulator behind it, out of a public dependency tree.
var _ runnerConn = (relay.Conn)(nil)

// ---------------------------------------------------------------------------
// the fake host
// ---------------------------------------------------------------------------

// fakeHost is a host of the attach plane: it authenticates a dial-back
// against one synthetic bearer, records every command the broker sends, and
// names the dial-back URL of the httptest server the plane's BackHandler is
// mounted on. dialBack, when set, is the runner: Send calls it with the
// attach block, exactly as a live runnerd reacts to a dial_attach.
type fakeHost struct {
	token string
	base  string // ws:// base of the listener BackHandler is served on
	cmds  chan runner.ToRunner

	sendErr  error
	dialBack func(at *runner.Attach)
}

func (h *fakeHost) IdentifyRunner(_ context.Context, r *http.Request) (control.PoolID, control.RunnerID, error) {
	if r.Header.Get("Authorization") != "Bearer "+h.token {
		return "", "", control.ErrDenied
	}
	return "pool_test", "vm1", nil
}

func (h *fakeHost) Send(_ control.PoolID, _ control.RunnerID, m runner.ToRunner) error {
	if h.sendErr != nil {
		return h.sendErr
	}
	h.cmds <- m
	if h.dialBack != nil {
		go h.dialBack(m.Attach)
	}
	return nil
}

func (h *fakeHost) BackURL(attachID string) string {
	return h.base + "/v0/runners/attach-back?attach_id=" + attachID
}

// nextCmd returns the next command the broker sent the runner.
func (h *fakeHost) nextCmd(t *testing.T) runner.ToRunner {
	t.Helper()
	select {
	case m := <-h.cmds:
		return m
	case <-time.After(5 * time.Second):
		t.Fatal("no command reached the runner within 5s")
		return runner.ToRunner{}
	}
}

// newTestPlane builds a plane over a fake host and serves its BackHandler on
// an httptest listener. The listener has to exist before the host can name
// its own dial-back URL, so it is built unstarted and started once the
// handler is known — the same order controld's own attach fixture uses.
func newTestPlane(t *testing.T, o Options) (*Plane, *fakeHost, *httptest.Server) {
	t.Helper()
	ts := httptest.NewUnstartedServer(nil)
	h := &fakeHost{
		token: testRunnerToken,
		base:  "ws://" + ts.Listener.Addr().String(),
		cmds:  make(chan runner.ToRunner, 8),
	}
	if o.Logf == nil {
		o.Logf = t.Logf
	}
	p := New(h, o)
	mux := http.NewServeMux()
	mux.Handle("GET /v0/runners/attach-back", p.BackHandler())
	ts.Config.Handler = mux
	ts.Start()
	// Registered first so it runs LAST (cleanups are LIFO): every socket a
	// test opens is closed before the listener under it.
	t.Cleanup(ts.Close)
	return p, h, ts
}

func wsBase(ts *httptest.Server) string { return "ws" + strings.TrimPrefix(ts.URL, "http") }

// dialAttachBack performs the runner-side dial-back, the same way runnerd's
// agent does (bearer header, attach_id in the query).
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
// which is more than any envelope this plane writes.
func assertErrCode(t *testing.T, resp *http.Response, want string) {
	t.Helper()
	if resp == nil {
		t.Fatal("no HTTP response to read an error envelope from")
	}
	defer resp.Body.Close()
	var env struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
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
func pendingAttaches(p *Plane) int {
	p.attaches.mu.Lock()
	defer p.attaches.mu.Unlock()
	return len(p.attaches.m)
}

// ---------------------------------------------------------------------------
// the client stream stub
// ---------------------------------------------------------------------------

// scriptedStream is a control.TerminalStream a test drives from both ends.
// Receive takes what the test queued; Send hands the broker's output back to
// it; Close records the first (and only) reason the socket was closed with.
type scriptedStream struct {
	in     chan terminal.ClientMessage
	out    chan terminal.ServerMessage
	closed chan error
	// dead stands in for the closed socket: a real client conn fails every
	// pending read the moment it is closed, and a stub that did not would let
	// a splice pump outlive the attach it belongs to.
	dead chan struct{}
	once sync.Once
}

func newScriptedStream() *scriptedStream {
	return &scriptedStream{
		in:     make(chan terminal.ClientMessage, 8),
		out:    make(chan terminal.ServerMessage, 8),
		closed: make(chan error, 1),
		dead:   make(chan struct{}),
	}
}

func (s *scriptedStream) Receive(ctx context.Context) (terminal.ClientMessage, error) {
	select {
	case m := <-s.in:
		return m, nil
	case <-s.dead:
		return terminal.ClientMessage{}, io.EOF
	case <-ctx.Done():
		return terminal.ClientMessage{}, ctx.Err()
	}
}

func (s *scriptedStream) Send(ctx context.Context, m terminal.ServerMessage) error {
	select {
	case s.out <- m:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *scriptedStream) Close(err error) error {
	s.once.Do(func() {
		s.closed <- err
		close(s.dead)
	})
	return nil
}

// closeReason returns the reason the stream was closed with, failing the test
// if nothing closed it.
func (s *scriptedStream) closeReason(t *testing.T) error {
	t.Helper()
	select {
	case err := <-s.closed:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("the broker never closed the stream")
		return nil
	}
}

func (s *scriptedStream) nextServerMsg(t *testing.T) terminal.ServerMessage {
	t.Helper()
	select {
	case m := <-s.out:
		return m
	case <-time.After(5 * time.Second):
		t.Fatal("no server message reached the client stream within 5s")
		return terminal.ServerMessage{}
	}
}

// brokerTarget is the resolved binding the attachment service hands a broker.
// Its generations are fictional and unread by this plane: a self-hosted
// install has one placement per session and no viewer/controller split yet.
func brokerTarget(session, runnerName string) control.AttachTarget {
	return control.AttachTarget{
		WorkspaceID: "ws_test", SessionID: control.SessionID(session),
		PoolID: "pool_test", RunnerID: control.RunnerID(runnerName),
		PlacementGeneration: 1, ControllerGeneration: 1,
	}
}

// ---------------------------------------------------------------------------
// the four cases the plane exists for
// ---------------------------------------------------------------------------

// TestPairedAttachSplicesTheFirstSnapshot is the plane doing its job: the
// broker parks a client, the host's runner dials back, and the first frame
// the runner writes arrives at the client as a terminal message.
func TestPairedAttachSplicesTheFirstSnapshot(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{})

	// The runner half: dial back the moment the dial_attach is sent and
	// answer with a snapshot, over the same relay.Conn the plane splices.
	snapshot := make(chan error, 1)
	h.dialBack = func(at *runner.Attach) {
		c, _, err := dialAttachBack(t, ts, at.AttachID, testRunnerToken)
		if err != nil {
			snapshot <- err
			return
		}
		defer c.CloseNow()
		conn := relay.WSConn(c)
		raw, err := json.Marshal(terminal.ServerMessage{Type: "snapshot", Seq: 1, Data: []byte("scrollback")})
		if err != nil {
			snapshot <- err
			return
		}
		snapshot <- conn.Write(context.Background(), raw)
		<-time.After(time.Second) // hold the splice open long enough to read it
	}

	stream := newScriptedStream()
	stream.in <- terminal.ClientMessage{Type: "resize", Cols: 80, Rows: 24}
	go func() { _ = p.Broker().Attach(context.Background(), brokerTarget("sess_pair", "vm1"), stream) }()

	if cmd := h.nextCmd(t); cmd.Type != "dial_attach" || cmd.Attach == nil {
		t.Fatalf("command = %+v, want dial_attach with an attach block", cmd)
	}
	if err := <-snapshot; err != nil {
		t.Fatalf("the runner half never wrote its snapshot: %v", err)
	}
	if m := stream.nextServerMsg(t); m.Type != "snapshot" || string(m.Data) != "scrollback" {
		t.Fatalf("client received %+v, want the runner's snapshot", m)
	}
}

// TestUnpairedAttachTimesOutAtThePairTTL: nobody dials back, so the TTL is
// the only thing that can free the client — and it closes it with the one
// documented reason, not with silence.
func TestUnpairedAttachTimesOutAtThePairTTL(t *testing.T) {
	const ttl = 100 * time.Millisecond
	p, h, _ := newTestPlane(t, Options{PairTTL: ttl})

	stream := newScriptedStream()
	stream.in <- terminal.ClientMessage{Type: "resize", Cols: 80, Rows: 24}
	done := make(chan error, 1)
	go func() {
		done <- p.Broker().Attach(context.Background(), brokerTarget("sess_ttl", "vm1"), stream)
	}()
	h.nextCmd(t) // the dial_attach nobody answers

	select {
	case err := <-done:
		if !errors.Is(err, control.ErrUnavailable) {
			t.Fatalf("Attach after the pairing TTL = %v, want ErrUnavailable", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Attach never returned after the pairing TTL")
	}
	if got := stream.closeReason(t); !errors.Is(got, errAttachNoDialBack) {
		t.Fatalf("closed with %v, want the no-dial-back refusal", got)
	}
	if code, reason := attachCloseReason(errAttachNoDialBack); code != websocket.StatusTryAgainLater ||
		reason != "runner did not dial back" {
		t.Fatalf("close = %v %q, want try-again-later \"runner did not dial back\"", code, reason)
	}
	if n := pendingAttaches(p); n != 0 {
		t.Fatalf("pairing table holds %d entries after the TTL, want 0", n)
	}
}

// TestDialBackForAnUnknownAttachIDIsRefused: an expired or never-issued
// pairing is answered as plain HTTP, before any upgrade, so a late runner
// gets a status code it can log rather than a close reason to decode.
func TestDialBackForAnUnknownAttachIDIsRefused(t *testing.T) {
	_, _, ts := newTestPlane(t, Options{})

	c, resp, err := dialAttachBack(t, ts, "deadbeefdeadbeef", testRunnerToken)
	if err == nil {
		c.CloseNow()
		t.Fatal("dial-back for an unknown attach_id succeeded")
	}
	if resp == nil || resp.StatusCode != http.StatusNotFound {
		t.Fatalf("resp = %+v (%v), want 404", resp, err)
	}
	assertErrCode(t, resp, "not_found")
}

// TestUnauthenticatedDialBackIsRefusedBeforeAnyClaim: the credential is
// checked first, so a dial-back nobody authenticated cannot take (or expire)
// a pairing another runner is still owed — the parked client is still there
// afterwards, and the real runner still claims it.
func TestUnauthenticatedDialBackIsRefusedBeforeAnyClaim(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{})

	stream := newScriptedStream()
	stream.in <- terminal.ClientMessage{Type: "resize", Cols: 80, Rows: 24}
	go func() { _ = p.Broker().Attach(context.Background(), brokerTarget("sess_auth", "vm1"), stream) }()
	at := h.nextCmd(t).Attach
	if at == nil {
		t.Fatal("the broker sent no attach block")
	}

	c, resp, err := dialAttachBack(t, ts, at.AttachID, "rnr_wrong")
	if err == nil {
		c.CloseNow()
		t.Fatal("dial-back with a wrong token succeeded")
	}
	if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("resp = %+v (%v), want 401", resp, err)
	}
	if n := pendingAttaches(p); n != 1 {
		t.Fatalf("pairing table holds %d entries after a refused dial-back, want the pairing still parked", n)
	}
	// And the runner it was actually minted for still claims it.
	rc, resp, err := dialAttachBack(t, ts, at.AttachID, testRunnerToken)
	if err != nil {
		t.Fatalf("dial back: %v", err)
	}
	defer rc.CloseNow()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("dial-back status = %d, want 101", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// moved from internal/controld/adapt_attach_test.go
// ---------------------------------------------------------------------------

// TestBrokerRefusesAFirstMessageThatIsNotResize pins the resize-first
// contract at the port: the size the dial_attach carries comes from that
// message and there is nothing to fall back to, so a client that opens with
// anything else is closed — and no pairing and no dial_attach are created for
// it.
func TestBrokerRefusesAFirstMessageThatIsNotResize(t *testing.T) {
	p, h, _ := newTestPlane(t, Options{})

	stream := newScriptedStream()
	stream.in <- terminal.ClientMessage{Type: "stdin", Data: []byte("oops")}

	err := p.Broker().Attach(context.Background(), brokerTarget("sess_no_resize", "vm1"), stream)
	if !errors.Is(err, errAttachFirstMessage) {
		t.Fatalf("Attach = %v, want the first-message refusal", err)
	}
	if got := stream.closeReason(t); !errors.Is(got, errAttachFirstMessage) {
		t.Fatalf("closed with %v, want the first-message refusal", got)
	}
	if code, _ := attachCloseReason(err); code != websocket.StatusPolicyViolation {
		t.Fatalf("close code = %v, want a policy violation: the client broke the protocol", code)
	}
	if n := pendingAttaches(p); n != 0 {
		t.Fatalf("pairing table holds %d entries after a refused attach, want 0", n)
	}
	// The refusal happens before anything is sent, so a command the runner
	// was going to receive would already be in the channel.
	select {
	case cmd := <-h.cmds:
		t.Fatalf("the runner was asked to dial back for a rejected attach: %+v", cmd)
	default:
	}
}

// TestBrokerAsksTheRunnerToDialBack pins the one message this plane sends —
// including the cursor, which control.AttachTarget has no field for and which
// therefore travels beside the command, in the context the handler built.
func TestBrokerAsksTheRunnerToDialBack(t *testing.T) {
	const ttl = 250 * time.Millisecond
	p, h, ts := newTestPlane(t, Options{PairTTL: ttl})

	stream := newScriptedStream()
	stream.in <- terminal.ClientMessage{Type: "resize", Cols: 100, Rows: 30}

	done := make(chan error, 1)
	go func() {
		done <- p.Broker().Attach(WithSince(context.Background(), 11),
			brokerTarget("sess_dial", "vm1"), stream)
	}()

	cmd := h.nextCmd(t)
	if cmd.Type != "dial_attach" || cmd.Session != "sess_dial" || cmd.Attach == nil {
		t.Fatalf("command = %+v, want dial_attach for sess_dial with an attach block", cmd)
	}
	at := cmd.Attach
	if len(at.AttachID) != 16 {
		t.Fatalf("attach_id = %q, want 16 hex characters", at.AttachID)
	}
	if _, err := hex.DecodeString(at.AttachID); err != nil {
		t.Fatalf("attach_id = %q, want hex: %v", at.AttachID, err)
	}
	if at.Since != 11 || at.Cols != 100 || at.Rows != 30 {
		t.Fatalf("attach = since:%d cols:%d rows:%d, want since:11 cols:100 rows:30", at.Since, at.Cols, at.Rows)
	}
	if want := wsBase(ts) + "/v0/runners/attach-back?attach_id=" + at.AttachID; at.TargetURL != want {
		t.Fatalf("target_url = %q, want %q", at.TargetURL, want)
	}

	// Nobody dials back: the TTL is the only thing that can free this client.
	select {
	case err := <-done:
		if !errors.Is(err, control.ErrUnavailable) {
			t.Fatalf("Attach after the pairing TTL = %v, want ErrUnavailable", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Attach never returned after the pairing TTL")
	}
	if got := stream.closeReason(t); !errors.Is(got, errAttachNoDialBack) {
		t.Fatalf("closed with %v, want the no-dial-back refusal", got)
	}
	if code, _ := attachCloseReason(errAttachNoDialBack); code != websocket.StatusTryAgainLater {
		t.Fatalf("close code = %v, want try-again-later: the client's remedy is another attach", code)
	}
	if p.attaches.has(at.AttachID) {
		t.Fatalf("pairing %s is still in the table after its TTL", at.AttachID)
	}
}

// TestBrokerSplicesBothDirections is the pairing doing its job: a runner dials
// back, and from then on every message crosses in both directions — decoded
// off the client port and re-encoded onto the runner's raw frames, and back.
func TestBrokerSplicesBothDirections(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{})

	stream := newScriptedStream()
	stream.in <- terminal.ClientMessage{Type: "resize", Cols: 80, Rows: 24}
	done := make(chan error, 1)
	go func() {
		done <- p.Broker().Attach(context.Background(), brokerTarget("sess_splice", "vm1"), stream)
	}()

	cmd := h.nextCmd(t)
	if cmd.Type != "dial_attach" || cmd.Attach == nil {
		t.Fatalf("command = %+v, want dial_attach", cmd)
	}
	rc, resp, err := dialAttachBack(t, ts, cmd.Attach.AttachID, testRunnerToken)
	if err != nil {
		t.Fatalf("dial back: %v", err)
	}
	defer rc.CloseNow()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("dial-back status = %d, want 101", resp.StatusCode)
	}
	rc.SetReadLimit(attachReadLimit)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// runner → client
	out, err := json.Marshal(terminal.ServerMessage{Type: "snapshot", Seq: 1, Data: []byte("scrollback")})
	if err != nil {
		t.Fatal(err)
	}
	if err := rc.Write(ctx, websocket.MessageText, out); err != nil {
		t.Fatalf("write to the dial-back socket: %v", err)
	}
	if m := stream.nextServerMsg(t); m.Type != "snapshot" || string(m.Data) != "scrollback" {
		t.Fatalf("client received %+v, want the runner's snapshot", m)
	}

	// client → runner
	stream.in <- terminal.ClientMessage{Type: "stdin", Data: []byte("hi")}
	_, raw, err := rc.Read(ctx)
	if err != nil {
		t.Fatalf("read from the dial-back socket: %v", err)
	}
	var got terminal.ClientMessage
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("the runner half received something that is not a client message: %v", err)
	}
	if got.Type != "stdin" || string(got.Data) != "hi" {
		t.Fatalf("the runner received %+v, want the client's stdin", got)
	}

	// Either side ending ends the attach, and the pairing goes with it.
	rc.CloseNow()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Attach after a spliced attach ended = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Attach never returned after the runner half closed")
	}
	if n := pendingAttaches(p); n != 0 {
		t.Fatalf("pairing table holds %d entries after the splice, want 0", n)
	}
}

// TestAttachCloseReasons pins the whole close vocabulary in one place: which
// failures are the client's fault (policy violation) and which are an
// invitation to try again, and that no reason can overflow what a close frame
// carries or quote anything but our own words.
func TestAttachCloseReasons(t *testing.T) {
	cases := []struct {
		name string
		err  error
		code websocket.StatusCode
	}{
		{"first message", errAttachFirstMessage, websocket.StatusPolicyViolation},
		{"denied", control.ErrDenied, websocket.StatusPolicyViolation},
		{"id collision", errAttachIDCollision, websocket.StatusInternalError},
		{"no dial-back", errAttachNoDialBack, websocket.StatusTryAgainLater},
		{"not attachable", control.ErrConflict, websocket.StatusTryAgainLater},
		{"the attach ended", errAttachEnded, websocket.StatusTryAgainLater},
		{"unavailable", control.ErrUnavailable, websocket.StatusTryAgainLater},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, reason := attachCloseReason(tc.err)
			if code != tc.code {
				t.Fatalf("code = %v, want %v", code, tc.code)
			}
			if reason == "" || len(reason) > 123 {
				t.Fatalf("reason = %q (%d bytes); a close frame carries at most 123", reason, len(reason))
			}
			if strings.Contains(reason, "controld:") {
				t.Fatalf("reason = %q, want a sentence for the client, not the sentinel's own text", reason)
			}
		})
	}
}

// TestAttachSinceRidesTheContext: the cursor rides the context because
// control.AttachTarget has no field for it, and a context that never carried
// one reads as 0 — the same full replay a malformed `since` gets on the wire.
func TestAttachSinceRidesTheContext(t *testing.T) {
	if got := Since(context.Background()); got != 0 {
		t.Fatalf("Since with no value = %d, want 0", got)
	}
	if got := Since(WithSince(context.Background(), 42)); got != 42 {
		t.Fatalf("Since = %d, want 42", got)
	}
}

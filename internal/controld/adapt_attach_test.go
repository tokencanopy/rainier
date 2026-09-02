package controld

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// These tests drive attachBroker directly, over the attach harness in
// attach_test.go: a real controld with a fake runner on its control plane, and
// a scripted control.TerminalStream where a client socket would be. That is
// the seam the broker is written against — the attachment service hands it a
// stream and a resolved target, and everything about who may attach has
// already been settled — so nothing here seeds a session or logs in.

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
// Its generations are fictional and unread by this adapter: self-hosted has
// one placement per session and no viewer/controller split yet.
func brokerTarget(session, runner string) control.AttachTarget {
	return control.AttachTarget{
		WorkspaceID: installWorkspace, SessionID: control.SessionID(session),
		PoolID: installPool, RunnerID: control.RunnerID(runner),
		PlacementGeneration: 1, ControllerGeneration: 1,
	}
}

// TestBrokerRefusesAFirstMessageThatIsNotResize pins the resize-first
// contract at the port: the size the dial_attach carries comes from that
// message and there is nothing to fall back to, so a client that opens with
// anything else is closed — and no pairing and no dial_attach are created for
// it.
func TestBrokerRefusesAFirstMessageThatIsNotResize(t *testing.T) {
	s, _, ts := newAttachControld(t)
	f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})

	stream := newScriptedStream()
	stream.in <- terminal.ClientMessage{Type: "stdin", Data: []byte("oops")}

	err := attachBroker{srv: s}.Attach(context.Background(),
		brokerTarget("sess_no_resize", "vm1"), stream)
	if !errors.Is(err, errAttachFirstMessage) {
		t.Fatalf("Attach = %v, want the first-message refusal", err)
	}
	if got := stream.closeReason(t); !errors.Is(got, errAttachFirstMessage) {
		t.Fatalf("closed with %v, want the first-message refusal", got)
	}
	if code, _ := attachCloseReason(err); code != websocket.StatusPolicyViolation {
		t.Fatalf("close code = %v, want a policy violation: the client broke the protocol", code)
	}
	if n := pendingAttaches(s); n != 0 {
		t.Fatalf("pairing table holds %d entries after a refused attach, want 0", n)
	}
	// The refusal happens before anything is sent, so a command the runner
	// was going to receive would already be in the channel.
	select {
	case cmd := <-f.cmds:
		t.Fatalf("the runner was asked to dial back for a rejected attach: %+v", cmd)
	default:
	}
}

// TestBrokerAsksTheRunnerToDialBack pins the one message this adapter sends —
// including the cursor, which control.AttachTarget has no field for and which
// therefore travels beside the command, in the context the handler built.
func TestBrokerAsksTheRunnerToDialBack(t *testing.T) {
	const ttl = 250 * time.Millisecond
	s, _, ts := newAttachControld(t, func(c *Config) { c.AttachPairTTL = ttl })
	f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})

	stream := newScriptedStream()
	stream.in <- terminal.ClientMessage{Type: "resize", Cols: 100, Rows: 30}

	done := make(chan error, 1)
	go func() {
		done <- attachBroker{srv: s}.Attach(withAttachSince(context.Background(), 11),
			brokerTarget("sess_dial", "vm1"), stream)
	}()

	cmd := f.nextCmd(t)
	if cmd.Type != "dial_attach" || cmd.Session != "sess_dial" || cmd.Attach == nil {
		t.Fatalf("command = %+v, want dial_attach for sess_dial with an attach block", cmd)
	}
	at := cmd.Attach
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
	if s.attaches.has(at.AttachID) {
		t.Fatalf("pairing %s is still in the table after its TTL", at.AttachID)
	}
}

// TestBrokerSplicesBothDirections is the pairing doing its job: a runner dials
// back, and from then on every message crosses in both directions — decoded
// off the client port and re-encoded onto the runner's raw frames, and back.
func TestBrokerSplicesBothDirections(t *testing.T) {
	s, _, ts := newAttachControld(t)
	f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})

	stream := newScriptedStream()
	stream.in <- terminal.ClientMessage{Type: "resize", Cols: 80, Rows: 24}
	done := make(chan error, 1)
	go func() {
		done <- attachBroker{srv: s}.Attach(context.Background(),
			brokerTarget("sess_splice", "vm1"), stream)
	}()

	cmd := f.nextCmd(t)
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
	if n := pendingAttaches(s); n != 0 {
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

// TestAttachSinceIsAdapterInternal: the cursor rides the context because
// control.AttachTarget has no field for it, and a context that never carried
// one reads as 0 — the same full replay a malformed `since` gets on the wire.
func TestAttachSinceIsAdapterInternal(t *testing.T) {
	if got := attachSince(context.Background()); got != 0 {
		t.Fatalf("attachSince with no value = %d, want 0", got)
	}
	if got := attachSince(withAttachSince(context.Background(), 42)); got != 42 {
		t.Fatalf("attachSince = %d, want 42", got)
	}
}

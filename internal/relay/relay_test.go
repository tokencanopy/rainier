package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/internal/session"
	"github.com/tokencanopy/rainier/internal/wire"
)

// pipeConn is an in-memory Conn pair for tests. The two ends returned by
// newPipe share one closed signal: Close() on either end tears down the
// whole logical connection, so a blocked or future Read/Write on *either*
// end errors out afterward — the same way closing one side of a real
// duplex connection eventually surfaces as a read/write error on the peer.
// This is what lets a regression test simulate a dead outbound conn by
// closing just one pipeConn and observing the failure cascade through
// ServeSession/Hub reach all the way to an unrelated client conn.
type pipeConn struct {
	in     chan []byte
	out    chan []byte
	closed chan struct{}
	once   *sync.Once
}

func newPipe() (a, b *pipeConn) {
	c1, c2 := make(chan []byte, 64), make(chan []byte, 64)
	closed := make(chan struct{})
	once := &sync.Once{}
	return &pipeConn{in: c1, out: c2, closed: closed, once: once},
		&pipeConn{in: c2, out: c1, closed: closed, once: once}
}
func (p *pipeConn) Read(ctx context.Context) ([]byte, error) {
	select {
	case b := <-p.in:
		return b, nil
	case <-p.closed:
		return nil, io.EOF
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (p *pipeConn) Write(ctx context.Context, b []byte) error {
	cp := append([]byte(nil), b...)
	select {
	case p.out <- cp:
		return nil
	case <-p.closed:
		return io.ErrClosedPipe
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (p *pipeConn) Close() error {
	p.once.Do(func() { close(p.closed) })
	return nil
}

// readServerMsg reads one message off a client-facing pipe and decodes it as
// a wire.ServerMsg. The Hub forwards a FrameServer's Payload to the client
// verbatim (see runnerd_side.go's readLoop), so what lands on this pipe is
// raw wire.ServerMsg JSON, not a Frame — rattach never needs to know about
// relay.Frame at all.
func readServerMsg(t *testing.T, c *pipeConn) wire.ServerMsg {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	raw, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("read server msg: %v", err)
	}
	var m wire.ServerMsg
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal ServerMsg: %v (raw: %s)", err, raw)
	}
	return m
}

// writeClientMsg encodes m as raw wire.ClientMsg JSON and writes it to a
// client-facing pipe. AttachClient wraps whatever it reads off this pipe
// into a FrameClient before forwarding it to the session conn (see
// runnerd_side.go), so the client itself only ever speaks the raw wire
// protocol.
func writeClientMsg(t *testing.T, c *pipeConn, m wire.ClientMsg) {
	t.Helper()
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal ClientMsg: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Write(ctx, raw); err != nil {
		t.Fatalf("write client msg: %v", err)
	}
}

func contains(data []byte, want string) bool { return bytes.Contains(data, []byte(want)) }

func TestRelayAttachStreamsOutput(t *testing.T) {
	s, err := session.New(
		session.Config{Argv: []string{"sh", "-i"}, Cols: 80, Rows: 24, LogPath: filepath.Join(t.TempDir(), "s.log")},
		session.StartProc,
	)
	if err != nil { t.Fatal(err) }

	sessConn, runConn := newPipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ServeSession(ctx, sessConn, s)

	hub := NewHub(ctx, runConn)
	defer hub.Close()

	client, hubClient := newPipe()
	go hub.AttachClient(ctx, hubClient, 0, 80, 24)

	// Client should receive a snapshot frame first (as a FrameServer wrapping a
	// wire.ServerMsg of type "snapshot"), then output after we send stdin.
	first := readServerMsg(t, client)
	if first.Type != "snapshot" { t.Fatalf("first msg = %s, want snapshot", first.Type) }

	// Send stdin through the client → hub → session → shell, expect echo.
	writeClientMsg(t, client, wire.ClientMsg{Type: "stdin", Data: []byte("echo relay-marker\n")})
	deadline := time.After(3 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("never saw relay-marker echoed through the relay")
		default:
		}
		m := readServerMsg(t, client)
		if m.Type == "output" && contains(m.Data, "relay-marker") { return }
	}
}

// TestControlFramesReachHub pins the CONTROL channel that rides the same
// outbound conn as the terminal mux: a sessiond-originated payload sent
// through a ControlSender must arrive verbatim at the Hub's control handler,
// and — because control writes and ServeSession's own frame writes now go
// through one shared writer — an attachment opened afterwards must still
// stream normally. A broken sharing (interleaved/corrupted frames, or a
// deadlocked write mutex) fails in the second half of this test, which is
// exactly why the terminal round trip is asserted here and not left to
// TestRelayAttachStreamsOutput alone.
func TestControlFramesReachHub(t *testing.T) {
	s, err := session.New(
		session.Config{Argv: []string{"sh", "-i"}, Cols: 80, Rows: 24, LogPath: filepath.Join(t.TempDir(), "s.log")},
		session.StartProc,
	)
	if err != nil { t.Fatal(err) }
	defer s.Stop()

	sessConn, runConn := newPipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// nil inbound handler: the Plan 4 behaviour — a session that never expects
	// a request from runnerd still relays and still sends control events.
	sender, errc := ServeSessionWithControl(ctx, sessConn, s, nil)

	// Wired through the constructor, which is the only way to install a
	// handler: NewHubWithControl sets it before starting readLoop, so there
	// is neither a race with that goroutine nor a window in which an early
	// control frame arrives unhandled.
	got := make(chan []byte, 4)
	hub := NewHubWithControl(ctx, runConn, func(p []byte) { got <- append([]byte(nil), p...) })
	defer hub.Close()

	want := []byte(`{"kind":"setup_done"}`)
	if err := sender.Send(want); err != nil { t.Fatalf("send control: %v", err) }
	select {
	case p := <-got:
		if !bytes.Equal(p, want) { t.Fatalf("control payload = %q, want %q", p, want) }
	case <-time.After(5 * time.Second):
		t.Fatal("control frame never reached the hub's control handler")
	}

	// Terminal mux still works over the same conn after a control write.
	client, hubClient := newPipe()
	go hub.AttachClient(ctx, hubClient, 0, 80, 24)
	first := readServerMsg(t, client)
	if first.Type != "snapshot" { t.Fatalf("first msg = %s, want snapshot", first.Type) }
	writeClientMsg(t, client, wire.ClientMsg{Type: "stdin", Data: []byte("echo control-marker\n")})
	deadline := time.After(5 * time.Second)
	for done := false; !done; {
		select {
		case <-deadline:
			t.Fatal("never saw control-marker echoed through the relay after a control frame")
		default:
		}
		m := readServerMsg(t, client)
		if m.Type == "output" && contains(m.Data, "control-marker") { done = true }
	}

	// The error channel yields the relay's exit EXACTLY once: one value, and
	// then nothing — neither a second value nor a close. The close half
	// matters because a closed channel hands out an endless supply of nil
	// errors, which a caller selecting on it would read as "the relay is
	// alive and well" forever; the documented contract is one value, take it
	// once, and treat it as the end of this conn's life.
	sessConn.Close()
	select {
	case err := <-errc:
		if err == nil { t.Fatal("error channel yielded nil after session conn death, want the read error") }
	case <-time.After(5 * time.Second):
		t.Fatal("error channel never yielded after session conn death")
	}
	select {
	case v, ok := <-errc:
		if !ok { t.Fatal("error channel was closed after its one value; the contract is one value and no close") }
		t.Fatalf("error channel yielded a second value %v, want exactly one", v)
	default:
	}
	// A closed channel is always ready, so had ServeSessionWithControl closed
	// it the receive above would have fired instantly rather than falling to
	// default. Give it a moment to close late, too — the relay goroutine has
	// already returned by now, so anything it was going to do, it has done.
	select {
	case v, ok := <-errc:
		if !ok { t.Fatal("error channel closed after a delay; the contract is one value and no close") }
		t.Fatalf("error channel yielded a late second value %v, want exactly one", v)
	case <-time.After(100 * time.Millisecond):
	}
}

// recvControl takes one control payload off a handler's hand-off channel and
// decodes it as a ControlEvent. Decoding happens HERE, on the test goroutine,
// and not inside the handler, because t.Fatalf from a handler goroutine is
// not allowed — and because a handler that unmarshals is a handler that does
// work, which is precisely what the hand-off contract on both ends forbids.
func recvControl(t *testing.T, ch <-chan []byte, what string) ControlEvent {
	t.Helper()
	select {
	case p := <-ch:
		var ev ControlEvent
		if err := json.Unmarshal(p, &ev); err != nil {
			t.Fatalf("decode %s: %v (raw %s)", what, err, p)
		}
		return ev
	case <-time.After(5 * time.Second):
		t.Fatalf("%s never arrived", what)
	}
	return ControlEvent{}
}

func encodeControl(t *testing.T, ev ControlEvent) []byte {
	t.Helper()
	b, err := json.Marshal(ev)
	if err != nil { t.Fatalf("marshal control event: %v", err) }
	return b
}

// TestControlRPCRoundTripBothDirections pins the control channel as a
// BIDIRECTIONAL request/response transport, which is the one primitive the
// GitHub connector and the credential vault are both built on: a mint request
// travels sandbox→controld, while diff and push/pull travel controld→sandbox.
// Plan 4 only ever pushed events upward, so both new halves are asserted here
// — the hub originating a request (Hub.SendControl) and the session side
// receiving one (the handler ServeSessionWithControl now takes) — in both
// orders, because a transport that only works when the first request happened
// to come from one particular end is not bidirectional.
//
// The third act re-asserts the terminal mux after all that control traffic,
// mirroring TestControlFramesReachHub: control frames and terminal frames
// share one conn in each direction, so a botched write discipline shows up as
// a stalled or corrupted attachment rather than as a failed control round
// trip.
func TestControlRPCRoundTripBothDirections(t *testing.T) {
	s, err := session.New(
		session.Config{Argv: []string{"sh", "-i"}, Cols: 80, Rows: 24, LogPath: filepath.Join(t.TempDir(), "s.log")},
		session.StartProc,
	)
	if err != nil { t.Fatal(err) }
	defer s.Stop()

	sessConn, runConn := newPipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Both handlers do nothing but hand the payload off to a buffered channel:
	// that is the documented contract on both ends, and modelling it here
	// keeps the test from passing only because its handler was fast.
	sessIn := make(chan []byte, 8)
	sender, _ := ServeSessionWithControl(ctx, sessConn, s, func(p []byte) { sessIn <- append([]byte(nil), p...) })
	hubIn := make(chan []byte, 8)
	hub := NewHubWithControl(ctx, runConn, func(p []byte) { hubIn <- append([]byte(nil), p...) })
	defer hub.Close()

	// Act 1 — runnerd → sessiond request, sessiond responds.
	if err := hub.SendControl(encodeControl(t, ControlEvent{
		Kind: "req:ping", ID: 1, Payload: json.RawMessage(`{"n":1}`),
	})); err != nil {
		t.Fatalf("hub SendControl: %v", err)
	}
	req := recvControl(t, sessIn, "req:ping at the session")
	if req.Kind != "req:ping" || req.ID != 1 || string(req.Payload) != `{"n":1}` {
		t.Fatalf("request reached the session mangled: %+v", req)
	}
	if err := sender.Send(encodeControl(t, ControlEvent{
		Kind: "resp", ID: req.ID, OK: true, Payload: json.RawMessage(`{"pong":1}`),
	})); err != nil {
		t.Fatalf("session Send resp: %v", err)
	}
	resp := recvControl(t, hubIn, "resp at the hub")
	if resp.Kind != "resp" || resp.ID != 1 || !resp.OK || string(resp.Payload) != `{"pong":1}` {
		t.Fatalf("response reached the hub mangled: %+v", resp)
	}

	// Act 2 — the REVERSE: sessiond → runnerd request, runnerd responds. The
	// ID space is per-direction, so 1 is a legitimate id here too and reusing
	// it would hide a reply routed back to the wrong side's table; 2 keeps the
	// two halves of this test distinguishable.
	if err := sender.Send(encodeControl(t, ControlEvent{
		Kind: "req:pong", ID: 2, Payload: json.RawMessage(`{"n":2}`),
	})); err != nil {
		t.Fatalf("session Send req: %v", err)
	}
	up := recvControl(t, hubIn, "req:pong at the hub")
	if up.Kind != "req:pong" || up.ID != 2 || string(up.Payload) != `{"n":2}` {
		t.Fatalf("request reached the hub mangled: %+v", up)
	}
	if err := hub.SendControl(encodeControl(t, ControlEvent{
		Kind: "resp", ID: up.ID, OK: false, Payload: json.RawMessage(`{"err":"nope"}`),
	})); err != nil {
		t.Fatalf("hub SendControl resp: %v", err)
	}
	down := recvControl(t, sessIn, "resp at the session")
	// OK false is the interesting case for a bool with omitempty: it is absent
	// from the wire, so a decoder that guessed a default of true would pass
	// every happy-path assertion and silently turn failures into successes.
	if down.Kind != "resp" || down.ID != 2 || down.OK || string(down.Payload) != `{"err":"nope"}` {
		t.Fatalf("response reached the session mangled: %+v", down)
	}

	// Act 3 — the terminal mux still works, in both directions, after control
	// traffic has crossed the conn both ways.
	client, hubClient := newPipe()
	go hub.AttachClient(ctx, hubClient, 0, 80, 24)
	first := readServerMsg(t, client)
	if first.Type != "snapshot" { t.Fatalf("first msg = %s, want snapshot", first.Type) }
	writeClientMsg(t, client, wire.ClientMsg{Type: "stdin", Data: []byte("echo rpc-marker\n")})
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("never saw rpc-marker echoed through the relay after RPC traffic both ways")
		default:
		}
		m := readServerMsg(t, client)
		if m.Type == "output" && contains(m.Data, "rpc-marker") { return }
	}
}

// TestHubSendControlSurfacesWriteError is Hub.SendControl's half of what
// TestControlSenderSurfacesWriteError pins for the session side: an
// undelivered control frame must be reported, never swallowed. On this end it
// matters more, not less — a dropped request leaves the caller's pending-RPC
// entry waiting for a response that nothing will ever send, so the error is
// the only thing that lets it fail fast instead of at a timeout.
func TestHubSendControlSurfacesWriteError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub := NewHub(ctx, deadConn{})
	defer hub.Close()
	if err := hub.SendControl([]byte(`{"kind":"req:ping","id":1}`)); err == nil {
		t.Fatal("SendControl on a dead conn returned nil, want the write error")
	}
}

// deadConn stands in for an outbound conn that has died: every write fails,
// and a read parks until its context is cancelled. Deliberately not a closed
// pipeConn — a pipeConn's Write races its own buffered channel against the
// closed signal and can still succeed after Close, which makes it useless for
// asserting a *guaranteed* write failure.
type deadConn struct{}

func (deadConn) Read(ctx context.Context) ([]byte, error)  { <-ctx.Done(); return nil, ctx.Err() }
func (deadConn) Write(ctx context.Context, b []byte) error { return io.ErrClosedPipe }
func (deadConn) Close() error                              { return nil }

// TestControlSenderSurfacesWriteError: an undelivered control event must be
// reported back to its caller, not swallowed. sessiond's setup watcher is the
// only thing that knows a setup outcome exists at all, so a Send that
// silently returned nil on a dead conn would lose the event entirely.
func TestControlSenderSurfacesWriteError(t *testing.T) {
	sender := &ControlSender{w: newConnWriter(context.Background(), deadConn{})}
	if err := sender.Send([]byte(`{"kind":"setup_done"}`)); err == nil {
		t.Fatal("Send on a dead conn returned nil, want the write error")
	}
}

// TestSessionConnDeathClosesClient is the regression test for cascading
// cleanup: when the outbound conn a session opened to runnerd dies,
// ServeSession's read loop must detach every live attachment (so its
// forwarder goroutine, and the session viewer it holds, don't leak for the
// session's lifetime), and that death must propagate through the Hub to
// every client attached over it — otherwise a client is left parked forever
// with nothing left to ever write it another byte or a close. This asserts
// on the observable end of that cascade: the client conn gets closed.
func TestSessionConnDeathClosesClient(t *testing.T) {
	s, err := session.New(
		session.Config{Argv: []string{"sh", "-i"}, Cols: 80, Rows: 24, LogPath: filepath.Join(t.TempDir(), "s.log")},
		session.StartProc,
	)
	if err != nil { t.Fatal(err) }

	sessConn, runConn := newPipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ServeSession(ctx, sessConn, s)

	hub := NewHub(ctx, runConn)
	defer hub.Close()

	client, hubClient := newPipe()
	go hub.AttachClient(ctx, hubClient, 0, 80, 24)

	// Wait for the snapshot so the attachment is fully live end to end
	// (session.Attach'd, registered in the Hub) before killing the conn.
	first := readServerMsg(t, client)
	if first.Type != "snapshot" { t.Fatalf("first msg = %s, want snapshot", first.Type) }

	// Kill the session-side conn — the one ServeSession reads. This is what
	// a dropped/dead outbound WebSocket to runnerd looks like in production.
	sessConn.Close()

	// The death must cascade all the way to the client: assert its next Read
	// errors out promptly. The inner Read deliberately carries no deadline of
	// its own (context.Background()) — the *only* bound on this wait is the
	// outer select's time.After. That matters: if the Read instead used its
	// own e.g. 3s inner context.WithTimeout, a reverted/broken cascade would
	// leave the client conn open forever, its Read would still return a
	// non-nil error (context.DeadlineExceeded) once that inner deadline
	// fired, and a check for "any error" would wrongly call that a pass —
	// the test would never catch a regression of the cascade it exists to
	// guard. With no inner deadline, a broken cascade instead blocks the
	// background Read forever, so only the outer time.After fires, and it
	// fails the test via t.Fatal below rather than a false pass.
	done := make(chan error, 1)
	go func() {
		_, err := client.Read(context.Background())
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil { t.Fatal("expected client conn Read to error after session conn death, got nil") }
	case <-time.After(3 * time.Second):
		t.Fatal("client conn was never closed after session conn death — cascade failed")
	}
}

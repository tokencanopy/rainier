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

	"rainier/internal/session"
	"rainier/internal/wire"
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
	// errors out within a bounded deadline, rather than hanging forever.
	done := make(chan error, 1)
	go func() {
		rctx, rcancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer rcancel()
		_, err := client.Read(rctx)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil { t.Fatal("expected client conn Read to error after session conn death, got nil") }
	case <-time.After(3 * time.Second):
		t.Fatal("client conn was never closed after session conn death")
	}
}

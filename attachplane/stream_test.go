package attachplane

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// clientPair returns a live client stream and the socket on the other end of
// it, with the peer reading nothing until drain is closed. It is a client that
// is alive and slow, which is a different thing from one that is wedged — and
// the difference is what these two tests are about.
func clientPair(t *testing.T, drain <-chan struct{}) (control.TerminalStream, *websocket.Conn) {
	t.Helper()
	served := make(chan struct{})
	accepted := make(chan *websocket.Conn, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		accepted <- c
		<-served
	}))
	t.Cleanup(func() { close(served); ts.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	client, _, err := websocket.Dial(ctx, "ws"+ts.URL[len("http"):], nil)
	if err != nil {
		t.Fatalf("dialing the test client: %v", err)
	}
	t.Cleanup(func() { client.CloseNow() })
	client.SetReadLimit(attachReadLimit)
	go func() {
		<-drain
		for {
			if _, _, err := client.Read(context.Background()); err != nil {
				return
			}
		}
	}()
	c := <-accepted
	return ClientStream(c), c
}

// TestACourtesyNoticeNeverClosesAHealthyClient is the other side of the write
// deadline, and the thing it must not do. A websocket serialises its writes,
// so a handoff's `control_changed` — which carries ONE acknowledgement
// timeout, because a handoff must never wait on a client — queues behind a
// snapshot the plane is already replaying to that same client and expires
// waiting for the write lock. The socket has failed at nothing. Ending it
// there would let one take-over anywhere on the session disconnect a client
// that is merely reading a large scrollback over a slow link.
//
// The rule is that this stream closes on ITS OWN budget and on nobody else's.
func TestACourtesyNoticeNeverClosesAHealthyClient(t *testing.T) {
	drain := make(chan struct{})
	defer close(drain)
	stream, conn := clientPair(t, drain)

	// The snapshot, as the splice's output pump has it: a writer that owns
	// the socket and has not finished. Holding the lock explicitly is what
	// makes the queue below a fact rather than a hope.
	w, err := conn.Writer(context.Background(), websocket.MessageText)
	if err != nil {
		t.Fatalf("taking the write lock: %v", err)
	}

	// And now a handoff's courtesy notice, with a handoff's deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if err := stream.Send(ctx, terminal.ServerMessage{
		Type: terminal.TypeControlChanged, Mode: terminal.ModeView, Generation: "2"}); err == nil {
		t.Fatal("the notice was written while another writer held the socket")
	}

	// The snapshot finishes, and the client is still there.
	if _, err := w.Write([]byte(`{"type":"snapshot"}`)); err != nil {
		t.Fatalf("a courtesy notice timing out killed a healthy client mid-snapshot: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("finishing the snapshot: %v", err)
	}
	if err := stream.Send(context.Background(), terminal.ServerMessage{Type: "output", Seq: 2}); err != nil {
		t.Fatalf("the client was closed by somebody else's deadline: %v", err)
	}
}

// TestAClientThatNeverDrainsIsClosedFromBehindTheWriteLock is the case the
// explicit close IS for. A write that never acquires the conn's write lock
// never reaches the socket, so the websocket library's own teardown — which
// fires when a write in FLIGHT runs out of time — never happens. Without a
// close of our own, a client that has taken nothing for the whole of this
// stream's budget would be left open with writers parked behind it.
func TestAClientThatNeverDrainsIsClosedFromBehindTheWriteLock(t *testing.T) {
	restore := clientWriteTimeout
	clientWriteTimeout = 300 * time.Millisecond
	defer func() { clientWriteTimeout = restore }()

	never := make(chan struct{}) // a peer that never reads a byte
	defer close(never)
	stream, conn := clientPair(t, never)

	// Somebody else owns the socket and is not giving it back.
	if _, err := conn.Writer(context.Background(), websocket.MessageText); err != nil {
		t.Fatalf("taking the write lock: %v", err)
	}
	// The socket is closed when this fires, so a read that would otherwise
	// block forever is how the test sees it happen.
	closed := make(chan error, 1)
	go func() {
		_, _, err := conn.Read(context.Background())
		closed <- err
	}()

	started := time.Now()
	if err := stream.Send(context.Background(), terminal.ServerMessage{Type: "output", Seq: 2}); err == nil {
		t.Fatal("a write that never got the socket reported success")
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("the queued write took %s to give up", elapsed)
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("the socket was left open behind a client that had taken nothing for its whole budget")
	}
}

// TestAWedgedClientIsClosedRatherThanHeld is the transport half of the
// fourth review's blocker. A client that stops READING its socket — a closed
// lid, a TCP zero window, a paused browser tab — accepts a little and then
// accepts nothing, and no RST ever arrives to end it. An unbounded write to
// such a socket is an unbounded wait in whatever goroutine is carrying it, and
// this stream is what the plane and the application above it both write
// through.
//
// Without the deadline this test does not fail, it HANGS: the write never
// returns at all, which is exactly the production symptom.
func TestAWedgedClientIsClosedRatherThanHeld(t *testing.T) {
	restore := clientWriteTimeout
	clientWriteTimeout = 250 * time.Millisecond
	t.Cleanup(func() { clientWriteTimeout = restore })

	never := make(chan struct{}) // the client that never reads
	defer close(never)
	stream, _ := clientPair(t, never)
	// Big frames, because the kernel's own buffers take the first megabytes
	// whatever the peer does. One of these writes blocks with nothing left to
	// unblock it.
	big := terminal.ServerMessage{Type: "output", Seq: 1, Data: make([]byte, 8<<20)}
	started := time.Now()
	var sendErr error
	for i := 0; i < 8 && sendErr == nil; i++ {
		sendErr = stream.Send(context.Background(), big)
	}
	if sendErr == nil {
		t.Fatal("a client that never read its socket absorbed 64MB of writes; the test cannot say anything")
	}
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Fatalf("the write to a wedged client took %s to give up", elapsed)
	}

	// And the socket is CLOSED rather than left open with a writer parked on
	// it: the next write fails at once instead of waiting out another
	// deadline.
	started = time.Now()
	if err := stream.Send(context.Background(), terminal.ServerMessage{Type: "output", Seq: 2}); err == nil {
		t.Fatal("the wedged socket was still open after its write deadline expired")
	} else if elapsed := time.Since(started); elapsed >= clientWriteTimeout {
		t.Fatalf("a write to the wedged socket took %s; it was held, not closed", elapsed)
	}
}

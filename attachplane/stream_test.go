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

	served := make(chan struct{})
	accepted := make(chan control.TerminalStream, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		accepted <- ClientStream(c)
		<-served // hold the handler open for the whole test
	}))
	t.Cleanup(func() { close(served); ts.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// The client that never reads. Nothing about it is broken: it is dialed,
	// upgraded, and simply never calls Read.
	client, _, err := websocket.Dial(ctx, "ws"+ts.URL[len("http"):], nil)
	if err != nil {
		t.Fatalf("dialing the test client: %v", err)
	}
	defer client.CloseNow()

	stream := <-accepted
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

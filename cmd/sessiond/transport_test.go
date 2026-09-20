package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/tokencanopy/rainier/internal/relay"
)

// TestTheWebSocketTransportDialsTheURLItAlwaysHas is the compatibility floor
// of this whole change, pinned at the one hop that moved.
//
// dialLoop used to call websocket.Dial inline; it now calls a dialSession.
// A session keeps the sessiond it booted with for life, so the URL that hop
// produces is a contract with a runnerd that may be months newer — and the
// seam must have changed where the call lives and nothing about what it
// sends.
func TestTheWebSocketTransportDialsTheURLItAlwaysHas(t *testing.T) {
	got := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- r.URL.RequestURI()
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		<-r.Context().Done()
	}))
	defer srv.Close()

	base := strings.Replace(srv.URL, "http", "ws", 1) + "/register"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := websocketTransport(base, "sess_example")(ctx)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	select {
	case uri := <-got:
		if uri != "/register?session=sess_example" {
			t.Fatalf("the transport dialed %q, want /register?session=sess_example", uri)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the transport never reached the server")
	}
}

// TestTheBootConnectionIsServedOnceAndThenTheDialerIs pins the one piece of
// state sessionTransport holds: the connection the microVM boot already
// established is served first and then dropped, because its configuration
// has been read and its exchange made. Running the preamble over it again
// would wait for a second configuration that is not coming.
func TestTheBootConnectionIsServedOnceAndThenTheDialerIs(t *testing.T) {
	dial, _ := fakeTransport(t)
	first, _ := fakeTransport(t)
	boot, err := first(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	preambles := 0
	tr := sessionTransport{
		dial:     dial,
		first:    boot,
		preamble: func(context.Context, relay.Conn) error { preambles++; return nil },
	}

	got, err := tr.connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != boot {
		t.Fatal("the first connect did not serve the connection the boot established")
	}
	if preambles != 0 {
		t.Fatalf("the boot connection was put through the preamble %d time(s)", preambles)
	}

	next, err := tr.connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if next == boot {
		t.Fatal("the boot connection was served twice")
	}
	if preambles != 1 {
		t.Fatalf("a redial ran the preamble %d time(s), want exactly once", preambles)
	}
}

// TestAFailedPreambleClosesTheConnection: a connection whose configuration
// could not be read is not a connection to serve, and leaving it open would
// leak one per redial for the life of the session.
func TestAFailedPreambleClosesTheConnection(t *testing.T) {
	dial, hosts := fakeTransport(t)
	tr := sessionTransport{
		dial:     dial,
		preamble: func(context.Context, relay.Conn) error { return errPreambleFailed },
	}
	if _, err := tr.connect(context.Background()); err != errPreambleFailed {
		t.Fatalf("connect = %v, want the preamble's own error", err)
	}
	host := <-hosts
	// The guest end is closed, so the host's read ends rather than blocking.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := host.conn.Read(ctx); err == nil {
		t.Fatal("a connection whose preamble failed was left open")
	}
}

var errPreambleFailed = errStub("the boot configuration never arrived")

type errStub string

func (e errStub) Error() string { return string(e) }

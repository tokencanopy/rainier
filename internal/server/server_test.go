package server

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/tokencanopy/rainier/internal/session"
	"github.com/tokencanopy/rainier/internal/wire"
)

func startBash(t *testing.T) *session.Session {
	t.Helper()
	s, err := session.New(
		session.Config{Argv: []string{"sh", "-i"}, Cols: 80, Rows: 24, LogPath: filepath.Join(t.TempDir(), "s.log")},
		session.StartProc,
	)
	if err != nil { t.Fatal(err) }
	return s
}

func dial(t *testing.T, url string, since string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, strings.Replace(url, "http", "ws", 1)+"/attach?since="+since, nil)
	if err != nil { t.Fatal(err) }
	// Match the real client fix (cmd/rattach): raise the default 32KiB
	// per-message read limit so an oversized PTY-output frame doesn't close
	// the test connection with StatusMessageTooBig.
	c.SetReadLimit(16 << 20)
	wsjson.Write(ctx, c, wire.ClientMsg{Type: "resize", Cols: 80, Rows: 24})
	return c
}

func dialSize(t *testing.T, url string, cols, rows int) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, strings.Replace(url, "http", "ws", 1)+"/attach?since=0", nil)
	if err != nil { t.Fatal(err) }
	// Match the real client fix (cmd/rattach): raise the default 32KiB
	// per-message read limit so an oversized PTY-output frame doesn't close
	// the test connection with StatusMessageTooBig.
	c.SetReadLimit(16 << 20)
	wsjson.Write(ctx, c, wire.ClientMsg{Type: "resize", Cols: cols, Rows: rows})
	return c
}

func readUntil(t *testing.T, c *websocket.Conn, want string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var all strings.Builder
	for {
		var m wire.ServerMsg
		if err := wsjson.Read(ctx, c, &m); err != nil {
			t.Fatalf("read: %v (so far: %q)", err, all.String())
		}
		all.Write(m.Data)
		if strings.Contains(all.String(), want) { return }
	}
}

// connPump is a background goroutine that reads ServerMsgs off one *websocket.Conn
// forever and forwards them (or the terminal read error) to a channel.
//
// readUntilOrTimeout cannot simply call wsjson.Read with a short-lived
// context the way readUntil does: coder/websocket treats the context passed
// to Read as a hard connection deadline, not a soft "give up on this one
// read" signal — (*Conn).setupReadTimeout arms a context.AfterFunc that
// calls (*Conn).close() the instant that context is done (see
// github.com/coder/websocket@v1.8.15/conn.go). A bounded polling call whose
// context simply expires because nothing matched yet would therefore tear
// down the whole connection out from under every later poll, rather than
// just abandoning that one wait. Routing every read through one long-lived
// pump goroutine (started once, read with context.Background() so it never
// expires) and having each bounded wait select against its output channel
// keeps polling non-destructive, and keeps every read serialized through a
// single goroutine per connection — coder/websocket only guarantees Read is
// safe to call from one goroutine at a time.
type connPump struct {
	msgs chan wire.ServerMsg
	err  chan error
}

var (
	connPumpsMu sync.Mutex
	connPumps   = map[*websocket.Conn]*connPump{}
)

func pumpFor(c *websocket.Conn) *connPump {
	connPumpsMu.Lock()
	defer connPumpsMu.Unlock()
	if p, ok := connPumps[c]; ok { return p }
	p := &connPump{msgs: make(chan wire.ServerMsg, 256), err: make(chan error, 1)}
	connPumps[c] = p
	go func() {
		for {
			var m wire.ServerMsg
			if err := wsjson.Read(context.Background(), c, &m); err != nil {
				p.err <- err
				close(p.msgs)
				return
			}
			p.msgs <- m
		}
	}()
	return p
}

// readUntilOrTimeout is readUntil but bounded by a per-call deadline and
// returning bool instead of failing the test — used to poll for a condition
// that may take a few retries (e.g. waiting on a liveness ping interval)
// without accumulating one giant fixed sleep or fatal-ing on the first miss.
// A call that times out leaves c open and unread messages queued for the
// next call (see connPump above) rather than closing the connection.
func readUntilOrTimeout(t *testing.T, c *websocket.Conn, want string, timeout time.Duration) bool {
	t.Helper()
	p := pumpFor(c)
	deadline := time.After(timeout)
	var all strings.Builder
	for {
		select {
		case m, ok := <-p.msgs:
			if !ok { return false } // connection's read loop ended (err on p.err)
			all.Write(m.Data)
			if strings.Contains(all.String(), want) { return true }
		case <-deadline:
			return false
		}
	}
}

// readUntilExit reads ServerMsgs on ctx until it observes one with
// Type == "exit". It leaves c positioned to read whatever comes next.
func readUntilExit(t *testing.T, ctx context.Context, c *websocket.Conn) {
	t.Helper()
	for {
		var m wire.ServerMsg
		if err := wsjson.Read(ctx, c, &m); err != nil {
			t.Fatalf("read: %v (exit message never observed)", err)
		}
		if m.Type == "exit" { return }
	}
}

// TestSessionExitClosesSocket covers the adaptation made to the brief's
// serve(): once the session's child process exits, att.Msgs closes on its
// own (session.Attach's documented exit contract), so the writer goroutine's
// `for range att.Msgs` drains normally instead of erroring out of a failed
// write. Without serve() explicitly closing the socket at that point, the
// reader loop's blocked wsjson.Read would hang forever — the client would
// never learn the connection is dead. This is also the concurrency-sensitive
// path Plan 2's outbound-dial reuse of serve() depends on.
func TestSessionExitClosesSocket(t *testing.T) {
	s := startBash(t)
	srv := httptest.NewServer(New(s))
	defer srv.Close()

	c := dial(t, srv.URL, "0")
	defer c.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Make the child exit: sh -i reads "exit\n" from stdin and terminates.
	if err := wsjson.Write(ctx, c, wire.ClientMsg{Type: "stdin", Data: []byte("exit\n")}); err != nil {
		t.Fatalf("write stdin: %v", err)
	}

	// (1) The client must observe an exit ServerMsg.
	readUntilExit(t, ctx, c)

	// (2) THEN — not before — the read loop must terminate: the very next
	// read must fail because serve() closed the socket after draining
	// att.Msgs, rather than hang until the shared 5s deadline expires.
	var m wire.ServerMsg
	err := wsjson.Read(ctx, c, &m)
	if err == nil {
		t.Fatalf("expected the read loop to terminate with an error after exit, got another message: %+v", m)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("read loop did not terminate within the deadline after exit (client would hang): %v", err)
	}
}

func TestAttachTypeReattach(t *testing.T) {
	s := startBash(t)
	srv := httptest.NewServer(New(s))
	defer srv.Close()

	c1 := dial(t, srv.URL, "0")
	ctx := context.Background()
	wsjson.Write(ctx, c1, wire.ClientMsg{Type: "stdin", Data: []byte("echo marker-123\n")})
	readUntil(t, c1, "marker-123")
	c1.Close(websocket.StatusNormalClosure, "detach") // client vanishes; session lives

	c2 := dial(t, srv.URL, "0") // fresh attach → snapshot must contain prior output
	readUntil(t, c2, "marker-123")
	c2.Close(websocket.StatusNormalClosure, "")
}

// burstProc is a minimal session.Proc that skips a real PTY and calls
// onOutput directly via session.New's dependency-injected start function.
//
// A real child process can't be used here: on this platform the PTY read
// queue caps individual reads at ~1KB (confirmed by direct measurement of
// internal/session.StartProc's chunking — a `head -c 40000 | tr` burst like
// the one this finding was diagnosed from arrives as ~40 separate ~1KB
// onOutput calls, never one chunk anywhere near coder/websocket's 32768-byte
// default per-message read limit). The defect is about a single oversized
// write reaching the websocket layer as one JSON message, so a scripted fake
// that issues that single write directly is what actually exercises it —
// deterministically, and independent of the host OS's tty buffering.
type burstProc struct{ done chan struct{} }

func (p *burstProc) Write(b []byte) (int, error) { return len(b), nil }
func (p *burstProc) Resize(cols, rows int) error  { return nil }
func (p *burstProc) Wait() int                    { <-p.done; return 0 }
func (p *burstProc) Stop()                        {}

// startBurst scripts: a small "start" marker (giving a since=1 reattach
// something to resume after), a pause (so a test client can be attached and
// reading *before* the burst arrives — proving the live path, not just a
// snapshot), a single >32KB write, then an end-of-burst marker.
func startBurst(argv []string, cols, rows int, onOutput func([]byte)) (session.Proc, error) {
	p := &burstProc{done: make(chan struct{})}
	go func() {
		onOutput([]byte("start\n"))
		time.Sleep(300 * time.Millisecond)
		onOutput(bytes.Repeat([]byte("x"), 40000)) // > 32768-byte default read limit
		onOutput([]byte("END-MARKER\n"))
		close(p.done)
	}()
	return p, nil
}

// TestOversizedFrameSurvivesLiveAndReplay is the regression test for the
// 32KiB default coder/websocket read limit: a single output frame that
// exceeds it closes the connection with StatusMessageTooBig, and since that
// oversized frame is what lands in the event log, --since replay would hit
// the exact same wall forever without SetReadLimit raised on both ends.
func TestOversizedFrameSurvivesLiveAndReplay(t *testing.T) {
	s, err := session.New(
		session.Config{
			Argv:    []string{"fake-burst"},
			Cols:    80, Rows: 24,
			LogPath: filepath.Join(t.TempDir(), "s.log"),
		},
		startBurst,
	)
	if err != nil { t.Fatal(err) }
	srv := httptest.NewServer(New(s))
	defer srv.Close()

	// Live path: attach before the burst happens and read straight through
	// it. Without SetReadLimit, the >32KB frame kills the connection and
	// readUntil's wsjson.Read fails before it ever sees END-MARKER.
	c1 := dial(t, srv.URL, "0")
	readUntil(t, c1, "start")
	readUntil(t, c1, "END-MARKER")
	c1.Close(websocket.StatusNormalClosure, "live done")

	// Wait for the child to fully exit so the whole burst is guaranteed
	// flushed to the event log before we test replay against it.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case <-s.Exited():
	case <-ctx.Done():
		t.Fatal("session did not exit in time")
	}

	// Snapshot-path reattach: since=0 attaches fresh and gets a
	// term.Serialize() repaint of the current screen, not the raw logged
	// frame — a basic sanity check that the fixed clients still work here.
	c2 := dial(t, srv.URL, "0")
	readUntil(t, c2, "END-MARKER")
	c2.Close(websocket.StatusNormalClosure, "snapshot reattach done")

	// Replay path: since=1 forces the server to resend everything logged
	// after the "start" marker (seq 1) — including the oversized raw output
	// frame(s) verbatim. This is the path that previously died forever once
	// that frame was written to the log.
	c3 := dial(t, srv.URL, "1")
	readUntil(t, c3, "END-MARKER")
	c3.Close(websocket.StatusNormalClosure, "replay done")
}

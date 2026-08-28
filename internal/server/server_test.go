package server

import (
	"context"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"rainier/internal/session"
	"rainier/internal/wire"
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
	wsjson.Write(ctx, c, wire.ClientMsg{Type: "resize", Cols: 80, Rows: 24})
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

package server

import (
	"context"
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

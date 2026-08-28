// internal/runnerd/runnerd_test.go
package runnerd

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"rainier/internal/driver"
	"rainier/internal/relay"
	"rainier/internal/session"
	"rainier/internal/wire"
)

// This test wires the real relay end-to-end without Docker: create a session
// via the API, then simulate the container by dialing /register from an
// in-process sessiond bound to a real session.Session, then attach a client
// via /attach and assert output flows.
func TestRunnerdCreateRegisterAttach(t *testing.T) {
	srv := httptest.NewServer(New(driver.NewFake(4), "", "").Handler())
	defer srv.Close()
	base := strings.Replace(srv.URL, "http", "ws", 1)
	ctx := context.Background()

	// Create.
	id := createSession(t, srv.URL) // helper: POST /sessions, returns session_id

	// Simulate the container's sessiond dialing /register.
	sess, err := session.New(
		session.Config{Argv: []string{"sh", "-i"}, Cols: 80, Rows: 24, LogPath: t.TempDir() + "/s.log"},
		session.StartProc,
	)
	if err != nil {
		t.Fatal(err)
	}
	regConn, _, err := websocket.Dial(ctx, base+"/register?session="+id, nil)
	if err != nil {
		t.Fatal(err)
	}
	regConn.SetReadLimit(16 << 20)
	go relay.ServeSession(ctx, relay.WSConn(regConn), sess)

	// Attach a client.
	cli, _, err := websocket.Dial(ctx, base+"/attach?session="+id, nil)
	if err != nil {
		t.Fatal(err)
	}
	cli.SetReadLimit(16 << 20)
	wsjson.Write(ctx, cli, wire.ClientMsg{Type: "resize", Cols: 80, Rows: 24})

	// Expect snapshot then echoed marker.
	wsjson.Write(ctx, cli, wire.ClientMsg{Type: "stdin", Data: []byte("echo runnerd-marker\n")})
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("no runnerd-marker through runnerd relay")
		default:
		}
		var m wire.ServerMsg
		if err := wsjson.Read(ctx, cli, &m); err != nil {
			t.Fatal(err)
		}
		if m.Type == "output" && strings.Contains(string(m.Data), "runnerd-marker") {
			return
		}
	}
}

func createSession(t *testing.T, baseURL string) string {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"name": "t", "image": "x"})
	resp, err := http.Post(baseURL+"/sessions", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		SessionID string `json:"session_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.SessionID == "" {
		t.Fatal("empty session_id")
	}
	return out.SessionID
}

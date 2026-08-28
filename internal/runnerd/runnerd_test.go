// internal/runnerd/runnerd_test.go
package runnerd

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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
	dialRegisterAndServe(t, ctx, base, id)

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
	id, err := postSession(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Fatal("empty session_id")
	}
	return id
}

// postSession is createSession's error-returning core, split out so the
// concurrent-create test below can call it from goroutines without calling
// t.Fatal off the test goroutine (FailNow/Fatal are documented as unsafe to
// call from anywhere else).
func postSession(baseURL string) (string, error) {
	body, _ := json.Marshal(map[string]any{"name": "t", "image": "x"})
	resp, err := http.Post(baseURL+"/sessions", "application/json", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var out struct {
		SessionID string `json:"session_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	return out.SessionID, nil
}

// TestSessionsConcurrentCreateUniqueIDs is the regression test for C1: newID
// used to do an unsynchronized s.seq++, so concurrent POST /sessions (the
// normal fleet operating mode — many callers creating sessions at once)
// could collide on the same id, and registry.put would silently overwrite
// the first session's entry.
func TestSessionsConcurrentCreateUniqueIDs(t *testing.T) {
	const n = 20
	srv := httptest.NewServer(New(driver.NewFake(n), "", "").Handler())
	defer srv.Close()

	var wg sync.WaitGroup
	ids := make([]string, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ids[i], errs[i] = postSession(srv.URL)
		}(i)
	}
	wg.Wait()

	seen := make(map[string]bool, n)
	for i, id := range ids {
		if errs[i] != nil {
			t.Fatalf("create %d: %v", i, errs[i])
		}
		if id == "" {
			t.Fatalf("create %d: empty session id", i)
		}
		if seen[id] {
			t.Fatalf("duplicate session id %q", id)
		}
		seen[id] = true
	}
	if len(seen) != n {
		t.Fatalf("got %d unique ids, want %d", len(seen), n)
	}

	resp, err := http.Get(srv.URL + "/sessions")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var rows []struct{ ID, State string }
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != n {
		t.Fatalf("GET /sessions listed %d entries, want %d", len(rows), n)
	}
}

// dialRegisterAndServe simulates a container's sessiond: it creates a real
// (Docker-free) session.Session running argv, dials /register?session=id,
// and pumps relay.ServeSession over that connection in the background. It
// returns the client-side conn so the test can kill it to simulate the
// container dying.
func dialRegisterAndServe(t *testing.T, ctx context.Context, wsBase, id string) *websocket.Conn {
	t.Helper()
	sess, err := session.New(
		session.Config{Argv: []string{"sh", "-i"}, Cols: 80, Rows: 24, LogPath: t.TempDir() + "/s.log"},
		session.StartProc,
	)
	if err != nil {
		t.Fatal(err)
	}
	regConn, _, err := websocket.Dial(ctx, wsBase+"/register?session="+id, nil)
	if err != nil {
		t.Fatal(err)
	}
	regConn.SetReadLimit(16 << 20)
	go relay.ServeSession(ctx, relay.WSConn(regConn), sess)
	return regConn
}

// waitForHub polls the registry (directly — this test is in-package) until
// the given session has a hub registered, or fails the test after a bound.
func waitForHub(t *testing.T, rd *Server, id string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := rd.reg.hub(id); ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("session %s never registered a hub", id)
}

// TestRegisterCleansUpOnSessionConnDeath is the regression test for the
// register-lifetime fix: killing the sessiond-side conn (simulating the
// container dying) must reap the registry entry within a bounded deadline,
// not leak it (and the /register handler goroutine) forever.
func TestRegisterCleansUpOnSessionConnDeath(t *testing.T) {
	rd := New(driver.NewFake(4), "", "")
	srv := httptest.NewServer(rd.Handler())
	defer srv.Close()
	base := strings.Replace(srv.URL, "http", "ws", 1)
	ctx := context.Background()

	id := createSession(t, srv.URL)
	regConn := dialRegisterAndServe(t, ctx, base, id)
	waitForHub(t, rd, id)

	// Confirm the path actually works end to end before killing it, so a
	// failure below is unambiguously about cleanup, not plumbing.
	cli, _, err := websocket.Dial(ctx, base+"/attach?session="+id, nil)
	if err != nil {
		t.Fatal(err)
	}
	cli.SetReadLimit(16 << 20)
	wsjson.Write(ctx, cli, wire.ClientMsg{Type: "resize", Cols: 80, Rows: 24})
	wsjson.Write(ctx, cli, wire.ClientMsg{Type: "stdin", Data: []byte("echo pre-death-marker\n")})
	found := false
	echoDeadline := time.After(5 * time.Second)
	for !found {
		select {
		case <-echoDeadline:
			t.Fatal("no pre-death-marker before killing the sessiond conn")
		default:
		}
		var m wire.ServerMsg
		if err := wsjson.Read(ctx, cli, &m); err != nil {
			t.Fatal(err)
		}
		if m.Type == "output" && strings.Contains(string(m.Data), "pre-death-marker") {
			found = true
		}
	}
	cli.CloseNow()

	// Simulate the container dying: close the sessiond-side conn out from
	// under the hub.
	regConn.CloseNow()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := rd.reg.get(id); !ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("session %s still in registry 3s after sessiond conn closed", id)
}

// TestDeleteSessionRemovesRegistryEntryAndClosesHub is the regression test
// for DELETE now closing a registered session's hub: the entry must be gone
// synchronously with the 204 response, and stay gone (register()'s own
// unblock-and-cleanup racing the same removal must be a no-op, not a re-add
// or a panic) for a subsequent GET /sessions.
func TestDeleteSessionRemovesRegistryEntryAndClosesHub(t *testing.T) {
	rd := New(driver.NewFake(4), "", "")
	srv := httptest.NewServer(rd.Handler())
	defer srv.Close()
	base := strings.Replace(srv.URL, "http", "ws", 1)
	ctx := context.Background()

	id := createSession(t, srv.URL)
	dialRegisterAndServe(t, ctx, base, id)
	waitForHub(t, rd, id)

	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/sessions/"+id, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}

	if _, ok := rd.reg.get(id); ok {
		t.Fatal("session still in registry immediately after DELETE response")
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		resp, err := http.Get(srv.URL + "/sessions")
		if err != nil {
			t.Fatal(err)
		}
		var rows []struct{ ID, State string }
		json.NewDecoder(resp.Body).Decode(&rows)
		resp.Body.Close()
		found := false
		for _, row := range rows {
			if row.ID == id {
				found = true
			}
		}
		if !found {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("session %s still listed in GET /sessions after DELETE", id)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

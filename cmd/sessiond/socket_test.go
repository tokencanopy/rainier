package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// socketPath returns a short path for a test's unix socket. t.TempDir() is not
// used: a unix socket's path lives in a fixed-size sun_path field (104 bytes on
// darwin, 108 on linux), and macOS's per-test TMPDIR plus a test name can
// overrun it — a failure that reads as "invalid argument" and has nothing to do
// with what the test is checking.
func socketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "rnr")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return filepath.Join(dir, "agent.sock")
}

// startSocket serves calls on a fresh listener and returns its path.
func startSocket(t *testing.T, deadline time.Duration, call func(string, json.RawMessage) (json.RawMessage, error)) string {
	t.Helper()
	path := socketPath(t)
	ln, err := listenAgentSocket(path)
	if err != nil {
		t.Fatalf("listenAgentSocket: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go (&agentSocket{call: call, deadline: deadline}).serve(ctx, ln)
	return path
}

// ask performs one request/response exchange, the way the credential helper
// does: dial, write one JSON object, read one back, close.
func ask(t *testing.T, path, body string) socketResponse {
	t.Helper()
	c, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial %s: %v", path, err)
	}
	defer c.Close()
	if _, err := io.WriteString(c, body); err != nil {
		t.Fatalf("write: %v", err)
	}
	var resp socketResponse
	if err := json.NewDecoder(c).Decode(&resp); err != nil {
		t.Fatalf("read response: %v", err)
	}
	return resp
}

// TestAgentSocketRoundTrip: one request per connection, one response, and the
// method and payload reach the call unchanged — this socket is the entire
// interface between the in-sandbox credential helper and sessiond.
func TestAgentSocketRoundTrip(t *testing.T) {
	type got struct {
		method  string
		payload string
	}
	calls := make(chan got, 4)
	path := startSocket(t, time.Second, func(method string, payload json.RawMessage) (json.RawMessage, error) {
		calls <- got{method, string(payload)}
		return json.RawMessage(`{"token":"ghs_x"}`), nil
	})

	resp := ask(t, path, `{"method":"mint_git_credential","payload":{"host":"github.com"}}`)
	if !resp.OK {
		t.Fatalf("response = %+v, want ok", resp)
	}
	if string(resp.Payload) != `{"token":"ghs_x"}` {
		t.Fatalf("payload = %s, want the call's own answer", resp.Payload)
	}
	select {
	case c := <-calls:
		if c.method != "mint_git_credential" || c.payload != `{"host":"github.com"}` {
			t.Fatalf("call = %+v, want the request's method and payload verbatim", c)
		}
	default:
		t.Fatal("the request never reached the call")
	}

	// The listener keeps serving: a helper runs once per git operation, so a
	// second connection has to work exactly like the first.
	if resp := ask(t, path, `{"method":"mint_git_credential"}`); !resp.OK {
		t.Fatalf("second response = %+v, want ok", resp)
	}
}

// TestAgentSocketReportsFailures: a refusal upstream (a credential needing a
// refresh) must reach the helper as a message it can print, not as a dropped
// connection the helper would report as "sessiond is broken".
func TestAgentSocketReportsFailures(t *testing.T) {
	const msg = "github credentials need a refresh: run `rainier login --refresh github`"
	path := startSocket(t, time.Second, func(string, json.RawMessage) (json.RawMessage, error) {
		return nil, errors.New(msg)
	})

	resp := ask(t, path, `{"method":"mint_git_credential"}`)
	if resp.OK {
		t.Fatalf("response = %+v, want ok:false", resp)
	}
	if resp.Error != msg {
		t.Fatalf("error = %q, want the upstream message verbatim", resp.Error)
	}
}

// TestAgentSocketRejectsAMethodlessRequest: an empty method has nothing to
// call, and answering it rather than hanging up is what lets a buggy client
// see why.
func TestAgentSocketRejectsAMethodlessRequest(t *testing.T) {
	path := startSocket(t, time.Second, func(string, json.RawMessage) (json.RawMessage, error) {
		t.Error("the call ran for a request with no method")
		return nil, nil
	})
	if resp := ask(t, path, `{"payload":{}}`); resp.OK || resp.Error == "" {
		t.Fatalf("response = %+v, want an ok:false naming the problem", resp)
	}
}

// TestAgentSocketDeadlineClosesAStalledClient: a client that connects and then
// says nothing must not hold a goroutine (and a connection) open for the life
// of the session. Nothing on this socket is long-lived — the helper writes its
// request immediately or it is broken.
func TestAgentSocketDeadlineClosesAStalledClient(t *testing.T) {
	path := startSocket(t, 100*time.Millisecond, func(string, json.RawMessage) (json.RawMessage, error) {
		t.Error("the call ran for a client that never sent a request")
		return nil, nil
	})

	c, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	c.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.ReadAll(c); err != nil {
		// A read error is fine too (the peer closed abruptly); what must not
		// happen is the read still blocking after the deadline should have
		// closed the connection.
		if os.IsTimeout(err) {
			t.Fatal("the stalled connection was still open after its deadline")
		}
	}
}

// TestListenAgentSocketReplacesAStaleSocket: /workspace persists across a cold
// park, so a previous boot's socket file is normally still sitting there. Bind
// would fail on it ("address already in use") and the credential helper would
// have nothing to talk to for the whole life of the session.
func TestListenAgentSocketReplacesAStaleSocket(t *testing.T) {
	path := socketPath(t)
	if err := os.WriteFile(path, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}

	ln, err := listenAgentSocket(path)
	if err != nil {
		t.Fatalf("listenAgentSocket over a stale file: %v", err)
	}
	defer ln.Close()

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o700 {
		t.Fatalf("socket mode = %04o, want 0700 — only the session's own user may ask for its credentials", perm)
	}
}

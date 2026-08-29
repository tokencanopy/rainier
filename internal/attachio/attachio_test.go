package attachio

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"rainier/internal/wire"
)

// TestAttachURL is moved verbatim from cmd/rattach/main_test.go's
// TestAttachURL (unexported attachURL there) — same cases, now against the
// exported AttachURL.
func TestAttachURL(t *testing.T) {
	t.Run("session present", func(t *testing.T) {
		got := AttachURL("ws://127.0.0.1:8080", 42, "sess-7")
		if !strings.Contains(got, "since=42") {
			t.Errorf("AttachURL(...) = %q, want it to contain since=42", got)
		}
		if !strings.Contains(got, "session=sess-7") {
			t.Errorf("AttachURL(...) = %q, want it to contain session=sess-7", got)
		}
	})

	t.Run("session present with characters needing escaping", func(t *testing.T) {
		got := AttachURL("ws://127.0.0.1:8080", 0, "sess a/b")
		if !strings.Contains(got, "session=sess+a%2Fb") {
			t.Errorf("AttachURL(...) = %q, want an escaped session param", got)
		}
	})

	t.Run("session empty", func(t *testing.T) {
		got := AttachURL("ws://127.0.0.1:7070", 0, "")
		if !strings.Contains(got, "since=0") {
			t.Errorf("AttachURL(...) = %q, want it to contain since=0", got)
		}
		if strings.Contains(got, "session=") {
			t.Errorf("AttachURL(...) = %q, want no session param when session is empty", got)
		}
	})
}

// TestScanDetach pins the three cases the brief calls out: mid-buffer,
// absent, and first byte.
func TestScanDetach(t *testing.T) {
	t.Run("mid-buffer", func(t *testing.T) {
		buf := []byte("hello\x1dworld")
		if got := ScanDetach(buf); got != 5 {
			t.Errorf("ScanDetach(%q) = %d, want 5", buf, got)
		}
	})

	t.Run("absent", func(t *testing.T) {
		buf := []byte("hello world")
		if got := ScanDetach(buf); got != -1 {
			t.Errorf("ScanDetach(%q) = %d, want -1", buf, got)
		}
	})

	t.Run("first byte", func(t *testing.T) {
		buf := []byte("\x1dhello")
		if got := ScanDetach(buf); got != 0 {
			t.Errorf("ScanDetach(%q) = %d, want 0", buf, got)
		}
	})

	t.Run("empty buffer", func(t *testing.T) {
		if got := ScanDetach(nil); got != -1 {
			t.Errorf("ScanDetach(nil) = %d, want -1", got)
		}
	})
}

// ---------------------------------------------------------------------------
// Run: the stdout race probe (review round 1, finding 1) and the
// ErrSessionNotReady sentinel (finding 2).
// ---------------------------------------------------------------------------

// TestRunNoRaceOnFloodedOutputDuringDetach is the reviewer's reproduction,
// kept as a permanent regression test: a session flooding "output" as fast
// as possible, right up to (and past) the instant the client detaches.
// Before the stdoutMu fix, only the three DECISION paths (disconnect/exit/
// detach) were gated — the reader goroutine's ordinary
// os.Stdout.Write(m.Data) for a plain "output" message was not, so with
// output still streaming at detach time the reader could still be mid-write
// when Run() returned and handed control back to a caller free to reassign
// os.Stdout immediately (exactly what this test does). Run with -race;
// asserted clean at -count=60 during this fix (see task-12-report.md).
func TestRunNoRaceOnFloodedOutputDuringDetach(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		c.SetReadLimit(16 << 20)
		ctx := r.Context()

		// The resize-first contract's required first message.
		var first wire.ClientMsg
		if err := wsjson.Read(ctx, c, &first); err != nil {
			return
		}

		stop := make(chan struct{})
		// Drain whatever the client sends (stdin) so Run's writes never
		// block on an unread server-side buffer; closes stop once the
		// client goes away.
		go func() {
			defer close(stop)
			for {
				var m wire.ClientMsg
				if err := wsjson.Read(ctx, c, &m); err != nil {
					return
				}
			}
		}()

		// Flood output as fast as possible until the client disconnects —
		// the point is to keep the reader goroutine continuously busy
		// writing to stdout right up to the moment of detach.
		for seq := uint64(1); ; seq++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := wsjson.Write(ctx, c, wire.ServerMsg{Type: "output", Seq: seq, Data: []byte("x")}); err != nil {
				return
			}
		}
	}))
	defer ts.Close()
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/attach"

	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe (stdin): %v", err)
	}
	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe (stdout): %v", err)
	}
	origStdin, origStdout := os.Stdin, os.Stdout
	os.Stdin, os.Stdout = stdinR, stdoutW
	restoreStd := func() { os.Stdin, os.Stdout = origStdin, origStdout }
	t.Cleanup(restoreStd)

	var drained atomic.Int64
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		buf := make([]byte, 32*1024)
		for {
			n, err := stdoutR.Read(buf)
			drained.Add(int64(n))
			if err != nil {
				return
			}
		}
	}()

	runErr := make(chan error, 1)
	go func() { runErr <- Run(context.Background(), wsURL, nil, 0) }()

	// Let the flood actually get going — several KB in — before detaching,
	// rather than detaching the instant Run starts: the goal is a real
	// backlog of already-buffered frames in the socket (and the reader
	// goroutine actively mid read-decode-write) at the moment of detach, not
	// a cold start that races nothing.
	deadline := time.Now().Add(5 * time.Second)
	for drained.Load() < 8192 {
		if !time.Now().Before(deadline) {
			t.Fatal("flood never produced 8KiB of output within 5s")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := stdinW.Write([]byte{0x1d}); err != nil {
		t.Fatalf("write detach key: %v", err)
	}

	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return within 5s of the detach key")
	}

	// The instant Run returns, reassign os.Stdout — exactly the caller
	// pattern the reviewer's repro exercises. Under -race this must not
	// report a write racing this reassignment.
	restoreStd()
	stdinW.Close()
	stdoutW.Close()
	<-drainDone
}

// TestRunSessionNotReadyMapsToSentinel pins finding 2: a 503 dial response
// (controld's session_not_ready) must be errors.Is-matchable against
// ErrSessionNotReady, wrapped in a *DialError carrying the status.
func TestRunSessionNotReadyMapsToSentinel(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"error":{"code":"session_not_ready","message":"session is creating, not running"}}`))
	}))
	defer ts.Close()
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/attach"

	err := Run(context.Background(), wsURL, nil, 0)
	if err == nil {
		t.Fatal("Run: want an error for a 503 response, got nil")
	}
	if !errors.Is(err, ErrSessionNotReady) {
		t.Fatalf("Run error = %v, want errors.Is(err, ErrSessionNotReady)", err)
	}
	var de *DialError
	if !errors.As(err, &de) {
		t.Fatalf("Run error = %v (%T), want a *DialError", err, err)
	}
	if de.Status != http.StatusServiceUnavailable {
		t.Fatalf("DialError.Status = %d, want %d", de.Status, http.StatusServiceUnavailable)
	}
}

// TestRunNon503DialErrorDoesNotMatchSentinel asserts the sentinel is
// specific to 503 — a *DialError for any other status must not match
// ErrSessionNotReady, so callers retrying on it can't accidentally treat an
// unrelated failure as "just not ready yet".
func TestRunNon503DialErrorDoesNotMatchSentinel(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/attach"

	err := Run(context.Background(), wsURL, nil, 0)
	if err == nil {
		t.Fatal("Run: want an error for a 500 response, got nil")
	}
	if errors.Is(err, ErrSessionNotReady) {
		t.Fatalf("Run error = %v, want it NOT to match ErrSessionNotReady for a 500", err)
	}
	var de *DialError
	if !errors.As(err, &de) || de.Status != http.StatusInternalServerError {
		t.Fatalf("Run error = %v, want a *DialError with Status=500", err)
	}
}

// TestRunTransportErrorIsNotADialError asserts a pure transport failure
// (nothing listening — no HTTP response at all) is returned unwrapped, not
// as a *DialError, so callers can still match the underlying net/url error.
func TestRunTransportErrorIsNotADialError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	closedURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/attach"
	ts.Close() // nothing is listening here any more

	err := Run(context.Background(), closedURL, nil, 0)
	if err == nil {
		t.Fatal("Run: want a transport error, got nil")
	}
	var de *DialError
	if errors.As(err, &de) {
		t.Fatalf("Run error = %v, want a plain transport error (no HTTP response), not a *DialError", err)
	}
}

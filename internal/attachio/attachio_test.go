package attachio

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/creack/pty"
	"golang.org/x/sys/unix"

	"rainier/internal/wire"
)

// TestAttachURL is moved from cmd/rattach/main_test.go's TestAttachURL
// (unexported attachURL there) — same cases, now against the exported
// AttachURL, whose cursor argument moved into Run (see
// TestRunDialsWithTheCursor).
func TestAttachURL(t *testing.T) {
	t.Run("session present", func(t *testing.T) {
		got := AttachURL("ws://127.0.0.1:8080", "sess-7")
		if !strings.HasPrefix(got, "ws://127.0.0.1:8080/attach") {
			t.Errorf("AttachURL(...) = %q, want the base's /attach path", got)
		}
		if !strings.Contains(got, "session=sess-7") {
			t.Errorf("AttachURL(...) = %q, want it to contain session=sess-7", got)
		}
	})

	t.Run("session present with characters needing escaping", func(t *testing.T) {
		got := AttachURL("ws://127.0.0.1:8080", "sess a/b")
		if !strings.Contains(got, "session=sess+a%2Fb") {
			t.Errorf("AttachURL(...) = %q, want an escaped session param", got)
		}
	})

	t.Run("session empty", func(t *testing.T) {
		got := AttachURL("ws://127.0.0.1:7070", "")
		if got != "ws://127.0.0.1:7070/attach" {
			t.Errorf("AttachURL(...) = %q, want a bare /attach URL", got)
		}
	})
}

type shortWriter struct {
	bytes.Buffer
}

func (w *shortWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return w.Buffer.Write(p[:len(p)-1])
}

// TestCursor pins the one mapping both CLIs share: a `--since` flag that was
// never typed is not the same request as `--since 0`, and the whole reported
// bug lives in the gap between them.
func TestCursor(t *testing.T) {
	if got := Cursor(false, 0); got != 0 {
		t.Errorf("Cursor(not given) = %d, want 0 (no cursor: snapshot then live)", got)
	}
	if got := Cursor(true, 0); got != wire.SinceAll {
		t.Errorf("Cursor(--since 0) = %d, want wire.SinceAll (%d)", got, wire.SinceAll)
	}
	if got := Cursor(true, 19); got != 19 {
		t.Errorf("Cursor(--since 19) = %d, want 19", got)
	}
	// A flag value that happens to equal the sentinel is still the sentinel;
	// there is no cursor above it to confuse it with.
	if got := Cursor(true, wire.SinceAll); got != wire.SinceAll {
		t.Errorf("Cursor(--since SinceAll) = %d, want %d", got, wire.SinceAll)
	}
}

// TestRunDialsWithTheCursor is the regression test for the bug the Plan 3
// overnight run found on live infrastructure: `rainier attach <id> --since 0`
// rendered a screen snapshot and nothing else, while the session's event log
// was intact and contiguous on the server. Run accepted a `since` argument
// and never put it anywhere — the URL it dialed carried no cursor at all, so
// every `--since N` the CLI ever passed was dropped on the floor before it
// left the client. (cmd/rattach was unaffected only because it happened to
// spell the cursor into the URL itself.)
//
// The cursor is the one thing this test looks at: whatever URL a caller
// hands Run, the URL Run actually dials must carry `since=<cursor>`.
func TestRunDialsWithTheCursor(t *testing.T) {
	for _, tc := range []struct {
		name  string
		url   func(base string) string
		since uint64
		want  string
	}{
		{"resume cursor", func(b string) string { return b + "/v1/sessions/sess_1/attach" }, 7, "since=7"},
		{"no cursor", func(b string) string { return b + "/v1/sessions/sess_1/attach" }, 0, "since=0"},
		{"whole log", func(b string) string { return b + "/v1/sessions/sess_1/attach" },
			wire.SinceAll, "since=" + strconv.FormatUint(wire.SinceAll, 10)},
		// A URL that already carries a query (rattach's --session) must get
		// the cursor appended, not a second '?' that swallows it.
		{"url already has a query", func(b string) string { return b + "/attach?session=sess-7" }, 3, "since=3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			queries := make(chan string, 1)
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case queries <- r.URL.RawQuery:
				default:
				}
				// Refuse the upgrade: the dial URL is the whole subject here,
				// and a failed handshake gets Run back without any terminal
				// I/O to stage.
				w.WriteHeader(http.StatusInternalServerError)
			}))
			defer ts.Close()

			_, _ = Run(context.Background(), tc.url("ws"+strings.TrimPrefix(ts.URL, "http")), nil, tc.since)

			select {
			case got := <-queries:
				if !strings.Contains(got, tc.want) {
					t.Fatalf("dialed query = %q, want it to contain %q", got, tc.want)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Run never dialed the server")
			}
		})
	}
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

	type runResult struct {
		out Outcome
		err error
	}
	runDone := make(chan runResult, 1)
	go func() {
		out, err := Run(context.Background(), wsURL, nil, 0)
		runDone <- runResult{out: out, err: err}
	}()

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
	case result := <-runDone:
		if result.err != nil {
			t.Fatalf("Run: %v", result.err)
		}
		if result.out.Reason != Detached {
			t.Fatalf("Run outcome = %+v, want reason Detached", result.out)
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

	_, err := Run(context.Background(), wsURL, nil, 0)
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

	_, err := Run(context.Background(), wsURL, nil, 0)
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

	_, err := Run(context.Background(), closedURL, nil, 0)
	if err == nil {
		t.Fatal("Run: want a transport error, got nil")
	}
	var de *DialError
	if errors.As(err, &de) {
		t.Fatalf("Run error = %v, want a plain transport error (no HTTP response), not a *DialError", err)
	}
}

// TestRunReportsDisconnectCursor is the contract the reconnecting product CLI
// needs: only a transport loss is retryable, and the next attach must start
// after the last frame that actually reached local stdout. A snapshot counts
// as rendered state and therefore advances the cursor too.
func TestRunReportsDisconnectCursor(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		var first wire.ClientMsg
		if err := wsjson.Read(r.Context(), c, &first); err != nil {
			return
		}
		wsjson.Write(r.Context(), c, wire.ServerMsg{Type: "snapshot", Seq: 17, Data: []byte("ready")})
		c.Close(websocket.StatusGoingAway, "synthetic network interruption")
	}))
	defer ts.Close()

	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe (stdin): %v", err)
	}
	defer stdinR.Close()
	defer stdinW.Close()
	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe (stdout): %v", err)
	}
	defer stdoutR.Close()
	origStdin, origStdout := os.Stdin, os.Stdout
	os.Stdin, os.Stdout = stdinR, stdoutW
	t.Cleanup(func() { os.Stdin, os.Stdout = origStdin, origStdout })
	drainDone := make(chan struct{})
	go func() {
		io.Copy(io.Discard, stdoutR)
		close(drainDone)
	}()

	outcome, err := Run(context.Background(), "ws"+strings.TrimPrefix(ts.URL, "http")+"/attach", nil, 9)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if outcome.Reason != Disconnected || outcome.LastSeq != 17 {
		t.Fatalf("Run outcome = %+v, want disconnected at seq 17", outcome)
	}

	// No frame arrived on this attempt, so the cursor the caller supplied is
	// still the last known rendered frame. Reconnecting from zero here would
	// repaint or replay unrelated history.
	beforeFrame := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		var first wire.ClientMsg
		if err := wsjson.Read(r.Context(), c, &first); err != nil {
			return
		}
		c.Close(websocket.StatusGoingAway, "synthetic interruption before first frame")
	}))
	defer beforeFrame.Close()
	outcome, err = Run(context.Background(), "ws"+strings.TrimPrefix(beforeFrame.URL, "http")+"/attach", nil, 23)
	if err != nil {
		t.Fatalf("Run before first frame: %v", err)
	}
	if outcome.Reason != Disconnected || outcome.LastSeq != 23 {
		t.Fatalf("Run before first frame outcome = %+v, want disconnected at original seq 23", outcome)
	}

	exitServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		var first wire.ClientMsg
		if err := wsjson.Read(r.Context(), c, &first); err != nil {
			return
		}
		wsjson.Write(r.Context(), c, wire.ServerMsg{Type: "exit", ExitCode: 7})
	}))
	defer exitServer.Close()
	outcome, err = Run(context.Background(), "ws"+strings.TrimPrefix(exitServer.URL, "http")+"/attach", nil, 23)
	if err != nil {
		t.Fatalf("Run exit: %v", err)
	}
	if outcome.Reason != Exited || outcome.LastSeq != 23 || outcome.ExitCode != 7 {
		t.Fatalf("Run exit outcome = %+v, want exited at seq 23 with code 7", outcome)
	}

	stdoutW.Close()
	<-drainDone
}

func TestRunPreservesSinceAllBeforeFirstFrame(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		var first wire.ClientMsg
		if err := wsjson.Read(r.Context(), c, &first); err != nil {
			return
		}
		c.Close(websocket.StatusGoingAway, "synthetic interruption before replay")
	}))
	defer ts.Close()

	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdinR.Close()
	defer stdinW.Close()
	outcome, err := runWithIO(context.Background(), "ws"+strings.TrimPrefix(ts.URL, "http")+"/attach", nil, wire.SinceAll, stdinR, io.Discard)
	if err != nil {
		t.Fatalf("runWithIO: %v", err)
	}
	if outcome.Reason != Disconnected || outcome.LastSeq != wire.SinceAll {
		t.Fatalf("outcome = %+v, want disconnected at SinceAll", outcome)
	}
}

func TestRunRejectsPermanentWebSocketClose(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		var first wire.ClientMsg
		if err := wsjson.Read(r.Context(), c, &first); err != nil {
			return
		}
		c.Close(websocket.StatusPolicyViolation, "synthetic policy rejection")
	}))
	defer ts.Close()

	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdinR.Close()
	defer stdinW.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	_, err = runWithIO(ctx, "ws"+strings.TrimPrefix(ts.URL, "http")+"/attach", nil, 0, stdinR, io.Discard)
	if err == nil || websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
		t.Fatalf("runWithIO error = %v, want policy close", err)
	}
}

func TestOversizedFrameIsNotRetryable(t *testing.T) {
	if retryableWebSocketReadError(websocket.ErrMessageTooBig) {
		t.Fatal("websocket.ErrMessageTooBig must be a permanent local protocol error")
	}
}

func TestRunRejectsUnknownServerMessage(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		var first wire.ClientMsg
		if err := wsjson.Read(r.Context(), c, &first); err != nil {
			return
		}
		wsjson.Write(r.Context(), c, wire.ServerMsg{Type: "synthetic-unknown"})
		<-r.Context().Done()
	}))
	defer ts.Close()
	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdinR.Close()
	defer stdinW.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	_, err = runWithIO(ctx, "ws"+strings.TrimPrefix(ts.URL, "http")+"/attach", nil, 0, stdinR, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "unsupported server message type") {
		t.Fatalf("runWithIO error = %v, want permanent unknown-message error", err)
	}
}

func TestRunDoesNotAdvanceCursorAfterShortOutputWrite(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		var first wire.ClientMsg
		if err := wsjson.Read(r.Context(), c, &first); err != nil {
			return
		}
		wsjson.Write(r.Context(), c, wire.ServerMsg{Type: "output", Seq: 17, Data: []byte("render-me")})
		<-r.Context().Done()
	}))
	defer ts.Close()

	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdinR.Close()
	defer stdinW.Close()
	w := &shortWriter{}
	outcome, err := runWithIO(context.Background(), "ws"+strings.TrimPrefix(ts.URL, "http")+"/attach", nil, 9, stdinR, w)
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("runWithIO error = %v, want io.ErrShortWrite", err)
	}
	if outcome.LastSeq != 9 {
		t.Fatalf("outcome cursor = %d, want prior cursor 9", outcome.LastSeq)
	}
}

func TestDiscardPendingInputFlushesTTYButPreservesPipes(t *testing.T) {
	master, slave, err := pty.Open()
	if err != nil {
		t.Fatalf("pty.Open: %v", err)
	}
	defer master.Close()
	defer slave.Close()
	if _, err := master.Write([]byte("typed-during-outage\n")); err != nil {
		t.Fatalf("queue tty input: %v", err)
	}
	if err := discardPendingInput(slave); err != nil {
		t.Fatalf("discardPendingInput(tty): %v", err)
	}
	fds := []unix.PollFd{{Fd: int32(slave.Fd()), Events: unix.POLLIN}}
	ready, err := unix.Poll(fds, 0)
	if err != nil {
		t.Fatalf("poll flushed tty: %v", err)
	}
	if ready != 0 {
		t.Fatalf("poll flushed tty = %d ready descriptors, want no queued input", ready)
	}

	pipeR, pipeW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer pipeR.Close()
	defer pipeW.Close()
	if _, err := pipeW.Write([]byte("scripted-input")); err != nil {
		t.Fatal(err)
	}
	if err := discardPendingInput(pipeR); err != nil {
		t.Fatalf("discardPendingInput(pipe): %v", err)
	}
	got := make([]byte, len("scripted-input"))
	if _, err := io.ReadFull(pipeR, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != "scripted-input" {
		t.Fatalf("pipe input = %q, want preserved scripted input", got)
	}
}

func TestRunDiscardsTTYInputQueuedDuringDial(t *testing.T) {
	handlerStarted := make(chan struct{})
	allowUpgrade := make(chan struct{})
	receivedInput := make(chan bool, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(handlerStarted)
		<-allowUpgrade
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		var resize wire.ClientMsg
		if err := wsjson.Read(r.Context(), c, &resize); err != nil {
			return
		}
		readCtx, cancel := context.WithTimeout(r.Context(), 150*time.Millisecond)
		defer cancel()
		var next wire.ClientMsg
		err = wsjson.Read(readCtx, c, &next)
		receivedInput <- err == nil && next.Type == "stdin" && len(next.Data) > 0
		wsjson.Write(r.Context(), c, wire.ServerMsg{Type: "exit", ExitCode: 0})
	}))
	defer ts.Close()

	master, slave, err := pty.Open()
	if err != nil {
		t.Fatalf("pty.Open: %v", err)
	}
	defer master.Close()
	defer slave.Close()
	done := make(chan error, 1)
	go func() {
		_, err := runWithIO(context.Background(), "ws"+strings.TrimPrefix(ts.URL, "http")+"/attach", nil, 0, slave, io.Discard)
		done <- err
	}()
	<-handlerStarted
	if _, err := master.Write([]byte("typed-while-dialing\n")); err != nil {
		t.Fatalf("queue input during dial: %v", err)
	}
	close(allowUpgrade)
	if err := <-done; err != nil {
		t.Fatalf("runWithIO: %v", err)
	}
	if <-receivedInput {
		t.Fatal("server received input queued while the reconnect dial was pending")
	}
}

// cmd/sessiond/main_test.go
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"rainier/internal/eventlog"
	"rainier/internal/relay"
)

// TestNextBackoff pins dialLoop's backoff step: double, then clamp to the
// design's 30s cap (spec: "jittered exponential backoff 1s..30s cap"). This
// is the regression test for the review-round-1 finding that the old inline
// `if backoff < 30*time.Second { backoff *= 2 }` overshoots to 32s and
// freezes there (16s < 30s passes the guard, then doubles to 32s, and every
// later step also fails the < 30s guard, freezing forever above the stated
// cap) instead of clamping at 30s.
func TestNextBackoff(t *testing.T) {
	cases := []struct {
		in, want time.Duration
	}{
		{time.Second, 2 * time.Second},
		{2 * time.Second, 4 * time.Second},
		{4 * time.Second, 8 * time.Second},
		{8 * time.Second, 16 * time.Second},
		{16 * time.Second, 30 * time.Second}, // clamped: 16*2=32 > 30 cap
		{30 * time.Second, 30 * time.Second}, // already at cap: stays put
	}
	for _, c := range cases {
		if got := nextBackoff(c.in); got != c.want {
			t.Errorf("nextBackoff(%s) = %s, want %s", c.in, got, c.want)
		}
	}
}

// --- Plan 4: setup execution ---

// TestSetupWrapperArgv pins the exact child argv sessiond runs when its
// environment ships a setup script. The script text is spelled out here
// rather than derived from the production const on purpose: it is a contract
// between the driver's env injection and a shell inside a container nobody
// runs during unit tests, so changing it has to be a deliberate edit in two
// places, not a silently-agreeing refactor.
func TestSetupWrapperArgv(t *testing.T) {
	const wantScript = `sh /workspace/.rainier/setup.sh; rc=$?; echo $rc > /workspace/.rainier/setup.rc; [ "$rc" -eq 0 ] && exec "$@"; exit $rc`
	want := []string{"sh", "-c", wantScript, "wrapper", "claude", "--foo"}
	got := setupWrapperArgv(setupScriptPath, setupRCPath, []string{"claude", "--foo"})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("setupWrapperArgv =\n  %q\nwant\n  %q", got, want)
	}
	// $0 is "wrapper", which is what makes "$@" expand to exactly the real
	// argv and nothing else — drop it and the agent's own command would
	// become $0 and vanish from "$@".
	if got[3] != "wrapper" {
		t.Errorf("argv[3] = %q, want the \"wrapper\" $0 placeholder", got[3])
	}
}

// TestSetupWrapperAgainstRealSh executes the composed wrapper against the
// real /bin/sh, which is the only way to prove the two properties the
// contract actually rests on: `"$@"` hands quoting-hostile arguments to the
// agent byte for byte, and the rc file plus the wrapper's own exit status
// carry the setup script's exit code — including the non-zero case, where
// the agent must never start at all.
func TestSetupWrapperAgainstRealSh(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("no sh on PATH; skipping wrapper execution test")
	}
	// Arguments chosen to break naive quoting: an embedded space, a shell
	// variable reference, an apostrophe, and a glob.
	hostile := []string{"a b", "$HOME", "it's", "*"}
	childArgv := append([]string{"sh", "-c", `printf "<%s>" "$@"`, "child"}, hostile...)
	const wantChildOutput = `<a b><$HOME><it's><*>`

	cases := []struct {
		name     string
		setup    string
		wantRC   int
		childRan bool
	}{
		{"rc 0 execs the agent", "echo setup-ran\nexit 0\n", 0, true},
		{"rc 7 skips the agent", "echo setup-boom\nexit 7\n", 7, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			script := filepath.Join(dir, "setup.sh")
			rcPath := filepath.Join(dir, "setup.rc")
			if err := os.WriteFile(script, []byte(c.setup), 0o755); err != nil {
				t.Fatal(err)
			}

			argv := setupWrapperArgv(script, rcPath, childArgv)
			out, err := exec.Command(argv[0], argv[1:]...).CombinedOutput()
			rc := 0
			var ee *exec.ExitError
			if errors.As(err, &ee) {
				rc = ee.ExitCode()
			} else if err != nil {
				t.Fatalf("running the wrapper: %v (output %q)", err, out)
			}

			if rc != c.wantRC {
				t.Errorf("wrapper exit status = %d, want %d (output %q)", rc, c.wantRC, out)
			}
			b, err := os.ReadFile(rcPath)
			if err != nil {
				t.Fatalf("reading the rc file the wrapper must write: %v", err)
			}
			if got := strings.TrimSpace(string(b)); got != strconv.Itoa(c.wantRC) {
				t.Errorf("%s = %q, want %q", rcPath, got, strconv.Itoa(c.wantRC))
			}
			// The setup script's own output shares the child's stdout — in a
			// real session that is the PTY, which is what lets a viewer watch
			// setup run live.
			if !strings.Contains(string(out), "setup-") {
				t.Errorf("setup output missing from the wrapper's stdout: %q", out)
			}
			if ran := strings.Contains(string(out), wantChildOutput); ran != c.childRan {
				t.Errorf("agent ran = %v, want %v (output %q, wanted %q)", ran, c.childRan, out, wantChildOutput)
			}
		})
	}
}

// TestPrepareSetup covers the boot-time file work: decode, land the script
// 0755 under a created directory, and clear any rc file left by an earlier
// boot of the same (persistent) workspace volume — without that last step a
// cold-resumed session would report the PREVIOUS run's outcome the instant
// its watcher started, while this boot's setup was still running.
func TestPrepareSetup(t *testing.T) {
	t.Run("writes the script and clears a stale rc", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "nested", ".rainier")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		stale := filepath.Join(dir, "setup.rc")
		if err := os.WriteFile(stale, []byte("3\n"), 0o644); err != nil {
			t.Fatal(err)
		}

		const script = "echo hello\nexit 0\n"
		if err := prepareSetup(dir, base64.StdEncoding.EncodeToString([]byte(script))); err != nil {
			t.Fatal(err)
		}

		b, err := os.ReadFile(filepath.Join(dir, "setup.sh"))
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != script {
			t.Errorf("setup.sh = %q, want %q", b, script)
		}
		fi, err := os.Stat(filepath.Join(dir, "setup.sh"))
		if err != nil {
			t.Fatal(err)
		}
		if fi.Mode().Perm() != 0o755 {
			t.Errorf("setup.sh mode = %v, want 0755", fi.Mode().Perm())
		}
		if _, err := os.Stat(stale); !os.IsNotExist(err) {
			t.Errorf("stale setup.rc survived prepareSetup (err = %v)", err)
		}
	})

	t.Run("creates the directory", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "a", "b", ".rainier")
		if err := prepareSetup(dir, base64.StdEncoding.EncodeToString([]byte("true"))); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(dir, "setup.sh")); err != nil {
			t.Fatalf("script not written into a created directory: %v", err)
		}
	})

	t.Run("rejects undecodable base64", func(t *testing.T) {
		if err := prepareSetup(t.TempDir(), "not!base64!"); err == nil {
			t.Fatal("prepareSetup accepted a payload that is not base64")
		}
	})
}

// decodeControl unmarshals a watcher payload for assertions, failing the
// test on anything that is not the JSON object runnerd will have to parse.
func decodeControl(t *testing.T, payload []byte) relay.ControlEvent {
	t.Helper()
	var ev relay.ControlEvent
	if err := json.Unmarshal(payload, &ev); err != nil {
		t.Fatalf("watcher payload %q is not a control event: %v", payload, err)
	}
	return ev
}

// writeLog writes a session event log holding one "output" entry per chunk,
// in the on-disk JSONL shape eventlog.Log appends.
func writeLog(t *testing.T, path string, chunks ...string) {
	t.Helper()
	var b strings.Builder
	for i, c := range chunks {
		line, err := json.Marshal(eventlog.Entry{Seq: uint64(i + 1), Type: "output", Data: []byte(c)})
		if err != nil {
			t.Fatal(err)
		}
		b.Write(line)
		b.WriteByte('\n')
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestWatchSetup covers the three outcomes the watcher can reach, each one a
// control payload runnerd turns into an event.
func TestWatchSetup(t *testing.T) {
	const poll = 5 * time.Millisecond

	t.Run("rc 0 is setup_done", func(t *testing.T) {
		dir := t.TempDir()
		rc := filepath.Join(dir, "setup.rc")
		if err := os.WriteFile(rc, []byte("0\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		got := decodeControl(t, watchSetup(context.Background(), func() {}, rc, filepath.Join(dir, "s.log"), poll, time.Minute))
		if got.Kind != "setup_done" || got.RC != 0 || got.Tail != "" {
			t.Fatalf("outcome = %+v, want {Kind:setup_done}", got)
		}
	})

	t.Run("non-zero rc is setup_failed with the log tail", func(t *testing.T) {
		dir := t.TempDir()
		rc := filepath.Join(dir, "setup.rc")
		logPath := filepath.Join(dir, "s.log")
		writeLog(t, logPath, "installing...\n", "boom: no such package\n")
		// The rc file appears only after the watcher has already polled once.
		go func() {
			time.Sleep(20 * time.Millisecond)
			os.WriteFile(rc, []byte("7\n"), 0o644)
		}()
		got := decodeControl(t, watchSetup(context.Background(), func() {}, rc, logPath, poll, time.Minute))
		if got.Kind != "setup_failed" || got.RC != 7 {
			t.Fatalf("outcome = %+v, want {Kind:setup_failed RC:7}", got)
		}
		if !strings.Contains(got.Tail, "boom: no such package") {
			t.Errorf("tail = %q, want it to carry the session's output", got.Tail)
		}
	})

	t.Run("timeout stops the session and reports rc -1", func(t *testing.T) {
		dir := t.TempDir()
		var mu sync.Mutex
		stops := 0
		stop := func() { mu.Lock(); stops++; mu.Unlock() }
		const timeout = 60 * time.Millisecond
		got := decodeControl(t, watchSetup(context.Background(), stop, filepath.Join(dir, "setup.rc"), filepath.Join(dir, "s.log"), poll, timeout))
		if got.Kind != "setup_failed" || got.RC != -1 {
			t.Fatalf("outcome = %+v, want {Kind:setup_failed RC:-1}", got)
		}
		if got.Tail != setupTimedOutTail(timeout) {
			t.Errorf("tail = %q, want %q", got.Tail, setupTimedOutTail(timeout))
		}
		mu.Lock()
		defer mu.Unlock()
		if stops != 1 {
			t.Errorf("session stopped %d times on timeout, want exactly 1", stops)
		}
	})

	t.Run("a half-written rc file is not an outcome", func(t *testing.T) {
		// `echo $rc > file` truncates before it writes, so a poll can catch
		// the file existing and empty. Treating that as rc 0 would report
		// setup_done for a setup that had not finished.
		dir := t.TempDir()
		rc := filepath.Join(dir, "setup.rc")
		if err := os.WriteFile(rc, nil, 0o644); err != nil {
			t.Fatal(err)
		}
		go func() {
			time.Sleep(30 * time.Millisecond)
			os.WriteFile(rc, []byte("5\n"), 0o644)
		}()
		got := decodeControl(t, watchSetup(context.Background(), func() {}, rc, filepath.Join(dir, "s.log"), poll, time.Minute))
		if got.Kind != "setup_failed" || got.RC != 5 {
			t.Fatalf("outcome = %+v, want {Kind:setup_failed RC:5} — an empty rc file must not read as 0", got)
		}
	})

	t.Run("a cancelled context reports nothing", func(t *testing.T) {
		dir := t.TempDir()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if p := watchSetup(ctx, func() {}, filepath.Join(dir, "setup.rc"), filepath.Join(dir, "s.log"), poll, time.Minute); p != nil {
			t.Fatalf("payload = %q, want nil when the process is shutting down", p)
		}
	})
}

// TestSetupTimedOutTail pins the timeout message's shape, which controld
// renders into a session's error text verbatim.
func TestSetupTimedOutTail(t *testing.T) {
	if got, want := setupTimedOutTail(900*time.Second), "setup timed out after 900s"; got != want {
		t.Errorf("setupTimedOutTail(900s) = %q, want %q", got, want)
	}
}

// TestSetupTimeout pins how RAINIER_SETUP_TIMEOUT is read. controld owns the
// default (it sends 900 when an environment declares none), so sessiond's
// only job is to honor what arrived and treat anything non-positive or
// unreadable as "no timeout bound" rather than inventing a policy of its own.
func TestSetupTimeout(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
	}{
		{"900", 900 * time.Second},
		{"1", time.Second},
		{"", 0},
		{"0", 0},
		{"-5", 0},
		{"junk", 0},
	}
	for _, c := range cases {
		if got := setupTimeout(c.in); got != c.want {
			t.Errorf("setupTimeout(%q) = %s, want %s", c.in, got, c.want)
		}
	}
}

// TestLogTail pins the tail a setup failure carries: the session's plain
// terminal bytes, decoded out of the log's JSONL envelope, capped at the
// last n bytes.
func TestLogTail(t *testing.T) {
	dir := t.TempDir()

	t.Run("concatenates output entries and keeps the last n bytes", func(t *testing.T) {
		p := filepath.Join(dir, "a.log")
		writeLog(t, p, "0123456789", "abcdefghij")
		if got, want := logTail(p, 8), "cdefghij"; got != want {
			t.Errorf("logTail = %q, want %q", got, want)
		}
		if got, want := logTail(p, 1000), "0123456789abcdefghij"; got != want {
			t.Errorf("logTail = %q, want %q", got, want)
		}
	})

	t.Run("skips entries that are not output and lines that are not entries", func(t *testing.T) {
		p := filepath.Join(dir, "b.log")
		line, err := json.Marshal(eventlog.Entry{Seq: 1, Type: "input", Data: []byte("secret")})
		if err != nil {
			t.Fatal(err)
		}
		out, err := json.Marshal(eventlog.Entry{Seq: 2, Type: "output", Data: []byte("kept")})
		if err != nil {
			t.Fatal(err)
		}
		body := string(line) + "\n{not json\n" + string(out) + "\n{half-written"
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if got, want := logTail(p, 1000), "kept"; got != want {
			t.Errorf("logTail = %q, want %q", got, want)
		}
	})

	t.Run("a missing log is an empty tail, not a failure", func(t *testing.T) {
		if got := logTail(filepath.Join(dir, "nope.log"), 100); got != "" {
			t.Errorf("logTail of a missing file = %q, want \"\"", got)
		}
	})
}

// stubSender records what a connection's control channel was asked to send,
// and can fail on demand — standing in for relay.ControlSender, whose only
// method serveConn uses is Send.
type stubSender struct {
	mu       sync.Mutex
	sent     [][]byte
	attempts int
	err      error
}

func (s *stubSender) Send(p []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts++
	if s.err != nil {
		return s.err
	}
	s.sent = append(s.sent, append([]byte(nil), p...))
	return nil
}

func (s *stubSender) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sent)
}

func (s *stubSender) tries() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts
}

// waitFor polls f until it reports true, or fails the test — the tests below
// hand serveConn one thing at a time, so each step has to be observed before
// the next is set up.
func waitFor(t *testing.T, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// sentStrings renders what a stubSender received, for readable assertions on
// a queue's contents and order.
func (s *stubSender) sentStrings() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.sent))
	for i, p := range s.sent {
		out[i] = string(p)
	}
	return out
}

// pendingStrings renders a returned queue the same way.
func pendingStrings(q [][]byte) []string {
	out := make([]string, len(q))
	for i, p := range q {
		out[i] = string(p)
	}
	return out
}

// TestServeConnDeliversControlEvents covers the delivery half of the
// contract: an event is computed once by whichever watcher produced it, but a
// connection can die before or during its delivery, so it stays queued until
// some connection actually accepts it — and is never sent twice.
func TestServeConnDeliversControlEvents(t *testing.T) {
	payload := []byte(`{"kind":"setup_done"}`)

	t.Run("an event arriving mid-connection is delivered", func(t *testing.T) {
		events := make(chan []byte, pendingCap)
		errc := make(chan error, 1)
		snd := &stubSender{}
		done := make(chan struct{})
		go func() {
			pending, err := serveConn(snd, errc, events, nil)
			if len(pending) != 0 {
				t.Errorf("after delivery: pending=%q, want empty", pendingStrings(pending))
			}
			if err == nil {
				t.Error("serveConn returned a nil relay error")
			}
			close(done)
		}()
		events <- payload
		// Only once the send has landed does the connection end.
		waitFor(t, "the event to be sent", func() bool { return snd.count() == 1 })
		errc <- errors.New("conn closed")
		<-done
		if got := snd.sentStrings(); len(got) != 1 || got[0] != string(payload) {
			t.Fatalf("sent = %q, want exactly one %q", got, payload)
		}
	})

	t.Run("a failed send keeps the event queued for the next connection", func(t *testing.T) {
		events := make(chan []byte, pendingCap)
		errc := make(chan error, 1)
		dead := &stubSender{err: errors.New("write: broken pipe")}

		res := make(chan [][]byte, 1)
		go func() {
			pending, _ := serveConn(dead, errc, events, nil)
			res <- pending
		}()
		// Hand over the event first and wait for the send to be attempted
		// and fail: with both channels ready at once the select would be
		// free to end the connection before the event was ever taken (a
		// legal outcome — it stays in the channel — but not the one under
		// test here).
		events <- payload
		waitFor(t, "the failing send to be attempted", func() bool { return dead.tries() == 1 })
		errc <- errors.New("conn closed")
		first := <-res
		if got := pendingStrings(first); len(got) != 1 || got[0] != string(payload) {
			t.Fatalf("pending = %q, want the undelivered %q", got, payload)
		}

		// The redial: same queue, a live conn this time.
		errc2 := make(chan error, 1)
		errc2 <- errors.New("conn closed later")
		live := &stubSender{}
		pending, _ := serveConn(live, errc2, events, first)
		if len(pending) != 0 {
			t.Fatalf("after the retry: pending=%q, want empty", pendingStrings(pending))
		}
		if got := live.sentStrings(); len(got) != 1 || got[0] != string(payload) {
			t.Fatalf("retry sent %q, want exactly one %q", got, payload)
		}
	})

	t.Run("an event is never lost when a connection dies under it", func(t *testing.T) {
		// Both channels ready at once, which is exactly what a conn dying the
		// instant setup finished looks like: the select may take either
		// first, so the event comes back either as a queued payload (it was
		// taken, and the send failed) or still sitting in the channel (the
		// conn ended before it was taken). The invariant is that it survives
		// whichever way that lands — the next connection delivers it, once.
		events := make(chan []byte, pendingCap)
		events <- payload
		errc := make(chan error, 1)
		errc <- errors.New("conn closed")
		pending, _ := serveConn(&stubSender{err: errors.New("write: broken pipe")}, errc, events, nil)

		errc2 := make(chan error, 1)
		live := &stubSender{}
		done := make(chan struct{})
		go func() {
			serveConn(live, errc2, events, pending)
			close(done)
		}()
		waitFor(t, "the retained event to be delivered", func() bool { return live.count() == 1 })
		errc2 <- errors.New("conn closed later")
		<-done
		if got := live.sentStrings(); got[0] != string(payload) {
			t.Fatalf("delivered %q, want %q", got[0], payload)
		}
	})

	t.Run("no events means nothing is ever sent", func(t *testing.T) {
		errc := make(chan error, 1)
		errc <- errors.New("conn closed")
		snd := &stubSender{}
		pending, err := serveConn(snd, errc, nil, nil)
		if len(pending) != 0 || err == nil {
			t.Fatalf("pending=%q err=%v, want empty/non-nil", pendingStrings(pending), err)
		}
		if snd.count() != 0 {
			t.Fatalf("sent %d payloads with no events configured, want 0", snd.count())
		}
	})
}

// TestServeConnQueuesEventsFIFO is why the single pending payload became a
// queue: a session now has two things to report on this channel — its setup's
// verdict and its agent's exit — and both can be waiting at once. That is not
// a corner case: a FAILING setup produces both, since the wrapper writes its
// exit code and then exits with it. One slot would have silently dropped
// whichever arrived first.
//
// Delivery order is the order of arrival, so what reaches controld is the
// order things actually happened in rather than one that falls out of
// scheduling — and so that "drop the oldest" has a well-defined meaning.
func TestServeConnQueuesEventsFIFO(t *testing.T) {
	setup := []byte(`{"kind":"setup_done"}`)
	exited := []byte(`{"kind":"child_exited","rc":0}`)

	events := make(chan []byte, pendingCap)
	errc := make(chan error, 1)
	dead := &stubSender{err: errors.New("write: broken pipe")}

	res := make(chan [][]byte, 1)
	go func() {
		pending, _ := serveConn(dead, errc, events, nil)
		res <- pending
	}()
	events <- setup
	waitFor(t, "the first failing send", func() bool { return dead.tries() == 1 })
	events <- exited
	waitFor(t, "the second failing send", func() bool { return dead.tries() >= 2 })
	errc <- errors.New("conn closed")

	queued := <-res
	want := []string{string(setup), string(exited)}
	if got := pendingStrings(queued); !reflect.DeepEqual(got, want) {
		t.Fatalf("queued = %q, want both in arrival order %q", got, want)
	}

	// The redial drains the whole queue, in that order, on one connection.
	errc2 := make(chan error, 1)
	errc2 <- errors.New("conn closed later")
	live := &stubSender{}
	pending, _ := serveConn(live, errc2, events, queued)
	if len(pending) != 0 {
		t.Fatalf("after the drain: pending=%q, want empty", pendingStrings(pending))
	}
	if got := live.sentStrings(); !reflect.DeepEqual(got, want) {
		t.Fatalf("delivered %q, want %q", got, want)
	}
}

// TestPendingQueueDropsTheOldest bounds the queue. Nothing in v0's vocabulary
// produces more than two events, so overflowing this is a bug somewhere else
// — but an UNBOUNDED queue turns that bug into a sessiond whose memory grows
// for as long as its runnerd stays away, which is the one failure mode a
// session (the thing this whole system promises outlives everything) must not
// have. Dropping the oldest keeps the most recent news, which is the half a
// late-arriving consumer can still act on.
func TestPendingQueueDropsTheOldest(t *testing.T) {
	var q [][]byte
	for i := range pendingCap + 3 {
		q = appendPending(q, fmt.Appendf(nil, "e%d", i))
	}
	if len(q) != pendingCap {
		t.Fatalf("queue length = %d, want the %d cap", len(q), pendingCap)
	}
	want := make([]string, 0, pendingCap)
	for i := 3; i < pendingCap+3; i++ {
		want = append(want, fmt.Sprintf("e%d", i))
	}
	if got := pendingStrings(q); !reflect.DeepEqual(got, want) {
		t.Fatalf("queue = %q, want the newest %d %q", got, pendingCap, want)
	}
}

// TestChildExitedPayload pins the event sessiond sends when the agent process
// ends. Exit 0 is an ANSWER, not an absence — a session whose agent finished
// cleanly has to be distinguishable from one still running — and
// relay.ControlEvent.RC is `omitempty`, so a clean exit puts NO rc on the wire
// at all. That is safe only because the field's zero value and the value it is
// carrying are the same number; the assertion that matters is therefore the
// round trip, not the bytes, and it is pinned for 0 first.
func TestChildExitedPayload(t *testing.T) {
	for _, code := range []int{0, 1, 137, -1} {
		p := childExitedPayload(code)
		var ev relay.ControlEvent
		if err := json.Unmarshal(p, &ev); err != nil {
			t.Fatalf("child_exited payload for %d does not decode: %v (%s)", code, err, p)
		}
		if ev.Kind != "child_exited" {
			t.Fatalf("kind = %q, want child_exited", ev.Kind)
		}
		if ev.RC != code {
			t.Fatalf("rc = %d, want %d (payload %s)", ev.RC, code, p)
		}
		if ev.ID != 0 {
			t.Fatalf("id = %d, want 0 — child_exited is an event, not a request", ev.ID)
		}
	}
}

// TestOfferControlNeverBlocks: the watchers that produce these events run on
// their own goroutines with no connection to wait for, and a blocking send
// would park one forever the moment the queue backed up. The channel is the
// buffer; a full one drops rather than wedges.
func TestOfferControlNeverBlocks(t *testing.T) {
	out := make(chan []byte, pendingCap)
	done := make(chan struct{})
	go func() {
		for range pendingCap + 5 {
			offerControl(out, []byte(`{"kind":"setup_done"}`))
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("offerControl blocked on a full queue")
	}
	if len(out) != pendingCap {
		t.Fatalf("queue holds %d, want the %d cap", len(out), pendingCap)
	}
	// A nil payload is what controlPayload returns when encoding failed; it
	// must not become an empty control frame on the wire.
	offerControl(out, nil)
	if len(out) != pendingCap {
		t.Fatalf("a nil payload changed the queue: %d", len(out))
	}
}

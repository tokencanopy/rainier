// internal/session/session_test.go
package session

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/internal/wire"
)

// fakeProc lets tests drive output and observe stdin/resize without a real PTY.
type fakeProc struct {
	onOutput func([]byte)
	stdin    chan []byte
	resizes  chan Size
	exit     chan int
}

func (f *fakeProc) Write(p []byte) (int, error) { f.stdin <- append([]byte(nil), p...); return len(p), nil }
func (f *fakeProc) Resize(c, r int) error       { f.resizes <- Size{c, r}; return nil }
func (f *fakeProc) Wait() int                   { return <-f.exit }
func (f *fakeProc) Stop()                       { close(f.exit) }

func newFakeSession(t *testing.T) (*Session, *fakeProc) {
	t.Helper()
	fp := &fakeProc{stdin: make(chan []byte, 8), resizes: make(chan Size, 8), exit: make(chan int)}
	s, err := New(Config{Argv: []string{"fake"}, Cols: 20, Rows: 5, LogPath: filepath.Join(t.TempDir(), "s.log")},
		func(argv []string, cols, rows int, onOutput func([]byte)) (Proc, error) {
			fp.onOutput = onOutput
			return fp, nil
		})
	if err != nil { t.Fatal(err) }
	return s, fp
}

func recv(t *testing.T, ch <-chan wire.ServerMsg) wire.ServerMsg {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for msg")
		return wire.ServerMsg{}
	}
}

// drainN reads exactly n messages off ch, each guarded by recv's timeout, so
// a stuck sender (e.g. a deadlocked Attach) fails the test instead of
// hanging the suite.
func drainN(t *testing.T, ch <-chan wire.ServerMsg, n int) []wire.ServerMsg {
	t.Helper()
	out := make([]wire.ServerMsg, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, recv(t, ch))
	}
	return out
}

func TestFreshAttachGetsSnapshotThenLive(t *testing.T) {
	s, fp := newFakeSession(t)
	fp.onOutput([]byte("hello"))
	a, err := s.Attach(0, Size{20, 5})
	if err != nil { t.Fatal(err) }
	m1 := recv(t, a.Msgs)
	if m1.Type != "snapshot" || m1.Seq == 0 { t.Fatalf("first = %+v", m1) }
	fp.onOutput([]byte(" world"))
	m2 := recv(t, a.Msgs)
	if m2.Type != "output" || string(m2.Data) != " world" || m2.Seq != m1.Seq+1 {
		t.Fatalf("live = %+v", m2)
	}
}

func TestResumeReplaysOnlyMissedFrames(t *testing.T) {
	s, fp := newFakeSession(t)
	fp.onOutput([]byte("one"))
	fp.onOutput([]byte("two"))
	a, _ := s.Attach(0, Size{20, 5})
	snap := recv(t, a.Msgs) // snapshot at seq 2
	s.Detach(a.ID)
	fp.onOutput([]byte("three"))
	b, _ := s.Attach(snap.Seq, Size{20, 5})
	m := recv(t, b.Msgs)
	if m.Type != "output" || string(m.Data) != "three" { t.Fatalf("resume = %+v", m) }
}

// TestSinceAllReplaysTheWholeLog is the second half of the `--since 0`
// regression (Plan 5 T10): the whole-log cursor must replay the event log
// from its FIRST entry, not paint a snapshot. `since > 0` alone could never
// express that — 0 is "no cursor, paint me a screen" and always has been
// (TestFreshAttachGetsSnapshotThenLive pins it), so `attach --since 0`, the
// runbook's way to read a failed session's full setup output, silently got a
// 24-row screen instead of the log the server was still holding.
//
// 50 entries, matching the plan's scene: every one of them arrives, in
// order, starting at seq 1 — no snapshot frame anywhere.
func TestSinceAllReplaysTheWholeLog(t *testing.T) {
	s, fp := newFakeSession(t)
	const n = 50
	for i := 0; i < n; i++ {
		fp.onOutput([]byte{byte('a' + i%26)})
	}

	a, err := s.Attach(wire.SinceAll, Size{20, 5})
	if err != nil {
		t.Fatal(err)
	}
	// Frame by frame rather than drainN, so the failure that matters — a
	// snapshot where the log's first entry should be — is reported as
	// itself instead of as a timeout waiting for 50 frames that never come.
	for i := 0; i < n; i++ {
		m := recv(t, a.Msgs)
		wantSeq := uint64(i + 1)
		wantByte := byte('a' + i%26)
		if m.Type != "output" || m.Seq != wantSeq || len(m.Data) != 1 || m.Data[0] != wantByte {
			t.Fatalf("frame %d = %+v, want an output frame seq=%d byte=%q", i, m, wantSeq, wantByte)
		}
	}

	// And live output still follows the replay, on the same channel.
	fp.onOutput([]byte("!"))
	m := recv(t, a.Msgs)
	if m.Type != "output" || string(m.Data) != "!" || m.Seq != n+1 {
		t.Fatalf("live frame after replay = %+v, want output %q at seq %d", m, "!", n+1)
	}
}

// TestSinceAllOnAnEmptyLogFallsBackToSnapshot: asking for the whole log of a
// session that has not produced a byte yet must still leave the viewer with
// a screen (and its size), exactly like a cursor past the end does. A replay
// of nothing would open with silence instead.
func TestSinceAllOnAnEmptyLogFallsBackToSnapshot(t *testing.T) {
	s, _ := newFakeSession(t)
	a, err := s.Attach(wire.SinceAll, Size{20, 5})
	if err != nil {
		t.Fatal(err)
	}
	if m := recv(t, a.Msgs); m.Type != "snapshot" {
		t.Fatalf("first frame = %+v, want a snapshot for a whole-log attach with an empty log", m)
	}
}

func TestStdinForwarded(t *testing.T) {
	s, fp := newFakeSession(t)
	s.Stdin([]byte("x"))
	select {
	case got := <-fp.stdin:
		if string(got) != "x" { t.Fatalf("stdin = %q", got) }
	case <-time.After(time.Second):
		t.Fatal("stdin not forwarded")
	}
}

func TestSmallestViewerResizesProc(t *testing.T) {
	s, fp := newFakeSession(t)
	a, _ := s.Attach(0, Size{120, 40})
	recv(t, a.Msgs)
	<-fp.resizes // 120x40 from first attach
	b, _ := s.Attach(0, Size{80, 50})
	recv(t, b.Msgs)
	got := <-fp.resizes
	if got != (Size{80, 40}) { t.Fatalf("resize = %+v, want {80 40}", got) }
	_ = b
}

// Fix round 1 #1: a replay backlog longer than the channel's steady-state
// 256 buffer must not deadlock Attach (which stages the whole replay while
// holding s.mu, before any caller can drain the channel).
func TestReplayLargerThanBufferDoesNotDeadlock(t *testing.T) {
	s, fp := newFakeSession(t)
	const n = 400
	for i := 0; i < n; i++ {
		fp.onOutput([]byte{byte('a' + i%26)})
	}
	a, err := s.Attach(1, Size{20, 5})
	if err != nil { t.Fatal(err) }
	msgs := drainN(t, a.Msgs, n-1) // since=1: entries with seq 2..n replay (n-1 frames)
	for i, m := range msgs {
		wantSeq := uint64(i + 2)
		wantByte := byte('a' + int(wantSeq-1)%26)
		if m.Type != "output" || m.Seq != wantSeq || len(m.Data) != 1 || m.Data[0] != wantByte {
			t.Fatalf("frame %d: = %+v, want seq=%d byte=%q", i, m, wantSeq, wantByte)
		}
	}
}

// Fix round 1 #2a: exit must close a viewer's channel after the exit
// message, not just send it.
func TestExitSendsExitThenClosesViewerChannel(t *testing.T) {
	s, fp := newFakeSession(t)
	a, err := s.Attach(0, Size{20, 5})
	if err != nil { t.Fatal(err) }
	recv(t, a.Msgs) // snapshot
	fp.Stop()
	m := recv(t, a.Msgs)
	if m.Type != "exit" || m.ExitCode != 0 { t.Fatalf("exit msg = %+v", m) }
	select {
	case _, ok := <-a.Msgs:
		if ok { t.Fatalf("channel not closed after exit") }
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for channel close")
	}
}

// Fix round 1 #2b: a viewer that attaches after the child has already
// exited must still get a snapshot, then the exit message, then closure —
// not silence.
func TestAttachAfterExitGetsSnapshotThenExit(t *testing.T) {
	s, fp := newFakeSession(t)
	fp.Stop()
	<-s.Exited() // deterministic: exit goroutine has fully run
	a, err := s.Attach(0, Size{20, 5})
	if err != nil { t.Fatal(err) }
	m1 := recv(t, a.Msgs)
	if m1.Type != "snapshot" { t.Fatalf("first = %+v", m1) }
	m2 := recv(t, a.Msgs)
	if m2.Type != "exit" || m2.ExitCode != 0 { t.Fatalf("exit = %+v", m2) }
	select {
	case _, ok := <-a.Msgs:
		if ok { t.Fatalf("channel not closed after exit") }
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for channel close")
	}
}

// Fix round 2: the exit goroutine used to clear s.viewers, unlock s.mu, and
// only then close(s.exited) — leaving a window where a concurrent Attach
// could lock s.mu, see the cleared viewers map, but still observe s.exited
// as not-yet-closed (default branch), registering a viewer that would never
// be exit-notified or closed. This hammers that window across many attempts
// and asserts the invariant that must always hold: every attachment
// obtained around an exit eventually has its Msgs channel closed.
func TestConcurrentAttachDuringExitNeverStrands(t *testing.T) {
	for i := 0; i < 50; i++ {
		s, fp := newFakeSession(t)
		done := make(chan *Attachment, 8)
		go func() {
			for j := 0; j < 4; j++ {
				if a, err := s.Attach(0, Size{20, 5}); err == nil { done <- a }
			}
			close(done)
		}()
		fp.exit <- 0 // trigger exit concurrently
		for a := range done {
			deadline := time.After(2 * time.Second)
			for open := true; open; {
				select {
				case _, ok := <-a.Msgs:
					open = ok // drain until closed
				case <-deadline:
					t.Fatal("attachment channel never closed after exit")
				}
			}
		}
	}
}

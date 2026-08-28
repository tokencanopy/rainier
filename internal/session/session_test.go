// internal/session/session_test.go
package session

import (
	"path/filepath"
	"testing"
	"time"

	"rainier/internal/wire"
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

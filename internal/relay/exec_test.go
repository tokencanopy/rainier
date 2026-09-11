package relay

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/internal/eventlog"
	"github.com/tokencanopy/rainier/internal/session"
	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// ---------------------------------------------------------------------------
// fakes
// ---------------------------------------------------------------------------

// silentProc is a session process that records everything written to it and
// produces nothing. It is what makes "an exec never reaches the pty" a
// checkable claim rather than a hope.
type silentProc struct {
	mu      sync.Mutex
	written []byte
	sizes   int
	done    chan struct{}
}

func newSilentProc() *silentProc { return &silentProc{done: make(chan struct{})} }

func (p *silentProc) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.written = append(p.written, b...)
	return len(b), nil
}

func (p *silentProc) Resize(cols, rows int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sizes++
	return nil
}

func (p *silentProc) Wait() int { <-p.done; return 0 }
func (p *silentProc) Stop()     { close(p.done) }

func (p *silentProc) bytes() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]byte(nil), p.written...)
}

// fakeExecer is the relay's exec seam, scripted. It records the specs it was
// handed and what each attachment was told.
type fakeExecer struct {
	mu    sync.Mutex
	specs []runner.ExecSpec
	atts  []*fakeExecAttachment
	open  chan *fakeExecAttachment
}

func newFakeExecer() *fakeExecer {
	return &fakeExecer{open: make(chan *fakeExecAttachment, 8)}
}

func (f *fakeExecer) OpenExec(spec runner.ExecSpec) ExecAttachment {
	a := &fakeExecAttachment{msgs: make(chan terminal.ServerMessage, 16),
		closed: make(chan struct{})}
	f.mu.Lock()
	f.specs = append(f.specs, spec)
	f.atts = append(f.atts, a)
	f.mu.Unlock()
	f.open <- a
	return a
}

func (f *fakeExecer) lastSpec(t *testing.T) runner.ExecSpec {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.specs) == 0 {
		t.Fatal("no exec was ever opened")
	}
	return f.specs[len(f.specs)-1]
}

type fakeExecAttachment struct {
	msgs chan terminal.ServerMessage

	mu       sync.Mutex
	client   []terminal.ClientMessage
	closed   chan struct{}
	closeOne sync.Once
}

func (a *fakeExecAttachment) Msgs() <-chan terminal.ServerMessage { return a.msgs }

func (a *fakeExecAttachment) Client(m terminal.ClientMessage) {
	a.mu.Lock()
	a.client = append(a.client, m)
	a.mu.Unlock()
}

func (a *fakeExecAttachment) Close() { a.closeOne.Do(func() { close(a.closed) }) }

func (a *fakeExecAttachment) received() []terminal.ClientMessage {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]terminal.ClientMessage(nil), a.client...)
}

// execTestRig stands the whole sandbox-side hop up: a real session over a
// silent process, a relay serving it, a hub on the other end, and a fake
// exec runner.
type execTestRig struct {
	sess   *session.Session
	proc   *silentProc
	execer *fakeExecer
	hub    *Hub
	log    string
}

func newExecTestRig(t *testing.T) *execTestRig {
	t.Helper()
	proc := newSilentProc()
	logPath := filepath.Join(t.TempDir(), "s.log")
	s, err := session.New(
		session.Config{Argv: []string{"agent"}, Cols: 80, Rows: 24, LogPath: logPath},
		func(argv []string, cols, rows int, onOutput func([]byte)) (session.Proc, error) {
			return proc, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	sessConn, runConn := newPipe()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	ex := newFakeExecer()
	ServeSessionWithExec(ctx, sessConn, s, nil, ex)
	hub := NewHub(ctx, runConn)
	t.Cleanup(hub.Close)
	return &execTestRig{sess: s, proc: proc, execer: ex, hub: hub, log: logPath}
}

func (r *execTestRig) awaitExec(t *testing.T) *fakeExecAttachment {
	t.Helper()
	select {
	case a := <-r.execer.open:
		return a
	case <-time.After(5 * time.Second):
		t.Fatal("no exec was opened")
		return nil
	}
}

// ---------------------------------------------------------------------------
// routing
// ---------------------------------------------------------------------------

// TestFrameOpenRoutesByKind is the table the design asks for: an empty kind
// reaches session.Attach exactly as today, and "exec" reaches the exec runner
// and NOTHING else.
func TestFrameOpenRoutesByKind(t *testing.T) {
	rig := newExecTestRig(t)
	ctx := context.Background()

	// A terminal attachment: the session answers with a snapshot, and the
	// exec runner is never asked anything.
	termClient, hubTerm := newPipe()
	go rig.hub.AttachClient(ctx, hubTerm, Open{Cols: 80, Rows: 24})
	if m := readServerMsg(t, termClient); m.Type != "snapshot" {
		t.Fatalf("a terminal attachment opened with %q, want a snapshot", m.Type)
	}
	rig.execer.mu.Lock()
	opened := len(rig.execer.specs)
	rig.execer.mu.Unlock()
	if opened != 0 {
		t.Fatalf("a terminal attachment reached the exec runner %d times", opened)
	}

	// An exec attachment: the runner is asked, with the spec verbatim.
	execClient, hubExec := newPipe()
	spec := runner.ExecSpec{Argv: []string{"git", "status"}, Cwd: "/workspace/repo", TTY: true}
	go rig.hub.AttachClient(ctx, hubExec, Open{Cols: 100, Rows: 40, Kind: runner.KindExec, Exec: &spec})
	att := rig.awaitExec(t)

	got := rig.execer.lastSpec(t)
	if len(got.Argv) != 2 || got.Argv[1] != "status" || got.Cwd != "/workspace/repo" || !got.TTY {
		t.Fatalf("the spec arrived as %+v", got)
	}

	// And what the exec says reaches its own client, and only its own.
	att.msgs <- terminal.ServerMessage{Type: terminal.TypeExecStarted}
	if m := readServerMsg(t, execClient); m.Type != terminal.TypeExecStarted {
		t.Fatalf("the exec client got %q", m.Type)
	}
	close(att.msgs)
}

// TestAnExecNeverReachesTheTerminal is the third row of the model table, and
// the whole reason exec is a second KIND rather than a flag: an exec's bytes
// must never reach the pty, the emulator, the event log, or any viewer's
// screen.
func TestAnExecNeverReachesTheTerminal(t *testing.T) {
	rig := newExecTestRig(t)
	ctx := context.Background()

	// A viewer is watching, so "no other viewer sees it" is a claim with a
	// witness rather than a vacuous truth.
	viewer, hubViewer := newPipe()
	go rig.hub.AttachClient(ctx, hubViewer, Open{Cols: 80, Rows: 24})
	if m := readServerMsg(t, viewer); m.Type != "snapshot" {
		t.Fatalf("the viewer opened with %q", m.Type)
	}

	spec := runner.ExecSpec{Argv: []string{"cat", "huge.log"}}
	execClient, hubExec := newPipe()
	go rig.hub.AttachClient(ctx, hubExec, Open{Kind: runner.KindExec, Exec: &spec})
	att := rig.awaitExec(t)

	att.msgs <- terminal.ServerMessage{Type: terminal.TypeExecStarted}
	readServerMsg(t, execClient)
	att.msgs <- terminal.ServerMessage{Type: terminal.TypeExecStdout, Data: []byte("a megabyte of log")}
	if m := readServerMsg(t, execClient); string(m.Data) != "a megabyte of log" {
		t.Fatalf("the exec's own client got %+v", m)
	}

	// The caller types into the exec; none of it reaches the pty.
	writeClientMsg(t, execClient, terminal.ClientMessage{Type: "stdin", Data: []byte("typed into the exec")})
	writeClientMsg(t, execClient, terminal.ClientMessage{Type: "resize", Cols: 999, Rows: 999})
	deadline := time.Now().Add(2 * time.Second)
	for len(att.received()) < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := att.received(); len(got) != 2 {
		t.Fatalf("the exec attachment received %v", got)
	}

	close(att.msgs)

	// Nothing of the exec touched the session: no byte at the pty, no entry
	// in the event log, and nothing queued for the viewer beyond its opening
	// snapshot.
	if b := rig.proc.bytes(); len(b) != 0 {
		t.Fatalf("an exec wrote %q into the agent's pty", b)
	}
	lg, err := eventlog.Open(rig.log)
	if err != nil {
		t.Fatal(err)
	}
	defer lg.Close()
	if lg.LastSeq() != 0 {
		t.Fatalf("an exec put %d entries in the session's event log", lg.LastSeq())
	}
	select {
	case raw := <-viewer.in:
		var m terminal.ServerMessage
		_ = json.Unmarshal(raw, &m)
		t.Fatalf("a viewer was sent %+v because of an exec", m)
	default:
	}
}

// TestAnExecIsNeverStampedOrFenced: an exec attachment holds no binding, so a
// generation on one of its frames is neither read here nor acted on, and the
// session's own fence never moves because of it.
func TestAnExecIsNeverStampedOrFenced(t *testing.T) {
	rig := newExecTestRig(t)
	ctx := context.Background()

	// A controller attachment, so the session HAS a fence to move.
	ctrl, hubCtrl := newPipe()
	go rig.hub.AttachClient(ctx, hubCtrl, Open{Cols: 80, Rows: 24,
		Mode: terminal.ModeControl, Generation: 4})
	readServerMsg(t, ctrl)
	writeClientMsg(t, ctrl, terminal.ClientMessage{
		Type: "stdin", Data: []byte("ls\n"), Generation: terminal.GenOf(4)})
	deadline := time.Now().Add(2 * time.Second)
	for len(rig.proc.bytes()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if string(rig.proc.bytes()) != "ls\n" {
		t.Fatalf("the controller's keystroke did not reach the pty: %q", rig.proc.bytes())
	}

	// An exec arrives carrying a generation far past the session's. If
	// anything read it, the session's fence would move and the controller
	// above would be silently fenced out of its own terminal.
	spec := runner.ExecSpec{Argv: []string{"true"}}
	execClient, hubExec := newPipe()
	go rig.hub.AttachClient(ctx, hubExec, Open{Kind: runner.KindExec, Exec: &spec,
		Mode: terminal.ModeControl, Generation: 999})
	att := rig.awaitExec(t)
	att.msgs <- terminal.ServerMessage{Type: terminal.TypeExecStarted}
	readServerMsg(t, execClient)
	writeClientMsg(t, execClient, terminal.ClientMessage{
		Type: "stdin", Data: []byte("x"), Generation: terminal.GenOf(999)})
	close(att.msgs)

	// The controller can still type, which is the whole assertion.
	writeClientMsg(t, ctrl, terminal.ClientMessage{
		Type: "stdin", Data: []byte("pwd\n"), Generation: terminal.GenOf(4)})
	deadline = time.Now().Add(2 * time.Second)
	for string(rig.proc.bytes()) != "ls\npwd\n" && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := string(rig.proc.bytes()); got != "ls\npwd\n" {
		t.Fatalf("the pty holds %q; an exec moved the session's controller fence", got)
	}
}

// TestAnExecFrameOnASandboxWithNoExecRunnerIsClosed is the permanent case in
// the compatibility matrix, at this hop: a sessiond with no exec runner
// answers an exec open by closing the attachment. The caller never receives an
// exec_started, which is exactly what the plane refuses on.
func TestAnExecFrameOnASandboxWithNoExecRunnerIsClosed(t *testing.T) {
	proc := newSilentProc()
	s, err := session.New(
		session.Config{Argv: []string{"agent"}, Cols: 80, Rows: 24,
			LogPath: filepath.Join(t.TempDir(), "s.log")},
		func(argv []string, cols, rows int, onOutput func([]byte)) (session.Proc, error) {
			return proc, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	sessConn, runConn := newPipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// No Execer at all — the shape a build that predates exec has.
	ServeSessionWithExec(ctx, sessConn, s, nil, nil)
	hub := NewHub(ctx, runConn)
	defer hub.Close()

	spec := runner.ExecSpec{Argv: []string{"true"}}
	client, hubClient := newPipe()
	go hub.AttachClient(ctx, hubClient, Open{Kind: runner.KindExec, Exec: &spec})

	select {
	case <-client.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("an exec against a sandbox with no exec runner was not closed")
	}
	if b := proc.bytes(); len(b) != 0 {
		t.Fatalf("the refused exec still reached the pty: %q", b)
	}
}

// TestAConnDeathClosesEveryExec: an exec whose conn died has lost the one
// consumer its output was ever for, so it is closed — which is what kills its
// process group.
func TestAConnDeathClosesEveryExec(t *testing.T) {
	rig := newExecTestRig(t)
	ctx := context.Background()

	spec := runner.ExecSpec{Argv: []string{"sleep", "1000"}}
	_, hubExec := newPipe()
	go rig.hub.AttachClient(ctx, hubExec, Open{Kind: runner.KindExec, Exec: &spec})
	att := rig.awaitExec(t)

	rig.hub.Close()
	select {
	case <-att.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("the conn died and the exec was never closed")
	}
}

// TestAClientCloseClosesItsExec is the caller-disconnect rule at this hop.
func TestAClientCloseClosesItsExec(t *testing.T) {
	rig := newExecTestRig(t)
	ctx := context.Background()

	spec := runner.ExecSpec{Argv: []string{"sleep", "1000"}}
	client, hubExec := newPipe()
	go rig.hub.AttachClient(ctx, hubExec, Open{Kind: runner.KindExec, Exec: &spec})
	att := rig.awaitExec(t)
	att.msgs <- terminal.ServerMessage{Type: terminal.TypeExecStarted}
	readServerMsg(t, client)

	client.Close()
	select {
	case <-att.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("the caller hung up and the exec was never closed")
	}
}

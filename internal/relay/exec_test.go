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

// ---------------------------------------------------------------------------
// the shared writer
// ---------------------------------------------------------------------------

// wedgingConn blocks its FIRST write until released and passes every one
// after it, which is the shape of a conn backed up behind one slow consumer:
// the frame in flight cannot move, and everything queued behind it is fine the
// instant it does. Reads come off a channel the test feeds.
type wedgingConn struct {
	entered chan struct{}
	release chan struct{}
	in      chan []byte
	once    sync.Once

	mu      sync.Mutex
	written [][]byte
}

func newWedgingConn() *wedgingConn {
	return &wedgingConn{entered: make(chan struct{}), release: make(chan struct{}),
		in: make(chan []byte, 8)}
}

func (c *wedgingConn) Read(ctx context.Context) ([]byte, error) {
	select {
	case b := <-c.in:
		return b, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *wedgingConn) Write(ctx context.Context, b []byte) error {
	first := false
	c.once.Do(func() { first = true })
	if first {
		close(c.entered)
		select {
		case <-c.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	c.mu.Lock()
	c.written = append(c.written, append([]byte(nil), b...))
	c.mu.Unlock()
	return nil
}

func (c *wedgingConn) Close() error { return nil }

func (c *wedgingConn) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.written)
}

// hasFrame reports whether a frame of this type and id has reached the wire.
func (c *wedgingConn) hasFrame(typ FrameType, id uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, raw := range c.written {
		if f, err := Decode(raw); err == nil && f.Type == typ && f.AttachID == id {
			return true
		}
	}
	return false
}

// TestAWedgedExecCallerDoesNotParkOnTheWriterForever is the bound that is
// actually free: an exec that cannot get a turn at the shared writer inside
// its wait budget is dropped, with NOTHING written and therefore the conn
// untouched.
//
// It is deliberately not named "does not stall the session's writer", which an
// earlier version of this test claimed. It cannot claim that: the peer holding
// the writer is holding it in conn.Write, and the only way to take it away is
// to close the conn — which would stall the session a great deal more. What
// bounds the wedge itself is the plane's own per-frame budget for an exec
// caller, which drops the caller and unwedges this hop; see execWriterWait.
func TestAWedgedExecCallerDoesNotParkOnTheWriterForever(t *testing.T) {
	c := newWedgingConn()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := newConnWriter(ctx, c)

	// Somebody else takes the writer and does not give it back.
	go func() { _ = w.write(Frame{Type: FrameControl, Payload: []byte(`{"kind":"x"}`)}) }()
	<-c.entered

	start := time.Now()
	err := w.writeWithin(Frame{Type: FrameServer, AttachID: 1, Payload: []byte(`{}`)},
		100*time.Millisecond, time.Minute)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("an exec frame reported success while another writer held the conn")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("the exec waited %s for a writer it was given 100ms to get", elapsed)
	}
	// Nothing of it was written, which is what keeps the conn out of it.
	if n := c.count(); n != 0 {
		t.Fatalf("%d frames reached the wire; a frame that missed the WAIT must not "+
			"be written at all", n)
	}
	close(c.release)
}

// TestTheWaitAndTheWriteHaveSeparateBudgets. One shared deadline converts
// contention into conn teardown: a frame that spent nearly all of it waiting
// for the writer would get the remainder to write, and an expired WRITE
// deadline closes the whole conn — so the destructive outcome would become
// more likely exactly under the load these bounds exist for.
func TestTheWaitAndTheWriteHaveSeparateBudgets(t *testing.T) {
	if execWriteDeadline <= execWriterWait {
		t.Fatalf("the write deadline (%s) must be longer than the wait (%s): the wait "+
			"costs one exec, the write costs the conn", execWriteDeadline, execWriterWait)
	}
	// And the wait must be longer than the budget the PLANE gives an exec
	// caller (20s plus a byte allowance), or a healthy exec would be dropped
	// merely because some other peer is in the process of being dropped.
	if execWriterWait < 25*time.Second {
		t.Fatalf("execWriterWait is %s; it must outlast the plane's own per-frame "+
			"budget for an exec caller", execWriterWait)
	}

	c := newWedgingConn()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// A generous wait and a short write deadline: the frame gets the writer at
	// once and then spends its own budget in the conn.
	w := newConnWriterBudget(ctx, c, 5*time.Second, 100*time.Millisecond)
	start := time.Now()
	err := w.writeWithin(Frame{Type: FrameServer, AttachID: 1, Payload: []byte(`{}`)},
		w.execWait, w.execDeadline)
	if err == nil {
		t.Fatal("a write that never completed reported success")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("the write took %s; it was given 100ms and must not spend the WAIT "+
			"budget as well", elapsed)
	}
	close(c.release)
}

// TestADroppedExecTellsItsCaller is the other half of the drop, and the half
// that was missing.
//
// On the WAIT path nothing has been written, so the conn is alive — and
// nothing else in the system will ever close that client: the hub's cascade
// fires only on conn death, and the plane's own budget only on a write it
// never gets to make. Without a FrameClose the CLI blocks forever on an exec
// the sandbox has already killed, which is the one thing exec may not do (the
// design promises a clean 125 with a sentence, never a stream that simply
// stops).
func TestADroppedExecTellsItsCaller(t *testing.T) {
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
	// A conn whose FIRST write wedges — which is what makes the exec's frame
	// miss the WAIT — and which passes everything after it, so the conn is
	// demonstrably alive when the FrameClose has to go out.
	c := newWedgingConn()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ex := newFakeExecer()
	w := newConnWriterBudget(ctx, c, 150*time.Millisecond, 10*time.Second)
	go serveSession(ctx, c, s, w, nil, ex)

	open, err := Encode(Frame{Type: FrameOpen, AttachID: 1, Kind: runner.KindExec,
		Exec: &runner.ExecSpec{Argv: []string{"true"}}})
	if err != nil {
		t.Fatal(err)
	}
	c.in <- open
	var a *fakeExecAttachment
	select {
	case a = <-ex.open:
	case <-time.After(5 * time.Second):
		t.Fatal("no exec was opened")
	}

	// Somebody else takes the writer and wedges it, so the exec's frame
	// cannot get a turn inside its 150ms wait.
	go func() { _ = w.write(Frame{Type: FrameControl, Payload: []byte(`{"kind":"x"}`)}) }()
	<-c.entered
	a.msgs <- terminal.ServerMessage{Type: terminal.TypeExecStdout, Data: []byte("x")}

	select {
	case <-a.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("an exec that missed its write budget was left running")
	}
	// The writer comes back, and the caller must be told.
	close(c.release)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if c.hasFrame(FrameClose, 1) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the dropped exec's caller was never sent a FrameClose on a LIVE " +
				"conn; its CLI waits forever for an exit status that is not coming")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestASlowExecConsumerIsDroppedAndClosed is the wiring the budget exists for.
// A frame that misses it means this exec's only reader is gone, so the exec
// leaves the table and its process group is killed rather than being left
// holding one of the session's eight slots with nobody to stream to.
func TestASlowExecConsumerIsDroppedAndClosed(t *testing.T) {
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
	c := newWedgingConn()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ex := newFakeExecer()
	w := newConnWriterBudget(ctx, c, 150*time.Millisecond, 150*time.Millisecond)
	go serveSession(ctx, c, s, w, nil, ex)

	open, err := Encode(Frame{Type: FrameOpen, AttachID: 1, Kind: runner.KindExec,
		Exec: &runner.ExecSpec{Argv: []string{"true"}}})
	if err != nil {
		t.Fatal(err)
	}
	c.in <- open

	var a *fakeExecAttachment
	select {
	case a = <-ex.open:
	case <-time.After(5 * time.Second):
		t.Fatal("no exec was opened")
	}
	// One frame, which wedges in the conn and never comes out.
	a.msgs <- terminal.ServerMessage{Type: terminal.TypeExecStdout, Data: []byte("x")}

	select {
	case <-a.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("an exec whose frame missed its write budget was left running")
	}
	close(c.release)
}

// TestAnUnknownAttachmentKindIsRefused is the nit with teeth: `if Kind ==
// KindExec` was correct for the two kinds that exist, and the next one added
// would have fallen into the terminal branch and opened the AGENT's pty with
// no handshake to save it — a take-over nobody asked for, which is the one
// outcome this whole design is built to make impossible.
func TestAnUnknownAttachmentKindIsRefused(t *testing.T) {
	rig := newExecTestRig(t)
	ctx := context.Background()
	client, hubClient := newPipe()
	defer client.Close()
	go rig.hub.AttachClient(ctx, hubClient, Open{Cols: 80, Rows: 24, Kind: "kind.v2"})

	// The client socket is closed rather than answered with a snapshot.
	readCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if b, err := client.Read(readCtx); err == nil {
		// The bytes are deliberately not printed: a snapshot is the whole
		// screen, and a failing test that scrolls a terminal for a page is a
		// failing test nobody reads.
		var m terminal.ServerMessage
		_ = json.Unmarshal(b, &m)
		t.Fatalf("an unknown kind was answered with a %q (%d bytes), want a close",
			m.Type, len(b))
	}
	if got := rig.proc.bytes(); len(got) != 0 {
		t.Fatalf("an unknown kind reached the agent's pty: %q", got)
	}
}

// TestASecondOpenOnALiveExecIdIsRefused: serveSession is the sandbox's trust
// boundary. Overwriting the table entry would leave the previous process
// unkilled and its slot held for the life of the session.
func TestASecondOpenOnALiveExecIdIsRefused(t *testing.T) {
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
	sessConn, peer := newPipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ex := newFakeExecer()
	ServeSessionWithExec(ctx, sessConn, s, nil, ex)

	open, err := Encode(Frame{Type: FrameOpen, AttachID: 9, Kind: runner.KindExec,
		Exec: &runner.ExecSpec{Argv: []string{"true"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := peer.Write(ctx, open); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ex.open:
	case <-time.After(5 * time.Second):
		t.Fatal("the first exec was never opened")
	}
	if err := peer.Write(ctx, open); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ex.open:
		t.Fatal("a second FrameOpen on a live exec id opened a SECOND process, " +
			"orphaning the first and its slot")
	case <-time.After(500 * time.Millisecond):
	}
}

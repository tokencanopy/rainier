package sandboxexec

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/internal/reap"
	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// ---------------------------------------------------------------------------
// the fake starter
// ---------------------------------------------------------------------------

// fakeProc is a scripted exec'd process: nothing is spawned, and the test
// decides what it writes, when it exits and with what. It is the same shape
// session.New's `start` seam gives the session's own tests — the whole exec
// runner is exercised with no pty, no container and no real process.
type fakeProc struct {
	pid int

	mu          sync.Mutex
	stdin       []byte
	stdinClosed bool
	sizes       [][2]int
	signals     []syscall.Signal
	ignoring    bool
	// signalled closes on the first signal, so a test can WAIT for one rather
	// than poll for it — which matters when the test does it a hundred
	// thousand times.
	signalled  chan struct{}
	signalOnce sync.Once

	onStdout func([]byte) error
	onStderr func([]byte) error
	// blocked, when set, parks every stdin write until it is closed.
	blocked chan struct{}

	done   chan struct{}
	status Status
	once   sync.Once
}

func (p *fakeProc) Write(b []byte) (int, error) {
	p.mu.Lock()
	blocked := p.blocked
	p.mu.Unlock()
	if blocked != nil {
		// A child that has stopped reading its stdin: the write parks, as a
		// real pipe write does once the kernel buffer is full.
		<-blocked
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stdinClosed {
		return 0, errors.New("stdin closed")
	}
	p.stdin = append(p.stdin, b...)
	return len(b), nil
}

// blockWrites makes every later stdin write park until the process exits,
// which is what a child that reads none of its input does to the pipe.
func (p *fakeProc) blockWrites() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blocked = p.done
}

func (p *fakeProc) CloseStdin() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stdinClosed = true
	return nil
}

func (p *fakeProc) Resize(cols, rows int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sizes = append(p.sizes, [2]int{cols, rows})
	return nil
}

func (p *fakeProc) Signal(sig syscall.Signal) error {
	p.mu.Lock()
	p.signals = append(p.signals, sig)
	ignore := p.ignoring
	ch := p.signalled
	p.mu.Unlock()
	if ch != nil {
		p.signalOnce.Do(func() { close(ch) })
	}
	// A real SIGKILL to a process group ends the process; the fake honours
	// that so the runner's grace-then-kill path terminates in a test.
	if !ignore && (sig == syscall.SIGKILL || sig == syscall.SIGTERM) {
		p.exit(Status{Signal: signalWireName(sig)})
	}
	return nil
}

// ignoreSignals makes this process record a signal without dying of it, which
// is what lets a test COUNT the callers that reached kill() rather than having
// the first one end the race for the rest.
func (p *fakeProc) ignoreSignals() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ignoring = true
}

func (p *fakeProc) Wait() Status { <-p.done; return p.status }
func (p *fakeProc) Pid() int     { return p.pid }

func (p *fakeProc) exit(st Status) {
	p.once.Do(func() {
		p.status = st
		close(p.done)
	})
}

func (p *fakeProc) sentSignals() []syscall.Signal {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]syscall.Signal(nil), p.signals...)
}

func (p *fakeProc) stdinBytes() ([]byte, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]byte(nil), p.stdin...), p.stdinClosed
}

// stdinProgress is stdinBytes without the copy, for the tests that poll a
// large stream: copying megabytes under the same lock the writer takes is
// itself the bottleneck, and a test that measured that would be measuring
// nothing about exec.
func (p *fakeProc) stdinProgress() (int, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.stdin), p.stdinClosed
}

type fakeStarter struct {
	mu      sync.Mutex
	reqs    []Request
	procs   []*fakeProc
	err     error
	started chan *fakeProc
}

func newFakeStarter() *fakeStarter {
	return &fakeStarter{started: make(chan *fakeProc, 16)}
}

func (f *fakeStarter) start(req Request, onStdout, onStderr func([]byte) error) (Proc, error) {
	f.mu.Lock()
	f.reqs = append(f.reqs, req)
	err := f.err
	pid := 1000 + len(f.procs)
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	p := &fakeProc{pid: pid, onStdout: onStdout, onStderr: onStderr, done: make(chan struct{})}
	f.mu.Lock()
	f.procs = append(f.procs, p)
	f.mu.Unlock()
	f.started <- p
	return p, nil
}

func (f *fakeStarter) lastRequest(t *testing.T) Request {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.reqs) == 0 {
		t.Fatal("nothing was ever started")
	}
	return f.reqs[len(f.reqs)-1]
}

// await takes the next started process, failing rather than hanging.
func (f *fakeStarter) await(t *testing.T) *fakeProc {
	t.Helper()
	select {
	case p := <-f.started:
		return p
	case <-time.After(5 * time.Second):
		t.Fatal("no process was started")
		return nil
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func testRunner(t *testing.T, start Starter) (*Runner, string) {
	t.Helper()
	root := t.TempDir()
	return NewRunner(root, []string{"PATH=/usr/bin:/bin", "HOME=/home/agent"}, start), root
}

// firstExecMessage takes the attachment's first message under a short bound.
// It is what turns a refusal regression into a FAILURE rather than a timeout:
// a refusal is the first thing an attachment says, and an exec that was
// wrongly accepted says exec_started and then runs.
func firstExecMessage(t *testing.T, a relay.ExecAttachment) terminal.ServerMessage {
	t.Helper()
	select {
	case m, ok := <-a.Msgs():
		if !ok {
			t.Fatal("the attachment closed without saying anything at all")
		}
		return m
	case <-time.After(5 * time.Second):
		t.Fatal("the attachment said nothing")
		return terminal.ServerMessage{}
	}
}

// drain reads an attachment to the end, which is how a test sees the whole
// conversation an exec had with its caller.
func drainAttachment(t *testing.T, a relay.ExecAttachment) []terminal.ServerMessage {
	t.Helper()
	var out []terminal.ServerMessage
	timeout := time.After(10 * time.Second)
	for {
		select {
		case m, ok := <-a.Msgs():
			if !ok {
				return out
			}
			out = append(out, m)
		case <-timeout:
			t.Fatalf("the attachment never closed; got %d messages so far", len(out))
			return out
		}
	}
}

func types(msgs []terminal.ServerMessage) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = m.Type
	}
	return out
}

// spec is a minimal runnable spec: an executable this test wrote into the
// workspace, so the lookup succeeds without depending on the host's /bin.
func execSpecIn(root string, argv ...string) runner.ExecSpec {
	return runner.ExecSpec{Argv: argv}
}

// withTool puts an executable named `name` on the runner's session PATH and
// returns the spec that names it.
func withTool(t *testing.T, r *Runner, name string) runner.ExecSpec {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	r.env = append(r.env, "PATH="+dir)
	return execSpecIn(r.root, name)
}

// ---------------------------------------------------------------------------
// exit status
// ---------------------------------------------------------------------------

// TestExecReportsTheCommandsExitCode is the contract in one line: the
// command's status IS what the caller is told, verbatim, including the zero
// that omitempty leaves off the wire.
func TestExecReportsTheCommandsExitCode(t *testing.T) {
	for _, code := range []int{0, 1, 7, 126, 255} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			start := newFakeStarter()
			r, _ := testRunner(t, start.start)
			spec := withTool(t, r, "tool")

			a := r.OpenExec(spec)
			p := start.await(t)
			p.exit(Status{Code: code})

			msgs := drainAttachment(t, a)
			if got := types(msgs); len(got) != 2 ||
				got[0] != terminal.TypeExecStarted || got[1] != terminal.TypeExecExit {
				t.Fatalf("message types = %v, want [exec_started exec_exit]", got)
			}
			if msgs[1].ExitCode != code || msgs[1].Signal != "" {
				t.Fatalf("exit = code %d signal %q, want code %d and no signal",
					msgs[1].ExitCode, msgs[1].Signal, code)
			}
		})
	}
}

// TestExecReportsASignalRatherThanACode is the other half, and the reason
// exec_exit carries two fields: a command may legitimately exit 137, so
// reporting a signalled death as an exit code would tell a script something
// false about a command that never chose its own status.
func TestExecReportsASignalRatherThanACode(t *testing.T) {
	start := newFakeStarter()
	r, _ := testRunner(t, start.start)
	spec := withTool(t, r, "tool")

	a := r.OpenExec(spec)
	p := start.await(t)
	p.exit(Status{Signal: "killed"})

	msgs := drainAttachment(t, a)
	last := msgs[len(msgs)-1]
	if last.Type != terminal.TypeExecExit || last.Signal != "killed" || last.ExitCode != 0 {
		t.Fatalf("signalled exit = %+v", last)
	}
}

// TestExecStartedIsAlwaysFirst pins the handshake at its source. The plane
// refuses an exec whose first server message is anything else, so a sandbox
// that let one byte of output overtake it would be refused by its own plane.
func TestExecStartedIsAlwaysFirst(t *testing.T) {
	start := newFakeStarter()
	r, _ := testRunner(t, start.start)
	spec := withTool(t, r, "tool")

	a := r.OpenExec(spec)
	p := start.await(t)
	go func() {
		for i := 0; i < 20; i++ {
			_ = p.onStdout([]byte("x"))
		}
		p.exit(Status{})
	}()

	msgs := drainAttachment(t, a)
	if msgs[0].Type != terminal.TypeExecStarted {
		t.Fatalf("the first message was %q, want exec_started", msgs[0].Type)
	}
	for _, m := range msgs[1:] {
		if m.Type == terminal.TypeExecStarted {
			t.Fatal("exec_started was sent twice")
		}
	}
}

// TestExecSeparatesStdoutFromStderr is the stream rule the CLI depends on to
// keep `rainier exec s -- cat f > out` producing exactly f.
func TestExecSeparatesStdoutFromStderr(t *testing.T) {
	start := newFakeStarter()
	r, _ := testRunner(t, start.start)
	spec := withTool(t, r, "tool")

	a := r.OpenExec(spec)
	p := start.await(t)
	go func() {
		_ = p.onStdout([]byte("out"))
		_ = p.onStderr([]byte("err"))
		p.exit(Status{})
	}()

	var out, errs string
	for _, m := range drainAttachment(t, a) {
		switch m.Type {
		case terminal.TypeExecStdout:
			out += string(m.Data)
		case terminal.TypeExecStderr:
			errs += string(m.Data)
		}
	}
	if out != "out" || errs != "err" {
		t.Fatalf("stdout=%q stderr=%q, want \"out\" and \"err\"", out, errs)
	}
}

// ---------------------------------------------------------------------------
// backpressure
// ---------------------------------------------------------------------------

// TestExecBackpressureDropsNothing is the design's deliberate opposite of
// session.trySend. A terminal viewer that stalls is force-detached because it
// is one of many and must not hold the session; an exec caller is the ONLY
// reader its process will ever have, so its output is never dropped — the
// sandbox stops taking chunks and the process blocks on write(2).
//
// The consumer here is slow on purpose and the producer writes far more than
// the queue holds; every byte must still arrive, in order.
func TestExecBackpressureDropsNothing(t *testing.T) {
	start := newFakeStarter()
	r, _ := testRunner(t, start.start)
	spec := withTool(t, r, "tool")

	a := r.OpenExec(spec)
	p := start.await(t)

	const chunks = 200
	go func() {
		for i := 0; i < chunks; i++ {
			if err := p.onStdout([]byte(fmt.Sprintf("%04d", i))); err != nil {
				t.Errorf("a chunk was refused: %v", err)
				return
			}
		}
		p.exit(Status{})
	}()

	var got strings.Builder
	var closed bool
	for !closed {
		select {
		case m, ok := <-a.Msgs():
			if !ok {
				closed = true
				break
			}
			if m.Type == terminal.TypeExecStdout {
				got.Write(m.Data)
			}
			// Slow on purpose: the queue is 8 deep, so the producer is
			// blocked for most of this.
			time.Sleep(time.Millisecond)
		case <-time.After(20 * time.Second):
			t.Fatal("the attachment never closed")
		}
	}
	var want strings.Builder
	for i := 0; i < chunks; i++ {
		fmt.Fprintf(&want, "%04d", i)
	}
	if got.String() != want.String() {
		t.Fatalf("received %d bytes, want %d — backpressure dropped output",
			got.Len(), want.Len())
	}
}

// ---------------------------------------------------------------------------
// stdin
// ---------------------------------------------------------------------------

// TestExecForwardsStdinAndItsEOF is amendment 3: end-of-input is an explicit
// frame, because a socket that is still open cannot express "no more input"
// and `cat` with a pipe on the other end has to terminate.
func TestExecForwardsStdinAndItsEOF(t *testing.T) {
	start := newFakeStarter()
	r, _ := testRunner(t, start.start)
	spec := withTool(t, r, "tool")

	a := r.OpenExec(spec)
	p := start.await(t)

	a.Client(terminal.ClientMessage{Type: "stdin", Data: []byte("hello ")})
	a.Client(terminal.ClientMessage{Type: "stdin", Data: []byte("world")})
	a.Client(terminal.ClientMessage{Type: terminal.TypeExecStdinEOF})

	deadline := time.Now().Add(5 * time.Second)
	for {
		in, closed := p.stdinBytes()
		if string(in) == "hello world" && closed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stdin = %q closed=%v, want \"hello world\" closed", in, closed)
		}
		time.Sleep(5 * time.Millisecond)
	}
	p.exit(Status{})
	drainAttachment(t, a)
}

// TestExecResizeAndSignalReachTheProcess pins the two client messages that
// are delivered AHEAD of queued stdin, and deliberately so: a Ctrl-C has to
// reach a child that has stopped reading its input, which is exactly the
// child a caller is most likely to be interrupting.
func TestExecResizeAndSignalReachTheProcess(t *testing.T) {
	start := newFakeStarter()
	r, _ := testRunner(t, start.start)
	spec := withTool(t, r, "tool")
	spec.TTY = true

	a := r.OpenExec(spec)
	p := start.await(t)

	a.Client(terminal.ClientMessage{Type: "resize", Cols: 100, Rows: 40})
	a.Client(terminal.ClientMessage{Type: terminal.TypeExecSignal, Signal: terminal.SignalINT})

	deadline := time.Now().Add(5 * time.Second)
	for {
		p.mu.Lock()
		sizes := append([][2]int(nil), p.sizes...)
		p.mu.Unlock()
		if len(sizes) == 1 && sizes[0] == [2]int{100, 40} {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("sizes = %v, want one 100x40", sizes)
		}
		time.Sleep(5 * time.Millisecond)
	}
	var sawINT bool
	for _, s := range p.sentSignals() {
		if s == syscall.SIGINT {
			sawINT = true
		}
	}
	if !sawINT {
		t.Fatalf("signals = %v, want SIGINT among them", p.sentSignals())
	}
	p.exit(Status{})
	drainAttachment(t, a)
}

// TestExecDropsTheOwnershipVocabulary is the mirror of the rule #84 put on
// the splice: an exec holds no lease, its frames carry no generation, and a
// claim on one has nothing to claim. Honouring one would let the far end of a
// socket name its own authority at a pty this attachment must never touch.
func TestExecDropsTheOwnershipVocabulary(t *testing.T) {
	start := newFakeStarter()
	r, _ := testRunner(t, start.start)
	spec := withTool(t, r, "tool")

	a := r.OpenExec(spec)
	p := start.await(t)

	for _, m := range []terminal.ClientMessage{
		{Type: terminal.TypeClaim, Expected: terminal.GenOf(1)},
		{Type: terminal.TypeRelease},
		{Type: terminal.TypeControl, Mode: terminal.ModeControl, Generation: terminal.GenOf(99)},
		{Type: terminal.TypeControlAck},
	} {
		a.Client(m)
	}
	// Nothing reached the process, and nothing was answered.
	time.Sleep(50 * time.Millisecond)
	if in, closed := p.stdinBytes(); len(in) != 0 || closed {
		t.Fatalf("an ownership message reached the process: stdin=%q closed=%v", in, closed)
	}
	if sigs := p.sentSignals(); len(sigs) != 0 {
		t.Fatalf("an ownership message signalled the process: %v", sigs)
	}
	p.exit(Status{})
	for _, m := range drainAttachment(t, a) {
		switch m.Type {
		case terminal.TypeAttached, terminal.TypeStale,
			terminal.TypeControlChanged, terminal.TypeControlAck:
			t.Fatalf("an exec answered with the ownership vocabulary: %q", m.Type)
		}
	}
}

// ---------------------------------------------------------------------------
// kill on disconnect
// ---------------------------------------------------------------------------

// TestExecKillsTheProcessWhenItsCallerGoes is the v1 lifetime rule for an
// ATTACHED exec: its life is its caller's. A process that survived its caller
// would need somewhere for its output to go, a way to be listed and a way to
// be killed — which is --detach, and which is tested separately.
func TestExecKillsTheProcessWhenItsCallerGoes(t *testing.T) {
	start := newFakeStarter()
	r, _ := testRunner(t, start.start)
	spec := withTool(t, r, "tool")

	a := r.OpenExec(spec)
	p := start.await(t)
	// Take the exec_started so the attachment is genuinely live.
	if m := <-a.Msgs(); m.Type != terminal.TypeExecStarted {
		t.Fatalf("first message = %q", m.Type)
	}

	a.Close()
	drainAttachment(t, a)

	var sawTERM bool
	for _, s := range p.sentSignals() {
		if s == syscall.SIGTERM {
			sawTERM = true
		}
	}
	if !sawTERM {
		t.Fatalf("signals after a caller disconnect = %v, want SIGTERM", p.sentSignals())
	}
	// Polled rather than read once: the attachment is finished by whichever
	// of the kill and the exit gets there first, and the slot comes back on
	// the exit's own path. A single read here would be asserting an ordering
	// between two goroutines that deliberately do not have one.
	deadline := time.Now().Add(5 * time.Second)
	for r.LiveCount() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the exec slot was not released: %d still live", r.LiveCount())
		}
		time.Sleep(time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// the concurrency cap
// ---------------------------------------------------------------------------

// TestThePublishedCapsAreWhatIsPublished. §3.9 of the command contract says
// "at most 8" and the design says four of those may be detached, so these are
// LITERALS here: reading them back out of the code under test made setting
// MaxConcurrent to 9999 a change no test noticed, which is the opposite of
// what a published number is for.
func TestThePublishedCapsAreWhatIsPublished(t *testing.T) {
	if MaxConcurrent != 8 {
		t.Fatalf("MaxConcurrent = %d; the contract publishes 8 concurrent execs "+
			"per session and a caller's retry loop is written against it", MaxConcurrent)
	}
	if MaxDetached != 4 {
		t.Fatalf("MaxDetached = %d; the design publishes 4, and the refusal for "+
			"the fifth names that number", MaxDetached)
	}
	if MaxDetached >= MaxConcurrent {
		t.Fatal("the detached sub-cap must leave room for the exec that stops one: " +
			"`rainier exec s -- kill <pid>` needs a slot of its own")
	}
	if ptyDefaultCols != 80 || ptyDefaultRows != 24 {
		t.Fatalf("the default pty is %dx%d, want 80x24 — what a terminal that "+
			"named no size gets is a thing a caller sees", ptyDefaultCols, ptyDefaultRows)
	}
	if stdinQueueBytes != 8<<20 {
		t.Fatalf("stdinQueueBytes = %d, want 8 MiB: the bound the design states, "+
			"and the one TestOrdinaryStdinIsNeverRefused is written against",
			stdinQueueBytes)
	}
}

// TestExecCapRefusesTheNinth pins the fork-bomb bound. Eight is slack rather
// than a working limit; what matters is that the ninth is refused BEFORE
// anything is spawned, and that a finished exec gives its slot back.
func TestExecCapRefusesTheNinth(t *testing.T) {
	start := newFakeStarter()
	r, _ := testRunner(t, start.start)
	spec := withTool(t, r, "tool")

	var live []relay.ExecAttachment
	var procs []*fakeProc
	for i := 0; i < MaxConcurrent; i++ {
		a := r.OpenExec(spec)
		live = append(live, a)
		procs = append(procs, start.await(t))
		if m := <-a.Msgs(); m.Type != terminal.TypeExecStarted {
			t.Fatalf("exec %d opened with %q", i, m.Type)
		}
	}

	refused := r.OpenExec(spec)
	msgs := drainAttachment(t, refused)
	if len(msgs) != 1 || msgs[0].Type != terminal.TypeExecError ||
		msgs[0].Reason != terminal.ReasonTooManyExecs {
		t.Fatalf("the ninth exec got %+v, want one exec_error too_many_execs", msgs)
	}
	start.mu.Lock()
	spawned := len(start.procs)
	start.mu.Unlock()
	if spawned != MaxConcurrent {
		t.Fatalf("the refused exec spawned something: %d processes", spawned)
	}

	// A finished exec gives its slot back.
	procs[0].exit(Status{})
	drainAttachment(t, live[0])
	deadline := time.Now().Add(5 * time.Second)
	for r.LiveCount() != MaxConcurrent-1 {
		if time.Now().After(deadline) {
			t.Fatalf("live count stuck at %d", r.LiveCount())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if next := r.OpenExec(spec); func() bool {
		m := <-next.Msgs()
		return m.Type != terminal.TypeExecStarted
	}() {
		t.Fatal("the freed slot was not reusable")
	}
}

// ---------------------------------------------------------------------------
// validation
// ---------------------------------------------------------------------------

// TestExecRefusesACwdOutsideTheWorkspace applies the existing push/pull rule
// at a new door — including the symlink escape, which is the version of the
// rule the kernel agrees with.
func TestExecRefusesACwdOutsideTheWorkspace(t *testing.T) {
	start := newFakeStarter()
	r, root := testRunner(t, start.start)
	spec := withTool(t, r, "tool")

	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "escape")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := os.Mkdir(filepath.Join(root, "inside"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "afile"), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	for name, cwd := range map[string]string{
		"absolute outside":  "/etc",
		"dot dot":           "../..",
		"through a symlink": "escape",
		"not a directory":   "afile",
		"does not exist":    "nope",
	} {
		t.Run(name, func(t *testing.T) {
			s := spec
			s.Cwd = cwd
			msgs := drainAttachment(t, r.OpenExec(s))
			if len(msgs) != 1 || msgs[0].Reason != terminal.ReasonCwdRefused {
				t.Fatalf("cwd %q got %+v, want one exec_error cwd_refused", cwd, msgs)
			}
		})
	}

	// And the inside case still runs, or the rule would be "refuse
	// everything" rather than "refuse an escape".
	s := spec
	s.Cwd = "inside"
	a := r.OpenExec(s)
	p := start.await(t)
	if got := start.lastRequest(t).Dir; got != filepath.Join(root, "inside") {
		t.Fatalf("cwd resolved to %q", got)
	}
	p.exit(Status{})
	drainAttachment(t, a)
}

// TestExecEnvRule is the rule about NAMES, and the reason it is a rule rather
// than a table: a user's own build legitimately needs arbitrary names, and the
// short list of refusals is exactly the names that turn "run this command"
// into "run something else".
func TestExecEnvRule(t *testing.T) {
	// The refusal table is DERIVED from the code's own list rather than
	// transcribed from it. Transcribing left six of the reserved names named
	// by no test at all, and a name added to the list later would have joined
	// them; deriving means the table cannot fall behind.
	refused := []string{
		"", "1BAD", "BAD-NAME", "BAD NAME", "lower case name",
	}
	for name := range reservedEnv {
		refused = append(refused, name)
	}
	for _, prefix := range reservedEnvPrefixes {
		refused = append(refused, prefix+"SOMETHING", prefix+"0")
	}
	sort.Strings(refused)
	if len(refused) < len(reservedEnv) {
		t.Fatal("the derived table is smaller than the list it is derived from")
	}
	for _, name := range refused {
		t.Run("refused "+name, func(t *testing.T) {
			start := newFakeStarter()
			r, _ := testRunner(t, start.start)
			spec := withTool(t, r, "tool")
			const value = "s3cr3t-value"
			spec.Env = map[string]string{name: value}
			a := r.OpenExec(spec)
			// The REFUSAL is asserted before the close is waited for, and
			// that is not a style preference: an exec that was wrongly
			// accepted spawns and runs, so waiting for the attachment to
			// close costs the whole drain budget for every case — a policy
			// regression would be a CI timeout across forty subtests rather
			// than a test that says which rule broke.
			first := firstExecMessage(t, a)
			if first.Type != terminal.TypeExecError || first.Reason != terminal.ReasonEnvRefused {
				t.Fatalf("env %q got %+v, want exec_error env_refused", name, first)
			}
			msgs := append([]terminal.ServerMessage{first}, drainAttachment(t, a)...)
			if len(msgs) != 1 {
				t.Fatalf("env %q got %+v, want ONE exec_error", name, msgs)
			}
			// The refusal says the reason and nothing else. Values are
			// secrets as often as not: never logged, never audited, never
			// quoted in an error, and — since this message is the one thing
			// that crosses back to the caller — never on the wire either.
			raw, err := json.Marshal(msgs[0])
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(raw), value) {
				t.Fatalf("the refusal carried the value: %s", raw)
			}
		})
	}

	t.Run("a NUL in a value", func(t *testing.T) {
		start := newFakeStarter()
		r, _ := testRunner(t, start.start)
		spec := withTool(t, r, "tool")
		spec.Env = map[string]string{"OK_NAME": "a\x00b"}
		msgs := drainAttachment(t, r.OpenExec(spec))
		if len(msgs) != 1 || msgs[0].Reason != terminal.ReasonEnvRefused {
			t.Fatalf("a NUL value got %+v", msgs)
		}
	})

	t.Run("an ordinary name is accepted and overrides", func(t *testing.T) {
		start := newFakeStarter()
		r, _ := testRunner(t, start.start)
		spec := withTool(t, r, "tool")
		spec.Env = map[string]string{"CI": "1", "_private": "2", "MY_VAR3": "3"}
		a := r.OpenExec(spec)
		p := start.await(t)
		env := start.lastRequest(t).Env
		for _, want := range []string{"CI=1", "_private=2", "MY_VAR3=3"} {
			if !contains(env, want) {
				t.Fatalf("env = %v, want %q in it", env, want)
			}
		}
		p.exit(Status{})
		drainAttachment(t, a)
	})
}

func contains(env []string, want string) bool {
	for _, kv := range env {
		if kv == want {
			return true
		}
	}
	return false
}

// TestExecEnvRefusalIsDeterministic: two bad names in one spec must always
// name the same reason, or the same command reports differently on different
// runs because a map ranged in a different order.
func TestExecEnvRefusalIsDeterministic(t *testing.T) {
	for i := 0; i < 20; i++ {
		start := newFakeStarter()
		r, _ := testRunner(t, start.start)
		spec := withTool(t, r, "tool")
		spec.Env = map[string]string{"PATH": "x", "HOME": "y", "1BAD": "z"}
		msgs := drainAttachment(t, r.OpenExec(spec))
		if len(msgs) != 1 || msgs[0].Reason != terminal.ReasonEnvRefused {
			t.Fatalf("run %d got %+v", i, msgs)
		}
	}
}

// TestExecLooksArgvZeroUpOnTheSessionPath is amendment 2. os/exec's LookPath
// reads the AMBIENT PATH, which is sessiond's; the environment an exec runs
// in is the agent's, and the day the two differ is the day a session's
// environment adds a toolchain.
func TestExecLooksArgvZeroUpOnTheSessionPath(t *testing.T) {
	start := newFakeStarter()
	r, _ := testRunner(t, start.start)

	toolDir := t.TempDir()
	tool := filepath.Join(toolDir, "make")
	if err := os.WriteFile(tool, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	// The session's PATH has it; this process's does not, which is the whole
	// point — t.Setenv guarantees the ambient one cannot accidentally agree.
	t.Setenv("PATH", "/nonexistent-for-this-test")
	r.env = []string{"PATH=" + toolDir}

	a := r.OpenExec(runner.ExecSpec{Argv: []string{"make", "test"}})
	p := start.await(t)
	req := start.lastRequest(t)
	if req.Path != tool {
		t.Fatalf("argv[0] resolved to %q, want the session PATH's %q", req.Path, tool)
	}
	if len(req.Argv) != 2 || req.Argv[0] != "make" {
		t.Fatalf("argv = %v; the child must still see the name the caller typed", req.Argv)
	}
	p.exit(Status{})
	drainAttachment(t, a)
}

// TestExecLookupFailures keeps 127 and 126 apart, because a caller's script
// acts on the difference: "you typed a name this sandbox does not have"
// against "it is there and it will not run".
func TestExecLookupFailures(t *testing.T) {
	start := newFakeStarter()
	r, root := testRunner(t, start.start)

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "notexec"), []byte("#!/bin/sh\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "adir"), 0o755); err != nil {
		t.Fatal(err)
	}
	r.env = []string{"PATH=" + dir}

	for name, tc := range map[string]struct {
		argv []string
		want string
	}{
		"missing":        {[]string{"definitely-not-here"}, terminal.ReasonNotFound},
		"not executable": {[]string{"notexec"}, terminal.ReasonNotExecutable},
		"a directory":    {[]string{"adir"}, terminal.ReasonNotExecutable},
		"empty argv":     {nil, terminal.ReasonNotFound},
		"empty name":     {[]string{""}, terminal.ReasonNotFound},
		// "Could not be executed", not "not found": nothing was looked for.
		// A NUL cannot cross execve at all, so this is Rainier refusing the
		// request — 126 — rather than the 127 a script reads as "you typed a
		// name this sandbox does not have".
		"a NUL in argv":                      {[]string{"tool", "a\x00b"}, terminal.ReasonNotExecutable},
		"an absolute path that is not there": {[]string{"/nope/nope"}, terminal.ReasonNotFound},
	} {
		t.Run(name, func(t *testing.T) {
			msgs := drainAttachment(t, r.OpenExec(runner.ExecSpec{Argv: tc.argv}))
			if len(msgs) != 1 || msgs[0].Type != terminal.TypeExecError || msgs[0].Reason != tc.want {
				t.Fatalf("%v got %+v, want one exec_error %s", tc.argv, msgs, tc.want)
			}
		})
	}
	_ = root
}

// TestExecPathSearchSkipsANonExecutable mirrors a shell: a stray
// non-executable `make` early on the path must not shadow the real one.
func TestExecPathSearchSkipsANonExecutable(t *testing.T) {
	start := newFakeStarter()
	r, _ := testRunner(t, start.start)

	first, second := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(first, "make"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	real := filepath.Join(second, "make")
	if err := os.WriteFile(real, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	r.env = []string{"PATH=" + first + string(os.PathListSeparator) + second}

	a := r.OpenExec(runner.ExecSpec{Argv: []string{"make"}})
	p := start.await(t)
	if got := start.lastRequest(t).Path; got != real {
		t.Fatalf("resolved to %q, want %q", got, real)
	}
	p.exit(Status{})
	drainAttachment(t, a)
}

// TestExecComposesTheSessionEnvironment pins what a child starts from: this
// process's environment — the container's, and therefore the agent's — plus
// the boot chain's exports. Without them `git status` in an exec would find
// no credential helper and no identity.
func TestExecComposesTheSessionEnvironment(t *testing.T) {
	env := SessionEnv(
		[]string{"PATH=/usr/bin", "HOME=/home/agent", "PATH=/usr/bin:/opt/bin"},
		[]string{"GIT_CONFIG_GLOBAL=/workspace/.rainier/gitconfig", "GIT_TERMINAL_PROMPT=0"})
	if !contains(env, "GIT_CONFIG_GLOBAL=/workspace/.rainier/gitconfig") ||
		!contains(env, "GIT_TERMINAL_PROMPT=0") || !contains(env, "HOME=/home/agent") {
		t.Fatalf("composed env = %v", env)
	}
	// Duplicates collapse to the LAST assignment, which is what makes "append
	// the additions" mean "override" and what execve would have done anyway.
	if contains(env, "PATH=/usr/bin") || !contains(env, "PATH=/usr/bin:/opt/bin") {
		t.Fatalf("PATH did not collapse to the last assignment: %v", env)
	}
}

// TestExecTTYAddsTERM matches session.StartProc: a pty without TERM is a
// terminal no full-screen program will draw on.
func TestExecTTYAddsTERM(t *testing.T) {
	start := newFakeStarter()
	r, _ := testRunner(t, start.start)
	spec := withTool(t, r, "tool")
	spec.TTY = true
	spec.Cols, spec.Rows = 100, 40

	a := r.OpenExec(spec)
	p := start.await(t)
	req := start.lastRequest(t)
	if !contains(req.Env, "TERM=xterm-256color") {
		t.Fatalf("a --tty exec got no TERM: %v", req.Env)
	}
	if !req.TTY || req.Cols != 100 || req.Rows != 40 {
		t.Fatalf("the pty request = %+v", req)
	}
	p.exit(Status{})
	drainAttachment(t, a)

	// And without --tty there is no TERM to inherit from the exec itself.
	plain := withTool(t, r, "tool2")
	a2 := r.OpenExec(plain)
	p2 := start.await(t)
	if contains(start.lastRequest(t).Env, "TERM=xterm-256color") {
		t.Fatal("a non-tty exec was given a TERM it has no terminal for")
	}
	p2.exit(Status{})
	drainAttachment(t, a2)
}

// ---------------------------------------------------------------------------
// detach (amendment 1)
// ---------------------------------------------------------------------------

// TestDetachedExecReportsAPidAndCloses is the whole shape of --detach: the
// sandbox answers exec_started with the pid and closes the attachment, and
// the process keeps running. `rainier exec s -- kill <pid>` is the way it is
// stopped, which is why the pid is the one thing reported.
func TestDetachedExecReportsAPidAndCloses(t *testing.T) {
	start := newFakeStarter()
	r, root := testRunner(t, start.start)
	spec := withTool(t, r, "tool")
	spec.Detach = true
	spec.LogPath = "run.log"

	a := r.OpenExec(spec)
	p := start.await(t)
	msgs := drainAttachment(t, a)
	if len(msgs) != 1 || msgs[0].Type != terminal.TypeExecStarted || msgs[0].PID != p.Pid() {
		t.Fatalf("a detached exec answered %+v, want one exec_started with pid %d", msgs, p.Pid())
	}
	if _, err := os.Stat(filepath.Join(root, "run.log")); err != nil {
		t.Fatalf("the log the caller named was not created: %v", err)
	}
	req := start.lastRequest(t)
	if !req.Detach || req.Log == nil {
		t.Fatalf("the request was not a detached one: %+v", req)
	}

	// A caller disconnecting does NOT kill it: that is the difference the
	// whole flag exists for.
	a.Close()
	time.Sleep(50 * time.Millisecond)
	if sigs := p.sentSignals(); len(sigs) != 0 {
		t.Fatalf("a detached exec was signalled by its caller's disconnect: %v", sigs)
	}
	if r.LiveCount() != 1 {
		t.Fatalf("a detached exec stopped counting against the cap while still running")
	}

	// The SESSION ending does kill it. That is the bound on its life.
	r.KillAll()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if len(p.sentSignals()) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the session's shutdown did not kill the detached exec")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestDetachRequiresALogInsideTheWorkspace: a detached process writing
// outside the tree its session owns is the same escape --cwd refuses, by a
// slower route.
func TestDetachRequiresALogInsideTheWorkspace(t *testing.T) {
	start := newFakeStarter()
	r, root := testRunner(t, start.start)
	spec := withTool(t, r, "tool")

	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "escape")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	for name, tc := range map[string]struct {
		detach bool
		log    string
	}{
		"detach with no log":     {true, ""},
		"log outside":            {true, "/etc/rainier.log"},
		"log through a symlink":  {true, "escape/run.log"},
		"log with a dot dot":     {true, "../run.log"},
		"a log without --detach": {false, "run.log"},
	} {
		t.Run(name, func(t *testing.T) {
			s := spec
			s.Detach, s.LogPath = tc.detach, tc.log
			msgs := drainAttachment(t, r.OpenExec(s))
			if len(msgs) != 1 || msgs[0].Reason != terminal.ReasonLogRefused {
				t.Fatalf("got %+v, want one exec_error log_refused", msgs)
			}
		})
	}
	start.mu.Lock()
	spawned := len(start.procs)
	start.mu.Unlock()
	if spawned != 0 {
		t.Fatalf("a refused detach spawned %d processes", spawned)
	}
}

// ---------------------------------------------------------------------------
// the shared demux
// ---------------------------------------------------------------------------

// TestClientNeverBlocksTheSharedDemux is the one rule here that is about the
// RELAY rather than about this exec: Client runs on the demux every
// attachment on this session's conn shares — the session's own terminal and
// the session RPC included — so no path through it may block at all.
//
// The case is a child that reads none of its stdin while its caller keeps
// writing. Blocking would be the textbook answer, and it is the wrong one
// here: a demux that does not return never reads the FrameClose that would
// have ended the exec, nor the exec_signal a Ctrl-C sends, so the wedge would
// have no way out at all. The input is buffered up to a generous bound and
// the exec is then ended — and ended with a NAMED reason, so its caller
// learns what happened rather than being told the connection dropped.
func TestClientNeverBlocksTheSharedDemux(t *testing.T) {
	for name, write := range map[string]func(a relay.ExecAttachment){
		// Many tiny writes: fills the queue's SLOTS long before its bytes.
		"many small writes": func(a relay.ExecAttachment) {
			for i := 0; i < stdinQueueDepth+64; i++ {
				a.Client(terminal.ClientMessage{Type: "stdin", Data: []byte("x")})
			}
		},
		// Few large writes: fills the queue's BYTES long before its slots.
		"few large writes": func(a relay.ExecAttachment) {
			chunk := make([]byte, 1<<20)
			for i := 0; i < 16; i++ {
				a.Client(terminal.ClientMessage{Type: "stdin", Data: chunk})
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			start := newFakeStarter()
			r, _ := testRunner(t, start.start)
			spec := withTool(t, r, "tool")

			a := r.OpenExec(spec)
			p := start.await(t)
			if m := <-a.Msgs(); m.Type != terminal.TypeExecStarted {
				t.Fatalf("first message = %q", m.Type)
			}
			// The child stops reading: every write to it now parks the pump,
			// so the queue fills and stays full.
			p.blockWrites()

			done := make(chan struct{})
			go func() { defer close(done); write(a) }()
			select {
			case <-done:
			case <-time.After(30 * time.Second):
				t.Fatal("Client wedged the relay's shared demux; a stalled exec " +
					"would have frozen the session's terminal with it")
			}

			// It ended the exec rather than dropping the caller's bytes, and
			// it said which.
			var reason string
			for _, m := range drainAttachment(t, a) {
				if m.Type == terminal.TypeExecError {
					reason = m.Reason
				}
			}
			if reason != terminal.ReasonStdinOverrun {
				t.Fatalf("a stalled stdin ended with reason %q, want %q — a bare "+
					"disconnect would be misattributed to the connection",
					reason, terminal.ReasonStdinOverrun)
			}
			var sawKill bool
			for _, s := range p.sentSignals() {
				if s == syscall.SIGTERM || s == syscall.SIGKILL {
					sawKill = true
				}
			}
			if !sawKill {
				t.Fatalf("a stalled stdin neither delivered nor ended the exec: %v",
					p.sentSignals())
			}
		})
	}
}

// TestOrdinaryStdinIsNeverRefused is the other half: the bound is generous
// enough that a command which reads its input normally never approaches it,
// however much the caller pipes.
func TestOrdinaryStdinIsNeverRefused(t *testing.T) {
	start := newFakeStarter()
	r, _ := testRunner(t, start.start)
	spec := withTool(t, r, "tool")

	a := r.OpenExec(spec)
	p := start.await(t)
	if m := <-a.Msgs(); m.Type != terminal.TypeExecStarted {
		t.Fatalf("first message = %q", m.Type)
	}

	// 32 MiB in 32 KiB chunks, the shape the CLI actually sends, into a child
	// that reads it as fast as it arrives.
	//
	// The feed is PACED, and that is the honest model rather than a
	// concession: in production this loop is the relay demux reading a
	// socket, so the rate is the network's. A test that fed from a tight
	// local loop would be measuring whether one goroutine can outrun another
	// on this machine, which is a question about the Go scheduler and not
	// about exec.
	chunk := make([]byte, 32<<10)
	for i := range chunk {
		chunk[i] = byte(i)
	}
	const chunks = 512 // 16 MiB, four times the sandbox's own stdin bound
	for i := 0; i < chunks; i++ {
		a.Client(terminal.ClientMessage{Type: "stdin", Data: chunk})
		time.Sleep(time.Millisecond) // ≈32 MB/s, faster than most links
	}
	a.Client(terminal.ClientMessage{Type: terminal.TypeExecStdinEOF})

	// PROGRESS, not wall clock. The claim is that an ordinary stream is never
	// REFUSED; a fixed deadline on a paced 16 MiB feed measures this machine's
	// throughput under -race instead, which is how this failed at -count=10 on
	// a loaded box while passing on its own.
	last, moved := -1, time.Now()
	for {
		n, closed := p.stdinProgress()
		if n == chunks*len(chunk) && closed {
			break
		}
		if n != last {
			last, moved = n, time.Now()
		}
		if time.Since(moved) > 30*time.Second {
			t.Fatalf("stalled at %d of %d bytes (closed=%v); an ordinary stream was refused",
				n, chunks*len(chunk), closed)
		}
		time.Sleep(5 * time.Millisecond)
	}
	p.exit(Status{})
	for _, m := range drainAttachment(t, a) {
		if m.Type == terminal.TypeExecError {
			t.Fatalf("an ordinary stream was refused: %+v", m)
		}
	}
}

// ---------------------------------------------------------------------------
// the session ending underneath an exec
// ---------------------------------------------------------------------------

// TestKillAllRacingAnOpeningExecDoesNotPanic is the crash this sandbox must
// never have. The slot is taken before the spawn, so the session's own
// shutdown can reach an attachment that has no process yet — and the closing
// it does there used to race the `exec_started` and the refusal that follow,
// which is a send on a closed channel and takes sessiond down with it. It
// takes it down BEFORE the shutdown path flushes the agent's last write,
// which is the part that costs somebody their work.
//
// It is a loop rather than one attempt because the window is microseconds
// wide: a single pass would pass on a build with the bug.
func TestKillAllRacingAnOpeningExecDoesNotPanic(t *testing.T) {
	for i := 0; i < 200; i++ {
		start := newFakeStarter()
		r, _ := testRunner(t, start.start)
		spec := withTool(t, r, "tool")

		opened := make(chan relay.ExecAttachment, 1)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); opened <- r.OpenExec(spec) }()
		go func() { defer wg.Done(); r.KillAll() }()
		wg.Wait()

		// The race has run. Whether the shutdown reached this exec or missed
		// it entirely is the whole point — so the attachment is ended here
		// either way, and the assertion is that nothing panicked and
		// everything still closes.
		a := <-opened
		drained := make(chan struct{})
		go func() { defer close(drained); drainAttachment(t, a) }()
		for {
			select {
			case <-drained:
			case <-time.After(2 * time.Millisecond):
				r.KillAll()
				continue
			}
			break
		}
	}
}

// TestASpawnThatRacesTheSessionsEndIsStillKilled is the other half: the spawn
// cannot be undone, so a process created in the instant the session was
// stopped has to be ended rather than left behind. It is what keeps "a
// detached exec dies with its session" true even at the boundary.
func TestASpawnThatRacesTheSessionsEndIsStillKilled(t *testing.T) {
	start := newFakeStarter()
	r, _ := testRunner(t, start.start)
	spec := withTool(t, r, "tool")
	spec.Detach, spec.LogPath = true, "run.log"

	// Hold the spawn until the session has already ended.
	release := make(chan struct{})
	r.start = func(req Request, onStdout, onStderr func([]byte) error) (Proc, error) {
		<-release
		return start.start(req, onStdout, onStderr)
	}

	a := r.OpenExec(spec)
	// Wait until the slot is taken, which is what makes the attachment
	// reachable by KillAll.
	deadline := time.Now().Add(5 * time.Second)
	for r.LiveCount() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the exec never took its slot")
		}
		time.Sleep(time.Millisecond)
	}
	r.KillAll()
	close(release)

	p := start.await(t)
	drainAttachment(t, a)
	deadline = time.Now().Add(5 * time.Second)
	for len(p.sentSignals()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("a process spawned as the session ended was left running")
		}
		time.Sleep(time.Millisecond)
	}
}

// TestDetachedExecsCannotFillEverySlot is the reason the detached cap is
// lower than the total. A detached process holds its slot for as long as it
// runs, and the design's only way to stop one is `rainier exec s -- kill
// <pid>` — which is itself an exec. A session that could fill all eight slots
// with detached work would have no way left to stop any of it, and no listing
// and no kill API to fall back on.
func TestDetachedExecsCannotFillEverySlot(t *testing.T) {
	start := newFakeStarter()
	r, root := testRunner(t, start.start)
	spec := withTool(t, r, "tool")
	detached := spec
	detached.Detach, detached.LogPath = true, "run.log"

	for i := 0; i < MaxDetached; i++ {
		msgs := drainAttachment(t, r.OpenExec(detached))
		if len(msgs) != 1 || msgs[0].Type != terminal.TypeExecStarted {
			t.Fatalf("detached exec %d answered %+v", i, msgs)
		}
		start.await(t)
	}
	// The next DETACHED one is refused, with its OWN word. Reusing
	// too_many_execs made the CLI print "this session is already running as
	// many commands as it may" with half the slots free — a false sentence,
	// and a different remedy: stop a detached run, do not wait for one.
	msgs := drainAttachment(t, r.OpenExec(detached))
	if len(msgs) != 1 || msgs[0].Reason != terminal.ReasonTooManyDetached {
		t.Fatalf("the %dth detached exec got %+v, want %q",
			MaxDetached+1, msgs, terminal.ReasonTooManyDetached)
	}
	// ...and an ATTACHED one — the `kill <pid>` that ends them — still runs.
	a := r.OpenExec(spec)
	p := start.await(t)
	if m := <-a.Msgs(); m.Type != terminal.TypeExecStarted {
		t.Fatalf("the command that stops a detached run was refused: %+v", m)
	}
	p.exit(Status{})
	drainAttachment(t, a)
	_ = root
}

// TestALogThatWouldBlockIsRefused. The path is the CALLER's, and a caller who
// can run one command can create anything on it: `open(2)` O_WRONLY on a FIFO
// with no reader never returns. This used to run on the relay's shared demux,
// where that froze the whole session — no terminal input, no session RPC, no
// FrameClose — until the conn died.
func TestALogThatWouldBlockIsRefused(t *testing.T) {
	start := newFakeStarter()
	r, root := testRunner(t, start.start)
	spec := withTool(t, r, "tool")
	spec.Detach, spec.LogPath = true, "wedge.log"

	if err := syscall.Mkfifo(filepath.Join(root, "wedge.log"), 0o644); err != nil {
		t.Skipf("mkfifo unavailable: %v", err)
	}

	done := make(chan []terminal.ServerMessage, 1)
	go func() { done <- drainAttachment(t, r.OpenExec(spec)) }()
	select {
	case msgs := <-done:
		if len(msgs) != 1 || msgs[0].Reason != terminal.ReasonLogRefused {
			t.Fatalf("a FIFO log got %+v, want one exec_error log_refused", msgs)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("opening a FIFO log never returned")
	}
	if r.LiveCount() != 0 {
		t.Fatalf("the refused exec kept its slot: %d live", r.LiveCount())
	}
}

// TestTheEnvRuleCoversTheChannelsThatChangeWhatRuns. The list is not a
// privilege boundary — a caller who may exec may run `sh -c` — but it is what
// keeps "somebody ran git here" from being the record of somebody running
// something else through git, and what keeps `-- make` running make.
func TestTheEnvRuleCoversTheChannelsThatChangeWhatRuns(t *testing.T) {
	refused := []string{
		// The loader, as a prefix: LD_AUDIT is LD_PRELOAD by another name.
		"LD_PRELOAD", "LD_LIBRARY_PATH", "LD_AUDIT", "LD_PROFILE",
		"LD_DEBUG_OUTPUT", "LD_ORIGIN_PATH", "LD_ANYTHING_AT_ALL",
		// git's config injection, which reaches core.sshCommand,
		// credential.helper and core.pager — every one of the named hooks.
		"GIT_CONFIG_COUNT", "GIT_CONFIG_KEY_0", "GIT_CONFIG_VALUE_0",
		"GIT_CONFIG_KEY_17", "GIT_CONFIG_SYSTEM", "GIT_CONFIG_GLOBAL", "GIT_CONFIG",
		"GIT_SSH", "GIT_SSH_COMMAND", "GIT_EXTERNAL_DIFF", "GIT_PROXY_COMMAND",
		"GIT_EDITOR", "GIT_PAGER", "GIT_SEQUENCE_EDITOR", "GIT_ASKPASS",
		"GIT_DIR", "GIT_WORK_TREE",
		// Shell startup files and trace hooks.
		"BASH_ENV", "ENV", "SHELLOPTS", "BASHOPTS", "PS4",
		// Interpreter option channels: --eval under another name.
		"NODE_OPTIONS", "PYTHONSTARTUP", "PERL5OPT", "RUBYOPT",
		// The originals.
		"HOME", "PATH", "SHELL", "IFS", "RAINIER_DIAL", "PAGER", "EDITOR",
	}
	for _, name := range refused {
		t.Run("refused "+name, func(t *testing.T) {
			start := newFakeStarter()
			r, _ := testRunner(t, start.start)
			spec := withTool(t, r, "tool")
			spec.Env = map[string]string{name: "x"}
			msgs := drainAttachment(t, r.OpenExec(spec))
			if len(msgs) != 1 || msgs[0].Reason != terminal.ReasonEnvRefused {
				t.Fatalf("%s was accepted (%+v); it changes what a named binary runs", name, msgs)
			}
		})
	}

	// And the names an ordinary build actually needs are still allowed: a
	// prefix rule over GIT_ or PYTHON would have taken these away.
	allowed := []string{"GIT_AUTHOR_NAME", "GIT_COMMITTER_EMAIL", "PYTHONPATH",
		"NODE_ENV", "CI", "CARGO_TERM_COLOR", "_private", "MAKEFLAGS"}
	for _, name := range allowed {
		t.Run("allowed "+name, func(t *testing.T) {
			start := newFakeStarter()
			r, _ := testRunner(t, start.start)
			spec := withTool(t, r, "tool")
			spec.Env = map[string]string{name: "x"}
			a := r.OpenExec(spec)
			p := start.await(t)
			if !contains(start.lastRequest(t).Env, name+"=x") {
				t.Fatalf("%s was refused; an ordinary build needs it", name)
			}
			p.exit(Status{})
			drainAttachment(t, a)
		})
	}
}

// TestAnEnormousEnvIsBounded: a name or a value is not a way to make this
// process allocate, nor to push an execve over E2BIG in a way that would
// report as "not executable" rather than as the refusal it is.
func TestAnEnormousEnvIsBounded(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"a huge name":  {"A" + strings.Repeat("B", maxEnvNameBytes): "x"},
		"a huge value": {"OK_NAME": strings.Repeat("v", maxEnvValueBytes+1)},
	} {
		t.Run(name, func(t *testing.T) {
			start := newFakeStarter()
			r, _ := testRunner(t, start.start)
			spec := withTool(t, r, "tool")
			spec.Env = env
			msgs := drainAttachment(t, r.OpenExec(spec))
			if len(msgs) != 1 || msgs[0].Reason != terminal.ReasonEnvRefused {
				t.Fatalf("got %+v, want one exec_error env_refused", msgs)
			}
		})
	}
}

// TestAPtySizeIsClampedRatherThanRefused: the ioctl takes a uint16, so a
// plain conversion turns 65536 into a nought-column terminal — a size no
// program draws on, installed as though the caller had asked for it.
func TestAPtySizeIsClampedRatherThanRefused(t *testing.T) {
	for name, tc := range map[string]struct {
		cols, rows, wantCols, wantRows int
	}{
		// Literals, not ptyDefaultCols/Rows: a table that reads the value it
		// is checking cannot notice it change.
		"none at all":   {0, 0, 80, 24},
		"negative":      {-1, -1, 80, 24},
		"past a uint16": {65536, 100000, 65535, 65535},
		"ordinary":      {100, 40, 100, 40},
	} {
		t.Run(name, func(t *testing.T) {
			start := newFakeStarter()
			r, _ := testRunner(t, start.start)
			spec := withTool(t, r, "tool")
			spec.TTY, spec.Cols, spec.Rows = true, tc.cols, tc.rows
			a := r.OpenExec(spec)
			p := start.await(t)
			req := start.lastRequest(t)
			if req.Cols != tc.wantCols || req.Rows != tc.wantRows {
				t.Fatalf("%dx%d became %dx%d, want %dx%d",
					tc.cols, tc.rows, req.Cols, req.Rows, tc.wantCols, tc.wantRows)
			}
			p.exit(Status{})
			drainAttachment(t, a)
		})
	}
}

// TestKillIsIdempotentUnderAnOverrunFlood is finding 9 as a count.
//
// kill() is documented idempotent and was not: closeOnce guarded only
// close(closing), so every caller armed its own SIGTERM and its own five
// second SIGKILL timer against the same process group — and offerStdin spawns
// one endWith per over-bound message, so two thousand messages after an
// overrun measured two thousand signals and two thousand live timers.
func TestKillIsIdempotentUnderAnOverrunFlood(t *testing.T) {
	start := newFakeStarter()
	r, _ := testRunner(t, start.start)
	spec := withTool(t, r, "tool")

	a := r.OpenExec(spec)
	p := start.await(t)
	if m := <-a.Msgs(); m.Type != terminal.TypeExecStarted {
		t.Fatalf("first message = %q", m.Type)
	}
	p.blockWrites()

	// Fill the queue past its bound, then keep going — which is exactly what
	// a caller piping into a command that has stopped reading does.
	chunk := make([]byte, 64<<10)
	for i := 0; i < (stdinQueueBytes/len(chunk))+16; i++ {
		a.Client(terminal.ClientMessage{Type: "stdin", Data: chunk})
	}
	for i := 0; i < 2000; i++ {
		a.Client(terminal.ClientMessage{Type: "stdin", Data: []byte("x")})
	}
	drainAttachment(t, a)

	// The fake's SIGTERM ends the process, so the escalation never runs and
	// every signal counted here is a separate caller reaching kill().
	sigs := p.sentSignals()
	if len(sigs) != 1 {
		t.Fatalf("an overrun delivered %d signals to the process group, want exactly 1 — "+
			"each one also arms a %s SIGKILL timer", len(sigs), killGrace)
	}
	if sigs[0] != syscall.SIGTERM {
		t.Fatalf("the one signal was %v, want SIGTERM", sigs[0])
	}
}

// TestOnlyOneReasonReachesACallerAfterAnOverrun: a caller learns WHY once, not
// once per message it had already sent. Two thousand exec_errors would be two
// thousand sentences for one fact.
func TestOnlyOneReasonReachesACallerAfterAnOverrun(t *testing.T) {
	start := newFakeStarter()
	r, _ := testRunner(t, start.start)
	spec := withTool(t, r, "tool")

	a := r.OpenExec(spec)
	p := start.await(t)
	if m := <-a.Msgs(); m.Type != terminal.TypeExecStarted {
		t.Fatalf("first message = %q", m.Type)
	}
	p.blockWrites()

	chunk := make([]byte, 64<<10)
	for i := 0; i < (stdinQueueBytes/len(chunk))+512; i++ {
		a.Client(terminal.ClientMessage{Type: "stdin", Data: chunk})
	}
	n := 0
	for _, m := range drainAttachment(t, a) {
		if m.Type == terminal.TypeExecError {
			n++
			if m.Reason != terminal.ReasonStdinOverrun {
				t.Fatalf("reason = %q, want %q", m.Reason, terminal.ReasonStdinOverrun)
			}
		}
	}
	if n != 1 {
		t.Fatalf("%d exec_errors reached the caller for one overrun, want 1", n)
	}
}

// TestPostEOFStdinCannotKillAHealthyCommand is finding 10. pumpStdin returns
// on the EOF, so nothing drains the queue afterwards; later stdin accumulated
// until it crossed the bound and then ended a command that had already been
// told its input was complete — reported as stdin_overrun, for input the child
// was never going to read. A closed pipe drops what is written to it.
func TestPostEOFStdinCannotKillAHealthyCommand(t *testing.T) {
	start := newFakeStarter()
	r, _ := testRunner(t, start.start)
	spec := withTool(t, r, "tool")

	a := r.OpenExec(spec)
	p := start.await(t)
	if m := <-a.Msgs(); m.Type != terminal.TypeExecStarted {
		t.Fatalf("first message = %q", m.Type)
	}
	a.Client(terminal.ClientMessage{Type: "stdin", Data: []byte("hello")})
	a.Client(terminal.ClientMessage{Type: terminal.TypeExecStdinEOF})

	// The stdin the caller's own pipe drained after it sent the EOF — well
	// past the bound, and none of it the command's business.
	chunk := make([]byte, 64<<10)
	for i := 0; i < (stdinQueueBytes/len(chunk))+64; i++ {
		a.Client(terminal.ClientMessage{Type: "stdin", Data: chunk})
	}

	// The command finishes normally, on its own terms.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, closed := p.stdinProgress(); closed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the EOF never reached the child")
		}
		time.Sleep(5 * time.Millisecond)
	}
	p.exit(Status{Code: 0})

	var reason string
	var exit *terminal.ServerMessage
	for _, m := range drainAttachment(t, a) {
		switch m.Type {
		case terminal.TypeExecError:
			reason = m.Reason
		case terminal.TypeExecExit:
			mm := m
			exit = &mm
		}
	}
	if reason != "" {
		t.Fatalf("stdin sent after the EOF ended a healthy command as %q", reason)
	}
	if exit == nil || exit.ExitCode != 0 || exit.Signal != "" {
		t.Fatalf("the command reported %+v, want a clean exit 0", exit)
	}
	if got, _ := p.stdinBytes(); string(got) != "hello" {
		t.Fatalf("the child received %q, want only the bytes sent before the EOF", got)
	}
}

// TestADetachedExecReturnsItsSlotWhenItEnds is the mutant that four
// SUCCESSFUL runs would have exposed and no test did: deleting the goroutine
// that waits out a detached process and releases its slot left the whole tree
// green, and a session that ran four detached commands could never run
// another for as long as it lived — with the sentence for the refusal naming
// the wrong cap.
func TestADetachedExecReturnsItsSlotWhenItEnds(t *testing.T) {
	start := newFakeStarter()
	r, root := testRunner(t, start.start)

	runOneDetached := func(name string) {
		t.Helper()
		spec := withTool(t, r, "tool")
		spec.Detach, spec.LogPath = true, name
		if err := os.WriteFile(filepath.Join(root, name), nil, 0o644); err != nil {
			t.Fatal(err)
		}
		a := r.OpenExec(spec)
		p := start.await(t)
		msgs := drainAttachment(t, a)
		if len(msgs) != 1 || msgs[0].Type != terminal.TypeExecStarted {
			t.Fatalf("a detached exec answered %v", types(msgs))
		}
		// It runs, and then it ends — the ordinary shape of a detached run.
		p.exit(Status{Code: 0})
	}

	// MaxDetached of them, each one finishing.
	for i := 0; i < MaxDetached; i++ {
		runOneDetached(fmt.Sprintf("run-%d.log", i))
	}

	deadline := time.Now().Add(10 * time.Second)
	for r.LiveCount() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d detached exec(s) that have ENDED still hold their slots; "+
				"this session can never run another", r.LiveCount())
		}
		time.Sleep(5 * time.Millisecond)
	}

	// And the next one is accepted rather than refused.
	runOneDetached("run-next.log")
}

// TestStdinJustUnderTheBoundIsNeverRefused is what makes stdinQueueBytes a
// number rather than a comment. TestOrdinaryStdinIsNeverRefused paces its feed
// into a child that consumes instantly, so the queue never accumulates at all
// — shrinking the bound from 8 MiB to 8 KiB survived it. This one blocks the
// child and writes to just under the bound, which is precisely the case the
// bound decides.
func TestStdinJustUnderTheBoundIsNeverRefused(t *testing.T) {
	start := newFakeStarter()
	r, _ := testRunner(t, start.start)
	spec := withTool(t, r, "tool")

	a := r.OpenExec(spec)
	p := start.await(t)
	if m := <-a.Msgs(); m.Type != terminal.TypeExecStarted {
		t.Fatalf("first message = %q", m.Type)
	}
	// The child reads NOTHING, so every byte below stays in the queue.
	p.blockWrites()

	const chunk = 64 << 10
	buf := make([]byte, chunk)
	// One chunk short of the bound: the last accepted write is the one that
	// brings the queue to exactly stdinQueueBytes.
	for sent := 0; sent+chunk <= stdinQueueBytes; sent += chunk {
		a.Client(terminal.ClientMessage{Type: "stdin", Data: buf})
	}

	// Nothing was refused, and the exec is still live: it is a caller ahead of
	// a slow child, which is the ordinary shape of a pipe.
	select {
	case m, ok := <-a.Msgs():
		if !ok {
			t.Fatal("a caller that stayed inside the bound had its exec ended")
		}
		t.Fatalf("a caller that stayed inside the bound was answered %+v", m)
	case <-time.After(300 * time.Millisecond):
	}
	if r.LiveCount() != 1 {
		t.Fatalf("live count = %d, want the exec still running", r.LiveCount())
	}

	p.exit(Status{Code: 0})
	for _, m := range drainAttachment(t, a) {
		if m.Type == terminal.TypeExecError {
			t.Fatalf("a caller inside the bound was refused: %q", m.Reason)
		}
	}
}

// TestTheTwoExhaustionsAreDifferentWords: a full session and a full detached
// pool are different facts with different remedies, and the sentence for one
// is false for the other.
func TestTheTwoExhaustionsAreDifferentWords(t *testing.T) {
	start := newFakeStarter()
	r, _ := testRunner(t, start.start)
	spec := withTool(t, r, "tool")

	// Fill every slot with ATTACHED execs; the next one is too_many_execs.
	for i := 0; i < MaxConcurrent; i++ {
		a := r.OpenExec(spec)
		start.await(t)
		if m := <-a.Msgs(); m.Type != terminal.TypeExecStarted {
			t.Fatalf("exec %d opened with %q", i, m.Type)
		}
	}
	msgs := drainAttachment(t, r.OpenExec(spec))
	if len(msgs) != 1 || msgs[0].Reason != terminal.ReasonTooManyExecs {
		t.Fatalf("a full session refused with %+v, want %q", msgs, terminal.ReasonTooManyExecs)
	}
	if terminal.ReasonTooManyExecs == terminal.ReasonTooManyDetached {
		t.Fatal("the two exhaustions are the same word again")
	}
}

// TestAPanicInThePlumbingCostsOneExecAndNotTheSession. sessiond has no
// recover around the relay's demux and this process by design outlives its
// agent, its connection and every viewer — a panic on one of this package's
// own goroutines would take the session's whole scrollback with it, and take
// it down BEFORE the shutdown path flushes the agent's last write.
//
// The caller is answered the way any connection that ended without a status
// answers: the attachment closes, which is exit 125 and a sentence.
func TestAPanicInThePlumbingCostsOneExecAndNotTheSession(t *testing.T) {
	boom := func(req Request, onStdout, onStderr func([]byte) error) (Proc, error) {
		panic("a bug in the spawn")
	}
	r, _ := testRunner(t, boom)
	spec := withTool(t, r, "tool")

	a := r.OpenExec(spec)
	// It closes rather than taking the process down with it.
	drainAttachment(t, a)

	// And the runner is still usable: the slot came back and the next command
	// runs, which is what "one exec and not the session" means.
	deadline := time.Now().Add(5 * time.Second)
	for r.LiveCount() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("a panicking spawn left %d slot(s) held", r.LiveCount())
		}
		time.Sleep(5 * time.Millisecond)
	}
	start := newFakeStarter()
	r2, _ := testRunner(t, start.start)
	spec2 := withTool(t, r2, "tool")
	b := r2.OpenExec(spec2)
	p := start.await(t)
	if m := <-b.Msgs(); m.Type != terminal.TypeExecStarted {
		t.Fatalf("the next exec opened with %q", m.Type)
	}
	p.exit(Status{})
	drainAttachment(t, b)
}

// ---------------------------------------------------------------------------
// ending a session, and the races around it
// ---------------------------------------------------------------------------

// TestAnExecOpenedDuringTheQuiesceIsRefused. KillAll sweeps what is live at
// the instant it runs, and the relay conn stays up for the whole of the
// suspend's budget — so an exec arriving mid-sweep was spawned, never
// signalled, and then FROZEN alive by `docker pause`, resuming hours later.
// That is exactly the lifetime rule the suspend path exists to establish.
func TestAnExecOpenedDuringTheQuiesceIsRefused(t *testing.T) {
	start := newFakeStarter()
	r, _ := testRunner(t, start.start)
	r.grace = 200 * time.Millisecond
	spec := withTool(t, r, "tool")

	// One exec that will not die politely, so the quiesce really is in
	// progress when the second one arrives.
	first := r.OpenExec(spec)
	p := start.await(t)
	if m := <-first.Msgs(); m.Type != terminal.TypeExecStarted {
		t.Fatalf("first message = %q", m.Type)
	}

	done := make(chan int, 1)
	go func() { done <- r.KillAllAndWait(5 * time.Second) }()
	time.Sleep(50 * time.Millisecond) // the sweep is under way

	late := drainAttachment(t, r.OpenExec(spec))
	if len(late) != 1 || late[0].Reason != terminal.ReasonSessionEnding {
		t.Fatalf("an exec opened while the session was ending got %+v, want one "+
			"exec_error %s — it would have been frozen alive", late, terminal.ReasonSessionEnding)
	}
	start.mu.Lock()
	spawned := len(start.procs)
	start.mu.Unlock()
	if spawned != 1 {
		t.Fatalf("%d processes were spawned; the late one must never reach a fork", spawned)
	}

	p.exit(Status{Signal: "TERM"})
	if left := <-done; left != 0 {
		t.Fatalf("%d exec(s) were still live when the quiesce gave up", left)
	}
	drainAttachment(t, first)
}

// TestASpawnInFlightWhenTheSessionEndsIsStillKilled is the other side of the
// latch, and the reason one sweep is enough. A spawn that took its slot before
// the latch closed is in the sweep's snapshot — `quiescing` is set and `live`
// is read under the one hold of the lock `reserve` also takes — so the kill
// finds an attachment with no process yet, and `arm` ends the process when the
// starter finally returns.
func TestASpawnInFlightWhenTheSessionEndsIsStillKilled(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	spawned := make(chan *fakeProc, 1)
	slow := func(req Request, onStdout, onStderr func([]byte) error) (Proc, error) {
		once.Do(func() { <-release })
		p := &fakeProc{pid: 4242, done: make(chan struct{})}
		spawned <- p
		return p, nil
	}
	r, _ := testRunner(t, slow)
	r.grace = 100 * time.Millisecond
	spec := withTool(t, r, "tool")

	a := r.OpenExec(spec)
	// It has taken its slot and is parked inside the starter.
	deadline := time.Now().Add(5 * time.Second)
	for r.LiveCount() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the exec never took its slot")
		}
		time.Sleep(2 * time.Millisecond)
	}

	done := make(chan int, 1)
	go func() { done <- r.KillAllAndWait(5 * time.Second) }()
	time.Sleep(50 * time.Millisecond)
	close(release) // the spawn completes into a session that is already ending

	var p *fakeProc
	select {
	case p = <-spawned:
	case <-time.After(5 * time.Second):
		t.Fatal("the spawn never completed")
	}
	if left := <-done; left != 0 {
		t.Fatalf("%d exec(s) survived; a spawn in flight when the session ended must "+
			"still be reaped", left)
	}
	if len(p.sentSignals()) == 0 {
		t.Fatal("the process that finished spawning into an ending session was never " +
			"signalled; on a warm suspend it would be frozen alive")
	}
	drainAttachment(t, a)
}

// TestKillFromTwoCallersAtOnceSignalsOnce is killOnce's own witness.
//
// The overrun flood drives thousands of messages through ONE path, so the
// `ending` gate collapses them before kill() is reached and killOnce is never
// exercised by more than one caller — both gates could be deleted one at a
// time with the suite green. This reaches kill() from the four callers that
// genuinely race in production: the caller's Close, the conn's death, the
// session's KillAll, and a stalled stdin.
func TestKillFromTwoCallersAtOnceSignalsOnce(t *testing.T) {
	for i := 0; i < 50; i++ {
		start := newFakeStarter()
		r, _ := testRunner(t, start.start)
		r.grace = time.Hour // the escalation must not be what ends the process
		spec := withTool(t, r, "tool")

		a := r.OpenExec(spec)
		p := start.await(t)
		if m := <-a.Msgs(); m.Type != terminal.TypeExecStarted {
			t.Fatalf("first message = %q", m.Type)
		}
		// A process that ignores the signal, so every caller's kill is
		// recorded rather than the first one ending the race.
		p.ignoreSignals()

		var wg sync.WaitGroup
		for _, kill := range []func(){a.Close, r.KillAll, a.Close, r.KillAll} {
			wg.Add(1)
			go func(fn func()) { defer wg.Done(); fn() }(kill)
		}
		wg.Wait()

		// Give every racer's goroutine a chance to have signalled.
		deadline := time.Now().Add(2 * time.Second)
		for len(p.sentSignals()) == 0 && time.Now().Before(deadline) {
			time.Sleep(2 * time.Millisecond)
		}
		time.Sleep(20 * time.Millisecond)
		if n := len(p.sentSignals()); n != 1 {
			t.Fatalf("iteration %d: four concurrent kills delivered %d signals to the "+
				"process group, want exactly 1 — each also arms its own SIGKILL timer",
				i, n)
		}
		p.exit(Status{Signal: "TERM"})
		drainAttachment(t, a)
	}
}

// TestAnOverrunSpawnsOneGoroutineNotOnePerMessage is the `ending` gate's own
// witness, and it is about allocation rather than about output: killOnce
// collapses the KILLS whatever happens here, and emit's closing-first check
// collapses the SENTENCES, so nothing a caller receives distinguishes one
// goroutine from two thousand. What distinguishes them is two thousand
// goroutine stacks for one fact.
//
// The outbox is deliberately left FULL and `closing` left open, so every
// goroutine that reaches emit parks there instead of returning — which is what
// turns "how many were spawned" into something a test can count at all.
func TestAnOverrunSpawnsOneGoroutineNotOnePerMessage(t *testing.T) {
	start := newFakeStarter()
	r, _ := testRunner(t, start.start)
	r.grace = time.Hour
	spec := withTool(t, r, "tool")

	a := r.OpenExec(spec)
	p := start.await(t)
	// exec_started is NOT read, and the child fills the rest of the outbox, so
	// the next emit blocks rather than returning.
	var filling sync.WaitGroup
	for i := 0; i < outQueue; i++ {
		filling.Add(1)
		go func() { defer filling.Done(); _ = p.onStdout([]byte("x")) }()
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(a.Msgs()) < outQueue {
		if time.Now().After(deadline) {
			t.Fatalf("the outbox never filled (%d/%d)", len(a.Msgs()), outQueue)
		}
		time.Sleep(2 * time.Millisecond)
	}
	p.blockWrites()
	p.ignoreSignals()

	chunk := make([]byte, 64<<10)
	for i := 0; i < (stdinQueueBytes/len(chunk))+8; i++ {
		a.Client(terminal.ClientMessage{Type: "stdin", Data: chunk})
	}
	base := runtime.NumGoroutine()
	const flood = 2000
	for i := 0; i < flood; i++ {
		a.Client(terminal.ClientMessage{Type: "stdin", Data: []byte("x")})
	}
	// Let anything that was going to be spawned be spawned.
	time.Sleep(200 * time.Millisecond)
	peak := runtime.NumGoroutine()
	if peak-base > flood/20 {
		t.Fatalf("%d messages after an overrun left %d extra goroutines parked; one "+
			"fact is one goroutine", flood, peak-base)
	}

	// Unwedge everything so the test can end.
	go func() {
		for range a.Msgs() {
		}
	}()
	filling.Wait()
	p.exit(Status{Signal: "TERM"})
	waitForLive(t, r, 0)
}

// waitForLive polls until the runner holds n execs, or fails.
func waitForLive(t *testing.T, r *Runner, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for r.LiveCount() != n {
		if time.Now().After(deadline) {
			t.Fatalf("live count is %d, want %d", r.LiveCount(), n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestStdinAfterTheChildClosedItsOwnCannotKillTheCommand is the other exit of
// the same function, and it has the same consequence.
//
// pumpStdin ends on the caller's EOF and also when the child closed its own
// stdin — `rainier exec s -- sh -c 'head -1; sleep 10' < bigfile` is the
// ordinary shape of the second. Guarding only the EOF left the second one
// accumulating to the 8 MiB bound and then killing a healthy command as
// `stdin_overrun`, for input the child was never going to read.
func TestStdinAfterTheChildClosedItsOwnCannotKillTheCommand(t *testing.T) {
	start := newFakeStarter()
	r, _ := testRunner(t, start.start)
	spec := withTool(t, r, "tool")

	a := r.OpenExec(spec)
	p := start.await(t)
	if m := <-a.Msgs(); m.Type != terminal.TypeExecStarted {
		t.Fatalf("first message = %q", m.Type)
	}
	a.Client(terminal.ClientMessage{Type: "stdin", Data: []byte("first line\n")})
	deadline := time.Now().Add(5 * time.Second)
	for {
		if n, _ := p.stdinProgress(); n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the first line never reached the child")
		}
		time.Sleep(2 * time.Millisecond)
	}

	// The CHILD closes its own stdin. The next write fails and the pump ends,
	// with no EOF from the caller anywhere in it.
	if err := p.CloseStdin(); err != nil {
		t.Fatal(err)
	}
	a.Client(terminal.ClientMessage{Type: "stdin", Data: []byte("ignored\n")})
	time.Sleep(50 * time.Millisecond)

	// The rest of the caller's file, which nobody is draining.
	chunk := make([]byte, 64<<10)
	for i := 0; i < (stdinQueueBytes/len(chunk))+64; i++ {
		a.Client(terminal.ClientMessage{Type: "stdin", Data: chunk})
	}
	p.exit(Status{Code: 0})

	var reason string
	var exited bool
	for _, m := range drainAttachment(t, a) {
		switch m.Type {
		case terminal.TypeExecError:
			reason = m.Reason
		case terminal.TypeExecExit:
			exited = m.ExitCode == 0 && m.Signal == ""
		}
	}
	if reason != "" {
		t.Fatalf("a command whose child closed its OWN stdin was killed with %q by "+
			"input it was never going to read", reason)
	}
	if !exited {
		t.Fatal("the command did not report its own clean exit")
	}
}

// TestASpawnTakesAReaperMark pins the CALL SITE of reap.Mark, which nothing
// else does: internal/reap's own tests drive AwaitStatus with marks a test
// made up, so the whole pid-wraparound half of the fix could be deleted here
// with the suite green.
//
// Linux only, because Mark is always zero elsewhere by construction.
func TestASpawnTakesAReaperMark(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("the reaper, and therefore a non-zero mark, is linux-only")
	}
	reap.Start()
	// Give the reaper something to count, so a mark taken after it is
	// non-zero and "no mark at all" is distinguishable from "mark 0".
	warm := exec.Command("/bin/sh", "-c", "exit 0")
	if err := warm.Start(); err != nil {
		t.Skipf("no /bin/sh: %v", err)
	}
	warmMark := reap.Mark()
	if _, ok := reap.AwaitStatus(warm.Process.Pid, warmMark); !ok {
		t.Skip("this build has no reaper")
	}
	if reap.Mark() == 0 {
		t.Skip("the reaper recorded nothing; nothing to distinguish")
	}

	dir := t.TempDir()
	tool := filepath.Join(dir, "tool")
	if err := os.WriteFile(tool, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	proc, err := NewSpawner().Start(Request{
		Path: tool, Argv: []string{"tool"}, Dir: dir, Env: []string{"PATH=" + dir},
	}, func([]byte) error { return nil }, func([]byte) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	p, ok := proc.(*process)
	if !ok {
		t.Fatalf("the spawner returned %T", proc)
	}
	if p.mark == 0 {
		t.Fatal("the spawn took no reaper mark. An outcome the reaper recorded before " +
			"this child was forked cannot be this child's, and pid_max wraps — without " +
			"the mark a stale orphan entry is read as this exec's status.")
	}
	p.Wait()
}

// TestArmNeverLosesTheRaceWithKill is the interleaving that leaves a live
// process with nobody left to signal it:
//
//	kill:  lock; read a.proc (nil); unlock
//	arm:   lock; install a.proc; unlock
//	arm:   `closing` still open -> report live, start the stdin pump
//	kill:  close(`closing`); a.proc was nil -> finish and return
//
// The process is armed, the attachment is closing, and no path reaches the
// process again. The ordering is old — this is not a regression — but the
// CONSEQUENCE is new: before the suspend path, KillAll ran only as the
// container was being taken away, where a SIGKILL followed regardless. On a
// warm suspend an escapee is frozen alive and resumes hours later, which is
// the exact rule the suspend path exists to establish.
//
// The invariant, whichever side wins: either `arm` reports the attachment
// already closing (and ends the process itself), or it reports live and
// `kill` finds the process and ends it. Never neither.
func TestArmNeverLosesTheRaceWithKill(t *testing.T) {
	// PROBABILISTIC, and it says so: the window is one unlocked instruction
	// pair, so the only way to reach it is to try the interleaving very many
	// times with real contention — which is why the workers run concurrently
	// rather than in one loop. Reverting the fix fails this in roughly half of
	// single runs and in every `-count=5`; passing it once is not proof, and
	// the deterministic guarantee is the lock pairing described above, which
	// TestBothOrderingsOfArmAndKillEndTheProcess covers from the other side.
	const (
		workers    = 16
		iterations = 20000
	)
	r, _ := testRunner(t, nil)
	fail := make(chan string, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				// Built directly rather than through newAttachment: its stdin
				// queue is stdinQueueDepth deep, a quarter of a megabyte per
				// attachment, which would make this a memory benchmark.
				// Nothing here feeds stdin.
				a := &attachment{
					runner:  r,
					msgs:    make(chan terminal.ServerMessage, outQueue),
					closing: make(chan struct{}),
					stdin:   make(chan stdinPiece, 1),
				}
				p := &fakeProc{pid: 7000 + i, done: make(chan struct{}),
					signalled: make(chan struct{})}

				killed := make(chan struct{})
				go func() { defer close(killed); a.kill() }()
				armed := a.arm(p, false)
				<-killed

				select {
				case <-p.signalled:
				case <-time.After(5 * time.Second):
					select {
					case fail <- fmt.Sprintf("worker %d iteration %d (arm reported "+
						"live=%v): the process was armed and NEVER signalled — on a "+
						"warm suspend it would be frozen alive", w, i, armed):
					default:
					}
					return
				}
				a.finish()
			}
		}(w)
	}
	wg.Wait()
	select {
	case msg := <-fail:
		t.Fatal(msg)
	default:
	}
}

// TestBothOrderingsOfArmAndKillEndTheProcess is the deterministic half of the
// same invariant: whichever of the two reaches the lock first, the process is
// ended. It cannot reach the interleaved window — nothing sequential can — so
// it is a guard against breaking the ordinary paths while fixing the race,
// not a guard against the race.
func TestBothOrderingsOfArmAndKillEndTheProcess(t *testing.T) {
	r, _ := testRunner(t, nil)

	t.Run("kill first, then the spawn completes", func(t *testing.T) {
		a := newAttachment(r)
		p := &fakeProc{pid: 11, done: make(chan struct{}), signalled: make(chan struct{})}
		a.kill()
		if a.arm(p, false) {
			t.Fatal("arm reported an attachment live after it had been killed")
		}
		select {
		case <-p.signalled:
		case <-time.After(5 * time.Second):
			t.Fatal("a spawn that completed into a killed attachment was never signalled")
		}
	})

	t.Run("the spawn completes, then kill", func(t *testing.T) {
		a := newAttachment(r)
		p := &fakeProc{pid: 12, done: make(chan struct{}), signalled: make(chan struct{})}
		if !a.arm(p, false) {
			t.Fatal("arm reported a live attachment closed")
		}
		a.kill()
		select {
		case <-p.signalled:
		case <-time.After(5 * time.Second):
			t.Fatal("an armed process was never signalled by kill")
		}
	})
}

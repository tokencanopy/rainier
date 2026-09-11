package sandboxexec

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

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

	onStdout func([]byte) error
	onStderr func([]byte) error

	done   chan struct{}
	status Status
	once   sync.Once
}

func (p *fakeProc) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stdinClosed {
		return 0, errors.New("stdin closed")
	}
	p.stdin = append(p.stdin, b...)
	return len(b), nil
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
	p.mu.Unlock()
	// A real SIGKILL to a process group ends the process; the fake honours
	// that so the runner's grace-then-kill path terminates in a test.
	if sig == syscall.SIGKILL || sig == syscall.SIGTERM {
		p.exit(Status{Signal: signalWireName(sig)})
	}
	return nil
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
	if r.LiveCount() != 0 {
		t.Fatalf("the exec slot was not released: %d still live", r.LiveCount())
	}
}

// ---------------------------------------------------------------------------
// the concurrency cap
// ---------------------------------------------------------------------------

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
	refused := []string{
		"", "1BAD", "BAD-NAME", "BAD NAME", "lower case name",
		"RAINIER_DIAL", "RAINIER_SESSION",
		"HOME", "PATH", "SHELL", "IFS",
		"LD_PRELOAD", "LD_LIBRARY_PATH",
		"GIT_CONFIG_GLOBAL", "GIT_SSH_COMMAND", "GIT_ASKPASS",
	}
	for _, name := range refused {
		t.Run("refused "+name, func(t *testing.T) {
			start := newFakeStarter()
			r, _ := testRunner(t, start.start)
			spec := withTool(t, r, "tool")
			const value = "s3cr3t-value"
			spec.Env = map[string]string{name: value}
			msgs := drainAttachment(t, r.OpenExec(spec))
			if len(msgs) != 1 || msgs[0].Reason != terminal.ReasonEnvRefused {
				t.Fatalf("env %q got %+v, want one exec_error env_refused", name, msgs)
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
		"missing":                            {[]string{"definitely-not-here"}, terminal.ReasonNotFound},
		"not executable":                     {[]string{"notexec"}, terminal.ReasonNotExecutable},
		"a directory":                        {[]string{"adir"}, terminal.ReasonNotExecutable},
		"empty argv":                         {nil, terminal.ReasonNotFound},
		"empty name":                         {[]string{""}, terminal.ReasonNotFound},
		"a NUL in argv":                      {[]string{"tool", "a\x00b"}, terminal.ReasonNotFound},
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

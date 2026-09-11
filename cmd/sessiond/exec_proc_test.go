package main

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// This file is the half of the exec runner that a fake cannot answer for: a
// real fork/exec, real pipes, a real pty, and a real process group. Everything
// above it is exercised against a scripted starter (exec_test.go); these tests
// exist because the seam cannot tell you whether the MINUS SIGN on a group
// signal is there.

// ---------------------------------------------------------------------------
// the subprocess target
// ---------------------------------------------------------------------------

// TestExecSubprocessTarget is this test binary acting as the command an exec
// runs. It is the same pattern TestGitHubCLIExecSubprocessTarget uses: a real
// executable with no dependency on what happens to be installed on the
// machine running the suite.
func TestExecSubprocessTarget(t *testing.T) {
	switch os.Getenv("RAINIER_EXEC_TEST_TARGET") {
	case "":
		return // an ordinary test run; this is not the target
	case "streams":
		fmt.Fprint(os.Stdout, "to stdout")
		fmt.Fprint(os.Stderr, "to stderr")
		os.Exit(7)
	case "env":
		fmt.Fprint(os.Stdout, os.Getenv("RAINIER_EXEC_TEST_REPORT"))
		os.Exit(0)
	case "cwd":
		dir, _ := os.Getwd()
		fmt.Fprint(os.Stdout, dir)
		os.Exit(0)
	case "cat":
		// Copy stdin to a file, which is what makes end-of-input observable:
		// without the explicit EOF frame this never returns.
		buf := make([]byte, 0, 1<<16)
		chunk := make([]byte, 4096)
		for {
			n, err := os.Stdin.Read(chunk)
			buf = append(buf, chunk[:n]...)
			if err != nil {
				break
			}
		}
		os.WriteFile(os.Getenv("RAINIER_EXEC_TEST_REPORT"), buf, 0o644)
		os.Exit(0)
	case "signal-self":
		syscall.Kill(os.Getpid(), syscall.SIGTERM)
		select {}
	case "sleep":
		fmt.Fprint(os.Stdout, "up")
		select {}
	case "grandchild":
		// A child that outlives its parent HOLDING THE SAME PIPES: the case
		// the drain's grandchild rule exists for. Without that rule EOF never
		// arrives and the exec never reports a status.
		child := exec.Command(os.Args[0], "-test.run=^TestExecSubprocessTarget$")
		child.Env = append(os.Environ(), "RAINIER_EXEC_TEST_TARGET=hold")
		child.Stdout, child.Stderr = os.Stdout, os.Stderr
		if err := child.Start(); err != nil {
			os.Exit(1)
		}
		fmt.Fprint(os.Stdout, "parent")
		os.Exit(3)
	case "hold":
		// Holds the inherited pipes and says nothing. Bounded so a failed run
		// cannot leave one behind.
		time.Sleep(30 * time.Second)
		os.Exit(0)
	case "winsize":
		ws, err := unix.IoctlGetWinsize(int(os.Stdin.Fd()), unix.TIOCGWINSZ)
		if err != nil {
			fmt.Fprintf(os.Stderr, "no winsize: %v", err)
			os.Exit(1)
		}
		fmt.Printf("size %dx%d\n", ws.Col, ws.Row)
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGWINCH)
		<-ch
		ws, _ = unix.IoctlGetWinsize(int(os.Stdin.Fd()), unix.TIOCGWINSZ)
		fmt.Printf("size %dx%d\n", ws.Col, ws.Row)
		os.Exit(0)
	case "tty-streams":
		fmt.Fprint(os.Stdout, "OUT")
		fmt.Fprint(os.Stderr, "ERR")
		os.Exit(0)
	}
	os.Exit(99)
}

// targetSpec is the command that runs this test binary as the exec'd process.
// The -test.run argument is what keeps the child from re-running the whole
// suite — the same shape TestGitHubCLIExecSubprocessTarget is invoked with.
func targetSpec(args ...string) runner.ExecSpec {
	return runner.ExecSpec{Argv: append(
		[]string{"target", "-test.run=^TestExecSubprocessTarget$"}, args...)}
}

// realRunner builds a runner over the real spawner, with this test binary on
// the session's PATH under the name `target` and a recorder around the kill.
func realRunner(t *testing.T, mode string, extraEnv ...string) (*execRunner, *killRecorder, string) {
	t.Helper()
	return realRunnerWithGrace(t, mode, 0, extraEnv...)
}

func realRunnerWithGrace(t *testing.T, mode string, grace time.Duration,
	extraEnv ...string) (*execRunner, *killRecorder, string) {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	binDir := t.TempDir()
	target := filepath.Join(binDir, "target")
	if err := os.Symlink(self, target); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	root := t.TempDir()

	rec := &killRecorder{}
	spawner := execSpawner{kill: rec.kill, drainGrace: grace}
	env := append([]string{
		"PATH=" + binDir,
		"RAINIER_EXEC_TEST_TARGET=" + mode,
		// The test binary must not re-run the whole suite in the child.
		"GOTRACEBACK=none",
	}, extraEnv...)
	return newExecRunner(root, env, spawner.start), rec, root
}

// withCwd is targetSpec with a working directory, spelled as a helper so the
// spec construction stays one line at each call site.
func withCwd(s runner.ExecSpec, cwd string) runner.ExecSpec {
	s.Cwd = cwd
	return s
}

// killRecorder forwards every signal and remembers what it was asked to do.
// The minus sign is the whole point of the group rule, and a rule nothing
// checks is a rule that gets deleted.
type killRecorder struct {
	mu   sync.Mutex
	sent [][2]int
}

func (k *killRecorder) kill(pid int, sig syscall.Signal) error {
	k.mu.Lock()
	k.sent = append(k.sent, [2]int{pid, int(sig)})
	k.mu.Unlock()
	return syscall.Kill(pid, sig)
}

func (k *killRecorder) calls() [][2]int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([][2]int(nil), k.sent...)
}

// collectExec drains an attachment into its three facts.
type execResult struct {
	stdout, stderr string
	exitCode       int
	signal         string
	reason         string
	pid            int
	types          []string
}

func collectExec(t *testing.T, a relay.ExecAttachment) execResult {
	t.Helper()
	var r execResult
	for _, m := range drain(t, a) {
		r.types = append(r.types, m.Type)
		switch m.Type {
		case terminal.TypeExecStdout:
			r.stdout += string(m.Data)
		case terminal.TypeExecStderr:
			r.stderr += string(m.Data)
		case terminal.TypeExecExit:
			r.exitCode, r.signal = m.ExitCode, m.Signal
		case terminal.TypeExecError:
			r.reason = m.Reason
		case terminal.TypeExecStarted:
			r.pid = m.PID
		}
	}
	return r
}

// ---------------------------------------------------------------------------
// a real process
// ---------------------------------------------------------------------------

// TestRealExecStreamsAndExits is the whole happy path through a real
// fork/exec: separate streams, the command's own status, and no shell
// anywhere — argv[0] is exec'd directly.
func TestRealExecStreamsAndExits(t *testing.T) {
	r, _, _ := realRunner(t, "streams")
	got := collectExec(t, r.OpenExec(targetSpec()))
	if got.stdout != "to stdout" || got.stderr != "to stderr" {
		t.Fatalf("streams = stdout %q stderr %q", got.stdout, got.stderr)
	}
	if got.exitCode != 7 || got.signal != "" {
		t.Fatalf("exit = %d signal %q, want 7", got.exitCode, got.signal)
	}
}

// TestRealExecReportsASignal is the 128+N half: a process a signal killed has
// no exit status, and saying it exited 0 would tell a script it succeeded.
func TestRealExecReportsASignal(t *testing.T) {
	r, _, _ := realRunner(t, "signal-self")
	got := collectExec(t, r.OpenExec(targetSpec()))
	if got.signal != "terminated" {
		t.Fatalf("a self-signalled process reported code %d signal %q",
			got.exitCode, got.signal)
	}
}

// TestRealExecRunsInTheRequestedCwd, resolved inside the workspace.
func TestRealExecRunsInTheRequestedCwd(t *testing.T) {
	r, _, root := realRunner(t, "cwd")
	if err := os.Mkdir(filepath.Join(root, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	got := collectExec(t, r.OpenExec(withCwd(targetSpec(), "sub")))
	if got.stdout != filepath.Join(root, "sub") {
		t.Fatalf("cwd = %q, want %q", got.stdout, filepath.Join(root, "sub"))
	}

	// And with no --cwd it is the workspace root.
	got = collectExec(t, r.OpenExec(targetSpec()))
	if got.stdout != root {
		t.Fatalf("default cwd = %q, want the workspace root %q", got.stdout, root)
	}
}

// TestRealExecCarriesTheCallersEnv through to the real process.
func TestRealExecCarriesTheCallersEnv(t *testing.T) {
	r, _, _ := realRunner(t, "env")
	spec := targetSpec()
	spec.Env = map[string]string{"RAINIER_EXEC_TEST_REPORT": "ignored"}
	got := collectExec(t, r.OpenExec(spec))
	// RAINIER_* is reserved, so that spec is refused before anything spawns —
	// which is the rule, stated at the one door that can enforce it.
	if got.reason != terminal.ReasonEnvRefused {
		t.Fatalf("a RAINIER_ name was not refused: %+v", got)
	}
}

// TestRealExecStdinEOFTerminatesTheCommand is the acceptance case from
// amendment 3, with a real process and a real pipe: `exec s -- cat > f` with
// piped input terminates, and f holds the bytes.
func TestRealExecStdinEOFTerminatesTheCommand(t *testing.T) {
	r, _, root := realRunner(t, "cat")
	out := filepath.Join(root, "out.txt")
	r.env = append(r.env, "RAINIER_EXEC_TEST_REPORT="+out)

	a := r.OpenExec(targetSpec())
	go func() {
		a.Client(terminal.ClientMessage{Type: "stdin", Data: []byte("first ")})
		a.Client(terminal.ClientMessage{Type: "stdin", Data: []byte("second")})
		a.Client(terminal.ClientMessage{Type: terminal.TypeExecStdinEOF})
	}()
	got := collectExec(t, a)
	if got.exitCode != 0 || got.signal != "" {
		t.Fatalf("cat exited %d signal %q", got.exitCode, got.signal)
	}
	body, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "first second" {
		t.Fatalf("the file holds %q, want %q", body, "first second")
	}
}

// TestRealExecKillsTheProcessGroup is the reason execProcess.Signal negates
// the pid. A child spawns grandchildren that inherit its pipes, and
// signalling the leader alone leaves them holding the far end.
func TestRealExecKillsTheProcessGroup(t *testing.T) {
	r, rec, _ := realRunner(t, "sleep")
	a := r.OpenExec(targetSpec())

	// Wait until the child is genuinely running before pulling the caller
	// away, so the signal has something to reach.
	if m := <-a.Msgs(); m.Type != terminal.TypeExecStarted {
		t.Fatalf("first message = %q", m.Type)
	}
	waitForExec(t, func() bool {
		select {
		case m, ok := <-a.Msgs():
			return ok && m.Type == terminal.TypeExecStdout && string(m.Data) == "up"
		default:
			return false
		}
	}, "the child never printed")

	a.Close()
	drain(t, a)

	calls := rec.calls()
	if len(calls) == 0 {
		t.Fatal("a caller disconnect signalled nothing")
	}
	first := calls[0]
	if first[0] >= 0 {
		t.Fatalf("the signal went to pid %d, not to the process GROUP (-pid)", first[0])
	}
	if syscall.Signal(first[1]) != syscall.SIGTERM {
		t.Fatalf("the first signal was %v, want SIGTERM", syscall.Signal(first[1]))
	}
}

// waitForExec polls until cond or fails, so a test never hangs the suite. It
// is the exec lane's own, with a longer budget than main_test.go's: these
// tests wait on real processes rather than on a channel this process feeds.
func waitForExec(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestRealDetachedExecOutlivesItsCaller and writes to the log the caller
// named — the motivating case, `rainier exec s -- claude --continue`, in the
// smallest form that can be checked.
func TestRealDetachedExecOutlivesItsCaller(t *testing.T) {
	r, rec, root := realRunner(t, "sleep")
	detached := targetSpec()
	detached.Detach, detached.LogPath = true, "run.log"
	a := r.OpenExec(detached)
	got := collectExec(t, a)
	if len(got.types) != 1 || got.types[0] != terminal.TypeExecStarted || got.pid <= 0 {
		t.Fatalf("a detached exec answered %+v", got)
	}

	// It is running, its output is in the file, and the caller going away
	// does not touch it.
	waitForExec(t, func() bool {
		body, err := os.ReadFile(filepath.Join(root, "run.log"))
		return err == nil && strings.Contains(string(body), "up")
	}, "the detached process never wrote to its log")
	a.Close()
	time.Sleep(100 * time.Millisecond)
	if len(rec.calls()) != 0 {
		t.Fatalf("a detached exec was signalled by its caller going away: %v", rec.calls())
	}
	if err := syscall.Kill(got.pid, 0); err != nil {
		t.Fatalf("the detached process is gone: %v", err)
	}

	// The session ending is what ends it.
	r.KillAll()
	waitForExec(t, func() bool { return syscall.Kill(got.pid, 0) != nil },
		"the session's shutdown did not reap the detached process")
	for _, c := range rec.calls() {
		if c[0] >= 0 {
			t.Fatalf("the session's kill went to pid %d, not to the group", c[0])
		}
	}
}

// ---------------------------------------------------------------------------
// a real pty
// ---------------------------------------------------------------------------

// TestRealExecPTYCarriesTheSizeAndResizes: the caller's opening size reaches
// the child's terminal, and a later resize moves THAT pty — never the
// session's, which is the property the whole design rests on.
func TestRealExecPTYCarriesTheSizeAndResizes(t *testing.T) {
	r, _, _ := realRunner(t, "winsize")
	ttySpec := targetSpec()
	ttySpec.TTY, ttySpec.Cols, ttySpec.Rows = true, 100, 40
	a := r.OpenExec(ttySpec)

	if m := <-a.Msgs(); m.Type != terminal.TypeExecStarted {
		t.Fatalf("first message = %q", m.Type)
	}
	// Resize once the child has had a chance to install its handler; the
	// child prints its size again when it arrives.
	go func() {
		time.Sleep(200 * time.Millisecond)
		for i := 0; i < 40; i++ {
			a.Client(terminal.ClientMessage{Type: "resize", Cols: 120, Rows: 50})
			time.Sleep(50 * time.Millisecond)
		}
	}()

	got := collectExec(t, a)
	if !strings.Contains(got.stdout, "size 100x40") {
		t.Fatalf("the opening size never reached the pty: %q", got.stdout)
	}
	if !strings.Contains(got.stdout, "size 120x50") {
		t.Fatalf("the resize never reached the pty: %q", got.stdout)
	}
}

// TestRealExecPTYMergesTheStreams: a pty has ONE stream, which is why --tty
// is a flag rather than the default and why the CLI says so in help.
func TestRealExecPTYMergesTheStreams(t *testing.T) {
	r, _, _ := realRunner(t, "tty-streams")
	ttySpec := targetSpec()
	ttySpec.TTY, ttySpec.Cols, ttySpec.Rows = true, 80, 24
	got := collectExec(t, r.OpenExec(ttySpec))
	if got.stderr != "" {
		t.Fatalf("a pty exec produced a separate stderr: %q", got.stderr)
	}
	if !strings.Contains(got.stdout, "OUT") || !strings.Contains(got.stdout, "ERR") {
		t.Fatalf("merged output = %q, want both OUT and ERR", got.stdout)
	}
}

// TestRealExecAcceptsALargeStream is the flood, at the sandbox end: every
// byte arrives and none is dropped, which is the deliberate opposite of the
// terminal's force-detach.
func TestRealExecAcceptsALargeStream(t *testing.T) {
	if testing.Short() {
		t.Skip("the flood is a long test")
	}
	r, _, root := realRunner(t, "")
	// A generator this test wrote, so the suite does not depend on `yes`.
	gen := filepath.Join(root, "gen")
	if err := os.WriteFile(gen, []byte(
		"#!/bin/sh\nexec dd if=/dev/zero bs=65536 count=320 2>/dev/null\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	r.env = append(r.env, "PATH="+root+":/usr/bin:/bin")

	a := r.OpenExec(runner.ExecSpec{Argv: []string{"gen"}})
	var n int
	var exitSeen bool
	for m := range a.Msgs() {
		switch m.Type {
		case terminal.TypeExecStdout:
			n += len(m.Data)
			// Slow enough that the queue is full for most of the run, which
			// is what makes this a backpressure test rather than a copy test.
			time.Sleep(time.Millisecond)
		case terminal.TypeExecExit:
			exitSeen = true
			if m.ExitCode != 0 {
				t.Fatalf("the generator exited %d", m.ExitCode)
			}
		case terminal.TypeExecError:
			t.Skipf("this machine has no /bin/sh or dd: %s", m.Reason)
		}
	}
	if !exitSeen {
		t.Fatal("no exit status arrived")
	}
	if n != 320*65536 {
		t.Fatalf("received %d bytes, want %d — the flood lost output", n, 320*65536)
	}
}

// TestRealExecSurvivesAGrandchildHoldingThePipes pins the drain's one rule.
// A child that spawns a grandchild and exits leaves the pipes held by a
// process nobody is waiting for, so EOF never comes — and an exec that waited
// for it would never report a status, which is a script hanging forever.
//
// The clock only runs while a reader is parked with nothing arriving AND the
// child has already exited, which is why a caller on a slow link is never cut
// off; TestRealExecAcceptsALargeStream is the other side of that claim.
func TestRealExecSurvivesAGrandchildHoldingThePipes(t *testing.T) {
	r, _, _ := realRunnerWithGrace(t, "grandchild", 150*time.Millisecond)
	got := collectExec(t, r.OpenExec(targetSpec()))
	if got.exitCode != 3 || got.signal != "" {
		t.Fatalf("exit = %d signal %q, want the parent's own 3", got.exitCode, got.signal)
	}
	if !strings.Contains(got.stdout, "parent") {
		t.Fatalf("the parent's output was lost: %q", got.stdout)
	}
}

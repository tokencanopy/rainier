// Package sandboxexec is the sandbox end of `rainier exec`: one command,
// spawned BESIDE the session and never inside it, streamed back to the one
// caller that asked for it.
//
// It is its own package rather than another file in cmd/sessiond for one
// reason that matters and one that is merely tidy. The one that matters is
// that the end-to-end suite drives the WHOLE path — the CLI's loop, the
// route, the plane's handshake, the runner's forwarding, the relay's framing,
// and a real fork/exec at the far end — and it can only do that against the
// real implementation; a package main is reachable from nothing. The tidy one
// is that "beside the session, never inside it" is easier to keep true when
// the code that must not touch *session.Session cannot see it.
//
// "Beside, never inside" is the load-bearing sentence. An exec's output must
// never reach the emulator, the event log, or another viewer — `rainier exec
// s -- cat huge.log` would otherwise scribble a megabyte into the scrollback
// of a session somebody is watching, and `rainier attach --since 0` would
// replay it forever. Nothing in this package imports internal/session, and
// the relay hands an exec id to neither Attach nor Bind nor Stdin.
//
// The second sentence is that an exec is not the terminal. It does not take
// the controller lease, does not advance the controller generation, is not
// displaced by a take-over and cannot displace anybody; its frames carry no
// generation and nothing here reads one off them.
//
// SECRET HYGIENE: an ExecSpec's Env values are secrets as often as not. They
// are never logged, never quoted in an error, and an env refusal names the
// VARIABLE'S NAME and nothing else. Neither argv beyond its first element,
// nor the cwd, nor a byte or a length of input or output is logged here.
package sandboxexec

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
	"golang.org/x/sys/unix"

	"github.com/tokencanopy/rainier/internal/reap"
	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/terminal"
	"github.com/tokencanopy/rainier/protocol/workspace"
)

const (
	// MaxConcurrent bounds how many execs one session may have at once.
	// Eight is slack rather than a working limit: it exists so a loop in a
	// script cannot fork-bomb a sandbox through a socket. Detached execs
	// count too, because they are processes in the same container.
	MaxConcurrent = 8
	// MaxDetached bounds how many of those may be DETACHED. It is lower than
	// MaxConcurrent on purpose: a detached process holds its slot for as long
	// as it runs, and the only way to stop one is `rainier exec s -- kill
	// <pid>`, which needs a slot of its own. See Runner.reserve.
	MaxDetached = 4
	// killGrace is how long a killed exec's process GROUP has between
	// SIGTERM and SIGKILL. The group matters for the reason files.go already
	// documents about `git fetch`: the child spawns grandchildren that
	// inherit the pipes, and signalling the leader alone leaves them holding
	// the far end.
	killGrace = 5 * time.Second
	// outQueue is how many output messages may be in flight between the
	// pipe readers and the relay's forwarder. It is small on purpose: the
	// queue is not a buffer, it is the slack that keeps a chunk-sized read
	// from having to wait for the previous chunk's socket write, and every
	// byte past it is held in the kernel's pipe and then in the process's
	// own blocked write(2), which is where backpressure belongs.
	outQueue = 8
	// readChunk is one read from a pipe or a pty.
	readChunk = 32 << 10
	// stdinQueueBytes bounds the caller's stdin waiting for a child that is
	// not reading it. Nothing on the delivery path BLOCKS: Client runs on the
	// relay demux, which is shared with every other attachment on this conn —
	// the session's own terminal and the session RPC included — so a wait
	// there freezes a person's keyboard, and a wait long enough to matter
	// also keeps the demux from ever reading the FrameClose that would have
	// ended the exec.
	//
	// So the queue is generous and the overflow is decisive. A caller feeding
	// a command that reads its input normally never approaches it; one
	// feeding a command that reads NONE of it is refused rather than
	// buffered, by ending the exec — which its caller sees as the connection
	// ending without an exit status, exit 125. Dropping the bytes instead
	// would be a silently corrupted stdin, which is worse than either.
	//
	// The bound is what it is because the producer is a network: stdin
	// reaches this sandbox at the speed of the caller's link, and the queue
	// only grows while the child is slower than that. Eight megabytes is
	// several seconds of a fast link against a child that has stopped reading
	// altogether, and nothing at all against one that is merely slower than
	// the network for a moment. The cost is bounded per session: at most
	// MaxConcurrent execs, each holding at most this many bytes.
	//
	// Real flow control — the sandbox telling the plane to stop reading the
	// caller's socket — is what would remove the bound entirely, and it is a
	// protocol addition rather than a constant. It is recorded as an open
	// question in the design rather than smuggled in here.
	stdinQueueBytes = 8 << 20
	stdinQueueDepth = 8192
	// drainGrace is how long the readers get, AFTER the child has exited,
	// to reach EOF on pipes a grandchild may still be holding open. The clock
	// only runs while a reader is parked in Read with nothing arriving: a
	// reader that is blocked handing a chunk to a slow caller is making
	// progress and is never cut off, which is what keeps this from truncating
	// the output of a caller on a slow link.
	drainGrace = 10 * time.Second
)

// ---------------------------------------------------------------------------
// the seams
// ---------------------------------------------------------------------------

// Status is one exec'd process's outcome: the code it exited with, or the
// name of the signal that killed it. Exactly one is meaningful and Signal
// says which — a command may legitimately exit 137, so a reserved code could
// not tell the two apart.
type Status struct {
	Code   int
	Signal string
}

// Proc is one running exec, as the runner sees it. It is the exec-shaped
// twin of session.Proc, and it exists for the same reason: the whole runner
// is testable against a scripted fake, with no pty, no container and no real
// process.
type Proc interface {
	// Write feeds the child's stdin.
	Write(p []byte) (int, error)
	// CloseStdin closes the child's stdin, which is what an `exec_stdin_eof`
	// asks for and what makes `cat` with a pipe on the other end terminate.
	CloseStdin() error
	// Resize resizes this exec's OWN pty, and nothing else. It never reaches
	// session.SetSize, so it cannot move the agent's terminal.
	Resize(cols, rows int) error
	// Signal delivers sig to the process GROUP.
	Signal(sig syscall.Signal) error
	// Wait blocks until the child has exited AND its output has been
	// delivered, and reports the outcome.
	Wait() Status
	// Pid is the child's process id, which a detached exec reports to its
	// caller as the only handle it will ever have on it.
	Pid() int
}

// Request is a VALIDATED spec: every path resolved inside the workspace,
// every environment name checked, argv[0] already found on the session's
// PATH. A starter runs it and asks no further questions.
type Request struct {
	Argv []string // as the caller typed it; Argv[0] is what the child sees as $0
	Path string   // the resolved executable
	Dir  string
	Env  []string
	TTY  bool
	Cols int
	Rows int
	// Detach and Log: the process is spawned in its own process group with
	// its output redirected to Log, and the attachment closes as soon as it
	// has reported the pid.
	Detach bool
	Log    *os.File
}

// Starter is the seam session.New already takes, in exec's shape: a
// function that spawns the request and calls back with output.
//
// onStdout and onStderr BLOCK, and their blocking is the backpressure: a
// starter must stop reading the process's pipes while a callback has not
// returned. They report an error when the consumer is gone, at which point
// the starter stops reading — the process is about to be killed anyway.
type Starter func(req Request, onStdout, onStderr func([]byte) error) (Proc, error)

// ---------------------------------------------------------------------------
// the runner
// ---------------------------------------------------------------------------

// Runner is this sandbox's exec runner: the session's environment, the
// workspace root every path is resolved against, and the live execs, so the
// session's own shutdown can end them.
type Runner struct {
	root  string
	env   []string
	start Starter
	max   int
	// maxDetached bounds the part of `max` that detached work may hold; see
	// reserve.
	maxDetached int
	grace       time.Duration

	mu   sync.Mutex
	live map[*attachment]struct{}
}

var _ relay.Execer = (*Runner)(nil)

// NewRunner builds the runner over the environment sessiond composed for
// the AGENT — not sessiond's own — which is what makes `claude --continue`
// and `git status` work and what argv[0] is looked up on.
func NewRunner(root string, env []string, start Starter) *Runner {
	return &Runner{root: root, env: env, start: start,
		max: MaxConcurrent, maxDetached: MaxDetached, grace: killGrace,
		live: map[*attachment]struct{}{}}
}

// OpenExec starts one exec and returns its live attachment. It never returns
// an error: every refusal is an `exec_error` the caller has to see and map to
// an exit code, so a refused exec is an attachment that emits its reason and
// closes.
func (r *Runner) OpenExec(spec runner.ExecSpec) relay.ExecAttachment {
	a := newAttachment(r)
	// On its OWN goroutine, and that is not an optimisation. OpenExec is
	// called inline from the relay's demux — the one goroutine that reads
	// every frame for every attachment on this session's conn, terminal
	// input and the session RPC included — so anything slow here freezes the
	// whole session. Opening a caller-named log file is a filesystem
	// operation on a path the caller chose, and `open(2)` on a FIFO with no
	// reader does not return at all.
	//
	// Nothing about the message ORDER depends on this being synchronous:
	// every message an exec will ever send goes through the same channel,
	// and `exec_started` is put on it before the output callbacks are armed.
	go r.open(a, spec)
	return a
}

func (r *Runner) open(a *attachment, spec runner.ExecSpec) {
	if !r.reserve(a, spec.Detach) {
		a.refuse(terminal.ReasonTooManyExecs)
		return
	}

	req, reason := r.validate(spec)
	if reason != "" {
		r.release(a)
		a.refuse(reason)
		return
	}

	// The callbacks are armed only once the spawn has succeeded, so
	// `exec_started` is genuinely the first message on the attachment however
	// promptly the child writes. A starter that fails never opens the gate,
	// and the reader goroutines it may already have started (it need not
	// have) find the attachment closing.
	ready := make(chan struct{})
	gate := func(send func([]byte) error) func([]byte) error {
		return func(b []byte) error {
			<-ready
			return send(b)
		}
	}
	proc, err := r.start(req, gate(a.sendStdout), gate(a.sendStderr))
	if err != nil {
		close(ready)
		r.release(a)
		if req.Log != nil {
			req.Log.Close()
		}
		a.refuse(startFailureReason(err))
		return
	}
	// The starter owns the log file's descriptor from here (it is the
	// child's stdout and stderr); this end has no further use for it.
	if req.Log != nil {
		req.Log.Close()
	}

	if !a.arm(proc, req.Detach) {
		// The session ended underneath this spawn. arm has signalled the
		// process; the slot comes back when it is gone, and the attachment
		// was already finished by the kill that beat us here.
		close(ready)
		go func() {
			proc.Wait()
			r.release(a)
		}()
		return
	}
	if req.Detach {
		// A detached exec reports its pid and closes: there is nothing to
		// stream, because its output is in the file the caller named, and
		// nothing to wait for, because its lifetime is the session's rather
		// than this attachment's. `rainier exec s -- kill <pid>` is how it is
		// stopped, which is why the pid is the one thing reported.
		a.emit(terminal.ServerMessage{Type: terminal.TypeExecStarted, PID: proc.Pid()})
		close(ready)
		a.finish()
		go func() {
			proc.Wait()
			r.release(a)
		}()
		return
	}

	a.emit(terminal.ServerMessage{Type: terminal.TypeExecStarted})
	close(ready)
	go a.run()
}

// KillAll ends every exec this session is running, detached ones included.
// It is wired to the session's own shutdown — the path that stops the agent
// when the session is suspended, stopped or destroyed — because that is the
// bound on a detached exec's life: it outlives its caller, never its session.
func (r *Runner) KillAll() {
	r.mu.Lock()
	all := make([]*attachment, 0, len(r.live))
	for a := range r.live {
		all = append(all, a)
	}
	r.mu.Unlock()
	for _, a := range all {
		a.kill()
	}
}

// reserve takes one of the session's exec slots, reporting false when they
// are all taken. The count is checked and taken in one locked step, so eight
// callers racing produce eight execs and not nine.
//
// A DETACHED exec takes a slot from a smaller pool as well, and that second
// bound is what keeps the feature usable rather than being belt and braces.
// A detached process holds its slot for as long as it runs, which can be
// hours; the design's only way to stop one is `rainier exec s -- kill <pid>`,
// which is itself an exec — so a session that filled all eight slots with
// detached work would have no way left to stop any of it, and no listing and
// no kill API to fall back on. Capping detached work below the total leaves
// room for the command that ends it.
func (r *Runner) reserve(a *attachment, detached bool) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.live) >= r.max {
		return false
	}
	if detached {
		n := 0
		for other := range r.live {
			if other.isDetached() {
				n++
			}
		}
		if n >= r.maxDetached {
			return false
		}
	}
	r.live[a] = struct{}{}
	a.setDetached(detached)
	return true
}

func (r *Runner) release(a *attachment) {
	r.mu.Lock()
	delete(r.live, a)
	r.mu.Unlock()
}

// LiveCount is how many execs this session is running right now, detached
// ones included. It is what the concurrency cap is measured against, and it
// is exported so a test one layer up can wait on "the caller hung up and the
// process is gone" without reaching into a process table.
func (r *Runner) LiveCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.live)
}

// startFailureReason maps a spawn failure onto the closed vocabulary. The
// validation above has already answered every question it can ask without
// calling execve, so what is left here is the race — the file was removed, or
// lost its exec bit, between the check and the syscall — plus everything else,
// which is reported as "could not be executed" because it is true and because
// a free-form string from inside a sandbox is a string somebody's terminal
// renders.
func startFailureReason(err error) string {
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return terminal.ReasonNotFound
	default:
		return terminal.ReasonNotExecutable
	}
}

// ---------------------------------------------------------------------------
// validation
// ---------------------------------------------------------------------------

// envNamePattern is the shape a caller-supplied environment variable's name must
// have. It is a rule about NAMES rather than a table of them, because a
// user's own build legitimately needs arbitrary ones.
var envNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// reservedEnv is the list of names a caller may not set, each of which turns
// "run this command" into "run something else" — a different request, and one
// that would arrive unaudited.
//
// WHAT THIS IS AND IS NOT. It is not a privilege boundary, and nothing here
// should be read as one: a caller who may exec at all may run `sh -c` and do
// whatever the session's user can do, which this design says plainly under
// Security. What the list protects is the AUDIT RECORD's meaning — "somebody
// ran git in this session" must not be the record of somebody running
// something else through git — and the caller's own expectation that
// `rainier exec s -- make` runs make.
var reservedEnv = map[string]bool{
	"HOME": true, "PATH": true, "SHELL": true, "IFS": true,
	// git's own "run something else" surface. GIT_CONFIG_COUNT/KEY/VALUE is
	// config injection on the command line, and through it core.sshCommand,
	// credential.helper and core.pager — which is every one of the others.
	"GIT_CONFIG_GLOBAL": true, "GIT_CONFIG_SYSTEM": true, "GIT_CONFIG": true,
	"GIT_CONFIG_COUNT": true, "GIT_SSH_COMMAND": true, "GIT_SSH": true,
	"GIT_ASKPASS": true, "GIT_EXTERNAL_DIFF": true, "GIT_PROXY_COMMAND": true,
	"GIT_EDITOR": true, "GIT_PAGER": true, "GIT_SEQUENCE_EDITOR": true,
	"GIT_DIR": true, "GIT_WORK_TREE": true, "GIT_ALTERNATE_OBJECT_DIRECTORIES": true,
	"SSH_ASKPASS": true, "EDITOR": true, "VISUAL": true, "PAGER": true,
	// Shell startup files and trace hooks: each runs code before the command.
	"BASH_ENV": true, "ENV": true, "SHELLOPTS": true, "BASHOPTS": true, "PS4": true,
	// Interpreter option channels, which are `--eval` by another name.
	"NODE_OPTIONS": true, "NODE_REPL_EXTERNAL_MODULE": true,
	"PYTHONSTARTUP": true, "PYTHONEXECUTABLE": true,
	"PERL5OPT": true, "PERL5DB": true, "RUBYOPT": true,
}

// The bounds on one caller-supplied variable. They are far above any real
// name or value, and they exist so that a spec cannot be a way to make this
// process allocate — or to push an execve over E2BIG, which would be reported
// as "not executable" rather than as the refusal it is.
const (
	maxEnvNameBytes  = 256
	maxEnvValueBytes = 128 << 10
)

// reservedEnvPrefixes are the namespaces no caller-supplied name may be in.
//
// They are PREFIXES where a prefix takes nothing legitimate away. Every LD_
// variable the dynamic loader reads exists to change which code runs, and
// LD_AUDIT is LD_PRELOAD by another name — a list would have to be kept in
// step with the loader forever. GIT_CONFIG_KEY_n / GIT_CONFIG_VALUE_n are
// numbered, so they cannot be listed at all. RAINIER_ is how the boot chain,
// the credential helper and the agent sync address each other.
//
// Everything else stays a list, because a prefix there would take something
// real away: a session's own build legitimately needs GIT_AUTHOR_NAME, and
// PYTHONPATH is how half of Python works.
var reservedEnvPrefixes = []string{"LD_", "RAINIER_", "GIT_CONFIG_KEY_", "GIT_CONFIG_VALUE_"}

// reservedEnvName reports whether a caller may not set this name.
func reservedEnvName(name string) bool {
	if reservedEnv[name] {
		return true
	}
	for _, prefix := range reservedEnvPrefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

// validate turns a wire spec into a request this sandbox will run, or names
// the reason it will not. It is the LAST hop before a syscall, so it trusts
// nothing the plane checked: the plane applies workspace.ValidatePath to
// refuse early, and this applies workspace.Resolve — symlink escape included
// — because the filesystem is the only thing that can actually answer.
func (r *Runner) validate(spec runner.ExecSpec) (Request, string) {
	if len(spec.Argv) == 0 || spec.Argv[0] == "" {
		return Request{}, terminal.ReasonNotFound
	}
	for _, arg := range spec.Argv {
		if strings.ContainsRune(arg, 0) {
			return Request{}, terminal.ReasonNotFound
		}
	}

	// Cwd. Default the workspace root; anything else is resolved inside it,
	// which is the existing push/pull rule applied at a new door. The agent
	// home is outside /workspace and is therefore not a legal cwd — a command
	// may still READ it, which is what makes `claude --continue` work,
	// because inheriting an environment is not the same as being able to cd
	// anywhere.
	dir := r.root
	if spec.Cwd != "" {
		resolved, err := workspace.Resolve(r.root, spec.Cwd)
		if err != nil {
			return Request{}, terminal.ReasonCwdRefused
		}
		info, err := os.Stat(resolved)
		if err != nil || !info.IsDir() {
			return Request{}, terminal.ReasonCwdRefused
		}
		dir = resolved
	}

	env, reason := r.composeEnv(spec)
	if reason != "" {
		return Request{}, reason
	}

	req := Request{
		Argv: spec.Argv, Dir: dir, Env: env,
		// The sizes are CLAMPED, because the pty ioctl takes a uint16 and a
		// plain conversion turns 65536 into a nought-column terminal — a size
		// no program draws on, reported as though the caller had asked for it.
		TTY: spec.TTY, Cols: ptySize(spec.Cols), Rows: ptySize(spec.Rows),
		Detach: spec.Detach,
	}
	if req.TTY && (req.Cols == 0 || req.Rows == 0) {
		req.Cols, req.Rows = ptyDefaultCols, ptyDefaultRows
	}

	// The log. Required with --detach and refused without it: a log path for
	// an attached exec would be a second, silent copy of a stream the caller
	// is already reading.
	switch {
	case spec.Detach && spec.LogPath == "":
		return Request{}, terminal.ReasonLogRefused
	case !spec.Detach && spec.LogPath != "":
		return Request{}, terminal.ReasonLogRefused
	case spec.Detach:
		f, err := openLog(r.root, spec.LogPath)
		if err != nil {
			return Request{}, terminal.ReasonLogRefused
		}
		req.Log = f
	}

	// argv[0] on the SESSION's PATH — the one composed above, never
	// sessiond's own. They are usually the same string, and the day they are
	// not is the day a session's environment adds a toolchain: `rainier exec
	// s -- make` has to find the make the agent would have found.
	path, err := lookPath(env, dir, spec.Argv[0])
	if err != nil {
		if req.Log != nil {
			req.Log.Close()
		}
		return Request{}, lookupReason(err)
	}
	req.Path = path
	return req, ""
}

// composeEnv builds the child's environment: the one sessiond composed for
// the agent, then TERM under --tty (matching session.StartProc), then the
// caller's own additions, each checked by name.
func (r *Runner) composeEnv(spec runner.ExecSpec) ([]string, string) {
	env := append([]string(nil), r.env...)
	if spec.TTY {
		env = append(env, "TERM=xterm-256color")
	}
	// Sorted, so a refusal is deterministic: two bad names in one spec must
	// always name the same one, or the same command reports different
	// reasons on different runs.
	for _, name := range sortedKeys(spec.Env) {
		value := spec.Env[name]
		switch {
		case !envNamePattern.MatchString(name),
			len(name) > maxEnvNameBytes,
			len(value) > maxEnvValueBytes,
			reservedEnvName(name),
			strings.ContainsRune(value, 0):
			// The NAME and nothing else. The value is never logged, never
			// audited and never quoted in an error.
			return nil, terminal.ReasonEnvRefused
		}
		env = append(env, name+"="+value)
	}
	return dedupEnv(env), ""
}

// ptySize clamps one dimension into what the pty ioctl can carry, and
// supplies a plain default for a client that named none.
//
// The clamp is not tidiness: the ioctl takes a uint16, so a plain conversion
// turns 65536 into a NOUGHT-column terminal — a size no program draws on,
// installed as though the caller had asked for it.
func ptySize(v int) int {
	switch {
	case v <= 0:
		return 0 // caller named none; ptyDefault fills both in together
	case v > 65535:
		return 65535
	default:
		return v
	}
}

// The size a --tty exec opens with when its client named none. A terminal has
// to be SOME size, and 80x24 is the one every program has a fallback for.
const (
	ptyDefaultCols = 80
	ptyDefaultRows = 24
)

// sortedKeys is the map's keys in order, so every decision made over a spec's
// env is deterministic.
func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	// A tiny insertion sort rather than a slices.Sort import: the map is at
	// most a handful of names.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// dedupEnv keeps the LAST assignment of each name, which is what makes
// "append the caller's additions" mean "override". Order is otherwise
// preserved so a reader of the environment sees it in the order it was built.
func dedupEnv(env []string) []string {
	last := map[string]int{}
	for i, kv := range env {
		if eq := strings.IndexByte(kv, '='); eq > 0 {
			last[kv[:eq]] = i
		}
	}
	out := env[:0:0]
	for i, kv := range env {
		eq := strings.IndexByte(kv, '=')
		if eq > 0 && last[kv[:eq]] != i {
			continue
		}
		out = append(out, kv)
	}
	return out
}

// openLog resolves and opens a detached exec's output file.
//
// O_NONBLOCK and the regular-file check are not belt and braces. The path is
// the CALLER's, and a caller who can run one command can create anything on
// it: `open(2)` O_WRONLY on a FIFO with no reader never returns — and this
// used to run on the relay's shared demux, where that froze the whole session
// — and a character device is a place output goes to be lost. A log has to be
// a file somebody can read afterwards, which is the point of the flag.
//
// The path itself is resolved exactly as a cwd is — inside the workspace, symlink escape
// included — because a detached process writing outside the tree its session
// owns is the same escape by a slower route.
func openLog(root, path string) (*os.File, error) {
	resolved, err := workspace.Resolve(root, path)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(resolved,
		os.O_CREATE|os.O_WRONLY|os.O_APPEND|syscall.O_NONBLOCK, 0o644)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		f.Close()
		if err == nil {
			err = errors.New("exec: the log path is not a regular file")
		}
		return nil, err
	}
	// O_NONBLOCK means nothing on a regular file, so the descriptor handed to
	// the child behaves exactly as one opened without it.
	return f, nil
}

// The two lookup failures, kept apart because a caller's script acts on the
// difference: 127 is "you typed a name this sandbox does not have", 126 is
// "it is there and it will not run".
var (
	errNotFound      = errors.New("exec: not found on the session PATH")
	errNotExecutable = errors.New("exec: not executable")
)

func lookupReason(err error) string {
	if errors.Is(err, errNotExecutable) {
		return terminal.ReasonNotExecutable
	}
	return terminal.ReasonNotFound
}

// lookPath resolves argv[0] against the SESSION's PATH — the env composed
// above — rather than against this process's own. os/exec's LookPath reads
// the ambient PATH, which is sessiond's, and the two are the same string only
// by coincidence.
//
// A name containing a slash is a path and is never searched for, exactly as a
// shell treats it; a relative one is resolved against the exec's cwd, which is
// where the child will be when it runs.
//
// The search mirrors a shell's: a candidate that exists but is not executable
// does not end the search, it is remembered, so that a stray non-executable
// `make` early on the path does not shadow the real one — and if nothing
// executable is found, the remembered one is what turns the answer from "not
// found" into "not executable".
func lookPath(env []string, dir, name string) (string, error) {
	if strings.ContainsRune(name, os.PathSeparator) {
		candidate := name
		if !filepath.IsAbs(candidate) {
			candidate = filepath.Join(dir, candidate)
		}
		return candidate, candidateOK(candidate)
	}
	sawFile := false
	for _, elem := range filepath.SplitList(envValue(env, "PATH")) {
		if elem == "" {
			// POSIX: an empty element means the current directory — and the
			// current directory that matters is the CHILD's, which is this
			// exec's cwd. Resolving it against sessiond's own would stat one
			// file and execute another.
			elem = dir
		}
		candidate := filepath.Join(elem, name)
		err := candidateOK(candidate)
		switch {
		case err == nil:
			return candidate, nil
		case errors.Is(err, errNotExecutable):
			sawFile = true
		}
	}
	if sawFile {
		return "", errNotExecutable
	}
	return "", errNotFound
}

// candidateOK reports whether one candidate is a runnable file. The
// permission test is the mode bits rather than a real access(2) check
// because every process in this sandbox — sessiond, the agent, and every exec
// — is the same uid, so "somebody may execute it" and "we may execute it" are
// the same question.
func candidateOK(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return errNotFound
	}
	if info.IsDir() {
		return errNotExecutable
	}
	if info.Mode().Perm()&0o111 == 0 {
		return errNotExecutable
	}
	return nil
}

// envValue reads one variable out of a composed environment, last assignment
// winning — the same rule dedupEnv applies and the same one execve does.
func envValue(env []string, name string) string {
	value := ""
	prefix := name + "="
	for _, kv := range env {
		if strings.HasPrefix(kv, prefix) {
			value = kv[len(prefix):]
		}
	}
	return value
}

// SessionEnv is the environment an exec's child starts from: sessiond's own —
// which is the container's, and therefore the agent's — plus the boot chain's
// exports, which is where GIT_CONFIG_GLOBAL and the no-prompting rules live.
// Without them `git status` in an exec would find no credential helper and no
// identity, and `claude --continue` would find no agent home.
//
// Both halves are already-formatted KEY=VALUE strings, so this package never
// learns the shape sessiond happens to keep its boot-chain variables in. The
// LAST assignment of a name wins, which is what makes "append the exports"
// mean "override" and is what execve would have done anyway.
func SessionEnv(base, extra []string) []string {
	return dedupEnv(append(append([]string(nil), base...), extra...))
}

// ---------------------------------------------------------------------------
// one attachment
// ---------------------------------------------------------------------------

// attachment is one live exec as the relay sees it. Msgs carries every
// server message this exec will ever send and is closed when it is over;
// sends on it block, which is the backpressure.
type attachment struct {
	runner *Runner
	msgs   chan terminal.ServerMessage
	// closing is closed by kill() and by the process's own exit. Every
	// blocking operation on this attachment selects on it, which is what
	// keeps a dead consumer from parking a reader goroutine forever.
	closing   chan struct{}
	closeOnce sync.Once

	// out guards the OUTBOX against the one race that can crash a sandbox:
	// closing msgs while a sender is mid-send is a "send on closed channel"
	// panic, and sessiond has no recover around the relay demux — a panic
	// there takes the whole session down, and takes it down BEFORE the
	// shutdown path flushes the agent's last write.
	//
	// A sender takes it for reading, so senders do not serialise against
	// each other; finish takes it for writing, which waits for every
	// in-flight send to return. That cannot deadlock, because finish closes
	// `closing` FIRST and every send selects on it.
	out       sync.RWMutex
	outClosed bool

	// stdin is the caller's input on its way to the child, on its own
	// goroutine so that the relay's demux never waits on a pipe.
	stdin chan stdinPiece

	mu       sync.Mutex
	proc     Proc
	detached bool
	queued   int // bytes of stdin waiting
}

func (a *attachment) setDetached(v bool) {
	a.mu.Lock()
	a.detached = v
	a.mu.Unlock()
}

func (a *attachment) isDetached() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.detached
}

// stdinPiece is one piece of the caller's input: bytes, or the end of it.
type stdinPiece struct {
	data []byte
	eof  bool
}

func newAttachment(r *Runner) *attachment {
	return &attachment{
		runner:  r,
		msgs:    make(chan terminal.ServerMessage, outQueue),
		closing: make(chan struct{}),
		stdin:   make(chan stdinPiece, stdinQueueDepth),
	}
}

func (a *attachment) Msgs() <-chan terminal.ServerMessage { return a.msgs }

// arm installs the process and starts the stdin pump. It is called once, and
// only after a successful spawn.
//
// It reports whether the attachment is still live. It can already be closing:
// the session's own shutdown can land between the slot being taken and the
// process existing, and a kill that arrived then found no process to signal.
// The spawn cannot be undone, so the process is ended here instead — which is
// what keeps "a detached exec dies with its session" true even for one
// started in the same instant the session was stopped.
func (a *attachment) arm(p Proc, detached bool) bool {
	a.mu.Lock()
	a.proc = p
	a.detached = detached
	a.mu.Unlock()
	select {
	case <-a.closing:
		go func() {
			a.killProc(p)
			a.finish()
		}()
		return false
	default:
	}
	if !detached {
		go a.pumpStdin()
	}
	return true
}

// refuse emits one exec_error and ends the attachment. The channel has room
// for it, so this never blocks — a refusal must not be able to wedge on a
// caller that has already gone.
func (a *attachment) refuse(reason string) {
	a.out.RLock()
	if !a.outClosed {
		select {
		case a.msgs <- terminal.ServerMessage{Type: terminal.TypeExecError, Reason: reason}:
		default:
		}
	}
	a.out.RUnlock()
	a.finish()
}

// emit queues one message, blocking until the relay's forwarder takes it or
// the attachment is closing.
//
// The outbox lock is what makes it safe for the session's own shutdown to end
// an exec at any instant: without it, a KillAll landing between the slot being
// taken and the process being armed would close msgs under a sender and panic
// the sandbox.
func (a *attachment) emit(m terminal.ServerMessage) error {
	a.out.RLock()
	defer a.out.RUnlock()
	if a.outClosed {
		return errAttachmentClosed
	}
	select {
	case a.msgs <- m:
		return nil
	case <-a.closing:
		return errAttachmentClosed
	}
}

var errAttachmentClosed = errors.New("exec: the attachment is closed")

func (a *attachment) sendStdout(b []byte) error {
	return a.emit(terminal.ServerMessage{Type: terminal.TypeExecStdout, Data: append([]byte(nil), b...)})
}

func (a *attachment) sendStderr(b []byte) error {
	return a.emit(terminal.ServerMessage{Type: terminal.TypeExecStderr, Data: append([]byte(nil), b...)})
}

// finish closes the message channel exactly once, which is what tells the
// relay's forwarder the exec is over and makes it send the FrameClose.
//
// `closing` is closed FIRST and the outbox lock is taken SECOND, in that
// order: every sender selects on `closing`, so the ones already inside the
// lock return instead of parking, and the ones not yet in it find outClosed.
func (a *attachment) finish() {
	a.closeOnce.Do(func() { close(a.closing) })
	a.out.Lock()
	defer a.out.Unlock()
	if a.outClosed {
		return
	}
	a.outClosed = true
	close(a.msgs)
}

// run waits out an attached exec and reports its outcome. The exit message is
// sent AFTER Wait, which does not return until the child has exited and every
// byte it wrote has been handed over — so a caller that receives an exec_exit
// has received the whole of the output that preceded it.
func (a *attachment) run() {
	st := a.proc.Wait()
	a.runner.release(a)
	msg := terminal.ServerMessage{Type: terminal.TypeExecExit}
	if st.Signal != "" {
		msg.Signal = st.Signal
	} else {
		msg.ExitCode = st.Code
	}
	_ = a.emit(msg)
	a.finish()
}

// Client delivers one message from the caller.
//
// stdin and its EOF are QUEUED, so a child that is not reading cannot park
// the relay's shared demux on a pipe write; resize and exec_signal are
// delivered straight through, deliberately ahead of any queued stdin, because
// a Ctrl-C must reach a child that has stopped reading its input — which is
// exactly the child a caller is most likely to be interrupting.
//
// Everything else — a claim, a release, a control — is dropped. There is
// nothing for an exec to claim: it holds no lease, its frames carry no
// generation, and honouring one would be letting the far end of a socket name
// its own authority at a pty this attachment has no business touching.
func (a *attachment) Client(m terminal.ClientMessage) {
	switch m.Type {
	case "stdin":
		a.offerStdin(stdinPiece{data: append([]byte(nil), m.Data...)})
	case terminal.TypeExecStdinEOF:
		a.offerStdin(stdinPiece{eof: true})
	case "resize":
		a.mu.Lock()
		p := a.proc
		a.mu.Unlock()
		if p != nil {
			// Without a pty there is nothing to size, and the starter's own
			// Resize is a no-op. It never reaches session.SetSize either way.
			_ = p.Resize(m.Cols, m.Rows)
		}
	case terminal.TypeExecSignal:
		sig, ok := signalNamed(m.Signal)
		if !ok {
			return
		}
		a.mu.Lock()
		p := a.proc
		a.mu.Unlock()
		if p != nil {
			_ = p.Signal(sig)
		}
	}
}

// signalNamed is the closed signal vocabulary. Nothing else is translated: a
// caller who wants to send a different signal runs `kill` in the sandbox,
// where it is audited like any other command.
func signalNamed(name string) (syscall.Signal, bool) {
	switch name {
	case terminal.SignalTERM:
		return syscall.SIGTERM, true
	case terminal.SignalINT:
		return syscall.SIGINT, true
	default:
		return 0, false
	}
}

// offerStdin hands one piece of input to the pump WITHOUT EVER BLOCKING. That
// is the property, not an optimisation: this call runs on the relay's shared
// demux. See stdinQueueBytes for what overflow means and why it ends the exec
// rather than dropping bytes.
func (a *attachment) offerStdin(in stdinPiece) {
	a.mu.Lock()
	over := a.queued > 0 && a.queued+len(in.data) > stdinQueueBytes
	if !over {
		a.queued += len(in.data)
	}
	a.mu.Unlock()
	if over {
		go a.endWith(terminal.ReasonStdinOverrun)
		return
	}
	select {
	case a.stdin <- in:
	case <-a.closing:
		a.mu.Lock()
		a.queued -= len(in.data)
		a.mu.Unlock()
	default:
		// The queue is under its byte bound and still full, which is what a
		// caller sending very many very small writes to a child that reads
		// none of them produces. Same answer, same reason.
		a.mu.Lock()
		a.queued -= len(in.data)
		a.mu.Unlock()
		go a.endWith(terminal.ReasonStdinOverrun)
	}
}

// endWith ends this exec and SAYS WHY first. It is the difference between a
// caller learning that its input could not be delivered and a caller being
// told "the connection ended before the command reported an exit status" for
// something Rainier did on purpose — which would be a misattribution, and the
// kind a script cannot act on.
func (a *attachment) endWith(reason string) {
	_ = a.emit(terminal.ServerMessage{Type: terminal.TypeExecError, Reason: reason})
	a.kill()
}

// pumpStdin is the one goroutine that writes the child's stdin. A write that
// fails ends the pump but not the exec: a child that closed its own stdin
// (`head -1`) is a perfectly ordinary thing for a caller to be piping into,
// and its exit status is still the answer.
func (a *attachment) pumpStdin() {
	for {
		select {
		case in := <-a.stdin:
			a.mu.Lock()
			a.queued -= len(in.data)
			p := a.proc
			a.mu.Unlock()
			if p == nil {
				return
			}
			if in.eof {
				_ = p.CloseStdin()
				return
			}
			if _, err := p.Write(in.data); err != nil {
				return
			}
		case <-a.closing:
			return
		}
	}
}

// Close ends an ATTACHED exec — the caller went away, so the process it owned
// goes with it. A detached exec is deliberately untouched: its lifetime is the
// session's, and a caller hanging up is the normal way a detached run begins.
func (a *attachment) Close() {
	a.mu.Lock()
	detached := a.detached
	a.mu.Unlock()
	if detached {
		return
	}
	a.kill()
}

// kill ends this exec whatever its kind: SIGTERM to the process GROUP, a
// grace period, then SIGKILL to the group. The group is the point — the child
// spawns grandchildren that inherit the pipes, and signalling the leader alone
// leaves them holding the far end.
//
// It is idempotent and safe to call from several places at once: the conn's
// death, an explicit close, a stalled stdin, and the session's own shutdown
// can all reach it.
func (a *attachment) kill() {
	a.mu.Lock()
	p := a.proc
	a.mu.Unlock()
	a.closeOnce.Do(func() { close(a.closing) })
	if p == nil {
		// Nothing has been spawned (yet). Ending the attachment is the whole
		// of what there is to do; a spawn still in flight finds `closing`
		// closed in arm and ends the process it just made.
		a.finish()
		return
	}
	// The attachment is finished when the process is GONE, whichever path
	// ended it — a caller disconnect, the session's own shutdown, a spawn
	// that raced it. finish is idempotent, so the ordinary exit path calling
	// it too is not a second closing, it is whichever got there first.
	go func() {
		a.killProc(p)
		a.finish()
	}()
}

// killProc is the signal half on its own: SIGTERM to the process GROUP, a
// grace period, then SIGKILL to the group.
func (a *attachment) killProc(p Proc) {
	_ = p.Signal(syscall.SIGTERM)
	done := make(chan struct{})
	go func() { p.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(a.runner.grace):
		_ = p.Signal(syscall.SIGKILL)
	}
}

// ---------------------------------------------------------------------------
// the real starter
// ---------------------------------------------------------------------------

// Spawner is the production Starter. kill is a field rather than a
// direct syscall.Kill so a test can watch what is signalled — the whole point
// of the group rule is the MINUS SIGN, and a rule nothing checks is a rule
// that gets deleted.
type Spawner struct {
	kill func(pid int, sig syscall.Signal) error
	// drainGrace overrides drainGrace, so the grandchild rule is
	// testable in milliseconds instead of in ten seconds. Zero is the
	// production value.
	drainGrace time.Duration
}

func NewSpawner() Spawner { return Spawner{kill: syscall.Kill} }

func (s Spawner) grace() time.Duration {
	if s.drainGrace > 0 {
		return s.drainGrace
	}
	return drainGrace
}

// start spawns one validated request. No shell: argv[0] is exec'd directly,
// so there is no glob expansion, no $VAR substitution and no `&&`. A caller
// who wants a shell names one, which is visible in what they typed and in
// what is audited.
//
// No user is chosen either, and that is the point: the container runs as
// uid 1000, sessiond runs as that user, and a plain fork/exec inherits it.
// There is no --user, no setuid and no path here that can produce a process
// running as anybody but the session's own user.
func (s Spawner) Start(req Request, onStdout, onStderr func([]byte) error) (Proc, error) {
	cmd := &exec.Cmd{Path: req.Path, Args: req.Argv, Dir: req.Dir, Env: req.Env}
	p := &process{cmd: cmd, kill: s.kill, done: make(chan struct{})}

	switch {
	case req.Detach:
		// Its own process group, its output in the file the caller named, and
		// no stdin at all: nothing is on the other end of a detached run.
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		cmd.Stdout = req.Log
		cmd.Stderr = req.Log
		if err := cmd.Start(); err != nil {
			return nil, err
		}
		go p.reap(nil)
		return p, nil

	case req.TTY:
		// A pty has one stream, so --tty merges stdout and stderr — which is
		// why it is a flag and not the default, and why the CLI says so in
		// help. pty.StartWithSize sets Setsid and Setctty, so the child is a
		// session leader and its process group is its own pid, which is what
		// the group signal below addresses.
		ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{Cols: uint16(req.Cols), Rows: uint16(req.Rows)})
		if err != nil {
			return nil, err
		}
		p.ptmx = ptmx
		drain := newDrain(p, 1, s.grace())
		go func() { drain.pump(ptmx, onStdout); drain.done() }()
		go func() { drain.wait(); drain.finish() }()
		go p.reap(drain)
		return p, nil

	default:
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		// os.Pipe rather than cmd.StdoutPipe, deliberately: cmd.Wait CLOSES
		// the pipes StdoutPipe hands out, and this end calls Wait to release
		// the Go-side resources of a child the reaper has already collected.
		// With the library's pipes that close races the readers and silently
		// truncates the tail of the output — which is the one thing an exec
		// may never do.
		stdinR, stdinW, err := os.Pipe()
		if err != nil {
			return nil, err
		}
		stdoutR, stdoutW, err := os.Pipe()
		if err != nil {
			stdinR.Close()
			stdinW.Close()
			return nil, err
		}
		stderrR, stderrW, err := os.Pipe()
		if err != nil {
			stdinR.Close()
			stdinW.Close()
			stdoutR.Close()
			stdoutW.Close()
			return nil, err
		}
		cmd.Stdin, cmd.Stdout, cmd.Stderr = stdinR, stdoutW, stderrW
		err = cmd.Start()
		// The child holds its own duplicates now; this end keeps only the
		// halves it uses. Keeping the write ends open here would mean EOF
		// never arriving on a child that exited.
		stdinR.Close()
		stdoutW.Close()
		stderrW.Close()
		if err != nil {
			stdinW.Close()
			stdoutR.Close()
			stderrR.Close()
			return nil, err
		}
		stdin, stdout, stderr := stdinW, stdoutR, stderrR
		p.stdin = stdin
		p.pipes = []io.Closer{stdout, stderr}
		drain := newDrain(p, 2, s.grace())
		go func() { drain.pump(stdout, onStdout); drain.done() }()
		go func() { drain.pump(stderr, onStderr); drain.done() }()
		go func() { drain.wait(); drain.finish() }()
		go p.reap(drain)
		return p, nil
	}
}

// process is one spawned exec.
type process struct {
	cmd   *exec.Cmd
	kill  func(pid int, sig syscall.Signal) error
	ptmx  *os.File
	stdin io.WriteCloser
	pipes []io.Closer

	// done is closed once the child has exited AND every byte it wrote has
	// been handed to the consumer (or the drain gave up on a grandchild
	// holding the pipes open). Wait blocks on it, so an exec_exit can never
	// overtake the output that preceded it.
	done   chan struct{}
	status Status

	// fdmu guards this process's descriptors against being CLOSED while
	// another goroutine is using one. It is a read/write lock rather than a
	// plain mutex because the users — a stdin write, a resize — are
	// concurrent with each other and only the close is exclusive.
	//
	// It is not belt and braces. pty.Setsize reaches for the descriptor
	// NUMBER (File.Fd), which steps outside the runtime's own reference
	// counting, so a resize racing teardown reads a descriptor the kernel may
	// already have handed to something else. A caller resizing their window
	// as a command finishes is an ordinary thing to do.
	fdmu   sync.RWMutex
	fdsOut bool
}

func (p *process) Pid() int { return p.cmd.Process.Pid }

func (p *process) Write(b []byte) (int, error) {
	p.fdmu.RLock()
	defer p.fdmu.RUnlock()
	if p.fdsOut {
		return 0, os.ErrClosed
	}
	if p.ptmx != nil {
		return p.ptmx.Write(b)
	}
	if p.stdin == nil {
		return 0, errors.New("exec: this process has no stdin")
	}
	return p.stdin.Write(b)
}

// CloseStdin closes the child's stdin. Under a pty there is no separate stdin
// to close — the pty is one bidirectional stream, and closing it would take
// the output with it — so the end of input is sent as the terminal's own EOT,
// which is what a terminal driver turns into an end-of-file for the reader.
func (p *process) CloseStdin() error {
	p.fdmu.RLock()
	defer p.fdmu.RUnlock()
	if p.fdsOut {
		return os.ErrClosed
	}
	if p.ptmx != nil {
		_, err := p.ptmx.Write([]byte{0x04})
		return err
	}
	if p.stdin == nil {
		return nil
	}
	return p.stdin.Close()
}

// Resize resizes THIS exec's pty and nothing else. Without one there is
// nothing to size, and saying so is the whole of the no-tty rule.
func (p *process) Resize(cols, rows int) error {
	p.fdmu.RLock()
	defer p.fdmu.RUnlock()
	if p.fdsOut || p.ptmx == nil {
		return nil
	}
	return pty.Setsize(p.ptmx, &pty.Winsize{Cols: uint16(cols), Rows: uint16(rows)})
}

// closeFDs releases every descriptor this end holds, once. Two callers reach
// it: the reap, when the exec is over, and the drain's watchdog, when a
// grandchild is holding the pipes open past the child's own death.
func (p *process) closeFDs() {
	p.fdmu.Lock()
	defer p.fdmu.Unlock()
	if p.fdsOut {
		return
	}
	p.fdsOut = true
	if p.ptmx != nil {
		p.ptmx.Close()
	}
	for _, c := range p.pipes {
		c.Close()
	}
	if p.stdin != nil {
		p.stdin.Close()
	}
}

// Signal delivers sig to the process GROUP — the negative pid — because the
// child's grandchildren inherit its pipes and signalling the leader alone
// leaves them holding the far end. The child is a group leader by
// construction (Setpgid, or Setsid under a pty), so its pid IS its group.
func (p *process) Signal(sig syscall.Signal) error {
	if p.cmd.Process == nil {
		return nil
	}
	return p.kill(-p.cmd.Process.Pid, sig)
}

func (p *process) Wait() Status {
	<-p.done
	return p.status
}

// reap waits for the child through the sandbox's single authoritative waiter
// and then for its output to be drained. cmd.Wait alone would race the reaper
// and report ECHILD instead of a status, which is the same reason
// session/proc.go goes through reap.
func (p *process) reap(drain *drain) {
	pid := p.cmd.Process.Pid
	if st, ok := reap.AwaitStatus(pid); ok {
		p.status = statusOf(st.Code, st.Signal)
		// The child is already reaped, so this returns ECHILD; it is called
		// only to release the Go-side resources.
		_ = p.cmd.Wait()
	} else {
		// No reaper (a host build, a test): cmd.Wait is the waiter.
		p.status = statusOfWait(p.cmd.Wait())
	}
	if drain != nil {
		// The child is gone; anything still holding the pipes is a
		// grandchild, and the drain's own clock decides how long to wait for
		// it. A reader that is merely blocked on a slow caller is never cut
		// off — see drain.
		drain.childExited()
		<-drain.finished
	}
	// Every descriptor this end still holds. The readers are finished (or
	// were cut off by the drain), so nothing is using one.
	p.closeFDs()
	close(p.done)
}

func statusOf(code int, sig syscall.Signal) Status {
	if sig != 0 {
		return Status{Signal: signalWireName(sig)}
	}
	return Status{Code: code}
}

// signalWireName is the SHORT name an exec_exit carries: "TERM", not
// "SIGTERM" and not the Go runtime's "terminated".
//
// The runtime's spelling is an implementation detail of this sandbox, and a
// CLI that had to recognise it would be coupled to the sandbox's language —
// which is the wrong way round, since the CLI is what turns the answer into
// the shell's 128+N. A signal this build's table does not name travels as its
// decimal number, so a caller still reports a correct 128+N rather than a
// wrong one.
func signalWireName(sig syscall.Signal) string {
	name := unix.SignalName(sig)
	if name == "" {
		return strconv.Itoa(int(sig))
	}
	return strings.TrimPrefix(name, "SIG")
}

// statusOfWait reads cmd.Wait's error, which is the non-Linux path (no
// reaper) and the one a unit test on a developer's machine takes.
func statusOfWait(err error) Status {
	if err == nil {
		return Status{}
	}
	var ee *exec.ExitError
	if !errors.As(err, &ee) {
		return Status{Code: -1}
	}
	if ws, ok := ee.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
		return Status{Signal: signalWireName(ws.Signal())}
	}
	return Status{Code: ee.ExitCode()}
}

// ---------------------------------------------------------------------------
// draining
// ---------------------------------------------------------------------------

// drain is the output half of a spawned exec: the reader goroutines, and
// the one rule that decides when to stop waiting for them.
//
// The rule exists for grandchildren. A child that spawns one and exits leaves
// the pipes held by a process nobody is waiting for, so EOF never comes and
// the exec would never report its status. The clock only runs while a reader
// is parked in Read with nothing arriving AND the child has already exited:
// a reader blocked handing a chunk to a slow caller is making progress and is
// never cut off, which is what keeps this from truncating the output of a
// caller on a slow link.
type drain struct {
	proc     *process
	grace    time.Duration
	wg       sync.WaitGroup
	finished chan struct{}
	exited   chan struct{}
	exitOnce sync.Once

	mu         sync.Mutex
	reads      uint64 // bytes read so far
	delivering int    // readers currently blocked handing a chunk over
}

// newDrain builds the drain for `readers` reader goroutines under grace.
func newDrain(p *process, readers int, grace time.Duration) *drain {
	d := &drain{proc: p, grace: grace,
		finished: make(chan struct{}), exited: make(chan struct{})}
	d.wg.Add(readers)
	return d
}

func (d *drain) done() { d.wg.Done() }
func (d *drain) wait() { d.wg.Wait() }

func (d *drain) childExited() { d.exitOnce.Do(func() { close(d.exited) }) }
func (d *drain) finish()      { close(d.finished) }

// pump reads r until EOF, handing every chunk to send. It starts the watchdog
// on the first read so a pipe nobody ever writes to is bounded too.
func (d *drain) pump(r io.Reader, send func([]byte) error) {
	go d.watch()
	buf := make([]byte, readChunk)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			d.mu.Lock()
			d.reads += uint64(n)
			d.delivering++
			d.mu.Unlock()
			sendErr := send(buf[:n])
			d.mu.Lock()
			d.delivering--
			d.mu.Unlock()
			if sendErr != nil {
				// The consumer is gone: stop reading. The process is about to
				// be killed by whoever closed the attachment.
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// watch closes the pipes if, after the child has exited, a whole grace period
// passes with no byte read and no reader mid-delivery. It is the grandchild
// rule and nothing else — see drain.
func (d *drain) watch() {
	select {
	case <-d.exited:
	case <-d.finished:
		return
	}
	last := uint64(0)
	for {
		d.mu.Lock()
		last = d.reads
		d.mu.Unlock()
		select {
		case <-time.After(d.grace):
		case <-d.finished:
			return
		}
		d.mu.Lock()
		stuck := d.reads == last && d.delivering == 0
		d.mu.Unlock()
		if stuck {
			// Break the hold: the readers are parked on a descriptor a
			// grandchild is keeping open, and closing it is what turns their
			// Read into an error so the exec can report the status it already
			// has.
			d.proc.closeFDs()
			return
		}
	}
}

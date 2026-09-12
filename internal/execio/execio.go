// Package execio is the client half of `rainier exec`: one command, inside a
// live session's sandbox, with the caller's terminal on this end.
//
// It is deliberately NOT attachio. The two loops look alike — a websocket,
// stdin in one direction, output in the other — and they answer different
// questions. attachio splices a person's terminal onto a session's shared
// pty: it has a detach key, a resume cursor, a reconnect, and an ownership
// negotiation, because the thing on the far end belongs to everybody watching
// it. An exec's far end is a process this caller created and owns: there is
// nothing to resume, nothing to reconnect to (the process dies with its
// caller), nobody to take control from, and the answer is an exit status
// rather than a screen. Sharing one loop would mean a flag for every one of
// those differences.
package execio

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"golang.org/x/term"

	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// readLimit matches every other hop of this path — the plane's, runnerd's and
// sessiond's — because the biggest frame either direction carries is bounded
// there and a smaller limit here would close a socket over a frame the rest of
// the fleet considers ordinary.
const readLimit = 16 << 20

// stdinChunk is one read from the caller's stdin. It is well under the read
// limit above: stdin is typed or piped, and a larger buffer would only delay
// the first byte reaching the command.
const stdinChunk = 32 << 10

// Options is one exec, as the caller typed it plus the streams it runs over.
type Options struct {
	// Spec is the command. It travels in the first message and never on the
	// URL: a URL is written to the access log of every proxy between here and
	// the cell, and an argv in a URL is an argument in a log file.
	Spec runner.ExecSpec
	// Stdin is the caller's input. nil means "no input at all", which is
	// reported to the sandbox as an immediate end-of-input so a command that
	// reads until EOF still terminates.
	Stdin *os.File
	// Stdout and Stderr receive the command's own bytes, byte for byte,
	// unbuffered, with nothing added and no trailing newline invented. With
	// --json both are the caller's STDERR, because a --json document is the
	// only thing that may be on stdout.
	Stdout io.Writer
	Stderr io.Writer
	// Header carries the caller's bearer.
	Header http.Header
}

// Result is everything the caller learns about one exec. Exactly one of
// ExitCode, Signal and Reason is meaningful, and Started says whether the
// command ever existed.
type Result struct {
	// Started reports that the sandbox answered `exec_started` — the
	// handshake. Without it the command never ran and Reason says why.
	Started bool
	// ExitCode is the command's own status, and Signal the name of the signal
	// that killed it. A command may legitimately exit 137, so the two are
	// separate facts rather than one integer with reserved values.
	ExitCode *int
	Signal   string
	// Reason is an exec_error's cause, from the closed vocabulary in
	// protocol/terminal.
	Reason string
	// PID is a detached exec's process id — the only handle its caller will
	// ever have on it, and why `rainier exec s -- kill <pid>` is the way one
	// is stopped.
	PID int
	// Detached reports that this exec was a --detach: it answered with a pid
	// and closed, and the process it started is still running.
	Detached bool
	// Interrupted reports that the caller pressed Ctrl-C twice and left,
	// which kills the command rather than waiting for it.
	Interrupted bool

	StartedAt time.Time
	ExitedAt  time.Time
	// Queued is the request-to-exec_started time — the dial-back and the
	// spawn — kept apart from Duration so a slow cell is not read as a slow
	// build.
	Queued   time.Duration
	Duration time.Duration
}

// DialError is a dial that got an HTTP response instead of an upgrade, so a
// caller can branch on the status: a 404 is a control plane older than this
// CLI, a 409 carries the session's state, and each has its own sentence.
type DialError struct {
	Status int
	// Code and Message are the server's error envelope, when it sent one.
	Code    string
	Message string
	// State is the session's state on a 409, which is the one fact that makes
	// "not running" actionable.
	State string
	err   error
}

func (e *DialError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("exec: %s (%d)", e.Message, e.Status)
	}
	return fmt.Sprintf("exec: the server answered %d", e.Status)
}

func (e *DialError) Unwrap() error { return e.err }

// Run dials wsURL, sends the opening exec_start, and pumps the command both
// ways until it exits, the caller interrupts it, or the connection ends.
//
// A Result with Started true and no ExitCode, no Signal and no Reason is the
// connection having died mid-run: the caller reports 125, because the command
// may well have done everything it was asked and nothing here can tell.
func Run(ctx context.Context, wsURL string, o Options) (Result, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	requested := time.Now()
	var dialOpts *websocket.DialOptions
	if o.Header != nil {
		dialOpts = &websocket.DialOptions{HTTPHeader: o.Header}
	}
	c, resp, err := websocket.Dial(ctx, wsURL, dialOpts)
	if err != nil {
		if resp != nil {
			return Result{}, dialError(resp, err)
		}
		return Result{}, err
	}
	defer c.CloseNow()
	c.SetReadLimit(readLimit)

	if err := wsjson.Write(ctx, c, terminal.ClientMessage{
		Type: terminal.TypeExecStart, Exec: &o.Spec}); err != nil {
		return Result{}, err
	}

	s := &session{c: c, o: o, res: Result{Detached: o.Spec.Detach}}
	s.run(ctx, requested)
	return s.res, nil
}

// dialError reads the server's error envelope off a refused upgrade.
// coder/websocket keeps the first 1KB of a failed handshake's body available,
// which is more than any envelope this API writes.
func dialError(resp *http.Response, cause error) *DialError {
	out := &DialError{Status: resp.StatusCode, err: cause}
	defer resp.Body.Close()
	var body struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
		State string `json:"state"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&body) == nil {
		out.Code, out.Message, out.State = body.Error.Code, body.Error.Message, body.State
	}
	return out
}

// session is one live exec on this end.
type session struct {
	c   *websocket.Conn
	o   Options
	res Result

	mu      sync.Mutex
	closing bool
}

// run reads the sandbox's messages until the command is over. Everything else
// this end does — pumping stdin, forwarding a window change, answering a
// Ctrl-C — is started only after `exec_started`, because until then there is
// no process to send anything to and the plane forwards nothing anyway.
func (s *session) run(ctx context.Context, requested time.Time) {
	var stopInput func()
	defer func() {
		if stopInput != nil {
			stopInput()
		}
	}()

	for {
		var m terminal.ServerMessage
		if err := wsjson.Read(ctx, s.c, &m); err != nil {
			return
		}
		switch m.Type {
		case terminal.TypeExecStarted:
			if s.res.Started {
				continue // the plane drops a second one; be safe here too
			}
			s.res.Started = true
			s.res.PID = m.PID
			s.res.StartedAt = time.Now()
			s.res.Queued = s.res.StartedAt.Sub(requested)
			if s.o.Spec.Detach {
				// A detached exec has nothing to stream and nothing to wait
				// for: the sandbox closes the attachment right behind this.
				return
			}
			stopInput = s.startInput(ctx)
		case terminal.TypeExecStdout:
			s.write(s.o.Stdout, m.Data)
		case terminal.TypeExecStderr:
			s.write(s.o.Stderr, m.Data)
		case terminal.TypeExecExit:
			s.res.ExitedAt = time.Now()
			s.res.Duration = s.res.ExitedAt.Sub(s.res.StartedAt)
			switch {
			case m.Signal != "":
				s.res.Signal = m.Signal
			case m.ExitCode >= 0 && m.ExitCode <= 255:
				code := m.ExitCode
				s.res.ExitCode = &code
			default:
				// A status outside 0–255 is not a status. It matters because
				// os.Exit masks to eight bits: a sandbox reporting 256 would
				// make `rainier exec` exit 0, telling a script a failing
				// command succeeded. Leaving both fields unset reports 125 —
				// "no exit status ever arrived" — which is exactly true.
			}
			return
		case terminal.TypeExecError:
			s.res.Reason = m.Reason
			return
		}
	}
}

// write puts the command's bytes on the caller's stream, byte for byte. It is
// deliberately not routed through the CLI's redactor: that exists to make
// untrusted server PROSE safe to print, and running it over a byte stream
// would corrupt a tarball, a JSON document, or anything else a caller pipes.
// A caller who asked to run a command in their own sandbox asked for its
// output.
func (s *session) write(w io.Writer, b []byte) {
	if len(b) == 0 || w == nil {
		return
	}
	_, _ = w.Write(b)
}

// startInput begins everything that flows toward the command: stdin, window
// changes under --tty, and the caller's own interrupt. It returns the stopper.
func (s *session) startInput(ctx context.Context) func() {
	var stops []func()

	restore := s.rawMode()
	if restore != nil {
		stops = append(stops, restore)
	}
	stops = append(stops, s.pumpStdin(ctx))
	if s.o.Spec.TTY {
		stops = append(stops, s.watchWindow(ctx))
	}
	stops = append(stops, s.watchInterrupt(ctx))

	return func() {
		for i := len(stops) - 1; i >= 0; i-- {
			stops[i]()
		}
	}
}

// rawMode puts the caller's terminal in raw mode for a --tty exec, and only
// then. Without a pty on the far end there is nothing that wants raw bytes,
// and taking the terminal out of line mode would only break the caller's own
// editing; with one, raw mode is what makes the remote program see the keys
// it was written for — Ctrl-C included, which is why the interrupt handler
// below leaves a raw tty alone.
func (s *session) rawMode() func() {
	if !s.o.Spec.TTY || s.o.Stdin == nil {
		return nil
	}
	fd := int(s.o.Stdin.Fd())
	if !term.IsTerminal(fd) {
		return nil
	}
	state, err := term.MakeRaw(fd)
	if err != nil {
		return nil
	}
	return func() { _ = term.Restore(fd, state) }
}

// pumpStdin forwards the caller's input and then says so. The end of input is
// an EXPLICIT frame because a socket that is still open cannot express "no
// more input", and `rainier exec s -- cat > f` with a pipe on the other end
// has to terminate.
func (s *session) pumpStdin(ctx context.Context) func() {
	done := make(chan struct{})
	if s.o.Stdin == nil {
		// No input at all. Say so at once, or a command that reads to EOF
		// waits forever for a caller that was never going to type.
		s.send(ctx, terminal.ClientMessage{Type: terminal.TypeExecStdinEOF})
		close(done)
		return func() {}
	}
	go func() {
		defer close(done)
		buf := make([]byte, stdinChunk)
		for {
			n, err := s.o.Stdin.Read(buf)
			if n > 0 {
				if !s.send(ctx, terminal.ClientMessage{
					Type: "stdin", Data: append([]byte(nil), buf[:n]...)}) {
					return
				}
			}
			if err != nil {
				s.send(ctx, terminal.ClientMessage{Type: terminal.TypeExecStdinEOF})
				return
			}
		}
	}()
	// The reader is deliberately NOT waited for: a blocking read on a
	// terminal nobody is typing into does not return, and the exec is over
	// when the command says so rather than when its caller stops typing.
	return func() {}
}

// watchWindow forwards SIGWINCH to the exec's OWN pty. It resizes that pty
// and nothing else — it never reaches the session's terminal, so it cannot
// move the agent's screen or squeeze a laptop's window from a phone.
func (s *session) watchWindow(ctx context.Context) func() {
	if s.o.Stdin == nil || !term.IsTerminal(int(s.o.Stdin.Fd())) {
		return func() {}
	}
	fd := int(s.o.Stdin.Fd())
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGWINCH)
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-ch:
				if cols, rows, err := term.GetSize(fd); err == nil {
					s.send(ctx, terminal.ClientMessage{Type: "resize", Cols: cols, Rows: rows})
				}
			case <-stop:
				return
			}
		}
	}()
	return func() {
		signal.Stop(ch)
		close(stop)
	}
}

// watchInterrupt turns the caller's Ctrl-C into the command's. The first
// press is forwarded as exec_signal{INT} — which is what a person pressing it
// means, and what the command's own handler is written for — and a second
// press gives up on it: the socket closes, and the sandbox kills the process
// group because its caller went away.
//
// Under --tty on a real terminal none of this runs: raw mode already delivers
// the Ctrl-C BYTE to the remote pty, whose line discipline signals the
// command directly, and catching it here as well would send the signal twice.
func (s *session) watchInterrupt(ctx context.Context) func() {
	if s.o.Spec.TTY && s.o.Stdin != nil && term.IsTerminal(int(s.o.Stdin.Fd())) {
		return func() {}
	}
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, os.Interrupt)
	stop := make(chan struct{})
	go func() {
		pressed := 0
		for {
			select {
			case <-ch:
				pressed++
				if pressed == 1 {
					s.send(ctx, terminal.ClientMessage{
						Type: terminal.TypeExecSignal, Signal: terminal.SignalINT})
					continue
				}
				s.mu.Lock()
				s.res.Interrupted = true
				s.closing = true
				s.mu.Unlock()
				_ = s.c.Close(websocket.StatusNormalClosure, "interrupted")
				return
			case <-stop:
				return
			}
		}
	}()
	return func() {
		signal.Stop(ch)
		close(stop)
	}
}

// send writes one client message, reporting whether it went. A failed write
// is a socket that is already going, so the caller stops rather than
// retrying: the command is about to be killed by the disconnect rule anyway.
func (s *session) send(ctx context.Context, m terminal.ClientMessage) bool {
	s.mu.Lock()
	closing := s.closing
	s.mu.Unlock()
	if closing {
		return false
	}
	return wsjson.Write(ctx, s.c, m) == nil
}

// ---------------------------------------------------------------------------
// the exit code
// ---------------------------------------------------------------------------

// The three codes Rainier reserves for itself. They are env(1) and shell
// convention, so they need no learning, and they keep 1 meaning what the CLI
// contract already says it means.
//
// The overlap with a command's own status is real and unavoidable — a command
// may itself exit 126 — and it is the right trade: a caller who needs
// certainty reads --json, where the three facts are separate fields.
const (
	// ExitNoStatus is "accepted, but no exit status ever arrived": the
	// connection to the session ended before the command reported one.
	ExitNoStatus = 125
	// ExitNotExecutable is "the command could not be executed".
	ExitNotExecutable = 126
	// ExitNotFound is "the command was not found".
	ExitNotFound = 127
)

// ExitCodeFor is the whole exit-code policy, as a pure function of a Result.
// The command's status IS the CLI's status; Rainier's own failures use the
// codes the shell vocabulary already reserves for a wrapper, so a script can
// always tell "the command failed" from "rainier failed".
//
// ok reports whether this Result decides the exit code at all. It is false
// only for an exec that never started and named no reason — a refusal the
// caller reports as an ordinary failure (1) with whatever sentence it has.
func ExitCodeFor(r Result) (code int, ok bool) {
	switch r.Reason {
	case terminal.ReasonNotFound:
		return ExitNotFound, true
	case terminal.ReasonNotExecutable, terminal.ReasonCwdRefused,
		terminal.ReasonEnvRefused, terminal.ReasonLogRefused:
		return ExitNotExecutable, true
	case terminal.ReasonUnsupported, terminal.ReasonTooManyExecs,
		terminal.ReasonTooManyDetached, terminal.ReasonSessionEnding,
		terminal.ReasonNoAnswer, terminal.ReasonStdinOverrun:
		// None of these is the command failing: a sandbox that cannot run
		// commands at all, one that did not answer in time, a session already
		// running as many as it may, or input this caller sent faster than
		// its command would take it. All four are Rainier's own failure,
		// which is 1, with a sentence that says which.
		return 1, false
	}
	if !r.Started {
		return 1, false
	}
	if r.Interrupted && r.ExitCode == nil && r.Signal == "" {
		// The caller pressed Ctrl-C twice and left, which kills the command.
		// 130 is what a shell reports for a command an interrupt ended, and
		// it is the honest answer here: the command did not report a status
		// because this caller stopped waiting for one, not because anything
		// went wrong with the connection.
		return 128 + interruptSignalNumber, true
	}
	if r.Detached {
		// The process exists and its caller is done: a detached run's success
		// is that it started, which is the whole point of the flag.
		return 0, true
	}
	if r.Signal != "" {
		if n, ok := terminal.ExitSignalNumber(r.Signal); ok {
			return 128 + n, true
		}
		// A signal this build cannot name still killed the command, and
		// saying "it exited 0" would be a lie a script would act on.
		return ExitNoStatus, true
	}
	if r.ExitCode != nil {
		return *r.ExitCode, true
	}
	// Started, and no status ever arrived.
	return ExitNoStatus, true
}

// errNoStatus is what a caller prints for 125.
// interruptSignalNumber is SIGINT's, for the 128+N a double Ctrl-C reports.
var interruptSignalNumber = func() int {
	n, _ := terminal.ExitSignalNumber(terminal.SignalINT)
	return n
}()

var errNoStatus = errors.New(
	"the connection to the session ended before the command reported an exit status")

// NoStatusError is that sentence as an error, for a caller that reports
// failures through one path.
func NoStatusError() error { return errNoStatus }

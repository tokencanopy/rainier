// Package attachio is the raw-mode terminal attach loop, extracted
// from cmd/rattach: raw mode (tty stdin only), SIGWINCH-driven resize,
// Ctrl-] detach, and sequence-number tracking for --since resume. It is the
// one place that contract lives, so cmd/rattach and cmd/rainier's `attach`
// (and `new`'s auto-attach) share the same resize-first handshake, detach key,
// and final detach/exit messages. A reconnectable disconnect returns its
// cursor silently: only the caller knows whether the terminal is being handed
// back to a shell or must remain untouched for cursor-relative replay.
//
// Restructuring note: rattach's original loop called os.Exit(0) from its
// reader goroutine the instant the connection dropped or the session
// exited — fine for a binary whose entire job is that one attach, wrong for
// a library. Run below never exits the process: it returns nil on a clean
// detach/disconnect/session-exit (the same three cases rattach used to
// os.Exit(0) on) and a non-nil error otherwise (dial failure, raw-mode
// setup failure). Final detach/exit messages retain their original text;
// disconnect diagnostics belong to the one-shot caller, not the live stream.
package attachio

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"golang.org/x/sys/unix"
	"golang.org/x/term"

	"github.com/tokencanopy/rainier/protocol/terminal"
)

// detachKey is Ctrl-].
const detachKey = 0x1d

// takeKey is Ctrl-\, the one key binding conditional ownership adds. It sits
// beside Ctrl-] deliberately: the two keys that are about the ATTACH rather
// than about the terminal are adjacent and neither is a printing character.
//
// It is intercepted only inside a negotiated attach — one where the server
// actually answered with a mode. Against a plane that does not speak
// conditional ownership it is forwarded as an ordinary byte, so nothing about
// today's key handling changes for a client that never negotiated.
const takeKey = 0x1c

// The three notices, which are UI copy and nothing more: no identity, no
// device, no session content. They are constants so a test asserts the
// sentence rather than a substring of it, and so the wording changes in one
// place.
const (
	NoticeViewing     = "[viewing — another device has control; press Ctrl-\\ to take it]"
	NoticeTaken       = "[another device took control; press Ctrl-\\ to take it back]"
	NoticeHaveControl = "[you have control]"
	NoticeStale       = "[somebody else got there first; press Ctrl-\\ to try again]"
)

// attachReadLimit matches sessiond/runnerd/controld's own raised read limit
// (see internal/server/server.go): a snapshot replaying a large scrollback
// is the biggest frame this splice ever carries in either direction.
const attachReadLimit = 16 << 20

// ErrSessionNotReady is Run's answer, via errors.Is, when the dial's HTTP
// response carried a 503 — controld's session_not_ready (the session
// hasn't reached "running" yet, see internal/controld/attach.go's
// waitRunning). It is never returned directly: it's what a *DialError
// wrapping a 503 response matches against, so callers write
// `errors.Is(err, attachio.ErrSessionNotReady)` rather than parsing error
// text (the old — and fragile — strings.Contains(err.Error(), "503")).
var ErrSessionNotReady = errors.New("attachio: session not ready")

// DialError wraps a websocket dial failure that received an HTTP response —
// as opposed to a pure transport failure (DNS, connection refused), which
// Run returns unwrapped so callers can still match on the underlying
// net/url error type. Status is that response's status code.
type DialError struct {
	Status int
	err    error
}

// Reason says why an established attach ended. Dial and local terminal setup
// failures are errors instead, because no terminal session started.
type Reason string

const (
	Detached     Reason = "detached"
	Disconnected Reason = "disconnected"
	Exited       Reason = "exited"
)

// Outcome is the non-error result of one established attach. LastSeq is the
// last frame successfully rendered to local stdout and is therefore the only
// safe reconnect cursor. ExitCode is meaningful only when Reason is Exited.
type Outcome struct {
	Reason   Reason
	LastSeq  uint64
	ExitCode int
	// Mode is what this attach held when it ended: terminal.ModeControl,
	// terminal.ModeView, or "" when nothing was negotiated — an attach that
	// asked for no conditional ownership, or one against a plane that does
	// not speak it. A caller reconnects as what it actually had, which is
	// what keeps a device that was superseded while it was away from taking
	// control back on its own.
	Mode string
	// Generation is the controller generation this attach was last
	// acknowledged at. It is the value a reconnect presents, and the value
	// `info` compares against to answer "is it this device?".
	Generation uint64
}

// Options is what an attach asks for about ownership. The zero value asks for
// nothing, which is an attach exactly as it behaved before conditional
// ownership existed — what cmd/rattach and every direct-to-sandbox caller
// wants, and what a caller gets by not thinking about it.
type Options struct {
	// Control advertises the capability. Without it the server sends today's
	// message set and this attach types unconditionally.
	Control bool
	// Mode is terminal.ModeControl (claim control when it is free) or
	// terminal.ModeView (never claim). Empty means control.
	Mode string
	// Expected is the generation this device last held. It makes the claim
	// conditional: take control only while the session is still at this
	// generation. It is what a RECONNECT presents.
	Expected uint64
	// Take sends exactly one claim if the attach comes back as a viewer —
	// `rainier attach --take`. Exactly one, and never again: a client that
	// re-claimed every time it was refused would be two devices fighting for
	// a keyboard instead of one person deciding.
	Take bool
}

func (e *DialError) Error() string { return e.err.Error() }
func (e *DialError) Unwrap() error { return e.err }

// Is reports whether target is ErrSessionNotReady and e wraps exactly the
// 503 controld answers a not-yet-running attach with.
func (e *DialError) Is(target error) bool {
	return target == ErrSessionNotReady && e.Status == http.StatusServiceUnavailable
}

// AttachURL builds the /attach URL from a base (sessiond directly, or
// runnerd's/controld's relay — same contract either way). session is
// appended as an extra query param only when non-empty: direct sessiond has
// no use for it and ignores unknown params, while a relay's /attach handler
// reads it to find the right hub. Factored out so the URL contract (base is
// just "scheme://host[:port]", never anything with a path already on it)
// has one place to get right and one place to unit test.
//
// The cursor is deliberately NOT part of it: Run puts `since` on whatever
// URL it is handed (see withSince), so a caller that builds its own attach
// URL — cmd/rainier's, which is a controld route, not a `<base>/attach` —
// cannot end up dialing without one. That is exactly the bug this split
// fixes; see TestRunDialsWithTheCursor.
func AttachURL(base, session string) string {
	u := base + "/attach"
	if session != "" {
		u += "?session=" + url.QueryEscape(session)
	}
	return u
}

// Cursor maps a `--since` flag to the cursor an attach carries on the wire.
// given reports whether the user actually typed the flag, which is the whole
// point: a uint64 flag defaulting to 0 cannot tell "no cursor" from "--since
// 0" on its own, and those are opposite requests.
//
//   - not given → 0: no cursor. Snapshot of the current screen, then live —
//     what a plain `attach` has always done and should keep doing (replaying
//     a day's raw log to paint a screen is the thing spec §5 forbids).
//   - `--since 0` → terminal.SinceAll: the whole log, first entry onward. "I
//     have seen nothing" is a coherent thing for a user to say, and it is
//     what the runbook's read-the-failed-setup-log flow means by it.
//   - `--since N` → N: resume, replaying the entries after N — the value the
//     disconnect line prints ("rattach --since %d to resume").
//
// Shared by cmd/rainier and cmd/rattach so both spell it identically, which
// is this package's whole reason for existing.
func Cursor(given bool, since uint64) uint64 {
	switch {
	case !given:
		return 0
	case since == 0:
		return terminal.SinceAll
	default:
		return since
	}
}

// withSince returns wsURL with the attach cursor on its query string. It
// appends rather than parsing-and-re-encoding so the caller's URL comes back
// otherwise byte-identical; the separator is the only thing that has to be
// right.
func withSince(wsURL string, since uint64) string {
	return appendParam(wsURL, "since", strconv.FormatUint(since, 10))
}

// withOwnership puts the conditional-ownership request on the attach URL. A
// caller that asks for nothing gets the URL back unchanged, which is what
// keeps a direct-to-sandbox attach byte-identical to what it always was.
func withOwnership(wsURL string, o Options) string {
	if !o.Control {
		return wsURL
	}
	wsURL = appendParam(wsURL, terminal.ParamControl, terminal.CapabilityControl)
	if o.Mode == terminal.ModeView {
		wsURL = appendParam(wsURL, terminal.ParamMode, terminal.ModeView)
	}
	if o.Expected > 0 {
		wsURL = appendParam(wsURL, terminal.ParamExpected, strconv.FormatUint(o.Expected, 10))
	}
	return wsURL
}

func appendParam(wsURL, key, value string) string {
	sep := "?"
	if strings.Contains(wsURL, "?") {
		sep = "&"
	}
	return wsURL + sep + key + "=" + url.QueryEscape(value)
}

// ScanDetach reports the index of the first Ctrl-] byte in buf, or -1 if
// buf contains none. Pure and side-effect-free so it's unit-testable on its
// own — Run uses it to split a stdin chunk into "forward this, then
// detach".
func ScanDetach(buf []byte) int {
	for i, b := range buf {
		if b == detachKey {
			return i
		}
	}
	return -1
}

// RestoreTerminal relinquishes remote emulator input modes and discards queued
// TTY input before returning to the local shell. Call only after the entire
// attach lifetime (all reconnect attempts) and after Run has joined its pumps:
// cursor-only replay need not repeat the remote application's mode enables.
// It does not clear screen/scrollback or read/discard piped input. Output resets
// are emitted only to a TTY; write/flush failures are returned together.
func RestoreTerminal(stdin, stdout *os.File) error {
	var err error
	if term.IsTerminal(int(stdout.Fd())) {
		_, err = fmt.Fprint(stdout, "\x1b[?1000l\x1b[?1002l\x1b[?1003l\x1b[?1005l\x1b[?1006l\x1b[?1015l\x1b[?1016l\x1b[?1004l\x1b[?2004l\x1b[?25h\x1b[0m")
	}
	return errors.Join(err, discardPendingInput(stdin))
}

// Run dials wsURL with header (nil for no extra headers — e.g. rattach's
// direct-to-sessiond/runnerd use, non-nil to carry an Authorization bearer
// against controld), performs the resize-first contract, and pipes the
// local terminal (os.Stdin/os.Stdout) until detach, session exit, or
// disconnect.
//
// since is the attach cursor (see Cursor), and Run is what puts it on the
// URL it dials — wsURL is a plain attach URL, with or without a query of its
// own, and never carries `since` itself. It used to be the caller's job to
// spell it into the URL AND pass it here, which is how `rainier attach
// --since N` came to dial with no cursor at all for two plans: the argument
// was accepted and never used. One place decides now, and it is the place
// that dials.
//
// It prints final terminal-handoff status lines:
//
//	"\r\n[detached at seq %d; session still running]\r\n"
//	"\r\n[session process exited: %d]\r\n"
//
// Raw mode is entered only when os.Stdin is a real tty; a non-tty stdin
// (piped input, a test) skips raw mode entirely and announces a fixed
// 80x24 size so the server's resize-first contract is still satisfied.
//
// Run returns an explicit Outcome for detach, disconnect, and process exit, and a
// non-nil error for anything that keeps the loop from starting (the dial or
// putting the terminal in raw mode). It stops every stdin/stdout goroutine
// before returning, so a caller may safely start another Run after a
// Disconnected outcome without creating two readers for the local terminal.
// A dial failure that received an HTTP response
// (rather than a pure transport failure) comes back as a *DialError;
// errors.Is(err, ErrSessionNotReady) matches specifically controld's 503
// session_not_ready — callers (cmd/rainier's `new`, retrying an attach
// immediately after create) should match on that sentinel rather than
// inspecting error text.
func Run(ctx context.Context, wsURL string, header http.Header, since uint64, o Options) (Outcome, error) {
	return runWithIO(ctx, wsURL, header, since, o, os.Stdin, os.Stdout)
}

func runWithIO(ctx context.Context, wsURL string, header http.Header, since uint64, o Options, stdin *os.File, stdout io.Writer) (Outcome, error) {
	var opts *websocket.DialOptions
	if header != nil {
		opts = &websocket.DialOptions{HTTPHeader: header}
	}
	c, resp, err := websocket.Dial(ctx, withOwnership(withSince(wsURL, since), o), opts)
	if err != nil {
		if resp != nil {
			// A response came back but didn't upgrade (a plain HTTP error
			// status, e.g. controld's 503 session_not_ready before the
			// handshake) — as opposed to a transport failure, where resp is
			// nil and err is returned unwrapped below.
			return Outcome{}, &DialError{Status: resp.StatusCode, err: err}
		}
		return Outcome{}, err
	}
	defer c.CloseNow()
	// See internal/server/server.go: match the server's raised read limit so
	// a single oversized PTY-output frame (live or replayed from the event
	// log) doesn't close the connection with StatusMessageTooBig. 16MiB
	// explicit cap, not unlimited (-1); a real protocol-level max-frame size
	// is deferred to Plan 2.
	c.SetReadLimit(attachReadLimit)

	fd := int(stdin.Fd())
	isTTY := term.IsTerminal(fd)

	// restore is a no-op unless stdin is a real tty. Raw mode, and the
	// SIGWINCH-driven size reporting that depends on term.GetSize working,
	// are both skipped for non-tty stdin (e.g. a script piping input in, or
	// a headless test) — there is no terminal state to save or restore in
	// that case.
	restore := func() {}
	if isTTY {
		// The WebSocket dial happens while the terminal is still in cooked mode.
		// Drop anything queued during that gap only after the connection is ready,
		// immediately before this attempt takes input ownership. Otherwise a line
		// typed at a reconnect notice can execute remotely after recovery.
		if err := discardPendingInput(stdin); err != nil {
			return Outcome{}, err
		}
		oldState, err := term.MakeRaw(fd)
		if err != nil {
			return Outcome{}, err
		}
		restore = func() { term.Restore(fd, oldState) }
	}
	defer restore()

	own := newOwnership(o)

	var resizeDone chan struct{}
	var resizeWG sync.WaitGroup
	var winch chan os.Signal
	if isTTY {
		sendSize := func() {
			w, h, err := term.GetSize(fd)
			if err != nil {
				return
			}
			// The opening resize always goes: it is the message the attach
			// protocol requires first, and it is what sizes the session. Every
			// LATER one is the controller's to send — a viewer's would be
			// dropped at the plane and discarded at the pty, and the terminal
			// follows whoever is typing.
			if own.mayType() {
				wsjson.Write(ctx, c, terminal.ClientMessage{
					Type: "resize", Cols: w, Rows: h, Generation: terminal.GenOf(own.stamp())})
			}
		}
		// The required first message, sent before any answer can have
		// arrived, and therefore never stamped and never suppressed.
		if w, h, err := term.GetSize(fd); err == nil {
			wsjson.Write(ctx, c, terminal.ClientMessage{Type: "resize", Cols: w, Rows: h})
		}

		winch = make(chan os.Signal, 1)
		signal.Notify(winch, syscall.SIGWINCH)
		resizeDone = make(chan struct{})
		resizeWG.Add(1)
		go func() {
			defer resizeWG.Done()
			for {
				select {
				case <-winch:
					sendSize()
				case <-resizeDone:
					return
				}
			}
		}()
	} else {
		// No tty to size from: announce a fixed default so the server's
		// resize-first contract is still satisfied.
		wsjson.Write(ctx, c, terminal.ClientMessage{Type: "resize", Cols: 80, Rows: 24})
	}

	var lastSeq atomic.Uint64
	// If the connection drops before its first frame, resume from the caller's
	// actual cursor rather than inventing zero. That includes SinceAll: changing
	// the sentinel to zero would silently turn a requested full-log replay into
	// a current-screen snapshot. A fully rendered frame replaces this value.
	lastSeq.Store(since)
	// decided/claim together decide which of the reader/stdin
	// goroutines gets to own Run's outcome — exactly one of them prints its
	// status line and unblocks Run, whichever notices its end condition
	// first. Without claim's CAS gate, the loser — e.g. the reader
	// goroutine, still blocked in wsjson.Read when the stdin side detaches
	// and Run's deferred c.CloseNow() then kills its read — would print its
	// own status line right after Run has already returned (rattach's
	// original os.Exit(0) sidestepped this entirely by ending the whole
	// process on whichever path got there first).
	//
	// stdoutMu serializes ordinary terminal writes with the winning status
	// line. Run additionally closes the socket and joins both goroutines before
	// returning, so neither stdin nor stdout remains owned by an old attempt
	// when the product CLI reconnects.
	var decided atomic.Bool
	var stdoutMu sync.Mutex
	type runResult struct {
		out Outcome
		err error
	}
	result := make(chan runResult, 1)
	claim := func() bool { return decided.CompareAndSwap(false, true) }
	finish := func(out Outcome, err error) { result <- runResult{out: out, err: err} }
	var pumps sync.WaitGroup
	pumps.Add(2)

	go func() {
		defer pumps.Done()
		for {
			var m terminal.ServerMessage
			if err := wsjson.Read(ctx, c, &m); err != nil {
				if !claim() {
					return
				}
				if ctxErr := ctx.Err(); ctxErr != nil {
					finish(Outcome{}, ctxErr)
					return
				}
				if !retryableWebSocketReadError(err) {
					finish(Outcome{LastSeq: lastSeq.Load()}, fmt.Errorf("terminal connection closed: %w", err))
					return
				}
				stdoutMu.Lock()
				restore()
				seq := lastSeq.Load()
				stdoutMu.Unlock()
				mode, gen := own.state()
				finish(Outcome{Reason: Disconnected, LastSeq: seq, Mode: mode, Generation: gen}, nil)
				return
			}
			if isOwnershipMessage(m.Type) {
				notice := own.observe(m)
				stdoutMu.Lock()
				if notice != "" && !decided.Load() {
					writeNotice(stdout, notice)
				}
				stdoutMu.Unlock()
				if claim, ok := ownTakeClaim(own); ok {
					wsjson.Write(ctx, c, claim)
				}
				continue
			}
			switch m.Type {
			case "snapshot", "output":
				// Terminal traffic without an answer means a plane that does
				// not speak conditional ownership. From here this attach is
				// an ordinary one, byte for byte.
				own.settleLegacy()
				stdoutMu.Lock()
				if !decided.Load() {
					n, writeErr := stdout.Write(m.Data)
					if writeErr == nil && n != len(m.Data) {
						writeErr = io.ErrShortWrite
					}
					if writeErr != nil {
						if claim() {
							restore()
							finish(Outcome{LastSeq: lastSeq.Load()}, fmt.Errorf("writing terminal output: %w", writeErr))
						}
						stdoutMu.Unlock()
						return
					}
					if m.Seq > 0 {
						lastSeq.Store(m.Seq)
					}
				}
				// else: a decision already claimed Run's outcome — drop this
				// (and any later) output rather than risk racing the caller,
				// who is free to touch os.Stdout the instant Run returns.
				stdoutMu.Unlock()
			case "exit":
				own.settleLegacy()
				if !claim() {
					return
				}
				stdoutMu.Lock()
				restore()
				fmt.Fprintf(stdout, "\r\n[session process exited: %d]\r\n", m.ExitCode)
				stdoutMu.Unlock()
				mode, gen := own.state()
				finish(Outcome{Reason: Exited, LastSeq: lastSeq.Load(), ExitCode: m.ExitCode, Mode: mode, Generation: gen}, nil)
				return
			default:
				if !claim() {
					return
				}
				finish(Outcome{LastSeq: lastSeq.Load()}, errors.New("terminal protocol: unsupported server message type"))
				return
			}
		}
	}()

	go func() {
		defer pumps.Done()
		buf := make([]byte, 1024)
		for {
			n, err, stopped := readStdin(decided.Load, stdin, buf)
			if stopped {
				return
			}
			if err != nil {
				// Stdin closed (EOF on a pipe, or a read error on a real
				// tty). There's nothing left to forward, but the session may
				// still have output in flight, so this goroutine simply
				// stops rather than deciding an outcome — the reader
				// goroutine above is what ends Run, on disconnect or session
				// exit (or the caller canceling ctx / the process being
				// killed externally).
				return
			}
			// send is the one place local bytes become a frame: a viewer's
			// keystrokes stop here, and a controller's carry the generation
			// they were typed under so a frame that outlives its authority is
			// discarded rather than executed.
			send := func(b []byte) {
				if len(b) == 0 || !own.mayType() {
					return
				}
				wsjson.Write(ctx, c, terminal.ClientMessage{
					Type: "stdin", Data: append([]byte(nil), b...), Generation: terminal.GenOf(own.stamp())})
			}
			at, key := scanKeys(buf[:n], own.keyActive())
			if key == keyNone {
				send(buf[:n])
				continue
			}
			// Forward whatever preceded the key in this chunk; anything after
			// it in the same chunk is discarded, as it always has been.
			send(buf[:at])
			if key == keyTake {
				if msg, ok := own.claim(); ok {
					wsjson.Write(ctx, c, msg)
				}
				// Nothing is printed when this device already has control:
				// the answer to "take control" when you have it is silence,
				// not a line of noise over somebody's editor.
				continue
			}
			if !claim() {
				return
			}
			stdoutMu.Lock()
			restore()
			seq := lastSeq.Load()
			fmt.Fprintf(stdout, "\r\n[detached at seq %d; session still running]\r\n", seq)
			stdoutMu.Unlock()
			mode, gen := own.state()
			finish(Outcome{Reason: Detached, LastSeq: seq, Mode: mode, Generation: gen}, nil)
			return
		}
	}()

	r := <-result
	// Unblock the losing WebSocket reader, then wait for both pumps. The stdin
	// side polls with a bounded timeout specifically so it observes decided and
	// joins here even when nobody is typing.
	c.CloseNow()
	pumps.Wait()
	if resizeDone != nil {
		signal.Stop(winch)
		close(resizeDone)
		resizeWG.Wait()
	}
	return r.out, r.err
}

func retryableWebSocketReadError(err error) bool {
	if errors.Is(err, websocket.ErrMessageTooBig) {
		return false
	}
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &syntaxErr) || errors.As(err, &typeErr) {
		return false
	}
	switch websocket.CloseStatus(err) {
	case websocket.StatusPolicyViolation:
		// The hosted gateway deliberately ends its authorization lease. Only
		// this exact wire reason invites a fresh edge authorization decision;
		// ordinary policy denials must remain terminal, never an endless retry.
		var closeErr websocket.CloseError
		return errors.As(err, &closeErr) && closeErr.Reason == "attach lease expired; reattach"
	case -1, websocket.StatusNoStatusRcvd, websocket.StatusAbnormalClosure,
		websocket.StatusGoingAway, websocket.StatusInternalError,
		websocket.StatusServiceRestart, websocket.StatusTryAgainLater,
		websocket.StatusBadGateway:
		return true
	default:
		return false
	}
}

func discardPendingInput(stdin *os.File) error {
	fd := int(stdin.Fd())
	if !term.IsTerminal(fd) {
		return nil
	}
	return flushTTYInput(fd)
}

const stdinPollInterval = 100 * time.Millisecond

// readStdin waits for one local input chunk while periodically checking done.
// A plain os.File.Read on a terminal can block forever, which used to leave an
// old Run's goroutine alive after disconnect. Poll makes ownership bounded so
// the reconnecting caller never creates two readers for the same terminal.
func readStdin(done func() bool, stdin *os.File, buf []byte) (n int, err error, stopped bool) {
	fd := int32(stdin.Fd())
	for {
		if done() {
			return 0, nil, true
		}
		fds := []unix.PollFd{{Fd: fd, Events: unix.POLLIN}}
		ready, pollErr := unix.Poll(fds, int(stdinPollInterval/time.Millisecond))
		if pollErr != nil {
			if errors.Is(pollErr, syscall.EINTR) {
				continue
			}
			return 0, pollErr, false
		}
		if ready == 0 {
			continue
		}
		if fds[0].Revents&(unix.POLLIN|unix.POLLHUP) != 0 {
			n, err := stdin.Read(buf)
			return n, err, false
		}
		if fds[0].Revents&(unix.POLLERR|unix.POLLNVAL) != 0 {
			return 0, fmt.Errorf("polling stdin: event %#x", fds[0].Revents), false
		}
	}
}

// ownTakeClaim is --take spending its one claim: the attach came back a
// viewer, the user asked for control on the command line, and this is the
// single message that asks for it. Exactly once, and never in response to
// being refused — that is the difference between a request and a loop.
func ownTakeClaim(o *ownership) (terminal.ClientMessage, bool) {
	if !o.takeOnce() {
		return terminal.ClientMessage{}, false
	}
	return o.claim()
}

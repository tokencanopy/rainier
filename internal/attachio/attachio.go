// Package attachio is the raw-mode terminal attach loop, extracted verbatim
// from cmd/rattach: raw mode (tty stdin only), SIGWINCH-driven resize,
// Ctrl-] detach, and sequence-number tracking for --since resume. It is the
// one place that contract lives, so cmd/rattach and cmd/rainier's `attach`
// (and `new`'s auto-attach) share identical behavior — same status lines,
// same resize-first handshake, same detach key.
//
// Restructuring note: rattach's original loop called os.Exit(0) from its
// reader goroutine the instant the connection dropped or the session
// exited — fine for a binary whose entire job is that one attach, wrong for
// a library. Run below never exits the process: it returns nil on a clean
// detach/disconnect/session-exit (the same three cases rattach used to
// os.Exit(0) on) and a non-nil error otherwise (dial failure, raw-mode
// setup failure). The printed status lines and their exact text are
// unchanged; only how the loop hands control back changed. This is the one
// intentional internal restructuring the extraction makes.
package attachio

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"golang.org/x/term"

	"rainier/internal/wire"
)

// detachKey is Ctrl-].
const detachKey = 0x1d

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
func AttachURL(base string, since uint64, session string) string {
	u := base + "/attach?since=" + strconv.FormatUint(since, 10)
	if session != "" {
		u += "&session=" + url.QueryEscape(session)
	}
	return u
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

// Run dials wsURL with header (nil for no extra headers — e.g. rattach's
// direct-to-sessiond/runnerd use, non-nil to carry an Authorization bearer
// against controld), performs the resize-first contract, and pipes the
// local terminal (os.Stdin/os.Stdout) until detach, session exit, or
// disconnect. It prints the exact status lines rattach always has:
//
//	"\r\n[detached at seq %d; session still running]\r\n"
//	"\r\n[disconnected at seq %d; rattach --since %d to resume]\r\n"
//	"\r\n[session process exited: %d]\r\n"
//
// Raw mode is entered only when os.Stdin is a real tty; a non-tty stdin
// (piped input, a test) skips raw mode entirely and announces a fixed
// 80x24 size so the server's resize-first contract is still satisfied.
//
// Run returns nil for all three status lines above (detach, disconnect,
// session exit — rattach's old os.Exit(0) cases) and a non-nil error for
// anything that keeps the loop from ever starting (the dial, or putting the
// terminal in raw mode). A dial failure that received an HTTP response
// (rather than a pure transport failure) comes back as a *DialError;
// errors.Is(err, ErrSessionNotReady) matches specifically controld's 503
// session_not_ready — callers (cmd/rainier's `new`, retrying an attach
// immediately after create) should match on that sentinel rather than
// inspecting error text.
func Run(ctx context.Context, wsURL string, header http.Header, since uint64) error {
	var opts *websocket.DialOptions
	if header != nil {
		opts = &websocket.DialOptions{HTTPHeader: header}
	}
	c, resp, err := websocket.Dial(ctx, wsURL, opts)
	if err != nil {
		if resp != nil {
			// A response came back but didn't upgrade (a plain HTTP error
			// status, e.g. controld's 503 session_not_ready before the
			// handshake) — as opposed to a transport failure, where resp is
			// nil and err is returned unwrapped below.
			return &DialError{Status: resp.StatusCode, err: err}
		}
		return err
	}
	defer c.CloseNow()
	// See internal/server/server.go: match the server's raised read limit so
	// a single oversized PTY-output frame (live or replayed from the event
	// log) doesn't close the connection with StatusMessageTooBig. 16MiB
	// explicit cap, not unlimited (-1); a real protocol-level max-frame size
	// is deferred to Plan 2.
	c.SetReadLimit(attachReadLimit)

	fd := int(os.Stdin.Fd())
	isTTY := term.IsTerminal(fd)

	// restore is a no-op unless stdin is a real tty. Raw mode, and the
	// SIGWINCH-driven size reporting that depends on term.GetSize working,
	// are both skipped for non-tty stdin (e.g. a script piping input in, or
	// a headless test) — there is no terminal state to save or restore in
	// that case.
	restore := func() {}
	if isTTY {
		oldState, err := term.MakeRaw(fd)
		if err != nil {
			return err
		}
		restore = func() { term.Restore(fd, oldState) }
	}
	defer restore()

	if isTTY {
		sendSize := func() {
			w, h, err := term.GetSize(fd)
			if err == nil {
				wsjson.Write(ctx, c, wire.ClientMsg{Type: "resize", Cols: w, Rows: h})
			}
		}
		sendSize() // required first message

		winch := make(chan os.Signal, 1)
		signal.Notify(winch, syscall.SIGWINCH)
		defer signal.Stop(winch)
		winchDone := make(chan struct{})
		defer close(winchDone)
		go func() {
			for {
				select {
				case <-winch:
					sendSize()
				case <-winchDone:
					return
				}
			}
		}()
	} else {
		// No tty to size from: announce a fixed default so the server's
		// resize-first contract is still satisfied.
		wsjson.Write(ctx, c, wire.ClientMsg{Type: "resize", Cols: 80, Rows: 24})
	}

	var lastSeq atomic.Uint64
	// decided/claim/finish together decide which of the reader/stdin
	// goroutines gets to own Run's outcome — exactly one of them prints its
	// status line and unblocks Run, whichever notices its end condition
	// first. Without claim's CAS gate, the loser — e.g. the reader
	// goroutine, still blocked in wsjson.Read when the stdin side detaches
	// and Run's deferred c.CloseNow() then kills its read — would print its
	// own status line right after Run has already returned (rattach's
	// original os.Exit(0) sidestepped this entirely by ending the whole
	// process on whichever path got there first).
	//
	// claim and finish are deliberately separate calls, not one combined
	// win(err): claim only decides (CAS), and finish is what actually sends
	// to result and lets Run's `return <-result` complete. Combining them —
	// an earlier version of this file sent to result as part of the same
	// call as the CAS — let Run return to ITS caller while the winning
	// goroutine was still inside restore()/fmt.Printf(), still writing to
	// os.Stdout; a caller that swaps os.Stdout right after Run returns (a
	// test capturing output, e.g.) is entitled to assume Run has stopped
	// touching it by then. The race detector caught exactly that. Calling
	// finish only once restore()+fmt.Printf() have both already run is what
	// makes "Run has returned" a true happens-after for "this goroutine is
	// done touching the terminal".
	//
	// That earlier fix was still incomplete on its own: it only gated the
	// three DECISION paths (disconnect/exit/detach), not the reader's
	// ordinary os.Stdout.Write(m.Data) for plain "snapshot"/"output"
	// messages. With output still streaming at the instant of detach, the
	// reader can be mid-write to os.Stdout the moment the stdin goroutine
	// claims and Run returns — a caller that reassigns os.Stdout right
	// after Run returns (a test capturing output, e.g.) races that
	// in-flight write. A bare `if !decided.Load() { write }` check doesn't
	// close this: decided can flip to true, and the deciding goroutine's
	// finish() can unblock Run and hand control back to the caller, all
	// while a write that passed the check a moment earlier is still in
	// progress. stdoutMu is what actually serializes "touching stdout" —
	// every write (the reader's ordinary writes AND each decision path's
	// restore()+fmt.Printf()) holds it, and a decision path only calls
	// finish() after releasing it, so by the time Run returns, no goroutine
	// can still be mid-write: any reader write already in flight has
	// necessarily completed (it holds the mutex the decision path is
	// waiting on), and any reader write that hasn't started yet will see
	// decided already true once it acquires the mutex and skip.
	var decided atomic.Bool
	var stdoutMu sync.Mutex
	result := make(chan error, 1)
	claim := func() bool { return decided.CompareAndSwap(false, true) }
	finish := func(err error) { result <- err }

	go func() {
		for {
			var m wire.ServerMsg
			if err := wsjson.Read(ctx, c, &m); err != nil {
				if !claim() {
					return
				}
				stdoutMu.Lock()
				restore()
				fmt.Printf("\r\n[disconnected at seq %d; rattach --since %d to resume]\r\n", lastSeq.Load(), lastSeq.Load())
				stdoutMu.Unlock()
				finish(nil)
				return
			}
			switch m.Type {
			case "snapshot", "output":
				stdoutMu.Lock()
				if !decided.Load() {
					os.Stdout.Write(m.Data)
					if m.Seq > 0 {
						lastSeq.Store(m.Seq)
					}
				}
				// else: a decision already claimed Run's outcome — drop this
				// (and any later) output rather than risk racing the caller,
				// who is free to touch os.Stdout the instant Run returns.
				stdoutMu.Unlock()
			case "exit":
				if !claim() {
					return
				}
				stdoutMu.Lock()
				restore()
				fmt.Printf("\r\n[session process exited: %d]\r\n", m.ExitCode)
				stdoutMu.Unlock()
				finish(nil)
				return
			}
		}
	}()

	go func() {
		buf := make([]byte, 1024)
		for {
			n, err := os.Stdin.Read(buf)
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
			detach := ScanDetach(buf[:n])
			if detach < 0 {
				wsjson.Write(ctx, c, wire.ClientMsg{Type: "stdin", Data: append([]byte(nil), buf[:n]...)})
				continue
			}
			// Forward whatever preceded the detach key in this chunk before
			// detaching; anything after it in the same chunk is discarded.
			if detach > 0 {
				wsjson.Write(ctx, c, wire.ClientMsg{Type: "stdin", Data: append([]byte(nil), buf[:detach]...)})
			}
			if !claim() {
				return
			}
			stdoutMu.Lock()
			restore()
			fmt.Printf("\r\n[detached at seq %d; session still running]\r\n", lastSeq.Load())
			stdoutMu.Unlock()
			finish(nil)
			return
		}
	}()

	return <-result
}

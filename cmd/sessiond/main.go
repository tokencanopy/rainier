package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	mrand "math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/coder/websocket"

	"rainier/internal/eventlog"
	"rainier/internal/reap"
	"rainier/internal/relay"
	"rainier/internal/server"
	"rainier/internal/session"
)

func main() {
	// Install the SIGCHLD reaper before anything can spawn a child: SIGCHLD's
	// default disposition is ignore, and the kernel discards an ignored
	// signal at generation (it is never queued for later delivery). If the
	// agent's exit raced a later Start() call, that SIGCHLD would be lost
	// forever — sessiond hosts exactly one child, so there's no next SIGCHLD
	// to catch it, and AwaitExit would then block indefinitely.
	reap.Start() // single authoritative waiter on Linux; agent exit code flows back through Proc.Wait via Session

	listen := flag.String("listen", "127.0.0.1:7070", "dev listener address")
	logPath := flag.String("log", "/tmp/session.log", "event log path")
	cols := flag.Int("cols", 120, "initial cols")
	rows := flag.Int("rows", 32, "initial rows")
	dial := flag.String("dial", "", "runnerd URL to dial and register with (relay mode)")
	sessionID := flag.String("session", "", "session id to register as (relay mode)")
	flag.Parse()
	argv := flag.Args()
	if len(argv) == 0 {
		log.Fatal("usage: sessiond [flags] -- <command> [args...]")
	}

	// Env fallback: the Docker driver injects RAINIER_DIAL/RAINIER_SESSION as
	// env vars, not flags, so honor them whenever the flags were left empty.
	// Resolved here, before the child argv is composed, because the setup
	// wrapper below is a relay-mode concern and this is what decides whether
	// this process is in relay mode at all.
	if *dial == "" {
		if v := os.Getenv("RAINIER_DIAL"); v != "" {
			*dial = v
		}
	}
	if *sessionID == "" {
		if v := os.Getenv("RAINIER_SESSION"); v != "" {
			*sessionID = v
		}
	}

	// An environment's setup script (Plan 4) arrives as RAINIER_SETUP_B64,
	// injected by the driver. When it is present the child is not the agent
	// but a wrapper that runs setup first and only execs the agent if it
	// succeeded, and a watcher reports the outcome upstream over the relay's
	// control channel. When it is absent — a scratch session, or one whose
	// environment was already snapshot-cached, where the image IS the
	// finished setup — none of this happens and sessiond behaves exactly as
	// it did before.
	//
	// Gated on relay mode: the control channel a setup outcome travels on
	// only exists on a dialed conn, and the local dev listener has no runnerd
	// to report to.
	setupB64 := os.Getenv("RAINIER_SETUP_B64")
	runSetup := *dial != "" && setupB64 != ""
	if setupB64 != "" && *dial == "" {
		// Can't happen from the driver (it injects RAINIER_DIAL with the
		// script), but a human running sessiond by hand with the var set
		// would otherwise get an environment silently missing its setup.
		log.Print("RAINIER_SETUP_B64 is set but sessiond has no runnerd to dial; skipping setup")
	}
	if runSetup {
		if err := prepareSetup(setupDir, setupB64); err != nil {
			// Deliberately fatal. Running the agent anyway would hand a user
			// a session that looks healthy in an environment that was never
			// built — the one outcome worse than not starting. Dying is what
			// makes runnerd notice the container is gone and controld mark
			// the session failed.
			log.Fatalf("setup: %v", err)
		}
		argv = setupWrapperArgv(setupScriptPath, setupRCPath, argv)
	}

	s, err := session.New(session.Config{Argv: argv, Cols: *cols, Rows: *rows, LogPath: *logPath}, session.StartProc)
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		<-s.Exited()
		log.Printf("child exited with code %d; sessiond stays up for viewers", s.ExitCode())
	}()

	// setupCtx stops the watcher on shutdown: a sessiond on its way out has
	// no business emitting a setup verdict for a session nobody will run.
	setupCtx, stopWatching := context.WithCancel(context.Background())
	defer stopWatching()

	term := make(chan os.Signal, 1)
	signal.Notify(term, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-term
		stopWatching()
		// Graceful: ask the agent to exit; the exit path closes viewers and the
		// process ends when the child is reaped. Give it a moment, then hard-exit.
		s.Stop()
		select {
		case <-s.Exited():
		case <-time.After(5 * time.Second):
		}
		os.Exit(0)
	}()

	if *dial != "" {
		var setup <-chan []byte
		if runSetup {
			setup = startSetupWatcher(setupCtx, s, *logPath, setupTimeout(os.Getenv("RAINIER_SETUP_TIMEOUT")))
		}
		dialLoop(context.Background(), *dial, *sessionID, s, setup)
		return
	}

	log.Printf("sessiond listening on %s", *listen)
	if err := http.ListenAndServe(*listen, server.New(s)); err != nil {
		log.Fatal(err)
	}
	_ = os.Stdout
}

// dialLoop keeps sessiond registered with runnerd for the life of the
// process: dial failures at boot and conn deaths later both retry with
// jittered exponential backoff (1s..30s cap). The session — the PTY, the
// agent, the event log — is never coupled to any single connection's
// lifetime (spec §10: sessions outlive everything else). A destroyed
// session's container is removed by runnerd itself, which is what actually
// ends this loop (SIGTERM → main's handler).
//
// setup, when non-nil, is where the setup watcher's single verdict arrives.
// It is threaded through the loop rather than sent from the watcher directly
// because the conn it has to travel on is this loop's, and there may not be
// one at the moment the verdict lands — see serveConn.
func dialLoop(ctx context.Context, dial, sessionID string, s *session.Session, setup <-chan []byte) {
	backoff := time.Second
	var pending []byte // a setup outcome no connection has accepted yet
	for {
		c, _, err := websocket.Dial(ctx, dial+"?session="+sessionID, nil)
		if err == nil {
			c.SetReadLimit(16 << 20)
			log.Printf("sessiond registered with runnerd as %s", sessionID)
			backoff = time.Second
			// WithControl, not plain ServeSession: the returned sender shares
			// the relay's single writer, so a control event and a terminal
			// frame can never interleave on the conn. With no setup running
			// the sender is simply never used, and the relay behaves exactly
			// as ServeSession did.
			//
			// The inbound handler is nil until sessiond has RPC methods to
			// serve (the git and file operations controld drives from the
			// other end): a nil handler reads inbound control frames and drops
			// them, which is the right behaviour for a session that cannot
			// answer anything yet — the alternative, refusing to decode them,
			// would only be a way for a newer controld to kill an older
			// sandbox's relay.
			sender, errc := relay.ServeSessionWithControl(ctx, relay.WSConn(c), s, nil)
			var relayErr error
			pending, setup, relayErr = serveConn(sender, errc, setup, pending)
			log.Printf("relay ended: %v; redialing", relayErr)
		} else {
			log.Printf("dial runnerd: %v; retrying in %s", err, backoff)
		}
		// mrand jitter: timing spread to avoid a reconnect thundering herd
		// after a runnerd restart, not a security-sensitive use of
		// randomness — math/rand is fine here.
		jitter := time.Duration(mrand.Int63n(int64(backoff / 2)))
		select {
		case <-time.After(backoff + jitter):
		case <-ctx.Done():
			return
		}
		backoff = nextBackoff(backoff)
	}
}

// controlSender is the one method serveConn needs from
// relay.ControlSender — named as an interface so the delivery rules below
// can be tested without standing up a conn to have a sender for.
type controlSender interface {
	Send(payload []byte) error
}

// serveConn waits for one connection's relay to end, delivering the setup
// outcome over that connection's control channel if one lands (or is already
// pending) while it lives. It returns the relay's error together with the
// still-undelivered outcome state, so the next dial picks up exactly where
// this one left off.
//
// The outcome is computed once by the watcher but may have to be offered
// more than once: a conn can die between "setup finished" and "runnerd heard
// about it", and there is nothing else in the system that knows a setup
// outcome exists — controld would wait forever for an event no one will
// resend. So a failed Send keeps the payload pending for the next
// connection, and a successful one drops both the payload and the channel so
// it can never be sent twice.
func serveConn(sender controlSender, errc <-chan error, setup <-chan []byte, pending []byte) ([]byte, <-chan []byte, error) {
	for {
		if pending != nil {
			if err := sender.Send(pending); err == nil {
				pending, setup = nil, nil
			} else {
				// This conn is already dying; errc says so below, and the
				// payload rides along to the next one. Not retried on this
				// conn: a Send that failed once on a dead conn fails again.
				log.Printf("setup outcome not delivered (%v); retrying on the next connection", err)
			}
		}
		select {
		case err := <-errc:
			return pending, setup, err
		case p := <-setup:
			// Taken once: the watcher sends exactly one verdict, and nil'ing
			// the channel here keeps a second receive (which would spin on a
			// drained-and-nil'd channel) out of the loop entirely.
			pending, setup = p, nil
		}
	}
}

// nextBackoff doubles d and clamps to the 30s cap. Extracted as a pure step
// (no loop, no clock) so the cap arithmetic is table-testable on its own:
// the previous inline `if backoff < 30*time.Second { backoff *= 2 }` guarded
// on the PRE-doubled value, so 16s (< 30s) still passed and doubled to 32s —
// one step over the design's stated 1s..30s cap, and every step after that
// also failed the guard, freezing backoff at 32s forever instead of 30s.
// Doubling unconditionally and clamping the RESULT is what actually holds
// the cap.
func nextBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

// The setup pipeline's files, all inside the session's own workspace volume
// (the one writable, persistent path a session gets — the driver seeds this
// directory when it initializes the volume, see internal/driver).
const (
	setupDir        = "/workspace/.rainier"
	setupScriptName = "setup.sh"
	setupRCName     = "setup.rc"
	setupScriptPath = setupDir + "/" + setupScriptName
	setupRCPath     = setupDir + "/" + setupRCName
)

const (
	// setupPollInterval is how often the watcher looks for the wrapper's rc
	// file. Setup runs for minutes; a second's granularity on noticing it
	// finished costs nothing and keeps the poll invisible.
	setupPollInterval = time.Second
	// setupTailBytes is how much of the session's output a failure carries
	// upstream. Enough for the last error and its context, small enough to
	// sit in a control frame and a database column without thought.
	setupTailBytes = 2 << 10
)

// setupWrapperFmt is the exact program sessiond runs in place of the agent
// when an environment ships a setup script: run setup, record its exit code
// where the watcher can read it, and exec the agent ONLY if it succeeded.
//
// Every piece is load-bearing:
//   - `sh <path>` rather than executing the script directly: the file is
//     written 0755, but the workspace volume's mount options are not this
//     program's to assume, and a setup script is not required to carry a
//     shebang.
//   - `rc=$?` captured immediately, before anything else can overwrite `$?`.
//   - the rc file is written BEFORE the exec, because after a successful
//     exec this shell no longer exists to write anything.
//   - `exec` rather than a plain call: the agent must BE the session's
//     process (pid 1's child, the PTY's owner), not a grandchild behind a
//     shell that would swallow its exit status and its signals.
//   - `"$@"` with a `wrapper` $0: the agent's argv arrives as positional
//     parameters and is passed on byte for byte, so arguments containing
//     spaces, quotes, globs or `$` reach it exactly as controld sent them.
//     Interpolating the argv into this string instead would make every one
//     of those a shell injection.
const setupWrapperFmt = `sh %s; rc=$?; echo $rc > %s; [ "$rc" -eq 0 ] && exec "$@"; exit $rc`

// setupWrapperArgv composes the child argv: the wrapper program, a $0
// placeholder, and then the real argv verbatim as "$@".
func setupWrapperArgv(scriptPath, rcPath string, argv []string) []string {
	return append([]string{"sh", "-c", fmt.Sprintf(setupWrapperFmt, scriptPath, rcPath), "wrapper"}, argv...)
}

// prepareSetup lands the setup script in dir, ready for the wrapper to run.
//
// Clearing a stale rc file is not housekeeping: /workspace is a persistent
// volume, so a cold-parked session that is started again still has the
// PREVIOUS boot's setup.rc sitting in it, and the watcher — which reports
// the first rc it sees — would announce that old verdict within a second of
// starting, while this boot's setup was still running.
func prepareSetup(dir, b64 string) error {
	script, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return fmt.Errorf("decoding RAINIER_SETUP_B64 (%d bytes): %w", len(b64), err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(dir, setupRCName)); err != nil && !os.IsNotExist(err) {
		return err
	}
	path := filepath.Join(dir, setupScriptName)
	if err := os.WriteFile(path, script, 0o755); err != nil {
		return err
	}
	// WriteFile's mode applies only when it CREATES the file; on a resumed
	// container the script is already there with whatever mode it had.
	return os.Chmod(path, 0o755)
}

// startSetupWatcher runs the watcher on its own goroutine and returns the
// channel its single verdict arrives on. The channel is buffered so the
// watcher never blocks on a connection that isn't there yet, and is never
// closed: a closed channel reads as a ready value, which serveConn's select
// would take for a delivered outcome.
func startSetupWatcher(ctx context.Context, s *session.Session, logPath string, timeout time.Duration) <-chan []byte {
	out := make(chan []byte, 1)
	go func() {
		if p := watchSetup(ctx, s.Stop, setupRCPath, logPath, setupPollInterval, timeout); p != nil {
			out <- p
		}
	}()
	return out
}

// watchSetup polls for the exit code the wrapper writes and returns the one
// control payload describing what happened — or nil if ctx ended first,
// which means sessiond is shutting down and there is no one left to tell.
//
// timeout <= 0 means no bound. sessiond does not have a default of its own:
// controld owns that policy (it sends 900s when an environment declares
// none, design §4.3), and inventing a second one here would mean two
// components disagreeing about when a setup is too slow.
func watchSetup(ctx context.Context, stop func(), rcPath, logPath string, poll, timeout time.Duration) []byte {
	var timedOut <-chan time.Time
	if timeout > 0 {
		t := time.NewTimer(timeout)
		defer t.Stop()
		timedOut = t.C
	}
	tick := time.NewTicker(poll)
	defer tick.Stop()
	for {
		// Checked before the first wait, so an rc file that is already there
		// (the wrapper finished before this goroutine started) is noticed at
		// once rather than a poll interval later.
		if rc, ok := readSetupRC(rcPath); ok {
			if rc == 0 {
				return controlPayload(relay.ControlEvent{Kind: "setup_done"})
			}
			return controlPayload(relay.ControlEvent{
				Kind: "setup_failed", RC: rc, Tail: logTail(logPath, setupTailBytes),
			})
		}
		select {
		case <-tick.C:
		case <-timedOut:
			// SIGTERM the wrapper: a setup that has run past its bound is
			// not going to be allowed to finish, and leaving it running
			// would leave a container burning a slot on work whose result
			// nobody will accept. rc -1 is the "no exit code exists" marker
			// — the script never got to write one.
			stop()
			return controlPayload(relay.ControlEvent{
				Kind: "setup_failed", RC: -1, Tail: setupTimedOutTail(timeout),
			})
		case <-ctx.Done():
			return nil
		}
	}
}

// setupTimedOutTail is the diagnostic a timed-out setup carries in place of
// script output, which by definition has no ending to quote.
func setupTimedOutTail(d time.Duration) string {
	return fmt.Sprintf("setup timed out after %ds", int(d/time.Second))
}

// setupTimeout reads RAINIER_SETUP_TIMEOUT (whole seconds, injected by the
// driver). Anything non-positive or unparseable means no bound — see
// watchSetup on why sessiond holds no default of its own.
func setupTimeout(v string) time.Duration {
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || n <= 0 {
		return 0
	}
	return time.Duration(n) * time.Second
}

// readSetupRC reads the wrapper's exit-code file, reporting ok=false while
// there is nothing complete to read.
//
// "Nothing complete" covers more than a missing file: `echo $rc > file`
// truncates before it writes, so a poll can catch the file existing and
// empty. Parsing that as 0 would announce setup_done for a setup that had
// not finished — the one failure mode this function exists to prevent.
func readSetupRC(path string) (int, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	rc, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		return 0, false
	}
	return rc, true
}

// logTail returns the last n bytes of the session's plain terminal output,
// decoded out of the event log's JSONL envelope (each entry's data is
// base64 in the file). This is what a user whose session never came up
// actually sees of why: the setup script's own output, streamed to the PTY
// and logged like any other output.
//
// It reads the file rather than the live *eventlog.Log because that log
// belongs to the session and exposes no tail; opening it a second time
// through eventlog.Open would be worse than reading bytes — that constructor
// TRUNCATES at the first unparseable line. Every read failure here yields an
// empty tail: a failure that reaches controld with no detail is still a
// reported failure, while one that panics or blocks is not.
func logTail(path string, n int) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64<<10), 16<<20)
	var tail []byte
	for sc.Scan() {
		var e eventlog.Entry
		// A line that doesn't parse is a partial write racing the session's
		// own appender, or a type this tail has no use for. Skip it.
		if json.Unmarshal(sc.Bytes(), &e) != nil || e.Type != "output" {
			continue
		}
		tail = append(tail, e.Data...)
		if len(tail) > n {
			tail = tail[len(tail)-n:]
		}
	}
	// The cut lands mid-rune as often as not, and terminal output is not
	// required to be UTF-8 at all; dropping what isn't keeps the tail a
	// string every consumer down the line can print.
	return strings.ToValidUTF8(string(tail), "")
}

// controlPayload encodes one control event for the relay's control channel.
func controlPayload(ev relay.ControlEvent) []byte {
	b, err := json.Marshal(ev)
	if err != nil {
		// Unreachable — every field is a plain scalar — but silence here
		// would be a setup outcome that vanished.
		log.Printf("encoding %s control event: %v", ev.Kind, err)
		return nil
	}
	return b
}

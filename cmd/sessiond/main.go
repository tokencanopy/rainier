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
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/coder/websocket"

	"github.com/tokencanopy/rainier/internal/eventlog"
	"github.com/tokencanopy/rainier/internal/reap"
	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/internal/server"
	"github.com/tokencanopy/rainier/internal/session"
	"github.com/tokencanopy/rainier/protocol/runner"
)

func main() {
	// The credential helper is this same binary, re-invoked by git from inside
	// the sandbox (the gitconfig sessiond writes names it). It is dispatched
	// before anything else in main runs: it spawns no child, serves no session,
	// and lives for one exchange, so a reaper, a flag set and a PTY would all be
	// machinery for a process that is about to print two lines and exit.
	if len(os.Args) > 1 && os.Args[1] == credentialHelperSubcommand {
		os.Exit(runCredentialHelper(os.Args[2:], agentSocketPath, credentialHelperTimeout,
			os.Stdin, os.Stdout, os.Stderr))
	}

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

	// The boot chain (design §4.3). An environment's setup script (Plan 4), the
	// repositories controld resolved, and the environment's per-boot init hook
	// all arrive as base64 in the environment block, injected by the driver.
	// When any of them is present the child is not the agent but a staged
	// wrapper that runs them in order and only execs the agent if every one
	// succeeded, and a watcher reports the outcome upstream over the relay's
	// control channel. When none is — a scratch session with no repos, or one
	// whose environment was already snapshot-cached, where the image IS the
	// finished setup — none of this happens and sessiond behaves exactly as it
	// did before Plan 5.
	//
	// Gated on relay mode: the control channel a stage's verdict travels on only
	// exists on a dialed conn, the credential the clone stage needs can only be
	// minted over it, and the local dev listener has no runnerd to reach.
	bootEnvironment := bootEnvFromOS()
	var stages []bootStage
	if bootEnvironment.any() && *dial == "" {
		// Can't happen from the driver (it injects RAINIER_DIAL alongside), but
		// a human running sessiond by hand with the vars set would otherwise
		// get an environment silently missing its setup and its repositories.
		log.Print("a boot chain was requested but sessiond has no runnerd to dial; skipping setup, clone and init")
	}

	// events is where every control payload this sessiond originates — the
	// setup verdict, a rejected credential, the agent's exit — waits for a
	// connection to carry it. Buffered so a producer never blocks on a
	// connection that isn't there yet, and shared by all of them so their
	// events keep their order.
	//
	// Made before the socket below rather than beside the session, because the
	// socket is one of those producers: the credential helper's erase report is
	// answered by queueing an event here (see agentSocketCall).
	events := make(chan []byte, pendingCap)

	// The session RPC's sandbox end, built BEFORE the session so it is already
	// listening when the chain's first git runs: the dispatcher serves the
	// requests controld sends down and originates the ones this side needs, and
	// the unix socket is how a process inside the container (the git credential
	// helper) reaches it. Handler registration happens here, at boot, for the
	// same reason.
	var rpc *rpcDispatcher
	// agents keeps this session's agent homes equal to the control plane's
	// custody for as long as the session lives. It stays nil when the create
	// carried no manifest — a session with no creator, or an older controld —
	// and then nothing in agents.go runs at all.
	var agents *agentSync
	if *dial != "" {
		rpc = newRPCDispatcher()
		startAgentSocket(context.Background(), agentSocketPath, rpc, events)
		// The workspace-inspection methods controld drives INTO this sandbox:
		// the session diff and the bounded push/pull (files.go). Registered
		// here, before the relay is serving, so a request arriving on the
		// conn's first frame finds its method already installed.
		registerFileHandlers(rpc, bootEnvironment)

		var chainEnv []envVar
		var err error
		stages, chainEnv, err = prepareBoot(setupDir, workspaceRoot, bootEnvironment)
		if err != nil {
			// Deliberately fatal. Running the agent anyway would hand a user a
			// session that looks healthy in an environment that was never built
			// — the one outcome worse than not starting. Dying is what makes
			// runnerd notice the container is gone and controld mark the
			// session failed.
			log.Fatalf("boot chain: %v", err)
		}
		argv = chainArgv(chainEnv, stages, argv)

		// The agent homes. The fetch that fills them runs on its own goroutine,
		// concurrently with the stages above, because it needs the relay that
		// only comes up once the session is running — the chain's agents stage
		// is what makes the agent wait for it (agents.go). The downward revoke
		// is registered beside the file handlers and for the same reason: a
		// logout can arrive on the connection's first frame.
		//
		// The manifest is decoded here and separately inside prepareBoot, the
		// way files.go decodes the repository list separately from the clone
		// stage: the two consume the same fact for different purposes, and
		// threading one through would couple the boot chain to the sync's
		// lifetime.
		if entries := agentEntries(bootEnvironment); len(entries) > 0 {
			agents = newAgentSync(rpc, entries, events)
			rpc.RegisterRPCHandler(runner.MethodRevokeAgentCredentials, agents.handleRevoke)
			agents.start(setupDir + "/" + agentsDoneName)
		}
	}

	s, err := session.New(session.Config{Argv: argv, Cols: *cols, Rows: *rows, LogPath: *logPath}, session.StartProc)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		<-s.Exited()
		code := s.ExitCode()
		log.Printf("child exited with code %d; sessiond stays up for viewers", code)
		// Reported upstream, not acted on: the session deliberately outlives
		// its child so viewers can still read the scrollback, and this is the
		// only place in the whole system that knows what the agent's exit
		// status was. In dev mode (no -dial) nothing drains the channel and
		// the buffer simply absorbs it.
		offerControl(events, childExitedPayload(code))
	}()

	// stageCtx stops the watcher on shutdown: a sessiond on its way out has
	// no business emitting a stage verdict for a session nobody will run.
	stageCtx, stopWatching := context.WithCancel(context.Background())
	defer stopWatching()

	term := make(chan os.Signal, 1)
	signal.Notify(term, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-term
		stopWatching()
		// Graceful: ask the agent to exit; the exit path closes viewers and the
		// process ends when the child is reaped. Give it a moment, then hard-exit.
		s.Stop()
		// The last thing an agent wrote is usually the thing worth keeping — a
		// login completed seconds before the session was torn down — and the
		// sync's two-second tick must not be what decides whether it survives.
		// Placed after Stop so the child already has its signal while this
		// runs, and bounded by the RPC's own timeout so a control plane that
		// has gone away cannot hold the shutdown open.
		if agents != nil {
			agents.close()
		}
		select {
		case <-s.Exited():
		case <-time.After(5 * time.Second):
		}
		os.Exit(0)
	}()

	if *dial != "" {
		if len(stages) > 0 {
			startStageWatcher(stageCtx, s.Stop, stages, *logPath, events)
		}
		dialLoop(context.Background(), *dial, *sessionID, s, events, rpc)
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
// events is where this sessiond's control payloads (the setup verdict, the
// agent's exit) arrive. They are threaded through the loop rather than sent
// from their watchers directly because the conn they have to travel on is
// this loop's, and there may not be one at the moment they land — see
// serveConn.
//
// rpc is the session-RPC dispatcher, which this loop owns the connection half
// of: it handles the control frames arriving on each conn, and holds that
// conn's sender for as long as it lives. The asymmetry with events is
// deliberate — an event queues across a reconnect, a request does not (see
// rpcConn).
func dialLoop(ctx context.Context, dial, sessionID string, s *session.Session, events <-chan []byte, rpc *rpcDispatcher) {
	backoff := time.Second
	var pending [][]byte // control payloads no connection has accepted yet
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
			// The inbound handler is the RPC dispatcher: the control frames
			// arriving the other way are the requests controld drives into
			// this sandbox (a diff, a push) and the answers to the ones this
			// side asked for. It is wired at construction rather than
			// afterwards so a request on the conn's first frame finds it.
			//
			// The sender is installed for this conn's lifetime and taken back
			// when the relay ends: an upstream call in flight fails at that
			// moment rather than waiting out a timeout for an answer that
			// cannot arrive, and a handler that outlives the conn answers over
			// the (now dead) conn its request came in on rather than over a
			// later one — see rpc.go.
			sender, errc := relay.ServeSessionWithControl(ctx, relay.WSConn(c), s, rpc.OnControl)
			rpc.online(sender)
			var relayErr error
			pending, relayErr = serveConn(sender, errc, events, pending)
			rpc.offline()
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

// pendingCap bounds the queue of control payloads waiting for a connection,
// and sizes the channel they arrive on.
//
// v0's vocabulary produces two — a setup verdict and one child exit — so
// eight is slack, not a working limit, and overflowing it means something
// upstream is wrong. It is bounded anyway because the alternative failure is
// worse than a dropped event: an unbounded queue turns "runnerd has been away
// for a while" into a sessiond whose memory grows without limit, and a
// session outliving everything else (spec §10) is the one thing in this
// system that must not be able to do that.
const pendingCap = 8

// serveConn waits for one connection's relay to end, delivering this
// session's control payloads over that connection's control channel as they
// land (or as they already sit queued). It returns the relay's error together
// with whatever is still undelivered, so the next dial picks up exactly where
// this one left off.
//
// Each payload is computed once by its watcher but may have to be offered
// more than once: a conn can die between "the setup finished" and "runnerd
// heard about it", and nothing else in the system knows that outcome exists —
// controld would wait forever for an event no one will resend. So a failed
// Send leaves the payload at the head of the queue for the next connection,
// and a successful one takes it off, which is what keeps it from being sent
// twice.
//
// The queue is FIFO so that what a connection delivers is the order these
// events were produced in, rather than an order that falls out of scheduling
// — and so that "drop the oldest" (appendPending) has a well-defined meaning
// at all. controld's arms are independent today and would survive a swap, but
// a channel whose ordering is accidental is not one a later event can be
// added to safely.
//
// It was a single pending payload until Plan 5, which is exactly one slot too
// few now that setup and child_exited can both be waiting: a failing setup
// produces BOTH (the wrapper writes its rc and then exits with it), so the
// pair is the normal case, not a corner.
func serveConn(sender controlSender, errc <-chan error, events <-chan []byte, pending [][]byte) ([][]byte, error) {
	for {
		for len(pending) > 0 {
			if err := sender.Send(pending[0]); err != nil {
				// This conn is already dying; errc says so below, and the
				// queue rides along to the next one. Not retried on this
				// conn: a Send that failed once on a dead conn fails again,
				// and the rest of the queue would fail behind it.
				log.Printf("control event not delivered (%v); retrying on the next connection", err)
				break
			}
			pending = pending[1:]
		}
		select {
		case err := <-errc:
			return pending, err
		case p := <-events:
			pending = appendPending(pending, p)
		}
	}
}

// appendPending enqueues p, dropping the OLDEST payload when the queue is
// already at pendingCap. Oldest-first because the newest news is the half a
// late-arriving consumer can still act on, and because the drop is logged
// loudly enough to find: reaching this at all means something produced more
// events than v0's vocabulary has.
func appendPending(q [][]byte, p []byte) [][]byte {
	q = append(q, p)
	if len(q) > pendingCap {
		dropped := len(q) - pendingCap
		log.Printf("control queue is at its %d cap; dropping the %d oldest undelivered event(s)", pendingCap, dropped)
		q = q[dropped:]
	}
	return q
}

// offerControl hands one payload to the delivery queue without ever blocking
// the goroutine that produced it. Those goroutines (the setup watcher, the
// child-exit watcher) have no connection to wait for and nothing else to do
// afterwards, so a blocking send would simply park one forever the first time
// the queue backed up. A nil payload — what controlPayload returns when
// encoding failed — is dropped rather than sent as an empty control frame.
func offerControl(out chan<- []byte, p []byte) {
	if p == nil {
		return
	}
	select {
	case out <- p:
	default:
		log.Printf("control queue full; dropping an undelivered event")
	}
}

// childExitedPayload encodes the event sessiond sends when the agent process
// ends: kind child_exited, carrying the exit status.
//
// It is an EVENT, not a request — ID stays 0, nobody answers it, and the
// session's own state is unaffected (sessiond deliberately outlives its child
// so viewers can read the scrollback). relay.ControlEvent's RC is omitempty,
// so a clean exit puts no rc on the wire at all; that is safe here and only
// here, because the field's zero value and the value being carried are the
// same number, and it decodes back to the 0 that means "exited cleanly".
func childExitedPayload(code int) []byte {
	return controlPayload(relay.ControlEvent{Kind: "child_exited", RC: code})
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
	// stagePollInterval is how often the watcher looks for a stage's rc file.
	// A stage runs for minutes; a second's granularity on noticing it
	// finished costs nothing and keeps the poll invisible.
	stagePollInterval = time.Second
	// stageTailBytes is how much of the session's output a failure carries
	// upstream. Enough for the last error and its context, small enough to
	// sit in a control frame and a database column without thought.
	stageTailBytes = 2 << 10
)

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
	return writeStageScript(dir, setupScriptName, setupRCName, script)
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

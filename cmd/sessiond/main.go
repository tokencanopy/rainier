package main

import (
	"context"
	"flag"
	"log"
	mrand "math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/coder/websocket"

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
	s, err := session.New(session.Config{Argv: argv, Cols: *cols, Rows: *rows, LogPath: *logPath}, session.StartProc)
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		<-s.Exited()
		log.Printf("child exited with code %d; sessiond stays up for viewers", s.ExitCode())
	}()

	term := make(chan os.Signal, 1)
	signal.Notify(term, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-term
		// Graceful: ask the agent to exit; the exit path closes viewers and the
		// process ends when the child is reaped. Give it a moment, then hard-exit.
		s.Stop()
		select {
		case <-s.Exited():
		case <-time.After(5 * time.Second):
		}
		os.Exit(0)
	}()

	// Env fallback: the Docker driver injects RAINIER_DIAL/RAINIER_SESSION as
	// env vars, not flags, so honor them whenever the flags were left empty.
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

	if *dial != "" {
		dialLoop(context.Background(), *dial, *sessionID, s)
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
func dialLoop(ctx context.Context, dial, sessionID string, s *session.Session) {
	backoff := time.Second
	for {
		c, _, err := websocket.Dial(ctx, dial+"?session="+sessionID, nil)
		if err == nil {
			c.SetReadLimit(16 << 20)
			log.Printf("sessiond registered with runnerd as %s", sessionID)
			backoff = time.Second
			if err := relay.ServeSession(ctx, relay.WSConn(c), s); err != nil {
				log.Printf("relay ended: %v; redialing", err)
			}
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

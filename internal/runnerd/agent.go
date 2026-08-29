// internal/runnerd/agent.go
package runnerd

import (
	"context"
	"errors"
	"log"
	mrand "math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"rainier/internal/driver"
	"rainier/internal/relay"
	"rainier/internal/rwire"
)

// AgentConfig configures RunAgent's outbound dial to controld.
type AgentConfig struct {
	ControldURL string // e.g. ws://host:9090 — /v1/runners/connect appended
	Token       string
	RunnerName  string
	ProxyURL    string // forwarded into every driver.Spec (egress R4)
}

// jitter returns a random duration in [0, d/2) — timing spread, not security.
func jitter(d time.Duration) time.Duration { return time.Duration(mrand.Int63n(int64(d / 2))) }

// nextBackoff doubles d and clamps the RESULT to a 30s cap. Mirrors
// cmd/sessiond's nextBackoff (package main there, so not importable): that
// function's doc comment explains why clamping the doubled value — not
// guarding the pre-doubled one — is what actually holds a 1s..30s cap
// instead of drifting to 32s and freezing there.
func nextBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

// RunAgent dials controld and serves its commands until ctx is canceled,
// redialing with jittered backoff (1s..30s cap) whenever the connection
// ends. It only returns once ctx is done — any other agentSession error is
// logged and retried, since a runner with no control conn is still useful
// on its local HTTP surface but should keep trying to phone home.
func (s *Server) RunAgent(ctx context.Context, cfg AgentConfig) error {
	s.proxyURL = cfg.ProxyURL
	backoff := time.Second
	for {
		err := s.agentSession(ctx, cfg)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Printf("controld conn ended: %v; redialing in %s", err, backoff)
		select {
		case <-time.After(backoff + jitter(backoff)):
		case <-ctx.Done():
			return ctx.Err()
		}
		backoff = nextBackoff(backoff)
	}
}

// agentSession dials controld once, sends the announce as the FIRST message
// on the conn, then serves rwire.ToRunner commands until the conn ends.
//
// agentSession does not return until its writer goroutine has actually
// stopped (writerDone.Wait(), gated by connCtx). Review round 1, finding 3:
// the writer used to have no exit signal other than a future failed send —
// a quiet disconnect (nothing left to write, ever) left it (and the `out`
// channel it closed over) running forever, once per reconnect. connCtx is
// this one connection's scope: canceling it — on return here, or from the
// writer's own write-failure branch below — makes coder/websocket close the
// underlying conn out from under any in-flight Read/Write using it, which is
// what actually unblocks a stalled reader when only the write direction has
// died (not just the writer itself).
func (s *Server) agentSession(ctx context.Context, cfg AgentConfig) error {
	hdr := http.Header{"Authorization": {"Bearer " + cfg.Token}}
	c, _, err := websocket.Dial(ctx, cfg.ControldURL+"/v1/runners/connect", &websocket.DialOptions{HTTPHeader: hdr})
	if err != nil {
		return err
	}
	defer c.CloseNow()
	c.SetReadLimit(16 << 20)

	connCtx, cancel := context.WithCancel(ctx)
	var writerDone sync.WaitGroup
	// Registered right after connCtx's cancel so it's unconditionally
	// deferred on every path out of this function (satisfies go vet's
	// lostcancel check) while still doing cancel-THEN-wait in that exact
	// order: canceling first is what gives writerDone.Wait() something to
	// wait FOR (either the writer noticing connCtx.Done(), or — if the
	// writer hasn't even started yet, e.g. the announce write below failed
	// first — an Add-less WaitGroup, whose Wait() is then a no-op).
	defer func() {
		cancel()
		writerDone.Wait()
	}()

	out := make(chan rwire.FromRunner, 64)
	send := func(m rwire.FromRunner) {
		cctx, ccancel := context.WithTimeout(ctx, 5*time.Second)
		m.Used, m.Total, _ = s.drv.Capacity(cctx) // best-effort; piggybacked on every message
		ccancel()
		select {
		case out <- m:
		default: // drop under absurd backlog; a later announce restores truth
		}
	}
	// OnEvent must be swapped atomically per connection and cleared on exit,
	// or a dead conn's closure keeps receiving events (and writing to an out
	// channel nothing drains any more). SetOnEvent/fireEvent are the
	// synchronized accessors — see the Server.onEvent field's doc comment.
	s.SetOnEvent(func(id, state string) { send(rwire.FromRunner{Type: "event", Session: id, State: state}) })
	defer s.SetOnEvent(nil)

	used, total, _ := s.drv.Capacity(ctx)
	ann := rwire.FromRunner{Type: "announce", Proto: rwire.Proto, Runner: cfg.RunnerName,
		Sessions: s.Announce(), Used: used, Total: total}
	if err := wsjson.Write(connCtx, c, ann); err != nil {
		return err
	}

	writerDone.Add(1)
	s.agentWriterCount.Add(1)
	go func() { // single writer: every FromRunner goes out over this one goroutine
		// Deferred in this order (LIFO: Add(-1) runs before Done()) so that
		// by the time writerDone.Wait() (agentSession's own teardown defer)
		// unblocks, agentWriterCount has already been decremented — without
		// this ordering, Wait() could return while the count still briefly
		// reads the old value, and a caller that starts a new writer right
		// after Wait() returns (RunAgent's next agentSession) could observe
		// 2 instead of the 0-or-1 the count is meant to guarantee.
		defer writerDone.Done()
		defer s.agentWriterCount.Add(-1)
		for {
			select {
			case m := <-out:
				if err := wsjson.Write(connCtx, c, m); err != nil {
					cancel() // a dead write direction means this connection
					// is done; unblock the reader below too, not just this
					// goroutine.
					return
				}
			case <-connCtx.Done():
				return
			}
		}
	}()

	for {
		var m rwire.ToRunner
		if err := wsjson.Read(connCtx, c, &m); err != nil {
			return err
		}
		go s.execute(ctx, m, send, cfg) // ops are slow (docker); never block the reader
	}
}

// execute runs one ToRunner command and reports its result via send. Called
// in its own goroutine per inbound message (agentSession's read loop) so a
// slow docker op never blocks the next command from being read.
func (s *Server) execute(ctx context.Context, m rwire.ToRunner, send func(rwire.FromRunner), cfg AgentConfig) {
	switch m.Type {
	case "create":
		var spec driver.Spec
		var allow []string
		if m.Spec != nil {
			spec = driver.Spec{Name: m.Spec.Name, Image: m.Spec.Image, Cmd: m.Spec.Cmd, EgressAllow: m.Spec.EgressAllow}
			allow = m.Spec.EgressAllow
		}
		// Idempotency lives inside CreateWithID's own putIfAbsent now, not a
		// separate reg.get check here — review round 1, finding 2: a
		// pre-check-then-put pair is two lock acquisitions with a window
		// between them where two racing creates for the same id (controld
		// resending one it's unsure landed) could both pass the check and
		// both reach drv.Create. errSessionExists means some caller (this
		// one or a concurrent one) already claimed the id; either way the
		// desired state — a session exists under this id — is reached, so
		// it's reported the same as a fresh success.
		err := s.CreateWithID(ctx, m.Session, spec, allow)
		ok := err == nil || errors.Is(err, errSessionExists)
		send(rwire.FromRunner{Type: "result", ReqID: m.ReqID, OK: ok, Detail: errTextUnless(err, errSessionExists)})
	case "suspend", "resume", "snapshot":
		ref, err := s.Op(ctx, m.Session, m.Type, m.Warm)
		detail := ref
		if err != nil {
			detail = err.Error()
		}
		send(rwire.FromRunner{Type: "result", ReqID: m.ReqID, OK: err == nil, Detail: detail})
	case "destroy":
		err := s.Delete(ctx, m.Session)
		// A missing session is still ok: the desired end state (no session)
		// is already reached, whether we just deleted it or this destroy
		// simply arrived after some other path already had.
		ok := err == nil || errors.Is(err, errNoSuchSession)
		send(rwire.FromRunner{Type: "result", ReqID: m.ReqID, OK: ok, Detail: errTextUnless(err, errNoSuchSession)})
	case "dial_attach":
		// Deliberately not in a goroutine of its own: agentSession's read
		// loop already runs one execute per inbound command precisely so a
		// long-running one can't block the next command from being read —
		// and this one runs for the whole life of the viewer's attach.
		s.dialAttachBack(ctx, m, cfg)
	default:
		log.Printf("agent: unknown command type %q", m.Type)
	}
}

// dialAttachBack completes controld's attach pairing from the runner side
// (design §4.2): dial the target URL controld parked the client's socket
// under, then feed that socket into the session's hub as an ordinary client
// attachment. The dial is outbound, like every other connection a runner
// makes (spec rule 3) — controld never dials in.
//
// The target URL is checked against this runner's own controld before it is
// dialed: the dial carries the fleet runner token, so an attacker who could
// get a dial_attach onto this connection (a compromised or misconfigured
// controld, a misrouted command) would otherwise have the runner post that
// token to a host of their choosing. Same origin or no dial.
//
// It dials BEFORE waiting for the hub: the pairing controld is holding has a
// TTL, and claiming it promptly is what keeps a client attaching to a
// still-booting container from being dropped while this end waits. The trade
// is one socket held for up to hubWait when the session never registers —
// cheap, and closing it is what tells controld's splice to drop the client.
//
// ctx is the agent's lifetime, not one control connection's: an attach must
// survive the control conn flapping and redialing underneath it.
func (s *Server) dialAttachBack(ctx context.Context, m rwire.ToRunner, cfg AgentConfig) {
	at := m.Attach
	if at == nil {
		log.Printf("agent: dial_attach for %s carried no attach block; ignoring", m.Session)
		return
	}
	if !sameControld(cfg.ControldURL, at.TargetURL) {
		log.Printf("agent: refusing dial_attach for %s: target %q is not this runner's controld (%q)",
			m.Session, at.TargetURL, cfg.ControldURL)
		return
	}

	hdr := http.Header{"Authorization": {"Bearer " + cfg.Token}}
	// The timeout covers the handshake only, and deliberately sits under
	// controld's pairing TTL: a blackholed target must not park this
	// goroutine (and its socket) until the agent shuts down. Canceling after
	// a successful handshake is safe — net/http hands the upgraded
	// connection to the caller and stops watching the request context.
	dialCtx, cancel := context.WithTimeout(ctx, attachDialTimeout)
	c, _, err := websocket.Dial(dialCtx, at.TargetURL, &websocket.DialOptions{HTTPHeader: hdr})
	cancel()
	if err != nil {
		// Nothing to report back: dial_attach is fire-and-forget, and
		// controld's pairing TTL closes the client that was waiting.
		log.Printf("agent: attach-back dial for %s: %v", m.Session, err)
		return
	}
	c.SetReadLimit(16 << 20)

	hub, ok := s.waitHub(m.Session)
	if !ok {
		log.Printf("agent: attach-back for %s: session never registered a hub", m.Session)
		c.CloseNow()
		return
	}
	// Blocks for the life of the attach; the hub owns the conn's teardown on
	// either side dying (its readLoop closes clients when the session conn
	// dies, AttachClient closes the attachment when the client does).
	hub.AttachClient(ctx, relay.WSConn(c), at.Since, at.Cols, at.Rows)
}

// attachDialTimeout bounds one attach-back handshake. It sits below
// controld's 15s pairing TTL: past that the client is already gone, so a
// dial still in flight has nothing left to connect to.
const attachDialTimeout = 5 * time.Second

// sameControld reports whether target names the same ws(s) origin as this
// runner's configured controld — scheme, host, and port, with the default
// port filled in and http(s) read as its ws(s) equivalent (controld derives
// target_url from its own http(s) ExternalURL). A ws target for a wss
// controld is a downgrade, not a match: it would put the fleet token on the
// wire in the clear.
func sameControld(controldURL, target string) bool {
	want, ok := wsOrigin(controldURL)
	if !ok {
		return false
	}
	got, ok := wsOrigin(target)
	return ok && got == want
}

// wsOrigin renders raw as a comparable "scheme://host:port", normalizing
// http→ws and https→wss. Anything else — another scheme, an unparseable URL,
// a missing host — is not an origin this runner will dial.
func wsOrigin(raw string) (string, bool) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", false
	}
	var scheme, defaultPort string
	switch strings.ToLower(u.Scheme) {
	case "ws", "http":
		scheme, defaultPort = "ws", "80"
	case "wss", "https":
		scheme, defaultPort = "wss", "443"
	default:
		return "", false
	}
	host := strings.ToLower(u.Hostname())
	if host == "" {
		return "", false
	}
	port := u.Port()
	if port == "" {
		port = defaultPort
	}
	return scheme + "://" + host + ":" + port, true
}

// errTextUnless returns err's message, or "" if err is nil or matches
// sentinel — used by destroy (errNoSuchSession) and create
// (errSessionExists), where that particular error is a success case, not a
// failure detail worth surfacing.
func errTextUnless(err, sentinel error) string {
	if err == nil || errors.Is(err, sentinel) {
		return ""
	}
	return err.Error()
}

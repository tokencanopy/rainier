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
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/tokencanopy/rainier/internal/driver"
	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// AgentConfig configures RunAgent's outbound dial to controld.
type AgentConfig struct {
	ControldURL string // e.g. ws://host:9090 — /v0/runners/connect appended
	Token       string
	RunnerName  string
	ProxyURL    string // forwarded into every driver.Spec (egress R4)
	// Capabilities are the portable capabilities this runner announces —
	// claims about itself and nothing else (a GPU it has, a rootless docker
	// it runs). They are passed through verbatim: controld validates them
	// and answers with the set it accepted. Empty announces none.
	Capabilities []string
	// PingInterval and PingTimeout are the runner's half of the heartbeat:
	// how often it asks the control plane whether the socket is still
	// answered, and how long it waits before treating silence as a gone
	// peer and redialing. Zero means the defaults below.
	PingInterval time.Duration
	PingTimeout  time.Duration
	// RedialBackoffMin is the floor of RunAgent's redial delay: the wait
	// before the first redial, and the value the delay returns to once a
	// connection has held (see RedialResetAfter). Zero means the default below;
	// anything else is clamped to [minRedialBackoff, maxRedialBackoff],
	// because jitter cannot halve a sub-nanosecond bound and a floor above
	// the cap would put every redial above the ceiling RunAgent documents.
	RedialBackoffMin time.Duration
	// RedialResetAfter is how long an established connection must last for
	// its end to reset the redial delay to that floor. It is the brake on a
	// peer that accepts and immediately drops: without it, two runnerd
	// processes sharing one runner name evict each other from the control
	// plane's registry forever at the floor, each eviction costing a
	// generation mint and a fleet write. Zero means the default below.
	RedialResetAfter time.Duration
}

// The default liveness bounds. Together they bound how long a runner keeps
// believing in a control connection its peer has stopped answering: the
// control plane's heartbeat bound tells the cell when a runner is gone, and
// this tells the runner when the cell is.
const (
	defaultAgentPingInterval = 20 * time.Second
	defaultAgentPingTimeout  = 10 * time.Second
)

// The redial bounds.
//
// One jittered second is long enough that a control plane the whole fleet is
// redialing at once (a roll, a restart) is not answering every runner in the
// same instant, and short enough that a routine disconnect costs a session no
// meaningful reachability. Ten seconds of held connection is far below any
// routine lifetime — an hourly expiry, a deploy's minutes — and far above the
// milliseconds an eviction flap takes to come back around, which is the whole
// job of that bound.
//
// minRedialBackoff is the smallest floor this package will use rather than a
// validation error: jitter halves its argument and math/rand panics on a
// non-positive bound, so a floor of a nanosecond would take the process down
// on its first redial. maxRedialBackoff is the ceiling nextBackoff clamps to,
// named here because the floor is held to it as well.
const (
	defaultRedialBackoffMin = time.Second
	defaultRedialResetAfter = 10 * time.Second
	minRedialBackoff        = time.Millisecond
	maxRedialBackoff        = 30 * time.Second
)

func (cfg AgentConfig) pingInterval() time.Duration {
	if cfg.PingInterval > 0 {
		return cfg.PingInterval
	}
	return defaultAgentPingInterval
}

func (cfg AgentConfig) pingTimeout() time.Duration {
	if cfg.PingTimeout > 0 {
		return cfg.PingTimeout
	}
	return defaultAgentPingTimeout
}

func (cfg AgentConfig) redialBackoffMin() time.Duration {
	switch d := cfg.RedialBackoffMin; {
	case d <= 0:
		return defaultRedialBackoffMin
	case d < minRedialBackoff:
		return minRedialBackoff
	case d > maxRedialBackoff:
		return maxRedialBackoff
	default:
		return d
	}
}

func (cfg AgentConfig) redialResetAfter() time.Duration {
	if cfg.RedialResetAfter > 0 {
		return cfg.RedialResetAfter
	}
	return defaultRedialResetAfter
}

// resetsBackoff reports whether a connection that reached establishment at
// establishedAt — zero if controld never accepted it — and is ending now
// should return RunAgent's redial delay to its floor. Both halves are
// required: the accept says the connection was good, and the holding says it
// was good for long enough that coming straight back is not itself the
// problem. See RunAgent.
func (cfg AgentConfig) resetsBackoff(establishedAt time.Time) bool {
	return !establishedAt.IsZero() && time.Since(establishedAt) >= cfg.redialResetAfter()
}

// agentSessionState is one control connection's negotiated state. Today that
// is the runner generation controld granted in its accept: zero until one
// arrives (an unaccepted connection claims no authority, which the wire
// spells as "the connection's"), and read by the writer on every message the
// runner sends afterwards. Atomic because the accept is handled on the
// reader while events fire from session goroutines.
type agentSessionState struct {
	generation atomic.Uint64
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
	if d > maxRedialBackoff {
		d = maxRedialBackoff
	}
	return d
}

// RunAgent dials controld and serves its commands until ctx is canceled,
// redialing with jittered backoff (floor..30s cap) whenever the connection
// ends. It only returns once ctx is done — any other agentSession error is
// logged and retried, since a runner with no control conn is still useful
// on its local HTTP surface but should keep trying to phone home.
//
// The backoff returns to its floor when a connection was ESTABLISHED and
// HELD. Established means one thing: controld answered this connection's
// announce with an accept — the control plane's own statement that the fleet
// registered this runner, at a generation, on this socket, which is the point
// past which its sessions are dispatchable. Held means that connection then
// lasted RedialResetAfter. Anything else leaves the delay doubling to the cap.
//
// Without any reset the delay only ever grows: a process that has been up
// long enough to see six disconnects redials at the 30s cap from then on,
// however healthy each connection in between was. Under a control plane that
// routinely ends connections — an hourly credential or lease expiry, a
// rolling deploy — that turns a sub-second reconnect into a 30-45s hole in
// which every session on this runner reports unreachable and the scheduler
// can place nothing on it. Establishment is what says the previous connection
// was fine and the next one has no reason not to be.
//
// The accept and not merely a completed dial, because several of controld's
// refusals land after the websocket handshake (a name the credential does not
// cover, a generation its store cannot mint, a registration the fleet
// refuses, a failed reconcile) and a store outage puts the whole fleet on
// that path at once. Resetting on a dial would answer that outage with the
// entire fleet redialing at the floor for its duration.
//
// And the holding, because an accept alone can be handed out faster than it
// means anything. The control plane evicts a runner's previous connection
// whenever a new one registers under the same name, so two runnerd processes
// sharing one runner name accept-and-evict each other indefinitely — and
// every one of those dials costs a generation mint and a fleet write. A reset
// on the bare accept would hold that loop at the floor forever, turning a
// misconfiguration that used to decay to one exchange per 30s into a durable
// write storm. Requiring the connection to have lasted first leaves the
// runner that is genuinely being served resetting (any routine lifetime is
// orders of magnitude past the bound) and the flapping pair backing off.
func (s *Server) RunAgent(ctx context.Context, cfg AgentConfig) error {
	s.proxyURL = cfg.ProxyURL
	backoff := cfg.redialBackoffMin()
	for {
		established, err := s.agentSession(ctx, cfg)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if established {
			backoff = cfg.redialBackoffMin()
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
// on the conn, then serves runner.ToRunner commands until the conn ends. It
// reports whether the connection both reached establishment — controld's
// accept arrived — and held it long enough to reset RunAgent's redial delay
// (cfg.resetsBackoff). A connection that never got that far reports false
// however far into the handshake it died.
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
func (s *Server) agentSession(ctx context.Context, cfg AgentConfig) (established bool, err error) {
	hdr := http.Header{"Authorization": {"Bearer " + cfg.Token}}
	c, _, err := websocket.Dial(ctx, cfg.ControldURL+"/v0/runners/connect", &websocket.DialOptions{HTTPHeader: hdr})
	if err != nil {
		return false, err
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

	ag := &agentSessionState{}
	out := make(chan runner.FromRunner, 64)
	send := func(m runner.FromRunner) {
		cctx, ccancel := context.WithTimeout(ctx, 5*time.Second)
		m.Used, m.Total, _ = s.drv.Capacity(cctx) // best-effort; piggybacked on every message
		ccancel()
		// The two counts that turn "no free capacity" into something a person
		// can act on: how much of `used` is a working agent and how much is a
		// sandbox whose agent has finished. They ride the same message the
		// used/total pair already does, from the registry rather than the
		// driver — docker cannot say whether a container's child is still
		// running; only sessiond's report can, and this runner keeps it.
		m.Active, m.IdleExited = s.reg.counts()
		// The two generations every report carries (D19), stamped in the one
		// place every report passes through. The runner's own is whatever
		// controld granted this connection; the session's is the one its
		// create carried, which the registry has kept beside the sandbox.
		// Both are zero until they are known, and zero fences nothing.
		switch m.Type {
		case "event":
			m.Generation = ag.generation.Load()
			if m.Session != "" {
				m.PlacementGeneration = s.reg.placementGeneration(m.Session)
			}
		case "result":
			m.Generation = ag.generation.Load()
		}
		select {
		case out <- m:
		default: // drop under absurd backlog; a later announce restores truth
		}
	}
	// OnEvent must be swapped atomically per connection and cleared on exit,
	// or a dead conn's closure keeps receiving events (and writing to an out
	// channel nothing drains any more). SetOnEvent/fireEvent are the
	// synchronized accessors — see the Server.onEvent field's doc comment.
	s.SetOnEvent(func(id, state, detail string) {
		send(runner.FromRunner{Type: "event", Session: id, State: state, Detail: detail})
	})
	defer s.SetOnEvent(nil)
	// The same per-connection swap for the session RPC's upward direction: a
	// sandbox's request, or its response to one controld sent down, becomes a
	// "session_req" on this connection. Cleared on exit for the same reason —
	// a dead conn's closure would otherwise keep queueing onto an `out`
	// channel nothing drains.
	s.SetOnSessionRPC(func(id string, env runner.RPCEnvelope) {
		send(runner.FromRunner{Type: "session_req", Session: id, RPC: &env})
	})
	defer s.SetOnSessionRPC(nil)

	used, total, _ := s.drv.Capacity(ctx)
	active, idleExited := s.reg.counts()
	ann := runner.FromRunner{Type: "announce", Proto: runner.ProtocolVersion, Runner: cfg.RunnerName,
		Sessions: s.Announce(), Used: used, Total: total, Active: active, IdleExited: idleExited,
		Capabilities: cfg.Capabilities}
	if err := wsjson.Write(connCtx, c, ann); err != nil {
		return false, err // nothing can have been accepted before the announce
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

	// Liveness. The read loop below only learns the peer is gone when the
	// socket returns an error, and behind a load balancer it may never: a
	// control-plane instance replaced underneath us leaves this end half-open,
	// and a runner that waits for the error waits for the kernel's keepalive,
	// which is minutes to hours. So this end asks. A ping the peer does not
	// answer within the bound ends the session, and RunAgent redials.
	writerDone.Add(1)
	go func() {
		defer writerDone.Done()
		ticker := time.NewTicker(cfg.pingInterval())
		defer ticker.Stop()
		for {
			select {
			case <-connCtx.Done():
				return
			case <-ticker.C:
				pctx, pcancel := context.WithTimeout(connCtx, cfg.pingTimeout())
				err := c.Ping(pctx)
				pcancel()
				if err != nil && connCtx.Err() == nil {
					log.Printf("controld conn silent for %s; treating it as gone", cfg.pingTimeout())
					cancel()
					return
				}
			}
		}
	}()
	// When controld's accept arrived, and the zero time until it does. A
	// plain local rather than state on ag: the accept is handled on this
	// goroutine (below), and this goroutine is the one that returns, so
	// nothing else ever reads or writes it.
	var establishedAt time.Time
	for {
		var m runner.ToRunner
		if err := wsjson.Read(connCtx, c, &m); err != nil {
			return cfg.resetsBackoff(establishedAt), err
		}
		if m.Type == "accept" {
			if establishedAt.IsZero() {
				establishedAt = time.Now()
			}
			// Handled on the reader itself, not in a goroutine of its own:
			// the accept is this connection's negotiated state, and every
			// message read after it must already carry the generation it
			// grants. A goroutine would make that ordering a race.
			s.execute(ctx, m, send, cfg, ag)
			continue
		}
		go s.execute(ctx, m, send, cfg, ag) // ops are slow (docker); never block the reader
	}
}

// execute runs one ToRunner command and reports its result via send. Called
// in its own goroutine per inbound message (agentSession's read loop) so a
// slow docker op never blocks the next command from being read — except for
// the "accept", which the reader runs inline because it is negotiation, not
// work, and everything read after it depends on it having happened.
func (s *Server) execute(ctx context.Context, m runner.ToRunner, send func(runner.FromRunner), cfg AgentConfig, ag *agentSessionState) {
	switch m.Type {
	case "accept":
		// controld's answer to the announce, and the first thing it sends.
		// It grants this connection a generation; from here on every result
		// and event says which authority produced it. The capabilities are
		// informational — the set controld will schedule on, which is this
		// runner's own claims minus anything it refused.
		ag.generation.Store(m.Generation)
		log.Printf("agent: accepted at generation %d with %d capabilities", m.Generation, len(m.Capabilities))
	case "create":
		var spec driver.Spec
		var allow []string
		if m.Spec != nil {
			// Everything here is carried straight through to the driver:
			// controld resolved it all — the environment's scripts and their
			// timeouts, its declared vars plus decrypted secret values, the
			// repositories this session clones and the identity its commits
			// carry — and this runner's only job is to hand it to the
			// container. Nothing here logs Env; its values are secrets as
			// often as not.
			spec = driver.Spec{
				Name: m.Spec.Name, Image: m.Spec.Image, Cmd: m.Spec.Cmd, EgressAllow: m.Spec.EgressAllow,
				Setup: m.Spec.Setup, SetupTimeoutSec: m.Spec.SetupTimeoutSec, Env: m.Spec.Env,
				Repos: driverRepos(m.Spec.Repos),
				Init:  m.Spec.Init, InitTimeoutSec: m.Spec.InitTimeoutSec,
				GitAuthorName: m.Spec.GitAuthorName, GitAuthorEmail: m.Spec.GitAuthorEmail,
				Home: driverHome(m.Spec.Home),
			}
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
		// The create's placement generation travels with the sandbox, not
		// with this command: every event about this session echoes it, long
		// after the create is over.
		err := s.createWithID(ctx, m.Session, spec, allow, m.PlacementGeneration)
		ok := err == nil || errors.Is(err, errSessionExists)
		send(runner.FromRunner{Type: "result", ReqID: m.ReqID, OK: ok, Detail: errTextUnless(err, errSessionExists)})
		if errors.Is(err, errSessionExists) {
			// The result alone says "it exists", not what it is doing — and a
			// controld that requeued a create it never heard back on, then
			// re-placed it here, has a row that only leaves `creating` on an
			// event or the next announce. Re-fire the session's current state
			// now so it converges immediately instead of waiting for a
			// reconnect that may be minutes away. Sent after the result, so
			// controld settles the dispatch before it sees the state.
			s.reannounce(m.Session, send)
		}
	case "suspend", "resume":
		err := s.Op(ctx, m.Session, m.Type, m.Warm)
		send(runner.FromRunner{Type: "result", ReqID: m.ReqID, OK: err == nil, Detail: errText(err)})
	case "snapshot":
		// m.Ref is controld's content-addressed environment ref
		// (rainier-env:<envID>-<setupHash>), passed through untouched — see
		// driver.Driver.Snapshot for why the runner must not rename it. An
		// empty one still means "driver, mint a tag", which is what the local
		// HTTP surface sends; controld always names one.
		//
		// The detail carries the FINAL ref, which is the whole point on this
		// path: controld records what came back, so a driver-generated ref
		// reaches it just as an echoed one does.
		ref, err := s.OpSnapshot(ctx, m.Session, m.Ref)
		detail := ref
		if err != nil {
			detail = err.Error()
		}
		send(runner.FromRunner{Type: "result", ReqID: m.ReqID, OK: err == nil, Detail: detail})
	case "prepull":
		// Advisory and session-less: controld dispatches a prepull without a
		// pending entry to correlate against (design §4.3 — it is warming an
		// image, not driving a session's state machine), so this case must not
		// depend on m.Session or m.ReqID being set. The result is sent for the
		// same reason any other one is — it goes out over the writer's `out`
		// channel, which needs nothing on controld's side to be waiting for it
		// — and is informational: the ref on success, the reason on failure.
		//
		// `docker pull` of a cold image runs for minutes, and this deliberately
		// does NOT get a goroutine of its own: execute ALREADY runs one per
		// inbound command (agentSession's read loop does `go s.execute(...)`)
		// precisely so a slow docker op can't stall the next command. Nesting a
		// second goroutine here would buy nothing and cost the honest
		// "one command, one goroutine" accounting. Pinned by
		// TestAgentPrepullDoesNotBlockTheReader.
		err := s.drv.Prepull(ctx, m.Ref)
		detail := m.Ref
		if err != nil {
			detail = err.Error()
			// One informative line, not an error: on a multi-VM fleet this
			// failure is the NORMAL outcome and not a fault to chase. v0 has
			// no registry (design §6), so a rainier-env: ref names an image
			// that exists only in the local store of the runner that
			// committed it — every other runner's `docker pull` of it must
			// fail, by construction, until the registry upgrade lands. Nothing
			// breaks: a session placed on a non-holder runner is dispatched
			// with the setup script and rebuilds the environment itself, which
			// is exactly what would have happened with no prepull at all.
			log.Printf("agent: prepull %s did not land (expected without a registry; sessions here rebuild from setup): %v", m.Ref, err)
		}
		send(runner.FromRunner{Type: "result", ReqID: m.ReqID, OK: err == nil, Detail: detail})
	case "destroy":
		err := s.Delete(ctx, m.Session)
		// A missing session is still ok: the desired end state (no session)
		// is already reached, whether we just deleted it or this destroy
		// simply arrived after some other path already had.
		ok := err == nil || errors.Is(err, errNoSuchSession)
		send(runner.FromRunner{Type: "result", ReqID: m.ReqID, OK: ok, Detail: errTextUnless(err, errNoSuchSession)})
	case "remove_workspace":
		// The reclaim controld sends after a session it holds is explicitly
		// removed — including a crash-dead one, whose container went long ago
		// and whose volume the crash path deliberately kept. It names a
		// SESSION, not a handle, for exactly that reason.
		//
		// An absent volume is an ok result, not a failure: controld sends this
		// on every explicit rm, including the ones where the destroy that ran
		// first already took it. controld doesn't wait on the answer either
		// (req_id 0), so this result exists to be logged, not correlated.
		err := s.RemoveWorkspace(ctx, m.Session)
		if err != nil {
			log.Printf("agent: remove_workspace for %s: %v", m.Session, err)
		}
		send(runner.FromRunner{Type: "result", ReqID: m.ReqID, OK: err == nil, Detail: errText(err)})
	case "session_rpc":
		// The one command type this runner does not execute: it carries a
		// session RPC bound for the sandbox, and the answer (when there is
		// one) comes from inside that container, not from here. So there is no
		// result to send — m.ReqID is zero on this type — and correlation
		// lives entirely in the envelope's own id.
		s.forwardSessionRPC(m, send)
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

// forwardSessionRPC carries one envelope from controld into the sandbox it
// names. The runner reads nothing inside it: the method decides whether the
// frame lands as a "req:<method>" or a "resp", and the payload is passed
// through untouched.
//
// A request that cannot be delivered is answered with a failure rather than
// dropped, because the far end is holding a pending entry either way — the
// difference is whether it learns now or waits out its whole OpTimeout for an
// answer that was never coming. A RESPONSE that cannot be delivered is only
// logged: answering an answer is meaningless, and the sandbox that asked has
// already lost its own pending entry along with the conn.
func (s *Server) forwardSessionRPC(m runner.ToRunner, send func(runner.FromRunner)) {
	if m.RPC == nil {
		log.Printf("agent: session_rpc for %s carried no envelope; ignoring", m.Session)
		return
	}
	env := *m.RPC
	if env.ID == 0 || env.Method == "" {
		log.Printf("agent: session_rpc for %s carried no id or method; ignoring", m.Session)
		return
	}
	err := s.sendSessionRPC(m.Session, env)
	if err == nil {
		return
	}
	log.Printf("agent: forwarding %q to session %s: %v", env.Method, m.Session, err)
	if env.Method == "resp" {
		return
	}
	send(runner.FromRunner{Type: "session_req", Session: m.Session,
		RPC: &runner.RPCEnvelope{ID: env.ID, Method: "resp", Payload: rpcErrorPayload(err.Error())}})
}

// reannounce fires one session's current state as an event, using the same
// rendering the announce uses (announceState, off a locked registry
// snapshot). It reports nothing for a session that vanished in the meantime,
// or one still "starting" — that one's own create result speaks for it, and
// announceState omits it for the same reason.
//
// controld acts on "running" today and merely logs the suspended states as
// unrecognized; the mapping is shared with Announce anyway so the two can't
// drift as that vocabulary grows.
func (s *Server) reannounce(id string, send func(runner.FromRunner)) {
	e, ok := s.reg.snapshot(id)
	if !ok {
		return
	}
	state, ok := announceState(e)
	if !ok {
		return
	}
	send(runner.FromRunner{Type: "event", Session: id, State: state})
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
func (s *Server) dialAttachBack(ctx context.Context, m runner.ToRunner, cfg AgentConfig) {
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
	// The same idle accounting the local /attach front keeps, for the same
	// reason: a session with a viewer on it is not idle whichever door that
	// viewer came through, and the idle timer restarts when they leave.
	s.reg.attachStarted(m.Session)
	defer s.reg.attachEnded(m.Session, s.now())
	// Blocks for the life of the attach; the hub owns the conn's teardown on
	// either side dying (its readLoop closes clients when the session conn
	// dies, AttachClient closes the attachment when the client does).
	hub.AttachClient(ctx, relay.WSConn(c), relay.Open{
		Since: at.Since, Cols: at.Cols, Rows: at.Rows,
		Mode: at.Mode, Generation: at.Generation,
	})
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

// errText returns err's message, or "" if err is nil — the Detail a result
// carries for an op whose success has nothing else to report (suspend,
// resume). Snapshot and prepull don't use it: their success detail is the
// image ref, not silence.
func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// errTextUnless returns err's message, or "" if err is nil or matches
// sentinel — used by destroy (errNoSuchSession) and create
// (errSessionExists), where that particular error is a success case, not a
// failure detail worth surfacing.
func errTextUnless(err, sentinel error) string {
	if errors.Is(err, sentinel) {
		return ""
	}
	return errText(err)
}

// driverRepos converts the wire's repo list into the driver's, field for
// field. The two types are deliberately identical and deliberately separate
// (runner is the control-plane vocabulary, driver.Spec is the sandbox
// boundary), so this hop is a copy and nothing else — no defaulting, no
// validation, no reordering. A nil list stays nil: a session that clones
// nothing must not arrive at the driver carrying an empty instruction.
func driverRepos(repos []runner.RepoSpec) []driver.RepoSpec {
	if len(repos) == 0 {
		return nil
	}
	out := make([]driver.RepoSpec, len(repos))
	for i, r := range repos {
		out[i] = driver.RepoSpec{
			Owner: r.Owner, Name: r.Name, BaseBranch: r.BaseBranch,
			SessionBranch: r.SessionBranch, Dir: r.Dir,
		}
	}
	return out
}

// driverHome converts the wire's agent home into the driver's, for the same
// reason and in the same way driverRepos converts the repo list: two
// deliberately identical, deliberately separate types across the boundary
// between the control-plane vocabulary and the sandbox. A copy and nothing
// else — this runner does not parse the volume name (it is a hash, and there
// is nothing in it to read), does not default the path, and does not check
// whether either exists.
//
// A nil home stays nil: a session whose create carried none — one with no
// creator, or one from a control plane older than the field — must reach the
// driver with nothing to mount rather than with an empty mount instruction.
func driverHome(h *runner.HomeMount) *driver.HomeMount {
	if h == nil {
		return nil
	}
	return &driver.HomeMount{Volume: h.Volume, Path: h.Path}
}

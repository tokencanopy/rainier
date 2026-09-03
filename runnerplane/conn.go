package runnerplane

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// runnerConn is one runnerd control connection. Exactly one goroutine reads
// it (the HTTP handler) and exactly one writes it (the writer goroutine
// draining out), so the websocket itself needs no lock; mu guards only the
// pending table, which dispatchers touch from arbitrary goroutines.
type runnerConn struct {
	// binding is the authoritative scope this connection acts in: the host
	// named the workspace and pool, and the runner id is the one the host
	// bound or the one the announce claimed.
	binding Binding
	name    string
	ws      *websocket.Conn
	out     chan runner.ToRunner
	// gen is the runner generation THIS connection registered under. Every
	// event read off this socket is stamped with it rather than with the
	// runner's current generation, so a message that outlives its own
	// connection is fenced by the fleet service (ErrStale) instead of being
	// applied under the generation of the socket that replaced it.
	gen uint64
	// caps is the capability list this connection registered: the plane's
	// spelling of the runner's own name plus what its announce claimed. It is
	// held here because the heartbeat rewrites the whole runner row, and a
	// heartbeat that rebuilt the list from the name alone would erase the
	// runner's own claims the first time it said anything at all. Written
	// once, in connectRunner, before any goroutine reads it.
	caps []string

	mu      sync.Mutex
	pending map[uint64]chan runner.FromRunner

	seq atomic.Uint64
	// srpc is the pending table for the session RPCs this plane sent INTO the
	// sandboxes this runner holds. Separate from pending above because the two
	// correlate different things — see srpcTable.
	srpc *srpcTable
	done chan struct{}

	closeOnce sync.Once
}

func newRunnerConn(b Binding, ws *websocket.Conn) *runnerConn {
	return &runnerConn{
		binding: b,
		name:    string(b.RunnerID),
		ws:      ws,
		out:     make(chan runner.ToRunner, runnerSendQueue),
		pending: map[uint64]chan runner.FromRunner{},
		srpc:    newSRPCTable(),
		done:    make(chan struct{}),
	}
}

// shutdown closes the connection exactly once. Closing done is also how every
// dispatch still waiting on this conn learns it will never be answered — they
// select on it — so no separate "fail the pending table" pass is needed or
// possible to race with.
func (rc *runnerConn) shutdown() {
	rc.closeOnce.Do(func() {
		close(rc.done)
		if rc.ws != nil {
			rc.ws.CloseNow()
		}
	})
}

// enqueue hands m to the writer goroutine without ever blocking the caller: a
// dead conn or a full backlog is an error, not a wait.
func (rc *runnerConn) enqueue(m runner.ToRunner) error {
	select {
	case <-rc.done:
		return fmt.Errorf("runner %s: connection closed: %w", rc.name, control.ErrUnavailable)
	default:
	}
	select {
	case rc.out <- m:
		return nil
	case <-rc.done:
		return fmt.Errorf("runner %s: connection closed: %w", rc.name, control.ErrUnavailable)
	default:
		return fmt.Errorf("runner %s: send queue full: %w", rc.name, control.ErrUnavailable)
	}
}

// deliver hands a result to the dispatcher waiting on its ReqID, reporting
// whether anyone was. The channel is buffered by one and the dispatcher
// always removes its entry, so this never blocks and a duplicate result for
// the same id is dropped rather than stalling the reader.
func (rc *runnerConn) deliver(m runner.FromRunner) bool {
	rc.mu.Lock()
	ch, ok := rc.pending[m.ReqID]
	rc.mu.Unlock()
	if !ok {
		return false
	}
	select {
	case ch <- m:
	default:
	}
	return true
}

// writeLoop is the connection's single writer.
func (p *Plane) writeLoop(ctx context.Context, rc *runnerConn) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-rc.done:
			return
		case m := <-rc.out:
			wctx, cancel := context.WithTimeout(ctx, runnerWriteTimeout)
			err := wsjson.Write(wctx, rc.ws, m)
			cancel()
			if err != nil {
				p.logf("write to runner %s: %v", rc.name, err)
				// A dead write direction means the connection is done; this
				// also unblocks the reader, which may otherwise sit on a
				// socket that will never produce another frame.
				rc.shutdown()
				return
			}
		}
	}
}

// ---------------------------------------------------------------------------
// the transport
// ---------------------------------------------------------------------------

// transport is control.RunnerTransport over the plane's connection map. It
// owns nothing — the connections are registered and retired by the plane —
// and it never reads a runner's free text into an error. Every failure a
// caller sees is control.ErrUnavailable wrapped with our own words: the
// command type and the runner's name, both of which are ours.
type transport struct{ p *Plane }

var _ control.RunnerTransport = transport{}

// Connected reports whether id currently holds a control connection here.
func (t transport) Connected(pool control.PoolID, id control.RunnerID) bool {
	return t.p.connIn(pool, string(id)) != nil
}

// Dispatch sends m to id and waits for its answer, bounded by OpTimeout, the
// connection's lifetime, and ctx. Two message kinds correlate their answers
// differently: an ordinary command is answered by a "result" carrying the
// ReqID assigned here, while a "session_rpc" is answered by a "session_req"
// whose envelope ID the caller minted; the latter comes back re-wrapped in
// the exact FromRunner shape controlapp's sessionRPC validates.
func (t transport) Dispatch(ctx context.Context, pool control.PoolID, id control.RunnerID, m runner.ToRunner) (runner.FromRunner, error) {
	rc := t.p.connIn(pool, string(id))
	if rc == nil {
		return runner.FromRunner{}, unreachable(m.Type, id)
	}
	if m.Type == "session_rpc" {
		return t.sessionRPC(ctx, rc, id, m)
	}
	if m.Type == "remove_workspace" {
		// Fire-and-forget on the wire, as it has always been: no ReqID, so
		// the runner sends no result, and the caller — a delete reclaiming a
		// volume — is not held for a round trip. An absent volume is a
		// success there anyway, so the only answer worth having is "sent".
		if err := rc.enqueue(m); err != nil {
			return runner.FromRunner{}, unreachable(m.Type, id)
		}
		return runner.FromRunner{OK: true}, nil
	}

	m.ReqID = rc.seq.Add(1)
	ch := make(chan runner.FromRunner, 1)
	rc.mu.Lock()
	rc.pending[m.ReqID] = ch
	rc.mu.Unlock()
	defer func() {
		rc.mu.Lock()
		delete(rc.pending, m.ReqID)
		rc.mu.Unlock()
	}()
	if err := rc.enqueue(m); err != nil {
		return runner.FromRunner{}, unreachable(m.Type, id)
	}
	res, ok := awaitAnswer(ctx, rc, ch, t.p.opTimeout)
	if !ok {
		return runner.FromRunner{}, unreachable(m.Type, id)
	}
	return res, nil
}

// sessionRPC is the session_rpc half of Dispatch. The envelope ID is the
// caller's correlation key: it is parked in the connection's srpc table, and
// routeSessionReq delivers the runner's "resp" envelope to it.
func (t transport) sessionRPC(ctx context.Context, rc *runnerConn, id control.RunnerID, m runner.ToRunner) (runner.FromRunner, error) {
	if m.RPC == nil || m.RPC.ID == 0 || m.Session == "" {
		return runner.FromRunner{}, control.ErrInvalid
	}
	ch := rc.srpc.add(m.RPC.ID)
	defer rc.srpc.remove(m.RPC.ID)
	if err := rc.enqueue(m); err != nil {
		return runner.FromRunner{}, unreachable(m.Type, id)
	}
	env, ok := awaitAnswer(ctx, rc, ch, t.p.opTimeout)
	if !ok {
		return runner.FromRunner{}, unreachable(m.Type, id)
	}
	return runner.FromRunner{Type: "session_req", Session: m.Session, RPC: &env}, nil
}

// awaitAnswer waits on ch the way dispatch always has: an answer wins; a
// connection that closes or a timeout that fires still drains an answer that
// raced in; and a caller that goes away takes what arrived, else nothing.
func awaitAnswer[T any](ctx context.Context, rc *runnerConn, ch chan T, timeout time.Duration) (T, bool) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case v := <-ch:
		return v, true
	case <-rc.done:
		return drainAnswer(ch)
	case <-timer.C:
		return drainAnswer(ch)
	case <-ctx.Done():
		return drainAnswer(ch)
	}
}

func drainAnswer[T any](ch chan T) (T, bool) {
	select {
	case v := <-ch:
		return v, true
	default:
		var zero T
		return zero, false
	}
}

// unreachable is the one error Dispatch returns: the closed sentinel, wrapped
// with the command type and the runner name — ours, never the runner's.
func unreachable(typ string, id control.RunnerID) error {
	return fmt.Errorf("dispatch %s to runner %q: %w", typ, id, control.ErrUnavailable)
}

// ---------------------------------------------------------------------------
// the session RPC's pending table and its routing
// ---------------------------------------------------------------------------

// The session RPC is the plane's request/response channel to the inside of a
// sandbox, and it has two directions.
//
// Downward, a request rides a ToRunner "session_rpc" and its answer comes back
// as a FromRunner "session_req" whose envelope Method is "resp", correlated by
// the envelope's own ID — the transport's half, over the srpcTable below.
//
// Upward (routeSessionReq): a sandbox asks for something (a git credential)
// and waits for the answer. The request arrives as a "session_req" with a
// method, the host answers it, and the answer goes back down as a
// "session_rpc" whose envelope Method is "resp", echoing the id the SANDBOX
// chose.
//
// Those two id spaces never collide, and nothing anywhere remaps an id,
// because a response always travels in the opposite direction to its request:
// a "resp" arriving here can only ever be answering a request this end sent,
// so it is matched against this end's table and no other. runnerd in between
// forwards envelopes verbatim, routing by session name alone.

// srpcTable is one runner connection's pending table for the session RPCs this
// replica originated. It is deliberately separate from runnerConn.pending (the
// runner-dispatch table keyed by ReqID): the two id spaces mean different
// things — a ReqID correlates a command the RUNNER executes and answers, an
// envelope ID correlates a request the SANDBOX answers — and sharing a counter
// between them would make a runner's result and a sandbox's response
// indistinguishable to whichever one happened to be waiting.
type srpcTable struct {
	mu      sync.Mutex
	pending map[uint64]chan runner.RPCEnvelope
}

func newSRPCTable() *srpcTable {
	return &srpcTable{pending: map[uint64]chan runner.RPCEnvelope{}}
}

// add registers a pending call and returns the channel its answer will arrive
// on. Buffered by one, so deliver never blocks the connection's reader even if
// the caller has already stopped waiting.
func (t *srpcTable) add(id uint64) chan runner.RPCEnvelope {
	ch := make(chan runner.RPCEnvelope, 1)
	t.mu.Lock()
	t.pending[id] = ch
	t.mu.Unlock()
	return ch
}

func (t *srpcTable) remove(id uint64) {
	t.mu.Lock()
	delete(t.pending, id)
	t.mu.Unlock()
}

// deliver hands a response to whoever is waiting on its id, reporting whether
// anyone was. A duplicate response for the same id is dropped rather than
// stalling the reader (the channel is buffered by one and the caller always
// removes its own entry).
func (t *srpcTable) deliver(env runner.RPCEnvelope) bool {
	t.mu.Lock()
	ch, ok := t.pending[env.ID]
	t.mu.Unlock()
	if !ok {
		return false
	}
	select {
	case ch <- env:
	default:
	}
	return true
}

// len reports how many calls are still pending on this connection. Tests use
// it to prove every exit path — answer, timeout, conn death — takes its entry
// with it; production code does not read it.
func (t *srpcTable) len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.pending)
}

// routeSessionReq handles one "session_req" from a runner: either the response
// to a request this replica sent down, or a fresh request from inside a
// sandbox that the host has to answer.
//
// It runs on the connection's reader, so the response half is delivered inline
// (a buffered channel hand-off) and the request half is not: answering reads
// the host's store, and this goroutine is the only one delivering every result
// and event that runner sends. Anything unroutable is logged and dropped —
// this message crossed a container boundary, and a malformed one must not be
// able to end the connection every session on that runner depends on.
func (p *Plane) routeSessionReq(ctx context.Context, rc *runnerConn, m runner.FromRunner) {
	if m.RPC == nil {
		p.logf("runner %s: session_req for %s carried no envelope", rc.name, clip(m.Session))
		return
	}
	env := *m.RPC
	if env.ID == 0 {
		p.logf("runner %s: session_req for %s carried no id; dropping", rc.name, clip(m.Session))
		return
	}
	if env.Method == "resp" {
		if !rc.srpc.deliver(env) {
			p.logf("runner %s: session-RPC response for unknown id %d (timed out?)", rc.name, env.ID)
		}
		return
	}
	if m.Session == "" {
		// Nothing to authorize the request against, and nowhere to send the
		// answer: a session_req names its session or it is not routable.
		p.logf("runner %s: session_req %q named no session; dropping", rc.name, clip(env.Method))
		return
	}
	go p.answerSessionRequest(ctx, rc, m.Session, env)
}

// answerSessionRequest asks the host to answer one sandbox-initiated request
// and sends back whatever it answered — always exactly one response, because
// the sandbox is holding a pending entry (and, for a credential mint, a git
// process) until one arrives.
func (p *Plane) answerSessionRequest(ctx context.Context, rc *runnerConn, sessionID string, env runner.RPCEnvelope) {
	ans := p.host.SessionRequest(ctx, rc.binding, control.SessionID(sessionID), env)
	// The id and the method are this layer's to set, never the host's: the id
	// is what the sandbox correlates against, and every answer is a "resp".
	ans.ID = env.ID
	ans.Method = "resp"
	if err := rc.enqueue(runner.ToRunner{Type: "session_rpc", Session: sessionID, RPC: &ans}); err != nil {
		p.logf("answering %s for session %s on runner %s: %v",
			clip(env.Method), clip(sessionID), rc.name, err)
	}
}

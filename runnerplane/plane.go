package runnerplane

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
)

const (
	// defaultOpTimeout bounds one dispatch round-trip to a runner. Image
	// pulls are the slow case; a minute is generous without letting a client
	// request hang indefinitely on a wedged runner.
	defaultOpTimeout = 60 * time.Second
	// defaultReadLimit matches runnerd's own read limit; announces of a full
	// fleet member are the largest message either side sends.
	defaultReadLimit = 16 << 20
	// announceFirstTimeout bounds how long a connected-but-silent socket may
	// hold a slot before saying who it is.
	announceFirstTimeout = 15 * time.Second
	// runnerWriteTimeout bounds one frame write to a runner; a peer that
	// stops reading kills the connection rather than wedging the writer.
	runnerWriteTimeout = 15 * time.Second
	// runnerSendQueue is the writer's backlog. Dispatches are bounded by
	// OpTimeout, so a runner far enough behind to fill this is one whose
	// commands are already stale.
	runnerSendQueue = 64
	// Orphan teardown is prompted by an announce, so a failed attempt would
	// otherwise wait for another reconnect before it ran again. Three tracked
	// attempts cover transient driver errors without keeping a dead connection
	// alive indefinitely; a later reconnect starts a fresh bounded series.
	orphanDestroyAttempts   = 3
	orphanDestroyRetryDelay = 250 * time.Millisecond
	// storeCleanupTimeout bounds the store writes done while tearing a
	// connection down, which must not inherit the dead request's context.
	storeCleanupTimeout = 5 * time.Second
)

// Options configures a plane. Every field has a working default.
type Options struct {
	// OpTimeout is one dispatch round trip; zero means 60s.
	OpTimeout time.Duration
	// ReadLimit is the per-message read limit on a runner socket; zero means
	// 16 MiB, runnerd's own.
	ReadLimit int64
	// Logf is where the plane's diagnostics go; zero means log.Printf. A host
	// that prefixes its own program name supplies a wrapper.
	Logf func(string, ...any)
}

// Plane is one replica's runner plane: the endpoint, the connections it
// holds, and the transport over them. Its zero value is not usable —
// construct it with New.
type Plane struct {
	host      Host
	opTimeout time.Duration
	readLimit int64
	logf      func(string, ...any)

	// mu guards runners and runnerLocks. It is held only for map reads and
	// writes, never across a store call or a socket write, so a slow runner
	// can't stall registration for the rest of the fleet.
	mu      sync.Mutex
	runners map[string]*runnerConn
	// runnerLocks serializes the store writes that describe one runner
	// (connected flag and capacity) — see nameLock. Keyed by runner name,
	// never held while mu is.
	runnerLocks map[string]*sync.Mutex
}

// New returns a plane over h.
func New(h Host, o Options) *Plane {
	if o.OpTimeout <= 0 {
		o.OpTimeout = defaultOpTimeout
	}
	if o.ReadLimit <= 0 {
		o.ReadLimit = defaultReadLimit
	}
	if o.Logf == nil {
		o.Logf = log.Printf
	}
	return &Plane{
		host:        h,
		opTimeout:   o.OpTimeout,
		readLimit:   o.ReadLimit,
		logf:        o.Logf,
		runners:     map[string]*runnerConn{},
		runnerLocks: map[string]*sync.Mutex{},
	}
}

// Handler is the runner WebSocket endpoint. The host mounts it at whatever
// path its API names (self-hosted: GET /v0/runners/connect).
func (p *Plane) Handler() http.Handler { return http.HandlerFunc(p.handleConnect) }

// Transport is control.RunnerTransport over this plane's connections: the one
// downward path every application service takes to a runner.
func (p *Plane) Transport() control.RunnerTransport { return transport{p} }

// Send queues a command whose result nobody waits for (attach dial-backs and
// best-effort broadcasts). It reports only whether the command was queued for
// delivery. Orphan destroys use the transport instead: a driver failure must
// be visible so it can be retried without another reconnect.
func (p *Plane) Send(pool control.PoolID, id control.RunnerID, m runner.ToRunner) error {
	rc := p.connIn(pool, string(id))
	if rc == nil {
		return fmt.Errorf("send %s to runner %q: not connected: %w", m.Type, id, control.ErrUnavailable)
	}
	if err := rc.enqueue(m); err != nil {
		return fmt.Errorf("send %s to runner %q: %w", m.Type, id, err)
	}
	return nil
}

// Broadcast queues m for every runner of pool connected to this replica
// except the one named (pass "" to reach all of them) — how a fact one runner
// just produced reaches the rest of the fleet, which is the prepull of a
// freshly built environment snapshot.
//
// Fire-and-forget by nature: there is no aggregate result worth waiting for,
// and a runner that misses the message loses nothing but a head start. The
// connection set is snapshotted under p.mu and the sends happen after it is
// released, so one wedged runner never stalls registration for the fleet.
func (p *Plane) Broadcast(pool control.PoolID, m runner.ToRunner, except control.RunnerID) {
	p.mu.Lock()
	conns := make([]*runnerConn, 0, len(p.runners))
	for name, rc := range p.runners {
		if rc.binding.PoolID == pool && name != string(except) {
			conns = append(conns, rc)
		}
	}
	p.mu.Unlock()

	for _, rc := range conns {
		if err := rc.enqueue(m); err != nil {
			p.logf("broadcasting %s to runner %s: %v", m.Type, rc.name, err)
		}
	}
}

// Close hangs up on every connection this plane holds. Each handler then
// retires its own connection on the way out, exactly as a runner's own
// disconnect does.
func (p *Plane) Close() {
	p.mu.Lock()
	conns := make([]*runnerConn, 0, len(p.runners))
	for _, rc := range p.runners {
		conns = append(conns, rc)
	}
	p.mu.Unlock()
	for _, rc := range conns {
		rc.shutdown()
	}
}

// ---------------------------------------------------------------------------
// the connection registry
// ---------------------------------------------------------------------------

func (p *Plane) conn(name string) *runnerConn {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.runners[name]
}

// connIn is conn, scoped: a connection answers for its own pool and no other.
func (p *Plane) connIn(pool control.PoolID, name string) *runnerConn {
	rc := p.conn(name)
	if rc == nil || rc.binding.PoolID != pool {
		return nil
	}
	return rc
}

func (p *Plane) isCurrentConn(rc *runnerConn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.runners[rc.name] == rc
}

// nameLock returns the mutex serializing the store writes that describe one
// runner. The pointer guard alone cannot order those writes: between a dying
// connection's identity check and its SetRunnerConnected(false) sits a whole
// store round-trip, and a redial that registers and writes connected:true
// inside that gap loses to the stale disconnect that lands after it. The
// protocol is announce-once, so an idle runner sends nothing further to
// correct the row — the fleet member most likely to have free slots would
// stay invisible to the scheduler until its next redial. Every path that
// writes a runner row therefore takes this lock, re-checks the registered
// conn under it, and only then writes.
//
// Entries are never removed: the key set is runner names, one small mutex per
// fleet member, and removing one safely would need refcounting for no
// practical gain. p.mu is released before the lock is taken, so the two never
// nest the wrong way round.
func (p *Plane) nameLock(name string) *sync.Mutex {
	p.mu.Lock()
	defer p.mu.Unlock()
	mu, ok := p.runnerLocks[name]
	if !ok {
		mu = &sync.Mutex{}
		p.runnerLocks[name] = mu
	}
	return mu
}

// registerRunner installs rc as the live connection for its runner and closes
// whatever it replaces. A redial that arrives before the old connection's
// teardown must win, so the swap happens under the lock and the old conn is
// closed after it. Callers hold the runner's name lock.
func (p *Plane) registerRunner(rc *runnerConn) {
	p.mu.Lock()
	old := p.runners[rc.name]
	p.runners[rc.name] = rc
	p.mu.Unlock()

	if old != nil && old != rc {
		p.logf("runner %s reconnected; closing the previous connection", rc.name)
		old.shutdown()
	}
}

// retireRunner tears rc down: closes it (which fails every dispatch waiting
// on it with control.ErrUnavailable) and, only if rc is still the registered
// connection for its name, deregisters it and records the runner as
// disconnected. The pointer guard is what keeps a replaced connection's
// teardown from marking the connection that replaced it dead; holding the
// name lock across the check and the write is what keeps a redial from
// slipping into the gap between them (see nameLock).
func (p *Plane) retireRunner(rc *runnerConn) {
	rc.shutdown()

	nl := p.nameLock(rc.name)
	nl.Lock()
	defer nl.Unlock()

	p.mu.Lock()
	current := p.runners[rc.name] == rc
	if current {
		delete(p.runners, rc.name)
	}
	p.mu.Unlock()
	if !current {
		return
	}

	// Deliberately not the request's context: it is being torn down along
	// with this connection, and the store write must still happen.
	ctx, cancel := context.WithTimeout(context.Background(), storeCleanupTimeout)
	defer cancel()
	if err := p.host.FleetRepository().SetRunnerConnected(ctx, rc.binding.PoolID, rc.binding.RunnerID, false); err != nil && !errors.Is(err, control.ErrNotFound) {
		p.logf("marking runner %s disconnected: %v", rc.name, err)
	}
	// A runner going away frees nothing by itself, but its sessions are
	// unreachable and its redial (with a fresh announce) follows shortly.
	p.host.Wake(rc.binding.PoolID)
}

// closeConn closes c with a reason the protocol can actually carry. Close
// reasons cap at 123 bytes and runner-supplied text can exceed that once
// quoted; an over-long reason would make coder/websocket drop the close frame
// entirely, leaving the runner with a bare EOF instead of the diagnostic that
// tells its operator what to fix.
func (p *Plane) closeConn(c *websocket.Conn, code websocket.StatusCode, reason string) {
	const maxReason = 123
	if len(reason) > maxReason {
		reason = strings.ToValidUTF8(reason[:maxReason], "")
	}
	if err := c.Close(code, reason); err != nil {
		p.logf("closing runner connection: %v", err)
	}
}

// clip bounds runner-supplied text before it reaches a log line or a
// websocket close reason (the protocol caps those at 123 bytes), keeping the
// result valid UTF-8 even when the cut lands mid-rune.
func clip(s string) string {
	const max = 48
	if len(s) <= max {
		return s
	}
	return strings.ToValidUTF8(s[:max], "") + "..."
}

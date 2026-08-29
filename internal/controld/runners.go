// internal/controld/runners.go
package controld

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"rainier/internal/rwire"
)

// ErrRunnerUnreachable is what every dispatch that never produced an answer
// wraps: the runner has no control connection, the connection died before
// the result arrived, or OpTimeout elapsed first. Callers (the API in a
// later task) map it to the `runner_unreachable` error code — from the
// client's point of view those three are the same fact, "the runner did not
// answer", and the message text carries which one it was.
var ErrRunnerUnreachable = errors.New("runner unreachable")

// ErrDispatchTimeout is the one no-answer outcome that is NOT evidence the
// command went undelivered: OpTimeout elapsed while the control connection
// was still live, so the runner has the command and may well have executed
// it — controld just didn't hear back in time (a cold image pull routinely
// outlasts a 60s OpTimeout).
//
// It wraps ErrRunnerUnreachable rather than standing alone, deliberately, so
// that every existing errors.Is(err, ErrRunnerUnreachable) call site keeps
// its behavior and only callers that ask specifically see the distinction:
//
//   - api.go's destroy/suspend/resume/snapshot handlers (4 sites) keep
//     answering 502 `runner_unreachable`. That is the honest answer for a
//     timeout too — controld cannot confirm the op either way, and the row
//     is left exactly as the store had it, so the client's retry is against
//     unchanged state. Splitting a second client-visible code here would
//     tell the caller nothing it could act on differently.
//   - sched.go's dispatchCreate is the one caller that must branch: a
//     requeue on a delivered-but-unconfirmed create is what births a
//     duplicate container. See its doc comment.
var ErrDispatchTimeout = fmt.Errorf("timed out with the connection still live: %w", ErrRunnerUnreachable)

const (
	// runnerReadLimit matches runnerd's own read limit; announces of a full
	// fleet member are the largest message either side sends.
	runnerReadLimit = 16 << 20
	// announceFirstTimeout bounds how long a connected-but-silent socket may
	// hold a slot before saying who it is.
	announceFirstTimeout = 15 * time.Second
	// runnerWriteTimeout bounds one frame write to a runner; a peer that
	// stops reading kills the connection rather than wedging the writer.
	runnerWriteTimeout = 15 * time.Second
	// runnerSendQueue is the writer's backlog. Dispatches are bounded by
	// OpTimeout and orphan destroys are fire-and-forget, so a runner far
	// enough behind to fill this is one whose commands are already stale.
	runnerSendQueue = 64
	// storeCleanupTimeout bounds the store writes done while tearing a
	// connection down, which must not inherit the dead request's context.
	storeCleanupTimeout = 5 * time.Second
	// lostAtAnnounce is the error recorded on a session the store believed
	// was alive on a runner that just announced without it.
	lostAtAnnounce = "lost at announce"
	// deadByRunner is the error recorded when a runner reports a session
	// dead.
	deadByRunner = "runner reported dead"
)

// liveOnRunner is the set of states in which a session is placed on a runner
// and expected to exist there: the from-list for adopting an announced
// state, and the filter for "what should this runner be holding".
var liveOnRunner = []SessionState{StateCreating, StateRunning, StateSuspendedWarm, StateSuspendedCold}

// runnerConn is one runnerd control connection. Exactly one goroutine reads
// it (the HTTP handler) and exactly one writes it (the writer goroutine
// draining out), so the websocket itself needs no lock; mu guards only the
// pending table, which dispatchers touch from arbitrary goroutines.
type runnerConn struct {
	name string
	ws   *websocket.Conn
	out  chan rwire.ToRunner

	mu      sync.Mutex
	pending map[uint64]chan rwire.FromRunner

	seq  atomic.Uint64
	done chan struct{}

	closeOnce sync.Once
}

func newRunnerConn(name string, ws *websocket.Conn) *runnerConn {
	return &runnerConn{
		name:    name,
		ws:      ws,
		out:     make(chan rwire.ToRunner, runnerSendQueue),
		pending: map[uint64]chan rwire.FromRunner{},
		done:    make(chan struct{}),
	}
}

// shutdown closes the connection exactly once. Closing done is also how
// every dispatch still waiting on this conn learns it will never be
// answered — they select on it — so no separate "fail the pending table"
// pass is needed or possible to race with.
func (rc *runnerConn) shutdown() {
	rc.closeOnce.Do(func() {
		close(rc.done)
		rc.ws.CloseNow()
	})
}

// enqueue hands m to the writer goroutine without ever blocking the caller:
// a dead conn or a full backlog is an error, not a wait.
func (rc *runnerConn) enqueue(m rwire.ToRunner) error {
	select {
	case <-rc.done:
		return fmt.Errorf("runner %s: connection closed: %w", rc.name, ErrRunnerUnreachable)
	default:
	}
	select {
	case rc.out <- m:
		return nil
	case <-rc.done:
		return fmt.Errorf("runner %s: connection closed: %w", rc.name, ErrRunnerUnreachable)
	default:
		return fmt.Errorf("runner %s: send queue full: %w", rc.name, ErrRunnerUnreachable)
	}
}

// deliver hands a result to the dispatcher waiting on its ReqID, reporting
// whether anyone was. The channel is buffered by one and the dispatcher
// always removes its entry, so this never blocks and a duplicate result for
// the same id is dropped rather than stalling the reader.
func (rc *runnerConn) deliver(m rwire.FromRunner) bool {
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
func (rc *runnerConn) writeLoop(ctx context.Context) {
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
				log.Printf("controld: write to runner %s: %v", rc.name, err)
				// A dead write direction means the connection is done; this
				// also unblocks the reader, which may otherwise sit on a
				// socket that will never produce another frame.
				rc.shutdown()
				return
			}
		}
	}
}

// handleRunnerConnect serves GET /v1/runners/connect: authenticate, upgrade,
// take the announce, reconcile against it, then serve the connection until
// it dies.
func (s *Server) handleRunnerConnect(w http.ResponseWriter, r *http.Request) {
	if !s.runnerTokenOK(r.Header.Get("Authorization")) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return // Accept already answered the request
	}
	defer c.CloseNow()
	c.SetReadLimit(runnerReadLimit)

	// connCtx scopes every read and write on this socket. It is canceled
	// when the handler returns, which is what stops the writer goroutine.
	connCtx, cancel := context.WithCancel(r.Context())
	defer cancel()

	ann, ok := readAnnounce(connCtx, c)
	if !ok {
		return
	}
	name := ann.Runner

	rc := newRunnerConn(name, c)
	connErr := s.connectRunner(connCtx, rc, ann.Used, ann.Total)

	var writerDone sync.WaitGroup
	defer func() {
		s.retireRunner(rc) // closes done, which is what lets the writer exit
		writerDone.Wait()
	}()
	if connErr != nil {
		// connectRunner registers before it writes, so rc is in the map even
		// now; the deferred retire above is what takes it back out.
		log.Printf("controld: upsert runner %s: %v", name, connErr)
		closeRunner(c, websocket.StatusInternalError, "store unavailable")
		return
	}

	writerDone.Add(1)
	go func() {
		defer writerDone.Done()
		rc.writeLoop(connCtx)
	}()

	log.Printf("controld: runner %s connected (used %d/%d, %d announced sessions)",
		name, ann.Used, ann.Total, len(ann.Sessions))
	s.reconcile(connCtx, name, ann.Sessions)
	// One wake covers everything reconcile can free (a dead session, an
	// adopted cold suspend) plus the capacity the announce itself reports.
	s.wakeScheduler()

	s.readLoop(connCtx, rc)
}

// readAnnounce reads and validates the connection's first message, which
// must be an announce in a proto we speak. A rejection closes the socket
// with a reason naming both versions, so the operator reading runnerd's log
// learns what to upgrade (design §4.3).
func readAnnounce(ctx context.Context, c *websocket.Conn) (rwire.FromRunner, bool) {
	annCtx, cancel := context.WithTimeout(ctx, announceFirstTimeout)
	defer cancel()

	var ann rwire.FromRunner
	if err := wsjson.Read(annCtx, c, &ann); err != nil {
		log.Printf("controld: reading runner announce: %v", err)
		return rwire.FromRunner{}, false
	}
	switch {
	case ann.Type != "announce":
		closeRunner(c, websocket.StatusPolicyViolation,
			fmt.Sprintf("first message must be announce, got %q", clip(ann.Type)))
		return rwire.FromRunner{}, false
	case ann.Proto != rwire.Proto:
		closeRunner(c, websocket.StatusPolicyViolation,
			fmt.Sprintf("unsupported proto %d, want proto %d", ann.Proto, rwire.Proto))
		return rwire.FromRunner{}, false
	case ann.Runner == "":
		closeRunner(c, websocket.StatusPolicyViolation, "announce is missing a runner name")
		return rwire.FromRunner{}, false
	}
	return ann, true
}

// readLoop serves one connection's inbound messages until it dies.
func (s *Server) readLoop(ctx context.Context, rc *runnerConn) {
	for {
		var m rwire.FromRunner
		if err := wsjson.Read(ctx, rc.ws, &m); err != nil {
			log.Printf("controld: runner %s connection ended: %v", rc.name, err)
			return
		}
		// Capacity rides every message, not just announces, so the fleet
		// view is current without a separate capacity message. touchRunner
		// reports false when a reconnect has replaced us: the new conn owns
		// the runner now, and anything we did here would write yesterday's
		// news over today's.
		if !s.touchRunner(ctx, rc, m) {
			return
		}

		switch m.Type {
		case "result":
			if m.ReqID == 0 {
				continue // a fire-and-forget command's ack; nobody is waiting
			}
			if !rc.deliver(m) {
				log.Printf("controld: runner %s: result for unknown req_id %d (timed out?)", rc.name, m.ReqID)
			}
		case "event":
			s.applyEvent(ctx, rc.name, m)
		default:
			log.Printf("controld: runner %s: unexpected message type %q", rc.name, clip(m.Type))
		}
	}
}

// touchRunner refreshes the runner row from any message's piggybacked
// capacity, reporting whether rc is still the registered connection — the
// caller stops reading when it isn't. The identity check and the write both
// happen under the runner's name lock, so a reconnect can neither slip
// between them nor have its own row write overtaken by this one.
func (s *Server) touchRunner(ctx context.Context, rc *runnerConn, m rwire.FromRunner) bool {
	nl := s.nameLock(rc.name)
	nl.Lock()
	defer nl.Unlock()

	if !s.isCurrentConn(rc) {
		return false
	}
	err := s.st.UpsertRunner(ctx, Runner{
		Name:          rc.name,
		CapacityUsed:  m.Used,
		CapacityTotal: m.Total,
		Connected:     true,
		LastSeenAt:    time.Now(),
	})
	if err != nil {
		log.Printf("controld: upsert runner %s: %v", rc.name, err)
	}
	return true
}

// applyEvent applies a runner's unsolicited state report. Events race
// reconciliation by nature — the announce is truth, an event is news — so a
// guarded transition that doesn't apply is expected, not an error.
//
// A runner may only report on sessions the store places on it, or on ones it
// places nowhere yet (a "running" event can outrun the create's own
// queued→creating transition; "dead" tightens that to an exact match — see
// its arm below). Without that guard the fleet-wide runner token would let
// any runner drive any session to a terminal state, and the stale holder of
// a duplicate — the case reconcileUnplaced destroys — could mark a session
// dead that has since been re-placed and is running fine somewhere else.
func (s *Server) applyEvent(ctx context.Context, runner string, m rwire.FromRunner) {
	if m.Session == "" {
		log.Printf("controld: runner %s: event with no session", runner)
		return
	}
	row, err := s.st.GetSession(ctx, m.Session)
	switch {
	case errors.Is(err, ErrNotFound):
		log.Printf("controld: runner %s: event for unknown session %s; ignoring", runner, clip(m.Session))
		return
	case err != nil:
		log.Printf("controld: runner %s: event for %s: %v", runner, clip(m.Session), err)
		return
	case row.Runner != "" && row.Runner != runner:
		log.Printf("controld: runner %s reported %s for %s, which the store places on %s; ignoring",
			runner, clip(m.State), row.ID, row.Runner)
		return
	}

	switch m.State {
	case "running":
		s.transitionQuiet(ctx, m.Session, liveOnRunner, StateRunning, TransitionOpts{})
		// A session reaching running frees no slot, but it does change
		// freeCapacity's math: the row leaves the `creating` count while the
		// runner's reported Used already includes its container, so the
		// double-count that was hiding one slot of headroom clears here.
		// Without this wake that headroom stays invisible until the 10s
		// safety tick — the burst e2e measured 20s of a queue sitting on
		// capacity that already existed.
		s.wakeScheduler()
	case "dead":
		// Unlike "running", dead demands an exact placement match. The guard
		// above deliberately accepts row.Runner == "" (a running event can
		// outrun the create's own queued->creating transition), but dead is
		// terminal and unhealable: an unplaced row is one a requeue cleared
		// and the scheduler may have re-placed elsewhere, so a stale holder
		// reporting its own copy dead must not kill the live one.
		if row.Runner != runner {
			log.Printf("controld: runner %s reported %s dead, but the store places it on %q; ignoring",
				runner, row.ID, row.Runner)
			return
		}
		reason := deadByRunner
		s.transitionQuiet(ctx, m.Session, NonTerminal, StateDead, TransitionOpts{Error: &reason})
		s.wakeScheduler() // a dead session frees its slot
	default:
		log.Printf("controld: runner %s: unknown event state %q for %s", runner, clip(m.State), clip(m.Session))
	}
}

// reconcile makes the store agree with the reality a runner just announced
// (design §4.8). runnerd is truth for liveness, the store is truth for
// desired state, so each row is settled by which of the two knows something
// the other doesn't.
func (s *Server) reconcile(ctx context.Context, name string, announced []rwire.SessionInfo) {
	byID := make(map[string]rwire.SessionInfo, len(announced))
	for _, a := range announced {
		byID[a.ID] = a
	}

	rows, err := s.st.SessionsOnRunner(ctx, name, liveOnRunner)
	if err != nil {
		// Without the store's view there is no reconciliation to do; the
		// runner stays connected and the next announce tries again. Acting
		// on half the picture would destroy live sessions.
		log.Printf("controld: reconcile %s: listing sessions: %v", name, err)
		return
	}

	placed := make(map[string]bool, len(rows))
	for _, row := range rows {
		placed[row.ID] = true
		if ann, present := byID[row.ID]; present {
			s.reconcilePresent(ctx, row, ann)
		} else {
			s.reconcileMissing(ctx, name, row)
		}
	}
	for _, a := range announced {
		if !placed[a.ID] {
			s.reconcileUnplaced(ctx, name, a)
		}
	}
}

// reconcilePresent settles a row the runner does have: adopt whatever state
// it reports, and count even agreement as news.
func (s *Server) reconcilePresent(ctx context.Context, row Session, ann rwire.SessionInfo) {
	want, ok := announcedState(ann.State)
	if !ok {
		log.Printf("controld: runner %s announced %s in unknown state %q; leaving it alone",
			row.Runner, row.ID, clip(ann.State))
		return
	}
	if want == row.State {
		// Same state still means "demonstrably alive just now": the
		// transition is a no-op except for the last_event_at bump, which is
		// the whole point.
		s.transitionQuiet(ctx, row.ID, []SessionState{want}, want, TransitionOpts{})
		return
	}
	s.transitionQuiet(ctx, row.ID, NonTerminal, want, TransitionOpts{})
}

// reconcileMissing settles a row the runner should have and doesn't: a
// create that never landed goes back on the queue, anything further along is
// gone for good.
func (s *Server) reconcileMissing(ctx context.Context, name string, row Session) {
	if row.State == StateCreating {
		none := ""
		log.Printf("controld: runner %s did not announce creating session %s; requeuing", name, row.ID)
		s.transitionQuiet(ctx, row.ID, []SessionState{StateCreating}, StateQueued, TransitionOpts{Runner: &none})
		return
	}
	reason := lostAtAnnounce
	log.Printf("controld: runner %s did not announce %s (%s); marking dead", name, row.ID, row.State)
	s.transitionQuiet(ctx, row.ID, NonTerminal, StateDead, TransitionOpts{Error: &reason})
}

// reconcileUnplaced settles an announced session the store does not have
// placed on this runner: an unknown or terminal id is an orphan and gets
// destroyed, while a live row the store still wants is adopted onto the
// runner that actually has it.
func (s *Server) reconcileUnplaced(ctx context.Context, name string, ann rwire.SessionInfo) {
	row, err := s.st.GetSession(ctx, ann.ID)
	switch {
	case errors.Is(err, ErrNotFound):
		log.Printf("controld: runner %s announced unknown session %s; destroying orphan", name, clip(ann.ID))
		s.destroyOrphan(name, ann.ID)
	case err != nil:
		log.Printf("controld: reconcile %s: get session %s: %v", name, clip(ann.ID), err)
	case row.State.Terminal():
		log.Printf("controld: runner %s announced %s, which the store already finished as %s; destroying orphan",
			name, row.ID, row.State)
		s.destroyOrphan(name, row.ID)
	case row.Runner != "" && row.Runner != name:
		// The store has this session placed on a different runner, so this
		// one is holding a duplicate — the losing half of a requeue that
		// re-placed elsewhere while this runner was away. Postgres owns
		// placement (§4.8), so the copy here is an orphan. Adopting it
		// instead would leave both copies alive and make Runner ping-pong
		// between the two on every reconnect, with the loser's container
		// holding a slot for the life of the runner.
		log.Printf("controld: runner %s announced %s, which the store places on %s; destroying the duplicate",
			name, row.ID, row.Runner)
		s.destroyOrphan(name, row.ID)
	default:
		// The store wants this session alive and has it either here or
		// nowhere (e.g. requeued while the runner was away, never
		// re-placed). runnerd is truth for liveness: adopt it onto the
		// runner that actually holds it rather than destroying something
		// the store still wants.
		want, ok := announcedState(ann.State)
		if !ok {
			log.Printf("controld: runner %s announced %s in unknown state %q; leaving it alone",
				name, row.ID, clip(ann.State))
			return
		}
		log.Printf("controld: runner %s announced %s, which the store has as %s on %q; adopting onto %s",
			name, row.ID, row.State, row.Runner, name)
		runner := name
		s.transitionQuiet(ctx, row.ID, NonTerminal, want, TransitionOpts{Runner: &runner})
	}
}

// destroyOrphan tells a runner to drop a session the store has no live row
// for. Fire-and-forget on purpose: there is no row whose state the result
// could update, and the next announce trues it up if this one is lost.
func (s *Server) destroyOrphan(runner, id string) {
	if err := s.sendToRunner(runner, rwire.ToRunner{Type: "destroy", Session: id}); err != nil {
		log.Printf("controld: destroying orphan %s on %s: %v", clip(id), runner, err)
	}
}

// transitionQuiet applies a guarded transition, swallowing exactly the two
// outcomes reconciliation and events produce by racing each other:
// ErrConflict (the row moved on under us) and ErrNotFound (it's gone). The
// announce is truth and arrives again shortly; anything else is a real
// store problem worth a log line.
func (s *Server) transitionQuiet(ctx context.Context, id string, from []SessionState, to SessionState, opts TransitionOpts) {
	err := s.st.Transition(ctx, id, from, to, opts)
	if err == nil || errors.Is(err, ErrConflict) || errors.Is(err, ErrNotFound) {
		return
	}
	log.Printf("controld: transition %s -> %s: %v", id, to, err)
}

// announcedState maps an announced session state onto a SessionState,
// rejecting anything outside the three a runner may report.
func announcedState(s string) (SessionState, bool) {
	switch st := SessionState(s); st {
	case StateRunning, StateSuspendedWarm, StateSuspendedCold:
		return st, true
	}
	return "", false
}

// dispatch sends m to a runner and waits for the matching result. It assigns
// the ReqID (per-connection, so ids never collide across runners) and
// returns an error wrapping ErrRunnerUnreachable whenever no answer arrives:
// no connection, a connection that died, or OpTimeout. That last one — and
// only that one — also wraps ErrDispatchTimeout, because it alone leaves the
// command delivered and possibly executed (see that error's doc comment).
//
// The one exception is the caller's own ctx being canceled, which returns a
// bare ctx.Err() — the runner is not implicated, and callers that map
// ErrRunnerUnreachable to a 502 should let that one surface as the client
// disconnect it is.
func (s *Server) dispatch(ctx context.Context, runner string, m rwire.ToRunner) (rwire.FromRunner, error) {
	rc := s.conn(runner)
	if rc == nil {
		return rwire.FromRunner{}, fmt.Errorf("dispatch %s to runner %q: not connected: %w",
			m.Type, runner, ErrRunnerUnreachable)
	}

	m.ReqID = rc.seq.Add(1)
	ch := make(chan rwire.FromRunner, 1)
	rc.mu.Lock()
	rc.pending[m.ReqID] = ch
	rc.mu.Unlock()
	defer func() {
		rc.mu.Lock()
		delete(rc.pending, m.ReqID)
		rc.mu.Unlock()
	}()

	if err := rc.enqueue(m); err != nil {
		return rwire.FromRunner{}, fmt.Errorf("dispatch %s to runner %q: %w", m.Type, runner, err)
	}

	timer := time.NewTimer(s.cfg.OpTimeout)
	defer timer.Stop()
	select {
	case res := <-ch:
		return res, nil
	case <-rc.done:
		// select is random among ready cases: prefer a result that landed
		// in the same instant the connection died.
		if res, ok := drain(ch); ok {
			return res, nil
		}
		return rwire.FromRunner{}, fmt.Errorf("dispatch %s to runner %q: connection closed before the result: %w",
			m.Type, runner, ErrRunnerUnreachable)
	case <-timer.C:
		if res, ok := drain(ch); ok {
			return res, nil
		}
		// select is random among ready cases, so a connection that died at
		// the same instant the timer fired could surface here as a timeout.
		// Re-check explicitly: conn death is the stronger fact (the command
		// is definitively unanswerable now), and ErrDispatchTimeout's whole
		// contract is "the connection was still live", which callers act on.
		select {
		case <-rc.done:
			return rwire.FromRunner{}, fmt.Errorf("dispatch %s to runner %q: connection closed before the result: %w",
				m.Type, runner, ErrRunnerUnreachable)
		default:
		}
		return rwire.FromRunner{}, fmt.Errorf("dispatch %s to runner %q: no result within %s: %w",
			m.Type, runner, s.cfg.OpTimeout, ErrDispatchTimeout)
	case <-ctx.Done():
		if res, ok := drain(ch); ok {
			return res, nil
		}
		return rwire.FromRunner{}, fmt.Errorf("dispatch %s to runner %q: %w", m.Type, runner, ctx.Err())
	}
}

func drain(ch chan rwire.FromRunner) (rwire.FromRunner, bool) {
	select {
	case res := <-ch:
		return res, true
	default:
		return rwire.FromRunner{}, false
	}
}

// sendToRunner queues a command whose result nobody waits for (orphan
// destroys, attach dial-backs). It reports only whether the command was
// queued for delivery.
func (s *Server) sendToRunner(runner string, m rwire.ToRunner) error {
	rc := s.conn(runner)
	if rc == nil {
		return fmt.Errorf("send %s to runner %q: not connected: %w", m.Type, runner, ErrRunnerUnreachable)
	}
	if err := rc.enqueue(m); err != nil {
		return fmt.Errorf("send %s to runner %q: %w", m.Type, runner, err)
	}
	return nil
}

// broadcastToRunners queues m for every runner connected to this replica
// except the one named (pass "" to reach all of them) — how a fact one runner
// just produced reaches the rest of the fleet, which in Plan 4 is the prepull
// of a freshly built environment snapshot.
//
// Fire-and-forget by nature: there is no aggregate result worth waiting for,
// and a runner that misses the message loses nothing but a head start. The
// connection set is snapshotted under s.mu and the sends happen after it is
// released, so one wedged runner never stalls registration for the fleet.
func (s *Server) broadcastToRunners(m rwire.ToRunner, except string) {
	s.mu.Lock()
	conns := make([]*runnerConn, 0, len(s.runners))
	for name, rc := range s.runners {
		if name != except {
			conns = append(conns, rc)
		}
	}
	s.mu.Unlock()

	for _, rc := range conns {
		if err := rc.enqueue(m); err != nil {
			log.Printf("controld: broadcasting %s to runner %s: %v", m.Type, rc.name, err)
		}
	}
}

// runnerConnected reports whether a runner currently holds a control
// connection to this replica.
func (s *Server) runnerConnected(name string) bool { return s.conn(name) != nil }

func (s *Server) conn(name string) *runnerConn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.runners[name]
}

func (s *Server) isCurrentConn(rc *runnerConn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.runners[rc.name] == rc
}

// nameLock returns the mutex serializing the store writes that describe one
// runner. The pointer guard alone cannot order those writes: between a
// dying connection's identity check and its SetRunnerConnected(false) sits a
// whole store round-trip, and a redial that registers and writes
// connected:true inside that gap loses to the stale disconnect that lands
// after it. The protocol is announce-once, so an idle runner sends nothing
// further to correct the row — the fleet member most likely to have free
// slots would stay invisible to the scheduler until its next redial. Every
// path that writes a runner row therefore takes this lock, re-checks the
// registered conn under it, and only then writes.
//
// Entries are never removed: the key set is runner names, one small mutex
// per fleet member, and removing one safely would need refcounting for no
// practical gain. s.mu is released before the lock is taken, so the two
// never nest the wrong way round.
func (s *Server) nameLock(name string) *sync.Mutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	mu, ok := s.runnerLocks[name]
	if !ok {
		mu = &sync.Mutex{}
		s.runnerLocks[name] = mu
	}
	return mu
}

// connectRunner installs rc as the live connection for its runner and
// records the capacity it announced, both under the runner's name lock so
// that a connection being retired at this same instant either writes its
// disconnect before us or (seeing itself replaced) not at all.
func (s *Server) connectRunner(ctx context.Context, rc *runnerConn, used, total int) error {
	nl := s.nameLock(rc.name)
	nl.Lock()
	defer nl.Unlock()

	s.registerRunner(rc)
	return s.st.UpsertRunner(ctx, Runner{
		Name:          rc.name,
		CapacityUsed:  used,
		CapacityTotal: total,
		Connected:     true,
		LastSeenAt:    time.Now(),
	})
}

// registerRunner installs rc as the live connection for its runner and
// closes whatever it replaces. A redial that arrives before the old
// connection's teardown must win, so the swap happens under the lock and the
// old conn is closed after it. Callers hold the runner's name lock.
func (s *Server) registerRunner(rc *runnerConn) {
	s.mu.Lock()
	old := s.runners[rc.name]
	s.runners[rc.name] = rc
	s.mu.Unlock()

	if old != nil && old != rc {
		log.Printf("controld: runner %s reconnected; closing the previous connection", rc.name)
		old.shutdown()
	}
}

// retireRunner tears rc down: closes it (which fails every dispatch waiting
// on it with ErrRunnerUnreachable) and, only if rc is still the registered
// connection for its name, deregisters it and records the runner as
// disconnected. The pointer guard is what keeps a replaced connection's
// teardown from marking the connection that replaced it dead; holding the
// name lock across the check and the write is what keeps a redial from
// slipping into the gap between them (see nameLock).
func (s *Server) retireRunner(rc *runnerConn) {
	rc.shutdown()

	nl := s.nameLock(rc.name)
	nl.Lock()
	defer nl.Unlock()

	s.mu.Lock()
	current := s.runners[rc.name] == rc
	if current {
		delete(s.runners, rc.name)
	}
	s.mu.Unlock()
	if !current {
		return
	}

	// Deliberately not the request's context: it is being torn down along
	// with this connection, and the store write must still happen.
	ctx, cancel := context.WithTimeout(context.Background(), storeCleanupTimeout)
	defer cancel()
	if err := s.st.SetRunnerConnected(ctx, rc.name, false); err != nil && !errors.Is(err, ErrNotFound) {
		log.Printf("controld: marking runner %s disconnected: %v", rc.name, err)
	}
	// A runner going away frees nothing by itself, but its sessions are
	// unreachable and its redial (with a fresh announce) follows shortly.
	s.wakeScheduler()
}

// runnerTokenOK compares the presented bearer against the fleet token in
// constant time. The scheme is matched exactly as runnerd sends it.
func (s *Server) runnerTokenOK(authz string) bool {
	const prefix = "Bearer "
	if !strings.HasPrefix(authz, prefix) {
		return false
	}
	got := []byte(strings.TrimPrefix(authz, prefix))
	return subtle.ConstantTimeCompare(got, []byte(s.cfg.RunnerToken)) == 1
}

// closeRunner closes c with a reason the protocol can actually carry. Close
// reasons cap at 123 bytes and runner-supplied text can exceed that once
// quoted; an over-long reason would make coder/websocket drop the close
// frame entirely, leaving the runner with a bare EOF instead of the
// diagnostic that tells its operator what to fix.
func closeRunner(c *websocket.Conn, code websocket.StatusCode, reason string) {
	const maxReason = 123
	if len(reason) > maxReason {
		reason = strings.ToValidUTF8(reason[:maxReason], "")
	}
	if err := c.Close(code, reason); err != nil {
		log.Printf("controld: closing runner connection: %v", err)
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

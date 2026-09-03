// internal/controld/runners.go
package controld

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
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
	// setupStage is the boot stage Plan 4's `setup_failed` event names, and
	// the stage a `stage_failed` that names none is read as. A session's boot
	// is a chain — setup, then clone, then init (cmd/sessiond/gitchain.go) —
	// and every stage composes its failure the same way; see stageFailure.
	setupStage = "setup"
	// unnamedStage stands in when a stage_failed's detail carries no stage in
	// front of it, which only a sender that is not following the contract can
	// produce. The session behind it really has stopped booting, so it still
	// fails — under a name that claims nothing about which stage it was.
	unnamedStage = "boot"
	// snapshotRefHashLen is how much of an environment's setup hash goes into
	// its snapshot ref: 12 hex characters, enough that no fleet collides and
	// short enough to read in a `docker images` listing (design §4.3).
	snapshotRefHashLen = 12
)

// runnerConn is one runnerd control connection. Exactly one goroutine reads
// it (the HTTP handler) and exactly one writes it (the writer goroutine
// draining out), so the websocket itself needs no lock; mu guards only the
// pending table, which dispatchers touch from arbitrary goroutines.
type runnerConn struct {
	name string
	ws   *websocket.Conn
	out  chan runner.ToRunner
	// gen is the runner generation THIS connection registered under. Every
	// event read off this socket is stamped with it rather than with the
	// runner's current generation, so a message that outlives its own
	// connection is fenced by the fleet service (ErrStale) instead of being
	// applied under the generation of the socket that replaced it.
	gen uint64

	mu      sync.Mutex
	pending map[uint64]chan runner.FromRunner

	seq atomic.Uint64
	// srpc is the pending table for the session RPCs controld sent INTO the
	// sandboxes this runner holds. Separate from pending above because the two
	// correlate different things — see srpcTable (srpc.go).
	srpc *srpcTable
	done chan struct{}

	closeOnce sync.Once
}

func newRunnerConn(name string, ws *websocket.Conn) *runnerConn {
	return &runnerConn{
		name:    name,
		ws:      ws,
		out:     make(chan runner.ToRunner, runnerSendQueue),
		pending: map[uint64]chan runner.FromRunner{},
		srpc:    newSRPCTable(),
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
func (rc *runnerConn) enqueue(m runner.ToRunner) error {
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

// handleRunnerConnect serves GET /v0/runners/connect: authenticate, upgrade,
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

	// The generation is minted before anything is registered, so it is the
	// one this connection acts under for its whole life: registration,
	// reconciliation, every event read off the socket, and every heartbeat.
	// It comes from the STORE, so it continues across a restart and no two
	// replicas can hand out the same authority. A store that cannot mint one
	// is a connection that must not be served at all — everything downstream
	// fences on this number, so serving without it would be serving with no
	// authority rather than with fresh authority.
	gen, err := s.st.NextRunnerGeneration(connCtx, installPool, control.RunnerID(name))
	if err != nil {
		log.Printf("controld: opening a generation for runner %s: %v", name, err)
		closeRunner(c, websocket.StatusInternalError, "registration refused")
		return
	}

	rc := newRunnerConn(name, c)
	rc.gen = gen
	connErr := s.connectRunner(connCtx, rc, ann)

	var writerDone sync.WaitGroup
	defer func() {
		s.retireRunner(rc) // closes done, which is what lets the writer exit
		writerDone.Wait()
	}()
	if connErr != nil {
		// connectRunner registers before it calls the service, so rc is in
		// the map even now; the deferred retire above is what takes it back
		// out.
		log.Printf("controld: registering runner %s (generation %d): %v", name, rc.gen, connErr)
		closeRunner(c, websocket.StatusInternalError, "registration refused")
		return
	}

	writerDone.Add(1)
	go func() {
		defer writerDone.Done()
		rc.writeLoop(connCtx)
	}()

	log.Printf("controld: runner %s connected (used %d/%d, %d announced sessions)",
		name, ann.Used, ann.Total, len(ann.Sessions))

	// Reconciliation is the fleet service's: the announce is authoritative
	// for liveness, the store for desired state, and the service settles the
	// two and hands back the ids this runner must tear down. The writer is
	// already running, because tearing an orphan down means dispatching to
	// this very connection.
	res, err := s.fleet.ReconcileRunner(connCtx, control.RunnerSnapshot{
		WorkspaceID: installWorkspace, PoolID: installPool,
		RunnerID:      control.RunnerID(name),
		Generation:    rc.gen,
		CapacityUsed:  ann.Used,
		CapacityTotal: ann.Total,
		Sessions:      announcedSessions(ann.Sessions),
	})
	if err != nil {
		log.Printf("controld: reconciling runner %s (generation %d): %v", name, rc.gen, err)
		closeRunner(c, websocket.StatusInternalError, "reconcile failed")
		return
	}
	for _, id := range res.Destroy {
		s.destroyOrphan(connCtx, name, string(id))
	}
	// One wake covers everything reconciliation can free (a dead session, an
	// adopted cold suspend) plus the capacity the announce itself reports.
	s.fleet.Wake(installPool)

	s.readLoop(connCtx, rc)
}

// announcedSessions re-spells a runner's announced session list in the
// control vocabulary the fleet service reconciles against.
func announcedSessions(in []runner.SessionInfo) []control.RunnerSession {
	if len(in) == 0 {
		return nil
	}
	out := make([]control.RunnerSession, 0, len(in))
	for _, a := range in {
		out = append(out, control.RunnerSession{
			SessionID: control.SessionID(a.ID),
			State:     control.SessionState(a.State),
		})
	}
	return out
}

// readAnnounce reads and validates the connection's first message, which
// must be an announce in a proto we speak. A rejection closes the socket
// with a reason naming both versions, so the operator reading runnerd's log
// learns what to upgrade (design §4.3).
func readAnnounce(ctx context.Context, c *websocket.Conn) (runner.FromRunner, bool) {
	annCtx, cancel := context.WithTimeout(ctx, announceFirstTimeout)
	defer cancel()

	var ann runner.FromRunner
	if err := wsjson.Read(annCtx, c, &ann); err != nil {
		log.Printf("controld: reading runner announce: %v", err)
		return runner.FromRunner{}, false
	}
	switch {
	case ann.Type != "announce":
		closeRunner(c, websocket.StatusPolicyViolation,
			fmt.Sprintf("first message must be announce, got %q", clip(ann.Type)))
		return runner.FromRunner{}, false
	case ann.Proto != runner.ProtocolVersion:
		closeRunner(c, websocket.StatusPolicyViolation,
			fmt.Sprintf("unsupported proto %d, want proto %d", ann.Proto, runner.ProtocolVersion))
		return runner.FromRunner{}, false
	case ann.Runner == "":
		closeRunner(c, websocket.StatusPolicyViolation, "announce is missing a runner name")
		return runner.FromRunner{}, false
	}
	return ann, true
}

// readLoop serves one connection's inbound messages until it dies.
func (s *Server) readLoop(ctx context.Context, rc *runnerConn) {
	for {
		var m runner.FromRunner
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
			s.applyRunnerEvent(ctx, rc, m)
		case "session_req":
			// One message type, both halves of the session RPC's upward
			// direction: a sandbox's own request, and the answer to one this
			// replica sent down. routeSessionReq tells them apart and keeps
			// the slow half off this goroutine — see its doc comment.
			s.routeSessionReq(ctx, rc, m)
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
func (s *Server) touchRunner(ctx context.Context, rc *runnerConn, m runner.FromRunner) bool {
	nl := s.nameLock(rc.name)
	nl.Lock()
	defer nl.Unlock()

	if !s.isCurrentConn(rc) {
		return false
	}
	err := s.st.Fleet().UpsertRunner(ctx, installPool, control.Runner{
		ID:            control.RunnerID(rc.name),
		PoolID:        installPool,
		CapacityUsed:  m.Used,
		CapacityTotal: m.Total,
		Connected:     true,
		Generation:    rc.gen,
		Capabilities:  runnerCapabilities(rc.name),
		LastSeenAt:    time.Now(),
	})
	switch {
	case errors.Is(err, control.ErrStale):
		// Another replica (or a redial this process never saw) opened a
		// newer generation for this runner. Registration is not the only
		// place authority can be lost, so the heartbeat is the second fence:
		// this connection is superseded and must stop reading, which ends it
		// and closes the socket the runner will redial on.
		log.Printf("controld: runner %s: connection at generation %d is superseded", rc.name, rc.gen)
		return false
	case err != nil:
		log.Printf("controld: upsert runner %s: %v", rc.name, err)
	}
	return true
}

// applyRunnerEvent hands a runner's unsolicited state report to the fleet
// service, which owns every session transition an event can cause. This
// function's whole job is translation: runnerd's event vocabulary into the
// control one, and the runner's free text into the composed sentence a
// session's error column carries.
//
// Two arms never reach the service because they are not about the session at
// all — a finished setup is news about the ENVIRONMENT and a rejected
// credential is news about its OWNER — so they stay here, in the adapter that
// owns the vault and the snapshot cache (see applyAdapterArm).
//
// The event is stamped with rc.gen, the generation of the connection it
// arrived on, never the runner's current one: a message that outlived its own
// socket must be fenced by the service (ErrStale), not applied under the
// generation of the connection that replaced it. touchRunner already drops
// such a message on the way in; this is the second lock on the same door.
func (s *Server) applyRunnerEvent(ctx context.Context, rc *runnerConn, m runner.FromRunner) {
	name := rc.name
	if m.Session == "" {
		log.Printf("controld: runner %s: event with no session", name)
		return
	}
	ev := control.RunnerEvent{
		WorkspaceID: installWorkspace,
		PoolID:      installPool,
		RunnerID:    control.RunnerID(name),
		Generation:  rc.gen,
		SessionID:   control.SessionID(m.Session),
	}
	switch m.State {
	case "running":
		ev.State = control.StateRunning
	case "dead":
		ev.State = control.StateDead
	case "setup_failed":
		// Plan 4's name for what is now one stage among three, and one that
		// stays accepted forever: sessiond ships INSIDE the session image
		// while runnerd runs on the host, so a session whose image predates
		// the boot chain still reports its setup failure under this name.
		// m.Detail is already the composed "rc N: <tail>" with no stage in
		// front, so it goes to the stage arm as the stage it always meant.
		ev.State = control.StateFailed
		ev.Detail = stageFailure(setupStage, m.Detail)
	case "stage_failed":
		// One event for every stage of the boot chain. The stage rides at the
		// FRONT of the detail because a runner event has exactly one free-text
		// field: "clone: rc 128: fatal: Authentication failed" splits at the
		// first ": " into the stage and the sentence runnerd composed, which
		// is never parsed further — how a stage failure is described is the
		// runner's half of the contract.
		stage, rest, ok := strings.Cut(m.Detail, ": ")
		if !ok {
			stage, rest = unnamedStage, m.Detail
			log.Printf("controld: runner %s: stage_failed for %s named no stage (%q)",
				name, clip(m.Session), clip(m.Detail))
		}
		ev.State = control.StateFailed
		ev.Detail = stageFailure(stage, rest)
	case "child_exited":
		// An OBSERVATION, not a transition. The agent process ended but the
		// session did not: sessiond outlives its child so viewers can still
		// read the scrollback, so the container is up, attachable, and holding
		// its slot. ApplyRunnerEvent ignores State on a child-exit event;
		// Running is the state the row is expected to be in.
		code, err := strconv.Atoi(m.Detail)
		if err != nil {
			// Dropped rather than defaulted: Atoi's zero would land in the
			// column as a CLEAN exit, which is the most misleading value
			// there is. A number we cannot read is better recorded as no
			// number at all.
			log.Printf("controld: runner %s: child_exited for %s carried an unreadable code %q; ignoring",
				name, clip(m.Session), clip(m.Detail))
			return
		}
		ev.State = control.StateRunning
		ev.ChildExitCode = &code
	case "setup_done", "credential_rejected":
		s.applyAdapterArm(ctx, name, m)
		return
	default:
		log.Printf("controld: runner %s: unknown event state %q for %s", name, clip(m.State), clip(m.Session))
		return
	}
	if err := s.fleet.ApplyRunnerEvent(ctx, ev); err != nil {
		// ErrStale and ErrConflict are the races reconciliation and events
		// have always had with each other — an event for a session this
		// runner no longer holds, or one the row has already moved past. They
		// are expected, not errors, and they are logged exactly where the old
		// applyEvent logged its own refusals.
		log.Printf("controld: runner %s: event %s for %s not applied: %v",
			name, clip(m.State), clip(m.Session), err)
	}
}

// applyAdapterArm handles the two events that transition nothing: a finished
// setup, which is news about the environment's snapshot cache, and a rejected
// credential, which is news about the owner's stored token. Both are host
// policy — the image cache and the vault are controld's, not the service's —
// and both need the session row, which they read directly (one of the direct
// store reads this composition keeps).
//
// Both keep the exact-placement guard. Without it the fleet-wide runner token
// would let the stale holder of a duplicate publish its own container as an
// environment's cache, or invalidate the credential of a user whose work it is
// no longer running.
func (s *Server) applyAdapterArm(ctx context.Context, name string, m runner.FromRunner) {
	row, err := s.st.Sessions().GetSession(ctx, installWorkspace, control.SessionID(m.Session))
	switch {
	case errors.Is(err, control.ErrNotFound):
		log.Printf("controld: runner %s: event for unknown session %s; ignoring", name, clip(m.Session))
		return
	case err != nil:
		log.Printf("controld: runner %s: event for %s: %v", name, clip(m.Session), err)
		return
	case !placedExactlyOn(row, name, m.State):
		return
	}

	switch m.State {
	case "setup_done":
		// Deliberately no transition: a finished setup is news about the
		// ENVIRONMENT, not about the session, whose state the registration
		// "running" event governs exactly as it does for a scratch session.
		s.cacheEnvironment(ctx, name, row)
	case "credential_rejected":
		// A git operation inside the sandbox was refused by GitHub. The vault
		// mints OPTIMISTICALLY — no GitHub round-trip per mint (design §4.2) —
		// so an observed refusal is the only signal a stored token has been
		// revoked, and this is where the fleet acts on it: the credential
		// flips to needs_refresh, so the next mint refuses with the named
		// action instead of handing out a value known not to work.
		//
		// The event carries nothing, deliberately: WHOSE credential it was is
		// the row's own answer, and a token — or anything derived from one —
		// has no business on this channel.
		if row.CreatorID == "" {
			log.Printf("controld: runner %s: credential_rejected for %s, which has no owner; ignoring", name, row.ID)
			return
		}
		s.rejectCredential(ctx, string(row.CreatorID), githubProvider)
	}
}

// stageFailure composes what a failed boot stage reads as in a session's
// error column: "<stage> failed: rc N: <tail of the output>". controld writes
// the verdict, the runner supplies the evidence — front-loading the rc is what
// keeps the two halves legible as one sentence — and nothing here parses the
// rc back out.
//
// The stage is clipped because it is runner-supplied text being promoted to
// the first word of controld's own sentence; every stage the contract defines
// (setup, clone, init) passes through untouched.
func stageFailure(stage, detail string) string {
	if detail == "" {
		return clip(stage) + " failed"
	}
	return clip(stage) + " failed: " + detail
}

// placedExactlyOn reports whether the store places row on the runner now
// reporting about it, logging the mismatch when it doesn't.
//
// applyEvent's own guard is deliberately looser — it accepts a row the store
// places nowhere, because a "running" event can outrun the create's own
// queued→creating transition. The events that END a session (dead, a failed
// boot stage), publish a fleet-wide fact (setup_done), or act on the OWNER's
// behalf (credential_rejected) need the exact match instead: an unplaced row
// is one a requeue cleared and the scheduler may have re-placed elsewhere, so
// a stale holder must not be able to kill the live copy, to have its own
// container's image published as an environment's cache, or to invalidate a
// credential for work it is no longer running.
func placedExactlyOn(row control.Session, runner, state string) bool {
	if string(row.RunnerID) == runner {
		return true
	}
	log.Printf("controld: runner %s reported %s for %s, but the store places it on %q; ignoring",
		runner, clip(state), row.ID, row.RunnerID)
	return false
}

// snapshotRef mints an environment snapshot's image ref:
// rainier-env:<envID>-<first 12 hex of the setup hash> (design §4.3). It is
// content-addressed, so every replica derives the same name from the same
// build inputs and stale refs stay prunable by prefix.
func snapshotRef(envID, setupHash string) string {
	if len(setupHash) > snapshotRefHashLen {
		setupHash = setupHash[:snapshotRefHashLen]
	}
	return "rainier-env:" + envID + "-" + setupHash
}

// cacheEnvironment turns one session's finished setup script into its
// environment's cached snapshot (design §4.3): the runner that ran the script
// is asked to commit the container, the resulting ref is recorded against the
// environment under a guarded write, and the rest of the fleet is told to
// warm it.
//
// The decision to snapshot at all is made HERE, in the connection's reader,
// so that a no-op is complete by the time the event is handled. The work that
// follows must not be: dispatch waits for a result THIS reader is the one to
// deliver, so running it inline would deadlock the connection until OpTimeout
// and stall every other event and result the runner sends meanwhile.
func (s *Server) cacheEnvironment(ctx context.Context, runner string, row control.Session) {
	env, hash, ok := s.snapshotWanted(ctx, runner, row)
	if !ok {
		return
	}
	go s.buildSnapshot(ctx, runner, row, env, hash)
}

// snapshotWanted decides whether row's finished setup is worth snapshotting,
// returning the environment it belongs to and the setup hash to cache it
// under. Everything is read fresh at event time: the environment may have
// been edited, deleted, or cached by a sibling session since this one was
// created.
//
// Five answers are "no", each an ordinary outcome rather than a failure:
//
//   - a scratch session has no environment to cache;
//   - the environment is gone;
//   - it is already cached at exactly this hash — a sibling session won the
//     race, or a cold resume re-ran an idempotent script;
//   - the container is not built from the environment's own image, so the
//     image it would produce is not this environment's cache at all. A
//     session that overrode `image` still runs the setup script, and an
//     environment whose image moved after this session was created is in the
//     same position: publishing either under the environment's hash would
//     hand every later session an image nobody asked for;
//   - the setup this session actually ran is not the setup the environment
//     describes now — its pinned SetupHash says so. This is the edit that
//     lands WHILE the script runs: nothing about the row changes, the image
//     still matches, and only the pin can tell that the container holds the
//     old script. A row with no pin at all (dispatched before the column
//     existed) fails the same way, which is the safe direction.
//
// The last two divide the work between them: the image check catches an
// environment whose image moved (and a session that overrode it), the pin
// catches a script edit — and since the hash is f(image, setup), together
// they leave no edit uncovered.
func (s *Server) snapshotWanted(ctx context.Context, runner string, row control.Session) (control.Environment, string, bool) {
	if row.EnvironmentID == "" {
		log.Printf("controld: runner %s: setup finished for scratch session %s; nothing to cache", runner, row.ID)
		return control.Environment{}, "", false
	}
	env, err := s.st.Environments().GetEnvironment(ctx, installWorkspace, row.EnvironmentID)
	switch {
	case errors.Is(err, control.ErrNotFound):
		log.Printf("controld: runner %s: setup finished for %s, whose environment %s is gone; nothing to cache",
			runner, row.ID, clip(string(row.EnvironmentID)))
		return control.Environment{}, "", false
	case err != nil:
		log.Printf("controld: runner %s: setup finished for %s: reading environment %s: %v",
			runner, row.ID, clip(string(row.EnvironmentID)), err)
		return control.Environment{}, "", false
	}

	// Recomputed rather than read out of env.SetupHash: this hash is both the
	// cache key and the value the guarded store write is checked against, so
	// deriving it from the very two fields the snapshot's content comes from
	// is what keeps the pair honest.
	hash := SetupHash(env.Image, env.Setup)
	switch {
	case env.SnapshotHash == hash:
		log.Printf("controld: environment %s is already cached as %s; %s needs no snapshot",
			env.ID, env.Snapshot.Ref, row.ID)
		return control.Environment{}, "", false
	case row.Spec.Image != env.Image:
		log.Printf("controld: environment %s: not caching %s — it ran the setup over %q, not the environment's %q",
			env.ID, row.ID, clip(row.Spec.Image), clip(env.Image))
		return control.Environment{}, "", false
	case row.SetupHash != hash:
		log.Printf("controld: environment %s: not caching %s — the setup it ran predates an edit to the environment",
			env.ID, row.ID)
		return control.Environment{}, "", false
	}
	return env, hash, true
}

// buildSnapshot asks runner to commit row's container under the environment's
// content-addressed ref, records it, and warms the rest of the fleet. It runs
// in its own goroutine (see cacheEnvironment).
//
// Nothing here needs undoing when a step fails: an environment with no
// snapshot recorded is exactly an environment whose next session runs the
// setup script again — slower, never wrong.
func (s *Server) buildSnapshot(ctx context.Context, runnerName string, row control.Session, env control.Environment, hash string) {
	ref := snapshotRef(string(env.ID), hash)
	res, err := s.transport.Dispatch(ctx, installPool, control.RunnerID(runnerName),
		runner.ToRunner{Type: "snapshot", Session: string(row.ID), Ref: ref})
	switch {
	case err != nil:
		log.Printf("controld: snapshotting %s for environment %s on %s: %v", row.ID, env.ID, runnerName, err)
		return
	case !res.OK:
		log.Printf("controld: snapshotting %s for environment %s on %s: runner reported failure: %s",
			row.ID, env.ID, runnerName, clip(res.Detail))
		return
	case res.Detail != "" && res.Detail != ref:
		// A runner echoes the ref it was given (the driver contract returns an
		// explicit ref verbatim). One that doesn't is a runner bug worth
		// saying out loud — and the ref recorded stays OURS, because the
		// content-addressed name is what every other replica derives
		// independently and what a later create looks the image up by.
		log.Printf("controld: runner %s answered the snapshot of environment %s with ref %q, not the %q it was given; recording ours",
			runnerName, env.ID, clip(res.Detail), ref)
	}

	// Deliberately not under the connection's context: the image exists on the
	// runner now, and a connection dying in this instant must not cost the
	// fleet a rebuild. Bounded so a wedged store cannot leak this goroutine.
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), storeCleanupTimeout)
	defer cancel()

	switch err := s.st.Environments().SetEnvironmentSnapshot(wctx, installWorkspace, env.ID, hash, ref, control.RunnerID(runnerName)); {
	case errors.Is(err, control.ErrStale), errors.Is(err, control.ErrNotFound):
		// The environment was edited (stale) or deleted (not found) while the
		// snapshot was building, so this image is of a setup nobody asked for
		// any more. The guarded write is precisely what keeps it from becoming
		// the cache (design §4.3); the next session rebuilds from the new
		// script.
		log.Printf("controld: environment %s changed while %s was being snapshotted; dropping %s",
			env.ID, row.ID, ref)
		return
	case err != nil:
		log.Printf("controld: recording snapshot %s for environment %s: %v", ref, env.ID, err)
		return
	}
	log.Printf("controld: environment %s cached as %s on %s", env.ID, ref, runnerName)

	// Warm every OTHER connected runner. The holder is excluded: it just built
	// the image, and with no registry in v0 that ref names something only it
	// has, so a prepull there could only fail. Fire-and-forget by design — a
	// prepull is a head start, never a precondition for a create (design §4.3).
	s.broadcastToRunners(runner.ToRunner{Type: "prepull", Ref: ref}, runnerName)
}

// destroyOrphan tells a runner to drop a session the fleet service named in
// its reconcile result: one the store has no live row for on this runner. It
// writes nothing — the service already settled the rows — and it runs outside
// the connection reader because the dispatch's result must be delivered by
// that reader. A failed driver teardown remains registered
// on runnerd, so retry it on this connection instead of waiting indefinitely
// for another announce. The series is bounded; reconnect reconciliation
// starts a fresh one if the orphan is still present.
func (s *Server) destroyOrphan(ctx context.Context, runnerName, id string) {
	go func() {
		for attempt := 1; attempt <= orphanDestroyAttempts; attempt++ {
			res, err := s.transport.Dispatch(ctx, installPool, control.RunnerID(runnerName),
				runner.ToRunner{Type: "destroy", Session: id})
			if err == nil && res.OK {
				return
			}

			if err != nil {
				log.Printf("controld: destroying orphan %s on %s (attempt %d/%d): %v",
					clip(id), runnerName, attempt, orphanDestroyAttempts, err)
			} else {
				log.Printf("controld: runner %s failed to destroy orphan %s (attempt %d/%d): %s",
					runnerName, clip(id), attempt, orphanDestroyAttempts, clip(res.Detail))
			}
			if attempt == orphanDestroyAttempts || ctx.Err() != nil {
				return
			}
			delay := orphanDestroyRetryDelay * time.Duration(1<<(attempt-1))
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return
			}
		}
	}()
}

// sendToRunner queues a command whose result nobody waits for (attach
// dial-backs and best-effort broadcasts). It reports only whether the command
// was queued for delivery. Orphan destroys use dispatch instead: a driver
// failure must be visible so it can be retried without another reconnect.
func (s *Server) sendToRunner(runner string, m runner.ToRunner) error {
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
func (s *Server) broadcastToRunners(m runner.ToRunner, except string) {
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

// errRegistrationRefused is what connectRunner reports when the fleet service
// declines the claim outright — an older generation than the store already
// holds. It says nothing the runner supplied.
var errRegistrationRefused = errors.New("registration refused")

// connectRunner installs rc as the live connection for its runner and
// registers the generation it claims with the fleet service, both under the
// runner's name lock so that a connection being retired at this same instant
// either writes its disconnect before us or (seeing itself replaced) not at
// all. The row write itself is the service's; the capabilities a runner
// advertises are the two synthesized for its own name (adapt_scope.go), and
// the native fleet repository now persists them.
func (s *Server) connectRunner(ctx context.Context, rc *runnerConn, ann runner.FromRunner) error {
	nl := s.nameLock(rc.name)
	nl.Lock()
	defer nl.Unlock()

	s.registerRunner(rc)
	reg, err := s.fleet.RegisterRunner(ctx, control.RunnerRegistration{
		WorkspaceID: installWorkspace, PoolID: installPool,
		RunnerID:      control.RunnerID(rc.name),
		Generation:    rc.gen,
		CapacityUsed:  ann.Used,
		CapacityTotal: ann.Total,
		Capabilities:  runnerCapabilities(rc.name),
		Sessions:      announcedSessions(ann.Sessions),
	})
	switch {
	case err != nil:
		return err
	case !reg.Accepted:
		return fmt.Errorf("%w: the fleet holds generation %d", errRegistrationRefused, reg.Generation)
	}
	return nil
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
	if err := s.st.Fleet().SetRunnerConnected(ctx, installPool, control.RunnerID(rc.name), false); err != nil && !errors.Is(err, control.ErrNotFound) {
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

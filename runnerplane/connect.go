package runnerplane

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// handleConnect serves the runner endpoint: authenticate, upgrade, take the
// announce, reconcile against it, then serve the connection until it dies.
func (p *Plane) handleConnect(w http.ResponseWriter, r *http.Request) {
	// The host answers who this is BEFORE the upgrade, so a connection it
	// declines costs nothing and is refused with an HTTP status the dialing
	// runner can read, rather than with a close frame after a handshake.
	b, err := p.host.Identify(r.Context(), r, "")
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return // Accept already answered the request
	}
	defer c.CloseNow()
	c.SetReadLimit(p.readLimit)

	// connCtx scopes every read and write on this socket. It is canceled when
	// the handler returns, which is what stops the writer goroutine.
	connCtx, cancel := context.WithCancel(r.Context())
	defer cancel()

	ann, ok := p.readAnnounce(connCtx, c)
	if !ok {
		return
	}
	switch {
	case b.RunnerID == "":
		// The host's credential is fleet-wide, so the runner names itself.
		b.RunnerID = control.RunnerID(ann.Runner)
	case string(b.RunnerID) != ann.Runner:
		// The host bound this connection to a runner id of its own, and the
		// announce claims another. The credential wins, and the connection
		// does not continue under a name it may not use.
		p.logf("runner %s announced the name %q, which its credential does not name; refusing",
			b.RunnerID, clip(ann.Runner))
		p.closeConn(c, websocket.StatusPolicyViolation, "the announced name is not this connection's runner")
		return
	}
	name := string(b.RunnerID)

	// The generation is minted before anything is registered, so it is the
	// one this connection acts under for its whole life: registration,
	// reconciliation, every event read off the socket, and every heartbeat.
	// It comes from the HOST's store, so it continues across a restart and no
	// two replicas can hand out the same authority. A store that cannot mint
	// one is a connection that must not be served at all — everything
	// downstream fences on this number, so serving without it would be
	// serving with no authority rather than with fresh authority.
	gen, err := p.host.NextGeneration(connCtx, b)
	if err != nil {
		p.logf("opening a generation for runner %s: %v", name, err)
		p.closeConn(c, websocket.StatusInternalError, "registration refused")
		return
	}

	rc := newRunnerConn(b, c)
	rc.gen = gen
	connErr := p.connectRunner(connCtx, rc, ann)

	var writerDone sync.WaitGroup
	defer func() {
		p.retireRunner(rc) // closes done, which is what lets the writer exit
		writerDone.Wait()
	}()
	if connErr != nil {
		// A refused claim was never installed as the live connection (#36),
		// so the deferred retire above finds rc is not current and leaves the
		// accepted connection — if any — untouched.
		p.logf("registering runner %s (generation %d): %v", name, rc.gen, connErr)
		p.closeConn(c, websocket.StatusInternalError, "registration refused")
		return
	}

	writerDone.Add(1)
	go func() {
		defer writerDone.Done()
		p.writeLoop(connCtx, rc)
	}()

	p.logf("runner %s connected (used %d/%d, %d announced sessions)",
		name, ann.Used, ann.Total, len(ann.Sessions))

	// Reconciliation is the fleet service's: the announce is authoritative
	// for liveness, the store for desired state, and the service settles the
	// two and hands back the ids this runner must tear down. The writer is
	// already running, because tearing an orphan down means dispatching to
	// this very connection.
	res, err := p.host.Fleet().ReconcileRunner(connCtx, control.RunnerSnapshot{
		WorkspaceID: b.WorkspaceID, PoolID: b.PoolID,
		RunnerID:      b.RunnerID,
		Generation:    rc.gen,
		CapacityUsed:  ann.Used,
		CapacityTotal: ann.Total,
		Sessions:      announcedSessions(ann.Sessions),
	})
	if err != nil {
		p.logf("reconciling runner %s (generation %d): %v", name, rc.gen, err)
		p.closeConn(c, websocket.StatusInternalError, "reconcile failed")
		return
	}
	for _, id := range res.Destroy {
		p.destroyOrphan(connCtx, rc, string(id))
	}
	// One wake covers everything reconciliation can free (a dead session, an
	// adopted cold suspend) plus the capacity the announce itself reports.
	p.host.Wake(b.PoolID)

	p.readLoop(connCtx, rc)
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

// readAnnounce reads and validates the connection's first message, which must
// be an announce in a proto we speak. A rejection closes the socket with a
// reason naming both versions, so the operator reading runnerd's log learns
// what to upgrade.
func (p *Plane) readAnnounce(ctx context.Context, c *websocket.Conn) (runner.FromRunner, bool) {
	annCtx, cancel := context.WithTimeout(ctx, announceFirstTimeout)
	defer cancel()

	var ann runner.FromRunner
	if err := wsjson.Read(annCtx, c, &ann); err != nil {
		p.logf("reading runner announce: %v", err)
		return runner.FromRunner{}, false
	}
	switch {
	case ann.Type != "announce":
		p.closeConn(c, websocket.StatusPolicyViolation,
			fmt.Sprintf("first message must be announce, got %q", clip(ann.Type)))
		return runner.FromRunner{}, false
	case ann.Proto != runner.ProtocolVersion:
		p.closeConn(c, websocket.StatusPolicyViolation,
			fmt.Sprintf("unsupported proto %d, want proto %d", ann.Proto, runner.ProtocolVersion))
		return runner.FromRunner{}, false
	case ann.Runner == "":
		p.closeConn(c, websocket.StatusPolicyViolation, "announce is missing a runner name")
		return runner.FromRunner{}, false
	}
	return ann, true
}

// errRegistrationRefused is what connectRunner reports when the fleet service
// declines the claim outright — an older generation than the store already
// holds. It says nothing the runner supplied.
var errRegistrationRefused = errors.New("registration refused")

// connectRunner registers the generation rc claims with the fleet service
// before installing it as the runner's live connection, both under the
// runner's name lock so that a connection being retired at this same instant
// either writes its disconnect before us or (seeing itself replaced) not at
// all. The row write itself is the service's; the capability the plane spells
// for a runner's own name rides along with what the runner claimed.
func (p *Plane) connectRunner(ctx context.Context, rc *runnerConn, ann runner.FromRunner) error {
	// Validated BEFORE anything is registered: a runner claiming something it
	// may not claim is not a runner this fleet will schedule on at all, and
	// the refusal must leave nothing behind.
	if err := validateAnnouncedCapabilities(ann.Capabilities); err != nil {
		return fmt.Errorf("%w: %v", errRegistrationRefused, err)
	}

	nl := p.nameLock(rc.name)
	nl.Lock()
	defer nl.Unlock()

	// The plane's spelling of this runner's own name first, then what the
	// runner claims about itself. One list, two authors, and the order says
	// which is which. Kept on the connection so the heartbeat writes the same
	// list this registration did.
	rc.caps = append(hostCapabilities(rc.name), ann.Capabilities...)

	reg, err := p.host.Fleet().RegisterRunner(ctx, control.RunnerRegistration{
		WorkspaceID: rc.binding.WorkspaceID, PoolID: rc.binding.PoolID,
		RunnerID:      rc.binding.RunnerID,
		Generation:    rc.gen,
		CapacityUsed:  ann.Used,
		CapacityTotal: ann.Total,
		Capabilities:  rc.caps,
		Sessions:      announcedSessions(ann.Sessions),
	})
	switch {
	case err != nil:
		return err
	case !reg.Accepted:
		return fmt.Errorf("%w: the fleet holds generation %d", errRegistrationRefused, reg.Generation)
	}
	// Only a claim the fleet accepted becomes the live connection. Swapping
	// the socket in first would close whatever it replaces — possibly an
	// already-accepted, higher-generation connection — on the way to being
	// refused itself, leaving the runner fully disconnected until its next
	// redial (#36).
	p.registerRunner(rc)

	// The answer to the announce, and the first thing this runner hears: the
	// generation it acts under, and the capabilities the plane took from it.
	// Enqueued while the registration still holds the name lock and before
	// reconcile dispatches a single destroy, so it cannot arrive behind a
	// command — the writer drains this queue in order.
	if err := rc.enqueue(runner.ToRunner{
		Type: "accept", Generation: rc.gen, Capabilities: ann.Capabilities,
	}); err != nil {
		return err
	}
	return nil
}

// readLoop serves one connection's inbound messages until it dies.
func (p *Plane) readLoop(ctx context.Context, rc *runnerConn) {
	for {
		var m runner.FromRunner
		if err := wsjson.Read(ctx, rc.ws, &m); err != nil {
			p.logf("runner %s connection ended: %v", rc.name, err)
			return
		}
		// Capacity rides every message, not just announces, so the fleet view
		// is current without a separate capacity message. touchRunner reports
		// false when a reconnect has replaced us: the new conn owns the runner
		// now, and anything we did here would write yesterday's news over
		// today's.
		if !p.touchRunner(ctx, rc, m) {
			return
		}

		switch m.Type {
		case "result":
			if m.ReqID == 0 {
				continue // a fire-and-forget command's ack; nobody is waiting
			}
			if !rc.deliver(m) {
				p.logf("runner %s: result for unknown req_id %d (timed out?)", rc.name, m.ReqID)
			}
		case "event":
			p.applyRunnerEvent(ctx, rc, m)
		case "session_req":
			// One message type, both halves of the session RPC's upward
			// direction: a sandbox's own request, and the answer to one this
			// replica sent down. routeSessionReq tells them apart and keeps
			// the slow half off this goroutine — see its doc comment.
			p.routeSessionReq(ctx, rc, m)
		default:
			p.logf("runner %s: unexpected message type %q", rc.name, clip(m.Type))
		}
	}
}

// destroyOrphan tells a runner to drop a session the fleet service named in
// its reconcile result: one the store has no live row for on this runner. It
// writes nothing — the service already settled the rows — and it runs outside
// the connection reader because the dispatch's result must be delivered by
// that reader. A failed driver teardown remains registered on runnerd, so
// retry it on this connection instead of waiting indefinitely for another
// announce. The series is bounded; reconnect reconciliation starts a fresh one
// if the orphan is still present.
func (p *Plane) destroyOrphan(ctx context.Context, rc *runnerConn, id string) {
	pool, runnerID, name := rc.binding.PoolID, rc.binding.RunnerID, rc.name
	go func() {
		for attempt := 1; attempt <= orphanDestroyAttempts; attempt++ {
			res, err := p.Transport().Dispatch(ctx, pool, runnerID,
				runner.ToRunner{Type: "destroy", Session: id})
			if err == nil && res.OK {
				return
			}

			if err != nil {
				p.logf("destroying orphan %s on %s (attempt %d/%d): %v",
					clip(id), name, attempt, orphanDestroyAttempts, err)
			} else {
				p.logf("runner %s failed to destroy orphan %s (attempt %d/%d): %s",
					name, clip(id), attempt, orphanDestroyAttempts, clip(res.Detail))
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

// ---------------------------------------------------------------------------
// capabilities
// ---------------------------------------------------------------------------

// MaxCapabilities is the most capabilities any one claim may carry.
const MaxCapabilities = 32

// placementCapabilityPrefix is the capability spelling of an explicit runner
// pin, which control.Requirements cannot name directly. It is the plane's own
// prefix, and the only one it synthesizes for a runner's name.
const placementCapabilityPrefix = "placement:"

// capabilityToken is the one token rule for a portable capability: a
// lowercase token. It is deliberately narrow — a capability is matched by
// exact string equality across a fleet of runners nobody re-deploys at once,
// so a spelling that varies by case or whitespace is a placement that
// silently never happens. A colon can never match it, which is what reserves
// prefixed capabilities for the plane and its host.
var capabilityToken = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

// ValidCapability reports whether s is a well-formed portable capability. It
// is the one token rule, exported once so a host applies exactly the same one
// to the capabilities an operator writes on an environment.
func ValidCapability(s string) bool { return capabilityToken.MatchString(s) }

// hostCapabilities is what every runner advertises for its own name: the one
// capability an environment pinned to it by placement requires.
func hostCapabilities(name string) []string {
	return []string{placementCapabilityPrefix + name}
}

// validateAnnouncedCapabilities checks what a runner claimed on its announce.
// A failure refuses the registration outright rather than dropping the
// offending token: a runner that announces something it may not announce is
// not misconfigured in a way the plane can silently paper over, and
// half-accepting its claims would place sessions on a runner nobody described.
//
// Host prefixes are refused rather than ignored: the capability the plane
// spells for a runner's own name is the HOST's claim about it, and a runner or
// an operator that could write one could pin any environment to any runner.
func validateAnnouncedCapabilities(caps []string) error {
	const what = "announced capabilities"
	if len(caps) > MaxCapabilities {
		return fmt.Errorf("%s: at most %d are allowed, got %d", what, MaxCapabilities, len(caps))
	}
	seen := make(map[string]bool, len(caps))
	for _, c := range caps {
		switch {
		case strings.Contains(c, ":"):
			return fmt.Errorf("%s: %q carries a host prefix, which only the control plane may claim", what, clip(c))
		case !ValidCapability(c):
			return fmt.Errorf("%s: %q must match [a-z0-9][a-z0-9._-]{0,63}", what, clip(c))
		case seen[c]:
			return fmt.Errorf("%s: %q is listed twice", what, clip(c))
		}
		seen[c] = true
	}
	return nil
}

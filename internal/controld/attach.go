// internal/controld/attach.go
package controld

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"rainier/internal/relay"
	"rainier/internal/rwire"
	"rainier/internal/wire"
)

const (
	// attachReadLimit matches runnerd's and sessiond's own read limits: a
	// snapshot replaying a large scrollback is the biggest frame either
	// direction of this splice ever carries.
	attachReadLimit = 16 << 20
	// attachPollInterval is how often the bounded wait re-reads a session
	// row while waiting for it to reach `running`.
	attachPollInterval = 100 * time.Millisecond
	// attachFirstMsgTimeout bounds how long an upgraded-but-silent client
	// may hold a socket before sending the resize the protocol requires
	// first. Without it a client that connects and says nothing parks a
	// goroutine and a file descriptor indefinitely.
	attachFirstMsgTimeout = 15 * time.Second
)

// errSessionNotReady is waitRunning's answer when the session did not reach
// `running` within the wait budget — or never will, because it is already
// terminal. The route maps it to 503 `session_not_ready`.
var errSessionNotReady = errors.New("session not ready")

// ---------------------------------------------------------------------------
// pairing table
// ---------------------------------------------------------------------------

// pendingAttach is one client socket parked between the dial_attach sent to
// its runner and the dial-back that claims it.
//
// done is closed by whoever claims the entry — the dial-back once its splice
// has finished, or the TTL timer when nobody ever came — and is what releases
// the client handler. That handler must not return before then: returning
// runs its deferred close on a socket the splice is still using.
type pendingAttach struct {
	client relay.Conn
	done   chan struct{}
}

// attachTable holds the pairings this replica is waiting on, keyed by
// attach_id. The state is deliberately replica-local (design §6): the
// dial-back's target_url names this exact replica, so no other one can be
// asked to claim an entry, and a replica dying takes only its own live
// attaches with it — clients re-attach, nothing else is lost.
type attachTable struct {
	mu sync.Mutex
	m  map[string]*pendingAttach
}

func newAttachTable() *attachTable { return &attachTable{m: map[string]*pendingAttach{}} }

// park registers pa under id, reporting false if that id is already parked
// rather than overwriting it. An overwrite would orphan the previous client
// on a `done` nobody holds any more — it would hang until its own handler's
// TTL fired, and the TTL would then close a socket the table no longer knows
// about. Ids are 8 random bytes, so a genuine collision is not a thing that
// happens; a duplicate means something is wrong and the caller says so
// loudly rather than papering over it.
func (t *attachTable) park(id string, pa *pendingAttach) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.m[id]; exists {
		return false
	}
	t.m[id] = pa
	return true
}

// claim removes and returns id's entry. The lookup and the removal are one
// locked step, which is what makes ownership of a parked socket exclusive:
// the dial-back and the TTL timer both race for it and exactly one wins, so
// the socket is never closed by one while the other is splicing it, and
// pendingAttach.done is never closed twice.
func (t *attachTable) claim(id string) (*pendingAttach, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	pa, ok := t.m[id]
	if ok {
		delete(t.m, id)
	}
	return pa, ok
}

// has reports whether id is currently parked. It is a hint, not a claim: the
// dial-back uses it to answer an expired attach_id with a plain HTTP 404
// before upgrading. Claiming that early would be a bug — a failed upgrade
// would then leave the client parked on a `done` nobody can ever close — so
// claim, after the upgrade, stays authoritative.
func (t *attachTable) has(id string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.m[id]
	return ok
}

// ---------------------------------------------------------------------------
// WS GET /v1/sessions/{id}/attach?since=
// ---------------------------------------------------------------------------

// handleClientAttach serves the client half of the dial-back attach plane
// (design §4.2). controld never interprets terminal traffic: it authenticates
// the caller, waits (bounded) for the session to be attachable, parks the
// socket under a fresh attach_id, asks the owning runner to dial back, and
// then splices the two sockets as a dumb byte pipe. The client speaks
// wire.ClientMsg/ServerMsg end to end — byte-identical to attaching to
// runnerd directly.
//
// Every failure that can be reported as HTTP is reported before the upgrade:
// once the socket is a websocket, a status code has nowhere to go and the
// only thing left is a close reason.
//
// Attach carries stdin, so it is a mutation in every sense that matters and
// takes §4.4's owner-or-admin rule, not the team-wide read rule — and that
// check runs BEFORE the bounded wait: a caller who may not touch this
// session learns so at once instead of after holding a request open for ten
// seconds (an unauthorized caller must never get to occupy a slot, nor to
// probe another team member's session state by timing the answer).
func (s *Server) handleClientAttach(w http.ResponseWriter, r *http.Request, u User) {
	id := r.PathValue("id")
	// A malformed since is read as 0, exactly like runnerd's own /attach: it
	// costs a full replay, never an error the client can do anything about.
	since, _ := strconv.ParseUint(r.URL.Query().Get("since"), 10, 64)

	row, err := s.st.GetSession(r.Context(), id)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeErr(w, http.StatusNotFound, "not_found", "session not found")
			return
		}
		log.Printf("controld: attach %s: %v", id, err)
		writeErr(w, http.StatusInternalServerError, "internal", "could not attach to session")
		return
	}
	if !authorizeOwnerOrAdmin(u, row) {
		writeErr(w, http.StatusForbidden, "forbidden", "not authorized to attach to this session")
		return
	}

	// Re-read through the bounded wait: authorization is settled above, and
	// what this needs now is the state (and placement) as of the moment the
	// session actually becomes attachable.
	row, err = s.waitRunning(r.Context(), id)
	switch {
	case errors.Is(err, ErrNotFound):
		writeErr(w, http.StatusNotFound, "not_found", "session not found")
		return
	case errors.Is(err, errSessionNotReady):
		writeErr(w, http.StatusServiceUnavailable, "session_not_ready",
			fmt.Sprintf("session is %s, not running", row.State))
		return
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return // the client hung up while we waited; there is nobody to answer
	case err != nil:
		log.Printf("controld: attach %s: %v", id, err)
		writeErr(w, http.StatusInternalServerError, "internal", "could not attach to session")
		return
	}
	// Running but unreachable: the row says a runner holds this session and
	// that runner has no control connection here, so there is nothing to
	// send a dial_attach down.
	if row.Runner == "" || !s.runnerConnected(row.Runner) {
		writeErr(w, http.StatusBadGateway, "runner_unreachable", "runner is not connected")
		return
	}

	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return // Accept has already answered the request
	}
	defer c.CloseNow()
	c.SetReadLimit(attachReadLimit)

	first, err := readFirstResize(r.Context(), c)
	if err != nil {
		closeAttach(c, websocket.StatusPolicyViolation, err.Error())
		return
	}

	attachID := randHex(8) // 16 hex characters, crypto/rand
	pa := &pendingAttach{client: relay.WSConn(c), done: make(chan struct{})}
	// Park before sending: the runner can dial back the instant it reads the
	// command, and an entry that isn't there yet would be refused.
	if !s.attaches.park(attachID, pa) {
		log.Printf("controld: attach %s: attach id %s is already parked; refusing rather than "+
			"overwriting another client's pairing", id, attachID)
		closeAttach(c, websocket.StatusInternalError, "attach id collision")
		return
	}

	dial := rwire.ToRunner{Type: "dial_attach", Session: id, Attach: &rwire.Attach{
		AttachID:  attachID,
		Since:     since,
		Cols:      first.Cols,
		Rows:      first.Rows,
		TargetURL: s.attachBackURL(attachID),
	}}
	if err := s.sendToRunner(row.Runner, dial); err != nil {
		// The command never left this process, so no runner can ever claim
		// this entry: take it back and close the client ourselves. The 502
		// this would have been is moot post-upgrade — a close reason is all
		// the client can still be told.
		s.attaches.claim(attachID)
		log.Printf("controld: attach %s: %v", id, err)
		closeAttach(c, websocket.StatusTryAgainLater, "runner unreachable")
		return
	}

	// Nobody may hold a parked socket forever. If the dial-back never comes
	// (the runner died between reading the command and dialing, the command
	// was lost with a flapping conn), the TTL is what closes the client
	// rather than leaving it waiting on a terminal that will never speak.
	ttl := time.AfterFunc(s.cfg.AttachPairTTL, func() {
		if _, ok := s.attaches.claim(attachID); !ok {
			return // the dial-back got here first; it owns the socket now
		}
		log.Printf("controld: attach %s: no dial-back from %s within %s; closing the client",
			id, row.Runner, s.cfg.AttachPairTTL)
		closeAttach(c, websocket.StatusTryAgainLater, "runner did not dial back")
		close(pa.done)
	})
	defer ttl.Stop()

	// Hold the handler open for the life of the attach: the socket now
	// belongs to whoever claims the pairing, and this goroutine's deferred
	// close must not fire until they are done with it.
	<-pa.done
}

// waitRunning polls id's row until the session is running, bounded by
// cfg.AttachWait. A session created a moment ago is legitimately a few
// seconds from running (§5's "attach to a not-yet-running session"), and
// holding the request open for that is friendlier than making every client
// implement its own retry loop — the CLI still gets a 503 to spin on when the
// budget runs out.
func (s *Server) waitRunning(ctx context.Context, id string) (Session, error) {
	deadline := time.Now().Add(s.cfg.AttachWait)
	for {
		row, err := s.st.GetSession(ctx, id)
		if err != nil {
			return Session{}, err
		}
		switch {
		case row.State == StateRunning:
			return row, nil
		case row.State.Terminal():
			// Terminal is permanent: waiting out the budget would buy the
			// client nothing but a slower identical answer.
			return row, errSessionNotReady
		case !time.Now().Before(deadline):
			return row, errSessionNotReady
		}
		select {
		case <-time.After(attachPollInterval):
		case <-ctx.Done():
			return Session{}, ctx.Err()
		}
	}
}

// readFirstResize reads exactly one wire.ClientMsg off a freshly attached
// client and requires it to be a "resize" — the same contract runnerd's own
// readFirstResize enforces, so a client attaching through controld and one
// attaching to runnerd directly behave identically.
//
// Its cols/rows go into dial_attach, which become the FrameOpen's — so this
// message is consumed here and deliberately NOT forwarded into the splice:
// the FrameOpen already conveys the size, and forwarding would double-deliver
// the same resize. Every later resize flows through the splice as ordinary
// client traffic.
func readFirstResize(ctx context.Context, c *websocket.Conn) (wire.ClientMsg, error) {
	ctx, cancel := context.WithTimeout(ctx, attachFirstMsgTimeout)
	defer cancel()

	var m wire.ClientMsg
	if err := wsjson.Read(ctx, c, &m); err != nil {
		return wire.ClientMsg{}, fmt.Errorf("reading the first attach message: %w", err)
	}
	if m.Type != "resize" {
		return wire.ClientMsg{}, fmt.Errorf("first attach message must be resize, got %q", clip(m.Type))
	}
	return m, nil
}

// attachBackURL renders the dial-back URL for attachID: this replica's own
// ExternalURL with the scheme switched to ws(s). Naming the replica
// explicitly is what keeps the pairing correct once more than one controld
// runs (design §6) — the runner must come back to the replica holding the
// client socket, not to whichever one a load balancer picks.
func (s *Server) attachBackURL(attachID string) string {
	base := s.cfg.ExternalURL // New guarantees an absolute http(s) URL
	switch {
	case strings.HasPrefix(base, "https://"):
		base = "wss://" + strings.TrimPrefix(base, "https://")
	case strings.HasPrefix(base, "http://"):
		base = "ws://" + strings.TrimPrefix(base, "http://")
	}
	return base + "/v1/runners/attach-back?attach_id=" + attachID
}

// ---------------------------------------------------------------------------
// WS GET /v1/runners/attach-back?attach_id=
// ---------------------------------------------------------------------------

// handleAttachBack serves the runner half of the pairing: runnerd dials this
// outbound (spec rule 3 — nothing dials into a runner) carrying the attach_id
// controld handed it, and controld splices that socket onto the client
// waiting under it. Authentication is the fleet runner token, the same check
// handleRunnerConnect makes.
func (s *Server) handleAttachBack(w http.ResponseWriter, r *http.Request) {
	if !s.runnerTokenOK(r.Header.Get("Authorization")) {
		writeErr(w, http.StatusUnauthorized, "unauthenticated", "invalid runner token")
		return
	}
	attachID := r.URL.Query().Get("attach_id")
	// Answer an expired or unknown pairing as plain HTTP, before upgrading:
	// a runner that dialed back too late gets a status code it can log
	// rather than a websocket it must decode a close reason from. This is
	// only a hint — claim below is what actually takes ownership.
	if !s.attaches.has(attachID) {
		writeErr(w, http.StatusNotFound, "not_found", "unknown attach id")
		return
	}

	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return // Accept has already answered the request
	}
	defer c.CloseNow()
	c.SetReadLimit(attachReadLimit)

	pa, ok := s.attaches.claim(attachID)
	if !ok {
		// The TTL fired between the check above and here: the client socket
		// is already closed and gone. Not a protocol violation on the
		// runner's part — it did exactly what it was told, just too late —
		// so it gets "try again later", the same code the expired client got.
		closeAttach(c, websocket.StatusTryAgainLater, "attach pairing expired")
		return
	}
	// Release the client handler once the splice is over, whatever ends it.
	defer close(pa.done)

	splice(r.Context(), pa.client, relay.WSConn(c))
}

// splice pumps text frames both directions until either side dies, then
// closes both. controld stays a dumb relay: payloads are opaque bytes.
func splice(ctx context.Context, a, b relay.Conn) {
	done := make(chan struct{}, 2)
	pump := func(src, dst relay.Conn) {
		for {
			m, err := src.Read(ctx)
			if err != nil {
				break
			}
			if dst.Write(ctx, m) != nil {
				break
			}
		}
		done <- struct{}{}
	}
	go pump(a, b)
	go pump(b, a)
	<-done
	a.Close()
	b.Close()
	<-done // let the second pump exit before returning
}

// closeAttach closes an attach socket with a reason the protocol can actually
// carry. Close reasons cap at 123 bytes and a wrapped read error can exceed
// that; an over-long reason makes coder/websocket drop the close frame
// entirely, leaving the peer with a bare EOF instead of the diagnostic.
// Same discipline as closeRunner on the runner plane.
func closeAttach(c *websocket.Conn, code websocket.StatusCode, reason string) {
	const maxReason = 123
	if len(reason) > maxReason {
		reason = strings.ToValidUTF8(reason[:maxReason], "")
	}
	if err := c.Close(code, reason); err != nil {
		log.Printf("controld: closing attach socket: %v", err)
	}
}

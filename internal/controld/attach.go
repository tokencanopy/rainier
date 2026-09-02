// internal/controld/attach.go
package controld

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/terminal"
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
	stream control.TerminalStream
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
// WS GET /v0/sessions/{id}/attach?since=
// ---------------------------------------------------------------------------

// handleClientAttach serves the client half of the dial-back attach plane
// (design §4.2). controld never interprets terminal traffic: it authenticates
// the caller, waits (bounded) for the session to be attachable, and hands the
// upgraded socket to the attachment service, whose broker parks it under a
// fresh attach_id, asks the owning runner to dial back, and splices the two.
// A message is decoded only to be forwarded, and the client speaks
// terminal.ClientMessage/ServerMsg end to end — indistinguishable from
// attaching to runnerd directly.
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

	if !s.mayAttach(w, r, u, id) {
		return
	}

	// Re-read through the bounded wait: authorization is settled above, and
	// what this needs now is the state (and placement) as of the moment the
	// session actually becomes attachable.
	row, err := s.waitRunning(r.Context(), id)
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
	if row.Runner == "" || !s.transport.Connected(installPool, control.RunnerID(row.Runner)) {
		writeErr(w, http.StatusBadGateway, "runner_unreachable", "runner is not connected")
		return
	}

	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return // Accept has already answered the request
	}
	defer c.CloseNow()
	c.SetReadLimit(attachReadLimit)

	// From here the attach belongs to the attachment service: it re-checks
	// authority and attachability against the authoritative row, fences the
	// controller generation, and hands the stream to the broker, which parks
	// it and asks the runner to dial back. Every answer left is a close
	// reason, so a failure is closed rather than written.
	stream := newWSTerminalStream(c)
	ctx := withAttachSince(withUser(r.Context(), u), since)
	err = s.attachments.AttachTerminal(ctx, userScope(u), control.AttachTerminal{
		SessionID: control.SessionID(id),
		Since:     since,
		Mode:      control.AttachmentController,
	}, stream)
	if err != nil {
		_ = stream.Close(err)
	}
}

// mayAttach is the pre-upgrade half of the authorization the attachment
// service performs again for itself, and it exists for one reason: a 403 is a
// status code, and a status code has nowhere to go once the socket has been
// upgraded. Refusing here also keeps an unauthorized caller from occupying a
// wait slot, or from timing the answer to learn whether a session they may
// not touch exists and is starting.
//
// The decision itself is not a second implementation: it is the same
// ownerOrAdmin policy adapter, asked the same question about the same
// resource, and the service's own answer downstream stays authoritative.
func (s *Server) mayAttach(w http.ResponseWriter, r *http.Request, u User, id string) bool {
	row, err := s.st.GetSession(r.Context(), id)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeErr(w, http.StatusNotFound, "not_found", "session not found")
			return false
		}
		log.Printf("controld: attach %s: %v", clip(id), err)
		writeErr(w, http.StatusInternalServerError, "internal", "could not attach to session")
		return false
	}
	resource := control.Resource{Kind: control.ResourceSession, WorkspaceID: installWorkspace,
		ID: row.ID, CreatorID: control.ActorID(row.OwnerID)}
	if err := (ownerOrAdmin{}).AuthorizeAttachment(withUser(r.Context(), u), userScope(u),
		resource, control.AttachmentController); err != nil {
		writeErr(w, http.StatusForbidden, "forbidden", "not authorized to attach to this session")
		return false
	}
	return true
}

// waitRunning polls id's row until the session is attachable, bounded by
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
		case s.failedButAttachable(row):
			// Immediately, without consuming any of the budget: this state is
			// as settled as running is, and the whole point of admitting it is
			// that the diagnosis is waiting on the other side.
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

// failedButAttachable reports whether a `failed` session can still be attached
// to: it is placed on a runner this replica holds a control connection to.
//
// A failed setup is the case this exists for. The session's error column
// carries only the last 2KB the script printed, and the rest of the log — the
// package that actually 404'd, the compiler line that broke — is inside the
// container, where sessiond is still running and still serving viewers (it
// only ever execs the agent on rc 0; the process itself stays up). Refusing
// the attach left a user with a truncated tail and a container burning a slot
// they had no reason to keep. Design §4.3's "attach --since 0 shows
// everything" is written about setup output; it holds for the failures too, or
// it means nothing where it matters most.
//
// Only `failed`, never the other terminal states: canceled and destroyed have
// no container by construction, and `dead` is the runner reporting the
// container gone. And only while the runner is connected, because the dial-back
// is the only way in.
//
// A create that failed BEFORE its container existed (failCreate — a bad spec, a
// secret deleted mid-flight) also lands here and is also admitted: the row
// looks identical, and the runner is the only authority on whether a container
// is there. That attach ends in the pairing TTL closing the client rather than
// an immediate 503 — a slower "no", for the case where the diagnosis was in the
// error column all along. Discriminating it here would mean parsing the error
// text, which nothing else in this codebase does and which would go stale the
// first time the wording changed.
//
// The window is one control connection wide, and deliberately not widened: the
// next announce from that runner carries a container the store has already
// finished, and reconciliation collects it as an orphan (§4.8). Keeping a
// failed session's container across reconnects would mean exempting it from
// that rule with nothing left to reap it. Documented in deploy-gce.md §7 as
// "read the log before restarting runnerd".
func (s *Server) failedButAttachable(row Session) bool {
	return row.State == StateFailed && row.Runner != "" && s.runnerConnected(row.Runner)
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
	return base + "/v0/runners/attach-back?attach_id=" + attachID
}

// ---------------------------------------------------------------------------
// WS GET /v0/runners/attach-back?attach_id=
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

	splice(r.Context(), pa.stream, relay.WSConn(c))
}

// splice pumps one live attach both directions until either side ends, then
// closes both. The two halves are typed differently and deliberately so: the
// client speaks whole terminal messages across control.TerminalStream, while
// the runner's dial-back socket is raw relay frames — and protocol/terminal
// is the wire format on both, so re-encoding between them is lossless.
// controld still interprets nothing: a message is decoded to be forwarded and
// for no other reason, and none of it is logged.
func splice(ctx context.Context, client control.TerminalStream, runner relay.Conn) {
	done := make(chan struct{}, 2)
	go func() {
		defer func() { done <- struct{}{} }()
		for {
			m, err := client.Receive(ctx)
			if err != nil {
				return
			}
			raw, err := json.Marshal(m)
			if err != nil {
				return
			}
			if runner.Write(ctx, raw) != nil {
				return
			}
		}
	}()
	go func() {
		defer func() { done <- struct{}{} }()
		for {
			raw, err := runner.Read(ctx)
			if err != nil {
				return
			}
			var m terminal.ServerMessage
			if json.Unmarshal(raw, &m) != nil {
				// A frame that is not a server message is the runner half
				// breaking the protocol; ending the attach says so, where
				// dropping it would leave a client missing output it has no
				// way to notice. Nothing about the frame is logged.
				return
			}
			if client.Send(ctx, m) != nil {
				return
			}
		}
	}()
	<-done
	_ = client.Close(errAttachEnded)
	runner.Close()
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

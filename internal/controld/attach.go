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
	"time"

	"github.com/coder/websocket"

	"github.com/tokencanopy/rainier/attachplane"
	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
)

const (
	// attachPollInterval is how often the bounded wait re-reads a session
	// row while waiting for it to reach `running`.
	attachPollInterval = 100 * time.Millisecond
)

// errSessionNotReady is waitRunning's answer when the session did not reach
// `running` within the wait budget — or never will, because it is already
// terminal. The route maps it to 503 `session_not_ready`.
var errSessionNotReady = errors.New("session not ready")

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
	case errors.Is(err, control.ErrNotFound):
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
	if row.RunnerID == "" || !s.transport.Connected(installPool, row.RunnerID) {
		writeErr(w, http.StatusBadGateway, "runner_unreachable", "runner is not connected")
		return
	}

	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return // Accept has already answered the request
	}
	defer c.CloseNow()

	// From here the attach belongs to the attachment service: it re-checks
	// authority and attachability against the authoritative row, fences the
	// controller generation, and hands the stream to the broker, which parks
	// it and asks the runner to dial back. Every answer left is a close
	// reason, so a failure is closed rather than written.
	stream := attachplane.ClientStream(c)
	ctx := attachplane.WithSince(withUser(r.Context(), u), since)
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
	row, err := s.st.Sessions().GetSession(r.Context(), installWorkspace, control.SessionID(id))
	if err != nil {
		if errors.Is(err, control.ErrNotFound) {
			writeErr(w, http.StatusNotFound, "not_found", "session not found")
			return false
		}
		log.Printf("controld: attach %s: %v", clip(id), err)
		writeErr(w, http.StatusInternalServerError, "internal", "could not attach to session")
		return false
	}
	resource := control.Resource{Kind: control.ResourceSession, WorkspaceID: installWorkspace,
		ID: string(row.ID), CreatorID: row.CreatorID}
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
func (s *Server) waitRunning(ctx context.Context, id string) (control.Session, error) {
	deadline := time.Now().Add(s.cfg.AttachWait)
	for {
		row, err := s.st.Sessions().GetSession(ctx, installWorkspace, control.SessionID(id))
		if err != nil {
			return control.Session{}, err
		}
		switch {
		case row.State == control.StateRunning:
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
			return control.Session{}, ctx.Err()
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
func (s *Server) failedButAttachable(row control.Session) bool {
	return row.State == control.StateFailed && row.RunnerID != "" && s.runnerConnected(string(row.RunnerID))
}

// ---------------------------------------------------------------------------
// the attach plane's host
// ---------------------------------------------------------------------------

// attachHost is the self-hosted host of the attach plane (attachplane.Host):
// the fleet token as the dial-back's credential, this replica's runner
// connections as the way a dial_attach reaches a runner, and this replica's
// own ExternalURL as the dial-back address.
type attachHost struct{ srv *Server }

var _ attachplane.Host = attachHost{}

// IdentifyRunner authenticates a dial-back with the same fleet runner token
// handleRunnerConnect checks. The token is fleet-wide and the dial-back
// carries nothing else — runnerd dials the target_url it was handed, with a
// bearer and no name — so the runner is left unnamed here; the installation
// pool is the only pool there is.
func (h attachHost) IdentifyRunner(_ context.Context, r *http.Request) (control.PoolID, control.RunnerID, error) {
	if !h.srv.runnerTokenOK(r.Header.Get("Authorization")) {
		return "", "", control.ErrDenied
	}
	return installPool, "", nil
}

// Send queues the dial_attach on the runner's control connection. Self-hosted
// has one pool, so the pool is not consulted.
func (h attachHost) Send(_ control.PoolID, id control.RunnerID, m runner.ToRunner) error {
	return h.srv.sendToRunner(string(id), m)
}

// BackURL renders the dial-back URL for attachID: this replica's own
// ExternalURL with the scheme switched to ws(s). Naming the replica
// explicitly is what keeps the pairing correct once more than one controld
// runs (design §6) — the runner must come back to the replica holding the
// client socket, not to whichever one a load balancer picks.
func (h attachHost) BackURL(attachID string) string {
	base := h.srv.cfg.ExternalURL // New guarantees an absolute http(s) URL
	switch {
	case strings.HasPrefix(base, "https://"):
		base = "wss://" + strings.TrimPrefix(base, "https://")
	case strings.HasPrefix(base, "http://"):
		base = "ws://" + strings.TrimPrefix(base, "http://")
	}
	return base + "/v0/runners/attach-back?attach_id=" + attachID
}

// internal/controld/api.go
package controld

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"rainier/internal/rwire"
)

// sessionsBodyLimit caps every request body this file decodes: the create
// body and the optional suspend body are both small, fixed-shape JSON
// objects, so 64KB is generous while still bounding a client (or an
// adversary) from streaming an unbounded body at the decoder.
const sessionsBodyLimit = 64 << 10

// defaultListLimit and maxListLimit bound GET /v1/sessions's page size: a
// caller who names no limit gets defaultListLimit rows; one who asks for
// more than maxListLimit is silently capped rather than rejected (a big
// number is not malformed, just optimistic).
const (
	defaultListLimit = 50
	maxListLimit     = 100
)

// ---------------------------------------------------------------------------
// wire shapes
// ---------------------------------------------------------------------------

// sessionView is the client-facing rendering of a Session: every field the
// route table promises, RFC 3339 UTC timestamps, and nil Cmd/EgressAllow
// normalized to "[]" so the API never exposes memstore-vs-pgstore's
// nil-vs-empty-slice difference. No field is omitempty — the key set is
// meant to be identical on every session, which is what the response-shape
// regression tests pin.
type sessionView struct {
	ID          string   `json:"id"`
	OwnerID     string   `json:"owner_id"`
	Name        string   `json:"name"`
	Image       string   `json:"image"`
	Cmd         []string `json:"cmd"`
	EgressAllow []string `json:"egress_allow"`
	State       string   `json:"state"`
	Runner      string   `json:"runner"`
	Reachable   bool     `json:"reachable"`
	Error       string   `json:"error"`
	CreatedAt   string   `json:"created_at"`
	UpdatedAt   string   `json:"updated_at"`
	LastEventAt string   `json:"last_event_at"`
}

// sessionJSON renders s as its client-facing view. reachable is computed by
// the caller — it depends on live connection state (runnerConnected), which
// is not part of the row itself — per the rule: s.Runner != "" &&
// runnerConnected(s.Runner) && !s.State.Terminal().
func sessionJSON(s Session, reachable bool) sessionView {
	return sessionView{
		ID:          s.ID,
		OwnerID:     s.OwnerID,
		Name:        s.Name,
		Image:       s.Image,
		Cmd:         emptyIfNil(s.Cmd),
		EgressAllow: emptyIfNil(s.EgressAllow),
		State:       string(s.State),
		Runner:      s.Runner,
		Reachable:   reachable,
		Error:       s.Error,
		CreatedAt:   s.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:   s.UpdatedAt.UTC().Format(time.RFC3339),
		LastEventAt: s.LastEventAt.UTC().Format(time.RFC3339),
	}
}

// emptyIfNil returns ss, or a non-nil empty slice in its place, so
// json.Marshal always produces "[]" rather than the JSON scalar null.
func emptyIfNil(ss []string) []string {
	if ss == nil {
		return []string{}
	}
	return ss
}

// reachable computes sessionJSON's "reachable" flag for row.
func (s *Server) reachable(row Session) bool {
	return row.Runner != "" && s.runnerConnected(row.Runner) && !row.State.Terminal()
}

type sessionEnvelope struct {
	Session sessionView `json:"session"`
}

type sessionsEnvelope struct {
	Sessions   []sessionView `json:"sessions"`
	NextCursor string        `json:"next_cursor"`
}

type runnerSummary struct {
	Name          string `json:"name"`
	Connected     bool   `json:"connected"`
	CapacityUsed  int    `json:"capacity_used"`
	CapacityTotal int    `json:"capacity_total"`
	LastSeenAt    string `json:"last_seen_at"`
}

type runnersEnvelope struct {
	Runners []runnerSummary `json:"runners"`
}

type snapshotResponse struct {
	Ref string `json:"ref"`
}

// ---------------------------------------------------------------------------
// request bodies
// ---------------------------------------------------------------------------

type createSessionRequest struct {
	Name        string   `json:"name,omitempty"`
	Image       string   `json:"image,omitempty"`
	Cmd         []string `json:"cmd,omitempty"`
	EgressAllow []string `json:"egress_allow,omitempty"`
}

// suspendRequest's Warm is a pointer so an absent field is distinguishable
// from an explicit false — the default is true either way.
type suspendRequest struct {
	Warm *bool `json:"warm,omitempty"`
}

// decodeJSONBody decodes r's body (capped at sessionsBodyLimit) into v,
// rejecting unknown fields and a body holding more than one JSON value. An
// empty body decodes to v's zero value: every field in every body this API
// accepts is optional, so "no body at all" and "{}" are the same request.
// It writes a 400 invalid_request response and returns false on any
// failure; callers should return immediately when it does.
func decodeJSONBody(w http.ResponseWriter, r *http.Request, v any) bool {
	return decodeJSONBodyLimit(w, r, v, sessionsBodyLimit)
}

// decodeJSONBodyLimit is decodeJSONBody with an explicit byte cap, for the
// one body that isn't a small fixed-shape object: PUT /v1/secrets/{name}
// carries a value up to maxSecretValueBytes, which JSON escaping can inflate
// well past the sessions limit.
func decodeJSONBodyLimit(w http.ResponseWriter, r *http.Request, v any, limit int64) bool {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil && !errors.Is(err, io.EOF) {
		writeErr(w, http.StatusBadRequest, "invalid_request", "malformed request body")
		return false
	}
	if dec.More() {
		writeErr(w, http.StatusBadRequest, "invalid_request", "request body must contain a single JSON object")
		return false
	}
	return true
}

// authorizeOwnerOrAdmin reports whether u may mutate row: object-level
// authorization per design §4.4 — reads are team-wide, mutations are
// owner-or-admin.
func authorizeOwnerOrAdmin(u User, row Session) bool {
	return u.Role == "admin" || u.ID == row.OwnerID
}

// ---------------------------------------------------------------------------
// POST /v1/sessions
// ---------------------------------------------------------------------------

// handleCreateSession serves POST /v1/sessions. The row commits to the
// store before wakeScheduler and before the 202 is written — a client that
// gets a response, or a controld that crashes right after this call, always
// has a durable row to show for it (design §4.6, "create is write-ahead
// durable").
func (s *Server) handleCreateSession(w http.ResponseWriter, r *http.Request, u User) {
	var req createSessionRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}

	idemKey := r.Header.Get("Idempotency-Key")
	row := Session{
		ID:             NewSessionID(),
		OwnerID:        u.ID,
		Name:           req.Name,
		Image:          req.Image,
		Cmd:            req.Cmd,
		EgressAllow:    req.EgressAllow,
		State:          StateQueued,
		IdempotencyKey: idemKey,
	}

	created, err := s.st.CreateSession(r.Context(), row)
	switch {
	case errors.Is(err, ErrIdemReplay):
		existing, gErr := s.st.SessionByIdem(r.Context(), u.ID, idemKey)
		if gErr != nil {
			log.Printf("controld: idempotency replay lookup for owner %s: %v", u.ID, gErr)
			writeErr(w, http.StatusInternalServerError, "internal", "could not create session")
			return
		}
		s.writeSessionCreated(w, existing)
		return
	case errors.Is(err, ErrConflict):
		writeErr(w, http.StatusConflict, "conflict", "a non-terminal session with that name already exists")
		return
	case err != nil:
		log.Printf("controld: create session: %v", err)
		writeErr(w, http.StatusInternalServerError, "internal", "could not create session")
		return
	}

	// The row is durably committed at this point; only now may the
	// scheduler learn about it or the client learn it exists.
	s.wakeScheduler()
	s.writeSessionCreated(w, created)
}

func (s *Server) writeSessionCreated(w http.ResponseWriter, row Session) {
	w.Header().Set("Location", "/v1/sessions/"+row.ID)
	writeJSON(w, http.StatusAccepted, sessionEnvelope{Session: sessionJSON(row, s.reachable(row))})
}

// writeCurrentSession re-fetches id and writes it as a 200 session response
// (404 if it no longer exists). Used when a guarded Transition lost a race
// against a concurrent mutation after a runner op had already executed: the
// runner op is real, so the response is still success — it just has to
// report what the store actually holds now, not the state the handler
// merely hoped to reach. Never let a caller of this function then serialize
// its own guessed state instead.
func (s *Server) writeCurrentSession(w http.ResponseWriter, r *http.Request, id string) {
	row, err := s.st.GetSession(r.Context(), id)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeErr(w, http.StatusNotFound, "not_found", "session not found")
			return
		}
		log.Printf("controld: get session %s: %v", id, err)
		writeErr(w, http.StatusInternalServerError, "internal", "could not get session")
		return
	}
	writeJSON(w, http.StatusOK, sessionEnvelope{Session: sessionJSON(row, s.reachable(row))})
}

// ---------------------------------------------------------------------------
// GET /v1/sessions
// ---------------------------------------------------------------------------

// handleListSessions serves GET /v1/sessions: team-visible, cursor-paginated,
// terminal states hidden unless all=true.
func (s *Server) handleListSessions(w http.ResponseWriter, r *http.Request, u User) {
	q := r.URL.Query()

	limit := defaultListLimit
	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			writeErr(w, http.StatusBadRequest, "invalid_request", "limit must be a positive integer")
			return
		}
		limit = n
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}

	query := SessionQuery{
		Runner:          q.Get("runner"),
		IncludeTerminal: q.Get("all") == "true",
		Limit:           limit,
		Cursor:          q.Get("cursor"),
	}
	if st := q.Get("state"); st != "" {
		query.States = []SessionState{SessionState(st)}
	}

	rows, next, err := s.st.ListSessions(r.Context(), query)
	if err != nil {
		// The store's cursor decode error is unsentineled: any ListSessions
		// error while a (necessarily non-empty) cursor was supplied is
		// treated as an invalid cursor, per Task 2's ruling. A store error
		// with no cursor involved is a genuine internal failure.
		if query.Cursor != "" {
			writeErr(w, http.StatusBadRequest, "invalid_request", "invalid cursor")
			return
		}
		log.Printf("controld: list sessions: %v", err)
		writeErr(w, http.StatusInternalServerError, "internal", "could not list sessions")
		return
	}

	views := make([]sessionView, len(rows))
	for i, row := range rows {
		views[i] = sessionJSON(row, s.reachable(row))
	}
	writeJSON(w, http.StatusOK, sessionsEnvelope{Sessions: views, NextCursor: next})
}

// ---------------------------------------------------------------------------
// GET /v1/sessions/{id}
// ---------------------------------------------------------------------------

func (s *Server) handleGetSession(w http.ResponseWriter, r *http.Request, u User) {
	id := r.PathValue("id")
	row, err := s.st.GetSession(r.Context(), id)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeErr(w, http.StatusNotFound, "not_found", "session not found")
			return
		}
		log.Printf("controld: get session %s: %v", id, err)
		writeErr(w, http.StatusInternalServerError, "internal", "could not get session")
		return
	}
	writeJSON(w, http.StatusOK, sessionEnvelope{Session: sessionJSON(row, s.reachable(row))})
}

// ---------------------------------------------------------------------------
// DELETE /v1/sessions/{id}
// ---------------------------------------------------------------------------

// handleDeleteSession serves DELETE /v1/sessions/{id} per the route table:
// queued cancels outright; creating is rejected (409, nothing to destroy
// yet, dispatch may already be in flight); a placed session is destroyed on
// its runner if that runner is connected, or marked destroyed directly if
// not — reconcile's terminal-row-orphan rule cleans the container up later
// if the runner ever comes back (design §4.8); a terminal session is a
// no-op 204 (idempotent). Every success path wakes the scheduler: even the
// no-op paths are cheap to wake on, and it keeps the rule simple.
func (s *Server) handleDeleteSession(w http.ResponseWriter, r *http.Request, u User) {
	id := r.PathValue("id")
	row, err := s.st.GetSession(r.Context(), id)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeErr(w, http.StatusNotFound, "not_found", "session not found")
			return
		}
		log.Printf("controld: get session %s: %v", id, err)
		writeErr(w, http.StatusInternalServerError, "internal", "could not delete session")
		return
	}
	if !authorizeOwnerOrAdmin(u, row) {
		writeErr(w, http.StatusForbidden, "forbidden", "not authorized to modify this session")
		return
	}

	switch {
	case row.State.Terminal():
		s.wakeScheduler()
		w.WriteHeader(http.StatusNoContent)
		return
	case row.State == StateCreating:
		writeErr(w, http.StatusConflict, "conflict", "session is still creating")
		return
	case row.State == StateQueued:
		if err := s.st.Transition(r.Context(), id, []SessionState{StateQueued}, StateCanceled, TransitionOpts{}); err != nil {
			if !errors.Is(err, ErrConflict) && !errors.Is(err, ErrNotFound) {
				log.Printf("controld: cancel session %s: %v", id, err)
				writeErr(w, http.StatusInternalServerError, "internal", "could not delete session")
				return
			}
			// The row moved out of queued between our read and the guarded
			// update (a placement, or a concurrent delete). Either way the
			// caller's intent — this session should not exist — is already
			// being honored by whatever it became; report success.
		}
		s.wakeScheduler()
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// running, suspended_warm, or suspended_cold: destroy it on the runner
	// that holds it, if that runner is here to ask.
	if row.Runner != "" && s.runnerConnected(row.Runner) {
		res, err := s.dispatch(r.Context(), row.Runner, rwire.ToRunner{Type: "destroy", Session: id})
		switch {
		// ErrDispatchTimeout wraps ErrRunnerUnreachable, so it lands here
		// too — deliberately, here and at the three sibling ops
		// (suspend/resume/snapshot). Whether the runner never got the
		// command or got it and didn't answer in time, controld cannot
		// confirm the op and leaves the row untouched, so `runner_unreachable`
		// is the honest answer and the caller's retry runs against the same
		// state it just read.
		case errors.Is(err, ErrRunnerUnreachable):
			writeErr(w, http.StatusBadGateway, "runner_unreachable", "runner did not respond")
			return
		case err != nil:
			log.Printf("controld: destroy %s on %s: %v", id, row.Runner, err)
			writeErr(w, http.StatusInternalServerError, "internal", "could not delete session")
			return
		case !res.OK:
			log.Printf("controld: destroy %s on %s: runner reported failure: %s", id, row.Runner, res.Detail)
			writeErr(w, http.StatusInternalServerError, "internal", "could not delete session")
			return
		}
	}
	s.transitionQuiet(r.Context(), id, NonTerminal, StateDestroyed, TransitionOpts{})
	s.wakeScheduler()
	w.WriteHeader(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// POST /v1/sessions/{id}/suspend
// ---------------------------------------------------------------------------

func (s *Server) handleSuspendSession(w http.ResponseWriter, r *http.Request, u User) {
	id := r.PathValue("id")
	row, err := s.st.GetSession(r.Context(), id)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeErr(w, http.StatusNotFound, "not_found", "session not found")
			return
		}
		log.Printf("controld: get session %s: %v", id, err)
		writeErr(w, http.StatusInternalServerError, "internal", "could not suspend session")
		return
	}
	if !authorizeOwnerOrAdmin(u, row) {
		writeErr(w, http.StatusForbidden, "forbidden", "not authorized to modify this session")
		return
	}

	var req suspendRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	warm := true
	if req.Warm != nil {
		warm = *req.Warm
	}

	if row.State != StateRunning {
		writeErr(w, http.StatusConflict, "conflict", "session is not running")
		return
	}

	res, err := s.dispatch(r.Context(), row.Runner, rwire.ToRunner{Type: "suspend", Session: id, Warm: warm})
	switch {
	case errors.Is(err, ErrRunnerUnreachable):
		writeErr(w, http.StatusBadGateway, "runner_unreachable", "runner did not respond")
		return
	case err != nil:
		log.Printf("controld: suspend %s on %s: %v", id, row.Runner, err)
		writeErr(w, http.StatusInternalServerError, "internal", "could not suspend session")
		return
	case !res.OK:
		log.Printf("controld: suspend %s on %s: runner reported failure: %s", id, row.Runner, res.Detail)
		writeErr(w, http.StatusInternalServerError, "internal", "could not suspend session")
		return
	}

	to := StateSuspendedWarm
	if !warm {
		to = StateSuspendedCold
	}
	if err := s.st.Transition(r.Context(), id, []SessionState{StateRunning}, to, TransitionOpts{}); err != nil {
		if !errors.Is(err, ErrConflict) && !errors.Is(err, ErrNotFound) {
			log.Printf("controld: suspend %s: transition: %v", id, err)
			writeErr(w, http.StatusInternalServerError, "internal", "could not suspend session")
			return
		}
		// The runner op already executed, but the row moved out from under
		// us before we could record it (e.g. a concurrent DELETE won the
		// race). The response must tell the truth about persisted state,
		// not the state we hoped to reach — never fabricate row.State here.
		s.wakeScheduler()
		s.writeCurrentSession(w, r, id)
		return
	}
	s.wakeScheduler()

	row.State = to
	writeJSON(w, http.StatusOK, sessionEnvelope{Session: sessionJSON(row, s.reachable(row))})
}

// ---------------------------------------------------------------------------
// POST /v1/sessions/{id}/resume
// ---------------------------------------------------------------------------

func (s *Server) handleResumeSession(w http.ResponseWriter, r *http.Request, u User) {
	id := r.PathValue("id")
	row, err := s.st.GetSession(r.Context(), id)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeErr(w, http.StatusNotFound, "not_found", "session not found")
			return
		}
		log.Printf("controld: get session %s: %v", id, err)
		writeErr(w, http.StatusInternalServerError, "internal", "could not resume session")
		return
	}
	if !authorizeOwnerOrAdmin(u, row) {
		writeErr(w, http.StatusForbidden, "forbidden", "not authorized to modify this session")
		return
	}

	if row.State != StateSuspendedWarm && row.State != StateSuspendedCold {
		writeErr(w, http.StatusConflict, "conflict", "session is not suspended")
		return
	}

	// Cold resume pins to (and must fit on) the runner that already holds
	// the volume; warm sessions already occupy a slot there (OccupiesSlot),
	// so only cold needs a fresh capacity check before we commit to it.
	if row.State == StateSuspendedCold {
		views, err := s.freeCapacity(r.Context())
		if err != nil {
			log.Printf("controld: resume %s: computing free capacity: %v", id, err)
			writeErr(w, http.StatusInternalServerError, "internal", "could not resume session")
			return
		}
		free, connected := 0, false
		for _, v := range views {
			if v.Name == row.Runner {
				free, connected = v.Free, true
				break
			}
		}
		if !connected {
			writeErr(w, http.StatusBadGateway, "runner_unreachable", "runner is not connected")
			return
		}
		if free <= 0 {
			writeErr(w, http.StatusConflict, "no_capacity", fmt.Sprintf("runner %s has no free capacity", row.Runner))
			return
		}
	}

	res, err := s.dispatch(r.Context(), row.Runner, rwire.ToRunner{Type: "resume", Session: id})
	switch {
	case errors.Is(err, ErrRunnerUnreachable):
		writeErr(w, http.StatusBadGateway, "runner_unreachable", "runner did not respond")
		return
	case err != nil:
		log.Printf("controld: resume %s on %s: %v", id, row.Runner, err)
		writeErr(w, http.StatusInternalServerError, "internal", "could not resume session")
		return
	case !res.OK:
		log.Printf("controld: resume %s on %s: runner reported failure: %s", id, row.Runner, res.Detail)
		writeErr(w, http.StatusInternalServerError, "internal", "could not resume session")
		return
	}

	if err := s.st.Transition(r.Context(), id, []SessionState{row.State}, StateRunning, TransitionOpts{}); err != nil {
		if !errors.Is(err, ErrConflict) && !errors.Is(err, ErrNotFound) {
			log.Printf("controld: resume %s: transition: %v", id, err)
			writeErr(w, http.StatusInternalServerError, "internal", "could not resume session")
			return
		}
		// The runner op already executed, but the row moved out from under
		// us before we could record it. Report what the store actually
		// holds now, not the state we hoped to reach.
		s.wakeScheduler()
		s.writeCurrentSession(w, r, id)
		return
	}
	s.wakeScheduler()

	row.State = StateRunning
	writeJSON(w, http.StatusOK, sessionEnvelope{Session: sessionJSON(row, s.reachable(row))})
}

// ---------------------------------------------------------------------------
// POST /v1/sessions/{id}/snapshot
// ---------------------------------------------------------------------------

func (s *Server) handleSnapshotSession(w http.ResponseWriter, r *http.Request, u User) {
	id := r.PathValue("id")
	row, err := s.st.GetSession(r.Context(), id)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeErr(w, http.StatusNotFound, "not_found", "session not found")
			return
		}
		log.Printf("controld: get session %s: %v", id, err)
		writeErr(w, http.StatusInternalServerError, "internal", "could not snapshot session")
		return
	}
	if !authorizeOwnerOrAdmin(u, row) {
		writeErr(w, http.StatusForbidden, "forbidden", "not authorized to modify this session")
		return
	}

	switch row.State {
	case StateRunning, StateSuspendedWarm, StateSuspendedCold:
	default:
		writeErr(w, http.StatusConflict, "conflict", "session is not running or suspended")
		return
	}

	res, err := s.dispatch(r.Context(), row.Runner, rwire.ToRunner{Type: "snapshot", Session: id})
	switch {
	case errors.Is(err, ErrRunnerUnreachable):
		writeErr(w, http.StatusBadGateway, "runner_unreachable", "runner did not respond")
		return
	case err != nil:
		log.Printf("controld: snapshot %s on %s: %v", id, row.Runner, err)
		writeErr(w, http.StatusInternalServerError, "internal", "could not snapshot session")
		return
	case !res.OK:
		log.Printf("controld: snapshot %s on %s: runner reported failure: %s", id, row.Runner, res.Detail)
		writeErr(w, http.StatusInternalServerError, "internal", "could not snapshot session")
		return
	}

	writeJSON(w, http.StatusOK, snapshotResponse{Ref: res.Detail})
}

// ---------------------------------------------------------------------------
// GET /v1/runners
// ---------------------------------------------------------------------------

func (s *Server) handleListRunners(w http.ResponseWriter, r *http.Request, u User) {
	rows, err := s.st.ListRunners(r.Context())
	if err != nil {
		log.Printf("controld: list runners: %v", err)
		writeErr(w, http.StatusInternalServerError, "internal", "could not list runners")
		return
	}
	out := make([]runnerSummary, len(rows))
	for i, row := range rows {
		out[i] = runnerSummary{
			Name:          row.Name,
			Connected:     row.Connected,
			CapacityUsed:  row.CapacityUsed,
			CapacityTotal: row.CapacityTotal,
			LastSeenAt:    row.LastSeenAt.UTC().Format(time.RFC3339),
		}
	}
	writeJSON(w, http.StatusOK, runnersEnvelope{Runners: out})
}

// ---------------------------------------------------------------------------
// secrets: PUT/GET/DELETE /v1/secrets
// ---------------------------------------------------------------------------

const (
	// maxSecretValueBytes caps one secret's plaintext at 64KB. Secrets are
	// environment variables injected into a session (design §4.1) — API
	// tokens, keys, the occasional PEM — not file storage.
	maxSecretValueBytes = 64 << 10
	// secretsBodyLimit caps the whole PUT body. It is deliberately far above
	// maxSecretValueBytes: JSON escaping can inflate a value up to six-fold
	// (every byte of a control character becomes \u00xx), and a request that
	// carries a legal 64KB value must fail the value check with a message
	// about the value, not get cut off mid-body and reported as malformed.
	secretsBodyLimit = 8 * maxSecretValueBytes
)

// secretNamePattern is the whole vocabulary of a secret name: the shell
// environment-variable spelling, since that is exactly what a secret becomes
// inside a session. Anything outside it is rejected at the API rather than
// producing a variable no setup script can reference.
var secretNamePattern = regexp.MustCompile(`^[A-Z0-9_]{1,64}$`)

// secretView is the client-facing rendering of a SecretMeta. There is
// deliberately no value field anywhere in this file's response types: a
// secret's value is write-only at this API, and the only way to keep that
// true under future edits is for the wire type to have nowhere to put one.
type secretView struct {
	Name      string `json:"name"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

type secretsEnvelope struct {
	Secrets []secretView `json:"secrets"`
}

// putSecretRequest is the decoded body of PUT /v1/secrets/{name}. Value is
// never logged and never echoed — not in a success response, not in an
// error.
type putSecretRequest struct {
	Value string `json:"value"`
}

// handlePutSecret serves PUT /v1/secrets/{name} (admin): seal the value
// under the fleet's secrets key and upsert it. The response is a bare 204 —
// there is nothing to return that the caller doesn't already have, and a
// body echoing the name is one refactor away from echoing the value.
func (s *Server) handlePutSecret(w http.ResponseWriter, r *http.Request, u User) {
	name := r.PathValue("name")
	if !secretNamePattern.MatchString(name) {
		writeErr(w, http.StatusBadRequest, "invalid_request", "secret name must match [A-Z0-9_]{1,64}")
		return
	}

	var req putSecretRequest
	if !decodeJSONBodyLimit(w, r, &req, secretsBodyLimit) {
		return
	}
	if req.Value == "" {
		writeErr(w, http.StatusBadRequest, "invalid_request", "value is required")
		return
	}
	if len(req.Value) > maxSecretValueBytes {
		writeErr(w, http.StatusBadRequest, "invalid_request",
			fmt.Sprintf("value must be at most %d bytes", maxSecretValueBytes))
		return
	}

	ciphertext, nonce, err := Seal(s.cfg.SecretsKey, []byte(req.Value))
	if err != nil {
		// Seal only fails if the OS entropy source is broken; the detail is
		// for the log, and carries nothing about the value either way.
		log.Printf("controld: sealing secret %s: %v", name, err)
		writeErr(w, http.StatusInternalServerError, "internal", "could not store secret")
		return
	}
	if err := s.st.PutSecret(r.Context(), name, ciphertext, nonce); err != nil {
		log.Printf("controld: put secret %s: %v", name, err)
		writeErr(w, http.StatusInternalServerError, "internal", "could not store secret")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleListSecrets serves GET /v1/secrets: every secret's name and
// timestamps, name ascending. Team-visible like every other read on this
// API — knowing which names exist is what lets a member write an
// environment that references them; the values stay unreadable.
func (s *Server) handleListSecrets(w http.ResponseWriter, r *http.Request, u User) {
	rows, err := s.st.ListSecrets(r.Context())
	if err != nil {
		log.Printf("controld: list secrets: %v", err)
		writeErr(w, http.StatusInternalServerError, "internal", "could not list secrets")
		return
	}
	out := make([]secretView, len(rows))
	for i, row := range rows {
		out[i] = secretView{
			Name:      row.Name,
			CreatedAt: row.CreatedAt.UTC().Format(time.RFC3339),
			UpdatedAt: row.UpdatedAt.UTC().Format(time.RFC3339),
		}
	}
	writeJSON(w, http.StatusOK, secretsEnvelope{Secrets: out})
}

// handleDeleteSecret serves DELETE /v1/secrets/{name} (admin). Unlike
// DELETE /v1/sessions/{id}, this one is not idempotent: an unknown name is
// 404, because deleting the wrong secret and deleting a nonexistent one look
// identical to the caller otherwise, and there is no "already gone" state to
// report the way a terminal session has.
func (s *Server) handleDeleteSecret(w http.ResponseWriter, r *http.Request, u User) {
	name := r.PathValue("name")
	if !secretNamePattern.MatchString(name) {
		writeErr(w, http.StatusBadRequest, "invalid_request", "secret name must match [A-Z0-9_]{1,64}")
		return
	}
	if err := s.st.DeleteSecret(r.Context(), name); err != nil {
		if errors.Is(err, ErrNotFound) {
			writeErr(w, http.StatusNotFound, "not_found", "secret not found")
			return
		}
		log.Printf("controld: delete secret %s: %v", name, err)
		writeErr(w, http.StatusInternalServerError, "internal", "could not delete secret")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// GET /healthz
// ---------------------------------------------------------------------------

// handleHealthz serves GET /healthz: the one other unauthenticated route
// besides POST /v1/auth/github. Plain "ok", no internals, ever.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, "ok")
}

// ---------------------------------------------------------------------------
// middleware: request id, nosniff, no-store on GET
// ---------------------------------------------------------------------------

// maxRequestIDLen bounds the X-Request-Id this server will echo back
// unchanged; a longer one is replaced rather than trusted verbatim into
// logs.
const maxRequestIDLen = 128

// withMiddleware wraps the whole mux in the chain every route shares:
// X-Request-Id (accepted from the client if present and short enough,
// generated otherwise, always echoed and attached to the outcome log line),
// X-Content-Type-Options: nosniff on every response, and Cache-Control:
// no-store on every GET — this API's data is private and changes
// continuously, so nothing here is safe for an intermediary to cache.
func withMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := r.Header.Get("X-Request-Id")
		if reqID == "" || len(reqID) > maxRequestIDLen {
			reqID = randHex(8)
		}
		w.Header().Set("X-Request-Id", reqID)
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if r.Method == http.MethodGet {
			w.Header().Set("Cache-Control", "no-store")
		}

		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(sw, r)
		log.Printf("controld: %s %s -> %d (%s) request_id=%s", r.Method, r.URL.Path, sw.status, time.Since(start), reqID)
	})
}

// statusWriter records the status code a handler wrote, for the outcome log
// line. It forwards Unwrap so a handler that hijacks the connection
// (websocket.Accept, via coder/websocket's Unwrap-following hijacker lookup)
// still reaches the real ResponseWriter underneath rather than failing to
// find http.Hijacker on this wrapper.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

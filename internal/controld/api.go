// internal/controld/api.go
package controld

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/controlapp"
	"github.com/tokencanopy/rainier/protocol/workspace"
)

// sessionsBodyLimit caps every request body this file decodes: the create
// body and the optional suspend body are both small, fixed-shape JSON
// objects, so 64KB is generous while still bounding a client (or an
// adversary) from streaming an unbounded body at the decoder.
const sessionsBodyLimit = 64 << 10

// defaultListLimit and maxListLimit bound GET /v0/sessions's page size: a
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
//
// Image is the image the session ACTUALLY runs: for a session started from an
// environment that is the resolved one (the environment's image, or its
// cached snapshot), not the empty override the client sent. Environment and
// QueueReason are derived, never stored — see sessionDerived.
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
	Environment string   `json:"environment"`
	QueueReason string   `json:"queue_reason"`
	// ChildExitCode is the exit status of the session's agent process, once
	// it has one, and null until then. A POINTER, and rendered as null rather
	// than omitted, because exit 0 is an ANSWER: a session whose agent
	// finished cleanly has to be distinguishable from one still working, and
	// a plain int would make those two the same 0. Present on every session
	// like every other field here — a key that appears only sometimes cannot
	// be told apart from an older controld that never had it.
	ChildExitCode *int   `json:"child_exit_code"`
	CreatedAt     string `json:"created_at"`
	UpdatedAt     string `json:"updated_at"`
	LastEventAt   string `json:"last_event_at"`
}

// sessionDerived carries the three view fields that cannot be read off the
// session row: each depends on live connection state or on another table, and
// none is stored. Reachable follows the rule s.Runner != "" &&
// runnerConnected(s.Runner) && !s.State.Terminal(); Environment is the
// environment's NAME ("" for a scratch session, or one whose environment has
// since been deleted); QueueReason explains a queued session that is waiting
// on a specific runner. sessionRenderer computes all three.
type sessionDerived struct {
	Reachable   bool
	Environment string
	QueueReason string
}

// sessionJSON renders s as its client-facing view, with d supplying the
// fields the row itself cannot answer for.
func sessionJSON(s Session, d sessionDerived) sessionView {
	return sessionView{
		ID:          s.ID,
		OwnerID:     s.OwnerID,
		Name:        s.Name,
		Image:       s.effectiveImage(),
		Cmd:         emptyIfNil(s.Cmd),
		EgressAllow: emptyIfNil(s.EgressAllow),
		State:       string(s.State),
		Runner:      s.Runner,
		Reachable:   d.Reachable,
		Error:       s.Error,
		Environment: d.Environment,
		QueueReason: d.QueueReason,
		// Copied, never aliased: the row's pointer may be into a store's own
		// map (memstore hands out clones for exactly this reason, but a view
		// that relied on that would be one refactor away from letting a
		// response mutate the store).
		ChildExitCode: copyIntPtr(s.ChildExitCode),
		CreatedAt:     s.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:     s.UpdatedAt.UTC().Format(time.RFC3339),
		LastEventAt:   s.LastEventAt.UTC().Format(time.RFC3339),
	}
}

// ---------------------------------------------------------------------------
// rendering a session view
// ---------------------------------------------------------------------------

// sessionRenderer answers the derived half of a session view for every
// session rendered by ONE request. Both answers live outside the session row
// — the environments table, and the live fleet's free capacity — so a page of
// N sessions would otherwise repeat the same handful of lookups N times. The
// renderer memoizes them for the life of one request and no longer: a cached
// environment or capacity reading that outlived its request would start
// describing a fleet that has since moved on.
type sessionRenderer struct {
	srv   *Server
	ctx   context.Context
	scope control.Scope

	// envs maps environment id to the environment the service returned; a nil
	// value records "already looked up, and there is nothing there".
	envs map[string]*control.Environment
	// free maps a connected runner's name to its free slots, computed at most
	// once per request and only if some queued session's pin asks for it.
	free     map[string]int
	freeDone bool
}

// renderer returns a fresh per-request renderer. The scope it reads
// environments under is the one the request already authenticated — the
// handler put the user in ctx with withUser, so the renderer needs no second
// argument to stay inside the caller's own authorization.
func (s *Server) renderer(ctx context.Context) *sessionRenderer {
	r := &sessionRenderer{srv: s, ctx: ctx, envs: map[string]*control.Environment{}}
	if u, ok := userFromContext(ctx); ok {
		r.scope = userScope(u)
	}
	return r
}

// view renders one session.
func (r *sessionRenderer) view(row control.Session) sessionView {
	d := sessionDerived{Reachable: r.srv.reachable(row)}
	if env := r.environment(string(row.EnvironmentID)); env != nil {
		d.Environment = env.Name
		d.QueueReason = r.queueReason(row, *env)
	}
	return sessionJSON(sessionFromControl(row), d)
}

// environment returns the environment for id, or nil for a scratch session,
// an environment that has since been deleted, or a service that could not
// answer. None of those is worth failing a read over: the environment name is
// a convenience on a session view, and a session outlives its environment
// perfectly well — it carries its own resolved image.
func (r *sessionRenderer) environment(id string) *control.Environment {
	if id == "" {
		return nil
	}
	if env, seen := r.envs[id]; seen {
		return env
	}
	var env *control.Environment
	row, err := r.srv.environments.GetEnvironment(r.ctx, r.scope, control.EnvironmentID(id))
	switch {
	case errors.Is(err, control.ErrNotFound):
	case err != nil:
		log.Printf("controld: rendering a session view: get environment %s: %v", id, err)
	default:
		env = &row
	}
	r.envs[id] = env
	return env
}

// queueReason explains a queued session its environment's placement pin is
// holding back: the pinned runner is not connected to this replica, or has no
// free slot, and until one of those changes the scheduler will keep passing
// this session over (design §4.6). Everything else — a session that is not
// queued, an unpinned environment, a pin that could be honored right now —
// has no reason to give, and says nothing rather than guessing.
//
// The pin is read off the environment's portable requirements: control names
// no runner, so an operator's `placement` is carried as the capability
// "placement:<runner>" (adapt_store.go).
func (r *sessionRenderer) queueReason(row control.Session, env control.Environment) string {
	pin := capabilityValue(env.Requirements.Capabilities, placementCapabilityPrefix)
	if row.State != control.StateQueued || pin == "" || r.runnerHasRoom(pin) {
		return ""
	}
	return "waiting for runner " + pin
}

// runnerHasRoom reports whether name is connected to this replica AND has a
// free slot right now — the same two conditions placement itself applies.
//
// This is the one store read the renderer makes for itself. It is view logic
// and decides nothing: the free-capacity question belongs to the scheduler,
// and asking it through a service would mean a placement pass per rendered
// page.
func (r *sessionRenderer) runnerHasRoom(name string) bool {
	if !r.freeDone {
		r.freeDone = true
		// Without the fleet's capacity the honest answer is "no room known" —
		// which renders the waiting-for-runner line. That is the safer way to
		// be wrong: it explains a session that is in fact about to be placed,
		// rather than silently explaining nothing about one that is genuinely
		// stuck.
		r.free = r.freeSlots()
	}
	return r.free[name] > 0 && r.srv.transport.Connected(installPool, control.RunnerID(name))
}

// freeSlots is free capacity per connected runner: its reported total less
// its reported use less the sessions it is currently creating, whose slots
// the runner has not counted yet. A store that cannot answer any part of it
// yields an empty map rather than a partial one — half a capacity picture is
// not a smaller truth, it is a different fleet.
func (r *sessionRenderer) freeSlots() map[string]int {
	rows, err := r.srv.st.ListRunners(r.ctx)
	if err != nil {
		log.Printf("controld: rendering a session view: listing runners: %v", err)
		return map[string]int{}
	}
	free := make(map[string]int, len(rows))
	for _, row := range rows {
		if !row.Connected {
			continue
		}
		creating, err := r.srv.st.SessionsOnRunner(r.ctx, row.Name, []SessionState{StateCreating})
		if err != nil {
			log.Printf("controld: rendering a session view: sessions creating on a runner: %v", err)
			return map[string]int{}
		}
		free[row.Name] = row.CapacityTotal - row.CapacityUsed - len(creating)
	}
	return free
}

// emptyIfNil returns ss, or a non-nil empty slice in its place, so
// json.Marshal always produces "[]" rather than the JSON scalar null.
func emptyIfNil(ss []string) []string {
	if ss == nil {
		return []string{}
	}
	return ss
}

// copyIntPtr clones a nullable int so a rendered view never shares storage
// with the row it came from. nil stays nil — which is the wire's null, and
// the honest answer for a session whose agent has not exited.
func copyIntPtr(p *int) *int {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

// reachable computes sessionJSON's "reachable" flag for row: a session held
// by a runner that has a control connection to this replica right now, and is
// still live enough for that connection to mean anything.
func (s *Server) reachable(row control.Session) bool {
	return row.RunnerID != "" && s.transport.Connected(installPool, row.RunnerID) && !row.State.Terminal()
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

// createSessionRequest is POST /v0/sessions's body. Environment names the
// environment this session starts from, by name or by id; omitting it is a
// scratch session, exactly as before environments existed. Image and
// EgressAllow are overrides when it is present — see resolveEnvironment.
type createSessionRequest struct {
	Name        string   `json:"name,omitempty"`
	Image       string   `json:"image,omitempty"`
	Cmd         []string `json:"cmd,omitempty"`
	EgressAllow []string `json:"egress_allow,omitempty"`
	Environment string   `json:"environment,omitempty"`
	// Repos overrides the repositories the environment's github connectors
	// declare. Absent inherits them; an explicit empty array clones nothing,
	// exactly the nil-vs-empty distinction egress_allow already draws — and
	// for the same reason: "I didn't say" and "I said none" are different
	// answers, and a session that means to be scratch under a repo-carrying
	// environment has no other way to say so.
	Repos []repoRequest `json:"repos,omitempty"`
}

// repoRequest is one entry of that array. BaseBranch is a pointer for the
// same reason the github connector's is: an explicitly empty base_branch is a
// typo, never a request for the default, and it must not reach the clone as
// one.
type repoRequest struct {
	Repo       string  `json:"repo"`
	BaseBranch *string `json:"base_branch"`
}

// repoOverrides validates a create body's `repos` and returns the refs to
// record on the session row. It preserves nil-vs-empty: nil in, nil out
// (inherit the environment's connectors); empty in, empty out (clone
// nothing).
//
// The errors are the caller's to read — each names the offending entry by
// index and what was wrong with it — and none carries internal detail.
func repoOverrides(reqs []repoRequest) ([]RepoRef, error) {
	if reqs == nil {
		return nil, nil
	}
	out := make([]RepoRef, 0, len(reqs))
	for i, req := range reqs {
		if !validRepoRef(req.Repo) {
			return nil, fmt.Errorf("repos[%d].repo must be \"owner/name\", got %q", i, req.Repo)
		}
		ref := RepoRef{Repo: req.Repo}
		if req.BaseBranch != nil {
			if *req.BaseBranch == "" {
				return nil, fmt.Errorf("repos[%d].base_branch is empty; omit it for the default (%s)", i, defaultBaseBranch)
			}
			ref.BaseBranch = *req.BaseBranch
		}
		out = append(out, ref)
	}
	return out, nil
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
// one body that isn't a small fixed-shape object: PUT /v0/secrets/{name}
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
// the session service's errors on the wire
// ---------------------------------------------------------------------------

// sessionErrText is the pair of sentences a session handler owns, because the
// service reports both situations as one sentinel and cannot know which
// operation was being asked for.
//
// Conflict replaces the generic "conflict" where today's handler had a
// specific refusal. Refused is the "could not <verb> session" a runner that
// answered NO gets — the operation genuinely failed here, not at an
// unreachable dependency, so it is 500 rather than 502.
type sessionErrText struct {
	Conflict string
	Refused  string
}

// writeSessionErr answers a session-service error with controlStatus's fixed
// mapping (adapt_http.go), with the three refinements every session handler
// shares.
//
// The order matters. A runner that received the command and reported failure
// (controlapp.ErrRunnerRefused) is checked FIRST: it wraps ErrUnavailable, so
// the connectivity refinement below would otherwise call it unreachable —
// which is exactly backwards, since the runner is right here and answering.
// Everything else that is ErrUnavailable for a placed session IS about the
// runner, and unavailableStatus says which way (no connection, or no answer)
// off a re-read of the row. A re-read that fails leaves the fixed 500, the
// honest answer when we cannot even say whose runner it was.
func (s *Server) writeSessionErr(w http.ResponseWriter, ctx context.Context, id string, err error, text sessionErrText) {
	status, code, msg := controlStatus(err)
	if status == 0 {
		return // the caller went away; there is nobody to answer
	}
	switch {
	case errors.Is(err, control.ErrConflict) && text.Conflict != "":
		msg = text.Conflict
	case errors.Is(err, controlapp.ErrRunnerRefused):
		// The detail the runner gave is not ours to relay and never reached
		// us; the log line that has it lives in the runner plane.
		status, code, msg = http.StatusInternalServerError, "internal", text.Refused
	case errors.Is(err, control.ErrUnavailable) && id != "":
		u, ok := userFromContext(ctx)
		if !ok {
			break
		}
		if row, gErr := s.sessions.GetSession(ctx, userScope(u), control.SessionID(id)); gErr == nil {
			status, code, msg = s.unavailableStatus(row)
		}
	}
	writeErr(w, status, code, msg)
}

// ---------------------------------------------------------------------------
// POST /v0/sessions
// ---------------------------------------------------------------------------

// handleCreateSession serves POST /v0/sessions: decode, run the three host
// policy preflights the service cannot make for itself, and hand the create
// to the session service, which owns the row, the pool selection and the
// wake.
//
// The preflights run BEFORE the create, deliberately: every way they can fail
// (an environment nobody has heard of, a secret it references that has since
// been deleted, a clone with no credential to clone with) must leave no
// session behind at all, so the caller fixes one thing and retries rather
// than cleaning up a half-built row first. Each is host policy the control
// contract deliberately does not carry — a name index, a vault, and a
// per-user GitHub credential.
func (s *Server) handleCreateSession(w http.ResponseWriter, r *http.Request, u User) {
	var req createSessionRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}

	repos, err := repoOverrides(req.Repos)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	// A create names an environment OR carries a scratch spec, never both
	// (control.CreateSession.Validate). The spec is all three fields, not just
	// the image: an environment session runs the environment's image, command
	// and egress allowlist, so a caller who sent any of them beside
	// `environment` asked for something that cannot happen. Refusing says so;
	// accepting the create and dropping the field would hand back a session
	// running an allowlist the caller did not ask for and cannot see they
	// didn't get.
	if req.Environment != "" && (req.Image != "" || req.Cmd != nil || req.EgressAllow != nil) {
		writeErr(w, http.StatusBadRequest, "invalid_request",
			"an environment session cannot override the image, command, or egress allowlist")
		return
	}
	// The other half of the same rule: a create that names neither an
	// environment nor anything to run describes no session at all. The
	// service refuses it too; saying which field is missing is the handler's
	// job, because the service can only report that the command was invalid.
	if req.Environment == "" && req.Image == "" && req.Cmd == nil && req.EgressAllow == nil {
		writeErr(w, http.StatusBadRequest, "invalid_request", "a session must name an environment or an image")
		return
	}

	ctx := withUser(r.Context(), u)
	scope := userScope(u)

	var env *control.Environment
	if req.Environment != "" {
		resolved, ok := s.createEnvironment(w, ctx, scope, req.Environment)
		if !ok {
			return
		}
		env = &resolved
	}
	if !s.createPreflight(w, ctx, u, repos, env) {
		return
	}

	cmd := control.CreateSession{
		Name:           req.Name,
		Repos:          reposToControl(repos),
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
	}
	if env != nil {
		cmd.EnvironmentID = env.ID
	} else {
		cmd.Spec = control.PortableSpec{
			Image:       req.Image,
			Cmd:         req.Cmd,
			EgressAllow: req.EgressAllow,
		}
	}
	created, err := s.sessions.CreateSession(ctx, scope, cmd)
	if err != nil {
		s.writeSessionErr(w, ctx, "", err, sessionErrText{
			Conflict: "a non-terminal session with that name already exists",
			Refused:  "could not create session",
		})
		return
	}

	w.Header().Set("Location", "/v0/sessions/"+string(created.ID))
	writeJSON(w, http.StatusAccepted, sessionEnvelope{Session: s.renderer(ctx).view(created)})
}

// createEnvironment resolves a create body's `environment` to the environment
// the session starts from. A reference nothing answers to is the caller's
// mistake, not a missing resource: it is 400 naming the reference, exactly as
// before, because the thing that was not found is a field of the request.
func (s *Server) createEnvironment(w http.ResponseWriter, ctx context.Context, scope control.Scope, ref string) (control.Environment, bool) {
	env, err := s.environmentRef(ctx, scope, ref)
	switch {
	case errors.Is(err, control.ErrNotFound):
		writeErr(w, http.StatusBadRequest, "invalid_request",
			fmt.Sprintf("environment %q does not exist", ref))
		return control.Environment{}, false
	case err != nil:
		log.Printf("controld: create session: get environment %q: %v", ref, err)
		writeControlErr(w, err)
		return control.Environment{}, false
	}
	return env, true
}

// createPreflight runs the two host-policy gates a create has to pass and the
// service cannot: the environment's secret references must all still resolve
// in the vault, and a session that will clone must have a GitHub credential
// to clone with.
//
// Neither answer carries anything sensitive. The secret refusal names the
// reference (a name, never a value); the credential refusal names the command
// that fixes it and never reads the credential row it checked for.
//
// ANY stored credential passes, valid or needs_refresh. The two differ in
// whether they can become usable without the user creating this session again:
// a stale credential is refreshable mid-flight — `rainier login --refresh
// github` while the session sits there, and the clone that follows works — so
// the clone, not the create, is the right place for it to say so. A
// credential that is not there at all never becomes present that way.
func (s *Server) createPreflight(w http.ResponseWriter, ctx context.Context, u User, repos []RepoRef, env *control.Environment) bool {
	var storeEnv *Environment
	if env != nil {
		row := environmentFromControl(*env)
		storeEnv = &row
		_, missing, err := s.secretEnv(ctx, row)
		switch {
		case err != nil:
			log.Printf("controld: create session: resolving secrets of environment %s: %v", row.ID, err)
			writeErr(w, http.StatusInternalServerError, "internal", "could not create session")
			return false
		case missing != "":
			writeErr(w, http.StatusConflict, "conflict", missingSecretMessage(row, missing))
			return false
		}
	}

	refs, err := sessionRepoRefs(Session{Repos: repos}, storeEnv)
	if err != nil {
		log.Printf("controld: create session: resolving the repositories of environment %s: %v", envID(storeEnv), err)
		writeErr(w, http.StatusInternalServerError, "internal", "could not create session")
		return false
	}
	if len(refs) == 0 {
		return true
	}
	// The row itself is never read — only its existence is — so no credential
	// material is loaded, let alone rendered into the response.
	if _, err := s.st.GetCredential(ctx, u.ID, githubProvider); err != nil {
		if errors.Is(err, ErrNotFound) {
			writeErr(w, http.StatusConflict, "conflict", ErrCredentialMissing.Error())
			return false
		}
		log.Printf("controld: create session: reading the github credential of user %s: %v", u.ID, err)
		writeErr(w, http.StatusInternalServerError, "internal", "could not create session")
		return false
	}
	return true
}

// secretEnv decrypts every secret env references into the environment map its
// sessions' containers get. A reference no stored secret answers to comes back
// as its name rather than as an error — a dangling reference is the caller's
// to report (409 at create, a failed session at dispatch), not a store
// failure. No return value and no error text here ever carries a secret VALUE.
// The three results are, in order: the variables, the name of the first
// reference that had no secret behind it, and a genuine failure.
func (s *Server) secretEnv(ctx context.Context, env Environment) (map[string]string, string, error) {
	if len(env.SecretRefs) == 0 {
		return nil, "", nil
	}
	vars := make(map[string]string, len(env.SecretRefs))
	for _, name := range env.SecretRefs {
		ciphertext, nonce, err := s.st.GetSecret(ctx, name)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil, name, nil
			}
			return nil, "", fmt.Errorf("get secret %s: %w", name, err)
		}
		plaintext, err := Open(s.cfg.SecretsKey, ciphertext, nonce)
		if err != nil {
			return nil, "", fmt.Errorf("open secret %s: %w", name, err)
		}
		vars[name] = string(plaintext)
	}
	return vars, "", nil
}

// missingSecretMessage is the one client-facing sentence for a dangling
// secret_ref, shared by the create's 409 and the dispatch's failure text so
// an operator meets the same words wherever the reference breaks.
func missingSecretMessage(env Environment, name string) string {
	return fmt.Sprintf("environment %q references secret %q, which no longer exists", env.Name, name)
}

// ---------------------------------------------------------------------------
// GET /v0/sessions
// ---------------------------------------------------------------------------

// handleListSessions serves GET /v0/sessions: team-visible, cursor-paginated,
// terminal states hidden unless all=true. name is an optional exact filter;
// the CLI uses it to resolve one resource without paging through team history.
//
// control.SessionQuery carries only the three things pagination needs, so
// state/name/runner are applied to the page the service returns rather than
// pushed into the store's query. They are exact-match conveniences for the
// CLI, not a query language, and the cursor stays the service's.
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

	cursor := q.Get("cursor")
	ctx := withUser(r.Context(), u)
	page, err := s.sessions.ListSessions(ctx, userScope(u), control.SessionQuery{
		IncludeTerminal: q.Get("all") == "true",
		Limit:           limit,
		Cursor:          cursor,
	})
	if err != nil {
		// The only per-request failure this query has is a cursor nothing can
		// decode, and that is the caller's mistake rather than a service that
		// cannot answer. The store's decode failure is unsentineled and the
		// service collapses a repository sentinel into ErrUnavailable, so any
		// failure while a (necessarily non-empty) cursor was supplied is
		// treated as an invalid cursor — the ruling this handler has always
		// applied.
		if cursor != "" && (errors.Is(err, control.ErrInvalid) || errors.Is(err, control.ErrUnavailable)) {
			writeErr(w, http.StatusBadRequest, "invalid_request", "invalid cursor")
			return
		}
		writeControlErr(w, err)
		return
	}

	// One renderer for the whole page: the environment name and queue reason
	// each row needs come from outside the session table, and this is what
	// keeps a page of N sessions from repeating those lookups N times.
	rend := s.renderer(ctx)
	views := make([]sessionView, 0, len(page.Sessions))
	for _, row := range page.Sessions {
		if !matchesSessionFilters(row, q) {
			continue
		}
		views = append(views, rend.view(row))
	}
	writeJSON(w, http.StatusOK, sessionsEnvelope{Sessions: views, NextCursor: page.NextCursor})
}

// matchesSessionFilters applies the three exact-match query filters to one
// row of a returned page. An absent filter matches everything.
func matchesSessionFilters(row control.Session, q url.Values) bool {
	if state := q.Get("state"); state != "" && string(row.State) != state {
		return false
	}
	if name := q.Get("name"); name != "" && row.Name != name {
		return false
	}
	if runnerName := q.Get("runner"); runnerName != "" && string(row.RunnerID) != runnerName {
		return false
	}
	return true
}

// ---------------------------------------------------------------------------
// GET /v0/sessions/{id}
// ---------------------------------------------------------------------------

func (s *Server) handleGetSession(w http.ResponseWriter, r *http.Request, u User) {
	id := r.PathValue("id")
	ctx := withUser(r.Context(), u)
	row, err := s.sessions.GetSession(ctx, userScope(u), control.SessionID(id))
	if err != nil {
		s.writeSessionErr(w, ctx, id, err, sessionErrText{Refused: "could not get session"})
		return
	}
	writeJSON(w, http.StatusOK, sessionEnvelope{Session: s.renderer(ctx).view(row)})
}

// ---------------------------------------------------------------------------
// DELETE /v0/sessions/{id}
// ---------------------------------------------------------------------------

// handleDeleteSession serves DELETE /v0/sessions/{id}. The state machine is
// the session service's: queued cancels outright; creating is rejected (409,
// nothing to destroy yet, dispatch may already be in flight); a placed
// session is destroyed on its runner when that runner is connected, and
// destroyed in the store either way — reconcile's terminal-row-orphan rule
// cleans the container up later if the runner ever comes back (design §4.8).
// Most terminal sessions are a no-op 204 (idempotent), but failed is
// deliberately different: setup/clone failures retain a live container for
// attach, so rm must destroy it. Every path that can reach a runner also
// reclaims the session's workspace volume, which is the half of the
// durability rider that keeps "a crash preserves the workspace" from reading,
// on a host's disk, as "every crash leaks one".
func (s *Server) handleDeleteSession(w http.ResponseWriter, r *http.Request, u User) {
	id := r.PathValue("id")
	ctx := withUser(r.Context(), u)
	if err := s.sessions.DeleteSession(ctx, userScope(u), control.DeleteSession{ID: control.SessionID(id)}); err != nil {
		s.writeSessionErr(w, ctx, id, err, sessionErrText{
			Conflict: "session is still creating",
			Refused:  "could not delete session",
		})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// POST /v0/sessions/{id}/suspend
// ---------------------------------------------------------------------------

func (s *Server) handleSuspendSession(w http.ResponseWriter, r *http.Request, u User) {
	id := r.PathValue("id")
	var req suspendRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	// An absent `warm` is indistinguishable from an explicit true; the
	// default is true either way.
	warm := req.Warm == nil || *req.Warm

	ctx := withUser(r.Context(), u)
	row, err := s.sessions.SuspendSession(ctx, userScope(u), control.SuspendSession{
		ID: control.SessionID(id), Warm: warm,
	})
	if err != nil {
		s.writeSessionErr(w, ctx, id, err, sessionErrText{
			Conflict: "session is not running",
			Refused:  "could not suspend session",
		})
		return
	}
	writeJSON(w, http.StatusOK, sessionEnvelope{Session: s.renderer(ctx).view(row)})
}

// ---------------------------------------------------------------------------
// POST /v0/sessions/{id}/resume
// ---------------------------------------------------------------------------

// handleResumeSession serves POST /v0/sessions/{id}/resume. Both ways a
// resume can be refused — the session is not suspended, or a cold one no
// longer fits on the runner holding its volume — reach this handler as
// ErrConflict, so both get one sentence. The capacity answer is the service's
// and recomputing it here to recover a second slug would be a second
// placement decision in the one place that must not make any.
func (s *Server) handleResumeSession(w http.ResponseWriter, r *http.Request, u User) {
	id := r.PathValue("id")
	ctx := withUser(r.Context(), u)
	row, err := s.sessions.ResumeSession(ctx, userScope(u), control.ResumeSession{ID: control.SessionID(id)})
	if err != nil {
		s.writeSessionErr(w, ctx, id, err, sessionErrText{
			Conflict: "session cannot be resumed right now",
			Refused:  "could not resume session",
		})
		return
	}
	writeJSON(w, http.StatusOK, sessionEnvelope{Session: s.renderer(ctx).view(row)})
}

// ---------------------------------------------------------------------------
// POST /v0/sessions/{id}/snapshot
// ---------------------------------------------------------------------------

func (s *Server) handleSnapshotSession(w http.ResponseWriter, r *http.Request, u User) {
	id := r.PathValue("id")
	ctx := withUser(r.Context(), u)
	ck, err := s.sessions.SnapshotSession(ctx, userScope(u), control.SnapshotSession{ID: control.SessionID(id)})
	if err != nil {
		s.writeSessionErr(w, ctx, id, err, sessionErrText{
			Conflict: "session is not running or suspended",
			Refused:  "could not snapshot session",
		})
		return
	}
	writeJSON(w, http.StatusOK, snapshotResponse{Ref: ck.Ref})
}

// ---------------------------------------------------------------------------
// GET /v0/runners
// ---------------------------------------------------------------------------

func (s *Server) handleListRunners(w http.ResponseWriter, r *http.Request, u User) {
	ctx := withUser(r.Context(), u)
	page, err := s.fleet.ListRunners(ctx, userScope(u), control.RunnerQuery{Limit: maxListLimit})
	if err != nil {
		writeControlErr(w, err)
		return
	}
	out := make([]runnerSummary, len(page.Runners))
	for i, row := range page.Runners {
		out[i] = runnerSummary{
			Name:          string(row.ID),
			Connected:     row.Connected,
			CapacityUsed:  row.CapacityUsed,
			CapacityTotal: row.CapacityTotal,
			LastSeenAt:    row.LastSeenAt.UTC().Format(time.RFC3339),
		}
	}
	writeJSON(w, http.StatusOK, runnersEnvelope{Runners: out})
}

// ---------------------------------------------------------------------------
// secrets: PUT/GET/DELETE /v0/secrets
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

// putSecretRequest is the decoded body of PUT /v0/secrets/{name}. Value is
// never logged and never echoed — not in a success response, not in an
// error.
type putSecretRequest struct {
	Value string `json:"value"`
}

// handlePutSecret serves PUT /v0/secrets/{name} (admin): seal the value
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

// handleListSecrets serves GET /v0/secrets: every secret's name and
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

// handleDeleteSecret serves DELETE /v0/secrets/{name} (admin). Unlike
// DELETE /v0/sessions/{id}, this one is not idempotent: an unknown name is
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
// credentials: GET /v0/credentials
// ---------------------------------------------------------------------------

// credentialView is the client-facing rendering of a Credential: what the
// vault holds ABOUT a credential, never the credential. Like secretView, it
// has nowhere to put a value on purpose — Credential itself carries four
// sealed byte slices, and the only durable way to keep them off the wire is
// for the wire type to have no field they could be assigned to.
type credentialView struct {
	Provider       string `json:"provider"`
	Status         string `json:"status"`
	Scopes         string `json:"scopes"`
	ObtainedAt     string `json:"obtained_at"`
	LastVerifiedAt string `json:"last_verified_at"`
	LastUsedAt     string `json:"last_used_at"`
}

type credentialsEnvelope struct {
	Credentials []credentialView `json:"credentials"`
}

// handleListCredentials serves GET /v0/credentials: the CALLER's own
// credentials, provider ascending.
//
// This is the one listing on this API that is not team-visible, and the
// asymmetry is deliberate. A team secret is the team's, and knowing its name
// is what lets a member wire it into an environment; a credential is one
// person's GitHub identity, and no teammate — admin included — has any use
// for its status. So the store call is scoped to u.ID and there is no
// "somebody else's" query parameter to add later without noticing.
func (s *Server) handleListCredentials(w http.ResponseWriter, r *http.Request, u User) {
	rows, err := s.st.ListCredentials(r.Context(), u.ID)
	if err != nil {
		log.Printf("controld: list credentials for user %s: %v", u.ID, err)
		writeErr(w, http.StatusInternalServerError, "internal", "could not list credentials")
		return
	}
	out := make([]credentialView, len(rows))
	for i, row := range rows {
		out[i] = credentialView{
			Provider:       row.Provider,
			Status:         row.Status,
			Scopes:         row.Scopes,
			ObtainedAt:     row.ObtainedAt.UTC().Format(time.RFC3339),
			LastVerifiedAt: row.LastVerifiedAt.UTC().Format(time.RFC3339),
			LastUsedAt:     row.LastUsedAt.UTC().Format(time.RFC3339),
		}
	}
	writeJSON(w, http.StatusOK, credentialsEnvelope{Credentials: out})
}

// ---------------------------------------------------------------------------
// environments: POST/GET /v0/environments, GET/PATCH/DELETE /v0/environments/{id}
// ---------------------------------------------------------------------------

// environmentsBodyLimit caps every environment request body. It is larger
// than sessionsBodyLimit for one field: `setup` is a shell script, and JSON
// escaping inflates one (every newline becomes two bytes, every quote two).
// A script that would not fit here belongs in the image, not in an
// environment row.
const environmentsBodyLimit = 256 << 10

// defaultSetupTimeoutSec bounds an environment's setup script when the
// environment names no timeout of its own: fifteen minutes, which covers a
// language toolchain and a dependency install on a cold container, and is
// short enough that a hung script gives its slot back inside the working hour.
const defaultSetupTimeoutSec = 900

// defaultInitTimeoutSec bounds an environment's init hook when it names no
// timeout of its own. Same fifteen minutes as the setup default and for the
// same reason, even though init is usually far shorter: it runs on EVERY boot,
// so a hook that hangs holds a slot the operator is waiting on, and the bound
// is what turns that into a failed stage with a tail rather than a session
// that never finishes starting.
const defaultInitTimeoutSec = 900

// environmentIDPrefix is what NewEnvironmentID puts in front of every
// environment id, and therefore what tells an id from a name on the
// {id}-shaped routes: names can never contain "_" (envNamePattern), so no
// name can be mistaken for an id.
const environmentIDPrefix = "env_"

// envNamePattern is the whole vocabulary of an environment name: it is a CLI
// handle (`rainier new --env dev`) and half of a snapshot ref, so it stays in
// the lowercase-kebab alphabet that is safe in both a shell word and an OCI
// tag.
var envNamePattern = regexp.MustCompile(`^[a-z0-9-]{1,64}$`)

// environmentView is the client-facing rendering of an Environment. Like
// sessionView, no field is omitempty: the key set is identical on every
// environment, including the three snapshot fields, which are present and
// empty until a session build caches one (Task 7's resolution compares
// snapshot_hash against setup_hash, so a client can see staleness too).
type environmentView struct {
	ID              string            `json:"id"`
	Name            string            `json:"name"`
	Image           string            `json:"image"`
	Setup           string            `json:"setup"`
	SetupHash       string            `json:"setup_hash"`
	Init            string            `json:"init"`
	InitTimeoutSec  int               `json:"init_timeout_sec"`
	EgressAllow     []string          `json:"egress_allow"`
	SecretRefs      []string          `json:"secret_refs"`
	Connectors      []json.RawMessage `json:"connectors"`
	Placement       string            `json:"placement"`
	SetupTimeoutSec int               `json:"setup_timeout_sec"`
	SnapshotRef     string            `json:"snapshot_ref"`
	SnapshotRunner  string            `json:"snapshot_runner"`
	SnapshotHash    string            `json:"snapshot_hash"`
	CreatedAt       string            `json:"created_at"`
	UpdatedAt       string            `json:"updated_at"`
}

type environmentEnvelope struct {
	Environment environmentView `json:"environment"`
}

type environmentsEnvelope struct {
	Environments []environmentView `json:"environments"`
}

// environmentJSON renders e as its client-facing view.
//
// Two of the wire's fields have no field of their own on control.Environment,
// which names no runner: `placement` is carried as the portable capability
// "placement:<runner>" (adapt_store.go) and read back out of it here, and
// `snapshot_runner` — which the control model does not carry at all — is
// passed in by the handler, which read it off the store row for the view.
func environmentJSON(e control.Environment, snapshotRunner string) environmentView {
	return environmentView{
		ID:              string(e.ID),
		Name:            e.Name,
		Image:           e.Image,
		Setup:           e.Setup,
		SetupHash:       e.SetupHash,
		Init:            e.Init,
		InitTimeoutSec:  e.InitTimeoutSec,
		EgressAllow:     emptyIfNil(e.EgressAllow),
		SecretRefs:      emptyIfNil(e.SecretRefs),
		Connectors:      connectorsJSON(connectorsFromControl(e.Connectors)),
		Placement:       capabilityValue(e.Requirements.Capabilities, placementCapabilityPrefix),
		SetupTimeoutSec: e.SetupTimeoutSec,
		SnapshotRef:     e.Snapshot.Ref,
		SnapshotRunner:  snapshotRunner,
		SnapshotHash:    e.SnapshotHash,
		CreatedAt:       e.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:       e.UpdatedAt.UTC().Format(time.RFC3339),
	}
}

// connectorsJSON renders cs as the JSON array a client sent: the stored bytes
// of each connector, handed back without re-rendering. (What survives the
// round trip is the JSON VALUE — memstore keeps the client's exact bytes,
// while Postgres's jsonb preserves the value but may re-render whitespace and
// member order; storetest's sameJSON is where that contract lives.) Never
// nil, so the array renders as "[]" rather than null.
func connectorsJSON(cs []Connector) []json.RawMessage {
	out := make([]json.RawMessage, 0, len(cs))
	for _, c := range cs {
		raw := c.Raw
		if len(raw) == 0 {
			// Unreachable through this API — validateConnectors always keeps
			// the caller's original bytes — but a Connector with no Raw would
			// encode as invalid JSON and truncate the whole response, so a
			// row from anywhere else degrades to the one field we still know.
			raw = json.RawMessage(`{"type":` + strconv.Quote(c.Type) + `}`)
		}
		out = append(out, raw)
	}
	return out
}

// ---------------------------------------------------------------------------
// connector vocabulary
//
// A connector is a declared attachment an environment's sessions get. In
// Plan 4 the vocabulary is VALIDATED AND STORED ONLY — nothing here connects
// anything: github clones arrive in Plan 5, files/tunnel in Plan 6, browser
// in Plans 6-7 (design §4.2). Validating the shape now is what lets those
// plans land without a migration, and rejecting unknown types now is what
// keeps an old server from silently ignoring a connector a client relied on.
// ---------------------------------------------------------------------------

// repoPattern is the "owner/name" spelling of a GitHub repository — the same
// two-segment shape `gh repo clone` accepts, and nothing else. It is the SHAPE
// check; validRepoRef below is the whole rule.
var repoPattern = regexp.MustCompile(`^[\w.-]+/[\w.-]+$`)

// validRepoRef reports whether s names a repository this API will accept.
//
// The shape above is not sufficient on its own, because the name does not stay
// a name: Plan 5's scheduler splits it (sched.go's expandRepos) and puts the
// second segment straight into a session's repo Dir, which sessiond joins to
// /workspace un-cleaned (cmd/sessiond/gitchain.go's repoDir) and later hands to
// `git -C`. Validating that here is the point of a boundary — the alternative
// is relying on git's own accidents downstream, and an accident is not a rule.
func validRepoRef(s string) bool {
	if !repoPattern.MatchString(s) {
		return false
	}
	owner, name, _ := strings.Cut(s, "/")
	return validRepoSegment(owner) && validRepoSegment(name)
}

// validRepoSegment refuses the two segments that are not names at all.
//
//   - "." and "..": path elements. `/workspace/..` is `/`, and today the only
//     thing standing between that and a clone outside the workspace is git
//     refusing a non-empty destination — its accident, not this boundary's
//     rule. GitHub does not allow either as a repository name anyway.
//   - a leading "-": an option wherever this string later sits in an argv, and
//     neither a GitHub login nor a repository name starts with one.
//
// A leading "." is deliberately still ALLOWED: `.github` is a real and common
// repository name, and refusing it would reject a legitimate connector to close
// nothing (a dotted directory under /workspace is still under /workspace).
func validRepoSegment(s string) bool {
	return s != "." && s != ".." && !strings.HasPrefix(s, "-")
}

// githubConnector is the github connector's v0 shape. BaseBranch is a pointer
// so an absent base_branch (which means defaultBaseBranch) is distinguishable
// from an explicit empty one — an empty branch name is a typo, never a
// request for the default, and it must not reach Plan 5's clone as one.
type githubConnector struct {
	Type       string  `json:"type"`
	Repo       string  `json:"repo"`
	BaseBranch *string `json:"base_branch"`
}

type filesConnector struct {
	Type  string   `json:"type"`
	Paths []string `json:"paths"`
}

type tunnelConnector struct {
	Type       string `json:"type"`
	Name       string `json:"name"`
	TargetHost string `json:"target_host"`
	TargetPort int    `json:"target_port"`
}

type browserConnector struct {
	Type string `json:"type"`
	Tier string `json:"tier"`
}

// validateConnectors decodes and validates raw — the "connectors" member of
// an environment body — into the rows the store persists. An absent or empty
// array is no connectors at all.
//
// Every returned Connector carries the element's ORIGINAL bytes in Raw: the
// stores render an empty Raw differently, so keeping Raw always-populated
// here is what keeps that difference out of reachable space, and it is what
// lets a client read back exactly the object it wrote.
//
// Errors are written for the caller: each names the offending element by
// index and says what was wrong with it, and none carries internal detail.
func validateConnectors(raw json.RawMessage) ([]Connector, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var elems []json.RawMessage
	if err := json.Unmarshal(raw, &elems); err != nil {
		return nil, errors.New("connectors must be an array of objects")
	}
	if len(elems) == 0 {
		return nil, nil
	}

	out := make([]Connector, 0, len(elems))
	for i, elem := range elems {
		// Loose decode first, for the discriminator alone: the strict decode
		// below can't run until we know which shape to check against.
		var head struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(elem, &head); err != nil {
			return nil, fmt.Errorf("connectors[%d] must be an object", i)
		}
		if head.Type == "" {
			return nil, fmt.Errorf("connectors[%d] is missing type", i)
		}
		if err := validateConnector(head.Type, elem); err != nil {
			return nil, fmt.Errorf("connectors[%d]: %w", i, err)
		}
		out = append(out, Connector{Type: head.Type, Raw: elem})
	}
	return out, nil
}

// validateConnector strictly decodes one connector element against the shape
// its already-decoded connType names, and checks its fields. Unknown types
// are rejected by name (fail closed, design §4.2).
func validateConnector(connType string, elem json.RawMessage) error {
	switch connType {
	case "github":
		_, err := decodeGitHubConnector(elem)
		return err

	case "files":
		var c filesConnector
		if err := strictDecode(elem, &c); err != nil {
			return err
		}
		if len(c.Paths) == 0 {
			return errors.New("files connector needs at least one entry in paths")
		}
		for _, p := range c.Paths {
			if p == "" {
				return errors.New("files connector has an empty string in paths")
			}
		}
		return nil

	case "tunnel":
		var c tunnelConnector
		if err := strictDecode(elem, &c); err != nil {
			return err
		}
		if c.Name == "" {
			return errors.New("tunnel connector needs a name")
		}
		if c.TargetHost == "" {
			return errors.New("tunnel connector needs a target_host")
		}
		if c.TargetPort < 1 || c.TargetPort > 65535 {
			return fmt.Errorf("tunnel connector target_port %d is outside 1..65535", c.TargetPort)
		}
		return nil

	case "browser":
		var c browserConnector
		if err := strictDecode(elem, &c); err != nil {
			return err
		}
		if c.Tier != "dedicated" && c.Tier != "extension" {
			return fmt.Errorf("browser connector tier must be dedicated or extension, got %q", c.Tier)
		}
		return nil

	default:
		return fmt.Errorf("unknown connector type %q", connType)
	}
}

// decodeGitHubConnector strictly decodes elem as a github connector. The
// returned BaseBranch is never nil: an absent base_branch is filled in with
// defaultBaseBranch here, in the decode Plan 5's clone path repeats against
// the stored bytes — the default lives here, not in the stored row, so an
// environment keeps exactly the object its author wrote.
func decodeGitHubConnector(elem json.RawMessage) (githubConnector, error) {
	var c githubConnector
	if err := strictDecode(elem, &c); err != nil {
		return githubConnector{}, err
	}
	if !validRepoRef(c.Repo) {
		return githubConnector{}, fmt.Errorf("github connector repo must be \"owner/name\", got %q", c.Repo)
	}
	if c.BaseBranch == nil {
		def := defaultBaseBranch
		c.BaseBranch = &def
	} else if *c.BaseBranch == "" {
		return githubConnector{}, errors.New("github connector base_branch is empty; omit it for the default (" + defaultBaseBranch + ")")
	}
	return c, nil
}

// strictDecode decodes elem into v rejecting unknown fields — the per-type
// half of connector validation, and the reason a typo'd key is a 400 instead
// of a silently dropped setting.
func strictDecode(elem json.RawMessage, v any) error {
	dec := json.NewDecoder(bytes.NewReader(elem))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

// ---------------------------------------------------------------------------
// environment request bodies
// ---------------------------------------------------------------------------

type createEnvironmentRequest struct {
	Name            string          `json:"name,omitempty"`
	Image           string          `json:"image,omitempty"`
	Setup           string          `json:"setup,omitempty"`
	Init            string          `json:"init,omitempty"`
	InitTimeoutSec  int             `json:"init_timeout_sec,omitempty"`
	EgressAllow     []string        `json:"egress_allow,omitempty"`
	SecretRefs      []string        `json:"secret_refs,omitempty"`
	Connectors      json.RawMessage `json:"connectors,omitempty"`
	Placement       string          `json:"placement,omitempty"`
	SetupTimeoutSec int             `json:"setup_timeout_sec,omitempty"`
}

// patchEnvironmentRequest is PATCH's body: every field is a pointer (or, for
// connectors, a nil-able raw message) so "absent" is distinguishable from
// "set to the zero value" — clearing a list and leaving it alone are
// different requests.
type patchEnvironmentRequest struct {
	Name            *string         `json:"name,omitempty"`
	Image           *string         `json:"image,omitempty"`
	Setup           *string         `json:"setup,omitempty"`
	Init            *string         `json:"init,omitempty"`
	InitTimeoutSec  *int            `json:"init_timeout_sec,omitempty"`
	EgressAllow     *[]string       `json:"egress_allow,omitempty"`
	SecretRefs      *[]string       `json:"secret_refs,omitempty"`
	Connectors      json.RawMessage `json:"connectors,omitempty"`
	Placement       *string         `json:"placement,omitempty"`
	SetupTimeoutSec *int            `json:"setup_timeout_sec,omitempty"`
}

// validateEnvironmentBasics checks the four scalar rules create and patch
// share, returning a client-facing message (or "" when the row is fine).
// Placement is deliberately unchecked: an environment may be pinned to a
// runner that hasn't joined the fleet yet, which is exactly how the hardware
// case is set up (design §4.6). Neither script is checked either — a shell
// script is only wrong once it runs.
func validateEnvironmentBasics(name, image string, setupTimeoutSec, initTimeoutSec int) string {
	switch {
	case !envNamePattern.MatchString(name):
		return "name must match [a-z0-9-]{1,64}"
	case image == "":
		return "image is required"
	case setupTimeoutSec < 0:
		return "setup_timeout_sec must not be negative"
	case initTimeoutSec < 0:
		return "init_timeout_sec must not be negative"
	}
	return ""
}

// missingSecretRef returns the first name in refs that no stored secret
// answers to, or "" when they all exist. It reads the secret LISTING —
// names and timestamps — rather than fetching values: an existence check has
// no business touching ciphertext.
func (s *Server) missingSecretRef(ctx context.Context, refs []string) (string, error) {
	if len(refs) == 0 {
		return "", nil
	}
	rows, err := s.st.ListSecrets(ctx)
	if err != nil {
		return "", err
	}
	have := make(map[string]bool, len(rows))
	for _, row := range rows {
		have[row.Name] = true
	}
	for _, name := range refs {
		if !have[name] {
			return name, nil
		}
	}
	return "", nil
}

// environmentRef resolves ref — an environment id, or otherwise a name — to
// the environment the service returns, the same disambiguation the CLI does
// for session refs.
//
// The by-name half is the one lookup the control contract has no query for:
// EnvironmentRepository is keyed by id, so the name index is read straight
// from the store and the row it finds is then fetched through the service,
// which is what authorizes the read.
func (s *Server) environmentRef(ctx context.Context, scope control.Scope, ref string) (control.Environment, error) {
	if !strings.HasPrefix(ref, environmentIDPrefix) {
		row, err := s.st.GetEnvironmentByName(ctx, ref)
		if err != nil {
			return control.Environment{}, storeErr(err)
		}
		return s.environments.GetEnvironment(ctx, scope, control.EnvironmentID(row.ID))
	}
	return s.environments.GetEnvironment(ctx, scope, control.EnvironmentID(ref))
}

// snapshotRunnerOf is the environment view's one store read: the name of the
// runner holding an environment's cached snapshot. control.Environment names
// no runner — a snapshot's affinity to its builder is carried as a capability
// and only while the snapshot is current — but the wire has always shown the
// column, stale or not, so the view reads the column. It decides nothing; an
// unreadable row simply shows no runner.
func (s *Server) snapshotRunnerOf(ctx context.Context, id control.EnvironmentID) string {
	row, err := s.st.GetEnvironment(ctx, string(id))
	if err != nil {
		if !errors.Is(err, ErrNotFound) {
			log.Printf("controld: rendering environment %s: reading its snapshot runner: %v", id, err)
		}
		return ""
	}
	return row.SnapshotRunner
}

// placementRequirements is the portable spelling of an operator's runner pin:
// control.Environment names no runner, so `placement` round-trips through the
// capability "placement:<runner>" (adapt_store.go). An empty placement is no
// capability at all rather than an empty one.
func placementRequirements(placement string) control.Requirements {
	if placement == "" {
		return control.Requirements{}
	}
	return control.Requirements{Capabilities: []string{placementCapabilityPrefix + placement}}
}

// handleCreateEnvironment serves POST /v0/environments (admin): validate the
// whole body, then commit. Nothing is stored until every check has passed —
// a rejected create leaves no half-built environment behind. The two checks
// the service cannot make for itself are the request's own vocabulary (the
// name pattern, the connector shapes) and the vault's answer about
// secret_refs.
func (s *Server) handleCreateEnvironment(w http.ResponseWriter, r *http.Request, u User) {
	var req createEnvironmentRequest
	if !decodeJSONBodyLimit(w, r, &req, environmentsBodyLimit) {
		return
	}
	if bad := validateEnvironmentBasics(req.Name, req.Image, req.SetupTimeoutSec, req.InitTimeoutSec); bad != "" {
		writeErr(w, http.StatusBadRequest, "invalid_request", bad)
		return
	}
	conns, err := validateConnectors(req.Connectors)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	ctx := withUser(r.Context(), u)
	if !s.secretRefsExist(w, ctx, req.SecretRefs, "could not create environment") {
		return
	}

	created, err := s.environments.CreateEnvironment(ctx, userScope(u), control.CreateEnvironment{
		Name:            req.Name,
		Image:           req.Image,
		Setup:           req.Setup,
		Init:            req.Init,
		InitTimeoutSec:  req.InitTimeoutSec,
		EgressAllow:     req.EgressAllow,
		SecretRefs:      req.SecretRefs,
		Connectors:      connectorsToControl(conns),
		Requirements:    placementRequirements(req.Placement),
		SetupTimeoutSec: req.SetupTimeoutSec,
	})
	if err != nil {
		if errors.Is(err, control.ErrConflict) {
			writeErr(w, http.StatusConflict, "conflict",
				fmt.Sprintf("an environment named %q already exists", req.Name))
			return
		}
		log.Printf("controld: create environment %q: %v", req.Name, err)
		writeControlErr(w, err)
		return
	}

	// A brand-new environment has no cached snapshot, so no runner holds one.
	w.Header().Set("Location", "/v0/environments/"+string(created.ID))
	writeJSON(w, http.StatusCreated, environmentEnvelope{Environment: environmentJSON(created, "")})
}

// secretRefsExist answers the vault question create and patch share: every
// name in refs must still be a stored secret. It writes the client's response
// and reports false when it is not, so a caller never stores a reference that
// would fail its first session create.
func (s *Server) secretRefsExist(w http.ResponseWriter, ctx context.Context, refs []string, failMsg string) bool {
	missing, err := s.missingSecretRef(ctx, refs)
	if err != nil {
		log.Printf("controld: listing secrets to check secret_refs: %v", err)
		writeErr(w, http.StatusInternalServerError, "internal", failMsg)
		return false
	}
	if missing != "" {
		writeErr(w, http.StatusBadRequest, "invalid_request", fmt.Sprintf("secret_ref %q does not exist", missing))
		return false
	}
	return true
}

// handleListEnvironments serves GET /v0/environments: every environment, name
// ascending. Team-visible like every other read on this API — a member has to
// see the environments to start a session from one. There are few
// environments per deployment, so this page is the whole table (no cursor).
func (s *Server) handleListEnvironments(w http.ResponseWriter, r *http.Request, u User) {
	ctx := withUser(r.Context(), u)
	page, err := s.environments.ListEnvironments(ctx, userScope(u), control.EnvironmentQuery{})
	if err != nil {
		log.Printf("controld: list environments: %v", err)
		writeControlErr(w, err)
		return
	}
	// One store read for the whole page rather than one per row: the snapshot
	// runner is a view-only column (see snapshotRunnerOf) and a listing that
	// asked for it row by row would read the table twice over.
	runners := map[string]string{}
	if rows, err := s.st.ListEnvironments(ctx); err != nil {
		log.Printf("controld: list environments: reading their snapshot runners: %v", err)
	} else {
		for _, row := range rows {
			runners[row.ID] = row.SnapshotRunner
		}
	}
	out := make([]environmentView, len(page.Environments))
	for i, row := range page.Environments {
		out[i] = environmentJSON(row, runners[string(row.ID)])
	}
	writeJSON(w, http.StatusOK, environmentsEnvelope{Environments: out})
}

// handleGetEnvironment serves GET /v0/environments/{id}, by id or by name.
func (s *Server) handleGetEnvironment(w http.ResponseWriter, r *http.Request, u User) {
	ref := r.PathValue("id")
	ctx := withUser(r.Context(), u)
	row, err := s.environmentRef(ctx, userScope(u), ref)
	if err != nil {
		writeEnvironmentLookupErr(w, ref, err)
		return
	}
	writeJSON(w, http.StatusOK, environmentEnvelope{
		Environment: environmentJSON(row, s.snapshotRunnerOf(ctx, row.ID)),
	})
}

// writeEnvironmentLookupErr turns an environmentRef failure into its
// response through the one sentinel mapping, logging anything that is not a
// plain miss (the detail is never sent).
func writeEnvironmentLookupErr(w http.ResponseWriter, ref string, err error) {
	if !errors.Is(err, control.ErrNotFound) {
		log.Printf("controld: get environment %q: %v", ref, err)
	}
	writeControlErr(w, err)
}

// handleUpdateEnvironment serves PATCH /v0/environments/{id} (admin): a
// partial update of the create fields, applied to the row as it stands. The
// service owns setup_hash (recomputed from the merged image+setup) and the
// store owns the three snapshot columns — an edit that moves the hash
// deliberately leaves the old snapshot in place and visibly stale, for the
// next build to notice and rebuild.
func (s *Server) handleUpdateEnvironment(w http.ResponseWriter, r *http.Request, u User) {
	ref := r.PathValue("id")
	ctx := withUser(r.Context(), u)
	scope := userScope(u)
	cur, err := s.environmentRef(ctx, scope, ref)
	if err != nil {
		writeEnvironmentLookupErr(w, ref, err)
		return
	}

	var req patchEnvironmentRequest
	if !decodeJSONBodyLimit(w, r, &req, environmentsBodyLimit) {
		return
	}

	// The four scalars are validated against the MERGED row — a patch that
	// only sets `image` still has to leave a legal name behind — and the
	// specific sentence is the handler's, because the service reports every
	// bad field as one ErrInvalid.
	name, image := cur.Name, cur.Image
	setupTimeout, initTimeout := cur.SetupTimeoutSec, cur.InitTimeoutSec
	if req.Name != nil {
		name = *req.Name
	}
	if req.Image != nil {
		image = *req.Image
	}
	if req.SetupTimeoutSec != nil {
		setupTimeout = *req.SetupTimeoutSec
	}
	if req.InitTimeoutSec != nil {
		initTimeout = *req.InitTimeoutSec
	}
	if bad := validateEnvironmentBasics(name, image, setupTimeout, initTimeout); bad != "" {
		writeErr(w, http.StatusBadRequest, "invalid_request", bad)
		return
	}

	cmd := control.UpdateEnvironment{
		ID:              cur.ID,
		Name:            req.Name,
		Image:           req.Image,
		Setup:           req.Setup,
		Init:            req.Init,
		InitTimeoutSec:  req.InitTimeoutSec,
		EgressAllow:     req.EgressAllow,
		SetupTimeoutSec: req.SetupTimeoutSec,
	}
	if req.Connectors != nil {
		conns, err := validateConnectors(req.Connectors)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		converted := connectorsToControl(conns)
		cmd.Connectors = &converted
	}
	// Only the refs this request supplies are checked for existence: a patch
	// answers for what it sets, and refs that were valid when they were set
	// but whose secret has since been deleted are the session create's to
	// refuse (design §4.5 — it fails loudly there, naming the secret).
	if req.SecretRefs != nil {
		if !s.secretRefsExist(w, ctx, *req.SecretRefs, "could not update environment") {
			return
		}
		cmd.SecretRefs = req.SecretRefs
	}
	// The pin is set only when the patch names it; leaving Requirements nil is
	// what keeps an untouched placement (and the snapshot affinity beside it)
	// exactly as the store has it.
	if req.Placement != nil {
		reqs := placementRequirements(*req.Placement)
		cmd.Requirements = &reqs
	}

	updated, err := s.environments.UpdateEnvironment(ctx, scope, cmd)
	if err != nil {
		if errors.Is(err, control.ErrConflict) {
			writeErr(w, http.StatusConflict, "conflict",
				fmt.Sprintf("an environment named %q already exists", name))
			return
		}
		if !errors.Is(err, control.ErrNotFound) && !errors.Is(err, control.ErrInvalid) {
			log.Printf("controld: update environment %s: %v", cur.ID, err)
		}
		writeControlErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, environmentEnvelope{
		Environment: environmentJSON(updated, s.snapshotRunnerOf(ctx, updated.ID)),
	})
}

// handleDeleteEnvironment serves DELETE /v0/environments/{id} (admin). An
// environment that live sessions still came from is not deleted: those
// sessions pin their own resolved_image and would survive, but the
// environment is how an operator reasons about them, so removing it out from
// under them is refused with the count (design §5).
func (s *Server) handleDeleteEnvironment(w http.ResponseWriter, r *http.Request, u User) {
	ref := r.PathValue("id")
	ctx := withUser(r.Context(), u)
	scope := userScope(u)
	row, err := s.environmentRef(ctx, scope, ref)
	if err != nil {
		writeEnvironmentLookupErr(w, ref, err)
		return
	}

	if err := s.environments.DeleteEnvironment(ctx, scope, control.DeleteEnvironment{ID: row.ID}); err != nil {
		if errors.Is(err, control.ErrConflict) {
			writeErr(w, http.StatusConflict, "conflict", s.stillInUseMessage(ctx, row))
			return
		}
		if !errors.Is(err, control.ErrNotFound) {
			log.Printf("controld: delete environment %s: %v", row.ID, err)
		}
		writeControlErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// stillInUseMessage is the refusal an operator reads when an environment is
// still in use. The service has already made the decision; this counts the
// sessions only to say how many, and falls back to the countless sentence
// when the store cannot answer. The count is keyed by the RESOLVED id, never
// by the caller's ref: a scratch session carries environment_id "", so
// counting against anything but a real id would sweep in sessions that belong
// to no environment at all.
func (s *Server) stillInUseMessage(ctx context.Context, env control.Environment) string {
	n, err := s.st.CountSessionsByEnvironment(ctx, string(env.ID), NonTerminal)
	if err != nil || n <= 0 {
		return fmt.Sprintf("environment %q still has non-terminal session(s)", env.Name)
	}
	return fmt.Sprintf("environment %q still has %d non-terminal session(s)", env.Name, n)
}

// ---------------------------------------------------------------------------
// workspace inspection:
//   GET  /v0/sessions/{id}/diff
//   POST /v0/sessions/{id}/files        one chunk of an upload
//   GET  /v0/sessions/{id}/files?path=  the whole download, streamed
//
// All three are the same shape: check the session is reachable (and, for the
// two that carry files, that this caller owns it), then drive a session RPC
// into the sandbox and render what it says. The wire types belong to
// protocol/workspace, which the CLI and sessiond import too — one definition per hop
// rather than three that happen to match.
//
// V0 CRUDENESS, DELIBERATE (design §4.5). The transfer is a chunk per request:
// the client POSTs a megabyte, waits for its ack, and POSTs the next one, and
// a pull is the same loop run from here. It is a slow way to move 200MB and an
// obviously correct one — no new plane, no pairing, no backpressure protocol,
// no half-transferred state anywhere but the sandbox's own staging file, and
// no way for a transfer to starve the terminal traffic sharing the session's
// connection, because only one chunk is ever in flight.
//
// THE UPGRADE PATH IS NAMED AND NOT BUILT: when the 256MiB cap starts to hurt,
// the attach plane already does exactly what an unbounded transfer needs —
// pairing, a runner dial-back, and a bidirectional byte stream that never
// touches this replica's memory (attach.go). A transfer would ride it as one
// more attachment. That is a task-week; this is two hundred lines, and the two
// have the same REST surface, so the swap is invisible to the CLI.
// ---------------------------------------------------------------------------

// filesBodyLimit caps one push chunk's request body. A chunk carries at most
// workspace.ChunkBytes of payload, which base64 inflates by a third; 2MiB leaves
// room for that and the envelope around it, and matches the session RPC's own
// payload cap (plan §Global Constraints) — the body is about to become one.
const filesBodyLimit = 2 << 20

// handleSessionDiff serves GET /v0/sessions/{id}/diff: one `--stat` per
// repository the session cloned, straight from the sandbox.
//
// Team-visible, like the other session reads — nil owner below, deliberately
// (design §4.6; see sessionForRPC for why this route and not the two beneath
// it).
func (s *Server) handleSessionDiff(w http.ResponseWriter, r *http.Request, u User) {
	row, ok := s.sessionForRPC(w, r, nil, "inspect")
	if !ok {
		return
	}
	ans, err := s.sessionDiff(r.Context(), row.ID)
	if err != nil {
		writeSandboxErr(w, row.ID, "diff", err)
		return
	}
	writeJSON(w, http.StatusOK, ans)
}

// handlePushFiles serves POST /v0/sessions/{id}/files: one chunk, forwarded to
// the sandbox, answered with the sandbox's ack.
//
// controld holds NO per-transfer state, and must not: a session's replica is
// whichever one the client's request reaches, so state kept here would have to
// be shared between them. Everything a chunk needs to be understood — the
// transfer id, the destination, the sequence number — rides on the chunk
// itself, and the sandbox is the one place that remembers.
//
// That is also why the TOTAL size cap is the sandbox's to enforce on this
// direction and not this replica's: the sandbox is the only end that sees a
// whole transfer. What is bounded here is one request — the body limit and the
// chunk cap — which is all this side ever holds at once, so an oversized push
// costs the pusher's own session's disk quota and nothing of controld's.
func (s *Server) handlePushFiles(w http.ResponseWriter, r *http.Request, u User) {
	row, ok := s.sessionForRPC(w, r, &u, "transfer files to")
	if !ok {
		return
	}
	var chunk workspace.PushChunk
	if !decodeJSONBodyLimit(w, r, &chunk, filesBodyLimit) {
		return
	}
	if msg := validatePushChunk(chunk); msg != "" {
		writeErr(w, http.StatusBadRequest, "invalid_request", msg)
		return
	}

	ack, err := s.sessionPushChunk(r.Context(), row.ID, chunk)
	if err != nil {
		writeSandboxErr(w, row.ID, "push", err)
		return
	}
	writeJSON(w, http.StatusOK, ack)
}

// validatePushChunk checks everything about a chunk that can be checked
// without asking the sandbox, and returns the caller-facing reason it cannot
// be forwarded (empty when it can).
//
// It is not a duplicate of the sandbox's own checks, it is the OUTER one: a
// path that never leaves this process is a path that never had a chance to
// escape a workspace, and a chunk refused here costs a round trip rather than
// a container's disk.
func validatePushChunk(c workspace.PushChunk) string {
	switch {
	case c.Xfer == "":
		return "xfer is required: every chunk names the transfer it belongs to"
	case len(c.Xfer) > maxXferIDLen:
		return fmt.Sprintf("xfer must be at most %d characters", maxXferIDLen)
	case c.Seq < 0:
		return "seq must not be negative"
	case len(c.Data) > workspace.ChunkBytes:
		return fmt.Sprintf("data is %s; one chunk carries at most %s",
			workspace.HumanBytes(int64(len(c.Data))), workspace.HumanBytes(workspace.ChunkBytes))
	}
	if err := workspace.ValidatePath(c.Path); err != nil {
		return err.Error()
	}
	return ""
}

// maxXferIDLen bounds the client-chosen transfer id. It is an opaque
// correlation token, never a filename (the sandbox stages under a name of its
// own choosing), but it does reach log lines and error messages.
const maxXferIDLen = 64

// handlePullFiles serves GET /v0/sessions/{id}/files?path=…: the sandbox's
// archive of that path, streamed out chunk by chunk as it arrives.
//
// Errors have two eras. Before the first byte, a failure is an ordinary JSON
// envelope like every other route's. After it, the status line is already
// sent and the only honest signal left is to abandon the response mid-body —
// http.ErrAbortHandler, which closes the connection rather than ending the
// body cleanly, so a client sees a transport failure instead of a truncated
// archive it might mistake for a complete one. (Its own gzip trailer would
// catch it too; this fails it sooner and louder.)
func (s *Server) handlePullFiles(w http.ResponseWriter, r *http.Request, u User) {
	path := r.URL.Query().Get("path")
	if err := workspace.ValidatePath(path); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	row, ok := s.sessionForRPC(w, r, &u, "transfer files from")
	if !ok {
		return
	}

	id := randHex(8)
	var sent int64
	var started bool // whether the 200 and its headers have been written
	// Two chunks of slack over the cap's worth: a transfer within the cap
	// cannot need more than that unless the far end is sending short chunks,
	// which nothing honest does. It is the second of the two rules that keep
	// this loop finite — the first is that only the last chunk may be empty
	// (sessionPullChunk) — and the belt to that one's braces.
	maxChunks := int(s.xferMax/workspace.ChunkBytes) + 2
	for seq := 0; ; seq++ {
		if seq > maxChunks {
			log.Printf("controld: pull %s from %s took more than %d chunks; abandoning", path, row.ID, maxChunks)
			panic(http.ErrAbortHandler)
		}
		chunk, err := s.sessionPullChunk(r.Context(), row.ID, workspace.PullRequest{Xfer: id, Path: path, Seq: seq})
		if err != nil {
			if !started {
				writeSandboxErr(w, row.ID, "pull", err)
				return
			}
			log.Printf("controld: pull %s from %s failed after %d bytes: %v", path, row.ID, sent, err)
			panic(http.ErrAbortHandler)
		}
		// The cap is checked BEFORE the write, so the bytes this replica
		// relays never exceed it even by one chunk. A sandbox that never says
		// done is the case this exists for: nothing else would stop it.
		if sent+int64(len(chunk.Data)) > s.xferMax {
			log.Printf("controld: pull %s from %s exceeded the %s transfer limit; abandoning",
				path, row.ID, workspace.HumanBytes(s.xferMax))
			if !started {
				writeErr(w, http.StatusConflict, "conflict",
					fmt.Sprintf("this path is larger than the %s transfer limit", workspace.HumanBytes(s.xferMax)))
				return
			}
			panic(http.ErrAbortHandler)
		}
		if !started {
			// Written on the first chunk rather than up front: everything that
			// can fail before any byte moves gets to answer with a JSON
			// envelope, which is only possible while the header is unwritten.
			w.Header().Set("Content-Type", "application/gzip")
			w.WriteHeader(http.StatusOK)
			started = true
		}
		if _, err := w.Write(chunk.Data); err != nil {
			// The client hung up. Nothing to report to anyone.
			log.Printf("controld: pull %s from %s: writing to the client: %v", path, row.ID, err)
			return
		}
		sent += int64(len(chunk.Data))
		// Flushed per chunk so the client sees progress on a slow transfer
		// rather than a stall followed by everything at once. ResponseController
		// follows statusWriter's Unwrap to the real writer.
		http.NewResponseController(w).Flush()
		if chunk.Done {
			return
		}
	}
}

// sessionForRPC is the preamble every route above shares: find the session,
// establish that there is a sandbox to talk to, and — when the route carries
// files rather than metadata — that this caller may reach into it. It answers
// the client and reports false on every failure.
//
// AUTHORIZATION SPLITS BY WHAT THE ROUTE CARRIES, on the line design §4.4 draws
// between reads and mutations:
//
//   - The DIFF is team-visible, like every other session read (§4.6 says so
//     explicitly, and handleGetSession takes no ownership check either).
//     `git diff --stat` is metadata — file paths and churn counts, no content —
//     and seeing which files a teammate's branch touched is the point of the
//     endpoint rather than an incidental read. The posture it fits is already
//     the fleet's: an admin may attach to any session and push as its owner.
//   - PUSH and PULL are owner-or-admin. They carry the working tree itself —
//     raw file bytes out, and writes into somebody's checkout — which puts them
//     on the attach side of that line, not the list-sessions side.
//
// owner is the caller to authorize against, or nil for the team-visible read.
func (s *Server) sessionForRPC(w http.ResponseWriter, r *http.Request, owner *User, verb string) (Session, bool) {
	id := r.PathValue("id")
	row, err := s.st.GetSession(r.Context(), id)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeErr(w, http.StatusNotFound, "not_found", "session not found")
			return Session{}, false
		}
		log.Printf("controld: get session %s: %v", clip(id), err)
		writeErr(w, http.StatusInternalServerError, "internal", "could not read session")
		return Session{}, false
	}
	if owner != nil && !authorizeOwnerOrAdmin(*owner, row) {
		writeErr(w, http.StatusForbidden, "forbidden", "not authorized to "+verb+" this session")
		return Session{}, false
	}
	if row.State != StateRunning {
		// No bounded wait here, unlike attach: these are one-shot requests a
		// client can simply repeat, and holding one open would tie up a
		// connection for a session that may be minutes from starting.
		writeErr(w, http.StatusServiceUnavailable, "session_not_ready",
			fmt.Sprintf("session is %s, not running", row.State))
		return Session{}, false
	}
	if row.Runner == "" || !s.runnerConnected(row.Runner) {
		writeErr(w, http.StatusBadGateway, "runner_unreachable", "runner is not connected")
		return Session{}, false
	}
	return row, true
}

// writeSandboxErr maps a session-RPC failure onto this API's error envelope.
//
// A *sandboxError is a REFUSAL: the request crossed into the sandbox, was
// understood, and was declined — a path that does not exist, a git that could
// not fetch, a chunk out of order. Its message is the sandbox's own and travels
// verbatim (clipped), because that sentence is usually the only thing that says
// what to do; `conflict` is this API's code for "understood and declined", the
// same one a create with no credential gets.
func writeSandboxErr(w http.ResponseWriter, sessionID, what string, err error) {
	var sbx *sandboxError
	switch {
	case errors.As(err, &sbx):
		writeErr(w, http.StatusConflict, "conflict", sandboxMessage(sbx.Error()))
	case errors.Is(err, ErrRunnerUnreachable):
		writeErr(w, http.StatusBadGateway, "runner_unreachable", "session did not answer")
	default:
		log.Printf("controld: %s for %s: %v", what, clip(sessionID), err)
		writeErr(w, http.StatusInternalServerError, "internal", "could not "+what+" this session")
	}
}

// maxSandboxMessage bounds a sentence that came from inside a container before
// it reaches a user's terminal. clip() is 48 characters — right for a log line
// or a websocket close reason, far too short for git's own diagnostics, which
// are the whole reason these messages are passed through.
const maxSandboxMessage = 512

func sandboxMessage(s string) string {
	if len(s) <= maxSandboxMessage {
		return s
	}
	return clipTo(s, maxSandboxMessage) + "..."
}

// ---------------------------------------------------------------------------
// GET /healthz
// ---------------------------------------------------------------------------

// handleHealthz serves GET /healthz: the one other unauthenticated route
// besides POST /v0/auth/github. Plain "ok", no internals, ever.
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

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
	"sync"
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
func sessionJSON(s control.Session, d sessionDerived) sessionView {
	return sessionView{
		ID:          string(s.ID),
		OwnerID:     string(s.CreatorID),
		Name:        s.Name,
		Image:       s.Spec.Image,
		Cmd:         emptyIfNil(s.Spec.Cmd),
		EgressAllow: emptyIfNil(s.Spec.EgressAllow),
		State:       string(s.State),
		Runner:      string(s.RunnerID),
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
	// caps is the union of every connected runner's capabilities, on the same
	// once-per-request terms and for the same reason.
	caps     map[string]bool
	capsDone bool
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
	return sessionJSON(row, d)
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

// queueReason explains a queued session its environment's requirements are
// holding back: the pinned runner is not connected to this replica or has no
// free slot, or no connected runner claims a capability the environment
// requires — and until one of those changes the scheduler will keep passing
// this session over (design §4.6). Everything else — a session that is not
// queued, an environment that requires nothing, requirements that could be
// honored right now — has no reason to give, and says nothing rather than
// guessing.
//
// The pin answers first when both apply: it names one runner, which is the
// more specific and more actionable of the two answers. Both are read off the
// environment's portable requirements: control names no runner, so an
// operator's `placement` is carried as the capability "placement:<runner>"
// (adapt_scope.go), and everything without a host prefix beside it is a
// capability a runner has to advertise.
func (r *sessionRenderer) queueReason(row control.Session, env control.Environment) string {
	if row.State != control.StateQueued {
		return ""
	}
	pin := capabilityValue(env.Requirements.Capabilities, placementCapabilityPrefix)
	if pin != "" && !r.runnerHasRoom(pin) {
		return "waiting for runner " + pin
	}
	if want := r.missingCapability(env.Requirements.Capabilities); want != "" {
		return "waiting for a runner with capability " + want
	}
	return ""
}

// missingCapability returns the first portable capability in reqs that no
// connected runner advertises, or "" when the fleet covers them all. It
// answers in requirement order because that is the order the operator wrote,
// and it is the first unmet one that is worth naming — a list of everything
// missing tells an operator nothing more about what to do next.
//
// The host's own spellings are skipped: a pin has its own, better sentence
// above, and a snapshot affinity is never a reason to queue (the scheduler
// treats it as a preference, not a requirement).
func (r *sessionRenderer) missingCapability(reqs []string) string {
	wanted := portableCapabilities(reqs)
	if len(wanted) == 0 {
		return ""
	}
	advertised := r.fleetCapabilities()
	for _, c := range wanted {
		if !advertised[c] {
			return c
		}
	}
	return ""
}

// fleetCapabilities is the union of what every CONNECTED runner claims,
// computed at most once per request. Connected is the same filter the
// scheduler places under, so the reason a session is given and the reason it
// is actually being passed over are the same fact.
//
// A store that cannot answer yields an empty set, which renders the
// waiting-for-a-capability line. That is the safer way to be wrong, for the
// same reason freeSlots gives: it explains a session that is in fact about to
// be placed, rather than silently explaining nothing about one that is stuck.
func (r *sessionRenderer) fleetCapabilities() map[string]bool {
	if r.capsDone {
		return r.caps
	}
	r.capsDone = true
	r.caps = map[string]bool{}
	rows, err := r.srv.st.Fleet().ListRunners(r.ctx, installPool)
	if err != nil {
		log.Printf("controld: rendering a session view: listing runners: %v", err)
		return r.caps
	}
	for _, row := range rows {
		if !row.Connected {
			continue
		}
		for _, c := range row.Capabilities {
			r.caps[c] = true
		}
	}
	return r.caps
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
	fleet := r.srv.st.Fleet()
	rows, err := fleet.ListRunners(r.ctx, installPool)
	if err != nil {
		log.Printf("controld: rendering a session view: listing runners: %v", err)
		return map[string]int{}
	}
	free := make(map[string]int, len(rows))
	for _, row := range rows {
		if !row.Connected {
			continue
		}
		creating, err := fleet.SessionsOnRunner(r.ctx, installPool, row.ID, []control.SessionState{control.StateCreating})
		if err != nil {
			log.Printf("controld: rendering a session view: sessions creating on a runner: %v", err)
			return map[string]int{}
		}
		free[string(row.ID)] = row.CapacityTotal - row.CapacityUsed - len(creating)
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
func repoOverrides(reqs []repoRequest) ([]control.RepoRef, error) {
	if reqs == nil {
		return nil, nil
	}
	out := make([]control.RepoRef, 0, len(reqs))
	for i, req := range reqs {
		if !validRepoRef(req.Repo) {
			return nil, fmt.Errorf("repos[%d].repo must be \"owner/name\", got %q", i, req.Repo)
		}
		ref := control.RepoRef{Repo: req.Repo}
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

	ctx := withUser(r.Context(), u)
	scope := userScope(u)

	var env *control.Environment
	if req.Environment != "" {
		resolved, ok := s.createSessionEnvironment(w, ctx, scope, req.Environment)
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
		Repos:          repos,
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
	}
	// The session's own spec always travels: for a scratch session it is the
	// whole description, for an environment session it is layered over the
	// environment field by field (control.PortableSpec) by the service.
	cmd.Spec = control.PortableSpec{
		Image:       req.Image,
		Cmd:         req.Cmd,
		EgressAllow: req.EgressAllow,
	}
	if env != nil {
		cmd.EnvironmentID = env.ID
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

// createSessionEnvironment resolves a create body's `environment` to the environment
// the session starts from. A reference nothing answers to is the caller's
// mistake, not a missing resource: it is 400 naming the reference, exactly as
// before, because the thing that was not found is a field of the request.
func (s *Server) createSessionEnvironment(w http.ResponseWriter, ctx context.Context, scope control.Scope, ref string) (control.Environment, bool) {
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
func (s *Server) createPreflight(w http.ResponseWriter, ctx context.Context, u User, repos []control.RepoRef, env *control.Environment) bool {
	if env != nil {
		_, missing, err := s.secretEnv(ctx, *env)
		switch {
		case err != nil:
			log.Printf("controld: create session: resolving secrets of environment %s: %v", env.ID, err)
			writeErr(w, http.StatusInternalServerError, "internal", "could not create session")
			return false
		case missing != "":
			writeErr(w, http.StatusConflict, "conflict", missingSecretMessage(*env, missing))
			return false
		}
	}

	refs, err := sessionRepoRefs(control.Session{Spec: control.PortableSpec{Repos: repos}}, env)
	if err != nil {
		log.Printf("controld: create session: resolving the repositories of environment %s: %v", envID(env), err)
		writeErr(w, http.StatusInternalServerError, "internal", "could not create session")
		return false
	}
	if len(refs) == 0 {
		return true
	}
	// The row itself is never read — only its existence is — so no credential
	// material is loaded, let alone rendered into the response.
	if _, err := s.st.GetCredential(ctx, u.ID, githubProvider); err != nil {
		if errors.Is(err, control.ErrNotFound) {
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
func (s *Server) secretEnv(ctx context.Context, env control.Environment) (map[string]string, string, error) {
	if len(env.SecretRefs) == 0 {
		return nil, "", nil
	}
	vars := make(map[string]string, len(env.SecretRefs))
	for _, name := range env.SecretRefs {
		ciphertext, nonce, err := s.st.GetSecret(ctx, name)
		if err != nil {
			if errors.Is(err, control.ErrNotFound) {
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
func missingSecretMessage(env control.Environment, name string) string {
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
		if errors.Is(err, control.ErrNotFound) {
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
	Capabilities    []string          `json:"capabilities"`
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
// "placement:<runner>" (adapt_scope.go) and read back out of it here, and
// `snapshot_runner` — which the control model does not carry at all — is
// passed in by the handler, which read it off the store row for the view.
// `capabilities` shares Requirements with the pin and is the rest of it: what
// this environment needs a runner to be able to DO, with the host's own
// spellings of WHERE (placement:, snapshot:) filtered back out.
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
		Connectors:      connectorsJSON(e.Connectors),
		Placement:       capabilityValue(e.Requirements.Capabilities, placementCapabilityPrefix),
		Capabilities:    emptyIfNil(portableCapabilities(e.Requirements.Capabilities)),
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
func connectorsJSON(cs []control.Connector) []json.RawMessage {
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
func validateConnectors(raw json.RawMessage) ([]control.Connector, error) {
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

	out := make([]control.Connector, 0, len(elems))
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
		out = append(out, control.Connector{Type: head.Type, Raw: elem})
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
	Capabilities    []string        `json:"capabilities,omitempty"`
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
	Capabilities    *[]string       `json:"capabilities,omitempty"`
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
		id, err := s.st.EnvironmentByName(ctx, scope.WorkspaceID, ref)
		if err != nil {
			return control.Environment{}, err
		}
		return s.environments.GetEnvironment(ctx, scope, id)
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
	holder, err := s.st.SnapshotRunner(ctx, installWorkspace, id)
	if err != nil {
		if !errors.Is(err, control.ErrNotFound) {
			log.Printf("controld: rendering environment %s: reading its snapshot runner: %v", id, err)
		}
		return ""
	}
	return string(holder)
}

// environmentRequirements composes an environment's one requirements list out
// of the two halves the wire keeps apart: the operator's runner pin, which
// control.Environment cannot name directly and so round-trips through the
// capability "placement:<runner>" (adapt_scope.go), and the portable
// capabilities the operator asked for. The pin goes first, so the list reads
// where-then-what, and environmentJSON takes the two halves back out by the
// same rule. No pin and no capabilities is no requirements at all rather than
// an empty list.
func environmentRequirements(placement string, capabilities []string) control.Requirements {
	var caps []string
	if placement != "" {
		caps = append(caps, placementCapabilityPrefix+placement)
	}
	caps = append(caps, capabilities...)
	return control.Requirements{Capabilities: caps}
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
	if err := validateCapabilities("capabilities", req.Capabilities); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_request", err.Error())
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
		Connectors:      conns,
		Requirements:    environmentRequirements(req.Placement, req.Capabilities),
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
	// The snapshot runner is a view-only column (see snapshotRunnerOf), and
	// the page the service returned cannot answer for it: control.Environment
	// carries a snapshot's holder only as a capability, and only while the
	// snapshot is still current, whereas this column has always shown the
	// runner stale or not. So it is the host lookup, once per row on the page
	// — there are few environments per deployment, and this is the same
	// question the single-environment view asks.
	runners := make(map[string]string, len(page.Environments))
	for _, row := range page.Environments {
		runners[string(row.ID)] = s.snapshotRunnerOf(ctx, row.ID)
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
		cmd.Connectors = &conns
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
	// Requirements are one field on the row and two on the wire, so a patch
	// that names either half is rebuilt from the merged pair — otherwise
	// setting `capabilities` would erase a placement nobody asked to change.
	// Naming neither leaves Requirements nil, which is what keeps an
	// untouched placement (and the snapshot affinity beside it) exactly as
	// the store has it.
	if req.Placement != nil || req.Capabilities != nil {
		placement := capabilityValue(cur.Requirements.Capabilities, placementCapabilityPrefix)
		if req.Placement != nil {
			placement = *req.Placement
		}
		capabilities := portableCapabilities(cur.Requirements.Capabilities)
		if req.Capabilities != nil {
			if err := validateCapabilities("capabilities", *req.Capabilities); err != nil {
				writeErr(w, http.StatusBadRequest, "invalid_request", err.Error())
				return
			}
			capabilities = *req.Capabilities
		}
		reqs := environmentRequirements(placement, capabilities)
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
	n, err := s.st.Environments().CountSessionsByEnvironment(ctx, installWorkspace, env.ID, control.NonTerminal)
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
// Team-visible, like the other session reads (design §4.6): `git diff --stat`
// is metadata — file paths and churn counts, no content — and the attachment
// service enforces that itself, because ActionDiff is where the policy
// adapter draws §4.4's read/mutate line.
func (s *Server) handleSessionDiff(w http.ResponseWriter, r *http.Request, u User) {
	id := sessionForRPC(r)
	ctx := withUser(r.Context(), u)
	ans, err := s.attachments.WorkspaceDiff(ctx, userScope(u), control.WorkspaceDiff{
		SessionID: control.SessionID(id),
	})
	if err != nil {
		s.writeWorkspaceErr(w, ctx, id, err, workspaceErrText{
			Verb: "inspect", Refused: "the session refused the diff"})
		return
	}
	writeJSON(w, http.StatusOK, ans)
}

// ---------------------------------------------------------------------------
// the push transfer table
//
// The wire is one chunk per request under a client-chosen `xfer`; the
// attachment service streams a whole archive from one io.Reader and mints its
// own sandbox-side transfer id. A pipe per transfer is what joins the two: the
// first chunk opens the pipe and starts the one PushWorkspace call, every
// chunk writes into it — blocking until the service has consumed the bytes,
// which is exactly the backpressure a chunk-at-a-time client already had —
// and the last one closes the writer and reports what the service made of the
// whole archive.
//
// The sandbox-side chunk numbering is the service's own from here on and is
// independent of the HTTP `seq` a client counts with. A client only ever sees
// its own acks, so the wire is unchanged.
// ---------------------------------------------------------------------------

// maxOpenPushes bounds how many uploads one replica relays at once. Each open
// transfer pins a goroutine, an io.Pipe and the service's own chunk buffer,
// and holds a staging file inside a sandbox; 64 is far above what any client
// produces (the CLI runs one transfer per invocation) and low enough that a
// client opening transfers it never continues cannot spend this replica.
//
// The refusal is 409 rather than 429, deliberately: "understood and declined,
// try again" is what conflict already means on this API, and a 429 would be a
// status code the /v0/ wire has never carried.
const maxOpenPushes = 64

var (
	// errPushDuplicate is a second chunk 0 under a transfer id already open.
	// Continuing would interleave two archives into one pipe.
	errPushDuplicate = errors.New("controld: that transfer is already open")
	// errPushBusy is maxOpenPushes.
	errPushBusy = errors.New("controld: too many transfers are open")
)

// pushKey names one transfer: the session it writes into and the client's own
// transfer id. Keyed by both, so one session's transfer can never be continued
// against another's, whatever id a client picks.
type pushKey struct{ session, xfer string }

// pushTransfer is one upload in flight across many requests.
//
// mu serializes the chunks of ONE transfer (a client sends them in order, but
// nothing on the wire guarantees it), while finish is the one closing step,
// raced for by the last chunk, a service failure, and the TTL. result is read
// only inside that Once, so every caller sees the same outcome.
type pushTransfer struct {
	path   string
	pw     *io.PipeWriter
	done   chan error
	cancel context.CancelFunc
	ttl    *time.Timer

	mu   sync.Mutex
	next int // the seq the next chunk must carry

	finish sync.Once
	result error
}

// pushTable holds this replica's open uploads. Its zero value is usable; the
// map is built on the first transfer.
type pushTable struct {
	mu sync.Mutex
	m  map[pushKey]*pushTransfer
}

// openPush starts one transfer: a pipe, and the single PushWorkspace call
// that drains it.
//
// The service's context is deliberately NOT the request's. It must outlive
// this one chunk — every later chunk of the same archive feeds the same call —
// so it is a background context carrying the caller (the authorization the
// service performs reads it from there) and bounded by the longest a whole
// transfer could honestly take: one dispatch budget per chunk of the public
// maximum, plus slack.
func (s *Server) openPush(key pushKey, u User, chunk workspace.PushChunk) error {
	s.pushes.mu.Lock()
	defer s.pushes.mu.Unlock()
	if s.pushes.m == nil {
		s.pushes.m = map[pushKey]*pushTransfer{}
	}
	if _, dup := s.pushes.m[key]; dup {
		return errPushDuplicate
	}
	if len(s.pushes.m) >= maxOpenPushes {
		return errPushBusy
	}

	pr, pw := io.Pipe()
	ctx, cancel := context.WithTimeout(withUser(context.Background(), u), s.pushBudget())
	t := &pushTransfer{path: chunk.Path, pw: pw, done: make(chan error, 1), cancel: cancel}
	go func() {
		err := s.attachments.PushWorkspace(ctx, userScope(u), control.PushWorkspace{
			SessionID: control.SessionID(key.session), Path: chunk.Path, Body: pr,
		})
		// Whatever ended the call, the writing half has to learn about it:
		// a handler blocked in pw.Write is released with this very error, so
		// a client hears about a failure on its next chunk rather than at the
		// end of an archive nobody was reading.
		pr.CloseWithError(err)
		t.done <- err
	}()
	// A transfer nobody continues is closed on the same cadence a parked
	// attach is: the client that would have finished it is gone, and the
	// pipe, the goroutine and the sandbox's staging file are not free.
	t.ttl = time.AfterFunc(s.cfg.AttachPairTTL, func() {
		s.pushes.remove(key, t)
		t.end(context.DeadlineExceeded)
	})
	s.pushes.m[key] = t
	return nil
}

// pushBudget bounds one whole transfer: a dispatch budget per chunk of the
// public maximum, plus two chunks' slack.
func (s *Server) pushBudget() time.Duration {
	return s.cfg.OpTimeout * time.Duration(workspace.MaxBytes/workspace.ChunkBytes+2)
}

func (t *pushTable) get(key pushKey) (*pushTransfer, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	tr, ok := t.m[key]
	return tr, ok
}

// remove deletes key only while it still names this exact transfer, so a
// late TTL cannot retire a transfer that has already been finished and
// replaced under the same id.
func (t *pushTable) remove(key pushKey, tr *pushTransfer) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.m[key] == tr {
		delete(t.m, key)
	}
}

// end closes the writing half and returns what the service made of the whole
// archive. cause nil ends the archive normally (the client said done);
// anything else abandons it, and the service's own error still wins, because
// it is the one that says what actually happened.
//
// It deliberately does not take t.mu: the TTL calls it while a chunk handler
// may be blocked in pw.Write holding that lock, and closing the pipe is what
// releases that handler.
func (t *pushTransfer) end(cause error) error {
	t.finish.Do(func() {
		t.ttl.Stop()
		if cause != nil {
			t.pw.CloseWithError(cause)
		} else {
			t.pw.Close()
		}
		t.result = <-t.done
		if t.result == nil && cause != nil {
			t.result = cause
		}
		t.cancel()
	})
	return t.result
}

// pushErrText names the push in a denial and in a sandbox's refusal.
var pushErrText = workspaceErrText{
	Verb:    "transfer files to",
	Refused: "the session refused the file transfer",
}

// handlePushFiles serves POST /v0/sessions/{id}/files: one chunk of an upload,
// answered with an ack for that chunk.
//
// The ack's `synced` is now this replica's answer rather than the sandbox's:
// false while the archive is still arriving, true on the chunk that ends it,
// which is the only one whose answer a client acts on. `seq` is echoed, which
// is what the CLI correlates against.
func (s *Server) handlePushFiles(w http.ResponseWriter, r *http.Request, u User) {
	id := sessionForRPC(r)
	var chunk workspace.PushChunk
	if !decodeJSONBodyLimit(w, r, &chunk, filesBodyLimit) {
		return
	}
	if msg := validatePushChunk(chunk); msg != "" {
		writeErr(w, http.StatusBadRequest, "invalid_request", msg)
		return
	}
	ctx := withUser(r.Context(), u)

	key := pushKey{session: id, xfer: chunk.Xfer}
	if chunk.Seq == 0 {
		switch err := s.openPush(key, u, chunk); {
		case errors.Is(err, errPushDuplicate):
			writeErr(w, http.StatusConflict, "conflict", "that transfer is already open")
			return
		case errors.Is(err, errPushBusy):
			writeErr(w, http.StatusConflict, "conflict",
				"too many file transfers are open on this server; retry shortly")
			return
		}
	}
	t, ok := s.pushes.get(key)
	if !ok {
		// Either it was never opened (a client that started mid-archive) or
		// it expired. Both are "there is no such transfer", and a client's
		// remedy for both is to start one at chunk 0.
		writeErr(w, http.StatusNotFound, "not_found", "unknown transfer: a transfer starts at chunk 0")
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if chunk.Path != t.path {
		writeErr(w, http.StatusBadRequest, "invalid_request",
			"every chunk of one transfer names the same path")
		return
	}
	if chunk.Seq != t.next {
		writeErr(w, http.StatusBadRequest, "invalid_request",
			fmt.Sprintf("this transfer expects chunk %d", t.next))
		return
	}

	// The write blocks until the service has taken the bytes, which is the
	// backpressure the chunk-per-request wire has always had. It fails only
	// when the service has already ended the transfer, and then the error it
	// fails with IS the service's.
	if _, err := t.pw.Write(chunk.Data); err != nil {
		s.pushes.remove(key, t)
		s.writeWorkspaceErr(w, ctx, id, t.end(err), pushErrText)
		return
	}
	t.next++
	if chunk.Done {
		s.pushes.remove(key, t)
		if err := t.end(nil); err != nil {
			s.writeWorkspaceErr(w, ctx, id, err, pushErrText)
			return
		}
		writeJSON(w, http.StatusOK, workspace.PushAck{Seq: chunk.Seq, Synced: true})
		return
	}
	t.ttl.Reset(s.cfg.AttachPairTTL)
	writeJSON(w, http.StatusOK, workspace.PushAck{Seq: chunk.Seq, Synced: false})
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
// own choosing), but it does reach this replica's transfer table.
const maxXferIDLen = 64

// handlePullFiles serves GET /v0/sessions/{id}/files?path=…: the sandbox's
// archive of that path, streamed out as the service relays it.
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
	id := sessionForRPC(r)
	ctx := withUser(r.Context(), u)

	body := &firstWriteHeader{w: w}
	err := s.attachments.PullWorkspace(ctx, userScope(u), control.PullWorkspace{
		SessionID: control.SessionID(id), Path: path, Body: body,
	})
	switch {
	case err == nil:
		// An empty archive still gets its 200: nothing was written, so the
		// header this writer defers has not gone out yet.
		body.start()
	case body.clientGone:
		// The client hung up mid-body. Nothing to report to anyone.
		log.Printf("controld: pull from %s: the client stopped reading", clip(id))
	case body.started:
		log.Printf("controld: pull from %s failed after the first byte; abandoning the response", clip(id))
		panic(http.ErrAbortHandler)
	case errors.Is(err, control.ErrInvalid):
		// The path validated above, so the only ErrInvalid left from a pull
		// is the transfer bound: this archive is bigger than this replica
		// relays. Same status it has always had.
		writeErr(w, http.StatusConflict, "conflict", "this path is larger than the transfer limit")
	default:
		s.writeWorkspaceErr(w, ctx, id, err, workspaceErrText{
			Verb: "transfer files from", Refused: "the session refused the file transfer"})
	}
}

// firstWriteHeader defers the 200 and its Content-Type to the first byte, so
// everything that can fail before any byte moves still gets to answer with a
// JSON envelope — which is only possible while the header is unwritten.
//
// It also remembers a write that failed, because "the client hung up" and
// "the service failed mid-archive" are the same sentinel by the time they
// come back and are not the same event: one is nobody's fault and is not
// worth abandoning a connection over.
type firstWriteHeader struct {
	w          http.ResponseWriter
	started    bool
	clientGone bool
}

func (f *firstWriteHeader) Write(p []byte) (int, error) {
	f.start()
	n, err := f.w.Write(p)
	if err != nil {
		f.clientGone = true
		return n, err
	}
	// Flushed per chunk so the client sees progress on a slow transfer rather
	// than a stall followed by everything at once. ResponseController follows
	// statusWriter's Unwrap to the real writer.
	http.NewResponseController(f.w).Flush()
	return n, nil
}

// start writes the status line once, whether the archive had bytes or not.
func (f *firstWriteHeader) start() {
	if f.started {
		return
	}
	f.w.Header().Set("Content-Type", "application/gzip")
	f.w.WriteHeader(http.StatusOK)
	f.started = true
}

// sessionForRPC is all these three routes still do for themselves: name the
// session. Reading it, deciding whether this caller may reach into it, and
// whether there is a sandbox to reach at all are the attachment service's,
// which answers all three as one closed sentinel apiece.
func sessionForRPC(r *http.Request) string { return r.PathValue("id") }

// writeWorkspaceErr maps an attachment-service error onto this API's error
// envelope, with the two refinements these three routes own.
//
// ErrConflict is theirs because the service reports "this session is not
// running" that way and these routes have always answered it 503
// session_not_ready — a session a client can simply ask again about in a
// moment, unlike the 409 a create conflict gets. ErrDenied is theirs because
// the sentence names the operation, and the service cannot know which of the
// three was asked for. Everything else — including the ErrUnavailable that
// covers a runner with no connection, one that did not answer, and a sandbox
// that refused — goes through the session handlers' shared mapping.
func (s *Server) writeWorkspaceErr(w http.ResponseWriter, ctx context.Context, id string, err error, text workspaceErrText) {
	switch {
	case errors.Is(err, control.ErrDenied):
		writeErr(w, http.StatusForbidden, "forbidden", "not authorized to "+text.Verb+" this session")
	case errors.Is(err, control.ErrConflict):
		writeErr(w, http.StatusServiceUnavailable, "session_not_ready", "session is not running")
	case errors.Is(err, controlapp.ErrRunnerRefused):
		// The sandbox received the request and declined it. Checked before
		// the ErrUnavailable refinement below, which would otherwise call it
		// unreachable — exactly backwards, since the sandbox is right there
		// and answering. `conflict` is this API's code for "understood and
		// declined", the same one this route has always answered with; what
		// it no longer carries is the sandbox's own sentence.
		writeErr(w, http.StatusConflict, "conflict", text.Refused)
	default:
		s.writeSessionErr(w, ctx, id, err, sessionErrText{})
	}
}

// workspaceErrText is the pair of sentences one workspace route owns: the
// operation named in a denial, and the refusal a sandbox that answered no
// gets. The service reports both as one sentinel apiece and cannot know which
// of the three operations was asked for.
type workspaceErrText struct {
	Verb    string
	Refused string
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

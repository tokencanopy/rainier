// internal/runnerd/runnerd.go
package runnerd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/tokencanopy/rainier/internal/driver"
	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/internal/wire"
	"github.com/tokencanopy/rainier/protocol/runner"
)

type Server struct {
	drv         driver.Driver
	reg         *registry
	dialBase    string // e.g. ws://runnerd:8080 — what sessiond dials to register
	seq         atomic.Int64
	egressAdmin string // http://egressd:3129 (optional)
	// proxyURL, when non-empty, is injected into every driver.Spec this
	// server creates (egress R4). New sets it from its proxyURL parameter
	// (cmd/runnerd's --proxy-url, forwarded from fleet-up.sh's derived
	// value) so the HTTP-only surface gets it too, not just agent (dial)
	// mode — Task 13's fix: before this, HTTP-only mode's CreateWithID
	// always saw Spec.ProxyURL == "" (New had no way to set it), even
	// though driver.Create had supported injecting it since Task 4.
	// RunAgent overwrites it from AgentConfig.ProxyURL before dialing —
	// today cmd/runnerd passes the same flag value to both, so that's a
	// harmless no-op reassignment, not a second source of truth.
	proxyURL string
	// onEvent is fired with "running" after a successful register() setHub
	// and "dead" when the crash path destroys a container. Guarded by
	// atomic.Pointer, not a plain field: register()'s and the hub-death
	// path's net/http request goroutines call fireEvent (reading it)
	// concurrently with agentSession swapping it in/out on every
	// (re)connect (review round 1, finding 1) — a plain field read/write
	// pair across goroutines with no synchronization is undefined behavior
	// under the Go memory model regardless of whether `-race` happens to
	// catch a given test run's particular interleaving. nil (unset) is the
	// correct zero value: HTTP-only mode never calls SetOnEvent, and
	// fireEvent's nil check is then a no-op, matching the old field's
	// nil-safety.
	//
	// detail is the event's one free-text field: empty for the lifecycle
	// states ("running", "dead" — the state IS the whole message), and the
	// rc plus output tail for a "setup_failed", which has more to say than a
	// state name can carry. It rides in the same callback rather than a
	// parallel hook so there is exactly one path an event can take out of
	// this server.
	onEvent atomic.Pointer[func(sessionID, state, detail string)]
	// agentWriterCount tracks currently-running agentSession writer
	// goroutines. It exists solely so agent_test.go can assert the
	// writer-goroutine-leak fix (review round 1, finding 3)
	// deterministically — polling this to 0/1 — instead of via
	// runtime.NumGoroutine(), which is too easily perturbed by unrelated
	// goroutines elsewhere in the test binary to be a reliable per-test
	// signal. No production code reads it.
	agentWriterCount atomic.Int64
	// onSessionRPC carries session-RPC envelopes a sandbox sent UP — its own
	// requests (a credential mint) and its responses to requests controld sent
	// down — to whatever is holding the controld connection. Same
	// atomic.Pointer discipline, and for the same reason, as onEvent: the
	// agent swaps it in on every (re)connect and clears it on exit, while
	// hub read-loop goroutines fire it.
	//
	// nil is the correct zero value and a real state, not just an
	// unconfigured one: an HTTP-only runner, or one whose controld connection
	// is down, has nowhere to forward a request to — see routeControl, which
	// answers the sandbox itself rather than leaving it waiting.
	onSessionRPC atomic.Pointer[func(sessionID string, env runner.RPCEnvelope)]
	// hubWait is how long waitHub gives a session to register (see
	// defaultHubWait, which New sets it to). It is a field rather than a
	// constant so a test can shorten it — the paths that answer "this session
	// never registered" would otherwise take ten seconds each — and it is only
	// ever written immediately after New, before this server serves anything.
	hubWait time.Duration
}

// SetOnEvent installs f as the session-event callback (nil clears it).
// Synchronized against fireEvent via atomic.Pointer so register()'s and the
// hub-death path's request goroutines can call fireEvent concurrently with
// agentSession swapping the callback in on every (re)connect and clearing it
// via defer on exit — see the onEvent field's doc comment.
func (s *Server) SetOnEvent(f func(sessionID, state, detail string)) {
	if f == nil {
		s.onEvent.Store(nil)
		return
	}
	s.onEvent.Store(&f)
}

// fireEvent reports a state that speaks for itself — the container lifecycle
// events, where there is nothing to add beyond the state name.
func (s *Server) fireEvent(sessionID, state string) { s.fireEventDetail(sessionID, state, "") }

// fireEventDetail calls the current OnEvent callback, if any is installed,
// with a state and the free-text detail that goes with it.
func (s *Server) fireEventDetail(sessionID, state, detail string) {
	if p := s.onEvent.Load(); p != nil {
		(*p)(sessionID, state, detail)
	}
}

// SetOnSessionRPC installs f as the sink for session-RPC envelopes coming up
// out of a sandbox (nil clears it). The agent installs one per controld
// connection; see the onSessionRPC field.
func (s *Server) SetOnSessionRPC(f func(sessionID string, env runner.RPCEnvelope)) {
	if f == nil {
		s.onSessionRPC.Store(nil)
		return
	}
	s.onSessionRPC.Store(&f)
}

// fireSessionRPC hands one envelope upstream, reporting whether there was
// anywhere to hand it to. Callers act on false: there is no queue behind this,
// deliberately — a request nobody can forward is answered here and now, not
// held for a connection that may be minutes away (see routeControl).
func (s *Server) fireSessionRPC(sessionID string, env runner.RPCEnvelope) bool {
	p := s.onSessionRPC.Load()
	if p == nil {
		return false
	}
	(*p)(sessionID, env)
	return true
}

// Sentinel errors returned by the extracted core ops (CreateWithID/Op/Delete)
// so the HTTP handlers can map them to the right status code and the agent's
// execute can special-case them (e.g. destroy on an already-gone session, or
// create on an id that already exists, are both still "ok" — desired state
// reached) without string-matching error text.
var (
	errNoSuchSession   = errors.New("no such session")
	errSessionStarting = errors.New("session still starting")
	errUnknownOp       = errors.New("unknown op")
	// errSessionExists is CreateWithID's answer when its putIfAbsent finds
	// the id already claimed — see CreateWithID's doc comment for the race
	// this closes.
	errSessionExists = errors.New("session already exists")
)

// egressError wraps a pushEgress failure so CreateWithID's caller can tell it
// apart from a driver.Create failure (502 vs 500) without string-matching,
// while Error() still renders the exact "egress setup: <cause>" text the HTTP
// handler used to construct inline.
type egressError struct{ err error }

func (e *egressError) Error() string { return "egress setup: " + e.err.Error() }
func (e *egressError) Unwrap() error { return e.err }

func New(drv driver.Driver, dialBase, egressAdmin, proxyURL string) *Server {
	return &Server{drv: drv, reg: newRegistry(), dialBase: dialBase, egressAdmin: egressAdmin,
		proxyURL: proxyURL, hubWait: defaultHubWait}
}

// Recover rebuilds the in-memory registry from the driver's labeled
// containers, so a restarted runnerd is truthful about sessions that
// outlived it instead of forgetting them outright. Hubs stay nil until each
// sessiond redials /register — with the entry already present, that redial
// finds its session instead of 404ing; actually driving sessiond to redial
// on reconnect is Task 5's job, not this one.
//
// Recovered entries carry allow: nil — egress rules were pushed to egressd
// (a separate process) at create time and it still holds them, so there is
// nothing to re-push here. If egressd itself restarts and loses its rules,
// that's a separate, already-ledgered known gap: v0 has no reconciliation
// between the two processes' restarts.
func (s *Server) Recover(ctx context.Context) error {
	listed, err := s.drv.List(ctx)
	if err != nil {
		return err
	}
	for _, l := range listed {
		state := "running"
		if l.Handle.State == driver.StateSuspended {
			state = "suspended"
		}
		e := &sessionEntry{id: l.SessionID, handle: l.Handle.ID, state: state}
		s.reg.put(l.SessionID, e)
	}
	log.Printf("runnerd: recovered %d session(s) from labeled containers", len(listed))
	return nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/sessions", s.sessions)   // POST create, GET list
	mux.HandleFunc("/sessions/", s.sessionOp) // /sessions/{id}/{op}
	mux.HandleFunc("/register", s.register)   // ws: sessiond dials in
	mux.HandleFunc("/attach", s.attach)       // ws: client attaches
	return mux
}

// newID is called from concurrent POST /sessions handlers (the normal fleet
// operating mode — many callers creating sessions at once), so the counter
// must be a real atomic increment: a plain s.seq++ is a read-modify-write
// with no synchronization, and two concurrent POSTs can both read the same
// value, both increment to the same next value, and mint the same id —
// registry.put then silently overwrites the first session's entry, making
// its driver handle unreachable and its capacity accounting wrong.
func (s *Server) newID() string { return "sess-" + strconv.FormatInt(s.seq.Add(1), 10) }

func (s *Server) sessions(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var body struct {
			Name        string   `json:"name"`
			Image       string   `json:"image"`
			Cmd         []string `json:"cmd"`
			EgressAllow []string `json:"egress_allow"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		id := s.newID()
		spec := driver.Spec{Name: body.Name, Image: body.Image, Cmd: body.Cmd, EgressAllow: body.EgressAllow}
		if err := s.CreateWithID(r.Context(), id, spec, body.EgressAllow); err != nil {
			var ee *egressError
			switch {
			case errors.As(err, &ee):
				http.Error(w, err.Error(), http.StatusBadGateway)
			case errors.Is(err, errSessionExists):
				// newID's atomic counter makes this practically unreachable
				// here (unlike the agent's controld-supplied ids), but
				// CreateWithID's contract covers both callers — map it
				// rather than let it fall through to a bare 500.
				http.Error(w, "session id already exists", http.StatusConflict)
			default:
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"session_id": id})
	case http.MethodGet:
		type row struct{ ID, State string }
		var rows []row
		for _, e := range s.reg.list() {
			rows = append(rows, row{e.id, e.state})
		}
		json.NewEncoder(w).Encode(rows)
	default:
		http.Error(w, "method", http.StatusMethodNotAllowed)
	}
}

// CreateWithID runs a session's full creation sequence under a caller-chosen
// id: atomically claim the id, push its egress rules, create it via the
// driver, then land the handle and "running" state. Both fronts (POST
// /sessions' HTTP handler and the agent's execute) drive this same
// sequence — id minting (POST /sessions mints one via newID; the agent uses
// controld's session id verbatim) and status-code/JSON mapping are each
// caller's own job, not this one's.
//
// The id claim (reg.putIfAbsent) is a single locked call and runs FIRST,
// before anything else — this is what makes create idempotent-by-id under
// real concurrency (review round 1, finding 2, design §4.8's binding
// requirement). Before this fix, the caller (the agent's execute) checked
// existence via a separate reg.get, then this function did its own
// reg.put — two distinct lock acquisitions with a window between them where
// two racing create commands for the same controld-supplied id (e.g. a
// retried create the sender is unsure landed) could both observe "absent"
// and both reach drv.Create: `docker run` has no name-uniqueness guard, so
// that produced a duplicate, orphaned container. Now there is exactly one
// lock acquisition deciding "is this id claimed", so at most one caller ever
// gets past it; every other one gets errSessionExists back immediately,
// before touching egress or the driver at all — the caller (agent's
// execute, or in principle the HTTP handler) treats that the same as a
// successful create, since the desired state (a session exists under this
// id) is already reached either way.
//
// This does mean pushEgress now runs AFTER the entry is claimed, not
// before as the pre-Task-6 handler had it — a required reordering, not
// incidental: the claim has to be the very first thing that can fail
// nothing else can jump the queue on. A pushEgress failure now rolls the
// claim back (reg.remove) so the id isn't left stuck "starting" forever.
//
// spec's SessionID/DialURL/ProxyURL are set here, not by the caller: they're
// this server's own concerns (the id parameter, s.dialBase, s.proxyURL), not
// anything a caller — HTTP body or runner.Spec — should be trusted to supply.
func (s *Server) CreateWithID(ctx context.Context, id string, spec driver.Spec, allow []string) error {
	// The env KEYS are captured here, at the claim, because this is the last
	// moment the Spec exists: a snapshot minutes later has to name them so the
	// commit doesn't bake their values into the environment's cached image,
	// and nothing else in this process remembers what was injected. Keys only
	// — see sessionEntry.envKeys.
	if !s.reg.putIfAbsent(id, &sessionEntry{id: id, state: "starting", allow: allow, envKeys: envKeys(spec.Env)}) {
		return errSessionExists
	}
	if err := s.pushEgress(id, allow); err != nil {
		s.reg.remove(id)
		return &egressError{err: err}
	}
	spec.SessionID = id
	spec.DialURL = s.dialBase + "/register"
	spec.ProxyURL = s.proxyURL
	h, err := s.drv.Create(ctx, spec)
	if err != nil {
		s.reg.remove(id)
		return err
	}
	s.reg.setHandle(id, h.ID)
	s.reg.setState(id, "running")
	return nil
}

func (s *Server) sessionOp(w http.ResponseWriter, r *http.Request) {
	// /sessions/{id} (DELETE) or /sessions/{id}/{op} (POST)
	rest := strings.TrimPrefix(r.URL.Path, "/sessions/")
	parts := strings.SplitN(rest, "/", 2)
	id := parts[0]
	// Existence/starting is checked BEFORE the method branch below, for
	// every method — including GET — exactly like the pre-refactor handler
	// did. Review round 1, finding 4: an earlier version of this refactor
	// checked method first, so GET on an unknown or still-starting session
	// returned 405 instead of 404/409; no test caught it (nothing here
	// sends GET), but it was a real, if narrow, behavior change from the
	// behavior-preserving mandate — restored, and now pinned by
	// TestSessionOpGetUnknownSessionReturns404.
	if _, state, ok := s.reg.opTarget(id); !ok {
		http.Error(w, "no such session", http.StatusNotFound)
		return
	} else if state == "starting" {
		http.Error(w, "session still starting", http.StatusConflict)
		return
	}
	ctx := r.Context()
	if r.Method == http.MethodDelete {
		mapOpErr(w, s.Delete(ctx, id), func() { w.WriteHeader(http.StatusNoContent) })
		return
	}
	// Every op below is a mutation (suspend/resume) or driver call
	// (snapshot); only POST may trigger them — a GET on
	// /sessions/{id}/suspend must not be able to execute one.
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	op := ""
	if len(parts) == 2 {
		op = parts[1]
	}
	if op == "snapshot" {
		// "" ref: this surface is the local dev/runnerctl one and has no
		// environment to name, so the driver mints the tag. Only the agent —
		// where controld supplies a content-addressed ref — passes one.
		ref, err := s.OpSnapshot(ctx, id, "")
		mapOpErr(w, err, func() { json.NewEncoder(w).Encode(map[string]string{"ref": ref}) })
		return
	}
	warm := op == "suspend" && r.URL.Query().Get("warm") != "false"
	mapOpErr(w, s.Op(ctx, id, op, warm), func() { w.WriteHeader(http.StatusNoContent) })
}

// mapOpErr maps Op/OpSnapshot/Delete's sentinel errors to the status codes the
// HTTP surface has always returned, or calls onOK to write the success response
// (which varies: 204 for delete/suspend/resume, a JSON ref for snapshot).
func mapOpErr(w http.ResponseWriter, err error, onOK func()) {
	switch {
	case err == nil:
		onOK()
	case errors.Is(err, errNoSuchSession):
		http.Error(w, "no such session", http.StatusNotFound)
	case errors.Is(err, errSessionStarting):
		http.Error(w, "session still starting", http.StatusConflict)
	case errors.Is(err, errUnknownOp):
		http.Error(w, "unknown op", http.StatusBadRequest)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// opHandle resolves a session id to the driver handle an op should act on,
// rejecting the two states that have none. Shared by Op, OpSnapshot and
// Delete so the guard can't drift between them.
//
// It reads through registry.opTarget rather than get()+e.handle: handle is a
// post-put field (set by CreateWithID's setHandle once drv.Create returns), so
// reading it off an unlocked pointer returned by get() would race that write.
// See registry.opTarget's doc comment.
//
// A "starting" entry (handle == "") means CreateWithID is still inside
// drv.Create — there is no driver handle yet for any op to act on. Ids can be
// predictable (POST /sessions' sequential newID) or externally chosen (the
// agent's controld-supplied id), so an op can land in exactly that window;
// reject rather than either no-op on an empty handle or block the caller until
// Create finishes.
func (s *Server) opHandle(id string) (string, error) {
	handle, _, err := s.opTarget(id)
	return handle, err
}

// opTarget is opHandle's state-carrying form for Delete, which must restore
// the prior state if driver teardown fails. Keeping the existence/starting
// guards here prevents the two operation paths from drifting.
func (s *Server) opTarget(id string) (handle, state string, err error) {
	handle, state, ok := s.reg.opTarget(id)
	if !ok {
		return "", "", errNoSuchSession
	}
	if state == "starting" {
		return "", "", errSessionStarting
	}
	return handle, state, nil
}

// OpSnapshot commits a session's current filesystem as an image and returns
// the resulting ref.
//
// It is separate from Op rather than another case inside it because ref is a
// snapshot-only concern: threading it through Op would put an image tag in
// suspend's and resume's signature, where it means nothing and every caller
// would pass "". Both fronts drive this one function — the HTTP dev surface
// with an empty ref (it has no environment to name, so the driver mints the
// tag) and the agent with controld's content-addressed rainier-env: ref.
func (s *Server) OpSnapshot(ctx context.Context, id, ref string) (string, error) {
	handle, err := s.opHandle(id)
	if err != nil {
		return "", err
	}
	snap, err := s.drv.Snapshot(ctx, handle, ref, s.stripEnvFor(id))
	if err != nil {
		return "", err
	}
	return snap.Ref, nil
}

// driverEnvKeys are the variables the DRIVER injects into every session
// container, all of which are stripped from every snapshot unconditionally.
// Two kinds, both per-session and neither meaningful in an image:
//
//   - the setup channel. An image that keeps RAINIER_SETUP_B64 makes every
//     session booted from it re-run the setup script its cache exists to
//     skip; RAINIER_SETUP_TIMEOUT is that script's bound, meaningless without
//     it.
//   - the session's own work: the repositories it was told to clone
//     (RAINIER_REPOS_B64), the init hook and its bound, and the git identity
//     its commits carry. All four are per-session decisions controld makes at
//     dispatch, and every one of them is actively wrong inside an image: a
//     baked repo list re-clones the build's repositories into every later
//     session, and a baked RAINIER_GIT_AUTHOR_* attributes everyone's commits
//     to whoever happened to trigger the build.
//   - the session's own identity and egress wiring. RAINIER_SESSION and
//     RAINIER_DIAL name the container that happened to build the image, and
//     the proxy URLs are worse than stale: the driver embeds the session id in
//     them as URL userinfo, so egressd reads it as that session's identity on
//     every CONNECT (internal/driver.withSessionUserinfo). Left in the image
//     config, the BUILD session's id outlives the build inside a cache every
//     later session boots — a credential-shaped value with no business
//     surviving its session. Both cases of each proxy var, because the driver
//     injects both.
//
// Stripping is safe for the same reason it is for secrets: the driver injects
// all of these fresh on every `docker run`, and a later -e wins over the
// image's empty value.
var driverEnvKeys = []string{
	"RAINIER_SETUP_B64", "RAINIER_SETUP_TIMEOUT",
	"RAINIER_REPOS_B64", "RAINIER_INIT_B64", "RAINIER_INIT_TIMEOUT",
	"RAINIER_GIT_AUTHOR_NAME", "RAINIER_GIT_AUTHOR_EMAIL",
	"RAINIER_DIAL", "RAINIER_SESSION",
	"HTTP_PROXY", "http_proxy",
	"HTTPS_PROXY", "https_proxy",
	"NO_PROXY", "no_proxy",
}

// stripEnvFor is the list of environment keys a snapshot of id must not leave
// in the committed image: everything the create injected (an environment's
// decrypted secrets) plus everything the driver injects (driverEnvKeys).
//
// Always both, and never conditional on what this particular session looks
// like now. A snapshot is taken minutes after the create, of a container whose
// state has moved on; "this one had no setup script" is exactly the reasoning
// that would let one image through carrying a credential. The cost of naming a
// key that was not there is nothing — the commit sets it empty, which is what
// it already was.
func (s *Server) stripEnvFor(id string) []string {
	return append(s.reg.envKeys(id), driverEnvKeys...)
}

// envKeys returns env's keys, sorted. Values are deliberately not returned,
// copied, or logged anywhere on this path.
func envKeys(env map[string]string) []string {
	if len(env) == 0 {
		return nil
	}
	return slices.Sorted(maps.Keys(env))
}

// Op runs suspend/resume against a session's current driver handle. Both
// fronts (sessionOp's HTTP handler and the agent's execute) drive this same
// sequence; warm is pre-parsed by the caller (HTTP's `?warm=` query, or the
// agent's runner.ToRunner.Warm) since query-string parsing is an HTTP concern
// this function has no business knowing about. Snapshot has its own entry
// point — see OpSnapshot.
func (s *Server) Op(ctx context.Context, id, op string, warm bool) error {
	handle, err := s.opHandle(id)
	if err != nil {
		return err
	}
	switch op {
	case "suspend":
		if !warm {
			// Cold suspend (docker stop) kills the container's sessiond,
			// which closes its /register conn — the exact same socket-level
			// event as a crash. Mark "suspending" BEFORE calling Suspend so
			// the register goroutine's hubDied (which can fire the instant
			// the container dies, racing ahead of the setState below) sees a
			// state that means "keep the entry" rather than defaulting to
			// the crash path and destroying a container we deliberately just
			// stopped.
			s.reg.setState(id, "suspending")
		}
		if err := s.drv.Suspend(ctx, handle, warm); err != nil {
			if !warm {
				s.reg.setState(id, "running") // stop failed: we're still running
			}
			return err
		}
		// e.state is mutated here from a concurrent request goroutine, so it
		// goes through the registry lock rather than a direct field write —
		// see registry.setState. Warm suspend (docker pause) doesn't kill
		// sessiond, so its conn — and the hub — stay alive; the state still
		// lands on "suspended" either way for a consistent GET /sessions view.
		s.reg.setState(id, "suspended")
		return nil
	case "resume":
		if err := s.drv.Resume(ctx, handle); err != nil {
			return err
		}
		s.reg.setState(id, "running")
		return nil
	default:
		return errUnknownOp
	}
}

// Delete tears down a session: close its hub (if it ever registered) before
// removing the registry entry and destroying the driver resource, then
// remove the entry. hub.Close() cancels its ctx, which is what unblocks
// register()'s own `<-hub.Done()` wait so that goroutine cleans up
// synchronously with this deliberate teardown instead of being left to find
// out later. register() also calls reg.remove/hub.Close on its own unblock —
// both are safe, idempotent no-ops the second time.
//
// The entry is marked "destroying" BEFORE hub.Close(), exactly as Op's cold
// suspend marks "suspending" before `docker stop` and for the same reason:
// the hub death this is about to cause is indistinguishable at the socket
// level from a crash, so the register goroutine it unblocks would otherwise
// run its crash path — Inspect the (already destroyed) container, find it
// gone, and both destroy it a second time and fire a spurious "dead" event
// while this Delete is still between its hub.Close() and its reg.remove().
// controld would then mark the session dead instead of destroyed. The marker
// makes that goroutine stand down (see register's hub-death tail).
func (s *Server) Delete(ctx context.Context, id string) error {
	handle, previousState, err := s.opTarget(id)
	if err != nil {
		return err
	}
	s.reg.setState(id, "destroying")
	if h, ok := s.reg.hub(id); ok {
		h.Close()
	}
	if err := s.drv.Destroy(ctx, handle); err != nil {
		// The container may still be alive and consuming capacity. Keep the
		// entry so Announce/reconciliation and an explicit retry can still
		// find it. Restore the prior state: sessiond redials after hub.Close,
		// and a successful redial can then install a fresh hub normally.
		s.reg.setState(id, previousState)
		return err
	}
	s.reg.remove(id)
	return nil
}

// Announce snapshots the registry in runner's session-state vocabulary, for
// the agent's announce message (and reconnect re-announces).
//
// "starting" entries are skipped — they're mid-CreateWithID, and that call's
// own result (a "create" command's result, or the id simply appearing once
// it finishes) speaks for them once they land. Omitting "starting" is safe
// specifically because Postgres shows that session as "creating" on
// controld's side, and controld's reconciliation requeues a "creating"
// session it doesn't see in an announce idempotently — a later announce, or
// a later create result, is enough to correct it either way.
//
// "suspending" (mid-cold-suspend — drv.Suspend's `docker stop` is in flight,
// hub not yet confirmed dead) is NOT skipped, unlike "starting" — review
// round 2's finding: omitting it creates an unhealable race. If the
// controld connection reconnects while a cold suspend is mid-flight,
// omitting that session from the announce reads to controld's
// reconciliation as "in Postgres but absent from announce", which marks it
// dead — a TERMINAL state that no later announce can revive, for a session
// that is (or will shortly be) perfectly healthy. So "suspending" reports
// suspended_cold immediately instead: if the stop then fails and Op's
// suspend case rolls the entry back to "running", the very next announce
// simply reports "running" and controld adopts it. Every non-terminal state
// heals itself on the next announce; only omission does not — so nothing
// runnerd doesn't know for certain is dead, dead, or gone should ever be
// omitted here.
func (s *Server) Announce() []runner.SessionInfo {
	var out []runner.SessionInfo
	for _, e := range s.reg.list() {
		state, ok := announceState(e)
		if !ok {
			continue
		}
		out = append(out, runner.SessionInfo{ID: e.id, State: state})
	}
	return out
}

// announceState renders one registry entry in runner's session-state
// vocabulary, reporting false for an entry that must not be announced at all
// ("starting" — see Announce's doc comment). It takes a value copy, never a
// live *sessionEntry, because it reads both mutable fields (state, hub); the
// caller is responsible for having snapshotted it under the registry lock
// (reg.list or reg.snapshot).
//
// Announce and the agent's idempotent-create re-announce (execute's create
// case) share this so the two renderings can never drift apart.
func announceState(e sessionEntry) (string, bool) {
	var state string
	switch {
	case e.state == "running":
		state = "running"
	case e.state == "suspended" && e.hub != nil:
		// Warm pause keeps sessiond's conn (and the hub) alive.
		state = "suspended_warm"
	case e.state == "suspended", e.state == "suspending":
		state = "suspended_cold"
	case e.state == "destroying":
		// Mid-Delete (drv.Destroy in flight). Reported, not omitted, for
		// the same reason "suspending" is: omission is the one direction
		// controld cannot heal from — it marks the row dead, terminally,
		// and a client retrying the rm then gets a session that reads
		// "dead" rather than "destroyed". Reporting it live keeps the row
		// non-terminal, so the retry (which runnerd answers as an
		// already-gone session, i.e. ok) lands it on destroyed. "running"
		// is the vocabulary's closest live state; the entry disappears
		// moments later either way.
		state = "running"
	default:
		return "", false // "starting" only — see Announce's doc comment
	}
	return state, true
}

func (s *Server) register(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("session")
	if _, ok := s.reg.get(id); !ok {
		http.Error(w, "unknown session", http.StatusNotFound)
		return
	}
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	c.SetReadLimit(16 << 20)
	hub := relay.NewHubWithControl(r.Context(), relay.WSConn(c), func(payload []byte) {
		// On its own goroutine, deliberately: this runs on the hub's read
		// loop, the single goroutine demultiplexing every attachment
		// multiplexed over this session's conn, and routeControl ends in the
		// agent's send() — which piggybacks a driver capacity call (a real
		// `docker ps`) onto every message. Running that inline would stall
		// every viewer's output for its duration. See NewHubWithControl's
		// contract.
		//
		// One goroutine per control frame means two frames could in
		// principle be reported out of order. That is fine for the whole
		// vocabulary this channel carries: a session has exactly one setup
		// outcome and one child exit, with nothing left to race after
		// either, and every session-RPC message names the request it
		// belongs to — both ends match on that id, never on arrival order
		// (which is exactly why the id is on the wire). A control channel
		// that grows ORDERED events needs a queue here instead.
		go s.routeControl(id, payload)
	})
	if !s.reg.setHub(id, hub) {
		// The entry vanished between our existence check above and now — a
		// concurrent DELETE raced this dial-in (session torn down while its
		// container was still booting). No registry entry will ever exist to
		// reap this hub later, so close it now rather than leak its readLoop
		// goroutine and the underlying fd.
		hub.Close()
		return
	}
	log.Printf("session %s registered", id)
	s.fireEvent(id, "running")
	// Block on the hub's own liveness signal, not r.Context(). websocket.Accept
	// hijacks the connection for HTTP/1.1, and net/http only cancels
	// r.Context() when this handler itself returns (conn.serve's deferred
	// w.cancelCtx) or the server's base context is canceled — the stdlib's
	// background-read-based "cancel ctx on peer close" watcher is explicitly
	// stopped by Hijack(), so r.Context() never reflects sessiond's socket
	// actually dying. hub.Done() does: it closes both when hub.readLoop
	// notices h.conn.Read fail (container/network death — readLoop already
	// cancels the hub's own ctx and tears down every attached client) and
	// when sessionOp's DELETE branch calls hub.Close() directly (deliberate
	// teardown). Either way, unblocking here is what lets us actually remove
	// the now-dead entry and let this goroutine (and its fd) go — leaving
	// this on r.Context() instead would leak both per session, forever, on
	// every abrupt death or explicit rm.
	<-hub.Done()
	handle, state, ok := s.reg.hubDied(id, hub)
	hub.Close()
	if !ok {
		return
	} // stale: a newer hub already replaced this one
	if state == "suspending" || state == "suspended" {
		return
	} // deliberate cold suspend: keep
	if state == "destroying" {
		return
	} // deliberate teardown: Delete owns the destroy and the entry removal
	// The conn died but that no longer proves the container did: sessiond
	// now survives conn loss and redials (see cmd/sessiond). Ask the driver
	// — bounded, so a hung docker daemon can't strand this goroutine (and
	// the entry) forever.
	inspectCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	h, err := s.drv.Inspect(inspectCtx, handle)
	cancel()
	if err != nil {
		// Inspect failing is NOT proof the container is gone — it's proof we
		// couldn't get an answer (daemon hiccup, timeout, ...). Destroying on
		// that uncertainty risks killing a still-running container, which is
		// the catastrophic direction; keeping a hub-less entry around risks
		// nothing worse than a stale row until a later hub death or a
		// restart's Recover resolves it. Always pick the safe direction.
		log.Printf("session %s: inspect failed after hub death (%v); keeping the entry rather than risk destroying a live container", id, err)
		return
	}
	if h.State == driver.StateRunning {
		log.Printf("session %s lost its conn but the container is alive; awaiting re-register", id)
		return
	}
	if s.reg.removeIfHubless(id) {
		// DestroyContainer, NOT Destroy: this is the crash path. The container
		// is confirmed gone and its slot has to come back, but the session's
		// workspace volume is the user's work and nobody has asked for it to
		// be thrown away — a crashed session is the one people most want their
		// files back from. controld reclaims the volume when (and only when)
		// the session is explicitly removed; see its handleDeleteSession.
		s.drv.DestroyContainer(context.Background(), handle)
		s.fireEvent(id, "dead")
	}
}

// RemoveWorkspace reclaims a session's workspace volume: the second act of an
// explicit teardown, run when the crash path deliberately kept it and the user
// has now asked for the session to be gone for good. controld dispatches it as
// a "remove_workspace" command.
//
// It refuses while the registry still holds the session. remove_workspace
// names a session rather than a container precisely because it is meant to run
// when no container is left, so a command arriving for a LIVE one is a
// misrouted or stale message — and honoring it would erase a running agent's
// working tree. Docker itself would refuse (the volume is in use by a
// container), which makes this guard belt-and-braces against the real driver;
// against the fake it is the belt, and it is what keeps the two agreeing.
func (s *Server) RemoveWorkspace(ctx context.Context, id string) error {
	if _, ok := s.reg.get(id); ok {
		return fmt.Errorf("session %s is still registered here; refusing to remove its workspace", id)
	}
	return s.drv.RemoveWorkspace(ctx, id)
}

// routeControl turns one FrameControl payload from session id into the message
// controld is waiting for, of which there are two families.
//
// EVENTS are the runner's half of the setup pipeline and the child's exit:
// sessiond watches the outcome inside the container and reports it here, and
// controld's orchestration (snapshot the image on success, fail the session on
// failure, record the exit code) acts on the resulting runner event.
//
// SESSION-RPC messages — a request the sandbox originated, or its response to
// one controld sent down — are forwarded upstream verbatim instead. This runner
// reads nothing inside them: it matches the message to the session it came from
// and passes the envelope on, ids included, because a response can only ever be
// matched by the end that assigned its id (see sendSessionRPC for the mirror
// direction).
//
// An undecodable payload or an unknown kind is logged and dropped, never
// escalated: this arrives from inside a container over a conn that also
// carries every viewer's terminal traffic, and the one thing that must not
// happen is a malformed frame taking the session down with it.
func (s *Server) routeControl(id string, payload []byte) {
	var ev relay.ControlEvent
	if err := json.Unmarshal(payload, &ev); err != nil {
		log.Printf("session %s: undecodable control payload (%d bytes): %v", id, len(payload), err)
		return
	}
	switch ev.Kind {
	case "setup_done":
		s.fireEventDetail(id, "setup_done", "")
	case "setup_failed", "stage_failed":
		// One event under two names. A session's boot is a chain of stages
		// (setup, then clone, then init — see cmd/sessiond/gitchain.go), and
		// any of them can be the one that fails.
		//
		// "setup_failed" is Plan 4's name for the only stage that existed then,
		// and it stays accepted forever: sessiond ships INSIDE the session
		// image while this runs on the host, so the two are routinely different
		// builds. sessiond still SENDS the legacy name for the setup stage, for
		// the mirror-image reason (a Plan 4 runnerd would drop a stage_failed),
		// and this arm makes the other pairing work too. An event that names no
		// stage is read as the setup one — the only sender that can omit it is
		// one speaking the old vocabulary, and mis-attributing a failure is
		// still better than dropping it.
		stage := ev.Stage
		if stage == "" {
			stage = "setup"
		}
		log.Printf("session %s: the %s stage failed (rc %d)", id, stage, ev.RC)
		if stage == "setup" {
			s.fireEventDetail(id, "setup_failed", setupFailedDetail(ev.RC, ev.Tail))
			return
		}
		s.fireEventDetail(id, "stage_failed", stageFailedDetail(stage, ev.RC, ev.Tail))
	case "credential_rejected":
		// A git operation in the sandbox was refused by GitHub. The vault mints
		// optimistically (no GitHub round-trip per mint, design §4.2), so an
		// observed refusal is the ONLY signal a stored token has been revoked,
		// and controld acts on it by flipping the credential to needs_refresh.
		//
		// It carries nothing at all, deliberately: controld knows whose
		// credential it minted for this session, and a token — or anything
		// derived from one — has no business on this channel.
		log.Printf("session %s: a git operation was refused by GitHub; reporting the credential", id)
		s.fireEventDetail(id, "credential_rejected", "")
	case "child_exited":
		// The agent process inside the container ended. This is news, not a
		// verdict: the session stays up (sessiond outlives its child so
		// viewers can read the scrollback), so runnerd reports the number and
		// changes nothing.
		//
		// The detail is the bare exit code, undecorated, because controld
		// parses it straight back into an integer column — anything added
		// here would have to be stripped there. rc 0 is a real answer and
		// travels as "0"; relay.ControlEvent's RC is `omitempty`, so a clean
		// exit puts no rc on the wire at all and decodes back to the same 0.
		log.Printf("session %s: agent exited with code %d", id, ev.RC)
		s.fireEventDetail(id, "child_exited", strconv.Itoa(ev.RC))
	case "resp":
		// The sandbox's answer to a request controld sent down. Forwarded
		// verbatim, id included: an id assigned by one end and echoed by the
		// other can only ever be matched by the end that assigned it, so this
		// hop has nothing to remap and no table to keep.
		if ev.ID == 0 {
			log.Printf("session %s: control response with no id; dropping", id)
			return
		}
		if !s.fireSessionRPC(id, runner.RPCEnvelope{ID: ev.ID, Method: "resp", OK: ev.OK, Payload: ev.Payload}) {
			// Nothing to report to and nothing to answer: answering an answer
			// is meaningless, and whoever asked has already given up (its
			// pending entry died with the connection this would have gone out
			// on).
			log.Printf("session %s: no controld connection for the response to request %d; dropping", id, ev.ID)
		}
	default:
		method, isReq := strings.CutPrefix(ev.Kind, "req:")
		if !isReq || method == "" || ev.ID == 0 {
			log.Printf("session %s: unknown control kind %q", id, ev.Kind)
			return
		}
		if s.fireSessionRPC(id, runner.RPCEnvelope{ID: ev.ID, Method: method, Payload: ev.Payload}) {
			return
		}
		// This runner has no controld connection to forward the request to.
		// The sandbox is blocked on the answer — a git process waiting on the
		// credential helper, in the case this exists for — so refusing now
		// turns a wait for its whole timeout into an immediate, explainable
		// failure the user can retry.
		log.Printf("session %s: no controld connection for %q; refusing it locally", id, method)
		if err := s.sendSessionRPC(id, runner.RPCEnvelope{ID: ev.ID, Method: "resp",
			Payload: rpcErrorPayload("this runner has no controld connection")}); err != nil {
			log.Printf("session %s: refusing %q locally: %v", id, method, err)
		}
	}
}

// sendSessionRPC delivers one envelope into a sandbox as a control frame: a
// request controld originated, or the response to one the sandbox originated.
// It is the mirror of routeControl's forwarding, and the one place the
// envelope/ControlEvent translation happens in this direction.
//
// It waits for the session's hub the same way an attach does: a container that
// was just created is legitimately a second or two from dialing in, and both
// callers run on a goroutine of their own (one command per goroutine in the
// agent's execute; one frame per goroutine out of the hub's read loop), so the
// wait blocks nothing that matters.
func (s *Server) sendSessionRPC(id string, env runner.RPCEnvelope) error {
	ev := relay.ControlEvent{Kind: "req:" + env.Method, ID: env.ID, Payload: env.Payload}
	if env.Method == "resp" {
		ev = relay.ControlEvent{Kind: "resp", ID: env.ID, OK: env.OK, Payload: env.Payload}
	}
	b, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("encoding the control frame: %w", err)
	}
	hub, ok := s.waitHub(id)
	if !ok {
		return fmt.Errorf("session %s is not registered on this runner", id)
	}
	return hub.SendControl(b)
}

// rpcErrorPayload is the {"error": ...} body every failed response on this
// channel carries — the shape controld's sandboxError and sessiond's Call both
// read the message out of.
func rpcErrorPayload(msg string) json.RawMessage {
	b, err := json.Marshal(struct {
		Error string `json:"error"`
	}{msg})
	if err != nil {
		// Unreachable (a string always marshals); an empty body still decodes
		// as a failure, just one with no reason attached.
		log.Printf("runnerd: encoding an RPC error payload: %v", err)
		return nil
	}
	return b
}

// setupFailedDetail composes the one string a runner event has room for out
// of the two things a setup failure has to say: the script's exit code (-1
// when it was killed at its timeout) and the tail of what it printed.
//
// Front-loading the rc keeps it legible when controld prefixes its own words
// ("setup failed: rc 7: E: unable to locate package foo"), and this side
// deliberately does not spell out "setup failed" itself — that is controld's
// sentence to write, and repeating it here would double it.
func setupFailedDetail(rc int, tail string) string {
	d := "rc " + strconv.Itoa(rc)
	if tail == "" {
		return d
	}
	return d + ": " + tail
}

// stageFailedDetail is the same string for a stage that is not setup, with the
// stage's name in front of it: "clone: rc 128: fatal: Authentication failed".
//
// The stage has to ride in the detail because a runner event has exactly one
// free-text field, and it goes FIRST so controld can read it back off the front
// (split at the first ": ") and compose the sentence it writes into the
// session's error column — "clone failed: rc 128: …", the same shape the setup
// prefix produces.
func stageFailedDetail(stage string, rc int, tail string) string {
	return stage + ": " + setupFailedDetail(rc, tail)
}

// defaultHubWait bounds how long an attach (or a session RPC) waits for a
// session's sessiond to register, and hubPollInterval is how often it
// re-checks. A container that was just created is legitimately a second or two
// from dialing in, so both attach fronts wait rather than failing a client that
// arrived early.
const (
	defaultHubWait  = 10 * time.Second
	hubPollInterval = 100 * time.Millisecond
)

// waitHub returns the session's hub, waiting up to hubWait for it to appear.
// Both attach fronts use it: the local HTTP /attach handler and the agent's
// dial-back for controld's attach plane.
//
// It reads the hub through the registry lock (registry.hub) rather than
// dereferencing a get()-returned pointer's .hub field, which would race
// register's setHub call on the connection's own goroutine.
func (s *Server) waitHub(id string) (*relay.Hub, bool) {
	deadline := time.Now().Add(s.hubWait)
	for {
		if h, ok := s.reg.hub(id); ok {
			return h, true
		}
		if !time.Now().Before(deadline) {
			return nil, false
		}
		time.Sleep(hubPollInterval)
	}
}

func (s *Server) attach(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("session")
	since, _ := strconv.ParseUint(r.URL.Query().Get("since"), 10, 64)

	// Wait briefly for the session to register (container may still be
	// booting).
	hub, ok := s.waitHub(id)
	if !ok {
		http.Error(w, "session not registered", http.StatusServiceUnavailable)
		return
	}

	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer c.CloseNow()
	c.SetReadLimit(16 << 20)
	// The client speaks wire.ClientMsg/ServerMsg; the hub forwards raw payloads.
	// The relay expects the first client frame to be a resize (like Plan 1 serve);
	// rattach sends it. cols/rows for the FrameOpen come from that first message.
	first, err := readFirstResize(r.Context(), c)
	if err != nil {
		c.CloseNow()
		return
	}
	hub.AttachClient(r.Context(), relay.WSConn(c), since, first.Cols, first.Rows)
}

// readFirstResize reads exactly one wire.ClientMsg off a freshly attached
// client's websocket and requires it to be a "resize" — mirroring Plan 1's
// resize-first contract (internal/server/server.go's serve()) so a client
// relayed through runnerd and one attached directly to sessiond behave
// identically.
//
// Its cols/rows size the FrameOpen sent to the session (which applies them
// via session.Attach), so this message must be consumed here and NOT also
// forwarded as a FrameClient once hub.AttachClient starts pumping: the
// FrameOpen already conveys the size, and re-sending it would double-deliver
// the same resize. Every resize after this first one flows normally, as a
// FrameClient carrying a "resize" ClientMsg.
func readFirstResize(ctx context.Context, c *websocket.Conn) (wire.ClientMsg, error) {
	var m wire.ClientMsg
	if err := wsjson.Read(ctx, c, &m); err != nil {
		return wire.ClientMsg{}, err
	}
	if m.Type != "resize" {
		return wire.ClientMsg{}, fmt.Errorf("first attach message must be resize, got %q", m.Type)
	}
	return m, nil
}

func (s *Server) pushEgress(session string, hosts []string) error {
	if s.egressAdmin == "" {
		return nil // egress optional in unit tests
	}
	q := "session=" + session
	for _, h := range hosts {
		q += "&host=" + h
	}
	resp, err := http.Post(s.egressAdmin+"/allow?"+q, "application/json", nil)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

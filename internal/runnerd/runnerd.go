// internal/runnerd/runnerd.go
package runnerd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"rainier/internal/driver"
	"rainier/internal/relay"
	"rainier/internal/rwire"
	"rainier/internal/wire"
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
	onEvent atomic.Pointer[func(sessionID, state string)]
	// agentWriterCount tracks currently-running agentSession writer
	// goroutines. It exists solely so agent_test.go can assert the
	// writer-goroutine-leak fix (review round 1, finding 3)
	// deterministically — polling this to 0/1 — instead of via
	// runtime.NumGoroutine(), which is too easily perturbed by unrelated
	// goroutines elsewhere in the test binary to be a reliable per-test
	// signal. No production code reads it.
	agentWriterCount atomic.Int64
}

// SetOnEvent installs f as the session-event callback (nil clears it).
// Synchronized against fireEvent via atomic.Pointer so register()'s and the
// hub-death path's request goroutines can call fireEvent concurrently with
// agentSession swapping the callback in on every (re)connect and clearing it
// via defer on exit — see the onEvent field's doc comment.
func (s *Server) SetOnEvent(f func(sessionID, state string)) {
	if f == nil {
		s.onEvent.Store(nil)
		return
	}
	s.onEvent.Store(&f)
}

// fireEvent calls the current OnEvent callback, if any is installed.
func (s *Server) fireEvent(sessionID, state string) {
	if p := s.onEvent.Load(); p != nil {
		(*p)(sessionID, state)
	}
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
	return &Server{drv: drv, reg: newRegistry(), dialBase: dialBase, egressAdmin: egressAdmin, proxyURL: proxyURL}
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
// anything a caller — HTTP body or rwire.Spec — should be trusted to supply.
func (s *Server) CreateWithID(ctx context.Context, id string, spec driver.Spec, allow []string) error {
	if !s.reg.putIfAbsent(id, &sessionEntry{id: id, state: "starting", allow: allow}) {
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
	handle, state, ok := s.reg.opTarget(id)
	if !ok {
		return "", errNoSuchSession
	}
	if state == "starting" {
		return "", errSessionStarting
	}
	return handle, nil
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
	snap, err := s.drv.Snapshot(ctx, handle, ref)
	if err != nil {
		return "", err
	}
	return snap.Ref, nil
}

// Op runs suspend/resume against a session's current driver handle. Both
// fronts (sessionOp's HTTP handler and the agent's execute) drive this same
// sequence; warm is pre-parsed by the caller (HTTP's `?warm=` query, or the
// agent's rwire.ToRunner.Warm) since query-string parsing is an HTTP concern
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
	handle, err := s.opHandle(id)
	if err != nil {
		return err
	}
	s.reg.setState(id, "destroying")
	if h, ok := s.reg.hub(id); ok {
		h.Close()
	}
	s.drv.Destroy(ctx, handle)
	s.reg.remove(id)
	return nil
}

// Announce snapshots the registry in rwire's session-state vocabulary, for
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
func (s *Server) Announce() []rwire.SessionInfo {
	var out []rwire.SessionInfo
	for _, e := range s.reg.list() {
		state, ok := announceState(e)
		if !ok {
			continue
		}
		out = append(out, rwire.SessionInfo{ID: e.id, State: state})
	}
	return out
}

// announceState renders one registry entry in rwire's session-state
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
	hub := relay.NewHub(r.Context(), relay.WSConn(c))
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
		s.drv.Destroy(context.Background(), handle)
		s.fireEvent(id, "dead")
	}
}

// hubWait bounds how long an attach waits for a session's sessiond to
// register, and hubPollInterval is how often it re-checks. A container that
// was just created is legitimately a second or two from dialing in, so both
// attach fronts wait rather than failing a client that arrived early.
const (
	hubWait         = 10 * time.Second
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
	deadline := time.Now().Add(hubWait)
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

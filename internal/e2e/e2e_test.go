// Package e2e is Rainier's in-process end-to-end suite: a real controld on a
// real TCP listener (memstore by default, pgstore when RAINIER_TEST_PG_DSN is
// set), real runnerd agents dialing it over real websockets with fake
// drivers, scripted sessionds standing in for containers, and a real HTTP
// client driving the REST API. Nothing here is mocked except the container
// runtime (driver.Fake) and GitHub's /user.
//
// The scenes are the design's chaos list (§7.4) and map 1:1 onto its success
// criteria (§1) — Plan 3's first, then Plan 4's environments:
//
//	TestBurstQueuesAndDrains      P3 criterion 5 (burst over capacity queues, drains)
//	TestControldRestartMidSession P3 criterion 2 (controld dies mid-attach; nothing else moves)
//	TestRunnerRestartReRegisters  P3 criterion 3 (runnerd dies; no container destroyed)
//	TestDeleteDisconnectedRunner  P3 §4.8's terminal-row-orphan rule
//	TestEnvSetupStreamsAndCaches  P4 criterion 2 (first session runs setup, second boots the cache)
//	TestEnvEditInvalidatesCache   P4 criterion 2's other half (an edited script rebuilds)
//	TestSetupFailureLandsFailed   P4 §4.3's failure path (rc + tail reach the session's error)
//	TestSecretsReachSpec          P4 criterion 4 (a secret is decrypted into the dispatched Spec)
//	TestPlacementPinQueuesWithReason P4 criterion 5 (a pin queues visibly, then places)
//	TestFullReplayReachesTheViewer P5 §4.7's rider (`attach --since 0` replays the whole log)
//
// Every scene builds its OWN stack from public surfaces only (controld.New/
// Handler/Run/NewMemStore, runnerd.New/Handler/Recover/RunAgent,
// driver.NewFake, relay.WSConn/Encode/Decode, cli.Client) — the same way a
// real operator's fleet is assembled — and every wait is a bounded poll on an
// observable condition. There is not a single sleep-and-hope in this file: a
// slow machine makes the suite slower, never flakier.
package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/jackc/pgx/v5"

	"rainier/internal/attachio"
	"rainier/internal/cli"
	"rainier/internal/controld"
	"rainier/internal/controld/pgstore"
	"rainier/internal/driver"
	"rainier/internal/relay"
	"rainier/internal/runnerd"
	"rainier/internal/wire"
)

const (
	// e2eGHToken is the GitHub access token the fake GitHub below accepts;
	// e2eLogin is the login it answers with, allowlisted as controld's admin
	// so one identity can drive every session in a scene.
	e2eGHToken = "gho_e2e_alice"
	e2eLogin   = "alice"
	// e2eRunnerToken is the fleet-wide bearer every runnerd in these scenes
	// presents on /v1/runners/connect.
	e2eRunnerToken = "rnr_e2e_fleet_token"
	// e2eSecretsKeyHex is the AES-256 key these scenes' controld seals team
	// secrets under; controld.New requires one (fail closed).
	e2eSecretsKeyHex = "e2e0e2e0e2e0e2e0e2e0e2e0e2e0e2e0e2e0e2e0e2e0e2e0e2e0e2e0e2e0e2e0"

	// pollInterval is how often every bounded wait re-checks its condition.
	pollInterval = 20 * time.Millisecond
	// wsReadLimit matches controld's, runnerd's, and sessiond's own limits.
	wsReadLimit = 16 << 20
	// pgAdminTimeout bounds the RAINIER_TEST_PG_DSN admin round trips (create
	// and drop the scene's own database), so an unreachable server fails the
	// scene rather than hanging it.
	pgAdminTimeout = 30 * time.Second
)

// e2eSecretsKey is e2eSecretsKeyHex parsed once, for every scene's controld.
var e2eSecretsKey = func() [32]byte {
	k, err := controld.ParseSecretsKey(e2eSecretsKeyHex)
	if err != nil {
		panic("e2e: bad secrets key: " + err.Error())
	}
	return k
}()

// ---------------------------------------------------------------------------
// store: memstore by default, pgstore when RAINIER_TEST_PG_DSN is set
// ---------------------------------------------------------------------------

// newStore returns the Store a scene runs against. Default is memstore, so
// `go test ./...` needs no services. Set RAINIER_TEST_PG_DSN to a URL-form
// Postgres DSN (e.g. postgres://postgres:test@127.0.0.1:5433/postgres) to run
// the same scenes against pgstore instead — each scene gets its own freshly
// created database on that server, so scenes never see each other's rows.
func newStore(t *testing.T) controld.Store {
	t.Helper()
	dsn := os.Getenv("RAINIER_TEST_PG_DSN")
	if dsn == "" {
		return controld.NewMemStore()
	}
	return newPGStore(t, dsn)
}

// newPGStore creates a fresh database on dsn's server and opens a pgstore on
// it (Open runs the migrations). The database is dropped on cleanup, after
// the pool is closed — a best-effort tidy-up, since the intended target is a
// throwaway container either way.
func newPGStore(t *testing.T, dsn string) controld.Store {
	t.Helper()
	// Bounded: a DSN pointing at an unreachable or wedged server must fail
	// this scene with a legible error, not park it until `go test`'s own
	// panic-after-10-minutes.
	ctx, cancel := context.WithTimeout(context.Background(), pgAdminTimeout)
	defer cancel()

	name := "e2e_" + cli.RandHex(8)
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("RAINIER_TEST_PG_DSN: connect: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		admin.Close(ctx)
		t.Fatalf("RAINIER_TEST_PG_DSN: create database %s: %v", name, err)
	}
	admin.Close(ctx)

	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("RAINIER_TEST_PG_DSN must be a URL-form DSN: %v", err)
	}
	u.Path = "/" + name
	st, err := pgstore.Open(ctx, u.String())
	if err != nil {
		t.Fatalf("pgstore.Open(%s): %v", name, err)
	}
	t.Cleanup(func() {
		st.Close()
		// A fresh bounded context: ctx above is already canceled by the time
		// cleanup runs (its defer fired when newPGStore returned), and a
		// canceled context would turn this best-effort tidy-up into a
		// guaranteed no-op that silently leaks a database per scene.
		dropCtx, dropCancel := context.WithTimeout(context.Background(), pgAdminTimeout)
		defer dropCancel()
		if c, err := pgx.Connect(dropCtx, dsn); err == nil {
			c.Exec(dropCtx, "DROP DATABASE IF EXISTS "+name)
			c.Close(dropCtx)
		}
	})
	return st
}

// ---------------------------------------------------------------------------
// the fleet: one controld + N runnerds + their sessionds
// ---------------------------------------------------------------------------

// fleet is one scene's whole stack. Every field is touched from the test
// goroutine only; the concurrency in the scenes lives inside controld,
// runnerd, and the scripted sessionds, all of which are internally
// synchronized.
type fleet struct {
	t     *testing.T
	ctx   context.Context // test-scoped: dials made with it stay valid for the scene
	store controld.Store
	gh    *httptest.Server
	token string // controld bearer for e2eLogin

	cd      *controldNode
	runners map[string]*runnerNode
	// sessionds tracks which sessions already have a scripted sessiond
	// attached, keyed by session id — see boot.
	sessionds map[string]*scriptedSessiond
}

// newFleet stands up a fake GitHub, a store, and a controld on an ephemeral
// loopback port, then logs in as e2eLogin and returns the fleet ready for
// runners to be added.
func newFleet(t *testing.T) *fleet {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	f := &fleet{
		t:         t,
		ctx:       ctx,
		store:     newStore(t),
		gh:        newFakeGitHub(t),
		runners:   map[string]*runnerNode{},
		sessionds: map[string]*scriptedSessiond{},
	}
	f.cd = startControld(t, f.store, f.gh.URL, "127.0.0.1:0")
	f.token = f.login()
	return f
}

func (f *fleet) baseURL() string { return "http://" + f.cd.addr }
func (f *fleet) wsBase() string  { return "ws://" + f.cd.addr }

// client returns an authenticated REST client. It carries a timeout so a
// wedged control plane fails the scene with a legible error instead of
// hanging until the go test binary's own deadline.
func (f *fleet) client() *cli.Client {
	return &cli.Client{
		Base:  f.baseURL(),
		Token: f.token,
		HTTP:  &http.Client{Timeout: 20 * time.Second},
	}
}

// login exchanges the fake GitHub token for a controld bearer through the
// real POST /v1/auth/github handler.
func (f *fleet) login() string {
	f.t.Helper()
	anon := &cli.Client{Base: f.baseURL(), HTTP: &http.Client{Timeout: 20 * time.Second}}
	var resp struct {
		Token string `json:"token"`
		User  struct {
			Login string `json:"login"`
			Role  string `json:"role"`
		} `json:"user"`
	}
	if err := anon.Do(http.MethodPost, "/v1/auth/github", map[string]string{"access_token": e2eGHToken}, &resp); err != nil {
		f.t.Fatalf("POST /v1/auth/github: %v", err)
	}
	if resp.Token == "" || resp.User.Login != e2eLogin || resp.User.Role != "admin" {
		f.t.Fatalf("login response = %+v, want a token for admin %q", resp, e2eLogin)
	}
	return resp.Token
}

// newFakeGitHub serves the one endpoint controld's token exchange calls.
func newFakeGitHub(t *testing.T) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/user" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") != "Bearer "+e2eGHToken {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		// The exchange reads a token's scopes off this same response and
		// seals the credential with them (Plan 5 vault); reporting `repo`
		// keeps the fixture's logins free of the missing-scope warning.
		w.Header().Set("X-OAuth-Scopes", "repo, read:user")
		json.NewEncoder(w).Encode(map[string]any{"id": 1001, "login": e2eLogin})
	}))
	t.Cleanup(ts.Close)
	return ts
}

// ---------------------------------------------------------------------------
// controld node
// ---------------------------------------------------------------------------

// controldNode is one controld process's worth of state: the Server, the
// http.Server hosting it, and the context both its scheduler loop and every
// in-flight request (including the hijacked websockets) hang off.
//
// That last part is why the node owns a context at all: http.Server.Close
// does not close hijacked connections, so killing controld the way a `kill`
// does — every socket dropped, runners forced to redial — means canceling
// the base context the handlers' request contexts derive from.
type controldNode struct {
	srv    *controld.Server
	http   *http.Server
	cancel context.CancelFunc
	addr   string
	done   chan struct{}
	once   sync.Once
}

// startControld binds addr ("host:port"; port 0 for an ephemeral one) and
// serves a fresh controld.Server over st there. ExternalURL must name the
// listener's own address — the attach plane derives the runner dial-back URL
// from it — so the listener is created before the Server.
func startControld(t *testing.T, st controld.Store, githubAPI, addr string) *controldNode {
	t.Helper()
	ln := listenOn(t, addr)
	actual := ln.Addr().String()

	srv, err := controld.New(st, controld.Config{
		RunnerToken:   e2eRunnerToken,
		SecretsKey:    e2eSecretsKey,
		Admins:        []string{e2eLogin},
		GitHubAPIBase: githubAPI,
		ExternalURL:   "http://" + actual,
		OpTimeout:     10 * time.Second,
		AttachWait:    10 * time.Second,
		AttachPairTTL: 10 * time.Second,
	})
	if err != nil {
		ln.Close()
		t.Fatalf("controld.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	n := &controldNode{
		srv:    srv,
		cancel: cancel,
		addr:   actual,
		done:   make(chan struct{}),
	}
	n.http = &http.Server{
		Handler:     srv.Handler(),
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	go func() {
		defer close(n.done)
		n.http.Serve(ln)
	}()
	go srv.Run(ctx)
	t.Cleanup(n.stop)
	return n
}

// stop kills this controld: cancel first (which is what tears down the
// hijacked runner and attach sockets, and stops the scheduler loop), then
// close the listener and every non-hijacked connection. Idempotent, so the
// scenes can stop a node explicitly and still have t.Cleanup registered.
func (n *controldNode) stop() {
	n.once.Do(func() {
		n.cancel()
		n.http.Close()
		<-n.done
	})
}

// restartControld throws the current controld away and builds a new one on
// the SAME store and the SAME listen address — the runners' RunAgent loops
// are still dialing that address, so this is exactly a controld process
// restart from their point of view.
func (f *fleet) restartControld() {
	f.t.Helper()
	addr := f.cd.addr
	f.cd.stop()
	f.cd = startControld(f.t, f.store, f.gh.URL, addr)
}

// listenOn binds addr, retrying briefly: rebinding the port a just-stopped
// controld held is the whole point of the restart scene, and the kernel can
// take a moment to release it.
func listenOn(t *testing.T, addr string) net.Listener {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		ln, err := net.Listen("tcp", addr)
		if err == nil {
			return ln
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("listen on %s: %v", addr, err)
		}
		time.Sleep(pollInterval)
	}
}

// ---------------------------------------------------------------------------
// runner node
// ---------------------------------------------------------------------------

// runnerNode is one runnerd process: a fake driver, the Server, its local
// HTTP surface (which is what scripted sessionds dial to register), and the
// context its RunAgent loop runs under.
type runnerNode struct {
	name   string
	drv    *driver.Fake
	rd     *runnerd.Server
	ts     *httptest.Server
	wsBase string
	cancel context.CancelFunc
	once   sync.Once
}

// addRunner starts a new runnerd with a fresh fake driver of `slots` slots
// and dials it into controld.
func (f *fleet) addRunner(name string, slots int) *runnerNode {
	f.t.Helper()
	return f.startRunner(name, driver.NewFake(slots))
}

// startRunner starts a runnerd over drv. Reusing a driver that already holds
// containers is how the restart scenes simulate a process death that the
// containers outlived: Recover rebuilds the registry from exactly that.
func (f *fleet) startRunner(name string, drv *driver.Fake) *runnerNode {
	f.t.Helper()

	// The local surface's address has to be known before runnerd.New (it is
	// the dial-base handed to every session), so the server is built
	// unstarted and its handler installed once the Server exists.
	ts := httptest.NewUnstartedServer(nil)
	wsBase := "ws://" + ts.Listener.Addr().String()
	rd := runnerd.New(drv, wsBase, "", "")
	ts.Config.Handler = rd.Handler()
	ts.Start()

	if err := rd.Recover(f.ctx); err != nil {
		ts.Close()
		f.t.Fatalf("runnerd %s: Recover: %v", name, err)
	}

	ctx, cancel := context.WithCancel(f.ctx)
	n := &runnerNode{name: name, drv: drv, rd: rd, ts: ts, wsBase: wsBase, cancel: cancel}
	go rd.RunAgent(ctx, runnerd.AgentConfig{
		ControldURL: f.wsBase(),
		Token:       e2eRunnerToken,
		RunnerName:  name,
	})
	f.runners[name] = n
	f.t.Cleanup(n.stop)
	return n
}

func (n *runnerNode) stop() {
	n.once.Do(func() {
		n.cancel()
		n.ts.Close()
	})
}

// stopRunner kills a runner the way a `kill -9` on the VM would: the agent
// loop stops and the local surface goes away, while the driver keeps every
// container it holds.
func (f *fleet) stopRunner(name string) *driver.Fake {
	f.t.Helper()
	n := f.runners[name]
	if n == nil {
		f.t.Fatalf("no runner named %q", name)
	}
	n.stop()
	return n.drv
}

// restartRunner brings a stopped runner back with the same driver contents —
// a restarted runnerd on a VM whose containers survived it.
func (f *fleet) restartRunner(name string) *runnerNode {
	f.t.Helper()
	old := f.runners[name]
	if old == nil {
		f.t.Fatalf("no runner named %q", name)
	}
	old.stop()
	return f.startRunner(name, old.drv)
}

// containers returns the session ids the runner's driver currently holds.
func (n *runnerNode) containers(t *testing.T) []string {
	t.Helper()
	listed, err := n.drv.List(context.Background())
	if err != nil {
		t.Fatalf("driver.List: %v", err)
	}
	ids := make([]string, 0, len(listed))
	for _, l := range listed {
		ids = append(ids, l.SessionID)
	}
	sort.Strings(ids)
	return ids
}

// ---------------------------------------------------------------------------
// scripted sessiond (stands in for a container)
// ---------------------------------------------------------------------------

// scriptedSessiond speaks relay frames on a session's behalf: it answers
// every attach (FrameOpen) with a snapshot naming its session and echoes
// every stdin ClientMsg back as output. It is the same shape as
// internal/cli's smoke-test helper and internal/controld's fakeSessiond,
// rebuilt here from relay's public API because both of those are
// package-internal to their own tests.
type scriptedSessiond struct {
	id   string
	conn relay.Conn

	// mu guards the scripted event log and the record of what every attach
	// asked for; serve() runs on its own goroutine, and the scene reads both
	// from the test's.
	mu    sync.Mutex
	log   [][]byte // scripted event log: entry i has sequence number i+1
	opens []uint64 // the cursor of every FrameOpen served, in order
}

// script gives this sessiond an n-entry event log to replay — output events
// the way a real session's would already be in its log by the time a viewer
// asks for them. Entry i is "line-<i+1>\n", so a replay's order and
// completeness are both checkable from the bytes alone.
func (ss *scriptedSessiond) script(n int) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.log = make([][]byte, n)
	for i := range ss.log {
		ss.log[i] = []byte(fmt.Sprintf("line-%d\n", i+1))
	}
}

// cursors returns the cursor every attach served so far arrived with — the
// value that survived (or didn't) the trip from the client's query string
// through controld's dial_attach, runnerd's hub, and relay's Frame.
func (ss *scriptedSessiond) cursors() []uint64 {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return slices.Clone(ss.opens)
}

// openFrames is what an attach opens with, mirroring session.Attach's rule
// (whose own tests pin the real one): the whole log for wire.SinceAll, the
// entries after a resume cursor, and the snapshot for everything else —
// including a cursor the log cannot answer.
func (ss *scriptedSessiond) openFrames(since uint64) []wire.ServerMsg {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.opens = append(ss.opens, since)

	last := uint64(len(ss.log))
	from := since
	if since == wire.SinceAll {
		from = 0
	}
	if last == 0 || (since != wire.SinceAll && (since == 0 || since > last)) {
		return []wire.ServerMsg{{Type: "snapshot", Seq: 1, Data: []byte(snapshotFor(ss.id))}}
	}
	out := make([]wire.ServerMsg, 0, last-from)
	for i := from; i < last; i++ {
		out = append(out, wire.ServerMsg{Type: "output", Seq: i + 1, Data: ss.log[i]})
	}
	return out
}

// snapshotFor is the snapshot text a session's scripted sessiond replays, so
// an attach can prove it reached the session it asked for and not some other.
func snapshotFor(sessionID string) string { return "snapshot-of-" + sessionID }

// dialSessiond registers a scripted sessiond for sessionID against runnerd's
// local surface. It reports false rather than failing the test when the dial
// doesn't land — the session's registry entry may not exist yet (create still
// in flight), and boot simply tries again on the next poll pass.
//
// ctx is the scene's, not a per-dial timeout: coder/websocket ties the
// handshake to the dial context via net/http, and canceling it after a
// successful handshake would close the connection underneath us.
func dialSessiond(ctx context.Context, wsBase, sessionID string) (*scriptedSessiond, bool) {
	c, _, err := websocket.Dial(ctx, wsBase+"/register?session="+sessionID, nil)
	if err != nil {
		return nil, false
	}
	c.SetReadLimit(wsReadLimit)
	ss := &scriptedSessiond{id: sessionID, conn: relay.WSConn(c)}
	go ss.serve()
	return ss, true
}

func (ss *scriptedSessiond) serve() {
	ctx := context.Background()
	for {
		raw, err := ss.conn.Read(ctx)
		if err != nil {
			return // conn closed: this "container" is done
		}
		f, err := relay.Decode(raw)
		if err != nil {
			continue
		}
		switch f.Type {
		case relay.FrameOpen:
			for _, m := range ss.openFrames(f.Since) {
				ss.send(ctx, f.AttachID, m)
			}
		case relay.FrameClient:
			var m wire.ClientMsg
			if json.Unmarshal(f.Payload, &m) != nil {
				continue
			}
			if m.Type == "stdin" {
				ss.send(ctx, f.AttachID, wire.ServerMsg{Type: "output", Seq: 2, Data: m.Data})
			}
		}
	}
}

func (ss *scriptedSessiond) send(ctx context.Context, attachID uint64, m wire.ServerMsg) {
	payload, err := json.Marshal(m)
	if err != nil {
		return
	}
	raw, err := relay.Encode(relay.Frame{Type: relay.FrameServer, AttachID: attachID, Payload: payload})
	if err != nil {
		return
	}
	ss.conn.Write(ctx, raw)
}

// control sends one FrameControl — sessiond reporting on the session as a
// whole rather than on any one viewer, which is how the setup pipeline's two
// outcomes travel (design §4.3). A real sessiond sends exactly this frame
// when its setup wrapper writes an exit code; runnerd's hub hands the payload
// to routeControl, which turns it into the rwire event controld's snapshot
// orchestration is waiting on. Playing it from here is what lets these scenes
// exercise the whole pipeline without a container.
//
// AttachID is deliberately left zero: no attachment owns a control frame, and
// relay.Conn's own Send does the same.
func (ss *scriptedSessiond) control(t *testing.T, ev relay.ControlEvent) {
	t.Helper()
	payload, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshaling control event %+v: %v", ev, err)
	}
	raw, err := relay.Encode(relay.Frame{Type: relay.FrameControl, Payload: payload})
	if err != nil {
		t.Fatalf("encoding control frame for %s: %v", ss.id, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := ss.conn.Write(ctx, raw); err != nil {
		t.Fatalf("session %s: sending %s: %v", ss.id, ev.Kind, err)
	}
}

func (ss *scriptedSessiond) close() { ss.conn.Close() }

// boot starts a scripted sessiond for every placed session that doesn't have
// one yet — what a real container does a second or two after `docker run`,
// and the only thing that ever drives a session from `creating` to `running`
// (runnerd's "running" event fires on register). It is called on every poll
// pass, so a session placed between passes boots on the next one.
func (f *fleet) boot(rows map[string]apiSession) {
	for id, row := range rows {
		if row.Runner == "" || (row.State != "creating" && row.State != "running") {
			continue
		}
		if _, ok := f.sessionds[id]; ok {
			continue
		}
		rn := f.runners[row.Runner]
		if rn == nil {
			continue
		}
		ss, ok := dialSessiond(f.ctx, rn.wsBase, id)
		if !ok {
			continue // create still in flight, or the runner is down; try again next pass
		}
		f.sessionds[id] = ss
		f.t.Cleanup(ss.close)
	}
}

// sessiond returns the scripted sessiond standing in for id's container,
// waiting (and booting, on every poll pass) until there is one. It is how a
// scene reaches inside "the container" to play a control event — the session
// has to be placed and registered first, which is exactly the state its real
// sessiond would be in when its setup script finishes.
func (f *fleet) sessiond(id string) *scriptedSessiond {
	f.t.Helper()
	f.waitSessions(60*time.Second, "session "+id+" to register a sessiond", func(map[string]apiSession) bool {
		_, ok := f.sessionds[id]
		return ok
	})
	return f.sessionds[id]
}

// dropSessiond closes a session's scripted sessiond and forgets it, so the
// next boot pass dials a fresh one — what a real sessiond's redial loop does
// after its runnerd dies underneath it (design §4.8, carried hardening).
func (f *fleet) dropSessiond(id string) {
	if ss, ok := f.sessionds[id]; ok {
		ss.close()
		delete(f.sessionds, id)
	}
}

// ---------------------------------------------------------------------------
// REST helpers
// ---------------------------------------------------------------------------

// apiSession mirrors the fields of controld's client-facing session view
// (api.go's sessionView) these scenes assert on.
type apiSession struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Image     string `json:"image"`
	State     string `json:"state"`
	Runner    string `json:"runner"`
	Reachable bool   `json:"reachable"`
	Error     string `json:"error"`
	// Environment is the environment's NAME, and QueueReason explains a
	// queued session its environment's placement pin is holding back — both
	// derived by controld per request, never stored on the row.
	Environment string `json:"environment"`
	QueueReason string `json:"queue_reason"`
	// ChildExitCode is the agent process's own exit status once it has one —
	// null while it is still running, which is why it is a pointer: exit 0 is
	// an answer and must not read as "no answer yet".
	ChildExitCode *int `json:"child_exit_code"`
}

type sessionEnvelope struct {
	Session apiSession `json:"session"`
}

type sessionsEnvelope struct {
	Sessions   []apiSession `json:"sessions"`
	NextCursor string       `json:"next_cursor"`
}

// create posts a session exactly as `rainier new` does, fresh
// Idempotency-Key and all, and returns the 202's view of it.
func (f *fleet) create(name string) apiSession {
	f.t.Helper()
	var resp sessionEnvelope
	body := map[string]any{"name": name, "image": "e2e-image"}
	err := f.client().Do(http.MethodPost, "/v1/sessions", body, &resp, cli.IdempotencyKey(cli.RandHex(8)))
	if err != nil {
		f.t.Fatalf("POST /v1/sessions (%s): %v", name, err)
	}
	if resp.Session.State != "queued" {
		f.t.Fatalf("created session %s state = %q, want queued", name, resp.Session.State)
	}
	return resp.Session
}

// list pages GET /v1/sessions?all=true and returns every session by id —
// terminal ones included, since half of what these scenes assert is what a
// session ended up as.
func (f *fleet) list() map[string]apiSession {
	f.t.Helper()
	out := map[string]apiSession{}
	c := f.client()
	cursor := ""
	for {
		path := "/v1/sessions?all=true&limit=100"
		if cursor != "" {
			path += "&cursor=" + url.QueryEscape(cursor)
		}
		var page sessionsEnvelope
		if err := c.Do(http.MethodGet, path, nil, &page); err != nil {
			f.t.Fatalf("GET %s: %v", path, err)
		}
		for _, s := range page.Sessions {
			out[s.ID] = s
		}
		if page.NextCursor == "" {
			return out
		}
		cursor = page.NextCursor
	}
}

// createFrom posts a session that starts FROM an environment, exactly as
// `rainier new --env <ref>` does: no image of its own, so everything the
// session runs — the resolved image, the setup script, the egress list, the
// decrypted secrets — is the environment's to decide.
func (f *fleet) createFrom(name, envRef string) apiSession {
	f.t.Helper()
	var resp sessionEnvelope
	body := map[string]any{"name": name, "environment": envRef}
	err := f.client().Do(http.MethodPost, "/v1/sessions", body, &resp, cli.IdempotencyKey(cli.RandHex(8)))
	if err != nil {
		f.t.Fatalf("POST /v1/sessions (%s from environment %s): %v", name, envRef, err)
	}
	if resp.Session.State != "queued" {
		f.t.Fatalf("created session %s state = %q, want queued", name, resp.Session.State)
	}
	return resp.Session
}

// delete removes a session exactly as `rainier rm` does.
func (f *fleet) delete(id string) {
	f.t.Helper()
	if err := f.client().Do(http.MethodDelete, "/v1/sessions/"+id, nil, nil); err != nil {
		f.t.Fatalf("DELETE /v1/sessions/%s: %v", id, err)
	}
}

type runnersEnvelope struct {
	Runners []struct {
		Name          string `json:"name"`
		Connected     bool   `json:"connected"`
		CapacityUsed  int    `json:"capacity_used"`
		CapacityTotal int    `json:"capacity_total"`
	} `json:"runners"`
}

// waitRunner polls GET /v1/runners until name's connected flag matches want.
func (f *fleet) waitRunner(name string, want bool, timeout time.Duration) {
	f.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		var resp runnersEnvelope
		if err := f.client().Do(http.MethodGet, "/v1/runners", nil, &resp); err != nil {
			f.t.Fatalf("GET /v1/runners: %v", err)
		}
		for _, r := range resp.Runners {
			if r.Name == name && r.Connected == want {
				return
			}
		}
		if !time.Now().Before(deadline) {
			f.t.Fatalf("runner %s never reported connected=%v within %s (runners: %+v)", name, want, timeout, resp.Runners)
		}
		time.Sleep(pollInterval)
	}
}

// waitSessions polls the session list until cond holds, booting a scripted
// sessiond for every newly placed session on each pass, and fails the scene
// with the full session table when the budget runs out.
func (f *fleet) waitSessions(timeout time.Duration, what string, cond func(map[string]apiSession) bool) map[string]apiSession {
	f.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		rows := f.list()
		f.boot(rows)
		if cond(rows) {
			return rows
		}
		if !time.Now().Before(deadline) {
			f.t.Fatalf("timed out after %s waiting for %s; sessions:\n%s", timeout, what, describe(rows))
		}
		time.Sleep(pollInterval)
	}
}

// waitUntil is waitSessions' shape for conditions that aren't about the
// session table (a driver's container set, say).
func waitUntil(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(pollInterval)
	}
}

// describe renders the session table for a failure message, sorted by name so
// two runs of the same failure read the same.
func describe(rows map[string]apiSession) string {
	list := make([]apiSession, 0, len(rows))
	for _, r := range rows {
		list = append(list, r)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
	var b strings.Builder
	for _, r := range list {
		fmt.Fprintf(&b, "  %s %-10s state=%-14s runner=%-6s reachable=%v err=%q\n",
			r.ID, r.Name, r.State, r.Runner, r.Reachable, r.Error)
	}
	return b.String()
}

// counts tallies a session table by state.
func counts(rows map[string]apiSession) map[string]int {
	out := map[string]int{}
	for _, r := range rows {
		out[r.State]++
	}
	return out
}

// liveOn returns the ids of the sessions placed on runner and not terminal,
// sorted for determinism.
func liveOn(rows map[string]apiSession, runner string) []string {
	var ids []string
	for _, r := range rows {
		if r.Runner != runner {
			continue
		}
		switch r.State {
		case "creating", "running", "suspended_warm", "suspended_cold":
			ids = append(ids, r.ID)
		}
	}
	sort.Strings(ids)
	return ids
}

// ---------------------------------------------------------------------------
// environments and secrets (Plan 4)
// ---------------------------------------------------------------------------

// apiEnvironment mirrors the fields of controld's client-facing environment
// view (api.go's environmentView) these scenes assert on. The three snapshot
// fields are the whole cache: a ref, the runner that holds it, and the setup
// hash it was built from — which is stale exactly when it stops matching
// SetupHash.
type apiEnvironment struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	Image           string `json:"image"`
	Setup           string `json:"setup"`
	SetupHash       string `json:"setup_hash"`
	Placement       string `json:"placement"`
	SetupTimeoutSec int    `json:"setup_timeout_sec"`
	SnapshotRef     string `json:"snapshot_ref"`
	SnapshotRunner  string `json:"snapshot_runner"`
	SnapshotHash    string `json:"snapshot_hash"`
}

type environmentEnvelope struct {
	Environment apiEnvironment `json:"environment"`
}

// snapshotRefHashLen is how much of an environment's setup hash goes into its
// snapshot ref (design §4.3).
const snapshotRefHashLen = 12

// snapshotRefFor builds the ref controld content-addresses env's cached image
// under: rainier-env:<env id>-<first 12 hex of its setup hash>.
//
// It spells the format out rather than asking controld what name it picked —
// controld's own helper is unexported, and a scene that read the answer back
// off the same code that produced it could not tell a correct ref from a
// silently changed format. This is the wire-visible contract: every replica
// derives this name independently, and a runner's `docker images` shows it.
func snapshotRefFor(t *testing.T, env apiEnvironment) string {
	t.Helper()
	if len(env.SetupHash) < snapshotRefHashLen {
		t.Fatalf("environment %s has setup_hash %q, too short to build a snapshot ref from", env.ID, env.SetupHash)
	}
	return "rainier-env:" + env.ID + "-" + env.SetupHash[:snapshotRefHashLen]
}

// createEnv posts an environment as `rainier env create` does. body is a raw
// map so a scene can send exactly the fields it means to — the API rejects
// unknown ones, so a typo here fails loudly rather than silently defaulting.
func (f *fleet) createEnv(body map[string]any) apiEnvironment {
	f.t.Helper()
	var resp environmentEnvelope
	if err := f.client().Do(http.MethodPost, "/v1/environments", body, &resp); err != nil {
		f.t.Fatalf("POST /v1/environments (%v): %v", body["name"], err)
	}
	return resp.Environment
}

// getEnv reads one environment by id or name.
func (f *fleet) getEnv(ref string) apiEnvironment {
	f.t.Helper()
	var resp environmentEnvelope
	if err := f.client().Do(http.MethodGet, "/v1/environments/"+ref, nil, &resp); err != nil {
		f.t.Fatalf("GET /v1/environments/%s: %v", ref, err)
	}
	return resp.Environment
}

// patchEnv edits an environment as `rainier env update` does.
func (f *fleet) patchEnv(ref string, patch map[string]any) apiEnvironment {
	f.t.Helper()
	var resp environmentEnvelope
	if err := f.client().Do(http.MethodPatch, "/v1/environments/"+ref, patch, &resp); err != nil {
		f.t.Fatalf("PATCH /v1/environments/%s (%v): %v", ref, patch, err)
	}
	return resp.Environment
}

// waitEnv polls one environment until cond holds — the bounded wait for work
// controld does off the request path, namely the snapshot it commits after a
// setup script reports success.
func (f *fleet) waitEnv(ref string, timeout time.Duration, what string, cond func(apiEnvironment) bool) apiEnvironment {
	f.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		env := f.getEnv(ref)
		if cond(env) {
			return env
		}
		if !time.Now().Before(deadline) {
			f.t.Fatalf("timed out after %s waiting for %s; environment: %+v", timeout, what, env)
		}
		time.Sleep(pollInterval)
	}
}

// putSecret stores a team secret as `rainier secret set` does.
func (f *fleet) putSecret(name, value string) {
	f.t.Helper()
	if err := f.client().Do(http.MethodPut, "/v1/secrets/"+name, map[string]string{"value": value}, nil); err != nil {
		f.t.Fatalf("PUT /v1/secrets/%s: %v", name, err)
	}
}

// ---------------------------------------------------------------------------
// attach client
// ---------------------------------------------------------------------------

// attachConn is a client attached through controld's attach plane, speaking
// the same wire.ClientMsg/ServerMsg protocol `rainier attach` speaks.
type attachConn struct {
	t      *testing.T
	c      *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	once   sync.Once
}

// attach opens WS /v1/sessions/{id}/attach and completes the resize-first
// contract. It retries the dial briefly: a session that just became reachable
// can still answer 503 session_not_ready for a beat, exactly as `rainier new`
// expects (attachio.ErrSessionNotReady).
func (f *fleet) attach(id string, since uint64) *attachConn {
	f.t.Helper()
	// The dial context must outlive the handshake: net/http closes a hijacked
	// connection when its request context is canceled, so a short-lived dial
	// context would take the attach down with it.
	ctx, cancel := context.WithCancel(f.ctx)
	u := f.wsBase() + "/v1/sessions/" + id + "/attach?since=" + strconv.FormatUint(since, 10)
	hdr := http.Header{"Authorization": {"Bearer " + f.token}}

	deadline := time.Now().Add(30 * time.Second)
	for {
		c, _, err := websocket.Dial(ctx, u, &websocket.DialOptions{HTTPHeader: hdr})
		if err == nil {
			c.SetReadLimit(wsReadLimit)
			a := &attachConn{t: f.t, c: c, ctx: ctx, cancel: cancel}
			f.t.Cleanup(a.close)
			wctx, wcancel := context.WithTimeout(ctx, 10*time.Second)
			defer wcancel()
			if err := wsjson.Write(wctx, c, wire.ClientMsg{Type: "resize", Cols: 80, Rows: 24}); err != nil {
				f.t.Fatalf("attach %s: sending the first resize: %v", id, err)
			}
			return a
		}
		if !time.Now().Before(deadline) {
			cancel()
			f.t.Fatalf("attach %s: %v", id, err)
		}
		time.Sleep(pollInterval)
	}
}

// read takes the next server message, failing the test if none arrives.
func (a *attachConn) read() wire.ServerMsg {
	a.t.Helper()
	m, err := a.readErr(10 * time.Second)
	if err != nil {
		a.t.Fatalf("attach read: %v", err)
	}
	return m
}

// readErr is read without the assertion — for the scene that wants to prove
// the attach DID die.
func (a *attachConn) readErr(timeout time.Duration) (wire.ServerMsg, error) {
	ctx, cancel := context.WithTimeout(a.ctx, timeout)
	defer cancel()
	var m wire.ServerMsg
	err := wsjson.Read(ctx, a.c, &m)
	return m, err
}

// stdin sends keystrokes, which the scripted sessiond echoes back.
func (a *attachConn) stdin(s string) {
	a.t.Helper()
	ctx, cancel := context.WithTimeout(a.ctx, 10*time.Second)
	defer cancel()
	if err := wsjson.Write(ctx, a.c, wire.ClientMsg{Type: "stdin", Data: []byte(s)}); err != nil {
		a.t.Fatalf("attach write: %v", err)
	}
}

// expect asserts the next message carries exactly want.
func (a *attachConn) expect(want string) {
	a.t.Helper()
	m := a.read()
	if got := string(m.Data); got != want {
		a.t.Fatalf("attach message = %+v (data %q), want data %q", m, got, want)
	}
}

func (a *attachConn) close() {
	a.once.Do(func() {
		a.c.CloseNow()
		a.cancel()
	})
}

// echoes drives one full round trip through the attach plane: the snapshot
// the session replays on open, then a keystroke echoed back. It is the proof
// that controld's pairing, the runner's dial-back, the splice, and the
// session's hub are all working right now.
func (f *fleet) echoes(id, keys string) {
	f.t.Helper()
	a := f.attach(id, 0)
	defer a.close()
	a.expect(snapshotFor(id))
	a.stdin(keys)
	a.expect(keys)
}

// ---------------------------------------------------------------------------
// Scene 1 — criterion 5: burst over capacity queues, and drains as it frees
// ---------------------------------------------------------------------------

// TestBurstQueuesAndDrains is success criterion 5, mechanized: "Burst 10
// creates against a fleet with 4 free slots: 4 run, 6 sit visibly queued, and
// the queue drains as capacity frees — no failed creates, no lost sessions."
//
// Phases 1 and 2 are that criterion. Their 2-and-2 spread proves capacity is
// respected and never overcommitted — but at saturation it does NOT
// discriminate least-loaded from any other capacity-respecting policy: with
// four slots and ten creates, every policy fills both runners. Phase 3 is
// what actually tests §4.7's placement rule, by building the one situation
// where policies disagree — two runners with free capacity, in different
// amounts, and an empty queue.
func TestBurstQueuesAndDrains(t *testing.T) {
	f := newFleet(t)
	f.addRunner("vm-a", 2)
	f.addRunner("vm-b", 2)
	f.waitRunner("vm-a", true, 30*time.Second)
	f.waitRunner("vm-b", true, 30*time.Second)

	const total = 10
	for i := 0; i < total; i++ {
		f.create(fmt.Sprintf("burst-%02d", i))
	}

	// --- phase 1: 4 slots, 10 creates → four run, six wait, nothing fails.
	rows := f.waitSessions(90*time.Second, "4 running and 6 queued", func(rows map[string]apiSession) bool {
		c := counts(rows)
		return len(rows) == total && c["running"] == 4 && c["queued"] == 6
	})
	assertNoCasualties(t, rows)

	if a, b := liveOn(rows, "vm-a"), liveOn(rows, "vm-b"); len(a) != 2 || len(b) != 2 {
		t.Fatalf("placement spread = vm-a:%d vm-b:%d, want 2 and 2 — capacity is respected and evenly filled\n%s",
			len(a), len(b), describe(rows))
	}

	// --- phase 2: free two slots on vm-a; the queue drains into exactly those.
	destroyed := liveOn(rows, "vm-a")
	for _, id := range destroyed {
		f.delete(id)
	}

	rows = f.waitSessions(90*time.Second, "the queue to drain into the freed slots", func(rows map[string]apiSession) bool {
		c := counts(rows)
		return len(rows) == total && c["running"] == 4 && c["queued"] == 4 && c["destroyed"] == 2
	})
	assertNoCasualties(t, rows)

	if a, b := liveOn(rows, "vm-a"), liveOn(rows, "vm-b"); len(a) != 2 || len(b) != 2 {
		t.Fatalf("post-drain spread = vm-a:%d vm-b:%d, want 2 and 2\n%s", len(a), len(b), describe(rows))
	}
	for _, id := range destroyed {
		if row := rows[id]; row.State != "destroyed" {
			t.Fatalf("deleted session %s state = %q, want destroyed\n%s", id, row.State, describe(rows))
		}
		for _, live := range liveOn(rows, "vm-a") {
			if live == id {
				t.Fatalf("destroyed session %s is still counted live on vm-a", id)
			}
		}
	}

	// --- phase 3: least-loaded, actually discriminating.
	//
	// The queue has to be empty first: while anything is queued, free capacity
	// is consumed the instant it appears, so there is no way to hold two
	// runners at different free counts long enough to place against them.
	// Cancelling queued rows is the cheap way there (queued → canceled needs
	// no runner at all) and exercises that path besides.
	for id, row := range rows {
		if row.State == "queued" {
			f.delete(id)
		}
	}
	rows = f.waitSessions(60*time.Second, "the queue to empty out", func(rows map[string]apiSession) bool {
		return counts(rows)["queued"] == 0 && counts(rows)["canceled"] == 4
	})

	// A third runner joins with 2 free slots (criterion 6's join path, and the
	// only runner that will have the most free capacity in a moment), while
	// vm-a is left holding exactly one free slot.
	f.addRunner("vm-c", 2)
	f.waitRunner("vm-c", true, 30*time.Second)
	freed := liveOn(rows, "vm-a")[0]
	f.delete(freed)
	rows = f.waitSessions(60*time.Second, "vm-a to drop to one live session", func(rows map[string]apiSession) bool {
		return rows[freed].State == "destroyed" && len(liveOn(rows, "vm-a")) == 1
	})
	if got := len(liveOn(rows, "vm-c")); got != 0 {
		t.Fatalf("vm-c holds %d sessions before phase 3's first create; the queue was not empty\n%s", got, describe(rows))
	}

	// vm-a: 1 free. vm-b: 0 free. vm-c: 2 free. Least-loaded must pick vm-c —
	// first-fit, round-robin, or "whoever announced first" would all pick
	// vm-a, so this is the assertion that tells them apart.
	mostFree := f.create("least-loaded-most-free")
	rows = f.waitSessions(60*time.Second, "the most-free placement to land", func(rows map[string]apiSession) bool {
		return rows[mostFree.ID].Runner != ""
	})
	if got := rows[mostFree.ID].Runner; got != "vm-c" {
		t.Fatalf("placed on %q with vm-a at 1 free and vm-c at 2 free; least-loaded must pick vm-c\n%s",
			got, describe(rows))
	}

	// Now vm-a and vm-c are both at exactly 1 free, so the tie-break decides:
	// lexicographically smaller name (§4.7, deterministic for exactly this
	// reason). vm-a, not vm-c.
	tieBreak := f.create("least-loaded-tie-break")
	rows = f.waitSessions(60*time.Second, "the tie-broken placement to land", func(rows map[string]apiSession) bool {
		return rows[tieBreak.ID].Runner != ""
	})
	if got := rows[tieBreak.ID].Runner; got != "vm-a" {
		t.Fatalf("placed on %q with vm-a and vm-c both at 1 free; the name tie-break must pick vm-a\n%s",
			got, describe(rows))
	}
	assertNoCasualties(t, rows)
}

// assertNoCasualties pins criterion 5's "no failed creates, no lost sessions":
// nothing may end up failed or dead, and nothing may carry an error.
func assertNoCasualties(t *testing.T, rows map[string]apiSession) {
	t.Helper()
	for _, r := range rows {
		if r.State == "failed" || r.State == "dead" {
			t.Fatalf("session %s (%s) is %s: %q\n%s", r.ID, r.Name, r.State, r.Error, describe(rows))
		}
		if r.Error != "" {
			t.Fatalf("session %s (%s) carries error %q\n%s", r.ID, r.Name, r.Error, describe(rows))
		}
	}
}

// ---------------------------------------------------------------------------
// Scene 2 — criterion 2: controld dies mid-attach; nothing else moves
// ---------------------------------------------------------------------------

// TestControldRestartMidSession is success criterion 2: "Kill controld
// mid-attach; restart it; rainier attach reconnects to the same session with
// full scrollback. The agent process never noticed."
//
// Attach downtime is expected and asserted (design §4.8 accepts it). Session
// state loss is the failure this scene is looking for: the runner keeps
// running the session throughout, and the new controld — a different Server
// object over the same store and listen address — must adopt it back
// unchanged on reconcile rather than deciding it is dead.
func TestControldRestartMidSession(t *testing.T) {
	f := newFleet(t)
	f.addRunner("vm-a", 2)
	f.waitRunner("vm-a", true, 30*time.Second)

	created := f.create("survives-controld")
	f.waitSessions(60*time.Second, "the session to reach running", func(rows map[string]apiSession) bool {
		return rows[created.ID].State == "running"
	})

	// Attached and talking, right up to the moment controld dies.
	live := f.attach(created.ID, 0)
	live.expect(snapshotFor(created.ID))
	live.stdin("before")
	live.expect("before")

	f.cd.stop()

	// The attach dies with controld — accepted downtime, but an actual
	// teardown, not a socket that quietly stops answering. Our own read
	// deadline expiring would be the latter, so it is called out separately:
	// a client left holding a half-open terminal has no way to know it should
	// re-attach.
	if _, err := live.readErr(15 * time.Second); err == nil {
		t.Fatal("the attach outlived controld; it should have been torn down with it")
	} else if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("the attach socket went quiet instead of being closed when controld died")
	}
	live.close()

	// A NEW controld, same store, same address: the runner's own dial loop
	// finds it without being told anything.
	f.restartControld()
	f.waitRunner("vm-a", true, 60*time.Second)

	// A fresh attach reaches the same session, replaying its scrollback and
	// echoing again — the agent process never noticed. This also orders what
	// follows: the dial_attach that made it work was served by the same
	// connection goroutine that runs reconcile before it ever reads a
	// command, so the row check below is necessarily post-reconcile.
	f.echoes(created.ID, "after")

	row := f.list()[created.ID]
	if row.State != "running" || row.Runner != "vm-a" || row.Error != "" {
		t.Fatalf("session after controld restart = %+v, want state=running runner=vm-a error=\"\"", row)
	}
	if !row.Reachable {
		t.Fatalf("session after controld restart is not reachable: %+v", row)
	}
}

// ---------------------------------------------------------------------------
// Scene 3 — criterion 3: runnerd dies; no container is destroyed
// ---------------------------------------------------------------------------

// TestRunnerRestartReRegisters is success criterion 3: "Kill runnerd on a VM
// with live sessions; restart it; sessions re-register and are attachable. No
// container is destroyed."
//
// The fake driver is the VM's container set: the restarted runnerd is handed
// the very same driver, so Recover sees exactly the containers that outlived
// the process — and the announce built from it must true controld's rows up
// without marking anything dead.
func TestRunnerRestartReRegisters(t *testing.T) {
	f := newFleet(t)
	rn := f.addRunner("vm-a", 2)
	f.waitRunner("vm-a", true, 30*time.Second)

	created := f.create("survives-runnerd")
	f.waitSessions(60*time.Second, "the session to reach running", func(rows map[string]apiSession) bool {
		return rows[created.ID].State == "running"
	})
	f.echoes(created.ID, "before")

	before := rn.containers(t)
	if len(before) != 1 || before[0] != created.ID {
		t.Fatalf("containers before the restart = %v, want just %s", before, created.ID)
	}

	// Kill runnerd. The container survives it, and so must the row.
	f.stopRunner("vm-a")
	f.waitRunner("vm-a", false, 30*time.Second)
	f.dropSessiond(created.ID) // its conn died with runnerd; the real one redials

	rows := f.list()
	if row := rows[created.ID]; row.State != "running" || row.Reachable {
		t.Fatalf("session while its runner is down = %+v, want state=running reachable=false", row)
	}

	// Same driver, new process: Recover + announce.
	restarted := f.restartRunner("vm-a")
	f.waitRunner("vm-a", true, 60*time.Second)

	rows = f.waitSessions(60*time.Second, "the session to be reachable again", func(rows map[string]apiSession) bool {
		return rows[created.ID].State == "running" && rows[created.ID].Reachable
	})
	if row := rows[created.ID]; row.Runner != "vm-a" || row.Error != "" {
		t.Fatalf("session after the runner restart = %+v, want runner=vm-a error=\"\"", row)
	}
	if after := restarted.containers(t); len(after) != 1 || after[0] != created.ID {
		t.Fatalf("containers after the restart = %v, want just %s (nothing may be destroyed)", after, created.ID)
	}

	// And it is attachable again, through the re-registered sessiond.
	f.echoes(created.ID, "after")
}

// ---------------------------------------------------------------------------
// Scene 4 — §4.8's terminal-row-orphan rule
// ---------------------------------------------------------------------------

// TestDeleteDisconnectedRunner pins the last row of the reconciliation table
// (design §4.8): a session destroyed while its runner was away leaves a
// container nobody asked for, and the announce that brings that container back
// into view must be answered with a destroy.
//
// Deleting a session whose runner has no control connection is the ordinary
// path for it — controld has nothing to dispatch to, so it marks the row
// destroyed and relies on reconciliation to collect the container later
// (api.go's handleDeleteSession says exactly this).
func TestDeleteDisconnectedRunner(t *testing.T) {
	f := newFleet(t)
	rn := f.addRunner("vm-a", 2)
	f.waitRunner("vm-a", true, 30*time.Second)

	created := f.create("orphan-to-be")
	f.waitSessions(60*time.Second, "the session to reach running", func(rows map[string]apiSession) bool {
		return rows[created.ID].State == "running"
	})

	f.stopRunner("vm-a")
	f.waitRunner("vm-a", false, 30*time.Second)
	f.dropSessiond(created.ID)

	// rm while the runner is down: the row goes terminal even though the
	// container is still out there.
	f.delete(created.ID)
	if row := f.list()[created.ID]; row.State != "destroyed" {
		t.Fatalf("session deleted while its runner was down = %+v, want state=destroyed", row)
	}
	if got := rn.containers(t); len(got) != 1 {
		t.Fatalf("containers on the down runner = %v, want the orphan to still exist", got)
	}

	// The runner comes back announcing a session the store has already
	// finished. controld must tell it to destroy it.
	restarted := f.restartRunner("vm-a")
	f.waitRunner("vm-a", true, 60*time.Second)
	waitUntil(t, 60*time.Second, "the orphan container to be destroyed", func() bool {
		return len(restarted.containers(t)) == 0
	})

	if row := f.list()[created.ID]; row.State != "destroyed" {
		t.Fatalf("session after the orphan was collected = %+v, want it still destroyed", row)
	}
}

// handleFor returns the driver handle the runner holds for session id, so a
// scene can reach past runnerd and do to a container what a crash would.
func (n *runnerNode) handleFor(t *testing.T, id string) string {
	t.Helper()
	listed, err := n.drv.List(context.Background())
	if err != nil {
		t.Fatalf("driver.List: %v", err)
	}
	for _, l := range listed {
		if l.SessionID == id {
			return l.Handle.ID
		}
	}
	t.Fatalf("no driver handle for session %s: %+v", id, listed)
	return ""
}

// ---------------------------------------------------------------------------
// Scene 4b — Plan 5's durability rider: a crash keeps the workspace
// ---------------------------------------------------------------------------

// TestCrashKeepsTheWorkspaceAndRmReclaimsIt is the whole of the rider in one
// scene, because its two halves only mean something together and neither
// component can see both.
//
// A container dying is not a user throwing their work away. Everything a
// session has produced lives on its workspace volume, and a crashed session is
// precisely the one people come back for — so runnerd's crash path removes the
// container (the slot must come back) and leaves the volume standing.
//
// That is only safe because of the second half: the volume then has NOTHING
// left on the host that names it, so if an explicit rm did not reclaim it,
// "a crash preserves the workspace" would read on disk as "every crash leaks
// one, forever". controld sends remove_workspace on every rm, terminal rows
// included, and that is what this scene follows all the way to the driver.
func TestCrashKeepsTheWorkspaceAndRmReclaimsIt(t *testing.T) {
	f := newFleet(t)
	rn := f.addRunner("vm-a", 2)
	f.waitRunner("vm-a", true, 30*time.Second)

	created := f.create("crashes")
	f.waitSessions(60*time.Second, "the session to reach running", func(rows map[string]apiSession) bool {
		return rows[created.ID].State == "running"
	})
	volume := "rainier-ws-" + created.ID
	if !slices.Contains(rn.drv.Volumes(), volume) {
		t.Fatalf("session %s came up with no workspace volume: %v", created.ID, rn.drv.Volumes())
	}

	// The crash, modeled as the host actually presents one: the container's
	// process died, so the container is no longer RUNNING — but its record and
	// its volume are both still sitting there, which is what `docker ps -a`
	// shows for an exited container and what runnerd's Inspect will find. (A
	// container already removed out from under runnerd would be a weaker
	// scene: there would then be no label left to derive a volume name from,
	// so even a buggy whole-teardown would leave the volume alone by accident.)
	handle := rn.handleFor(t, created.ID)
	if err := rn.drv.Suspend(context.Background(), handle, false); err != nil {
		t.Fatal(err)
	}
	f.dropSessiond(created.ID)

	// waitUntil, not waitSessions: the latter boots a scripted sessiond for
	// every running row on each pass, which would re-register this one
	// mid-crash and heal exactly what the scene is trying to observe.
	waitUntil(t, 60*time.Second, "controld to mark the crashed session dead", func() bool {
		return f.list()[created.ID].State == "dead"
	})
	if got := rn.containers(t); len(got) != 0 {
		t.Fatalf("containers after the crash = %v, want none — the slot must come back", got)
	}
	if !slices.Contains(rn.drv.Volumes(), volume) {
		t.Fatalf("the crash took %s with it; a crashed session's workspace is what a user comes back for", volume)
	}

	// The rm. The row is already terminal, so this is the idempotent 204 path
	// — and it is the ONLY thing left that knows this volume exists.
	f.delete(created.ID)
	waitUntil(t, 60*time.Second, "the explicit rm to reclaim the workspace", func() bool {
		return !slices.Contains(rn.drv.Volumes(), volume)
	})
	if row := f.list()[created.ID]; row.State != "dead" {
		t.Fatalf("session after the rm = %+v, want it still dead — reclaiming a volume is not a state change", row)
	}
}

// ---------------------------------------------------------------------------
// Scene 4b — Plan 5 rider: `attach --since 0` replays the whole log
// ---------------------------------------------------------------------------

// TestFullReplayReachesTheViewer is the plan's Step 1 scene for the `--since`
// bug the Plan 3 overnight run found: three attempts at
// `rainier attach <id> --since 0` rendered a screen snapshot and nothing
// else, while `docker exec` into the container showed the event log intact and
// contiguous, seq 1 → 756. Nothing was wrong with the log; the request never
// arrived as the request that was typed.
//
// Two things had to be true for that, and this scene is the integration half
// of both (the unit halves are attachio's TestRunDialsWithTheCursor and
// session's TestSinceAllReplaysTheWholeLog):
//
//   - The cursor has to survive the trip. It crosses the attach query string,
//     controld's dial_attach (rwire.Attach.Since), runnerd's hub, and
//     relay.Frame.Since — which is `json:"s,omitempty"`, so a cursor of 0 is
//     literally absent from that hop's bytes. This scene asserts the value
//     the "container" was opened with, not just what came back.
//   - `--since 0` has to mean the whole log, while a plain attach still means
//     the current screen. They are opposite requests and the flag's zero
//     value cannot tell them apart, so both halves are checked here in the
//     spelling the CLI produces (attachio.Cursor).
//
// The sessiond here is scripted, so what it replays is this file's rule and
// not sessiond's — mirroring it deliberately (openFrames), because what this
// scene is qualified to prove is that the request reaches the container
// intact and 50 frames make it back out to a viewer.
func TestFullReplayReachesTheViewer(t *testing.T) {
	f := newFleet(t)
	f.addRunner("vm-a", 2)
	f.waitRunner("vm-a", true, 30*time.Second)

	created := f.create("replays")
	f.waitSessions(60*time.Second, "the session to reach running", func(rows map[string]apiSession) bool {
		return rows[created.ID].State == "running"
	})

	const events = 50
	ss := f.sessiond(created.ID)
	ss.script(events)

	// A plain `rainier attach` (no --since) still opens on the screen: a
	// viewer that asked for no history must not be handed a day of it.
	plain := f.attach(created.ID, attachio.Cursor(false, 0))
	plain.expect(snapshotFor(created.ID))
	plain.close()

	// `rainier attach --since 0`: every event, in order, first to last.
	full := f.attach(created.ID, attachio.Cursor(true, 0))
	for i := 1; i <= events; i++ {
		m := full.read()
		want := fmt.Sprintf("line-%d\n", i)
		if m.Type != "output" || m.Seq != uint64(i) || string(m.Data) != want {
			t.Fatalf("replayed frame %d = %+v (data %q), want an output frame seq=%d data=%q",
				i, m, string(m.Data), i, want)
		}
	}
	full.close()

	// And the cursor the container was actually opened with is the one the
	// CLI spelled — the assertion that would have caught the omitempty hop
	// eating it, or controld dropping it out of dial_attach.
	got := ss.cursors()
	if len(got) != 2 || got[0] != 0 || got[1] != wire.SinceAll {
		t.Fatalf("cursors the session was attached with = %v, want [0 %d] (plain attach, then --since 0)",
			got, wire.SinceAll)
	}
}

// ---------------------------------------------------------------------------
// Scene 5 — Plan 4 criterion 2: setup runs once, then the cache boots
// ---------------------------------------------------------------------------

// buildEnvCache drives one environment from "never built" to "cached", and
// returns the runner holding the cache together with the environment row as
// the store has it afterwards. It is the shared opening of the two caching
// scenes: create a session on the environment, let it come up, play the setup
// script's success from inside the container, and wait for controld's
// orchestration to commit the image and record the ref.
//
// The two assertions on the way through belong here rather than in a caller
// because they are the precondition both scenes rest on: if the FIRST create
// didn't carry the environment's own image and script, "the second one
// didn't" would prove nothing at all.
func (f *fleet) buildEnvCache(env apiEnvironment, sessionName string) (runner string, cached apiEnvironment) {
	f.t.Helper()

	first := f.createFrom(sessionName, env.Name)
	rows := f.waitSessions(90*time.Second, "the first session on "+env.Name+" to come up", func(rows map[string]apiSession) bool {
		return rows[first.ID].State == "running"
	})
	runner = rows[first.ID].Runner

	spec := f.runners[runner].drv.LastSpec()
	if spec.Image != env.Image || spec.Setup != env.Setup {
		f.t.Fatalf("first create on %s dispatched image=%q setup=%q; want the environment's own image %q and its script %q — nothing has run it yet, so there is no cache to boot from",
			runner, spec.Image, spec.Setup, env.Image, env.Setup)
	}

	// The container reports the script finished; that is the news controld
	// snapshots on (design §4.3), and the session's own state is unaffected.
	f.sessiond(first.ID).control(f.t, relay.ControlEvent{Kind: "setup_done"})
	cached = f.waitEnv(env.ID, 90*time.Second, "environment "+env.Name+" to cache a snapshot", func(e apiEnvironment) bool {
		return e.SnapshotRef != ""
	})
	if row := f.list()[first.ID]; row.State != "running" {
		f.t.Fatalf("session %s is %q after its setup finished; a setup outcome is news about the ENVIRONMENT, not about the session", first.ID, row.State)
	}
	return runner, cached
}

// TestEnvSetupStreamsAndCaches is Plan 4 success criterion 2, mechanized:
// "The FIRST session on an environment runs setup live [...]; a snapshot is
// cached; the SECOND session boots from cache with no setup run."
//
// The timing half of that criterion belongs on real hardware — a fake driver
// costs nothing to "build" — so what this scene pins is the part timings can
// only hint at: WHAT was dispatched each time. The first create carries the
// environment's image and its script; the snapshot lands under the
// content-addressed ref every replica derives independently; the second create
// carries that ref and no script at all, and lands on the runner that holds it
// (v0 has no registry, so the image exists nowhere else).
func TestEnvSetupStreamsAndCaches(t *testing.T) {
	f := newFleet(t)
	f.addRunner("vm-a", 2)
	f.addRunner("vm-b", 2)
	f.waitRunner("vm-a", true, 30*time.Second)
	f.waitRunner("vm-b", true, 30*time.Second)

	f.putSecret("ENV_TOKEN", "toolchain-secret")
	env := f.createEnv(map[string]any{
		"name":              "toolchain",
		"image":             "e2e-image",
		"setup":             "install-toolchain v1",
		"setup_timeout_sec": 120,
		"secret_refs":       []string{"ENV_TOKEN"},
	})

	holder, cached := f.buildEnvCache(env, "first-on-toolchain")

	wantRef := snapshotRefFor(t, env)
	if cached.SnapshotRef != wantRef || cached.SnapshotRunner != holder || cached.SnapshotHash != env.SetupHash {
		t.Fatalf("cached environment = ref %q on runner %q built from hash %q; want ref %q on %q from %q",
			cached.SnapshotRef, cached.SnapshotRunner, cached.SnapshotHash, wantRef, holder, env.SetupHash)
	}
	// The environment's own timeout travels with the script — controld's
	// default only fills in for an environment that declares none.
	if got := f.runners[holder].drv.LastSpec().SetupTimeoutSec; got != 120 {
		t.Fatalf("first create dispatched setup_timeout_sec = %d, want the environment's 120", got)
	}

	// The snapshot command carried the strip list: everything the create
	// injected (this environment's decrypted secret) plus everything the
	// driver injects — the setup channel, the build session's own identity,
	// and the egress proxy vars, whose URLs embed that session id. Without it
	// the committed image would hand every later session that secret in its
	// config, re-run the very setup the cache exists to skip, and carry the
	// build session's id forward forever. runnerd composes the list from what
	// it recorded at create; this is the scene where that survives a real
	// dispatch over a real websocket.
	strips := f.runners[holder].drv.Strips()
	if len(strips) != 1 {
		t.Fatalf("driver saw %d snapshot(s) on %s, want exactly one: %v", len(strips), holder, strips)
	}
	wantStrip := []string{
		"ENV_TOKEN",
		"RAINIER_SETUP_B64", "RAINIER_SETUP_TIMEOUT",
		"RAINIER_REPOS_B64", "RAINIER_INIT_B64", "RAINIER_INIT_TIMEOUT",
		"RAINIER_GIT_AUTHOR_NAME", "RAINIER_GIT_AUTHOR_EMAIL",
		"RAINIER_DIAL", "RAINIER_SESSION",
		"HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy", "NO_PROXY", "no_proxy",
	}
	if !slices.Equal(strips[0], wantStrip) {
		t.Fatalf("snapshot strip list = %v, want %v", strips[0], wantStrip)
	}

	// The rest of the fleet is warmed with that very ref. This is the only
	// independent look at what actually went over the wire: the ref controld
	// recorded and the one it dispatched to the holder are one variable, but
	// the prepull is a separate message to a separate runner.
	other := "vm-b"
	if holder == "vm-b" {
		other = "vm-a"
	}
	waitUntil(t, 60*time.Second, "the prepull broadcast to reach "+other, func() bool {
		return slices.Contains(f.runners[other].drv.Pulls(), wantRef)
	})
	if pulled := f.runners[holder].drv.Pulls(); slices.Contains(pulled, wantRef) {
		t.Fatalf("the snapshot holder %s was told to prepull %q, an image only it has: %v", holder, wantRef, pulled)
	}

	// --- the second session: cache hit, no setup, and on the holder.
	second := f.createFrom("second-on-toolchain", env.Name)
	rows := f.waitSessions(90*time.Second, "the second session to boot from the cache", func(rows map[string]apiSession) bool {
		return rows[second.ID].State == "running"
	})
	if got := rows[second.ID].Runner; got != holder {
		t.Fatalf("second session placed on %q, want the snapshot holder %q — with no registry in v0 the cached image exists nowhere else\n%s",
			got, holder, describe(rows))
	}
	spec := f.runners[holder].drv.LastSpec()
	if spec.Setup != "" {
		t.Fatalf("second create dispatched setup %q; the cached image IS the finished setup", spec.Setup)
	}
	if spec.Image != wantRef {
		t.Fatalf("second create dispatched image %q, want the cached snapshot %q", spec.Image, wantRef)
	}
	if got := rows[second.ID].Image; got != wantRef {
		t.Fatalf("second session's view reports image %q, want the resolved %q — the API must show what the session actually runs", got, wantRef)
	}
	if got := rows[second.ID].Environment; got != env.Name {
		t.Fatalf("second session's view reports environment %q, want %q", got, env.Name)
	}

	// --- the agent inside that session finishes.
	//
	// child_exited is the one fact about a session only the container knows,
	// and this is the whole of its journey in one place: sessiond's control
	// frame → runnerd's event → controld's column → the session view every
	// `rainier ls` renders. Each component's own tests can prove its hop; only
	// here does the number actually arrive.
	//
	// Exit 0 deliberately, because it is the case a `!= 0` check anywhere on
	// that path would silently swallow — and the session must stay RUNNING
	// under it: the agent finishing is not the session ending, which is what
	// makes the container still attachable afterwards.
	if got := rows[second.ID].ChildExitCode; got != nil {
		t.Fatalf("session reports child_exit_code %d before its agent exited, want null", *got)
	}
	f.sessiond(second.ID).control(t, relay.ControlEvent{Kind: "child_exited", RC: 0})
	rows = f.waitSessions(60*time.Second, "the agent's exit code to reach the session view", func(rows map[string]apiSession) bool {
		return rows[second.ID].ChildExitCode != nil
	})
	if got := *rows[second.ID].ChildExitCode; got != 0 {
		t.Fatalf("child_exit_code = %d, want 0", got)
	}
	if got := rows[second.ID].State; got != "running" {
		t.Fatalf("session is %q after its agent exited, want running — the container stays up for viewers", got)
	}
	f.echoes(second.ID, "still here")
}

// ---------------------------------------------------------------------------
// Scene 6 — Plan 4 criterion 2's other half: an edit invalidates the cache
// ---------------------------------------------------------------------------

// TestEnvEditInvalidatesCache pins the cache's invalidation rule (design
// §4.3): the snapshot is a cache keyed by the content hash of (image, setup),
// so editing either makes the stored snapshot stale — visibly, not silently.
// The edited environment keeps its old ref and old hash on the row, and the
// next session is dispatched with the NEW script over the plain image.
//
// The middle create is what makes the last one mean anything: without it,
// "the third session ran setup" would be equally consistent with a cache that
// never worked.
func TestEnvEditInvalidatesCache(t *testing.T) {
	f := newFleet(t)
	f.addRunner("vm-a", 3)
	f.waitRunner("vm-a", true, 30*time.Second)

	env := f.createEnv(map[string]any{
		"name":  "toolchain",
		"image": "e2e-image",
		"setup": "install-toolchain v1",
	})

	holder, cached := f.buildEnvCache(env, "before-edit")
	wantRef := snapshotRefFor(t, env)
	if cached.SnapshotRef != wantRef {
		t.Fatalf("cached environment ref = %q, want %q", cached.SnapshotRef, wantRef)
	}

	// The cache is live: a session created now boots from it, no script.
	hit := f.createFrom("cache-hit", env.Name)
	f.waitSessions(90*time.Second, "the cache-hit session to come up", func(rows map[string]apiSession) bool {
		return rows[hit.ID].State == "running"
	})
	if spec := f.runners[holder].drv.LastSpec(); spec.Setup != "" || spec.Image != wantRef {
		t.Fatalf("the cache-hit create dispatched image=%q setup=%q, want image=%q and no setup", spec.Image, spec.Setup, wantRef)
	}

	// --- the edit.
	edited := f.patchEnv(env.ID, map[string]any{"setup": "install-toolchain v2"})
	if edited.SetupHash == env.SetupHash {
		t.Fatalf("editing the setup script left setup_hash at %q; the hash is the cache key", edited.SetupHash)
	}
	if edited.SnapshotRef != wantRef || edited.SnapshotHash != env.SetupHash {
		t.Fatalf("the edit moved the snapshot columns to ref %q / hash %q; they belong to the store and must stay put, reading stale (%q from %q)",
			edited.SnapshotRef, edited.SnapshotHash, wantRef, env.SetupHash)
	}

	third := f.createFrom("after-edit", env.Name)
	f.waitSessions(90*time.Second, "the post-edit session to come up", func(rows map[string]apiSession) bool {
		return rows[third.ID].State == "running"
	})
	spec := f.runners[holder].drv.LastSpec()
	if spec.Setup != "install-toolchain v2" {
		t.Fatalf("post-edit create dispatched setup %q, want the edited script", spec.Setup)
	}
	if spec.Image != env.Image {
		t.Fatalf("post-edit create dispatched image %q, want the environment's plain image %q — the stale snapshot must not be booted", spec.Image, env.Image)
	}
}

// ---------------------------------------------------------------------------
// Scene 7 — design §4.3's failure path: a setup that fails fails the session
// ---------------------------------------------------------------------------

// TestSetupFailureLandsFailed pins the seam three components share when a
// setup script exits non-zero: sessiond reports the rc and the tail of what
// the script printed, runnerd composes them into one sentence ("rc 7: boom"),
// and controld prefixes its own words and lands the whole thing in the
// session's error column, which the API hands straight back.
//
// All three pieces are asserted at once, from the outside, because that is the
// only place they are ever seen together — each component's own tests can only
// prove its half of the sentence.
func TestSetupFailureLandsFailed(t *testing.T) {
	f := newFleet(t)
	f.addRunner("vm-a", 2)
	f.waitRunner("vm-a", true, 30*time.Second)

	env := f.createEnv(map[string]any{
		"name":  "broken",
		"image": "e2e-image",
		"setup": "exit 7",
	})
	created := f.createFrom("setup-fails", env.Name)
	f.sessiond(created.ID).control(t, relay.ControlEvent{Kind: "setup_failed", RC: 7, Tail: "boom"})

	rows := f.waitSessions(90*time.Second, "the session to land failed", func(rows map[string]apiSession) bool {
		return rows[created.ID].State == "failed"
	})
	got := rows[created.ID].Error
	for _, want := range []string{"setup failed", "rc 7", "boom"} {
		if !strings.Contains(got, want) {
			t.Fatalf("failed session's error = %q, want it to carry %q — a user whose session never came up has nothing else to go on", got, want)
		}
	}

	// A failed setup caches nothing: the image it would publish is of a build
	// that didn't work.
	if e := f.getEnv(env.ID); e.SnapshotRef != "" {
		t.Fatalf("environment cached %q from a session whose setup failed", e.SnapshotRef)
	}

	// And the session is still attachable, which is the whole point of leaving
	// the container up: the error column holds 2KB of tail, and everything
	// else the script printed is in the scrollback on the other side of this
	// attach (design §4.3). Terminal, and still reachable through the full
	// plane — controld's pairing, the runner's dial-back, the session's hub.
	f.echoes(created.ID, "reading the log")

	// Reading it changed nothing: the row is still failed, still carrying its
	// diagnosis, and still holding its slot until an explicit rm.
	if row := f.list()[created.ID]; row.State != "failed" || row.Error != got {
		t.Fatalf("session after attaching to it = %s / %q, want it unchanged", row.State, row.Error)
	}
}

// ---------------------------------------------------------------------------
// Scene 8 — Plan 4 criterion 4: a secret reaches the container, and only it
// ---------------------------------------------------------------------------

// TestSecretsReachSpec is criterion 4: "Env-declared secrets are injected as
// env vars into sessions of that env, stored encrypted in Postgres, never
// readable via the API after write."
//
// The value makes one round trip through the whole system in this scene —
// sealed by controld's PUT, stored as ciphertext, decrypted at dispatch, and
// asserted where the container would read it (the driver's Spec.Env). The
// second half is the "never readable" clause, checked the blunt way: the raw
// listing body must name the secret and must not contain its value anywhere.
func TestSecretsReachSpec(t *testing.T) {
	const secretName = "E2E_TOKEN"
	const secretValue = "s3cr3t-e2e-value"

	f := newFleet(t)
	f.addRunner("vm-a", 1)
	f.waitRunner("vm-a", true, 30*time.Second)

	f.putSecret(secretName, secretValue)
	env := f.createEnv(map[string]any{
		"name":        "with-secrets",
		"image":       "e2e-image",
		"secret_refs": []string{secretName},
	})

	created := f.createFrom("needs-token", env.Name)
	f.waitSessions(90*time.Second, "the session to come up", func(rows map[string]apiSession) bool {
		return rows[created.ID].State == "running"
	})

	spec := f.runners["vm-a"].drv.LastSpec()
	if got := spec.Env[secretName]; got != secretValue {
		t.Fatalf("dispatched Spec.Env[%q] = %q, want the decrypted secret value", secretName, got)
	}

	var body json.RawMessage
	if err := f.client().Do(http.MethodGet, "/v1/secrets", nil, &body); err != nil {
		t.Fatalf("GET /v1/secrets: %v", err)
	}
	if !strings.Contains(string(body), secretName) {
		t.Fatalf("the secret listing does not name %s: %s", secretName, body)
	}
	if strings.Contains(string(body), secretValue) {
		t.Fatal("the secret listing carries the secret's VALUE; it is write-only at this API")
	}
}

// ---------------------------------------------------------------------------
// Scene 9 — Plan 4 criterion 5: a placement pin queues visibly, then places
// ---------------------------------------------------------------------------

// TestPlacementPinQueuesWithReason is criterion 5: "An environment with
// placement: rainier-1 always places there; placement to a non-existent runner
// queues with a visible reason."
//
// Both halves are one scene deliberately. The queue half alone is satisfiable
// by a fleet with no capacity at all; what makes the pin a pin is that a
// runner with room sits right there and is passed over — and that the session
// places the moment the runner it actually named turns up, with no create
// retried and nothing else touched.
func TestPlacementPinQueuesWithReason(t *testing.T) {
	f := newFleet(t)
	f.addRunner("vm-a", 2)
	f.waitRunner("vm-a", true, 30*time.Second)

	env := f.createEnv(map[string]any{
		"name":      "gpu-box",
		"image":     "e2e-image",
		"placement": "vm-gpu", // a runner that has not joined the fleet
	})
	created := f.createFrom("pinned", env.Name)

	rows := f.waitSessions(60*time.Second, "the pinned session to explain itself", func(rows map[string]apiSession) bool {
		return rows[created.ID].QueueReason != ""
	})
	row := rows[created.ID]
	if row.State != "queued" || row.Runner != "" {
		t.Fatalf("pinned session = %+v, want it queued and unplaced: vm-a has two free slots, but the pin is not for it\n%s", row, describe(rows))
	}
	if !strings.Contains(row.QueueReason, "vm-gpu") {
		t.Fatalf("queue_reason = %q, want it to name the runner the session is waiting for (vm-gpu)", row.QueueReason)
	}

	// The pinned runner joins. Nothing else changes — no controld config, no
	// re-create, no nudge.
	f.addRunner("vm-gpu", 1)
	f.waitRunner("vm-gpu", true, 30*time.Second)

	rows = f.waitSessions(90*time.Second, "the pinned session to place once its runner joins", func(rows map[string]apiSession) bool {
		return rows[created.ID].State == "running"
	})
	row = rows[created.ID]
	if row.Runner != "vm-gpu" {
		t.Fatalf("pinned session placed on %q, want vm-gpu\n%s", row.Runner, describe(rows))
	}
	if row.QueueReason != "" {
		t.Fatalf("a placed session still carries queue_reason %q", row.QueueReason)
	}
	if held := f.runners["vm-a"].containers(t); len(held) != 0 {
		t.Fatalf("vm-a holds %v; a pinned session must wait for its runner, never fall back to the fleet", held)
	}
}

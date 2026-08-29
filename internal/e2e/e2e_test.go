// Package e2e is Rainier's in-process end-to-end suite: a real controld on a
// real TCP listener (memstore by default, pgstore when RAINIER_TEST_PG_DSN is
// set), real runnerd agents dialing it over real websockets with fake
// drivers, scripted sessionds standing in for containers, and a real HTTP
// client driving the REST API. Nothing here is mocked except the container
// runtime (driver.Fake) and GitHub's /user.
//
// The scenes are the design's chaos list (§7.4) and map 1:1 onto its success
// criteria (§1):
//
//	TestBurstQueuesAndDrains     criterion 5 (burst over capacity queues, drains)
//	TestControldRestartMidSession criterion 2 (controld dies mid-attach; nothing else moves)
//	TestRunnerRestartReRegisters  criterion 3 (runnerd dies; no container destroyed)
//	TestDeleteDisconnectedRunner  §4.8's terminal-row-orphan rule
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
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/jackc/pgx/v5"

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

	// pollInterval is how often every bounded wait re-checks its condition.
	pollInterval = 20 * time.Millisecond
	// wsReadLimit matches controld's, runnerd's, and sessiond's own limits.
	wsReadLimit = 16 << 20
)

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
	ctx := context.Background()

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
		if c, err := pgx.Connect(ctx, dsn); err == nil {
			c.Exec(ctx, "DROP DATABASE IF EXISTS "+name)
			c.Close(ctx)
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
			ss.send(ctx, f.AttachID, wire.ServerMsg{Type: "snapshot", Seq: 1, Data: []byte(snapshotFor(ss.id))})
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
	State     string `json:"state"`
	Runner    string `json:"runner"`
	Reachable bool   `json:"reachable"`
	Error     string `json:"error"`
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
// Two runners of two slots each also make the placement assertion meaningful:
// least-loaded with a name tie-break (§4.7) has to spread the four across both
// runners, not stack them on whichever answered first.
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

	// 4 slots, 10 creates: four reach running, six wait. Nothing fails.
	rows := f.waitSessions(90*time.Second, "4 running and 6 queued", func(rows map[string]apiSession) bool {
		c := counts(rows)
		return len(rows) == total && c["running"] == 4 && c["queued"] == 6
	})
	assertNoCasualties(t, rows)

	if a, b := liveOn(rows, "vm-a"), liveOn(rows, "vm-b"); len(a) != 2 || len(b) != 2 {
		t.Fatalf("placement spread = vm-a:%d vm-b:%d, want 2 and 2 (least-loaded)\n%s", len(a), len(b), describe(rows))
	}

	// Free two slots on vm-a. The queue must drain into exactly those.
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

	// The attach dies with controld — accepted downtime, not a silent hang.
	if _, err := live.readErr(15 * time.Second); err == nil {
		t.Fatal("the attach outlived controld; it should have been torn down with it")
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

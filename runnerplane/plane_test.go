// runnerplane/plane_test.go
package runnerplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// ---------------------------------------------------------------------------
// the fake host: an in-memory fleet repository, a fleet service that accepts
// registrations and answers reconcile with a destroy list, and the two hooks
// (Aside, SessionRequest) the plane hands its host.
// ---------------------------------------------------------------------------

const (
	testWorkspace control.WorkspaceID = "ws_test"
	testPool      control.PoolID      = "pool_test"
	testToken                         = "rnr_test_runner_token"
)

// fakeRepo is control.FleetRepository over a map, plus the generation mint a
// host's NextGeneration answers from. Both live in one place deliberately:
// the generation is the STORE's, so a second Plane over the same fakeRepo is
// exactly a controld restart over the same database.
type fakeRepo struct {
	mu      sync.Mutex
	runners map[control.RunnerID]*control.Runner

	// Hooks for the redial race. Set before the plane serves anything.
	beforeDisconnect func()
	afterDisconnect  func()
	afterUpsert      func(control.Runner)
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{runners: map[control.RunnerID]*control.Runner{}}
}

// next mints the runner's next generation, creating the row if this is the
// first time the fleet has heard of it (memstore's NextRunnerGeneration).
func (r *fakeRepo) next(id control.RunnerID) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	run, ok := r.runners[id]
	if !ok {
		run = &control.Runner{ID: id, PoolID: testPool}
		r.runners[id] = run
	}
	run.Generation++
	return run.Generation
}

func (r *fakeRepo) generation(id control.RunnerID) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if run, ok := r.runners[id]; ok {
		return run.Generation
	}
	return 0
}

// UpsertRunner fences on the stored generation exactly as the store does: a
// write from a superseded connection changes nothing at all.
func (r *fakeRepo) UpsertRunner(ctx context.Context, pool control.PoolID, run control.Runner) error {
	if pool == "" {
		return control.ErrInvalid
	}
	r.mu.Lock()
	if cur, ok := r.runners[run.ID]; ok && cur.Generation > run.Generation {
		r.mu.Unlock()
		return control.ErrStale
	}
	cp := run
	cp.PoolID = pool
	cp.Capabilities = append([]string(nil), run.Capabilities...)
	r.runners[run.ID] = &cp
	hook := r.afterUpsert
	r.mu.Unlock()
	if hook != nil {
		hook(cp)
	}
	return nil
}

func (r *fakeRepo) SetRunnerConnected(ctx context.Context, pool control.PoolID, id control.RunnerID, connected bool) error {
	if pool == "" {
		return control.ErrInvalid
	}
	r.mu.Lock()
	before, after := r.beforeDisconnect, r.afterDisconnect
	r.mu.Unlock()
	if !connected && before != nil {
		before()
	}
	r.mu.Lock()
	run, ok := r.runners[id]
	if ok {
		run.Connected = connected
		run.LastSeenAt = time.Now()
	}
	r.mu.Unlock()
	if !connected && after != nil {
		after()
	}
	if !ok {
		return control.ErrNotFound
	}
	return nil
}

func (r *fakeRepo) ListRunners(ctx context.Context, pool control.PoolID) ([]control.Runner, error) {
	if pool == "" {
		return nil, control.ErrInvalid
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]control.Runner, 0, len(r.runners))
	for _, run := range r.runners {
		cp := *run
		cp.Capabilities = append([]string(nil), run.Capabilities...)
		out = append(out, cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (r *fakeRepo) SessionsOnRunner(ctx context.Context, pool control.PoolID, id control.RunnerID, states []control.SessionState) ([]control.Session, error) {
	return nil, nil
}

func (r *fakeRepo) OldestQueued(ctx context.Context, pool control.PoolID) ([]control.Session, error) {
	return nil, nil
}

// fakeFleet is control.Fleet: it accepts a registration whose generation is
// not below the store's, writes the row the registration describes, and
// answers every reconcile with the destroy list the test configured.
type fakeFleet struct {
	repo *fakeRepo

	mu       sync.Mutex
	destroy  []control.SessionID
	events   []control.RunnerEvent
	regs     []control.RunnerRegistration
	snaps    []control.RunnerSnapshot
	applyErr error
	eventCh  chan control.RunnerEvent
}

func (f *fakeFleet) RegisterRunner(ctx context.Context, reg control.RunnerRegistration) (control.RunnerRegistrationResult, error) {
	if cur := f.repo.generation(reg.RunnerID); reg.Generation < cur {
		return control.RunnerRegistrationResult{Generation: cur}, nil
	}
	f.mu.Lock()
	f.regs = append(f.regs, reg)
	f.mu.Unlock()
	err := f.repo.UpsertRunner(ctx, reg.PoolID, control.Runner{
		ID: reg.RunnerID, PoolID: reg.PoolID,
		CapacityUsed: reg.CapacityUsed, CapacityTotal: reg.CapacityTotal,
		Connected: true, Generation: reg.Generation, Capabilities: reg.Capabilities,
		LastSeenAt: time.Now(),
	})
	if err != nil {
		return control.RunnerRegistrationResult{}, err
	}
	return control.RunnerRegistrationResult{Accepted: true, Generation: reg.Generation}, nil
}

func (f *fakeFleet) ReconcileRunner(ctx context.Context, snap control.RunnerSnapshot) (control.ReconcileResult, error) {
	f.mu.Lock()
	f.snaps = append(f.snaps, snap)
	destroy := append([]control.SessionID(nil), f.destroy...)
	f.mu.Unlock()
	return control.ReconcileResult{Generation: snap.Generation, Destroy: destroy}, nil
}

func (f *fakeFleet) ApplyRunnerEvent(ctx context.Context, ev control.RunnerEvent) error {
	f.mu.Lock()
	f.events = append(f.events, ev)
	ch, err := f.eventCh, f.applyErr
	f.mu.Unlock()
	if ch != nil {
		select {
		case ch <- ev:
		default:
		}
	}
	return err
}

func (f *fakeFleet) ListRunners(ctx context.Context, sc control.Scope, q control.RunnerQuery) (control.RunnerPage, error) {
	return control.RunnerPage{}, nil
}

func (f *fakeFleet) applied() []control.RunnerEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]control.RunnerEvent(nil), f.events...)
}

// fakeHost is the Host a plane is built over in these tests.
type fakeHost struct {
	repo  *fakeRepo
	fleet *fakeFleet

	// identify overrides the default bearer check.
	identify func(r *http.Request, name string) (Binding, error)
	// answer overrides the default session_req answer.
	answer func(b Binding, id control.SessionID, env runner.RPCEnvelope) runner.RPCEnvelope

	asides chan runner.FromRunner
	wakes  chan control.PoolID
}

func newFakeHost() *fakeHost {
	repo := newFakeRepo()
	return &fakeHost{
		repo:   repo,
		fleet:  &fakeFleet{repo: repo},
		asides: make(chan runner.FromRunner, 16),
		wakes:  make(chan control.PoolID, 64),
	}
}

var errDenied = errors.New("unauthorized")

func (h *fakeHost) Identify(ctx context.Context, r *http.Request, name string) (Binding, error) {
	if h.identify != nil {
		return h.identify(r, name)
	}
	if r.Header.Get("Authorization") != "Bearer "+testToken {
		return Binding{}, errDenied
	}
	return Binding{WorkspaceID: testWorkspace, PoolID: testPool, RunnerID: control.RunnerID(name)}, nil
}

func (h *fakeHost) NextGeneration(ctx context.Context, b Binding) (uint64, error) {
	return h.repo.next(b.RunnerID), nil
}

func (h *fakeHost) Fleet() control.Fleet                     { return h.fleet }
func (h *fakeHost) FleetRepository() control.FleetRepository { return h.repo }

func (h *fakeHost) Wake(pool control.PoolID) {
	select {
	case h.wakes <- pool:
	default:
	}
}

func (h *fakeHost) Aside(ctx context.Context, b Binding, gen uint64, m runner.FromRunner) {
	select {
	case h.asides <- m:
	default:
	}
}

func (h *fakeHost) SessionRequest(ctx context.Context, b Binding, id control.SessionID, env runner.RPCEnvelope) runner.RPCEnvelope {
	if h.answer != nil {
		return h.answer(b, id, env)
	}
	body, _ := json.Marshal(struct {
		Method string `json:"method"`
	}{env.Method})
	return runner.RPCEnvelope{OK: true, Payload: body}
}

// ---------------------------------------------------------------------------
// the fake runner: a scripted runnerd over httptest
// ---------------------------------------------------------------------------

// newTestPlane returns a plane over a fresh fake host and an httptest server
// serving its Handler at the runner connect path.
func newTestPlane(t *testing.T, opts ...func(*Options)) (*Plane, *fakeHost, *httptest.Server) {
	t.Helper()
	h := newFakeHost()
	p, ts := newTestPlaneOver(t, h, opts...)
	return p, h, ts
}

func newTestPlaneOver(t *testing.T, h *fakeHost, opts ...func(*Options)) (*Plane, *httptest.Server) {
	t.Helper()
	// Generous on purpose: every happy-path round trip here crosses an
	// httptest websocket and a fake-runner goroutine, and none of these tests
	// is asserting a duration. The ones that test a timeout pass their own.
	o := Options{OpTimeout: 10 * time.Second, Logf: func(string, ...any) {}}
	for _, fn := range opts {
		fn(&o)
	}
	p := New(h, o)
	mux := http.NewServeMux()
	mux.Handle("GET /v0/runners/connect", p.Handler())
	ts := httptest.NewServer(mux)
	// Cleanups run LIFO, so every fakeRunner started after this call closes
	// its socket before ts.Close() runs.
	t.Cleanup(ts.Close)
	t.Cleanup(p.Close)
	return p, ts
}

func runnerWSURL(ts *httptest.Server) string {
	return "ws" + strings.TrimPrefix(ts.URL, "http") + "/v0/runners/connect"
}

func dialRunner(t *testing.T, ts *httptest.Server, token string) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	hdr := http.Header{}
	if token != "" {
		hdr.Set("Authorization", "Bearer "+token)
	}
	return websocket.Dial(ctx, runnerWSURL(ts), &websocket.DialOptions{HTTPHeader: hdr})
}

// runnerScript describes the fake runner's announce. Zero values mean
// "sensible default": name vm1, the real proto, the test token.
type runnerScript struct {
	Name         string
	Token        string
	Proto        *int
	Sessions     []runner.SessionInfo
	Used, Total  int
	Capabilities []string
}

// fakeRunner is a scripted runnerd: it dials the plane, writes one announce,
// then records every ToRunner the plane sends and answers only when the test
// tells it to.
type fakeRunner struct {
	name  string
	c     *websocket.Conn
	cmds  chan runner.ToRunner
	readE chan error

	wmu         sync.Mutex
	used, total int

	closeOnce sync.Once
}

func startFakeRunner(t *testing.T, ts *httptest.Server, sc runnerScript) *fakeRunner {
	t.Helper()
	if sc.Name == "" {
		sc.Name = "vm1"
	}
	if sc.Token == "" {
		sc.Token = testToken
	}
	proto := runner.ProtocolVersion
	if sc.Proto != nil {
		proto = *sc.Proto
	}
	c, _, err := dialRunner(t, ts, sc.Token)
	if err != nil {
		t.Fatalf("dial the plane: %v", err)
	}
	c.SetReadLimit(16 << 20)
	f := &fakeRunner{
		name:  sc.Name,
		c:     c,
		cmds:  make(chan runner.ToRunner, 64),
		readE: make(chan error, 1),
		used:  sc.Used,
		total: sc.Total,
	}
	t.Cleanup(f.close)
	f.write(t, runner.FromRunner{Type: "announce", Proto: proto, Runner: sc.Name,
		Sessions: sc.Sessions, Capabilities: sc.Capabilities})
	go f.readLoop()
	return f
}

func (f *fakeRunner) readLoop() {
	for {
		var m runner.ToRunner
		if err := wsjson.Read(context.Background(), f.c, &m); err != nil {
			f.readE <- err
			return
		}
		f.cmds <- m
	}
}

// write is the fake's single writer. Like the real agent it stamps its
// current capacity onto every outbound message, whatever the type.
func (f *fakeRunner) write(t *testing.T, m runner.FromRunner) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	f.wmu.Lock()
	defer f.wmu.Unlock()
	m.Used, m.Total = f.used, f.total
	if err := wsjson.Write(ctx, f.c, m); err != nil {
		t.Fatalf("write to the plane: %v", err)
	}
}

// writeStale is write for a socket the plane may already have hung up on:
// the error is the point of the test, not a failure of it.
func (f *fakeRunner) writeStale(m runner.FromRunner) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	f.wmu.Lock()
	defer f.wmu.Unlock()
	m.Used, m.Total = f.used, f.total
	_ = wsjson.Write(ctx, f.c, m)
}

func (f *fakeRunner) setCapacity(used, total int) {
	f.wmu.Lock()
	defer f.wmu.Unlock()
	f.used, f.total = used, total
}

func (f *fakeRunner) nextCmd(t *testing.T) runner.ToRunner {
	t.Helper()
	select {
	case m := <-f.cmds:
		return m
	case <-time.After(3 * time.Second):
		t.Fatal("no command from the plane within 3s")
		return runner.ToRunner{}
	}
}

func (f *fakeRunner) reply(t *testing.T, cmd runner.ToRunner, ok bool, detail string) {
	t.Helper()
	f.write(t, runner.FromRunner{Type: "result", ReqID: cmd.ReqID, OK: ok, Detail: detail})
}

func (f *fakeRunner) event(t *testing.T, session, state string) {
	t.Helper()
	f.write(t, runner.FromRunner{Type: "event", Session: session, State: state})
}

// answerRPC scripts the sandbox's answer to a plane-initiated request. It
// travels up as a session_req whose envelope Method is "resp", echoing the
// request's own id — the pass-through correlation runnerd performs verbatim.
func (f *fakeRunner) answerRPC(t *testing.T, cmd runner.ToRunner, ok bool, payload string) {
	t.Helper()
	f.write(t, runner.FromRunner{Type: "session_req", Session: cmd.Session,
		RPC: &runner.RPCEnvelope{ID: cmd.RPC.ID, Method: "resp", OK: ok, Payload: json.RawMessage(payload)}})
}

func (f *fakeRunner) waitClosed(t *testing.T) error {
	t.Helper()
	select {
	case err := <-f.readE:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("the plane did not close the connection within 3s")
		return nil
	}
}

func (f *fakeRunner) close() {
	f.closeOnce.Do(func() { f.c.CloseNow() })
}

// drainAccept takes the accept the plane sends every registered runner before
// any command off the front of the command stream.
func drainAccept(t *testing.T, f *fakeRunner) runner.ToRunner {
	t.Helper()
	cmd := f.nextCmd(t)
	if cmd.Type != "accept" {
		t.Fatalf("first message from the plane = %+v, want an accept", cmd)
	}
	return cmd
}

// nextOfType returns the next command of type typ f receives, skipping
// anything else the plane happens to send in the meantime.
func nextOfType(t *testing.T, f *fakeRunner, typ string) runner.ToRunner {
	t.Helper()
	for {
		if cmd := f.nextCmd(t); cmd.Type == typ {
			return cmd
		}
	}
}

func nextSessionRPC(t *testing.T, f *fakeRunner) runner.ToRunner {
	t.Helper()
	cmd := nextOfType(t, f, "session_rpc")
	if cmd.RPC == nil {
		t.Fatalf("session_rpc for %s carried no envelope: %+v", cmd.Session, cmd)
	}
	return cmd
}

// eventually polls fn until it returns nil or d elapses — the plane handles
// runner messages asynchronously, so assertions are polled, not slept on.
func eventually(t *testing.T, d time.Duration, fn func() error) {
	t.Helper()
	deadline := time.Now().Add(d)
	var last error
	for {
		if last = fn(); last == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition unmet after %s: %v", d, last)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func waitConnected(t *testing.T, p *Plane, name string) {
	t.Helper()
	eventually(t, 3*time.Second, func() error {
		if !p.Transport().Connected(testPool, control.RunnerID(name)) {
			return fmt.Errorf("runner %q not connected yet", name)
		}
		return nil
	})
}

func awaitChan(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not happen within 5s", what)
	}
}

// ---------------------------------------------------------------------------
// registration
// ---------------------------------------------------------------------------

// TestRegistrationAcceptsFirst is the plane's whole registration path in one
// case: the announce mints generation 1, the fleet takes the registration,
// the accept is the first thing the runner hears, reconciliation's destroy
// list is dispatched over the same connection, and the pool is woken.
func TestRegistrationAcceptsFirst(t *testing.T) {
	p, h, ts := newTestPlane(t)
	h.fleet.destroy = []control.SessionID{"sess_orphan"}

	f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Used: 1, Total: 4,
		Capabilities: []string{"gpu"}})
	waitConnected(t, p, "vm1")

	acc := drainAccept(t, f)
	if acc.Generation != 1 {
		t.Fatalf("accept Generation = %d, want 1", acc.Generation)
	}
	if !slices.Equal(acc.Capabilities, []string{"gpu"}) {
		t.Fatalf("accept Capabilities = %v, want the announced set", acc.Capabilities)
	}

	cmd := f.nextCmd(t)
	if cmd.Type != "destroy" || cmd.Session != "sess_orphan" {
		t.Fatalf("after the accept the plane sent %+v, want the reconcile destroy", cmd)
	}
	f.reply(t, cmd, true, "")

	rows, err := h.repo.ListRunners(context.Background(), testPool)
	if err != nil || len(rows) != 1 {
		t.Fatalf("ListRunners = %+v, %v; want one row", rows, err)
	}
	r := rows[0]
	if r.ID != "vm1" || !r.Connected || r.CapacityUsed != 1 || r.CapacityTotal != 4 {
		t.Fatalf("runner = %+v, want {vm1 used:1 total:4 connected:true}", r)
	}
	select {
	case pool := <-h.wakes:
		if pool != testPool {
			t.Fatalf("woke pool %q, want %q", pool, testPool)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("registration never woke the pool")
	}
}

// TestRefusedIdentifyClosesBeforeTheAnnounce: a host that declines the
// connection answers the HTTP request, so nothing is ever read off the socket
// and no row is written.
func TestRefusedIdentifyClosesBeforeTheAnnounce(t *testing.T) {
	p, h, ts := newTestPlane(t)

	for _, tc := range []struct {
		name  string
		token string
	}{{"no bearer", ""}, {"wrong bearer", "rnr_wrong"}} {
		t.Run(tc.name, func(t *testing.T) {
			c, resp, err := dialRunner(t, ts, tc.token)
			if err == nil {
				c.CloseNow()
				t.Fatal("dial succeeded, want rejection before upgrade")
			}
			if resp == nil {
				t.Fatalf("no HTTP response: %v", err)
			}
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", resp.StatusCode)
			}
		})
	}

	rows, _ := h.repo.ListRunners(context.Background(), testPool)
	if len(rows) != 0 {
		t.Fatalf("a refused dial left rows behind: %+v", rows)
	}
	if p.Transport().Connected(testPool, "vm1") {
		t.Fatal("a refused dial registered a connection")
	}
}

// TestAnnounceRejections: the first message must be an announce in a proto
// the plane speaks, and it must name its runner.
func TestAnnounceRejections(t *testing.T) {
	bad := 99
	t.Run("unsupported proto", func(t *testing.T) {
		_, h, ts := newTestPlane(t)
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Proto: &bad, Total: 4})
		err := f.waitClosed(t)
		var ce websocket.CloseError
		if !errors.As(err, &ce) {
			t.Fatalf("conn ended with %v, want a websocket close", err)
		}
		if !strings.Contains(ce.Reason, "proto 99") || !strings.Contains(ce.Reason, "proto 1") {
			t.Fatalf("close reason = %q, want it to name both proto 99 and proto 1", ce.Reason)
		}
		rows, _ := h.repo.ListRunners(context.Background(), testPool)
		if len(rows) != 0 {
			t.Fatalf("ListRunners = %+v, want none", rows)
		}
	})

	t.Run("no runner name", func(t *testing.T) {
		_, _, ts := newTestPlane(t)
		c, _, err := dialRunner(t, ts, testToken)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer c.CloseNow()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := wsjson.Write(ctx, c, runner.FromRunner{Type: "announce", Proto: runner.ProtocolVersion}); err != nil {
			t.Fatalf("write announce: %v", err)
		}
		var m runner.ToRunner
		err = wsjson.Read(ctx, c, &m)
		var ce websocket.CloseError
		if !errors.As(err, &ce) || !strings.Contains(ce.Reason, "missing a runner name") {
			t.Fatalf("conn ended with %v, want a close naming the missing runner name", err)
		}
	})
}

// TestGenerationContinuesAcrossRestart pins that the generation is the
// host's, not the plane's: a second Plane over the same fake repository
// registers the runner at the next generation, not at 1.
//
// Moved from internal/controld generations_test.go
// (TestRunnerGenerationContinuesAcrossRestart), against the fake host.
func TestGenerationContinuesAcrossRestart(t *testing.T) {
	h := newFakeHost()
	p1, ts1 := newTestPlaneOver(t, h)
	f1 := startFakeRunner(t, ts1, runnerScript{Name: "vm1", Total: 4})
	waitConnected(t, p1, "vm1")
	drainAccept(t, f1)
	ts1.Close()

	p2, ts2 := newTestPlaneOver(t, h)
	f2 := startFakeRunner(t, ts2, runnerScript{Name: "vm1", Total: 4})
	waitConnected(t, p2, "vm1")
	if acc := drainAccept(t, f2); acc.Generation != 2 {
		t.Fatalf("accept Generation = %d, want 2 after the restart", acc.Generation)
	}

	rows, err := h.repo.ListRunners(context.Background(), testPool)
	if err != nil || len(rows) != 1 || rows[0].Generation != 2 {
		t.Fatalf("after restart: %+v, %v; want vm1 at generation 2", rows, err)
	}
}

// TestSupersededConnectionIsFencedOnHeartbeat: once the store holds a newer
// generation for a runner (another replica registered it), this plane's
// connection is refused at its next heartbeat and ends.
//
// Moved from internal/controld generations_test.go, against the fake host.
func TestSupersededConnectionIsFencedOnHeartbeat(t *testing.T) {
	p, h, ts := newTestPlane(t)
	f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4})
	waitConnected(t, p, "vm1")
	drainAccept(t, f)

	h.repo.next("vm1") // another replica opened generation 2

	// Any message heartbeats, and the fake stamps its capacity onto whatever
	// it writes — so a heartbeat that got through would be visible as a used
	// slot on the row.
	f.setCapacity(1, 4)
	f.write(t, runner.FromRunner{Type: "event", Session: "sess_nobody", State: "running"})

	eventually(t, 2*time.Second, func() error {
		if p.Transport().Connected(testPool, "vm1") {
			return errors.New("superseded connection still registered")
		}
		return nil
	})
	rows, _ := h.repo.ListRunners(context.Background(), testPool)
	if rows[0].Generation != 2 || rows[0].CapacityUsed != 0 {
		t.Fatalf("the stale heartbeat wrote through: %+v", rows[0])
	}
}

// TestConnectedFlipsOnRetire: a runner that hangs up is deregistered and its
// row is marked disconnected.
func TestConnectedFlipsOnRetire(t *testing.T) {
	p, h, ts := newTestPlane(t)
	f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Used: 1, Total: 4})
	waitConnected(t, p, "vm1")
	drainAccept(t, f)

	f.close()

	eventually(t, 3*time.Second, func() error {
		rows, err := h.repo.ListRunners(context.Background(), testPool)
		if err != nil {
			return err
		}
		if len(rows) != 1 {
			return fmt.Errorf("ListRunners = %+v, want 1 runner", rows)
		}
		if rows[0].Connected {
			return errors.New("runner still connected after close")
		}
		return nil
	})
	if p.Transport().Connected(testPool, "vm1") {
		t.Fatal("Connected(vm1) still true after close")
	}
}

// TestAnnouncedCapabilities: what a runner claims about itself is validated,
// unioned with the capability the PLANE spells for its name, persisted, and
// acknowledged — the accept being the first thing that runner ever hears.
//
// Moved from internal/controld runners_test.go, against the fake host.
func TestAnnouncedCapabilities(t *testing.T) {
	t.Run("the announced set is unioned with the host's own and accepted first", func(t *testing.T) {
		p, h, ts := newTestPlane(t)
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Capabilities: []string{"gpu", "docker.rootless"}})
		waitConnected(t, p, "vm1")

		acc := f.nextCmd(t)
		if acc.Type != "accept" {
			t.Fatalf("first message = %+v, want an accept before any command", acc)
		}
		if acc.Generation != 1 {
			t.Fatalf("accept Generation = %d, want 1", acc.Generation)
		}
		if !slices.Equal(acc.Capabilities, []string{"gpu", "docker.rootless"}) {
			t.Fatalf("accept Capabilities = %v, want the announced pair", acc.Capabilities)
		}

		want := append(hostCapabilities("vm1"), "gpu", "docker.rootless")
		wantCapabilities := func(when string) {
			t.Helper()
			eventually(t, 3*time.Second, func() error {
				rows, err := h.repo.ListRunners(context.Background(), testPool)
				if err != nil {
					return err
				}
				if len(rows) != 1 {
					return fmt.Errorf("ListRunners = %+v, want 1 runner", rows)
				}
				if !slices.Equal(rows[0].Capabilities, want) {
					return fmt.Errorf("%s: capabilities = %v, want %v", when, rows[0].Capabilities, want)
				}
				return nil
			})
		}
		wantCapabilities("at registration")

		// Capacity rides every message, and the heartbeat that carries it
		// rewrites the runner's row: what the runner announced has to survive
		// that write, or a fleet would forget it the moment it did any work.
		f.event(t, "sess_heartbeat", "running")
		eventually(t, 3*time.Second, func() error {
			if len(h.fleet.applied()) == 0 {
				return errors.New("the heartbeat's event has not landed yet")
			}
			return nil
		})
		wantCapabilities("after a heartbeat")
	})

	for _, tc := range []struct {
		name string
		caps []string
	}{
		{"a host-prefixed capability is refused", []string{"placement:other"}},
		{"a capability that is not a token is refused", []string{"GPU"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, h, ts := newTestPlane(t)
			f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4, Capabilities: tc.caps})

			err := f.waitClosed(t)
			var ce websocket.CloseError
			if !errors.As(err, &ce) {
				t.Fatalf("conn ended with %v, want a websocket close", err)
			}
			if !strings.Contains(ce.Reason, "registration refused") {
				t.Fatalf("close reason = %q, want it to name a refused registration", ce.Reason)
			}
			rows, err := h.repo.ListRunners(context.Background(), testPool)
			if err != nil {
				t.Fatalf("ListRunners: %v", err)
			}
			for _, r := range rows {
				if r.Connected {
					t.Fatalf("a refused runner is in the fleet as connected: %+v", r)
				}
			}
		})
	}
}

// TestValidCapability is the exported token rule both planes of validation
// (this one's announce, the host's environment field) share.
func TestValidCapability(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want bool
	}{
		{"gpu", true},
		{"docker.rootless", true},
		{"a-b_c.9", true},
		{"GPU", false},
		{"placement:vm1", false},
		{"", false},
		{"-leading", false},
		{strings.Repeat("a", 64), true},
		{strings.Repeat("a", 65), false},
	} {
		if got := ValidCapability(tc.in); got != tc.want {
			t.Fatalf("ValidCapability(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
	if MaxCapabilities != 32 {
		t.Fatalf("MaxCapabilities = %d, want 32", MaxCapabilities)
	}
}

// TestConnectRunnerRefusalDoesNotCloseTheWinningConn pins the ordering
// registration must hold: a reconnect the fleet service is about to refuse
// must never take down the connection that already holds the accepted
// generation (#36).
//
// Moved from internal/controld runners_test.go, against the fake host.
func TestConnectRunnerRefusalDoesNotCloseTheWinningConn(t *testing.T) {
	p, h, ts := newTestPlane(t)

	f1 := startFakeRunner(t, ts, runnerScript{Name: "runner-a", Total: 4}) // generation 1
	waitConnected(t, p, "runner-a")
	drainAccept(t, f1)
	f2 := startFakeRunner(t, ts, runnerScript{Name: "runner-a", Total: 4}) // generation 2
	eventually(t, 3*time.Second, func() error {
		if h.repo.generation("runner-a") != 2 {
			return errors.New("the redial has not registered yet")
		}
		return nil
	})
	drainAccept(t, f2)

	live := p.conn("runner-a")
	if live == nil {
		t.Fatal("runner-a has no live connection")
	}
	rows, err := h.repo.ListRunners(context.Background(), testPool)
	if err != nil || len(rows) != 1 || rows[0].Generation != 2 {
		t.Fatalf("live generation: rows = %+v, err = %v; want runner-a at generation 2", rows, err)
	}

	// A redial that was assigned generation 1 before the reconnect above but
	// only now reaches registration, e.g. because its own handshake was slow.
	// The fleet service must refuse it: generation 1 is superseded.
	stale := newRunnerConn(Binding{WorkspaceID: testWorkspace, PoolID: testPool, RunnerID: "runner-a"}, nil)
	stale.gen = 1

	err = p.connectRunner(context.Background(), stale, runner.FromRunner{Total: 4})
	if !errors.Is(err, errRegistrationRefused) {
		t.Fatalf("connectRunner(stale gen 1) error = %v, want errRegistrationRefused", err)
	}

	select {
	case <-live.done:
		t.Fatal("the live (accepted, generation 2) connection was closed by a reconnect attempt the fleet service went on to refuse")
	default:
	}
	if got := p.conn("runner-a"); got != live {
		t.Fatalf("conn(runner-a) = %p, want the still-live connection %p: a refused reconnect replaced it in the registry", got, live)
	}
}

// TestReconnectReplacesConn pins the reconnect discipline: the new conn owns
// the runner, the old one is closed, and the old conn's teardown must not
// mark the (live) new conn disconnected.
//
// Moved from internal/controld runners_test.go, against the fake host.
func TestReconnectReplacesConn(t *testing.T) {
	p, h, ts := newTestPlane(t)
	first := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4})
	waitConnected(t, p, "vm1")

	second := startFakeRunner(t, ts, runnerScript{Name: "vm1", Used: 2, Total: 4})
	if err := first.waitClosed(t); err == nil {
		t.Fatal("first connection was not closed by the reconnect")
	}

	// The runner stays connected — the replaced conn's cleanup is
	// pointer-guarded — and dispatch reaches the new socket.
	eventually(t, 3*time.Second, func() error {
		rows, err := h.repo.ListRunners(context.Background(), testPool)
		if err != nil {
			return err
		}
		if len(rows) != 1 || !rows[0].Connected || rows[0].CapacityUsed != 2 {
			return fmt.Errorf("runners = %+v, want one connected runner with used:2", rows)
		}
		return nil
	})

	drainAccept(t, second)
	errc := make(chan error, 1)
	go func() {
		_, err := p.Transport().Dispatch(context.Background(), testPool, "vm1",
			runner.ToRunner{Type: "destroy", Session: "sess_x"})
		errc <- err
	}()
	cmd := second.nextCmd(t)
	if cmd.Type != "destroy" || cmd.Session != "sess_x" {
		t.Fatalf("second conn got %+v, want destroy of sess_x", cmd)
	}
	second.reply(t, cmd, true, "")
	if err := <-errc; err != nil {
		t.Fatalf("dispatch over the replacement conn: %v", err)
	}

	if !p.Transport().Connected(testPool, "vm1") {
		t.Fatal("Connected(vm1) = false after reconnect")
	}
}

// TestRedialSurvivesStaleDisconnect pins the ordering a pointer guard alone
// can't hold: a dying connection's disconnect write must never land on top of
// a redial's connect. The protocol is announce-once, so a runner that loses
// this race sends nothing further to correct the row.
//
// Moved from internal/controld runners_test.go; the raceStore is the fake
// repository's two hooks here.
func TestRedialSurvivesStaleDisconnect(t *testing.T) {
	h := newFakeHost()
	var (
		entered      = make(chan struct{})
		upserted     = make(chan struct{})
		wrote        = make(chan struct{})
		enteredOnce  sync.Once
		upsertedOnce sync.Once
		wroteOnce    sync.Once
	)
	h.repo.beforeDisconnect = func() {
		enteredOnce.Do(func() { close(entered) })
		select {
		case <-upserted:
		case <-time.After(500 * time.Millisecond):
		}
	}
	h.repo.afterDisconnect = func() { wroteOnce.Do(func() { close(wrote) }) }
	h.repo.afterUpsert = func(r control.Runner) {
		// Only a connect that happens after the teardown began is the one
		// this test is racing; the first runner's own announce must not
		// release it.
		select {
		case <-entered:
			if r.Connected {
				upsertedOnce.Do(func() { close(upserted) })
			}
		default:
		}
	}

	p, ts := newTestPlaneOver(t, h)
	first := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4})
	waitConnected(t, p, "vm1")
	drainAccept(t, first)

	first.close()

	// The teardown is now inside its disconnect write; redial into that gap.
	awaitChan(t, entered, "teardown's disconnect write")
	startFakeRunner(t, ts, runnerScript{Name: "vm1", Used: 1, Total: 4})
	awaitChan(t, wrote, "teardown's disconnect write returning")

	eventually(t, 3*time.Second, func() error {
		rows, err := h.repo.ListRunners(context.Background(), testPool)
		if err != nil {
			return err
		}
		if len(rows) != 1 {
			return fmt.Errorf("runners = %+v, want 1", rows)
		}
		if !rows[0].Connected {
			return fmt.Errorf("runner marked disconnected while its redial is live: %+v", rows[0])
		}
		return nil
	})
	if !p.Transport().Connected(testPool, "vm1") {
		t.Fatal("Connected(vm1) = false after the redial")
	}
}

// ---------------------------------------------------------------------------
// events
// ---------------------------------------------------------------------------

// TestEventTranslation walks the runner event vocabulary into the control
// one: the states, the composed failure sentences, and the child exit.
func TestEventTranslation(t *testing.T) {
	p, h, _ := newTestPlane(t)
	rc := newRunnerConn(Binding{WorkspaceID: testWorkspace, PoolID: testPool, RunnerID: "vm1"}, nil)
	rc.gen = 1

	code := 0
	for _, tc := range []struct {
		name string
		in   runner.FromRunner
		want control.RunnerEvent
	}{
		{"running", runner.FromRunner{Session: "s1", State: "running"},
			control.RunnerEvent{State: control.StateRunning}},
		{"dead", runner.FromRunner{Session: "s1", State: "dead"},
			control.RunnerEvent{State: control.StateDead}},
		{"setup_failed", runner.FromRunner{Session: "s1", State: "setup_failed", Detail: "rc 1: boom"},
			control.RunnerEvent{State: control.StateFailed, Detail: "setup failed: rc 1: boom"}},
		{"stage_failed names its stage", runner.FromRunner{Session: "s1", State: "stage_failed", Detail: "clone: rc 128: nope"},
			control.RunnerEvent{State: control.StateFailed, Detail: "clone failed: rc 128: nope"}},
		{"stage_failed with no stage", runner.FromRunner{Session: "s1", State: "stage_failed", Detail: "unparseable"},
			control.RunnerEvent{State: control.StateFailed, Detail: "boot failed: unparseable"}},
		{"child_exited", runner.FromRunner{Session: "s1", State: "child_exited", Detail: "0"},
			control.RunnerEvent{State: control.StateRunning, ChildExitCode: &code}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := len(h.fleet.applied())
			m := tc.in
			m.Type = "event"
			p.applyRunnerEvent(context.Background(), rc, m)
			got := h.fleet.applied()
			if len(got) != before+1 {
				t.Fatalf("applied %d events, want one more than %d", len(got), before)
			}
			ev := got[len(got)-1]
			if ev.WorkspaceID != testWorkspace || ev.PoolID != testPool || ev.RunnerID != "vm1" {
				t.Fatalf("event bindings = %+v, want the connection's", ev)
			}
			if ev.State != tc.want.State || ev.Detail != tc.want.Detail {
				t.Fatalf("event = {%q %q}, want {%q %q}", ev.State, ev.Detail, tc.want.State, tc.want.Detail)
			}
			if (ev.ChildExitCode == nil) != (tc.want.ChildExitCode == nil) {
				t.Fatalf("child exit code = %v, want %v", ev.ChildExitCode, tc.want.ChildExitCode)
			}
			if ev.ChildExitCode != nil && *ev.ChildExitCode != *tc.want.ChildExitCode {
				t.Fatalf("child exit code = %d, want %d", *ev.ChildExitCode, *tc.want.ChildExitCode)
			}
		})
	}

	t.Run("an unreadable child exit code is dropped", func(t *testing.T) {
		before := len(h.fleet.applied())
		p.applyRunnerEvent(context.Background(), rc,
			runner.FromRunner{Type: "event", Session: "s1", State: "child_exited", Detail: "nope"})
		if len(h.fleet.applied()) != before {
			t.Fatal("an unreadable exit code was applied")
		}
	})

	t.Run("an event with no session is dropped", func(t *testing.T) {
		before := len(h.fleet.applied())
		p.applyRunnerEvent(context.Background(), rc, runner.FromRunner{Type: "event", State: "running"})
		if len(h.fleet.applied()) != before {
			t.Fatal("an event with no session was applied")
		}
	})
}

// TestEventGenerationSource: an event says which generation it was produced
// under, and that claim is what the service fences it by. One that names none
// is applied under the connection's own.
//
// Moved from internal/controld runners_test.go; the fence itself is the
// service's, so what the plane owes is the generation it stamps.
func TestEventGenerationSource(t *testing.T) {
	p, h, _ := newTestPlane(t)
	live := newRunnerConn(Binding{WorkspaceID: testWorkspace, PoolID: testPool, RunnerID: "runner-a"}, nil)
	live.gen = 2

	dead := runner.FromRunner{Type: "event", Session: "sess_evgen", State: "dead"}

	// The connection is current, but the message itself claims generation 1.
	claimed := dead
	claimed.Generation = 1
	p.applyRunnerEvent(context.Background(), live, claimed)
	// The same event carrying no generation is the old runner's message, and
	// takes the connection's.
	p.applyRunnerEvent(context.Background(), live, dead)

	got := h.fleet.applied()
	if len(got) != 2 {
		t.Fatalf("applied %d events, want 2", len(got))
	}
	if got[0].Generation != 1 {
		t.Fatalf("event generation = %d, want the 1 the message claimed", got[0].Generation)
	}
	if got[1].Generation != 2 {
		t.Fatalf("event generation = %d, want the connection's 2", got[1].Generation)
	}
}

// TestEventPlacementGenerationIsFenced: a report echoing the placement
// generation its create carried is carried through untouched — the session's
// own authority, beside the runner's, checked by the service.
//
// Moved from internal/controld runners_test.go.
func TestEventPlacementGenerationIsFenced(t *testing.T) {
	p, h, _ := newTestPlane(t)
	rc := newRunnerConn(Binding{WorkspaceID: testWorkspace, PoolID: testPool, RunnerID: "runner-a"}, nil)
	rc.gen = 1

	p.applyRunnerEvent(context.Background(), rc,
		runner.FromRunner{Type: "event", Session: "sess_pgfence", State: "dead", PlacementGeneration: 2})
	p.applyRunnerEvent(context.Background(), rc,
		runner.FromRunner{Type: "event", Session: "sess_pgfence", State: "dead", PlacementGeneration: 3})

	got := h.fleet.applied()
	if len(got) != 2 || got[0].PlacementGeneration != 2 || got[1].PlacementGeneration != 3 {
		t.Fatalf("placement generations = %+v, want 2 then 3 carried through untouched", got)
	}
}

// TestRunnerGenerationFencesAStaleSocket pins the first of the two locks on
// the same door: a runner that redials owns its name from that instant, and
// the connection it replaced stops being read, so nothing it says afterwards
// is ever translated at all.
//
// Moved from internal/controld runners_test.go; the second lock (the
// service's generation fence) is TestEventGenerationSource.
func TestRunnerGenerationFencesAStaleSocket(t *testing.T) {
	p, h, ts := newTestPlane(t)
	first := startFakeRunner(t, ts, runnerScript{Name: "runner-a", Total: 4})
	waitConnected(t, p, "runner-a")
	drainAccept(t, first)

	second := startFakeRunner(t, ts, runnerScript{Name: "runner-a", Total: 4})
	if err := first.waitClosed(t); err == nil {
		t.Fatal("the superseded connection was not closed")
	}
	drainAccept(t, second)

	// The stale socket reports the session dead — terminal, and therefore
	// unrecoverable if it were ever applied.
	first.writeStale(runner.FromRunner{Type: "event", Session: "sess_fence", State: "dead"})
	// The live socket reports it running, which is what must actually land.
	second.event(t, "sess_fence", "running")

	eventually(t, 3*time.Second, func() error {
		if len(h.fleet.applied()) == 0 {
			return errors.New("no event applied yet")
		}
		return nil
	})
	// ...and stays the only one: a stale "dead" handled late would follow.
	time.Sleep(150 * time.Millisecond)
	got := h.fleet.applied()
	if len(got) != 1 || got[0].State != control.StateRunning {
		t.Fatalf("applied %+v, want only the live socket's running event", got)
	}
}

// TestAsideEventsReachTheHost: the two events that transition no session go
// to the host untranslated, with the generation the plane fenced them under.
func TestAsideEventsReachTheHost(t *testing.T) {
	p, h, ts := newTestPlane(t)
	f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4})
	waitConnected(t, p, "vm1")
	drainAccept(t, f)

	f.event(t, "sess_setup", "setup_done")
	f.event(t, "sess_cred", "credential_rejected")

	for _, want := range []struct{ session, state string }{
		{"sess_setup", "setup_done"},
		{"sess_cred", "credential_rejected"},
	} {
		select {
		case m := <-h.asides:
			if m.Session != want.session || m.State != want.state {
				t.Fatalf("aside = {%q %q}, want {%q %q}", m.Session, m.State, want.session, want.state)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("the %s event never reached the host", want.state)
		}
	}
	if got := h.fleet.applied(); len(got) != 0 {
		t.Fatalf("an aside was applied to the fleet service: %+v", got)
	}
}

// ---------------------------------------------------------------------------
// the transport
// ---------------------------------------------------------------------------

// transportFixture is a plane with one registered connection and no socket:
// the test plays the runner by reading rc.out and answering through deliver.
func transportFixture(t *testing.T, name string) (*Plane, *runnerConn) {
	t.Helper()
	h := newFakeHost()
	p := New(h, Options{OpTimeout: 200 * time.Millisecond, Logf: func(string, ...any) {}})
	rc := newRunnerConn(Binding{WorkspaceID: testWorkspace, PoolID: testPool, RunnerID: control.RunnerID(name)}, nil)
	p.mu.Lock()
	p.runners[name] = rc
	p.mu.Unlock()
	return p, rc
}

func TestTransportConnectedReflectsTheConnectionMap(t *testing.T) {
	p, _ := transportFixture(t, "runner-a")
	tr := p.Transport()
	if !tr.Connected(testPool, "runner-a") {
		t.Fatal("registered runner reported disconnected")
	}
	if tr.Connected(testPool, "runner-b") {
		t.Fatal("unknown runner reported connected")
	}
	if tr.Connected("pool_other", "runner-a") {
		t.Fatal("foreign pool reported connected")
	}
}

func TestTransportDispatchCorrelatesResultByReqID(t *testing.T) {
	p, rc := transportFixture(t, "runner-a")
	go func() {
		m := <-rc.out
		rc.deliver(runner.FromRunner{Type: "result", ReqID: m.ReqID, OK: true, Detail: "snap:example"})
	}()
	res, err := p.Transport().Dispatch(context.Background(), testPool, "runner-a", runner.ToRunner{Type: "snapshot", Session: "sess_example"})
	if err != nil {
		t.Fatal(err)
	}
	if !res.OK || res.Detail != "snap:example" {
		t.Fatalf("result = %+v", res)
	}
	rc.mu.Lock()
	pending := len(rc.pending)
	rc.mu.Unlock()
	if pending != 0 {
		t.Fatalf("pending table not cleaned up: %d", pending)
	}
}

func TestTransportDispatchSessionRPCCorrelatesByEnvelopeID(t *testing.T) {
	p, rc := transportFixture(t, "runner-a")
	go func() {
		m := <-rc.out
		if m.Type != "session_rpc" || m.RPC == nil || m.ReqID != 0 {
			return // the test's assertions below will report the timeout
		}
		rc.srpc.deliver(runner.RPCEnvelope{ID: m.RPC.ID, Method: "resp", OK: true, Payload: json.RawMessage(`{"repos":[]}`)})
	}()
	req := runner.ToRunner{Type: "session_rpc", Session: "sess_example",
		RPC: &runner.RPCEnvelope{ID: 77, Method: "diff"}}
	res, err := p.Transport().Dispatch(context.Background(), testPool, "runner-a", req)
	if err != nil {
		t.Fatal(err)
	}
	if res.Type != "session_req" || res.Session != "sess_example" || res.RPC == nil ||
		res.RPC.ID != 77 || res.RPC.Method != "resp" || !res.RPC.OK {
		t.Fatalf("answer = %+v", res)
	}
	if rc.srpc.len() != 0 {
		t.Fatalf("srpc table not cleaned up: %d", rc.srpc.len())
	}
	if _, err := p.Transport().Dispatch(context.Background(), testPool, "runner-a",
		runner.ToRunner{Type: "session_rpc", Session: "sess_example"}); !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("session_rpc without an envelope: got %v, want ErrInvalid", err)
	}
}

func TestTransportDispatchFailuresAreUnavailableWithoutRunnerText(t *testing.T) {
	p, rc := transportFixture(t, "runner-a")
	tr := p.Transport()
	cases := []struct {
		name string
		id   control.RunnerID
		pool control.PoolID
		prep func()
	}{
		{"unknown runner", "runner-b", testPool, func() {}},
		{"foreign pool", "runner-a", "pool_other", func() {}},
		{"no answer before OpTimeout", "runner-a", testPool, func() { go func() { <-rc.out }() }},
		{"connection closed", "runner-a", testPool, func() { go func() { <-rc.out; close(rc.done) }() }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.prep()
			_, err := tr.Dispatch(context.Background(), tc.pool, tc.id, runner.ToRunner{Type: "destroy", Session: "sess_example"})
			if !errors.Is(err, control.ErrUnavailable) {
				t.Fatalf("got %v, want ErrUnavailable", err)
			}
			if strings.Contains(err.Error(), "sess_example") {
				t.Fatalf("error carries the session id: %v", err)
			}
		})
	}
}

func TestTransportDispatchHonorsCallerCancellation(t *testing.T) {
	p, rc := transportFixture(t, "runner-a")
	p.opTimeout = time.Minute
	go func() { <-rc.out }()
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	start := time.Now()
	_, err := p.Transport().Dispatch(ctx, testPool, "runner-a", runner.ToRunner{Type: "suspend", Session: "sess_example"})
	if !errors.Is(err, control.ErrUnavailable) || time.Since(start) > time.Second {
		t.Fatalf("got %v after %s", err, time.Since(start))
	}
}

func TestTransportRemoveWorkspaceIsFireAndForget(t *testing.T) {
	p, rc := transportFixture(t, "runner-a")
	p.opTimeout = time.Minute
	got := make(chan runner.ToRunner, 1)
	go func() { got <- <-rc.out }()
	start := time.Now()
	res, err := p.Transport().Dispatch(context.Background(), testPool, "runner-a",
		runner.ToRunner{Type: "remove_workspace", Session: "sess_example"})
	if err != nil || !res.OK {
		t.Fatalf("got %+v, %v", res, err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("remove_workspace waited for an answer nobody sends")
	}
	if m := <-got; m.ReqID != 0 {
		t.Fatalf("remove_workspace carried req_id %d, want 0 (fire-and-forget)", m.ReqID)
	}
}

// TestDispatchCorrelatesResults drives two dispatches over one real socket and
// answers them out of order: correlation is by ReqID, not arrival order.
//
// Moved from internal/controld runners_test.go, against the fake host.
func TestDispatchCorrelatesResults(t *testing.T) {
	p, _, ts := newTestPlane(t)
	f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4})
	waitConnected(t, p, "vm1")
	drainAccept(t, f)

	type outcome struct {
		res runner.FromRunner
		err error
	}
	run := func(session string) <-chan outcome {
		ch := make(chan outcome, 1)
		go func() {
			res, err := p.Transport().Dispatch(context.Background(), testPool, "vm1",
				runner.ToRunner{Type: "snapshot", Session: session})
			ch <- outcome{res, err}
		}()
		return ch
	}
	a := run("sess_a")
	b := run("sess_b")

	c1 := f.nextCmd(t)
	c2 := f.nextCmd(t)
	if c1.ReqID == 0 || c2.ReqID == 0 || c1.ReqID == c2.ReqID {
		t.Fatalf("req ids = %d, %d: want distinct and non-zero", c1.ReqID, c2.ReqID)
	}
	f.reply(t, c2, true, "ref-"+c2.Session)
	f.reply(t, c1, true, "ref-"+c1.Session)

	for _, tc := range []struct {
		name string
		ch   <-chan outcome
	}{{"sess_a", a}, {"sess_b", b}} {
		select {
		case got := <-tc.ch:
			if got.err != nil {
				t.Fatalf("dispatch %s: %v", tc.name, got.err)
			}
			if want := "ref-" + tc.name; got.res.Detail != want {
				t.Fatalf("dispatch %s got detail %q, want %q", tc.name, got.res.Detail, want)
			}
			if !got.res.OK {
				t.Fatalf("dispatch %s: ok = false", tc.name)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("dispatch %s never returned", tc.name)
		}
	}
}

// TestDispatchUnreachable — moved from internal/controld runners_test.go.
func TestDispatchUnreachable(t *testing.T) {
	t.Run("never connected", func(t *testing.T) {
		p, _, _ := newTestPlane(t)
		_, err := p.Transport().Dispatch(context.Background(), testPool, "vm-nope",
			runner.ToRunner{Type: "destroy", Session: "sess_x"})
		if !errors.Is(err, control.ErrUnavailable) {
			t.Fatalf("err = %v, want control.ErrUnavailable", err)
		}
	})

	t.Run("connection dies in flight", func(t *testing.T) {
		p, _, ts := newTestPlane(t)
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4})
		waitConnected(t, p, "vm1")

		errc := make(chan error, 1)
		go func() {
			_, err := p.Transport().Dispatch(context.Background(), testPool, "vm1",
				runner.ToRunner{Type: "suspend", Session: "sess_x"})
			errc <- err
		}()
		f.nextCmd(t) // the accept; then the command reaches the runner
		f.nextCmd(t)
		f.close()

		select {
		case err := <-errc:
			if !errors.Is(err, control.ErrUnavailable) {
				t.Fatalf("err = %v, want control.ErrUnavailable", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("dispatch did not fail after the connection died")
		}
	})

	t.Run("no result before OpTimeout", func(t *testing.T) {
		p, _, ts := newTestPlane(t, func(o *Options) { o.OpTimeout = 150 * time.Millisecond })
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4})
		waitConnected(t, p, "vm1")
		drainAccept(t, f)

		start := time.Now()
		_, err := p.Transport().Dispatch(context.Background(), testPool, "vm1",
			runner.ToRunner{Type: "suspend", Session: "sess_x"})
		if !errors.Is(err, control.ErrUnavailable) {
			t.Fatalf("err = %v, want control.ErrUnavailable", err)
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Fatalf("dispatch took %s, want ~OpTimeout", elapsed)
		}
		f.nextCmd(t) // the command was still sent; the runner just never answered
	})
}

// TestDestroyOrphanRetriesAfterLiveQueueSaturation — moved from
// internal/controld runners_test.go.
func TestDestroyOrphanRetriesAfterLiveQueueSaturation(t *testing.T) {
	h := newFakeHost()
	p := New(h, Options{OpTimeout: time.Second, Logf: func(string, ...any) {}})
	rc := newRunnerConn(Binding{WorkspaceID: testWorkspace, PoolID: testPool, RunnerID: "vm1"}, nil)
	p.mu.Lock()
	p.runners[rc.name] = rc
	p.mu.Unlock()

	// Hold the writer still with a completely full live queue. The first
	// dispatch therefore fails in enqueue, not because the connection died.
	for i := 0; i < runnerSendQueue; i++ {
		rc.out <- runner.ToRunner{Type: "prepull", Ref: fmt.Sprintf("image-%d", i)}
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	p.destroyOrphan(ctx, rc, "sess_queue_retry")
	eventually(t, time.Second, func() error {
		if got := rc.seq.Load(); got != 1 {
			return fmt.Errorf("dispatch attempts = %d, want first saturated attempt", got)
		}
		return nil
	})
	// seq increments just before enqueue; let that enqueue and its deferred
	// pending-table cleanup finish while the queue is still provably full.
	<-time.After(50 * time.Millisecond)
	rc.mu.Lock()
	pendingAfterFirst := len(rc.pending)
	rc.mu.Unlock()
	if got := len(rc.out); got != runnerSendQueue || pendingAfterFirst != 0 {
		t.Fatalf("after first attempt: queue=%d pending=%d, want saturated queue and completed failed dispatch",
			got, pendingAfterFirst)
	}

	// Free one slot. The bounded retry must use it and produce a tracked
	// destroy rather than abandoning this orphan until another reconnect.
	<-rc.out
	var destroy runner.ToRunner
	for i := 0; i < runnerSendQueue; i++ {
		select {
		case cmd := <-rc.out:
			if cmd.Type == "destroy" {
				destroy = cmd
			}
		case <-time.After(3 * time.Second):
			t.Fatal("no retried orphan destroy after queue capacity returned")
		}
	}
	if destroy.Session != "sess_queue_retry" || destroy.ReqID == 0 {
		t.Fatalf("retry command = %+v, want tracked destroy of sess_queue_retry", destroy)
	}
	if destroy.ReqID == 1 {
		t.Fatalf("retry req_id = %d, want a fresh id after the saturated attempt", destroy.ReqID)
	}
	if !rc.deliver(runner.FromRunner{Type: "result", ReqID: destroy.ReqID, OK: true}) {
		t.Fatal("successful retry had no pending dispatcher")
	}
	eventually(t, time.Second, func() error {
		rc.mu.Lock()
		pending := len(rc.pending)
		rc.mu.Unlock()
		if pending != 0 {
			return fmt.Errorf("pending dispatches = %d, want none after success", pending)
		}
		if got := rc.seq.Load(); got != 2 {
			return fmt.Errorf("dispatch attempts = %d, want exactly 2", got)
		}
		return nil
	})
}

// ---------------------------------------------------------------------------
// Send and Broadcast
// ---------------------------------------------------------------------------

// TestBroadcastReachesEveryOtherRunner pins the fan-out the setup pipeline's
// prepull rides on: every connected runner except the one named.
func TestBroadcastReachesEveryOtherRunner(t *testing.T) {
	p, _, ts := newTestPlane(t)
	f1 := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4})
	f2 := startFakeRunner(t, ts, runnerScript{Name: "vm2", Total: 4})
	f3 := startFakeRunner(t, ts, runnerScript{Name: "vm3", Total: 4})
	for _, name := range []string{"vm1", "vm2", "vm3"} {
		waitConnected(t, p, name)
	}
	for _, f := range []*fakeRunner{f1, f2, f3} {
		drainAccept(t, f)
	}

	const ref = "rainier-env:env_x-0123456789ab"
	p.Broadcast(testPool, runner.ToRunner{Type: "prepull", Ref: ref}, "vm1")

	for _, f := range []*fakeRunner{f2, f3} {
		cmd := f.nextCmd(t)
		if cmd.Type != "prepull" || cmd.Ref != ref {
			t.Fatalf("%s got %+v, want the prepull of %s", f.name, cmd, ref)
		}
	}

	// vm1 was excluded. Sending it something else now and seeing that arrive
	// first proves no prepull was ever queued ahead of it — the connection
	// delivers in order.
	if err := p.Send(testPool, "vm1", runner.ToRunner{Type: "destroy", Session: "sess_probe"}); err != nil {
		t.Fatalf("Send(vm1): %v", err)
	}
	if cmd := f1.nextCmd(t); cmd.Type != "destroy" {
		t.Fatalf("vm1 got %+v, want the destroy (it must not have received the prepull)", cmd)
	}
}

// TestSendUnreachable pins the fire-and-forget path's error, which the host
// maps to the API's runner_unreachable code.
//
// Moved from internal/controld runners_test.go
// (TestSendToRunnerUnreachable's plane half); the sentinel is the public
// contract's, since the plane has no host-private error vocabulary.
func TestSendUnreachable(t *testing.T) {
	p, _, _ := newTestPlane(t)
	err := p.Send(testPool, "vm-nope", runner.ToRunner{Type: "destroy", Session: "sess_x"})
	if !errors.Is(err, control.ErrUnavailable) {
		t.Fatalf("err = %v, want control.ErrUnavailable", err)
	}
	if err := p.Send("pool_other", "vm-nope", runner.ToRunner{Type: "destroy"}); !errors.Is(err, control.ErrUnavailable) {
		t.Fatalf("foreign pool: err = %v, want control.ErrUnavailable", err)
	}
}

// ---------------------------------------------------------------------------
// the session RPC
// ---------------------------------------------------------------------------

// TestSessionRequestIsAnsweredByTheHost: a sandbox-initiated request is
// routed to the host, and the answer it returns is sent back down under the
// id the SANDBOX chose, as a "resp".
func TestSessionRequestIsAnsweredByTheHost(t *testing.T) {
	h := newFakeHost()
	h.answer = func(b Binding, id control.SessionID, env runner.RPCEnvelope) runner.RPCEnvelope {
		if b.RunnerID != "vm1" || id != "sess_req_up" {
			return runner.RPCEnvelope{}
		}
		return runner.RPCEnvelope{OK: true, Payload: json.RawMessage(`{"answered":"` + env.Method + `"}`)}
	}
	p, ts := newTestPlaneOver(t, h)
	f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4})
	waitConnected(t, p, "vm1")
	drainAccept(t, f)

	f.write(t, runner.FromRunner{Type: "session_req", Session: "sess_req_up",
		RPC: &runner.RPCEnvelope{ID: 41, Method: "mint_git_credential"}})

	cmd := nextSessionRPC(t, f)
	if cmd.Session != "sess_req_up" {
		t.Fatalf("answer routed to session %q, want sess_req_up", cmd.Session)
	}
	if cmd.RPC.ID != 41 {
		t.Fatalf("answer id = %d, want the request's 41", cmd.RPC.ID)
	}
	if cmd.RPC.Method != "resp" {
		t.Fatalf("answer method = %q, want \"resp\"", cmd.RPC.Method)
	}
	if !cmd.RPC.OK || string(cmd.RPC.Payload) != `{"answered":"mint_git_credential"}` {
		t.Fatalf("answer = %+v, want the host's own payload", cmd.RPC)
	}
}

// TestSessionRequestWithoutASessionIsDropped: a session_req names its session
// or it is not routable, and a malformed one must not end the connection every
// session on that runner depends on.
func TestSessionRequestWithoutASessionIsDropped(t *testing.T) {
	p, _, ts := newTestPlane(t)
	f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4})
	waitConnected(t, p, "vm1")
	drainAccept(t, f)

	f.write(t, runner.FromRunner{Type: "session_req",
		RPC: &runner.RPCEnvelope{ID: 3, Method: "mint_git_credential"}})
	f.write(t, runner.FromRunner{Type: "session_req", Session: "sess_x"}) // no envelope
	f.write(t, runner.FromRunner{Type: "session_req", Session: "sess_x",
		RPC: &runner.RPCEnvelope{Method: "mint_git_credential"}}) // no id

	// The connection still works.
	f.write(t, runner.FromRunner{Type: "session_req", Session: "sess_live",
		RPC: &runner.RPCEnvelope{ID: 9, Method: "ping"}})
	if cmd := nextSessionRPC(t, f); cmd.RPC.ID != 9 {
		t.Fatalf("answer id = %d, want 9 — the reader died on a malformed message", cmd.RPC.ID)
	}
}

// TestOrphanSessionRPCResponseIsDropped: a response whose id nobody is waiting
// on (its caller timed out, or the sandbox is confused) is logged and dropped.
// What must NOT happen is the connection's reader dying with it — proven by
// the ordinary RPC that still round-trips right after.
//
// Moved from internal/controld srpc_test.go, against the fake host.
func TestOrphanSessionRPCResponseIsDropped(t *testing.T) {
	p, _, ts := newTestPlane(t)
	f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4})
	waitConnected(t, p, "vm1")
	drainAccept(t, f)

	f.write(t, runner.FromRunner{Type: "session_req", Session: "sess_orphan",
		RPC: &runner.RPCEnvelope{ID: 999, Method: "resp", OK: true, Payload: json.RawMessage(`{"stale":true}`)}})

	type answer struct {
		Pong string `json:"pong"`
	}
	resc := make(chan runner.FromRunner, 1)
	go func() {
		res, err := p.Transport().Dispatch(context.Background(), testPool, "vm1",
			runner.ToRunner{Type: "session_rpc", Session: "sess_orphan",
				RPC: &runner.RPCEnvelope{ID: 7, Method: "ping"}})
		if err != nil {
			t.Errorf("dispatch after an orphan response: %v", err)
		}
		resc <- res
	}()
	cmd := nextSessionRPC(t, f)
	f.answerRPC(t, cmd, true, `{"pong":"yes"}`)

	res := <-resc
	if res.Type != "session_req" || res.RPC == nil || res.RPC.ID != 7 || !res.RPC.OK {
		t.Fatalf("answer = %+v, want the ok resp for envelope 7", res)
	}
	var got answer
	if err := json.Unmarshal(res.RPC.Payload, &got); err != nil || got.Pong != "yes" {
		t.Fatalf("payload = %s (%v), want the sandbox's own answer", res.RPC.Payload, err)
	}
	rc := p.conn("vm1")
	if rc == nil {
		t.Fatal("vm1 has no connection")
	}
	if n := rc.srpc.len(); n != 0 {
		t.Fatalf("%d pending entries after the answer landed, want 0", n)
	}
}

// TestSessionRequestsDoNotBlockTheRunnerReader: answering a sandbox request
// reads the host's store, so it must not run on the connection's reader —
// every result and event that runner sends is delivered by that goroutine.
func TestSessionRequestsDoNotBlockTheRunnerReader(t *testing.T) {
	h := newFakeHost()
	release := make(chan struct{})
	h.answer = func(b Binding, id control.SessionID, env runner.RPCEnvelope) runner.RPCEnvelope {
		<-release
		return runner.RPCEnvelope{OK: true}
	}
	p, ts := newTestPlaneOver(t, h)
	f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4})
	waitConnected(t, p, "vm1")
	drainAccept(t, f)

	f.write(t, runner.FromRunner{Type: "session_req", Session: "sess_slow",
		RPC: &runner.RPCEnvelope{ID: 1, Method: "mint_git_credential"}})

	// While that answer is blocked, an ordinary dispatch must still complete.
	errc := make(chan error, 1)
	go func() {
		_, err := p.Transport().Dispatch(context.Background(), testPool, "vm1",
			runner.ToRunner{Type: "destroy", Session: "sess_other"})
		errc <- err
	}()
	cmd := nextOfType(t, f, "destroy")
	f.reply(t, cmd, true, "")
	if err := <-errc; err != nil {
		t.Fatalf("dispatch while a session request was in flight: %v", err)
	}
	close(release)
}

// ---------------------------------------------------------------------------
// shutdown
// ---------------------------------------------------------------------------

// TestCloseEndsEveryConnection: Close hangs up on the whole fleet.
func TestCloseEndsEveryConnection(t *testing.T) {
	p, _, ts := newTestPlane(t)
	f1 := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4})
	f2 := startFakeRunner(t, ts, runnerScript{Name: "vm2", Total: 4})
	waitConnected(t, p, "vm1")
	waitConnected(t, p, "vm2")

	p.Close()

	for _, f := range []*fakeRunner{f1, f2} {
		if err := f.waitClosed(t); err == nil {
			t.Fatalf("%s was not closed by Close", f.name)
		}
	}
}

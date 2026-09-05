// internal/runnerd/agent_test.go
package runnerd

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/tokencanopy/rainier/internal/driver"
	"github.com/tokencanopy/rainier/protocol/runner"
)

const testToken = "testtoken"

// createTrackingFake wraps driver.Fake to record every Spec passed to
// Create, so a test can assert what the agent actually handed the driver
// (not just the fake's post-hoc state) — same pattern as
// destroyTrackingFake in runnerd_test.go.
type createTrackingFake struct {
	*driver.Fake
	mu      sync.Mutex
	created []driver.Spec
}

func newCreateTrackingFake(total int) *createTrackingFake {
	return &createTrackingFake{Fake: driver.NewFake(total)}
}

func (f *createTrackingFake) Create(ctx context.Context, spec driver.Spec) (driver.Handle, error) {
	f.mu.Lock()
	f.created = append(f.created, spec)
	f.mu.Unlock()
	return f.Fake.Create(ctx, spec)
}

func (f *createTrackingFake) createCalls() []driver.Spec {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]driver.Spec, len(f.created))
	copy(out, f.created)
	return out
}

// fakeControld stands up a bare-bones /v0/runners/connect endpoint: it
// checks the dial's bearer token, accepts the websocket, and hands the
// resulting conn to the test over a channel — one per dial, so a test can
// observe reconnects as additional values.
type fakeControld struct {
	srv   *httptest.Server
	conns chan *fakeConn
}

type fakeConn struct {
	t *testing.T
	c *websocket.Conn
}

func newFakeControld(t *testing.T, token string) *fakeControld {
	t.Helper()
	fc := &fakeControld{conns: make(chan *fakeConn, 8)}
	mux := http.NewServeMux()
	mux.HandleFunc("/v0/runners/connect", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer "+token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		c.SetReadLimit(16 << 20)
		fc.conns <- &fakeConn{t: t, c: c}
	})
	fc.srv = httptest.NewServer(mux)
	t.Cleanup(fc.srv.Close)
	return fc
}

func (fc *fakeControld) wsURL() string {
	return strings.Replace(fc.srv.URL, "http", "ws", 1)
}

// nextConn waits for the agent's next dial to land, or fails the test after
// 5s — the bound TestAgentReconnects checks its redial against.
func (fc *fakeControld) nextConn(t *testing.T) *fakeConn {
	t.Helper()
	select {
	case c := <-fc.conns:
		return c
	case <-time.After(5 * time.Second):
		t.Fatal("no controld connection within 5s")
		return nil
	}
}

func (fcn *fakeConn) readAnnounce(t *testing.T) runner.FromRunner {
	t.Helper()
	m := fcn.readMsg(t)
	if m.Type != "announce" {
		t.Fatalf("first message type = %q, want \"announce\"", m.Type)
	}
	return m
}

func (fcn *fakeConn) readMsg(t *testing.T) runner.FromRunner {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var m runner.FromRunner
	if err := wsjson.Read(ctx, fcn.c, &m); err != nil {
		t.Fatalf("read from agent: %v", err)
	}
	return m
}

func (fcn *fakeConn) send(t *testing.T, m runner.ToRunner) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := wsjson.Write(ctx, fcn.c, m); err != nil {
		t.Fatalf("write to agent: %v", err)
	}
}

// TestAgentAnnounces asserts the first message on a fresh dial is an
// announce naming the runner, carrying the registry's one seeded running
// session, and populated Used/Total (proof the capacity call actually ran,
// not just zero-valued fields).
func TestAgentAnnounces(t *testing.T) {
	fd := driver.NewFake(4)
	rd := New(fd, "", "", "")
	if err := rd.CreateWithID(context.Background(), "sess-1", driver.Spec{Image: "img"}, nil); err != nil {
		t.Fatal(err)
	}
	// A "suspending" entry (mid cold-suspend — drv.Suspend's `docker stop` in
	// flight, no HTTP path reaches this transient state deterministically,
	// so it's seeded directly) must appear as suspended_cold, not be
	// omitted — review round 2's finding: omission here is unhealable
	// (controld's reconciliation would see it in Postgres but absent from
	// the announce and mark it permanently dead), unlike "starting", which
	// stays safely omitted (see Announce's doc comment).
	rd.reg.put("sess-suspending", &sessionEntry{id: "sess-suspending", state: "suspending"})

	fc := newFakeControld(t, testToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rd.RunAgent(ctx, AgentConfig{ControldURL: fc.wsURL(), Token: testToken, RunnerName: "vm1"})

	ann := fc.nextConn(t).readAnnounce(t)
	if ann.Proto != runner.ProtocolVersion {
		t.Fatalf("Proto = %d, want %d", ann.Proto, runner.ProtocolVersion)
	}
	if ann.Runner != "vm1" {
		t.Fatalf("Runner = %q, want \"vm1\"", ann.Runner)
	}
	got := map[string]string{}
	for _, si := range ann.Sessions {
		got[si.ID] = si.State
	}
	want := map[string]string{"sess-1": "running", "sess-suspending": "suspended_cold"}
	if len(got) != len(want) {
		t.Fatalf("Sessions = %+v, want %+v", ann.Sessions, want)
	}
	for id, state := range want {
		if got[id] != state {
			t.Fatalf("Sessions[%s] state = %q, want %q (full: %+v)", id, got[id], state, ann.Sessions)
		}
	}
	if ann.Total == 0 {
		t.Fatal("Total not populated (want the fake driver's capacity)")
	}
	if ann.Used != 1 {
		t.Fatalf("Used = %d, want 1", ann.Used)
	}
}

// TestAgentExecutesCreateAndReportsResult asserts a create command reaches
// the driver with the right spec and reports back an ok result correlated
// by req_id.
func TestAgentExecutesCreateAndReportsResult(t *testing.T) {
	fd := newCreateTrackingFake(4)
	rd := New(fd, "", "", "")

	fc := newFakeControld(t, testToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rd.RunAgent(ctx, AgentConfig{ControldURL: fc.wsURL(), Token: testToken, RunnerName: "vm1"})

	conn := fc.nextConn(t)
	conn.readAnnounce(t)

	conn.send(t, runner.ToRunner{Type: "create", ReqID: 1, Session: "sess_x", Spec: &runner.Spec{Image: "img"}})
	res := conn.readMsg(t)
	if res.Type != "result" || res.ReqID != 1 || !res.OK {
		t.Fatalf("result = %+v, want ok result for req_id 1", res)
	}

	calls := fd.createCalls()
	if len(calls) != 1 {
		t.Fatalf("Create called %d times, want 1", len(calls))
	}
	if calls[0].SessionID != "sess_x" || calls[0].Image != "img" {
		t.Fatalf("Create spec = %+v, want SessionID=sess_x Image=img", calls[0])
	}
}

// TestAgentIdempotentCreate asserts a repeated create for the same session
// id (controld retrying a create it's unsure landed) returns ok without a
// second driver Create call.
func TestAgentIdempotentCreate(t *testing.T) {
	fd := newCreateTrackingFake(4)
	rd := New(fd, "", "", "")

	fc := newFakeControld(t, testToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rd.RunAgent(ctx, AgentConfig{ControldURL: fc.wsURL(), Token: testToken, RunnerName: "vm1"})

	conn := fc.nextConn(t)
	conn.readAnnounce(t)

	msg := runner.ToRunner{Type: "create", ReqID: 1, Session: "sess_x", Spec: &runner.Spec{Image: "img"}}
	conn.send(t, msg)
	res1 := conn.readMsg(t)
	if !res1.OK {
		t.Fatalf("first create result = %+v, want ok", res1)
	}

	msg.ReqID = 2
	conn.send(t, msg)
	res2 := conn.readMsg(t)
	if !res2.OK || res2.ReqID != 2 {
		t.Fatalf("second create result = %+v, want ok req_id=2", res2)
	}

	if calls := fd.createCalls(); len(calls) != 1 {
		t.Fatalf("Create called %d times, want 1 (idempotent)", len(calls))
	}

	// The ok result says the session exists; it does not say what state it is
	// in — and the controld that retried this create has a row sitting in
	// `creating` waiting to hear exactly that. So the re-create re-fires the
	// session's current state as an event, right after the result, and the
	// row converges now instead of at the next reconnect's announce. (That
	// the first create did NOT emit one is already pinned above: an event
	// queued after res1 would have been read in res2's place.)
	evt := conn.readMsg(t)
	if evt.Type != "event" || evt.Session != "sess_x" || evt.State != "running" {
		t.Fatalf("after the idempotent create: got %+v, want event{sess_x running}", evt)
	}
}

// TestCreateWithIDConcurrentSameIDCallsDriverOnce is the regression test for
// review round 1, finding 2: TestAgentIdempotentCreate's two sends are
// strictly sequential (the second is only sent after the first's result
// comes back), so it never actually exercised the race — two create
// commands for the SAME id genuinely concurrent with each other, both
// racing CreateWithID's id-claim. Before the fix, that claim was a separate
// reg.get (in execute) followed by a separate reg.put (in CreateWithID) —
// two distinct lock acquisitions with a window where both could observe
// "absent" and both reach drv.Create, which has no id-uniqueness guard of
// its own. This drives CreateWithID directly from many goroutines released
// simultaneously off one barrier channel (rather than over the WS conn,
// where read-loop scheduling timing would make the actual overlap
// non-deterministic and this test flaky) and asserts the driver saw exactly
// one Create call, with every caller getting a success outcome (nil or
// errSessionExists) — never a rejection.
func TestCreateWithIDConcurrentSameIDCallsDriverOnce(t *testing.T) {
	fd := newCreateTrackingFake(4)
	rd := New(fd, "", "", "")

	const n = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = rd.CreateWithID(context.Background(), "sess_race", driver.Spec{Image: "img"}, nil)
		}(i)
	}
	close(start) // release all n goroutines at once, maximizing overlap
	wg.Wait()

	for i, err := range errs {
		if err != nil && !errors.Is(err, errSessionExists) {
			t.Fatalf("goroutine %d: CreateWithID = %v, want nil or errSessionExists", i, err)
		}
	}
	if calls := fd.createCalls(); len(calls) != 1 {
		t.Fatalf("Create called %d times, want exactly 1; calls=%+v", len(calls), calls)
	}
	if calls := fd.createCalls(); calls[0].SessionID != "sess_race" || calls[0].Image != "img" {
		t.Fatalf("Create spec = %+v, want SessionID=sess_race Image=img", calls[0])
	}
}

// TestAgentForwardsEvents asserts a sessiond registering over the local HTTP
// surface (as tests today simulate a container's sessiond) forwards a
// "running" event over the control conn.
func TestAgentForwardsEvents(t *testing.T) {
	fd := driver.NewFake(4)
	rd := New(fd, "", "", "")
	httpSrv := httptest.NewServer(rd.Handler())
	defer httpSrv.Close()
	wsBase := strings.Replace(httpSrv.URL, "http", "ws", 1)

	fc := newFakeControld(t, testToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rd.RunAgent(ctx, AgentConfig{ControldURL: fc.wsURL(), Token: testToken, RunnerName: "vm1"})

	conn := fc.nextConn(t)
	conn.readAnnounce(t)

	conn.send(t, runner.ToRunner{Type: "create", ReqID: 1, Session: "sess_evt", Spec: &runner.Spec{Image: "img"}})
	res := conn.readMsg(t)
	if !res.OK {
		t.Fatalf("create result = %+v, want ok", res)
	}

	dialRegisterAndServe(t, context.Background(), wsBase, "sess_evt")

	evt := conn.readMsg(t)
	if evt.Type != "event" || evt.Session != "sess_evt" || evt.State != "running" {
		t.Fatalf("event = %+v, want event{sess_evt running}", evt)
	}
}

// TestAgentReconnects asserts a closed controld conn is redialed — a second
// announce lands within nextConn's 5s bound.
func TestAgentReconnects(t *testing.T) {
	fd := driver.NewFake(4)
	rd := New(fd, "", "", "")

	fc := newFakeControld(t, testToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rd.RunAgent(ctx, AgentConfig{ControldURL: fc.wsURL(), Token: testToken, RunnerName: "vm1"})

	conn1 := fc.nextConn(t)
	conn1.readAnnounce(t)
	conn1.c.Close(websocket.StatusNormalClosure, "bye")

	conn2 := fc.nextConn(t)
	conn2.readAnnounce(t)
}

// TestDialAttachBackChecksTargetOrigin pins the dial-back's origin guard: the
// dial carries the fleet runner token, so a dial_attach naming any host other
// than this runner's own controld must be refused BEFORE the dial — otherwise
// a compromised or misrouted control conn turns into token exfiltration. The
// probe server 404s every request, so a permitted dial fails immediately
// (nothing to wait for) while still proving it was attempted.
func TestDialAttachBackChecksTargetOrigin(t *testing.T) {
	var hits atomic.Int64
	probe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.Error(w, "no", http.StatusNotFound)
	}))
	defer probe.Close()
	probeWS := strings.Replace(probe.URL, "http", "ws", 1)

	dial := func(t *testing.T, controldURL string) int64 {
		t.Helper()
		hits.Store(0)
		rd := New(driver.NewFake(4), "", "", "")
		msg := runner.ToRunner{Type: "dial_attach", Session: "sess_x", Attach: &runner.Attach{
			AttachID: "0123456789abcdef", Cols: 80, Rows: 24,
			TargetURL: probeWS + "/v0/runners/attach-back?attach_id=0123456789abcdef",
		}}
		rd.dialAttachBack(context.Background(), msg, AgentConfig{ControldURL: controldURL, Token: testToken})
		return hits.Load()
	}

	t.Run("foreign host is never dialed", func(t *testing.T) {
		if n := dial(t, "ws://controld.invalid:9090"); n != 0 {
			t.Fatalf("dialed a target that is not this runner's controld (%d requests)", n)
		}
	})

	t.Run("own controld is dialed", func(t *testing.T) {
		if n := dial(t, probeWS); n != 1 {
			t.Fatalf("attach-back requests to this runner's own controld = %d, want 1", n)
		}
	})
}

// TestSameControld pins the origin comparison itself, including the two cases
// the e2e can't reach: controld deriving a ws(s) target_url from its own
// http(s) ExternalURL, and a wss→ws downgrade (which would put the fleet
// token on the wire in the clear).
func TestSameControld(t *testing.T) {
	for _, tc := range []struct {
		name             string
		controld, target string
		want             bool
	}{
		{"identical", "ws://c:9090", "ws://c:9090/v0/runners/attach-back?attach_id=a", true},
		{"http external url", "wss://c.example", "wss://c.example/v0/runners/attach-back", true},
		{"default port implied", "wss://c.example:443", "wss://c.example/x", true},
		{"http vs ws scheme alias", "ws://c:80", "http://c/x", true},
		{"foreign host", "ws://c:9090", "ws://evil.example:9090/x", false},
		{"foreign port", "ws://c:9090", "ws://c:9091/x", false},
		{"tls downgrade", "wss://c.example", "ws://c.example/x", false},
		{"unknown scheme", "ws://c:9090", "file:///etc/passwd", false},
		{"empty target", "ws://c:9090", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := sameControld(tc.controld, tc.target); got != tc.want {
				t.Fatalf("sameControld(%q, %q) = %v, want %v", tc.controld, tc.target, got, tc.want)
			}
		})
	}
}

// waitForAgentWriterCount polls rd's agentWriterCount until it equals want,
// or fails the test after 2s. Deterministic and flake-free by construction
// (unlike runtime.NumGoroutine(), which counts every goroutine in the test
// binary): agentSession now increments this before starting its writer and
// decrements it in that goroutine's own deferred cleanup, and — the actual
// fix under test — agentSession does not return to RunAgent's loop until
// writerDone.Wait() confirms the writer has already stopped. Since RunAgent
// never runs two agentSession calls concurrently, this count can only ever
// be observed as 0 or 1 for a single Server; anything else, or a value that
// never settles, means a writer leaked.
func waitForAgentWriterCount(t *testing.T, rd *Server, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if n := rd.agentWriterCount.Load(); n == want {
			return
		} else if time.Now().After(deadline) {
			t.Fatalf("agentWriterCount = %d after 2s, want %d", n, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestAgentReconnectDoesNotLeakWriterGoroutine is the regression test for
// review round 1, finding 3: a quiet disconnect — one where the writer never
// saw a failed send, e.g. the reader simply hit a read error first — used to
// leave the PREVIOUS connection's writer goroutine (and the `out` channel it
// closed over) blocked forever, since a future failed send was its only
// exit signal. By the time a SECOND announce arrives (proof RunAgent has
// already looped back into a fresh agentSession — which only happens after
// the first agentSession call has fully returned), the first connection's
// writer must already be gone.
func TestAgentReconnectDoesNotLeakWriterGoroutine(t *testing.T) {
	fd := driver.NewFake(4)
	rd := New(fd, "", "", "")

	fc := newFakeControld(t, testToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rd.RunAgent(ctx, AgentConfig{ControldURL: fc.wsURL(), Token: testToken, RunnerName: "vm1"})

	conn1 := fc.nextConn(t)
	conn1.readAnnounce(t)
	waitForAgentWriterCount(t, rd, 1) // conn1's writer is up

	conn1.c.Close(websocket.StatusNormalClosure, "bye") // quiet disconnect: no failed send ever occurs

	conn2 := fc.nextConn(t)
	conn2.readAnnounce(t)
	// conn1's writer must already be gone by now — not still lingering
	// alongside conn2's — or this settles at 2, never 1.
	waitForAgentWriterCount(t, rd, 1)

	cancel() // shut the agent down entirely
	waitForAgentWriterCount(t, rd, 0)
}

// blockingPrepullFake wraps driver.Fake with a Prepull that parks until the
// test releases it, so a test can hold one command mid-flight and prove the
// next one is still served. Same shadowing pattern as slowFake in
// runnerd_test.go: every other driver method goes straight through.
type blockingPrepullFake struct {
	*driver.Fake
	entered chan string
	release chan struct{}
}

func newBlockingPrepullFake(total int) *blockingPrepullFake {
	return &blockingPrepullFake{Fake: driver.NewFake(total), entered: make(chan string, 1), release: make(chan struct{})}
}

func (f *blockingPrepullFake) Prepull(ctx context.Context, ref string) error {
	f.entered <- ref
	<-f.release
	return f.Fake.Prepull(ctx, ref)
}

// TestAgentSnapshotUsesTheCommandRef: controld content-addresses an
// environment's cached image and sends the runner that exact tag, so a
// snapshot command's ref must reach the driver and come back as the result's
// detail verbatim. Before Task 6 the agent had no way to pass one — every
// snapshot got a driver-minted rainier-snap: tag, which controld could not
// have addressed a later create against.
func TestAgentSnapshotUsesTheCommandRef(t *testing.T) {
	fd := driver.NewFake(4)
	rd := New(fd, "", "", "")
	if err := rd.CreateWithID(context.Background(), "sess_env", driver.Spec{Image: "img"}, nil); err != nil {
		t.Fatal(err)
	}

	fc := newFakeControld(t, testToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rd.RunAgent(ctx, AgentConfig{ControldURL: fc.wsURL(), Token: testToken, RunnerName: "vm1"})

	conn := fc.nextConn(t)
	conn.readAnnounce(t)

	const ref = "rainier-env:e-1"
	conn.send(t, runner.ToRunner{Type: "snapshot", ReqID: 9, Session: "sess_env", Ref: ref})
	res := conn.readMsg(t)
	if res.Type != "result" || res.ReqID != 9 || !res.OK {
		t.Fatalf("result = %+v, want an ok result for req_id 9", res)
	}
	if res.Detail != ref {
		t.Fatalf("result detail = %q, want the commanded ref %q", res.Detail, ref)
	}
}

// TestAgentRemoveWorkspaceCommand pins the command controld sends to reclaim
// a workspace the crash path kept: it names a SESSION (the volume is derived
// from the session id, not from a container that no longer exists), it is
// fire-and-forget — req_id 0, nobody waiting — and an already-absent volume is
// an ok result, because every caller of it is a teardown that may be running
// second.
func TestAgentRemoveWorkspaceCommand(t *testing.T) {
	fd := driver.NewFake(4)
	rd := New(fd, "", "", "")
	ctx := context.Background()
	if err := rd.CreateWithID(ctx, "sess_ws", driver.Spec{Image: "img", SessionID: "sess_ws"}, nil); err != nil {
		t.Fatal(err)
	}
	// The crash already took the container and the registry entry; only the
	// volume is left, which is the state this command exists for.
	handle, _, ok := rd.reg.opTarget("sess_ws")
	if !ok {
		t.Fatal("session missing right after create")
	}
	if err := fd.DestroyContainer(ctx, handle); err != nil {
		t.Fatal(err)
	}
	rd.reg.remove("sess_ws")
	if !slices.Contains(fd.Volumes(), "rainier-ws-sess_ws") {
		t.Fatalf("no kept workspace to reclaim: %v", fd.Volumes())
	}

	fc := newFakeControld(t, testToken)
	actx, cancel := context.WithCancel(ctx)
	defer cancel()
	go rd.RunAgent(actx, AgentConfig{ControldURL: fc.wsURL(), Token: testToken, RunnerName: "vm1"})

	conn := fc.nextConn(t)
	conn.readAnnounce(t)

	conn.send(t, runner.ToRunner{Type: "remove_workspace", Session: "sess_ws"})
	res := conn.readMsg(t)
	if res.Type != "result" || !res.OK || res.ReqID != 0 {
		t.Fatalf("result = %+v, want an ok fire-and-forget result", res)
	}
	if slices.Contains(fd.Volumes(), "rainier-ws-sess_ws") {
		t.Fatalf("remove_workspace left the volume behind: %v", fd.Volumes())
	}

	// Again, against nothing: still ok. controld sends this on the explicit-rm
	// path whether or not a full destroy already took the volume.
	conn.send(t, runner.ToRunner{Type: "remove_workspace", Session: "sess_ws"})
	if res := conn.readMsg(t); res.Type != "result" || !res.OK {
		t.Fatalf("second result = %+v, want ok for an already-absent workspace", res)
	}
}

// The fleet wire must not turn a real driver teardown failure back into an
// apparent success. controld relies on OK=false to leave the row unchanged so
// a retry or reconciliation can still reach the runner's retained entry.
func TestAgentDestroyReportsDriverFailureAndAllowsRetry(t *testing.T) {
	wantErr := errors.New("synthetic destroy failure")
	fd := &failOnceDestroyFake{Fake: driver.NewFake(4), err: wantErr}
	rd := New(fd, "", "", "")
	if err := rd.CreateWithID(context.Background(), "sess_destroy", driver.Spec{Image: "img"}, nil); err != nil {
		t.Fatal(err)
	}

	fc := newFakeControld(t, testToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rd.RunAgent(ctx, AgentConfig{ControldURL: fc.wsURL(), Token: testToken, RunnerName: "vm1"})

	conn := fc.nextConn(t)
	conn.readAnnounce(t)

	conn.send(t, runner.ToRunner{Type: "destroy", ReqID: 31, Session: "sess_destroy"})
	res := conn.readMsg(t)
	if res.Type != "result" || res.ReqID != 31 || res.OK || res.Detail != wantErr.Error() {
		t.Fatalf("first destroy result = %+v, want failed req_id 31 carrying %q", res, wantErr)
	}
	if _, _, ok := rd.reg.opTarget("sess_destroy"); !ok {
		t.Fatal("failed destroy disappeared from the runner registry")
	}

	conn.send(t, runner.ToRunner{Type: "destroy", ReqID: 32, Session: "sess_destroy"})
	res = conn.readMsg(t)
	if res.Type != "result" || res.ReqID != 32 || !res.OK {
		t.Fatalf("retry destroy result = %+v, want ok req_id 32", res)
	}
	if _, _, ok := rd.reg.opTarget("sess_destroy"); ok {
		t.Fatal("successful retry left the runner registry entry behind")
	}
}

// TestAgentPrepullPullsAndReports: prepull is advisory — controld dispatches
// it with no pending entry to correlate against, so the command carries
// neither a session nor a req_id, and the agent must handle both absences.
// The result is informational: ok plus the ref it pulled.
func TestAgentPrepullPullsAndReports(t *testing.T) {
	fd := driver.NewFake(4)
	rd := New(fd, "", "", "")

	fc := newFakeControld(t, testToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rd.RunAgent(ctx, AgentConfig{ControldURL: fc.wsURL(), Token: testToken, RunnerName: "vm1"})

	conn := fc.nextConn(t)
	conn.readAnnounce(t)

	const ref = "rainier-env:e-2"
	conn.send(t, runner.ToRunner{Type: "prepull", Ref: ref})
	res := conn.readMsg(t)
	if res.Type != "result" || !res.OK {
		t.Fatalf("result = %+v, want an ok result", res)
	}
	if res.ReqID != 0 {
		t.Fatalf("result req_id = %d, want 0 echoed back from the ref-only command", res.ReqID)
	}
	if res.Detail != ref {
		t.Fatalf("result detail = %q, want the pulled ref %q", res.Detail, ref)
	}
	// The result is sent only after Prepull returns, so by now the driver has
	// certainly recorded it.
	if got := fd.Pulls(); len(got) != 1 || got[0] != ref {
		t.Fatalf("driver pulls = %v, want exactly [%s]", got, ref)
	}
}

// TestAgentPrepullFailureReportsTheError: a prepull for an image that cannot
// be fetched must come back as a failed result carrying the reason, not a
// silent drop — controld logs it, and a runner that answered nothing would
// look indistinguishable from one that pulled fine.
func TestAgentPrepullFailureReportsTheError(t *testing.T) {
	fd := driver.NewFake(4)
	rd := New(fd, "", "", "")

	fc := newFakeControld(t, testToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rd.RunAgent(ctx, AgentConfig{ControldURL: fc.wsURL(), Token: testToken, RunnerName: "vm1"})

	conn := fc.nextConn(t)
	conn.readAnnounce(t)

	// An empty ref is the one failure every driver rejects identically (see
	// driver.Fake.Prepull / Docker.Prepull), so it needs no fake of its own.
	conn.send(t, runner.ToRunner{Type: "prepull", ReqID: 4})
	res := conn.readMsg(t)
	if res.Type != "result" || res.ReqID != 4 || res.OK {
		t.Fatalf("result = %+v, want a failed result for req_id 4", res)
	}
	if res.Detail == "" {
		t.Fatal("failed prepull result carried no detail; want the error text")
	}
}

// TestAgentPrepullDoesNotBlockTheReader: `docker pull` of a cold image runs
// for minutes, and a prepull is exactly the command a runner receives while
// real work is in flight. If it were served inline on the read loop, every
// other command — the create the prepull was warming up FOR, included — would
// queue behind it. This parks a prepull mid-pull and proves a later snapshot
// is served and answered while it is still running.
func TestAgentPrepullDoesNotBlockTheReader(t *testing.T) {
	fd := newBlockingPrepullFake(4)
	rd := New(fd, "", "", "")
	if err := rd.CreateWithID(context.Background(), "sess_par", driver.Spec{Image: "img"}, nil); err != nil {
		t.Fatal(err)
	}

	fc := newFakeControld(t, testToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rd.RunAgent(ctx, AgentConfig{ControldURL: fc.wsURL(), Token: testToken, RunnerName: "vm1"})

	conn := fc.nextConn(t)
	conn.readAnnounce(t)

	conn.send(t, runner.ToRunner{Type: "prepull", ReqID: 1, Ref: "rainier-env:slow"})
	select {
	case <-fd.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("prepull never reached the driver")
	}

	// Sent while the prepull is parked inside the driver.
	conn.send(t, runner.ToRunner{Type: "snapshot", ReqID: 2, Session: "sess_par", Ref: "rainier-env:fast"})
	res := conn.readMsg(t)
	if res.ReqID != 2 || !res.OK || res.Detail != "rainier-env:fast" {
		t.Fatalf("first result = %+v, want the snapshot's (req_id 2) — the prepull is blocking the reader", res)
	}

	close(fd.release)
	res = conn.readMsg(t)
	if res.ReqID != 1 || !res.OK {
		t.Fatalf("second result = %+v, want the released prepull's (req_id 1)", res)
	}
}

// TestAgentCreateCarriesSetupAndEnvToTheDriver pins the mapping controld's
// environment resolution depends on: the setup script, its timeout, and the
// resolved environment (declared vars plus decrypted secret values) all have
// to reach driver.Spec, or a session boots with none of its environment and
// no setup ever runs. The runner→driver hop is the only place that can drop
// them silently, since both sides have a field of the same name.
func TestAgentCreateCarriesSetupAndEnvToTheDriver(t *testing.T) {
	fd := newCreateTrackingFake(4)
	rd := New(fd, "", "", "")

	fc := newFakeControld(t, testToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rd.RunAgent(ctx, AgentConfig{ControldURL: fc.wsURL(), Token: testToken, RunnerName: "vm1"})

	conn := fc.nextConn(t)
	conn.readAnnounce(t)

	const setup = "#!/bin/sh\nnpm ci\n"
	conn.send(t, runner.ToRunner{Type: "create", ReqID: 1, Session: "sess_env", Spec: &runner.Spec{
		Image:           "img",
		Setup:           setup,
		SetupTimeoutSec: 900,
		Env:             map[string]string{"NODE_ENV": "test", "TOKEN": "s3cret"},
	}})
	if res := conn.readMsg(t); !res.OK {
		t.Fatalf("create result = %+v, want ok", res)
	}

	calls := fd.createCalls()
	if len(calls) != 1 {
		t.Fatalf("Create called %d times, want 1", len(calls))
	}
	got := calls[0]
	if got.Setup != setup {
		t.Errorf("Spec.Setup = %q, want %q", got.Setup, setup)
	}
	if got.SetupTimeoutSec != 900 {
		t.Errorf("Spec.SetupTimeoutSec = %d, want 900", got.SetupTimeoutSec)
	}
	want := map[string]string{"NODE_ENV": "test", "TOKEN": "s3cret"}
	if !reflect.DeepEqual(got.Env, want) {
		t.Errorf("Spec.Env = %v, want %v", got.Env, want)
	}
}

// TestAgentCreateCarriesReposInitAndAttribution pins the other half of the
// create mapping: the repositories controld resolved for this session, the
// per-boot init hook, and the git identity its commits are attributed to all
// reach the driver unchanged. runnerd decides none of it — controld expanded
// the environment's connectors and the owner's GitHub account into these
// fields — so the only thing that can go wrong here is a field this hop
// forgets to copy, which would produce a container that silently clones
// nothing.
func TestAgentCreateCarriesReposInitAndAttribution(t *testing.T) {
	fd := newCreateTrackingFake(4)
	rd := New(fd, "", "", "")

	fc := newFakeControld(t, testToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rd.RunAgent(ctx, AgentConfig{ControldURL: fc.wsURL(), Token: testToken, RunnerName: "vm1"})

	conn := fc.nextConn(t)
	conn.readAnnounce(t)

	repos := []runner.RepoSpec{
		{Owner: "acme", Name: "app", BaseBranch: "main", SessionBranch: "rainier/work", Dir: "app"},
		{Owner: "other", Name: "app", BaseBranch: "dev", SessionBranch: "rainier/work", Dir: "other__app"},
	}
	conn.send(t, runner.ToRunner{Type: "create", ReqID: 1, Session: "sess_repo", Spec: &runner.Spec{
		Image: "img", Repos: repos,
		Init: "make dev-server &", InitTimeoutSec: 120,
		GitAuthorName: "alice", GitAuthorEmail: "42+alice@users.noreply.github.com",
	}})
	if res := conn.readMsg(t); !res.OK {
		t.Fatalf("create result = %+v, want ok", res)
	}

	calls := fd.createCalls()
	if len(calls) != 1 {
		t.Fatalf("Create called %d times, want 1", len(calls))
	}
	got := calls[0]
	want := []driver.RepoSpec{
		{Owner: "acme", Name: "app", BaseBranch: "main", SessionBranch: "rainier/work", Dir: "app"},
		{Owner: "other", Name: "app", BaseBranch: "dev", SessionBranch: "rainier/work", Dir: "other__app"},
	}
	if !reflect.DeepEqual(got.Repos, want) {
		t.Errorf("Spec.Repos = %+v, want %+v", got.Repos, want)
	}
	if got.Init != "make dev-server &" || got.InitTimeoutSec != 120 {
		t.Errorf("Spec init = %q/%d, want the dispatched hook and bound", got.Init, got.InitTimeoutSec)
	}
	if got.GitAuthorName != "alice" || got.GitAuthorEmail != "42+alice@users.noreply.github.com" {
		t.Errorf("Spec attribution = %q/%q, want alice's", got.GitAuthorName, got.GitAuthorEmail)
	}

	// A create carrying none of it leaves all of it zero: a scratch session
	// clones nothing and commits as nobody in particular.
	conn.send(t, runner.ToRunner{Type: "create", ReqID: 2, Session: "sess_bare",
		Spec: &runner.Spec{Image: "img"}})
	if res := conn.readMsg(t); !res.OK {
		t.Fatalf("create result = %+v, want ok", res)
	}
	calls = fd.createCalls()
	if len(calls) != 2 {
		t.Fatalf("Create called %d times, want 2", len(calls))
	}
	if bare := calls[1]; bare.Repos != nil || bare.Init != "" || bare.InitTimeoutSec != 0 ||
		bare.GitAuthorName != "" || bare.GitAuthorEmail != "" {
		t.Fatalf("Spec = %+v, want no repos, init or attribution", bare)
	}
}

// TestAgentCreateWithoutSetupLeavesTheSpecEmpty: a session whose environment
// was already snapshot-cached carries no setup at all (the image IS the
// finished setup), and a spec that invented one would make every cached
// create re-run it.
func TestAgentCreateWithoutSetupLeavesTheSpecEmpty(t *testing.T) {
	fd := newCreateTrackingFake(4)
	rd := New(fd, "", "", "")

	fc := newFakeControld(t, testToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rd.RunAgent(ctx, AgentConfig{ControldURL: fc.wsURL(), Token: testToken, RunnerName: "vm1"})

	conn := fc.nextConn(t)
	conn.readAnnounce(t)
	conn.send(t, runner.ToRunner{Type: "create", ReqID: 1, Session: "sess_cached",
		Spec: &runner.Spec{Image: "rainier-env:e1-abc123"}})
	if res := conn.readMsg(t); !res.OK {
		t.Fatalf("create result = %+v, want ok", res)
	}

	calls := fd.createCalls()
	if len(calls) != 1 {
		t.Fatalf("Create called %d times, want 1", len(calls))
	}
	if calls[0].Setup != "" || calls[0].SetupTimeoutSec != 0 || calls[0].Env != nil {
		t.Fatalf("Spec = %+v, want no setup and no env for a cached-image create", calls[0])
	}
}

// ---------------------------------------------------------------------------
// capability negotiation (plan 8, D19): what the runner claims, what controld
// grants, and the two generations every later message carries
// ---------------------------------------------------------------------------

// TestAgentAnnouncesItsCapabilities pins the operator's configured
// capabilities onto the announce verbatim and in order. They are claims about
// this runner and nothing else, so the agent neither invents nor reorders
// them; controld is where they are validated.
func TestAgentAnnouncesItsCapabilities(t *testing.T) {
	rd := New(driver.NewFake(4), "", "", "")

	fc := newFakeControld(t, testToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	caps := []string{"gpu", "docker.rootless"}
	go rd.RunAgent(ctx, AgentConfig{ControldURL: fc.wsURL(), Token: testToken, RunnerName: "vm1",
		Capabilities: caps})

	ann := fc.nextConn(t).readAnnounce(t)
	if !slices.Equal(ann.Capabilities, caps) {
		t.Fatalf("announce Capabilities = %v, want %v", ann.Capabilities, caps)
	}
}

// TestAgentStampsTheAcceptedGeneration: before an accept the runner has no
// granted authority and says so (zero — "the connection's"); after one, every
// result and event it sends carries the generation controld granted, which is
// what lets the store fence a report that outlived its own connection.
func TestAgentStampsTheAcceptedGeneration(t *testing.T) {
	rd := New(newCreateTrackingFake(4), "", "", "")

	fc := newFakeControld(t, testToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rd.RunAgent(ctx, AgentConfig{ControldURL: fc.wsURL(), Token: testToken, RunnerName: "vm1"})

	conn := fc.nextConn(t)
	conn.readAnnounce(t)

	msg := runner.ToRunner{Type: "create", ReqID: 1, Session: "sess_gen", Spec: &runner.Spec{Image: "img"}}
	conn.send(t, msg)
	if res := conn.readMsg(t); !res.OK || res.Generation != 0 {
		t.Fatalf("result before any accept = %+v, want ok with Generation 0", res)
	}

	conn.send(t, runner.ToRunner{Type: "accept", Generation: 7, Capabilities: []string{"gpu"}})

	// The same create again: idempotent, so it answers with a result AND
	// re-fires the session's state as an event — two messages that must both
	// carry the granted generation.
	msg.ReqID = 2
	conn.send(t, msg)
	res := conn.readMsg(t)
	if !res.OK || res.ReqID != 2 || res.Generation != 7 {
		t.Fatalf("result after the accept = %+v, want ok req_id=2 Generation=7", res)
	}
	evt := conn.readMsg(t)
	if evt.Type != "event" || evt.Session != "sess_gen" || evt.Generation != 7 {
		t.Fatalf("event after the accept = %+v, want event{sess_gen} at Generation 7", evt)
	}
}

// TestAgentEchoesThePlacementGeneration: the create that starts a sandbox
// carries the session's placement generation, the runner keeps it with that
// sandbox, and every event about it echoes it — so a report from a sandbox
// the session has already re-placed elsewhere is fenced by the session's own
// authority. A session created without one echoes nothing.
func TestAgentEchoesThePlacementGeneration(t *testing.T) {
	rd := New(newCreateTrackingFake(4), "", "", "")

	fc := newFakeControld(t, testToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rd.RunAgent(ctx, AgentConfig{ControldURL: fc.wsURL(), Token: testToken, RunnerName: "vm1"})

	conn := fc.nextConn(t)
	conn.readAnnounce(t)

	for _, tc := range []struct {
		name    string
		session string
		gen     uint64
	}{
		{"a create that carried one", "sess_placed", 3},
		{"a create that carried none", "sess_unplaced", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := runner.ToRunner{Type: "create", ReqID: 1, Session: tc.session,
				PlacementGeneration: tc.gen, Spec: &runner.Spec{Image: "img"}}
			conn.send(t, msg)
			if res := conn.readMsg(t); !res.OK {
				t.Fatalf("create result = %+v, want ok", res)
			}
			msg.ReqID = 2
			conn.send(t, msg)
			if res := conn.readMsg(t); !res.OK {
				t.Fatalf("idempotent create result = %+v, want ok", res)
			}
			evt := conn.readMsg(t)
			if evt.Type != "event" || evt.Session != tc.session || evt.PlacementGeneration != tc.gen {
				t.Fatalf("event = %+v, want event{%s} at PlacementGeneration %d", evt, tc.session, tc.gen)
			}
		})
	}
}

// TestAgentCreateCarriesTheAgentHome: the home is one more field controld
// resolved and this runner hands to the driver — a volume name and a path,
// copied and neither parsed nor defaulted. Absent stays absent: a session with
// no creator, and every create a control plane older than the field sent, must
// reach the driver with nothing to mount rather than with an empty instruction
// the driver would have to guess about.
func TestAgentCreateCarriesTheAgentHome(t *testing.T) {
	fd := newCreateTrackingFake(4)
	rd := New(fd, "", "", "")

	fc := newFakeControld(t, testToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rd.RunAgent(ctx, AgentConfig{ControldURL: fc.wsURL(), Token: testToken, RunnerName: "vm1"})

	conn := fc.nextConn(t)
	conn.readAnnounce(t)

	conn.send(t, runner.ToRunner{Type: "create", ReqID: 1, Session: "sess_example", Spec: &runner.Spec{
		Image: "img",
		Home:  &runner.HomeMount{Volume: "rainier-agents-0123456789abcdef", Path: "/rainier/agents"},
	}})
	if res := conn.readMsg(t); !res.OK {
		t.Fatalf("create result = %+v, want ok", res)
	}
	calls := fd.createCalls()
	if len(calls) != 1 {
		t.Fatalf("Create called %d times, want 1", len(calls))
	}
	want := &driver.HomeMount{Volume: "rainier-agents-0123456789abcdef", Path: "/rainier/agents"}
	if got := calls[0].Home; !reflect.DeepEqual(got, want) {
		t.Errorf("Spec.Home = %+v, want %+v", got, want)
	}

	conn.send(t, runner.ToRunner{Type: "create", ReqID: 2, Session: "sess_example2",
		Spec: &runner.Spec{Image: "img"}})
	if res := conn.readMsg(t); !res.OK {
		t.Fatalf("create result = %+v, want ok", res)
	}
	calls = fd.createCalls()
	if len(calls) != 2 {
		t.Fatalf("Create called %d times, want 2", len(calls))
	}
	if calls[1].Home != nil {
		t.Errorf("a create with no home reached the driver with %+v, want nil", calls[1].Home)
	}
}

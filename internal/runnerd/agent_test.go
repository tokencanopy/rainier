// internal/runnerd/agent_test.go
package runnerd

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"rainier/internal/driver"
	"rainier/internal/rwire"
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

// fakeControld stands up a bare-bones /v1/runners/connect endpoint: it
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
	mux.HandleFunc("/v1/runners/connect", func(w http.ResponseWriter, r *http.Request) {
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

func (fcn *fakeConn) readAnnounce(t *testing.T) rwire.FromRunner {
	t.Helper()
	m := fcn.readMsg(t)
	if m.Type != "announce" {
		t.Fatalf("first message type = %q, want \"announce\"", m.Type)
	}
	return m
}

func (fcn *fakeConn) readMsg(t *testing.T) rwire.FromRunner {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var m rwire.FromRunner
	if err := wsjson.Read(ctx, fcn.c, &m); err != nil {
		t.Fatalf("read from agent: %v", err)
	}
	return m
}

func (fcn *fakeConn) send(t *testing.T, m rwire.ToRunner) {
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
	rd := New(fd, "", "")
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
	if ann.Proto != rwire.Proto {
		t.Fatalf("Proto = %d, want %d", ann.Proto, rwire.Proto)
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
	rd := New(fd, "", "")

	fc := newFakeControld(t, testToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rd.RunAgent(ctx, AgentConfig{ControldURL: fc.wsURL(), Token: testToken, RunnerName: "vm1"})

	conn := fc.nextConn(t)
	conn.readAnnounce(t)

	conn.send(t, rwire.ToRunner{Type: "create", ReqID: 1, Session: "sess_x", Spec: &rwire.Spec{Image: "img"}})
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
	rd := New(fd, "", "")

	fc := newFakeControld(t, testToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rd.RunAgent(ctx, AgentConfig{ControldURL: fc.wsURL(), Token: testToken, RunnerName: "vm1"})

	conn := fc.nextConn(t)
	conn.readAnnounce(t)

	msg := rwire.ToRunner{Type: "create", ReqID: 1, Session: "sess_x", Spec: &rwire.Spec{Image: "img"}}
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
	rd := New(fd, "", "")

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
	rd := New(fd, "", "")
	httpSrv := httptest.NewServer(rd.Handler())
	defer httpSrv.Close()
	wsBase := strings.Replace(httpSrv.URL, "http", "ws", 1)

	fc := newFakeControld(t, testToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rd.RunAgent(ctx, AgentConfig{ControldURL: fc.wsURL(), Token: testToken, RunnerName: "vm1"})

	conn := fc.nextConn(t)
	conn.readAnnounce(t)

	conn.send(t, rwire.ToRunner{Type: "create", ReqID: 1, Session: "sess_evt", Spec: &rwire.Spec{Image: "img"}})
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
	rd := New(fd, "", "")

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
	rd := New(fd, "", "")

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

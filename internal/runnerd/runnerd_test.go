// internal/runnerd/runnerd_test.go
package runnerd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"rainier/internal/driver"
	"rainier/internal/relay"
	"rainier/internal/session"
	"rainier/internal/wire"
)

// This test wires the real relay end-to-end without Docker: create a session
// via the API, then simulate the container by dialing /register from an
// in-process sessiond bound to a real session.Session, then attach a client
// via /attach and assert output flows.
func TestRunnerdCreateRegisterAttach(t *testing.T) {
	srv := httptest.NewServer(New(driver.NewFake(4), "", "", "").Handler())
	defer srv.Close()
	base := strings.Replace(srv.URL, "http", "ws", 1)
	ctx := context.Background()

	// Create.
	id := createSession(t, srv.URL) // helper: POST /sessions, returns session_id

	// Simulate the container's sessiond dialing /register.
	dialRegisterAndServe(t, ctx, base, id)

	// Attach a client.
	cli, _, err := websocket.Dial(ctx, base+"/attach?session="+id, nil)
	if err != nil {
		t.Fatal(err)
	}
	cli.SetReadLimit(16 << 20)
	wsjson.Write(ctx, cli, wire.ClientMsg{Type: "resize", Cols: 80, Rows: 24})

	// Expect snapshot then echoed marker.
	wsjson.Write(ctx, cli, wire.ClientMsg{Type: "stdin", Data: []byte("echo runnerd-marker\n")})
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("no runnerd-marker through runnerd relay")
		default:
		}
		var m wire.ServerMsg
		if err := wsjson.Read(ctx, cli, &m); err != nil {
			t.Fatal(err)
		}
		if m.Type == "output" && strings.Contains(string(m.Data), "runnerd-marker") {
			return
		}
	}
}

func createSession(t *testing.T, baseURL string) string {
	t.Helper()
	id, err := postSession(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Fatal("empty session_id")
	}
	return id
}

// postSession is createSession's error-returning core, split out so the
// concurrent-create test below can call it from goroutines without calling
// t.Fatal off the test goroutine (FailNow/Fatal are documented as unsafe to
// call from anywhere else).
func postSession(baseURL string) (string, error) {
	body, _ := json.Marshal(map[string]any{"name": "t", "image": "x"})
	resp, err := http.Post(baseURL+"/sessions", "application/json", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var out struct {
		SessionID string `json:"session_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	return out.SessionID, nil
}

// TestNewPlumbsProxyURLToHTTPOnlyCreate is the regression test for Task 13's
// finding that the HTTP-only surface (cmd/runnerd with no --controld set —
// today's default, and every dev/CI compose run) never got a proxy URL onto
// driver.Spec at all: RunAgent set s.proxyURL from AgentConfig.ProxyURL, but
// New (which is all the HTTP-only path ever calls) had no way to set it,
// so CreateWithID's `spec.ProxyURL = s.proxyURL` was always "" outside
// agent (dial) mode. New must accept a proxyURL and CreateWithID must carry
// it onto every driver.Spec it builds, exactly like dialBase/egressAdmin.
func TestNewPlumbsProxyURLToHTTPOnlyCreate(t *testing.T) {
	fd := driver.NewFake(4)
	srv := httptest.NewServer(New(fd, "", "", "http://gw:3128").Handler())
	defer srv.Close()

	createSession(t, srv.URL)

	if got := fd.LastSpec().ProxyURL; got != "http://gw:3128" {
		t.Fatalf("driver.Spec.ProxyURL = %q, want %q (New's proxyURL never reached CreateWithID's HTTP path)", got, "http://gw:3128")
	}
}

// TestSessionsConcurrentCreateUniqueIDs is the regression test for C1: newID
// used to do an unsynchronized s.seq++, so concurrent POST /sessions (the
// normal fleet operating mode — many callers creating sessions at once)
// could collide on the same id, and registry.put would silently overwrite
// the first session's entry.
func TestSessionsConcurrentCreateUniqueIDs(t *testing.T) {
	const n = 20
	srv := httptest.NewServer(New(driver.NewFake(n), "", "", "").Handler())
	defer srv.Close()

	var wg sync.WaitGroup
	ids := make([]string, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ids[i], errs[i] = postSession(srv.URL)
		}(i)
	}
	wg.Wait()

	seen := make(map[string]bool, n)
	for i, id := range ids {
		if errs[i] != nil {
			t.Fatalf("create %d: %v", i, errs[i])
		}
		if id == "" {
			t.Fatalf("create %d: empty session id", i)
		}
		if seen[id] {
			t.Fatalf("duplicate session id %q", id)
		}
		seen[id] = true
	}
	if len(seen) != n {
		t.Fatalf("got %d unique ids, want %d", len(seen), n)
	}

	resp, err := http.Get(srv.URL + "/sessions")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var rows []struct{ ID, State string }
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != n {
		t.Fatalf("GET /sessions listed %d entries, want %d", len(rows), n)
	}
}

// slowFake wraps driver.Fake but blocks inside Create until the test closes
// (or sends on) unblock. It lets a test deterministically land a request in
// POST /sessions' window between registry.put (state "starting", handle "")
// and drv.Create returning — the exact window I1's fix (registry.opTarget +
// sessionOp's "starting" guard) protects.
type slowFake struct {
	*driver.Fake
	unblock chan struct{}
}

func newSlowFake(total int) *slowFake {
	return &slowFake{Fake: driver.NewFake(total), unblock: make(chan struct{})}
}

// Create shadows the embedded Fake's Create so slowFake satisfies
// driver.Driver with a blocking Create while every other method (Inspect,
// Destroy, Suspend, Resume, Snapshot, Capacity) still goes straight through
// to the real Fake.
func (f *slowFake) Create(ctx context.Context, spec driver.Spec) (driver.Handle, error) {
	<-f.unblock
	return f.Fake.Create(ctx, spec)
}

// TestSessionOpRejectsStartingSession is the regression test for I1: a
// DELETE (or suspend/resume/snapshot) landing between registry.put and
// drv.Create returning used to read handle=="" unlocked, act on it as a
// no-op (Destroy("") for DELETE), and remove the registry entry — while
// POST /sessions was still inside Create. Create then succeeded into a
// container the registry no longer tracked: its sessiond would dial
// /register, get a 404 (no registry entry), and fatally exit — an orphaned,
// exited container invisible to runnerd. Ops on a "starting" entry must be
// rejected (409), and the session must complete normally — not be
// orphaned — once Create actually finishes.
func TestSessionOpRejectsStartingSession(t *testing.T) {
	fd := newSlowFake(4)
	rd := New(fd, "", "", "")
	srv := httptest.NewServer(rd.Handler())
	defer srv.Close()

	type createResult struct {
		id  string
		err error
	}
	resultCh := make(chan createResult, 1)
	go func() {
		id, err := postSession(srv.URL)
		resultCh <- createResult{id, err}
	}()

	// Poll GET /sessions until the starting entry shows up (Create is
	// blocked, so this must observe the pre-Create registry.put).
	var id string
	deadline := time.Now().Add(2 * time.Second)
	for id == "" && time.Now().Before(deadline) {
		resp, err := http.Get(srv.URL + "/sessions")
		if err != nil {
			t.Fatal(err)
		}
		var rows []struct{ ID, State string }
		json.NewDecoder(resp.Body).Decode(&rows)
		resp.Body.Close()
		if len(rows) == 1 && rows[0].State == "starting" {
			id = rows[0].ID
		}
		time.Sleep(10 * time.Millisecond)
	}
	if id == "" {
		t.Fatal("session never appeared as \"starting\" in GET /sessions while Create was blocked")
	}

	// DELETE while Create is still blocked (handle=="") must be rejected,
	// not act on an empty handle and remove the entry out from under the
	// in-flight Create.
	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/sessions/"+id, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("DELETE on starting session = %d, want %d", resp.StatusCode, http.StatusConflict)
	}
	// The entry must still be there (not removed by the rejected DELETE).
	if _, _, ok := rd.reg.opTarget(id); !ok {
		t.Fatal("rejected DELETE removed the registry entry anyway")
	}

	// Unblock Create; POST /sessions must complete normally.
	close(fd.unblock)
	select {
	case r := <-resultCh:
		if r.err != nil {
			t.Fatal(r.err)
		}
		if r.id != id {
			t.Fatalf("create id = %q, want %q", r.id, id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("POST /sessions never returned after unblocking Create")
	}

	// The session must become running with a real handle — not orphaned —
	// and the fake driver's own item must still exist (never Destroy()'d by
	// the rejected DELETE).
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		handle, state, ok := rd.reg.opTarget(id)
		if ok && state == "running" && handle != "" {
			h, err := fd.Inspect(context.Background(), handle)
			if err != nil || h.State != driver.StateRunning {
				t.Fatalf("fake driver's container not running post-create (orphan/destroyed?): %+v, %v", h, err)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("session never became running with a handle after Create unblocked")
}

// dialRegisterAndServe simulates a container's sessiond: it creates a real
// (Docker-free) session.Session running argv, dials /register?session=id,
// and pumps relay.ServeSession over that connection in the background. It
// returns the client-side conn so the test can kill it to simulate the
// container dying.
func dialRegisterAndServe(t *testing.T, ctx context.Context, wsBase, id string) *websocket.Conn {
	t.Helper()
	sess, err := session.New(
		session.Config{Argv: []string{"sh", "-i"}, Cols: 80, Rows: 24, LogPath: t.TempDir() + "/s.log"},
		session.StartProc,
	)
	if err != nil {
		t.Fatal(err)
	}
	regConn, _, err := websocket.Dial(ctx, wsBase+"/register?session="+id, nil)
	if err != nil {
		t.Fatal(err)
	}
	regConn.SetReadLimit(16 << 20)
	go relay.ServeSession(ctx, relay.WSConn(regConn), sess)
	return regConn
}

// waitForHub polls the registry (directly — this test is in-package) until
// the given session has a hub registered, or fails the test after a bound.
func waitForHub(t *testing.T, rd *Server, id string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := rd.reg.hub(id); ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("session %s never registered a hub", id)
}

// TestRegisterCleansUpOnSessionConnDeath is the regression test for the
// register-lifetime fix: when the container is actually gone (confirmed via
// Inspect, not merely inferred from the conn closing — see Task 5's
// inspect-before-destroy change), killing the sessiond-side conn must reap
// the registry entry within a bounded deadline, not leak it (and the
// /register handler goroutine) forever. Before Task 5 any conn death alone
// was treated as proof of a crash; now the fake driver's item is explicitly
// destroyed (simulating the container having actually died) so this test
// still exercises "the container is really gone" rather than the
// alive-container/redial branch TestHubDeathAliveContainerKeepsSession
// covers.
func TestRegisterCleansUpOnSessionConnDeath(t *testing.T) {
	fd := driver.NewFake(4)
	rd := New(fd, "", "", "")
	srv := httptest.NewServer(rd.Handler())
	defer srv.Close()
	base := strings.Replace(srv.URL, "http", "ws", 1)
	ctx := context.Background()

	id := createSession(t, srv.URL)
	regConn := dialRegisterAndServe(t, ctx, base, id)
	waitForHub(t, rd, id)

	// Confirm the path actually works end to end before killing it, so a
	// failure below is unambiguously about cleanup, not plumbing.
	cli, _, err := websocket.Dial(ctx, base+"/attach?session="+id, nil)
	if err != nil {
		t.Fatal(err)
	}
	cli.SetReadLimit(16 << 20)
	wsjson.Write(ctx, cli, wire.ClientMsg{Type: "resize", Cols: 80, Rows: 24})
	wsjson.Write(ctx, cli, wire.ClientMsg{Type: "stdin", Data: []byte("echo pre-death-marker\n")})
	found := false
	echoDeadline := time.After(5 * time.Second)
	for !found {
		select {
		case <-echoDeadline:
			t.Fatal("no pre-death-marker before killing the sessiond conn")
		default:
		}
		var m wire.ServerMsg
		if err := wsjson.Read(ctx, cli, &m); err != nil {
			t.Fatal(err)
		}
		if m.Type == "output" && strings.Contains(string(m.Data), "pre-death-marker") {
			found = true
		}
	}
	cli.CloseNow()

	handle, _, ok := rd.reg.opTarget(id)
	if !ok || handle == "" {
		t.Fatal("session has no handle before killing the conn")
	}
	// Simulate the container itself dying (not just its conn): with
	// inspect-before-destroy, a conn death alone is no longer enough — the
	// driver must confirm the container is actually gone.
	if err := fd.Destroy(ctx, handle); err != nil {
		t.Fatal(err)
	}

	// Simulate the container's sessiond exiting with it: close the
	// sessiond-side conn out from under the hub.
	regConn.CloseNow()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := rd.reg.get(id); !ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("session %s still in registry 3s after sessiond conn closed", id)
}

// attachAndAssertEcho dials /attach for id, sends marker as stdin, and fails
// the test if it doesn't come back through an "output" frame within 5s.
// Local to this file's cold-suspend test so the two round-trip echo checks
// (pre-suspend, post-resume) don't duplicate the dial/resize/read loop.
func attachAndAssertEcho(t *testing.T, ctx context.Context, base, id, marker string) {
	t.Helper()
	cli, _, err := websocket.Dial(ctx, base+"/attach?session="+id, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.CloseNow()
	cli.SetReadLimit(16 << 20)
	wsjson.Write(ctx, cli, wire.ClientMsg{Type: "resize", Cols: 80, Rows: 24})
	wsjson.Write(ctx, cli, wire.ClientMsg{Type: "stdin", Data: []byte("echo " + marker + "\n")})
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("no %s through the relay", marker)
		default:
		}
		var m wire.ServerMsg
		if err := wsjson.Read(ctx, cli, &m); err != nil {
			t.Fatal(err)
		}
		if m.Type == "output" && strings.Contains(string(m.Data), marker) {
			return
		}
	}
}

// TestHubDeathDuringColdSuspendKeepsEntry is the regression test for C1/I1:
// `docker stop` (cold Suspend) kills the container's sessiond, which closes
// its /register conn — at the socket level this is indistinguishable from a
// crash. Before this fix, the register goroutine unconditionally removed
// the registry entry on ANY hub death, so a deliberate cold suspend 404'd
// every future resume forever, with the (stopped, still-existing) container
// orphaned; conversely, on an actual crash the entry was removed but the
// container itself was never destroyed, leaking its capacity slot forever
// (I1). This asserts the suspend path end to end: the entry SURVIVES hub
// death with state "suspended" and a cleared hub, the fake driver's
// container is NOT destroyed, and a subsequent resume + re-register
// (simulating `docker start` re-execing sessiond) re-arms a fresh hub that
// a new /attach can relay through.
func TestHubDeathDuringColdSuspendKeepsEntry(t *testing.T) {
	fd := driver.NewFake(4)
	rd := New(fd, "", "", "")
	srv := httptest.NewServer(rd.Handler())
	defer srv.Close()
	base := strings.Replace(srv.URL, "http", "ws", 1)
	ctx := context.Background()

	id := createSession(t, srv.URL)
	regConn := dialRegisterAndServe(t, ctx, base, id)
	waitForHub(t, rd, id)

	// Confirm the path works before suspending, so a later failure is
	// unambiguously about the suspend/hub-death interaction, not plumbing.
	attachAndAssertEcho(t, ctx, base, id, "pre-suspend-marker")

	handleBefore, _, ok := rd.reg.opTarget(id)
	if !ok || handleBefore == "" {
		t.Fatal("session has no handle before suspend")
	}

	// Cold suspend: POST .../suspend?warm=false. The fake driver's Suspend
	// just flips its own state map — it doesn't touch the register conn —
	// so the hub death below is a separate, explicit step simulating what a
	// real `docker stop` would also do to the container's sessiond.
	resp, err := http.Post(srv.URL+"/sessions/"+id+"/suspend?warm=false", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("suspend status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}

	// Simulate docker stop killing sessiond: close the sessiond-side conn
	// out from under the hub — same mechanism TestRegisterCleansUpOnSessionConnDeath
	// uses for a crash; what differs is the registry state this lands in.
	regConn.CloseNow()

	// The entry must SURVIVE (not be removed like a crash would) and its hub
	// must clear (nil) so a later resume's re-register can setHub a fresh
	// one instead of finding a stale one still occupying the slot. Poll on
	// the hub clearing, not on state=="suspended" alone: the suspend POST
	// above already set state "suspended" BEFORE regConn.CloseNow() ever
	// ran, so state=="suspended" is true from the very first iteration and
	// proves nothing about whether onHubDeath has actually processed the
	// hub death yet — the hub going from present to absent is the real
	// completion signal for that.
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, ok := rd.reg.get(id); !ok {
			t.Fatal("registry entry removed on cold-suspend hub death (should survive, not be treated as a crash)")
		}
		if _, hubOK := rd.reg.hub(id); !hubOK {
			break // onHubDeath has run and cleared the hub
		}
		if time.Now().After(deadline) {
			t.Fatal("hub never cleared after cold-suspend hub death (onHubDeath never ran, or kept the stale hub)")
		}
		time.Sleep(20 * time.Millisecond)
	}
	handle, state, ok := rd.reg.opTarget(id)
	if !ok {
		t.Fatal("registry entry vanished right after the hub cleared")
	}
	if state != "suspended" {
		t.Fatalf("state after cold-suspend hub death = %q, want \"suspended\"", state)
	}
	if handle != handleBefore {
		t.Fatalf("handle changed across hub death: %q -> %q", handleBefore, handle)
	}

	// The fake driver's own container must NOT have been destroyed.
	h, err := fd.Inspect(ctx, handleBefore)
	if err != nil || h.State == driver.StateGone {
		t.Fatalf("container was destroyed on cold-suspend hub death (should be kept, not destroyed): %+v, %v", h, err)
	}

	// Resume, then re-dial /register with the same session id — what a real
	// container does on `docker start`: sessiond re-execs and redials.
	resp, err = http.Post(srv.URL+"/sessions/"+id+"/resume", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("resume status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}

	dialRegisterAndServe(t, ctx, base, id)
	waitForHub(t, rd, id)

	if _, state, ok := rd.reg.opTarget(id); !ok || state != "running" {
		t.Fatalf("state after resume+re-register = %q (ok=%v), want \"running\"", state, ok)
	}

	attachAndAssertEcho(t, ctx, base, id, "post-resume-marker")
}

// TestDeleteSessionRemovesRegistryEntryAndClosesHub is the regression test
// for DELETE now closing a registered session's hub: the entry must be gone
// synchronously with the 204 response, and stay gone (register()'s own
// unblock-and-cleanup racing the same removal must be a no-op, not a re-add
// or a panic) for a subsequent GET /sessions.
// TestRecoverRebuildsRegistryFromDriverLabels is the regression test for
// Recover: on restart, runnerd's in-memory registry starts empty even though
// the driver's labeled containers (and the sessions they represent) are
// still alive. Recover must rebuild the registry from drv.List so a
// redialing sessiond finds its session instead of 404ing on /register, and
// a cold (stopped) session can still be resumed and re-registered.
func TestRecoverRebuildsRegistryFromDriverLabels(t *testing.T) {
	fd := driver.NewFake(4)
	ctx := context.Background()

	// Seed the driver directly (bypassing the runnerd API entirely), the way
	// containers from a PREVIOUS runnerd process would already exist when
	// this one starts up.
	if _, err := fd.Create(ctx, driver.Spec{SessionID: "sess-running"}); err != nil {
		t.Fatal(err)
	}
	hCold, err := fd.Create(ctx, driver.Spec{SessionID: "sess-cold"})
	if err != nil {
		t.Fatal(err)
	}
	if err := fd.Suspend(ctx, hCold.ID, false); err != nil { // cold
		t.Fatal(err)
	}

	rd := New(fd, "", "", "")
	if err := rd.Recover(ctx); err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(rd.Handler())
	defer srv.Close()
	base := strings.Replace(srv.URL, "http", "ws", 1)

	// GET /sessions must list both recovered sessions with the right states.
	resp, err := http.Get(srv.URL + "/sessions")
	if err != nil {
		t.Fatal(err)
	}
	var rows []struct{ ID, State string }
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	got := map[string]string{}
	for _, row := range rows {
		got[row.ID] = row.State
	}
	if len(got) != 2 {
		t.Fatalf("GET /sessions after Recover = %+v, want 2 entries", rows)
	}
	if got["sess-running"] != "running" {
		t.Fatalf("sess-running state = %q, want \"running\"", got["sess-running"])
	}
	if got["sess-cold"] != "suspended" {
		t.Fatalf("sess-cold state = %q, want \"suspended\"", got["sess-cold"])
	}

	// A redialing sessiond for the running session must find its (recovered,
	// hub-less) entry rather than 404 on /register — that's the whole point
	// of Recover. dialRegisterAndServe's websocket.Dial would itself fail
	// (non-101 response) if /register 404'd here.
	dialRegisterAndServe(t, ctx, base, "sess-running")
	waitForHub(t, rd, "sess-running")
	attachAndAssertEcho(t, ctx, base, "sess-running", "recovered-running-marker")

	// The cold-suspended session must be resumable, and its sessiond's
	// re-register (post `docker start`) must work too.
	resumeResp, err := http.Post(srv.URL+"/sessions/sess-cold/resume", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	resumeResp.Body.Close()
	if resumeResp.StatusCode != http.StatusNoContent {
		t.Fatalf("resume status = %d, want %d", resumeResp.StatusCode, http.StatusNoContent)
	}
	dialRegisterAndServe(t, ctx, base, "sess-cold")
	waitForHub(t, rd, "sess-cold")
	attachAndAssertEcho(t, ctx, base, "sess-cold", "recovered-cold-marker")

	if _, state, ok := rd.reg.opTarget("sess-cold"); !ok || state != "running" {
		t.Fatalf("sess-cold state after resume+re-register = %q (ok=%v), want \"running\"", state, ok)
	}
}

// destroyTrackingFake wraps driver.Fake to record every id passed to
// Destroy, so a test can assert whether runnerd's own hub-death path called
// it — as opposed to merely observing the fake's post-hoc state (which
// Destroy and a test's own setup calls would otherwise look identical
// through). Everything but Destroy goes straight through to the embedded
// *driver.Fake, matching the slowFake pattern above.
type destroyTrackingFake struct {
	*driver.Fake
	mu        sync.Mutex
	destroyed []string
}

func newDestroyTrackingFake(total int) *destroyTrackingFake {
	return &destroyTrackingFake{Fake: driver.NewFake(total)}
}

func (f *destroyTrackingFake) Destroy(ctx context.Context, id string) error {
	f.mu.Lock()
	f.destroyed = append(f.destroyed, id)
	f.mu.Unlock()
	return f.Fake.Destroy(ctx, id)
}

func (f *destroyTrackingFake) destroyCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.destroyed))
	copy(out, f.destroyed)
	return out
}

// TestHubDeathAliveContainerKeepsSession is the regression test for §4.8:
// hub death no longer implies container death, because sessiond now survives
// conn loss and redials (Task 5) instead of dying with it. Before this fix,
// ANY hub death (other than a marked cold-suspend) destroyed the container
// unconditionally — which used to be correct (sessiond dying was the only
// way its conn could die) but stopped being correct the moment sessiond
// started redialing: a runnerd restart, or any transient network blip, kills
// every conn without killing a single container. This asserts the container
// is inspected, found alive, and both the registry entry and the container
// survive — and that a fresh /register dial-in (simulating sessiond's
// redial) picks the session back up.
func TestHubDeathAliveContainerKeepsSession(t *testing.T) {
	fd := newDestroyTrackingFake(4)
	rd := New(fd, "", "", "")
	srv := httptest.NewServer(rd.Handler())
	defer srv.Close()
	base := strings.Replace(srv.URL, "http", "ws", 1)
	ctx := context.Background()

	id := createSession(t, srv.URL)
	regConn := dialRegisterAndServe(t, ctx, base, id)
	waitForHub(t, rd, id)

	attachAndAssertEcho(t, ctx, base, id, "pre-death-marker")

	handleBefore, state, ok := rd.reg.opTarget(id)
	if !ok || state != "running" || handleBefore == "" {
		t.Fatalf("session not running with a handle before hub death: state=%q ok=%v handle=%q", state, ok, handleBefore)
	}

	// Kill the sessiond-side conn but leave the fake container alive
	// (StateRunning) — the runnerd-restart / network-blip case, not a crash.
	regConn.CloseNow()

	// Poll until the hub clears (hubDied has run) rather than sleeping a
	// fixed duration, mirroring TestHubDeathDuringColdSuspendKeepsEntry.
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, ok := rd.reg.get(id); !ok {
			t.Fatal("registry entry removed on alive-container hub death (container is still running; should be kept for a redial)")
		}
		if _, hubOK := rd.reg.hub(id); !hubOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("hub never cleared after alive-container hub death")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// hubDied clearing the hub races slightly ahead of the Inspect-based
	// keep/destroy decision in register()'s tail (same goroutine, but give it
	// a moment rather than asserting in the same instant).
	time.Sleep(200 * time.Millisecond)

	if calls := fd.destroyCalls(); len(calls) != 0 {
		t.Fatalf("Destroy called on an alive container's hub death: %v", calls)
	}
	handle, state, ok := rd.reg.opTarget(id)
	if !ok {
		t.Fatal("registry entry removed even though the container is still alive")
	}
	if state != "running" {
		t.Fatalf("state after alive-container hub death = %q, want \"running\"", state)
	}
	if handle != handleBefore {
		t.Fatalf("handle changed across hub death: %q -> %q", handleBefore, handle)
	}

	// A fresh /register dial-in for the same id (sessiond's redial) must
	// succeed, and attach must work over the new hub.
	dialRegisterAndServe(t, ctx, base, id)
	waitForHub(t, rd, id)
	attachAndAssertEcho(t, ctx, base, id, "post-redial-marker")
}

// TestHubDeathGoneContainerDestroys is TestHubDeathAliveContainerKeepsSession's
// companion: when the driver's Inspect reports the container actually gone
// (a real crash — the process died, unlike the redial case above), today's
// behavior must be preserved — the registry entry is removed and the
// container is destroyed to reclaim its capacity slot (I1).
func TestHubDeathGoneContainerDestroys(t *testing.T) {
	fd := newDestroyTrackingFake(4)
	rd := New(fd, "", "", "")
	srv := httptest.NewServer(rd.Handler())
	defer srv.Close()
	base := strings.Replace(srv.URL, "http", "ws", 1)
	ctx := context.Background()

	id := createSession(t, srv.URL)
	regConn := dialRegisterAndServe(t, ctx, base, id)
	waitForHub(t, rd, id)

	attachAndAssertEcho(t, ctx, base, id, "pre-crash-marker")

	handle, state, ok := rd.reg.opTarget(id)
	if !ok || state != "running" || handle == "" {
		t.Fatalf("session not running with a handle before crash: state=%q ok=%v handle=%q", state, ok, handle)
	}

	// Simulate the container itself already being gone by the time runnerd
	// asks (crashed and reaped) — call the embedded *driver.Fake directly, not
	// through the tracking wrapper, so this setup step isn't mistaken for
	// runnerd's own Destroy call below.
	if err := fd.Fake.Destroy(ctx, handle); err != nil {
		t.Fatal(err)
	}

	// Kill the sessiond-side conn — socket-level indistinguishable from every
	// other hub-death test; what differs is what Inspect reports afterward.
	regConn.CloseNow()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := rd.reg.get(id); !ok {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, ok := rd.reg.get(id); ok {
		t.Fatal("registry entry survived a gone-container hub death (should be removed, matching today's crash behavior)")
	}

	calls := fd.destroyCalls()
	found := false
	for _, c := range calls {
		if c == handle {
			found = true
		}
	}
	if !found {
		t.Fatalf("Destroy(%q) not called by runnerd's own crash path; calls=%v", handle, calls)
	}
}

// inspectErrFake wraps destroyTrackingFake (inheriting its Destroy-call
// tracking and the underlying *driver.Fake) but makes Inspect always fail —
// simulating a transient driver failure (docker daemon hiccup, timeout) at
// exactly the moment register()'s hub-death tail asks it whether the
// container is still alive. Used by TestHubDeathInspectErrorKeepsSession.
type inspectErrFake struct {
	*destroyTrackingFake
}

func newInspectErrFake(total int) *inspectErrFake {
	return &inspectErrFake{destroyTrackingFake: newDestroyTrackingFake(total)}
}

func (f *inspectErrFake) Inspect(context.Context, string) (driver.Handle, error) {
	return driver.Handle{}, errors.New("inspect: docker daemon unreachable")
}

// TestHubDeathInspectErrorKeepsSession is the regression test for
// review-round-1 Finding 2: an Inspect failure is NOT proof the container is
// gone — it's proof runnerd couldn't get an answer. Destroying on that
// uncertainty risks killing a still-running container (the catastrophic
// direction); keeping a hub-less entry around risks nothing worse than a
// stale registry row until a later hub death (once Inspect works again) or a
// restart's Recover resolves it (the safe direction). Before this fix, the
// hub-death tail's `if err == nil && h.State == driver.StateRunning { keep
// }` fell through to the destroy branch on ANY Inspect error, including this
// one that says nothing about the container's actual state.
func TestHubDeathInspectErrorKeepsSession(t *testing.T) {
	fd := newInspectErrFake(4)
	rd := New(fd, "", "", "")
	srv := httptest.NewServer(rd.Handler())
	defer srv.Close()
	base := strings.Replace(srv.URL, "http", "ws", 1)
	ctx := context.Background()

	id := createSession(t, srv.URL)
	regConn := dialRegisterAndServe(t, ctx, base, id)
	waitForHub(t, rd, id)

	handleBefore, state, ok := rd.reg.opTarget(id)
	if !ok || state != "running" || handleBefore == "" {
		t.Fatalf("session not running with a handle before hub death: state=%q ok=%v handle=%q", state, ok, handleBefore)
	}

	regConn.CloseNow()

	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, ok := rd.reg.get(id); !ok {
			t.Fatal("registry entry removed on an Inspect error (should be kept — destroying on uncertainty is the catastrophic direction)")
		}
		if _, hubOK := rd.reg.hub(id); !hubOK {
			break // hubDied has run and cleared the hub
		}
		if time.Now().After(deadline) {
			t.Fatal("hub never cleared after hub death")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Give the Inspect-error branch a moment to run past the hub clearing,
	// mirroring TestHubDeathAliveContainerKeepsSession.
	time.Sleep(200 * time.Millisecond)

	if calls := fd.destroyCalls(); len(calls) != 0 {
		t.Fatalf("Destroy called despite an Inspect error (uncertain, not confirmed gone): %v", calls)
	}
	handle, state, ok := rd.reg.opTarget(id)
	if !ok {
		t.Fatal("registry entry removed even though Inspect only errored, never confirmed the container gone")
	}
	if state != "running" {
		t.Fatalf("state after Inspect-error hub death = %q, want \"running\"", state)
	}
	if handle != handleBefore {
		t.Fatalf("handle changed across hub death: %q -> %q", handleBefore, handle)
	}
}

// TestOnEventFiresRunningAndDead is the regression test for the OnEvent hook
// Task 6 wires to the control conn: "running" must fire after a successful
// register() setHub, and "dead" must fire when the crash path (gone
// container on hub death) actually destroys the container. It must stay
// nil-safe everywhere else — every other test in this file runs with
// OnEvent unset and must keep passing.
func TestOnEventFiresRunningAndDead(t *testing.T) {
	fd := newDestroyTrackingFake(4)
	rd := New(fd, "", "", "")

	type event struct{ sessionID, state string }
	var mu sync.Mutex
	var events []event
	rd.SetOnEvent(func(sessionID, state, _ string) {
		mu.Lock()
		events = append(events, event{sessionID, state})
		mu.Unlock()
	})

	srv := httptest.NewServer(rd.Handler())
	defer srv.Close()
	base := strings.Replace(srv.URL, "http", "ws", 1)
	ctx := context.Background()

	id := createSession(t, srv.URL)
	regConn := dialRegisterAndServe(t, ctx, base, id)
	waitForHub(t, rd, id)

	handle, _, ok := rd.reg.opTarget(id)
	if !ok || handle == "" {
		t.Fatal("session has no handle after register")
	}
	// Make the crash path's Inspect see the container as gone.
	if err := fd.Fake.Destroy(ctx, handle); err != nil {
		t.Fatal(err)
	}
	regConn.CloseNow()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := rd.reg.get(id); !ok {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	mu.Lock()
	got := append([]event(nil), events...)
	mu.Unlock()
	if len(got) != 2 || got[0] != (event{id, "running"}) || got[1] != (event{id, "dead"}) {
		t.Fatalf("OnEvent calls = %+v, want [{%s running} {%s dead}]", got, id, id)
	}
}

// TestSessionOpGetUnknownSessionReturns404 is the regression test for review
// round 1, finding 4: the Step 1 refactor briefly reordered sessionOp to
// check the request method before checking session existence, so a GET on
// an unknown session's op path returned 405 (method not allowed) instead of
// 404 (no such session). The pre-refactor handler always checked existence
// first, for every method including GET — restored, and pinned here.
func TestSessionOpGetUnknownSessionReturns404(t *testing.T) {
	rd := New(driver.NewFake(4), "", "", "")
	srv := httptest.NewServer(rd.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/sessions/does-not-exist/suspend")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET on unknown session op path = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

// blockingDestroyFake parks the first Destroy call after it has actually
// removed the container but before it returns, holding Delete in the one
// window that matters: past its hub.Close(), short of its reg.remove(). Later
// calls (the double-destroy this test exists to forbid) sail through the
// closed release channel and are still recorded.
type blockingDestroyFake struct {
	*destroyTrackingFake
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingDestroyFake(total int) *blockingDestroyFake {
	return &blockingDestroyFake{
		destroyTrackingFake: newDestroyTrackingFake(total),
		entered:             make(chan struct{}),
		release:             make(chan struct{}),
	}
}

func (f *blockingDestroyFake) Destroy(ctx context.Context, id string) error {
	err := f.destroyTrackingFake.Destroy(ctx, id)
	f.once.Do(func() { close(f.entered) })
	<-f.release
	return err
}

// TestDeleteRacingHubDeathFiresNoDeadEvent pins the rm-vs-spurious-dead race:
// Delete's own hub.Close() causes a hub death that is socket-level identical
// to a crash, so register()'s hub-death tail wakes up, Inspects the container
// Delete has just removed, finds it gone — and, before the "destroying"
// marker, would both destroy it a second time and fire a "dead" event while
// the Delete was still in flight. controld applies that event terminally, so
// the session lands on `dead` and the deliberate teardown's `destroyed` never
// sticks (e2e-fleet.sh asserts exactly that). With the marker set before
// hub.Close(), the register tail stands down: no dead event, one destroy, and
// Delete alone removes the entry.
func TestDeleteRacingHubDeathFiresNoDeadEvent(t *testing.T) {
	fd := newBlockingDestroyFake(4)
	rd := New(fd, "", "", "")

	var mu sync.Mutex
	var events []string
	rd.SetOnEvent(func(id, state, _ string) {
		mu.Lock()
		events = append(events, id+":"+state)
		mu.Unlock()
	})
	seen := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), events...)
	}

	srv := httptest.NewServer(rd.Handler())
	defer srv.Close()
	base := strings.Replace(srv.URL, "http", "ws", 1)
	ctx := context.Background()

	id := createSession(t, srv.URL)
	dialRegisterAndServe(t, ctx, base, id)
	waitForHub(t, rd, id)
	handle, _, ok := rd.reg.opTarget(id)
	if !ok || handle == "" {
		t.Fatal("session has no handle after register")
	}

	done := make(chan error, 1)
	go func() { done <- rd.Delete(ctx, id) }()

	select {
	case <-fd.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Delete never reached drv.Destroy")
	}

	// Delete is parked mid-Destroy. Wait for the hub death it caused to be
	// processed (hubDied clears the hub), then give register's tail the room
	// to run the Inspect-and-destroy path it must decline — the same
	// settle-and-check shape as the other hub-death tests here.
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, hubOK := rd.reg.hub(id); !hubOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("hub never cleared after Delete closed it")
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)

	close(fd.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Delete: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Delete did not return after the destroy was released")
	}
	// A dead event fired by the losing path would be queued by now, but let
	// it land before asserting its absence.
	time.Sleep(200 * time.Millisecond)

	if _, ok := rd.reg.get(id); ok {
		t.Fatal("registry entry survived Delete")
	}
	if calls := fd.destroyCalls(); len(calls) != 1 || calls[0] != handle {
		t.Fatalf("Destroy calls = %v, want exactly one for %q", calls, handle)
	}
	if got := seen(); len(got) != 1 || got[0] != id+":running" {
		t.Fatalf("events = %v, want only the register's %s:running — a deliberate rm must fire no \"dead\"", got, id)
	}
}

func TestDeleteSessionRemovesRegistryEntryAndClosesHub(t *testing.T) {
	rd := New(driver.NewFake(4), "", "", "")
	srv := httptest.NewServer(rd.Handler())
	defer srv.Close()
	base := strings.Replace(srv.URL, "http", "ws", 1)
	ctx := context.Background()

	id := createSession(t, srv.URL)
	dialRegisterAndServe(t, ctx, base, id)
	waitForHub(t, rd, id)

	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/sessions/"+id, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}

	if _, ok := rd.reg.get(id); ok {
		t.Fatal("session still in registry immediately after DELETE response")
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		resp, err := http.Get(srv.URL + "/sessions")
		if err != nil {
			t.Fatal(err)
		}
		var rows []struct{ ID, State string }
		json.NewDecoder(resp.Body).Decode(&rows)
		resp.Body.Close()
		found := false
		for _, row := range rows {
			if row.ID == id {
				found = true
			}
		}
		if !found {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("session %s still listed in GET /sessions after DELETE", id)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestSnapshotOverHTTPGeneratesARef pins the local dev surface (runnerctl
// snapshot). Task 6 moved this path off Op onto OpSnapshot(id, ""), because
// only the agent has an environment ref to commit to; a plain
// POST /sessions/{id}/snapshot names none, so the driver still mints one and
// it still comes back as {"ref": ...}. The observable behavior must not have
// moved with the plumbing.
func TestSnapshotOverHTTPGeneratesARef(t *testing.T) {
	fd := driver.NewFake(4)
	rd := New(fd, "", "", "")
	srv := httptest.NewServer(rd.Handler())
	defer srv.Close()
	if err := rd.CreateWithID(context.Background(), "sess-snap", driver.Spec{Image: "img"}, nil); err != nil {
		t.Fatal(err)
	}

	resp, err := http.Post(srv.URL+"/sessions/sess-snap/snapshot", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("snapshot status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Ref string `json:"ref"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	// The fake's generated shape. Asserting the prefix (rather than merely
	// "not empty") is what proves the empty ref actually reached the driver
	// and it minted one, instead of the handler echoing back a blank.
	if !strings.HasPrefix(body.Ref, "fake-image:") {
		t.Fatalf("snapshot ref = %q, want a driver-generated fake-image: ref", body.Ref)
	}
}

// TestSnapshotStripsTheCreateEnvAndSetupChannel pins runnerd's half of the
// cached-image safety rule: a commit captures the container's whole config, so
// the snapshot command must NAME everything that must not survive into the
// image — every variable the create injected (an environment's decrypted
// secrets) plus the setup channel that would otherwise make the cache re-run
// its own setup script.
//
// The driver is where the stripping happens; what this asserts is that runnerd
// remembered the right names from a create it handled minutes earlier, since
// the Spec is long gone by snapshot time and nothing else in the process knows
// them.
func TestSnapshotStripsTheCreateEnvAndSetupChannel(t *testing.T) {
	fd := driver.NewFake(4)
	rd := New(fd, "", "", "")
	ctx := context.Background()

	err := rd.CreateWithID(ctx, "sess-strip", driver.Spec{
		Image: "img",
		Setup: "install things",
		Env:   map[string]string{"GH_TOKEN": "secret-value", "API_KEY": "another"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rd.OpSnapshot(ctx, "sess-strip", "rainier-env:e1-abc"); err != nil {
		t.Fatal(err)
	}

	strips := fd.Strips()
	if len(strips) != 1 {
		t.Fatalf("Strips() = %v, want exactly one snapshot's list", strips)
	}
	// Env keys sorted, then everything the DRIVER injects: the setup channel,
	// the session's own identity, and the egress proxy vars (whose URLs embed
	// that session id as userinfo). A stable list, so a snapshot of the same
	// session always issues the same command.
	want := []string{
		"API_KEY", "GH_TOKEN",
		"RAINIER_SETUP_B64", "RAINIER_SETUP_TIMEOUT",
		"RAINIER_DIAL", "RAINIER_SESSION",
		"HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy", "NO_PROXY", "no_proxy",
	}
	if !reflect.DeepEqual(strips[0], want) {
		t.Fatalf("strip list = %v, want %v", strips[0], want)
	}

	// A session that injected nothing still strips the driver's own set —
	// unconditionally, because "this one looked harmless" is exactly the
	// reasoning that lets one image through carrying a credential.
	if err := rd.CreateWithID(ctx, "sess-bare", driver.Spec{Image: "img"}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := rd.OpSnapshot(ctx, "sess-bare", ""); err != nil {
		t.Fatal(err)
	}
	strips = fd.Strips()
	if got := strips[1]; !reflect.DeepEqual(got, driverEnvKeys) {
		t.Fatalf("strip list for a session with no injected env = %v, want %v", got, driverEnvKeys)
	}
}

// TestControlFramesBecomeEvents pins the setup half of the control channel:
// a FrameControl arriving on a session's register conn is not an attachment's
// frame at all — it belongs to the session — and runnerd turns it into the
// event controld's setup orchestration waits on. A setup_failed has two
// things to say (the exit code and what the script printed) and one string to
// say them in, so the composition is pinned here too: controld renders this
// detail into the session's error text.
func TestControlFramesBecomeEvents(t *testing.T) {
	fd := driver.NewFake(4)
	rd := New(fd, "", "", "")

	type event struct{ session, state, detail string }
	events := make(chan event, 8)
	rd.SetOnEvent(func(session, state, detail string) {
		events <- event{session, state, detail}
	})

	srv := httptest.NewServer(rd.Handler())
	defer srv.Close()
	base := strings.Replace(srv.URL, "http", "ws", 1)
	ctx := context.Background()

	id := createSession(t, srv.URL)
	c, _, err := websocket.Dial(ctx, base+"/register?session="+id, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()
	waitForHub(t, rd, id)

	// register()'s own "running" event fires first; drain it.
	if got := nextEvent(t, events); got.state != "running" {
		t.Fatalf("first event = %+v, want running", got)
	}

	sendControl := func(payload string) {
		t.Helper()
		f, err := relay.Encode(relay.Frame{Type: relay.FrameControl, Payload: []byte(payload)})
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Write(ctx, websocket.MessageText, f); err != nil {
			t.Fatal(err)
		}
	}

	// Garbage and unknown kinds are logged and dropped, never turned into an
	// event — and, crucially, never kill the hub's read loop: the setup_done
	// after them still lands.
	sendControl(`not json at all`)
	sendControl(`{"kind":"who knows"}`)
	sendControl(`{"kind":"setup_done"}`)
	if got, want := nextEvent(t, events), (event{id, "setup_done", ""}); got != want {
		t.Fatalf("event = %+v, want %+v", got, want)
	}

	sendControl(`{"kind":"setup_failed","rc":7,"tail":"E: unable to locate package foo"}`)
	got := nextEvent(t, events)
	if got.session != id || got.state != "setup_failed" {
		t.Fatalf("event = %+v, want a setup_failed for %s", got, id)
	}
	if !strings.Contains(got.detail, "7") || !strings.Contains(got.detail, "unable to locate package foo") {
		t.Fatalf("detail = %q, want it to carry both the rc and the tail", got.detail)
	}
}

// nextEvent takes one event off the channel or fails the test — the bound
// keeps a routing regression from hanging the suite.
func nextEvent[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case e := <-ch:
		return e
	case <-time.After(3 * time.Second):
		var zero T
		t.Fatal("no event within 3s")
		return zero
	}
}

// TestControlHandlerDoesNotBlockTheHubReadLoop: OnControl runs on the hub's
// read loop, the one goroutine demultiplexing every attachment on the
// session's conn. The agent's event path ends in a capacity call against the
// driver (a real `docker ps` in production), so a handler that fired it
// inline would stall every viewer's output for the duration. The handler must
// hand off — proven here by making the event callback block and asserting the
// session's terminal frames keep flowing anyway.
func TestControlHandlerDoesNotBlockTheHubReadLoop(t *testing.T) {
	rd := New(driver.NewFake(4), "", "", "")
	release := make(chan struct{})
	fired := make(chan struct{}, 1)
	rd.SetOnEvent(func(session, state, detail string) {
		if state != "setup_done" {
			return
		}
		fired <- struct{}{}
		<-release // a wedged consumer
	})
	defer close(release)

	srv := httptest.NewServer(rd.Handler())
	defer srv.Close()
	base := strings.Replace(srv.URL, "http", "ws", 1)
	ctx := context.Background()

	id := createSession(t, srv.URL)
	sessConn, _, err := websocket.Dial(ctx, base+"/register?session="+id, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sessConn.CloseNow()
	waitForHub(t, rd, id)

	// A client attaches; the "session" side answers its FrameOpen with one
	// FrameServer. Nothing here needs a real session.Session — the hub only
	// moves frames.
	cli, _, err := websocket.Dial(ctx, base+"/attach?session="+id, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.CloseNow()
	if err := wsjson.Write(ctx, cli, wire.ClientMsg{Type: "resize", Cols: 80, Rows: 24}); err != nil {
		t.Fatal(err)
	}
	// Read the FrameOpen the hub sends us, so we know the attach id.
	_, raw, err := sessConn.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	open, err := relay.Decode(raw)
	if err != nil || open.Type != relay.FrameOpen {
		t.Fatalf("first frame = %+v (%v), want a FrameOpen", open, err)
	}

	// Wedge the control handler, then prove terminal output still arrives.
	ctrl, _ := relay.Encode(relay.Frame{Type: relay.FrameControl, Payload: []byte(`{"kind":"setup_done"}`)})
	if err := sessConn.Write(ctx, websocket.MessageText, ctrl); err != nil {
		t.Fatal(err)
	}
	select {
	case <-fired:
	case <-time.After(3 * time.Second):
		t.Fatal("the control event never reached the callback")
	}

	msg, _ := json.Marshal(wire.ServerMsg{Type: "output", Seq: 1, Data: []byte("still alive")})
	out, _ := relay.Encode(relay.Frame{Type: relay.FrameServer, AttachID: open.AttachID, Payload: msg})
	if err := sessConn.Write(ctx, websocket.MessageText, out); err != nil {
		t.Fatal(err)
	}
	readCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var got wire.ServerMsg
	if err := wsjson.Read(readCtx, cli, &got); err != nil {
		t.Fatalf("client read while a control handler is wedged: %v (the hub read loop is blocked)", err)
	}
	if string(got.Data) != "still alive" {
		t.Fatalf("client got %q, want the frame sent after the wedged control event", got.Data)
	}
}

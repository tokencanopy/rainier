// internal/controld/runners_test.go
package controld

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/v0wire"
)

const testRunnerToken = "rnr_test_runner_token"

// ---------------------------------------------------------------------------
// helpers (Tasks 8 and 10 reuse these: fakeRunner is the scripted runnerd
// stand-in for every controld test that needs a runner on the other end)
// ---------------------------------------------------------------------------

// newTestControld returns a Server over a fresh memstore plus an
// httptest.Server serving its Handler(). OpTimeout is shorter than production's
// minute but not by much, for the reason spelled out below; opts override any
// field before New validates it, and the tests that assert a timeout pass their
// own.
func newTestControld(t *testing.T, opts ...func(*Config)) (*Server, MemStore, *httptest.Server) {
	t.Helper()
	st := NewMemStore()
	s, ts := newTestControldOver(t, st, opts...)
	return s, st, ts
}

// newTestControldOver is newTestControld over a caller-supplied store — for
// tests that wrap the store to force an interleaving.
func newTestControldOver(t *testing.T, st MemStore, opts ...func(*Config)) (*Server, *httptest.Server) {
	t.Helper()
	cfg := Config{
		RunnerToken: testRunnerToken,
		ExternalURL: "http://controld.test:9090",
		SecretsKey:  testSecretsKey,
		// The package-wide default, and generous on purpose. Every happy-path
		// round trip in this package — a runner dispatch, a session RPC —
		// has to cross an httptest websocket and a fake-runner goroutine
		// inside it, and none of those tests is ASSERTING a duration: they
		// assert an answer. A tight default is therefore a stopwatch nobody
		// asked for, and the first thing to give under `-race` on a loaded
		// machine (it is the best explanation for the single unreproduced
		// failure this package produced during Plan 5).
		//
		// The tests that genuinely test a timeout pass their own short
		// OpTimeout through the opts func (150ms, 250ms) — raising this
		// weakens none of them.
		OpTimeout: 10 * time.Second,
	}
	for _, o := range opts {
		o(&cfg)
	}
	s, err := New(st, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ts := httptest.NewServer(s.Handler())
	// Cleanups run LIFO, so every fakeRunner started after this call closes
	// its socket before ts.Close() runs — httptest.Server.Close waits on
	// in-flight handlers, and the runner handler lives as long as its
	// (hijacked) websocket does.
	t.Cleanup(ts.Close)
	return s, ts
}

func runnerWSURL(ts *httptest.Server) string {
	return "ws" + strings.TrimPrefix(ts.URL, "http") + "/v0/runners/connect"
}

// dialRunner performs the raw websocket dial with the given bearer token
// (empty ⇒ no Authorization header), returning the HTTP response so tests
// can assert on the pre-upgrade status.
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
	Name     string
	Token    string
	Proto    *int
	Sessions []runner.SessionInfo
	Used     int
	Total    int
	// Capabilities are the portable capabilities this fake claims on its
	// announce. Empty is an old runner: it claims none, and controld
	// registers only the two capabilities the host spells for its name.
	Capabilities []string
}

// fakeRunner is a scripted runnerd: it dials /v0/runners/connect, writes one
// announce, then records every ToRunner controld sends and answers only when
// the test tells it to. Its reader runs in its own goroutine, so a test never
// has to interleave reads with the assertions it wants to make.
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
		sc.Token = testRunnerToken
	}
	proto := runner.ProtocolVersion
	if sc.Proto != nil {
		proto = *sc.Proto
	}
	c, _, err := dialRunner(t, ts, sc.Token)
	if err != nil {
		t.Fatalf("dial controld: %v", err)
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
	f.write(t, runner.FromRunner{Type: "announce", Proto: proto, Runner: sc.Name, Sessions: sc.Sessions,
		Capabilities: sc.Capabilities})
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
		t.Fatalf("write to controld: %v", err)
	}
}

func (f *fakeRunner) setCapacity(used, total int) {
	f.wmu.Lock()
	defer f.wmu.Unlock()
	f.used, f.total = used, total
}

// nextCmd returns the next ToRunner controld sent, failing the test if none
// arrives in time.
func (f *fakeRunner) nextCmd(t *testing.T) runner.ToRunner {
	t.Helper()
	select {
	case m := <-f.cmds:
		return m
	case <-time.After(3 * time.Second):
		t.Fatal("no command from controld within 3s")
		return runner.ToRunner{}
	}
}

// reply answers one dispatched command, echoing its ReqID (the correlation
// the whole dispatch path turns on) and the runner's current capacity.
func (f *fakeRunner) reply(t *testing.T, cmd runner.ToRunner, ok bool, detail string) {
	t.Helper()
	f.write(t, runner.FromRunner{Type: "result", ReqID: cmd.ReqID, OK: ok, Detail: detail})
}

func (f *fakeRunner) event(t *testing.T, session, state string) {
	t.Helper()
	f.write(t, runner.FromRunner{Type: "event", Session: session, State: state})
}

// eventDetail is event with the Detail the setup events carry: runnerd's
// pre-composed "rc N: <tail>" sentence on a failure, empty on success.
func (f *fakeRunner) eventDetail(t *testing.T, session, state, detail string) {
	t.Helper()
	f.write(t, runner.FromRunner{Type: "event", Session: session, State: state, Detail: detail})
}

// waitClosed returns the error that ended the fake's read loop — how a test
// observes controld hanging up (and, for a protocol rejection, why).
func (f *fakeRunner) waitClosed(t *testing.T) error {
	t.Helper()
	select {
	case err := <-f.readE:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("controld did not close the connection within 3s")
		return nil
	}
}

func (f *fakeRunner) close() {
	f.closeOnce.Do(func() { f.c.CloseNow() })
}

// drainAccept takes the accept controld sends every registered runner before
// any command (D19) off the front of the command stream, so a fixture that
// asserts on the FIRST command it is sent keeps asserting on a command. The
// tests that care about the accept itself read it with nextCmd instead.
func drainAccept(t *testing.T, f *fakeRunner) runner.ToRunner {
	t.Helper()
	cmd := f.nextCmd(t)
	if cmd.Type != "accept" {
		t.Fatalf("first message from controld = %+v, want an accept", cmd)
	}
	return cmd
}

// ghostSession is announced by tests that need a reconcile-finished signal:
// controld's destroy for it is enqueued at the very end of reconcile, so
// receiving that destroy proves reconcile has run to completion.
const ghostSession = "sess_reconcile_probe"

func awaitReconciled(t *testing.T, f *fakeRunner) {
	t.Helper()
	drainAccept(t, f)
	cmd := f.nextCmd(t)
	if cmd.Type != "destroy" || cmd.Session != ghostSession {
		t.Fatalf("reconcile probe: got %+v, want destroy of %s", cmd, ghostSession)
	}
	f.reply(t, cmd, true, "")
}

// joinRunner starts a fake runner and returns only once it is both connected
// and finished reconciling, so anything the test seeds afterwards is safe from
// the announce sweep. Its probe id is per-runner, which is what lets a
// multi-runner test wait on each one independently.
func joinRunner(t *testing.T, s *Server, ts *httptest.Server, sc runnerScript) *fakeRunner {
	t.Helper()
	if sc.Name == "" {
		sc.Name = "vm1"
	}
	probe := ghostSession + "-" + sc.Name
	sc.Sessions = append(append([]runner.SessionInfo{}, sc.Sessions...), runner.SessionInfo{ID: probe, State: "running"})
	f := startFakeRunner(t, ts, sc)
	waitConnected(t, s, sc.Name)
	drainAccept(t, f)
	cmd := f.nextCmd(t)
	if cmd.Type != "destroy" || cmd.Session != probe {
		t.Fatalf("reconcile probe on %s: got %+v, want destroy of %s", sc.Name, cmd, probe)
	}
	f.reply(t, cmd, true, "")
	return f
}

// nextOfType returns the next command of type typ f receives, skipping
// anything else controld happens to send in the meantime.
func nextOfType(t *testing.T, f *fakeRunner, typ string) runner.ToRunner {
	t.Helper()
	for {
		if cmd := f.nextCmd(t); cmd.Type == typ {
			return cmd
		}
	}
}

func nextCreate(t *testing.T, f *fakeRunner) runner.ToRunner {
	t.Helper()
	return nextOfType(t, f, "create")
}

// wantNothingQueued proves controld sent f nothing: a destroy for a probe
// session is queued now, and a connection delivers in order, so that destroy
// arriving first means nothing else was ever enqueued ahead of it.
func wantNothingQueued(t *testing.T, s *Server, f *fakeRunner) {
	t.Helper()
	probe := "sess_quiet_probe_" + f.name
	if err := s.sendToRunner(f.name, runner.ToRunner{Type: "destroy", Session: probe}); err != nil {
		t.Fatalf("sendToRunner(%s): %v", f.name, err)
	}
	if cmd := f.nextCmd(t); cmd.Type != "destroy" || cmd.Session != probe {
		t.Fatalf("%s got %+v, want only the probe destroy — controld sent it something it shouldn't have", f.name, cmd)
	}
}

// wantEventHandled seeds a fresh session, sends a `running` event for it, and
// waits for that to land. One connection's messages are handled in order and
// in the reader, so this proves every event sent before it has already been
// applied — the synchronization the "and nothing happened" assertions need.
func wantEventHandled(t *testing.T, st MemStore, f *fakeRunner, id string) {
	t.Helper()
	seedSession(t, st, control.Session{ID: control.SessionID(id), State: control.StateCreating, RunnerID: control.RunnerID(f.name)})
	f.event(t, id, "running")
	wantState(t, st, id, control.StateRunning)
}

// eventually polls fn until it returns nil or d elapses — controld processes
// runner messages asynchronously, so assertions on the store are polled, not
// slept on.
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

func waitConnected(t *testing.T, s *Server, name string) {
	t.Helper()
	eventually(t, 3*time.Second, func() error {
		if !s.runnerConnected(name) {
			return fmt.Errorf("runner %q not connected yet", name)
		}
		return nil
	})
}

func seedSession(t *testing.T, st MemStore, s control.Session) control.Session {
	t.Helper()
	if s.CreatorID == "" {
		s.CreatorID = "usr_test"
	}
	// The pool is the installation's, as it is on every row this store has
	// ever held: a queued session is queued *in* the install pool, and that
	// is the key the fleet repository's queue and capacity reads use.
	if s.PoolID == "" {
		s.PoolID = installPool
	}
	out, err := st.Sessions().CreateSession(context.Background(), installWorkspace, s)
	if err != nil {
		t.Fatalf("seed session %s: %v", s.ID, err)
	}
	return out
}

// seedSetupSession seeds a session exactly as a create dispatch leaves one
// whose setup script is still running: placed on runner, resolved to the
// environment's image, and carrying the pin of the script it was dispatched
// with (see Session.SetupHash). Tests that want a DIFFERENT provenance say so
// by writing the row themselves.
func seedSetupSession(t *testing.T, st MemStore, id, runner string, env control.Environment) control.Session {
	t.Helper()
	return seedSession(t, st, control.Session{ID: control.SessionID(id), State: control.StateRunning, RunnerID: control.RunnerID(runner),
		EnvironmentID: env.ID, Spec: control.PortableSpec{Image: env.Image}, SetupHash: SetupHash(env.Image, env.Setup)})
}

func envRow(t *testing.T, st MemStore, id control.EnvironmentID) control.Environment {
	t.Helper()
	e, err := st.Environments().GetEnvironment(context.Background(), installWorkspace, id)
	if err != nil {
		t.Fatalf("get environment %s: %v", id, err)
	}
	return e
}

// envSnapshotRunner names the runner that built id's cached snapshot, "" when
// there is none. control.Environment carries the snapshot but not its holder,
// so the host lookup — not the row — answers for it.
func envSnapshotRunner(t *testing.T, st MemStore, id control.EnvironmentID) control.RunnerID {
	t.Helper()
	r, err := st.SnapshotRunner(context.Background(), installWorkspace, id)
	if err != nil {
		t.Fatalf("snapshot runner of %s: %v", id, err)
	}
	return r
}

func getSession(t *testing.T, st MemStore, id string) control.Session {
	t.Helper()
	s, err := st.Sessions().GetSession(context.Background(), installWorkspace, control.SessionID(id))
	if err != nil {
		t.Fatalf("get session %s: %v", id, err)
	}
	return s
}

// wantState polls until the session reaches want, reporting the state it was
// stuck in otherwise.
func wantState(t *testing.T, st MemStore, id string, want control.SessionState) control.Session {
	t.Helper()
	var got control.Session
	eventually(t, 3*time.Second, func() error {
		got = getSession(t, st, id)
		if got.State != want {
			return fmt.Errorf("session %s state = %q, want %q", id, got.State, want)
		}
		return nil
	})
	return got
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

func TestNewValidatesConfig(t *testing.T) {
	t.Run("runner token required", func(t *testing.T) {
		if _, err := New(NewMemStore(), Config{ExternalURL: "http://x:9090", SecretsKey: testSecretsKey}); err == nil {
			t.Fatal("New with empty RunnerToken: want error, got nil")
		}
	})
	t.Run("external url required", func(t *testing.T) {
		if _, err := New(NewMemStore(), Config{RunnerToken: "t", SecretsKey: testSecretsKey}); err == nil {
			t.Fatal("New with empty ExternalURL: want error, got nil")
		}
	})
	t.Run("external url must be absolute http", func(t *testing.T) {
		if _, err := New(NewMemStore(), Config{RunnerToken: "t", ExternalURL: "controld.test:9090", SecretsKey: testSecretsKey}); err == nil {
			t.Fatal("New with schemeless ExternalURL: want error, got nil")
		}
	})
	t.Run("defaults", func(t *testing.T) {
		s, err := New(NewMemStore(), Config{RunnerToken: "t", ExternalURL: "http://x:9090", SecretsKey: testSecretsKey})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if s.cfg.OpTimeout != 60*time.Second {
			t.Errorf("OpTimeout = %s, want 60s", s.cfg.OpTimeout)
		}
		if s.cfg.GitHubAPIBase != "https://api.github.com" {
			t.Errorf("GitHubAPIBase = %q, want https://api.github.com", s.cfg.GitHubAPIBase)
		}
	})
}

func TestRunnerAuthRequired(t *testing.T) {
	_, _, ts := newTestControld(t)

	for _, tc := range []struct {
		name  string
		token string
	}{
		{"no bearer", ""},
		{"wrong bearer", "rnr_wrong"},
	} {
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

	t.Run("correct bearer upgrades", func(t *testing.T) {
		c, resp, err := dialRunner(t, ts, testRunnerToken)
		if err != nil {
			t.Fatalf("dial with correct token: %v", err)
		}
		defer c.CloseNow()
		if resp.StatusCode != http.StatusSwitchingProtocols {
			t.Fatalf("status = %d, want 101", resp.StatusCode)
		}
	})
}

func TestProtoRejected(t *testing.T) {
	_, st, ts := newTestControld(t)

	bad := 99
	f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Proto: &bad, Total: 4})

	err := f.waitClosed(t)
	var ce websocket.CloseError
	if !errors.As(err, &ce) {
		t.Fatalf("conn ended with %v, want a websocket close", err)
	}
	if !strings.Contains(ce.Reason, "proto 99") || !strings.Contains(ce.Reason, "proto 1") {
		t.Fatalf("close reason = %q, want it to name both proto 99 and proto 1", ce.Reason)
	}

	// A runner we refused to speak to must not appear in the fleet.
	rs, err := st.Fleet().ListRunners(context.Background(), installPool)
	if err != nil {
		t.Fatalf("ListRunners: %v", err)
	}
	if len(rs) != 0 {
		t.Fatalf("ListRunners = %+v, want none", rs)
	}
}

func TestAnnounceUpsertsRunner(t *testing.T) {
	s, st, ts := newTestControld(t)
	f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Used: 1, Total: 4})
	waitConnected(t, s, "vm1")

	eventually(t, 3*time.Second, func() error {
		rs, err := st.Fleet().ListRunners(context.Background(), installPool)
		if err != nil {
			return err
		}
		if len(rs) != 1 {
			return fmt.Errorf("ListRunners = %+v, want 1 runner", rs)
		}
		r := rs[0]
		if r.ID != "vm1" || !r.Connected || r.CapacityUsed != 1 || r.CapacityTotal != 4 {
			return fmt.Errorf("runner = %+v, want {vm1 used:1 total:4 connected:true}", r)
		}
		if r.LastSeenAt.IsZero() {
			return fmt.Errorf("runner LastSeenAt is zero")
		}
		return nil
	})

	f.close()

	eventually(t, 3*time.Second, func() error {
		rs, err := st.Fleet().ListRunners(context.Background(), installPool)
		if err != nil {
			return err
		}
		if len(rs) != 1 {
			return fmt.Errorf("ListRunners = %+v, want 1 runner", rs)
		}
		if rs[0].Connected {
			return fmt.Errorf("runner still connected after close")
		}
		return nil
	})
	if s.runnerConnected("vm1") {
		t.Fatal("runnerConnected(vm1) still true after close")
	}
}

// TestReconcileTable walks design §4.8's reconciliation table, one subtest
// per row.
func TestReconcileTable(t *testing.T) {
	old := time.Now().Add(-time.Hour)

	t.Run("running present and agreeing bumps last_event_at", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		id := "sess_agree"
		seedSession(t, st, control.Session{ID: control.SessionID(id), State: control.StateRunning, RunnerID: "vm1", CreatedAt: old, UpdatedAt: old, LastEventAt: old})
		startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Sessions: []runner.SessionInfo{{ID: id, State: "running"}}})
		waitConnected(t, s, "vm1")

		eventually(t, 3*time.Second, func() error {
			got := getSession(t, st, id)
			if got.State != control.StateRunning {
				return fmt.Errorf("state = %q, want running", got.State)
			}
			if !got.LastEventAt.After(old) {
				return fmt.Errorf("LastEventAt = %s, want bumped past %s", got.LastEventAt, old)
			}
			return nil
		})
	})

	t.Run("running present with different state adopts announced state", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		id := "sess_adopt"
		seedSession(t, st, control.Session{ID: control.SessionID(id), State: control.StateRunning, RunnerID: "vm1"})
		startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Sessions: []runner.SessionInfo{{ID: id, State: "suspended_cold"}}})
		waitConnected(t, s, "vm1")

		got := wantState(t, st, id, control.StateSuspendedCold)
		if got.RunnerID != "vm1" {
			t.Fatalf("runner = %q, want vm1", got.RunnerID)
		}
	})

	t.Run("running absent is dead with lost at announce", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		id := "sess_lost"
		seedSession(t, st, control.Session{ID: control.SessionID(id), State: control.StateRunning, RunnerID: "vm1"})
		startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4})
		waitConnected(t, s, "vm1")

		got := wantState(t, st, id, control.StateDead)
		if got.Error != "lost at announce" {
			t.Fatalf("error = %q, want %q", got.Error, "lost at announce")
		}
	})

	t.Run("creating absent goes back to queued", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		id := "sess_requeue"
		seedSession(t, st, control.Session{ID: control.SessionID(id), State: control.StateCreating, RunnerID: "vm1"})
		startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4})
		waitConnected(t, s, "vm1")

		got := wantState(t, st, id, control.StateQueued)
		if got.RunnerID != "" {
			t.Fatalf("runner = %q, want cleared", got.RunnerID)
		}
	})

	t.Run("terminal row announced present is destroyed as an orphan", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		id := "sess_terminal"
		seedSession(t, st, control.Session{ID: control.SessionID(id), State: control.StateDestroyed, RunnerID: "vm1"})
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Sessions: []runner.SessionInfo{{ID: id, State: "running"}}})

		drainAccept(t, f)
		cmd := f.nextCmd(t)
		if cmd.Type != "destroy" || cmd.Session != id {
			t.Fatalf("got %+v, want destroy of %s", cmd, id)
		}
		f.reply(t, cmd, true, "")
		if got := getSession(t, st, id); got.State != control.StateDestroyed {
			t.Fatalf("state = %q, want destroyed (untouched)", got.State)
		}
	})

	t.Run("unknown id announced present is destroyed as an orphan", func(t *testing.T) {
		_, _, ts := newTestControld(t)
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Sessions: []runner.SessionInfo{{ID: "sess_ghost", State: "running"}}})

		drainAccept(t, f)
		cmd := f.nextCmd(t)
		if cmd.Type != "destroy" || cmd.Session != "sess_ghost" {
			t.Fatalf("got %+v, want destroy of sess_ghost", cmd)
		}
		f.reply(t, cmd, true, "")
	})

	// A live row the store places on ANOTHER runner: this runner is holding
	// a duplicate, so the copy here is an orphan. Adopting it would leave
	// both alive and ping-pong Runner between them on every reconnect.
	t.Run("live row placed on another runner is destroyed as a duplicate", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		id := "sess_dupe"
		seedSession(t, st, control.Session{ID: control.SessionID(id), State: control.StateRunning, RunnerID: "vm2"})
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Sessions: []runner.SessionInfo{{ID: id, State: "running"}}})

		drainAccept(t, f)
		cmd := f.nextCmd(t)
		if cmd.Type != "destroy" || cmd.Session != id {
			t.Fatalf("got %+v, want destroy of %s", cmd, id)
		}
		f.reply(t, cmd, true, "")
		got := getSession(t, st, id)
		if got.State != control.StateRunning || got.RunnerID != "vm2" {
			t.Fatalf("row = %s on %q, want running on vm2 (untouched)", got.State, got.RunnerID)
		}
	})

	// Not a table row, but the case the table's edges leave open: a live row
	// with no placement at all (requeued while the runner was away, never
	// re-placed). Postgres wants it alive and this runner has it, so it's
	// adopted — never destroyed. The ghost announced after it pins the
	// ordering: its destroy arriving first proves no destroy was sent for
	// the adopted session.
	t.Run("live row announced by a runner it is not placed on is adopted", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		id := "sess_unplaced"
		seedSession(t, st, control.Session{ID: control.SessionID(id), State: control.StateQueued})
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4, Sessions: []runner.SessionInfo{
			{ID: id, State: "running"},
			{ID: ghostSession, State: "running"},
		}})
		waitConnected(t, s, "vm1")

		awaitReconciled(t, f)
		got := wantState(t, st, id, control.StateRunning)
		if got.RunnerID != "vm1" {
			t.Fatalf("runner = %q, want vm1", got.RunnerID)
		}
	})
}

func TestDispatchCorrelatesResults(t *testing.T) {
	s, _, ts := newTestControld(t)
	f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4})
	waitConnected(t, s, "vm1")
	drainAccept(t, f)

	type outcome struct {
		res runner.FromRunner
		err error
	}
	run := func(session string) <-chan outcome {
		ch := make(chan outcome, 1)
		go func() {
			res, err := s.transport.Dispatch(context.Background(), installPool, "vm1",
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
	// Answer out of order: correlation is by ReqID, not arrival order.
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

func TestDestroyOrphanRetriesAfterLiveQueueSaturation(t *testing.T) {
	s, err := New(NewMemStore(), Config{
		RunnerToken: testRunnerToken,
		ExternalURL: "http://controld.test:9090",
		SecretsKey:  testSecretsKey,
		OpTimeout:   time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	rc := newRunnerConn("vm1", nil)
	s.mu.Lock()
	s.runners[rc.name] = rc
	s.mu.Unlock()

	// Hold the writer still with a completely full live queue. The first
	// dispatch therefore fails in enqueue, not because the connection died.
	for i := 0; i < runnerSendQueue; i++ {
		rc.out <- runner.ToRunner{Type: "prepull", Ref: fmt.Sprintf("image-%d", i)}
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s.destroyOrphan(ctx, rc.name, "sess_queue_retry")
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

func TestDispatchUnreachable(t *testing.T) {
	t.Run("never connected", func(t *testing.T) {
		s, _, _ := newTestControld(t)
		_, err := s.transport.Dispatch(context.Background(), installPool, "vm-nope",
			runner.ToRunner{Type: "destroy", Session: "sess_x"})
		if !errors.Is(err, control.ErrUnavailable) {
			t.Fatalf("err = %v, want control.ErrUnavailable", err)
		}
	})

	t.Run("connection dies in flight", func(t *testing.T) {
		s, _, ts := newTestControld(t)
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4})
		waitConnected(t, s, "vm1")

		errc := make(chan error, 1)
		go func() {
			_, err := s.transport.Dispatch(context.Background(), installPool, "vm1",
				runner.ToRunner{Type: "suspend", Session: "sess_x"})
			errc <- err
		}()
		f.nextCmd(t) // the command reached the runner; now the runner dies
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
		s, _, ts := newTestControld(t, func(c *Config) { c.OpTimeout = 150 * time.Millisecond })
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4})
		waitConnected(t, s, "vm1")

		start := time.Now()
		_, err := s.transport.Dispatch(context.Background(), installPool, "vm1",
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

func TestEventUpdatesStore(t *testing.T) {
	s, st, ts := newTestControld(t)
	// The ghost's destroy is the reconcile-finished signal: rows seeded after
	// it can't be swept by the announce reconciliation that runs first.
	f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
		Sessions: []runner.SessionInfo{{ID: ghostSession, State: "running"}}})
	waitConnected(t, s, "vm1")
	awaitReconciled(t, f)

	id := "sess_evt"
	seedSession(t, st, control.Session{ID: control.SessionID(id), State: control.StateCreating, RunnerID: "vm1"})

	f.event(t, id, "running")
	wantState(t, st, id, control.StateRunning)

	f.event(t, id, "dead")
	got := wantState(t, st, id, control.StateDead)
	if got.Error != "runner reported dead" {
		t.Fatalf("error = %q, want %q", got.Error, "runner reported dead")
	}

	// A runner may only speak for sessions the store places on it (or on
	// nobody yet): otherwise a stale holder of a duplicate could drive a
	// session that has since been re-placed to a terminal state.
	t.Run("event from a runner the session is not placed on is ignored", func(t *testing.T) {
		elsewhere := "sess_elsewhere"
		seedSession(t, st, control.Session{ID: control.SessionID(elsewhere), State: control.StateRunning, RunnerID: "vm2"})
		sync := "sess_sync"
		seedSession(t, st, control.Session{ID: control.SessionID(sync), State: control.StateCreating, RunnerID: "vm1"})

		f.event(t, elsewhere, "dead")
		f.event(t, sync, "running")
		// The reader handles messages in order, so the second event landing
		// proves the first has already been fully handled (and ignored).
		wantState(t, st, sync, control.StateRunning)

		got := getSession(t, st, elsewhere)
		if got.State != control.StateRunning || got.RunnerID != "vm2" {
			t.Fatalf("row = %s on %q, want running on vm2 (untouched)", got.State, got.RunnerID)
		}
	})

	// "running" tolerates an unplaced row (it can outrun the create's own
	// queued->creating transition), but "dead" must not: an unplaced row is
	// one a requeue cleared, and the scheduler may already be re-placing it.
	// A stale holder reporting its copy dead would otherwise kill a session
	// that is alive — terminally, since dead is not recoverable.
	t.Run("dead event for an unplaced (requeued) row is ignored", func(t *testing.T) {
		requeued := "sess_requeued"
		seedSession(t, st, control.Session{ID: control.SessionID(requeued), State: control.StateQueued})
		sync := "sess_sync2"
		seedSession(t, st, control.Session{ID: control.SessionID(sync), State: control.StateCreating, RunnerID: "vm1"})

		f.event(t, requeued, "dead")
		f.event(t, sync, "running")
		// In-order handling again: the second event landing proves the first
		// has already been handled (and ignored).
		wantState(t, st, sync, control.StateRunning)

		got := getSession(t, st, requeued)
		if got.State != control.StateQueued || got.RunnerID != "" {
			t.Fatalf("row = %s on %q, want queued and unplaced (untouched)", got.State, got.RunnerID)
		}
	})
}

// TestCapacityRidesEveryMessage pins the piggyback rule: Used/Total on a
// result or event updates the runner row just like an announce does.
func TestCapacityRidesEveryMessage(t *testing.T) {
	s, st, ts := newTestControld(t)
	f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Used: 0, Total: 4})
	waitConnected(t, s, "vm1")

	f.setCapacity(3, 8)
	f.event(t, "sess_unknown", "running") // unknown session: only capacity matters

	eventually(t, 3*time.Second, func() error {
		rs, err := st.Fleet().ListRunners(context.Background(), installPool)
		if err != nil {
			return err
		}
		if len(rs) != 1 || rs[0].CapacityUsed != 3 || rs[0].CapacityTotal != 8 {
			return fmt.Errorf("runners = %+v, want used:3 total:8", rs)
		}
		if !rs[0].Connected {
			return fmt.Errorf("runner marked disconnected")
		}
		return nil
	})
}

// TestReconnectReplacesConn pins the reconnect discipline: the new conn owns
// the runner, the old one is closed, and the old conn's teardown must not
// mark the (live) new conn disconnected.
func TestReconnectReplacesConn(t *testing.T) {
	s, st, ts := newTestControld(t)
	first := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4})
	waitConnected(t, s, "vm1")

	second := startFakeRunner(t, ts, runnerScript{Name: "vm1", Used: 2, Total: 4})
	if err := first.waitClosed(t); err == nil {
		t.Fatal("first connection was not closed by the reconnect")
	}

	// The runner stays connected — the replaced conn's cleanup is
	// pointer-guarded — and dispatch reaches the new socket.
	eventually(t, 3*time.Second, func() error {
		rs, err := st.Fleet().ListRunners(context.Background(), installPool)
		if err != nil {
			return err
		}
		if len(rs) != 1 || !rs[0].Connected || rs[0].CapacityUsed != 2 {
			return fmt.Errorf("runners = %+v, want one connected runner with used:2", rs)
		}
		return nil
	})

	drainAccept(t, second)
	errc := make(chan error, 1)
	go func() {
		_, err := s.transport.Dispatch(context.Background(), installPool, "vm1",
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

	if !s.runnerConnected("vm1") {
		t.Fatal("runnerConnected(vm1) = false after reconnect")
	}
}

// raceStore forces the one interleaving the runner row's connected flag can
// lose to: it holds a connection teardown's SetRunnerConnected(false) until
// a redial's UpsertRunner(connected: true) has landed, so the stale
// disconnect is guaranteed to write last. The wait has a grace period
// because a correctly serialized controld makes that upsert wait on *us* —
// without the fallback the test would wedge instead of passing.
type raceStore struct {
	MemStore
	entered  chan struct{} // the first disconnect write has begun
	upserted chan struct{} // some connected:true upsert has completed
	wrote    chan struct{} // the first disconnect write has returned

	enteredOnce  sync.Once
	upsertedOnce sync.Once
	wroteOnce    sync.Once
}

func newRaceStore() *raceStore {
	return &raceStore{
		MemStore: NewMemStore(),
		entered:  make(chan struct{}),
		upserted: make(chan struct{}),
		wrote:    make(chan struct{}),
	}
}

func (r *raceStore) Fleet() control.FleetRepository {
	return raceFleet{FleetRepository: r.MemStore.Fleet(), owner: r}
}

type raceFleet struct {
	control.FleetRepository
	owner *raceStore
}

func (r raceFleet) SetRunnerConnected(ctx context.Context, pool control.PoolID, id control.RunnerID, connected bool) error {
	if connected {
		return r.FleetRepository.SetRunnerConnected(ctx, pool, id, connected)
	}
	o := r.owner
	o.enteredOnce.Do(func() { close(o.entered) })
	select {
	case <-o.upserted:
	case <-time.After(500 * time.Millisecond):
	}
	err := r.FleetRepository.SetRunnerConnected(ctx, pool, id, connected)
	o.wroteOnce.Do(func() { close(o.wrote) })
	return err
}

func (r raceFleet) UpsertRunner(ctx context.Context, pool control.PoolID, run control.Runner) error {
	err := r.FleetRepository.UpsertRunner(ctx, pool, run)
	// Only a connect that happens *after* the teardown began is the one this
	// test is racing; the first runner's own announce must not release it.
	o := r.owner
	select {
	case <-o.entered:
		if run.Connected {
			o.upsertedOnce.Do(func() { close(o.upserted) })
		}
	default:
	}
	return err
}

func awaitChan(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not happen within 5s", what)
	}
}

// TestRedialSurvivesStaleDisconnect pins the ordering a pointer guard alone
// can't hold: a dying connection's disconnect write must never land on top
// of a redial's connect. The protocol is announce-once, so a runner that
// loses this race sends nothing further to correct the row — it would stay
// invisible to the scheduler until its next redial.
func TestRedialSurvivesStaleDisconnect(t *testing.T) {
	rs := newRaceStore()
	s, ts := newTestControldOver(t, rs)

	first := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4})
	waitConnected(t, s, "vm1")
	first.close()

	// The teardown is now inside its disconnect write; redial into that gap.
	awaitChan(t, rs.entered, "teardown's disconnect write")
	startFakeRunner(t, ts, runnerScript{Name: "vm1", Used: 1, Total: 4})
	awaitChan(t, rs.wrote, "teardown's disconnect write returning")

	eventually(t, 3*time.Second, func() error {
		runners, err := rs.Fleet().ListRunners(context.Background(), installPool)
		if err != nil {
			return err
		}
		if len(runners) != 1 {
			return fmt.Errorf("runners = %+v, want 1", runners)
		}
		if !runners[0].Connected {
			return fmt.Errorf("runner marked disconnected while its redial is live: %+v", runners[0])
		}
		return nil
	})
	if !s.runnerConnected("vm1") {
		t.Fatal("runnerConnected(vm1) = false after the redial")
	}
}

// TestBroadcastToRunners pins the fan-out helper the setup pipeline's prepull
// rides on: every connected runner except the one named receives the message.
func TestBroadcastToRunners(t *testing.T) {
	s, _, ts := newTestControld(t)
	f1 := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
	f2 := joinRunner(t, s, ts, runnerScript{Name: "vm2", Total: 4})
	f3 := joinRunner(t, s, ts, runnerScript{Name: "vm3", Total: 4})

	const ref = "rainier-env:env_x-0123456789ab"
	s.broadcastToRunners(runner.ToRunner{Type: "prepull", Ref: ref}, "vm1")

	for _, f := range []*fakeRunner{f2, f3} {
		cmd := f.nextCmd(t)
		if cmd.Type != "prepull" || cmd.Ref != ref {
			t.Fatalf("%s got %+v, want the prepull of %s", f.name, cmd, ref)
		}
	}

	// vm1 was excluded. Sending it something else now and seeing that arrive
	// first proves no prepull was ever queued ahead of it — the connection
	// delivers in order.
	if err := s.sendToRunner("vm1", runner.ToRunner{Type: "destroy", Session: "sess_probe"}); err != nil {
		t.Fatalf("sendToRunner(vm1): %v", err)
	}
	if cmd := f1.nextCmd(t); cmd.Type != "destroy" {
		t.Fatalf("vm1 got %+v, want the destroy (it must not have received the prepull)", cmd)
	}
}

// TestSendToRunnerUnreachable pins the fire-and-forget path's error, which
// later tasks map to the API's runner_unreachable code.
func TestSendToRunnerUnreachable(t *testing.T) {
	s, _, _ := newTestControld(t)
	err := s.sendToRunner("vm-nope", runner.ToRunner{Type: "destroy", Session: "sess_x"})
	if !errors.Is(err, ErrRunnerUnreachable) {
		t.Fatalf("err = %v, want ErrRunnerUnreachable", err)
	}
}

// ---------------------------------------------------------------------------
// setup orchestration (design §4.3): the four clauses of the setup pipeline's
// controld half — the failure arm, the caching arm, its no-ops, and the
// session state the setup events deliberately do NOT touch.
// ---------------------------------------------------------------------------

// TestSetupDoneCachesTheSnapshot drives the whole happy path through the real
// scheduler and the real connections: a queued session on an environment with
// a setup script is placed, its create carries the script, and the runner's
// setup_done turns into a snapshot at the content-addressed ref, a guarded
// store write, and a prepull for the rest of the fleet.
//
// It also pins the one structural fact the orchestration turns on: the
// snapshot's result is delivered by the SAME reader that handled the
// setup_done, so an orchestration that ran inline in that reader could never
// see this reply — the environment would simply never get cached.
func TestSetupDoneCachesTheSnapshot(t *testing.T) {
	s, st, ts := newTestControld(t)
	// vm1 has strictly more free capacity, so the session places there; vm2 is
	// the fleet member the prepull must reach.
	f1 := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
	f2 := joinRunner(t, s, ts, runnerScript{Name: "vm2", Total: 1})

	env := seedEnv(t, st, control.Environment{Name: "dev", Image: "img:1", Setup: "apt-get install -y jq"})
	seedSession(t, st, control.Session{ID: "sess_setup", State: control.StateQueued, Name: "setup1",
		EnvironmentID: env.ID, Spec: control.PortableSpec{Image: env.Image}, CreatedAt: time.Now().Add(-time.Hour)})
	startRun(t, s)

	create := nextCreate(t, f1)
	if create.Spec == nil || create.Spec.Setup != env.Setup {
		t.Fatalf("create spec = %+v, want one carrying the environment's setup script", create.Spec)
	}
	f1.reply(t, create, true, "")
	f1.event(t, "sess_setup", "running")
	wantState(t, st, "sess_setup", control.StateRunning)

	f1.eventDetail(t, "sess_setup", "setup_done", "")

	wantRef := "rainier-env:" + string(env.ID) + "-" + env.SetupHash[:12]
	snap := nextOfType(t, f1, "snapshot")
	if snap.Session != "sess_setup" || snap.Ref != wantRef {
		t.Fatalf("snapshot command = %+v, want sess_setup at ref %s", snap, wantRef)
	}
	f1.reply(t, snap, true, snap.Ref)

	eventually(t, 3*time.Second, func() error {
		got, holder := envRow(t, st, env.ID), envSnapshotRunner(t, st, env.ID)
		if got.Snapshot.Ref != wantRef || holder != "vm1" || got.SnapshotHash != env.SetupHash {
			return fmt.Errorf("environment cache = %q/%q/%q, want %q/vm1/%s",
				got.Snapshot.Ref, holder, got.SnapshotHash, wantRef, env.SetupHash)
		}
		return nil
	})

	// Clause 4: a setup event says nothing about the session's own state. The
	// `running` event governs, and it already did.
	if got := getSession(t, st, "sess_setup"); got.State != control.StateRunning {
		t.Fatalf("session = %q after setup_done, want running (untouched)", got.State)
	}

	// Every OTHER connected runner gets the head start...
	if cmd := f2.nextCmd(t); cmd.Type != "prepull" || cmd.Ref != wantRef {
		t.Fatalf("vm2 got %+v, want the prepull of %s", cmd, wantRef)
	}
	// ...and the runner that built it does not: it already holds the image,
	// and with no registry in v0 the ref names something only it has, so a
	// prepull there could only fail.
	wantNothingQueued(t, s, f1)
}

// TestSetupDoneNoOps covers clause 3: the setup finished, but there is
// nothing to cache. Each case must be a log line and nothing else — no
// snapshot command, no store write, no session change.
func TestSetupDoneNoOps(t *testing.T) {
	t.Run("scratch session", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		seedSession(t, st, control.Session{ID: "sess_scratch", State: control.StateRunning, RunnerID: "vm1", Spec: control.PortableSpec{Image: "img:1"}})

		f.eventDetail(t, "sess_scratch", "setup_done", "")
		wantEventHandled(t, st, f, "sess_sync_scratch")
		wantNothingQueued(t, s, f)

		if got := getSession(t, st, "sess_scratch"); got.State != control.StateRunning {
			t.Fatalf("session = %q, want running (untouched)", got.State)
		}
	})

	t.Run("environment already cached at the current hash", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		env := seedEnv(t, st, control.Environment{Name: "dev", Image: "img:1", Setup: "echo hi"})
		const ref = "rainier-env:already-cached"
		env = cacheEnvSnapshot(t, st, env, ref, "vm2")
		seedSetupSession(t, st, "sess_cached", "vm1", env)

		f.eventDetail(t, "sess_cached", "setup_done", "")
		wantEventHandled(t, st, f, "sess_sync_cached")
		wantNothingQueued(t, s, f)

		got, holder := envRow(t, st, env.ID), envSnapshotRunner(t, st, env.ID)
		if got.Snapshot.Ref != ref || holder != "vm2" {
			t.Fatalf("environment cache = %q on %q, want %q on vm2 (untouched)", got.Snapshot.Ref, holder, ref)
		}
	})

	t.Run("environment deleted while the setup ran", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		env := seedEnv(t, st, control.Environment{Name: "doomed", Image: "img:1", Setup: "echo hi"})
		seedSetupSession(t, st, "sess_orphan", "vm1", env)
		if err := st.Environments().DeleteEnvironment(context.Background(), installWorkspace, env.ID); err != nil {
			t.Fatalf("DeleteEnvironment: %v", err)
		}

		f.eventDetail(t, "sess_orphan", "setup_done", "")
		wantEventHandled(t, st, f, "sess_sync_orphan")
		wantNothingQueued(t, s, f)
	})

	t.Run("session that ran the setup over an image of its own", func(t *testing.T) {
		// An `image` override still gets the environment's setup script, so
		// setup_done arrives — but what that container holds is the script
		// applied to somebody else's base. Publishing it under the
		// environment's hash would hand every later session an image nobody
		// asked for.
		s, st, ts := newTestControld(t)
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		env := seedEnv(t, st, control.Environment{Name: "dev", Image: "img:1", Setup: "echo hi"})
		seedSession(t, st, control.Session{ID: "sess_override", State: control.StateRunning, RunnerID: "vm1",
			Spec: control.PortableSpec{Image: "myfork:latest"}, EnvironmentID: env.ID,
			SetupHash: SetupHash("myfork:latest", env.Setup)})

		f.eventDetail(t, "sess_override", "setup_done", "")
		wantEventHandled(t, st, f, "sess_sync_override")
		wantNothingQueued(t, s, f)

		if got := envRow(t, st, env.ID); got.Snapshot.Ref != "" {
			t.Fatalf("environment cached as %q from an overridden image; want no cache", got.Snapshot.Ref)
		}
	})
}

// TestSetupFailedFailsTheSession covers clause 1. The Detail runnerd sends is
// already a composed sentence ("rc N: <tail>", rc -1 for the timeout kill);
// controld prefixes it and never parses the rc back out.
func TestSetupFailedFailsTheSession(t *testing.T) {
	for _, tc := range []struct {
		name   string
		from   control.SessionState
		detail string
		want   string
	}{
		{
			name:   "from creating",
			from:   control.StateCreating,
			detail: "rc 7: boom",
			want:   "setup failed: rc 7: boom",
		},
		{
			// The wrapper only execs the agent on rc 0, so in practice the row
			// is still creating — but the registration `running` event can
			// outrun the rc write, so that from-state must be accepted too.
			name:   "from running",
			from:   control.StateRunning,
			detail: "rc -1: setup timed out after 900s",
			want:   "setup failed: rc -1: setup timed out after 900s",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, st, ts := newTestControld(t)
			f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
			seedSession(t, st, control.Session{ID: "sess_failed", State: tc.from, RunnerID: "vm1"})

			f.eventDetail(t, "sess_failed", "setup_failed", tc.detail)

			got := wantState(t, st, "sess_failed", control.StateFailed)
			if got.Error != tc.want {
				t.Fatalf("error = %q, want %q", got.Error, tc.want)
			}
		})
	}
}

// TestStageFailedFailsTheSession pins the generalization of the arm above: a
// session's boot is a CHAIN of stages (setup, then clone, then init) and any
// of them can be the one that fails. runnerd front-loads the stage into the
// one free-text field an event has ("clone: rc 128: ..."), controld reads it
// back off the front and writes the same sentence the setup stage has always
// produced.
func TestStageFailedFailsTheSession(t *testing.T) {
	for _, tc := range []struct {
		name   string
		from   control.SessionState
		detail string
		want   string
	}{
		{
			name:   "a clone that could not authenticate",
			from:   control.StateCreating,
			detail: "clone: rc 128: fatal: Authentication failed for 'https://github.com/acme/app.test/'",
			want:   "clone failed: rc 128: fatal: Authentication failed for 'https://github.com/acme/app.test/'",
		},
		{
			// The registration `running` event can outrun the stage's rc
			// write, exactly as it can for setup, so that from-state is
			// accepted too (see setupFailedFrom).
			name:   "an init that was killed at its timeout",
			from:   control.StateRunning,
			detail: "init: rc -1: init timed out after 300s",
			want:   "init failed: rc -1: init timed out after 300s",
		},
		{
			// The same failure the legacy `setup_failed` name carries, under
			// the new one: both spellings must compose the identical sentence,
			// because which one a session sends depends only on how old the
			// sessiond inside its image is.
			name:   "the setup stage under the new name",
			from:   control.StateCreating,
			detail: "setup: rc 7: boom",
			want:   "setup failed: rc 7: boom",
		},
		{
			// A detail that names no stage at all can only come from a sender
			// that is not following the contract — but the session behind it
			// really has stopped booting, and dropping the event would leave
			// the row `creating` forever. It fails under a stage name that
			// claims nothing.
			name:   "a detail with no stage in front of it",
			from:   control.StateCreating,
			detail: "rc 3",
			want:   "boot failed: rc 3",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, st, ts := newTestControld(t)
			f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
			seedSession(t, st, control.Session{ID: "sess_staged", State: tc.from, RunnerID: "vm1"})

			f.eventDetail(t, "sess_staged", "stage_failed", tc.detail)

			got := wantState(t, st, "sess_staged", control.StateFailed)
			if got.Error != tc.want {
				t.Fatalf("error = %q, want %q", got.Error, tc.want)
			}
		})
	}
}

// TestCredentialRejectedFlipsTheStoredCredential pins the lazy revocation
// arm. The vault mints optimistically — no GitHub round-trip per mint — so a
// git operation that GitHub refused is the ONLY signal a stored token has
// been revoked, and this event is how that signal gets out of the sandbox.
//
// The event carries nothing at all: controld knows whose credential it minted
// for this session, because the session row says who owns it.
func TestCredentialRejectedFlipsTheStoredCredential(t *testing.T) {
	s, st, ts := newTestControld(t)
	f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
	u := seedVaultUser(t, st, 8801, "alice")
	seedGitHubCredential(t, s, st, u.ID)
	seedSession(t, st, control.Session{ID: "sess_rejected", State: control.StateRunning, RunnerID: "vm1", CreatorID: control.ActorID(u.ID)})

	f.event(t, "sess_rejected", "credential_rejected")

	wantCredentialStatus(t, st, u.ID, CredentialNeedsRefresh)
	// A rejection says nothing about the SESSION: the git operation failed,
	// the container did not. It keeps running (and its slot).
	if got := getSession(t, st, "sess_rejected"); got.State != control.StateRunning {
		t.Fatalf("session state = %q, want it left running — a refused credential is not a dead session", got.State)
	}
}

// TestCredentialEventsFromANonPlacedRunnerAreIgnored is the credential half
// of the placement guard. The runner token is fleet-wide, so without it any
// runner in the fleet could flip any user's credential to needs_refresh by
// naming a session it does not hold — a one-message denial of service against
// every git operation that user has running anywhere.
func TestCredentialEventsFromANonPlacedRunnerAreIgnored(t *testing.T) {
	s, st, ts := newTestControld(t)
	f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
	u := seedVaultUser(t, st, 8802, "bob")
	seedGitHubCredential(t, s, st, u.ID)

	t.Run("credential_rejected for a session placed elsewhere", func(t *testing.T) {
		seedSession(t, st, control.Session{ID: "sess_cred_elsewhere", State: control.StateRunning, RunnerID: "vm2", CreatorID: control.ActorID(u.ID)})

		f.event(t, "sess_cred_elsewhere", "credential_rejected")
		wantEventHandled(t, st, f, "sess_sync_cr")

		if got := getCredential(t, st, u.ID); got.Status != CredentialValid {
			t.Fatalf("credential status = %q, want it untouched (%q)", got.Status, CredentialValid)
		}
	})

	t.Run("credential_rejected for an unplaced (requeued) row", func(t *testing.T) {
		seedSession(t, st, control.Session{ID: "sess_cred_requeued", State: control.StateQueued, CreatorID: control.ActorID(u.ID)})

		f.event(t, "sess_cred_requeued", "credential_rejected")
		wantEventHandled(t, st, f, "sess_sync_cq")

		if got := getCredential(t, st, u.ID); got.Status != CredentialValid {
			t.Fatalf("credential status = %q, want it untouched (%q)", got.Status, CredentialValid)
		}
	})

	t.Run("stage_failed for a session placed elsewhere", func(t *testing.T) {
		seedSession(t, st, control.Session{ID: "sess_stage_elsewhere", State: control.StateRunning, RunnerID: "vm2", CreatorID: control.ActorID(u.ID)})

		f.eventDetail(t, "sess_stage_elsewhere", "stage_failed", "clone: rc 128: nope")
		wantEventHandled(t, st, f, "sess_sync_se")

		got := getSession(t, st, "sess_stage_elsewhere")
		if got.State != control.StateRunning || got.RunnerID != "vm2" {
			t.Fatalf("row = %s on %q, want running on vm2 (untouched)", got.State, got.RunnerID)
		}
	})
}

// TestChildExitedRecordsTheExitCode pins the third event arm: the agent
// process inside a session finished, and its exit status is the one fact
// about a session that nothing else in the fleet can supply. It is an
// OBSERVATION — the session is still running, still attachable, still holding
// its slot — so it writes a column and moves no state machine.
func TestChildExitedRecordsTheExitCode(t *testing.T) {
	t.Run("records the code and leaves the session running", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		seedSession(t, st, control.Session{ID: "sess_exit", State: control.StateRunning, RunnerID: "vm1"})

		f.eventDetail(t, "sess_exit", "child_exited", "137")
		eventually(t, 3*time.Second, func() error {
			got := getSession(t, st, "sess_exit")
			if got.ChildExitCode == nil {
				return fmt.Errorf("child exit code not recorded yet")
			}
			if *got.ChildExitCode != 137 {
				return fmt.Errorf("child exit code = %d, want 137", *got.ChildExitCode)
			}
			return nil
		})
		if got := getSession(t, st, "sess_exit"); got.State != control.StateRunning {
			t.Fatalf("state = %q after child_exited, want running — an exited agent leaves the session up for viewers", got.State)
		}
	})

	t.Run("exit 0 is a recorded answer, not an absence", func(t *testing.T) {
		// The failure this guards is a `if code != 0` anywhere on the path:
		// a clean finish would then be indistinguishable from an agent still
		// working, which is the single most common case there is.
		s, st, ts := newTestControld(t)
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		seedSession(t, st, control.Session{ID: "sess_exit_zero", State: control.StateRunning, RunnerID: "vm1"})

		f.eventDetail(t, "sess_exit_zero", "child_exited", "0")
		eventually(t, 3*time.Second, func() error {
			got := getSession(t, st, "sess_exit_zero")
			if got.ChildExitCode == nil {
				return fmt.Errorf("exit 0 not recorded")
			}
			if *got.ChildExitCode != 0 {
				return fmt.Errorf("child exit code = %d, want 0", *got.ChildExitCode)
			}
			return nil
		})
	})

	t.Run("from a runner the session is not placed on, ignored", func(t *testing.T) {
		// Same guard as the setup arms, for the same reason: the runner token
		// is fleet-wide, so without it any runner could stamp any session with
		// any exit code.
		s, st, ts := newTestControld(t)
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		seedSession(t, st, control.Session{ID: "sess_exit_elsewhere", State: control.StateRunning, RunnerID: "vm2"})

		f.eventDetail(t, "sess_exit_elsewhere", "child_exited", "1")
		wantEventHandled(t, st, f, "sess_sync_exit_e")

		if got := getSession(t, st, "sess_exit_elsewhere"); got.ChildExitCode != nil {
			t.Fatalf("child exit code = %d, recorded from a runner the session is not placed on", *got.ChildExitCode)
		}
	})

	t.Run("an unparseable code is dropped, not guessed at", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		seedSession(t, st, control.Session{ID: "sess_exit_junk", State: control.StateRunning, RunnerID: "vm1"})

		f.eventDetail(t, "sess_exit_junk", "child_exited", "not a number")
		wantEventHandled(t, st, f, "sess_sync_exit_j")

		got := getSession(t, st, "sess_exit_junk")
		if got.ChildExitCode != nil {
			t.Fatalf("child exit code = %d from an undecodable detail; 0 would read as a clean exit", *got.ChildExitCode)
		}
		if got.State != control.StateRunning {
			t.Fatalf("state = %q, want running (untouched)", got.State)
		}
	})
}

// TestSetupEventsFromANonPlacedRunnerAreIgnored pins the placement guard on
// both setup arms. The fleet token is one token: without this, any runner
// could fail any session, or publish its own container's image as any
// environment's cache.
func TestSetupEventsFromANonPlacedRunnerAreIgnored(t *testing.T) {
	s, st, ts := newTestControld(t)
	f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
	env := seedEnv(t, st, control.Environment{Name: "dev", Image: "img:1", Setup: "echo hi"})

	t.Run("setup_failed for a session placed elsewhere", func(t *testing.T) {
		seedSetupSession(t, st, "sess_elsewhere_failed", "vm2", env)

		f.eventDetail(t, "sess_elsewhere_failed", "setup_failed", "rc 1: boom")
		wantEventHandled(t, st, f, "sess_sync_ef")

		got := getSession(t, st, "sess_elsewhere_failed")
		if got.State != control.StateRunning || got.RunnerID != "vm2" {
			t.Fatalf("row = %s on %q, want running on vm2 (untouched)", got.State, got.RunnerID)
		}
	})

	t.Run("setup_failed for an unplaced (requeued) row", func(t *testing.T) {
		seedSession(t, st, control.Session{ID: "sess_requeued_failed", State: control.StateQueued,
			EnvironmentID: env.ID, Spec: control.PortableSpec{Image: env.Image}, SetupHash: SetupHash(env.Image, env.Setup)})

		f.eventDetail(t, "sess_requeued_failed", "setup_failed", "rc 1: boom")
		wantEventHandled(t, st, f, "sess_sync_rf")

		got := getSession(t, st, "sess_requeued_failed")
		if got.State != control.StateQueued || got.RunnerID != "" {
			t.Fatalf("row = %s on %q, want queued and unplaced (untouched)", got.State, got.RunnerID)
		}
	})

	t.Run("setup_done for a session placed elsewhere", func(t *testing.T) {
		seedSetupSession(t, st, "sess_elsewhere_done", "vm2", env)

		f.eventDetail(t, "sess_elsewhere_done", "setup_done", "")
		wantEventHandled(t, st, f, "sess_sync_ed")
		wantNothingQueued(t, s, f)

		if got := envRow(t, st, env.ID); got.Snapshot.Ref != "" {
			t.Fatalf("environment cached as %q on a report from a runner it isn't placed on", got.Snapshot.Ref)
		}
	})

	t.Run("setup_done for an unplaced (requeued) row", func(t *testing.T) {
		seedSession(t, st, control.Session{ID: "sess_requeued_done", State: control.StateQueued,
			EnvironmentID: env.ID, Spec: control.PortableSpec{Image: env.Image}, SetupHash: SetupHash(env.Image, env.Setup)})

		f.eventDetail(t, "sess_requeued_done", "setup_done", "")
		wantEventHandled(t, st, f, "sess_sync_rd")
		wantNothingQueued(t, s, f)

		if got := envRow(t, st, env.ID); got.Snapshot.Ref != "" {
			t.Fatalf("environment cached as %q from an unplaced row", got.Snapshot.Ref)
		}
	})
}

// TestSetupDoneAfterAScriptEditDoesNotCache is the window the session's
// pinned SetupHash exists for: the environment's SCRIPT is edited while the
// first session's setup is still running. Nothing else can see it — the row's
// resolved image still matches, and the hash recomputed at event time is the
// NEW one, which the guarded store write would happily accept — so without
// the pin the old script's container would be published as the new script's
// cache, and every later session would get the wrong toolchain silently.
func TestSetupDoneAfterAScriptEditDoesNotCache(t *testing.T) {
	s, st, ts := newTestControld(t)
	f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})

	env := seedEnv(t, st, control.Environment{Name: "dev", Image: "img:1", Setup: "install v1"})
	seedSession(t, st, control.Session{ID: "sess_edited", State: control.StateQueued, Name: "edited",
		EnvironmentID: env.ID, Spec: control.PortableSpec{Image: env.Image}, CreatedAt: time.Now().Add(-time.Hour)})
	startRun(t, s)

	// The dispatch carries v1 of the script and pins it (production code, not
	// the test, writes that pin).
	create := nextCreate(t, f)
	if create.Spec == nil || create.Spec.Setup != "install v1" {
		t.Fatalf("create spec = %+v, want the v1 setup script", create.Spec)
	}
	f.reply(t, create, true, "")
	eventually(t, 3*time.Second, func() error {
		if got := getSession(t, st, "sess_edited"); got.SetupHash != SetupHash(env.Image, "install v1") {
			return fmt.Errorf("session setup hash = %q, want the dispatched script's", got.SetupHash)
		}
		return nil
	})

	// ...and the script changes while it runs.
	edited := env
	edited.Setup = "install v2"
	edited.SetupHash = SetupHash(edited.Image, edited.Setup)
	if _, err := st.Environments().UpdateEnvironment(context.Background(), installWorkspace, edited); err != nil {
		t.Fatalf("UpdateEnvironment: %v", err)
	}

	f.event(t, "sess_edited", "running")
	wantState(t, st, "sess_edited", control.StateRunning)
	f.eventDetail(t, "sess_edited", "setup_done", "")
	wantEventHandled(t, st, f, "sess_sync_edited")

	// No snapshot may be dispatched at all: what that container holds is v1.
	wantNothingQueued(t, s, f)
	if got := envRow(t, st, env.ID); got.Snapshot.Ref != "" || got.SnapshotHash != "" {
		t.Fatalf("environment cached as %q/%q from a container that ran the pre-edit script",
			got.Snapshot.Ref, got.SnapshotHash)
	}
}

// envSnapshotSpy records the outcome of every SetEnvironmentSnapshot the
// orchestration attempts, so a test can wait for the guarded write — and its
// verdict — instead of polling for the absence of one.
type envSnapshotSpy struct {
	MemStore
	calls chan error
}

func newEnvSnapshotSpy() *envSnapshotSpy {
	return &envSnapshotSpy{MemStore: NewMemStore(), calls: make(chan error, 8)}
}

func (e *envSnapshotSpy) Environments() control.EnvironmentRepository {
	return spyEnvironments{EnvironmentRepository: e.MemStore.Environments(), owner: e}
}

type spyEnvironments struct {
	control.EnvironmentRepository
	owner *envSnapshotSpy
}

func (e spyEnvironments) SetEnvironmentSnapshot(ctx context.Context, ws control.WorkspaceID, envID control.EnvironmentID, expectHash, ref string, runnerID control.RunnerID) error {
	err := e.EnvironmentRepository.SetEnvironmentSnapshot(ctx, ws, envID, expectHash, ref, runnerID)
	select {
	case e.owner.calls <- err:
	default:
	}
	return err
}

// TestSetupDoneStaleEnvironmentIsDropped is the race the guarded store write
// exists for: the environment is edited while its snapshot is being built, so
// the image that comes back is of a setup nobody asked for any more. It must
// be dropped, not recorded, and nothing may be broadcast for it.
func TestSetupDoneStaleEnvironmentIsDropped(t *testing.T) {
	spy := newEnvSnapshotSpy()
	s, ts := newTestControldOver(t, spy)
	f1 := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
	f2 := joinRunner(t, s, ts, runnerScript{Name: "vm2", Total: 4})

	env := seedEnv(t, spy, control.Environment{Name: "dev", Image: "img:1", Setup: "echo one"})
	seedSetupSession(t, spy, "sess_stale", "vm1", env)

	f1.eventDetail(t, "sess_stale", "setup_done", "")
	snap := nextOfType(t, f1, "snapshot")
	if want := "rainier-env:" + string(env.ID) + "-" + env.SetupHash[:12]; snap.Ref != want {
		t.Fatalf("snapshot ref = %q, want %q", snap.Ref, want)
	}

	// The edit lands while the runner is still building: the ref in flight now
	// names an image of the OLD script.
	edited := env
	edited.Setup = "echo two"
	edited.SetupHash = SetupHash(edited.Image, edited.Setup)
	if _, err := spy.Environments().UpdateEnvironment(context.Background(), installWorkspace, edited); err != nil {
		t.Fatalf("UpdateEnvironment: %v", err)
	}
	f1.reply(t, snap, true, snap.Ref)

	select {
	case err := <-spy.calls:
		if !errors.Is(err, control.ErrStale) {
			t.Fatalf("guarded write returned %v, want ErrStale — the stale snapshot must not land", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the orchestration never attempted the guarded write")
	}

	got, holder := envRow(t, spy, env.ID), envSnapshotRunner(t, spy, env.ID)
	if got.Snapshot.Ref != "" || holder != "" || got.SnapshotHash != "" {
		t.Fatalf("environment cache = %q/%q/%q, want all empty", got.Snapshot.Ref, holder, got.SnapshotHash)
	}
	// Nothing was cached, so there is nothing to warm.
	wantNothingQueued(t, s, f2)
}

// TestSetupDoneSnapshotFailureRecordsNothing: a snapshot the runner could not
// build leaves the environment exactly as it was. The next session on it
// simply runs setup again — which is why nothing here needs undoing.
func TestSetupDoneSnapshotFailureRecordsNothing(t *testing.T) {
	spy := newEnvSnapshotSpy()
	s, ts := newTestControldOver(t, spy)
	f1 := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
	f2 := joinRunner(t, s, ts, runnerScript{Name: "vm2", Total: 4})

	env := seedEnv(t, spy, control.Environment{Name: "dev", Image: "img:1", Setup: "echo one"})
	seedSetupSession(t, spy, "sess_snapfail", "vm1", env)

	f1.eventDetail(t, "sess_snapfail", "setup_done", "")
	snap := nextOfType(t, f1, "snapshot")
	f1.reply(t, snap, false, "no space left on device")

	select {
	case err := <-spy.calls:
		t.Fatalf("recorded a snapshot the runner never built (err %v)", err)
	case <-time.After(300 * time.Millisecond):
	}

	if got := envRow(t, spy, env.ID); got.Snapshot.Ref != "" {
		t.Fatalf("environment cached as %q after a failed snapshot", got.Snapshot.Ref)
	}
	wantNothingQueued(t, s, f2)
}

// TestUnroutedPathIs404 pins Handler()'s shape: a path no route claims 404s
// even once every task's routes are registered. (Task 10 claimed
// /v0/sessions itself, so this now probes a path nothing will ever claim.)
func TestUnroutedPathIs404(t *testing.T) {
	_, _, ts := newTestControld(t)
	resp, err := http.Get(ts.URL + "/v0/bogus")
	if err != nil {
		t.Fatalf("GET /v0/bogus: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// generation fencing (Task 5)
// ---------------------------------------------------------------------------

// apiSessionState reads a session's state back through the client API, which
// is the surface a stale runner's report would have to reach to matter.
func apiSessionState(t *testing.T, ts *httptest.Server, tok, id string) control.SessionState {
	t.Helper()
	resp := doRequest(t, ts, http.MethodGet, "/v0/sessions/"+id, tok, nil, nil)
	raw := readBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v0/sessions/%s = %d; body=%s", id, resp.StatusCode, raw)
	}
	var body v0wire.SessionEnvelope
	if err := json.Unmarshal([]byte(raw), &body); err != nil {
		t.Fatalf("decode session view: %v; body=%s", err, raw)
	}
	return control.SessionState(body.Session.State)
}

// writeStale writes m without failing the test when the socket has already
// been closed under it. A superseded connection may or may not still accept a
// frame — either way controld must not act on what arrives.
func (f *fakeRunner) writeStale(m runner.FromRunner) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	f.wmu.Lock()
	defer f.wmu.Unlock()
	m.Used, m.Total = f.used, f.total
	_ = wsjson.Write(ctx, f.c, m)
}

// TestRunnerGenerationFencesAStaleSocket pins both locks on the same door.
// A runner that redials owns its name from that instant: the connection it
// replaced is deregistered (so touchRunner stops reading it) AND every event
// is stamped with the generation of the connection it arrived on, so one that
// slipped past the first check is still refused by the service's fence.
func TestRunnerGenerationFencesAStaleSocket(t *testing.T) {
	t.Run("an event on the superseded socket does not transition the session", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		_, tok := loginUser(t, st, "alice", "member")

		first := joinRunner(t, s, ts, runnerScript{Name: "runner-a", Total: 4})
		second := joinRunner(t, s, ts, runnerScript{Name: "runner-a", Total: 4})

		// Seeded after both announces so neither reconciliation sweeps it.
		id := "sess_fence"
		seedSession(t, st, control.Session{ID: control.SessionID(id), State: control.StateCreating, RunnerID: "runner-a"})

		// The stale socket reports the session dead — terminal, and therefore
		// unrecoverable if it were ever applied.
		first.writeStale(runner.FromRunner{Type: "event", Session: id, State: "dead"})

		// The live socket reports it running, which is what must actually land.
		second.event(t, id, "running")
		eventually(t, 3*time.Second, func() error {
			if got := apiSessionState(t, ts, tok, id); got != control.StateRunning {
				return fmt.Errorf("session state = %q, want running", got)
			}
			return nil
		})
		// ...and stays there: a stale "dead" handled late would take it back.
		time.Sleep(150 * time.Millisecond)
		if got := apiSessionState(t, ts, tok, id); got != control.StateRunning {
			t.Fatalf("session state = %q, want still running — the superseded socket was obeyed", got)
		}
	})

	// The second lock, on its own. touchRunner drops a superseded socket's
	// messages before they are ever translated, so the fence underneath is
	// only observable by handing applyRunnerEvent a connection that claims a
	// generation the fleet has already moved past — which is exactly the
	// message an event racing a reconnect would carry.
	t.Run("an event stamped with a superseded generation is refused", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		joinRunner(t, s, ts, runnerScript{Name: "runner-a", Total: 4}) // generation 1
		joinRunner(t, s, ts, runnerScript{Name: "runner-a", Total: 4}) // generation 2

		id := "sess_gen"
		seedSession(t, st, control.Session{ID: control.SessionID(id), State: control.StateCreating, RunnerID: "runner-a"})
		dead := runner.FromRunner{Type: "event", Session: id, State: "dead"}

		stale := newRunnerConn("runner-a", nil)
		stale.gen = 1
		s.applyRunnerEvent(context.Background(), stale, dead)
		if got := getSession(t, st, id); got.State != control.StateCreating {
			t.Fatalf("state = %q, want still creating — a superseded generation was applied", got.State)
		}

		// The authoritative generation is the store's, not a table inside
		// this process: it is on the runner's own row.
		rows, err := st.Fleet().ListRunners(context.Background(), installPool)
		if err != nil || len(rows) != 1 {
			t.Fatalf("list runners: %+v, %v", rows, err)
		}
		live := newRunnerConn("runner-a", nil)
		live.gen = rows[0].Generation
		if live.gen == stale.gen {
			t.Fatalf("generation did not advance across the redial: %d", live.gen)
		}
		s.applyRunnerEvent(context.Background(), live, dead)
		if got := wantState(t, st, id, control.StateDead); got.Error != "runner reported dead" {
			t.Fatalf("error = %q, want %q", got.Error, "runner reported dead")
		}
	})
}

// ---------------------------------------------------------------------------
// capability negotiation (plan 8, D19)
// ---------------------------------------------------------------------------

// TestAnnouncedCapabilities: what a runner claims about itself is validated,
// unioned with the two capabilities the HOST spells for its name, persisted,
// and acknowledged — the accept being the first thing that runner ever hears.
func TestAnnouncedCapabilities(t *testing.T) {
	t.Run("the announced set is unioned with the host's own and accepted first", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Capabilities: []string{"gpu", "docker.rootless"}})
		waitConnected(t, s, "vm1")

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

		want := append(runnerCapabilities("vm1"), "gpu", "docker.rootless")
		wantCapabilities := func(when string) {
			t.Helper()
			eventually(t, 3*time.Second, func() error {
				rows, err := st.Fleet().ListRunners(context.Background(), installPool)
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
		wantEventHandled(t, st, f, "sess_heartbeat")
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
			_, st, ts := newTestControld(t)
			f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4, Capabilities: tc.caps})

			err := f.waitClosed(t)
			var ce websocket.CloseError
			if !errors.As(err, &ce) {
				t.Fatalf("conn ended with %v, want a websocket close", err)
			}
			if !strings.Contains(ce.Reason, "registration refused") {
				t.Fatalf("close reason = %q, want it to name a refused registration", ce.Reason)
			}
			rows, err := st.Fleet().ListRunners(context.Background(), installPool)
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

// TestEventGenerationSource: an event says which generation it was produced
// under, and that claim is what fences it. One that names a generation the
// store has moved past is refused; one that names none is applied under the
// connection's own, exactly as before this plan.
func TestEventGenerationSource(t *testing.T) {
	s, st, ts := newTestControld(t)
	joinRunner(t, s, ts, runnerScript{Name: "runner-a", Total: 4}) // generation 1
	joinRunner(t, s, ts, runnerScript{Name: "runner-a", Total: 4}) // generation 2

	live := newRunnerConn("runner-a", nil)
	live.gen = 2

	id := "sess_evgen"
	seedSession(t, st, control.Session{ID: control.SessionID(id), State: control.StateCreating, RunnerID: "runner-a"})

	// The connection is current, but the message itself claims generation 1.
	s.applyRunnerEvent(context.Background(), live,
		runner.FromRunner{Type: "event", Session: id, State: "dead", Generation: 1})
	if got := getSession(t, st, id); got.State != control.StateCreating {
		t.Fatalf("state = %q, want still creating — an event claiming a superseded generation was applied", got.State)
	}

	// The same event carrying no generation is the old runner's message, and
	// takes the connection's.
	s.applyRunnerEvent(context.Background(), live,
		runner.FromRunner{Type: "event", Session: id, State: "dead"})
	wantState(t, st, id, control.StateDead)
}

// TestEventPlacementGenerationIsFenced: a report echoing the placement
// generation its create carried is applied only while the session is still on
// that placement. The session's own authority, beside the runner's.
func TestEventPlacementGenerationIsFenced(t *testing.T) {
	s, st, ts := newTestControld(t)
	joinRunner(t, s, ts, runnerScript{Name: "runner-a", Total: 4})

	rc := newRunnerConn("runner-a", nil)
	rc.gen = 1

	id := "sess_pgfence"
	row := seedSession(t, st, control.Session{ID: control.SessionID(id), State: control.StateCreating,
		RunnerID: "runner-a", PlacementGeneration: 3})
	if row.PlacementGeneration != 3 {
		t.Fatalf("seeded row is at placement generation %d, want 3", row.PlacementGeneration)
	}
	// The session was requeued and placed again since this sandbox started,
	// so the runner's create carried 2 while the row has moved to 3.
	stale := uint64(2)

	s.applyRunnerEvent(context.Background(), rc,
		runner.FromRunner{Type: "event", Session: id, State: "dead", PlacementGeneration: stale})
	if got := getSession(t, st, id); got.State != control.StateCreating {
		t.Fatalf("state = %q, want still creating — an event from a superseded placement was applied", got.State)
	}

	s.applyRunnerEvent(context.Background(), rc,
		runner.FromRunner{Type: "event", Session: id, State: "dead", PlacementGeneration: row.PlacementGeneration})
	wantState(t, st, id, control.StateDead)
}

// TestCreateCarriesThePlacementGeneration: the create a placed session is
// dispatched with names the generation that placement opened, so every event
// the sandbox produces can be matched against the row.
func TestCreateCarriesThePlacementGeneration(t *testing.T) {
	s, st, ts := newTestControld(t)
	startRun(t, s)
	f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})

	id := "sess_pgcreate"
	seedQueued(t, st, id, 0)
	s.fleet.Wake(installPool)

	cmd := nextCreate(t, f)
	if cmd.Session != id {
		t.Fatalf("create = %+v, want one for %s", cmd, id)
	}
	row := getSession(t, st, id)
	if cmd.PlacementGeneration == 0 || cmd.PlacementGeneration != row.PlacementGeneration {
		t.Fatalf("create PlacementGeneration = %d, want the row's %d", cmd.PlacementGeneration, row.PlacementGeneration)
	}
}

// TestConnectRunnerRefusalDoesNotCloseTheWinningConn pins the ordering
// connectRunner must hold: a reconnect the fleet service is about to refuse
// must never take down the connection that already holds the accepted
// generation. Before #36 registerRunner ran before the fleet check, so a
// delayed, lower-generation redial reaching connectRunner after a newer one
// had already registered swapped itself in and closed the accepted connection
// on its way to being refused itself, leaving that runner disconnected until
// its next redial.
func TestConnectRunnerRefusalDoesNotCloseTheWinningConn(t *testing.T) {
	s, st, ts := newTestControld(t)

	joinRunner(t, s, ts, runnerScript{Name: "runner-a", Total: 4}) // generation 1
	joinRunner(t, s, ts, runnerScript{Name: "runner-a", Total: 4}) // generation 2, replaces gen 1 normally

	live := s.conn("runner-a")
	if live == nil {
		t.Fatal("runner-a has no live connection")
	}
	// The generation is the store's (plan 7): read it back from the fleet row.
	rows, err := st.Fleet().ListRunners(context.Background(), installPool)
	if err != nil || len(rows) != 1 || rows[0].Generation != 2 {
		t.Fatalf("live generation: rows = %+v, err = %v; want runner-a at generation 2", rows, err)
	}

	// A redial that was assigned generation 1 before the reconnect above but
	// only now reaches connectRunner, e.g. because its own handshake was
	// slow. The fleet service must refuse it: generation 1 is superseded.
	stale := newRunnerConn("runner-a", nil)
	stale.gen = 1

	err = s.connectRunner(context.Background(), stale, runner.FromRunner{Total: 4})
	if !errors.Is(err, errRegistrationRefused) {
		t.Fatalf("connectRunner(stale gen 1) error = %v, want errRegistrationRefused", err)
	}

	select {
	case <-live.done:
		t.Fatal("the live (accepted, generation 2) connection was closed by a reconnect attempt the fleet service went on to refuse")
	default:
	}
	if got := s.conn("runner-a"); got != live {
		t.Fatalf("s.conn(runner-a) = %p, want the still-live connection %p: a refused reconnect replaced it in the registry", got, live)
	}
}

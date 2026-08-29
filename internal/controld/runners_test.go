// internal/controld/runners_test.go
package controld

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"rainier/internal/rwire"
)

const testRunnerToken = "rnr_test_runner_token"

// ---------------------------------------------------------------------------
// helpers (Tasks 8 and 10 reuse these: fakeRunner is the scripted runnerd
// stand-in for every controld test that needs a runner on the other end)
// ---------------------------------------------------------------------------

// newTestControld returns a Server over a fresh memstore plus an
// httptest.Server serving its Handler(). OpTimeout is deliberately short so
// timeout-path tests don't wait the production minute; opts override any
// field before New validates it.
func newTestControld(t *testing.T, opts ...func(*Config)) (*Server, Store, *httptest.Server) {
	t.Helper()
	st := NewMemStore()
	s, ts := newTestControldOver(t, st, opts...)
	return s, st, ts
}

// newTestControldOver is newTestControld over a caller-supplied store — for
// tests that wrap the store to force an interleaving.
func newTestControldOver(t *testing.T, st Store, opts ...func(*Config)) (*Server, *httptest.Server) {
	t.Helper()
	cfg := Config{
		RunnerToken: testRunnerToken,
		ExternalURL: "http://controld.test:9090",
		SecretsKey:  testSecretsKey,
		OpTimeout:   2 * time.Second,
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
	return "ws" + strings.TrimPrefix(ts.URL, "http") + "/v1/runners/connect"
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
	Sessions []rwire.SessionInfo
	Used     int
	Total    int
}

// fakeRunner is a scripted runnerd: it dials /v1/runners/connect, writes one
// announce, then records every ToRunner controld sends and answers only when
// the test tells it to. Its reader runs in its own goroutine, so a test never
// has to interleave reads with the assertions it wants to make.
type fakeRunner struct {
	name  string
	c     *websocket.Conn
	cmds  chan rwire.ToRunner
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
	proto := rwire.Proto
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
		cmds:  make(chan rwire.ToRunner, 64),
		readE: make(chan error, 1),
		used:  sc.Used,
		total: sc.Total,
	}
	t.Cleanup(f.close)
	f.write(t, rwire.FromRunner{Type: "announce", Proto: proto, Runner: sc.Name, Sessions: sc.Sessions})
	go f.readLoop()
	return f
}

func (f *fakeRunner) readLoop() {
	for {
		var m rwire.ToRunner
		if err := wsjson.Read(context.Background(), f.c, &m); err != nil {
			f.readE <- err
			return
		}
		f.cmds <- m
	}
}

// write is the fake's single writer. Like the real agent it stamps its
// current capacity onto every outbound message, whatever the type.
func (f *fakeRunner) write(t *testing.T, m rwire.FromRunner) {
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
func (f *fakeRunner) nextCmd(t *testing.T) rwire.ToRunner {
	t.Helper()
	select {
	case m := <-f.cmds:
		return m
	case <-time.After(3 * time.Second):
		t.Fatal("no command from controld within 3s")
		return rwire.ToRunner{}
	}
}

// reply answers one dispatched command, echoing its ReqID (the correlation
// the whole dispatch path turns on) and the runner's current capacity.
func (f *fakeRunner) reply(t *testing.T, cmd rwire.ToRunner, ok bool, detail string) {
	t.Helper()
	f.write(t, rwire.FromRunner{Type: "result", ReqID: cmd.ReqID, OK: ok, Detail: detail})
}

func (f *fakeRunner) event(t *testing.T, session, state string) {
	t.Helper()
	f.write(t, rwire.FromRunner{Type: "event", Session: session, State: state})
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

// ghostSession is announced by tests that need a reconcile-finished signal:
// controld's destroy for it is enqueued at the very end of reconcile, so
// receiving that destroy proves reconcile has run to completion.
const ghostSession = "sess_reconcile_probe"

func awaitReconciled(t *testing.T, f *fakeRunner) {
	t.Helper()
	cmd := f.nextCmd(t)
	if cmd.Type != "destroy" || cmd.Session != ghostSession {
		t.Fatalf("reconcile probe: got %+v, want destroy of %s", cmd, ghostSession)
	}
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
	sc.Sessions = append(append([]rwire.SessionInfo{}, sc.Sessions...), rwire.SessionInfo{ID: probe, State: "running"})
	f := startFakeRunner(t, ts, sc)
	waitConnected(t, s, sc.Name)
	cmd := f.nextCmd(t)
	if cmd.Type != "destroy" || cmd.Session != probe {
		t.Fatalf("reconcile probe on %s: got %+v, want destroy of %s", sc.Name, cmd, probe)
	}
	return f
}

// nextCreate returns the next "create" command f receives, skipping anything
// else controld happens to send in the meantime.
func nextCreate(t *testing.T, f *fakeRunner) rwire.ToRunner {
	t.Helper()
	for {
		if cmd := f.nextCmd(t); cmd.Type == "create" {
			return cmd
		}
	}
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

func seedSession(t *testing.T, st Store, s Session) Session {
	t.Helper()
	if s.OwnerID == "" {
		s.OwnerID = "usr_test"
	}
	out, err := st.CreateSession(context.Background(), s)
	if err != nil {
		t.Fatalf("seed session %s: %v", s.ID, err)
	}
	return out
}

func getSession(t *testing.T, st Store, id string) Session {
	t.Helper()
	s, err := st.GetSession(context.Background(), id)
	if err != nil {
		t.Fatalf("get session %s: %v", id, err)
	}
	return s
}

// wantState polls until the session reaches want, reporting the state it was
// stuck in otherwise.
func wantState(t *testing.T, st Store, id string, want SessionState) Session {
	t.Helper()
	var got Session
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
	rs, err := st.ListRunners(context.Background())
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
		rs, err := st.ListRunners(context.Background())
		if err != nil {
			return err
		}
		if len(rs) != 1 {
			return fmt.Errorf("ListRunners = %+v, want 1 runner", rs)
		}
		r := rs[0]
		if r.Name != "vm1" || !r.Connected || r.CapacityUsed != 1 || r.CapacityTotal != 4 {
			return fmt.Errorf("runner = %+v, want {vm1 used:1 total:4 connected:true}", r)
		}
		if r.LastSeenAt.IsZero() {
			return fmt.Errorf("runner LastSeenAt is zero")
		}
		return nil
	})

	f.close()

	eventually(t, 3*time.Second, func() error {
		rs, err := st.ListRunners(context.Background())
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
		seedSession(t, st, Session{ID: id, State: StateRunning, Runner: "vm1", CreatedAt: old, UpdatedAt: old, LastEventAt: old})
		startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Sessions: []rwire.SessionInfo{{ID: id, State: "running"}}})
		waitConnected(t, s, "vm1")

		eventually(t, 3*time.Second, func() error {
			got := getSession(t, st, id)
			if got.State != StateRunning {
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
		seedSession(t, st, Session{ID: id, State: StateRunning, Runner: "vm1"})
		startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Sessions: []rwire.SessionInfo{{ID: id, State: "suspended_cold"}}})
		waitConnected(t, s, "vm1")

		got := wantState(t, st, id, StateSuspendedCold)
		if got.Runner != "vm1" {
			t.Fatalf("runner = %q, want vm1", got.Runner)
		}
	})

	t.Run("running absent is dead with lost at announce", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		id := "sess_lost"
		seedSession(t, st, Session{ID: id, State: StateRunning, Runner: "vm1"})
		startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4})
		waitConnected(t, s, "vm1")

		got := wantState(t, st, id, StateDead)
		if got.Error != "lost at announce" {
			t.Fatalf("error = %q, want %q", got.Error, "lost at announce")
		}
	})

	t.Run("creating absent goes back to queued", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		id := "sess_requeue"
		seedSession(t, st, Session{ID: id, State: StateCreating, Runner: "vm1"})
		startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4})
		waitConnected(t, s, "vm1")

		got := wantState(t, st, id, StateQueued)
		if got.Runner != "" {
			t.Fatalf("runner = %q, want cleared", got.Runner)
		}
	})

	t.Run("terminal row announced present is destroyed as an orphan", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		id := "sess_terminal"
		seedSession(t, st, Session{ID: id, State: StateDestroyed, Runner: "vm1"})
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Sessions: []rwire.SessionInfo{{ID: id, State: "running"}}})

		cmd := f.nextCmd(t)
		if cmd.Type != "destroy" || cmd.Session != id {
			t.Fatalf("got %+v, want destroy of %s", cmd, id)
		}
		if got := getSession(t, st, id); got.State != StateDestroyed {
			t.Fatalf("state = %q, want destroyed (untouched)", got.State)
		}
	})

	t.Run("unknown id announced present is destroyed as an orphan", func(t *testing.T) {
		_, _, ts := newTestControld(t)
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Sessions: []rwire.SessionInfo{{ID: "sess_ghost", State: "running"}}})

		cmd := f.nextCmd(t)
		if cmd.Type != "destroy" || cmd.Session != "sess_ghost" {
			t.Fatalf("got %+v, want destroy of sess_ghost", cmd)
		}
	})

	// A live row the store places on ANOTHER runner: this runner is holding
	// a duplicate, so the copy here is an orphan. Adopting it would leave
	// both alive and ping-pong Runner between them on every reconnect.
	t.Run("live row placed on another runner is destroyed as a duplicate", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		id := "sess_dupe"
		seedSession(t, st, Session{ID: id, State: StateRunning, Runner: "vm2"})
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Sessions: []rwire.SessionInfo{{ID: id, State: "running"}}})

		cmd := f.nextCmd(t)
		if cmd.Type != "destroy" || cmd.Session != id {
			t.Fatalf("got %+v, want destroy of %s", cmd, id)
		}
		got := getSession(t, st, id)
		if got.State != StateRunning || got.Runner != "vm2" {
			t.Fatalf("row = %s on %q, want running on vm2 (untouched)", got.State, got.Runner)
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
		seedSession(t, st, Session{ID: id, State: StateQueued})
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4, Sessions: []rwire.SessionInfo{
			{ID: id, State: "running"},
			{ID: ghostSession, State: "running"},
		}})
		waitConnected(t, s, "vm1")

		awaitReconciled(t, f)
		got := wantState(t, st, id, StateRunning)
		if got.Runner != "vm1" {
			t.Fatalf("runner = %q, want vm1", got.Runner)
		}
	})
}

func TestDispatchCorrelatesResults(t *testing.T) {
	s, _, ts := newTestControld(t)
	f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4})
	waitConnected(t, s, "vm1")

	type outcome struct {
		res rwire.FromRunner
		err error
	}
	run := func(session string) <-chan outcome {
		ch := make(chan outcome, 1)
		go func() {
			res, err := s.dispatch(context.Background(), "vm1", rwire.ToRunner{Type: "snapshot", Session: session})
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

func TestDispatchUnreachable(t *testing.T) {
	t.Run("never connected", func(t *testing.T) {
		s, _, _ := newTestControld(t)
		_, err := s.dispatch(context.Background(), "vm-nope", rwire.ToRunner{Type: "destroy", Session: "sess_x"})
		if !errors.Is(err, ErrRunnerUnreachable) {
			t.Fatalf("err = %v, want ErrRunnerUnreachable", err)
		}
	})

	t.Run("connection dies in flight", func(t *testing.T) {
		s, _, ts := newTestControld(t)
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4})
		waitConnected(t, s, "vm1")

		errc := make(chan error, 1)
		go func() {
			_, err := s.dispatch(context.Background(), "vm1", rwire.ToRunner{Type: "suspend", Session: "sess_x"})
			errc <- err
		}()
		f.nextCmd(t) // the command reached the runner; now the runner dies
		f.close()

		select {
		case err := <-errc:
			if !errors.Is(err, ErrRunnerUnreachable) {
				t.Fatalf("err = %v, want ErrRunnerUnreachable", err)
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
		_, err := s.dispatch(context.Background(), "vm1", rwire.ToRunner{Type: "suspend", Session: "sess_x"})
		if !errors.Is(err, ErrRunnerUnreachable) {
			t.Fatalf("err = %v, want ErrRunnerUnreachable", err)
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
		Sessions: []rwire.SessionInfo{{ID: ghostSession, State: "running"}}})
	waitConnected(t, s, "vm1")
	awaitReconciled(t, f)

	id := "sess_evt"
	seedSession(t, st, Session{ID: id, State: StateCreating, Runner: "vm1"})

	f.event(t, id, "running")
	wantState(t, st, id, StateRunning)

	f.event(t, id, "dead")
	got := wantState(t, st, id, StateDead)
	if got.Error != "runner reported dead" {
		t.Fatalf("error = %q, want %q", got.Error, "runner reported dead")
	}

	// A runner may only speak for sessions the store places on it (or on
	// nobody yet): otherwise a stale holder of a duplicate could drive a
	// session that has since been re-placed to a terminal state.
	t.Run("event from a runner the session is not placed on is ignored", func(t *testing.T) {
		elsewhere := "sess_elsewhere"
		seedSession(t, st, Session{ID: elsewhere, State: StateRunning, Runner: "vm2"})
		sync := "sess_sync"
		seedSession(t, st, Session{ID: sync, State: StateCreating, Runner: "vm1"})

		f.event(t, elsewhere, "dead")
		f.event(t, sync, "running")
		// The reader handles messages in order, so the second event landing
		// proves the first has already been fully handled (and ignored).
		wantState(t, st, sync, StateRunning)

		got := getSession(t, st, elsewhere)
		if got.State != StateRunning || got.Runner != "vm2" {
			t.Fatalf("row = %s on %q, want running on vm2 (untouched)", got.State, got.Runner)
		}
	})

	// "running" tolerates an unplaced row (it can outrun the create's own
	// queued->creating transition), but "dead" must not: an unplaced row is
	// one a requeue cleared, and the scheduler may already be re-placing it.
	// A stale holder reporting its copy dead would otherwise kill a session
	// that is alive — terminally, since dead is not recoverable.
	t.Run("dead event for an unplaced (requeued) row is ignored", func(t *testing.T) {
		requeued := "sess_requeued"
		seedSession(t, st, Session{ID: requeued, State: StateQueued})
		sync := "sess_sync2"
		seedSession(t, st, Session{ID: sync, State: StateCreating, Runner: "vm1"})

		f.event(t, requeued, "dead")
		f.event(t, sync, "running")
		// In-order handling again: the second event landing proves the first
		// has already been handled (and ignored).
		wantState(t, st, sync, StateRunning)

		got := getSession(t, st, requeued)
		if got.State != StateQueued || got.Runner != "" {
			t.Fatalf("row = %s on %q, want queued and unplaced (untouched)", got.State, got.Runner)
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
		rs, err := st.ListRunners(context.Background())
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
		rs, err := st.ListRunners(context.Background())
		if err != nil {
			return err
		}
		if len(rs) != 1 || !rs[0].Connected || rs[0].CapacityUsed != 2 {
			return fmt.Errorf("runners = %+v, want one connected runner with used:2", rs)
		}
		return nil
	})

	errc := make(chan error, 1)
	go func() {
		_, err := s.dispatch(context.Background(), "vm1", rwire.ToRunner{Type: "destroy", Session: "sess_x"})
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
	Store
	entered  chan struct{} // the first disconnect write has begun
	upserted chan struct{} // some connected:true upsert has completed
	wrote    chan struct{} // the first disconnect write has returned

	enteredOnce  sync.Once
	upsertedOnce sync.Once
	wroteOnce    sync.Once
}

func newRaceStore() *raceStore {
	return &raceStore{
		Store:    NewMemStore(),
		entered:  make(chan struct{}),
		upserted: make(chan struct{}),
		wrote:    make(chan struct{}),
	}
}

func (r *raceStore) SetRunnerConnected(ctx context.Context, name string, connected bool) error {
	if connected {
		return r.Store.SetRunnerConnected(ctx, name, connected)
	}
	r.enteredOnce.Do(func() { close(r.entered) })
	select {
	case <-r.upserted:
	case <-time.After(500 * time.Millisecond):
	}
	err := r.Store.SetRunnerConnected(ctx, name, connected)
	r.wroteOnce.Do(func() { close(r.wrote) })
	return err
}

func (r *raceStore) UpsertRunner(ctx context.Context, run Runner) error {
	err := r.Store.UpsertRunner(ctx, run)
	// Only a connect that happens *after* the teardown began is the one this
	// test is racing; the first runner's own announce must not release it.
	select {
	case <-r.entered:
		if run.Connected {
			r.upsertedOnce.Do(func() { close(r.upserted) })
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
		runners, err := rs.ListRunners(context.Background())
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
	s.broadcastToRunners(rwire.ToRunner{Type: "prepull", Ref: ref}, "vm1")

	for _, f := range []*fakeRunner{f2, f3} {
		cmd := f.nextCmd(t)
		if cmd.Type != "prepull" || cmd.Ref != ref {
			t.Fatalf("%s got %+v, want the prepull of %s", f.name, cmd, ref)
		}
	}

	// vm1 was excluded. Sending it something else now and seeing that arrive
	// first proves no prepull was ever queued ahead of it — the connection
	// delivers in order.
	if err := s.sendToRunner("vm1", rwire.ToRunner{Type: "destroy", Session: "sess_probe"}); err != nil {
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
	err := s.sendToRunner("vm-nope", rwire.ToRunner{Type: "destroy", Session: "sess_x"})
	if !errors.Is(err, ErrRunnerUnreachable) {
		t.Fatalf("err = %v, want ErrRunnerUnreachable", err)
	}
}

// TestUnroutedPathIs404 pins Handler()'s shape: a path no route claims 404s
// even once every task's routes are registered. (Task 10 claimed
// /v1/sessions itself, so this now probes a path nothing will ever claim.)
func TestUnroutedPathIs404(t *testing.T) {
	_, _, ts := newTestControld(t)
	resp, err := http.Get(ts.URL + "/v1/bogus")
	if err != nil {
		t.Fatalf("GET /v1/bogus: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

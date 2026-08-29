package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"rainier/internal/attachio"
	"rainier/internal/controld"
	"rainier/internal/driver"
	"rainier/internal/relay"
	"rainier/internal/runnerd"
	"rainier/internal/wire"
)

// ---------------------------------------------------------------------------
// Config round-trip
// ---------------------------------------------------------------------------

func TestConfigRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	t.Setenv("RAINIER_CONFIG", path)

	want := Config{ServerURL: "https://controld.example", Token: "rnr_abc123"}
	if err := Save(want); err != nil {
		t.Fatalf("Save: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat config file: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("config file mode = %o, want 0600", perm)
	}
	dirInfo, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat config dir: %v", err)
	}
	_ = dirInfo // dir already existed (t.TempDir()); the 0700 mkdir path is
	// covered below by a fresh, not-yet-existing directory.

	got, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got != want {
		t.Fatalf("Load() = %+v, want %+v", got, want)
	}
}

// TestConfigOwnerIDRoundTrips pins the field `rainier new` caches the
// caller's own owner_id into (see cmd/rainier's resolveSessionID) — added
// alongside review round 1, finding 3's ambiguity handling.
func TestConfigOwnerIDRoundTrips(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("RAINIER_CONFIG", filepath.Join(dir, "config.json"))

	want := Config{ServerURL: "https://controld.example", Token: "rnr_abc123", OwnerID: "usr_alice"}
	if err := Save(want); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got != want {
		t.Fatalf("Load() = %+v, want %+v", got, want)
	}
}

func TestSaveCreatesConfigDirMode0700(t *testing.T) {
	dir := t.TempDir()
	nested := filepath.Join(dir, "nested", "config.json")
	t.Setenv("RAINIER_CONFIG", nested)

	if err := Save(Config{ServerURL: "http://x", Token: "t"}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	info, err := os.Stat(filepath.Dir(nested))
	if err != nil {
		t.Fatalf("stat created dir: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o700 {
		t.Errorf("created config dir mode = %o, want 0700", perm)
	}
}

func TestLoadMissingFileReturnsZeroValue(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("RAINIER_CONFIG", filepath.Join(dir, "does-not-exist.json"))

	got, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got != (Config{}) {
		t.Fatalf("Load() = %+v, want zero value", got)
	}
}

// ---------------------------------------------------------------------------
// Client.Do
// ---------------------------------------------------------------------------

// TestDoSetsAuthHeader asserts Authorization, X-Request-Id, and (via the
// IdempotencyKey option) Idempotency-Key all reach the server.
func TestDoSetsAuthHeader(t *testing.T) {
	var gotAuth, gotReqID, gotIdem string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotReqID = r.Header.Get("X-Request-Id")
		gotIdem = r.Header.Get("Idempotency-Key")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"ok": "yes"})
	}))
	defer ts.Close()

	c := &Client{Base: ts.URL, Token: "rnr_test_token"}
	var out map[string]string
	if err := c.Do(http.MethodPost, "/v1/sessions", map[string]string{"name": "x"}, &out, IdempotencyKey("idem-123")); err != nil {
		t.Fatalf("Do: %v", err)
	}

	if gotAuth != "Bearer rnr_test_token" {
		t.Errorf("Authorization = %q, want %q", gotAuth, "Bearer rnr_test_token")
	}
	if len(gotReqID) != 16 {
		t.Errorf("X-Request-Id = %q, want 16 hex characters", gotReqID)
	}
	if gotIdem != "idem-123" {
		t.Errorf("Idempotency-Key = %q, want %q", gotIdem, "idem-123")
	}
	if out["ok"] != "yes" {
		t.Errorf("decoded out = %+v, want ok=yes", out)
	}
}

// TestDoRequestIDVariesPerInvocation pins "16-hex per invocation" — two
// calls must not reuse the same X-Request-Id.
func TestDoRequestIDVariesPerInvocation(t *testing.T) {
	var ids []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ids = append(ids, r.Header.Get("X-Request-Id"))
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	c := &Client{Base: ts.URL}
	for i := 0; i < 2; i++ {
		if err := c.Do(http.MethodGet, "/healthz", nil, nil); err != nil {
			t.Fatalf("Do: %v", err)
		}
	}
	if len(ids) != 2 || ids[0] == ids[1] {
		t.Fatalf("X-Request-Id values = %v, want two distinct values", ids)
	}
}

// TestDoDecodesErrorEnvelope pins the exact error contract: a 404 carrying
// controld's {"error":{"code":"not_found",...}} envelope becomes the error
// string "not_found: <message>".
func TestDoDecodesErrorEnvelope(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]string{"code": "not_found", "message": "session not found"},
		})
	}))
	defer ts.Close()

	c := &Client{Base: ts.URL}
	err := c.Do(http.MethodGet, "/v1/sessions/sess_x", nil, nil)
	if err == nil {
		t.Fatal("Do: want an error, got nil")
	}
	if got, want := err.Error(), "not_found: session not found"; got != want {
		t.Fatalf("Do error = %q, want %q", got, want)
	}
}

// TestDoJunkNonJSON5xxDoesNotPanic asserts an upstream 500 that isn't
// controld's envelope shape at all (plain text, as a misconfigured proxy or
// a crashed process might return) is reported as an error, not a panic.
func TestDoJunkNonJSON5xxDoesNotPanic(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("<html>502 Bad Gateway</html>\nnot json at all"))
	}))
	defer ts.Close()

	c := &Client{Base: ts.URL}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Do panicked on junk non-JSON 5xx body: %v", r)
		}
	}()

	err := c.Do(http.MethodGet, "/v1/sessions", nil, nil)
	if err == nil {
		t.Fatal("Do: want an error for a 500 response, got nil")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("Do error = %q, want it to mention the 500 status", err.Error())
	}
}

// TestDoEmptyBodyJunk5xxDoesNotPanic is the degenerate case of the above: no
// body at all.
func TestDoEmptyBodyJunk5xxDoesNotPanic(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer ts.Close()

	c := &Client{Base: ts.URL}
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Do panicked on empty 5xx body: %v", r)
		}
	}()
	if err := c.Do(http.MethodGet, "/v1/sessions", nil, nil); err == nil {
		t.Fatal("Do: want an error for a 503 response, got nil")
	}
}

// TestDoTransportErrorReturnedAsIs asserts a connection failure surfaces
// unwrapped — the caller (or a test) can still match on *url.Error /
// net.OpError underneath without Do having reformatted it.
func TestDoTransportErrorReturnedAsIs(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	closedURL := ts.URL
	ts.Close() // nothing is listening here any more

	c := &Client{Base: closedURL}
	err := c.Do(http.MethodGet, "/healthz", nil, nil)
	if err == nil {
		t.Fatal("Do: want a transport error, got nil")
	}
	if strings.Contains(err.Error(), ":") && strings.HasPrefix(err.Error(), "unexpected response") {
		t.Fatalf("Do error looks reformatted as an envelope fallback, want the raw transport error: %v", err)
	}
}

// TestDoSuccessNoOutDoesNotDecode asserts out:nil is honored even when the
// server sends a body (e.g. a 204 with no body, or a caller uninterested in
// the response) — Do must not error trying to decode into nothing.
func TestDoSuccessNoOutDoesNotDecode(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	c := &Client{Base: ts.URL}
	if err := c.Do(http.MethodDelete, "/v1/sessions/sess_x", nil, nil); err != nil {
		t.Fatalf("Do: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Step 5: CLI-against-real-controld smoke test.
//
// This drives Client through the exact same HTTP calls cmd/rainier's
// commands make, against a real in-process stack — controld (memstore) +
// runnerd (fake driver) + a scripted sessiond speaking relay frames — built
// entirely from controld's, runnerd's, driver's, and relay's PUBLIC
// surfaces (New/Handler/Run, New/Handler/RunAgent, WSConn/Encode/Decode).
// It deliberately does not reach into internal/controld's own _test.go
// helpers (loginUser, seedSession, etc.) — those are package-internal to
// controld and, per the brief, this package's stack has to be assembled the
// way a real client would see it. See internal/runnerd/agent_test.go's
// fakeControld and internal/controld/attach_test.go's fakeSessiond for the
// prior art this mirrors.
// ---------------------------------------------------------------------------

const (
	smokeGHToken     = "gho_smoke_good"
	smokeRunnerToken = "rnr_smoke_token"
	smokeSnapshot    = "smoke-snapshot"
)

// smoke*Response/Envelope mirror controld's client-facing JSON shapes
// (internal/controld/api.go, auth.go) — this file decodes only the fields
// it needs to assert on.
type smokeUserView struct {
	Login string `json:"login"`
	Role  string `json:"role"`
}

type smokeAuthResponse struct {
	Token string        `json:"token"`
	User  smokeUserView `json:"user"`
}

type smokeSession struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	State     string `json:"state"`
	Runner    string `json:"runner"`
	Reachable bool   `json:"reachable"`
}

type smokeSessionEnvelope struct {
	Session smokeSession `json:"session"`
}

type smokeSessionsEnvelope struct {
	Sessions   []smokeSession `json:"sessions"`
	NextCursor string         `json:"next_cursor"`
}

// newSmokeGitHub fakes GitHub's GET /user: 200 with {"id":99,"login":"alice"}
// for the fixed smokeGHToken bearer, 401 otherwise.
func newSmokeGitHub(t *testing.T) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/user" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") != "Bearer "+smokeGHToken {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"id": 99, "login": "alice"})
	}))
	t.Cleanup(ts.Close)
	return ts
}

// newSmokeControld starts a real controld.Server (memstore) on its own
// httptest.Server, GitHubAPIBase pointed at gh so `login` exercises the real
// exchange handler end to end, and its own scheduler loop running
// (s.Run(ctx)) so a create actually leaves "queued". ExternalURL must name
// this listener's own address before New validates the config — the attach
// plane derives the runner dial-back URL from it — so the server is built
// unstarted and started once the handler is known (the same pattern
// internal/controld's own attach tests use).
func newSmokeControld(t *testing.T, gh *httptest.Server) *httptest.Server {
	t.Helper()
	st := controld.NewMemStore()
	ts := httptest.NewUnstartedServer(nil)
	cfg := controld.Config{
		RunnerToken:   smokeRunnerToken,
		Members:       []string{"alice"},
		GitHubAPIBase: gh.URL,
		ExternalURL:   "http://" + ts.Listener.Addr().String(),
		OpTimeout:     5 * time.Second,
	}
	s, err := controld.New(st, cfg)
	if err != nil {
		ts.Listener.Close()
		t.Fatalf("controld.New: %v", err)
	}
	ts.Config.Handler = s.Handler()
	ts.Start()
	t.Cleanup(ts.Close)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go s.Run(ctx)

	return ts
}

// smokeWSBase renders ts's URL as its ws:// base.
func smokeWSBase(ts *httptest.Server) string { return "ws" + strings.TrimPrefix(ts.URL, "http") }

// waitRunnerConnected polls GET /v1/runners (the exact call `rainier` itself
// would have no reason to make, but the fixture needs, to know when it's
// safe to create a session) until name reports connected, or fails the test
// after 5s.
func waitRunnerConnected(t *testing.T, c *Client, name string) {
	t.Helper()
	type runnersResp struct {
		Runners []struct {
			Name      string `json:"name"`
			Connected bool   `json:"connected"`
		} `json:"runners"`
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		var resp runnersResp
		if err := c.Do(http.MethodGet, "/v1/runners", nil, &resp); err != nil {
			t.Fatalf("GET /v1/runners: %v", err)
		}
		for _, r := range resp.Runners {
			if r.Name == name && r.Connected {
				return
			}
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("runner %s never connected within 5s", name)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// waitSessionState polls GET /v1/sessions/{id} until its state matches want,
// or fails the test after timeout.
func waitSessionState(t *testing.T, c *Client, id, want string, timeout time.Duration) smokeSession {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last smokeSession
	for {
		var env smokeSessionEnvelope
		if err := c.Do(http.MethodGet, "/v1/sessions/"+id, nil, &env); err != nil {
			t.Fatalf("GET /v1/sessions/%s: %v", id, err)
		}
		last = env.Session
		if last.State == want {
			return last
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("session %s state = %q after %s, want %q", id, last.State, timeout, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// scriptedSessiond stands in for a container's sessiond: it dials runnerd's
// /register for one session and speaks relay frames directly — answering
// every attach (FrameOpen) with a snapshot and echoing every stdin
// ClientMsg back as output. Mirrors
// internal/controld/attach_test.go's fakeSessiond, rebuilt here from
// relay's public API since that helper is package-internal to controld.
type scriptedSessiond struct {
	conn relay.Conn
}

// startScriptedSessiond dials runnerd's /register for sessionID, retrying
// on failure for up to 5s: controld's scheduler places the session and
// dispatches its create to the runner asynchronously (dispatchCreate runs
// in its own goroutine), so runnerd's registry entry for this id may not
// exist yet the instant the caller learns the id.
func startScriptedSessiond(t *testing.T, ctx context.Context, runnerdWSBase, sessionID string) *scriptedSessiond {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		c, _, err := websocket.Dial(ctx, runnerdWSBase+"/register?session="+sessionID, nil)
		if err == nil {
			c.SetReadLimit(16 << 20)
			t.Cleanup(func() { c.CloseNow() })
			ss := &scriptedSessiond{conn: relay.WSConn(c)}
			go ss.serve(ctx)
			return ss
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("dial runnerd /register for %s: %v", sessionID, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (ss *scriptedSessiond) serve(ctx context.Context) {
	for {
		raw, err := ss.conn.Read(ctx)
		if err != nil {
			return
		}
		f, err := relay.Decode(raw)
		if err != nil {
			continue
		}
		switch f.Type {
		case relay.FrameOpen:
			ss.send(ctx, f.AttachID, wire.ServerMsg{Type: "snapshot", Seq: 1, Data: []byte(smokeSnapshot)})
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

// syncBuffer is a concurrency-safe io.Writer + String() — attachio.Run
// writes to os.Stdout from its own goroutine while the test polls the
// captured output from this one, so a plain strings.Builder/bytes.Buffer
// isn't safe here.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// waitContains polls sb until it contains want, or fails the test after
// timeout.
func waitContains(t *testing.T, sb *syncBuffer, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if strings.Contains(sb.String(), want) {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("stdout never contained %q within %s; got %q", want, timeout, sb.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestSmokeCLIAgainstRealControld is Step 5: login (against a fake GitHub),
// create, ls, get, and a non-tty headless attach — all driven through
// Client/attachio exactly as cmd/rainier's commands drive them, against a
// real controld+runnerd+scripted-sessiond stack. It asserts the CLI's HTTP
// and websocket usage actually matches the server contract, not a mock of
// it.
func TestSmokeCLIAgainstRealControld(t *testing.T) {
	gh := newSmokeGitHub(t)
	controldTS := newSmokeControld(t, gh)

	// --- login: POST /v1/auth/github against the real handler, which in
	// turn calls the fake GitHub above ---
	anon := &Client{Base: controldTS.URL}
	var auth smokeAuthResponse
	if err := anon.Do(http.MethodPost, "/v1/auth/github", map[string]string{"access_token": smokeGHToken}, &auth); err != nil {
		t.Fatalf("POST /v1/auth/github: %v", err)
	}
	if auth.User.Login != "alice" || auth.User.Role != "member" {
		t.Fatalf("auth response user = %+v, want login=alice role=member", auth.User)
	}
	if auth.Token == "" {
		t.Fatal("auth response carried no token")
	}
	c := &Client{Base: controldTS.URL, Token: auth.Token}

	// --- a real runnerd (fake driver) connected to controld ---
	rd := runnerd.New(driver.NewFake(4), "", "", "")
	rsrv := httptest.NewServer(rd.Handler())
	t.Cleanup(rsrv.Close)

	runnerCtx, cancelRunner := context.WithCancel(context.Background())
	t.Cleanup(cancelRunner)
	go rd.RunAgent(runnerCtx, runnerd.AgentConfig{
		ControldURL: smokeWSBase(controldTS),
		Token:       smokeRunnerToken,
		RunnerName:  "vm1",
	})
	waitRunnerConnected(t, c, "vm1")

	// --- create: POST /v1/sessions with a fresh Idempotency-Key, exactly
	// like cmd/rainier's `new` command ---
	var created smokeSessionEnvelope
	createBody := map[string]any{"name": "smoke-session", "image": "smoke-image"}
	if err := c.Do(http.MethodPost, "/v1/sessions", createBody, &created, IdempotencyKey(RandHex(8))); err != nil {
		t.Fatalf("POST /v1/sessions: %v", err)
	}
	id := created.Session.ID
	if !strings.HasPrefix(id, "sess_") {
		t.Fatalf("created session id = %q, want a sess_ prefix", id)
	}
	if created.Session.State != "queued" {
		t.Fatalf("created session state = %q, want queued", created.Session.State)
	}

	// The scheduler places the session on vm1 and dispatches its create to
	// the real runnerd, which creates it via the fake driver — but nothing
	// there ever reports "running" until a sessiond dials runnerd's
	// /register, the way the real container would. Stand in for it.
	startScriptedSessiond(t, runnerCtx, smokeWSBase(rsrv), id)
	got := waitSessionState(t, c, id, "running", 5*time.Second)
	if got.Runner != "vm1" || !got.Reachable {
		t.Fatalf("session once running = %+v, want runner=vm1 reachable=true", got)
	}

	// --- ls: GET /v1/sessions must list it ---
	var list smokeSessionsEnvelope
	if err := c.Do(http.MethodGet, "/v1/sessions", nil, &list); err != nil {
		t.Fatalf("GET /v1/sessions: %v", err)
	}
	var listed *smokeSession
	for i := range list.Sessions {
		if list.Sessions[i].ID == id {
			listed = &list.Sessions[i]
		}
	}
	if listed == nil {
		t.Fatalf("GET /v1/sessions did not list %s: %+v", id, list.Sessions)
	}
	if listed.Name != "smoke-session" || listed.State != "running" {
		t.Fatalf("listed session = %+v, want name=smoke-session state=running", listed)
	}

	// --- get: GET /v1/sessions/{id} ---
	var one smokeSessionEnvelope
	if err := c.Do(http.MethodGet, "/v1/sessions/"+id, nil, &one); err != nil {
		t.Fatalf("GET /v1/sessions/%s: %v", id, err)
	}
	if one.Session.State != "running" {
		t.Fatalf("GET /v1/sessions/%s state = %q, want running", id, one.Session.State)
	}

	// --- attach: attachio.Run's non-tty path, exactly like cmd/rainier's
	// `attach` command drives it, against controld's attach plane end to
	// end (controld's dial-back pairing, runnerd's splice, the scripted
	// sessiond's snapshot+echo) ---
	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe (stdin): %v", err)
	}
	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe (stdout): %v", err)
	}
	origStdin, origStdout := os.Stdin, os.Stdout
	os.Stdin, os.Stdout = stdinR, stdoutW
	restoreStd := func() { os.Stdin, os.Stdout = origStdin, origStdout }
	t.Cleanup(restoreStd) // safety net if a t.Fatalf below skips the explicit restore

	sb := &syncBuffer{}
	var drainWG sync.WaitGroup
	drainWG.Add(1)
	go func() {
		defer drainWG.Done()
		io.Copy(sb, stdoutR)
	}()

	wsURL := strings.Replace(controldTS.URL, "http", "ws", 1) + "/v1/sessions/" + id + "/attach"
	header := http.Header{"Authorization": {"Bearer " + auth.Token}}
	runErr := make(chan error, 1)
	go func() {
		runErr <- attachio.Run(context.Background(), wsURL, header, 0)
	}()

	waitContains(t, sb, smokeSnapshot, 5*time.Second)

	if _, err := stdinW.Write([]byte("hi")); err != nil {
		t.Fatalf("write to stdin pipe: %v", err)
	}
	waitContains(t, sb, "hi", 5*time.Second)

	if _, err := stdinW.Write([]byte{0x1d}); err != nil { // Ctrl-]: detach
		t.Fatalf("write detach key to stdin pipe: %v", err)
	}

	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("attachio.Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("attachio.Run did not return within 5s of the detach key")
	}

	restoreStd()
	stdinW.Close()
	stdoutW.Close()
	drainWG.Wait()

	out := sb.String()
	if !strings.Contains(out, smokeSnapshot) {
		t.Fatalf("attach output = %q, want it to contain the snapshot %q", out, smokeSnapshot)
	}
	if !strings.Contains(out, "hi") {
		t.Fatalf("attach output = %q, want the echoed stdin %q", out, "hi")
	}
	if !strings.Contains(out, "[detached at seq") {
		t.Fatalf("attach output = %q, want the detach status line", out)
	}
}

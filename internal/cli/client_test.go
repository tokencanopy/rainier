package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

	"github.com/tokencanopy/rainier/internal/attachio"
	"github.com/tokencanopy/rainier/internal/controld"
	"github.com/tokencanopy/rainier/internal/driver"
	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/internal/runnerd"
	"github.com/tokencanopy/rainier/internal/wire"
	"github.com/tokencanopy/rainier/internal/xfer"
)

func TestDoContextCancelsAStalledRequest(t *testing.T) {
	started := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := (&Client{Base: ts.URL}).DoContext(ctx, http.MethodGet, "/v0/sessions/sess_synthetic", nil, nil)
	<-started
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("DoContext error = %v, want context deadline exceeded", err)
	}
}

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

// TestConfigOwnerIDRoundTrips pins the field `rainier login` caches the
// caller's own user id into (see cmd/rainier's resolveSessionID) — added
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
	if err := c.Do(http.MethodPost, "/v0/sessions", map[string]string{"name": "x"}, &out, IdempotencyKey("idem-123")); err != nil {
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
	err := c.Do(http.MethodGet, "/v0/sessions/sess_x", nil, nil)
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

	err := c.Do(http.MethodGet, "/v0/sessions", nil, nil)
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
	if err := c.Do(http.MethodGet, "/v0/sessions", nil, nil); err == nil {
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
	if err := c.Do(http.MethodDelete, "/v0/sessions/sess_x", nil, nil); err != nil {
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
	smokeGHToken = "gho_smoke_good"
	// smokeAdminGHToken logs in as "root", the fixture's admin — secrets
	// (and, from Task 4, environments) are admin-only mutations, so the
	// smoke needs both roles to exercise the real authZ.
	smokeAdminGHToken = "gho_smoke_admin"
	smokeRunnerToken  = "rnr_smoke_token"
	smokeSnapshot     = "smoke-snapshot"
	// smokeSecretsKeyHex is the AES-256 key this fixture's controld seals
	// team secrets under; controld.New requires one (fail closed).
	smokeSecretsKeyHex = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"
)

// smokeSecretsKey is smokeSecretsKeyHex parsed once, for newSmokeControld.
var smokeSecretsKey = func() [32]byte {
	k, err := controld.ParseSecretsKey(smokeSecretsKeyHex)
	if err != nil {
		panic("cli smoke: bad secrets key: " + err.Error())
	}
	return k
}()

// smoke*Response/Envelope mirror controld's client-facing JSON shapes
// (internal/controld/api.go, auth.go) — this file decodes only the fields
// it needs to assert on.
type smokeUserView struct {
	Login string `json:"login"`
	Role  string `json:"role"`
}

type smokeAuthResponse struct {
	Token   string        `json:"token"`
	User    smokeUserView `json:"user"`
	Scopes  string        `json:"scopes"`
	Warning string        `json:"warning"`
}

// smokeCredential mirrors one element of GET /v0/credentials. Like
// smokeSecret it has no value field, because the response has none.
type smokeCredential struct {
	Provider       string `json:"provider"`
	Status         string `json:"status"`
	Scopes         string `json:"scopes"`
	LastVerifiedAt string `json:"last_verified_at"`
	LastUsedAt     string `json:"last_used_at"`
}

type smokeCredentialsEnvelope struct {
	Credentials []smokeCredential `json:"credentials"`
}

type smokeSession struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Image       string `json:"image"`
	State       string `json:"state"`
	Runner      string `json:"runner"`
	Reachable   bool   `json:"reachable"`
	Environment string `json:"environment"`
	QueueReason string `json:"queue_reason"`
}

type smokeSessionEnvelope struct {
	Session smokeSession `json:"session"`
}

type smokeSessionsEnvelope struct {
	Sessions   []smokeSession `json:"sessions"`
	NextCursor string         `json:"next_cursor"`
}

type smokeSecret struct {
	Name      string `json:"name"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

type smokeSecretsEnvelope struct {
	Secrets []smokeSecret `json:"secrets"`
}

// smokeEnvironment mirrors controld's environment view. Connectors stay raw:
// the CLI's own `env create` passes an operator's connector JSON through
// untouched, and this smoke asserts the same bytes come back out.
type smokeEnvironment struct {
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	Image        string            `json:"image"`
	Setup        string            `json:"setup"`
	SetupHash    string            `json:"setup_hash"`
	SecretRefs   []string          `json:"secret_refs"`
	Connectors   []json.RawMessage `json:"connectors"`
	Placement    string            `json:"placement"`
	SnapshotHash string            `json:"snapshot_hash"`
}

type smokeEnvironmentEnvelope struct {
	Environment smokeEnvironment `json:"environment"`
}

type smokeEnvironmentsEnvelope struct {
	Environments []smokeEnvironment `json:"environments"`
}

// smokeGHScopes is the X-OAuth-Scopes the fake GitHub reports, matching what
// the device flow now asks for — controld reads it off this same /user
// response and stores it with the credential.
const smokeGHScopes = "repo, read:user"

// newSmokeGitHub fakes GitHub's GET /user: 200 with {"id":99,"login":"alice"}
// for the smokeGHToken bearer and {"id":1,"login":"root"} for
// smokeAdminGHToken (the fixture's admin), 401 for anything else.
func newSmokeGitHub(t *testing.T) *httptest.Server {
	t.Helper()
	users := map[string]map[string]any{
		"Bearer " + smokeGHToken:      {"id": 99, "login": "alice"},
		"Bearer " + smokeAdminGHToken: {"id": 1, "login": "root"},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/user" {
			http.NotFound(w, r)
			return
		}
		user, ok := users[r.Header.Get("Authorization")]
		if !ok {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-OAuth-Scopes", smokeGHScopes)
		json.NewEncoder(w).Encode(user)
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
		SecretsKey:    smokeSecretsKey,
		Admins:        []string{"root"},
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

// waitRunnerConnected polls GET /v0/runners (the exact call `rainier` itself
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
		if err := c.Do(http.MethodGet, "/v0/runners", nil, &resp); err != nil {
			t.Fatalf("GET /v0/runners: %v", err)
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

// waitSessionState polls GET /v0/sessions/{id} until its state matches want,
// or fails the test after timeout.
func waitSessionState(t *testing.T, c *Client, id, want string, timeout time.Duration) smokeSession {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last smokeSession
	for {
		var env smokeSessionEnvelope
		if err := c.Do(http.MethodGet, "/v0/sessions/"+id, nil, &env); err != nil {
			t.Fatalf("GET /v0/sessions/%s: %v", id, err)
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
	// files is what a push left behind, keyed by its destination — the
	// sandbox's staging and extraction reduced to the one property a
	// round-trip smoke can check: the bytes that went in come back out.
	files map[string][]byte
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
			ss := &scriptedSessiond{conn: relay.WSConn(c), files: map[string][]byte{}}
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
		case relay.FrameControl:
			ss.serveControl(ctx, f.Payload)
		}
	}
}

// serveControl answers the session RPCs controld drives INTO a sandbox — the
// half cmd/sessiond implements for real (its diff, its staging file, its
// tar). Here it is cut down to what a round-trip smoke can check: remember the
// archive a push delivered, hand the same bytes back on a pull, and answer a
// diff with a fixed stat.
//
// It runs on the same goroutine as the frame loop above, which is also the
// conn's only writer — so there is no locking here and none needed.
func (ss *scriptedSessiond) serveControl(ctx context.Context, payload []byte) {
	var ev relay.ControlEvent
	if json.Unmarshal(payload, &ev) != nil || !strings.HasPrefix(ev.Kind, "req:") {
		return
	}
	reply := relay.ControlEvent{Kind: "resp", ID: ev.ID}
	out, err := ss.invoke(strings.TrimPrefix(ev.Kind, "req:"), ev.Payload)
	if err != nil {
		reply.Payload, _ = json.Marshal(map[string]string{"error": err.Error()})
	} else {
		reply.OK = true
		reply.Payload, _ = json.Marshal(out)
	}
	b, err := json.Marshal(reply)
	if err != nil {
		return
	}
	raw, err := relay.Encode(relay.Frame{Type: relay.FrameControl, Payload: b})
	if err != nil {
		return
	}
	ss.conn.Write(ctx, raw)
}

func (ss *scriptedSessiond) invoke(method string, payload []byte) (any, error) {
	switch method {
	case xfer.MethodDiff:
		return xfer.DiffAnswer{Repos: []xfer.RepoDiff{{
			Repo: "acme/widgets", BaseBranch: "main", SessionBranch: "rainier/smoke-session",
			Stat: " main.go | 2 +-\n",
		}}}, nil
	case xfer.MethodPushFiles:
		var c xfer.PushChunk
		if err := json.Unmarshal(payload, &c); err != nil {
			return nil, err
		}
		ss.files[c.Path] = append(ss.files[c.Path], c.Data...)
		return xfer.PushAck{Seq: c.Seq, Synced: c.Done}, nil
	case xfer.MethodPullFiles:
		var req xfer.PullRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			return nil, err
		}
		blob, ok := ss.files[req.Path]
		if !ok {
			return nil, fmt.Errorf("%s does not exist in this session's workspace", req.Path)
		}
		off := req.Seq * xfer.ChunkBytes
		if off > len(blob) {
			return nil, fmt.Errorf("pull chunk %d is past the end of the archive", req.Seq)
		}
		end := min(off+xfer.ChunkBytes, len(blob))
		return xfer.PullChunk{Seq: req.Seq, Data: blob[off:end], Done: end >= len(blob)}, nil
	}
	return nil, fmt.Errorf("unknown method %q", method)
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

	// --- login: POST /v0/auth/github against the real handler, which in
	// turn calls the fake GitHub above ---
	anon := &Client{Base: controldTS.URL}
	var auth smokeAuthResponse
	if err := anon.Do(http.MethodPost, "/v0/auth/github", map[string]string{"access_token": smokeGHToken}, &auth); err != nil {
		t.Fatalf("POST /v0/auth/github: %v", err)
	}
	if auth.User.Login != "alice" || auth.User.Role != "member" {
		t.Fatalf("auth response user = %+v, want login=alice role=member", auth.User)
	}
	if auth.Token == "" {
		t.Fatal("auth response carried no token")
	}
	if auth.Scopes != smokeGHScopes {
		t.Fatalf("auth response scopes = %q, want %q (read off GitHub's own /user response)", auth.Scopes, smokeGHScopes)
	}
	if auth.Warning != "" {
		t.Fatalf("auth response warning = %q, want none — this token has the repo scope", auth.Warning)
	}
	c := &Client{Base: controldTS.URL, Token: auth.Token}

	// --- creds: GET /v0/credentials, the one call cmd/rainier's `creds`
	// makes. Logging in above sealed alice's GitHub token into the vault;
	// this is the only view of it the API offers, and it carries metadata
	// only — the same write-only discipline as secrets ---
	var rawCreds json.RawMessage
	if err := c.Do(http.MethodGet, "/v0/credentials", nil, &rawCreds); err != nil {
		t.Fatalf("GET /v0/credentials: %v", err)
	}
	if strings.Contains(string(rawCreds), smokeGHToken) {
		t.Fatalf("GET /v0/credentials leaked the GitHub token: %s", rawCreds)
	}
	var creds smokeCredentialsEnvelope
	if err := json.Unmarshal(rawCreds, &creds); err != nil {
		t.Fatalf("decode credentials: %v; body=%s", err, rawCreds)
	}
	if len(creds.Credentials) != 1 {
		t.Fatalf("credentials = %+v, want exactly the github one login stored", creds.Credentials)
	}
	if got := creds.Credentials[0]; got.Provider != "github" || got.Status != "valid" || got.Scopes != smokeGHScopes {
		t.Fatalf("credential = %+v, want github/valid with the reported scopes", got)
	}
	if creds.Credentials[0].LastVerifiedAt == "" || creds.Credentials[0].LastUsedAt == "" {
		t.Fatalf("credential timestamps = %+v, want both populated", creds.Credentials[0])
	}

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

	// --- create: POST /v0/sessions with a fresh Idempotency-Key, exactly
	// like cmd/rainier's `new` command ---
	var created smokeSessionEnvelope
	createBody := map[string]any{"name": "smoke-session", "image": "smoke-image"}
	if err := c.Do(http.MethodPost, "/v0/sessions", createBody, &created, IdempotencyKey(RandHex(8))); err != nil {
		t.Fatalf("POST /v0/sessions: %v", err)
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

	// --- ls: GET /v0/sessions must list it ---
	var list smokeSessionsEnvelope
	if err := c.Do(http.MethodGet, "/v0/sessions", nil, &list); err != nil {
		t.Fatalf("GET /v0/sessions: %v", err)
	}
	var listed *smokeSession
	for i := range list.Sessions {
		if list.Sessions[i].ID == id {
			listed = &list.Sessions[i]
		}
	}
	if listed == nil {
		t.Fatalf("GET /v0/sessions did not list %s: %+v", id, list.Sessions)
	}
	if listed.Name != "smoke-session" || listed.State != "running" {
		t.Fatalf("listed session = %+v, want name=smoke-session state=running", listed)
	}

	// --- get: GET /v0/sessions/{id} ---
	var one smokeSessionEnvelope
	if err := c.Do(http.MethodGet, "/v0/sessions/"+id, nil, &one); err != nil {
		t.Fatalf("GET /v0/sessions/%s: %v", id, err)
	}
	if one.Session.State != "running" {
		t.Fatalf("GET /v0/sessions/%s state = %q, want running", id, one.Session.State)
	}

	// --- secrets: exactly the three calls cmd/rainier's `secret set|ls|rm`
	// make. Writes are admin-only, the listing is team-visible, and no
	// response on any path carries a value ---
	var adminAuth smokeAuthResponse
	if err := anon.Do(http.MethodPost, "/v0/auth/github", map[string]string{"access_token": smokeAdminGHToken}, &adminAuth); err != nil {
		t.Fatalf("POST /v0/auth/github (admin): %v", err)
	}
	if adminAuth.User.Login != "root" || adminAuth.User.Role != "admin" {
		t.Fatalf("admin auth user = %+v, want login=root role=admin", adminAuth.User)
	}
	admin := &Client{Base: controldTS.URL, Token: adminAuth.Token}

	const smokeSecretValue = "ghp_smoke_secret_value"
	if err := admin.Do(http.MethodPut, "/v0/secrets/SMOKE_TOKEN", map[string]string{"value": smokeSecretValue}, nil); err != nil {
		t.Fatalf("PUT /v0/secrets/SMOKE_TOKEN as admin: %v", err)
	}

	// The member (alice) may not write one; the error must be the envelope's
	// forbidden code, surfaced by Client.Do as "forbidden: ...".
	memberPutErr := c.Do(http.MethodPut, "/v0/secrets/MEMBER_TRY", map[string]string{"value": "nope"}, nil)
	if memberPutErr == nil {
		t.Fatal("PUT /v0/secrets as a member: want a forbidden error, got nil")
	}
	if !strings.Contains(memberPutErr.Error(), "forbidden") {
		t.Fatalf("member PUT error = %q, want a forbidden error", memberPutErr)
	}

	// The listing is decoded twice: once as raw JSON, so this asserts on the
	// bytes on the wire (no value key, no value), and once as the shape the
	// CLI renders.
	var rawSecrets json.RawMessage
	if err := c.Do(http.MethodGet, "/v0/secrets", nil, &rawSecrets); err != nil {
		t.Fatalf("GET /v0/secrets as a member: %v", err)
	}
	if strings.Contains(string(rawSecrets), "value") || strings.Contains(string(rawSecrets), smokeSecretValue) {
		t.Fatalf("GET /v0/secrets leaked a value: %s", rawSecrets)
	}
	var secrets smokeSecretsEnvelope
	if err := json.Unmarshal(rawSecrets, &secrets); err != nil {
		t.Fatalf("decode secrets: %v; body=%s", err, rawSecrets)
	}
	if len(secrets.Secrets) != 1 || secrets.Secrets[0].Name != "SMOKE_TOKEN" {
		t.Fatalf("secrets = %+v, want exactly SMOKE_TOKEN (the member's rejected PUT stored nothing)", secrets.Secrets)
	}
	if secrets.Secrets[0].CreatedAt == "" || secrets.Secrets[0].UpdatedAt == "" {
		t.Fatalf("secret timestamps = %+v, want both populated", secrets.Secrets[0])
	}

	// --- environments: exactly the calls cmd/rainier's `env create|ls|show`
	// make. Mutations are admin-only, reads are team-visible, and the
	// connector object the operator wrote comes back unrewritten (byte-for-
	// byte over this fixture's memstore; the value, over pgstore's jsonb) ---
	const smokeConnector = `{"type":"github","repo":"acme/widgets"}`
	envBody := map[string]any{
		"name":        "smoke-env",
		"image":       "smoke-image",
		"setup":       "echo provisioning",
		"secret_refs": []string{"SMOKE_TOKEN"},
		"connectors":  json.RawMessage("[" + smokeConnector + "]"),
		"placement":   "vm1",
	}

	// The environment's secret_refs must name a secret that exists, so this
	// runs while SMOKE_TOKEN is still stored (above) — the API refuses a
	// dangling reference, which is itself worth smoking.
	var createdEnv smokeEnvironmentEnvelope
	if err := admin.Do(http.MethodPost, "/v0/environments", envBody, &createdEnv); err != nil {
		t.Fatalf("POST /v0/environments as admin: %v", err)
	}
	if !strings.HasPrefix(createdEnv.Environment.ID, "env_") {
		t.Fatalf("created environment id = %q, want an env_ prefix", createdEnv.Environment.ID)
	}
	if createdEnv.Environment.SetupHash == "" || createdEnv.Environment.SnapshotHash != "" {
		t.Fatalf("new environment = %+v, want a setup hash and no snapshot yet", createdEnv.Environment)
	}
	if len(createdEnv.Environment.Connectors) != 1 || string(createdEnv.Environment.Connectors[0]) != smokeConnector {
		t.Fatalf("connectors = %v, want the operator's bytes verbatim (%s)", createdEnv.Environment.Connectors, smokeConnector)
	}

	// A member may not define one.
	memberEnvErr := c.Do(http.MethodPost, "/v0/environments", map[string]any{"name": "member-env", "image": "i"}, nil)
	if memberEnvErr == nil || !strings.Contains(memberEnvErr.Error(), "forbidden") {
		t.Fatalf("POST /v0/environments as a member = %v, want a forbidden error", memberEnvErr)
	}

	// `env ls` (team-visible) and `env show <name>` (the name→id resolution
	// the route does server-side).
	var envList smokeEnvironmentsEnvelope
	if err := c.Do(http.MethodGet, "/v0/environments", nil, &envList); err != nil {
		t.Fatalf("GET /v0/environments as a member: %v", err)
	}
	if len(envList.Environments) != 1 || envList.Environments[0].Name != "smoke-env" {
		t.Fatalf("environments = %+v, want exactly smoke-env (the member's rejected create stored nothing)", envList.Environments)
	}
	var shown smokeEnvironmentEnvelope
	if err := c.Do(http.MethodGet, "/v0/environments/smoke-env", nil, &shown); err != nil {
		t.Fatalf("GET /v0/environments/smoke-env as a member: %v", err)
	}
	if shown.Environment.ID != createdEnv.Environment.ID || shown.Environment.Placement != "vm1" {
		t.Fatalf("shown environment = %+v, want %s pinned to vm1", shown.Environment, createdEnv.Environment.ID)
	}

	// --- `rainier new --env smoke-env`: the create resolves the environment
	// server-side. The response is the evidence a client actually gets — the
	// environment's name, and the image resolution settled on (the environment
	// has no snapshot yet, so its plain image) rather than the empty override
	// this body carries. The environment pins placement to vm1, so reaching
	// `creating` there is the placement hint honored end to end ---
	var envSession smokeSessionEnvelope
	if err := c.Do(http.MethodPost, "/v0/sessions", map[string]any{"name": "smoke-env-session", "environment": "smoke-env"},
		&envSession, IdempotencyKey(RandHex(8))); err != nil {
		t.Fatalf("POST /v0/sessions with an environment: %v", err)
	}
	if envSession.Session.Environment != "smoke-env" {
		t.Fatalf("created session environment = %q, want smoke-env", envSession.Session.Environment)
	}
	if envSession.Session.Image != "smoke-image" {
		t.Fatalf("created session image = %q, want the environment's smoke-image", envSession.Session.Image)
	}
	placed := waitSessionState(t, c, envSession.Session.ID, "creating", 5*time.Second)
	if placed.Runner != "vm1" {
		t.Fatalf("environment session placed on %q, want vm1 (the environment's placement)", placed.Runner)
	}

	// An environment nobody has heard of is refused, and names itself in the
	// error the CLI would print.
	unknownEnvErr := c.Do(http.MethodPost, "/v0/sessions", map[string]any{"name": "nope", "environment": "no-such-env"}, nil)
	if unknownEnvErr == nil || !strings.Contains(unknownEnvErr.Error(), "no-such-env") {
		t.Fatalf("POST /v0/sessions with an unknown environment = %v, want an error naming it", unknownEnvErr)
	}

	// The secret delete runs last of the secrets calls: the environment above
	// had to reference a secret that still existed (the API refuses a
	// dangling secret_ref at create), and so did the session started from it.
	if err := admin.Do(http.MethodDelete, "/v0/secrets/SMOKE_TOKEN", nil, nil); err != nil {
		t.Fatalf("DELETE /v0/secrets/SMOKE_TOKEN as admin: %v", err)
	}
	var afterRm smokeSecretsEnvelope
	if err := c.Do(http.MethodGet, "/v0/secrets", nil, &afterRm); err != nil {
		t.Fatalf("GET /v0/secrets after delete: %v", err)
	}
	if len(afterRm.Secrets) != 0 {
		t.Fatalf("secrets after delete = %+v, want none", afterRm.Secrets)
	}

	// --- diff / push / pull: the workspace-inspection routes, driven exactly
	// as cmd/rainier's `diff`, `push` and `pull` drive them. The push's bytes
	// cross controld chunk by chunk into the scripted sessiond and come back
	// out through the pull's streamed response; extracting them has to
	// reproduce the tree that was pushed, file for file ---
	var diffAns xfer.DiffAnswer
	if err := c.Do(http.MethodGet, "/v0/sessions/"+id+"/diff", nil, &diffAns); err != nil {
		t.Fatalf("GET /v0/sessions/%s/diff: %v", id, err)
	}
	if len(diffAns.Repos) != 1 || diffAns.Repos[0].Repo != "acme/widgets" ||
		!strings.Contains(diffAns.Repos[0].Stat, "main.go") {
		t.Fatalf("diff = %+v, want the sandbox's own answer", diffAns.Repos)
	}

	// The diff is a team-visible read: this session is alice's, and root —
	// another account entirely — reads it without owning it (design §4.6).
	// The file routes below are the ones that take ownership into account.
	if err := admin.Do(http.MethodGet, "/v0/sessions/"+id+"/diff", nil, &diffAns); err != nil {
		t.Fatalf("GET diff as another user: %v; the diff is a team-visible read", err)
	}

	srcDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(srcDir, "pkg"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	pushed := map[string]string{
		"README.md":     "# smoke\n",
		"pkg/data.bin":  strings.Repeat("payload-", 4096),
		"pkg/script.sh": "#!/bin/sh\necho hi\n",
	}
	for name, body := range pushed {
		if err := os.WriteFile(filepath.Join(srcDir, name), []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	if err := Push(c, id, srcDir, "widget/vendor", nil); err != nil {
		t.Fatalf("Push: %v", err)
	}
	landing := filepath.Join(t.TempDir(), "landing")
	if err := Pull(c, id, "widget/vendor", landing, nil); err != nil {
		t.Fatalf("Pull: %v", err)
	}
	for name, want := range pushed {
		got, err := os.ReadFile(filepath.Join(landing, name))
		if err != nil {
			t.Fatalf("read %s after the round trip: %v", name, err)
		}
		if string(got) != want {
			t.Fatalf("%s came back %d bytes, want the %d that were pushed", name, len(got), len(want))
		}
	}

	// A path that is not there answers with the sandbox's own sentence, not a
	// truncated archive.
	if err := Pull(c, id, "widget/nothing", t.TempDir(), nil); err == nil {
		t.Fatal("Pull of a path the session does not have returned nil")
	} else if !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("Pull of a missing path = %v, want the sandbox's own sentence", err)
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

	wsURL := strings.Replace(controldTS.URL, "http", "ws", 1) + "/v0/sessions/" + id + "/attach"
	header := http.Header{"Authorization": {"Bearer " + auth.Token}}
	runErr := make(chan error, 1)
	go func() {
		_, err := attachio.Run(context.Background(), wsURL, header, 0)
		runErr <- err
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

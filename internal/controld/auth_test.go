// internal/controld/auth_test.go
package controld

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tokencanopy/rainier/control"
)

// ---------------------------------------------------------------------------
// fake GitHub helpers
// ---------------------------------------------------------------------------

// newFakeGitHub starts an httptest.Server running handler and registers its
// close as test cleanup.
func newFakeGitHub(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)
	return ts
}

// fakeGitHubOK serves GET /user -> {"id":42,"login":"alice"} when the
// bearer is "gho_good", 401 otherwise — the exact fixture the brief
// mandates, with the scopes a `repo read:user` login gets back.
func fakeGitHubOK(t *testing.T) *httptest.Server {
	t.Helper()
	return fakeGitHubWithScopes(t, "repo, read:user")
}

// fakeGitHubWithScopes is fakeGitHubOK with a caller-chosen X-OAuth-Scopes
// header on the /user response — the same response the exchange reads the
// user out of, which is exactly where GitHub reports what the token can
// actually do. Passing "" omits the header entirely, the way a token type
// that reports no scopes at all would.
func fakeGitHubWithScopes(t *testing.T, scopes string) *httptest.Server {
	t.Helper()
	return newFakeGitHub(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/user" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") != "Bearer gho_good" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if scopes != "" {
			w.Header().Set("X-OAuth-Scopes", scopes)
		}
		writeJSON(w, http.StatusOK, map[string]any{"id": 42, "login": "alice"})
	})
}

// ---------------------------------------------------------------------------
// HTTP test helpers
// ---------------------------------------------------------------------------

func postJSON(t *testing.T, ts *httptest.Server, path string, body any) *http.Response {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	resp, err := http.Post(ts.URL+path, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	return resp
}

func postRaw(t *testing.T, ts *httptest.Server, path, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(ts.URL+path, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	return resp
}

func getWithBearer(t *testing.T, ts *httptest.Server, path, token string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, ts.URL+path, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	return resp
}

// readBody drains and returns resp's body as a string, closing it.
func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(b)
}

// decodeErrBody unmarshals raw (already-read response body text) as the
// error envelope.
func decodeErrBody(t *testing.T, raw string) errorEnvelope {
	t.Helper()
	var e errorEnvelope
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		t.Fatalf("decode error envelope from %q: %v", raw, err)
	}
	return e
}

// ---------------------------------------------------------------------------
// mandated tests (brief Step 1, all seven cases)
// ---------------------------------------------------------------------------

// 1. exchange with valid token + allowlisted login -> 200 with token+user;
// the returned token then passes GET /v0/me.
func TestGitHubAuthExchangeSuccess(t *testing.T) {
	gh := fakeGitHubOK(t)
	_, _, ts := newTestControld(t, func(c *Config) {
		c.GitHubAPIBase = gh.URL
		c.Admins = []string{"alice"}
	})

	resp := postJSON(t, ts, "/v0/auth/github", map[string]any{"access_token": "gho_good"})
	raw := readBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", resp.StatusCode, raw)
	}

	var body authResponse
	if err := json.Unmarshal([]byte(raw), &body); err != nil {
		t.Fatalf("decode response: %v; body = %s", err, raw)
	}
	if !strings.HasPrefix(body.Token, "rnr_") {
		t.Errorf("token = %q, want rnr_ prefix", body.Token)
	}
	if body.User.Login != "alice" || body.User.Role != "admin" {
		t.Errorf("user = %+v, want {alice admin}", body.User)
	}

	meResp := getWithBearer(t, ts, "/v0/me", body.Token)
	meRaw := readBody(t, meResp)
	if meResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v0/me status = %d, want 200; body = %s", meResp.StatusCode, meRaw)
	}
	var me meResponse
	if err := json.Unmarshal([]byte(meRaw), &me); err != nil {
		t.Fatalf("decode /v0/me response: %v; body = %s", err, meRaw)
	}
	if me.User.Login != "alice" || me.User.Role != "admin" {
		t.Errorf("/v0/me user = %+v, want alice/admin", me.User)
	}
	// The id is what a client cannot learn any other way about itself, and
	// it must be the SAME identity the exchange just issued a token for —
	// a client that caches one and compares it against session owner_ids
	// would silently prefer nothing at all if these two disagreed.
	if me.User.ID == "" {
		t.Errorf("/v0/me user = %+v, want a non-empty id", me.User)
	}
	if me.User.ID != body.User.ID {
		t.Errorf("/v0/me id = %q, want the exchange's %q — one identity, two routes", me.User.ID, body.User.ID)
	}
	assertKeySet(t, meRaw, "user")
	var meOuter map[string]json.RawMessage
	if err := json.Unmarshal([]byte(meRaw), &meOuter); err != nil {
		t.Fatalf("decode /v0/me for the key-set check: %v; body = %s", err, meRaw)
	}
	assertKeySet(t, string(meOuter["user"]), "id", "login", "role")
}

// TestMeIDIsTheSessionOwnerID pins the property the CLI's owner-preference
// rests on: the id GET /v0/me hands a caller is exactly the owner_id its own
// sessions carry. They come from different tables through different views, so
// nothing but a test keeps them the same string — and if they ever drift, an
// ambiguous name silently stops resolving instead of failing loudly.
func TestMeIDIsTheSessionOwnerID(t *testing.T) {
	_, st, ts := newTestControld(t)
	u, tok := loginUser(t, st, "alice", "member")
	seedSession(t, st, control.Session{ID: "sess_owned", CreatorID: control.ActorID(u.ID), State: control.StateRunning})

	raw := readBody(t, getWithBearer(t, ts, "/v0/me", tok))
	var me meResponse
	if err := json.Unmarshal([]byte(raw), &me); err != nil {
		t.Fatalf("decode /v0/me: %v; body = %s", err, raw)
	}

	listRaw := readBody(t, getWithBearer(t, ts, "/v0/sessions", tok))
	var list struct {
		Sessions []struct {
			ID      string `json:"id"`
			OwnerID string `json:"owner_id"`
		} `json:"sessions"`
	}
	if err := json.Unmarshal([]byte(listRaw), &list); err != nil {
		t.Fatalf("decode /v0/sessions: %v; body = %s", err, listRaw)
	}
	if len(list.Sessions) != 1 {
		t.Fatalf("sessions = %+v, want the one seeded row", list.Sessions)
	}
	if list.Sessions[0].OwnerID != me.User.ID {
		t.Fatalf("session owner_id = %q, /v0/me id = %q; owner-preference compares these two",
			list.Sessions[0].OwnerID, me.User.ID)
	}
}

// 2. valid GitHub token, login not allowlisted -> 403 forbidden.
func TestGitHubAuthNotAllowlisted(t *testing.T) {
	gh := fakeGitHubOK(t)
	_, _, ts := newTestControld(t, func(c *Config) {
		c.GitHubAPIBase = gh.URL
		c.Admins = []string{"someone-else"}
		c.Members = []string{"another-one"}
	})

	resp := postJSON(t, ts, "/v0/auth/github", map[string]any{"access_token": "gho_good"})
	raw := readBody(t, resp)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403; body = %s", resp.StatusCode, raw)
	}
	if e := decodeErrBody(t, raw); e.Error.Code != "forbidden" {
		t.Errorf("code = %q, want forbidden", e.Error.Code)
	}
}

// 3. invalid GitHub token -> 401 unauthenticated (GitHub's 401 mapped, body
// not leaked).
func TestGitHubAuthUpstreamUnauthorized(t *testing.T) {
	const upstreamSecret = "Bad credentials — token abc123 revoked"
	gh := newFakeGitHub(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		io.WriteString(w, upstreamSecret)
	})
	_, _, ts := newTestControld(t, func(c *Config) {
		c.GitHubAPIBase = gh.URL
		c.Admins = []string{"alice"}
	})

	resp := postJSON(t, ts, "/v0/auth/github", map[string]any{"access_token": "gho_bad"})
	raw := readBody(t, resp)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401; body = %s", resp.StatusCode, raw)
	}
	if strings.Contains(raw, "abc123") || strings.Contains(raw, "Bad credentials") {
		t.Errorf("response leaked upstream body: %s", raw)
	}
	if e := decodeErrBody(t, raw); e.Error.Code != "unauthenticated" {
		t.Errorf("code = %q, want unauthenticated", e.Error.Code)
	}
}

// 4. GitHub 500 -> our 500 + code internal with a generic message (upstream
// text logged, not returned).
func TestGitHubAuthUpstreamServerError(t *testing.T) {
	const upstreamSecret = "panic: nil pointer at db.go:412"
	gh := newFakeGitHub(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, upstreamSecret)
	})
	_, _, ts := newTestControld(t, func(c *Config) {
		c.GitHubAPIBase = gh.URL
		c.Admins = []string{"alice"}
	})

	resp := postJSON(t, ts, "/v0/auth/github", map[string]any{"access_token": "gho_good"})
	raw := readBody(t, resp)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body = %s", resp.StatusCode, raw)
	}
	if strings.Contains(raw, "db.go") || strings.Contains(raw, "nil pointer") {
		t.Errorf("response leaked upstream body: %s", raw)
	}
	e := decodeErrBody(t, raw)
	if e.Error.Code != "internal" {
		t.Errorf("code = %q, want internal", e.Error.Code)
	}
	if e.Error.Message == upstreamSecret {
		t.Errorf("message echoed the upstream body verbatim")
	}
}

// 5. GET /v0/me without/with a bogus bearer -> 401.
func TestMeRequiresBearer(t *testing.T) {
	_, _, ts := newTestControld(t)

	cases := []struct {
		name  string
		token string
	}{
		{"no bearer", ""},
		{"bogus token", "rnr_" + strings.Repeat("0", 64)},
		{"garbage token", "not-even-the-right-shape"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := getWithBearer(t, ts, "/v0/me", tc.token)
			raw := readBody(t, resp)
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401; body = %s", resp.StatusCode, raw)
			}
			if e := decodeErrBody(t, raw); e.Error.Code != "unauthenticated" {
				t.Errorf("code = %q, want unauthenticated", e.Error.Code)
			}
		})
	}
}

// 6. empty allowlists: exchange with any valid GitHub user -> 403 (fail
// closed).
func TestGitHubAuthEmptyAllowlistFailsClosed(t *testing.T) {
	gh := fakeGitHubOK(t)
	_, _, ts := newTestControld(t, func(c *Config) {
		c.GitHubAPIBase = gh.URL
		// Admins/Members deliberately left empty.
	})

	resp := postJSON(t, ts, "/v0/auth/github", map[string]any{"access_token": "gho_good"})
	raw := readBody(t, resp)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403; body = %s", resp.StatusCode, raw)
	}
	if e := decodeErrBody(t, raw); e.Error.Code != "forbidden" {
		t.Errorf("code = %q, want forbidden", e.Error.Code)
	}
}

// 7. unknown fields in the request body -> 400 invalid_request (decoder
// with DisallowUnknownFields).
func TestGitHubAuthRejectsUnknownFields(t *testing.T) {
	gh := fakeGitHubOK(t)
	_, _, ts := newTestControld(t, func(c *Config) {
		c.GitHubAPIBase = gh.URL
		c.Admins = []string{"alice"}
	})

	resp := postRaw(t, ts, "/v0/auth/github", `{"access_token":"gho_good","extra_field":"nope"}`)
	raw := readBody(t, resp)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body = %s", resp.StatusCode, raw)
	}
	if e := decodeErrBody(t, raw); e.Error.Code != "invalid_request" {
		t.Errorf("code = %q, want invalid_request", e.Error.Code)
	}
}

// ---------------------------------------------------------------------------
// extra coverage beyond the mandated seven
// ---------------------------------------------------------------------------

// roleFor: admin beats member, matching is case-insensitive (GitHub logins
// are), and a login in neither list is rejected.
func TestRoleForPrecedenceAndFailClosed(t *testing.T) {
	s, err := New(NewMemStore(), Config{
		RunnerToken: "tok",
		ExternalURL: "http://controld.test",
		SecretsKey:  testSecretsKey,
		Admins:      []string{"alice"},
		Members:     []string{"alice", "Bob"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if role, ok := s.roleFor("alice"); !ok || role != "admin" {
		t.Errorf("roleFor(alice) = (%q, %v), want (admin, true) — admin must win over member", role, ok)
	}
	if role, ok := s.roleFor("Bob"); !ok || role != "member" {
		t.Errorf("roleFor(Bob) = (%q, %v), want (member, true)", role, ok)
	}
	if role, ok := s.roleFor("eve"); ok {
		t.Errorf("roleFor(eve) = (%q, %v), want ok=false — not listed", role, ok)
	}

	// GitHub returns the account's own casing, not whatever the operator
	// typed into --admins/--members, and the two are the same account. A
	// case-only mismatch must not read as "not authorized".
	if role, ok := s.roleFor("ALICE"); !ok || role != "admin" {
		t.Errorf("roleFor(ALICE) = (%q, %v), want (admin, true) — GitHub logins are case-insensitive", role, ok)
	}
	if role, ok := s.roleFor("bob"); !ok || role != "member" {
		t.Errorf("roleFor(bob) = (%q, %v), want (member, true) — allowlisted as %q", role, ok, "Bob")
	}
}

// A member (not admin) login round-trips with role "member" through the
// full exchange, distinct from the admin path exercised above.
func TestGitHubAuthMemberRole(t *testing.T) {
	gh := fakeGitHubOK(t)
	_, _, ts := newTestControld(t, func(c *Config) {
		c.GitHubAPIBase = gh.URL
		c.Members = []string{"alice"}
	})

	resp := postJSON(t, ts, "/v0/auth/github", map[string]any{"access_token": "gho_good"})
	raw := readBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", resp.StatusCode, raw)
	}
	var body authResponse
	if err := json.Unmarshal([]byte(raw), &body); err != nil {
		t.Fatalf("decode response: %v; body = %s", err, raw)
	}
	if body.User.Role != "member" {
		t.Errorf("role = %q, want member", body.User.Role)
	}
}

// The request body is capped at 4KB via http.MaxBytesReader; an oversized
// body is a 400 invalid_request, not a hang or a 500.
func TestGitHubAuthBodyTooLarge(t *testing.T) {
	gh := fakeGitHubOK(t)
	_, _, ts := newTestControld(t, func(c *Config) {
		c.GitHubAPIBase = gh.URL
		c.Admins = []string{"alice"}
	})

	huge := `{"access_token":"` + strings.Repeat("x", 5<<10) + `"}`
	resp := postRaw(t, ts, "/v0/auth/github", huge)
	raw := readBody(t, resp)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body = %s", resp.StatusCode, raw)
	}
	if e := decodeErrBody(t, raw); e.Error.Code != "invalid_request" {
		t.Errorf("code = %q, want invalid_request", e.Error.Code)
	}
}

// An empty access_token is a malformed request, not an upstream call.
func TestGitHubAuthEmptyAccessToken(t *testing.T) {
	gh := fakeGitHubOK(t)
	_, _, ts := newTestControld(t, func(c *Config) {
		c.GitHubAPIBase = gh.URL
		c.Admins = []string{"alice"}
	})

	resp := postJSON(t, ts, "/v0/auth/github", map[string]any{"access_token": ""})
	raw := readBody(t, resp)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body = %s", resp.StatusCode, raw)
	}
	if e := decodeErrBody(t, raw); e.Error.Code != "invalid_request" {
		t.Errorf("code = %q, want invalid_request", e.Error.Code)
	}
}

// GitHub's response shape is untrusted input: a malformed /user body (id
// missing/zero, or empty login) must not mint a token.
func TestGitHubAuthRejectsMalformedUpstreamShape(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"zero id", `{"id":0,"login":"alice"}`},
		{"empty login", `{"id":42,"login":""}`},
		{"not json", `not json at all`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gh := newFakeGitHub(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				io.WriteString(w, tc.body)
			})
			_, _, ts := newTestControld(t, func(c *Config) {
				c.GitHubAPIBase = gh.URL
				c.Admins = []string{"alice"}
			})

			resp := postJSON(t, ts, "/v0/auth/github", map[string]any{"access_token": "gho_good"})
			raw := readBody(t, resp)
			if resp.StatusCode != http.StatusInternalServerError {
				t.Fatalf("status = %d, want 500; body = %s", resp.StatusCode, raw)
			}
			if e := decodeErrBody(t, raw); e.Error.Code != "internal" {
				t.Errorf("code = %q, want internal", e.Error.Code)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// the exchange stores the credential (Plan 5 vault)
// ---------------------------------------------------------------------------

// Login no longer discards the GitHub token: it seals it into the vault so a
// session's git can mint from it later. What lands at rest must be sealed
// bytes, not the token, and must come back out through the fleet key.
func TestGitHubAuthStoresCredential(t *testing.T) {
	gh := fakeGitHubOK(t)
	s, st, ts := newTestControld(t, func(c *Config) {
		c.GitHubAPIBase = gh.URL
		c.Admins = []string{"alice"}
	})

	resp := postJSON(t, ts, "/v0/auth/github", map[string]any{"access_token": "gho_good"})
	raw := readBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", resp.StatusCode, raw)
	}
	if strings.Contains(raw, "gho_good") {
		t.Fatalf("the login response echoed the GitHub token: %s", raw)
	}

	var body authResponse
	if err := json.Unmarshal([]byte(raw), &body); err != nil {
		t.Fatalf("decode response: %v; body = %s", err, raw)
	}
	u, err := st.UserByToken(context.Background(), HashToken(body.Token))
	if err != nil {
		t.Fatalf("UserByToken: %v", err)
	}
	c, err := st.GetCredential(context.Background(), u.ID, "github")
	if err != nil {
		t.Fatalf("GetCredential after login: %v", err)
	}
	if c.Status != CredentialValid {
		t.Errorf("stored status = %q, want %q", c.Status, CredentialValid)
	}
	if c.Scopes != "repo, read:user" {
		t.Errorf("stored scopes = %q, want the X-OAuth-Scopes header verbatim", c.Scopes)
	}
	if strings.Contains(string(c.Ciphertext), "gho_good") {
		t.Fatal("the stored ciphertext contains the token in the clear")
	}
	plain, err := Open(s.cfg.SecretsKey, c.Ciphertext, c.Nonce)
	if err != nil {
		t.Fatalf("Open the stored credential: %v", err)
	}
	if string(plain) != "gho_good" {
		t.Errorf("Open round-trip = %q, want the token GitHub accepted", plain)
	}

	// And the whole point: it is mintable straight away.
	tok, err := s.mintGitCredential(context.Background(), u.ID)
	if err != nil {
		t.Fatalf("mintGitCredential after login: %v", err)
	}
	if tok != "gho_good" {
		t.Errorf("minted token = %q, want the one login stored", tok)
	}
}

// The scopes GitHub reports ride back in the response, and a token without
// `repo` gets a warning — never a failure: the login is legitimate, it just
// cannot do git yet, and saying so at login beats a mystifying clone failure
// twenty minutes later.
func TestGitHubAuthScopeWarning(t *testing.T) {
	cases := []struct {
		name        string
		scopes      string
		wantScopes  string
		wantWarning bool
	}{
		{"repo present", "repo, read:user", "repo, read:user", false},
		{"repo only", "repo", "repo", false},
		{"repo missing", "read:user", "read:user", true},
		{"public_repo is not repo", "public_repo, read:user", "public_repo, read:user", true},
		{"no header at all", "", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gh := fakeGitHubWithScopes(t, tc.scopes)
			_, _, ts := newTestControld(t, func(c *Config) {
				c.GitHubAPIBase = gh.URL
				c.Admins = []string{"alice"}
			})

			resp := postJSON(t, ts, "/v0/auth/github", map[string]any{"access_token": "gho_good"})
			raw := readBody(t, resp)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200; body = %s", resp.StatusCode, raw)
			}

			var body authResponse
			if err := json.Unmarshal([]byte(raw), &body); err != nil {
				t.Fatalf("decode response: %v; body = %s", err, raw)
			}
			if body.Scopes != tc.wantScopes {
				t.Errorf("scopes = %q, want %q", body.Scopes, tc.wantScopes)
			}
			if tc.wantWarning {
				if body.Warning != githubRepoScopeWarning {
					t.Errorf("warning = %q, want %q", body.Warning, githubRepoScopeWarning)
				}
				assertKeySet(t, raw, "token", "user", "scopes", "warning")
			} else {
				if body.Warning != "" {
					t.Errorf("warning = %q, want none when repo is granted", body.Warning)
				}
				assertKeySet(t, raw, "token", "user", "scopes")
			}
		})
	}
}

// requireUser never leaks whether a token merely doesn't exist vs. is
// malformed — both are the same 401 unauthenticated, so a scanning client
// learns nothing about which tokens are "close".
func TestRequireUserUniformOn401(t *testing.T) {
	_, _, ts := newTestControld(t)
	tok, _ := NewToken() // well-formed but never issued (no InsertToken)
	resp := getWithBearer(t, ts, "/v0/me", tok)
	raw := readBody(t, resp)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401; body = %s", resp.StatusCode, raw)
	}
	if e := decodeErrBody(t, raw); e.Error.Code != "unauthenticated" {
		t.Errorf("code = %q, want unauthenticated", e.Error.Code)
	}
}

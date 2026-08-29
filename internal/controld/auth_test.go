// internal/controld/auth_test.go
package controld

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
// mandates.
func fakeGitHubOK(t *testing.T) *httptest.Server {
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
// the returned token then passes GET /v1/me.
func TestGitHubAuthExchangeSuccess(t *testing.T) {
	gh := fakeGitHubOK(t)
	_, _, ts := newTestControld(t, func(c *Config) {
		c.GitHubAPIBase = gh.URL
		c.Admins = []string{"alice"}
	})

	resp := postJSON(t, ts, "/v1/auth/github", map[string]any{"access_token": "gho_good"})
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

	meResp := getWithBearer(t, ts, "/v1/me", body.Token)
	meRaw := readBody(t, meResp)
	if meResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/me status = %d, want 200; body = %s", meResp.StatusCode, meRaw)
	}
	var me meResponse
	if err := json.Unmarshal([]byte(meRaw), &me); err != nil {
		t.Fatalf("decode /v1/me response: %v; body = %s", err, meRaw)
	}
	if me.User.Login != "alice" || me.User.Role != "admin" {
		t.Errorf("/v1/me user = %+v, want {alice admin}", me.User)
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

	resp := postJSON(t, ts, "/v1/auth/github", map[string]any{"access_token": "gho_good"})
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

	resp := postJSON(t, ts, "/v1/auth/github", map[string]any{"access_token": "gho_bad"})
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

	resp := postJSON(t, ts, "/v1/auth/github", map[string]any{"access_token": "gho_good"})
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

// 5. GET /v1/me without/with a bogus bearer -> 401.
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
			resp := getWithBearer(t, ts, "/v1/me", tc.token)
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

	resp := postJSON(t, ts, "/v1/auth/github", map[string]any{"access_token": "gho_good"})
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

	resp := postRaw(t, ts, "/v1/auth/github", `{"access_token":"gho_good","extra_field":"nope"}`)
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

	resp := postJSON(t, ts, "/v1/auth/github", map[string]any{"access_token": "gho_good"})
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
	resp := postRaw(t, ts, "/v1/auth/github", huge)
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

	resp := postJSON(t, ts, "/v1/auth/github", map[string]any{"access_token": ""})
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

			resp := postJSON(t, ts, "/v1/auth/github", map[string]any{"access_token": "gho_good"})
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

// requireUser never leaks whether a token merely doesn't exist vs. is
// malformed — both are the same 401 unauthenticated, so a scanning client
// learns nothing about which tokens are "close".
func TestRequireUserUniformOn401(t *testing.T) {
	_, _, ts := newTestControld(t)
	tok, _ := NewToken() // well-formed but never issued (no InsertToken)
	resp := getWithBearer(t, ts, "/v1/me", tok)
	raw := readBody(t, resp)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401; body = %s", resp.StatusCode, raw)
	}
	if e := decodeErrBody(t, raw); e.Error.Code != "unauthenticated" {
		t.Errorf("code = %q, want unauthenticated", e.Error.Code)
	}
}

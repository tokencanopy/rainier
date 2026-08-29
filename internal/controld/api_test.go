// internal/controld/api_test.go
package controld

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"rainier/internal/rwire"
)

// ---------------------------------------------------------------------------
// HTTP + auth test helpers (auth_test.go carries the bearer-agnostic ones;
// these add bearer/method/header support for the sessions+runners surface)
// ---------------------------------------------------------------------------

// loginSeq hands out distinct GitHub ids for loginUser, so unrelated test
// cases never collide on memstore's upsert-by-github-id key even though
// they all share the small pool of test login names (alice, bob, root...).
var loginSeq atomic.Int64

// loginUser mints a User and a bearer token directly against st, bypassing
// the GitHub exchange entirely — that flow is auth_test.go's job. Every
// test in this file that needs an authenticated caller uses this.
func loginUser(t *testing.T, st Store, login, role string) (User, string) {
	t.Helper()
	id := loginSeq.Add(1)
	u, err := st.UpsertUser(context.Background(), id, login, role)
	if err != nil {
		t.Fatalf("UpsertUser(%s): %v", login, err)
	}
	tok, hash := NewToken()
	if err := st.InsertToken(context.Background(), u.ID, hash); err != nil {
		t.Fatalf("InsertToken(%s): %v", login, err)
	}
	return u, tok
}

// doRequest issues method against path, with an optional bearer token, an
// optional body, and optional extra headers.
func doRequest(t *testing.T, ts *httptest.Server, method, path, token string, body io.Reader, headers map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, ts.URL+path, body)
	if err != nil {
		t.Fatalf("new request %s %s: %v", method, path, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return resp
}

// doJSON is doRequest with body marshaled from a Go value.
func doJSON(t *testing.T, ts *httptest.Server, method, path, token string, body any, headers map[string]string) *http.Response {
	t.Helper()
	if body == nil {
		return doRequest(t, ts, method, path, token, nil, headers)
	}
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	return doRequest(t, ts, method, path, token, bytes.NewReader(b), headers)
}

// doRaw is doRequest with a literal body string, for tests that need to send
// deliberately malformed or oversized JSON.
func doRaw(t *testing.T, ts *httptest.Server, method, path, token, body string) *http.Response {
	t.Helper()
	return doRequest(t, ts, method, path, token, strings.NewReader(body), nil)
}

// assertKeySet decodes raw as a JSON object and fails unless its key set is
// exactly want — the response-shape regression pin every contract-tested
// endpoint gets.
func assertKeySet(t *testing.T, raw string, want ...string) {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("decode for key-set check: %v; body=%s", err, raw)
	}
	got := make([]string, 0, len(m))
	for k := range m {
		got = append(got, k)
	}
	slices.Sort(got)
	wantSorted := slices.Clone(want)
	slices.Sort(wantSorted)
	if !slices.Equal(got, wantSorted) {
		t.Fatalf("keys = %v, want %v", got, wantSorted)
	}
}

// drainWake empties a possibly-already-pending wake, so a later assertWoke
// only sees a wake this test's own action caused.
func drainWake(s *Server) {
	select {
	case <-s.schedWake:
	default:
	}
}

// assertWoke fails unless wakeScheduler was called (a wake is now pending)
// since the matching drainWake.
func assertWoke(t *testing.T, s *Server) {
	t.Helper()
	select {
	case <-s.schedWake:
	default:
		t.Fatal("wakeScheduler was not called")
	}
}

// ---------------------------------------------------------------------------
// authorizeOwnerOrAdmin (unit)
// ---------------------------------------------------------------------------

func TestAuthorizeOwnerOrAdmin(t *testing.T) {
	admin := User{ID: "usr_admin", Role: "admin"}
	owner := User{ID: "usr_owner", Role: "member"}
	other := User{ID: "usr_other", Role: "member"}
	row := Session{OwnerID: "usr_owner"}

	if !authorizeOwnerOrAdmin(admin, row) {
		t.Error("admin should be authorized regardless of ownership")
	}
	if !authorizeOwnerOrAdmin(owner, row) {
		t.Error("owner should be authorized")
	}
	if authorizeOwnerOrAdmin(other, row) {
		t.Error("non-owner non-admin should not be authorized")
	}
}

// ---------------------------------------------------------------------------
// POST /v1/sessions
// ---------------------------------------------------------------------------

func TestCreateSession(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		u, tok := loginUser(t, st, "alice", "member")

		resp := doJSON(t, ts, http.MethodPost, "/v1/sessions", tok,
			map[string]any{"name": "dev1", "image": "ubuntu:latest", "cmd": []string{"bash"}, "egress_allow": []string{"github.com"}}, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("status = %d, want 202; body=%s", resp.StatusCode, raw)
		}
		if loc := resp.Header.Get("Location"); !strings.HasPrefix(loc, "/v1/sessions/sess_") {
			t.Errorf("Location = %q, want /v1/sessions/sess_...", loc)
		}
		var body sessionEnvelope
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		if body.Session.OwnerID != u.ID {
			t.Errorf("owner_id = %q, want %q", body.Session.OwnerID, u.ID)
		}
		if body.Session.State != string(StateQueued) {
			t.Errorf("state = %q, want queued", body.Session.State)
		}
		if body.Session.Name != "dev1" {
			t.Errorf("name = %q, want dev1", body.Session.Name)
		}

		got := getSession(t, st, body.Session.ID)
		if got.State != StateQueued {
			t.Errorf("stored state = %q, want queued", got.State)
		}
	})

	t.Run("unknown field is 400 invalid_request", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, tok := loginUser(t, st, "alice", "member")
		resp := doRaw(t, ts, http.MethodPost, "/v1/sessions", tok, `{"name":"x","bogus":true}`)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "invalid_request" {
			t.Errorf("code = %q, want invalid_request", e.Error.Code)
		}
	})

	t.Run("oversized body is 400 invalid_request", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, tok := loginUser(t, st, "alice", "member")
		huge := `{"name":"` + strings.Repeat("x", 70<<10) + `"}`
		resp := doRaw(t, ts, http.MethodPost, "/v1/sessions", tok, huge)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "invalid_request" {
			t.Errorf("code = %q, want invalid_request", e.Error.Code)
		}
	})

	t.Run("name taken by a non-terminal session is 409 conflict", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, tok := loginUser(t, st, "alice", "member")
		readBody(t, doJSON(t, ts, http.MethodPost, "/v1/sessions", tok, map[string]any{"name": "dup"}, nil))

		resp := doJSON(t, ts, http.MethodPost, "/v1/sessions", tok, map[string]any{"name": "dup"}, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("status = %d, want 409; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "conflict" {
			t.Errorf("code = %q, want conflict", e.Error.Code)
		}
	})

	t.Run("idempotency key replay returns the existing row, still 202", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, tok := loginUser(t, st, "alice", "member")
		hdr := map[string]string{"Idempotency-Key": "idem-1"}

		firstRaw := readBody(t, doJSON(t, ts, http.MethodPost, "/v1/sessions", tok, map[string]any{"name": "once"}, hdr))
		var firstBody sessionEnvelope
		if err := json.Unmarshal([]byte(firstRaw), &firstBody); err != nil {
			t.Fatalf("decode first: %v; body=%s", err, firstRaw)
		}

		second := doJSON(t, ts, http.MethodPost, "/v1/sessions", tok, map[string]any{"name": "once"}, hdr)
		secondRaw := readBody(t, second)
		if second.StatusCode != http.StatusAccepted {
			t.Fatalf("replay status = %d, want 202; body=%s", second.StatusCode, secondRaw)
		}
		var secondBody sessionEnvelope
		if err := json.Unmarshal([]byte(secondRaw), &secondBody); err != nil {
			t.Fatalf("decode second: %v; body=%s", err, secondRaw)
		}
		if secondBody.Session.ID != firstBody.Session.ID {
			t.Fatalf("replay id = %q, want %q (same row)", secondBody.Session.ID, firstBody.Session.ID)
		}

		rows, _, err := st.ListSessions(context.Background(), SessionQuery{IncludeTerminal: true, Limit: 100})
		if err != nil {
			t.Fatalf("ListSessions: %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("stored rows = %d, want 1 (no duplicate created)", len(rows))
		}
	})

	t.Run("nil cmd and egress_allow render as empty arrays, not null", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		u, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, Session{ID: "sess_nilarr", OwnerID: u.ID, State: StateQueued})

		resp := doRequest(t, ts, http.MethodGet, "/v1/sessions/sess_nilarr", tok, nil, nil)
		raw := readBody(t, resp)
		if strings.Contains(raw, `"cmd":null`) || strings.Contains(raw, `"egress_allow":null`) {
			t.Fatalf("nil slice rendered as JSON null: %s", raw)
		}
		var body sessionEnvelope
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		if body.Session.Cmd == nil || len(body.Session.Cmd) != 0 {
			t.Errorf("cmd = %#v, want empty non-nil slice", body.Session.Cmd)
		}
		if body.Session.EgressAllow == nil || len(body.Session.EgressAllow) != 0 {
			t.Errorf("egress_allow = %#v, want empty non-nil slice", body.Session.EgressAllow)
		}
	})

	t.Run("response shape is pinned", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, tok := loginUser(t, st, "alice", "member")
		resp := doJSON(t, ts, http.MethodPost, "/v1/sessions", tok, map[string]any{"name": "shape"}, nil)
		raw := readBody(t, resp)
		assertKeySet(t, raw, "session")
		var outer map[string]json.RawMessage
		if err := json.Unmarshal([]byte(raw), &outer); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		assertKeySet(t, string(outer["session"]),
			"id", "owner_id", "name", "image", "cmd", "egress_allow", "state", "runner",
			"reachable", "error", "created_at", "updated_at", "last_event_at")
	})
}

// ---------------------------------------------------------------------------
// GET /v1/sessions
// ---------------------------------------------------------------------------

// spyListStore records the SessionQuery it was last called with, so a test
// can pin the default/cap on Limit without seeding 100+ rows.
type spyListStore struct {
	Store
	lastQuery SessionQuery
}

func (s *spyListStore) ListSessions(ctx context.Context, q SessionQuery) ([]Session, string, error) {
	s.lastQuery = q
	return s.Store.ListSessions(ctx, q)
}

func TestListSessions(t *testing.T) {
	t.Run("happy path is team-visible and hides terminal by default", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, tok := loginUser(t, st, "alice", "member")
		other, _ := loginUser(t, st, "bob", "member")

		seedSession(t, st, Session{ID: "sess_l1", OwnerID: owner.ID, State: StateQueued, Name: "l1"})
		seedSession(t, st, Session{ID: "sess_l2", OwnerID: other.ID, State: StateRunning, Name: "l2"})
		seedSession(t, st, Session{ID: "sess_l3", OwnerID: owner.ID, State: StateDestroyed, Name: "l3"})

		resp := doRequest(t, ts, http.MethodGet, "/v1/sessions", tok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200; body=%s", resp.StatusCode, raw)
		}
		var body sessionsEnvelope
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		ids := map[string]bool{}
		for _, sv := range body.Sessions {
			ids[sv.ID] = true
		}
		if !ids["sess_l1"] || !ids["sess_l2"] {
			t.Errorf("sessions = %v, want l1 (own) and l2 (team-visible) present", ids)
		}
		if ids["sess_l3"] {
			t.Errorf("terminal session l3 present without all=true")
		}
	})

	t.Run("all=true includes terminal sessions", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, Session{ID: "sess_term", OwnerID: owner.ID, State: StateDestroyed, Name: "term"})

		resp := doRequest(t, ts, http.MethodGet, "/v1/sessions?all=true", tok, nil, nil)
		raw := readBody(t, resp)
		var body sessionsEnvelope
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		found := false
		for _, sv := range body.Sessions {
			if sv.ID == "sess_term" {
				found = true
			}
		}
		if !found {
			t.Fatalf("all=true did not include terminal session; got %+v", body.Sessions)
		}
	})

	t.Run("invalid cursor is 400 invalid_request", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, tok := loginUser(t, st, "alice", "member")
		resp := doRequest(t, ts, http.MethodGet, "/v1/sessions?cursor=not-valid-base64", tok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "invalid_request" {
			t.Errorf("code = %q, want invalid_request", e.Error.Code)
		}
	})

	t.Run("limit defaults to 50 and caps at 100", func(t *testing.T) {
		spy := &spyListStore{Store: NewMemStore()}
		_, ts := newTestControldOver(t, spy)
		_, tok := loginUser(t, spy, "alice", "member")

		readBody(t, doRequest(t, ts, http.MethodGet, "/v1/sessions", tok, nil, nil))
		if spy.lastQuery.Limit != defaultListLimit {
			t.Errorf("default limit = %d, want %d", spy.lastQuery.Limit, defaultListLimit)
		}

		readBody(t, doRequest(t, ts, http.MethodGet, "/v1/sessions?limit=1000", tok, nil, nil))
		if spy.lastQuery.Limit != maxListLimit {
			t.Errorf("capped limit = %d, want %d", spy.lastQuery.Limit, maxListLimit)
		}
	})

	t.Run("non-numeric limit is 400 invalid_request", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, tok := loginUser(t, st, "alice", "member")
		resp := doRequest(t, ts, http.MethodGet, "/v1/sessions?limit=banana", tok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body=%s", resp.StatusCode, raw)
		}
	})

	t.Run("response shape is pinned", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, Session{ID: "sess_shape_list", OwnerID: owner.ID, State: StateQueued, Name: "shape"})
		resp := doRequest(t, ts, http.MethodGet, "/v1/sessions", tok, nil, nil)
		raw := readBody(t, resp)
		assertKeySet(t, raw, "sessions", "next_cursor")
	})
}

// ---------------------------------------------------------------------------
// GET /v1/sessions/{id}
// ---------------------------------------------------------------------------

func TestGetSession(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, Session{ID: "sess_get1", OwnerID: owner.ID, State: StateRunning, Name: "get1", Runner: "vm1"})

		resp := doRequest(t, ts, http.MethodGet, "/v1/sessions/sess_get1", tok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200; body=%s", resp.StatusCode, raw)
		}
		var body sessionEnvelope
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		if body.Session.ID != "sess_get1" {
			t.Errorf("id = %q, want sess_get1", body.Session.ID)
		}
		// vm1 was never connected, so reachable must be false even though
		// the row says running.
		if body.Session.Reachable {
			t.Errorf("reachable = true, want false (runner not connected)")
		}
	})

	t.Run("unknown id is 404 not_found", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, tok := loginUser(t, st, "alice", "member")
		resp := doRequest(t, ts, http.MethodGet, "/v1/sessions/sess_nope", tok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("status = %d, want 404; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "not_found" {
			t.Errorf("code = %q, want not_found", e.Error.Code)
		}
	})

	t.Run("reachable is true only when the runner is connected and the session is non-terminal", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, Session{ID: "sess_reach", OwnerID: owner.ID, State: StateRunning, Runner: "vm1"})
		// Announce the row present and agreeing so reconcile doesn't sweep
		// it (an announce silent on it would mark it dead).
		startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Sessions: []rwire.SessionInfo{{ID: "sess_reach", State: "running"}}})
		waitConnected(t, s, "vm1")

		resp := doRequest(t, ts, http.MethodGet, "/v1/sessions/sess_reach", tok, nil, nil)
		raw := readBody(t, resp)
		var body sessionEnvelope
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		if !body.Session.Reachable {
			t.Errorf("reachable = false, want true (runner connected, non-terminal)")
		}
	})

	t.Run("response shape is pinned", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, Session{ID: "sess_shape2", OwnerID: owner.ID, State: StateQueued})
		resp := doRequest(t, ts, http.MethodGet, "/v1/sessions/sess_shape2", tok, nil, nil)
		raw := readBody(t, resp)
		assertKeySet(t, raw, "session")
	})
}

// ---------------------------------------------------------------------------
// DELETE /v1/sessions/{id}
// ---------------------------------------------------------------------------

func TestDeleteSession(t *testing.T) {
	t.Run("queued cancels without dispatch and wakes the scheduler", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, Session{ID: "sess_del_q", OwnerID: owner.ID, State: StateQueued, Name: "delq"})

		drainWake(s)
		resp := doRequest(t, ts, http.MethodDelete, "/v1/sessions/sess_del_q", tok, nil, nil)
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("status = %d, want 204", resp.StatusCode)
		}
		got := getSession(t, st, "sess_del_q")
		if got.State != StateCanceled {
			t.Fatalf("state = %q, want canceled", got.State)
		}
		assertWoke(t, s)
	})

	t.Run("creating is 409 conflict, no dispatch", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, Session{ID: "sess_del_c", OwnerID: owner.ID, State: StateCreating, Runner: "vm1"})

		resp := doRequest(t, ts, http.MethodDelete, "/v1/sessions/sess_del_c", tok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("status = %d, want 409; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "conflict" {
			t.Errorf("code = %q, want conflict", e.Error.Code)
		}
		got := getSession(t, st, "sess_del_c")
		if got.State != StateCreating {
			t.Fatalf("state = %q, want unchanged (creating)", got.State)
		}
	})

	t.Run("non-owner non-admin is 403 forbidden", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, _ := loginUser(t, st, "alice", "member")
		_, otherTok := loginUser(t, st, "bob", "member")
		seedSession(t, st, Session{ID: "sess_del_authz", OwnerID: owner.ID, State: StateQueued})

		resp := doRequest(t, ts, http.MethodDelete, "/v1/sessions/sess_del_authz", otherTok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("status = %d, want 403; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "forbidden" {
			t.Errorf("code = %q, want forbidden", e.Error.Code)
		}
		got := getSession(t, st, "sess_del_authz")
		if got.State != StateQueued {
			t.Fatalf("state = %q, want unchanged (queued)", got.State)
		}
	})

	t.Run("admin may delete another owner's session", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, _ := loginUser(t, st, "alice", "member")
		_, adminTok := loginUser(t, st, "root", "admin")
		seedSession(t, st, Session{ID: "sess_del_admin", OwnerID: owner.ID, State: StateQueued})

		resp := doRequest(t, ts, http.MethodDelete, "/v1/sessions/sess_del_admin", adminTok, nil, nil)
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("status = %d, want 204", resp.StatusCode)
		}
	})

	t.Run("terminal is idempotent 204", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, Session{ID: "sess_del_term", OwnerID: owner.ID, State: StateDestroyed})

		resp := doRequest(t, ts, http.MethodDelete, "/v1/sessions/sess_del_term", tok, nil, nil)
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("status = %d, want 204", resp.StatusCode)
		}
		got := getSession(t, st, "sess_del_term")
		if got.State != StateDestroyed {
			t.Fatalf("state = %q, want unchanged (destroyed)", got.State)
		}
	})

	t.Run("placed on a disconnected runner marks destroyed directly", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, Session{ID: "sess_del_gone", OwnerID: owner.ID, State: StateRunning, Runner: "vm-ghost"})

		resp := doRequest(t, ts, http.MethodDelete, "/v1/sessions/sess_del_gone", tok, nil, nil)
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("status = %d, want 204", resp.StatusCode)
		}
		got := getSession(t, st, "sess_del_gone")
		if got.State != StateDestroyed {
			t.Fatalf("state = %q, want destroyed", got.State)
		}
	})

	t.Run("placed on a connected runner dispatches destroy", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Sessions: []rwire.SessionInfo{{ID: ghostSession, State: "running"}}})
		waitConnected(t, s, "vm1")
		awaitReconciled(t, f)

		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, Session{ID: "sess_del_live", OwnerID: owner.ID, State: StateRunning, Runner: "vm1"})

		type result struct{ resp *http.Response }
		resc := make(chan result, 1)
		go func() {
			resc <- result{doRequest(t, ts, http.MethodDelete, "/v1/sessions/sess_del_live", tok, nil, nil)}
		}()

		cmd := f.nextCmd(t)
		if cmd.Type != "destroy" || cmd.Session != "sess_del_live" {
			t.Fatalf("got %+v, want destroy of sess_del_live", cmd)
		}
		f.reply(t, cmd, true, "")

		resp := (<-resc).resp
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("status = %d, want 204", resp.StatusCode)
		}
		wantState(t, st, "sess_del_live", StateDestroyed)
	})

	t.Run("runner unreachable is 502 runner_unreachable", func(t *testing.T) {
		s, st, ts := newTestControld(t, func(c *Config) { c.OpTimeout = 150 * time.Millisecond })
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Sessions: []rwire.SessionInfo{{ID: ghostSession, State: "running"}}})
		waitConnected(t, s, "vm1")
		awaitReconciled(t, f)

		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, Session{ID: "sess_del_timeout", OwnerID: owner.ID, State: StateRunning, Runner: "vm1"})

		resp := doRequest(t, ts, http.MethodDelete, "/v1/sessions/sess_del_timeout", tok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("status = %d, want 502; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "runner_unreachable" {
			t.Errorf("code = %q, want runner_unreachable", e.Error.Code)
		}
		f.nextCmd(t) // the destroy did reach the runner; it just never answered
	})

	// Not in the route table explicitly: a runner that answers but reports
	// ok:false (as opposed to never answering) is a genuine, unexpected
	// failure — mapped to 500 internal, detail logged and never echoed, row
	// left alone so the client can retry or investigate.
	t.Run("runner-reported destroy failure is 500 internal, detail not leaked", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Sessions: []rwire.SessionInfo{{ID: ghostSession, State: "running"}}})
		waitConnected(t, s, "vm1")
		awaitReconciled(t, f)

		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, Session{ID: "sess_del_fail", OwnerID: owner.ID, State: StateRunning, Runner: "vm1"})

		type result struct{ resp *http.Response }
		resc := make(chan result, 1)
		go func() {
			resc <- result{doRequest(t, ts, http.MethodDelete, "/v1/sessions/sess_del_fail", tok, nil, nil)}
		}()
		cmd := f.nextCmd(t)
		f.reply(t, cmd, false, "docker: no such container")

		resp := (<-resc).resp
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "internal" {
			t.Errorf("code = %q, want internal", e.Error.Code)
		}
		if strings.Contains(raw, "docker: no such container") {
			t.Errorf("response leaked runner detail: %s", raw)
		}
		got := getSession(t, st, "sess_del_fail")
		if got.State != StateRunning {
			t.Fatalf("state = %q, want unchanged (running); a failed destroy must not be marked destroyed", got.State)
		}
	})
}

// raceTransitionStore deterministically forces the race a concurrent
// mutation (e.g. a DELETE landing during a suspend/resume's OpTimeout
// dispatch window) can win: the first time Transition is called for
// triggerID, it first moves the row to raceToState directly — exactly as an
// independent, already-guarded request would have, out from under the
// handler under test — and only then lets the real (now losing) Transition
// call proceed, so it observes the row already moved and returns
// ErrConflict.
type raceTransitionStore struct {
	Store
	triggerID   string
	raceToState SessionState
	triggered   bool
}

func (r *raceTransitionStore) Transition(ctx context.Context, id string, from []SessionState, to SessionState, opts TransitionOpts) error {
	if !r.triggered && id == r.triggerID {
		r.triggered = true
		if err := r.Store.Transition(ctx, id, NonTerminal, r.raceToState, TransitionOpts{}); err != nil {
			panic(fmt.Sprintf("raceTransitionStore: forcing the race: %v", err))
		}
	}
	return r.Store.Transition(ctx, id, from, to, opts)
}

// ---------------------------------------------------------------------------
// POST /v1/sessions/{id}/suspend
// ---------------------------------------------------------------------------

func TestSuspendSession(t *testing.T) {
	t.Run("happy path is warm by default", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Sessions: []rwire.SessionInfo{{ID: ghostSession, State: "running"}}})
		waitConnected(t, s, "vm1")
		awaitReconciled(t, f)

		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, Session{ID: "sess_susp1", OwnerID: owner.ID, State: StateRunning, Runner: "vm1"})

		type result struct{ resp *http.Response }
		resc := make(chan result, 1)
		go func() {
			resc <- result{doRequest(t, ts, http.MethodPost, "/v1/sessions/sess_susp1/suspend", tok, nil, nil)}
		}()
		cmd := f.nextCmd(t)
		if cmd.Type != "suspend" || cmd.Session != "sess_susp1" || !cmd.Warm {
			t.Fatalf("got %+v, want warm suspend of sess_susp1", cmd)
		}
		f.reply(t, cmd, true, "")

		resp := (<-resc).resp
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200; body=%s", resp.StatusCode, raw)
		}
		var body sessionEnvelope
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		if body.Session.State != string(StateSuspendedWarm) {
			t.Errorf("state = %q, want suspended_warm", body.Session.State)
		}
		wantState(t, st, "sess_susp1", StateSuspendedWarm)
	})

	t.Run("warm:false suspends cold", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Sessions: []rwire.SessionInfo{{ID: ghostSession, State: "running"}}})
		waitConnected(t, s, "vm1")
		awaitReconciled(t, f)

		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, Session{ID: "sess_susp2", OwnerID: owner.ID, State: StateRunning, Runner: "vm1"})

		type result struct{ resp *http.Response }
		resc := make(chan result, 1)
		go func() {
			resc <- result{doJSON(t, ts, http.MethodPost, "/v1/sessions/sess_susp2/suspend", tok, map[string]any{"warm": false}, nil)}
		}()
		cmd := f.nextCmd(t)
		if cmd.Warm {
			t.Fatalf("Warm = true, want false")
		}
		f.reply(t, cmd, true, "")

		resp := (<-resc).resp
		raw := readBody(t, resp)
		var body sessionEnvelope
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		if body.Session.State != string(StateSuspendedCold) {
			t.Errorf("state = %q, want suspended_cold", body.Session.State)
		}
	})

	t.Run("unknown field in body is 400 invalid_request", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, Session{ID: "sess_susp_badbody", OwnerID: owner.ID, State: StateRunning, Runner: "vm1"})

		resp := doRaw(t, ts, http.MethodPost, "/v1/sessions/sess_susp_badbody/suspend", tok, `{"warm":true,"bogus":1}`)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "invalid_request" {
			t.Errorf("code = %q, want invalid_request", e.Error.Code)
		}
	})

	t.Run("not running is 409 conflict", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, Session{ID: "sess_susp_bad", OwnerID: owner.ID, State: StateQueued})

		resp := doRequest(t, ts, http.MethodPost, "/v1/sessions/sess_susp_bad/suspend", tok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("status = %d, want 409; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "conflict" {
			t.Errorf("code = %q, want conflict", e.Error.Code)
		}
	})

	t.Run("non-owner non-admin is 403 forbidden", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, _ := loginUser(t, st, "alice", "member")
		_, otherTok := loginUser(t, st, "bob", "member")
		seedSession(t, st, Session{ID: "sess_susp_authz", OwnerID: owner.ID, State: StateRunning, Runner: "vm1"})

		resp := doRequest(t, ts, http.MethodPost, "/v1/sessions/sess_susp_authz/suspend", otherTok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("status = %d, want 403; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "forbidden" {
			t.Errorf("code = %q, want forbidden", e.Error.Code)
		}
	})

	t.Run("runner unreachable is 502 runner_unreachable", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, Session{ID: "sess_susp_unreach", OwnerID: owner.ID, State: StateRunning, Runner: "vm-nope"})

		resp := doRequest(t, ts, http.MethodPost, "/v1/sessions/sess_susp_unreach/suspend", tok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("status = %d, want 502; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "runner_unreachable" {
			t.Errorf("code = %q, want runner_unreachable", e.Error.Code)
		}
	})

	t.Run("response shape is pinned", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Sessions: []rwire.SessionInfo{{ID: ghostSession, State: "running"}}})
		waitConnected(t, s, "vm1")
		awaitReconciled(t, f)

		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, Session{ID: "sess_susp_shape", OwnerID: owner.ID, State: StateRunning, Runner: "vm1"})

		type result struct{ resp *http.Response }
		resc := make(chan result, 1)
		go func() {
			resc <- result{doRequest(t, ts, http.MethodPost, "/v1/sessions/sess_susp_shape/suspend", tok, nil, nil)}
		}()
		cmd := f.nextCmd(t)
		f.reply(t, cmd, true, "")
		resp := (<-resc).resp
		raw := readBody(t, resp)
		assertKeySet(t, raw, "session")
	})

	// The runner op executes (ok:true), but a concurrent DELETE moves the
	// row to destroyed between the handler's initial GetSession and its
	// guarded post-dispatch Transition. The 200 response must report the
	// store's real state (destroyed), never the suspended_warm the handler
	// was trying to reach.
	t.Run("concurrent mutation racing the runner round-trip: response reflects real persisted state", func(t *testing.T) {
		const id = "sess_susp_race"
		race := &raceTransitionStore{Store: NewMemStore(), triggerID: id, raceToState: StateDestroyed}
		s, ts := newTestControldOver(t, race)
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Sessions: []rwire.SessionInfo{{ID: ghostSession, State: "running"}}})
		waitConnected(t, s, "vm1")
		awaitReconciled(t, f)

		owner, tok := loginUser(t, race, "alice", "member")
		seedSession(t, race, Session{ID: id, OwnerID: owner.ID, State: StateRunning, Runner: "vm1"})

		type result struct{ resp *http.Response }
		resc := make(chan result, 1)
		go func() {
			resc <- result{doRequest(t, ts, http.MethodPost, "/v1/sessions/"+id+"/suspend", tok, nil, nil)}
		}()
		cmd := f.nextCmd(t)
		f.reply(t, cmd, true, "")

		resp := (<-resc).resp
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200 (the runner op did execute); body=%s", resp.StatusCode, raw)
		}
		var body sessionEnvelope
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		if body.Session.State != string(StateDestroyed) {
			t.Fatalf("response state = %q, want destroyed (the real persisted state) — got a fabricated state instead", body.Session.State)
		}
		got := getSession(t, race, id)
		if got.State != StateDestroyed {
			t.Fatalf("stored state = %q, want destroyed", got.State)
		}
	})
}

// ---------------------------------------------------------------------------
// POST /v1/sessions/{id}/resume
// ---------------------------------------------------------------------------

func TestResumeSession(t *testing.T) {
	t.Run("warm resume happy path", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Sessions: []rwire.SessionInfo{{ID: ghostSession, State: "running"}}})
		waitConnected(t, s, "vm1")
		awaitReconciled(t, f)

		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, Session{ID: "sess_res1", OwnerID: owner.ID, State: StateSuspendedWarm, Runner: "vm1"})

		type result struct{ resp *http.Response }
		resc := make(chan result, 1)
		go func() {
			resc <- result{doRequest(t, ts, http.MethodPost, "/v1/sessions/sess_res1/resume", tok, nil, nil)}
		}()
		cmd := f.nextCmd(t)
		if cmd.Type != "resume" || cmd.Session != "sess_res1" {
			t.Fatalf("got %+v, want resume of sess_res1", cmd)
		}
		f.reply(t, cmd, true, "")

		resp := (<-resc).resp
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200; body=%s", resp.StatusCode, raw)
		}
		var body sessionEnvelope
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		if body.Session.State != string(StateRunning) {
			t.Errorf("state = %q, want running", body.Session.State)
		}
	})

	t.Run("cold resume onto a full runner is 409 no_capacity naming the runner", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 1, Used: 1,
			Sessions: []rwire.SessionInfo{{ID: ghostSession, State: "running"}}})
		waitConnected(t, s, "vm1")
		awaitReconciled(t, f)

		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, Session{ID: "sess_res_full", OwnerID: owner.ID, State: StateSuspendedCold, Runner: "vm1"})

		resp := doRequest(t, ts, http.MethodPost, "/v1/sessions/sess_res_full/resume", tok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("status = %d, want 409; body=%s", resp.StatusCode, raw)
		}
		e := decodeErrBody(t, raw)
		if e.Error.Code != "no_capacity" {
			t.Errorf("code = %q, want no_capacity", e.Error.Code)
		}
		if !strings.Contains(e.Error.Message, "vm1") {
			t.Errorf("message = %q, want it to name vm1", e.Error.Message)
		}
	})

	t.Run("runner disconnected is 502 runner_unreachable", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, Session{ID: "sess_res_gone", OwnerID: owner.ID, State: StateSuspendedWarm, Runner: "vm-ghost"})

		resp := doRequest(t, ts, http.MethodPost, "/v1/sessions/sess_res_gone/resume", tok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("status = %d, want 502; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "runner_unreachable" {
			t.Errorf("code = %q, want runner_unreachable", e.Error.Code)
		}
	})

	t.Run("not suspended is 409 conflict", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, Session{ID: "sess_res_bad", OwnerID: owner.ID, State: StateQueued})

		resp := doRequest(t, ts, http.MethodPost, "/v1/sessions/sess_res_bad/resume", tok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("status = %d, want 409; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "conflict" {
			t.Errorf("code = %q, want conflict", e.Error.Code)
		}
	})

	t.Run("non-owner non-admin is 403 forbidden", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, _ := loginUser(t, st, "alice", "member")
		_, otherTok := loginUser(t, st, "bob", "member")
		seedSession(t, st, Session{ID: "sess_res_authz", OwnerID: owner.ID, State: StateSuspendedWarm, Runner: "vm1"})

		resp := doRequest(t, ts, http.MethodPost, "/v1/sessions/sess_res_authz/resume", otherTok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("status = %d, want 403; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "forbidden" {
			t.Errorf("code = %q, want forbidden", e.Error.Code)
		}
	})

	t.Run("response shape is pinned", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Sessions: []rwire.SessionInfo{{ID: ghostSession, State: "running"}}})
		waitConnected(t, s, "vm1")
		awaitReconciled(t, f)

		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, Session{ID: "sess_res_shape", OwnerID: owner.ID, State: StateSuspendedWarm, Runner: "vm1"})

		type result struct{ resp *http.Response }
		resc := make(chan result, 1)
		go func() {
			resc <- result{doRequest(t, ts, http.MethodPost, "/v1/sessions/sess_res_shape/resume", tok, nil, nil)}
		}()
		cmd := f.nextCmd(t)
		f.reply(t, cmd, true, "")
		resp := (<-resc).resp
		raw := readBody(t, resp)
		assertKeySet(t, raw, "session")
	})

	// Same race as suspend's, forced against resume: the runner op executes
	// (ok:true), but a concurrent DELETE moves the row to destroyed before
	// resume's guarded Transition lands. The response must report the real
	// persisted state, not a fabricated "running".
	t.Run("concurrent mutation racing the runner round-trip: response reflects real persisted state", func(t *testing.T) {
		const id = "sess_res_race"
		race := &raceTransitionStore{Store: NewMemStore(), triggerID: id, raceToState: StateDestroyed}
		s, ts := newTestControldOver(t, race)
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Sessions: []rwire.SessionInfo{{ID: ghostSession, State: "running"}}})
		waitConnected(t, s, "vm1")
		awaitReconciled(t, f)

		owner, tok := loginUser(t, race, "alice", "member")
		seedSession(t, race, Session{ID: id, OwnerID: owner.ID, State: StateSuspendedWarm, Runner: "vm1"})

		type result struct{ resp *http.Response }
		resc := make(chan result, 1)
		go func() {
			resc <- result{doRequest(t, ts, http.MethodPost, "/v1/sessions/"+id+"/resume", tok, nil, nil)}
		}()
		cmd := f.nextCmd(t)
		f.reply(t, cmd, true, "")

		resp := (<-resc).resp
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200 (the runner op did execute); body=%s", resp.StatusCode, raw)
		}
		var body sessionEnvelope
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		if body.Session.State != string(StateDestroyed) {
			t.Fatalf("response state = %q, want destroyed (the real persisted state) — got a fabricated state instead", body.Session.State)
		}
		got := getSession(t, race, id)
		if got.State != StateDestroyed {
			t.Fatalf("stored state = %q, want destroyed", got.State)
		}
	})
}

// ---------------------------------------------------------------------------
// POST /v1/sessions/{id}/snapshot
// ---------------------------------------------------------------------------

func TestSnapshotSession(t *testing.T) {
	t.Run("happy path from running", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Sessions: []rwire.SessionInfo{{ID: ghostSession, State: "running"}}})
		waitConnected(t, s, "vm1")
		awaitReconciled(t, f)

		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, Session{ID: "sess_snap1", OwnerID: owner.ID, State: StateRunning, Runner: "vm1"})

		type result struct{ resp *http.Response }
		resc := make(chan result, 1)
		go func() {
			resc <- result{doRequest(t, ts, http.MethodPost, "/v1/sessions/sess_snap1/snapshot", tok, nil, nil)}
		}()
		cmd := f.nextCmd(t)
		if cmd.Type != "snapshot" || cmd.Session != "sess_snap1" {
			t.Fatalf("got %+v, want snapshot of sess_snap1", cmd)
		}
		f.reply(t, cmd, true, "ref-abc123")

		resp := (<-resc).resp
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200; body=%s", resp.StatusCode, raw)
		}
		var body snapshotResponse
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		if body.Ref != "ref-abc123" {
			t.Errorf("ref = %q, want ref-abc123", body.Ref)
		}
	})

	t.Run("from queued is 409 conflict", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, Session{ID: "sess_snap_bad", OwnerID: owner.ID, State: StateQueued})

		resp := doRequest(t, ts, http.MethodPost, "/v1/sessions/sess_snap_bad/snapshot", tok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("status = %d, want 409; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "conflict" {
			t.Errorf("code = %q, want conflict", e.Error.Code)
		}
	})

	t.Run("non-owner non-admin is 403 forbidden", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, _ := loginUser(t, st, "alice", "member")
		_, otherTok := loginUser(t, st, "bob", "member")
		seedSession(t, st, Session{ID: "sess_snap_authz", OwnerID: owner.ID, State: StateRunning, Runner: "vm1"})

		resp := doRequest(t, ts, http.MethodPost, "/v1/sessions/sess_snap_authz/snapshot", otherTok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("status = %d, want 403; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "forbidden" {
			t.Errorf("code = %q, want forbidden", e.Error.Code)
		}
	})

	t.Run("runner unreachable is 502 runner_unreachable", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, Session{ID: "sess_snap_unreach", OwnerID: owner.ID, State: StateRunning, Runner: "vm-ghost"})

		resp := doRequest(t, ts, http.MethodPost, "/v1/sessions/sess_snap_unreach/snapshot", tok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("status = %d, want 502; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "runner_unreachable" {
			t.Errorf("code = %q, want runner_unreachable", e.Error.Code)
		}
	})

	t.Run("response shape is pinned", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Sessions: []rwire.SessionInfo{{ID: ghostSession, State: "running"}}})
		waitConnected(t, s, "vm1")
		awaitReconciled(t, f)

		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, Session{ID: "sess_snap_shape", OwnerID: owner.ID, State: StateRunning, Runner: "vm1"})

		type result struct{ resp *http.Response }
		resc := make(chan result, 1)
		go func() {
			resc <- result{doRequest(t, ts, http.MethodPost, "/v1/sessions/sess_snap_shape/snapshot", tok, nil, nil)}
		}()
		cmd := f.nextCmd(t)
		f.reply(t, cmd, true, "ref-xyz")
		resp := (<-resc).resp
		raw := readBody(t, resp)
		assertKeySet(t, raw, "ref")
	})
}

// ---------------------------------------------------------------------------
// secrets: the key is a required, fail-closed config
// ---------------------------------------------------------------------------

// TestNewRequiresSecretsKey pins the fail-closed rule: a controld with no
// secrets key must refuse to start rather than come up with a secrets API
// that would seal everything under a key of zeros.
func TestNewRequiresSecretsKey(t *testing.T) {
	t.Run("a zero SecretsKey is refused, naming the env var", func(t *testing.T) {
		_, err := New(NewMemStore(), Config{RunnerToken: "t", ExternalURL: "http://x:9090"})
		if err == nil {
			t.Fatal("New with no SecretsKey: want error, got nil")
		}
		if !strings.Contains(err.Error(), "RAINIER_SECRETS_KEY") {
			t.Errorf("error = %q, want it to name RAINIER_SECRETS_KEY", err)
		}
	})

	t.Run("a configured key is accepted", func(t *testing.T) {
		if _, err := New(NewMemStore(), Config{
			RunnerToken: "t",
			ExternalURL: "http://x:9090",
			SecretsKey:  testSecretsKey,
		}); err != nil {
			t.Fatalf("New with a SecretsKey: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// PUT /v1/secrets/{name}
// ---------------------------------------------------------------------------

// getSecretValue decrypts what the store actually holds for name, so a test
// can assert the round trip through the real sealing path rather than
// trusting the handler's own 204.
func getSecretValue(t *testing.T, st Store, name string) string {
	t.Helper()
	ct, nonce, err := st.GetSecret(context.Background(), name)
	if err != nil {
		t.Fatalf("GetSecret(%s): %v", name, err)
	}
	pt, err := Open(testSecretsKey, ct, nonce)
	if err != nil {
		t.Fatalf("Open(%s): %v", name, err)
	}
	return string(pt)
}

func TestPutSecret(t *testing.T) {
	t.Run("happy path stores the value sealed and answers 204", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")

		resp := doJSON(t, ts, http.MethodPut, "/v1/secrets/GH_TOKEN", adminTok, map[string]any{"value": "ghp_supersecret"}, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("status = %d, want 204; body=%s", resp.StatusCode, raw)
		}
		if raw != "" {
			t.Errorf("204 carried a body: %q", raw)
		}

		ct, nonce, err := st.GetSecret(context.Background(), "GH_TOKEN")
		if err != nil {
			t.Fatalf("GetSecret: %v", err)
		}
		if strings.Contains(string(ct), "ghp_supersecret") {
			t.Fatalf("stored ciphertext contains the plaintext: %q", ct)
		}
		if len(nonce) != 12 {
			t.Errorf("stored nonce is %d bytes, want 12", len(nonce))
		}
		if got := getSecretValue(t, st, "GH_TOKEN"); got != "ghp_supersecret" {
			t.Errorf("stored value = %q, want ghp_supersecret", got)
		}
	})

	t.Run("a second PUT replaces the value", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")

		readBody(t, doJSON(t, ts, http.MethodPut, "/v1/secrets/API_KEY", adminTok, map[string]any{"value": "first"}, nil))
		resp := doJSON(t, ts, http.MethodPut, "/v1/secrets/API_KEY", adminTok, map[string]any{"value": "second"}, nil)
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("status = %d, want 204; body=%s", resp.StatusCode, readBody(t, resp))
		}
		readBody(t, resp)
		if got := getSecretValue(t, st, "API_KEY"); got != "second" {
			t.Fatalf("stored value = %q, want second (the replacement)", got)
		}
	})

	t.Run("invalid names are 400 invalid_request", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")

		for _, name := range []string{
			"lowercase",
			"HAS-DASH",
			"HAS.DOT",
			"HAS SPACE",
			"HÉLLO",
			strings.Repeat("A", 65),
		} {
			resp := doJSON(t, ts, http.MethodPut, "/v1/secrets/"+url.PathEscape(name), adminTok, map[string]any{"value": "v"}, nil)
			raw := readBody(t, resp)
			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("PUT name=%q status = %d, want 400; body=%s", name, resp.StatusCode, raw)
				continue
			}
			if e := decodeErrBody(t, raw); e.Error.Code != "invalid_request" {
				t.Errorf("PUT name=%q code = %q, want invalid_request", name, e.Error.Code)
			}
		}
	})

	t.Run("a 64-character name is accepted (the boundary)", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		name := strings.Repeat("A", 64)
		resp := doJSON(t, ts, http.MethodPut, "/v1/secrets/"+name, adminTok, map[string]any{"value": "v"}, nil)
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("status = %d, want 204; body=%s", resp.StatusCode, readBody(t, resp))
		}
		readBody(t, resp)
	})

	t.Run("an empty value is 400 invalid_request", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		resp := doJSON(t, ts, http.MethodPut, "/v1/secrets/EMPTY", adminTok, map[string]any{"value": ""}, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "invalid_request" {
			t.Errorf("code = %q, want invalid_request", e.Error.Code)
		}
	})

	t.Run("a value over 64KB is 400 invalid_request", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		resp := doJSON(t, ts, http.MethodPut, "/v1/secrets/BIG", adminTok,
			map[string]any{"value": strings.Repeat("x", (64<<10)+1)}, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "invalid_request" {
			t.Errorf("code = %q, want invalid_request", e.Error.Code)
		}
		if _, _, err := st.GetSecret(context.Background(), "BIG"); !errors.Is(err, ErrNotFound) {
			t.Errorf("an over-cap value was stored anyway (GetSecret err = %v)", err)
		}
	})

	t.Run("a value at exactly 64KB is accepted (the boundary)", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		value := strings.Repeat("x", 64<<10)
		resp := doJSON(t, ts, http.MethodPut, "/v1/secrets/ATCAP", adminTok, map[string]any{"value": value}, nil)
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("status = %d, want 204; body=%s", resp.StatusCode, readBody(t, resp))
		}
		readBody(t, resp)
		if got := getSecretValue(t, st, "ATCAP"); got != value {
			t.Errorf("stored value length = %d, want %d", len(got), len(value))
		}
	})

	t.Run("an unbounded body is 400 invalid_request", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		huge := `{"value":"` + strings.Repeat("x", secretsBodyLimit+1) + `"}`
		resp := doRaw(t, ts, http.MethodPut, "/v1/secrets/HUGE", adminTok, huge)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "invalid_request" {
			t.Errorf("code = %q, want invalid_request", e.Error.Code)
		}
	})

	t.Run("unknown field is 400 invalid_request", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		resp := doRaw(t, ts, http.MethodPut, "/v1/secrets/UNKNOWN", adminTok, `{"value":"v","bogus":true}`)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "invalid_request" {
			t.Errorf("code = %q, want invalid_request", e.Error.Code)
		}
	})

	t.Run("a member is 403 forbidden and stores nothing", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, memberTok := loginUser(t, st, "alice", "member")

		resp := doJSON(t, ts, http.MethodPut, "/v1/secrets/MEMBER_TRY", memberTok, map[string]any{"value": "nope"}, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("status = %d, want 403; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "forbidden" {
			t.Errorf("code = %q, want forbidden", e.Error.Code)
		}
		if _, _, err := st.GetSecret(context.Background(), "MEMBER_TRY"); !errors.Is(err, ErrNotFound) {
			t.Errorf("a member's rejected PUT stored the secret anyway (err = %v)", err)
		}
	})

	t.Run("no token is 401 unauthenticated", func(t *testing.T) {
		_, _, ts := newTestControld(t)
		resp := doJSON(t, ts, http.MethodPut, "/v1/secrets/ANON", "", map[string]any{"value": "nope"}, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "unauthenticated" {
			t.Errorf("code = %q, want unauthenticated", e.Error.Code)
		}
	})

	// The value is the one thing this API accepts and never gives back: not
	// in the 204, not in an error, not anywhere.
	t.Run("no response on any path echoes the value", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		_, memberTok := loginUser(t, st, "alice", "member")
		const value = "ghp_never_echo_me"

		bodies := []string{
			readBody(t, doJSON(t, ts, http.MethodPut, "/v1/secrets/ECHO", adminTok, map[string]any{"value": value}, nil)),
			readBody(t, doJSON(t, ts, http.MethodPut, "/v1/secrets/bad-name", adminTok, map[string]any{"value": value}, nil)),
			readBody(t, doJSON(t, ts, http.MethodPut, "/v1/secrets/ECHO", memberTok, map[string]any{"value": value}, nil)),
			readBody(t, doRequest(t, ts, http.MethodGet, "/v1/secrets", adminTok, nil, nil)),
		}
		for i, b := range bodies {
			if strings.Contains(b, value) {
				t.Errorf("response %d echoed the secret value: %s", i, b)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// GET /v1/secrets
// ---------------------------------------------------------------------------

func TestListSecrets(t *testing.T) {
	// putSecret seals and stores a secret directly, so list/delete tests
	// don't have to go through the admin PUT route to have data.
	putSecret := func(t *testing.T, st Store, name, value string) {
		t.Helper()
		ct, nonce, err := Seal(testSecretsKey, []byte(value))
		if err != nil {
			t.Fatalf("Seal(%s): %v", name, err)
		}
		if err := st.PutSecret(context.Background(), name, ct, nonce); err != nil {
			t.Fatalf("PutSecret(%s): %v", name, err)
		}
	}

	t.Run("happy path lists names and timestamps, name ascending", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, tok := loginUser(t, st, "alice", "member")
		putSecret(t, st, "ZULU", "z")
		putSecret(t, st, "ALPHA", "a")

		resp := doRequest(t, ts, http.MethodGet, "/v1/secrets", tok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200; body=%s", resp.StatusCode, raw)
		}
		var body secretsEnvelope
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		if len(body.Secrets) != 2 {
			t.Fatalf("secrets = %+v, want 2", body.Secrets)
		}
		if body.Secrets[0].Name != "ALPHA" || body.Secrets[1].Name != "ZULU" {
			t.Errorf("order = %q, %q, want ALPHA, ZULU", body.Secrets[0].Name, body.Secrets[1].Name)
		}
		for _, sv := range body.Secrets {
			if _, err := time.Parse(time.RFC3339, sv.CreatedAt); err != nil {
				t.Errorf("created_at = %q, want RFC3339: %v", sv.CreatedAt, err)
			}
			if _, err := time.Parse(time.RFC3339, sv.UpdatedAt); err != nil {
				t.Errorf("updated_at = %q, want RFC3339: %v", sv.UpdatedAt, err)
			}
		}
	})

	t.Run("no secrets renders an empty array, not null", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, tok := loginUser(t, st, "alice", "member")
		raw := readBody(t, doRequest(t, ts, http.MethodGet, "/v1/secrets", tok, nil, nil))
		if strings.Contains(raw, `"secrets":null`) {
			t.Fatalf("empty list rendered as JSON null: %s", raw)
		}
	})

	// The mandated raw-JSON assertion: a listing must never grow a value
	// field, whatever a future refactor does to the view struct.
	t.Run("the raw JSON never contains a value key or any value", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, tok := loginUser(t, st, "alice", "member")
		putSecret(t, st, "GH_TOKEN", "ghp_never_listed")

		raw := readBody(t, doRequest(t, ts, http.MethodGet, "/v1/secrets", tok, nil, nil))
		if strings.Contains(raw, "value") {
			t.Fatalf("list body mentions \"value\": %s", raw)
		}
		if strings.Contains(raw, "ghp_never_listed") {
			t.Fatalf("list body leaked the secret value: %s", raw)
		}
		if strings.Contains(raw, "ciphertext") || strings.Contains(raw, "nonce") {
			t.Fatalf("list body leaked the sealed representation: %s", raw)
		}
	})

	t.Run("a member may list (reads are team-visible)", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, tok := loginUser(t, st, "alice", "member")
		putSecret(t, st, "SHARED", "v")
		resp := doRequest(t, ts, http.MethodGet, "/v1/secrets", tok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200; body=%s", resp.StatusCode, raw)
		}
	})

	t.Run("no token is 401 unauthenticated", func(t *testing.T) {
		_, _, ts := newTestControld(t)
		resp := doRequest(t, ts, http.MethodGet, "/v1/secrets", "", nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401; body=%s", resp.StatusCode, raw)
		}
	})

	t.Run("response shape is pinned", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, tok := loginUser(t, st, "alice", "member")
		putSecret(t, st, "PINNED", "v")

		raw := readBody(t, doRequest(t, ts, http.MethodGet, "/v1/secrets", tok, nil, nil))
		assertKeySet(t, raw, "secrets")
		var outer map[string]json.RawMessage
		if err := json.Unmarshal([]byte(raw), &outer); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		var arr []json.RawMessage
		if err := json.Unmarshal(outer["secrets"], &arr); err != nil {
			t.Fatalf("decode secrets array: %v", err)
		}
		assertKeySet(t, string(arr[0]), "name", "created_at", "updated_at")
	})
}

// ---------------------------------------------------------------------------
// DELETE /v1/secrets/{name}
// ---------------------------------------------------------------------------

func TestDeleteSecret(t *testing.T) {
	t.Run("happy path is 204 and the secret is gone", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		readBody(t, doJSON(t, ts, http.MethodPut, "/v1/secrets/DOOMED", adminTok, map[string]any{"value": "v"}, nil))

		resp := doRequest(t, ts, http.MethodDelete, "/v1/secrets/DOOMED", adminTok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("status = %d, want 204; body=%s", resp.StatusCode, raw)
		}
		if _, _, err := st.GetSecret(context.Background(), "DOOMED"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("GetSecret after delete: err = %v, want ErrNotFound", err)
		}
	})

	t.Run("unknown name is 404 not_found", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		resp := doRequest(t, ts, http.MethodDelete, "/v1/secrets/NEVER_EXISTED", adminTok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("status = %d, want 404; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "not_found" {
			t.Errorf("code = %q, want not_found", e.Error.Code)
		}
	})

	t.Run("invalid name is 400 invalid_request", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		resp := doRequest(t, ts, http.MethodDelete, "/v1/secrets/bad-name", adminTok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "invalid_request" {
			t.Errorf("code = %q, want invalid_request", e.Error.Code)
		}
	})

	t.Run("a member is 403 forbidden and the secret survives", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		_, memberTok := loginUser(t, st, "alice", "member")
		readBody(t, doJSON(t, ts, http.MethodPut, "/v1/secrets/SURVIVOR", adminTok, map[string]any{"value": "v"}, nil))

		resp := doRequest(t, ts, http.MethodDelete, "/v1/secrets/SURVIVOR", memberTok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("status = %d, want 403; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "forbidden" {
			t.Errorf("code = %q, want forbidden", e.Error.Code)
		}
		if got := getSecretValue(t, st, "SURVIVOR"); got != "v" {
			t.Fatalf("secret value after a rejected delete = %q, want v", got)
		}
	})

	t.Run("no token is 401 unauthenticated", func(t *testing.T) {
		_, _, ts := newTestControld(t)
		resp := doRequest(t, ts, http.MethodDelete, "/v1/secrets/ANON", "", nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401; body=%s", resp.StatusCode, raw)
		}
	})
}

// ---------------------------------------------------------------------------
// GET /v1/runners
// ---------------------------------------------------------------------------

func TestListRunners(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		startFakeRunner(t, ts, runnerScript{Name: "vm1", Used: 1, Total: 4})
		waitConnected(t, s, "vm1")
		_, tok := loginUser(t, st, "alice", "member")

		eventually(t, 3*time.Second, func() error {
			resp := doRequest(t, ts, http.MethodGet, "/v1/runners", tok, nil, nil)
			raw := readBody(t, resp)
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("status = %d; body=%s", resp.StatusCode, raw)
			}
			var body runnersEnvelope
			if err := json.Unmarshal([]byte(raw), &body); err != nil {
				return err
			}
			if len(body.Runners) != 1 {
				return fmt.Errorf("runners = %+v, want 1", body.Runners)
			}
			r := body.Runners[0]
			if r.Name != "vm1" || !r.Connected || r.CapacityUsed != 1 || r.CapacityTotal != 4 {
				return fmt.Errorf("runner = %+v, want {vm1 connected used:1 total:4}", r)
			}
			return nil
		})
	})

	t.Run("requires auth", func(t *testing.T) {
		_, _, ts := newTestControld(t)
		resp := doRequest(t, ts, http.MethodGet, "/v1/runners", "", nil, nil)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", resp.StatusCode)
		}
	})

	t.Run("response shape is pinned", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4})
		waitConnected(t, s, "vm1")
		_, tok := loginUser(t, st, "alice", "member")

		var raw string
		eventually(t, 3*time.Second, func() error {
			resp := doRequest(t, ts, http.MethodGet, "/v1/runners", tok, nil, nil)
			raw = readBody(t, resp)
			var body runnersEnvelope
			if err := json.Unmarshal([]byte(raw), &body); err != nil {
				return err
			}
			if len(body.Runners) != 1 {
				return fmt.Errorf("not populated yet")
			}
			return nil
		})
		assertKeySet(t, raw, "runners")
		var outer map[string]json.RawMessage
		if err := json.Unmarshal([]byte(raw), &outer); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		var arr []json.RawMessage
		if err := json.Unmarshal(outer["runners"], &arr); err != nil {
			t.Fatalf("decode runners array: %v", err)
		}
		assertKeySet(t, string(arr[0]), "name", "connected", "capacity_used", "capacity_total", "last_seen_at")
	})
}

// ---------------------------------------------------------------------------
// GET /healthz
// ---------------------------------------------------------------------------

func TestHealthz(t *testing.T) {
	_, _, ts := newTestControld(t)
	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	raw := readBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", resp.StatusCode, raw)
	}
	if raw != "ok" {
		t.Fatalf("body = %q, want %q", raw, "ok")
	}
}

// ---------------------------------------------------------------------------
// middleware: request id, nosniff, no-store on GET
// ---------------------------------------------------------------------------

func TestMiddleware(t *testing.T) {
	t.Run("generates a request id when absent", func(t *testing.T) {
		_, _, ts := newTestControld(t)
		resp, err := http.Get(ts.URL + "/healthz")
		if err != nil {
			t.Fatalf("GET /healthz: %v", err)
		}
		defer resp.Body.Close()
		id := resp.Header.Get("X-Request-Id")
		if id == "" {
			t.Fatal("X-Request-Id not set")
		}
		if len(id) != 16 {
			t.Errorf("generated request id = %q (%d chars), want 16 hex chars", id, len(id))
		}
	})

	t.Run("echoes a caller-supplied request id", func(t *testing.T) {
		_, _, ts := newTestControld(t)
		req, err := http.NewRequest(http.MethodGet, ts.URL+"/healthz", nil)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		req.Header.Set("X-Request-Id", "my-trace-id")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET /healthz: %v", err)
		}
		defer resp.Body.Close()
		if got := resp.Header.Get("X-Request-Id"); got != "my-trace-id" {
			t.Errorf("X-Request-Id = %q, want my-trace-id", got)
		}
	})

	t.Run("an over-long request id is replaced", func(t *testing.T) {
		_, _, ts := newTestControld(t)
		req, err := http.NewRequest(http.MethodGet, ts.URL+"/healthz", nil)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		over := strings.Repeat("x", 200)
		req.Header.Set("X-Request-Id", over)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET /healthz: %v", err)
		}
		defer resp.Body.Close()
		got := resp.Header.Get("X-Request-Id")
		if got == over {
			t.Error("over-long request id was echoed verbatim, want replaced")
		}
		if len(got) > maxRequestIDLen {
			t.Errorf("replacement request id too long: %d chars", len(got))
		}
	})

	t.Run("nosniff is set on every response", func(t *testing.T) {
		_, _, ts := newTestControld(t)
		resp, err := http.Get(ts.URL + "/healthz")
		if err != nil {
			t.Fatalf("GET /healthz: %v", err)
		}
		defer resp.Body.Close()
		if got := resp.Header.Get("X-Content-Type-Options"); got != "nosniff" {
			t.Errorf("X-Content-Type-Options = %q, want nosniff", got)
		}
	})

	t.Run("no-store is set on GET", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, tok := loginUser(t, st, "alice", "member")
		resp := doRequest(t, ts, http.MethodGet, "/v1/sessions", tok, nil, nil)
		defer resp.Body.Close()
		if got := resp.Header.Get("Cache-Control"); got != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", got)
		}
	})
}

// ---------------------------------------------------------------------------
// write-ahead durability (mandated: TestCreateDurableBeforeDispatch)
// ---------------------------------------------------------------------------

// commitTimingStore wraps a Store and records the wall-clock instant each
// CreateSession call completes — the "commit" half of the ordering
// TestCreateDurableBeforeDispatch pins against a fake runner's dispatch
// arrival time.
type commitTimingStore struct {
	Store
	mu        sync.Mutex
	committed map[string]time.Time
}

func newCommitTimingStore(st Store) *commitTimingStore {
	return &commitTimingStore{Store: st, committed: map[string]time.Time{}}
}

func (c *commitTimingStore) CreateSession(ctx context.Context, s Session) (Session, error) {
	out, err := c.Store.CreateSession(ctx, s)
	if err == nil {
		c.mu.Lock()
		c.committed[out.ID] = time.Now()
		c.mu.Unlock()
	}
	return out, err
}

func (c *commitTimingStore) commitTime(id string) (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	tm, ok := c.committed[id]
	return tm, ok
}

func TestCreateDurableBeforeDispatch(t *testing.T) {
	t.Run("commit strictly precedes dispatch", func(t *testing.T) {
		cst := newCommitTimingStore(NewMemStore())
		s, ts := newTestControldOver(t, cst)
		startRun(t, s)

		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4})
		waitConnected(t, s, "vm1")

		_, tok := loginUser(t, cst, "alice", "member")

		resp := doJSON(t, ts, http.MethodPost, "/v1/sessions", tok, map[string]any{"name": "durable1", "image": "img:latest"}, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("status = %d, want 202; body=%s", resp.StatusCode, raw)
		}
		var body sessionEnvelope
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		id := body.Session.ID

		commitAt, ok := cst.commitTime(id)
		if !ok {
			t.Fatalf("CreateSession never recorded a commit time for %s", id)
		}

		cmd := f.nextCmd(t)
		dispatchAt := time.Now()
		if cmd.Type != "create" || cmd.Session != id {
			t.Fatalf("got %+v, want create of %s", cmd, id)
		}

		if !commitAt.Before(dispatchAt) {
			t.Fatalf("commit at %s did not strictly precede dispatch at %s", commitAt, dispatchAt)
		}
	})

	t.Run("a runner that never answers still leaves a queued or creating row", func(t *testing.T) {
		cst := newCommitTimingStore(NewMemStore())
		s, ts := newTestControldOver(t, cst, func(c *Config) { c.OpTimeout = 150 * time.Millisecond })
		startRun(t, s)

		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4})
		waitConnected(t, s, "vm1")

		_, tok := loginUser(t, cst, "alice", "member")

		resp := doJSON(t, ts, http.MethodPost, "/v1/sessions", tok, map[string]any{"name": "durable2", "image": "img:latest"}, nil)
		raw := readBody(t, resp)
		var body sessionEnvelope
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		id := body.Session.ID

		cmd := f.nextCmd(t) // reaches the runner; never answered
		if cmd.Type != "create" || cmd.Session != id {
			t.Fatalf("got %+v, want create of %s", cmd, id)
		}

		got, err := cst.GetSession(context.Background(), id)
		if err != nil {
			t.Fatalf("GetSession(%s): %v (row was lost)", id, err)
		}
		if got.State != StateCreating && got.State != StateQueued {
			t.Fatalf("state = %q, want creating or queued (never lost)", got.State)
		}
	})
}

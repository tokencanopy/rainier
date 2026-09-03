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

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
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

// Handler route vocabulary — the /v0/ cut's contract pin
// ---------------------------------------------------------------------------

// TestHandlerRouteShape pins Handler()'s route table to the literal /v0/
// surface. Every claimed route must answer something other than 404 for its
// exact method+path (an unauthenticated 400/401 is fine; 404 is the only
// answer that means "no route claims this path"), and the representative old
// routes must 404: the clean pre-GA cut leaves no compatibility alias.
func TestHandlerRouteShape(t *testing.T) {
	_, _, ts := newTestControld(t)

	claimed := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/v0/runners/connect"},
		{http.MethodGet, "/v0/runners/attach-back"},
		{http.MethodPost, "/v0/auth/github"},
		{http.MethodGet, "/v0/me"},
		{http.MethodPost, "/v0/sessions"},
		{http.MethodGet, "/v0/sessions"},
		{http.MethodGet, "/v0/sessions/sess_test"},
		{http.MethodDelete, "/v0/sessions/sess_test"},
		{http.MethodPost, "/v0/sessions/sess_test/suspend"},
		{http.MethodPost, "/v0/sessions/sess_test/resume"},
		{http.MethodPost, "/v0/sessions/sess_test/snapshot"},
		{http.MethodGet, "/v0/sessions/sess_test/attach"},
		{http.MethodGet, "/v0/sessions/sess_test/diff"},
		{http.MethodPost, "/v0/sessions/sess_test/files"},
		{http.MethodGet, "/v0/sessions/sess_test/files"},
		{http.MethodGet, "/v0/runners"},
		{http.MethodPut, "/v0/secrets/name_test"},
		{http.MethodGet, "/v0/secrets"},
		{http.MethodDelete, "/v0/secrets/name_test"},
		{http.MethodGet, "/v0/credentials"},
		{http.MethodPost, "/v0/environments"},
		{http.MethodGet, "/v0/environments"},
		{http.MethodGet, "/v0/environments/env_test"},
		{http.MethodPatch, "/v0/environments/env_test"},
		{http.MethodDelete, "/v0/environments/env_test"},
		{http.MethodGet, "/healthz"},
	}

	for _, r := range claimed {
		req, err := http.NewRequest(r.method, ts.URL+r.path, nil)
		if err != nil {
			t.Fatalf("%s %s: %v", r.method, r.path, err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", r.method, r.path, err)
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound {
			t.Errorf("%s %s: 404 — route not registered", r.method, r.path)
		}
	}

	// The retired prefix is assembled from parts so this file never spells out
	// the retired version path literally, keeping the repository-wide
	// retired-path search clean while the assertion still names exactly the
	// routes that must not exist after the clean cut.
	old := "/v" + "1"
	for _, path := range []string{
		old + "/me",
		old + "/sessions",
		old + "/runners/connect",
		old + "/sessions/sess_test/attach",
	} {
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s: status = %d, want 404 (no compatibility alias)", path, resp.StatusCode)
		}
	}
}

// ---------------------------------------------------------------------------
// POST /v0/sessions
// ---------------------------------------------------------------------------

func TestCreateSession(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		u, tok := loginUser(t, st, "alice", "member")

		resp := doJSON(t, ts, http.MethodPost, "/v0/sessions", tok,
			map[string]any{"name": "dev1", "image": "ubuntu:latest", "cmd": []string{"bash"}, "egress_allow": []string{"github.com"}}, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("status = %d, want 202; body=%s", resp.StatusCode, raw)
		}
		if loc := resp.Header.Get("Location"); !strings.HasPrefix(loc, "/v0/sessions/sess_") {
			t.Errorf("Location = %q, want /v0/sessions/sess_...", loc)
		}
		var body sessionEnvelope
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		if body.Session.OwnerID != u.ID {
			t.Errorf("owner_id = %q, want %q", body.Session.OwnerID, u.ID)
		}
		if body.Session.State != string(control.StateQueued) {
			t.Errorf("state = %q, want queued", body.Session.State)
		}
		if body.Session.Name != "dev1" {
			t.Errorf("name = %q, want dev1", body.Session.Name)
		}

		got := getSession(t, st, body.Session.ID)
		if got.State != control.StateQueued {
			t.Errorf("stored state = %q, want queued", got.State)
		}
	})

	t.Run("unknown field is 400 invalid_request", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, tok := loginUser(t, st, "alice", "member")
		resp := doRaw(t, ts, http.MethodPost, "/v0/sessions", tok, `{"name":"x","bogus":true}`)
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
		resp := doRaw(t, ts, http.MethodPost, "/v0/sessions", tok, huge)
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
		readBody(t, doJSON(t, ts, http.MethodPost, "/v0/sessions", tok, map[string]any{"name": "dup"}, nil))

		resp := doJSON(t, ts, http.MethodPost, "/v0/sessions", tok, map[string]any{"name": "dup"}, nil)
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

		firstRaw := readBody(t, doJSON(t, ts, http.MethodPost, "/v0/sessions", tok, map[string]any{"name": "once"}, hdr))
		var firstBody sessionEnvelope
		if err := json.Unmarshal([]byte(firstRaw), &firstBody); err != nil {
			t.Fatalf("decode first: %v; body=%s", err, firstRaw)
		}

		second := doJSON(t, ts, http.MethodPost, "/v0/sessions", tok, map[string]any{"name": "once"}, hdr)
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

		rows, _, err := st.Sessions().ListSessions(context.Background(), installWorkspace,
			control.SessionQuery{IncludeTerminal: true, Limit: 100})
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
		seedSession(t, st, control.Session{ID: "sess_nilarr", CreatorID: control.ActorID(u.ID), State: control.StateQueued})

		resp := doRequest(t, ts, http.MethodGet, "/v0/sessions/sess_nilarr", tok, nil, nil)
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
		resp := doJSON(t, ts, http.MethodPost, "/v0/sessions", tok, map[string]any{"name": "shape"}, nil)
		raw := readBody(t, resp)
		assertKeySet(t, raw, "session")
		var outer map[string]json.RawMessage
		if err := json.Unmarshal([]byte(raw), &outer); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		assertKeySet(t, string(outer["session"]),
			"id", "owner_id", "name", "image", "cmd", "egress_allow", "state", "runner",
			"reachable", "error", "environment", "queue_reason", "child_exit_code",
			"created_at", "updated_at", "last_event_at")
		// child_exit_code is nullable and present on every session, never
		// omitted: "the agent has not exited" and "the field wasn't rendered"
		// have to be the same visible answer (null), or a client cannot tell
		// an old controld from a running agent.
		var view map[string]json.RawMessage
		if err := json.Unmarshal(outer["session"], &view); err != nil {
			t.Fatalf("decode session: %v", err)
		}
		if got := string(view["child_exit_code"]); got != "null" {
			t.Fatalf("child_exit_code on a fresh session = %s, want null", got)
		}
	})
}

// ---------------------------------------------------------------------------
// POST /v0/sessions with "environment" — the resolution rules (design §4.3,
// §4.5). The evidence for every rule is the runner.Spec controld actually
// dispatches, captured off a fake runner: that message IS the contract, and a
// resolution that only looks right in the store would still start the wrong
// container.
// ---------------------------------------------------------------------------

// createWithEnv POSTs a create body and returns the decoded session, failing
// the test unless it was accepted.
func createWithEnv(t *testing.T, ts *httptest.Server, tok string, body map[string]any) sessionView {
	t.Helper()
	resp := doJSON(t, ts, http.MethodPost, "/v0/sessions", tok, body, nil)
	raw := readBody(t, resp)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /v0/sessions status = %d, want 202; body=%s", resp.StatusCode, raw)
	}
	var env sessionEnvelope
	if err := json.Unmarshal([]byte(raw), &env); err != nil {
		t.Fatalf("decode: %v; body=%s", err, raw)
	}
	return env.Session
}

// countSessions returns how many session rows st holds, terminal included —
// the "nothing was written" assertion the pre-insert rejection paths need.
func countSessions(t *testing.T, st MemStore) int {
	t.Helper()
	rows, _, err := st.Sessions().ListSessions(context.Background(), installWorkspace, control.SessionQuery{IncludeTerminal: true})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	return len(rows)
}

func TestCreateSessionResolvesEnvironment(t *testing.T) {
	// oneRunnerFleet is the common fixture: a controld with its scheduler
	// running and a single connected runner to receive the create.
	oneRunnerFleet := func(t *testing.T) (MemStore, *httptest.Server, *fakeRunner, string) {
		t.Helper()
		s, st, ts := newTestControld(t)
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		startRun(t, s)
		_, tok := loginUser(t, st, "alice", "member")
		return st, ts, f, tok
	}

	t.Run("cache miss dispatches the environment's image, setup and secrets", func(t *testing.T) {
		st, ts, f, tok := oneRunnerFleet(t)
		putSecretValue(t, st, "GH_TOKEN", "ghp_resolved_value")
		env := seedEnv(t, st, control.Environment{Name: "dev", Image: "env-img:1", Setup: "apt-get install -y jq",
			EgressAllow: []string{"api.github.com"}, SecretRefs: []string{"GH_TOKEN"}})

		got := createWithEnv(t, ts, tok, map[string]any{"name": "e1", "environment": "dev"})
		if got.Environment != "dev" {
			t.Errorf("environment = %q, want dev", got.Environment)
		}

		row := getSession(t, st, got.ID)
		if row.EnvironmentID != env.ID {
			t.Errorf("stored environment_id = %q, want %q", row.EnvironmentID, env.ID)
		}
		if row.Spec.Image != "env-img:1" {
			t.Errorf("stored resolved_image = %q, want env-img:1", row.Spec.Image)
		}
		if !slices.Equal(row.Spec.EgressAllow, []string{"api.github.com"}) {
			t.Errorf("stored egress_allow = %v, want the environment's", row.Spec.EgressAllow)
		}

		cmd := nextCreate(t, f)
		if cmd.Session != got.ID || cmd.Spec == nil {
			t.Fatalf("dispatched %+v, want a create of %s with a spec", cmd, got.ID)
		}
		spec := cmd.Spec
		if spec.Image != "env-img:1" {
			t.Errorf("spec.Image = %q, want env-img:1", spec.Image)
		}
		if spec.Setup != "apt-get install -y jq" {
			t.Errorf("spec.Setup = %q, want the environment's setup", spec.Setup)
		}
		if spec.SetupTimeoutSec != defaultSetupTimeoutSec {
			t.Errorf("spec.SetupTimeoutSec = %d, want the %d default", spec.SetupTimeoutSec, defaultSetupTimeoutSec)
		}
		if spec.Env["GH_TOKEN"] != "ghp_resolved_value" {
			t.Errorf("spec.Env = %v, want GH_TOKEN decrypted", spec.Env)
		}
		if !slices.Equal(spec.EgressAllow, []string{"api.github.com"}) {
			t.Errorf("spec.EgressAllow = %v, want the environment's", spec.EgressAllow)
		}
	})

	t.Run("an explicit setup_timeout_sec is dispatched verbatim", func(t *testing.T) {
		st, ts, f, tok := oneRunnerFleet(t)
		seedEnv(t, st, control.Environment{Name: "slow", Image: "env-img:1", Setup: "sleep 1", SetupTimeoutSec: 120})

		createWithEnv(t, ts, tok, map[string]any{"name": "e2", "environment": "slow"})
		if got := nextCreate(t, f).Spec.SetupTimeoutSec; got != 120 {
			t.Fatalf("spec.SetupTimeoutSec = %d, want 120", got)
		}
	})

	t.Run("an environment with no setup dispatches neither setup nor a timeout", func(t *testing.T) {
		st, ts, f, tok := oneRunnerFleet(t)
		seedEnv(t, st, control.Environment{Name: "bare", Image: "env-img:1"})

		createWithEnv(t, ts, tok, map[string]any{"name": "e3", "environment": "bare"})
		spec := nextCreate(t, f).Spec
		if spec.Setup != "" || spec.SetupTimeoutSec != 0 {
			t.Fatalf("spec setup = %q/%d, want both zero", spec.Setup, spec.SetupTimeoutSec)
		}
	})

	t.Run("a current cache dispatches the snapshot with no setup", func(t *testing.T) {
		st, ts, f, tok := oneRunnerFleet(t)
		env := seedEnv(t, st, control.Environment{Name: "cached", Image: "env-img:1", Setup: "echo hi"})
		const ref = "rainier-env:cached-0123456789ab"
		cacheEnvSnapshot(t, st, env, ref, "vm1")

		got := createWithEnv(t, ts, tok, map[string]any{"name": "e4", "environment": "cached"})
		if row := getSession(t, st, got.ID); row.Spec.Image != ref {
			t.Errorf("stored resolved_image = %q, want the snapshot %q", row.Spec.Image, ref)
		}
		spec := nextCreate(t, f).Spec
		if spec.Image != ref {
			t.Errorf("spec.Image = %q, want the snapshot %q", spec.Image, ref)
		}
		if spec.Setup != "" || spec.SetupTimeoutSec != 0 {
			t.Errorf("spec setup = %q/%d, want none — the cached image IS the finished setup",
				spec.Setup, spec.SetupTimeoutSec)
		}
	})

	// D3: a current cache is the image the session runs, whoever holds it and
	// whatever room they have. The cache is a per-runner local image, not a
	// registry, so the affinity that used to be a fallback ("rebuild from the
	// plain image anywhere") became a placement pin instead: the environment
	// carries the capability snapshot:<runner> and the session waits for that
	// runner rather than booting a different image somewhere else.
	//
	// Both halves are asserted: the image the create resolves to, and where
	// the scheduler will (not) put it. The other half of the pin — a create
	// dispatched to the holder even when a roomier runner is standing by — is
	// sched_test.go's TestCacheTiebreakPrefersTheSnapshotHolder.
	t.Run("a cache whose holder has no free slot still resolves to the snapshot", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		joinRunner(t, s, ts, runnerScript{Name: "vm1", Used: 1, Total: 1}) // the holder, full
		f2 := joinRunner(t, s, ts, runnerScript{Name: "vm2", Total: 2})
		startRun(t, s)
		_, tok := loginUser(t, st, "alice", "member")

		const ref = "rainier-env:cached-0123456789ab"
		env := seedEnv(t, st, control.Environment{Name: "cached", Image: "env-img:1", Setup: "echo hi"})
		cacheEnvSnapshot(t, st, env, ref, "vm1")

		got := createWithEnv(t, ts, tok, map[string]any{"name": "e5", "environment": "cached"})
		if row := getSession(t, st, got.ID); row.Spec.Image != ref {
			t.Errorf("stored resolved_image = %q, want the snapshot %q", row.Spec.Image, ref)
		}
		// D3, the placement half: vm2 has two free slots and is never
		// offered the session, because the image it would boot exists only
		// on vm1. The session waits for vm1 instead.
		time.Sleep(150 * time.Millisecond)
		if row := getSession(t, st, got.ID); row.State != control.StateQueued || row.RunnerID != "" {
			t.Fatalf("session = %q on %q, want still queued and unplaced (pinned to the full holder)", row.State, row.RunnerID)
		}
		wantNothingQueued(t, s, f2)
	})

	t.Run("a cache whose holder is disconnected still resolves to the snapshot", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		startRun(t, s)
		_, tok := loginUser(t, st, "alice", "member")

		const ref = "rainier-env:cached-0123456789ab"
		env := seedEnv(t, st, control.Environment{Name: "cached", Image: "env-img:1", Setup: "echo hi"})
		cacheEnvSnapshot(t, st, env, ref, "vm-gone")

		got := createWithEnv(t, ts, tok, map[string]any{"name": "e6", "environment": "cached"})
		if row := getSession(t, st, got.ID); row.Spec.Image != ref {
			t.Errorf("stored resolved_image = %q, want the snapshot %q", row.Spec.Image, ref)
		}
		// D3 again, with the holder absent rather than full: no connected
		// runner advertises snapshot:vm-gone, so nothing is placed at all.
		time.Sleep(150 * time.Millisecond)
		if row := getSession(t, st, got.ID); row.State != control.StateQueued || row.RunnerID != "" {
			t.Fatalf("session = %q on %q, want still queued and unplaced (its holder is gone)", row.State, row.RunnerID)
		}
		wantNothingQueued(t, s, f)
	})

	t.Run("a snapshot built from superseded setup is not used", func(t *testing.T) {
		st, ts, f, tok := oneRunnerFleet(t)
		env := seedEnv(t, st, control.Environment{Name: "stale", Image: "env-img:1", Setup: "echo old"})
		cacheEnvSnapshot(t, st, env, "rainier-env:stale-0123456789ab", "vm1")

		env.Setup = "echo new"
		env.SetupHash = SetupHash(env.Image, env.Setup)
		if _, err := st.Environments().UpdateEnvironment(context.Background(), installWorkspace, env); err != nil {
			t.Fatalf("UpdateEnvironment: %v", err)
		}

		createWithEnv(t, ts, tok, map[string]any{"name": "e7", "environment": "stale"})
		spec := nextCreate(t, f).Spec
		if spec.Image != "env-img:1" || spec.Setup != "echo new" {
			t.Fatalf("spec = %+v, want the plain image rebuilt with the new setup", spec)
		}
	})

	t.Run("a session image overrides even a current cache", func(t *testing.T) {
		st, ts, f, tok := oneRunnerFleet(t)
		env := seedEnv(t, st, control.Environment{Name: "cached", Image: "env-img:1", Setup: "echo hi"})
		cacheEnvSnapshot(t, st, env, "rainier-env:cached-0123456789ab", "vm1")

		got := createWithEnv(t, ts, tok, map[string]any{"name": "e8", "environment": "cached", "image": "custom:9"})
		if row := getSession(t, st, got.ID); row.Spec.Image != "custom:9" {
			t.Errorf("stored resolved_image = %q, want custom:9", row.Spec.Image)
		}
		spec := nextCreate(t, f).Spec
		if spec.Image != "custom:9" {
			t.Errorf("spec.Image = %q, want custom:9", spec.Image)
		}
		// The override is not the snapshot, so the environment's setup has not
		// been baked into it — it has to run.
		if spec.Setup != "echo hi" {
			t.Errorf("spec.Setup = %q, want the environment's setup", spec.Setup)
		}
	})

	// A session's egress_allow extends the environment's list rather than
	// replacing it (control.PortableSpec): the environment's egress is what it
	// needs to work, and a session adds hosts to it.
	t.Run("a session egress_allow extends the environment's", func(t *testing.T) {
		st, ts, f, tok := oneRunnerFleet(t)
		seedEnv(t, st, control.Environment{Name: "dev", Image: "env-img:1", EgressAllow: []string{"api.github.com"}})

		createWithEnv(t, ts, tok, map[string]any{"name": "e9", "environment": "dev", "egress_allow": []string{"pypi.org"}})
		if got := nextCreate(t, f).Spec.EgressAllow; !slices.Equal(got, []string{"api.github.com", "pypi.org"}) {
			t.Fatalf("spec.EgressAllow = %v, want the environment's list extended by the session's", got)
		}
	})

	// An environment carries no command; the session's cmd is how a session
	// from one says what to run (control.PortableSpec).
	t.Run("a session cmd is the environment session's command", func(t *testing.T) {
		st, ts, f, tok := oneRunnerFleet(t)
		seedEnv(t, st, control.Environment{Name: "dev", Image: "env-img:1"})

		createWithEnv(t, ts, tok, map[string]any{"name": "e9b", "environment": "dev", "cmd": []string{"claude"}})
		if got := nextCreate(t, f).Spec.Cmd; !slices.Equal(got, []string{"claude"}) {
			t.Fatalf("spec.Cmd = %v, want the session's [claude]", got)
		}
	})

	t.Run("the environment may be named by id", func(t *testing.T) {
		st, ts, _, tok := oneRunnerFleet(t)
		env := seedEnv(t, st, control.Environment{Name: "dev", Image: "env-img:1"})

		got := createWithEnv(t, ts, tok, map[string]any{"name": "e10", "environment": env.ID})
		if got.Environment != "dev" {
			t.Fatalf("environment = %q, want the name dev", got.Environment)
		}
		if row := getSession(t, st, got.ID); row.EnvironmentID != env.ID {
			t.Fatalf("stored environment_id = %q, want %q", row.EnvironmentID, env.ID)
		}
	})

	t.Run("an unknown environment is 400 naming it, and nothing is stored", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, tok := loginUser(t, st, "alice", "member")

		resp := doJSON(t, ts, http.MethodPost, "/v0/sessions", tok,
			map[string]any{"name": "nope", "environment": "ghost"}, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body=%s", resp.StatusCode, raw)
		}
		e := decodeErrBody(t, raw)
		if e.Error.Code != "invalid_request" {
			t.Errorf("code = %q, want invalid_request", e.Error.Code)
		}
		if !strings.Contains(e.Error.Message, "ghost") {
			t.Errorf("message = %q, want it to name the environment", e.Error.Message)
		}
		if n := countSessions(t, st); n != 0 {
			t.Fatalf("stored %d sessions, want none", n)
		}
	})

	// An environment may reference a secret that has since been deleted. The
	// create fails loudly, naming the secret — and it fails BEFORE the row
	// exists, so there is no half-built session to explain afterwards.
	t.Run("a deleted secret_ref is 409 before the row is created", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, tok := loginUser(t, st, "alice", "member")
		seedEnv(t, st, control.Environment{Name: "dev", Image: "env-img:1", SecretRefs: []string{"GH_TOKEN"}})

		resp := doJSON(t, ts, http.MethodPost, "/v0/sessions", tok,
			map[string]any{"name": "orphan", "environment": "dev"}, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("status = %d, want 409; body=%s", resp.StatusCode, raw)
		}
		e := decodeErrBody(t, raw)
		if e.Error.Code != "conflict" {
			t.Errorf("code = %q, want conflict", e.Error.Code)
		}
		if !strings.Contains(e.Error.Message, "GH_TOKEN") {
			t.Errorf("message = %q, want it to name the missing secret", e.Error.Message)
		}
		if n := countSessions(t, st); n != 0 {
			t.Fatalf("stored %d sessions, want none — the create must fail before the insert", n)
		}
	})

	t.Run("a scratch create is untouched by any of it", func(t *testing.T) {
		st, ts, f, tok := oneRunnerFleet(t)

		got := createWithEnv(t, ts, tok, map[string]any{"name": "scratch", "image": "ubuntu:latest"})
		if got.Environment != "" || got.QueueReason != "" {
			t.Errorf("scratch session = %+v, want no environment and no queue reason", got)
		}
		// There is no resolved-image column any more: a scratch row's Spec
		// carries the caller's own image, because resolution never ran for it.
		if row := getSession(t, st, got.ID); row.EnvironmentID != "" || row.Spec.Image != "ubuntu:latest" {
			t.Errorf("stored row = %+v, want no environment_id and the caller's own image", row)
		}
		spec := nextCreate(t, f).Spec
		if spec.Image != "ubuntu:latest" || spec.Setup != "" || spec.SetupTimeoutSec != 0 || spec.Env != nil {
			t.Fatalf("spec = %+v, want exactly the pre-environments create", spec)
		}
	})
}

// ---------------------------------------------------------------------------
// POST /v0/sessions repo resolution (design §4.3): an environment's github
// connectors, or the session's own `repos`, become the RepoSpecs the create
// dispatches — plus the git identity they commit as, the three GitHub hosts
// they need to reach, and the credential gate that refuses a clone nobody can
// authenticate. Same evidence as the environment rules above: the runner.Spec
// controld actually sent.
// ---------------------------------------------------------------------------

// repoTestToken is the GitHub token the repo-resolution fixture seals. It is
// distinctive so "the response does not contain it" is a real assertion.
const repoTestToken = "gho_repo_resolution_token"

// githubConnectorJSON renders a github connector object the way a client
// would have written it — the bytes the store keeps verbatim.
func githubConnectorJSON(repo, baseBranch string) json.RawMessage {
	if baseBranch == "" {
		return json.RawMessage(fmt.Sprintf(`{"type":"github","repo":%q}`, repo))
	}
	return json.RawMessage(fmt.Sprintf(`{"type":"github","repo":%q,"base_branch":%q}`, repo, baseBranch))
}

// repoFleet is the repo tests' fixture: a controld with its scheduler
// running, one connected runner to receive the create, and an owner who has
// already logged in to GitHub.
func repoFleet(t *testing.T) (*Server, MemStore, *httptest.Server, *fakeRunner, User, string) {
	t.Helper()
	s, st, ts := newTestControld(t)
	f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
	startRun(t, s)
	u, tok := loginUser(t, st, "alice", "member")
	if err := s.storeGitHubCredential(context.Background(), u.ID, repoTestToken, "repo, read:user"); err != nil {
		t.Fatalf("storeGitHubCredential: %v", err)
	}
	return s, st, ts, f, u, tok
}

// noreplyFor renders the GitHub noreply address commits are attributed to.
func noreplyFor(u User) string {
	return fmt.Sprintf("%d+%s@users.noreply.github.com", u.GitHubID, u.Login)
}

func TestCreateSessionResolvesRepos(t *testing.T) {
	t.Run("a github connector becomes a RepoSpec, with attribution and the git egress", func(t *testing.T) {
		_, st, ts, f, u, tok := repoFleet(t)
		seedEnv(t, st, control.Environment{Name: "dev", Image: "env-img:1",
			EgressAllow: []string{"pypi.org"},
			Connectors: []control.Connector{
				{Type: "github", Raw: githubConnectorJSON("acme/app", "")},
				{Type: "github", Raw: githubConnectorJSON("acme/infra", "develop")},
			}})

		got := createWithEnv(t, ts, tok, map[string]any{"name": "work", "environment": "dev"})
		spec := nextCreate(t, f).Spec

		want := []runner.RepoSpec{
			{Owner: "acme", Name: "app", BaseBranch: "main", SessionBranch: "rainier/work", Dir: "app"},
			{Owner: "acme", Name: "infra", BaseBranch: "develop", SessionBranch: "rainier/work", Dir: "infra"},
		}
		if !slices.Equal(spec.Repos, want) {
			t.Errorf("spec.Repos =\n  %+v\nwant\n  %+v", spec.Repos, want)
		}
		// The human, not the fleet: a commit made in the sandbox has to show
		// up as the person whose credential pushed it.
		if spec.GitAuthorName != u.Login || spec.GitAuthorEmail != noreplyFor(u) {
			t.Errorf("spec attribution = %q/%q, want %q/%q",
				spec.GitAuthorName, spec.GitAuthorEmail, u.Login, noreplyFor(u))
		}
		// The three hosts a clone/push needs, appended at DISPATCH to what the
		// session already had. D6: the row and the view carry only what the
		// caller or the environment declared — the hosts a clone needs are the
		// launch material's knowledge, added where the clone is ordered.
		wantEgress := []string{"pypi.org", "github.com", "codeload.github.com", "objects.githubusercontent.com"}
		if !slices.Equal(spec.EgressAllow, wantEgress) {
			t.Errorf("spec.EgressAllow = %v, want %v", spec.EgressAllow, wantEgress)
		}
		if row := getSession(t, st, got.ID); !slices.Equal(row.Spec.EgressAllow, []string{"pypi.org"}) {
			t.Errorf("stored egress_allow = %v, want the environment's own [pypi.org]", row.Spec.EgressAllow)
		}
	})

	t.Run("the git hosts are appended once, not duplicated", func(t *testing.T) {
		_, st, ts, f, _, tok := repoFleet(t)
		seedEnv(t, st, control.Environment{Name: "dev", Image: "env-img:1",
			EgressAllow: []string{"github.com"},
			Connectors:  []control.Connector{{Type: "github", Raw: githubConnectorJSON("acme/app", "")}}})

		createWithEnv(t, ts, tok, map[string]any{"name": "dup", "environment": "dev"})
		want := []string{"github.com", "codeload.github.com", "objects.githubusercontent.com"}
		if got := nextCreate(t, f).Spec.EgressAllow; !slices.Equal(got, want) {
			t.Errorf("spec.EgressAllow = %v, want %v", got, want)
		}
	})

	t.Run("a session repos override beats the environment's connectors", func(t *testing.T) {
		_, st, ts, f, _, tok := repoFleet(t)
		seedEnv(t, st, control.Environment{Name: "dev", Image: "env-img:1",
			Connectors: []control.Connector{{Type: "github", Raw: githubConnectorJSON("acme/app", "")}}})

		createWithEnv(t, ts, tok, map[string]any{"name": "over", "environment": "dev",
			"repos": []map[string]any{{"repo": "other/svc", "base_branch": "develop"}}})
		want := []runner.RepoSpec{
			{Owner: "other", Name: "svc", BaseBranch: "develop", SessionBranch: "rainier/over", Dir: "svc"},
		}
		if got := nextCreate(t, f).Spec.Repos; !slices.Equal(got, want) {
			t.Errorf("spec.Repos = %+v, want the session's own override %+v", got, want)
		}
	})

	t.Run("an explicit empty repos array clones nothing", func(t *testing.T) {
		_, st, ts, f, _, tok := repoFleet(t)
		seedEnv(t, st, control.Environment{Name: "dev", Image: "env-img:1", EgressAllow: []string{"pypi.org"},
			Connectors: []control.Connector{{Type: "github", Raw: githubConnectorJSON("acme/app", "")}}})

		createWithEnv(t, ts, tok, map[string]any{"name": "bare", "environment": "dev", "repos": []any{}})
		spec := nextCreate(t, f).Spec
		if spec.Repos != nil {
			t.Errorf("spec.Repos = %+v, want none — an explicit [] is scratch semantics", spec.Repos)
		}
		if spec.GitAuthorName != "" || spec.GitAuthorEmail != "" {
			t.Errorf("spec attribution = %q/%q, want none for a session that clones nothing",
				spec.GitAuthorName, spec.GitAuthorEmail)
		}
		if !slices.Equal(spec.EgressAllow, []string{"pypi.org"}) {
			t.Errorf("spec.EgressAllow = %v, want the environment's untouched", spec.EgressAllow)
		}
	})

	t.Run("a session with no name branches off its id", func(t *testing.T) {
		_, st, ts, f, _, tok := repoFleet(t)
		seedEnv(t, st, control.Environment{Name: "dev", Image: "env-img:1",
			Connectors: []control.Connector{{Type: "github", Raw: githubConnectorJSON("acme/app", "")}}})

		got := createWithEnv(t, ts, tok, map[string]any{"environment": "dev"})
		wantBranch := "rainier/" + got.ID[len(got.ID)-12:]
		spec := nextCreate(t, f).Spec
		if len(spec.Repos) != 1 || spec.Repos[0].SessionBranch != wantBranch {
			t.Fatalf("spec.Repos = %+v, want the branch %q", spec.Repos, wantBranch)
		}
	})

	t.Run("two repos of the same name land in different directories", func(t *testing.T) {
		_, st, ts, f, _, tok := repoFleet(t)
		seedEnv(t, st, control.Environment{Name: "dev", Image: "env-img:1",
			Connectors: []control.Connector{
				{Type: "github", Raw: githubConnectorJSON("acme/app", "")},
				{Type: "github", Raw: githubConnectorJSON("other/app", "")},
			}})

		createWithEnv(t, ts, tok, map[string]any{"name": "multi", "environment": "dev"})
		want := []runner.RepoSpec{
			{Owner: "acme", Name: "app", BaseBranch: "main", SessionBranch: "rainier/multi", Dir: "app"},
			{Owner: "other", Name: "app", BaseBranch: "main", SessionBranch: "rainier/multi", Dir: "other__app"},
		}
		if got := nextCreate(t, f).Spec.Repos; !slices.Equal(got, want) {
			t.Errorf("spec.Repos =\n  %+v\nwant\n  %+v", got, want)
		}
	})

	t.Run("a scratch session may name its own repos", func(t *testing.T) {
		_, st, ts, f, u, tok := repoFleet(t)

		got := createWithEnv(t, ts, tok, map[string]any{"name": "solo", "image": "ubuntu:latest",
			"repos": []map[string]any{{"repo": "acme/app"}}})
		spec := nextCreate(t, f).Spec
		want := []runner.RepoSpec{
			{Owner: "acme", Name: "app", BaseBranch: "main", SessionBranch: "rainier/solo", Dir: "app"},
		}
		if !slices.Equal(spec.Repos, want) {
			t.Errorf("spec.Repos = %+v, want %+v", spec.Repos, want)
		}
		if spec.GitAuthorEmail != noreplyFor(u) {
			t.Errorf("spec.GitAuthorEmail = %q, want %q", spec.GitAuthorEmail, noreplyFor(u))
		}
		wantEgress := []string{"github.com", "codeload.github.com", "objects.githubusercontent.com"}
		if !slices.Equal(spec.EgressAllow, wantEgress) {
			t.Errorf("spec.EgressAllow = %v, want %v", spec.EgressAllow, wantEgress)
		}
		// D6: the row carries what the caller declared, which here is nothing.
		if row := getSession(t, st, got.ID); len(row.Spec.EgressAllow) != 0 {
			t.Errorf("stored egress_allow = %v, want none — the caller declared none", row.Spec.EgressAllow)
		}
	})

	// The gate. A session that will clone needs a credential to clone WITH,
	// and the create is where that can still be a fixable answer rather than a
	// container that boots and immediately fails a stage.
	t.Run("cloning with no github credential is 409 naming the login, before the row exists", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		alice, _ := loginUser(t, st, "alice", "member")
		if err := s.storeGitHubCredential(context.Background(), alice.ID, repoTestToken, "repo"); err != nil {
			t.Fatalf("storeGitHubCredential: %v", err)
		}
		_, bobTok := loginUser(t, st, "bob", "member")
		seedEnv(t, st, control.Environment{Name: "dev", Image: "env-img:1",
			Connectors: []control.Connector{{Type: "github", Raw: githubConnectorJSON("acme/app", "")}}})

		resp := doJSON(t, ts, http.MethodPost, "/v0/sessions", bobTok,
			map[string]any{"name": "nocred", "environment": "dev"}, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("status = %d, want 409; body=%s", resp.StatusCode, raw)
		}
		e := decodeErrBody(t, raw)
		if e.Error.Code != "conflict" {
			t.Errorf("code = %q, want conflict", e.Error.Code)
		}
		if !strings.Contains(e.Error.Message, "rainier login") {
			t.Errorf("message = %q, want it to name the action that fixes it", e.Error.Message)
		}
		// Someone else's credential is not this caller's business, and no
		// refusal may carry token material of any kind.
		if strings.Contains(raw, repoTestToken) {
			t.Fatalf("the refusal leaked a stored token: %s", raw)
		}
		if n := countSessions(t, st); n != 0 {
			t.Fatalf("stored %d sessions, want none — the gate must refuse before the insert", n)
		}
	})

	t.Run("a session that clones nothing needs no credential", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		startRun(t, s)
		_, tok := loginUser(t, st, "alice", "member")
		seedEnv(t, st, control.Environment{Name: "dev", Image: "env-img:1",
			Connectors: []control.Connector{{Type: "github", Raw: githubConnectorJSON("acme/app", "")}}})

		createWithEnv(t, ts, tok, map[string]any{"name": "none", "environment": "dev", "repos": []any{}})
		if spec := nextCreate(t, f).Spec; spec.Repos != nil {
			t.Fatalf("spec.Repos = %+v, want none", spec.Repos)
		}
	})

	// A credential a git operation has already seen rejected still passes the
	// gate: it is REFRESHABLE mid-flight (the user runs `rainier login
	// --refresh github` while the session sits there and the next clone
	// works), where a missing credential never becomes present without a
	// create the user has to make again anyway. The clone, not the create, is
	// where a stale credential says so.
	t.Run("a needs_refresh credential still passes the gate", func(t *testing.T) {
		_, st, ts, f, u, tok := repoFleet(t)
		if err := st.SetCredentialStatus(context.Background(), u.ID, "github", CredentialNeedsRefresh); err != nil {
			t.Fatalf("SetCredentialStatus: %v", err)
		}
		seedEnv(t, st, control.Environment{Name: "dev", Image: "env-img:1",
			Connectors: []control.Connector{{Type: "github", Raw: githubConnectorJSON("acme/app", "")}}})

		createWithEnv(t, ts, tok, map[string]any{"name": "stale", "environment": "dev"})
		if got := nextCreate(t, f).Spec.Repos; len(got) != 1 || got[0].Name != "app" {
			t.Fatalf("spec.Repos = %+v, want the clone dispatched anyway", got)
		}
	})

	t.Run("a malformed repos entry is 400, and nothing is stored", func(t *testing.T) {
		_, st, ts, _, _, tok := repoFleet(t)

		for _, tc := range []struct {
			name string
			body string
		}{
			{"not owner/name", `{"name":"x","repos":[{"repo":"nope"}]}`},
			// The session override reaches the same directory the connector's
			// does — expandRepos does not care which of them named the repo.
			{"a repo named ..", `{"name":"x","repos":[{"repo":"acme/.."}]}`},
			{"an owner named .", `{"name":"x","repos":[{"repo":"./app"}]}`},
			{"empty base_branch", `{"name":"x","repos":[{"repo":"acme/app","base_branch":""}]}`},
			{"unknown member", `{"name":"x","repos":[{"repo":"acme/app","branch":"main"}]}`},
			{"missing repo", `{"name":"x","repos":[{}]}`},
		} {
			resp := doRaw(t, ts, http.MethodPost, "/v0/sessions", tok, tc.body)
			raw := readBody(t, resp)
			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("%s: status = %d, want 400; body=%s", tc.name, resp.StatusCode, raw)
			}
			if e := decodeErrBody(t, raw); e.Error.Code != "invalid_request" {
				t.Errorf("%s: code = %q, want invalid_request", tc.name, e.Error.Code)
			}
		}
		if n := countSessions(t, st); n != 0 {
			t.Fatalf("stored %d sessions, want none", n)
		}
	})
}

// ---------------------------------------------------------------------------
// the init hook: dispatched on EVERY create, cache hit included (design §4.4)
// ---------------------------------------------------------------------------

func TestCreateSessionDispatchesInit(t *testing.T) {
	oneRunnerFleet := func(t *testing.T) (MemStore, *httptest.Server, *fakeRunner, string) {
		t.Helper()
		s, st, ts := newTestControld(t)
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		startRun(t, s)
		_, tok := loginUser(t, st, "alice", "member")
		return st, ts, f, tok
	}

	t.Run("an environment's init rides the create with the default bound", func(t *testing.T) {
		st, ts, f, tok := oneRunnerFleet(t)
		seedEnv(t, st, control.Environment{Name: "dev", Image: "env-img:1", Init: "make dev-server &"})

		createWithEnv(t, ts, tok, map[string]any{"name": "i1", "environment": "dev"})
		spec := nextCreate(t, f).Spec
		if spec.Init != "make dev-server &" {
			t.Errorf("spec.Init = %q, want the environment's init", spec.Init)
		}
		if spec.InitTimeoutSec != defaultInitTimeoutSec {
			t.Errorf("spec.InitTimeoutSec = %d, want the %d default", spec.InitTimeoutSec, defaultInitTimeoutSec)
		}
	})

	t.Run("an explicit init_timeout_sec is dispatched verbatim", func(t *testing.T) {
		st, ts, f, tok := oneRunnerFleet(t)
		seedEnv(t, st, control.Environment{Name: "dev", Image: "env-img:1", Init: "sleep 1", InitTimeoutSec: 60})

		createWithEnv(t, ts, tok, map[string]any{"name": "i2", "environment": "dev"})
		if got := nextCreate(t, f).Spec.InitTimeoutSec; got != 60 {
			t.Fatalf("spec.InitTimeoutSec = %d, want 60", got)
		}
	})

	t.Run("an environment with no init dispatches neither the hook nor a bound", func(t *testing.T) {
		st, ts, f, tok := oneRunnerFleet(t)
		seedEnv(t, st, control.Environment{Name: "dev", Image: "env-img:1", InitTimeoutSec: 60})

		createWithEnv(t, ts, tok, map[string]any{"name": "i3", "environment": "dev"})
		spec := nextCreate(t, f).Spec
		if spec.Init != "" || spec.InitTimeoutSec != 0 {
			t.Fatalf("spec init = %q/%d, want both zero", spec.Init, spec.InitTimeoutSec)
		}
	})

	// The cacheable/per-session split: setup built the image and is baked into
	// it, so a cache hit must not run it again — but init runs after the code
	// is in place, on every boot, and a cached image that skipped it would
	// hand the session a workspace with no dev server and no explanation.
	t.Run("a cache hit runs init but not setup", func(t *testing.T) {
		st, ts, f, tok := oneRunnerFleet(t)
		env := seedEnv(t, st, control.Environment{Name: "cached", Image: "env-img:1",
			Setup: "echo hi", Init: "make dev-server &", InitTimeoutSec: 60})
		const ref = "rainier-env:cached-0123456789ab"
		cacheEnvSnapshot(t, st, env, ref, "vm1")

		createWithEnv(t, ts, tok, map[string]any{"name": "i4", "environment": "cached"})
		spec := nextCreate(t, f).Spec
		if spec.Image != ref || spec.Setup != "" {
			t.Errorf("spec = %+v, want the snapshot with no setup", spec)
		}
		if spec.Init != "make dev-server &" || spec.InitTimeoutSec != 60 {
			t.Errorf("spec init = %q/%d, want the hook dispatched on a cache hit too",
				spec.Init, spec.InitTimeoutSec)
		}
	})
}

// ---------------------------------------------------------------------------
// sessionJSON's two derived fields: "environment" and "queue_reason"
// ---------------------------------------------------------------------------

// envReadCountingStore counts GetEnvironment calls, so the list handler's
// per-request cache can be pinned as a fact rather than a comment. It counts
// them where the handler makes them: on the environment repository.
type envReadCountingStore struct {
	MemStore
	mu sync.Mutex
	n  int
}

func (e *envReadCountingStore) Environments() control.EnvironmentRepository {
	return countingEnvironments{EnvironmentRepository: e.MemStore.Environments(), owner: e}
}

type countingEnvironments struct {
	control.EnvironmentRepository
	owner *envReadCountingStore
}

func (c countingEnvironments) GetEnvironment(ctx context.Context, ws control.WorkspaceID, id control.EnvironmentID) (control.Environment, error) {
	c.owner.mu.Lock()
	c.owner.n++
	c.owner.mu.Unlock()
	return c.EnvironmentRepository.GetEnvironment(ctx, ws, id)
}

func (e *envReadCountingStore) reads() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.n
}

func TestSessionEnvironmentAndQueueReason(t *testing.T) {
	t.Run("an environment-backed session renders the environment's name", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		u, tok := loginUser(t, st, "alice", "member")
		env := seedEnv(t, st, control.Environment{Name: "dev", Image: "img:1"})
		seedSession(t, st, control.Session{ID: "sess_env1", CreatorID: control.ActorID(u.ID), State: control.StateRunning, RunnerID: "vm1",
			EnvironmentID: env.ID, Spec: control.PortableSpec{Image: "img:1"}})

		resp := doRequest(t, ts, http.MethodGet, "/v0/sessions/sess_env1", tok, nil, nil)
		raw := readBody(t, resp)
		var body sessionEnvelope
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		if body.Session.Environment != "dev" {
			t.Errorf("environment = %q, want dev", body.Session.Environment)
		}
		if body.Session.QueueReason != "" {
			t.Errorf("queue_reason = %q, want empty (the session is running)", body.Session.QueueReason)
		}
	})

	t.Run("a session whose environment was deleted renders an empty name", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		u, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, control.Session{ID: "sess_orphan", CreatorID: control.ActorID(u.ID), State: control.StateRunning,
			EnvironmentID: "env_deadbeef", Spec: control.PortableSpec{Image: "img:1"}})

		resp := doRequest(t, ts, http.MethodGet, "/v0/sessions/sess_orphan", tok, nil, nil)
		raw := readBody(t, resp)
		var body sessionEnvelope
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		if body.Session.Environment != "" {
			t.Fatalf("environment = %q, want empty for an environment that no longer exists", body.Session.Environment)
		}
	})

	t.Run("a queued session pinned to a runner that is not here says so", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		u, tok := loginUser(t, st, "alice", "member")
		env := seedEnv(t, st, control.Environment{Name: "hardware", Image: "img:1", Requirements: control.Requirements{Capabilities: []string{placementCapabilityPrefix + "rainier-gpu"}}})
		seedSession(t, st, control.Session{ID: "sess_wait", CreatorID: control.ActorID(u.ID), State: control.StateQueued,
			EnvironmentID: env.ID, Spec: control.PortableSpec{Image: "img:1"}})

		resp := doRequest(t, ts, http.MethodGet, "/v0/sessions/sess_wait", tok, nil, nil)
		raw := readBody(t, resp)
		var body sessionEnvelope
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		if body.Session.State != string(control.StateQueued) {
			t.Errorf("state = %q, want queued", body.Session.State)
		}
		if want := "waiting for runner rainier-gpu"; body.Session.QueueReason != want {
			t.Fatalf("queue_reason = %q, want %q", body.Session.QueueReason, want)
		}
	})

	t.Run("a queued session pinned to a runner with room has no reason", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		u, tok := loginUser(t, st, "alice", "member")
		env := seedEnv(t, st, control.Environment{Name: "dev", Image: "img:1", Requirements: control.Requirements{Capabilities: []string{placementCapabilityPrefix + "vm1"}}})
		seedSession(t, st, control.Session{ID: "sess_soon", CreatorID: control.ActorID(u.ID), State: control.StateQueued,
			EnvironmentID: env.ID, Spec: control.PortableSpec{Image: "img:1"}})

		resp := doRequest(t, ts, http.MethodGet, "/v0/sessions/sess_soon", tok, nil, nil)
		raw := readBody(t, resp)
		var body sessionEnvelope
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		if body.Session.QueueReason != "" {
			t.Fatalf("queue_reason = %q, want empty — vm1 is connected with room", body.Session.QueueReason)
		}
	})

	t.Run("a queued session pinned to a full runner says so", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		joinRunner(t, s, ts, runnerScript{Name: "vm1", Used: 2, Total: 2})
		u, tok := loginUser(t, st, "alice", "member")
		env := seedEnv(t, st, control.Environment{Name: "dev", Image: "img:1", Requirements: control.Requirements{Capabilities: []string{placementCapabilityPrefix + "vm1"}}})
		seedSession(t, st, control.Session{ID: "sess_full", CreatorID: control.ActorID(u.ID), State: control.StateQueued,
			EnvironmentID: env.ID, Spec: control.PortableSpec{Image: "img:1"}})

		resp := doRequest(t, ts, http.MethodGet, "/v0/sessions/sess_full", tok, nil, nil)
		raw := readBody(t, resp)
		var body sessionEnvelope
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		if want := "waiting for runner vm1"; body.Session.QueueReason != want {
			t.Fatalf("queue_reason = %q, want %q", body.Session.QueueReason, want)
		}
	})

	t.Run("the list renders both fields, reading each environment once", func(t *testing.T) {
		cst := &envReadCountingStore{MemStore: NewMemStore()}
		_, ts := newTestControldOver(t, cst)
		u, tok := loginUser(t, cst, "alice", "member")
		env := seedEnv(t, cst, control.Environment{Name: "hardware", Image: "img:1", Requirements: control.Requirements{Capabilities: []string{placementCapabilityPrefix + "rainier-gpu"}}})
		for _, id := range []string{"sess_a", "sess_b", "sess_c"} {
			seedSession(t, cst, control.Session{ID: control.SessionID(id), CreatorID: control.ActorID(u.ID), Name: id, State: control.StateQueued,
				EnvironmentID: env.ID, Spec: control.PortableSpec{Image: "img:1"}})
		}
		seedSession(t, cst, control.Session{ID: "sess_scratch", CreatorID: control.ActorID(u.ID), Name: "scratch", State: control.StateQueued})

		before := cst.reads()
		resp := doRequest(t, ts, http.MethodGet, "/v0/sessions", tok, nil, nil)
		raw := readBody(t, resp)
		var body sessionsEnvelope
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		if len(body.Sessions) != 4 {
			t.Fatalf("listed %d sessions, want 4", len(body.Sessions))
		}
		for _, got := range body.Sessions {
			if got.ID == "sess_scratch" {
				if got.Environment != "" || got.QueueReason != "" {
					t.Errorf("scratch row = %+v, want neither field set", got)
				}
				continue
			}
			if got.Environment != "hardware" {
				t.Errorf("%s environment = %q, want hardware", got.ID, got.Environment)
			}
			if want := "waiting for runner rainier-gpu"; got.QueueReason != want {
				t.Errorf("%s queue_reason = %q, want %q", got.ID, got.QueueReason, want)
			}
		}
		if n := cst.reads() - before; n != 1 {
			t.Fatalf("the list read the environment %d times, want 1 (per-request cache)", n)
		}
	})
}

// ---------------------------------------------------------------------------
// GET /v0/sessions
// ---------------------------------------------------------------------------

// spyListStore records the SessionQuery it was last called with, so a test
// can pin the default/cap on Limit without seeding 100+ rows. The query it
// records is the one the session repository is asked, which is the one the
// handler composed.
type spyListStore struct {
	MemStore
	lastQuery control.SessionQuery
}

func (s *spyListStore) Sessions() control.SessionRepository {
	return spyListSessions{SessionRepository: s.MemStore.Sessions(), owner: s}
}

type spyListSessions struct {
	control.SessionRepository
	owner *spyListStore
}

func (s spyListSessions) ListSessions(ctx context.Context, ws control.WorkspaceID, q control.SessionQuery) ([]control.Session, string, error) {
	s.owner.lastQuery = q
	return s.SessionRepository.ListSessions(ctx, ws, q)
}

func TestListSessions(t *testing.T) {
	t.Run("happy path is team-visible and hides terminal by default", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, tok := loginUser(t, st, "alice", "member")
		other, _ := loginUser(t, st, "bob", "member")

		seedSession(t, st, control.Session{ID: "sess_l1", CreatorID: control.ActorID(owner.ID), State: control.StateQueued, Name: "l1"})
		seedSession(t, st, control.Session{ID: "sess_l2", CreatorID: control.ActorID(other.ID), State: control.StateRunning, Name: "l2"})
		seedSession(t, st, control.Session{ID: "sess_l3", CreatorID: control.ActorID(owner.ID), State: control.StateDestroyed, Name: "l3"})

		resp := doRequest(t, ts, http.MethodGet, "/v0/sessions", tok, nil, nil)
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
		seedSession(t, st, control.Session{ID: "sess_term", CreatorID: control.ActorID(owner.ID), State: control.StateDestroyed, Name: "term"})

		resp := doRequest(t, ts, http.MethodGet, "/v0/sessions?all=true", tok, nil, nil)
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

	t.Run("name filters exactly", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, control.Session{ID: "sess_name_want", CreatorID: control.ActorID(owner.ID), State: control.StateRunning, Name: "box"})
		seedSession(t, st, control.Session{ID: "sess_name_other", CreatorID: control.ActorID(owner.ID), State: control.StateRunning, Name: "box-extra"})

		resp := doRequest(t, ts, http.MethodGet, "/v0/sessions?name=box", tok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200; body=%s", resp.StatusCode, raw)
		}
		var body sessionsEnvelope
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		if len(body.Sessions) != 1 || body.Sessions[0].ID != "sess_name_want" {
			t.Fatalf("sessions = %+v, want only exact name box", body.Sessions)
		}
	})

	t.Run("invalid cursor is 400 invalid_request", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, tok := loginUser(t, st, "alice", "member")
		resp := doRequest(t, ts, http.MethodGet, "/v0/sessions?cursor=not-valid-base64", tok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "invalid_request" {
			t.Errorf("code = %q, want invalid_request", e.Error.Code)
		}
	})

	t.Run("limit defaults to 50 and caps at 100", func(t *testing.T) {
		spy := &spyListStore{MemStore: NewMemStore()}
		_, ts := newTestControldOver(t, spy)
		_, tok := loginUser(t, spy, "alice", "member")

		readBody(t, doRequest(t, ts, http.MethodGet, "/v0/sessions", tok, nil, nil))
		if spy.lastQuery.Limit != defaultListLimit {
			t.Errorf("default limit = %d, want %d", spy.lastQuery.Limit, defaultListLimit)
		}

		readBody(t, doRequest(t, ts, http.MethodGet, "/v0/sessions?limit=1000", tok, nil, nil))
		if spy.lastQuery.Limit != maxListLimit {
			t.Errorf("capped limit = %d, want %d", spy.lastQuery.Limit, maxListLimit)
		}
	})

	t.Run("non-numeric limit is 400 invalid_request", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, tok := loginUser(t, st, "alice", "member")
		resp := doRequest(t, ts, http.MethodGet, "/v0/sessions?limit=banana", tok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body=%s", resp.StatusCode, raw)
		}
	})

	t.Run("response shape is pinned", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, control.Session{ID: "sess_shape_list", CreatorID: control.ActorID(owner.ID), State: control.StateQueued, Name: "shape"})
		resp := doRequest(t, ts, http.MethodGet, "/v0/sessions", tok, nil, nil)
		raw := readBody(t, resp)
		assertKeySet(t, raw, "sessions", "next_cursor")
	})
}

// ---------------------------------------------------------------------------
// GET /v0/sessions/{id}
// ---------------------------------------------------------------------------

func TestGetSession(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, control.Session{ID: "sess_get1", CreatorID: control.ActorID(owner.ID), State: control.StateRunning, Name: "get1", RunnerID: "vm1"})

		resp := doRequest(t, ts, http.MethodGet, "/v0/sessions/sess_get1", tok, nil, nil)
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
		resp := doRequest(t, ts, http.MethodGet, "/v0/sessions/sess_nope", tok, nil, nil)
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
		seedSession(t, st, control.Session{ID: "sess_reach", CreatorID: control.ActorID(owner.ID), State: control.StateRunning, RunnerID: "vm1"})
		// Announce the row present and agreeing so reconcile doesn't sweep
		// it (an announce silent on it would mark it dead).
		startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Sessions: []runner.SessionInfo{{ID: "sess_reach", State: "running"}}})
		waitConnected(t, s, "vm1")

		resp := doRequest(t, ts, http.MethodGet, "/v0/sessions/sess_reach", tok, nil, nil)
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
		seedSession(t, st, control.Session{ID: "sess_shape2", CreatorID: control.ActorID(owner.ID), State: control.StateQueued})
		resp := doRequest(t, ts, http.MethodGet, "/v0/sessions/sess_shape2", tok, nil, nil)
		raw := readBody(t, resp)
		assertKeySet(t, raw, "session")
	})
}

// ---------------------------------------------------------------------------
// DELETE /v0/sessions/{id}
// ---------------------------------------------------------------------------

func TestDeleteSession(t *testing.T) {
	t.Run("queued cancels without dispatch and wakes the scheduler", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, control.Session{ID: "sess_del_q", CreatorID: control.ActorID(owner.ID), State: control.StateQueued, Name: "delq"})

		resp := doRequest(t, ts, http.MethodDelete, "/v0/sessions/sess_del_q", tok, nil, nil)
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("status = %d, want 204", resp.StatusCode)
		}
		got := getSession(t, st, "sess_del_q")
		if got.State != control.StateCanceled {
			t.Fatalf("state = %q, want canceled", got.State)
		}
		// The wake itself is the session service's now (controlapp's
		// DeleteSession wakes the row's pool; controlapp/sessions_test.go
		// asserts the "wake:pool_a" record), and controld no longer owns a
		// channel a test could watch — Task 5 deleted schedWake with the
		// scheduler loop it fed.
	})

	t.Run("creating is 409 conflict, no dispatch", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, control.Session{ID: "sess_del_c", CreatorID: control.ActorID(owner.ID), State: control.StateCreating, RunnerID: "vm1"})

		resp := doRequest(t, ts, http.MethodDelete, "/v0/sessions/sess_del_c", tok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("status = %d, want 409; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "conflict" {
			t.Errorf("code = %q, want conflict", e.Error.Code)
		}
		got := getSession(t, st, "sess_del_c")
		if got.State != control.StateCreating {
			t.Fatalf("state = %q, want unchanged (creating)", got.State)
		}
	})

	t.Run("non-owner non-admin is 403 forbidden", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, _ := loginUser(t, st, "alice", "member")
		_, otherTok := loginUser(t, st, "bob", "member")
		seedSession(t, st, control.Session{ID: "sess_del_authz", CreatorID: control.ActorID(owner.ID), State: control.StateQueued})

		resp := doRequest(t, ts, http.MethodDelete, "/v0/sessions/sess_del_authz", otherTok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("status = %d, want 403; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "forbidden" {
			t.Errorf("code = %q, want forbidden", e.Error.Code)
		}
		got := getSession(t, st, "sess_del_authz")
		if got.State != control.StateQueued {
			t.Fatalf("state = %q, want unchanged (queued)", got.State)
		}
	})

	t.Run("admin may delete another owner's session", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, _ := loginUser(t, st, "alice", "member")
		_, adminTok := loginUser(t, st, "root", "admin")
		seedSession(t, st, control.Session{ID: "sess_del_admin", CreatorID: control.ActorID(owner.ID), State: control.StateQueued})

		resp := doRequest(t, ts, http.MethodDelete, "/v0/sessions/sess_del_admin", adminTok, nil, nil)
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("status = %d, want 204", resp.StatusCode)
		}
	})

	t.Run("terminal is idempotent 204", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, control.Session{ID: "sess_del_term", CreatorID: control.ActorID(owner.ID), State: control.StateDestroyed})

		resp := doRequest(t, ts, http.MethodDelete, "/v0/sessions/sess_del_term", tok, nil, nil)
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("status = %d, want 204", resp.StatusCode)
		}
		got := getSession(t, st, "sess_del_term")
		if got.State != control.StateDestroyed {
			t.Fatalf("state = %q, want unchanged (destroyed)", got.State)
		}
	})

	// A failed create is terminal in the database but may still be live on
	// its runner so the user can attach and inspect the failure. Deleting it
	// must therefore tear down the runner-side session, not merely reclaim a
	// workspace from underneath the still-running container.
	t.Run("a failed session still present on its runner is destroyed", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})

		owner, tok := loginUser(t, st, "alice", "member")
		reason := "clone failed"
		seedSession(t, st, control.Session{ID: "sess_del_failed", CreatorID: control.ActorID(owner.ID), State: control.StateFailed,
			RunnerID: "vm1", Error: reason})

		type result struct{ resp *http.Response }
		resc := make(chan result, 1)
		go func() {
			resc <- result{doRequest(t, ts, http.MethodDelete, "/v0/sessions/sess_del_failed", tok, nil, nil)}
		}()

		cmd := f.nextCmd(t)
		if cmd.Type != "destroy" || cmd.Session != "sess_del_failed" {
			t.Fatalf("got %+v, want destroy of sess_del_failed", cmd)
		}
		f.reply(t, cmd, true, "")

		resp := (<-resc).resp
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("status = %d, want 204", resp.StatusCode)
		}
		if got := getSession(t, st, "sess_del_failed"); got.State != control.StateDestroyed || got.Error != reason {
			t.Fatalf("row = %s / %q, want destroyed with the failed diagnosis preserved", got.State, got.Error)
		}

		next := f.nextCmd(t)
		if next.Type != "remove_workspace" || next.Session != "sess_del_failed" {
			t.Fatalf("after the destroy: got %+v, want remove_workspace of sess_del_failed", next)
		}
	})

	// A dead session is where the durability rider's two halves meet. The
	// crash path deliberately KEPT that session's workspace volume, and its
	// container is long gone — so nothing on the runner will ever name that
	// volume again unless this delete does. Without this dispatch,
	// "crash preserves the workspace" would mean "every crash leaks a
	// workspace, forever".
	t.Run("a dead session's rm reclaims the workspace the crash kept", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})

		owner, tok := loginUser(t, st, "alice", "member")
		reason := "runner reported dead"
		seedSession(t, st, control.Session{ID: "sess_del_dead", CreatorID: control.ActorID(owner.ID), State: control.StateDead,
			RunnerID: "vm1", Error: reason})

		resp := doRequest(t, ts, http.MethodDelete, "/v0/sessions/sess_del_dead", tok, nil, nil)
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("status = %d, want 204", resp.StatusCode)
		}
		cmd := f.nextCmd(t)
		if cmd.Type != "remove_workspace" || cmd.Session != "sess_del_dead" {
			t.Fatalf("got %+v, want remove_workspace of sess_del_dead", cmd)
		}
		if cmd.ReqID != 0 {
			t.Fatalf("remove_workspace req_id = %d, want 0 — it is fire-and-forget", cmd.ReqID)
		}
		// The row is terminal and stays exactly as it was: a dead session's
		// diagnosis is what the user came back for.
		if got := getSession(t, st, "sess_del_dead"); got.State != control.StateDead || got.Error != reason {
			t.Fatalf("row = %s / %q, want it unchanged", got.State, got.Error)
		}
	})

	// A terminal row with no runner has nobody to ask; the delete is still a
	// 204 and must not wedge waiting on a dispatch that can never happen.
	t.Run("a terminal session with no runner reclaims nothing", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})
		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, control.Session{ID: "sess_del_unplaced", CreatorID: control.ActorID(owner.ID), State: control.StateFailed})

		resp := doRequest(t, ts, http.MethodDelete, "/v0/sessions/sess_del_unplaced", tok, nil, nil)
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("status = %d, want 204", resp.StatusCode)
		}
		wantNothingQueued(t, s, f)
	})

	t.Run("placed on a disconnected runner marks destroyed directly", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, control.Session{ID: "sess_del_gone", CreatorID: control.ActorID(owner.ID), State: control.StateRunning, RunnerID: "vm-ghost"})

		resp := doRequest(t, ts, http.MethodDelete, "/v0/sessions/sess_del_gone", tok, nil, nil)
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("status = %d, want 204", resp.StatusCode)
		}
		got := getSession(t, st, "sess_del_gone")
		if got.State != control.StateDestroyed {
			t.Fatalf("state = %q, want destroyed", got.State)
		}
	})

	t.Run("disconnected failed delete retries orphan destroy after reconnect", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, tok := loginUser(t, st, "alice", "member")
		id := "sess_del_retry"
		seedSession(t, st, control.Session{ID: control.SessionID(id), CreatorID: control.ActorID(owner.ID), State: control.StateFailed, RunnerID: "vm1"})

		resp := doRequest(t, ts, http.MethodDelete, "/v0/sessions/"+id, tok, nil, nil)
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("status = %d, want 204", resp.StatusCode)
		}
		wantState(t, st, id, control.StateDestroyed)

		// The runner was offline for the DELETE and still has the attachable
		// failed container. Its reconnect makes that container an orphan. A
		// transient driver failure must be retried on this same connection;
		// waiting for another reconnect would leave capacity occupied forever.
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Used: 1, Total: 4,
			Sessions: []runner.SessionInfo{{ID: id, State: "running"}}})
		drainAccept(t, f)
		first := f.nextCmd(t)
		if first.Type != "destroy" || first.Session != id || first.ReqID == 0 {
			t.Fatalf("first command = %+v, want tracked destroy of %s", first, id)
		}
		f.reply(t, first, false, "transient driver failure")

		second := f.nextCmd(t)
		if second.Type != "destroy" || second.Session != id || second.ReqID == 0 {
			t.Fatalf("retry command = %+v, want tracked destroy of %s", second, id)
		}
		if second.ReqID == first.ReqID {
			t.Fatalf("retry req_id = %d, want a fresh id", second.ReqID)
		}
		f.setCapacity(0, 4)
		f.reply(t, second, true, "")
		eventually(t, 3*time.Second, func() error {
			runners, err := st.Fleet().ListRunners(context.Background(), installPool)
			if err != nil {
				return err
			}
			if len(runners) != 1 || runners[0].CapacityUsed != 0 {
				return fmt.Errorf("runners = %+v, want released capacity", runners)
			}
			return nil
		})
	})

	t.Run("placed on a connected runner dispatches destroy", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Sessions: []runner.SessionInfo{{ID: ghostSession, State: "running"}}})
		waitConnected(t, s, "vm1")
		awaitReconciled(t, f)

		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, control.Session{ID: "sess_del_live", CreatorID: control.ActorID(owner.ID), State: control.StateRunning, RunnerID: "vm1"})

		type result struct{ resp *http.Response }
		resc := make(chan result, 1)
		go func() {
			resc <- result{doRequest(t, ts, http.MethodDelete, "/v0/sessions/sess_del_live", tok, nil, nil)}
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
		wantState(t, st, "sess_del_live", control.StateDestroyed)

		// The destroy already took the volume with it; the reclaim goes out
		// anyway, and the runner answers ok for an absent one. Belt and
		// braces on the path that must never leak: the ONE place a workspace
		// is allowed to outlive its session is a crash, and every explicit rm
		// says so out loud rather than trusting the destroy to have done it.
		next := f.nextCmd(t)
		if next.Type != "remove_workspace" || next.Session != "sess_del_live" {
			t.Fatalf("after the destroy: got %+v, want remove_workspace of sess_del_live", next)
		}
	})

	t.Run("runner unreachable is 502 runner_unreachable", func(t *testing.T) {
		s, st, ts := newTestControld(t, func(c *Config) { c.OpTimeout = 150 * time.Millisecond })
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Sessions: []runner.SessionInfo{{ID: ghostSession, State: "running"}}})
		waitConnected(t, s, "vm1")
		awaitReconciled(t, f)

		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, control.Session{ID: "sess_del_timeout", CreatorID: control.ActorID(owner.ID), State: control.StateRunning, RunnerID: "vm1"})

		resp := doRequest(t, ts, http.MethodDelete, "/v0/sessions/sess_del_timeout", tok, nil, nil)
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
			Sessions: []runner.SessionInfo{{ID: ghostSession, State: "running"}}})
		waitConnected(t, s, "vm1")
		awaitReconciled(t, f)

		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, control.Session{ID: "sess_del_fail", CreatorID: control.ActorID(owner.ID), State: control.StateRunning, RunnerID: "vm1"})

		type result struct{ resp *http.Response }
		resc := make(chan result, 1)
		go func() {
			resc <- result{doRequest(t, ts, http.MethodDelete, "/v0/sessions/sess_del_fail", tok, nil, nil)}
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
		if got.State != control.StateRunning {
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
	MemStore
	triggerID   string
	raceToState control.SessionState
	triggered   bool
}

func (r *raceTransitionStore) Sessions() control.SessionRepository {
	return raceTransitionSessions{SessionRepository: r.MemStore.Sessions(), owner: r}
}

type raceTransitionSessions struct {
	control.SessionRepository
	owner *raceTransitionStore
}

func (r raceTransitionSessions) Transition(ctx context.Context, ws control.WorkspaceID, id control.SessionID, from []control.SessionState, to control.SessionState, opts control.TransitionOpts) error {
	if o := r.owner; !o.triggered && string(id) == o.triggerID {
		o.triggered = true
		if err := r.SessionRepository.Transition(ctx, ws, id,
			control.NonTerminal, o.raceToState, control.TransitionOpts{}); err != nil {
			panic(fmt.Sprintf("raceTransitionStore: forcing the race: %v", err))
		}
	}
	return r.SessionRepository.Transition(ctx, ws, id, from, to, opts)
}

// ---------------------------------------------------------------------------
// POST /v0/sessions/{id}/suspend
// ---------------------------------------------------------------------------

func TestSuspendSession(t *testing.T) {
	t.Run("happy path is warm by default", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Sessions: []runner.SessionInfo{{ID: ghostSession, State: "running"}}})
		waitConnected(t, s, "vm1")
		awaitReconciled(t, f)

		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, control.Session{ID: "sess_susp1", CreatorID: control.ActorID(owner.ID), State: control.StateRunning, RunnerID: "vm1"})

		type result struct{ resp *http.Response }
		resc := make(chan result, 1)
		go func() {
			resc <- result{doRequest(t, ts, http.MethodPost, "/v0/sessions/sess_susp1/suspend", tok, nil, nil)}
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
		if body.Session.State != string(control.StateSuspendedWarm) {
			t.Errorf("state = %q, want suspended_warm", body.Session.State)
		}
		wantState(t, st, "sess_susp1", control.StateSuspendedWarm)
	})

	t.Run("warm:false suspends cold", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Sessions: []runner.SessionInfo{{ID: ghostSession, State: "running"}}})
		waitConnected(t, s, "vm1")
		awaitReconciled(t, f)

		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, control.Session{ID: "sess_susp2", CreatorID: control.ActorID(owner.ID), State: control.StateRunning, RunnerID: "vm1"})

		type result struct{ resp *http.Response }
		resc := make(chan result, 1)
		go func() {
			resc <- result{doJSON(t, ts, http.MethodPost, "/v0/sessions/sess_susp2/suspend", tok, map[string]any{"warm": false}, nil)}
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
		if body.Session.State != string(control.StateSuspendedCold) {
			t.Errorf("state = %q, want suspended_cold", body.Session.State)
		}
	})

	t.Run("unknown field in body is 400 invalid_request", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, control.Session{ID: "sess_susp_badbody", CreatorID: control.ActorID(owner.ID), State: control.StateRunning, RunnerID: "vm1"})

		resp := doRaw(t, ts, http.MethodPost, "/v0/sessions/sess_susp_badbody/suspend", tok, `{"warm":true,"bogus":1}`)
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
		seedSession(t, st, control.Session{ID: "sess_susp_bad", CreatorID: control.ActorID(owner.ID), State: control.StateQueued})

		resp := doRequest(t, ts, http.MethodPost, "/v0/sessions/sess_susp_bad/suspend", tok, nil, nil)
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
		seedSession(t, st, control.Session{ID: "sess_susp_authz", CreatorID: control.ActorID(owner.ID), State: control.StateRunning, RunnerID: "vm1"})

		resp := doRequest(t, ts, http.MethodPost, "/v0/sessions/sess_susp_authz/suspend", otherTok, nil, nil)
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
		seedSession(t, st, control.Session{ID: "sess_susp_unreach", CreatorID: control.ActorID(owner.ID), State: control.StateRunning, RunnerID: "vm-nope"})

		resp := doRequest(t, ts, http.MethodPost, "/v0/sessions/sess_susp_unreach/suspend", tok, nil, nil)
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
			Sessions: []runner.SessionInfo{{ID: ghostSession, State: "running"}}})
		waitConnected(t, s, "vm1")
		awaitReconciled(t, f)

		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, control.Session{ID: "sess_susp_shape", CreatorID: control.ActorID(owner.ID), State: control.StateRunning, RunnerID: "vm1"})

		type result struct{ resp *http.Response }
		resc := make(chan result, 1)
		go func() {
			resc <- result{doRequest(t, ts, http.MethodPost, "/v0/sessions/sess_susp_shape/suspend", tok, nil, nil)}
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
		race := &raceTransitionStore{MemStore: NewMemStore(), triggerID: id, raceToState: control.StateDestroyed}
		s, ts := newTestControldOver(t, race)
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Sessions: []runner.SessionInfo{{ID: ghostSession, State: "running"}}})
		waitConnected(t, s, "vm1")
		awaitReconciled(t, f)

		owner, tok := loginUser(t, race, "alice", "member")
		seedSession(t, race, control.Session{ID: control.SessionID(id), CreatorID: control.ActorID(owner.ID), State: control.StateRunning, RunnerID: "vm1"})

		type result struct{ resp *http.Response }
		resc := make(chan result, 1)
		go func() {
			resc <- result{doRequest(t, ts, http.MethodPost, "/v0/sessions/"+id+"/suspend", tok, nil, nil)}
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
		if body.Session.State != string(control.StateDestroyed) {
			t.Fatalf("response state = %q, want destroyed (the real persisted state) — got a fabricated state instead", body.Session.State)
		}
		got := getSession(t, race, id)
		if got.State != control.StateDestroyed {
			t.Fatalf("stored state = %q, want destroyed", got.State)
		}
	})
}

// ---------------------------------------------------------------------------
// POST /v0/sessions/{id}/resume
// ---------------------------------------------------------------------------

func TestResumeSession(t *testing.T) {
	t.Run("warm resume happy path", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Sessions: []runner.SessionInfo{{ID: ghostSession, State: "running"}}})
		waitConnected(t, s, "vm1")
		awaitReconciled(t, f)

		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, control.Session{ID: "sess_res1", CreatorID: control.ActorID(owner.ID), State: control.StateSuspendedWarm, RunnerID: "vm1"})

		type result struct{ resp *http.Response }
		resc := make(chan result, 1)
		go func() {
			resc <- result{doRequest(t, ts, http.MethodPost, "/v0/sessions/sess_res1/resume", tok, nil, nil)}
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
		if body.Session.State != string(control.StateRunning) {
			t.Errorf("state = %q, want running", body.Session.State)
		}
	})

	// D4: a cold resume that no longer fits is one of the two ways a resume
	// is refused, and both reach the handler as the same conflict. The slug
	// narrowed to `conflict` and the sentence stopped naming the runner:
	// recovering either would mean computing the fleet's free capacity a
	// second time, in the one place that must make no placement decision.
	t.Run("cold resume onto a full runner is 409 conflict", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 1, Used: 1,
			Sessions: []runner.SessionInfo{{ID: ghostSession, State: "running"}}})
		waitConnected(t, s, "vm1")
		awaitReconciled(t, f)

		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, control.Session{ID: "sess_res_full", CreatorID: control.ActorID(owner.ID), State: control.StateSuspendedCold, RunnerID: "vm1"})

		resp := doRequest(t, ts, http.MethodPost, "/v0/sessions/sess_res_full/resume", tok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("status = %d, want 409; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "conflict" {
			t.Errorf("code = %q, want conflict", e.Error.Code)
		}
	})

	t.Run("runner disconnected is 502 runner_unreachable", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, control.Session{ID: "sess_res_gone", CreatorID: control.ActorID(owner.ID), State: control.StateSuspendedWarm, RunnerID: "vm-ghost"})

		resp := doRequest(t, ts, http.MethodPost, "/v0/sessions/sess_res_gone/resume", tok, nil, nil)
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
		seedSession(t, st, control.Session{ID: "sess_res_bad", CreatorID: control.ActorID(owner.ID), State: control.StateQueued})

		resp := doRequest(t, ts, http.MethodPost, "/v0/sessions/sess_res_bad/resume", tok, nil, nil)
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
		seedSession(t, st, control.Session{ID: "sess_res_authz", CreatorID: control.ActorID(owner.ID), State: control.StateSuspendedWarm, RunnerID: "vm1"})

		resp := doRequest(t, ts, http.MethodPost, "/v0/sessions/sess_res_authz/resume", otherTok, nil, nil)
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
			Sessions: []runner.SessionInfo{{ID: ghostSession, State: "running"}}})
		waitConnected(t, s, "vm1")
		awaitReconciled(t, f)

		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, control.Session{ID: "sess_res_shape", CreatorID: control.ActorID(owner.ID), State: control.StateSuspendedWarm, RunnerID: "vm1"})

		type result struct{ resp *http.Response }
		resc := make(chan result, 1)
		go func() {
			resc <- result{doRequest(t, ts, http.MethodPost, "/v0/sessions/sess_res_shape/resume", tok, nil, nil)}
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
		race := &raceTransitionStore{MemStore: NewMemStore(), triggerID: id, raceToState: control.StateDestroyed}
		s, ts := newTestControldOver(t, race)
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Sessions: []runner.SessionInfo{{ID: ghostSession, State: "running"}}})
		waitConnected(t, s, "vm1")
		awaitReconciled(t, f)

		owner, tok := loginUser(t, race, "alice", "member")
		seedSession(t, race, control.Session{ID: control.SessionID(id), CreatorID: control.ActorID(owner.ID), State: control.StateSuspendedWarm, RunnerID: "vm1"})

		type result struct{ resp *http.Response }
		resc := make(chan result, 1)
		go func() {
			resc <- result{doRequest(t, ts, http.MethodPost, "/v0/sessions/"+id+"/resume", tok, nil, nil)}
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
		if body.Session.State != string(control.StateDestroyed) {
			t.Fatalf("response state = %q, want destroyed (the real persisted state) — got a fabricated state instead", body.Session.State)
		}
		got := getSession(t, race, id)
		if got.State != control.StateDestroyed {
			t.Fatalf("stored state = %q, want destroyed", got.State)
		}
	})
}

// ---------------------------------------------------------------------------
// POST /v0/sessions/{id}/snapshot
// ---------------------------------------------------------------------------

func TestSnapshotSession(t *testing.T) {
	t.Run("happy path from running", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		f := startFakeRunner(t, ts, runnerScript{Name: "vm1", Total: 4,
			Sessions: []runner.SessionInfo{{ID: ghostSession, State: "running"}}})
		waitConnected(t, s, "vm1")
		awaitReconciled(t, f)

		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, control.Session{ID: "sess_snap1", CreatorID: control.ActorID(owner.ID), State: control.StateRunning, RunnerID: "vm1"})

		type result struct{ resp *http.Response }
		resc := make(chan result, 1)
		go func() {
			resc <- result{doRequest(t, ts, http.MethodPost, "/v0/sessions/sess_snap1/snapshot", tok, nil, nil)}
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
		seedSession(t, st, control.Session{ID: "sess_snap_bad", CreatorID: control.ActorID(owner.ID), State: control.StateQueued})

		resp := doRequest(t, ts, http.MethodPost, "/v0/sessions/sess_snap_bad/snapshot", tok, nil, nil)
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
		seedSession(t, st, control.Session{ID: "sess_snap_authz", CreatorID: control.ActorID(owner.ID), State: control.StateRunning, RunnerID: "vm1"})

		resp := doRequest(t, ts, http.MethodPost, "/v0/sessions/sess_snap_authz/snapshot", otherTok, nil, nil)
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
		seedSession(t, st, control.Session{ID: "sess_snap_unreach", CreatorID: control.ActorID(owner.ID), State: control.StateRunning, RunnerID: "vm-ghost"})

		resp := doRequest(t, ts, http.MethodPost, "/v0/sessions/sess_snap_unreach/snapshot", tok, nil, nil)
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
			Sessions: []runner.SessionInfo{{ID: ghostSession, State: "running"}}})
		waitConnected(t, s, "vm1")
		awaitReconciled(t, f)

		owner, tok := loginUser(t, st, "alice", "member")
		seedSession(t, st, control.Session{ID: "sess_snap_shape", CreatorID: control.ActorID(owner.ID), State: control.StateRunning, RunnerID: "vm1"})

		type result struct{ resp *http.Response }
		resc := make(chan result, 1)
		go func() {
			resc <- result{doRequest(t, ts, http.MethodPost, "/v0/sessions/sess_snap_shape/snapshot", tok, nil, nil)}
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
// PUT /v0/secrets/{name}
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

		resp := doJSON(t, ts, http.MethodPut, "/v0/secrets/GH_TOKEN", adminTok, map[string]any{"value": "ghp_supersecret"}, nil)
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

		readBody(t, doJSON(t, ts, http.MethodPut, "/v0/secrets/API_KEY", adminTok, map[string]any{"value": "first"}, nil))
		resp := doJSON(t, ts, http.MethodPut, "/v0/secrets/API_KEY", adminTok, map[string]any{"value": "second"}, nil)
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
			resp := doJSON(t, ts, http.MethodPut, "/v0/secrets/"+url.PathEscape(name), adminTok, map[string]any{"value": "v"}, nil)
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
		resp := doJSON(t, ts, http.MethodPut, "/v0/secrets/"+name, adminTok, map[string]any{"value": "v"}, nil)
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("status = %d, want 204; body=%s", resp.StatusCode, readBody(t, resp))
		}
		readBody(t, resp)
	})

	t.Run("an empty value is 400 invalid_request", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		resp := doJSON(t, ts, http.MethodPut, "/v0/secrets/EMPTY", adminTok, map[string]any{"value": ""}, nil)
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
		resp := doJSON(t, ts, http.MethodPut, "/v0/secrets/BIG", adminTok,
			map[string]any{"value": strings.Repeat("x", (64<<10)+1)}, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "invalid_request" {
			t.Errorf("code = %q, want invalid_request", e.Error.Code)
		}
		if _, _, err := st.GetSecret(context.Background(), "BIG"); !errors.Is(err, control.ErrNotFound) {
			t.Errorf("an over-cap value was stored anyway (GetSecret err = %v)", err)
		}
	})

	t.Run("a value at exactly 64KB is accepted (the boundary)", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		value := strings.Repeat("x", 64<<10)
		resp := doJSON(t, ts, http.MethodPut, "/v0/secrets/ATCAP", adminTok, map[string]any{"value": value}, nil)
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
		resp := doRaw(t, ts, http.MethodPut, "/v0/secrets/HUGE", adminTok, huge)
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
		resp := doRaw(t, ts, http.MethodPut, "/v0/secrets/UNKNOWN", adminTok, `{"value":"v","bogus":true}`)
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

		resp := doJSON(t, ts, http.MethodPut, "/v0/secrets/MEMBER_TRY", memberTok, map[string]any{"value": "nope"}, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("status = %d, want 403; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "forbidden" {
			t.Errorf("code = %q, want forbidden", e.Error.Code)
		}
		if _, _, err := st.GetSecret(context.Background(), "MEMBER_TRY"); !errors.Is(err, control.ErrNotFound) {
			t.Errorf("a member's rejected PUT stored the secret anyway (err = %v)", err)
		}
	})

	t.Run("no token is 401 unauthenticated", func(t *testing.T) {
		_, _, ts := newTestControld(t)
		resp := doJSON(t, ts, http.MethodPut, "/v0/secrets/ANON", "", map[string]any{"value": "nope"}, nil)
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
			readBody(t, doJSON(t, ts, http.MethodPut, "/v0/secrets/ECHO", adminTok, map[string]any{"value": value}, nil)),
			readBody(t, doJSON(t, ts, http.MethodPut, "/v0/secrets/bad-name", adminTok, map[string]any{"value": value}, nil)),
			readBody(t, doJSON(t, ts, http.MethodPut, "/v0/secrets/ECHO", memberTok, map[string]any{"value": value}, nil)),
			readBody(t, doRequest(t, ts, http.MethodGet, "/v0/secrets", adminTok, nil, nil)),
		}
		for i, b := range bodies {
			if strings.Contains(b, value) {
				t.Errorf("response %d echoed the secret value: %s", i, b)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// GET /v0/secrets
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

		resp := doRequest(t, ts, http.MethodGet, "/v0/secrets", tok, nil, nil)
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
		raw := readBody(t, doRequest(t, ts, http.MethodGet, "/v0/secrets", tok, nil, nil))
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

		raw := readBody(t, doRequest(t, ts, http.MethodGet, "/v0/secrets", tok, nil, nil))
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
		resp := doRequest(t, ts, http.MethodGet, "/v0/secrets", tok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200; body=%s", resp.StatusCode, raw)
		}
	})

	t.Run("no token is 401 unauthenticated", func(t *testing.T) {
		_, _, ts := newTestControld(t)
		resp := doRequest(t, ts, http.MethodGet, "/v0/secrets", "", nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401; body=%s", resp.StatusCode, raw)
		}
	})

	t.Run("response shape is pinned", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, tok := loginUser(t, st, "alice", "member")
		putSecret(t, st, "PINNED", "v")

		raw := readBody(t, doRequest(t, ts, http.MethodGet, "/v0/secrets", tok, nil, nil))
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
// DELETE /v0/secrets/{name}
// ---------------------------------------------------------------------------

func TestDeleteSecret(t *testing.T) {
	t.Run("happy path is 204 and the secret is gone", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		readBody(t, doJSON(t, ts, http.MethodPut, "/v0/secrets/DOOMED", adminTok, map[string]any{"value": "v"}, nil))

		resp := doRequest(t, ts, http.MethodDelete, "/v0/secrets/DOOMED", adminTok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("status = %d, want 204; body=%s", resp.StatusCode, raw)
		}
		if _, _, err := st.GetSecret(context.Background(), "DOOMED"); !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("GetSecret after delete: err = %v, want ErrNotFound", err)
		}
	})

	t.Run("unknown name is 404 not_found", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		resp := doRequest(t, ts, http.MethodDelete, "/v0/secrets/NEVER_EXISTED", adminTok, nil, nil)
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
		resp := doRequest(t, ts, http.MethodDelete, "/v0/secrets/bad-name", adminTok, nil, nil)
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
		readBody(t, doJSON(t, ts, http.MethodPut, "/v0/secrets/SURVIVOR", adminTok, map[string]any{"value": "v"}, nil))

		resp := doRequest(t, ts, http.MethodDelete, "/v0/secrets/SURVIVOR", memberTok, nil, nil)
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
		resp := doRequest(t, ts, http.MethodDelete, "/v0/secrets/ANON", "", nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401; body=%s", resp.StatusCode, raw)
		}
	})
}

// ---------------------------------------------------------------------------
// GET /v0/credentials
// ---------------------------------------------------------------------------

// credentialsListToken is the fake access token every credentials-route test
// seals, so "the wire does not contain it" is a claim about this exact
// string.
const credentialsListToken = "gho_list_route_token"

// TestListCredentials covers the four kinds this house tests every route
// with — the happy read, authZ, the response-shape pin, and the secrets
// discipline (here: no token material on the wire, at all, ever).
func TestListCredentials(t *testing.T) {
	t.Run("lists the caller's own credential", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		u, tok := loginUser(t, st, "alice", "member")
		if err := s.storeGitHubCredential(context.Background(), u.ID, credentialsListToken, "repo, read:user"); err != nil {
			t.Fatalf("storeGitHubCredential: %v", err)
		}

		resp := doJSON(t, ts, http.MethodGet, "/v0/credentials", tok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200; body = %s", resp.StatusCode, raw)
		}
		assertKeySet(t, raw, "credentials")

		var body credentialsEnvelope
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			t.Fatalf("decode: %v; body = %s", err, raw)
		}
		if len(body.Credentials) != 1 {
			t.Fatalf("credentials = %+v, want exactly one", body.Credentials)
		}
		got := body.Credentials[0]
		if got.Provider != "github" || got.Status != CredentialValid || got.Scopes != "repo, read:user" {
			t.Errorf("credential = %+v, want github/valid with the stored scopes", got)
		}
		if got.ObtainedAt == "" || got.LastVerifiedAt == "" || got.LastUsedAt == "" {
			t.Errorf("credential timestamps = %+v, want all three populated", got)
		}

		// Shape pin on the row: a value field must have nowhere to live.
		var rows struct {
			Credentials []map[string]json.RawMessage `json:"credentials"`
		}
		if err := json.Unmarshal([]byte(raw), &rows); err != nil {
			t.Fatalf("decode rows: %v; body = %s", err, raw)
		}
		rowRaw, err := json.Marshal(rows.Credentials[0])
		if err != nil {
			t.Fatalf("re-marshal row: %v", err)
		}
		assertKeySet(t, string(rowRaw), "provider", "status", "scopes", "obtained_at", "last_verified_at", "last_used_at")
	})

	t.Run("no credentials is an empty array, not null", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, tok := loginUser(t, st, "alice", "member")

		resp := doJSON(t, ts, http.MethodGet, "/v0/credentials", tok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200; body = %s", resp.StatusCode, raw)
		}
		if !strings.Contains(raw, `"credentials":[]`) {
			t.Errorf("body = %s, want an empty array (a null breaks every client's range)", raw)
		}
	})

	t.Run("a teammate's credentials are invisible", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		alice, aliceTok := loginUser(t, st, "alice", "member")
		_, bobTok := loginUser(t, st, "bob", "admin")
		if err := s.storeGitHubCredential(context.Background(), alice.ID, credentialsListToken, "repo"); err != nil {
			t.Fatalf("storeGitHubCredential: %v", err)
		}

		// Even an admin sees only their own: a credential is not a team
		// resource the way a secret or an environment is.
		for _, tc := range []struct{ name, token string }{{"bob (admin)", bobTok}} {
			resp := doJSON(t, ts, http.MethodGet, "/v0/credentials", tc.token, nil, nil)
			raw := readBody(t, resp)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("%s: status = %d, want 200; body = %s", tc.name, resp.StatusCode, raw)
			}
			if !strings.Contains(raw, `"credentials":[]`) {
				t.Errorf("%s saw someone else's credentials: %s", tc.name, raw)
			}
		}

		resp := doJSON(t, ts, http.MethodGet, "/v0/credentials", aliceTok, nil, nil)
		if raw := readBody(t, resp); !strings.Contains(raw, `"provider":"github"`) {
			t.Errorf("alice's own listing = %s, want her github credential", raw)
		}
	})

	t.Run("unauthenticated", func(t *testing.T) {
		_, _, ts := newTestControld(t)
		for _, tok := range []string{"", "rnr_" + strings.Repeat("0", 64)} {
			resp := doJSON(t, ts, http.MethodGet, "/v0/credentials", tok, nil, nil)
			raw := readBody(t, resp)
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401; body = %s", resp.StatusCode, raw)
			}
			if e := decodeErrBody(t, raw); e.Error.Code != "unauthenticated" {
				t.Errorf("code = %q, want unauthenticated", e.Error.Code)
			}
		}
	})

	t.Run("the wire never carries token material", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		u, tok := loginUser(t, st, "alice", "member")
		if err := s.storeGitHubCredential(context.Background(), u.ID, credentialsListToken, "repo"); err != nil {
			t.Fatalf("storeGitHubCredential: %v", err)
		}
		// A needs_refresh row is rendered by the same path; check both.
		if err := st.SetCredentialStatus(context.Background(), u.ID, "github", CredentialNeedsRefresh); err != nil {
			t.Fatalf("SetCredentialStatus: %v", err)
		}

		resp := doJSON(t, ts, http.MethodGet, "/v0/credentials", tok, nil, nil)
		raw := readBody(t, resp)
		if strings.Contains(raw, credentialsListToken) {
			t.Fatalf("the listing leaked the token: %s", raw)
		}
		for _, forbidden := range []string{"ciphertext", "nonce", "value", "token"} {
			if strings.Contains(raw, forbidden) {
				t.Errorf("the listing carries a %q key: %s", forbidden, raw)
			}
		}
		if !strings.Contains(raw, CredentialNeedsRefresh) {
			t.Errorf("body = %s, want the needs_refresh status surfaced", raw)
		}
	})
}

// ---------------------------------------------------------------------------
// connector vocabulary (unit): validateConnectors
// ---------------------------------------------------------------------------

// The four connector shapes the v0 vocabulary defines, one canonical example
// each. They are string constants rather than Go values because what this API
// promises about a connector is the object itself: what a client sent is what
// the store keeps and what every later response hands back, with no member
// added, dropped, or rewritten.
const (
	ghConnJSON      = `{"type":"github","repo":"acme/widgets","base_branch":"trunk"}`
	filesConnJSON   = `{"type":"files","paths":["/etc/hosts","notes.md"]}`
	tunnelConnJSON  = `{"type":"tunnel","name":"mav","target_host":"127.0.0.1","target_port":14550}`
	browserConnJSON = `{"type":"browser","tier":"dedicated"}`
)

// connectorArray renders elems as the JSON array "connectors" accepts.
func connectorArray(elems ...string) json.RawMessage {
	return json.RawMessage("[" + strings.Join(elems, ",") + "]")
}

func TestValidateConnectors(t *testing.T) {
	t.Run("absent and empty both mean no connectors", func(t *testing.T) {
		got, err := validateConnectors(nil)
		if err != nil || len(got) != 0 {
			t.Fatalf("validateConnectors(nil) = %+v, %v; want none, nil", got, err)
		}
		got, err = validateConnectors(json.RawMessage(`[]`))
		if err != nil || len(got) != 0 {
			t.Fatalf("validateConnectors([]) = %+v, %v; want none, nil", got, err)
		}
	})

	t.Run("one of each type is accepted, type decoded and bytes kept verbatim", func(t *testing.T) {
		want := []string{ghConnJSON, filesConnJSON, tunnelConnJSON, browserConnJSON}
		wantTypes := []string{"github", "files", "tunnel", "browser"}

		got, err := validateConnectors(connectorArray(want...))
		if err != nil {
			t.Fatalf("validateConnectors: %v", err)
		}
		if len(got) != len(want) {
			t.Fatalf("got %d connectors, want %d", len(got), len(want))
		}
		for i := range got {
			if got[i].Type != wantTypes[i] {
				t.Errorf("connector %d type = %q, want %q", i, got[i].Type, wantTypes[i])
			}
			// Raw is never empty and never re-rendered: the stores diverge on
			// how they persist an empty Raw, so the API must keep that case
			// out of reachable space entirely.
			if string(got[i].Raw) != want[i] {
				t.Errorf("connector %d raw = %s, want %s", i, got[i].Raw, want[i])
			}
		}
	})

	t.Run("a github connector may omit base_branch (it defaults to main)", func(t *testing.T) {
		const in = `{"type":"github","repo":"acme/widgets"}`
		got, err := validateConnectors(connectorArray(in))
		if err != nil {
			t.Fatalf("validateConnectors: %v", err)
		}
		// The default is a decode-time value, not a stored one: the bytes
		// stay exactly as the client wrote them.
		if len(got) != 1 || string(got[0].Raw) != in {
			t.Fatalf("got %+v, want the original bytes kept verbatim", got)
		}
		gh, err := decodeGitHubConnector(json.RawMessage(in))
		if err != nil {
			t.Fatalf("decodeGitHubConnector: %v", err)
		}
		if gh.BaseBranch == nil || *gh.BaseBranch != "main" {
			t.Errorf("base_branch = %v, want the default main filled in", gh.BaseBranch)
		}
	})

	t.Run("a dot-leading repository name is still a repository name", func(t *testing.T) {
		// The path specials are refused, but `.github` is a real and extremely
		// common repository, and a dotted directory under /workspace is still
		// under /workspace. Refusing it would close nothing.
		for _, in := range []string{
			`{"type":"github","repo":"acme/.github"}`,
			`{"type":"github","repo":"acme/dot.name"}`,
			`{"type":"github","repo":"acme/with-dash"}`,
			`{"type":"github","repo":"acme/_under"}`,
		} {
			if _, err := validateConnectors(connectorArray(in)); err != nil {
				t.Errorf("validateConnectors(%s) = %v, want it accepted", in, err)
			}
		}
	})

	t.Run("rejections name what was wrong", func(t *testing.T) {
		cases := []struct {
			name, in, want string
		}{
			{"not an array", `{"type":"browser","tier":"dedicated"}`, "array"},
			{"element is not an object", `["github"]`, "connectors[0]"},
			{"missing type", `[{"repo":"acme/widgets"}]`, "type"},
			{"unknown type", `[{"type":"gitlab","repo":"acme/widgets"}]`, "gitlab"},
			{"unknown type is named even in a later element", `[` + ghConnJSON + `,{"type":"gitlab"}]`, "gitlab"},
			{"unknown field on github", `[{"type":"github","repo":"x/y","extra":1}]`, "extra"},
			{"unknown field on files", `[{"type":"files","paths":["a"],"recursive":true}]`, "recursive"},
			{"unknown field on tunnel", `[{"type":"tunnel","name":"n","target_host":"h","target_port":1,"proto":"tcp"}]`, "proto"},
			{"unknown field on browser", `[{"type":"browser","tier":"dedicated","profile":"x"}]`, "profile"},
			{"repo without an owner", `[{"type":"github","repo":"widgets"}]`, "repo"},
			{"repo with a space", `[{"type":"github","repo":"acme/wid gets"}]`, "repo"},
			{"repo with a path segment too many", `[{"type":"github","repo":"acme/widgets/deep"}]`, "repo"},
			// The name becomes a directory component under /workspace, so the
			// two path specials are refused HERE rather than left to git's
			// accident of declining a non-empty clone destination.
			{"repo named ..", `[{"type":"github","repo":"acme/.."}]`, "repo"},
			{"repo named .", `[{"type":"github","repo":"acme/."}]`, "repo"},
			{"owner named ..", `[{"type":"github","repo":"../widgets"}]`, "repo"},
			{"repo starting with a dash", `[{"type":"github","repo":"acme/-widgets"}]`, "repo"},
			{"owner starting with a dash", `[{"type":"github","repo":"-acme/widgets"}]`, "repo"},
			{"explicitly empty base_branch", `[{"type":"github","repo":"a/b","base_branch":""}]`, "base_branch"},
			{"files with no paths", `[{"type":"files","paths":[]}]`, "paths"},
			{"files with a missing paths key", `[{"type":"files"}]`, "paths"},
			{"files with an empty path", `[{"type":"files","paths":["ok",""]}]`, "paths"},
			{"tunnel without a name", `[{"type":"tunnel","target_host":"h","target_port":22}]`, "name"},
			{"tunnel without a host", `[{"type":"tunnel","name":"n","target_port":22}]`, "target_host"},
			{"tunnel port 0", `[{"type":"tunnel","name":"n","target_host":"h","target_port":0}]`, "target_port"},
			{"tunnel port 65536", `[{"type":"tunnel","name":"n","target_host":"h","target_port":65536}]`, "target_port"},
			{"tunnel port negative", `[{"type":"tunnel","name":"n","target_host":"h","target_port":-1}]`, "target_port"},
			{"browser with an unknown tier", `[{"type":"browser","tier":"daily"}]`, "tier"},
			{"browser with no tier", `[{"type":"browser"}]`, "tier"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				got, err := validateConnectors(json.RawMessage(tc.in))
				if err == nil {
					t.Fatalf("validateConnectors(%s) = %+v, want an error", tc.in, got)
				}
				if !strings.Contains(err.Error(), tc.want) {
					t.Errorf("error = %q, want it to mention %q", err, tc.want)
				}
			})
		}
	})

	t.Run("boundary ports are accepted", func(t *testing.T) {
		for _, port := range []int{1, 65535} {
			in := fmt.Sprintf(`{"type":"tunnel","name":"n","target_host":"h","target_port":%d}`, port)
			if _, err := validateConnectors(connectorArray(in)); err != nil {
				t.Errorf("port %d: %v", port, err)
			}
		}
	})

	t.Run("both browser tiers are accepted", func(t *testing.T) {
		for _, tier := range []string{"dedicated", "extension"} {
			in := fmt.Sprintf(`{"type":"browser","tier":%q}`, tier)
			if _, err := validateConnectors(connectorArray(in)); err != nil {
				t.Errorf("tier %s: %v", tier, err)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// environments: shared test helpers
// ---------------------------------------------------------------------------

// envCreateBody is the minimal valid POST /v0/environments body, with over
// merged in.
func envCreateBody(name string, over map[string]any) map[string]any {
	body := map[string]any{"name": name, "image": "img:1"}
	for k, v := range over {
		body[k] = v
	}
	return body
}

// createEnv POSTs body as tok and fails the test unless it is a 201,
// returning the decoded environment.
func createEnv(t *testing.T, ts *httptest.Server, tok string, body map[string]any) environmentView {
	t.Helper()
	resp := doJSON(t, ts, http.MethodPost, "/v0/environments", tok, body, nil)
	raw := readBody(t, resp)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /v0/environments status = %d, want 201; body=%s", resp.StatusCode, raw)
	}
	var env environmentEnvelope
	if err := json.Unmarshal([]byte(raw), &env); err != nil {
		t.Fatalf("decode: %v; body=%s", err, raw)
	}
	return env.Environment
}

// getEnv GETs ref and fails unless it is a 200, returning the environment.
func getEnv(t *testing.T, ts *httptest.Server, tok, ref string) environmentView {
	t.Helper()
	resp := doRequest(t, ts, http.MethodGet, "/v0/environments/"+ref, tok, nil, nil)
	raw := readBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v0/environments/%s status = %d, want 200; body=%s", ref, resp.StatusCode, raw)
	}
	var env environmentEnvelope
	if err := json.Unmarshal([]byte(raw), &env); err != nil {
		t.Fatalf("decode: %v; body=%s", err, raw)
	}
	return env.Environment
}

// putSecretDirect seals and stores a secret straight into st, so an
// environment test can reference a real secret without going through the
// admin PUT route.
func putSecretDirect(t *testing.T, st Store, name string) {
	t.Helper()
	putSecretValue(t, st, name, "v")
}

// putSecretValue is putSecretDirect with the plaintext spelled out, for the
// resolution tests — they assert the exact value that came back out of the
// envelope and into the dispatched Spec.Env.
func putSecretValue(t *testing.T, st Store, name, value string) {
	t.Helper()
	ct, nonce, err := Seal(testSecretsKey, []byte(value))
	if err != nil {
		t.Fatalf("Seal(%s): %v", name, err)
	}
	if err := st.PutSecret(context.Background(), name, ct, nonce); err != nil {
		t.Fatalf("PutSecret(%s): %v", name, err)
	}
}

// seedEnv stores e straight into st (minting an id when it has none) and
// returns the row the store actually holds — SetupHash included, since the
// resolution rules compare against it.
func seedEnv(t *testing.T, st MemStore, e control.Environment) control.Environment {
	t.Helper()
	if e.ID == "" {
		e.ID = control.EnvironmentID(NewEnvironmentID())
	}
	// SetupHash is the store's own column on the old surface, recomputed on
	// every write; the repository stores what it is given, so the seed
	// computes the same identity here.
	e.SetupHash = SetupHash(e.Image, e.Setup)
	out, err := st.Environments().CreateEnvironment(context.Background(), installWorkspace, e)
	if err != nil {
		t.Fatalf("seed environment %q: %v", e.Name, err)
	}
	return out
}

// cacheEnvSnapshot records a built snapshot against env — what Task 9's setup
// orchestration does once a session's setup script finishes — and returns the
// refreshed row.
func cacheEnvSnapshot(t *testing.T, st MemStore, env control.Environment, ref, runner string) control.Environment {
	t.Helper()
	if err := st.Environments().SetEnvironmentSnapshot(context.Background(), installWorkspace,
		env.ID, env.SetupHash, ref, control.RunnerID(runner)); err != nil {
		t.Fatalf("SetEnvironmentSnapshot(%s): %v", env.ID, err)
	}
	out, err := st.Environments().GetEnvironment(context.Background(), installWorkspace, env.ID)
	if err != nil {
		t.Fatalf("GetEnvironment(%s): %v", env.ID, err)
	}
	return out
}

// ---------------------------------------------------------------------------
// POST /v0/environments
// ---------------------------------------------------------------------------

func TestCreateEnvironment(t *testing.T) {
	t.Run("happy path stores the row and answers 201 with Location", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		putSecretDirect(t, st, "GH_TOKEN")

		body := envCreateBody("dev", map[string]any{
			"setup":             "echo hi",
			"egress_allow":      []string{"api.github.com"},
			"secret_refs":       []string{"GH_TOKEN"},
			"connectors":        connectorArray(ghConnJSON, browserConnJSON),
			"placement":         "rainier-1",
			"setup_timeout_sec": 600,
		})
		resp := doJSON(t, ts, http.MethodPost, "/v0/environments", adminTok, body, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("status = %d, want 201; body=%s", resp.StatusCode, raw)
		}
		var env environmentEnvelope
		if err := json.Unmarshal([]byte(raw), &env); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		got := env.Environment
		if !strings.HasPrefix(got.ID, "env_") {
			t.Errorf("id = %q, want an env_ prefix", got.ID)
		}
		if loc := resp.Header.Get("Location"); loc != "/v0/environments/"+got.ID {
			t.Errorf("Location = %q, want /v0/environments/%s", loc, got.ID)
		}
		if got.Name != "dev" || got.Image != "img:1" || got.Setup != "echo hi" {
			t.Errorf("name/image/setup = %q/%q/%q", got.Name, got.Image, got.Setup)
		}
		if got.SetupHash != SetupHash("img:1", "echo hi") {
			t.Errorf("setup_hash = %q, want %q", got.SetupHash, SetupHash("img:1", "echo hi"))
		}
		if !slices.Equal(got.EgressAllow, []string{"api.github.com"}) || !slices.Equal(got.SecretRefs, []string{"GH_TOKEN"}) {
			t.Errorf("egress/secret_refs = %v / %v", got.EgressAllow, got.SecretRefs)
		}
		if got.Placement != "rainier-1" || got.SetupTimeoutSec != 600 {
			t.Errorf("placement/timeout = %q/%d", got.Placement, got.SetupTimeoutSec)
		}
		// A brand-new environment has no cache yet: the three snapshot fields
		// are present in the response and empty.
		if got.SnapshotRef != "" || got.SnapshotRunner != "" || got.SnapshotHash != "" {
			t.Errorf("snapshot fields = %q/%q/%q, want all empty", got.SnapshotRef, got.SnapshotRunner, got.SnapshotHash)
		}
		if _, err := time.Parse(time.RFC3339, got.CreatedAt); err != nil {
			t.Errorf("created_at = %q: %v", got.CreatedAt, err)
		}
		if _, err := time.Parse(time.RFC3339, got.UpdatedAt); err != nil {
			t.Errorf("updated_at = %q: %v", got.UpdatedAt, err)
		}

		// The row the store actually holds, including verbatim connector bytes.
		row, err := st.Environments().GetEnvironment(context.Background(), installWorkspace, control.EnvironmentID(got.ID))
		if err != nil {
			t.Fatalf("GetEnvironment: %v", err)
		}
		if len(row.Connectors) != 2 {
			t.Fatalf("stored connectors = %+v, want 2", row.Connectors)
		}
		if row.Connectors[0].Type != "github" || string(row.Connectors[0].Raw) != ghConnJSON {
			t.Errorf("stored connector 0 = %+v, want %s", row.Connectors[0], ghConnJSON)
		}
		for i, c := range row.Connectors {
			if len(c.Raw) == 0 {
				t.Errorf("stored connector %d has empty Raw", i)
			}
		}
	})

	// Byte equality holds end to end over memstore, which is what pins that
	// neither the API nor the store rewrites a connector. Over pgstore the
	// contract is the JSON VALUE (jsonb re-renders whitespace and member
	// order) — storetest's sameJSON is where that half lives.
	t.Run("connectors round-trip verbatim in the response", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		all := connectorArray(ghConnJSON, filesConnJSON, tunnelConnJSON, browserConnJSON)

		got := createEnv(t, ts, adminTok, envCreateBody("dev", map[string]any{"connectors": all}))
		var want, have bytes.Buffer
		if err := json.Compact(&want, all); err != nil {
			t.Fatalf("compact want: %v", err)
		}
		encoded, err := json.Marshal(got.Connectors)
		if err != nil {
			t.Fatalf("marshal connectors: %v", err)
		}
		if err := json.Compact(&have, encoded); err != nil {
			t.Fatalf("compact have: %v", err)
		}
		if have.String() != want.String() {
			t.Fatalf("connectors = %s, want %s", have.String(), want.String())
		}
	})

	t.Run("response shape is pinned", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		resp := doJSON(t, ts, http.MethodPost, "/v0/environments", adminTok, envCreateBody("dev", nil), nil)
		raw := readBody(t, resp)
		assertKeySet(t, raw, "environment")
		var outer map[string]json.RawMessage
		if err := json.Unmarshal([]byte(raw), &outer); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		assertKeySet(t, string(outer["environment"]),
			"id", "name", "image", "setup", "setup_hash", "init", "init_timeout_sec", "egress_allow", "secret_refs",
			"connectors", "placement", "capabilities", "setup_timeout_sec", "snapshot_ref", "snapshot_runner",
			"snapshot_hash", "created_at", "updated_at")
	})

	t.Run("empty lists render as [] and never null", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		resp := doJSON(t, ts, http.MethodPost, "/v0/environments", adminTok, envCreateBody("dev", nil), nil)
		raw := readBody(t, resp)
		for _, key := range []string{"egress_allow", "secret_refs", "connectors"} {
			if strings.Contains(raw, `"`+key+`":null`) {
				t.Errorf("%s rendered as null: %s", key, raw)
			}
			if !strings.Contains(raw, `"`+key+`":[]`) {
				t.Errorf("%s did not render as []: %s", key, raw)
			}
		}
	})

	t.Run("the name must match [a-z0-9-]{1,64}", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		for _, name := range []string{
			"", "UPPER", "under_score", "has.dot", "has space", "héllo", "slash/ed",
			strings.Repeat("a", 65),
		} {
			resp := doJSON(t, ts, http.MethodPost, "/v0/environments", adminTok, envCreateBody(name, nil), nil)
			raw := readBody(t, resp)
			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("name=%q status = %d, want 400; body=%s", name, resp.StatusCode, raw)
				continue
			}
			if e := decodeErrBody(t, raw); e.Error.Code != "invalid_request" {
				t.Errorf("name=%q code = %q, want invalid_request", name, e.Error.Code)
			}
		}
	})

	t.Run("a 64-character name is accepted (the boundary)", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		createEnv(t, ts, adminTok, envCreateBody(strings.Repeat("a", 64), nil))
	})

	t.Run("image is required", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		resp := doJSON(t, ts, http.MethodPost, "/v0/environments", adminTok, map[string]any{"name": "dev"}, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "invalid_request" || !strings.Contains(e.Error.Message, "image") {
			t.Errorf("error = %+v, want invalid_request naming image", e.Error)
		}
	})

	// init is the per-boot hook. It rides on the same create/patch paths as
	// setup but must stay OUT of setup_hash: editing it cannot change the
	// image a cached snapshot holds, so it must not invalidate that cache.
	t.Run("init round-trips and does not move setup_hash", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")

		got := createEnv(t, ts, adminTok, envCreateBody("dev", map[string]any{
			"setup":            "make deps",
			"init":             "make dev-server &",
			"init_timeout_sec": 120,
		}))
		if got.Init != "make dev-server &" || got.InitTimeoutSec != 120 {
			t.Errorf("init/init_timeout_sec = %q/%d", got.Init, got.InitTimeoutSec)
		}
		if got.SetupHash != SetupHash("img:1", "make deps") {
			t.Errorf("setup_hash = %q, want the image+setup hash alone", got.SetupHash)
		}
		// An environment that names no init gets the empty pair, not a null.
		bare := createEnv(t, ts, adminTok, envCreateBody("bare", nil))
		if bare.Init != "" || bare.InitTimeoutSec != 0 {
			t.Errorf("bare environment init = %q/%d, want empty", bare.Init, bare.InitTimeoutSec)
		}
	})

	t.Run("a negative init_timeout_sec is rejected", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		resp := doJSON(t, ts, http.MethodPost, "/v0/environments", adminTok,
			envCreateBody("dev", map[string]any{"init_timeout_sec": -1}), nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); !strings.Contains(e.Error.Message, "init_timeout_sec") {
			t.Errorf("message = %q, want it to name init_timeout_sec", e.Error.Message)
		}
	})

	t.Run("a negative setup_timeout_sec is rejected", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		resp := doJSON(t, ts, http.MethodPost, "/v0/environments", adminTok,
			envCreateBody("dev", map[string]any{"setup_timeout_sec": -1}), nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); !strings.Contains(e.Error.Message, "setup_timeout_sec") {
			t.Errorf("message = %q, want it to name setup_timeout_sec", e.Error.Message)
		}
	})

	t.Run("a duplicate name is 409 conflict", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		createEnv(t, ts, adminTok, envCreateBody("dev", nil))

		resp := doJSON(t, ts, http.MethodPost, "/v0/environments", adminTok,
			envCreateBody("dev", map[string]any{"image": "img:2"}), nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("status = %d, want 409; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "conflict" {
			t.Errorf("code = %q, want conflict", e.Error.Code)
		}
	})

	t.Run("secret_refs must all exist, and the missing one is named", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		putSecretDirect(t, st, "PRESENT")

		resp := doJSON(t, ts, http.MethodPost, "/v0/environments", adminTok,
			envCreateBody("dev", map[string]any{"secret_refs": []string{"PRESENT", "ABSENT"}}), nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body=%s", resp.StatusCode, raw)
		}
		e := decodeErrBody(t, raw)
		if e.Error.Code != "invalid_request" || !strings.Contains(e.Error.Message, "ABSENT") {
			t.Errorf("error = %+v, want invalid_request naming ABSENT", e.Error)
		}
		if envs, _, err := st.Environments().ListEnvironments(context.Background(), installWorkspace, control.EnvironmentQuery{}); err != nil || len(envs) != 0 {
			t.Errorf("environments after a rejected create = %+v (err %v), want none", envs, err)
		}
	})

	t.Run("connector validation is enforced at the route", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		cases := []struct{ name, conn, want string }{
			{"unknown type", `{"type":"gitlab","repo":"a/b"}`, "gitlab"},
			{"unknown field", `{"type":"github","repo":"x/y","extra":1}`, "extra"},
			{"bad tier", `{"type":"browser","tier":"daily"}`, "tier"},
			{"bad port", `{"type":"tunnel","name":"n","target_host":"h","target_port":70000}`, "target_port"},
			{"empty paths", `{"type":"files","paths":[]}`, "paths"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				resp := doJSON(t, ts, http.MethodPost, "/v0/environments", adminTok,
					envCreateBody("dev", map[string]any{"connectors": connectorArray(tc.conn)}), nil)
				raw := readBody(t, resp)
				if resp.StatusCode != http.StatusBadRequest {
					t.Fatalf("status = %d, want 400; body=%s", resp.StatusCode, raw)
				}
				e := decodeErrBody(t, raw)
				if e.Error.Code != "invalid_request" || !strings.Contains(e.Error.Message, tc.want) {
					t.Errorf("error = %+v, want invalid_request naming %q", e.Error, tc.want)
				}
			})
		}
	})

	t.Run("an unknown body field is 400 invalid_request", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		resp := doRaw(t, ts, http.MethodPost, "/v0/environments", adminTok, `{"name":"dev","image":"i","bogus":true}`)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body=%s", resp.StatusCode, raw)
		}
	})

	t.Run("a member is 403 forbidden and stores nothing", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, memberTok := loginUser(t, st, "alice", "member")
		resp := doJSON(t, ts, http.MethodPost, "/v0/environments", memberTok, envCreateBody("dev", nil), nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("status = %d, want 403; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "forbidden" {
			t.Errorf("code = %q, want forbidden", e.Error.Code)
		}
		if envs, _, err := st.Environments().ListEnvironments(context.Background(), installWorkspace, control.EnvironmentQuery{}); err != nil || len(envs) != 0 {
			t.Errorf("environments after a member's create = %+v (err %v), want none", envs, err)
		}
	})

	t.Run("no token is 401 unauthenticated", func(t *testing.T) {
		_, _, ts := newTestControld(t)
		resp := doJSON(t, ts, http.MethodPost, "/v0/environments", "", envCreateBody("dev", nil), nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "unauthenticated" {
			t.Errorf("code = %q, want unauthenticated", e.Error.Code)
		}
	})
}

// ---------------------------------------------------------------------------
// GET /v0/environments
// ---------------------------------------------------------------------------

func TestListEnvironments(t *testing.T) {
	t.Run("happy path lists every environment, name ascending", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		_, memberTok := loginUser(t, st, "alice", "member")
		createEnv(t, ts, adminTok, envCreateBody("zulu", nil))
		createEnv(t, ts, adminTok, envCreateBody("alpha", nil))

		resp := doRequest(t, ts, http.MethodGet, "/v0/environments", memberTok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200; body=%s", resp.StatusCode, raw)
		}
		var body environmentsEnvelope
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		if len(body.Environments) != 2 {
			t.Fatalf("environments = %+v, want 2", body.Environments)
		}
		if body.Environments[0].Name != "alpha" || body.Environments[1].Name != "zulu" {
			t.Errorf("order = %q, %q; want alpha, zulu", body.Environments[0].Name, body.Environments[1].Name)
		}
	})

	t.Run("no environments render as an empty array, not null", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, tok := loginUser(t, st, "alice", "member")
		raw := readBody(t, doRequest(t, ts, http.MethodGet, "/v0/environments", tok, nil, nil))
		if strings.Contains(raw, `"environments":null`) {
			t.Fatalf("empty list rendered as JSON null: %s", raw)
		}
	})

	t.Run("response shape is pinned", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		createEnv(t, ts, adminTok, envCreateBody("dev", nil))

		raw := readBody(t, doRequest(t, ts, http.MethodGet, "/v0/environments", adminTok, nil, nil))
		assertKeySet(t, raw, "environments")
		var outer map[string]json.RawMessage
		if err := json.Unmarshal([]byte(raw), &outer); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		var arr []json.RawMessage
		if err := json.Unmarshal(outer["environments"], &arr); err != nil {
			t.Fatalf("decode environments array: %v", err)
		}
		assertKeySet(t, string(arr[0]),
			"id", "name", "image", "setup", "setup_hash", "init", "init_timeout_sec", "egress_allow", "secret_refs",
			"connectors", "placement", "capabilities", "setup_timeout_sec", "snapshot_ref", "snapshot_runner",
			"snapshot_hash", "created_at", "updated_at")
	})

	t.Run("no token is 401 unauthenticated", func(t *testing.T) {
		_, _, ts := newTestControld(t)
		resp := doRequest(t, ts, http.MethodGet, "/v0/environments", "", nil, nil)
		readBody(t, resp)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", resp.StatusCode)
		}
	})
}

// ---------------------------------------------------------------------------
// GET /v0/environments/{id}
// ---------------------------------------------------------------------------

func TestGetEnvironment(t *testing.T) {
	t.Run("by id and by name resolve to the same row", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		_, memberTok := loginUser(t, st, "alice", "member")
		created := createEnv(t, ts, adminTok, envCreateBody("dev", nil))

		byID := getEnv(t, ts, memberTok, created.ID)
		byName := getEnv(t, ts, memberTok, "dev")
		if byID.ID != created.ID || byName.ID != created.ID {
			t.Fatalf("byID=%q byName=%q, want %q", byID.ID, byName.ID, created.ID)
		}
	})

	t.Run("an unknown id or name is 404 not_found", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, tok := loginUser(t, st, "alice", "member")
		for _, ref := range []string{"env_" + strings.Repeat("0", 32), "nosuch"} {
			resp := doRequest(t, ts, http.MethodGet, "/v0/environments/"+ref, tok, nil, nil)
			raw := readBody(t, resp)
			if resp.StatusCode != http.StatusNotFound {
				t.Errorf("ref=%q status = %d, want 404; body=%s", ref, resp.StatusCode, raw)
				continue
			}
			if e := decodeErrBody(t, raw); e.Error.Code != "not_found" {
				t.Errorf("ref=%q code = %q, want not_found", ref, e.Error.Code)
			}
		}
	})

	t.Run("no token is 401 unauthenticated", func(t *testing.T) {
		_, _, ts := newTestControld(t)
		resp := doRequest(t, ts, http.MethodGet, "/v0/environments/dev", "", nil, nil)
		readBody(t, resp)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", resp.StatusCode)
		}
	})
}

// ---------------------------------------------------------------------------
// PATCH /v0/environments/{id}
// ---------------------------------------------------------------------------

func TestUpdateEnvironment(t *testing.T) {
	t.Run("a partial patch changes only what it names", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		created := createEnv(t, ts, adminTok, envCreateBody("dev", map[string]any{
			"setup":        "echo hi",
			"egress_allow": []string{"api.github.com"},
			"connectors":   connectorArray(ghConnJSON),
			"placement":    "rainier-1",
		}))

		resp := doJSON(t, ts, http.MethodPatch, "/v0/environments/"+created.ID, adminTok,
			map[string]any{"image": "img:2"}, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200; body=%s", resp.StatusCode, raw)
		}
		var env environmentEnvelope
		if err := json.Unmarshal([]byte(raw), &env); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		got := env.Environment
		if got.Image != "img:2" {
			t.Errorf("image = %q, want img:2", got.Image)
		}
		if got.Name != "dev" || got.Setup != "echo hi" || got.Placement != "rainier-1" {
			t.Errorf("untouched fields changed: %+v", got)
		}
		if !slices.Equal(got.EgressAllow, []string{"api.github.com"}) {
			t.Errorf("egress_allow = %v, want it untouched", got.EgressAllow)
		}
		if len(got.Connectors) != 1 || string(got.Connectors[0]) != ghConnJSON {
			t.Errorf("connectors = %v, want them untouched", got.Connectors)
		}
		// image is half of the setup hash, so this patch must move it.
		if got.SetupHash != SetupHash("img:2", "echo hi") {
			t.Errorf("setup_hash = %q, want it recomputed", got.SetupHash)
		}
	})

	t.Run("a patch that touches neither image nor setup leaves the hash alone", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		created := createEnv(t, ts, adminTok, envCreateBody("dev", map[string]any{"setup": "echo hi"}))

		resp := doJSON(t, ts, http.MethodPatch, "/v0/environments/dev", adminTok,
			map[string]any{"egress_allow": []string{"example.com"}}, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200; body=%s", resp.StatusCode, raw)
		}
		var env environmentEnvelope
		if err := json.Unmarshal([]byte(raw), &env); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		if env.Environment.SetupHash != created.SetupHash {
			t.Errorf("setup_hash = %q, want it unchanged (%q)", env.Environment.SetupHash, created.SetupHash)
		}
	})

	// Patching init is the case the whole "init is not a build input" rule
	// exists for: an operator edits the boot hook and the team's cached
	// snapshot must still be usable afterwards.
	t.Run("patching init leaves setup_hash and a cached snapshot alone", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		created := createEnv(t, ts, adminTok, envCreateBody("dev", map[string]any{
			"setup": "echo hi", "init": "old-init", "init_timeout_sec": 60,
		}))
		if err := st.Environments().SetEnvironmentSnapshot(context.Background(), installWorkspace,
			control.EnvironmentID(created.ID), created.SetupHash, "rainier-env:dev-aaaa", "vm1"); err != nil {
			t.Fatalf("SetEnvironmentSnapshot: %v", err)
		}

		resp := doJSON(t, ts, http.MethodPatch, "/v0/environments/dev", adminTok,
			map[string]any{"init": "new-init", "init_timeout_sec": 300}, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200; body=%s", resp.StatusCode, raw)
		}
		var env environmentEnvelope
		if err := json.Unmarshal([]byte(raw), &env); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		got := env.Environment
		if got.Init != "new-init" || got.InitTimeoutSec != 300 {
			t.Errorf("init/init_timeout_sec = %q/%d, want them patched", got.Init, got.InitTimeoutSec)
		}
		if got.Setup != "echo hi" {
			t.Errorf("setup = %q, want it untouched", got.Setup)
		}
		if got.SetupHash != created.SetupHash {
			t.Errorf("setup_hash = %q, want it unchanged (%q) — init is not a build input", got.SetupHash, created.SetupHash)
		}
		if got.SnapshotHash != got.SetupHash {
			t.Errorf("an init-only patch must leave the cache valid: snapshot_hash %q vs setup_hash %q", got.SnapshotHash, got.SetupHash)
		}
	})

	t.Run("the snapshot columns survive a patch, visibly stale", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		created := createEnv(t, ts, adminTok, envCreateBody("dev", nil))
		if err := st.Environments().SetEnvironmentSnapshot(context.Background(), installWorkspace,
			control.EnvironmentID(created.ID), created.SetupHash, "rainier-env:dev-aaaa", "vm1"); err != nil {
			t.Fatalf("SetEnvironmentSnapshot: %v", err)
		}

		resp := doJSON(t, ts, http.MethodPatch, "/v0/environments/"+created.ID, adminTok,
			map[string]any{"setup": "echo changed"}, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200; body=%s", resp.StatusCode, raw)
		}
		var env environmentEnvelope
		if err := json.Unmarshal([]byte(raw), &env); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		got := env.Environment
		if got.SnapshotRef != "rainier-env:dev-aaaa" || got.SnapshotRunner != "vm1" {
			t.Errorf("snapshot ref/runner = %q/%q, want them preserved", got.SnapshotRef, got.SnapshotRunner)
		}
		if got.SnapshotHash == got.SetupHash {
			t.Errorf("snapshot_hash %q still equals setup_hash after a setup change — the cache must read as stale", got.SnapshotHash)
		}
	})

	t.Run("clearing a list clears it", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		created := createEnv(t, ts, adminTok, envCreateBody("dev", map[string]any{
			"egress_allow": []string{"api.github.com"},
			"connectors":   connectorArray(ghConnJSON),
		}))

		resp := doJSON(t, ts, http.MethodPatch, "/v0/environments/"+created.ID, adminTok,
			map[string]any{"egress_allow": []string{}, "connectors": json.RawMessage(`[]`)}, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200; body=%s", resp.StatusCode, raw)
		}
		var env environmentEnvelope
		if err := json.Unmarshal([]byte(raw), &env); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		if len(env.Environment.EgressAllow) != 0 || len(env.Environment.Connectors) != 0 {
			t.Fatalf("after clearing: egress=%v connectors=%v", env.Environment.EgressAllow, env.Environment.Connectors)
		}
	})

	t.Run("a rename works, and a rename onto a taken name is 409", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		createEnv(t, ts, adminTok, envCreateBody("taken", nil))
		created := createEnv(t, ts, adminTok, envCreateBody("dev", nil))

		got := getEnv(t, ts, adminTok, created.ID)
		resp := doJSON(t, ts, http.MethodPatch, "/v0/environments/"+got.ID, adminTok,
			map[string]any{"name": "renamed"}, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("rename status = %d, want 200; body=%s", resp.StatusCode, raw)
		}
		if after := getEnv(t, ts, adminTok, "renamed"); after.ID != created.ID {
			t.Errorf("renamed lookup = %q, want %q", after.ID, created.ID)
		}

		resp = doJSON(t, ts, http.MethodPatch, "/v0/environments/renamed", adminTok,
			map[string]any{"name": "taken"}, nil)
		raw = readBody(t, resp)
		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("collision status = %d, want 409; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "conflict" {
			t.Errorf("code = %q, want conflict", e.Error.Code)
		}
	})

	t.Run("the patched row is validated like a create", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		created := createEnv(t, ts, adminTok, envCreateBody("dev", nil))

		cases := []struct {
			name  string
			patch map[string]any
			want  string
		}{
			{"bad name", map[string]any{"name": "NOPE"}, "name"},
			{"empty image", map[string]any{"image": ""}, "image"},
			{"negative timeout", map[string]any{"setup_timeout_sec": -5}, "setup_timeout_sec"},
			{"negative init timeout", map[string]any{"init_timeout_sec": -5}, "init_timeout_sec"},
			{"unknown connector type", map[string]any{"connectors": connectorArray(`{"type":"gitlab"}`)}, "gitlab"},
			{"missing secret", map[string]any{"secret_refs": []string{"ABSENT"}}, "ABSENT"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				resp := doJSON(t, ts, http.MethodPatch, "/v0/environments/"+created.ID, adminTok, tc.patch, nil)
				raw := readBody(t, resp)
				if resp.StatusCode != http.StatusBadRequest {
					t.Fatalf("status = %d, want 400; body=%s", resp.StatusCode, raw)
				}
				e := decodeErrBody(t, raw)
				if e.Error.Code != "invalid_request" || !strings.Contains(e.Error.Message, tc.want) {
					t.Errorf("error = %+v, want invalid_request naming %q", e.Error, tc.want)
				}
			})
		}
	})

	t.Run("an unknown environment is 404 not_found", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		resp := doJSON(t, ts, http.MethodPatch, "/v0/environments/nosuch", adminTok, map[string]any{"image": "img:2"}, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("status = %d, want 404; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "not_found" {
			t.Errorf("code = %q, want not_found", e.Error.Code)
		}
	})

	t.Run("a member is 403 forbidden and the row survives", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		_, memberTok := loginUser(t, st, "alice", "member")
		created := createEnv(t, ts, adminTok, envCreateBody("dev", nil))

		resp := doJSON(t, ts, http.MethodPatch, "/v0/environments/"+created.ID, memberTok,
			map[string]any{"image": "img:evil"}, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("status = %d, want 403; body=%s", resp.StatusCode, raw)
		}
		if got := getEnv(t, ts, adminTok, created.ID); got.Image != "img:1" {
			t.Errorf("image after a member's patch = %q, want img:1", got.Image)
		}
	})

	t.Run("no token is 401 unauthenticated", func(t *testing.T) {
		_, _, ts := newTestControld(t)
		resp := doJSON(t, ts, http.MethodPatch, "/v0/environments/dev", "", map[string]any{"image": "i"}, nil)
		readBody(t, resp)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", resp.StatusCode)
		}
	})

	t.Run("an unknown body field is 400 invalid_request", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		created := createEnv(t, ts, adminTok, envCreateBody("dev", nil))
		resp := doRaw(t, ts, http.MethodPatch, "/v0/environments/"+created.ID, adminTok, `{"bogus":true}`)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body=%s", resp.StatusCode, raw)
		}
	})
}

// ---------------------------------------------------------------------------
// DELETE /v0/environments/{id}
// ---------------------------------------------------------------------------

func TestDeleteEnvironment(t *testing.T) {
	t.Run("happy path is 204 and the environment is gone", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		created := createEnv(t, ts, adminTok, envCreateBody("dev", nil))

		resp := doRequest(t, ts, http.MethodDelete, "/v0/environments/"+created.ID, adminTok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("status = %d, want 204; body=%s", resp.StatusCode, raw)
		}
		if raw != "" {
			t.Errorf("204 carried a body: %q", raw)
		}
		if _, err := st.Environments().GetEnvironment(context.Background(), installWorkspace,
			control.EnvironmentID(created.ID)); !errors.Is(err, control.ErrNotFound) {
			t.Errorf("GetEnvironment after delete: err = %v, want control.ErrNotFound", err)
		}
	})

	t.Run("a name ref deletes the environment it names", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		created := createEnv(t, ts, adminTok, envCreateBody("dev", nil))

		resp := doRequest(t, ts, http.MethodDelete, "/v0/environments/dev", adminTok, nil, nil)
		readBody(t, resp)
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("status = %d, want 204", resp.StatusCode)
		}
		if _, err := st.Environments().GetEnvironment(context.Background(), installWorkspace,
			control.EnvironmentID(created.ID)); !errors.Is(err, control.ErrNotFound) {
			t.Errorf("GetEnvironment after delete by name: err = %v, want control.ErrNotFound", err)
		}
	})

	t.Run("non-terminal sessions block the delete with a 409 naming the count", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		u, adminTok := loginUser(t, st, "root", "admin")
		created := createEnv(t, ts, adminTok, envCreateBody("dev", nil))
		seedSession(t, st, control.Session{ID: control.SessionID(NewSessionID()), CreatorID: control.ActorID(u.ID), EnvironmentID: control.EnvironmentID(created.ID), State: control.StateRunning})
		seedSession(t, st, control.Session{ID: control.SessionID(NewSessionID()), CreatorID: control.ActorID(u.ID), EnvironmentID: control.EnvironmentID(created.ID), State: control.StateQueued})
		seedSession(t, st, control.Session{ID: control.SessionID(NewSessionID()), CreatorID: control.ActorID(u.ID), EnvironmentID: control.EnvironmentID(created.ID), State: control.StateDestroyed})

		resp := doRequest(t, ts, http.MethodDelete, "/v0/environments/dev", adminTok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("status = %d, want 409; body=%s", resp.StatusCode, raw)
		}
		e := decodeErrBody(t, raw)
		if e.Error.Code != "conflict" || !strings.Contains(e.Error.Message, "2") {
			t.Errorf("error = %+v, want conflict naming the count 2", e.Error)
		}
		if _, err := st.Environments().GetEnvironment(context.Background(), installWorkspace,
			control.EnvironmentID(created.ID)); err != nil {
			t.Errorf("a refused delete removed the environment anyway: %v", err)
		}
	})

	t.Run("only terminal sessions do not block the delete", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		u, adminTok := loginUser(t, st, "root", "admin")
		created := createEnv(t, ts, adminTok, envCreateBody("dev", nil))
		seedSession(t, st, control.Session{ID: control.SessionID(NewSessionID()), CreatorID: control.ActorID(u.ID), EnvironmentID: control.EnvironmentID(created.ID), State: control.StateDestroyed})

		resp := doRequest(t, ts, http.MethodDelete, "/v0/environments/dev", adminTok, nil, nil)
		readBody(t, resp)
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("status = %d, want 204", resp.StatusCode)
		}
	})

	// The regression that made the count take a resolved id: scratch sessions
	// carry environment_id "", so counting against an unresolved ref (or an
	// empty string) would count them and refuse a delete that has nothing to
	// do with them.
	t.Run("scratch sessions never block an environment delete", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		u, adminTok := loginUser(t, st, "root", "admin")
		created := createEnv(t, ts, adminTok, envCreateBody("dev", nil))
		seedSession(t, st, control.Session{ID: control.SessionID(NewSessionID()), CreatorID: control.ActorID(u.ID), State: control.StateRunning})
		seedSession(t, st, control.Session{ID: control.SessionID(NewSessionID()), CreatorID: control.ActorID(u.ID), State: control.StateQueued})

		resp := doRequest(t, ts, http.MethodDelete, "/v0/environments/dev", adminTok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("status = %d, want 204; body=%s", resp.StatusCode, raw)
		}
		if _, err := st.Environments().GetEnvironment(context.Background(), installWorkspace,
			control.EnvironmentID(created.ID)); !errors.Is(err, control.ErrNotFound) {
			t.Errorf("GetEnvironment after delete: err = %v, want control.ErrNotFound", err)
		}
	})

	t.Run("an unknown environment is 404 not_found", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		resp := doRequest(t, ts, http.MethodDelete, "/v0/environments/nosuch", adminTok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("status = %d, want 404; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "not_found" {
			t.Errorf("code = %q, want not_found", e.Error.Code)
		}
	})

	t.Run("a member is 403 forbidden and the row survives", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		_, memberTok := loginUser(t, st, "alice", "member")
		created := createEnv(t, ts, adminTok, envCreateBody("dev", nil))

		resp := doRequest(t, ts, http.MethodDelete, "/v0/environments/dev", memberTok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("status = %d, want 403; body=%s", resp.StatusCode, raw)
		}
		if e := decodeErrBody(t, raw); e.Error.Code != "forbidden" {
			t.Errorf("code = %q, want forbidden", e.Error.Code)
		}
		if _, err := st.Environments().GetEnvironment(context.Background(), installWorkspace,
			control.EnvironmentID(created.ID)); err != nil {
			t.Errorf("a member's delete removed the environment anyway: %v", err)
		}
	})

	t.Run("no token is 401 unauthenticated", func(t *testing.T) {
		_, _, ts := newTestControld(t)
		resp := doRequest(t, ts, http.MethodDelete, "/v0/environments/dev", "", nil, nil)
		readBody(t, resp)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", resp.StatusCode)
		}
	})
}

// ---------------------------------------------------------------------------
// GET /v0/runners
// ---------------------------------------------------------------------------

func TestListRunners(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		s, st, ts := newTestControld(t)
		startFakeRunner(t, ts, runnerScript{Name: "vm1", Used: 1, Total: 4})
		waitConnected(t, s, "vm1")
		_, tok := loginUser(t, st, "alice", "member")

		eventually(t, 3*time.Second, func() error {
			resp := doRequest(t, ts, http.MethodGet, "/v0/runners", tok, nil, nil)
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
		resp := doRequest(t, ts, http.MethodGet, "/v0/runners", "", nil, nil)
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
			resp := doRequest(t, ts, http.MethodGet, "/v0/runners", tok, nil, nil)
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
		resp := doRequest(t, ts, http.MethodGet, "/v0/sessions", tok, nil, nil)
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
	MemStore
	mu        sync.Mutex
	committed map[string]time.Time
}

func newCommitTimingStore(st MemStore) *commitTimingStore {
	return &commitTimingStore{MemStore: st, committed: map[string]time.Time{}}
}

func (c *commitTimingStore) Sessions() control.SessionRepository {
	return commitTimingSessions{SessionRepository: c.MemStore.Sessions(), owner: c}
}

type commitTimingSessions struct {
	control.SessionRepository
	owner *commitTimingStore
}

func (c commitTimingSessions) CreateSession(ctx context.Context, ws control.WorkspaceID, s control.Session) (control.Session, error) {
	out, err := c.SessionRepository.CreateSession(ctx, ws, s)
	if err == nil {
		c.owner.mu.Lock()
		c.owner.committed[string(out.ID)] = time.Now()
		c.owner.mu.Unlock()
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

		resp := doJSON(t, ts, http.MethodPost, "/v0/sessions", tok, map[string]any{"name": "durable1", "image": "img:latest"}, nil)
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

		drainAccept(t, f)
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

		resp := doJSON(t, ts, http.MethodPost, "/v0/sessions", tok, map[string]any{"name": "durable2", "image": "img:latest"}, nil)
		raw := readBody(t, resp)
		var body sessionEnvelope
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		id := body.Session.ID

		drainAccept(t, f)
		cmd := f.nextCmd(t) // reaches the runner; never answered
		if cmd.Type != "create" || cmd.Session != id {
			t.Fatalf("got %+v, want create of %s", cmd, id)
		}

		got, err := cst.Sessions().GetSession(context.Background(), installWorkspace, control.SessionID(id))
		if err != nil {
			t.Fatalf("GetSession(%s): %v (row was lost)", id, err)
		}
		if got.State != control.StateCreating && got.State != control.StateQueued {
			t.Fatalf("state = %q, want creating or queued (never lost)", got.State)
		}
	})
}

// ---------------------------------------------------------------------------
// environments: portable capabilities (plan 8, D18)
// ---------------------------------------------------------------------------

// TestEnvironmentCapabilities pins the field an operator uses to say what a
// runner must be able to do before this environment's sessions land on it:
// it round-trips through create, get and update, renders as [] rather than
// null when there is none, and is validated by the same token rule a runner's
// own announced capabilities are.
func TestEnvironmentCapabilities(t *testing.T) {
	t.Run("round-trips through create, get and update", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")

		created := createEnv(t, ts, adminTok, envCreateBody("dev", map[string]any{
			"capabilities": []string{"gpu", "docker.rootless"},
		}))
		if !slices.Equal(created.Capabilities, []string{"gpu", "docker.rootless"}) {
			t.Fatalf("created capabilities = %v, want [gpu docker.rootless]", created.Capabilities)
		}
		if got := getEnv(t, ts, adminTok, created.ID); !slices.Equal(got.Capabilities, created.Capabilities) {
			t.Fatalf("read back = %v, want %v", got.Capabilities, created.Capabilities)
		}

		resp := doJSON(t, ts, http.MethodPatch, "/v0/environments/"+created.ID, adminTok,
			map[string]any{"capabilities": []string{"gpu"}}, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("PATCH status = %d, want 200; body=%s", resp.StatusCode, raw)
		}
		if got := getEnv(t, ts, adminTok, created.ID); !slices.Equal(got.Capabilities, []string{"gpu"}) {
			t.Fatalf("after the patch = %v, want [gpu]", got.Capabilities)
		}

		// The pin and the portable requirements are independent fields: a
		// patch that names one must leave the other exactly as it was.
		resp = doJSON(t, ts, http.MethodPatch, "/v0/environments/"+created.ID, adminTok,
			map[string]any{"placement": "vm1"}, nil)
		readBody(t, resp)
		after := getEnv(t, ts, adminTok, created.ID)
		if after.Placement != "vm1" || !slices.Equal(after.Capabilities, []string{"gpu"}) {
			t.Fatalf("placement/capabilities = %q/%v, want vm1/[gpu]", after.Placement, after.Capabilities)
		}
	})

	t.Run("an environment with none renders [] and never null", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		resp := doJSON(t, ts, http.MethodPost, "/v0/environments", adminTok, envCreateBody("dev", nil), nil)
		raw := readBody(t, resp)
		var outer map[string]json.RawMessage
		if err := json.Unmarshal([]byte(raw), &outer); err != nil {
			t.Fatalf("decode: %v; body=%s", err, raw)
		}
		var env map[string]json.RawMessage
		if err := json.Unmarshal(outer["environment"], &env); err != nil {
			t.Fatalf("decode environment: %v", err)
		}
		if string(env["capabilities"]) != "[]" {
			t.Fatalf("capabilities = %s, want []", env["capabilities"])
		}
	})

	t.Run("a capability that is not a token is 400", func(t *testing.T) {
		_, st, ts := newTestControld(t)
		_, adminTok := loginUser(t, st, "root", "admin")
		resp := doJSON(t, ts, http.MethodPost, "/v0/environments", adminTok,
			envCreateBody("dev", map[string]any{"capabilities": []string{"GPU"}}), nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body=%s", resp.StatusCode, raw)
		}
		if !strings.Contains(raw, "capabilities") {
			t.Fatalf("body = %s, want it to name the capabilities field", raw)
		}
	})
}

// TestQueueReasonNamesAMissingCapability: a session that cannot be placed
// because no connected runner claims what its environment requires says so,
// naming the first requirement nothing in the fleet advertises. The pinned
// runner's own reason keeps precedence — a pin is the more specific answer.
func TestQueueReasonNamesAMissingCapability(t *testing.T) {
	s, st, ts := newTestControld(t)
	_, adminTok := loginUser(t, st, "root", "admin")
	joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})

	env := createEnv(t, ts, adminTok, envCreateBody("gpu-env", map[string]any{
		"capabilities": []string{"gpu"},
	}))
	owner, tok := loginUser(t, st, "alice", "member")
	seedSession(t, st, control.Session{ID: "sess_wants_gpu", CreatorID: control.ActorID(owner.ID),
		State: control.StateQueued, EnvironmentID: control.EnvironmentID(env.ID)})

	resp := doRequest(t, ts, http.MethodGet, "/v0/sessions/sess_wants_gpu", tok, nil, nil)
	raw := readBody(t, resp)
	var body sessionEnvelope
	if err := json.Unmarshal([]byte(raw), &body); err != nil {
		t.Fatalf("decode: %v; body=%s", err, raw)
	}
	if got, want := body.Session.QueueReason, "waiting for a runner with capability gpu"; got != want {
		t.Fatalf("queue_reason = %q, want %q", got, want)
	}
}

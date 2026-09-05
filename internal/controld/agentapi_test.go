// internal/controld/agentapi_test.go
package controld

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/controlapp"
	"github.com/tokencanopy/rainier/v0wire"
)

// The agent listing and logout routes. Every fixture here is the plan's:
// the credential bytes are the literal "credential_example", the provider is
// whichever row the table hands back first (no test in this repository spells
// a provider name), and every assertion that matters is an ABSENCE — no
// response body on either route may carry a credential byte.

// seedAgentCredential puts one set for user through the self-hosted vault,
// which is the same path a sandbox's put takes. It returns the provider it
// used, read off the table.
func seedAgentCredential(t *testing.T, st Store, userID string) controlapp.AgentProvider {
	t.Helper()
	rows := controlapp.AgentProviders()
	if len(rows) < 2 {
		t.Fatalf("the provider table has %d rows; these tests need a logged-in one and an absent one", len(rows))
	}
	p := rows[0]
	files := map[string][]byte{p.Files[0]: []byte("credential_example")}
	if _, err := NewAgentVault(st, testSecretsKey).PutAgentCredentials(
		context.Background(), control.ActorID(userID), p.Name, files); err != nil {
		t.Fatalf("seeding an agent credential: %v", err)
	}
	return p
}

// getAgents issues GET /v0/agents and returns the decoded envelope beside the
// raw body, because both are asserted: the shape, and what the bytes do not
// contain.
func getAgents(t *testing.T, ts *httptest.Server, tok string) (v0wire.AgentsEnvelope, string) {
	t.Helper()
	resp := doRequest(t, ts, http.MethodGet, "/v0/agents", tok, nil, nil)
	raw := readBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v0/agents: status = %d, want 200; body=%s", resp.StatusCode, raw)
	}
	var env v0wire.AgentsEnvelope
	if err := json.Unmarshal([]byte(raw), &env); err != nil {
		t.Fatalf("decode: %v; body=%s", err, raw)
	}
	return env, raw
}

// TestListAgentsAnswersTheCallersOwnRows is the listing's contract: every
// provider the build knows appears, the one this person logged in says so
// with a version and a "since", the rest say "none", and the workspaces on
// each row are the installation's one workspace — a self-hosted controld IS
// one workspace, which is what makes "logged out of every workspace you are
// in" a complete promise here.
func TestListAgentsAnswersTheCallersOwnRows(t *testing.T) {
	_, st, ts := newTestControld(t)
	u, tok := loginUser(t, st, "alice", "member")
	p := seedAgentCredential(t, st, u.ID)

	env, raw := getAgents(t, ts, tok)
	if strings.Contains(raw, "credential_example") {
		t.Fatalf("the listing returned credential bytes: %s", raw)
	}
	assertKeySet(t, raw, "agents")
	if len(env.Agents) != len(controlapp.AgentProviders()) {
		t.Fatalf("agents = %d rows, want one per provider", len(env.Agents))
	}
	var seen int
	for _, row := range env.Agents {
		if !slices.Equal(row.Workspaces, []string{string(installWorkspace)}) {
			t.Errorf("%s workspaces = %v, want the installation workspace", row.Provider, row.Workspaces)
		}
		if row.Provider != p.Name {
			if row.Status != v0wire.AgentStatusNone || row.Version != 0 || row.Since != nil {
				t.Errorf("%s = %+v, want an empty row", row.Provider, row)
			}
			continue
		}
		seen++
		if row.Status != v0wire.AgentStatusLoggedIn {
			t.Errorf("status = %q, want %q", row.Status, v0wire.AgentStatusLoggedIn)
		}
		if row.Version != 1 {
			t.Errorf("version = %d, want 1 after one put", row.Version)
		}
		if row.Since == nil || row.Since.IsZero() {
			t.Errorf("since = %v, want the instant custody recorded", row.Since)
		}
	}
	if seen != 1 {
		t.Fatalf("the seeded provider appeared %d times, want once", seen)
	}
}

// One person's listing is theirs. A teammate — admin or not — has logged
// nothing in and sees nothing, because the route answers about the CALLER and
// has no way to name anybody else.
func TestListAgentsIsNotATeamListing(t *testing.T) {
	_, st, ts := newTestControld(t)
	alice, _ := loginUser(t, st, "alice", "member")
	seedAgentCredential(t, st, alice.ID)
	_, rootTok := loginUser(t, st, "root", "admin")

	env, raw := getAgents(t, ts, rootTok)
	if strings.Contains(raw, "credential_example") {
		t.Fatalf("the listing returned credential bytes: %s", raw)
	}
	for _, row := range env.Agents {
		if row.Status != v0wire.AgentStatusNone {
			t.Fatalf("an admin sees %s as %q; an agent credential is not the workspace's",
				row.Provider, row.Status)
		}
	}
}

// TestLogoutAgentDestroysTheSet: 204, no body, and the listing says "none"
// afterwards. A second logout is 204 too — revoking what is not there is the
// state the caller asked for either way.
func TestLogoutAgentDestroysTheSet(t *testing.T) {
	_, st, ts := newTestControld(t)
	u, tok := loginUser(t, st, "alice", "member")
	p := seedAgentCredential(t, st, u.ID)

	for i := range 2 {
		resp := doRequest(t, ts, http.MethodDelete, "/v0/agents/"+p.Name, tok, nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("logout %d: status = %d, want 204; body=%s", i, resp.StatusCode, raw)
		}
		if raw != "" {
			t.Fatalf("logout %d: body = %q, want empty", i, raw)
		}
	}

	env, raw := getAgents(t, ts, tok)
	if strings.Contains(raw, "credential_example") {
		t.Fatalf("the listing returned credential bytes: %s", raw)
	}
	for _, row := range env.Agents {
		if row.Status != v0wire.AgentStatusNone {
			t.Fatalf("%s = %q after a logout, want %q", row.Provider, row.Status, v0wire.AgentStatusNone)
		}
	}
	// And custody really is empty, not merely unrendered.
	set, err := NewAgentVault(st, testSecretsKey).FetchAgentCredentials(
		context.Background(), control.ActorID(u.ID), p.Name)
	if err != nil {
		t.Fatalf("fetch after logout: %v", err)
	}
	if set.Version != 0 || len(set.Files) != 0 {
		t.Fatalf("custody still holds v%d with %d files after a logout", set.Version, len(set.Files))
	}
}

// A provider that is not a row of the table is 404: {provider} is a path
// segment naming a resource, and one this build has never heard of is not
// found rather than malformed.
func TestLogoutUnknownAgentProviderIsNotFound(t *testing.T) {
	_, st, ts := newTestControld(t)
	_, tok := loginUser(t, st, "alice", "member")

	resp := doRequest(t, ts, http.MethodDelete, "/v0/agents/provider_example", tok, nil, nil)
	raw := readBody(t, resp)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", resp.StatusCode, raw)
	}
	assertKeySet(t, raw, "error")
	var body v0wire.ErrorEnvelope
	if err := json.Unmarshal([]byte(raw), &body); err != nil {
		t.Fatalf("decode: %v; body=%s", err, raw)
	}
	if body.Error.Code != "not_found" {
		t.Errorf("code = %q, want not_found", body.Error.Code)
	}
	if want, _ := controlapp.AgentRefusalSentence(controlapp.ErrUnknownAgentProvider); body.Error.Message != want {
		t.Errorf("message = %q, want custody's own sentence %q", body.Error.Message, want)
	}
}

// Both routes are behind requireUser: an anonymous caller learns nothing,
// including whether the route exists.
func TestAgentRoutesRequireAUser(t *testing.T) {
	_, _, ts := newTestControld(t)
	for _, r := range []struct{ method, path string }{
		{http.MethodGet, "/v0/agents"},
		{http.MethodDelete, "/v0/agents/provider_example"},
	} {
		resp := doRequest(t, ts, r.method, r.path, "", nil, nil)
		raw := readBody(t, resp)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("%s %s: status = %d, want 401; body=%s", r.method, r.path, resp.StatusCode, raw)
		}
	}
}

// TestAgentRefusalStatuses pins the two mappings the routes rely on, at the
// one place that decides them.
//
// The forbidden case cannot be reached through the HTTP surface at all:
// requireUser hands every handler a User, so control.ActorUser is the only
// actor kind a request can carry and controlapp.ErrAgentCredentialNotYours —
// the refusal for a principal that is not the account — has no caller here.
// It is still the answer a hosted cell's service principal would get from the
// same service, so the mapping is pinned rather than left to be discovered
// there.
func TestAgentRefusalStatuses(t *testing.T) {
	cases := []struct {
		err    error
		status int
		code   string
	}{
		{controlapp.ErrUnknownAgentProvider, http.StatusNotFound, "not_found"},
		{controlapp.ErrAgentCredentialNotYours, http.StatusForbidden, "forbidden"},
		{controlapp.ErrAgentCredentialUnreadable, http.StatusInternalServerError, "internal"},
	}
	for _, tc := range cases {
		rec := httptest.NewRecorder()
		writeAgentErr(rec, tc.err)
		if rec.Code != tc.status {
			t.Errorf("%v: status = %d, want %d", tc.err, rec.Code, tc.status)
		}
		var body v0wire.ErrorEnvelope
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("%v: decode: %v; body=%s", tc.err, err, rec.Body)
		}
		if body.Error.Code != tc.code {
			t.Errorf("%v: code = %q, want %q", tc.err, body.Error.Code, tc.code)
		}
		if want, _ := controlapp.AgentRefusalSentence(tc.err); body.Error.Message != want {
			t.Errorf("%v: message = %q, want %q", tc.err, body.Error.Message, want)
		}
	}
}

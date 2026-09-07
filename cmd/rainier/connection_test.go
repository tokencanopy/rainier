package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tokencanopy/rainier/internal/cli"
)

// connectionServer stands in for a hosted edge's connect surface. It serves
// GET /v0/workspaces and GET|PATCH /v0/connections/*, and records every PATCH
// body it was sent — which is what the preservation tests assert on, since the
// whole risk in a replace-the-set API is in the body the client builds.
type connectionServer struct {
	t          *testing.T
	workspaces string
	connection *connectionView

	// patched is every PATCH body received, in order.
	patched []map[string]any
	// status, when set, is returned for PATCH instead of applying it.
	status int
	// listStatus, when set, is returned for GET /v0/connections.
	listStatus int
	// afterGet runs once the list response has been written, which is exactly
	// the window a second client's PATCH lands in. It is how the lost-update
	// test reproduces a race deterministically instead of hoping for one.
	afterGet func()
	// afterPatch simulates a concurrent edit before the response is read.
	afterPatch func()
	// patchHeaders records the headers of every PATCH, so a test can assert
	// what precondition the client was able to send — today, none.
	patchHeaders []http.Header
}

func (s *connectionServer) start() *httptest.Server {
	s.t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v0/workspaces":
			io.WriteString(w, s.workspaces)
		case r.URL.Path == "/v0/connections" && r.Method == http.MethodGet:
			if s.listStatus != 0 {
				w.WriteHeader(s.listStatus)
				io.WriteString(w, `{"error":{"code":"not_found","message":"resource not found"}}`)
				return
			}
			out := connectionsEnvelope{Connections: []connectionView{}}
			if s.connection != nil {
				out.Connections = append(out.Connections, *s.connection)
			}
			if err := json.NewEncoder(w).Encode(out); err != nil {
				s.t.Errorf("encode connections: %v", err)
			}
			if s.afterGet != nil {
				s.afterGet()
			}
		case strings.HasPrefix(r.URL.Path, "/v0/connections/") && r.Method == http.MethodPatch:
			s.patchHeaders = append(s.patchHeaders, r.Header.Clone())
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				s.t.Errorf("decode patch: %v", err)
			}
			s.patched = append(s.patched, body)
			if s.status != 0 {
				w.WriteHeader(s.status)
				io.WriteString(w, `{"error":{"code":"conflict","message":"conflict"}}`)
				return
			}
			// Apply the replacement the way the edge does, and answer with the
			// connection as it now stands.
			next := []string{}
			for _, v := range body["workspaces"].([]any) {
				next = append(next, v.(string))
			}
			s.connection.Workspaces = next
			if s.afterPatch != nil {
				s.afterPatch()
			}
			if err := json.NewEncoder(w).Encode(*s.connection); err != nil {
				s.t.Errorf("encode connection: %v", err)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, `{"error":{"code":"not_found","message":"resource not found"}}`)
		}
	}))
	s.t.Cleanup(ts.Close)
	return ts
}

// hostedContext writes a config whose current context is a hosted one — a
// refresh token is what makes it hosted — scoped to workspace.
func hostedContext(t *testing.T, server, workspace string) {
	t.Helper()
	t.Setenv("RAINIER_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	cfg := cli.Config{
		Current: "dogfood",
		Contexts: map[string]cli.Context{"dogfood": {
			Server:       server,
			Token:        "rnr_access_example",
			RefreshToken: "rnr_refresh_example",
			Workspace:    workspace,
			OwnerID:      "usr_1",
		}},
	}
	if err := cli.Save(cfg); err != nil {
		t.Fatal(err)
	}
}

const twoWorkspaces = `{"workspaces":[{"id":"ws_dogfood","name":"Dogfood","role":"owner"},` +
	`{"id":"ws_other","name":"Other","role":"member"}]}`

func TestConnectionLsRendersTheConnectionAndItsWorkspaces(t *testing.T) {
	srv := &connectionServer{t: t, workspaces: twoWorkspaces, connection: &connectionView{
		Provider: "github", Login: "octocat", AccessMode: "selected",
		Workspaces: []string{"ws_dogfood"}, CreatedAt: "2026-09-01T00:00:00Z",
	}}
	ts := srv.start()
	hostedContext(t, ts.URL, "ws_dogfood")

	out, err := captureStdout(t, func() error { return runConnectionLs(nil) })
	if err != nil {
		t.Fatalf("connection ls: %v", err)
	}
	for _, want := range []string{"PROVIDER", "LOGIN", "ACCESS", "WORKSPACES", "github", "octocat", "selected", "ws_dogfood"} {
		if !strings.Contains(out, want) {
			t.Errorf("connection ls missing %q:\n%s", want, out)
		}
	}
	// The current workspace is shared, so there is no warning to print.
	if strings.Contains(out, "not shared") {
		t.Errorf("connection ls warned about a shared workspace:\n%s", out)
	}
}

// The failure this catches is the expensive one: a connection that exists,
// looks complete in the table, and cannot clone anything because it reaches no
// workspace. The row alone does not say that, so the command must.
func TestConnectionLsSaysWhenNothingIsShared(t *testing.T) {
	srv := &connectionServer{t: t, workspaces: twoWorkspaces, connection: &connectionView{
		Provider: "github", Login: "octocat", AccessMode: "selected",
		Workspaces: []string{}, CreatedAt: "2026-09-01T00:00:00Z",
	}}
	ts := srv.start()
	hostedContext(t, ts.URL, "ws_dogfood")

	out, err := captureStdout(t, func() error { return runConnectionLs(nil) })
	if err != nil {
		t.Fatalf("connection ls: %v", err)
	}
	if !strings.Contains(out, "shared with no workspace") || !strings.Contains(out, "rainier connection share github") {
		t.Errorf("connection ls did not name the next action:\n%s", out)
	}
}

func TestConnectionLsWarnsWhenTheCurrentWorkspaceIsNotShared(t *testing.T) {
	srv := &connectionServer{t: t, workspaces: twoWorkspaces, connection: &connectionView{
		Provider: "github", Login: "octocat", AccessMode: "selected",
		Workspaces: []string{"ws_other"}, CreatedAt: "2026-09-01T00:00:00Z",
	}}
	ts := srv.start()
	hostedContext(t, ts.URL, "ws_dogfood")

	out, err := captureStdout(t, func() error { return runConnectionLs(nil) })
	if err != nil {
		t.Fatalf("connection ls: %v", err)
	}
	if !strings.Contains(out, "not shared with your current workspace (ws_dogfood)") {
		t.Errorf("connection ls did not warn about the current workspace:\n%s", out)
	}
}

// An account that has not done the browser step gets the browser step, not an
// empty table it has to interpret.
func TestConnectionLsOnNoConnectionNamesTheBrowserStep(t *testing.T) {
	srv := &connectionServer{t: t, workspaces: twoWorkspaces}
	ts := srv.start()
	hostedContext(t, ts.URL, "ws_dogfood")

	out, err := captureStdout(t, func() error { return runConnectionLs(nil) })
	if err != nil {
		t.Fatalf("connection ls: %v", err)
	}
	if !strings.Contains(out, "GitHub is not connected") || !strings.Contains(out, "Connect GitHub") {
		t.Errorf("connection ls did not name the browser step:\n%s", out)
	}
	if !strings.Contains(out, ts.URL) {
		t.Errorf("connection ls did not name the server to log in to:\n%s", out)
	}
}

// The load-bearing test for this whole change: sharing one workspace must send
// the union, never just the new one. A PATCH carrying ["ws_dogfood"] alone
// would silently drop ws_other's access.
func TestConnectionSharePreservesTheOtherWorkspaces(t *testing.T) {
	srv := &connectionServer{t: t, workspaces: twoWorkspaces, connection: &connectionView{
		Provider: "github", Login: "octocat", AccessMode: "selected",
		Workspaces: []string{"ws_other"}, CreatedAt: "2026-09-01T00:00:00Z",
	}}
	ts := srv.start()
	hostedContext(t, ts.URL, "ws_dogfood")

	out, err := captureStdout(t, func() error { return runConnectionShare([]string{"github"}, true) })
	if err != nil {
		t.Fatalf("connection share: %v", err)
	}
	if len(srv.patched) != 1 {
		t.Fatalf("PATCH count = %d, want 1", len(srv.patched))
	}
	got := srv.patched[0]
	if diff := workspaceSet(t, got); !equalSets(diff, []string{"ws_dogfood", "ws_other"}) {
		t.Errorf("PATCH workspaces = %v, want both ws_dogfood and ws_other", diff)
	}
	// access_mode must not be in the body at all: the edge applies each field
	// only when present, and a share that restated the mode could reset it.
	if _, ok := got["access_mode"]; ok {
		t.Errorf("PATCH carried access_mode %v; sharing must never touch the mode", got["access_mode"])
	}
	if !strings.Contains(out, "ws_dogfood") || !strings.Contains(out, "Dogfood") {
		t.Errorf("share did not name the workspace it changed:\n%s", out)
	}
}

func TestConnectionUnsharePreservesTheOtherWorkspaces(t *testing.T) {
	srv := &connectionServer{t: t, workspaces: twoWorkspaces, connection: &connectionView{
		Provider: "github", Login: "octocat", AccessMode: "selected",
		Workspaces: []string{"ws_dogfood", "ws_other"}, CreatedAt: "2026-09-01T00:00:00Z",
	}}
	ts := srv.start()
	hostedContext(t, ts.URL, "ws_dogfood")

	if _, err := captureStdout(t, func() error { return runConnectionShare([]string{"github"}, false) }); err != nil {
		t.Fatalf("connection unshare: %v", err)
	}
	if len(srv.patched) != 1 {
		t.Fatalf("PATCH count = %d, want 1", len(srv.patched))
	}
	if got := workspaceSet(t, srv.patched[0]); !equalSets(got, []string{"ws_other"}) {
		t.Errorf("PATCH workspaces = %v, want ws_other alone", got)
	}
}

// --workspace acts on a workspace other than the current one, and leaves the
// current one's access alone.
func TestConnectionShareNamedWorkspace(t *testing.T) {
	srv := &connectionServer{t: t, workspaces: twoWorkspaces, connection: &connectionView{
		Provider: "github", Login: "octocat", AccessMode: "selected",
		Workspaces: []string{"ws_dogfood"}, CreatedAt: "2026-09-01T00:00:00Z",
	}}
	ts := srv.start()
	hostedContext(t, ts.URL, "ws_dogfood")

	if _, err := captureStdout(t, func() error {
		return runConnectionShare([]string{"github", "--workspace", "ws_other"}, true)
	}); err != nil {
		t.Fatalf("connection share: %v", err)
	}
	if got := workspaceSet(t, srv.patched[0]); !equalSets(got, []string{"ws_dogfood", "ws_other"}) {
		t.Errorf("PATCH workspaces = %v, want both", got)
	}
}

// Re-sharing what is already shared writes nothing. It is not just noise: every
// PATCH is a full replacement, so the safest no-op is the one that never sends
// a set at all.
func TestConnectionShareIsANoOpWhenAlreadyShared(t *testing.T) {
	srv := &connectionServer{t: t, workspaces: twoWorkspaces, connection: &connectionView{
		Provider: "github", Login: "octocat", AccessMode: "selected",
		Workspaces: []string{"ws_dogfood"}, CreatedAt: "2026-09-01T00:00:00Z",
	}}
	ts := srv.start()
	hostedContext(t, ts.URL, "ws_dogfood")

	out, err := captureStdout(t, func() error { return runConnectionShare([]string{"github"}, true) })
	if err != nil {
		t.Fatalf("connection share: %v", err)
	}
	if len(srv.patched) != 0 {
		t.Errorf("PATCH sent %d times for a no-op share", len(srv.patched))
	}
	if !strings.Contains(out, "already shared") {
		t.Errorf("share did not say it was already shared:\n%s", out)
	}
}

func TestConnectionUnshareIsANoOpWhenNotShared(t *testing.T) {
	srv := &connectionServer{t: t, workspaces: twoWorkspaces, connection: &connectionView{
		Provider: "github", Login: "octocat", AccessMode: "selected",
		Workspaces: []string{"ws_other"}, CreatedAt: "2026-09-01T00:00:00Z",
	}}
	ts := srv.start()
	hostedContext(t, ts.URL, "ws_dogfood")

	out, err := captureStdout(t, func() error { return runConnectionShare([]string{"github"}, false) })
	if err != nil {
		t.Fatalf("connection unshare: %v", err)
	}
	if len(srv.patched) != 0 {
		t.Errorf("PATCH sent %d times for a no-op unshare", len(srv.patched))
	}
	if !strings.Contains(out, "is not shared with") {
		t.Errorf("unshare did not say it was not shared:\n%s", out)
	}
}

// An "all" connection is not limited by its selection, so editing the selection
// would report a change in reach that is not happening. Share says so and does
// nothing; unshare refuses rather than pretending to narrow.
func TestConnectionShareOnAllAccessWritesNothing(t *testing.T) {
	srv := &connectionServer{t: t, workspaces: twoWorkspaces, connection: &connectionView{
		Provider: "github", Login: "octocat", AccessMode: "all",
		Workspaces: []string{}, CreatedAt: "2026-09-01T00:00:00Z",
	}}
	ts := srv.start()
	hostedContext(t, ts.URL, "ws_dogfood")

	out, err := captureStdout(t, func() error { return runConnectionShare([]string{"github"}, true) })
	if err != nil {
		t.Fatalf("connection share: %v", err)
	}
	if len(srv.patched) != 0 {
		t.Errorf("PATCH sent %d times against an all-access connection", len(srv.patched))
	}
	if !strings.Contains(out, "already shared with all your workspaces") {
		t.Errorf("share did not explain the all mode:\n%s", out)
	}
}

func TestConnectionUnshareOnAllAccessRefusesAndExplains(t *testing.T) {
	srv := &connectionServer{t: t, workspaces: twoWorkspaces, connection: &connectionView{
		Provider: "github", Login: "octocat", AccessMode: "all",
		Workspaces: []string{}, CreatedAt: "2026-09-01T00:00:00Z",
	}}
	ts := srv.start()
	hostedContext(t, ts.URL, "ws_dogfood")

	_, err := captureStdout(t, func() error { return runConnectionShare([]string{"github"}, false) })
	if err == nil {
		t.Fatal("unshare against an all-access connection succeeded; want a refusal")
	}
	if len(srv.patched) != 0 {
		t.Errorf("PATCH sent %d times against an all-access connection", len(srv.patched))
	}
	if !strings.Contains(err.Error(), "access_mode") {
		t.Errorf("unshare refusal did not name the way out: %v", err)
	}
}

// The selection is intent, not authorization, so the server would happily store
// a workspace the caller cannot reach and deliver to none of it. Catch it here.
func TestConnectionShareRefusesAWorkspaceYouAreNotIn(t *testing.T) {
	srv := &connectionServer{t: t, workspaces: twoWorkspaces, connection: &connectionView{
		Provider: "github", Login: "octocat", AccessMode: "selected",
		Workspaces: []string{}, CreatedAt: "2026-09-01T00:00:00Z",
	}}
	ts := srv.start()
	hostedContext(t, ts.URL, "ws_dogfood")

	_, err := captureStdout(t, func() error {
		return runConnectionShare([]string{"github", "--workspace", "ws_someone_else"}, true)
	})
	if err == nil {
		t.Fatal("share into a foreign workspace succeeded; want a refusal")
	}
	if !strings.Contains(err.Error(), "not a member") || !strings.Contains(err.Error(), "ws_dogfood") {
		t.Errorf("refusal did not say what you are a member of: %v", err)
	}
	if len(srv.patched) != 0 {
		t.Errorf("PATCH sent for a workspace the caller is not in")
	}
}

// A disconnect that lands between the read and the write comes back as the
// edge's contentless conflict. The CLI knows what it asked for, so it supplies
// the meaning the server deliberately withholds.
func TestConnectionShareTranslatesAConflictIntoReconnectAdvice(t *testing.T) {
	srv := &connectionServer{t: t, workspaces: twoWorkspaces, status: http.StatusConflict, connection: &connectionView{
		Provider: "github", Login: "octocat", AccessMode: "selected",
		Workspaces: []string{}, CreatedAt: "2026-09-01T00:00:00Z",
	}}
	ts := srv.start()
	hostedContext(t, ts.URL, "ws_dogfood")

	_, err := captureStdout(t, func() error { return runConnectionShare([]string{"github"}, true) })
	if err == nil {
		t.Fatal("share against a revoked connection succeeded; want a refusal")
	}
	if !strings.Contains(err.Error(), "disconnected") || !strings.Contains(err.Error(), "Connect GitHub") {
		t.Errorf("conflict was not translated into reconnect advice: %v", err)
	}
}

// A server with no connect surface at all — a self-hosted controld, or a hosted
// edge with the feature off — is an operator's problem, not a person's.
func TestConnectionLsOnAServerWithoutTheSurface(t *testing.T) {
	srv := &connectionServer{t: t, workspaces: twoWorkspaces, listStatus: http.StatusNotFound}
	ts := srv.start()
	hostedContext(t, ts.URL, "ws_dogfood")

	_, err := captureStdout(t, func() error { return runConnectionLs(nil) })
	if err == nil {
		t.Fatal("connection ls against a server with no surface succeeded")
	}
	if !strings.Contains(err.Error(), "no provider connections") {
		t.Errorf("unhelpful message for a server with no connect surface: %v", err)
	}
}

// Not logged in at all is the first thing a person hits, and the message has to
// name the command that fixes it.
func TestConnectionRequiresLogin(t *testing.T) {
	t.Setenv("RAINIER_CONFIG", filepath.Join(t.TempDir(), "absent.json"))
	for _, run := range []func() error{
		func() error { return runConnectionLs(nil) },
		func() error { return runConnectionShare([]string{"github"}, true) },
	} {
		_, err := captureStdout(t, run)
		if err == nil || !strings.Contains(err.Error(), "not logged in") {
			t.Errorf("want a not-logged-in error, got %v", err)
		}
	}
}

// A self-hosted context has no connections; saying so beats a 404 from a route
// that was never going to exist.
func TestConnectionShareRefusesASelfHostedContext(t *testing.T) {
	t.Setenv("RAINIER_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	if err := cli.Save(cli.Config{ServerURL: "http://127.0.0.1:1", Token: "rnr_test"}); err != nil {
		t.Fatal(err)
	}
	_, err := captureStdout(t, func() error { return runConnectionShare([]string{"github"}, true) })
	if err == nil || !strings.Contains(err.Error(), "not a hosted one") {
		t.Errorf("want a not-hosted error, got %v", err)
	}
}

func TestConnectionShareRejectsAnUnknownProvider(t *testing.T) {
	if _, err := connectionProviderNamed("gitlab", "share"); err == nil ||
		!strings.Contains(err.Error(), "unknown provider") {
		t.Errorf("want an unknown-provider error, got %v", err)
	}
	if _, err := connectionProviderNamed("", "share"); err == nil || !strings.Contains(err.Error(), "usage:") {
		t.Errorf("want a usage error for a missing provider, got %v", err)
	}
	if got, err := connectionProviderNamed("github", "share"); err != nil || got != "github" {
		t.Errorf("connectionProviderNamed(github) = %q, %v", got, err)
	}
}

// runCreds against a hosted context points at the command that owns the
// question instead of round-tripping to a route the edge does not serve.
func TestCredsOnAHostedContextPointsAtConnection(t *testing.T) {
	hostedContext(t, "http://127.0.0.1:1", "ws_dogfood")
	_, err := captureStdout(t, func() error { return runCreds(nil) })
	if err == nil || !strings.Contains(err.Error(), "rainier connection ls") {
		t.Errorf("want creds to redirect to connection ls, got %v", err)
	}
}

func TestApplyWorkspaceSelection(t *testing.T) {
	for _, tc := range []struct {
		name    string
		current []string
		id      string
		add     bool
		want    []string
		changed bool
	}{
		{"add to empty", nil, "ws_a", true, []string{"ws_a"}, true},
		{"add keeps others", []string{"ws_b"}, "ws_a", true, []string{"ws_a", "ws_b"}, true},
		{"add already present", []string{"ws_a", "ws_b"}, "ws_a", true, []string{"ws_a", "ws_b"}, false},
		{"remove keeps others", []string{"ws_a", "ws_b"}, "ws_a", false, []string{"ws_b"}, true},
		{"remove absent", []string{"ws_b"}, "ws_a", false, []string{"ws_b"}, false},
		{"remove last", []string{"ws_a"}, "ws_a", false, []string{}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, changed := applyWorkspaceSelection(tc.current, tc.id, tc.add)
			if changed != tc.changed {
				t.Errorf("changed = %v, want %v", changed, tc.changed)
			}
			if !equalSets(got, tc.want) {
				t.Errorf("selection = %v, want %v", got, tc.want)
			}
		})
	}
}

// No output or error text this command produces may contain anything
// token-shaped. The connection surface never sends one, and the CLI holds an
// access token of its own that must not leak into a message either.
func TestConnectionOutputCarriesNoCredential(t *testing.T) {
	srv := &connectionServer{t: t, workspaces: twoWorkspaces, connection: &connectionView{
		Provider: "github", Login: "octocat", AccessMode: "selected",
		Workspaces: []string{"ws_other"}, CreatedAt: "2026-09-01T00:00:00Z",
	}}
	ts := srv.start()
	hostedContext(t, ts.URL, "ws_dogfood")

	var text strings.Builder
	for _, run := range []func() error{
		func() error { return runConnectionLs(nil) },
		func() error { return runConnectionShare([]string{"github"}, true) },
		func() error { return runConnectionShare([]string{"github"}, false) },
	} {
		out, err := captureStdout(t, run)
		text.WriteString(out)
		if err != nil {
			text.WriteString(err.Error())
		}
	}
	// The synthetic values this test's config and the plan's fixtures use.
	for _, secret := range []string{"rnr_access_example", "rnr_refresh_example", "ghu_", "ghr_", "gho_", "client_secret"} {
		if strings.Contains(text.String(), secret) {
			t.Errorf("connection output contains %q:\n%s", secret, text.String())
		}
	}
}

func workspaceSet(t *testing.T, body map[string]any) []string {
	t.Helper()
	raw, ok := body["workspaces"]
	if !ok {
		t.Fatalf("PATCH body has no workspaces field: %v", body)
	}
	list, ok := raw.([]any)
	if !ok {
		t.Fatalf("PATCH workspaces is not a list: %v", raw)
	}
	out := make([]string, 0, len(list))
	for _, v := range list {
		out = append(out, v.(string))
	}
	return out
}

func equalSets(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := map[string]int{}
	for _, v := range a {
		seen[v]++
	}
	for _, v := range b {
		seen[v]--
		if seen[v] < 0 {
			return false
		}
	}
	return true
}

// Removing the last workspace must send an empty JSON list, not null. The edge
// distinguishes an absent field (leave the selection alone) from a present one
// (replace it), so a null here would be read as "no selection field" by a
// stricter decoder and quietly leave the workspace shared.
func TestConnectionUnshareTheLastWorkspaceSendsAnEmptyList(t *testing.T) {
	srv := &connectionServer{t: t, workspaces: twoWorkspaces, connection: &connectionView{
		Provider: "github", Login: "octocat", AccessMode: "selected",
		Workspaces: []string{"ws_dogfood"}, CreatedAt: "2026-09-01T00:00:00Z",
	}}
	ts := srv.start()
	hostedContext(t, ts.URL, "ws_dogfood")

	if _, err := captureStdout(t, func() error { return runConnectionShare([]string{"github"}, false) }); err != nil {
		t.Fatalf("connection unshare: %v", err)
	}
	if len(srv.patched) != 1 {
		t.Fatalf("PATCH count = %d, want 1", len(srv.patched))
	}
	raw, ok := srv.patched[0]["workspaces"]
	if !ok {
		t.Fatal("PATCH omitted the workspaces field entirely")
	}
	list, ok := raw.([]any)
	if !ok {
		t.Fatalf("PATCH workspaces = %#v, want an empty JSON list", raw)
	}
	if len(list) != 0 {
		t.Errorf("PATCH workspaces = %v, want empty", list)
	}
}

// The flag may precede the positional; reorderArgs is what makes that work, and
// a person types it both ways.
func TestConnectionShareAcceptsTheFlagBeforeTheProvider(t *testing.T) {
	srv := &connectionServer{t: t, workspaces: twoWorkspaces, connection: &connectionView{
		Provider: "github", Login: "octocat", AccessMode: "selected",
		Workspaces: []string{}, CreatedAt: "2026-09-01T00:00:00Z",
	}}
	ts := srv.start()
	hostedContext(t, ts.URL, "ws_dogfood")

	if _, err := captureStdout(t, func() error {
		return runConnectionShare([]string{"--workspace", "ws_other", "github"}, true)
	}); err != nil {
		t.Fatalf("connection share: %v", err)
	}
	if got := workspaceSet(t, srv.patched[0]); !equalSets(got, []string{"ws_other"}) {
		t.Errorf("PATCH workspaces = %v, want ws_other", got)
	}
}

// --------------------------------------------------------------------------
// What the read-modify-write actually guarantees
// --------------------------------------------------------------------------
//
// share and unshare are GET /v0/connections, edit one entry, PATCH the whole
// set. That preserves every grant present in the snapshot it read. It does NOT
// preserve a grant another client adds after that read: PATCH replaces the
// selection outright, and the API offers no precondition to make the write
// conditional on the snapshot still being current.
//
// These stub tests record the current client behavior, not the hosted server
// contract. Adding a validator to the real API will not make them fail; that
// change needs server contract tests and corresponding client support.

// A workspace another client shares while our share is in flight is dropped by
// our PATCH. This is a real lost update, not a theoretical one — and the point
// of the test is that the CLI cannot currently prevent it, so the behavior is
// recorded rather than claimed to be safe.
func TestConnectionShareLosesAConcurrentUpdate(t *testing.T) {
	srv := &connectionServer{t: t, workspaces: twoWorkspaces, connection: &connectionView{
		Provider: "github", Login: "octocat", AccessMode: "selected",
		Workspaces: []string{}, CreatedAt: "2026-09-01T00:00:00Z",
	}}
	// A second client shares ws_other in the window between our read and our
	// write. Our PATCH was already built from the empty snapshot.
	srv.afterGet = func() {
		srv.connection.Workspaces = []string{"ws_other"}
		srv.afterGet = nil
	}
	ts := srv.start()
	hostedContext(t, ts.URL, "ws_dogfood")

	if _, err := captureStdout(t, func() error { return runConnectionShare([]string{"github"}, true) }); err != nil {
		t.Fatalf("connection share: %v", err)
	}
	if len(srv.patched) != 1 {
		t.Fatalf("PATCH count = %d, want 1", len(srv.patched))
	}
	// The PATCH carries the stale snapshot plus our entry, and nothing else:
	// ws_other, shared moments earlier, is not in the set being written, so the
	// write removes it. The CLI had no way to know and no way to prevent it.
	got := workspaceSet(t, srv.patched[0])
	if !equalSets(got, []string{"ws_dogfood"}) {
		t.Fatalf("PATCH workspaces = %v, want exactly [ws_dogfood]. This test documents "+
			"a lost update; if the CLI now preserves ws_other, the API gained a "+
			"precondition and this test should assert that guarantee instead", got)
	}
}

// With a validator-free stub response, the client sends only a workspace
// replacement. This test makes no assertion about the real server contract.
func TestConnectionPatchRequestShapeWithoutValidator(t *testing.T) {
	srv := &connectionServer{t: t, workspaces: twoWorkspaces, connection: &connectionView{
		Provider: "github", Login: "octocat", AccessMode: "selected",
		Workspaces: []string{}, CreatedAt: "2026-09-01T00:00:00Z",
	}}
	ts := srv.start()
	hostedContext(t, ts.URL, "ws_dogfood")

	if _, err := captureStdout(t, func() error { return runConnectionShare([]string{"github"}, true) }); err != nil {
		t.Fatalf("connection share: %v", err)
	}
	if len(srv.patchHeaders) != 1 {
		t.Fatalf("PATCH count = %d, want 1", len(srv.patchHeaders))
	}
	// GET /v0/connections returns no validator, so there is nothing to echo.
	// Asserting the absence keeps the limitation visible in the test suite.
	for _, header := range []string{"If-Match", "If-Unmodified-Since", "If-None-Match"} {
		if v := srv.patchHeaders[0].Get(header); v != "" {
			t.Errorf("PATCH unexpectedly carried %s: %q", header, v)
		}
	}
	// The outgoing body contains only the workspace replacement.
	for _, field := range []string{"version", "revision", "etag", "updated_at"} {
		if _, ok := srv.patched[0][field]; ok {
			t.Errorf("PATCH body carried a %q precondition field", field)
		}
	}
}

// --------------------------------------------------------------------------
// Compatibility with the config-locking rework (origin/main 500a243)
// --------------------------------------------------------------------------
//
// That change moved every read-modify-write of the config file behind
// cli.UpdateConfig, because loading a Config and saving that stale copy could
// overwrite a sibling process's freshly rotated token pair. The connection
// commands are compatible by construction: they only ever READ config
// (requireLogin, Config.Active) and never call Save or UpdateConfig. These
// tests hold that line, since the failure mode — a share silently reverting
// another terminal's login or workspace switch — is invisible until it bites.

// With valid access tokens, these commands leave config byte-identical.
// Authentication recovery may legitimately persist rotated tokens via Client.
func TestConnectionCommandsWithValidTokensDoNotWriteConfig(t *testing.T) {
	srv := &connectionServer{t: t, workspaces: twoWorkspaces, connection: &connectionView{
		Provider: "github", Login: "octocat", AccessMode: "selected",
		Workspaces: []string{"ws_other"}, CreatedAt: "2026-09-01T00:00:00Z",
	}}
	ts := srv.start()
	hostedContext(t, ts.URL, "ws_dogfood")

	path := os.Getenv("RAINIER_CONFIG")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, run := range []func() error{
		func() error { return runConnectionLs(nil) },
		func() error { return runConnectionShare([]string{"github"}, true) },
		func() error { return runConnectionShare([]string{"github"}, false) },
	} {
		if _, err := captureStdout(t, run); err != nil {
			t.Fatalf("connection command: %v", err)
		}
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Errorf("a connection command rewrote the config file.\nbefore:\n%s\nafter:\n%s", before, after)
	}
}

// The sharper version: a sibling CLI rotates the token pair and switches the
// workspace while our share is mid-flight. Our command must not put the values
// it loaded at startup back on disk — that is precisely the regression
// cli.UpdateConfig exists to prevent, and a command that never writes cannot
// cause it.
func TestConnectionShareDoesNotRevertASiblingsNewerLogin(t *testing.T) {
	srv := &connectionServer{t: t, workspaces: twoWorkspaces, connection: &connectionView{
		Provider: "github", Login: "octocat", AccessMode: "selected",
		Workspaces: []string{}, CreatedAt: "2026-09-01T00:00:00Z",
	}}
	ts := srv.start()
	hostedContext(t, ts.URL, "ws_dogfood")

	// Between our read and our write, another terminal refreshes and moves to
	// a different workspace — both legitimate, both newer than what we hold.
	srv.afterGet = func() {
		if err := cli.UpdateConfig(func(c *cli.Config) error {
			ctx := c.Contexts["dogfood"]
			ctx.Token = "rnr_access_rotated_by_sibling"
			ctx.RefreshToken = "rnr_refresh_rotated_by_sibling"
			ctx.Workspace = "ws_other"
			c.UpdateContext("dogfood", ctx)
			return nil
		}); err != nil {
			t.Errorf("sibling update: %v", err)
		}
		srv.afterGet = nil
	}

	if _, err := captureStdout(t, func() error { return runConnectionShare([]string{"github"}, true) }); err != nil {
		t.Fatalf("connection share: %v", err)
	}

	cfg, err := cli.Load()
	if err != nil {
		t.Fatal(err)
	}
	ctx := cfg.Contexts["dogfood"]
	if ctx.Token != "rnr_access_rotated_by_sibling" || ctx.RefreshToken != "rnr_refresh_rotated_by_sibling" {
		t.Errorf("share reverted the sibling's rotated credentials: token=%q refresh=%q", ctx.Token, ctx.RefreshToken)
	}
	if ctx.Workspace != "ws_other" {
		t.Errorf("share reverted the sibling's workspace selection: %q, want ws_other", ctx.Workspace)
	}
	// And the share still applied to the workspace the command was resolved
	// against, not the one the sibling moved to mid-flight.
	if got := workspaceSet(t, srv.patched[0]); !equalSets(got, []string{"ws_dogfood"}) {
		t.Errorf("PATCH workspaces = %v, want [ws_dogfood]", got)
	}
}

func TestConnectionUnshareAfterLeavingWorkspace(t *testing.T) {
	for _, memberships := range []string{`{"workspaces":[]}`, twoWorkspaces} {
		t.Run(memberships, func(t *testing.T) {
			srv := &connectionServer{t: t, workspaces: memberships, connection: &connectionView{
				Provider: "github", Login: "octocat", AccessMode: "selected",
				Workspaces: []string{"ws_left", "ws_other"},
			}}
			ts := srv.start()
			hostedContext(t, ts.URL, "ws_left")
			_, err := captureStdout(t, func() error { return runConnectionShare([]string{"github", "--workspace", "ws_left"}, false) })
			if err != nil {
				t.Fatalf("unshare after leaving: %v", err)
			}
			if !equalSets(srv.connection.Workspaces, []string{"ws_other"}) {
				t.Fatalf("selection = %v, want [ws_other]", srv.connection.Workspaces)
			}
		})
	}
}

func TestConnectionEditChecksConfirmedState(t *testing.T) {
	for _, tc := range []struct {
		name       string
		add        bool
		mode       string
		workspaces []string
	}{
		{"unshare switched to all", false, "all", []string{"ws_other"}},
		{"unshare restored concurrently", false, "selected", []string{"ws_dogfood", "ws_other"}},
		{"share removed concurrently", true, "selected", []string{"ws_other"}},
		{"unknown response mode", false, "future", []string{"ws_other"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current := []string{"ws_dogfood", "ws_other"}
			if tc.add {
				current = []string{"ws_other"}
			}
			srv := &connectionServer{t: t, workspaces: twoWorkspaces, connection: &connectionView{
				Provider: "github", Login: "octocat", AccessMode: "selected", Workspaces: current,
			}}
			srv.afterPatch = func() { srv.connection.AccessMode = tc.mode; srv.connection.Workspaces = tc.workspaces }
			ts := srv.start()
			hostedContext(t, ts.URL, "ws_dogfood")
			out, err := captureStdout(t, func() error { return runConnectionShare([]string{"github"}, tc.add) })
			if err == nil {
				t.Fatalf("reported success for unconfirmed edit: %s", out)
			}
			if !strings.Contains(err.Error(), "rainier connection ls") {
				t.Errorf("missing recovery guidance: %v", err)
			}
			if strings.Contains(out, "stopped ") || strings.Contains(out, "shared your ") {
				t.Errorf("false success: %s", out)
			}
		})
	}
}

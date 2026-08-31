package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"rainier/internal/cli"
	"rainier/internal/wire"
	"rainier/internal/xfer"
)

// ---------------------------------------------------------------------------
// secret set: the stdin path (the one that keeps values out of shell history)
// ---------------------------------------------------------------------------

// TestReadSecretFromStdin pins exactly how much of a piped value is kept: a
// single trailing newline is the shell's, not the secret's, and everything
// else — interior newlines, leading and trailing spaces, blank lines before
// the last — belongs to the value and must survive verbatim. A secret that
// silently gains or loses a byte here fails much later, as an unexplainable
// 401 from whatever service it was for.
func TestReadSecretFromStdin(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		{"no trailing newline", "hunter2", "hunter2"},
		{"one trailing newline is stripped", "hunter2\n", "hunter2"},
		{"a trailing CRLF is stripped", "hunter2\r\n", "hunter2"},
		{"only the final newline is stripped", "line1\nline2\n\n", "line1\nline2\n"},
		{"interior and edge spaces are preserved", "  spaced value  \n", "  spaced value  "},
		{"a PEM keeps its internal newlines", "-----BEGIN-----\nabc\n-----END-----\n", "-----BEGIN-----\nabc\n-----END-----"},
		{"empty stdin", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatalf("os.Pipe: %v", err)
			}
			orig := os.Stdin
			os.Stdin = r
			t.Cleanup(func() { os.Stdin = orig })

			go func() {
				w.WriteString(tc.in)
				w.Close()
			}()

			got, err := readSecretFromStdin()
			if err != nil {
				t.Fatalf("readSecretFromStdin: %v", err)
			}
			if got != tc.want {
				t.Fatalf("readSecretFromStdin() = %q, want %q", got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// resolveSessionID — review round 1, finding 3: names are unique only
// per-owner, but GET /v0/sessions is team-visible, so two teammates can
// share a name.
// ---------------------------------------------------------------------------

// pagedSessions serves GET /v0/sessions from a fixed set of pages, keyed by
// the "cursor" query param ("" for the first page). Every test in this file
// uses it so the fixture stays in one place.
func pagedSessions(t *testing.T, pages map[string]sessionsEnvelope) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page, ok := pages[r.URL.Query().Get("cursor")]
		if !ok {
			t.Fatalf("unexpected cursor %q", r.URL.Query().Get("cursor"))
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(page)
	}))
	t.Cleanup(ts.Close)
	return ts
}

// TestResolveSessionIDAmbiguousNameAcrossOwners is the finding's mandated
// test: two owners ("alice", "bob") each have a session named "dev-box".
// Without a known owner id the CLI must refuse rather than silently acting
// on whichever paginated first; with one — the caller's own id, cached by
// login from the identity controld returns — it must prefer the caller's.
func TestResolveSessionIDAmbiguousNameAcrossOwners(t *testing.T) {
	ts := pagedSessions(t, map[string]sessionsEnvelope{
		"": {Sessions: []session{
			{ID: "sess_alice1", Name: "dev-box", OwnerID: "usr_alice", State: "running"},
			{ID: "sess_bob1", Name: "dev-box", OwnerID: "usr_bob", State: "running"},
		}},
	})
	c := &cli.Client{Base: ts.URL}

	t.Run("no owner id known: ambiguous, both matches listed", func(t *testing.T) {
		id, err := resolveSessionID(c, "", "dev-box")
		if err == nil {
			t.Fatalf("resolveSessionID = (%q, nil), want an ambiguous-name error", id)
		}
		msg := err.Error()
		if !strings.Contains(msg, "ambiguous name \"dev-box\"") {
			t.Errorf("error = %q, want it to name the ambiguous ref", msg)
		}
		if !strings.Contains(msg, "sess_alice1") || !strings.Contains(msg, "usr_alice") {
			t.Errorf("error = %q, want it to list sess_alice1 (owner usr_alice)", msg)
		}
		if !strings.Contains(msg, "sess_bob1") || !strings.Contains(msg, "usr_bob") {
			t.Errorf("error = %q, want it to list sess_bob1 (owner usr_bob)", msg)
		}
		if !strings.Contains(msg, "use the session id") {
			t.Errorf("error = %q, want it to tell the caller to use the session id", msg)
		}
	})

	t.Run("owner preference resolves it when exactly one match is the caller's own", func(t *testing.T) {
		id, err := resolveSessionID(c, "usr_bob", "dev-box")
		if err != nil {
			t.Fatalf("resolveSessionID: %v", err)
		}
		if id != "sess_bob1" {
			t.Fatalf("resolveSessionID = %q, want sess_bob1 (bob's own)", id)
		}
	})

	t.Run("an owner id that owns none of the matches is still ambiguous", func(t *testing.T) {
		_, err := resolveSessionID(c, "usr_carol", "dev-box")
		if err == nil {
			t.Fatal("resolveSessionID: want an ambiguous-name error, got nil")
		}
		if !strings.Contains(err.Error(), "ambiguous name") {
			t.Fatalf("error = %q, want an ambiguous-name error", err.Error())
		}
	})

	t.Run("sess_ prefix is used verbatim, no lookup performed", func(t *testing.T) {
		id, err := resolveSessionID(c, "", "sess_untouched")
		if err != nil || id != "sess_untouched" {
			t.Fatalf("resolveSessionID(sess_untouched) = (%q, %v), want (sess_untouched, nil)", id, err)
		}
	})
}

// TestResolveSessionIDCollectsMatchesAcrossPages asserts a match on page 1
// does not short-circuit the search — a second match on a later page must
// still be found and folded into the ambiguity decision. This is the
// regression the old "return on first match" implementation missed.
func TestResolveSessionIDCollectsMatchesAcrossPages(t *testing.T) {
	ts := pagedSessions(t, map[string]sessionsEnvelope{
		"": {
			Sessions:   []session{{ID: "sess_alice1", Name: "dev-box", OwnerID: "usr_alice", State: "running"}},
			NextCursor: "page2",
		},
		"page2": {
			Sessions: []session{{ID: "sess_bob1", Name: "dev-box", OwnerID: "usr_bob", State: "running"}},
		},
	})
	c := &cli.Client{Base: ts.URL}

	_, err := resolveSessionID(c, "", "dev-box")
	if err == nil {
		t.Fatal("resolveSessionID: want an ambiguous-name error spanning both pages, got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "sess_alice1") || !strings.Contains(msg, "sess_bob1") {
		t.Fatalf("error = %q, want it to list matches from both page 1 and page 2", msg)
	}
}

// TestResolveSessionIDUniqueNameNoAmbiguity is the ordinary case: one match,
// no ambiguity machinery involved.
func TestResolveSessionIDUniqueNameNoAmbiguity(t *testing.T) {
	ts := pagedSessions(t, map[string]sessionsEnvelope{
		"": {Sessions: []session{{ID: "sess_only", Name: "solo", OwnerID: "usr_alice", State: "running"}}},
	})
	c := &cli.Client{Base: ts.URL}

	id, err := resolveSessionID(c, "", "solo")
	if err != nil || id != "sess_only" {
		t.Fatalf("resolveSessionID = (%q, %v), want (sess_only, nil)", id, err)
	}
}

// TestResolveSessionIDNotFound asserts a name matching nothing is a plain
// not-found error, not an ambiguity error.
func TestResolveSessionIDNotFound(t *testing.T) {
	ts := pagedSessions(t, map[string]sessionsEnvelope{"": {}})
	c := &cli.Client{Base: ts.URL}

	_, err := resolveSessionID(c, "", "nope")
	if err == nil {
		t.Fatal("resolveSessionID: want a not-found error, got nil")
	}
	if !strings.Contains(err.Error(), `no session named "nope" found`) {
		t.Fatalf("error = %q, want the not-found message", err.Error())
	}
}

func TestAttachNameResolutionIncludesOnlyFailedTerminalRows(t *testing.T) {
	ts := pagedSessions(t, map[string]sessionsEnvelope{
		"": {Sessions: []session{
			{ID: "sess_failed", Name: "diagnostic-box", OwnerID: "usr_synthetic", State: "failed"},
			{ID: "sess_dead", Name: "diagnostic-box", OwnerID: "usr_synthetic", State: "dead"},
			{ID: "sess_destroyed", Name: "diagnostic-box", OwnerID: "usr_synthetic", State: "destroyed"},
		}},
	})
	id, err := resolveSessionIDWithScope(&cli.Client{Base: ts.URL}, "usr_synthetic", "diagnostic-box", resolveAttachable)
	if err != nil {
		t.Fatalf("resolve attachable name: %v", err)
	}
	if id != "sess_failed" {
		t.Fatalf("resolved id = %q, want the failed diagnostic session", id)
	}

	live := pagedSessions(t, map[string]sessionsEnvelope{
		"": {Sessions: []session{
			{ID: "sess_own_failed", Name: "shared-box", OwnerID: "usr_synthetic", State: "failed"},
			{ID: "sess_live", Name: "shared-box", OwnerID: "usr_teammate", State: "running"},
		}},
	})
	id, err = resolveSessionIDWithScope(&cli.Client{Base: live.URL}, "usr_synthetic", "shared-box", resolveAttachable)
	if err != nil {
		t.Fatalf("resolve live attachable name: %v", err)
	}
	if id != "sess_own_failed" {
		t.Fatalf("resolved id = %q, want the caller's own failed diagnostic row", id)
	}
}

// A failed create is hidden from the default list, but it is still a named
// resource the user must be able to remove. This test drives the real rm
// command boundary: it must opt into terminal rows during name resolution
// and then delete the id it found.
func TestRmResolvesALoneFailedSessionByName(t *testing.T) {
	var paths []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.RequestURI())
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Query().Get("all") == "true":
			json.NewEncoder(w).Encode(sessionsEnvelope{Sessions: []session{{
				ID: "sess_failed", Name: "broken", OwnerID: "usr_alice", State: "failed",
			}}})
		case r.Method == http.MethodDelete && r.URL.Path == "/v0/sessions/sess_failed":
			w.WriteHeader(http.StatusNoContent)
		default:
			json.NewEncoder(w).Encode(sessionsEnvelope{})
		}
	}))
	t.Cleanup(ts.Close)

	t.Setenv("RAINIER_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	if err := cli.Save(cli.Config{ServerURL: ts.URL, Token: "rnr_test", OwnerID: "usr_alice"}); err != nil {
		t.Fatal(err)
	}

	out, err := captureStdout(t, func() error { return runRm([]string{"broken"}) })
	if err != nil {
		t.Fatalf("rm failed session by name: %v; requests=%v", err, paths)
	}
	if !strings.Contains(out, "removed sess_failed") {
		t.Fatalf("rm output = %q, want removed sess_failed", out)
	}
	if len(paths) != 2 || paths[0] != "/v0/sessions?all=true&name=broken" || paths[1] != "/v0/sessions/sess_failed" {
		t.Fatalf("requests = %v, want terminal lookup then delete", paths)
	}
}

// Terminal names are reusable. Opting rm into terminal history must not turn
// the ordinary `rm current-name` path ambiguous when an older failed row has
// the same name; the active row remains the safe, unsurprising target.
func TestRmNamePrefersAnActiveSessionOverTerminalHistory(t *testing.T) {
	var deleted string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodDelete {
			deleted = r.URL.Path
			w.WriteHeader(http.StatusNoContent)
			return
		}
		json.NewEncoder(w).Encode(sessionsEnvelope{Sessions: []session{
			{ID: "sess_old", Name: "box", OwnerID: "usr_alice", State: "failed"},
			{ID: "sess_current", Name: "box", OwnerID: "usr_alice", State: "running"},
		}})
	}))
	t.Cleanup(ts.Close)

	t.Setenv("RAINIER_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	if err := cli.Save(cli.Config{ServerURL: ts.URL, Token: "rnr_test", OwnerID: "usr_alice"}); err != nil {
		t.Fatal(err)
	}

	if _, err := captureStdout(t, func() error { return runRm([]string{"box"}) }); err != nil {
		t.Fatalf("rm current session by reused name: %v", err)
	}
	if deleted != "/v0/sessions/sess_current" {
		t.Fatalf("deleted %q, want the active session", deleted)
	}
}

// Owner preference remains the first safety boundary even when rm can see
// terminal history. My failed session must win over a teammate's active row
// with the same team-visible name; otherwise rm would target their id and be
// rejected with 403 instead of cleaning up my leaked container.
func TestRmNamePrefersMyTerminalSessionOverATeammatesActiveSession(t *testing.T) {
	var deleted string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodDelete {
			deleted = r.URL.Path
			w.WriteHeader(http.StatusNoContent)
			return
		}
		json.NewEncoder(w).Encode(sessionsEnvelope{Sessions: []session{
			{ID: "sess_mine", Name: "shared", OwnerID: "usr_alice", State: "failed"},
			{ID: "sess_theirs", Name: "shared", OwnerID: "usr_bob", State: "running"},
		}})
	}))
	t.Cleanup(ts.Close)

	t.Setenv("RAINIER_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	if err := cli.Save(cli.Config{ServerURL: ts.URL, Token: "rnr_test", OwnerID: "usr_alice"}); err != nil {
		t.Fatal(err)
	}

	if _, err := captureStdout(t, func() error { return runRm([]string{"shared"}) }); err != nil {
		t.Fatalf("rm my failed session by shared name: %v", err)
	}
	if deleted != "/v0/sessions/sess_mine" {
		t.Fatalf("deleted %q, want my terminal session", deleted)
	}
}

// A pre-owner-id config cannot safely apply active precedence across owners:
// it does not know which same-name row belongs to the caller. Refuse with the
// established ambiguity error instead of selecting a teammate's active id.
func TestRmWithLegacyConfigDoesNotDiscardTerminalMatchesAcrossOwners(t *testing.T) {
	deleted := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodDelete {
			deleted = true
			w.WriteHeader(http.StatusNoContent)
			return
		}
		json.NewEncoder(w).Encode(sessionsEnvelope{Sessions: []session{
			{ID: "sess_alice", Name: "shared", OwnerID: "usr_alice", State: "failed"},
			{ID: "sess_bob", Name: "shared", OwnerID: "usr_bob", State: "running"},
		}})
	}))
	t.Cleanup(ts.Close)

	t.Setenv("RAINIER_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	if err := cli.Save(cli.Config{ServerURL: ts.URL, Token: "rnr_test"}); err != nil {
		t.Fatal(err)
	}

	_, err := captureStdout(t, func() error { return runRm([]string{"shared"}) })
	if err == nil || !strings.Contains(err.Error(), "ambiguous name") {
		t.Fatalf("rm legacy shared name error = %v, want ambiguity", err)
	}
	if deleted {
		t.Fatal("rm issued DELETE despite cross-owner ambiguity")
	}
}

// Default ls keeps terminal history out of the table, but a failed session
// must not disappear without a clue. The bounded probe asks only whether one
// failed row exists and points the user at the established --all view.
func TestLsHintsWhenFailedSessionsAreHidden(t *testing.T) {
	var paths []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.RequestURI())
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("state") == "failed" {
			json.NewEncoder(w).Encode(sessionsEnvelope{Sessions: []session{{
				ID: "sess_failed", Name: "broken", State: "failed",
			}}})
			return
		}
		json.NewEncoder(w).Encode(sessionsEnvelope{})
	}))
	t.Cleanup(ts.Close)

	t.Setenv("RAINIER_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	if err := cli.Save(cli.Config{ServerURL: ts.URL, Token: "rnr_test"}); err != nil {
		t.Fatal(err)
	}

	out, err := captureStdout(t, func() error { return runLs(nil) })
	if err != nil {
		t.Fatalf("ls: %v; requests=%v", err, paths)
	}
	if !strings.Contains(out, "rainier ls --all") {
		t.Fatalf("ls output = %q, want a --all hint", out)
	}
	if len(paths) != 2 || paths[0] != "/v0/sessions" || paths[1] != "/v0/sessions?all=true&limit=1&state=failed" {
		t.Fatalf("requests = %v, want default list then bounded failed-session probe", paths)
	}
}

func TestLsAllDoesNotPrintTheHiddenFailureHint(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(sessionsEnvelope{Sessions: []session{{
			ID: "sess_failed", Name: "broken", State: "failed",
		}}})
	}))
	t.Cleanup(ts.Close)

	t.Setenv("RAINIER_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	if err := cli.Save(cli.Config{ServerURL: ts.URL, Token: "rnr_test"}); err != nil {
		t.Fatal(err)
	}

	out, err := captureStdout(t, func() error { return runLs([]string{"--all"}) })
	if err != nil {
		t.Fatalf("ls --all: %v", err)
	}
	if strings.Contains(out, "sessions are hidden") {
		t.Fatalf("ls --all output = %q, want no hidden-session hint", out)
	}
}

// The hint is supplemental. A transient failure in its second request must
// not turn a successfully rendered session table into a failed command.
func TestLsIgnoresHiddenFailureProbeErrors(t *testing.T) {
	requests := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.Header().Set("Content-Type", "application/json")
		if requests == 1 {
			json.NewEncoder(w).Encode(sessionsEnvelope{})
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, `{"error":{"code":"internal","message":"synthetic probe failure"}}`)
	}))
	t.Cleanup(ts.Close)

	t.Setenv("RAINIER_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	if err := cli.Save(cli.Config{ServerURL: ts.URL, Token: "rnr_test"}); err != nil {
		t.Fatal(err)
	}

	out, err := captureStdout(t, func() error { return runLs(nil) })
	if err != nil {
		t.Fatalf("ls returned the optional probe error: %v", err)
	}
	if !strings.Contains(out, "ID") || strings.Contains(out, "sessions are hidden") {
		t.Fatalf("ls output = %q, want the table without an unproven hint", out)
	}
}

// ---------------------------------------------------------------------------
// ls columns
// ---------------------------------------------------------------------------

// TestSessionStateCell pins the STATE column. A bare "queued" invites the
// wrong question — is it stuck, is it broken? — so when controld says which
// runner a queued session is waiting on, `ls` says it too, in place.
func TestSessionStateCell(t *testing.T) {
	cases := []struct {
		name string
		in   session
		want string
	}{
		{"running", session{State: "running"}, "running"},
		{"queued with nothing to explain", session{State: "queued"}, "queued"},
		{
			"queued behind a placement pin",
			session{State: "queued", QueueReason: "waiting for runner rainier-gpu"},
			"queued (waiting for runner rainier-gpu)",
		},
		{
			// The session is still up — attachable, holding its slot — and the
			// agent inside it has finished. "running" alone would leave a user
			// watching a session that is never going to print anything again.
			"running with a finished agent",
			session{State: "running", ChildExitCode: intPtr(0)},
			"running (exited 0)",
		},
		{
			"running with a killed agent",
			session{State: "running", ChildExitCode: intPtr(137)},
			"running (exited 137)",
		},
		{
			// A dead session's exit code is the diagnosis, and `ls --all` is
			// where it is read.
			"dead with an exit code",
			session{State: "dead", ChildExitCode: intPtr(1)},
			"dead (exited 1)",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := sessionStateCell(tc.in); got != tc.want {
				t.Fatalf("sessionStateCell = %q, want %q", got, tc.want)
			}
		})
	}
}

// intPtr is the one-liner a nullable int column needs at a test's call site.
func intPtr(n int) *int { return &n }

// TestDashIfEmpty pins the ENV column's empty rendering: a scratch session has
// no environment, and a blank cell reads as a bug rather than as an answer.
func TestDashIfEmpty(t *testing.T) {
	if got := dashIfEmpty(""); got != "-" {
		t.Errorf("dashIfEmpty(\"\") = %q, want -", got)
	}
	if got := dashIfEmpty("dev"); got != "dev" {
		t.Errorf("dashIfEmpty(dev) = %q, want dev", got)
	}
}

// ---------------------------------------------------------------------------
// env: the pure helpers behind `rainier env create|update|ls`
// ---------------------------------------------------------------------------

// TestReadDevcontainer pins the whole of --from-devcontainer's contract: it
// reads ONE field (image) out of a devcontainer.json, and it reports every
// other key that was present so the operator learns exactly what rainier did
// not act on. A devcontainer is a much larger contract than an environment
// (features, mounts, lifecycle hooks); half-honoring it silently would be
// worse than ignoring it loudly.
func TestReadDevcontainer(t *testing.T) {
	const full = `{
	  "name": "go-dev",
	  "image": "mcr.microsoft.com/devcontainers/go:1.22",
	  "features": {"ghcr.io/devcontainers/features/node:1": {}},
	  "postCreateCommand": "go mod download",
	  "remoteUser": "vscode"
	}`

	t.Run("reads .devcontainer/devcontainer.json and names every ignored key", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(dir, ".devcontainer"), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		path := filepath.Join(dir, ".devcontainer", "devcontainer.json")
		if err := os.WriteFile(path, []byte(full), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}

		got, err := readDevcontainer(dir)
		if err != nil {
			t.Fatalf("readDevcontainer: %v", err)
		}
		if got.Image != "mcr.microsoft.com/devcontainers/go:1.22" {
			t.Errorf("image = %q", got.Image)
		}
		if got.Path != path {
			t.Errorf("path = %q, want %q", got.Path, path)
		}
		want := []string{"features", "name", "postCreateCommand", "remoteUser"}
		if !slices.Equal(got.Ignored, want) {
			t.Errorf("ignored = %v, want %v (sorted, and never including image)", got.Ignored, want)
		}
		report := strings.Join(got.report(), "\n")
		for _, key := range want {
			if !strings.Contains(report, key) {
				t.Errorf("report did not name the ignored key %q: %s", key, report)
			}
		}
	})

	t.Run("falls back to a bare devcontainer.json in the directory", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "devcontainer.json"), []byte(`{"image":"img:1"}`), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
		got, err := readDevcontainer(dir)
		if err != nil {
			t.Fatalf("readDevcontainer: %v", err)
		}
		if got.Image != "img:1" || len(got.Ignored) != 0 {
			t.Fatalf("got = %+v, want image img:1 and nothing ignored", got)
		}
	})

	t.Run("a path naming the file itself is read directly", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "custom.json")
		if err := os.WriteFile(path, []byte(`{"image":"img:2"}`), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
		got, err := readDevcontainer(path)
		if err != nil {
			t.Fatalf("readDevcontainer: %v", err)
		}
		if got.Image != "img:2" {
			t.Fatalf("image = %q, want img:2", got.Image)
		}
	})

	t.Run("a devcontainer with no image reports none, and the keys it ignored", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "devcontainer.json"),
			[]byte(`{"build":{"dockerfile":"Dockerfile"},"name":"x"}`), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
		got, err := readDevcontainer(dir)
		if err != nil {
			t.Fatalf("readDevcontainer: %v", err)
		}
		if got.Image != "" {
			t.Errorf("image = %q, want empty", got.Image)
		}
		if !slices.Equal(got.Ignored, []string{"build", "name"}) {
			t.Errorf("ignored = %v, want [build name]", got.Ignored)
		}
	})

	t.Run("a missing file is an error naming where it looked", func(t *testing.T) {
		dir := t.TempDir()
		_, err := readDevcontainer(dir)
		if err == nil {
			t.Fatal("readDevcontainer on an empty dir: want an error, got nil")
		}
		if !strings.Contains(err.Error(), "devcontainer.json") {
			t.Errorf("error = %q, want it to name devcontainer.json", err)
		}
	})

	t.Run("a JSONC file (comments) fails with an explanation, not a parser error alone", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "devcontainer.json"),
			[]byte("{\n // for format details see...\n \"image\":\"img:1\"\n}"), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
		_, err := readDevcontainer(dir)
		if err == nil {
			t.Fatal("readDevcontainer on JSONC: want an error, got nil")
		}
		if !strings.Contains(err.Error(), "comments") || !strings.Contains(err.Error(), "--image") {
			t.Errorf("error = %q, want it to explain comments and point at --image", err)
		}
	})
}

// TestAssembleConnectors pins that --connector-json passes the operator's
// bytes through untouched: the server validates unknown fields, so a CLI
// that re-marshaled them could turn a rejected typo into a silently dropped
// key.
func TestAssembleConnectors(t *testing.T) {
	const gh = `{"type":"github","repo":"acme/widgets"}`
	const browser = `{"type":"browser","tier":"dedicated"}`

	t.Run("nothing passed is nothing sent", func(t *testing.T) {
		got, err := assembleConnectors(nil)
		if err != nil || got != nil {
			t.Fatalf("assembleConnectors(nil) = %s, %v; want nil, nil", got, err)
		}
	})

	t.Run("a single object becomes a one-element array", func(t *testing.T) {
		got, err := assembleConnectors([]string{gh})
		if err != nil {
			t.Fatalf("assembleConnectors: %v", err)
		}
		if string(got) != "["+gh+"]" {
			t.Fatalf("got %s, want %s", got, "["+gh+"]")
		}
	})

	t.Run("an array is passed through, and repeats concatenate", func(t *testing.T) {
		got, err := assembleConnectors([]string{"[" + gh + "]", browser})
		if err != nil {
			t.Fatalf("assembleConnectors: %v", err)
		}
		if string(got) != "["+gh+","+browser+"]" {
			t.Fatalf("got %s, want %s", got, "["+gh+","+browser+"]")
		}
	})

	t.Run("an empty array clears the list", func(t *testing.T) {
		got, err := assembleConnectors([]string{"[]"})
		if err != nil {
			t.Fatalf("assembleConnectors: %v", err)
		}
		if string(got) != "[]" {
			t.Fatalf("got %s, want []", got)
		}
	})

	t.Run("junk is refused before the round trip", func(t *testing.T) {
		for _, in := range []string{"not json", `"a string"`, `42`, `[{"type":"github"},]`} {
			if _, err := assembleConnectors([]string{in}); err == nil {
				t.Errorf("assembleConnectors(%q): want an error, got nil", in)
			}
		}
	})
}

// TestSplitList pins how a comma-separated list flag reads, including the
// empty value that clears a list in a patch.
func TestSplitList(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", []string{}},
		{"a", []string{"a"}},
		{"a,b", []string{"a", "b"}},
		{" a , b ", []string{"a", "b"}},
		{"a,,b", []string{"a", "b"}},
	}
	for _, tc := range cases {
		got := splitList(tc.in)
		if !slices.Equal(got, tc.want) {
			t.Errorf("splitList(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// TestEnvCached pins the CACHED column: an environment is cached only while
// the snapshot was built from the setup it still has.
func TestEnvCached(t *testing.T) {
	cases := []struct {
		name string
		env  environment
		want bool
	}{
		{"fresh environment, no snapshot", environment{SetupHash: "abc"}, false},
		{"snapshot matches the current setup", environment{SetupHash: "abc", SnapshotHash: "abc", SnapshotRef: "r"}, true},
		{"snapshot built from superseded setup", environment{SetupHash: "def", SnapshotHash: "abc", SnapshotRef: "r"}, false},
		{"both empty is not a cache hit", environment{}, false},
	}
	for _, tc := range cases {
		if got := envCached(tc.env); got != tc.want {
			t.Errorf("%s: envCached = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestOptionalPathFlag pins --from-devcontainer's optional value: the bare
// flag means the current directory, and — the part that matters for
// reorderArgs — it must not swallow the environment name that follows it.
func TestOptionalPathFlag(t *testing.T) {
	newFS := func() (*flag.FlagSet, *optionalPathFlag) {
		fs := flag.NewFlagSet("env create", flag.ContinueOnError)
		var f optionalPathFlag
		fs.Var(&f, "from-devcontainer", "read image from a devcontainer.json")
		fs.String("image", "", "image")
		return fs, &f
	}

	t.Run("the bare flag means the current directory and keeps the positional", func(t *testing.T) {
		fs, f := newFS()
		if err := fs.Parse(reorderArgs(fs, []string{"--from-devcontainer", "dev"})); err != nil {
			t.Fatalf("parse: %v", err)
		}
		if !f.set || f.path != "." {
			t.Errorf("flag = %+v, want set with path .", f)
		}
		if got := fs.Args(); !slices.Equal(got, []string{"dev"}) {
			t.Fatalf("positionals = %v, want [dev]", got)
		}
	})

	t.Run("an explicit directory is taken", func(t *testing.T) {
		fs, f := newFS()
		if err := fs.Parse(reorderArgs(fs, []string{"--from-devcontainer=./repo", "dev"})); err != nil {
			t.Fatalf("parse: %v", err)
		}
		if !f.set || f.path != "./repo" {
			t.Errorf("flag = %+v, want set with path ./repo", f)
		}
		if got := fs.Args(); !slices.Equal(got, []string{"dev"}) {
			t.Fatalf("positionals = %v, want [dev]", got)
		}
	})

	t.Run("an absent flag stays unset", func(t *testing.T) {
		fs, f := newFS()
		if err := fs.Parse(reorderArgs(fs, []string{"--image", "img:1", "dev"})); err != nil {
			t.Fatalf("parse: %v", err)
		}
		if f.set {
			t.Errorf("flag = %+v, want unset", f)
		}
	})
}

// TestDevcontainerDir pins the two spellings of --from-devcontainer's
// optional directory. The one with a space is the one people type, and Go's
// flag package cannot express it: an optional-value flag never consumes the
// next token, so the directory lands in the positionals and has to be
// recovered from there. Anything else left over must come back as rest, so
// the caller can refuse it instead of ignoring a typo.
func TestDevcontainerDir(t *testing.T) {
	cases := []struct {
		name     string
		flag     optionalPathFlag
		extra    []string
		wantDir  string
		wantRest []string
	}{
		{"bare flag, nothing else", optionalPathFlag{set: true, path: "."}, nil, ".", nil},
		{"space-separated dir", optionalPathFlag{set: true, path: "."}, []string{"./repo"}, "./repo", nil},
		{"equals-separated dir", optionalPathFlag{set: true, path: "./repo"}, nil, "./repo", nil},
		{"equals dir plus a stray arg", optionalPathFlag{set: true, path: "./repo"}, []string{"junk"}, "./repo", []string{"junk"}},
		{"flag unset, stray arg is refused", optionalPathFlag{}, []string{"junk"}, "", []string{"junk"}},
		{"bare flag with two extras is ambiguous, so nothing is consumed",
			optionalPathFlag{set: true, path: "."}, []string{"a", "b"}, ".", []string{"a", "b"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir, rest := devcontainerDir(tc.flag, tc.extra)
			if dir != tc.wantDir || !slices.Equal(rest, tc.wantRest) {
				t.Fatalf("devcontainerDir = (%q, %v), want (%q, %v)", dir, rest, tc.wantDir, tc.wantRest)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// env create / env update: the init hook reaches the wire
// ---------------------------------------------------------------------------

// TestEnvInitFlagsReachTheWire pins the two ends of the `--init-file` /
// `--init-timeout-sec` plumbing: create sends the script's CONTENTS (not its
// path) as `init`, and update sends only the fields the operator actually
// passed — an absent flag means "leave it alone", which for an environment's
// boot hook is the difference between editing it and erasing it.
func TestEnvInitFlagsReachTheWire(t *testing.T) {
	var gotBody map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody = nil
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode request body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusCreated)
		}
		if _, err := w.Write([]byte(`{"environment":{"id":"env_test"}}`)); err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	t.Setenv("RAINIER_CONFIG", filepath.Join(dir, "config.json"))
	if err := cli.Save(cli.Config{ServerURL: ts.URL, Token: "rnr_test"}); err != nil {
		t.Fatal(err)
	}
	initPath := filepath.Join(dir, "init.sh")
	if err := os.WriteFile(initPath, []byte("make dev-server &\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := runEnvCreate([]string{"dev", "--image", "img:1", "--init-file", initPath, "--init-timeout-sec", "120"}); err != nil {
		t.Fatalf("env create: %v", err)
	}
	if gotBody["init"] != "make dev-server &\n" {
		t.Errorf("create init = %#v, want the script's contents", gotBody["init"])
	}
	if gotBody["init_timeout_sec"] != float64(120) {
		t.Errorf("create init_timeout_sec = %#v, want 120", gotBody["init_timeout_sec"])
	}

	if err := runEnvUpdate([]string{"dev", "--init-file", initPath}); err != nil {
		t.Fatalf("env update: %v", err)
	}
	if gotBody["init"] != "make dev-server &\n" {
		t.Errorf("update init = %#v, want the script's contents", gotBody["init"])
	}
	if len(gotBody) != 1 {
		t.Errorf("patch = %#v, want it to carry only the field that was passed", gotBody)
	}

	if err := runEnvUpdate([]string{"dev", "--init-timeout-sec", "300"}); err != nil {
		t.Fatalf("env update: %v", err)
	}
	if gotBody["init_timeout_sec"] != float64(300) || len(gotBody) != 1 {
		t.Errorf("patch = %#v, want only init_timeout_sec", gotBody)
	}
}

// ---------------------------------------------------------------------------
// creds / login --refresh (the credential vault's client surface)
// ---------------------------------------------------------------------------

// captureStdout runs fn with os.Stdout replaced by a pipe and returns what it
// printed. The CLI's rendering IS its contract for `creds` — the columns, and
// just as importantly the absence of anything token-shaped — so the test has
// to read the actual bytes a terminal would.
func captureStdout(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	saved := os.Stdout
	os.Stdout = w

	done := make(chan string, 1)
	go func() {
		var buf bytes.Buffer
		io.Copy(&buf, r)
		done <- buf.String()
	}()

	runErr := fn()
	os.Stdout = saved
	w.Close()
	out := <-done
	r.Close()
	return out, runErr
}

// credsServer serves GET /v0/credentials with the given body and records the
// path it was asked for.
func credsServer(t *testing.T, body string) (*httptest.Server, *string) {
	t.Helper()
	var gotPath string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		if _, err := io.WriteString(w, body); err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	t.Cleanup(ts.Close)
	return ts, &gotPath
}

func TestCredsRendersTable(t *testing.T) {
	const body = `{"credentials":[{"provider":"github","status":"needs_refresh","scopes":"repo, read:user",` +
		`"obtained_at":"2026-08-01T00:00:00Z","last_verified_at":"2026-08-01T00:00:00Z","last_used_at":"2026-08-02T00:00:00Z"}]}`
	ts, gotPath := credsServer(t, body)

	t.Setenv("RAINIER_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	if err := cli.Save(cli.Config{ServerURL: ts.URL, Token: "rnr_test"}); err != nil {
		t.Fatal(err)
	}

	out, err := captureStdout(t, func() error { return runCreds(nil) })
	if err != nil {
		t.Fatalf("creds: %v", err)
	}
	if *gotPath != "/v0/credentials" {
		t.Errorf("requested %q, want /v0/credentials", *gotPath)
	}
	for _, want := range []string{"PROVIDER", "STATUS", "SCOPES", "LAST_VERIFIED", "LAST_USED", "github", "needs_refresh", "repo, read:user"} {
		if !strings.Contains(out, want) {
			t.Errorf("creds output missing %q:\n%s", want, out)
		}
	}
}

// An empty vault renders the header and nothing else — a bare table, not an
// error and not a crash on a nil slice.
func TestCredsEmpty(t *testing.T) {
	ts, _ := credsServer(t, `{"credentials":[]}`)
	t.Setenv("RAINIER_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	if err := cli.Save(cli.Config{ServerURL: ts.URL, Token: "rnr_test"}); err != nil {
		t.Fatal(err)
	}

	out, err := captureStdout(t, func() error { return runCreds(nil) })
	if err != nil {
		t.Fatalf("creds: %v", err)
	}
	if !strings.Contains(out, "PROVIDER") {
		t.Errorf("creds output = %q, want the header row", out)
	}
	if strings.Contains(out, "github") {
		t.Errorf("creds output = %q, want no rows", out)
	}
}

// `login --refresh github` re-runs an acquisition path and posts it to the
// same exchange — the server upserts, so there is no second endpoint — and
// the CLI prints the server's scope warning when one comes back.
func TestLoginRefreshPostsTheExchangeAndPrintsTheWarning(t *testing.T) {
	var gotBody map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody = nil
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode request body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		if _, err := io.WriteString(w, `{"token":"rnr_new","user":{"login":"alice","role":"member"},`+
			`"scopes":"read:user","warning":"token lacks repo scope; git operations will require rainier login --refresh github"}`); err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	defer ts.Close()

	cfgPath := filepath.Join(t.TempDir(), "config.json")
	t.Setenv("RAINIER_CONFIG", cfgPath)
	if err := cli.Save(cli.Config{ServerURL: ts.URL, Token: "rnr_old", OwnerID: "usr_alice"}); err != nil {
		t.Fatal(err)
	}

	out, err := captureStdout(t, func() error {
		return runLogin([]string{"--refresh", "github", "--token", "gho_refreshed"})
	})
	if err != nil {
		t.Fatalf("login --refresh: %v", err)
	}
	if gotBody["access_token"] != "gho_refreshed" {
		t.Errorf("exchange body = %#v, want the refreshed GitHub token", gotBody)
	}
	if !strings.Contains(out, "warning: token lacks repo scope") {
		t.Errorf("login output = %q, want the server's scope warning printed", out)
	}
	if strings.Contains(out, "gho_refreshed") {
		t.Errorf("login output echoed the GitHub token: %q", out)
	}

	saved, err := cli.Load()
	if err != nil {
		t.Fatalf("cli.Load: %v", err)
	}
	if saved.Token != "rnr_new" {
		t.Errorf("saved token = %q, want the refreshed one", saved.Token)
	}
	// This server's identity carries no id (an older controld, or any
	// response that omits it), which must leave the cached one alone rather
	// than blanking it: forgetting who the caller is would silently switch
	// owner-preference off on the next ambiguous name.
	if saved.OwnerID != "usr_alice" {
		t.Errorf("saved owner id = %q, want the cached one preserved across a refresh", saved.OwnerID)
	}
}

// TestLoginStoresTheOwnerID pins where owner-preference now gets its answer:
// controld returns the caller's own id with the exchange (the same identity
// GET /v0/me serves), and login caches it, so the very next command can tell
// this user's sessions from a teammate's. It used to come only from a `new`
// response, which meant a fresh login could not break an ambiguous name at
// all until a session had been created with this CLI.
func TestLoginStoresTheOwnerID(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if _, err := io.WriteString(w, `{"token":"rnr_new","user":{"id":"usr_7","login":"alice","role":"member"},"scopes":"repo read:user"}`); err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	defer ts.Close()

	t.Setenv("RAINIER_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	if _, err := captureStdout(t, func() error {
		return runLogin([]string{"--server", ts.URL, "--token", "gho_fresh"})
	}); err != nil {
		t.Fatalf("login: %v", err)
	}

	saved, err := cli.Load()
	if err != nil {
		t.Fatalf("cli.Load: %v", err)
	}
	if saved.OwnerID != "usr_7" {
		t.Fatalf("saved owner id = %q, want the id the login returned", saved.OwnerID)
	}
	if saved.Token != "rnr_new" || saved.ServerURL != ts.URL {
		t.Fatalf("saved config = %+v, want the new token and server", saved)
	}
}

// --refresh names a provider, and github is the only one the vault knows in
// v0: a typo must be refused by name rather than silently refreshing github.
func TestLoginRefreshRejectsUnknownProvider(t *testing.T) {
	t.Setenv("RAINIER_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	if err := cli.Save(cli.Config{ServerURL: "http://controld.test", Token: "rnr_old"}); err != nil {
		t.Fatal(err)
	}
	err := runLogin([]string{"--refresh", "gitlab", "--token", "gho_x"})
	if err == nil || !strings.Contains(err.Error(), "gitlab") {
		t.Fatalf("login --refresh gitlab err = %v, want a refusal naming the provider", err)
	}
}

// ---------------------------------------------------------------------------
// push / pull / diff
// ---------------------------------------------------------------------------

// TestSplitRemote pins the "<session>:<path>" argument both transfer commands
// take. The split is at the FIRST colon: a session ref never contains one, and
// a remote path may.
func TestSplitRemote(t *testing.T) {
	cases := []struct {
		spec, ref, path string
		wantErr         bool
	}{
		{spec: "dev-box:widget/vendor", ref: "dev-box", path: "widget/vendor"},
		{spec: "sess_abc123:/workspace/out", ref: "sess_abc123", path: "/workspace/out"},
		{spec: "dev-box:a:b", ref: "dev-box", path: "a:b"},
		{spec: "dev-box", wantErr: true},
		{spec: ":path", wantErr: true},
		{spec: "dev-box:", wantErr: true},
		{spec: "", wantErr: true},
	}
	for _, tc := range cases {
		ref, path, err := splitRemote(tc.spec)
		if tc.wantErr {
			if err == nil {
				t.Errorf("splitRemote(%q) = %q, %q; want an error", tc.spec, ref, path)
			}
			continue
		}
		if err != nil {
			t.Errorf("splitRemote(%q): %v", tc.spec, err)
			continue
		}
		if ref != tc.ref || path != tc.path {
			t.Errorf("splitRemote(%q) = %q, %q; want %q, %q", tc.spec, ref, path, tc.ref, tc.path)
		}
	}
}

// TestRenderDiff: one heading per repository naming both branches, git's stat
// underneath, and an explicit line for a repository with nothing to show —
// silence there would read as a rendering bug.
func TestRenderDiff(t *testing.T) {
	var buf bytes.Buffer
	renderDiff(&buf, xfer.DiffAnswer{Repos: []xfer.RepoDiff{
		{Repo: "acme/widget", BaseBranch: "main", SessionBranch: "rainier/dev", Stat: " main.go | 2 +-\n 1 file changed\n"},
		{Repo: "acme/other", BaseBranch: "trunk", SessionBranch: "rainier/dev", Stat: ""},
	}})
	out := buf.String()
	for _, want := range []string{"acme/widget", "rainier/dev", "main", "main.go | 2 +-", "acme/other", "no changes"} {
		if !strings.Contains(out, want) {
			t.Errorf("diff output missing %q:\n%s", want, out)
		}
	}
}

// A session with no repositories says so rather than printing an empty page.
func TestRenderDiffWithNoRepos(t *testing.T) {
	var buf bytes.Buffer
	renderDiff(&buf, xfer.DiffAnswer{})
	if !strings.Contains(buf.String(), "no repositories") {
		t.Errorf("diff output = %q, want it to say the session has no repositories", buf.String())
	}
}

// ---------------------------------------------------------------------------
// attach: which of three requests --since spells
// ---------------------------------------------------------------------------

// TestAttachFlagsCursor pins the CLI half of the `--since` fix. The flag's
// value alone cannot say what the user asked for — a uint64 defaulting to 0
// makes "no flag" and "--since 0" identical, and those are opposite requests
// (the current screen vs. the entire event log). Both argument orders are
// checked because the acceptance run tried the flag before the positional
// argument when the first attempt showed nothing, and reorderArgs is the only
// reason that form parses at all.
func TestAttachFlagsCursor(t *testing.T) {
	for _, tc := range []struct {
		name   string
		args   []string
		ref    string
		cursor uint64
	}{
		{"no flag is no cursor", []string{"sess_a"}, "sess_a", 0},
		{"--since 0 is the whole log", []string{"--since", "0", "sess_a"}, "sess_a", wire.SinceAll},
		{"--since 0 after the ref", []string{"sess_a", "--since", "0"}, "sess_a", wire.SinceAll},
		{"--since N resumes", []string{"my-box", "--since", "19"}, "my-box", 19},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ref, cursor := attachFlags(tc.args)
			if ref != tc.ref || cursor != tc.cursor {
				t.Fatalf("attachFlags(%q) = (%q, %d), want (%q, %d)", tc.args, ref, cursor, tc.ref, tc.cursor)
			}
		})
	}
}

// TestPrepareAttach makes `attach` the only lifecycle command a terminal
// user needs. The API already has separate read and resume operations; this
// test pins how the CLI composes them without adding a second server-side
// lifecycle path.
func TestPrepareAttach(t *testing.T) {
	for _, state := range []string{"running", "creating", "queued", "failed"} {
		t.Run("does not resume "+state, func(t *testing.T) {
			var resumes int
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/v0/sessions/sess_attach":
					json.NewEncoder(w).Encode(sessionEnvelope{Session: session{ID: "sess_attach", State: state}})
				case r.Method == http.MethodPost && r.URL.Path == "/v0/sessions/sess_attach/resume":
					resumes++
					json.NewEncoder(w).Encode(sessionEnvelope{Session: session{ID: "sess_attach", State: "running"}})
				default:
					t.Fatalf("unexpected request: %s %s", r.Method, r.URL.RequestURI())
				}
			}))
			defer ts.Close()

			if err := prepareAttach(&cli.Client{Base: ts.URL}, "sess_attach"); err != nil {
				t.Fatalf("prepareAttach: %v", err)
			}
			if resumes != 0 {
				t.Fatalf("resume requests = %d, want 0 for state %q", resumes, state)
			}
		})
	}

	for _, state := range []string{"suspended_warm", "suspended_cold"} {
		t.Run("resumes "+state, func(t *testing.T) {
			var resumes int
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/v0/sessions/sess_attach":
					json.NewEncoder(w).Encode(sessionEnvelope{Session: session{ID: "sess_attach", State: state}})
				case r.Method == http.MethodPost && r.URL.Path == "/v0/sessions/sess_attach/resume":
					resumes++
					json.NewEncoder(w).Encode(sessionEnvelope{Session: session{ID: "sess_attach", State: "running"}})
				default:
					t.Fatalf("unexpected request: %s %s", r.Method, r.URL.RequestURI())
				}
			}))
			defer ts.Close()

			if err := prepareAttach(&cli.Client{Base: ts.URL}, "sess_attach"); err != nil {
				t.Fatalf("prepareAttach: %v", err)
			}
			if resumes != 1 {
				t.Fatalf("resume requests = %d, want 1 for state %q", resumes, state)
			}
		})
	}

	t.Run("refuses a permanently ended session", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(sessionEnvelope{Session: session{ID: "sess_attach", State: "dead"}})
		}))
		defer ts.Close()

		err := prepareAttach(&cli.Client{Base: ts.URL}, "sess_attach")
		if err == nil || !strings.Contains(err.Error(), `session sess_attach is dead and cannot be attached`) {
			t.Fatalf("prepareAttach error = %v, want a direct dead-session error", err)
		}
	})

	t.Run("a concurrent resume winner converges", func(t *testing.T) {
		gets := 0
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			switch r.Method {
			case http.MethodGet:
				gets++
				state := "suspended_warm"
				if gets > 3 {
					state = "running"
				}
				json.NewEncoder(w).Encode(sessionEnvelope{Session: session{ID: "sess_attach", State: state}})
			case http.MethodPost:
				w.WriteHeader(http.StatusConflict)
				io.WriteString(w, `{"error":{"code":"conflict","message":"session is not suspended"}}`)
			}
		}))
		defer ts.Close()

		if err := prepareAttach(&cli.Client{Base: ts.URL}, "sess_attach"); err != nil {
			t.Fatalf("prepareAttach: %v", err)
		}
		if gets != 4 {
			t.Fatalf("GET count = %d, want 4 (initial state plus delayed convergence)", gets)
		}
	})

	t.Run("a resume failure is preserved when the state did not advance", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if r.Method == http.MethodGet {
				json.NewEncoder(w).Encode(sessionEnvelope{Session: session{ID: "sess_attach", State: "suspended_cold"}})
				return
			}
			w.WriteHeader(http.StatusConflict)
			io.WriteString(w, `{"error":{"code":"no_capacity","message":"runner is full"}}`)
		}))
		defer ts.Close()

		err := prepareAttach(&cli.Client{Base: ts.URL}, "sess_attach")
		if err == nil || !strings.Contains(err.Error(), "no_capacity: runner is full") {
			t.Fatalf("prepareAttach error = %v, want the original resume failure", err)
		}
	})
}

func TestAttachWithRetryBacksOffTransientDisconnects(t *testing.T) {
	requests := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		var first wire.ClientMsg
		if err := wsjson.Read(r.Context(), c, &first); err != nil {
			return
		}
		if requests < 3 {
			c.Close(websocket.StatusGoingAway, "synthetic transient close")
			return
		}
		wsjson.Write(r.Context(), c, wire.ServerMsg{Type: "exit", ExitCode: 0})
	}))
	defer ts.Close()

	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdinR.Close()
	defer stdinW.Close()
	origStdin := os.Stdin
	os.Stdin = stdinR
	t.Cleanup(func() { os.Stdin = origStdin })

	var delays []time.Duration
	err = attachWithRetrySleep(cli.Config{ServerURL: ts.URL, Token: "rnr_synthetic"}, "sess_reconnect", 0, func(d time.Duration) {
		delays = append(delays, d)
	})
	if err != nil {
		t.Fatalf("attachWithRetrySleep: %v", err)
	}
	if !slices.Equal(delays, []time.Duration{100 * time.Millisecond, 200 * time.Millisecond}) {
		t.Fatalf("reconnect delays = %v, want exponential [100ms 200ms]", delays)
	}
}

func TestAttachWithRetryDoesNotRetryPolicyClose(t *testing.T) {
	requests := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		var first wire.ClientMsg
		if err := wsjson.Read(r.Context(), c, &first); err != nil {
			return
		}
		c.Close(websocket.StatusPolicyViolation, "synthetic permanent close")
	}))
	defer ts.Close()

	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdinR.Close()
	defer stdinW.Close()
	origStdin := os.Stdin
	os.Stdin = stdinR
	t.Cleanup(func() { os.Stdin = origStdin })

	err = attachWithRetrySleep(cli.Config{ServerURL: ts.URL, Token: "rnr_synthetic"}, "sess_reconnect", 0, func(time.Duration) {})
	if err == nil || websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
		t.Fatalf("error = %v, want policy close", err)
	}
	if requests != 1 {
		t.Fatalf("requests = %d, want one attempt for a permanent close", requests)
	}
}

func TestRunNewUsesSuppliedIdempotencyKey(t *testing.T) {
	var gotKey string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("Idempotency-Key")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(sessionEnvelope{Session: session{ID: "sess_synthetic", State: "queued"}})
	}))
	defer ts.Close()
	t.Setenv("RAINIER_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	if err := cli.Save(cli.Config{ServerURL: ts.URL, Token: "rnr_synthetic"}); err != nil {
		t.Fatal(err)
	}
	if _, err := captureStdout(t, func() error {
		return runNew([]string{"--detach", "--name", "synthetic-box", "--idempotency-key", "synthetic-create-key"})
	}); err != nil {
		t.Fatalf("runNew: %v", err)
	}
	if gotKey != "synthetic-create-key" {
		t.Fatalf("Idempotency-Key = %q, want supplied recovery key", gotKey)
	}
}

// TestAttachWithRetryReconnectsFromTheRenderedCursor crosses the real
// WebSocket boundary. The first established viewer renders seq 17 and loses
// its transport; the next dial gets a transient 503; the third must ask for
// entries after 17 and finish only when the session itself reports exit.
func TestAttachWithRetryReconnectsFromTheRenderedCursor(t *testing.T) {
	requests := 0
	var resumedQueries []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if requests > 1 {
			resumedQueries = append(resumedQueries, r.URL.Query().Get("since"))
		}
		if requests == 2 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			io.WriteString(w, `{"error":{"code":"session_not_ready","message":"runner is reconnecting"}}`)
			return
		}

		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		var first wire.ClientMsg
		if err := wsjson.Read(r.Context(), c, &first); err != nil {
			return
		}
		if requests == 1 {
			wsjson.Write(r.Context(), c, wire.ServerMsg{Type: "output", Seq: 17, Data: []byte("before-drop")})
			c.Close(websocket.StatusGoingAway, "synthetic network interruption")
			return
		}
		wsjson.Write(r.Context(), c, wire.ServerMsg{Type: "output", Seq: 18, Data: []byte("after-drop")})
		wsjson.Write(r.Context(), c, wire.ServerMsg{Type: "exit", ExitCode: 0})
	}))
	defer ts.Close()

	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe (stdin): %v", err)
	}
	defer stdinR.Close()
	defer stdinW.Close()
	origStdin := os.Stdin
	os.Stdin = stdinR
	t.Cleanup(func() { os.Stdin = origStdin })

	out, err := captureStdout(t, func() error {
		cfg := cli.Config{ServerURL: ts.URL, Token: "rnr_synthetic"}
		return attachWithRetry(cfg, "sess_reconnect", 0)
	})
	if err != nil {
		t.Fatalf("attachWithRetry: %v", err)
	}
	if requests != 3 {
		t.Fatalf("attach requests = %d, want 3 (connected, transient 503, reconnected)", requests)
	}
	if !slices.Equal(resumedQueries, []string{"17", "17"}) {
		t.Fatalf("reconnect cursors = %v, want [17 17]", resumedQueries)
	}
	for _, want := range []string{"before-drop", "after-drop", "reconnecting"} {
		if !strings.Contains(out, want) {
			t.Errorf("attach output missing %q:\n%s", want, out)
		}
	}
}

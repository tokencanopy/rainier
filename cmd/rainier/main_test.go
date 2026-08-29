package main

import (
	"encoding/json"
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"rainier/internal/cli"
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
// per-owner, but GET /v1/sessions is team-visible, so two teammates can
// share a name.
// ---------------------------------------------------------------------------

// pagedSessions serves GET /v1/sessions from a fixed set of pages, keyed by
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
// Without a cached owner id the CLI must refuse rather than silently acting
// on whichever paginated first; with one cached, it must prefer the
// caller's own.
func TestResolveSessionIDAmbiguousNameAcrossOwners(t *testing.T) {
	ts := pagedSessions(t, map[string]sessionsEnvelope{
		"": {Sessions: []session{
			{ID: "sess_alice1", Name: "dev-box", OwnerID: "usr_alice", State: "running"},
			{ID: "sess_bob1", Name: "dev-box", OwnerID: "usr_bob", State: "running"},
		}},
	})
	c := &cli.Client{Base: ts.URL}

	t.Run("no cached owner id: ambiguous, both matches listed", func(t *testing.T) {
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

	t.Run("a cached owner id that owns none of the matches is still ambiguous", func(t *testing.T) {
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

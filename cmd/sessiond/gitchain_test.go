// cmd/sessiond/gitchain_test.go
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"rainier/internal/relay"
)

// --- chain composition ---

// TestChainProgram pins the exact shell program sessiond runs in place of the
// agent, for every combination of stages an environment can ask for. Like
// TestSetupWrapperArgv, the strings are spelled out rather than derived from
// the production consts: this program is a contract between sessiond and a
// shell inside a container no unit test ever runs, so changing it has to be a
// deliberate edit in two places.
//
// The first case is the load-bearing one: with only a setup stage and no git
// exports, the composed program must be BYTE-IDENTICAL to the Plan 4 wrapper,
// so a session with no repos and no init boots exactly as it did before.
func TestChainProgram(t *testing.T) {
	setup := bootStage{Name: stageSetup, ScriptPath: "/w/setup.sh", RCPath: "/w/setup.rc"}
	clone := bootStage{Name: stageClone, ScriptPath: "/w/clones.sh", RCPath: "/w/clone.rc"}
	initS := bootStage{Name: stageInit, ScriptPath: "/w/init.sh", RCPath: "/w/init.rc"}
	gitEnv := []envVar{
		{Name: "GIT_CONFIG_GLOBAL", Value: "/w/gitconfig"},
		{Name: "GIT_TERMINAL_PROMPT", Value: "0"},
		{Name: "GIT_ASKPASS", Value: ""},
		{Name: "SSH_ASKPASS", Value: ""},
	}
	// Spelled out once, next to the cases that use it: the exports are a
	// contract with a shell no unit test runs, and an empty value has to
	// survive as `=''` rather than as a bare `=`.
	const gitExports = `export GIT_CONFIG_GLOBAL='/w/gitconfig'; export GIT_TERMINAL_PROMPT='0'; ` +
		`export GIT_ASKPASS=''; export SSH_ASKPASS=''; `

	cases := []struct {
		name   string
		env    []envVar
		stages []bootStage
		want   string
	}{
		{
			name:   "setup alone is the Plan 4 wrapper, byte for byte",
			stages: []bootStage{setup},
			want:   `sh /w/setup.sh; rc=$?; echo $rc > /w/setup.rc; [ "$rc" -eq 0 ] && exec "$@"; exit $rc`,
		},
		{
			name:   "clone alone",
			env:    gitEnv,
			stages: []bootStage{clone},
			want: gitExports +
				`sh /w/clones.sh; rc=$?; echo $rc > /w/clone.rc; [ "$rc" -eq 0 ] && exec "$@"; exit $rc`,
		},
		{
			name:   "clone then init",
			env:    gitEnv,
			stages: []bootStage{clone, initS},
			want: gitExports +
				`sh /w/clones.sh; rc=$?; echo $rc > /w/clone.rc; [ "$rc" -eq 0 ] || exit $rc; ` +
				`sh /w/init.sh; rc=$?; echo $rc > /w/init.rc; [ "$rc" -eq 0 ] && exec "$@"; exit $rc`,
		},
		{
			name:   "the whole chain",
			env:    gitEnv,
			stages: []bootStage{setup, clone, initS},
			want: gitExports +
				`sh /w/setup.sh; rc=$?; echo $rc > /w/setup.rc; [ "$rc" -eq 0 ] || exit $rc; ` +
				`sh /w/clones.sh; rc=$?; echo $rc > /w/clone.rc; [ "$rc" -eq 0 ] || exit $rc; ` +
				`sh /w/init.sh; rc=$?; echo $rc > /w/init.rc; [ "$rc" -eq 0 ] && exec "$@"; exit $rc`,
		},
		{
			name:   "no stages is no program",
			stages: nil,
			want:   "",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := chainProgram(c.env, c.stages); got != c.want {
				t.Errorf("chainProgram =\n  %q\nwant\n  %q", got, c.want)
			}
		})
	}
}

// TestChainArgvKeepsTheAgentArgvVerbatim pins the argv shape: the wrapper
// program, the "wrapper" $0 placeholder that makes "$@" expand to exactly the
// agent's own argv, and then that argv untouched.
func TestChainArgvKeepsTheAgentArgvVerbatim(t *testing.T) {
	stages := []bootStage{{Name: stageClone, ScriptPath: "/w/clones.sh", RCPath: "/w/clone.rc"}}
	got := chainArgv(nil, stages, []string{"claude", "--foo", "a b"})
	want := []string{
		"sh", "-c",
		`sh /w/clones.sh; rc=$?; echo $rc > /w/clone.rc; [ "$rc" -eq 0 ] && exec "$@"; exit $rc`,
		"wrapper", "claude", "--foo", "a b",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("chainArgv =\n  %q\nwant\n  %q", got, want)
	}

	// With no stages there is nothing to wrap: the agent is the child, exactly
	// as it is for a session that declares no setup, no repos and no init.
	if got := chainArgv(nil, nil, []string{"claude"}); !reflect.DeepEqual(got, []string{"claude"}) {
		t.Fatalf("chainArgv with no stages = %q, want the argv unchanged", got)
	}
}

// --- shell quoting ---

// TestShQuote covers the one function standing between a repository name that
// came from a database and a shell that would otherwise run it. Every value in
// clones.sh goes through it.
func TestShQuote(t *testing.T) {
	cases := []struct{ in, want string }{
		{"main", `'main'`},
		{"", `''`},
		{"rainier/my-session", `'rainier/my-session'`},
		{"it's", `'it'\''s'`},
		{"$(rm -rf /)", `'$(rm -rf /)'`},
		{"a; rm -rf /", `'a; rm -rf /'`},
		{"`id`", "'`id`'"},
		{`a"b`, `'a"b'`},
	}
	for _, c := range cases {
		if got := shQuote(c.in); got != c.want {
			t.Errorf("shQuote(%q) = %s, want %s", c.in, got, c.want)
		}
	}
}

// TestShQuoteSurvivesRealSh proves the quoting empirically: whatever goes in
// comes back out of a real shell byte for byte, which is the property the
// clone stage rests on — a session branch name is NOT sanitized on its way
// here (controller ruling), so a git-illegal or shell-hostile one must reach
// git verbatim and be rejected by git, not by this program.
func TestShQuoteSurvivesRealSh(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("no sh on PATH")
	}
	for _, s := range []string{"main", "it's", "$(id)", "a b", "`id`", `a"b`, "rainier/-- --oops", "a\\b"} {
		out, err := exec.Command("sh", "-c", "printf '%s' "+shQuote(s)).Output()
		if err != nil {
			t.Fatalf("sh rejected %s: %v", shQuote(s), err)
		}
		if string(out) != s {
			t.Errorf("sh printed %q for %q", out, s)
		}
	}
}

// --- clones.sh ---

// TestClonesScript is the golden test for the generated clone script. The git
// invocations are the design's verbatim ones (§4.3) and every interpolated
// value is quoted.
func TestClonesScript(t *testing.T) {
	repos := []repoSpec{
		{Owner: "acme", Name: "api", BaseBranch: "main", SessionBranch: "rainier/nightly", Dir: "api"},
		{Owner: "acme", Name: "web", BaseBranch: "release/v2", SessionBranch: "rainier/it's", Dir: "acme__web"},
	}
	want := `# generated by sessiond from RAINIER_REPOS_B64 — do not edit.
if [ -d '/workspace/api/.git' ]; then
echo '+ rainier clone: /workspace/api is already cloned; skipping'
else
echo '+ rainier clone: acme/api -> /workspace/api (branch rainier/nightly from main)'
git clone --branch 'main' -- 'https://github.com/acme/api.git' '/workspace/api' && git -C '/workspace/api' checkout -b 'rainier/nightly'
rc=$?; [ "$rc" -eq 0 ] || exit $rc
fi
if [ -d '/workspace/acme__web/.git' ]; then
echo '+ rainier clone: /workspace/acme__web is already cloned; skipping'
else
echo '+ rainier clone: acme/web -> /workspace/acme__web (branch rainier/it'\''s from release/v2)'
git clone --branch 'release/v2' -- 'https://github.com/acme/web.git' '/workspace/acme__web' && git -C '/workspace/acme__web' checkout -b 'rainier/it'\''s'
rc=$?; [ "$rc" -eq 0 ] || exit $rc
fi
`
	if got := clonesScript(workspaceRoot, repos); got != want {
		t.Fatalf("clonesScript =\n%s\nwant\n%s", got, want)
	}

	// No token, ever: the URL is the plain public one and git asks the
	// credential helper for the rest. A token in this file would survive the
	// session on a persistent volume.
	if strings.Contains(want, "@github.com") || strings.Contains(want, "x-access-token") {
		t.Fatal("the clone script embeds a credential in a URL")
	}
}

// TestClonesScriptDefaultsTheDirectory: Dir is controld's to set, but an empty
// one must not become `git clone <url> ”`.
func TestClonesScriptDefaultsTheDirectory(t *testing.T) {
	got := clonesScript("/workspace", []repoSpec{{Owner: "acme", Name: "api", BaseBranch: "main", SessionBranch: "b"}})
	if !strings.Contains(got, `'/workspace/api'`) {
		t.Fatalf("script = %s, want it to fall back to the repo name for the directory", got)
	}
}

// TestDecodeRepos covers the RAINIER_REPOS_B64 channel: the JSON array the
// driver encodes, and the two ways it can arrive broken.
func TestDecodeRepos(t *testing.T) {
	blob := `[{"owner":"acme","name":"api","base_branch":"main","session_branch":"rainier/x","dir":"api"}]`
	got, err := decodeRepos(base64.StdEncoding.EncodeToString([]byte(blob)))
	if err != nil {
		t.Fatal(err)
	}
	want := []repoSpec{{Owner: "acme", Name: "api", BaseBranch: "main", SessionBranch: "rainier/x", Dir: "api"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decodeRepos = %+v, want %+v", got, want)
	}
	if _, err := decodeRepos("not!base64!"); err == nil {
		t.Error("decodeRepos accepted a payload that is not base64")
	}
	if _, err := decodeRepos(base64.StdEncoding.EncodeToString([]byte(`{"owner":"acme"}`))); err == nil {
		t.Error("decodeRepos accepted a payload that is not the JSON array the driver sends")
	}
}

// --- gitconfig ---

// TestGitConfig is the golden test for the file every git process in the
// sandbox reads. It carries an identity and the helper command; it must never
// carry a credential.
func TestGitConfig(t *testing.T) {
	got := gitConfig("alice", "42+alice@users.noreply.github.com")
	for _, want := range []string{
		// The empty helper first: git runs every configured helper, and an
		// image carrying `credential.helper = store` would otherwise persist
		// every minted token to disk. Resetting the list is what keeps the
		// helper's stdout the only path a token can take.
		"[credential]\n\thelper =\n\thelper = /usr/local/bin/sessiond git-credential-helper\n",
		"[user]\n\tname = \"alice\"\n\temail = \"42+alice@users.noreply.github.com\"\n",
		"[push]\n\tdefault = current\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("gitconfig =\n%s\nmissing\n%s", got, want)
		}
	}
	// The whole point of the helper: nothing token-shaped is ever written to
	// disk. /workspace persists across a cold park, so a token here would
	// outlive the session that minted it.
	for _, forbidden := range []string{"password", "x-access-token", "ghp_", "ghs_", "gho_", "github_pat_", "Authorization"} {
		if strings.Contains(got, forbidden) {
			t.Errorf("gitconfig contains %q — the token reaches git through the helper's stdout ONLY:\n%s", forbidden, got)
		}
	}

	t.Run("an unattributed session gets no user section", func(t *testing.T) {
		got := gitConfig("", "")
		if strings.Contains(got, "[user]") {
			t.Errorf("gitconfig =\n%s\nwant no [user] section when no identity was injected", got)
		}
		if !strings.Contains(got, "[credential]") {
			t.Error("the credential helper must be configured even without an identity")
		}
	})

	t.Run("values are quoted and escaped", func(t *testing.T) {
		// git config treats # and ; as comments and trims surrounding space:
		// an unquoted value would be silently truncated.
		got := gitConfig(`a "b" #c`, "x\\y")
		if !strings.Contains(got, "\tname = \"a \\\"b\\\" #c\"\n") {
			t.Errorf("gitconfig =\n%s\nwant an escaped, quoted name", got)
		}
		if !strings.Contains(got, "\temail = \"x\\\\y\"\n") {
			t.Errorf("gitconfig =\n%s\nwant an escaped, quoted email", got)
		}
	})
}

// --- boot preparation ---

// bootFiles reads the .rainier directory a prepareBoot call wrote.
func bootFiles(t *testing.T, dir string) map[string]string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]string{}
		}
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, e := range ents {
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		out[e.Name()] = string(b)
	}
	return out
}

func stageNames(stages []bootStage) []string {
	var out []string
	for _, s := range stages {
		out = append(out, s.Name)
	}
	return out
}

// TestPrepareBoot covers what boot lands on disk and which stages come out of
// it, for the combinations the driver can inject.
func TestPrepareBoot(t *testing.T) {
	b64 := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
	reposB64 := b64(`[{"owner":"acme","name":"api","base_branch":"main","session_branch":"rainier/x","dir":"api"}]`)

	t.Run("nothing injected is no chain at all", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), ".rainier")
		stages, env, err := prepareBoot(dir, "/workspace", bootEnv{})
		if err != nil {
			t.Fatal(err)
		}
		if len(stages) != 0 || len(env) != 0 {
			t.Fatalf("stages = %v, env = %v, want neither", stageNames(stages), env)
		}
		if files := bootFiles(t, dir); len(files) != 0 {
			t.Fatalf("files = %v, want none — a scratch session's boot writes nothing", files)
		}
	})

	t.Run("setup alone writes no gitconfig and exports nothing", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), ".rainier")
		stages, env, err := prepareBoot(dir, "/workspace", bootEnv{SetupB64: b64("echo hi\n"), SetupTimeout: "900"})
		if err != nil {
			t.Fatal(err)
		}
		if got := stageNames(stages); !reflect.DeepEqual(got, []string{stageSetup}) {
			t.Fatalf("stages = %v, want just the setup stage", got)
		}
		if stages[0].Timeout != 900*time.Second {
			t.Errorf("setup timeout = %s, want 900s", stages[0].Timeout)
		}
		if len(env) != 0 {
			t.Errorf("env = %v, want none — a session with no repos and no init runs no git", env)
		}
		files := bootFiles(t, dir)
		if _, ok := files["gitconfig"]; ok {
			t.Error("a setup-only boot wrote a gitconfig")
		}
		if files["setup.sh"] != "echo hi\n" {
			t.Errorf("setup.sh = %q", files["setup.sh"])
		}
	})

	t.Run("repos and init compose the whole chain", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), ".rainier")
		stages, env, err := prepareBoot(dir, "/workspace", bootEnv{
			SetupB64: b64("echo setup\n"), SetupTimeout: "60",
			ReposB64: reposB64,
			InitB64:  b64("echo init\n"), InitTimeout: "120",
			GitAuthorName: "alice", GitAuthorEmail: "42+alice@users.noreply.github.com",
		})
		if err != nil {
			t.Fatal(err)
		}
		if got, want := stageNames(stages), []string{stageSetup, stageClone, stageInit}; !reflect.DeepEqual(got, want) {
			t.Fatalf("stages = %v, want %v — setup is pre-clone and init is post-clone", got, want)
		}
		if got, want := stages[1].Timeout, cloneTimeoutPerRepo; got != want {
			t.Errorf("clone timeout = %s, want %s for one repo", got, want)
		}
		if got, want := stages[2].Timeout, 120*time.Second; got != want {
			t.Errorf("init timeout = %s, want %s", got, want)
		}
		// The gitconfig, and the three that keep git from ever asking a human
		// for a password. A refused mint makes the helper exit 1 with nothing
		// on stdout, and git's answer to that is to prompt — on the session's
		// own PTY, where it would block for the whole clone bound.
		want := []envVar{
			{Name: "GIT_CONFIG_GLOBAL", Value: dir + "/gitconfig"},
			{Name: "GIT_TERMINAL_PROMPT", Value: "0"},
			{Name: "GIT_ASKPASS", Value: ""},
			{Name: "SSH_ASKPASS", Value: ""},
		}
		if !reflect.DeepEqual(env, want) {
			t.Errorf("env = %v, want %v", env, want)
		}
		files := bootFiles(t, dir)
		for _, name := range []string{"setup.sh", "clones.sh", "init.sh", "gitconfig"} {
			if _, ok := files[name]; !ok {
				t.Errorf("%s was not written (files: %v)", name, keysOf(files))
			}
		}
		if !strings.Contains(files["clones.sh"], "https://github.com/acme/api.git") {
			t.Errorf("clones.sh = %q", files["clones.sh"])
		}
		if !strings.Contains(files["gitconfig"], "alice") {
			t.Errorf("gitconfig = %q, want the injected identity", files["gitconfig"])
		}
	})

	t.Run("init alone still gets a gitconfig", func(t *testing.T) {
		// An init hook is where a project's own bootstrap runs, and that
		// routinely means git: the helper and the identity have to be in place
		// even when this session clones nothing itself.
		dir := filepath.Join(t.TempDir(), ".rainier")
		stages, env, err := prepareBoot(dir, "/workspace", bootEnv{InitB64: b64("echo init\n")})
		if err != nil {
			t.Fatal(err)
		}
		if got := stageNames(stages); !reflect.DeepEqual(got, []string{stageInit}) {
			t.Fatalf("stages = %v, want just init", got)
		}
		if len(env) != 4 {
			t.Fatalf("env = %v, want the gitconfig and the three no-prompt variables", env)
		}
		if _, ok := bootFiles(t, dir)["gitconfig"]; !ok {
			t.Error("no gitconfig for an init-only boot")
		}
	})

	t.Run("stale rc files from an earlier boot are cleared", func(t *testing.T) {
		// /workspace persists across a cold park, so a resumed session finds
		// the PREVIOUS boot's verdicts sitting there. The watcher reports the
		// first rc it sees, so leaving them would announce a stale outcome
		// within a second of starting.
		dir := filepath.Join(t.TempDir(), ".rainier")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"setup.rc", "clone.rc", "init.rc"} {
			if err := os.WriteFile(filepath.Join(dir, name), []byte("7\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		if _, _, err := prepareBoot(dir, "/workspace", bootEnv{
			SetupB64: b64("true"), ReposB64: reposB64, InitB64: b64("true"),
		}); err != nil {
			t.Fatal(err)
		}
		files := bootFiles(t, dir)
		for _, name := range []string{"setup.rc", "clone.rc", "init.rc"} {
			if v, ok := files[name]; ok {
				t.Errorf("%s survived boot with %q", name, v)
			}
		}
	})

	t.Run("an unreadable repo list fails the clone stage rather than the boot", func(t *testing.T) {
		// The driver's contract (internal/driver/docker.go): an ABSENT
		// variable means "nothing to clone"; a present-but-unparseable one is
		// a failed stage, so the user gets a tail saying why instead of a
		// container that vanished.
		dir := filepath.Join(t.TempDir(), ".rainier")
		stages, _, err := prepareBoot(dir, "/workspace", bootEnv{ReposB64: "not!base64!"})
		if err != nil {
			t.Fatalf("prepareBoot = %v, want a failing clone stage instead of an error", err)
		}
		if got := stageNames(stages); !reflect.DeepEqual(got, []string{stageClone}) {
			t.Fatalf("stages = %v, want a clone stage", got)
		}
		script := bootFiles(t, dir)["clones.sh"]
		if !strings.Contains(script, "exit 1") {
			t.Errorf("clones.sh = %q, want a script that fails loudly", script)
		}
		if _, err := exec.LookPath("sh"); err == nil {
			out, err := exec.Command("sh", stages[0].ScriptPath).CombinedOutput()
			if err == nil {
				t.Errorf("the generated script exited 0 (output %q)", out)
			}
			if !strings.Contains(string(out), "RAINIER_REPOS_B64") {
				t.Errorf("output = %q, want it to name the variable that could not be read", out)
			}
		}
	})

	t.Run("a bad setup script is still fatal", func(t *testing.T) {
		// Plan 4 behavior, deliberately unchanged: setup has always been
		// fatal-on-garbage, and a session that runs its agent in an
		// environment that was never built is the outcome worst of all.
		dir := filepath.Join(t.TempDir(), ".rainier")
		if _, _, err := prepareBoot(dir, "/workspace", bootEnv{SetupB64: "not!base64!"}); err == nil {
			t.Fatal("prepareBoot accepted an undecodable RAINIER_SETUP_B64")
		}
	})
}

func keysOf(m map[string]string) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

// --- the chain against a real shell ---

// stubGit installs a fake `git` on PATH that records every invocation, one
// line per call with the argument boundaries preserved. Returns the bin
// directory and the log path.
func stubGit(t *testing.T, body string) (binDir, logPath string) {
	t.Helper()
	binDir = filepath.Join(t.TempDir(), "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	logPath = filepath.Join(binDir, "git.log")
	script := "#!/bin/sh\n" +
		"for a in \"$@\"; do printf '<%s>' \"$a\" >> " + shQuote(logPath) + "; done\n" +
		"printf '\\n' >> " + shQuote(logPath) + "\n" + body
	if err := os.WriteFile(filepath.Join(binDir, "git"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return binDir, logPath
}

func runChain(t *testing.T, binDir string, argv []string) (string, int) {
	t.Helper()
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = append(os.Environ(), "PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	out, err := cmd.CombinedOutput()
	rc := 0
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		rc = ee.ExitCode()
	} else if err != nil {
		t.Fatalf("running the chain: %v (output %q)", err, out)
	}
	return string(out), rc
}

func readRC(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		return "<missing>"
	}
	return strings.TrimSpace(string(b))
}

// TestChainAgainstRealSh runs the composed chain through /bin/sh with a
// PATH-shimmed git, which is the only way to prove the properties the whole
// boot rests on: the stages run in order, each writes its own rc file, a
// failure stops the chain there and never reaches the agent, and a
// quoting-hostile branch name arrives at git byte for byte.
func TestChainAgainstRealSh(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("no sh on PATH; skipping chain execution test")
	}
	const childMarker = "AGENT-RAN"
	childArgv := []string{"sh", "-c", `printf "<%s>" "$@"; echo ` + childMarker, "child", "a b", "it's"}

	// A session branch that is hostile to a shell AND illegal to git: session
	// names are not sanitized into branch names (controller ruling), so this
	// has to reach git verbatim and be rejected there.
	const hostileBranch = `rainier/a b'; touch /tmp/pwned; echo '`

	newBoot := func(t *testing.T, env bootEnv) (dir string, stages []bootStage, argv []string) {
		t.Helper()
		root := t.TempDir()
		dir = filepath.Join(root, ".rainier")
		stages, vars, err := prepareBoot(dir, root, env)
		if err != nil {
			t.Fatal(err)
		}
		return dir, stages, chainArgv(vars, stages, childArgv)
	}
	reposB64 := base64.StdEncoding.EncodeToString([]byte(
		`[{"owner":"acme","name":"api","base_branch":"main","session_branch":` +
			strconv.Quote(hostileBranch) + `,"dir":"api"}]`))
	setupB64 := base64.StdEncoding.EncodeToString([]byte("echo SETUP-RAN\n"))
	initB64 := base64.StdEncoding.EncodeToString([]byte("echo INIT-RAN\n"))

	t.Run("every stage succeeds and the agent execs", func(t *testing.T) {
		binDir, gitLog := stubGit(t, "exit 0\n")
		dir, _, argv := newBoot(t, bootEnv{SetupB64: setupB64, ReposB64: reposB64, InitB64: initB64})
		out, rc := runChain(t, binDir, argv)

		if rc != 0 {
			t.Fatalf("chain rc = %d, want 0 (output %q)", rc, out)
		}
		// Order is the contract: setup is pre-clone (it is what gets cached),
		// init is post-clone (it may depend on repo content).
		wantOrder := []string{"SETUP-RAN", "rainier clone:", "INIT-RAN", childMarker}
		at := -1
		for _, m := range wantOrder {
			i := strings.Index(out, m)
			if i < 0 {
				t.Fatalf("output %q is missing %q", out, m)
			}
			if i < at {
				t.Fatalf("output %q has %q out of order (want %v)", out, m, wantOrder)
			}
			at = i
		}
		if !strings.Contains(out, `<a b><it's>`) {
			t.Errorf("output %q: the agent's argv did not survive the chain", out)
		}
		for _, name := range []string{"setup.rc", "clone.rc", "init.rc"} {
			if got := readRC(t, filepath.Join(dir, name)); got != "0" {
				t.Errorf("%s = %s, want 0", name, got)
			}
		}
		// The hostile branch name reaches git as ONE argument, unexpanded.
		log, err := os.ReadFile(gitLog)
		if err != nil {
			t.Fatal(err)
		}
		wantClone := "<clone><--branch><main><--><https://github.com/acme/api.git><" + filepath.Dir(dir) + "/api>"
		if !strings.Contains(string(log), wantClone) {
			t.Errorf("git log =\n%s\nwant a call %s", log, wantClone)
		}
		if !strings.Contains(string(log), "<checkout><-b><"+hostileBranch+">") {
			t.Errorf("git log =\n%s\nwant the session branch verbatim (%q)", log, hostileBranch)
		}
		if _, err := os.Stat("/tmp/pwned"); err == nil {
			os.Remove("/tmp/pwned")
			t.Fatal("the session branch name was executed by the shell")
		}
	})

	t.Run("a failing clone stops the chain and names its stage", func(t *testing.T) {
		binDir, _ := stubGit(t, "echo \"fatal: Authentication failed for 'https://github.com/acme/api.git/'\" >&2\nexit 128\n")
		dir, _, argv := newBoot(t, bootEnv{SetupB64: setupB64, ReposB64: reposB64, InitB64: initB64})
		out, rc := runChain(t, binDir, argv)

		if rc != 128 {
			t.Fatalf("chain rc = %d, want git's own 128 (output %q)", rc, out)
		}
		if got := readRC(t, filepath.Join(dir, "setup.rc")); got != "0" {
			t.Errorf("setup.rc = %s, want 0", got)
		}
		if got := readRC(t, filepath.Join(dir, "clone.rc")); got != "128" {
			t.Errorf("clone.rc = %s, want 128", got)
		}
		if got := readRC(t, filepath.Join(dir, "init.rc")); got != "<missing>" {
			t.Errorf("init.rc = %s, want no file — init must not run after a failed clone", got)
		}
		if strings.Contains(out, childMarker) {
			t.Fatalf("the agent ran after a failed clone: %q", out)
		}
		if !strings.Contains(out, "Authentication failed") {
			t.Errorf("output %q lost git's own error — it is the only diagnostic the user gets", out)
		}
	})

	t.Run("a failing init stops the chain", func(t *testing.T) {
		binDir, _ := stubGit(t, "exit 0\n")
		dir, _, argv := newBoot(t, bootEnv{
			ReposB64: reposB64,
			InitB64:  base64.StdEncoding.EncodeToString([]byte("echo INIT-BOOM\nexit 3\n")),
		})
		out, rc := runChain(t, binDir, argv)
		if rc != 3 {
			t.Fatalf("chain rc = %d, want 3 (output %q)", rc, out)
		}
		if got := readRC(t, filepath.Join(dir, "init.rc")); got != "3" {
			t.Errorf("init.rc = %s, want 3", got)
		}
		if strings.Contains(out, childMarker) {
			t.Fatalf("the agent ran after a failed init: %q", out)
		}
	})

	t.Run("an already-cloned workspace is not re-cloned", func(t *testing.T) {
		// A cold-parked session keeps its volume, so the second boot finds the
		// repo already there. `git clone` into a non-empty directory fails, so
		// without the guard every resume of a repo session would fail its
		// clone stage.
		binDir, gitLog := stubGit(t, "exit 0\n")
		root := t.TempDir()
		dir := filepath.Join(root, ".rainier")
		if err := os.MkdirAll(filepath.Join(root, "api", ".git"), 0o755); err != nil {
			t.Fatal(err)
		}
		stages, vars, err := prepareBoot(dir, root, bootEnv{ReposB64: reposB64})
		if err != nil {
			t.Fatal(err)
		}
		out, rc := runChain(t, binDir, chainArgv(vars, stages, childArgv))
		if rc != 0 {
			t.Fatalf("chain rc = %d, want 0 (output %q)", rc, out)
		}
		if !strings.Contains(out, "already cloned") {
			t.Errorf("output %q, want the skip to be visible on the PTY", out)
		}
		if b, err := os.ReadFile(gitLog); err == nil && len(b) > 0 {
			t.Errorf("git was called on a resume: %s", b)
		}
		if !strings.Contains(out, childMarker) {
			t.Errorf("output %q: the agent must still start on a resume", out)
		}
	})

	t.Run("the git environment reaches the agent, not just the stages", func(t *testing.T) {
		// The agent's own `git push` is as much a user of this environment as
		// the clone stage is — and it is the one nothing bounds, so a git that
		// could still prompt there would hang the session's terminal forever.
		// The variables are read back from a child that runs where the AGENT
		// runs, after every stage, with `set -u` so an unset one is a failure
		// rather than an empty string that looks like the right answer.
		binDir, _ := stubGit(t, "exit 0\n")
		root := t.TempDir()
		dir := filepath.Join(root, ".rainier")
		stages, vars, err := prepareBoot(dir, root, bootEnv{ReposB64: reposB64})
		if err != nil {
			t.Fatal(err)
		}
		argv := chainArgv(vars, stages, []string{"sh", "-c",
			`set -u; echo "cfg=$GIT_CONFIG_GLOBAL prompt=[$GIT_TERMINAL_PROMPT] ask=[$GIT_ASKPASS] sshask=[$SSH_ASKPASS]"`,
			"child"})
		out, rc := runChain(t, binDir, argv)
		if rc != 0 {
			t.Fatalf("chain rc = %d (output %q)", rc, out)
		}
		want := "cfg=" + filepath.Join(dir, "gitconfig") + " prompt=[0] ask=[] sshask=[]"
		if !strings.Contains(out, want) {
			t.Errorf("output %q, want %q — the agent's own git must use the same config and never prompt", out, want)
		}
	})
}

// --- the stage watcher ---

// TestWatchStage covers the outcomes one stage can reach. It generalizes the
// Plan 4 setup watcher: the setup stage keeps its exact wire vocabulary
// (setup_done / setup_failed) and the new stages report stage_failed.
func TestWatchStage(t *testing.T) {
	const poll = 5 * time.Millisecond
	stage := func(dir, name, rcName string, timeout time.Duration) bootStage {
		return bootStage{Name: name, ScriptPath: filepath.Join(dir, "s.sh"), RCPath: filepath.Join(dir, rcName), Timeout: timeout}
	}

	t.Run("setup rc 0 is setup_done", func(t *testing.T) {
		dir := t.TempDir()
		st := stage(dir, stageSetup, "setup.rc", time.Minute)
		if err := os.WriteFile(st.RCPath, []byte("0\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		ps, cont := watchStage(context.Background(), func() {}, st, filepath.Join(dir, "s.log"), poll)
		if !cont {
			t.Fatal("a successful stage must let the chain continue")
		}
		if len(ps) != 1 {
			t.Fatalf("payloads = %d, want exactly the setup verdict", len(ps))
		}
		if got := decodeControl(t, ps[0]); got.Kind != "setup_done" || got.RC != 0 || got.Tail != "" {
			t.Fatalf("outcome = %+v, want {Kind:setup_done}", got)
		}
	})

	t.Run("a clone or init that succeeds says nothing", func(t *testing.T) {
		// setup_done is what controld's snapshot orchestration keys on; a
		// finished clone or init is not a fleet-wide fact and has no listener.
		for _, name := range []string{stageClone, stageInit} {
			dir := t.TempDir()
			st := stage(dir, name, name+".rc", time.Minute)
			if err := os.WriteFile(st.RCPath, []byte("0\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			ps, cont := watchStage(context.Background(), func() {}, st, filepath.Join(dir, "s.log"), poll)
			if !cont || len(ps) != 0 {
				t.Fatalf("%s: payloads = %d, cont = %v; want silence and a continue", name, len(ps), cont)
			}
		}
	})

	t.Run("a failing setup is the legacy setup_failed", func(t *testing.T) {
		dir := t.TempDir()
		st := stage(dir, stageSetup, "setup.rc", time.Minute)
		logPath := filepath.Join(dir, "s.log")
		writeLog(t, logPath, "installing...\n", "boom: no such package\n")
		go func() {
			time.Sleep(20 * time.Millisecond)
			os.WriteFile(st.RCPath, []byte("7\n"), 0o644)
		}()
		ps, cont := watchStage(context.Background(), func() {}, st, logPath, poll)
		if cont {
			t.Fatal("a failed stage must stop the chain")
		}
		if len(ps) != 1 {
			t.Fatalf("payloads = %d, want one", len(ps))
		}
		got := decodeControl(t, ps[0])
		if got.Kind != "setup_failed" || got.RC != 7 {
			t.Fatalf("outcome = %+v, want {Kind:setup_failed RC:7} — the wire name a Plan 4 runnerd knows", got)
		}
		if got.Stage != "" {
			t.Errorf("stage = %q, want the legacy event to stay byte-identical", got.Stage)
		}
		if !strings.Contains(got.Tail, "boom: no such package") {
			t.Errorf("tail = %q, want it to carry the session's output", got.Tail)
		}
	})

	t.Run("a failing clone or init is stage_failed", func(t *testing.T) {
		for _, name := range []string{stageClone, stageInit} {
			dir := t.TempDir()
			st := stage(dir, name, name+".rc", time.Minute)
			logPath := filepath.Join(dir, "s.log")
			writeLog(t, logPath, "fatal: could not read from remote\n")
			if err := os.WriteFile(st.RCPath, []byte("128\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			ps, cont := watchStage(context.Background(), func() {}, st, logPath, poll)
			if cont {
				t.Fatalf("%s: a failed stage must stop the chain", name)
			}
			if len(ps) != 1 {
				t.Fatalf("%s: payloads = %d, want one", name, len(ps))
			}
			got := decodeControl(t, ps[0])
			if got.Kind != "stage_failed" || got.Stage != name || got.RC != 128 {
				t.Fatalf("outcome = %+v, want {Kind:stage_failed Stage:%s RC:128}", got, name)
			}
			if !strings.Contains(got.Tail, "could not read from remote") {
				t.Errorf("tail = %q", got.Tail)
			}
		}
	})

	t.Run("an auth-shaped clone failure reports the credential first", func(t *testing.T) {
		dir := t.TempDir()
		st := stage(dir, stageClone, "clone.rc", time.Minute)
		logPath := filepath.Join(dir, "s.log")
		writeLog(t, logPath, "remote: Support for password authentication was removed.\n",
			"fatal: Authentication failed for 'https://github.com/acme/api.git/'\n")
		if err := os.WriteFile(st.RCPath, []byte("128\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		ps, _ := watchStage(context.Background(), func() {}, st, logPath, poll)
		if len(ps) != 2 {
			t.Fatalf("payloads = %d, want the rejection and the stage failure", len(ps))
		}
		if got := decodeControl(t, ps[0]); got.Kind != "credential_rejected" || got.ID != 0 {
			t.Fatalf("first payload = %+v, want {Kind:credential_rejected ID:0}", got)
		}
		// The token itself must never travel with the report: controld knows
		// whose credential it minted.
		if !strings.Contains(string(ps[0]), `"kind":"credential_rejected"`) || len(ps[0]) > 64 {
			t.Errorf("credential_rejected payload = %s, want the bare event", ps[0])
		}
		if got := decodeControl(t, ps[1]); got.Kind != "stage_failed" || got.Stage != stageClone {
			t.Fatalf("second payload = %+v, want the clone's stage_failed", got)
		}
	})

	t.Run("an auth-shaped failure in another stage is not a rejection", func(t *testing.T) {
		// Only the clone stage's git talks to GitHub with a minted token. A
		// setup script printing "403" is about something else entirely, and
		// flipping the user's credential over it would be a false accusation.
		dir := t.TempDir()
		st := stage(dir, stageSetup, "setup.rc", time.Minute)
		logPath := filepath.Join(dir, "s.log")
		writeLog(t, logPath, "curl: (22) The requested URL returned error: 403\n")
		if err := os.WriteFile(st.RCPath, []byte("22\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		ps, _ := watchStage(context.Background(), func() {}, st, logPath, poll)
		if len(ps) != 1 {
			t.Fatalf("payloads = %d, want only the stage failure", len(ps))
		}
	})

	t.Run("timeout stops the session and reports rc -1", func(t *testing.T) {
		dir := t.TempDir()
		var mu sync.Mutex
		stops := 0
		stop := func() { mu.Lock(); stops++; mu.Unlock() }
		const timeout = 60 * time.Millisecond
		st := stage(dir, stageClone, "clone.rc", timeout)
		ps, cont := watchStage(context.Background(), stop, st, filepath.Join(dir, "s.log"), poll)
		if cont || len(ps) != 1 {
			t.Fatalf("payloads = %d, cont = %v; want one failure and a stop", len(ps), cont)
		}
		got := decodeControl(t, ps[0])
		if got.Kind != "stage_failed" || got.RC != -1 {
			t.Fatalf("outcome = %+v, want {Kind:stage_failed RC:-1}", got)
		}
		if got.Tail != stageTimedOutTail(stageClone, timeout) {
			t.Errorf("tail = %q, want %q", got.Tail, stageTimedOutTail(stageClone, timeout))
		}
		mu.Lock()
		defer mu.Unlock()
		if stops != 1 {
			t.Errorf("session stopped %d times on timeout, want exactly 1", stops)
		}
	})

	t.Run("a half-written rc file is not an outcome", func(t *testing.T) {
		// `echo $rc > file` truncates before it writes, so a poll can catch
		// the file existing and empty. Treating that as rc 0 would report a
		// stage done that had not finished.
		dir := t.TempDir()
		st := stage(dir, stageSetup, "setup.rc", time.Minute)
		if err := os.WriteFile(st.RCPath, nil, 0o644); err != nil {
			t.Fatal(err)
		}
		go func() {
			time.Sleep(30 * time.Millisecond)
			os.WriteFile(st.RCPath, []byte("5\n"), 0o644)
		}()
		ps, _ := watchStage(context.Background(), func() {}, st, filepath.Join(dir, "s.log"), poll)
		if got := decodeControl(t, ps[0]); got.Kind != "setup_failed" || got.RC != 5 {
			t.Fatalf("outcome = %+v, want {Kind:setup_failed RC:5} — an empty rc file must not read as 0", got)
		}
	})

	t.Run("a cancelled context reports nothing", func(t *testing.T) {
		dir := t.TempDir()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		st := stage(dir, stageSetup, "setup.rc", time.Minute)
		ps, cont := watchStage(ctx, func() {}, st, filepath.Join(dir, "s.log"), poll)
		if len(ps) != 0 || cont {
			t.Fatalf("payloads = %v, cont = %v; want nothing when the process is shutting down", ps, cont)
		}
	})

	t.Run("no timeout means no bound", func(t *testing.T) {
		dir := t.TempDir()
		st := stage(dir, stageInit, "init.rc", 0)
		go func() {
			time.Sleep(20 * time.Millisecond)
			os.WriteFile(st.RCPath, []byte("0\n"), 0o644)
		}()
		ps, cont := watchStage(context.Background(), func() { t.Error("an unbounded stage was stopped") },
			st, filepath.Join(dir, "s.log"), poll)
		if !cont || len(ps) != 0 {
			t.Fatalf("payloads = %d, cont = %v", len(ps), cont)
		}
	})
}

// TestWatchStagesWalksTheChain proves the watcher follows the chain the
// wrapper runs: every stage in order, the first failure ends it, and stages
// after that failure are never waited on (their scripts never ran, so their rc
// files never appear).
func TestWatchStagesWalksTheChain(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "s.log")
	writeLog(t, logPath, "fatal: Authentication failed\n")
	stages := []bootStage{
		{Name: stageSetup, RCPath: filepath.Join(dir, "setup.rc"), Timeout: time.Minute},
		{Name: stageClone, RCPath: filepath.Join(dir, "clone.rc"), Timeout: time.Minute},
		{Name: stageInit, RCPath: filepath.Join(dir, "init.rc"), Timeout: time.Minute},
	}
	if err := os.WriteFile(stages[0].RCPath, []byte("0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stages[1].RCPath, []byte("128\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// init.rc deliberately never appears: the chain stopped at the clone.

	out := make(chan []byte, 8)
	watchStages(context.Background(), func() {}, stages, logPath, 5*time.Millisecond, out)
	close(out)

	var kinds []string
	for p := range out {
		kinds = append(kinds, decodeControl(t, p).Kind)
	}
	want := []string{"setup_done", "credential_rejected", "stage_failed"}
	if !reflect.DeepEqual(kinds, want) {
		t.Fatalf("events = %v, want %v", kinds, want)
	}
}

// TestAuthRejected pins the shape of a git failure that means "GitHub said
// no": it is what makes controld flip the vault row to needs_refresh, so the
// NEXT operation gets a clear named action instead of another opaque 403.
func TestAuthRejected(t *testing.T) {
	cases := []struct {
		tail string
		want bool
	}{
		{"fatal: Authentication failed for 'https://github.com/acme/api.git/'", true},
		{"remote: Invalid username or password.\nfatal: authentication failed", true},
		{"The requested URL returned error: 403", true},
		{"error: The requested URL returned error: 401 Unauthorized", true},
		{"fatal: repository 'https://github.com/acme/api.git/' not found", false},
		{"fatal: Remote branch nope not found in upstream origin", false},
		{"", false},
	}
	for _, c := range cases {
		if got := authRejected(c.tail); got != c.want {
			t.Errorf("authRejected(%q) = %v, want %v", c.tail, got, c.want)
		}
	}
}

// TestStageTimedOutTail pins the timeout message's shape, which controld
// renders into a session's error text verbatim.
func TestStageTimedOutTail(t *testing.T) {
	if got, want := stageTimedOutTail(stageSetup, 900*time.Second), "setup timed out after 900s"; got != want {
		t.Errorf("stageTimedOutTail(setup, 900s) = %q, want %q", got, want)
	}
	if got, want := stageTimedOutTail(stageClone, 600*time.Second), "clone timed out after 600s"; got != want {
		t.Errorf("stageTimedOutTail(clone, 600s) = %q, want %q", got, want)
	}
}

// TestNoTokenEscapesToDisk is the hygiene sweep for everything this file
// writes: the ONLY path a credential may take is the helper's stdout, so
// nothing composed here — scripts, config, control events — may be able to
// carry one.
func TestNoTokenEscapesToDisk(t *testing.T) {
	dir := filepath.Join(t.TempDir(), ".rainier")
	repos := base64.StdEncoding.EncodeToString([]byte(
		`[{"owner":"acme","name":"api","base_branch":"main","session_branch":"rainier/x","dir":"api"}]`))
	if _, _, err := prepareBoot(dir, "/workspace", bootEnv{
		ReposB64: repos, InitB64: base64.StdEncoding.EncodeToString([]byte("true")),
		GitAuthorName: "alice", GitAuthorEmail: "42+alice@users.noreply.github.com",
	}); err != nil {
		t.Fatal(err)
	}
	for name, body := range bootFiles(t, dir) {
		for _, forbidden := range []string{"x-access-token", "password=", "ghp_", "ghs_", "://x-", "Authorization"} {
			if strings.Contains(body, forbidden) {
				t.Errorf("%s contains %q:\n%s", name, forbidden, body)
			}
		}
	}
	// The mint request itself carries no secret in either direction on disk.
	b, err := json.Marshal(relay.ControlEvent{Kind: "credential_rejected"})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"kind":"credential_rejected"}` {
		t.Errorf("credential_rejected wire shape = %s", b)
	}
	_ = fmt.Sprint()
}

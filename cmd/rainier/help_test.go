package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func buildCLI(t *testing.T, ldflags string) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "rainier")
	args := []string{"build", "-o", bin}
	if ldflags != "" {
		args = append(args, "-ldflags", ldflags)
	}
	args = append(args, ".")
	if out, err := exec.Command("go", args...).CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	return bin
}

func runCLI(t *testing.T, bin, config string, args ...string) (string, int) {
	t.Helper()
	cmd := exec.Command(bin, args...)
	cmd.Env = append(os.Environ(), "RAINIER_CONFIG="+config, "RAINIER_NO_BROWSER=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		return string(out), 0
	}
	if e, ok := err.(*exec.ExitError); ok {
		return string(out), e.ExitCode()
	}
	t.Fatal(err)
	return "", -1
}

// runCLISplit is runCLI with the two streams kept apart, for the tests that
// are about which stream something went to.
func runCLISplit(t *testing.T, bin, config string, args ...string) (stdout, stderr string, code int) {
	t.Helper()
	cmd := exec.Command(bin, args...)
	cmd.Env = append(os.Environ(), "RAINIER_CONFIG="+config, "RAINIER_NO_BROWSER=1")
	var out, errBuf strings.Builder
	cmd.Stdout, cmd.Stderr = &out, &errBuf
	err := cmd.Run()
	code = 0
	if err != nil {
		e, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatal(err)
		}
		code = e.ExitCode()
	}
	return out.String(), errBuf.String(), code
}

// TestRootHelpInventory is the contract's most load-bearing test: the whole
// point of this surface is that a new developer can read it off one screen
// (docs/cli-v0-contract.md §1, §2), and that property is destroyed one
// well-meaning line at a time. So the inventory is exact — a command added to
// the default help fails this test until somebody decides it belongs in the
// product's first impression — and the height is bounded at a normal
// terminal's.
func TestRootHelpInventory(t *testing.T) {
	bin := buildCLI(t, "")
	config := filepath.Join(t.TempDir(), "absent.json")

	// Exactly the public surface. Nothing else may appear as a command.
	public := []string{
		"login", "logout", "status",
		"new", "ls", "info", "attach", "stop", "delete",
		"agent login", "agent status", "agent logout",
		"help", "version",
	}
	// Removed outright, or moved behind `rainier help all`. None of these may
	// be advertised in the first-run experience.
	hidden := []string{
		"diff", "doctor", "suspend", "resume", "snapshot", "rm",
		"push", "pull", "creds", "connection", "secret", "env",
		"context", "workspace",
	}

	for _, args := range [][]string{{"--help"}, {"-h"}, {"help"}} {
		out, code := runCLI(t, bin, config, args...)
		if code != 0 {
			t.Fatalf("%v: exit %d\n%s", args, code, out)
		}
		lines := strings.Count(strings.TrimRight(out, "\n"), "\n") + 1
		if lines > 24 {
			t.Errorf("%v: %d lines; the default help must fit one terminal screen\n%s", args, lines, out)
		}
		for _, want := range public {
			if !strings.Contains(out, want) {
				t.Errorf("%v: default help is missing the public command %q\n%s", args, want, out)
			}
		}
		for _, unwanted := range hidden {
			if commandMentioned(out, unwanted) {
				t.Errorf("%v: default help advertises %q, which belongs under `rainier help all`\n%s", args, unwanted, out)
			}
		}
	}
}

// commandMentioned reports whether help text OFFERS a command, as opposed to
// happening to contain the word. "Sign in to your Rainier workspace" is prose;
// an indented line beginning with "workspace" is an offer, and so is any
// "rainier workspace" in running text. Both are what the inventory is about;
// neither is a plain substring search, which would flag the prose.
func commandMentioned(out, command string) bool {
	if strings.Contains(out, "rainier "+command+" ") || strings.HasSuffix(strings.TrimRight(out, "\n"), "rainier "+command) {
		return true
	}
	for _, line := range strings.Split(out, "\n") {
		if !strings.HasPrefix(line, "  ") {
			continue
		}
		if fields := strings.Fields(line); len(fields) > 0 && fields[0] == command {
			return true
		}
	}
	return false
}

// `rainier help all` is where everything the default screen leaves out has to
// be findable, clearly labeled as advanced rather than as part of the primary
// journey.
func TestHelpAllDocumentsWhatTheDefaultHides(t *testing.T) {
	bin := buildCLI(t, "")
	out, code := runCLI(t, bin, filepath.Join(t.TempDir(), "absent.json"), "help", "all")
	if code != 0 {
		t.Fatalf("help all: exit %d\n%s", code, out)
	}
	for _, want := range []string{
		"doctor", "suspend", "rm", "agent ls", // the compatibility aliases
		"resume", "snapshot", "push", "pull", // low-level operations
		"creds", "connection", "secret", "env", "context", "workspace",
		"ADVANCED", "COMPATIBILITY ALIASES",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("help all does not document %q:\n%s", want, out)
		}
	}
	// diff is gone completely, not relocated (contract §2.3).
	if commandMentioned(out, "diff") {
		t.Errorf("help all still advertises `diff`:\n%s", out)
	}
}

// `rainier diff` was removed completely. An invocation of it must be an
// invalid invocation — exit 2 with the usage — not a command that silently
// does something else.
func TestDiffIsGone(t *testing.T) {
	bin := buildCLI(t, "")
	config := filepath.Join(t.TempDir(), "absent.json")
	for _, args := range [][]string{{"diff", "sess_example"}, {"diff"}} {
		out, code := runCLI(t, bin, config, args...)
		if code != 2 {
			t.Errorf("%v: exit %d, want 2\n%s", args, code, out)
		}
		if !strings.Contains(out, "unknown command") {
			t.Errorf("%v: does not report an unknown command\n%s", args, out)
		}
	}
	if out, code := runCLI(t, bin, config, "help", "diff"); code != 2 || strings.Contains(out, "usage: rainier diff") {
		t.Errorf("help diff: exit %d\n%s", code, out)
	}
}

// Every command the CLI dispatches — public, alias, or advanced — must answer
// `rainier help X` and `rainier X --help` identically, because a user who
// found the command in `help all` will reach for either.
func TestPerCommandHelp(t *testing.T) {
	bin := buildCLI(t, "")
	config := filepath.Join(t.TempDir(), "absent.json")
	// `version` and `help` are intercepted before any handler, so they have
	// no `--help` of their own; every other command must answer both spellings
	// identically.
	commands := []string{
		"login", "logout", "status", "new", "ls", "info", "attach", "stop",
		"delete", "agent",
		"doctor", "suspend", "rm",
		"env", "secret", "context", "workspace", "resume", "snapshot",
		"push", "pull", "creds", "connection",
	}
	for _, command := range []string{"version", "help"} {
		if out, code := runCLI(t, bin, config, "help", command); code != 0 || !strings.Contains(out, "usage: rainier "+command) {
			t.Errorf("help %s: exit %d\n%s", command, code, out)
		}
	}
	for _, command := range commands {
		a, code := runCLI(t, bin, config, "help", command)
		b, code2 := runCLI(t, bin, config, command, "--help")
		if code != 0 || code2 != 0 || a != b {
			t.Errorf("help %s mismatch: %d/%d\n--- help ---\n%s--- --help ---\n%s", command, code, code2, a, b)
			continue
		}
		if !strings.Contains(a, "usage: rainier "+command) {
			t.Errorf("help %s does not lead with its usage line:\n%s", command, a)
		}
	}
	for _, args := range [][]string{{"env", "create", "--help"}, {"agent", "login", "--help"}, {"secret", "set", "--help"}, {"context", "use", "--help"}} {
		if out, code := runCLI(t, bin, config, args...); code != 0 || !strings.Contains(out, "usage:") {
			t.Errorf("%v: %d %s", args, code, out)
		}
	}
}

// Help somebody asked for is output and belongs on stdout; usage printed
// because an invocation was wrong is a diagnostic and belongs on stderr
// (contract §6.1).
func TestHelpStreamsAndExitCodes(t *testing.T) {
	bin := buildCLI(t, "")
	config := filepath.Join(t.TempDir(), "absent.json")

	stdout, stderr, code := runCLISplit(t, bin, config, "help")
	if code != 0 || !strings.Contains(stdout, "usage: rainier") || strings.Contains(stderr, "usage: rainier") {
		t.Errorf("requested help went to the wrong stream: exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	stdout, stderr, code = runCLISplit(t, bin, config, "nonesuch")
	if code != 2 || stdout != "" || !strings.Contains(stderr, "usage: rainier") {
		t.Errorf("usage-on-error went to the wrong stream: exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	stdout, _, code = runCLISplit(t, bin, config)
	if code != 2 || stdout != "" {
		t.Errorf("bare invocation: exit %d, stdout %q", code, stdout)
	}
}

func TestVersion(t *testing.T) {
	bin := buildCLI(t, "-X main.version=v9.8.7-test")
	config := filepath.Join(t.TempDir(), "absent.json")
	for _, args := range [][]string{{"version"}, {"--version"}} {
		out, code := runCLI(t, bin, config, args...)
		if code != 0 || strings.TrimSpace(out) != "rainier v9.8.7-test" {
			t.Errorf("version: %d %s", code, out)
		}
	}
	for _, args := range [][]string{{"help", "unknown"}, {"version", "extra"}, {"doctor", "extra"}} {
		if out, code := runCLI(t, bin, config, args...); code != 2 {
			t.Errorf("%v: exit %d %s", args, code, out)
		}
	}
}

func TestCLISourceVersionIsHonest(t *testing.T) {
	out, code := runCLI(t, buildCLI(t, ""), filepath.Join(t.TempDir(), "absent"), "version")
	if code != 0 || strings.TrimSpace(out) != "rainier dev" {
		t.Fatalf("source version: %d %s", code, out)
	}
}

// A real nested worktree catches Go toolchains that stamp the enclosing
// checkout's revision instead of the tree whose source is being compiled.
func TestMakeBuildVersionFromWorktree(t *testing.T) {
	repo := t.TempDir()
	for _, dir := range []string{"cmd/rainier", "scripts"} {
		if err := os.MkdirAll(filepath.Join(repo, dir), 0755); err != nil {
			t.Fatal(err)
		}
	}
	write := func(path, data string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(data), 0644); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{"Makefile", "cmd/rainier/help.go", "scripts/build.sh"} {
		data, err := os.ReadFile(filepath.Join("../..", path))
		if err != nil {
			t.Fatal(err)
		}
		write(filepath.Join(repo, path), string(data))
	}
	write(filepath.Join(repo, "go.mod"), "module example.test/versionfixture\n\ngo 1.25\n")
	write(filepath.Join(repo, ".gitignore"), "bin/\n.worktrees/\n")
	write(filepath.Join(repo, "cmd/rainier/main.go"), `package main
import "fmt"
const envUsage, agentUsage, secretUsage, connectionUsage = "", "", "", ""
var connectionProviders []string
func agentProviderNames() []string { return nil }
func printManual() {}
func main() { fmt.Println("rainier " + buildVersion()) }
`)
	git := func(dir string, args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-c", "user.name=Test Developer", "-c", "user.email=developer@example.test", "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null"}, args...)...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	check := func(dir, want string, env ...string) {
		t.Helper()
		cmd := exec.Command("make", "build")
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), env...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("make build: %v\n%s", err, out)
		}
		out, code := runCLI(t, filepath.Join(dir, "bin/rainier"), filepath.Join(t.TempDir(), "absent"), "version")
		if code != 0 || strings.TrimSpace(out) != want {
			t.Fatalf("build in %s: got %q (%d), want %q", dir, out, code, want)
		}
	}
	git(repo, "init", "--initial-branch=main")
	git(repo, "add", ".")
	git(repo, "commit", "-m", "synthetic version fixture")
	rootRevision := git(repo, "rev-parse", "HEAD")
	check(repo, "rainier dev ("+rootRevision[:12]+")")
	worktree := filepath.Join(repo, ".worktrees", "version-test")
	git(repo, "worktree", "add", "-b", "version-test", worktree)
	write(filepath.Join(worktree, "fixture.txt"), "worktree-only commit\n")
	git(worktree, "add", "fixture.txt")
	git(worktree, "commit", "-m", "distinct synthetic worktree")
	revision := git(worktree, "rev-parse", "HEAD")
	write(filepath.Join(repo, "root-only.txt"), "root dirt must not affect the worktree\n")
	check(worktree, "rainier dev ("+revision[:12]+")")
	write(filepath.Join(worktree, "fixture.txt"), "uncommitted worktree change\n")
	check(worktree, "rainier dev ("+revision[:12]+", modified)")
	// An exported source directory must not claim the surrounding repo's HEAD.
	archive := filepath.Join(repo, "exported-source")
	for _, path := range []string{"Makefile", "go.mod", "scripts/build.sh", "cmd/rainier/help.go", "cmd/rainier/main.go"} {
		data, err := os.ReadFile(filepath.Join(repo, path))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(filepath.Join(archive, path)), 0755); err != nil {
			t.Fatal(err)
		}
		write(filepath.Join(archive, path), string(data))
	}
	check(archive, "rainier dev")
	// Explicit linker customization owns the version, just as with go build.
	check(worktree, "rainier v9.8.7-synthetic", "GOFLAGS=-ldflags=-X=main.version=v9.8.7-synthetic")
	check(worktree, "rainier dev", "GOFLAGS=-ldflags=-s")
}

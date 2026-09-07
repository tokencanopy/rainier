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

func TestCLIHelpAndVersion(t *testing.T) {
	bin := buildCLI(t, "-X main.version=v9.8.7-test")
	config := filepath.Join(t.TempDir(), "corrupt.json")
	if err := os.WriteFile(config, []byte("invalid secret config"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"--help"}, {"-h"}, {"help"}} {
		out, code := runCLI(t, bin, config, args...)
		lines := 0
		for _, line := range strings.Split(out, "\n") {
			if strings.TrimSpace(line) != "" {
				lines++
			}
		}
		if code != 0 || lines > 26 || !strings.Contains(out, "doctor") {
			t.Errorf("%v: exit %d, %d lines\n%s", args, code, lines, out)
		}
	}
	for _, command := range []string{"login", "new", "attach", "doctor", "env", "agent", "secret", "context", "workspace", "push", "pull", "ls", "suspend", "resume", "snapshot", "rm", "diff", "creds"} {
		a, code := runCLI(t, bin, config, "help", command)
		b, code2 := runCLI(t, bin, config, command, "--help")
		if code != 0 || code2 != 0 || a != b || !strings.Contains(a, "usage: rainier "+command) {
			t.Errorf("help %s mismatch: %d/%d\n%s\n%s", command, code, code2, a, b)
		}
	}
	for _, args := range [][]string{{"env", "create", "--help"}, {"agent", "login", "--help"}, {"secret", "set", "--help"}, {"context", "use", "--help"}} {
		if out, code := runCLI(t, bin, config, args...); code != 0 || !strings.Contains(out, "usage:") {
			t.Errorf("%v: %d %s", args, code, out)
		}
	}
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

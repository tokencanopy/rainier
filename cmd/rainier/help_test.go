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
	if code != 0 || !strings.HasPrefix(out, "rainier dev") {
		t.Fatalf("source version: %d %s", code, out)
	}
}

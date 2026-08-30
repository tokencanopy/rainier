// cmd/sessiond/gitprompt_test.go
package main

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/creack/pty"
)

// This file proves ONE property, against a real git on a real terminal: a git
// operation inside a session never blocks waiting for a human.
//
// It is here because the property cannot be established by reading the code.
// The boot chain runs on the session's PTY (internal/session/proc.go →
// pty.StartWithSize, which sets Setsid/Setctty), and git's behaviour when a
// credential helper declines is to fall through to its own prompt — which
// finds that terminal and blocks. What the clone stage then reports is "clone
// timed out after 600s", ten minutes later, with the named action the vault
// sent ("run: rainier login --refresh github") scrolled out of the tail; the
// agent's own `git push`, which nothing bounds at all, simply hangs forever.
//
// The three subtests below are the refusal path (git must die at once), the
// control that shows which variable is doing the work, and the revocation path
// (git's own words must still be the ones authRejected matches, so the vault
// still flips).
//
// Everything is local: a 401-always HTTP server on 127.0.0.1 stands in for
// GitHub, and the credential helpers are three-line shell scripts. No network,
// no token, no GitHub account.

// gitPromptTimeout is how long a subtest waits for a git that is supposed to
// fail immediately. It is enormously more than git needs (the failures below
// take milliseconds) because the assertion is "it ended", not "it was fast" —
// a bound tight enough to be a stopwatch would be the first thing to give on a
// loaded machine.
const gitPromptTimeout = 30 * time.Second

// gitBlockedProbe is how long the control subtest lets a git that has no
// GIT_TERMINAL_PROMPT run before concluding it is blocked on the prompt. The
// assertion can only fail in the safe direction: it fails if git EXITED, which
// is exactly the outcome that would mean the variable is not what holds the
// property.
const gitBlockedProbe = 3 * time.Second

func TestGitNeverPromptsOnTheSessionTerminal(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("no git on PATH; skipping the terminal-prompt tests")
	}

	// A remote that answers every request with a Basic-auth challenge: the
	// shape of GitHub refusing a token, and the shape of GitHub asking for one
	// in the first place. Which of the two a git run below is exercising is
	// decided by the helper it is given, not by this server.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("WWW-Authenticate", `Basic realm="rainier-test"`)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	t.Cleanup(srv.Close)
	remote := srv.URL + "/acme/api.git"

	t.Run("a refused mint fails at once instead of prompting", func(t *testing.T) {
		// The vault said needs_refresh: the helper prints controld's sentence
		// on stderr, prints NOTHING on stdout, and exits 1 (helper.go).
		env := gitTestEnv(t, refusingHelper(t))
		out, exited, took := runGitOnPTY(t, env, gitPromptTimeout, "ls-remote", remote)
		if !exited {
			t.Fatalf("git was still running after %s — it is blocked on a terminal prompt.\n"+
				"This is the defect GIT_TERMINAL_PROMPT=0 exists to prevent: the clone stage\n"+
				"would burn its whole %s bound and report a timeout instead of the named action.\n"+
				"--- git output ---\n%s", gitPromptTimeout, cloneTimeoutPerRepo, out)
		}
		// The two sentences a user needs, in the order they need them: what to
		// run, and then why git stopped. Both are inside stageTailBytes of each
		// other, so both reach the session's error column.
		if !strings.Contains(out, "rainier login --refresh github") {
			t.Errorf("git output lost the named action:\n%s", out)
		}
		if !strings.Contains(out, "terminal prompts disabled") {
			t.Errorf("git output does not carry its own reason for stopping:\n%s", out)
		}
		t.Logf("git failed in %s", took.Round(time.Millisecond))
	})

	t.Run("without GIT_TERMINAL_PROMPT the same git blocks", func(t *testing.T) {
		// The control. It is what makes the subtest above a property of the
		// boot chain's environment rather than a property of this machine's
		// git: strip the one variable and the identical run hangs.
		env := gitTestEnv(t, refusingHelper(t))
		env = withoutVars(env, "GIT_TERMINAL_PROMPT")
		out, exited, took := runGitOnPTY(t, env, gitBlockedProbe, "ls-remote", remote)
		if exited {
			t.Skipf("git exited in %s without GIT_TERMINAL_PROMPT (output %q); "+
				"this git does not prompt on this terminal, so the control proves nothing here",
				took.Round(time.Millisecond), out)
		}
		if !strings.Contains(out, "Username for") {
			t.Errorf("git is stuck on something other than the username prompt:\n%s", out)
		}
	})

	t.Run("a revoked token still reads as an auth failure", func(t *testing.T) {
		// The other half of the same environment, and the one the review's F1
		// note is careful about: with a credential in hand git needs no prompt
		// at all, so GIT_TERMINAL_PROMPT changes nothing here — the operation
		// fails with GitHub's own refusal, and that text is what makes the
		// clone stage emit credential_rejected and flip the vault.
		env := gitTestEnv(t, mintingHelper(t))
		out, exited, took := runGitOnPTY(t, env, gitPromptTimeout, "ls-remote", remote)
		if !exited {
			t.Fatalf("git was still running after %s with a credential in hand:\n%s", gitPromptTimeout, out)
		}
		if !authRejected(out) {
			t.Fatalf("git's own words for a rejected credential are not ones authRejected matches,\n"+
				"so the vault would never flip and `rainier creds` would read valid forever:\n%s", out)
		}
		t.Logf("git failed in %s", took.Round(time.Millisecond))
	})
}

// gitTestEnv builds the environment a session's git actually runs in: the
// variables prepareBoot exports, over a gitconfig naming the test's own helper
// in place of sessiond's.
//
// The variable LIST is production's, taken from prepareBoot rather than
// retyped, which is the whole point — a variable dropped there fails these
// tests. Only the gitconfig's helper line is the test's, because the real one
// names a path to sessiond that does not exist on this machine.
func gitTestEnv(t *testing.T, helper string) []string {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, ".rainier")
	_, vars, err := prepareBoot(dir, root, bootEnv{ReposB64: "W10="}) // "[]" — a git session with nothing to clone
	if err != nil {
		t.Fatal(err)
	}
	cfg := filepath.Join(dir, gitConfigName)
	// The same shape sessiond writes: the empty helper first, which is git's
	// spelling of "forget every helper configured before this file".
	if err := os.WriteFile(cfg, []byte("[credential]\n\thelper =\n\thelper = "+helper+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// A closed environment rather than os.Environ(): a developer with
	// GIT_ASKPASS or a credential helper of their own in it would otherwise
	// change what these tests measure. GIT_CONFIG_NOSYSTEM keeps /etc/gitconfig
	// out for the same reason.
	env := []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + root,
		"GIT_CONFIG_NOSYSTEM=1",
	}
	for _, v := range vars {
		env = append(env, v.Name+"="+v.Value)
	}
	return env
}

// withoutVars drops named variables from an environment, so a subtest can
// remove exactly one and see what it was holding up.
func withoutVars(env []string, names ...string) []string {
	out := make([]string, 0, len(env))
	for _, kv := range env {
		drop := false
		for _, n := range names {
			if strings.HasPrefix(kv, n+"=") {
				drop = true
			}
		}
		if !drop {
			out = append(out, kv)
		}
	}
	return out
}

// helperScript writes a credential helper on disk and returns the value to put
// in credential.helper — an absolute path, which git reads as a shell command
// and appends the operation to, exactly as it does for sessiond's own.
func helperScript(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "helper.sh")
	// `cat >/dev/null` drains git's request block: a helper that exits without
	// reading it hands git an EPIPE in place of a clean answer.
	if err := os.WriteFile(path, []byte("#!/bin/sh\ncat >/dev/null\n"+body), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

// refusingHelper is the vault saying no: controld's own sentence on stderr,
// nothing at all on stdout, exit 1 (cmd/sessiond/helper.go).
func refusingHelper(t *testing.T) string {
	t.Helper()
	return helperScript(t, "echo 'github credential needs refresh — run: rainier login --refresh github' >&2\nexit 1\n")
}

// mintingHelper is a successful mint: a credential the server will refuse.
// The value is a fixed synthetic string, never anything token-shaped.
func mintingHelper(t *testing.T) string {
	t.Helper()
	return helperScript(t, "printf 'username=x-access-token\\npassword=not-a-real-credential\\n'\nexit 0\n")
}

// runGitOnPTY runs one git command on a real pseudoterminal — Setsid and
// Setctty, the same shape internal/session gives the agent — and reports what
// it printed, whether it ended within wait, and how long it took.
//
// The PTY is not decoration. Git's fallback prompt reads /dev/tty, so a git
// with its stdin on a pipe fails immediately whatever the environment says,
// and a test run that way would pass with the fix reverted.
func runGitOnPTY(t *testing.T, env []string, wait time.Duration, args ...string) (string, bool, time.Duration) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Env = env
	f, err := pty.Start(cmd)
	if err != nil {
		t.Fatalf("starting git on a pty: %v", err)
	}

	// The reader has to outlive the process: the pty master returns EIO on
	// Linux once the slave side closes, which is an ordinary end of output
	// here, not a failure.
	var mu sync.Mutex
	var buf bytes.Buffer
	var readDone sync.WaitGroup
	readDone.Add(1)
	go func() {
		defer readDone.Done()
		chunk := make([]byte, 4<<10)
		for {
			n, err := f.Read(chunk)
			if n > 0 {
				mu.Lock()
				buf.Write(chunk[:n])
				mu.Unlock()
			}
			if err != nil {
				return
			}
		}
	}()

	start := time.Now()
	waited := make(chan error, 1)
	go func() { waited <- cmd.Wait() }()

	var exited bool
	select {
	case err := <-waited:
		exited = true
		var ee *exec.ExitError
		if err != nil && !errors.As(err, &ee) {
			t.Fatalf("waiting for git: %v", err)
		}
	case <-time.After(wait):
		// Still running, which for these tests is an outcome rather than an
		// error. Kill it and read what it managed to print.
		_ = cmd.Process.Kill()
		<-waited
	}
	took := time.Since(start)

	f.Close()
	readDone.Wait()
	mu.Lock()
	out := buf.String()
	mu.Unlock()
	return out, exited, took
}

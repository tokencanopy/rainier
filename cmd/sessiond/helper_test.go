// cmd/sessiond/helper_test.go
package main

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// gitAsks composes the key=value block git writes to a credential helper's
// stdin: one pair per line, terminated by a blank line.
func gitAsks(pairs ...string) string {
	return strings.Join(pairs, "\n") + "\n\n"
}

// runHelper invokes the credential helper the way git does — argv[1] is the
// operation, the request arrives on stdin — and returns its exit code and both
// streams.
func runHelper(t *testing.T, sock string, timeout time.Duration, args []string, stdin string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errOut strings.Builder
	code = runCredentialHelper(args, sock, timeout, strings.NewReader(stdin), &out, &errOut)
	return code, out.String(), errOut.String()
}

// TestCredentialHelperMints pins the exact two lines git reads back. The
// username is the conventional token-auth one — GitHub accepts any non-empty
// username when the password is a token — and the token appears on stdout and
// nowhere else in the process.
func TestCredentialHelperMints(t *testing.T) {
	var gotMethod, gotPayload string
	sock := startSocket(t, time.Second, func(method string, payload json.RawMessage) (json.RawMessage, error) {
		gotMethod, gotPayload = method, string(payload)
		return json.RawMessage(`{"token":"ghs_secretvalue"}`), nil
	})

	code, stdout, stderr := runHelper(t, sock, 5*time.Second, []string{"get"},
		gitAsks("protocol=https", "host=github.com", "path=acme/api.git"))

	if code != 0 {
		t.Fatalf("exit = %d, want 0 (stderr %q)", code, stderr)
	}
	if stdout != "username=x-access-token\npassword=ghs_secretvalue\n" {
		t.Fatalf("stdout = %q, want the two credential lines exactly", stdout)
	}
	if stderr != "" {
		t.Errorf("stderr = %q, want silence on success — stderr is what git shows the user", stderr)
	}
	if gotMethod != "mint_git_credential" {
		t.Errorf("method = %q, want mint_git_credential", gotMethod)
	}
	if gotPayload != "" && gotPayload != "{}" {
		t.Errorf("payload = %q, want an empty request — the mint's subject is the session, not its arguments", gotPayload)
	}
}

// TestCredentialHelperAnswersOnlyGitHubGets: anything else exits 0 with no
// output, which is how a helper tells git "I have nothing" and lets the next
// helper (or the prompt) run. Nothing else may reach the socket — a mint is a
// real credential operation upstream.
func TestCredentialHelperAnswersOnlyGitHubGets(t *testing.T) {
	cases := []struct {
		name  string
		args  []string
		stdin string
	}{
		{"store", []string{"store"}, gitAsks("protocol=https", "host=github.com", "username=x", "password=y")},
		{"erase", []string{"erase"}, gitAsks("protocol=https", "host=github.com")},
		{"another host", []string{"get"}, gitAsks("protocol=https", "host=gitlab.com")},
		{"a lookalike host", []string{"get"}, gitAsks("protocol=https", "host=github.com.evil.test")},
		{"no host at all", []string{"get"}, gitAsks("protocol=https")},
		{"no operation", nil, gitAsks("protocol=https", "host=github.com")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sock := startSocket(t, time.Second, func(string, json.RawMessage) (json.RawMessage, error) {
				t.Error("the helper asked sessiond for a credential it had no business minting")
				return json.RawMessage(`{"token":"ghs_x"}`), nil
			})
			code, stdout, stderr := runHelper(t, sock, time.Second, c.args, c.stdin)
			if code != 0 {
				t.Errorf("exit = %d, want 0 (stderr %q)", code, stderr)
			}
			if stdout != "" {
				t.Errorf("stdout = %q, want nothing — git falls through on an empty answer", stdout)
			}
		})
	}
}

// TestCredentialHelperSurfacesARefusal: the upstream message is the named
// action the user has to run, and git prints a helper's stderr straight to the
// terminal. It must arrive verbatim, and nothing may go to stdout — a partial
// credential would make git retry with garbage instead of showing the reason.
func TestCredentialHelperSurfacesARefusal(t *testing.T) {
	const msg = "github credentials need a refresh: run `rainier login --refresh github`"
	sock := startSocket(t, time.Second, func(string, json.RawMessage) (json.RawMessage, error) {
		return nil, errors.New(msg)
	})

	code, stdout, stderr := runHelper(t, sock, 5*time.Second, []string{"get"}, gitAsks("host=github.com"))
	if code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if stdout != "" {
		t.Errorf("stdout = %q, want nothing on a refusal", stdout)
	}
	if stderr != msg+"\n" {
		t.Fatalf("stderr = %q, want the server's message verbatim (%q)", stderr, msg)
	}
}

// TestCredentialHelperReportsLocalFailures: when sessiond cannot be reached at
// all the helper still has to say something git can print, or the user sees a
// bare "could not read Username" and no reason.
func TestCredentialHelperReportsLocalFailures(t *testing.T) {
	t.Run("no socket", func(t *testing.T) {
		sock := filepath.Join(t.TempDir(), "absent.sock")
		code, stdout, stderr := runHelper(t, sock, time.Second, []string{"get"}, gitAsks("host=github.com"))
		if code != 1 || stdout != "" {
			t.Fatalf("exit = %d, stdout = %q; want 1 and no output", code, stdout)
		}
		if !strings.Contains(stderr, "sessiond") {
			t.Errorf("stderr = %q, want it to name what could not be reached", stderr)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		block := make(chan struct{})
		t.Cleanup(func() { close(block) })
		sock := startSocket(t, time.Minute, func(string, json.RawMessage) (json.RawMessage, error) {
			<-block
			return nil, nil
		})
		code, stdout, stderr := runHelper(t, sock, 80*time.Millisecond, []string{"get"}, gitAsks("host=github.com"))
		if code != 1 || stdout != "" {
			t.Fatalf("exit = %d, stdout = %q; want 1 and no output", code, stdout)
		}
		if stderr == "" {
			t.Error("a timed-out mint said nothing")
		}
	})

	t.Run("an answer with no token", func(t *testing.T) {
		sock := startSocket(t, time.Second, func(string, json.RawMessage) (json.RawMessage, error) {
			return json.RawMessage(`{}`), nil
		})
		code, stdout, stderr := runHelper(t, sock, time.Second, []string{"get"}, gitAsks("host=github.com"))
		if code != 1 || stdout != "" {
			t.Fatalf("exit = %d, stdout = %q; want 1 and no output", code, stdout)
		}
		if stderr == "" {
			t.Error("an empty mint said nothing")
		}
	})
}

// TestParseCredentialRequest covers git's stdin format: key=value lines, a
// blank line ending the block, and keys this helper does not care about
// (git sends wwwauth[] and more) passing through without upsetting it.
func TestParseCredentialRequest(t *testing.T) {
	in := "protocol=https\nhost=github.com\npath=acme/api.git\nwwwauth[]=Basic realm=\"GitHub\"\n\ntrailing garbage\n"
	got := parseCredentialRequest(strings.NewReader(in))
	if got["host"] != "github.com" || got["protocol"] != "https" {
		t.Fatalf("request = %v", got)
	}
	if got["wwwauth[]"] != `Basic realm="GitHub"` {
		t.Errorf("wwwauth[] = %q, want the value's own = signs kept", got["wwwauth[]"])
	}
	if _, ok := got["trailing garbage"]; ok {
		t.Error("parsing continued past the blank line that ends git's request")
	}
	// A request that just ends (no blank line) is still a request.
	if got := parseCredentialRequest(strings.NewReader("host=github.com\n")); got["host"] != "github.com" {
		t.Errorf("request = %v, want an EOF-terminated block to parse", got)
	}
}

// TestHelperCommandMatchesTheGitconfig: the gitconfig sessiond writes names
// the binary and subcommand git will run. If they ever disagree, every git
// operation in every sandbox prompts for a password instead.
func TestHelperCommandMatchesTheGitconfig(t *testing.T) {
	if !strings.Contains(gitConfig("a", "b"), credentialHelperCommand) {
		t.Fatalf("the gitconfig does not name %q", credentialHelperCommand)
	}
	if !strings.HasSuffix(credentialHelperCommand, " "+credentialHelperSubcommand) {
		t.Fatalf("credential helper command %q does not end in the subcommand %q main dispatches on",
			credentialHelperCommand, credentialHelperSubcommand)
	}
}

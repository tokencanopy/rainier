// internal/egress/gitclient_docker_test.go
package egress

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// This file proves ONE property, against real clients over a real socket: a
// session container's git can reach an allowlisted host through egressd.
//
// It is here because the property cannot be established from the status code
// alone. egressd answered an unidentified CONNECT with 403 for two plans, and
// every egress test passed the whole time, because every one of them used
// curl — which sends the proxy URL's userinfo preemptively and so never needs
// to be asked. git does not: it hands libcurl CURLAUTH_ANY, which has no
// scheme to pick until a 407 names one, so its first CONNECT is deliberately
// bare and a 403 to that is final. Plan 5 is the first time git goes through
// this proxy, and the first live rehearsal found it as
// "fatal: unable to access '...': CONNECT tunnel failed, response 403" — with
// the session id sitting unread in the proxy URL the whole time.
//
// So the test runs BOTH clients, side by side, exactly as the failure was
// reproduced by hand, and it runs them against BOTH answers: the real handler
// (which challenges) and a stub that replies 403 the way egressd used to. The
// stub half is the control — it is what makes "git works now" a statement
// about the fix rather than about this machine's git.
//
// Everything is local. The origin is a bare git repository this test creates,
// served over TLS from the host; no GitHub, no network, no token.

// dockerAvailable follows internal/driver's skip pattern: no docker CLI or no
// responding daemon is a skip, never a failure. CI without docker still runs
// the rest of the package.
func dockerAvailable(t *testing.T) string {
	t.Helper()
	bin, err := exec.LookPath("docker")
	if err != nil {
		// Docker Desktop does not put its CLI on a login shell's PATH.
		fallback := "/Applications/Docker.app/Contents/Resources/bin/docker"
		if _, statErr := os.Stat(fallback); statErr != nil {
			t.Skip("docker CLI not found; skipping the real-client egress test")
		}
		bin = fallback
	}
	if err := exec.Command(bin, "info").Run(); err != nil {
		t.Skip("docker daemon not responding; skipping")
	}
	// The session image is what carries the git and curl a real session runs.
	// Building it here would make a proxy test own a docker build; `make` and
	// scripts/fleet-up.sh already do that, so an absent image is a skip.
	if err := exec.Command(bin, "image", "inspect", sessionImage).Run(); err != nil {
		t.Skipf("%s is not built (run scripts/fleet-up.sh or `docker build -t %s .`); skipping",
			sessionImage, sessionImage)
	}
	return bin
}

// sessionImage is the image a real session boots, and therefore the git and
// curl this test must exercise rather than the host's.
const sessionImage = "rainier-session:latest"

// containerHostAlias is the name a container uses for the host that started
// it. --add-host=...:host-gateway makes it resolve on engines that don't
// provide it natively (Linux), and is accepted where they do (Docker
// Desktop). It is what the container's proxy URL names, and the only name the
// container itself has to resolve.
const containerHostAlias = "host.docker.internal"

// originHost is the hostname of the target the container asks for. It is
// resolved by the PROXY, not by the container — a CONNECT client only writes
// the name into the request line — so it has to name the origin from the
// host's side, and "localhost" does. It is also the allowlist entry, so the
// request under test goes through exactly the host-matching path a real
// `egress_allow: ["github.com"]` does.
const originHost = "localhost"

// TestRealClientsThroughTheProxy is the regression pin for the 407 challenge,
// driven by the clients that disagreed about it.
func TestRealClientsThroughTheProxy(t *testing.T) {
	docker := dockerAvailable(t)
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("no git on PATH to build the fixture repository; skipping")
	}

	originPort := serveFixtureRepo(t)
	repoURL := fmt.Sprintf("https://%s:%s/repo.git", originHost, originPort)

	t.Run("egressd challenges, so both clients get through", func(t *testing.T) {
		var audit bytes.Buffer
		p := New(&audit)
		p.SetAllow("sess-real", []string{originHost})
		proxyPort := serveOnAllInterfaces(t, p.Handler())

		out := runClientsInContainer(t, docker, repoURL, proxyPort)

		// curl first, because curl is the client that was ALWAYS fine: if
		// this half fails the fixture or the docker networking is broken and
		// the git half below would prove nothing either way.
		if !strings.Contains(out, "curl=200") {
			t.Fatalf("curl could not reach the allowlisted origin through the proxy:\n%s", out)
		}
		if !strings.Contains(out, "git=ok") {
			t.Fatalf("real git could not reach an ALLOWLISTED host through the proxy — this is the\n"+
				"production blocker: a session with a github connector cannot clone.\n%s", out)
		}
		if !strings.Contains(out, "refs/heads/main") {
			t.Fatalf("git exited 0 but listed no refs, so nothing actually came back through the tunnel:\n%s", out)
		}

		// The audit proves git took the path that was broken, rather than
		// having authenticated preemptively like curl: a "challenge" line can
		// only come from a CONNECT that carried no credentials at all.
		if !strings.Contains(audit.String(), `"decision":"challenge"`) {
			t.Errorf("no challenge in the audit log, so git never exercised the 407 path:\n%s", audit.String())
		}
		if !strings.Contains(audit.String(), `"session":"sess-real"`) ||
			!strings.Contains(audit.String(), `"decision":"allow"`) {
			t.Errorf("audit has no allow line naming the session:\n%s", audit.String())
		}
	})

	t.Run("a proxy that answers 403 instead is what broke git", func(t *testing.T) {
		// The control, and the reason this test can claim the fix is the fix.
		// It is egressd's OLD answer, nothing else changed: same image, same
		// clients, same allowlisted origin.
		legacy := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "egress denied", http.StatusForbidden)
		})
		proxyPort := serveOnAllInterfaces(t, legacy)

		out := runClientsInContainer(t, docker, repoURL, proxyPort)

		if !strings.Contains(out, "git=fail") {
			t.Fatalf("git succeeded against a proxy that only ever answers 403, so this control\n"+
				"proves nothing about the challenge:\n%s", out)
		}
		// The verbatim wording git produces, which is also the string
		// cmd/sessiond's authRejected must NOT read as GitHub rejecting a
		// credential (its own test carries this text as a case).
		if !strings.Contains(out, "CONNECT tunnel failed, response 403") {
			t.Errorf("git's proxy-refusal wording is not the one this repo keys on;\n"+
				"cmd/sessiond.authRejected's proxy exclusion may need updating for this git:\n%s", out)
		}
	})
}

// serveFixtureRepo builds a one-commit bare repository and serves it over TLS
// on every interface, so a container can fetch it from the host. It returns
// the port.
//
// Dumb HTTP is deliberate: a plain file server is enough for `git ls-remote`
// (git reads info/refs and falls back from the smart protocol on its own), and
// it keeps the origin a fixture rather than a second service to run. What is
// being tested is the tunnel, not git's transport.
func serveFixtureRepo(t *testing.T) string {
	t.Helper()
	work := t.TempDir()
	src := filepath.Join(work, "src")
	www := filepath.Join(work, "www")
	bare := filepath.Join(www, "repo.git")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(www, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "README"), []byte("rainier egress fixture\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// -c on every invocation: this must not read, or depend on, whatever
	// identity the machine running the test has configured.
	ident := []string{
		"-c", "user.name=rainier test",
		"-c", "user.email=test@example.invalid",
		"-c", "commit.gpgsign=false",
		"-c", "init.defaultBranch=main",
	}
	git := func(dir string, args ...string) {
		t.Helper()
		cmd := exec.Command("git", append(append([]string(nil), ident...), args...)...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, out)
		}
	}
	git(src, "init", "-q")
	git(src, "add", "README")
	git(src, "commit", "-q", "-m", "fixture")
	git(work, "clone", "--bare", "-q", src, bare)
	git(bare, "update-server-info")

	ln, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(http.FileServer(http.Dir(www)))
	srv.Listener.Close()
	srv.Listener = ln
	srv.StartTLS()
	t.Cleanup(srv.Close)
	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	return port
}

// serveOnAllInterfaces runs h on 0.0.0.0 — httptest binds loopback, which a
// container cannot reach — and returns the port.
func serveOnAllInterfaces(t *testing.T, h http.Handler) string {
	t.Helper()
	ln, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: h}
	go srv.Serve(ln)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	})
	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	return port
}

// runClientsInContainer runs curl and then git against repoURL from inside the
// session image, wired exactly the way internal/driver wires a real session:
// the proxy in http_proxy/https_proxy with the session id as URL userinfo, and
// nothing else to carry identity with.
//
// It returns the combined output rather than asserting on it, because both
// subtests want the same run and disagree about what it should say.
func runClientsInContainer(t *testing.T, docker, repoURL, proxyPort string) string {
	t.Helper()
	proxy := fmt.Sprintf("http://sess-real:@%s:%s", containerHostAlias, proxyPort)
	// GIT_SSL_NO_VERIFY / curl -k: the fixture's certificate is httptest's
	// self-signed one. TLS trust is not what this test is about, and the
	// bytes still cross a real TLS session inside the tunnel either way.
	script := `
curl -k -sS -o /dev/null -w 'curl=%{http_code}\n' "$REPO/info/refs" 2>&1 || echo "curl=error"
if git ls-remote "$REPO" 2>/tmp/git.err; then echo git=ok; else echo git=fail; fi
cat /tmp/git.err
`
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, docker, "run", "--rm",
		"--add-host", containerHostAlias+":host-gateway",
		"--entrypoint", "/bin/sh",
		"-e", "REPO="+repoURL,
		"-e", "http_proxy="+proxy,
		"-e", "https_proxy="+proxy,
		"-e", "HTTP_PROXY="+proxy,
		"-e", "HTTPS_PROXY="+proxy,
		"-e", "GIT_SSL_NO_VERIFY=1",
		"-e", "GIT_TERMINAL_PROMPT=0",
		sessionImage, "-c", script,
	)
	out, err := cmd.CombinedOutput()
	// A non-zero exit is expected in the 403 control (the script's last
	// command is `cat`, so it isn't, but git's failure is still news worth
	// keeping) — the assertions read the output, not the status.
	if ctx.Err() != nil {
		t.Fatalf("docker run timed out: %v\n%s", err, out)
	}
	t.Logf("container output:\n%s", out)
	return string(out)
}

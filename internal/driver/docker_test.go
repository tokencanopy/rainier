// internal/driver/docker_test.go
package driver

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"os/exec"
	"strings"
	"testing"
)

func dockerAvailable(t *testing.T) {
	t.Helper()
	if _, err := dockerPath(); err != nil {
		t.Skip("docker CLI not found; skipping docker driver test")
	}
	if err := exec.Command(mustDockerPath(t), "info").Run(); err != nil {
		t.Skip("docker daemon not responding; skipping")
	}
}

// TestIsNotFoundErr pins the classification Docker.Inspect relies on to
// distinguish a genuinely-gone container from any other dockerRun failure
// (daemon unreachable, timeout, permission — review-round-1 Finding 3).
// dockerRun wraps a failed command's stderr into the returned error's text
// (see dockerRun in docker_exec.go), so the classification is a substring
// check on that text, not a distinct error type or exit code — real
// `docker inspect` on a missing id emits "Error: No such object: <id>" on
// stderr, confirmed against a live docker daemon.
func TestIsNotFoundErr(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{
			"genuine not-found, real dockerRun wrap shape",
			errors.New(`docker inspect -f {{.State.Status}} deadbeef: exit status 1: Error: No such object: deadbeef`),
			true,
		},
		{
			"daemon unreachable — must NOT be classified as gone",
			errors.New(`docker inspect -f {{.State.Status}} deadbeef: exit status 1: Cannot connect to the Docker daemon at unix:///var/run/docker.sock. Is the docker daemon running?`),
			false,
		},
		{
			"context deadline exceeded — must NOT be classified as gone",
			errors.New(`docker inspect -f {{.State.Status}} deadbeef: signal: killed: `),
			false,
		},
		{"empty message", errors.New(""), false},
	}
	for _, c := range cases {
		if got := isNotFoundErr(c.err); got != c.want {
			t.Errorf("%s: isNotFoundErr(%v) = %v, want %v", c.name, c.err, got, c.want)
		}
	}
}

func TestDockerDriverContract(t *testing.T) {
	dockerAvailable(t)
	RunContract(t, func(t *testing.T) (Driver, func()) {
		d := NewDocker(DockerOpts{
			Image:      "alpine:3.20",
			Network:    "bridge", // plain bridge for the driver test; internal net is a fleet concern
			TotalSlots: 8,
			Label:      "rainier.test",
		})
		// The contract creates containers with Cmd empty; alpine's default cmd
		// exits immediately, so override to a sleeper for lifecycle assertions.
		d.defaultCmd = []string{"sleep", "3600"}
		return d, func() { d.destroyAllLabeled(context.Background()) }
	})
}

// TestDockerCreateInjectsProxyEnv asserts that Spec.ProxyURL, when set,
// injects both cases of HTTP_PROXY/HTTPS_PROXY/NO_PROXY — tools disagree on
// which they read (BusyBox wget and curl read lowercase, many Go/Node tools
// read uppercase), so both must be present rather than just one. The proxy
// URL itself carries the session id as URL userinfo
// (http://<session-id>:@host:port, Task 13 egress R4): curl-family tools
// send that automatically as `Proxy-Authorization: Basic
// base64(session-id:)` on every CONNECT, which is the only way a plain
// env-var proxy flow can carry identity for egressd's allowlist lookup at
// all (verified against real curl during the Task 13 spike). NO_PROXY
// carries no userinfo — it's a bare host list, not a proxy URL.
func TestDockerCreateInjectsProxyEnv(t *testing.T) {
	dockerAvailable(t)
	d := NewDocker(DockerOpts{
		Image:      "alpine:3.20",
		Network:    "bridge",
		TotalSlots: 8,
		Label:      "rainier.test",
	})
	d.defaultCmd = []string{"sleep", "3600"}
	defer d.destroyAllLabeled(context.Background())
	ctx := context.Background()

	h, err := d.Create(ctx, Spec{
		Name: "tproxy", SessionID: "sproxy", DialURL: "ws://x",
		ProxyURL: "http://proxy.internal:3128",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Destroy(ctx, h.ID)

	out, err := dockerRun(ctx, "inspect", "-f", "{{range .Config.Env}}{{println .}}{{end}}", h.ID)
	if err != nil {
		t.Fatal(err)
	}
	env := map[string]string{}
	for _, line := range strings.Split(out, "\n") {
		if k, v, ok := strings.Cut(line, "="); ok {
			env[k] = v
		}
	}
	const wantProxy = "http://sproxy:@proxy.internal:3128"
	// NO_PROXY carries the dial URL's host ("x", from DialURL above) on top
	// of the base list: sessiond's register dial must never be sent through
	// the proxy. See noProxyFor.
	const wantNoProxy = "localhost,127.0.0.1,host.docker.internal,x"
	want := map[string]string{
		"HTTP_PROXY":  wantProxy,
		"http_proxy":  wantProxy,
		"HTTPS_PROXY": wantProxy,
		"https_proxy": wantProxy,
		"NO_PROXY":    wantNoProxy,
		"no_proxy":    wantNoProxy,
	}
	for k, wantV := range want {
		if got := env[k]; got != wantV {
			t.Errorf("env %s = %q, want %q", k, got, wantV)
		}
	}
}

// TestWithSessionUserinfo unit-tests the pure URL-construction helper
// directly (no docker daemon needed), including the edge cases the
// docker-gated integration test above can't cheaply cover: no session id,
// and an unparseable base URL.
func TestWithSessionUserinfo(t *testing.T) {
	cases := []struct {
		name      string
		base      string
		sessionID string
		want      string
	}{
		{"normal case", "http://proxy.internal:3128", "sess-1", "http://sess-1:@proxy.internal:3128"},
		{"empty session id leaves the URL unchanged", "http://proxy.internal:3128", "", "http://proxy.internal:3128"},
		{
			// Control characters are invalid in a URL and make url.Parse fail
			// — falling back to the unmodified base (no userinfo, so
			// egressd's allowlist lookup would deny) is safer than a panic
			// or a mangled URL passed to `docker run -e`.
			"unparseable base URL falls back unchanged",
			"http://proxy.internal:3128/\x7f", "sess-1", "http://proxy.internal:3128/\x7f",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := withSessionUserinfo(c.base, c.sessionID); got != c.want {
				t.Errorf("withSessionUserinfo(%q, %q) = %q, want %q", c.base, c.sessionID, got, c.want)
			}
		})
	}
}

// TestNoProxyForExemptsTheRegisterListener is the regression test for the bug
// scripts/e2e-fleet.sh's first run surfaced: with HTTP_PROXY set and the
// runnerd listener missing from NO_PROXY, sessiond's ws:// register dial goes
// through egressd (Go proxies ws/wss from the same env vars as http/https),
// which answers 405 — the container comes up and is permanently mute. The
// dial URL's host:port has to be in NO_PROXY, and it cannot come from the
// literal "host.docker.internal" in the base list because fleet-up.sh
// resolves that name to an IP before handing it over as --dial-base.
func TestNoProxyForExemptsTheRegisterListener(t *testing.T) {
	cases := []struct {
		name    string
		dialURL string
		want    string
	}{
		{"ip dial base", "ws://192.168.5.2:8080/register", noProxyBase + ",192.168.5.2:8080"},
		{"bridge gateway", "ws://172.17.0.1:8080/register", noProxyBase + ",172.17.0.1:8080"},
		{"named host", "ws://runnerd:8080/register", noProxyBase + ",runnerd:8080"},
		{"no port exempts the whole host", "ws://runnerd/register", noProxyBase + ",runnerd"},
		{"already in the base list", "ws://host.docker.internal:8080/register", noProxyBase},
		{"no dial url", "", noProxyBase},
		{"unparseable", "ws://[::1", noProxyBase},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := noProxyFor(tc.dialURL); got != tc.want {
				t.Fatalf("noProxyFor(%q) = %q, want %q", tc.dialURL, got, tc.want)
			}
		})
	}
}

// TestNoProxyForIsHonoredByGoProxyResolution is the half of the regression
// that matters: not "the string looks right" but "Go's own proxy resolution
// bypasses the proxy for the register dial, and still proxies everything
// else on that same address". The second assertion is what pins the
// exemption to one port — a host-wide NO_PROXY entry passes the first check
// and fails this one.
//
// coder/websocket dials ws:// as an ordinary http:// request through
// http.DefaultTransport, so http scheme + ProxyFromEnvironment is exactly the
// code path sessiond takes. net/http caches its proxy config on first use per
// process (there is no exported reset), so this test makes both its calls
// against one env and skips if something already primed that cache — an
// honest "couldn't measure" rather than a false pass.
func TestNoProxyForIsHonoredByGoProxyResolution(t *testing.T) {
	const dialURL = "ws://172.17.0.1:8080/register"
	t.Setenv("HTTP_PROXY", "http://egressd.invalid:3128")
	t.Setenv("HTTPS_PROXY", "http://egressd.invalid:3128")
	t.Setenv("NO_PROXY", noProxyFor(dialURL))

	proxyFor := func(t *testing.T, raw string) *url.URL {
		t.Helper()
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatalf("parse %q: %v", raw, err)
		}
		p, err := http.ProxyFromEnvironment(&http.Request{URL: u})
		if err != nil {
			t.Fatalf("ProxyFromEnvironment(%q): %v", raw, err)
		}
		return p
	}

	// A URL nothing exempts must resolve to the proxy. If it doesn't, this
	// process cached a proxy config before this test set its env, and neither
	// assertion below would mean anything.
	if p := proxyFor(t, "http://not-exempt.invalid/"); p == nil {
		t.Skip("net/http cached a proxy config before this test set its env; nothing to measure here")
	}

	// The register listener: no proxy, or sessiond never gets home.
	if p := proxyFor(t, "http://172.17.0.1:8080/register"); p != nil {
		t.Errorf("the register dial resolves to proxy %s; it must bypass the proxy entirely", p)
	}
	// Any other port on the same address: still proxied. This is what keeps
	// the exemption one port wide instead of one host wide.
	if p := proxyFor(t, "http://172.17.0.1:9999/"); p == nil {
		t.Error("another port on the runnerd host bypasses the proxy; the NO_PROXY exemption is host-wide, not listener-wide")
	}
}

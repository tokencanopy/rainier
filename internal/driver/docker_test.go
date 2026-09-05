// internal/driver/docker_test.go
package driver

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"
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
// (http://<session-id>:rainier@host:port, Task 13 egress R4; the placeholder password is what lets Claude Code use the proxy at all): curl-family tools
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
		// Spec.Env rides the same `docker run -e` channel as the proxy vars,
		// so the same inspect proves both. Two keys, deliberately out of
		// sorted order in the literal, because the driver emits them sorted.
		Env: map[string]string{"FOO": "bar", "ALPHA": "1"},
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
	const wantProxy = "http://sproxy:rainier@proxy.internal:3128"
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
		"FOO":         "bar",
		"ALPHA":       "1",
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
		{"normal case", "http://proxy.internal:3128", "sess-1", "http://sess-1:rainier@proxy.internal:3128"},
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

// flagValues returns every value passed to flag in args, in order — e.g.
// flagValues(args, "-e") for the injected environment.
func flagValues(args []string, flag string) []string {
	var out []string
	for i := 0; i < len(args)-1; i++ {
		if args[i] == flag {
			out = append(out, args[i+1])
		}
	}
	return out
}

// hasFlag reports whether args carries `flag value` as an adjacent pair.
func hasFlag(args []string, flag, value string) bool {
	for i := 0; i < len(args)-1; i++ {
		if args[i] == flag && args[i+1] == value {
			return true
		}
	}
	return false
}

// TestDockerRunArgs pins the argv Create hands to `docker run`. It needs no
// daemon, which is the point: the ordering rules below (Spec.Env last, sorted)
// are invisible in a live container's `docker inspect` output — env vars come
// back as an unordered set — so the only place they can be asserted is the
// argv itself.
func TestDockerRunArgs(t *testing.T) {
	d := NewDocker(DockerOpts{Image: "img:default", Network: "rainier-int", TotalSlots: 8, Label: "rainier.session"})

	t.Run("workspace volume and workdir", func(t *testing.T) {
		args := d.runArgs(Spec{SessionID: "sess-1", DialURL: "ws://x"}, "img:1")
		if !hasFlag(args, "-v", "rainier-ws-sess-1:/workspace") {
			t.Errorf("missing the session workspace volume mount: %v", args)
		}
		if !hasFlag(args, "-w", "/workspace") {
			t.Errorf("missing -w /workspace: %v", args)
		}
		// The Plan 3 hardening flags are load-bearing and must survive this
		// change untouched: the workspace volume is the ONE writable path a
		// session gets, and it only means that while the rest stays read-only
		// and unprivileged.
		for _, f := range []string{"--read-only", "-d"} {
			if !slicesContains(args, f) {
				t.Errorf("hardening flag %s dropped: %v", f, args)
			}
		}
		for _, pair := range [][2]string{
			{"--user", "1000:1000"},
			{"--security-opt", "no-new-privileges"},
			{"--tmpfs", "/tmp"},
			{"--network", "rainier-int"},
			{"--label", "rainier.session=sess-1"},
		} {
			if !hasFlag(args, pair[0], pair[1]) {
				t.Errorf("hardening flag %s %s dropped: %v", pair[0], pair[1], args)
			}
		}
		// The image, then the command, must stay last — `docker run` positional
		// order, not a style preference.
		if args[len(args)-1] != "img:1" {
			t.Errorf("image is not the last arg with an empty Cmd: %v", args)
		}
	})

	t.Run("a setup script drops read-only and nothing else", func(t *testing.T) {
		// The one conditional flag in runArgs, and the reason Plan 4's cache
		// can hold anything: an environment's build has to be able to install
		// into /usr/local and $HOME, which a read-only rootfs forbids and
		// `docker commit` is the only thing that can keep (it excludes
		// volumes, so /workspace is per-session by construction).
		args := d.runArgs(Spec{SessionID: "sess-3", DialURL: "ws://x", Setup: "apt-get install -y make"}, "img:1")
		if slicesContains(args, "--read-only") {
			t.Errorf("a create with a setup script kept --read-only, so its installs cannot be committed: %v", args)
		}
		// Every OTHER hardening flag is untouched. The trade is one flag on
		// one container per environment edit, not a relaxed session.
		for _, pair := range [][2]string{
			{"--user", "1000:1000"},
			{"--security-opt", "no-new-privileges"},
			{"--tmpfs", "/tmp"},
			{"--network", "rainier-int"},
			{"--label", "rainier.session=sess-3"},
			{"-v", "rainier-ws-sess-3:/workspace"},
			{"-w", "/workspace"},
		} {
			if !hasFlag(args, pair[0], pair[1]) {
				t.Errorf("a setup create lost %s %s: %v", pair[0], pair[1], args)
			}
		}
		// And the very next session — the cache-booted one, which carries no
		// script — is hardened again. This is the population that matters.
		cached := d.runArgs(Spec{SessionID: "sess-4", DialURL: "ws://x", Image: "rainier-env:e-abc"}, "rainier-env:e-abc")
		if !slicesContains(cached, "--read-only") {
			t.Errorf("a create with no setup script lost --read-only: %v", cached)
		}
	})

	t.Run("no session id means no volume and no workdir", func(t *testing.T) {
		// A volume literally named "rainier-ws-" would be SHARED by every
		// id-less session, and -w on a path no volume backs would leave the
		// container's workdir on the read-only rootfs. Neither is wanted:
		// with no session id there is nothing to key a workspace on.
		args := d.runArgs(Spec{DialURL: "ws://x"}, "img:1")
		for _, v := range flagValues(args, "-v") {
			t.Errorf("id-less spec still got a volume mount %q: %v", v, args)
		}
		for _, w := range flagValues(args, "-w") {
			t.Errorf("id-less spec still got a workdir %q: %v", w, args)
		}
	})

	t.Run("Spec.Env is sorted and comes after the proxy vars", func(t *testing.T) {
		args := d.runArgs(Spec{
			SessionID: "sess-2", DialURL: "ws://runnerd:8080/register",
			ProxyURL: "http://proxy.internal:3128",
			Env:      map[string]string{"ZED": "3", "ALPHA": "1", "MID": "2"},
		}, "img:1")
		proxyURL := "http://sess-2:rainier@proxy.internal:3128"
		noProxy := noProxyBase + ",runnerd:8080"
		want := []string{
			"RAINIER_DIAL=ws://runnerd:8080/register",
			"RAINIER_SESSION=sess-2",
			"HTTP_PROXY=" + proxyURL,
			"http_proxy=" + proxyURL,
			"HTTPS_PROXY=" + proxyURL,
			"https_proxy=" + proxyURL,
			"NO_PROXY=" + noProxy,
			"no_proxy=" + noProxy,
			// Sorted by key, and last.
			"ALPHA=1",
			"MID=2",
			"ZED=3",
		}
		if got := flagValues(args, "-e"); !reflect.DeepEqual(got, want) {
			t.Errorf("-e values =\n  %v\nwant\n  %v", got, want)
		}
	})

	t.Run("Spec.Env ordering is stable across runs", func(t *testing.T) {
		// Go randomizes map iteration, so an unsorted loop would emit a
		// different argv on most calls — untestable, and a moving target for
		// anyone diffing two containers' configs.
		spec := Spec{SessionID: "sess-3", Env: map[string]string{
			"A": "1", "B": "2", "C": "3", "D": "4", "E": "5", "F": "6", "G": "7", "H": "8",
		}}
		first := flagValues(d.runArgs(spec, "img:1"), "-e")
		for i := 0; i < 20; i++ {
			if got := flagValues(d.runArgs(spec, "img:1"), "-e"); !reflect.DeepEqual(got, first) {
				t.Fatalf("run %d emitted %v, first run emitted %v", i, got, first)
			}
		}
	})

	t.Run("Spec.Setup becomes RAINIER_SETUP_B64 and RAINIER_SETUP_TIMEOUT", func(t *testing.T) {
		// base64 because a setup script is arbitrary text — newlines above
		// all — and `docker run -e K=V` carries one line per var. The
		// container's sessiond decodes it back to /workspace/.rainier/setup.sh.
		const script = "#!/bin/sh\nset -e\napt-get install -y 'a b'\n"
		args := d.runArgs(Spec{SessionID: "sess-5", Setup: script, SetupTimeoutSec: 900}, "img:1")
		env := flagValues(args, "-e")
		wantB64 := "RAINIER_SETUP_B64=" + base64.StdEncoding.EncodeToString([]byte(script))
		if !slicesContains(env, wantB64) {
			t.Errorf("-e values %v missing %s", env, wantB64)
		}
		if !slicesContains(env, "RAINIER_SETUP_TIMEOUT=900") {
			t.Errorf("-e values %v missing RAINIER_SETUP_TIMEOUT=900", env)
		}
	})

	t.Run("no Spec.Setup means no setup vars", func(t *testing.T) {
		// A session whose environment was already snapshot-cached carries no
		// setup: the image IS the finished setup, and sessiond must run the
		// agent directly rather than wrap it.
		env := flagValues(d.runArgs(Spec{SessionID: "sess-6", SetupTimeoutSec: 900}, "img:1"), "-e")
		for _, e := range env {
			if strings.HasPrefix(e, "RAINIER_SETUP") {
				t.Errorf("setup var %q injected for a spec with no Setup: %v", e, env)
			}
		}
	})

	t.Run("Spec.Repos becomes RAINIER_REPOS_B64", func(t *testing.T) {
		// The repo list is structured data, so it rides as base64'd JSON for
		// the same reason the setup script does: `docker run -e K=V` carries
		// one line per var, and JSON with no encoding would put quoting and
		// (for a branch name with a newline in it) line breaks into an argv
		// sessiond has to parse back. One decode, one json.Unmarshal.
		repos := []RepoSpec{
			{Owner: "acme", Name: "app", BaseBranch: "main", SessionBranch: "rainier/work", Dir: "app"},
		}
		args := d.runArgs(Spec{SessionID: "sess-8", Repos: repos}, "img:1")
		blob, err := json.Marshal(repos)
		if err != nil {
			t.Fatal(err)
		}
		want := "RAINIER_REPOS_B64=" + base64.StdEncoding.EncodeToString(blob)
		if env := flagValues(args, "-e"); !slicesContains(env, want) {
			t.Errorf("-e values %v missing %s", env, want)
		}
	})

	t.Run("Spec.Init becomes RAINIER_INIT_B64 and RAINIER_INIT_TIMEOUT", func(t *testing.T) {
		// Same channel as the setup script, and a separate one from it: init
		// runs on EVERY boot, after the clones, including the cache-hit boots
		// that deliberately carry no setup at all.
		const script = "#!/bin/sh\nmake dev-server &\n"
		args := d.runArgs(Spec{SessionID: "sess-9", Init: script, InitTimeoutSec: 120}, "img:1")
		env := flagValues(args, "-e")
		wantB64 := "RAINIER_INIT_B64=" + base64.StdEncoding.EncodeToString([]byte(script))
		if !slicesContains(env, wantB64) {
			t.Errorf("-e values %v missing %s", env, wantB64)
		}
		if !slicesContains(env, "RAINIER_INIT_TIMEOUT=120") {
			t.Errorf("-e values %v missing RAINIER_INIT_TIMEOUT=120", env)
		}
		// And a cache-hit create — no setup, init still dispatched — carries
		// the init channel without the setup one.
		for _, e := range env {
			if strings.HasPrefix(e, "RAINIER_SETUP") {
				t.Errorf("setup var %q injected for a spec with only an init hook: %v", e, env)
			}
		}
	})

	t.Run("attribution rides as RAINIER_GIT_AUTHOR_NAME and _EMAIL", func(t *testing.T) {
		args := d.runArgs(Spec{SessionID: "sess-10",
			GitAuthorName: "alice", GitAuthorEmail: "42+alice@users.noreply.github.com"}, "img:1")
		env := flagValues(args, "-e")
		for _, want := range []string{
			"RAINIER_GIT_AUTHOR_NAME=alice",
			"RAINIER_GIT_AUTHOR_EMAIL=42+alice@users.noreply.github.com",
		} {
			if !slicesContains(env, want) {
				t.Errorf("-e values %v missing %s", env, want)
			}
		}
	})

	t.Run("no repos, init or attribution means none of those vars", func(t *testing.T) {
		// A scratch session clones nothing and commits as nobody in
		// particular; an orphan RAINIER_REPOS_B64 or a git identity with no
		// repository to use it would read like one was expected.
		env := flagValues(d.runArgs(Spec{SessionID: "sess-11", InitTimeoutSec: 120}, "img:1"), "-e")
		for _, e := range env {
			for _, prefix := range []string{"RAINIER_REPOS", "RAINIER_INIT", "RAINIER_GIT_AUTHOR"} {
				if strings.HasPrefix(e, prefix) {
					t.Errorf("%s var %q injected for a spec with none: %v", prefix, e, env)
				}
			}
		}
	})

	t.Run("Spec.Env still comes last, after the setup vars", func(t *testing.T) {
		args := d.runArgs(Spec{
			SessionID: "sess-7", Setup: "true", SetupTimeoutSec: 5,
			Repos: []RepoSpec{{Owner: "acme", Name: "app", BaseBranch: "main", SessionBranch: "rainier/w", Dir: "app"}},
			Init:  "make dev", InitTimeoutSec: 60,
			GitAuthorName: "alice", GitAuthorEmail: "42+alice@users.noreply.github.com",
			Env: map[string]string{"AAA": "1"},
		}, "img:1")
		env := flagValues(args, "-e")
		if len(env) == 0 || env[len(env)-1] != "AAA=1" {
			t.Errorf("-e values = %v, want Spec.Env last", env)
		}
	})

	t.Run("Spec.Env can override a proxy var", func(t *testing.T) {
		// `docker run` takes the LAST -e for a repeated key (verified against
		// docker 29), so putting Spec.Env after the proxy block is what makes
		// an explicit override possible at all. Nothing relies on this today;
		// the ordering is deliberate so that it stays available.
		args := d.runArgs(Spec{
			SessionID: "sess-4", ProxyURL: "http://proxy.internal:3128",
			Env: map[string]string{"HTTP_PROXY": "http://override:1"},
		}, "img:1")
		env := flagValues(args, "-e")
		last := map[string]string{}
		for _, e := range env {
			if k, v, ok := strings.Cut(e, "="); ok {
				last[k] = v
			}
		}
		if last["HTTP_PROXY"] != "http://override:1" {
			t.Errorf("HTTP_PROXY resolves to %q, want the Spec.Env override to win: %v", last["HTTP_PROXY"], env)
		}
	})
}

func slicesContains(args []string, want string) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
}

// TestDockerWorkspaceSurvivesColdPark is the file-level half of the contract
// suite's `workspace survives cold park`: that one asserts the driver surface
// (still running, still listed), this one asserts that what the session wrote
// to /workspace is still on disk after a cold park round trip.
//
// The container's Cmd APPENDS a line on every start rather than writing a
// fixed marker, because a marker's mere presence proves nothing here — the Cmd
// re-runs on `docker start`, so it would rewrite the file even onto a blank
// workspace. Two lines after resume can only come from a volume that carried
// the first line across the stop.
//
// `docker exec` here is the test harness peeking inside a container, exactly
// as in Plan 3's docker-gated tests — it is not something the runtime depends
// on.
func TestDockerWorkspaceSurvivesColdPark(t *testing.T) {
	dockerAvailable(t)
	d := NewDocker(DockerOpts{
		Image:      "alpine:3.20",
		Network:    "bridge",
		TotalSlots: 8,
		Label:      "rainier.test",
	})
	defer d.destroyAllLabeled(context.Background())
	ctx := context.Background()

	h, err := d.Create(ctx, Spec{
		Name: "tws", SessionID: "sws", DialURL: "ws://x",
		Cmd: []string{"sh", "-c", "echo tick >> /workspace/ticks; sleep 3600"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Destroy(ctx, h.ID)

	// The workspace is mounted where every later task expects it, and the
	// container's cwd is there too.
	wd, err := dockerRun(ctx, "inspect", "-f", "{{.Config.WorkingDir}}", h.ID)
	if err != nil {
		t.Fatal(err)
	}
	if wd != "/workspace" {
		t.Errorf("container workdir = %q, want /workspace", wd)
	}
	mounts, err := dockerRun(ctx, "inspect", "-f", `{{range .Mounts}}{{.Name}}:{{.Destination}}:{{.RW}} {{end}}`, h.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(mounts, "rainier-ws-sws:/workspace:true") {
		t.Errorf("mounts = %q, want the session volume rw at /workspace", mounts)
	}

	waitTicks := func(want int) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		var last string
		for time.Now().Before(deadline) {
			out, err := dockerRun(ctx, "exec", h.ID, "cat", "/workspace/ticks")
			if err == nil {
				last = out
				if n := len(strings.Split(strings.TrimSpace(out), "\n")); n == want {
					return
				}
			} else {
				last = err.Error()
			}
			time.Sleep(200 * time.Millisecond)
		}
		t.Fatalf("waiting for %d tick(s) in /workspace/ticks; last read: %q", want, last)
	}

	// One start, one tick — which already proves the unprivileged session user
	// can write to /workspace at all, under --read-only.
	waitTicks(1)

	if err := d.Suspend(ctx, h.ID, false); err != nil { // cold park: docker stop
		t.Fatal(err)
	}
	if err := d.Resume(ctx, h.ID); err != nil {
		t.Fatal(err)
	}
	// Two ticks: the second start appended to the file the first one wrote.
	waitTicks(2)
}

// TestDockerDestroyRemovesWorkspaceVolume: a session's volume is the session's,
// so it goes when the session does. Leaving it behind would grow the host's
// disk by one workspace per session ever created, with nothing left that names
// it — the container that did is gone.
func TestDockerDestroyRemovesWorkspaceVolume(t *testing.T) {
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

	// `docker volume ls --filter name=` matches on substring, so compare the
	// returned names exactly rather than trusting the filter to be anchored.
	volumeExists := func(name string) bool {
		t.Helper()
		out, err := dockerRun(ctx, "volume", "ls", "-q", "--filter", "name="+name)
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(out, "\n") {
			if strings.TrimSpace(line) == name {
				return true
			}
		}
		return false
	}

	h, err := d.Create(ctx, Spec{Name: "tvol", SessionID: "svol", DialURL: "ws://x"})
	if err != nil {
		t.Fatal(err)
	}
	if !volumeExists("rainier-ws-svol") {
		t.Fatal("Create did not leave a rainier-ws-svol volume behind")
	}

	if err := d.Destroy(ctx, h.ID); err != nil {
		t.Fatal(err)
	}
	if volumeExists("rainier-ws-svol") {
		t.Error("Destroy removed the container but left rainier-ws-svol behind")
	}

	// Destroying a container whose volume is already gone must not error —
	// Destroy is called on recovery and cleanup paths where the volume may
	// have been reaped independently.
	h2, err := d.Create(ctx, Spec{Name: "tvol2", SessionID: "svol2", DialURL: "ws://x"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dockerRun(ctx, "rm", "-f", h2.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := dockerRun(ctx, "volume", "rm", "-f", "rainier-ws-svol2"); err != nil {
		t.Fatal(err)
	}
	if err := d.Destroy(ctx, h2.ID); err != nil {
		t.Errorf("Destroy with an already-absent container and volume = %v, want nil", err)
	}
}

// TestDockerDestroyContainerKeepsWorkspaceVolume is the crash half of the
// split, against a real daemon: `docker rm -f` and nothing else, so the named
// volume is still listed afterwards and a later RemoveWorkspace is what
// finally takes it. The fake's version of this proves runnerd calls the right
// method; only this one proves the method does the right thing to a host.
func TestDockerDestroyContainerKeepsWorkspaceVolume(t *testing.T) {
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

	h, err := d.Create(ctx, Spec{Name: "tcrash", SessionID: "scrash", DialURL: "ws://x"})
	if err != nil {
		t.Fatal(err)
	}
	if !workspaceExists(t, d, "scrash") {
		t.Fatal("Create did not leave a rainier-ws-scrash volume behind")
	}

	if err := d.DestroyContainer(ctx, h.ID); err != nil {
		t.Fatal(err)
	}
	if g, _ := d.Inspect(ctx, h.ID); g.State != StateGone {
		t.Fatalf("state after DestroyContainer = %s, want gone", g.State)
	}
	if !workspaceExists(t, d, "scrash") {
		t.Fatal("DestroyContainer removed rainier-ws-scrash; a crash must keep the workspace")
	}

	// Only now, and only because something asked for it.
	if err := d.RemoveWorkspace(ctx, "scrash"); err != nil {
		t.Fatal(err)
	}
	if workspaceExists(t, d, "scrash") {
		t.Fatal("RemoveWorkspace left rainier-ws-scrash behind")
	}
	if err := d.RemoveWorkspace(ctx, "scrash"); err != nil {
		t.Errorf("RemoveWorkspace of an already-absent volume = %v, want nil", err)
	}
}

// TestDockerRemoveWorkspaceRefusesAnEmptySessionID: `rainier-ws-` with nothing
// after it is a perfectly valid docker volume name, and a driver that built
// it from an empty session id would `volume rm -f` whatever happened to be
// sitting under it. The id-less case is a no-op, not a prefix-only removal.
func TestDockerRemoveWorkspaceRefusesAnEmptySessionID(t *testing.T) {
	dockerAvailable(t)
	d := NewDocker(DockerOpts{Image: "alpine:3.20", Network: "bridge", TotalSlots: 8, Label: "rainier.test"})
	ctx := context.Background()

	if _, err := dockerRun(ctx, "volume", "create", workspaceVolumePrefix); err != nil {
		t.Fatal(err)
	}
	defer dockerRun(ctx, "volume", "rm", "-f", workspaceVolumePrefix)

	if err := d.RemoveWorkspace(ctx, ""); err != nil {
		t.Fatalf("RemoveWorkspace(\"\") = %v, want nil", err)
	}
	out, err := dockerRun(ctx, "volume", "ls", "-q", "--filter", "name="+workspaceVolumePrefix)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, line := range strings.Split(out, "\n") {
		if strings.TrimSpace(line) == workspaceVolumePrefix {
			found = true
		}
	}
	if !found {
		t.Fatalf("RemoveWorkspace(\"\") removed the bare %q volume", workspaceVolumePrefix)
	}
}

// TestWorkspaceInitArgs pins the one-shot container that prepares a fresh
// workspace volume. It runs as root inside a user-supplied image, which is the
// only place in the driver that happens — so every clamp on it is deliberate
// and none of them is safe to drop by accident.
func TestWorkspaceInitArgs(t *testing.T) {
	args := workspaceInitArgs("rainier-ws-sess-1", "img:1")

	for _, pair := range [][2]string{
		{"--network", "none"},                   // it has no business reaching anything
		{"--user", "0:0"},                       // chowning a volume takes root
		{"--security-opt", "no-new-privileges"}, //
		{"--cap-drop", "ALL"},                   // ...but root with no capabilities
		{"--cap-add", "CHOWN"},                  // ...except the single one it needs
		{"-v", "rainier-ws-sess-1:/workspace"},  // the volume it exists to prepare
		{"--entrypoint", "sh"},                  // the image's own entrypoint must not run
		{"-c", initWorkspaceScript},             //
	} {
		if !hasFlag(args, pair[0], pair[1]) {
			t.Errorf("workspace init lost %s %s: %v", pair[0], pair[1], args)
		}
	}
	for _, f := range []string{"--rm", "--read-only"} {
		if !slicesContains(args, f) {
			t.Errorf("workspace init lost %s: %v", f, args)
		}
	}
	// The image has to come before the -c script: `docker run [flags] IMAGE
	// [args]`, and `sh` reads its script from the args.
	img, script := -1, -1
	for i, a := range args {
		if a == "img:1" {
			img = i
		}
		if a == initWorkspaceScript {
			script = i
		}
	}
	if img < 0 || script < img {
		t.Errorf("image at %d, script at %d: the script must follow the image: %v", img, script, args)
	}
}

// TestDockerSnapshotToExplicitRefCreatesThatImage is the docker half of the
// contract's "snapshot honors an explicit ref" subtest: the contract can only
// assert the ref comes back verbatim, but what actually matters to Plan 4 is
// that `docker image inspect` finds a real image under the tag controld
// content-addressed. A driver that returned the ref while committing under a
// name of its own would pass the contract and still leave every later create
// pulling an image nobody tagged.
func TestDockerSnapshotToExplicitRefCreatesThatImage(t *testing.T) {
	dockerAvailable(t)
	d := NewDocker(DockerOpts{Image: "alpine:3.20", Network: "bridge", TotalSlots: 8, Label: "rainier.test"})
	d.defaultCmd = []string{"sleep", "3600"}
	defer d.destroyAllLabeled(context.Background())
	ctx := context.Background()

	h, err := d.Create(ctx, Spec{Name: "tsnapref", SessionID: "ssnapref", DialURL: "ws://x"})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Destroy(ctx, h.ID)

	const ref = "rainier-env:dockertest-abc123"
	defer dockerRun(ctx, "image", "rm", "-f", ref)
	snap, err := d.Snapshot(ctx, h.ID, ref, nil)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Ref != ref {
		t.Fatalf("snapshot ref = %q, want %q verbatim", snap.Ref, ref)
	}
	if _, err := dockerRun(ctx, "image", "inspect", "-f", "{{.Id}}", ref); err != nil {
		t.Fatalf("docker image inspect %s: %v (the commit did not land under the ref the driver reported)", ref, err)
	}
}

// TestDockerSetupCreateCommitsRootfsWrites is THE regression pin for Plan 4's
// first acceptance defect: an environment's setup script installed a toolchain
// and the cached image did not have it, because every session ran with a
// read-only rootfs and `docker commit` excludes the /workspace volume — so
// there was no path a build could write that the commit could keep.
//
// A create carrying Spec.Setup now drops that one flag, and this proves the
// consequence end to end at the driver's level: write outside the volume,
// commit, and the file is in the image a later create would boot.
//
// The marker write is exec'd as root deliberately. What that half tests is the
// DRIVER's property — the rootfs is writable and the commit captures it —
// which is separate from which uid the session image lets write where; that
// second half is the session image's own contract (see the Dockerfile's
// /opt/rainier-env prefix) and is covered end to end by scripts/e2e-fleet.sh
// against the real image. Mixing them would leave this test failing for a
// reason that has nothing to do with the flag it exists to pin.
//
// The uid-1000 probe below is the exception, and belongs here rather than in
// the rehearsal: it is the security property that makes the writable window
// acceptable at all, and it must hold for any image, not just ours.
func TestDockerSetupCreateCommitsRootfsWrites(t *testing.T) {
	dockerAvailable(t)
	d := NewDocker(DockerOpts{Image: "alpine:3.20", Network: "bridge", TotalSlots: 8, Label: "rainier.test"})
	d.defaultCmd = []string{"sleep", "3600"}
	defer d.destroyAllLabeled(context.Background())
	ctx := context.Background()

	const marker = "/opt/rainier-env/rainier-setup-marker"
	const ref = "rainier-env:dockertest-setupcache"
	defer dockerRun(ctx, "image", "rm", "-f", ref)

	h, err := d.Create(ctx, Spec{Name: "tsetup", SessionID: "ssetup", DialURL: "ws://x", Setup: "true"})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Destroy(ctx, h.ID)

	if _, err := dockerRun(ctx, "exec", "-u", "0:0", h.ID, "sh", "-c", "mkdir -p /opt/rainier-env && echo baked > "+marker); err != nil {
		t.Fatalf("a create with a setup script could not write %s: %v — the rootfs is still read-only, so nothing a setup installs can ever be cached", marker, err)
	}

	// The security boundary that makes the writable window acceptable:
	// dropping --read-only must not be mistaken for making root-owned paths
	// writable by uid 1000, the uid a setup script actually runs as.
	// /usr/local/bin is where sessiond — the session's PID 1 — lives, and a
	// setup script is untrusted in exactly the way design §10 means: an agent
	// that could rewrite PID 1 would have it baked into the cached image every
	// later session of that environment boots.
	//
	// This is the general half, true of any image whose /usr/local is
	// root-owned (alpine included). It deliberately does NOT pin that the
	// container runs as uid 1000 — `docker exec -u` overrides that either way,
	// and the runArgs subtest above is what pins the --user flag. The STOCK
	// session image's half — that our own Dockerfile hands the session user
	// /opt/rainier-env and NOT /usr/local — is asserted against the real image
	// by scripts/e2e-fleet.sh.
	if _, err := dockerRun(ctx, "exec", "-u", "1000:1000", h.ID, "sh", "-c", "touch /usr/local/bin/probe"); err == nil {
		t.Fatal("uid 1000 can write /usr/local/bin during a setup build; sessiond (PID 1) must stay out of reach even while the rootfs is writable")
	}
	if _, err := d.Snapshot(ctx, h.ID, ref, nil); err != nil {
		t.Fatal(err)
	}
	out, err := dockerRun(ctx, "run", "--rm", "--entrypoint", "sh", ref, "-c", "cat "+marker)
	if err != nil {
		t.Fatalf("the cached image has no %s: %v — the commit did not keep the setup's work", marker, err)
	}
	if strings.TrimSpace(out) != "baked" {
		t.Fatalf("cached %s = %q, want %q", marker, strings.TrimSpace(out), "baked")
	}

	// Negative control: a create with NO setup script is still hardened, and
	// the same write is refused. Without this the test above would pass just as
	// well against a driver that dropped --read-only for every session.
	cached, err := d.Create(ctx, Spec{Name: "tcached", SessionID: "scached", DialURL: "ws://x"})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Destroy(ctx, cached.ID)
	if _, err := dockerRun(ctx, "exec", "-u", "0:0", cached.ID, "sh", "-c", "mkdir -p /opt/rainier-env && echo nope > "+marker); err == nil {
		t.Fatalf("a create with no setup script accepted a write to %s; its rootfs must stay read-only", marker)
	}
}

// TestDockerSnapshotStripsSecretsAndSetupChannel is the regression pin for the
// other two acceptance defects, which share one cause: `docker commit`
// captures the container's whole config, environment block included.
//
// Left alone, that block put an environment's DECRYPTED secrets inside an
// image anyone with a docker socket could read, and carried RAINIER_SETUP_B64
// along so every session booted from the cache re-ran the setup script the
// cache exists to skip. Both are asserted here against the real committed
// config, because a fake cannot have one.
func TestDockerSnapshotStripsSecretsAndSetupChannel(t *testing.T) {
	dockerAvailable(t)
	d := NewDocker(DockerOpts{Image: "alpine:3.20", Network: "bridge", TotalSlots: 8, Label: "rainier.test"})
	d.defaultCmd = []string{"sleep", "3600"}
	defer d.destroyAllLabeled(context.Background())
	ctx := context.Background()

	const secretValue = "s3cr3t-must-not-survive"
	const ref = "rainier-env:dockertest-strip"
	defer dockerRun(ctx, "image", "rm", "-f", ref)

	h, err := d.Create(ctx, Spec{
		Name: "tstrip", SessionID: "sstrip", DialURL: "ws://runnerd:8080/register",
		ProxyURL: "http://egressd:3128",
		Setup:    "echo hi", SetupTimeoutSec: 900,
		// The Plan 5 channels belong in the same commit-time sweep. A repo
		// list and an init hook baked into a cached image make every later
		// session re-clone and re-init from the build's inputs, and the git
		// identity would attribute every one of their commits to whoever
		// happened to trigger the build.
		Repos: []RepoSpec{{Owner: "acme", Name: "app", BaseBranch: "main", SessionBranch: "rainier/w", Dir: "app"}},
		Init:  "echo init", InitTimeoutSec: 120,
		GitAuthorName: "buildbot", GitAuthorEmail: "42+buildbot@users.noreply.github.com",
		Env: map[string]string{"SECRET_A": secretValue},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Destroy(ctx, h.ID)

	// The container really does carry all of it — otherwise the assertions
	// below would pass against a create that never injected anything. The
	// proxy URLs matter as much as the secret: the driver embeds the session
	// id in them as userinfo, which is how egressd identifies the caller, so
	// they are per-session credential-shaped values with no business outliving
	// the build inside an image every later session boots.
	before, err := dockerRun(ctx, "inspect", "-f", "{{range .Config.Env}}{{println .}}{{end}}", h.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"SECRET_A=" + secretValue, "RAINIER_SETUP_B64=",
		"RAINIER_REPOS_B64=", "RAINIER_INIT_B64=", "RAINIER_INIT_TIMEOUT=120",
		"RAINIER_GIT_AUTHOR_NAME=buildbot", "RAINIER_GIT_AUTHOR_EMAIL=",
		"RAINIER_SESSION=sstrip", "RAINIER_DIAL=", "HTTP_PROXY=", "http_proxy=", "NO_PROXY=",
	} {
		if !strings.Contains(before, want) {
			t.Fatalf("the container's own env does not contain %q, so this test proves nothing:\n%s", want, before)
		}
	}
	if !strings.Contains(before, "sstrip:rainier@egressd") {
		t.Fatalf("the proxy URL carries no session-id userinfo, so stripping it would prove nothing:\n%s", before)
	}

	// The whole always-stripped set, spelled out here rather than imported
	// from runnerd: this is the DRIVER's contract, and a list that could only
	// ever agree with itself would prove nothing about the caller composing
	// it. runnerd's own test asserts it composes exactly this.
	strip := []string{
		"SECRET_A",
		"RAINIER_SETUP_B64", "RAINIER_SETUP_TIMEOUT",
		"RAINIER_REPOS_B64", "RAINIER_INIT_B64", "RAINIER_INIT_TIMEOUT",
		"RAINIER_GIT_AUTHOR_NAME", "RAINIER_GIT_AUTHOR_EMAIL",
		"RAINIER_DIAL", "RAINIER_SESSION",
		"HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy", "NO_PROXY", "no_proxy",
	}
	if _, err := d.Snapshot(ctx, h.ID, ref, strip); err != nil {
		t.Fatal(err)
	}
	after, err := dockerRun(ctx, "image", "inspect", "-f", "{{range .Config.Env}}{{println .}}{{end}}", ref)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(after, secretValue) {
		t.Fatalf("the committed image's config carries the secret's VALUE:\n%s", after)
	}
	if strings.Contains(after, "sstrip") {
		t.Fatalf("the committed image's config still names the build session (id or proxy userinfo):\n%s", after)
	}
	for _, line := range strings.Split(after, "\n") {
		for _, k := range strip {
			if v, ok := strings.CutPrefix(line, k+"="); ok && v != "" {
				t.Fatalf("stripped key %s carries %q in the committed image:\n%s", k, v, after)
			}
		}
	}

	// And a container booted from that image sees the setup channel as absent,
	// which is what sessiond's `RAINIER_SETUP_B64 != ""` gate reads — the
	// empty-equals-absent contract the strip relies on.
	out, err := dockerRun(ctx, "run", "--rm", "--entrypoint", "sh", ref, "-c", `[ -z "$RAINIER_SETUP_B64" ] && echo no-setup`)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(out) != "no-setup" {
		t.Fatalf("a container from the cached image still sees a setup script: %q", out)
	}
}

// TestDockerCommitArgsRejectsAMalformedStripKey: a key carrying '=' or
// whitespace would corrupt the `--change "ENV K="` it becomes, silently
// leaving a secret in the image. Aborting the snapshot is the safe direction —
// no cache just means the next session runs setup again. No daemon needed.
func TestDockerCommitArgsRejectsAMalformedStripKey(t *testing.T) {
	for _, bad := range []string{"", "K=V", "HAS SPACE"} {
		if _, err := commitArgs("cid", "ref", []string{"FINE", bad}, ""); err == nil {
			t.Errorf("commitArgs accepted strip key %q; want an error", bad)
		}
	}
	args, err := commitArgs("cid", "ref", []string{"A", "B"}, "")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"commit", "--change", "ENV A=", "--change", "ENV B=", "cid", "ref"}
	if !reflect.DeepEqual(args, want) {
		t.Errorf("commitArgs = %v, want %v", args, want)
	}
}

// TestDockerCommitArgsPinTheBaseImageCommand: `docker commit` records the
// container's own Cmd into the image, and the session that builds an
// environment's cache is not always a shell — a login session runs the
// agent's login command and exits. The base image's command, as docker
// renders it, is pinned back on the way in; an image with no command ("null")
// pins nothing.
func TestDockerCommitArgsPinTheBaseImageCommand(t *testing.T) {
	args, err := commitArgs("cid", "ref", []string{"A"}, `["--","bash","-i"]`)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"commit", "--change", `CMD ["--","bash","-i"]`, "--change", "ENV A=", "cid", "ref"}
	if !reflect.DeepEqual(args, want) {
		t.Errorf("commitArgs = %v, want %v", args, want)
	}
	for _, none := range []string{"", "null", " null\n"} {
		args, err := commitArgs("cid", "ref", nil, none)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(args, []string{"commit", "cid", "ref"}) {
			t.Errorf("commitArgs with base command %q = %v, want no CMD change", none, args)
		}
	}
}

// TestDockerPrepullPullsTheImage exercises the real `docker pull`. alpine:3.20
// is the same image the rest of this file's docker-gated tests already require
// present, so this adds no dependency the suite didn't already have.
func TestDockerPrepullPullsTheImage(t *testing.T) {
	dockerAvailable(t)
	d := NewDocker(DockerOpts{Image: "alpine:3.20", TotalSlots: 8, Label: "rainier.test"})
	ctx := context.Background()
	if err := d.Prepull(ctx, "alpine:3.20"); err != nil {
		t.Fatalf("Prepull(alpine:3.20) = %v", err)
	}
	if _, err := dockerRun(ctx, "image", "inspect", "-f", "{{.Id}}", "alpine:3.20"); err != nil {
		t.Fatalf("after Prepull, docker image inspect alpine:3.20: %v", err)
	}
}

// TestDockerPrepullRejectsEmptyRef: an empty ref is an upstream bug (a
// prepull command that carried no ref at all), and `docker pull ""` reports it
// as a CLI usage error that reads like a driver defect. Rejecting before the
// exec keeps the message legible — and needs no docker daemon, so it runs
// everywhere.
func TestDockerPrepullRejectsEmptyRef(t *testing.T) {
	d := NewDocker(DockerOpts{Image: "alpine:3.20", TotalSlots: 8, Label: "rainier.test"})
	if err := d.Prepull(context.Background(), ""); err == nil {
		t.Fatal("Prepull with an empty ref = nil, want an error")
	}
}

// TestCreateRejectsAnOversizedSetup: the setup script rides to the container
// as an environment variable, and an environment block is not an unbounded
// place to put a file — a runaway setup (a paste of a tarball, a generated
// script) would fail deep inside `docker run` with an errno, or worse, be
// truncated silently. Both drivers reject it up front, before anything with a
// side effect runs, so the message names the limit and the caller learns which
// of its inputs is wrong. Needs no daemon: the check precedes every exec.
func TestCreateRejectsAnOversizedSetup(t *testing.T) {
	spec := Spec{SessionID: "sess-big", Setup: strings.Repeat("x", MaxSetupBytes+1)}
	ctx := context.Background()

	for name, create := range map[string]func() error{
		"docker": func() error {
			_, err := NewDocker(DockerOpts{Image: "img", TotalSlots: 8, Label: "rainier.test"}).Create(ctx, spec)
			return err
		},
		"fake": func() error {
			_, err := NewFake(4).Create(ctx, spec)
			return err
		},
	} {
		err := create()
		if err == nil {
			t.Errorf("%s driver accepted a %d-byte setup script, want an error", name, len(spec.Setup))
			continue
		}
		if !strings.Contains(err.Error(), "524288") {
			t.Errorf("%s driver error %q does not name the %d-byte limit", name, err, MaxSetupBytes)
		}
	}

	// Exactly at the limit is fine — the fake proves the boundary without a
	// daemon (the docker driver's next step would be a real `docker run`).
	if _, err := NewFake(4).Create(ctx, Spec{SessionID: "sess-ok", Setup: strings.Repeat("x", MaxSetupBytes)}); err != nil {
		t.Errorf("a setup script exactly at the limit was rejected: %v", err)
	}
}

// ---------------------------------------------------------------------------
// the agent home mount
// ---------------------------------------------------------------------------

// The synthetic home a test create carries. The volume name is opaque on
// purpose — the control plane hands down a hash of (workspace, creator), and
// the driver treats it as a name to mount and never as something to parse —
// so these are exactly as meaningless as a real one.
const (
	testHomeVolume = "rainier-agents-0123456789abcdef"
	testHomePath   = "/rainier/agents"
)

// countPairs reports how many times args carries `flag value` adjacently.
// hasFlag answers "at all", which cannot catch a mount emitted twice — and a
// repeated -v is exactly the shape a careless append produces.
func countPairs(args []string, flag, value string) int {
	n := 0
	for i := 0; i < len(args)-1; i++ {
		if args[i] == flag && args[i+1] == value {
			n++
		}
	}
	return n
}

// indexOfPair returns the position of the first `flag value` pair, or -1.
func indexOfPair(args []string, flag, value string) int {
	for i := 0; i < len(args)-1; i++ {
		if args[i] == flag && args[i+1] == value {
			return i
		}
	}
	return -1
}

// TestRunArgsMountTheHome pins where the agent home lands in the `docker run`
// argv. Position is not cosmetic here: the mounts have to precede the network
// flag and the image the way every other -v does, and the home must never
// displace -w, which stays on the workspace — the home is where an agent keeps
// its configuration, not where the session works.
func TestRunArgsMountTheHome(t *testing.T) {
	d := NewDocker(DockerOpts{Image: "img:default", Network: "rainier-int", TotalSlots: 8, Label: "rainier.session"})
	home := &HomeMount{Volume: testHomeVolume, Path: testHomePath}

	t.Run("once, after the workspace and before the network", func(t *testing.T) {
		args := d.runArgs(Spec{SessionID: "sess_example", DialURL: "ws://x", Home: home}, "img:1")
		mount := testHomeVolume + ":" + testHomePath
		if n := countPairs(args, "-v", mount); n != 1 {
			t.Fatalf("-v %s appears %d times, want exactly 1: %v", mount, n, args)
		}
		ws := indexOfPair(args, "-v", "rainier-ws-sess_example:"+workspaceMount)
		at := indexOfPair(args, "-v", mount)
		net := indexOfPair(args, "--network", "rainier-int")
		if ws < 0 {
			t.Fatalf("the workspace mount is gone: %v", args)
		}
		if at < ws {
			t.Errorf("the home mount at %d precedes the workspace mount at %d: %v", at, ws, args)
		}
		if net < 0 || at > net {
			t.Errorf("the home mount at %d does not precede --network at %d: %v", at, net, args)
		}
		if !hasFlag(args, "-w", workspaceMount) {
			t.Errorf("the home mount displaced -w %s: %v", workspaceMount, args)
		}
		// Nothing about the hardening changes for a session with a home: it is
		// one more volume, not a relaxation.
		for _, fl := range []string{"--read-only", "-d"} {
			if !slicesContains(args, fl) {
				t.Errorf("a create with a home lost %s: %v", fl, args)
			}
		}
	})

	t.Run("no home, no mount", func(t *testing.T) {
		// The old-control-plane and no-creator case. Every existing golden in
		// TestDockerRunArgs is this case, so it has to be byte-identical to
		// what it was before the field existed.
		args := d.runArgs(Spec{SessionID: "sess_example", DialURL: "ws://x"}, "img:1")
		for _, a := range args {
			if strings.Contains(a, "rainier-agents-") || strings.Contains(a, testHomePath) {
				t.Fatalf("a spec with no home produced %q: %v", a, args)
			}
		}
	})

	t.Run("a half-specified home mounts nothing", func(t *testing.T) {
		// `docker run -v :/rainier/agents` is a daemon-side syntax error whose
		// message names nothing useful. runArgs stays total and emits no
		// mount; Create is where a malformed home becomes an error naming the
		// field (TestCreateRejectsAHalfSpecifiedHome).
		for _, h := range []*HomeMount{{Volume: testHomeVolume}, {Path: testHomePath}} {
			args := d.runArgs(Spec{SessionID: "sess_example", DialURL: "ws://x", Home: h}, "img:1")
			for i := 0; i < len(args)-1; i++ {
				if args[i] == "-v" && strings.HasPrefix(args[i+1], testHomeVolume+":") {
					t.Errorf("home %+v produced a mount: %v", h, args)
				}
				if args[i] == "-v" && strings.HasSuffix(args[i+1], ":"+testHomePath) {
					t.Errorf("home %+v produced a mount: %v", h, args)
				}
			}
		}
	})
}

// TestCreateRejectsAHalfSpecifiedHome: a HomeMount missing either half is a
// control-plane defect, and the cheapest place to say so is before the first
// side effect — the same reason an oversized setup script is rejected there.
func TestCreateRejectsAHalfSpecifiedHome(t *testing.T) {
	f := newFakeDocker()
	f.install(t)
	d := NewDocker(DockerOpts{Image: "img:default", TotalSlots: 8, Label: "rainier.test"})
	for _, h := range []*HomeMount{{Volume: testHomeVolume}, {Path: testHomePath}, {}} {
		if _, err := d.Create(context.Background(), Spec{SessionID: "sess_example", Home: h}); err == nil {
			t.Errorf("Create with home %+v = nil error, want a refusal", h)
		}
	}
	if len(f.calls) != 0 {
		t.Errorf("a refused create still talked to docker: %v", f.calls)
	}
}

// fakeDocker records every docker argv the driver builds and answers the few
// queries the create and destroy paths make. It exists because the volume
// bookkeeping this task adds — created once, initialized once, never removed
// by a teardown — is a statement about which commands run and which do NOT,
// and "did not run" is not observable from a live daemon's end state.
type fakeDocker struct {
	calls   [][]string
	volumes map[string]bool
	label   string // the session id `docker inspect` reports back
}

func newFakeDocker() *fakeDocker { return &fakeDocker{volumes: map[string]bool{}} }

func (f *fakeDocker) run(ctx context.Context, args ...string) (string, error) {
	f.calls = append(f.calls, append([]string(nil), args...))
	if len(args) == 0 {
		return "", nil
	}
	sub := ""
	if len(args) > 1 {
		sub = args[1]
	}
	last := args[len(args)-1]
	switch {
	case args[0] == "ps":
		return "", nil // nothing running: capacity is free
	case args[0] == "volume" && sub == "inspect":
		if f.volumes[last] {
			return last, nil
		}
		// The shape dockerRun wraps a failed command in — ensureVolume only
		// asks whether the error is nil, but a realistic one keeps the fake
		// honest if that ever changes.
		return "", errors.New("docker volume inspect " + last + ": exit status 1: Error response from daemon: No such object: " + last)
	case args[0] == "volume" && sub == "create":
		f.volumes[last] = true
		return last, nil
	case args[0] == "volume" && sub == "rm":
		delete(f.volumes, last)
		return "", nil
	case args[0] == "inspect":
		return f.label, nil
	case args[0] == "run" && slicesContains(args, "-d"):
		return "container_example", nil
	}
	return "", nil
}

// install swaps the package's docker exec for the duration of one test.
func (f *fakeDocker) install(t *testing.T) {
	t.Helper()
	prev := dockerRun
	dockerRun = f.run
	t.Cleanup(func() { dockerRun = prev })
}

// initJobs returns every recorded one-shot init container for volume at mount.
func (f *fakeDocker) initJobs(volume, mount string) [][]string {
	var out [][]string
	for _, c := range f.calls {
		if len(c) > 0 && c[0] == "run" && hasFlag(c, "-v", volume+":"+mount) && hasFlag(c, "--user", "0:0") {
			out = append(out, c)
		}
	}
	return out
}

// volumeCreates counts `docker volume create <name>` calls.
func (f *fakeDocker) volumeCreates(name string) int {
	n := 0
	for _, c := range f.calls {
		if len(c) == 3 && c[0] == "volume" && c[1] == "create" && c[2] == name {
			n++
		}
	}
	return n
}

// mentions reports whether any call in calls carries needle in any argument.
func mentions(calls [][]string, needle string) []string {
	for _, c := range calls {
		for _, a := range c {
			if strings.Contains(a, needle) {
				return c
			}
		}
	}
	return nil
}

// TestHomeVolumeIsPreparedOnceAndChowned: the home volume is created and
// chowned by the create that first needs it and by no create after, because
// the second one's volume already holds the first session's login — a
// recursive chown over it would be pointless at best, and the volume create
// would be a lie about what is there.
//
// The init job is the driver's one uid-0 window (see workspaceInitArgs), so
// every clamp on it is asserted here for the home exactly as
// TestWorkspaceInitArgs asserts it for the workspace.
func TestHomeVolumeIsPreparedOnceAndChowned(t *testing.T) {
	f := newFakeDocker()
	f.install(t)
	d := NewDocker(DockerOpts{Image: "img:default", TotalSlots: 8, Label: "rainier.test"})
	home := &HomeMount{Volume: testHomeVolume, Path: testHomePath}
	ctx := context.Background()

	if _, err := d.Create(ctx, Spec{SessionID: "sess_example", DialURL: "ws://x", Home: home}); err != nil {
		t.Fatalf("first create: %v", err)
	}
	if n := f.volumeCreates(testHomeVolume); n != 1 {
		t.Fatalf("`volume create %s` ran %d times on the first create, want 1: %v", testHomeVolume, n, f.calls)
	}
	jobs := f.initJobs(testHomeVolume, testHomePath)
	if len(jobs) != 1 {
		t.Fatalf("the home init job ran %d times on the first create, want 1: %v", len(jobs), f.calls)
	}
	job := jobs[0]
	for _, pair := range [][2]string{
		{"--network", "none"},                       // it has no business reaching anything
		{"--user", "0:0"},                           // chowning a volume takes root
		{"--security-opt", "no-new-privileges"},     //
		{"--cap-drop", "ALL"},                       // ...but root with no capabilities
		{"--cap-add", "CHOWN"},                      // ...except the single one it needs
		{"-v", testHomeVolume + ":" + testHomePath}, // the volume it exists to prepare
		{"--entrypoint", "sh"},                      // the image's own entrypoint must not run
		{"-c", initVolumeScript(testHomePath)},      //
	} {
		if !hasFlag(job, pair[0], pair[1]) {
			t.Errorf("the home init job lost %s %s: %v", pair[0], pair[1], job)
		}
	}
	if caps := flagValues(job, "--cap-add"); len(caps) != 1 || caps[0] != "CHOWN" {
		t.Errorf("the home init job adds capabilities %v, want CHOWN alone", caps)
	}
	for _, fl := range []string{"--rm", "--read-only"} {
		if !slicesContains(job, fl) {
			t.Errorf("the home init job lost %s: %v", fl, job)
		}
	}
	// The prepared volume is what the session actually mounts.
	ran := false
	for _, c := range f.calls {
		if len(c) > 0 && c[0] == "run" && slicesContains(c, "-d") && hasFlag(c, "-v", testHomeVolume+":"+testHomePath) {
			ran = true
		}
	}
	if !ran {
		t.Errorf("the session container did not mount the prepared home: %v", f.calls)
	}

	// A second session of the same creator in the same workspace: same home
	// volume, a workspace volume of its own.
	mark := len(f.calls)
	if _, err := d.Create(ctx, Spec{SessionID: "sess_example2", DialURL: "ws://x", Home: home}); err != nil {
		t.Fatalf("second create: %v", err)
	}
	if n := f.volumeCreates(testHomeVolume); n != 1 {
		t.Errorf("`volume create %s` ran %d times across two creates, want 1: %v", testHomeVolume, n, f.calls[mark:])
	}
	if jobs := f.initJobs(testHomeVolume, testHomePath); len(jobs) != 1 {
		t.Errorf("the home init job ran %d times across two creates, want 1: %v", len(jobs), f.calls[mark:])
	}
	// ...and "once" is per volume, not per driver: the second session's own
	// workspace is prepared exactly as the first one's was.
	if n := f.volumeCreates("rainier-ws-sess_example2"); n != 1 {
		t.Errorf("the second session's workspace volume was created %d times, want 1: %v", n, f.calls[mark:])
	}
	if jobs := f.initJobs("rainier-ws-sess_example2", workspaceMount); len(jobs) != 1 {
		t.Errorf("the second session's workspace init ran %d times, want 1: %v", len(jobs), f.calls[mark:])
	}
}

// TestDestroyLeavesTheHome: the home belongs to the (creator, workspace), not
// to the session mounted on it. A teardown that took it would log every other
// session of that person in that workspace out — including ones running right
// now on this runner — which is the opposite of what "log in once" promises.
func TestDestroyLeavesTheHome(t *testing.T) {
	f := newFakeDocker()
	f.install(t)
	f.label = "sess_example"
	d := NewDocker(DockerOpts{Image: "img:default", TotalSlots: 8, Label: "rainier.test"})
	ctx := context.Background()

	h, err := d.Create(ctx, Spec{SessionID: "sess_example", DialURL: "ws://x",
		Home: &HomeMount{Volume: testHomeVolume, Path: testHomePath}})
	if err != nil {
		t.Fatal(err)
	}

	mark := len(f.calls)
	if err := d.Destroy(ctx, h.ID); err != nil {
		t.Fatal(err)
	}
	teardown := f.calls[mark:]
	if c := mentions(teardown, testHomeVolume); c != nil {
		t.Errorf("Destroy named the home volume: %v", c)
	}
	if !f.volumes[testHomeVolume] {
		t.Error("Destroy removed the home volume")
	}
	// The session's own workspace still goes, which is the half Destroy is for.
	if mentions(teardown, "rainier-ws-sess_example") == nil {
		t.Errorf("Destroy did not remove the session's workspace volume: %v", teardown)
	}
	if f.volumes["rainier-ws-sess_example"] {
		t.Error("Destroy left the session's workspace volume behind")
	}

	// And the second act of an explicit teardown does not reach it either.
	mark = len(f.calls)
	if err := d.RemoveWorkspace(ctx, "sess_example"); err != nil {
		t.Fatal(err)
	}
	if c := mentions(f.calls[mark:], testHomeVolume); c != nil {
		t.Errorf("RemoveWorkspace named the home volume: %v", c)
	}
	mark = len(f.calls)
	if err := d.DestroyContainer(ctx, h.ID); err != nil {
		t.Fatal(err)
	}
	if c := mentions(f.calls[mark:], testHomeVolume); c != nil {
		t.Errorf("DestroyContainer named the home volume: %v", c)
	}
}

// TestSnapshotExcludesTheHome is the mechanical proof of the promise a
// credential set rests on: `docker commit` captures the container's writable
// layer and NOT its volumes, so nothing a person's agent wrote under the home
// can reach an environment's cached image — which is shared with every other
// member of the workspace and, for a registry-backed cache, published.
//
// It has to be docker-backed. The exclusion is the daemon's behavior, not this
// driver's: there is no argv that would show it, and a test that asserted one
// would pass while the real commit did whatever it liked.
func TestSnapshotExcludesTheHome(t *testing.T) {
	dockerAvailable(t)
	d := NewDocker(DockerOpts{Image: "alpine:3.20", Network: "bridge", TotalSlots: 8, Label: "rainier.test"})
	d.defaultCmd = []string{"sleep", "3600"}
	ctx := context.Background()
	// Registered BEFORE the container cleanup so it runs AFTER it (defers are
	// LIFO): docker refuses to remove a volume a container still references,
	// -f or not. The home outlives every session mounted on it — which is the
	// property under test — so this is the only thing that will ever remove
	// the one this test made.
	defer dockerRun(ctx, "volume", "rm", "-f", testHomeVolume)
	defer d.destroyAllLabeled(ctx)

	h, err := d.Create(ctx, Spec{Name: "thome", SessionID: "sess_example", DialURL: "ws://x",
		Home: &HomeMount{Volume: testHomeVolume, Path: testHomePath}})
	if err != nil {
		t.Fatal(err)
	}

	// As the session user, into the home — which proves the chown landed as
	// well: an unwritable home is the failure mode this whole init job exists
	// to prevent, and it would make the exclusion assertion below vacuous.
	probe := testHomePath + "/probe"
	if _, err := dockerRun(ctx, "exec", "-u", sessionUser, h.ID, "sh", "-c", "echo home_example > "+probe); err != nil {
		t.Fatalf("the session user cannot write to its own home: %v", err)
	}
	if out, err := dockerRun(ctx, "exec", h.ID, "cat", probe); err != nil || out != "home_example" {
		t.Fatalf("read back %q, %v; want home_example", out, err)
	}

	ref := "rainier-test-home-snap:1"
	defer dockerRun(ctx, "image", "rm", "-f", ref)
	if _, err := d.Snapshot(ctx, h.ID, ref, nil); err != nil {
		t.Fatal(err)
	}
	out, err := dockerRun(ctx, "run", "--rm", "--entrypoint", "sh", ref, "-c",
		"[ -e "+probe+" ] && echo present || echo absent")
	if err != nil {
		t.Fatal(err)
	}
	if out != "absent" {
		t.Fatalf("the committed image reports %q at %s: a credential set reached an environment image", out, probe)
	}
}

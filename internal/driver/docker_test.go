// internal/driver/docker_test.go
package driver

import (
	"context"
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
		proxyURL := "http://sess-2:@proxy.internal:3128"
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
	snap, err := d.Snapshot(ctx, h.ID, ref)
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

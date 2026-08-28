// internal/driver/docker_test.go
package driver

import (
	"context"
	"errors"
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
// read uppercase), so both must be present rather than just one.
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
	want := map[string]string{
		"HTTP_PROXY":  "http://proxy.internal:3128",
		"http_proxy":  "http://proxy.internal:3128",
		"HTTPS_PROXY": "http://proxy.internal:3128",
		"https_proxy": "http://proxy.internal:3128",
		"NO_PROXY":    "localhost,127.0.0.1,host.docker.internal",
		"no_proxy":    "localhost,127.0.0.1,host.docker.internal",
	}
	for k, wantV := range want {
		if got := env[k]; got != wantV {
			t.Errorf("env %s = %q, want %q", k, got, wantV)
		}
	}
}

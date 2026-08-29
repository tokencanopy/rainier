// internal/driver/docker.go
package driver

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
)

var errNotImpl = errors.New("not implemented yet")

type Docker struct {
	opts       DockerOpts
	defaultCmd []string
	snapSeq    atomic.Int64 // per-call uniqueness for Snapshot refs — see Snapshot
}

type DockerOpts struct {
	Image      string // default session image
	Network    string // internal docker network name (created by fleet compose)
	TotalSlots int    // capacity budget
	Label      string // docker label key marking rainier-managed containers
}

func NewDocker(opts DockerOpts) *Docker {
	if opts.Label == "" {
		opts.Label = "rainier.session"
	}
	if opts.TotalSlots == 0 {
		opts.TotalSlots = 16
	}
	return &Docker{opts: opts}
}

func (d *Docker) Create(ctx context.Context, spec Spec) (Handle, error) {
	used, total, err := d.Capacity(ctx)
	if err != nil {
		return Handle{}, err
	}
	if used >= total {
		return Handle{}, fmt.Errorf("no capacity: %d/%d", used, total)
	}

	// Per-session image selection is the whole point of Spec.Image: it wins
	// when set, and d.opts.Image (the fleet's configured default session
	// image) is only the fallback.
	image := spec.Image
	if image == "" {
		image = d.opts.Image
	}
	args := []string{"run", "-d",
		"--label", d.opts.Label + "=" + spec.SessionID,
		"--user", "1000:1000",
		"--security-opt", "no-new-privileges",
		"--read-only", "--tmpfs", "/tmp",
	}
	if d.opts.Network != "" {
		args = append(args, "--network", d.opts.Network)
	}
	if spec.DialURL != "" {
		args = append(args, "-e", "RAINIER_DIAL="+spec.DialURL)
	}
	if spec.SessionID != "" {
		args = append(args, "-e", "RAINIER_SESSION="+spec.SessionID)
	}
	if spec.ProxyURL != "" {
		// Embed the session id as the proxy URL's userinfo so egressd's
		// allowlist lookup has an identity to check at all (egress R4, Task
		// 13). A plain HTTP_PROXY/HTTPS_PROXY env var is the only channel a
		// non-agent-aware tool (curl, wget) reads, and it has no way to set
		// an arbitrary header from an env var — URL userinfo
		// (http://<session-id>:@host:port) is the one thing curl-family
		// tools do send automatically, as `Proxy-Authorization: Basic
		// base64(session-id:)` on every CONNECT. See
		// internal/egress.sessionFromProxyAuth, which decodes this form
		// (alongside a literal Bearer header, for any client that can set
		// one directly).
		proxyURL := withSessionUserinfo(spec.ProxyURL, spec.SessionID)
		// Inject both cases of each var: tools disagree on which they read —
		// BusyBox wget and curl read lowercase, many Go/Node tools read
		// uppercase — so set both rather than guess what's inside the image.
		args = append(args,
			"-e", "HTTP_PROXY="+proxyURL,
			"-e", "http_proxy="+proxyURL,
			"-e", "HTTPS_PROXY="+proxyURL,
			"-e", "https_proxy="+proxyURL,
			"-e", "NO_PROXY=localhost,127.0.0.1,host.docker.internal",
			"-e", "no_proxy=localhost,127.0.0.1,host.docker.internal",
		)
	}
	args = append(args, image)
	cmd := spec.Cmd
	if len(cmd) == 0 {
		cmd = d.defaultCmd
	}
	args = append(args, cmd...)

	id, err := dockerRun(ctx, args...)
	if err != nil {
		return Handle{}, err
	}
	return Handle{ID: id, State: StateRunning}, nil
}

// withSessionUserinfo embeds sessionID as base's URL userinfo
// (http://<session-id>:@host:port), so curl-family tools reading it from a
// plain HTTP_PROXY/HTTPS_PROXY env var send it automatically as HTTP Basic
// auth on the CONNECT request — the only channel such tools have for
// carrying identity at all. Falls back to base unchanged if sessionID is
// empty or base fails to parse, rather than panicking or emitting a mangled
// URL to `docker run -e`: an unmodified base URL with no userinfo still
// egresses through the proxy, it just won't carry an identity egressd's
// allowlist can match — a safe (default-deny) direction to fail in, not a
// silent bypass.
func withSessionUserinfo(base, sessionID string) string {
	if sessionID == "" {
		return base
	}
	u, err := url.Parse(base)
	if err != nil {
		return base
	}
	u.User = url.UserPassword(sessionID, "")
	return u.String()
}

func (d *Docker) Destroy(ctx context.Context, id string) error {
	_, err := dockerRun(ctx, "rm", "-f", id)
	return err
}

// isNotFoundErr reports whether a dockerRun error indicates the object
// genuinely does not exist — Docker's own "No such object" message — as
// opposed to some other, non-terminal failure (daemon unreachable, a
// context timeout, a permission error). dockerRun wraps the command's
// stderr into the returned error's text (see dockerRun in docker_exec.go),
// so this is a substring check on that text rather than a distinct error
// type or exit code. Review-round-1 Finding 3: Inspect used to treat EVERY
// dockerRun failure as "gone", so a transient docker hiccup at hub-death
// time (internal/runnerd's register()) read as a confirmed-gone container
// and destroyed one that was, in fact, still alive.
func isNotFoundErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "No such object")
}

func (d *Docker) Inspect(ctx context.Context, id string) (Handle, error) {
	out, err := dockerRun(ctx, "inspect", "-f", "{{.State.Status}}", id)
	if err != nil {
		if isNotFoundErr(err) {
			// `docker inspect` reports this when the container genuinely no
			// longer exists — a real "gone", not a transient failure.
			return Handle{ID: id, State: StateGone}, nil
		}
		// Any other failure is NOT proof the container is gone — propagate
		// it so callers (notably runnerd's hub-death path) don't mistake
		// "we couldn't get an answer" for "the container doesn't exist" and
		// destroy something that may still be running.
		return Handle{}, err
	}
	st := StateRunning
	switch out {
	case "running":
		st = StateRunning
	case "paused", "exited", "created":
		st = StateSuspended
	default:
		st = StateSuspended
	}
	return Handle{ID: id, State: st}, nil
}

// Capacity counts only slot-occupying containers — running or paused (warm
// suspend) — not cold-parked (stopped) ones. A stopped container still
// exists (its volume is kept for Resume) but isn't using a runtime slot, so
// counting it here would make cold sessions eat capacity they don't need.
// The two `--filter status=` flags OR together in `docker ps`, unlike most
// docker ps filters of the same key which AND across categories but OR
// within one — see https://docs.docker.com/reference/cli/docker/container/ls/#filter.
func (d *Docker) Capacity(ctx context.Context) (int, int, error) {
	out, err := dockerRun(ctx, "ps", "-q",
		"--filter", "label="+d.opts.Label,
		"--filter", "status=running",
		"--filter", "status=paused",
	)
	if err != nil {
		return 0, d.opts.TotalSlots, err
	}
	used := 0
	if strings.TrimSpace(out) != "" {
		used = len(strings.Split(strings.TrimSpace(out), "\n"))
	}
	return used, d.opts.TotalSlots, nil
}

// List returns every rainier-labeled container, in any state — unlike
// Capacity, which only counts slot-occupying ones. Used by runnerd.Recover
// to rebuild its registry after a restart.
func (d *Docker) List(ctx context.Context) ([]Listed, error) {
	format := `{{.ID}}` + "\t" + `{{.Label "` + d.opts.Label + `"}}` + "\t" + `{{.State}}`
	// --no-trunc: docker ps's default {{.ID}} is the 12-char short id, but
	// Create's Handle.ID is the full id `docker run` prints — without this,
	// every listed handle would mismatch the one Create/Inspect/Destroy use.
	out, err := dockerRun(ctx, "ps", "-a", "--no-trunc", "--filter", "label="+d.opts.Label, "--format", format)
	if err != nil {
		return nil, err
	}
	trimmed := strings.TrimSpace(out)
	if trimmed == "" {
		return nil, nil
	}
	lines := strings.Split(trimmed, "\n")
	listed := make([]Listed, 0, len(lines))
	for _, line := range lines {
		parts := strings.SplitN(line, "\t", 3)
		if len(parts) != 3 {
			continue
		}
		id, sessionID, state := parts[0], parts[1], parts[2]
		// Same mapping as Inspect: only "running" maps to StateRunning,
		// everything else (paused, exited, created, ...) is StateSuspended.
		st := StateRunning
		if state != "running" {
			st = StateSuspended
		}
		listed = append(listed, Listed{SessionID: sessionID, Handle: Handle{ID: id, State: st}})
	}
	return listed, nil
}

func (d *Docker) destroyAllLabeled(ctx context.Context) {
	out, err := dockerRun(ctx, "ps", "-aq", "--filter", "label="+d.opts.Label)
	if err != nil || strings.TrimSpace(out) == "" {
		return
	}
	for _, id := range strings.Split(strings.TrimSpace(out), "\n") {
		dockerRun(ctx, "rm", "-f", id)
	}
}

func (d *Docker) Suspend(ctx context.Context, id string, warm bool) error {
	if warm {
		_, err := dockerRun(ctx, "pause", id)
		return err
	}
	_, err := dockerRun(ctx, "stop", id)
	return err
}

func (d *Docker) Resume(ctx context.Context, id string) error {
	// Determine current status to pick unpause vs start.
	out, err := dockerRun(ctx, "inspect", "-f", "{{.State.Status}}", id)
	if err != nil {
		return err
	}
	switch out {
	case "paused":
		_, err = dockerRun(ctx, "unpause", id)
	case "exited", "created":
		_, err = dockerRun(ctx, "start", id)
	case "running":
		err = nil // already running
	default:
		_, err = dockerRun(ctx, "start", id)
	}
	return err
}

func (d *Docker) Snapshot(ctx context.Context, id string) (Snapshot, error) {
	if _, err := dockerRun(ctx, "inspect", "-f", "{{.Id}}", id); err != nil {
		return Snapshot{}, fmt.Errorf("snapshot: no such container %s: %w", id, err)
	}
	short := id
	if len(short) > 12 {
		short = short[:12]
	}
	// The suffix must make each call's ref unique, not just each container's:
	// snapshotting the same container twice used to reuse len(short) (always
	// 12 for a real container id) as the suffix, so both calls produced the
	// identical ref and the second `docker commit` silently overwrote the
	// first snapshot under the same tag. A per-call atomic counter fixes that
	// regardless of how many times Snapshot is called on the same handle.
	ref := "rainier-snap:" + short + "-" + strconv.FormatInt(d.snapSeq.Add(1), 10)
	if _, err := dockerRun(ctx, "commit", id, ref); err != nil {
		return Snapshot{}, err
	}
	return Snapshot{Ref: ref}, nil
}

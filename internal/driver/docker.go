// internal/driver/docker.go
package driver

import (
	"context"
	"errors"
	"fmt"
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

func (d *Docker) Destroy(ctx context.Context, id string) error {
	_, err := dockerRun(ctx, "rm", "-f", id)
	return err
}

func (d *Docker) Inspect(ctx context.Context, id string) (Handle, error) {
	out, err := dockerRun(ctx, "inspect", "-f", "{{.State.Status}}", id)
	if err != nil {
		// `docker inspect` errors when the container no longer exists.
		return Handle{ID: id, State: StateGone}, nil
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

func (d *Docker) Capacity(ctx context.Context) (int, int, error) {
	out, err := dockerRun(ctx, "ps", "-aq", "--filter", "label="+d.opts.Label)
	if err != nil {
		return 0, d.opts.TotalSlots, err
	}
	used := 0
	if strings.TrimSpace(out) != "" {
		used = len(strings.Split(strings.TrimSpace(out), "\n"))
	}
	return used, d.opts.TotalSlots, nil
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

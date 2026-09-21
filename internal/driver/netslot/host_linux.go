//go:build linux

// internal/driver/netslot/host_linux.go
//
// The production Host: `ip` from iproute2, one process per operation.
//
// Why not a netlink library. go.mod has no netlink dependency today
// (golang.org/x/sys is there, but it carries the syscall surface, not the
// route/link/namespace object model E2B gets from vishvananda/netlink), and
// adding one would be a new transitive tree in a repository whose only
// non-stdlib dependencies are the terminal stack, pgx and websockets. The
// operations here are a dozen calls made a handful of times per session
// lifetime, not a data path, so the cost of a fork is irrelevant and the
// benefit of a library that speaks netlink directly — no `ip` on the host, no
// output parsing, no namespace-switching on a locked OS thread — does not pay
// for the dependency yet. If it ever does, this file is the only thing that
// changes: Host is the seam, and the pool above it never learns which one it
// got.
package netslot

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// LinuxHost runs `ip` for every operation.
type LinuxHost struct {
	ipPath   string
	netnsDir string
}

var _ Host = (*LinuxHost)(nil)

// NewLinuxHost resolves iproute2, or fails.
//
// Fail closed, like every other host capability this driver needs: a runner
// that started without `ip` would accept sessions and give each of them a
// microVM with no network and no firewall, which is worse than not starting.
func NewLinuxHost(netnsDir string) (*LinuxHost, error) {
	p, err := exec.LookPath("ip")
	if err != nil {
		return nil, fmt.Errorf("netslot: iproute2 (`ip`) is not on PATH: every microVM session needs a network namespace, a veth pair and a TAP device: %w", err)
	}
	if netnsDir == "" {
		netnsDir = DefaultNetnsDir
	}
	return &LinuxHost{ipPath: p, netnsDir: netnsDir}, nil
}

// run execs one `ip` invocation, in netns when it is named.
func (h *LinuxHost) run(ctx context.Context, netns string, args ...string) error {
	full := args
	if netns != "" {
		full = append([]string{"-n", netns}, args...)
	}
	cmd := exec.CommandContext(ctx, h.ipPath, full...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("ip %s: %w: %s", strings.Join(full, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

// runTolerating execs one `ip` invocation and swallows the failure when the
// kernel's complaint is that the thing is already gone. Teardown is retried
// from several places and every one of them has to be able to finish.
func (h *LinuxHost) runTolerating(ctx context.Context, netns string, absent []string, args ...string) error {
	err := h.run(ctx, netns, args...)
	if err == nil {
		return nil
	}
	msg := strings.ToLower(err.Error())
	for _, needle := range absent {
		if strings.Contains(msg, needle) {
			return nil
		}
	}
	return err
}

func (h *LinuxHost) AddNetns(ctx context.Context, name string) error {
	return h.run(ctx, "", "netns", "add", name)
}

func (h *LinuxHost) DelNetns(ctx context.Context, name string) error {
	return h.runTolerating(ctx, "", []string{"no such file", "does not exist", "cannot remove namespace file"}, "netns", "delete", name)
}

func (h *LinuxHost) ListNetns(_ context.Context) ([]string, error) {
	// Read the directory rather than parse `ip netns list`: the listing's
	// format has changed across iproute2 versions (it grew a "(id: N)"
	// suffix), and the directory is what `ip` itself reads.
	entries, err := os.ReadDir(h.netnsDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// No namespace directory means no namespaces, which is the
			// honest answer on a host that has never run one.
			return nil, nil
		}
		return nil, fmt.Errorf("read %s: %w", h.netnsDir, err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		names = append(names, e.Name())
	}
	return names, nil
}

func (h *LinuxHost) AddVeth(ctx context.Context, hostName, peerName, peerNetns string) error {
	if err := h.run(ctx, "", "link", "add", hostName, "type", "veth", "peer", "name", peerName); err != nil {
		return err
	}
	if peerNetns == "" {
		return nil
	}
	if err := h.run(ctx, "", "link", "set", peerName, "netns", peerNetns); err != nil {
		// The pair exists and only one end is where it belongs. Remove it
		// here rather than leave a half-built link for the caller's teardown
		// to find under a name it may not recognise.
		_ = h.DelLink(ctx, "", hostName)
		return err
	}
	return nil
}

func (h *LinuxHost) DelLink(ctx context.Context, netns, name string) error {
	return h.runTolerating(ctx, netns, []string{"cannot find device", "no such device"}, "link", "delete", name)
}

func (h *LinuxHost) AddTap(ctx context.Context, netns, name, mac string) error {
	if err := h.run(ctx, netns, "tuntap", "add", "dev", name, "mode", "tap"); err != nil {
		return err
	}
	if mac == "" {
		return nil
	}
	if err := h.run(ctx, netns, "link", "set", "dev", name, "address", mac); err != nil {
		_ = h.DelLink(ctx, netns, name)
		return err
	}
	return nil
}

func (h *LinuxHost) AddAddr(ctx context.Context, netns, link, cidr string) error {
	return h.run(ctx, netns, "addr", "add", cidr, "dev", link)
}

func (h *LinuxHost) LinkUp(ctx context.Context, netns, link string) error {
	return h.run(ctx, netns, "link", "set", "dev", link, "up")
}

func (h *LinuxHost) AddRoute(ctx context.Context, netns, dst, via string) error {
	return h.run(ctx, netns, "route", "add", dst, "via", via)
}

func (h *LinuxHost) DelRoute(ctx context.Context, netns, dst, via string) error {
	return h.runTolerating(ctx, netns, []string{"no such process", "no such file", "cannot find device"}, "route", "delete", dst, "via", via)
}

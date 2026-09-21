// internal/driver/microvm_caps.go
//
// What a microVM runner needs from the host, named.
//
// ADR-0003 §4.5 says `runnerd` runs "as an unprivileged user with the minimum
// capabilities needed to create TAP devices and cgroups". The honest version
// of that sentence is longer, and until now it was nowhere: the driver
// checked for /dev/kvm and a Firecracker binary, and everything else — a
// network namespace, a veth pair, an nftables table, a chroot, a uid drop, a
// device node — failed at the first create, on a runner that had already
// registered with the control plane and accepted a placement.
//
// So the requirements are a list, the list is checked at startup, and a host
// that is missing one is told which one and what it is for. The check is
// deliberately NOT "are we root": root is one way to hold these capabilities
// and the worst one, and a runner that demanded euid 0 would make the
// unprivileged deployment ADR-0003 asks for impossible to express.
//
// Three of these are candidates to move behind a privileged helper later,
// and the seams are already where they would go:
//
//   - the network operations are netslot.Host, whose whole reason for being
//     an interface is that something else can implement it;
//   - the one filesystem operation that needs privilege is chownJailPath,
//     called from one place;
//   - the chroot, the cgroup, the device nodes and the uid drop are already
//     a helper — they are what `jailer` does, and runnerd performs none of
//     them itself.
//
// What runnerd cannot hand to a helper today is CAP_NET_ADMIN, because the
// slot operations are synchronous with a create, and CAP_SYS_ADMIN, because
// the jailer needs it in the process it is exec'd from.
package driver

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// hostCapability is one Linux capability this driver needs, and why.
type hostCapability struct {
	Name string
	Bit  uint
	Why  string
}

// microvmCapabilities is the whole list. The bit numbers are
// <linux/capability.h>'s.
var microvmCapabilities = []hostCapability{
	{
		Name: "CAP_CHOWN", Bit: 0,
		Why: "give each VM's jail directory, control socket and disk images to that VM's own uid (chownJailPath)",
	},
	{
		Name: "CAP_SETGID", Bit: 6,
		Why: "let the jailer drop the VMM to its per-VM gid",
	},
	{
		Name: "CAP_SETUID", Bit: 7,
		Why: "let the jailer drop the VMM to its per-VM uid",
	},
	{
		Name: "CAP_NET_ADMIN", Bit: 12,
		Why: "create each session's network namespace, veth pair and TAP device, and install its nftables ruleset (internal/driver/netslot)",
	},
	{
		Name: "CAP_SYS_CHROOT", Bit: 18,
		Why: "let the jailer chroot the VMM into its per-session directory",
	},
	{
		Name: "CAP_SYS_ADMIN", Bit: 21,
		Why: "enter a session's network namespace, and the mount work the jailer does inside the chroot",
	},
	{
		Name: "CAP_MKNOD", Bit: 27,
		Why: "let the jailer create /dev/kvm and /dev/net/tun inside each VM's chroot",
	},
}

// MicrovmHostRequirements is the human-readable list, for documentation and
// for the startup error. It is generated from the same list the check reads,
// so the two cannot drift.
func MicrovmHostRequirements() string {
	var b strings.Builder
	b.WriteString("the microvm driver needs, on a Linux host:\n")
	for _, c := range microvmCapabilities {
		fmt.Fprintf(&b, "  %-16s %s\n", c.Name, c.Why)
	}
	b.WriteString("  /dev/kvm         open for read and write (group `kvm` on most distributions)\n")
	b.WriteString("  cgroup v2        a writable parent cgroup for the jailer to create each VM's under\n")
	b.WriteString("\nNone of it requires running as root, and this runner never checks for euid 0. On a systemd unit:\n")
	b.WriteString("  AmbientCapabilities=" + capabilityNames() + "\n")
	b.WriteString("  CapabilityBoundingSet=" + capabilityNames() + "\n")
	b.WriteString("  SupplementaryGroups=kvm\n")
	b.WriteString("  Delegate=yes\n")
	return b.String()
}

func capabilityNames() string {
	names := make([]string, len(microvmCapabilities))
	for i, c := range microvmCapabilities {
		names[i] = c.Name
	}
	return strings.Join(names, " ")
}

// checkMicrovmPrivileges is the startup gate.
//
// It fails closed and names the FIRST thing that is missing rather than
// summarising: an operator fixing a unit file wants the next thing to add,
// and a list of seven is a list nobody reads to the end.
func checkMicrovmPrivileges(opts MicrovmOpts) error {
	effective, err := effectiveCapabilities()
	if err != nil {
		return fmt.Errorf("microvm: cannot read this process's capabilities, so this runner cannot tell whether it may build a session's network or jail: %w", err)
	}
	for _, c := range microvmCapabilities {
		if effective&(uint64(1)<<c.Bit) != 0 {
			continue
		}
		return fmt.Errorf("microvm: this runner does not hold %s, which it needs to %s.\n\n%s", c.Name, c.Why, MicrovmHostRequirements())
	}
	return checkCgroupParent(opts)
}

// checkCgroupParent makes sure the jailer will be able to create each VM's
// cgroup, and that metering will be able to read it (ADR-0003 §4.6).
//
// A cgroup v1 host, or a v2 host that has not delegated a subtree, is not a
// host this driver can meter — and metering integrity is one of the
// bakeoff's hard gates, so a runner that started anyway would be producing
// billing facts nobody can stand behind.
func checkCgroupParent(opts MicrovmOpts) error {
	root := opts.CgroupRoot
	if root == "" {
		root = defaultCgroupRoot
	}
	parent := opts.Jail.CgroupParent
	if parent == "" {
		parent = defaultJailCgroupParent
	}

	// cgroup v2 has a single unified hierarchy with this file at its root;
	// v1 does not have it at all.
	if _, err := os.Stat(filepath.Join(root, "cgroup.controllers")); err != nil {
		return fmt.Errorf("microvm: %s does not look like a cgroup v2 mount (no cgroup.controllers): the jailer is asked for cgroup v2, and ADR-0003 §4.6 meters each VM from cpu.stat and memory.current under it: %w", root, err)
	}

	dir := filepath.Join(root, parent)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("microvm: cannot create the parent cgroup %s, which the jailer puts each VM's cgroup under: %w.\n\n%s", dir, err, MicrovmHostRequirements())
	}
	// Creating a child is the operation that actually has to work, and a
	// writable-looking directory is not proof of it on a delegated subtree.
	probe := filepath.Join(dir, ".rainier-probe")
	if err := os.Mkdir(probe, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("microvm: cannot create a cgroup under %s: the jailer creates one per VM there: %w.\n\n%s", dir, err, MicrovmHostRequirements())
	}
	_ = os.Remove(probe)
	return nil
}

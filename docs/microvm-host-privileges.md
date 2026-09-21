# What a microVM runner needs from its host

`runnerd --driver=microvm` is not a root daemon, and ADR-0003 §4.5 is explicit
that it must not become one: it "runs as an unprivileged user with the minimum
capabilities needed". This is the list of what that means, what each item is
for, and what happens when one is missing.

The runner checks all of it at startup and refuses to run without it. It never
checks for `euid 0`. Running as root satisfies the check — root holds every
capability — but it is the worst way to satisfy it, because it also grants
everything not on this list.

## The list

| | What it is for |
|---|---|
| `CAP_CHOWN` | Give each VM's jail directory, control socket and disk images to that VM's own uid. |
| `CAP_SETGID` | Let the jailer drop the VMM to its per-VM gid. |
| `CAP_SETUID` | Let the jailer drop the VMM to its per-VM uid. |
| `CAP_NET_ADMIN` | Create each session's network namespace, veth pair and TAP device, and install its `nftables` ruleset. |
| `CAP_SYS_CHROOT` | Let the jailer chroot the VMM into its per-session directory. |
| `CAP_SYS_ADMIN` | Enter a session's network namespace, and the mount work the jailer does inside the chroot. |
| `CAP_MKNOD` | Let the jailer create `/dev/kvm` and `/dev/net/tun` inside each VM's chroot. |
| `/dev/kvm`, read and write | A microVM session is a hardware-isolated VM. There is no software fallback. Usually the `kvm` group. |
| A writable cgroup v2 parent | The jailer creates one cgroup per VM under it; ADR-0003 §4.6 meters each session from `cpu.stat` and `memory.current` there. |
| `ip` (iproute2) and `nft` (nftables) on `PATH` | The network slot and its firewall. |
| `firecracker` and `jailer` on `PATH` | There is no unjailed launch path. |
| `mkfs.ext4` on `PATH` | Session workspace and agent-home images are formatted before a guest is handed them. |

The runner's startup error names the first item it is missing and what that
item is for. `driver.MicrovmHostRequirements()` prints the whole list; it is
generated from the same table the check reads, so the two cannot drift.

## A systemd unit

```ini
[Service]
User=rainier
Group=rainier
SupplementaryGroups=kvm
AmbientCapabilities=CAP_CHOWN CAP_SETGID CAP_SETUID CAP_NET_ADMIN CAP_SYS_CHROOT CAP_SYS_ADMIN CAP_MKNOD
CapabilityBoundingSet=CAP_CHOWN CAP_SETGID CAP_SETUID CAP_NET_ADMIN CAP_SYS_CHROOT CAP_SYS_ADMIN CAP_MKNOD
Delegate=yes
ExecStart=/usr/local/bin/runnerd --driver=microvm ...
```

`Delegate=yes` is what gives the unit a cgroup subtree it may create children
in. Without it the jailer cannot make a cgroup for a VM and the runner refuses
to start.

## What runnerd does NOT do itself

Most of the privileged work is already behind a seam, and naming them is the
point of this section: these are what a future privileged helper would take
over, and nothing else would have to move.

- **The chroot, the cgroup, the device nodes and the uid drop** are `jailer`'s.
  `runnerd` never chroots, never `mknod`s and never changes its own uid; it
  hands the jailer a directory and a number. The jailer *is* the privileged
  helper for this half.
- **Every network operation** goes through `netslot.Host`
  (`internal/driver/netslot/host.go`). The Linux implementation shells out to
  `ip` and `nft`; a privileged helper implementing the same interface would
  remove `CAP_NET_ADMIN` from this list without the slot allocator changing.
- **The one privileged filesystem operation** is `chownJailPath`
  (`internal/driver/microvm_jailer.go`), called from one place, giving a jail's
  contents to the VM's uid. It is why `CAP_CHOWN` is here.

What cannot be moved today is `CAP_NET_ADMIN`, because slot setup is
synchronous with a create, and `CAP_SYS_ADMIN`, because the jailer needs it in
the process it is exec'd from.

## What only a real host can verify

Everything above is checked on a host that has it. On a machine without KVM,
without `CAP_NET_ADMIN` and without `nft` — a developer laptop, CI — the driver
is exercised through its fakes: `netslot.FakeHost` records the operations that
would have been performed, and the jail tests build the chroot layout with the
test process's own uid. None of that proves a namespace was created, a rule was
accepted by the kernel, a chroot held, or a cgroup accounted. Those are host
qualification (ADR-0003 Phase 1 and the bakeoff's isolation gates), not unit
tests.

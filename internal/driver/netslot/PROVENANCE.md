# Provenance: `internal/driver/netslot`

ADR-0003 §2.1 says that where E2B's Apache-2.0 runtime has already solved a
sub-problem below the driver seam — network slot allocation is named
explicitly — that code is copied into `internal/driver` **with attribution and
a provenance record**, and maintained as Rainier code from then on. This file
is that record.

## Upstream

| | |
|---|---|
| Project | [`e2b-dev/infra`](https://github.com/e2b-dev/infra) |
| License | Apache License 2.0 |
| Commit read | `926fbd6da6c8bf825d6e62674755ab482b1bba26` |
| Date read | 2026-09-21 |

Upstream paths consulted, all under `packages/orchestrator/pkg/sandbox/`:

- `network/slot.go` — the per-slot addressing scheme: one index, addresses
  derived from that index inside a configurable host-local CIDR, a fixed
  locally-administered MAC prefix, `ns-<idx>` namespace naming.
- `network/network.go` — the create/remove order for a slot's namespace, veth
  pair, TAP device, addresses and routes, and the rule that the namespace is
  deleted **last** so it stays a rediscovery anchor when host-side teardown
  fails.
- `network/storage_local.go` — the pool: a namespace this pool did not create
  blocks its index and is never deleted; a release whose teardown failed
  records the index as *leaked* rather than free; leaked indexes are handed
  out again only when no clean index is left, and the allocation that takes
  one finishes the teardown first.
- `network/reclaim.go` — startup reclaim: leftovers from a previous run are
  discovered by scanning the host's network-namespace directory, because the
  namespace entry is created first and removed last.
- `network/firewall.go` — the shape of the per-slot ruleset: a filter chain at
  prerouting matching on the slot's TAP interface, an established/related
  accept, an always-allow set, an always-deny set.

## Files in this package that are E2B-derived

| File | Derived from | Carries the Apache-2.0 header |
|---|---|---|
| `slot.go` | `network/slot.go` (addressing from one index) | yes |
| `pool.go` | `network/storage_local.go`, `network/reclaim.go`, and `network/network.go` for the create/remove operation order | yes |
| `firewall.go` | `network/firewall.go` (rule shape only) | yes |

`host.go`, `host_linux.go`, `host_other.go`, `fake.go`, `config.go` and the
tests are Rainier's own and carry no upstream header. So is the jailer launch
in `internal/driver/microvm_jailer.go`: E2B does not use Firecracker's jailer
at all — its orchestrator starts the VMM under `unshare` with `ip netns exec`
and a hand-built mount namespace (`fc/script_builder.go`, `fc/process.go` at
the same commit) — so there was nothing to copy, and that file is written
against Firecracker's own jailer documentation.

## What changed

- **Types.** Upstream's `Slot` carries E2B's `Config`, an `EgressProxy`, a
  DSCP marker, a hyperloop/portmapper/NFS redirect set and an
  `orchestrator.SandboxNetworkConfig`. None of that is adopted. Rainier's
  `Slot` is a value: index, names, addresses, MAC. It has no methods that
  touch the host.
- **The host seam.** Upstream calls `vishvananda/netlink`, `netns`,
  `coreos/go-iptables` and `google/nftables` directly, inside
  `runtime.LockOSThread` namespace switches. Rainier puts every host operation
  behind the `Host` interface in `host.go`, so the driver's tests run on a
  machine with no `CAP_NET_ADMIN` and a later privileged helper can own the
  implementation without the pool changing. The Linux implementation shells
  out to `ip`, `nft` and `sysctl` (see `host_linux.go`) rather than adding a
  netlink dependency to `go.mod`.
- **How a foreign index is discovered.** Retiring one is *upstream's*
  behaviour and not Rainier's invention: `StorageLocal` takes a snapshot of
  the namespace directory at construction (`getForeignNamespaces`), and
  `Acquire` skips every index in it for the life of the process — beside a
  per-scan `isNamespaceAvailable` check that blocks an index only while its
  namespace is there, and a `leakedNs` set for teardowns that failed. Rainier
  keeps all three states. What changed is where the foreign set comes from:
  there is no startup snapshot, because the namespace directory is not this
  pool's to interpret before `Reclaim` has run and the driver has re-adopted
  its own survivors. Rainier discovers the collision by trying to create the
  namespace, which means it has to tell "the name is taken" apart from "the
  create failed": a confirmed collision retires the index and the allocation
  moves to the next one, while any other failure marks it *leaked*, which is
  recoverable. A runner that loses `CAP_NET_ADMIN` for a minute must not come
  back with a permanently smaller host.
- **Reclaim is not bounded by the slot count.** Upstream's
  `SlotIndexFromNamespace` rejects an index above the configured size, so
  shrinking the pool orphans the namespaces above it. Rainier bounds what is
  handed *out*, not what is recognised as its own, so a resize is followed by
  a reclaim that actually cleans up.
- **Addressing.** Upstream gives each slot a /32 host address, a /31 veth
  block and a fixed `169.254.0.22/30` TAP address reused in every namespace.
  Rainier follows ADR-0003 §5.2 instead: one **/30 per slot** carved from a
  configurable host-local range for the guest link (gateway + guest), and a
  second /30 per slot from a configurable uplink range for the veth pair. No
  address is shared between slots, so a rendered firewall can name a
  neighbour.
- **MAC.** Upstream already uses two addresses for the two ends of the guest
  link — `tapMAC` `02:FC:00:00:00:05` for the guest NIC and `tapHostMAC`
  `02:FC:00:00:00:06` for the TAP device itself — but both are *constants*,
  the same on every sandbox, which is safe there because each TAP is alone in
  its namespace. Rainier keeps the two-address split and derives both from the
  slot index instead (`02:fc:00:<side>:hi:lo`), so a packet capture on the
  host names the slot it came from.
- **Firewall.** Upstream builds the ruleset programmatically with
  `google/nftables` and expression trees, with a default policy of *accept* and
  the TCP path handed to a userspace proxy by an iptables `REDIRECT`. Rainier
  renders a text ruleset from a template and applies it with `nft -f -`, so the
  exact bytes the kernel is asked for are reviewable and golden-testable, and
  the allow list is exactly one destination: the egress proxy
  `runnerd` was started with (ADR-0003 §4.3). The conntrack rule is
  `established` and not upstream's `established,related`: a RELATED match
  covers conntrack helper expectations, and a helper active in the namespace
  would let a guest talking to the allowed proxy conjure an accept for some
  other address ahead of the deny set.
- **Iptables, NAT, DSCP, the egress proxy hooks, the hyperloop/portmapper/NFS
  redirects, telemetry and tracing** are not copied. Rainier's guest reaches
  the network only through `egressd`, so the slot needs no in-namespace NAT
  fan-out.
- **Dropped upstream behaviour.** `Slot.ConfigureInternet`, `UpdateInternet`,
  `DenyEgress`, `ResetInternet` and the user allow/deny sets have no Rainier
  counterpart: an environment's egress policy is `egressd`'s allowlist, not a
  per-slot nftables set.

Nothing from E2B's orchestrator, API, configuration, storage or telemetry
packages is vendored.

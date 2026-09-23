#!/usr/bin/env bash
# scripts/microvm-kvm-test.sh — run the Firecracker-backed driver harness on a
# real KVM host, and leave behind evidence the Phase A runbook can cite.
#
# This is ADR-0003 §7 Phase 1 and §8 acceptance criterion 1, and it is the
# command the MicroVM Phase A feasibility runbook's experiment (a) has been
# waiting for. The harness itself is internal/driver/microvm_kvm_test.go; this
# script is the operator's end of it: check what this host is missing and say
# ALL of it, set the environment the harness reads, run it with a bounded
# timeout, and assemble one small JSON of timings and pinned artefact digests.
#
# It never escalates. Every capability the driver needs is checked and named,
# and a host that is short of one is told to re-run under `sudo -E` rather than
# being put there silently.
#
# Usage:
#
#   RAINIER_MICROVM_TEST_IMAGES=/mnt/mvm scripts/microvm-kvm-test.sh
#   make microvm-kvm-test                       # the same thing
#
# Inputs (all environment, none of them a flag, so the command in a runbook and
# the command in a shell history are the same shape):
#
#   RAINIER_MICROVM_TEST_IMAGES   required. The directory holding the guest
#                                 kernel and the base ext4 rootfs.
#   RAINIER_MICROVM_TEST_KERNEL   the kernel's name in there (default vmlinux).
#   RAINIER_MICROVM_TEST_ROOTFS   the rootfs's name (default rootfs.ext4).
#   RAINIER_MICROVM_TEST_STATE_DIR  where the per-run state directory is made.
#                                 Put it on a filesystem that reflinks (XFS) or
#                                 the harness logs that every create is a full
#                                 copy of the environment image.
#   RAINIER_MICROVM_TEST_PROXY    ip:port of the egress proxy, the one
#                                 host-side destination a slot's firewall
#                                 allows. Empty means no exception, which is a
#                                 legal runner and never a firewall left off.
#   RAINIER_MICROVM_KVM_OUT       where the evidence lands (default
#                                 ./microvm-kvm-evidence).
#   RAINIER_MICROVM_KVM_TIMEOUT   the go test timeout (default 30m).
#   GO                            the go binary (default `go`).
#
# The four places this script reads the machine itself are each overridable,
# and for one reason: so this script's own test can fabricate a host. Without
# them the test would only be able to assert the behaviour of whichever branch
# the machine running `go test` happens to take — a macOS developer's laptop
# has no /proc and skips the capability check entirely, a Linux CI runner has
# one and fails it — and the script would be tested on neither of the paths an
# operator actually runs. An operator sets none of these.
#
#   RAINIER_MICROVM_KVM_DEVICE       the KVM device node (default /dev/kvm).
#   RAINIER_MICROVM_KVM_PROC_STATUS  where this process's capability set is
#                                    read from (default /proc/self/status).
#   RAINIER_MICROVM_KVM_IP_FORWARD   where IPv4 forwarding is read from
#                                    (default /proc/sys/net/ipv4/ip_forward).
#   RAINIER_MICROVM_KVM_CGROUP_ROOT  the cgroup v2 mount point (default
#                                    /sys/fs/cgroup).
#
# Exit status: 0 the harness passed, 2 a precondition is missing and nothing
# ran, 3 the harness SKIPPED (which is not a pass and must never be recorded as
# one), 4 the harness ran but nothing in it did (also not a pass), 1 the
# harness failed.
#
# Nothing it writes carries a hostname, an address, a project id or any
# customer data: the evidence is durations, digests of artefacts the operator
# staged, and this machine's shape (Bakeoff §12).

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

images="${RAINIER_MICROVM_TEST_IMAGES:-}"
kernel_name="${RAINIER_MICROVM_TEST_KERNEL:-vmlinux}"
rootfs_name="${RAINIER_MICROVM_TEST_ROOTFS:-rootfs.ext4}"
kvm_device="${RAINIER_MICROVM_KVM_DEVICE:-/dev/kvm}"
proc_status="${RAINIER_MICROVM_KVM_PROC_STATUS:-/proc/self/status}"
ip_forward="${RAINIER_MICROVM_KVM_IP_FORWARD:-/proc/sys/net/ipv4/ip_forward}"
cgroup_root="${RAINIER_MICROVM_KVM_CGROUP_ROOT:-/sys/fs/cgroup}"
out_dir="${RAINIER_MICROVM_KVM_OUT:-$repo_root/microvm-kvm-evidence}"
test_timeout="${RAINIER_MICROVM_KVM_TIMEOUT:-30m}"
go_bin="${GO:-go}"

# The two tests the harness holds, named rather than matched by prefix, so the
# command in the runbook says which tests it is evidence for.
#
# Naming them is NOT what stops a drifted pattern being reported as a pass:
# `go test -run` matching nothing exits 0 and prints no SKIP, so a renamed test
# would leave this script with a silent, successful, empty run. What stops that
# is the evidence check further down, which will not write "pass" without
# something in the log or the timings file to say a test actually ran.
test_pattern='^(TestMicrovmBootSmokeOnKVM|TestMicrovmSatisfiesContractOnKVM)$'

# ---------------------------------------------------------------------------
# Preconditions — all of them, not the first
# ---------------------------------------------------------------------------

missing=()

note_missing() { missing+=("$1"); }

abs_or_under() {
  # Resolve an artefact name against the images directory unless it is already
  # absolute, matching the harness's own rule.
  case "$1" in
    /*) printf '%s' "$1" ;;
    *) printf '%s/%s' "$images" "$1" ;;
  esac
}

if [[ -z "$images" ]]; then
  note_missing "RAINIER_MICROVM_TEST_IMAGES is not set. It must name the directory holding the guest kernel and the base ext4 rootfs (Phase A runbook, Step 2)."
elif [[ ! -d "$images" ]]; then
  note_missing "RAINIER_MICROVM_TEST_IMAGES=$images is not a directory."
fi

kernel=""
rootfs=""
if [[ -d "${images:-/nonexistent}" ]]; then
  kernel="$(abs_or_under "$kernel_name")"
  rootfs="$(abs_or_under "$rootfs_name")"
  [[ -f "$kernel" && -r "$kernel" ]] ||
    note_missing "the guest kernel $kernel is not a readable file. Stage it, or name it with RAINIER_MICROVM_TEST_KERNEL."
  [[ -f "$rootfs" && -r "$rootfs" ]] ||
    note_missing "the base rootfs image $rootfs is not a readable file. Stage it, or name it with RAINIER_MICROVM_TEST_ROOTFS. Its /init must exec \`sessiond --transport=vsock\`."
fi

if [[ ! -r "$kvm_device" || ! -w "$kvm_device" ]]; then
  note_missing "$kvm_device is not readable and writable by this user. On GCE the instance must have been created with --enable-nested-virtualization, and this user must be in the \`kvm\` group."
fi

while read -r bin why; do
  command -v "$bin" >/dev/null 2>&1 || note_missing "\`$bin\` is not on PATH, and it is needed for: ${why}"
done <<'BINS'
firecracker the VMM every microVM session is
jailer the per-VM uid, cgroup, netns, chroot and seccomp envelope (ADR-0003 §4.5); there is no unjailed launch path
ip each session's network namespace, veth pair and TAP device
nft each session's per-slot firewall (ADR-0003 §4.3)
sysctl forwarding inside each slot's own network namespace
mkfs.ext4 formatting a session's workspace and agent-home disk images
BINS

command -v "$go_bin" >/dev/null 2>&1 ||
  note_missing "\`$go_bin\` is not on PATH. Set GO to the go binary, or install one."

# The capability set, from the same seven bits internal/driver checks. A host
# with no /proc is not a microVM host and will have failed above; the guard is
# here so this script's own test can run somewhere else.
if [[ -r "$proc_status" ]]; then
  capeff="$(awk '/^CapEff:/ {print $2}' "$proc_status")"
  if [[ -n "$capeff" ]]; then
    while read -r bit name why; do
      if (( ( 0x$capeff >> bit ) & 1 )); then continue; fi
      note_missing "this process does not hold $name, needed to ${why}. Re-run under \`sudo -E\`, or give runnerd's user the ambient set (see MicrovmHostRequirements)."
    done <<'CAPS'
0 CAP_CHOWN give each VM's jail, control socket and disk images to that VM's own uid
6 CAP_SETGID let the jailer drop the VMM to its per-VM gid
7 CAP_SETUID let the jailer drop the VMM to its per-VM uid
12 CAP_NET_ADMIN create each session's namespace, veth pair, TAP device and nftables ruleset
18 CAP_SYS_CHROOT let the jailer chroot the VMM into its per-session directory
21 CAP_SYS_ADMIN enter a session's network namespace, and the jailer's mount work
27 CAP_MKNOD let the jailer create /dev/kvm and /dev/net/tun inside each chroot
CAPS
  fi
fi

if [[ -r "$ip_forward" ]]; then
  if [[ "$(cat "$ip_forward")" == "0" ]]; then
    note_missing "net.ipv4.ip_forward is 0, so every packet a guest sends would be forwarded into its slot's veth and dropped by this host. \`sysctl -w net.ipv4.ip_forward=1\`."
  fi
fi

# Guarded on the mount point existing at all, like the two checks above: a
# machine with no /sys/fs/cgroup is not a microVM host and has already failed
# on /dev/kvm, and the harness checks this again by name either way.
if [[ -d "$cgroup_root" && ! -r "$cgroup_root/cgroup.controllers" ]]; then
  note_missing "$cgroup_root does not look like a cgroup v2 mount (no cgroup.controllers). The jailer is asked for cgroup v2 and ADR-0003 §4.6 meters each VM from cpu.stat and memory.current under it."
fi

if (( ${#missing[@]} > 0 )); then
  echo "microvm-kvm-test: this host cannot run the harness yet. ${#missing[@]} precondition(s) missing:" >&2
  for m in "${missing[@]}"; do
    echo "  - $m" >&2
  done
  exit 2
fi

# ---------------------------------------------------------------------------
# The run
# ---------------------------------------------------------------------------

mkdir -p "$out_dir"
timings="$out_dir/timings.json"
log="$out_dir/go-test.log"
# Removed rather than truncated, so a run that never reached the smoke cannot
# leave the previous run's numbers in the evidence.
rm -f "$timings"

export RAINIER_MICROVM_KVM_TEST=1
export RAINIER_MICROVM_TEST_IMAGES="$images"
export RAINIER_MICROVM_TEST_KERNEL="$kernel_name"
export RAINIER_MICROVM_TEST_ROOTFS="$rootfs_name"
export RAINIER_MICROVM_TEST_TIMINGS="$timings"

echo "microvm-kvm-test: images=$images kernel=$kernel_name rootfs=$rootfs_name timeout=$test_timeout"
echo "microvm-kvm-test: evidence -> $out_dir"

status=0
(
  cd "$repo_root"
  "$go_bin" test ./internal/driver -run "$test_pattern" -v -count=1 -timeout "$test_timeout"
) >"$log" 2>&1 || status=$?
# The log is the operator's, so it goes to the terminal too — after the run
# rather than through a pipe, because a pipe would hide go test's exit status
# behind tee's.
cat "$log"

result="pass"
if (( status != 0 )); then
  result="fail"
elif grep -q -- '--- SKIP' "$log"; then
  # A skip is not a pass and must never be recorded as one. The harness's own
  # skip message names the precondition; this script's checks above are a
  # superset in intent but not in fact — the harness knows things this script
  # does not, such as whether the kernel image is world-readable.
  result="skipped"
  status=3
elif ! { grep -q -- '--- PASS: TestMicrovmBootSmokeOnKVM' "$log" &&
         grep -q -- '--- PASS: TestMicrovmSatisfiesContractOnKVM' "$log"; } &&
     [[ ! -s "$timings" ]]; then
  # `go test -run` that matches no test exits 0 and prints no SKIP, so exit
  # status alone cannot tell "both tests passed" from "the -run pattern above
  # drifted and nothing ran at all". A pass is therefore written only against
  # positive evidence: the harness's own PASS lines, or the timings file it
  # writes while booting a guest. Neither means nothing ran, which is the one
  # outcome that must never be filed as ADR-0003 §8 item 1 evidenced.
  result="no-tests-ran"
  status=4
fi

# ---------------------------------------------------------------------------
# The evidence
# ---------------------------------------------------------------------------

json_string() {
  # A JSON string literal from arbitrary shell output: backslashes and quotes
  # escaped, control characters and newlines dropped. Written out rather than
  # shelled to jq, which a bare Debian feasibility host does not have.
  local s="${1:-}"
  s="${s//\\/\\\\}"
  s="${s//\"/\\\"}"
  s="$(printf '%s' "$s" | tr -d '\000-\037')"
  printf '"%s"' "$s"
}

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | cut -d' ' -f1
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | cut -d' ' -f1
  else
    printf 'unavailable'
  fi
}

size_of() {
  if command -v wc >/dev/null 2>&1; then
    wc -c <"$1" | tr -d ' '
  else
    printf '0'
  fi
}

commit="$(git -C "$repo_root" rev-parse HEAD 2>/dev/null || printf 'unknown')"
fc_version="$(firecracker --version 2>/dev/null | head -1 || printf 'unknown')"
jailer_version="$(jailer --version 2>/dev/null | head -1 || printf 'unknown')"
go_version="$("$go_bin" version 2>/dev/null || printf 'unknown')"
host_arch="$(uname -m 2>/dev/null || printf 'unknown')"
host_kernel="$(uname -r 2>/dev/null || printf 'unknown')"
host_cpus="$(getconf _NPROCESSORS_ONLN 2>/dev/null || printf '0')"

timings_json='null'
if [[ -s "$timings" ]]; then
  timings_json="$(cat "$timings")"
fi

evidence="$out_dir/evidence.json"
{
  printf '{\n'
  printf '  "schema": "rainier.microvm.kvm-test.run/v1",\n'
  printf '  "recorded_at": %s,\n' "$(json_string "$(date -u +%Y-%m-%dT%H:%M:%SZ)")"
  printf '  "result": %s,\n' "$(json_string "$result")"
  printf '  "oss_commit": %s,\n' "$(json_string "$commit")"
  printf '  "firecracker": %s,\n' "$(json_string "$fc_version")"
  printf '  "jailer": %s,\n' "$(json_string "$jailer_version")"
  printf '  "go": %s,\n' "$(json_string "$go_version")"
  printf '  "host": { "arch": %s, "kernel": %s, "cpus": %s },\n' \
    "$(json_string "$host_arch")" "$(json_string "$host_kernel")" "$host_cpus"
  printf '  "artifacts": {\n'
  printf '    "kernel_sha256": %s,\n' "$(json_string "$(sha256_of "$kernel")")"
  printf '    "rootfs_sha256": %s,\n' "$(json_string "$(sha256_of "$rootfs")")"
  printf '    "rootfs_bytes": %s\n' "$(size_of "$rootfs")"
  printf '  },\n'
  printf '  "timings": %s\n' "$timings_json"
  printf '}\n'
} >"$evidence"

echo "microvm-kvm-test: $result"
echo "microvm-kvm-test: $evidence"
exit "$status"

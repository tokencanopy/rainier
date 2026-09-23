# Wiring the portable workspace checkpoint into cold suspend and resume

Status: proposed, with the wiring in this change. Implements rainier-cloud
`adr-0003-serverless-microvm-architecture.md` §2.3 (the dormancy tiers), §4.1
(the Suspend/Resume/RemoveWorkspace rows) and §4.4 (the durability barrier), and
`hosted-product-prd.md` §10 (a checkpoint created and restore-tested after every
clean cold suspension).

Companions: `docs/design/2026-09-20-portable-workspace-checkpoint.md` (the format
and the library, [#99](https://github.com/tokencanopy/rainier/pull/99)) and
`docs/design/2026-09-20-microvm-bootstrap-token-and-vsock.md`, whose §4 is the
cold-suspend handshake this note extends.

> `Suspend(warm=false)` must not report success, and the host must not release
> the workspace slot, until the portable checkpoint upload completes and its
> manifest is committed atomically. (ADR-0003 §4.4)

## 1. The problem

The checkpoint library takes an `fs.FS`. A session's workspace is an **ext4
image** the guest mounts as a block device, and the host holds nothing but that
file. So the wiring question is one question: **how does a file tree get from
inside the guest's filesystem to the checkpoint writer running on the host?**

What decides it is that the host must never mount or parse a tenant's ext4
image. A loop mount is the host kernel's ext4 driver reading attacker-controlled
metadata in ring 0 — the largest attack surface a multi-tenant host could
volunteer for, on a design (ADR-0003 §4.5) whose premise is that the tenant's
kernel is not the host's.

## 2. Six options

1. **Host loop-mount** (`mount -o loop`). Simplest to write, and it puts the host
   kernel's ext4 driver in front of tenant-controlled bytes — and needs root or
   `CAP_SYS_ADMIN` in the runner, which ADR-0003 §4.5 spends a jailer to avoid.
   **Refused on the kernel attack surface alone.**
2. **Host userspace ext4 reader** (a Go ext4 parser, or `debugfs -R rdump`). The
   same hostile bytes parsed out of ring 0 by something nobody here maintains —
   still the host parsing tenant filesystem metadata, only slower. **Refused.**
3. **Guest-streamed tar.** The guest already has the filesystem mounted, is the
   only party that must parse it, and is inside the isolation boundary that
   exists for exactly this; the host receives a byte stream and applies its own
   limits. The cost is a new stream and an ordering constraint (§4). **Chosen.**
4. **Opaque image blob** — checkpoint the ext4 file itself. Trivially correct and
   completely non-portable: PRD §10 wants a checkpoint restorable on another
   qualified provider, and this is a 10 GiB mostly-zero artifact only an ext4
   host can open. It also defeats the tree digest and the exclusions. **Refused.**
5. **Network filesystem** (9p/virtiofs for the workspace instead of a block
   device). The host would hold the tree directly — and would then be serving a
   filesystem to a hostile guest, with every session's I/O path changed for a
   feature far smaller than the change. **Refused; worth revisiting only if the
   workspace device changes for other reasons.**
6. **Block-level diff** (dirty-page tracking, incremental images). The cheapest
   steady state, and it re-introduces (4)'s portability problem plus a chain of
   diffs whose base may never be deleted. **Refused for v1**; §4.2 of the format
   note keeps a differential format open *above* the tree, not below.

## 3. The shape

**Cold suspend.** During the `suspending{cold:true}` handshake, after sessiond
has flushed, killed its execs and unmounted the agent home, it opens a stream on
the existing relay conn and writes the workspace tree. runnerd feeds that stream
to a tar-backed `fs.FS` (`internal/wstream`), the checkpoint writer walks it,
encrypts frame by frame and commits to a blob store. Nothing plaintext reaches
host disk; the host holds one frame, one copy buffer and one entry index (§5).
When the manifest commits, the host runs `Verify`, tells the guest, and only
then terminates the VM, releases the slot and discards the rootfs. A cold
`Resume` that finds the image absent and a checkpoint recorded rebuilds the
image from it: §7.

## 4. The handshake

```
runnerd/driver                                   sessiond (guest)
  |  FrameControl suspending{cold,id=N}  ------------>|
  |<---------------  FrameControl suspend_ack{id=N}   |  (2 s budget)
  |                                                   |  flush, kill execs,
  |                                                   |  unmount agent home,
  |                                                   |  forget secrets
  |<---------------  FrameStream[id=N] chunk          |  (30 s to the 1st chunk)
  |<---------------  FrameStream[id=N] chunk          |  (60 s idle budget,
  |       ... checkpoint writer consumes as it goes ..|   30 min total)
  |<--- FrameControl workspace_end{id=N,ok,entries,bytes}
  |<---------------  FrameControl suspend_ready{id=N} |  (30 s budget)
  |  ... Writer.Write commits the manifest; the       |
  |      generation is recorded and persisted HERE    |
  |  ... Reader.Verify reads the committed bytes ...  |  (10 min budget)
  |  FrameControl checkpoint_committed{id=N} -------->|  (best effort)
  |  engine.Stop, release slot, discard rootfs        |
```

`FrameStream` is a new frame type (5, spelled out because it is wire-visible)
carrying the suspend **nonce** in `AttachID` — the token that already keeps a
late answer from satisfying the next suspend, so a stream from an abandoned one
is routed to nothing rather than into the live one. Stream and control frames
share the sessiond-side `connWriter` and the hub's single read loop, so the end
marker can neither overtake the last chunk nor be reordered against it.

**Budgets.** #98's cold suspend was 2 s + 30 s, which bought a flush, a
ten-second exec kill and an unmount; it cannot also buy a 10 GiB copy. The new
numbers, all fields a test can drive in milliseconds:

| leg | budget | why |
| --- | --- | --- |
| notice → ack | 2 s | unchanged (#98): a round trip on an open socket |
| ack → first chunk | 30 s | exactly #98's old ready budget: flush, kill, unmount |
| chunk → chunk, and last chunk → end marker | 60 s | progress, not throughput |
| end marker → `suspend_ready` | 30 s | the guest has nothing left to do |
| whole stream | 30 min | 10 GiB (the workspace disk) at ~6 MiB/s |
| `Verify` | 10 min | one sequential read of the committed object |

Worst case ≈ 41 minutes, and only for a session that is genuinely stuck. The
idle budget covers the HOST's slowness too: the stream is back-pressured by the
store through the pipe, so a store that stopped taking bytes stalls it and fails
the suspend — which keeps the VM.

**A cold suspend is now one handshake, not two.** `Op` sent the cold notice
itself before calling the driver; a checkpointing driver owns the notice,
because it owns the stream that follows. runnerd asks
(`CheckpointsColdSuspend()`) and sends nothing when the driver does.

## 5. The stream, and why it is not only a tar

`fs.WalkDir` is not streamable: it calls `ReadDir(".")` and needs **every** root
child's name before it visits the first one, while a root child's subtree sits
between it and its next sibling. No emission order fixes that, and buffering
entry *content* to answer it is what this design refuses. So, two sections:

```
"rainier.wsstream.v1\n"
index:  repeated uvarint(len) record, terminated by uvarint(0)
        record := kind byte ('d' | 'f' | 'l') || name
tar:    archive/tar, the same entries in the same order, bodies for regular files
```

The index is the tree's **shape only** — name and kind; every other attribute
(mode, size, mtime, link target) comes from the tar header when the walk reaches
that entry, so each fact has one source. The host buffers the index (capped by
`MaxEntries` and `MaxIndexBytes`, fail closed) and **never** content: the tar
reader is a one-way cursor the walk drags forward, and `Next` discards a skipped
body for free.

The order is `fs.WalkDir` order — depth-first, lexical per directory — because
that is the order the checkpoint writer walks in and its reader's directory
stack depends on. `ReadDir` sorts, the cursor only moves forward and is checked
to be ON the entry it answers about, so an out-of-order stream fails the suspend
rather than producing a wrong checkpoint. Entries the host excludes are dropped
from the index and skipped by the same cursor; whatever follows the last entry
the walk wanted is drained under the same total-byte limit, before the guest's
end marker can arrive.

**Limits the host enforces, all configurable** (`wstream.Limits`), each on bytes
the guest chose:

| limit | default | refusal |
| --- | --- | --- |
| entries | 500,000 | index too long |
| index bytes | 64 MiB | index too large |
| entry name | 4096 bytes, 255 per element | not a valid tree entry |
| a name absolute, uncleaned, with `..`/`.`/NUL | — | not a valid tree entry |
| a symlink target absolute or leaving the tree | — | refused |
| one entry's bytes / the whole stream | 8 GiB / 32 GiB | too large |
| entry kind | dir, regular file, symlink only | refused |
| order | strict `fs.WalkDir` order, no duplicates | refused |

They are a second fence, not the only one: `checkpoint` re-applies the name and
symlink rules on the way in and again on the way out. The host applies them
because it must not be the hop that trusts the guest. "Index bytes" is charged
what the structure *costs* — names plus ~96 bytes of record, node, map entry and
child pointer — so it bounds host memory rather than something that correlates
with it, and both ends charge it the same way.

## 6. What happens when something fails

- **The guest dies mid-stream** (or the conn dies): the reader sees the pipe
  close before the end marker, the write fails, nothing is committed, and the VM
  is left running — `settleFailedColdSuspend` rolls the entry back to "running"
  when the VM is still up. The failure is named as the STREAM rather than as a
  parse error, because the sandbox is what went away.
- **The guest reports its own failure**: `workspace_end{ok:false, stage}`, the
  `stage_failed` shape, with counts and never a path.
- **The store is unreachable or refuses**: the manifest was never put, so that
  generation stays free for the retry and the VM stays.
- **`Verify` fails**: the manifest IS committed, so the generation is spent — a
  manifest key has no attempt suffix and put-if-absent never overwrites — and is
  therefore **recorded and persisted the moment `Write` returns**, before the
  verify. The suspend still fails, the VM still stays, and the retry takes the
  NEXT generation. A record that advanced only on success would have every retry
  lose its manifest put to `ErrExists`: a session that can never be cold-parked
  again. The driver also probes for a free generation before it streams, so a
  record *behind* the store heals itself. The unverified objects are left for
  the sweeper; deleting them would race a reconciler that just committed them.
- **The sandbox is gone** (a crashed sessiond on a live VM): there is no
  handshake to run, so the suspend fails and the VM stays — and keeps staying,
  since nothing else will produce a checkpoint for it. The escape is `Destroy`.
  Stricter than §4.4 needs, and a known cost: §10.
- **A tampered checkpoint, or a restore's `mkfs` or chown failing**: the scratch
  directory and the partial image are removed, nothing is attached, and the
  session stays parked with its checkpoint intact.

Every failure keeps the VM and names the **stage** — `stream`, `write`,
`verify`, `restore`, `owner`, `mkfs` — and never a path, a name or a value,
because these reach a session's error column (tenancy §15.1). That extends to
`mkfs.ext4 -d`'s own output, which names the files it was copying, and is
therefore counted rather than quoted.

**A kept VM is a degraded VM**, inherently: by the time the stream runs, the
guest has done what #98's cold notice asks — execs dead, agent home unmounted,
delivered secrets forgotten — so a failed suspend leaves the VM running in that
state. The right outcome, and still not the session the user left.

## 7. The restore path

`Resume` on a cold record whose workspace image is gone:

1. Refuse unless the record names a committed generation: "no image and no
   checkpoint" is a session whose work is gone, and saying so beats booting an
   empty workspace that looks resumed.
2. `mkdir` a scratch directory `0700` at `<state>/restore/<id>`, replacing
   whatever a previous attempt left, removed by `defer` on every path.
3. `Reader.Restore` into it: authorization hook, manifest authentication, tree
   digest, all of the library's checks.
4. **Give the tree to the user the guest runs as.** A checkpoint records modes
   and not owners — a uid is not portable and the format is meant to be — so a
   restored tree belongs to whoever restored it, which is root, while the agent
   runs as the image's own user and the guest's `/init` chowns the mount point
   only when it is EMPTY (so a resumed workspace's contents are left alone).
   Without this the session comes back unable to write its own files. The uid
   and gid are runner configuration (`--checkpoint-restore-uid/-gid`, default
   1000): only whoever built the image knows which user it runs as.
5. Build the image at `<path>.partial` — `mkfs.ext4 -d <scratch>` through
   `DiskFormatter`, which grows a `FormatFromDir` method for it — and rename it
   into place only once it is a whole filesystem. The rename is load-bearing:
   the trigger above is the image's *absence*, so a file created at the final
   path and populated over minutes is one a crash leaves for the next resume to
   find, skip the restore for, and hand the guest unformatted. No `-N`: mke2fs
   sizes the inode table from the image (~655,000 inodes for 10 GiB, above the
   entry ceiling), and sizing it for the restored tree would starve what the
   session writes next. Then remove the scratch tree, attach, and boot.

The scratch directory is no new exposure: the workspace image on the host is
plaintext today, in the same state directory, with provider encryption as
defense in depth. It is `0700`, never inside a jail, removed before the guest
boots on every path — and whatever a *crashed* host left under `<state>/restore`
is removed when the driver starts, because a tree that outlives the process that
made it is a different claim than this one. `RemoveWorkspace` removes the
**image only**: the driver has no retention policy, and one that deleted a
checkpoint on teardown would make §4.4's deep-dormant tier unreachable.

## 8. The self-hosted store and key

Two required flags when `--driver=microvm`, both fail-closed:

- `--checkpoint-store-dir`: a local directory blob store. `PutIfAbsent` writes a
  temp file beside the key, fsyncs it, and `link(2)`s it into place — `EEXIST`
  is `ErrExists` where a rename would silently replace a committed manifest —
  and a failed write leaves nothing at the key and nothing beside it.
- `--checkpoint-key-file`: 32 bytes of key material (64 hex characters or 32 raw
  bytes), mode `0600` or tighter, owned by the runner, becoming a
  `checkpoint.StaticKeyWrapper` under `selfhosted/checkpoint/v1`. A missing
  file, a wrong size, an all-zero key, a mode any other user can read, or an
  owner that is not this runner are all refusals to START.

Both are built in `cmd/runnerd` and handed to the driver as ports, so a cell
substitutes GCS and KMS without touching the driver.

## 9. What rainier-cloud owes

- A **GCS blob store** (`x-goog-if-generation-match: 0` for `PutIfAbsent`,
  resumable upload for the content object) behind `checkpoint.BlobStore`.
- A **KMS wrapper** passing the authenticated context as AAD and returning the
  concrete key version, plus the regional key-readiness rule `Preflight` is for.
- The **workspace identity**: this driver has no workspace id in its `Spec` and
  uses the workspace volume name, so a cell carrying the control plane's
  workspace id should pass that instead.
- **Checkpoint-age policy and the freshness view** (PRD §10), the **orphan
  sweep** for objects whose manifest lost a race or failed its verify, and
  deletion (tenancy §14.2) — above the driver, out of this change.
- A **dispatch timeout** for a cold stop covering §4's budgets: a cell that
  cancels at 30 s gets a failed suspend and a live VM — safe, and useless.

## 10. Open questions

- **The index's memory, and how many suspends may spend it at once.** The host's
  `O(entries)` index is the one place this design spends what the library
  refuses to: up to 64 MiB per in-flight cold suspend (§5), with nothing
  bounding how many are in flight — a host drain suspends every session at once,
  so 32 of them is ~2 GB. A host-wide bound on concurrent checkpoints is the
  fix; an index carried *incrementally per directory* would remove the cost
  instead, at the price of a format that no longer round-trips through plain
  `archive/tar`.
- **A session whose sandbox is gone cannot be cold-parked**, and pins a VM and a
  slot until somebody destroys it (§6). The narrower rule — park without a
  checkpoint, gate only the deep-dormant *disk deletion* on one — is what §4.4
  asks for, and is a change to what the runner records, not to this barrier.
- **Compression on the wire** (measurable, unmeasured), and **incremental
  checkpoints**: every cold suspend writes the whole tree, and the generation
  counter plus the attempt-suffixed content key leave room for a differential
  format nothing here forecloses.

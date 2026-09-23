# Wiring the portable workspace checkpoint into cold suspend and resume

Status: proposed, with the wiring in this change.
Implements rainier-cloud `docs/architecture/adr-0003-serverless-microvm-architecture.md`
§2.3 (the dormancy tiers), §4.1 (the Suspend/Resume/RemoveWorkspace rows) and
§4.4 (the durability barrier), and `docs/product/hosted-product-prd.md` §10
(a checkpoint created and restore-tested after every clean cold suspension).

Companions: `docs/design/2026-09-20-portable-workspace-checkpoint.md`, which is
the format and the library ([#99](https://github.com/tokencanopy/rainier/pull/99)),
and `docs/design/2026-09-20-microvm-bootstrap-token-and-vsock.md`, whose §4 is
the cold-suspend handshake this note extends.

> `Suspend(warm=false)` must not report success, and the host must not release
> the workspace slot, until the portable checkpoint upload completes and its
> manifest is committed atomically. (ADR-0003 §4.4)

## 1. The problem

The checkpoint library takes an `fs.FS`. A session's workspace is an **ext4
image** the guest mounts as a block device, and the host holds nothing but that
file. So the wiring question is one question: **how does a file tree get from
inside the guest's filesystem to the checkpoint writer running on the host?**

The constraint that decides it is that the host must never mount or parse a
tenant's ext4 image. A loop mount is the host kernel's ext4 driver reading
attacker-controlled metadata in ring 0 — the single largest piece of attack
surface a multi-tenant host could volunteer for, on a design (ADR-0003 §4.5)
whose whole premise is that the tenant's kernel is not the host's kernel.

## 2. Six options

1. **Host loop-mount** (`mount -o loop`). Simplest to write, and it puts the
   host kernel's ext4 driver in front of tenant-controlled bytes. It also needs
   root or `CAP_SYS_ADMIN` in the runner, which ADR-0003 §4.5 spends a jailer
   to avoid. **Refused on the kernel attack surface alone.**
2. **Host userspace ext4 reader** (a Go ext4 parser, or `debugfs -R rdump`).
   Moves the parsing out of ring 0 and into a parser nobody in this repository
   maintains, over the same hostile bytes; `debugfs` on a corrupt image is a
   host-side dependency with a CVE history. Still the host parsing tenant
   filesystem metadata, only more slowly. **Refused.**
3. **Guest-streamed tar.** The guest already has the filesystem mounted, it is
   the only party that must parse it, and it is inside the isolation boundary
   that exists for exactly this. The host receives a byte stream and applies its
   own limits to it. The cost is a new stream on the control channel and an
   ordering constraint (§4). **Chosen.**
4. **Opaque image blob** — checkpoint the ext4 file itself. Trivially correct
   and completely non-portable: PRD §10 wants a checkpoint restorable on another
   qualified provider, and an ext4 image is a 10 GiB mostly-zero artifact that
   only an ext4 host can open. It would also defeat the library's per-entry tree
   digest and its exclusions. **Refused.**
5. **Network filesystem** (9p/virtiofs for the workspace instead of a block
   device). The host would hold the tree directly and no streaming would be
   needed — but the host process would then serve a filesystem to a hostile
   guest, the workspace's I/O path would change for every session, and the
   change is far larger than the feature. **Refused; worth revisiting only if
   the workspace device changes for other reasons.**
6. **Block-level diff** (dirty-page tracking, incremental images). The cheapest
   possible steady state, and it re-introduces (4)'s portability problem plus a
   chain of diffs whose base must never be deleted. **Refused for v1**; §4.2 of
   the format note keeps a differential format open *above* the tree, not below.

## 3. The shape

**Cold suspend.** During the `suspending{cold:true}` handshake, after sessiond
has flushed and killed its execs and unmounted the agent home, sessiond opens a
stream on the existing relay conn and writes the workspace tree. runnerd feeds
that stream to a tar-backed `fs.FS` (`internal/wstream`), the checkpoint writer
walks it, encrypts frame by frame and commits to a blob store. Nothing plaintext
is written to host disk; the host holds one frame, one copy buffer and one entry
index (§5). When the manifest commits, the host runs `Verify` against the
committed bytes, tells the guest, and only then terminates the VM, releases the
slot and discards the rootfs.

**Deep-dormant resume.** When a cold `Resume` finds the workspace image absent
and a checkpoint recorded, the driver restores the checkpoint into a 0700
scratch directory under the state dir, builds a fresh ext4 from it with
`mkfs.ext4 -d` (unprivileged, the same call `infra/scripts/build-env-image.sh`
uses), removes the scratch directory, attaches the image and boots.

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
  |<---------------  FrameControl workspace_end{id=N, |
  |                     ok, entries, bytes}           |
  |<---------------  FrameControl suspend_ready{id=N} |  (30 s budget)
  |  ... Writer.Write commits the manifest ...        |
  |  ... Reader.Verify reads the committed bytes ...  |  (10 min budget)
  |  FrameControl checkpoint_committed{id=N} -------->|  (best effort)
  |  engine.Stop, release slot, discard rootfs        |
```

`FrameStream` is a new frame type (5, spelled out because it is wire-visible)
carrying the suspend **nonce** in `AttachID`. The nonce is already the token
that keeps a late answer from satisfying the next suspend; reusing it means a
stream from an abandoned suspend is routed to nothing rather than into the live
one. Stream frames and control frames share the sessiond-side `connWriter`, so
the end marker cannot overtake the last chunk, and the hub demultiplexes both on
its single read loop, so it cannot reorder them either.

**Budgets.** #98's cold suspend was 2 s + 30 s. The 30 s bought a flush, a
ten-second exec kill and an unmount; it cannot also buy a 10 GiB copy. The new
numbers, all fields so a test can drive them in milliseconds:

| leg | budget | why |
| --- | --- | --- |
| notice → ack | 2 s | unchanged (#98): a round trip on an open socket |
| ack → first chunk | 30 s | exactly #98's old ready budget: flush, kill, unmount |
| chunk → chunk, and last chunk → end marker | 60 s | progress, not throughput |
| end marker → `suspend_ready` | 30 s | the guest has nothing left to do |
| whole stream | 30 min | 10 GiB (the workspace disk) at ~6 MiB/s |
| `Verify` | 10 min | one sequential read of the committed object |

Worst case ≈ 41 minutes, and only for a session that is genuinely stuck; the
ordinary case is the guest's ack plus the copy. The idle budget deliberately
covers the HOST's slowness too: the stream is back-pressured by the blob store
through the pipe, so a store that has stopped taking bytes stalls the stream and
fails the suspend — which keeps the VM, which is the right answer.

**A cold suspend is now one handshake, not two.** `Op` sent the cold notice
itself before calling the driver; with a checkpointing driver the driver owns
the notice, because it owns the stream that follows. runnerd asks the driver
(`CheckpointsColdSuspend()`) and sends nothing when the driver does.

## 5. The stream, and why it is not only a tar

`fs.WalkDir` is not streamable: it calls `ReadDir(".")` and needs **every**
root child's name before it visits the first one, and a root child's subtree sits
between it and its next sibling. No emission order fixes that. Buffering entry
*content* to answer it is what this design refuses to do.

So the stream has two sections:

```
"rainier.wsstream.v1\n"
index:  repeated uvarint(len) record, terminated by uvarint(0)
        record := kind byte ('d' | 'f' | 'l') || name
tar:    archive/tar, the same entries in the same order, bodies for regular files
```

The index is the tree's **shape only** — name and kind. Every other attribute
(mode, size, mtime, link target) comes from the tar header when the walk reaches
that entry, so there is one source of truth for it and nothing to cross-check.
The host buffers the index (`O(entries)` names, capped by `MaxEntries` and
`MaxIndexBytes`, fail closed) and **never** buffers content: the tar reader is a
one-way cursor that the walk drags forward, and `tar.Reader.Next` discards a
skipped body for free.

The order is `fs.WalkDir` order — depth-first, lexical per directory — because
that is the order the checkpoint writer walks in and the order its reader's
directory stack depends on. `ReadDir` sorts, the cursor only moves forward, and
an out-of-order stream therefore fails the suspend rather than producing a
wrong checkpoint. Entries the host excludes (`.rainier`, §3.2 of the format
note) are dropped from the index and skipped in the stream by the same cursor.

**Limits the host enforces, all configurable** (`wstream.Limits`), each of them
on bytes the guest chose:

| limit | default | refusal |
| --- | --- | --- |
| entries | 500,000 | index too long |
| index bytes | 16 MiB | index too large |
| entry name | 4096 bytes, 255 per element | not a valid tree entry |
| absolute name, `..` or `.` element, NUL | — | not a valid tree entry |
| symlink target absolute, or escaping the tree | — | refused |
| one entry's bytes | 8 GiB | entry too large |
| total stream bytes | 32 GiB | stream too large |
| entry kind | dir, regular file, symlink only | refused |
| order | strict `fs.WalkDir` order, no duplicates | refused |

They are a second fence, not the only one: `checkpoint` re-applies the name and
symlink rules on the way in and again on the way out. The host applies them
because it must not be the hop that trusts the guest.

## 6. What happens when something fails

- **The guest dies mid-stream** (or the conn dies): the pipe reader sees EOF or
  an error before the end marker, the write fails, nothing is committed, and the
  suspend returns `the workspace stream from the guest ended early`. The VM is
  left running; runnerd's `settleFailedColdSuspend` already rolls the entry back
  to "running" when the container is still up.
- **The guest reports its own failure**: `workspace_end{ok:false, stage}` — the
  `stage_failed` shape, with counts and never a path — and the suspend fails
  naming the stage.
- **The store is unreachable or refuses**: `Write` returns the store's error;
  the manifest was never put, so there is no checkpoint at that generation, the
  generation counter is not advanced, and the VM stays.
- **`Verify` fails**: the manifest IS committed, so the generation is consumed;
  the suspend still fails and the VM still stays. Retrying takes the next
  generation. A committed-but-unverified checkpoint is left for the sweeper —
  deleting it here would race a reconciler that had just committed it.
- **The restore's `mkfs` fails**: the scratch directory and the half-built image
  are both removed, and the resume fails with the session still parked. Nothing
  partial is ever attached.
- **A tampered checkpoint**: `Restore` fails `ErrAuth` before a byte is written;
  the scratch directory is removed and no image is created.

Every failure before the manifest commits keeps the VM and names the **stage** —
`stream`, `write`, `verify`, `restore`, `mkfs` — and never a path, a name or a
value, because these errors reach a session's error column (tenancy §15.1).

## 7. The restore path

`Resume` on a cold record whose workspace image is gone:

1. Refuse unless the record names a committed checkpoint generation. "No image
   and no checkpoint" is a session whose work is gone, and saying so beats
   booting an empty workspace that looks like a successful resume.
2. `mkdir` a scratch directory `0700` under `<state>/restore/<id>-<n>`, removed
   by `defer` on every path.
3. `Reader.Restore` into it — authorization hook, manifest authentication, tree
   digest, all of the library's checks.
4. Create the sparse image and `mkfs.ext4 -d <scratch>` through `DiskFormatter`,
   which grows a `FormatFromDir` method for it.
5. Remove the scratch tree, attach, boot.

The scratch directory is no new exposure: the workspace image on the host is
plaintext today, in the same state directory, with provider encryption as
defense in depth. It is `0700`, it is removed before the guest boots, and it is
never inside a jail.

`RemoveWorkspace` removes the **image only**. Checkpoint deletion stays above the
driver: the driver does not know the retention policy, and a driver that deleted
a checkpoint on a teardown would make ADR §4.4's deep-dormant tier unreachable.

## 8. The self-hosted store and key

Two required flags when `--driver=microvm`, both fail-closed:

- `--checkpoint-store-dir`: a local directory blob store. `PutIfAbsent` writes a
  temp file in the same directory and `link(2)`s it into place — `EEXIST` is
  `ErrExists`, and a failed write leaves the temp file removed and nothing at
  the key, which is the atomicity `BlobStore` demands.
- `--checkpoint-key-file`: 32 bytes of key material (64 hex characters or 32 raw
  bytes), mode `0600` or tighter, owned by the runner. It becomes a
  `checkpoint.StaticKeyWrapper` under the reference `selfhosted/checkpoint/v1`.
  A missing file, a wrong size, an all-zero key or a group/world-readable mode
  are all refusals to start, for the reason every other microVM preflight is: a
  runner that started anyway would accept placements and fail every cold suspend.

Both are constructed in `cmd/runnerd` and handed to the driver as ports, so
rainier-cloud substitutes GCS and KMS without touching the driver.

## 9. What rainier-cloud owes

- A **GCS blob store** (`x-goog-if-generation-match: 0` for `PutIfAbsent`,
  resumable upload for the content object) behind `checkpoint.BlobStore`.
- A **KMS wrapper** that passes the authenticated context as AAD and returns the
  concrete key version, plus the regional key-readiness rule `Preflight` exists
  for.
- The **workspace identity**. This driver has no workspace id in its `Spec`, so
  it uses the workspace volume name as the checkpoint's workspace component. A
  cell that carries the control plane's workspace id should pass it instead.
- **Checkpoint-age policy and the freshness view** (PRD §10), the **orphan
  sweep** for content objects whose manifest lost a race, and deletion (tenancy
  §14.2) — all above the driver, all out of this change.
- A **dispatch timeout** for a cold stop that covers the budgets in §4. A cell
  that cancels the request at 30 s gets a failed suspend and a live VM, which is
  safe but useless.

## 10. Open questions

- **The index's memory.** `O(entries)` names on the host is the one place this
  design spends what the library refuses to. 500,000 entries is roughly 50 MB of
  names; a large `node_modules` gets close. A second stream section carrying the
  index *incrementally per directory* would remove it, at the cost of a format
  that no longer round-trips through plain `archive/tar`.
- **Compression on the wire.** The stream is plaintext over a vsock socket
  inside one host; the checkpoint is uncompressed by §4.6 of the format note.
  Compressing the hop alone is measurable and unmeasured.
- **Incremental checkpoints.** Every cold suspend writes the whole tree. The
  generation counter and the attempt-suffixed content key already leave room for
  a differential format; nothing here forecloses it.

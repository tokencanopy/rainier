# The portable workspace checkpoint: a format, and a library that cannot be misused into losing work

Status: proposed, with the library in this change. No driver integration.
Implements rainier-cloud `docs/product/hosted-product-prd.md` §10 (the portable
workspace checkpoint, created and restore-tested after every clean cold
suspension), `docs/security/hosted-tenancy-and-security.md` §11 (envelope
encryption with versioned keys and authenticated context) and §14.2
(key-wrapper deletion), and `docs/architecture/adr-0003-serverless-microvm-architecture.md`
§2.3 (the three dormancy tiers) and §4.4 (the durability barrier).

Companion: `docs/design/2026-09-20-microvm-bootstrap-token-and-vsock.md`, whose
non-goals list said "Workspace disks and the portable checkpoint (§2.3)". This
is that.

The three requirements this note is written against, quoted, because everything
below is an attempt to satisfy exactly them and nothing more:

> A **portable workspace checkpoint** contains the session filesystem and
> native agent resume state; it does not contain arbitrary process memory or
> PTY state. Rainier maintains a versioned, encrypted, integrity-checked
> checkpoint format that can be restored onto another qualified provider within
> the workspace's product-region policy. (PRD §10)

> Portable checkpoints use a provider-neutral envelope-encryption hierarchy with
> versioned key references and authenticated workspace, session, and asset
> context. […] A destination proves authorization and key readiness before
> restore. (PRD §10)

> `Suspend(warm=false)` must not report success, and the host must not release
> the workspace slot, until the portable checkpoint upload to regional GCS
> completes and its manifest is committed atomically. […] Deep-dormant
> transition (disk deletion) is permitted only after a verified checkpoint
> exists that is newer than the disk's last write. (ADR-0003 §4.4)

## 1. Problem

A cold-suspended Serverless session is a row in a database and some bytes in
object storage. Everything else — the microVM, its memory, its PTY, the
credentials the agent was holding — is gone on purpose (ADR §2.2). So the bytes
in object storage are the product: if they cannot be read back, the session's
work is destroyed, and no amount of correct control-plane bookkeeping recovers
it.

That makes three properties non-negotiable and one property a trap.

**Non-negotiable.** The bytes must be *decryptable only in the context they
were written for* (§11 and §18 item 39: "Ciphertext copied to another context
fails authenticated decryption"). They must be *integrity-checked end to end*,
because a silently truncated object is worse than a missing one — a missing one
fails loudly. And the commit must be *atomic*, because ADR §4.4 makes the
commit the exact instant at which the host is allowed to throw the disk away.

**The trap** is memory. A workspace is a `node_modules` directory and a Rust
`target/` directory; tens of gigabytes and a million small files is ordinary.
Anything in the checkpoint path that is O(tree) in memory — a list of entries, a
map of digests, a buffered archive, a one-shot `Seal` over the whole stream —
works in every test and dies on the first real session. This is why the library
below has no function that takes or returns a slice of entries.

There is also a smaller problem, which is that nothing in this repository can
do any of this today. `internal/controld/seal.go` is the right cryptographic
construction (AES-256-GCM, a fresh 12-byte nonce per seal, `SealAAD`/`OpenAAD`
binding an identity into the ciphertext, one flat `errSecretAuth` for every
authentication failure) but it is one fleet-wide `[32]byte`, one-shot over a
`[]byte`, and in `internal/`, which a package rainier-cloud imports may not
reach (`scripts/check-public-control.sh`). The format needs the same
construction, streaming, over a key hierarchy.

## 2. Goals and non-goals

**Goals.** One versioned, integrity-checked, provider-neutral on-the-wire
format. Streaming in both directions with memory bounded by a frame, not by the
tree. Authenticated context that binds workspace, session, checkpoint
generation and format version to every byte, at three independent layers. An
atomic commit expressible against any blob store with put-if-absent. A restore
test that is a real test and not a second copy. A deletion story that is one
key-wrapper delete. Errors that carry no path, no file byte, and no key
material.

**Non-goals.** Scheduling, retention, and checkpoint-age policy (the cloud's
`cell-worker`). Upload retry and backoff. Any storage backend other than the
in-memory one used by tests. Driver integration — `internal/driver/` is not
touched by this change. Incremental or differential checkpoints. Deduplication.
Compression (§4.6). Resumable upload as a *format* feature (§4.7). Provider
snapshots, which ADR §2.3 keeps as an acceleration and not as the durable
representation.

## 3. What travels, and what cannot

### 3.1 In

**The session filesystem tree**, as one `fs.FS` rooted at the workspace, minus
the default exclusion list and anything the caller adds to it (§3.2:
the default set is not opt-in). Directories, regular files and
symbolic links travel. A file's permission bits (masked to `0o777`) and
modification time travel. A symlink's target travels, and is refused at pack
time if it escapes the tree — the same rule `protocol/workspace`'s `checkLink`
applies to a transfer, applied for the same reason and at the same end (refuse
before the bytes move, not after).

**Native agent resume state**, to the exact extent that it is a file under that
root. This is the one place where the PRD and the ADR have to be read together
and the answer is uncomfortable, so it is stated plainly: PRD §10 says a
checkpoint "contains the session filesystem and native agent resume state",
while ADR §2.3 says "the agent home is never part of any session's workspace
disk or portable checkpoint". Both hold only if the resume state a session needs
lives *in the workspace tree* rather than in the agent home. For some agents it
does (a project-local state directory); for others it does not (a per-user state
directory under the agent home). ADR §9 already has this open as "agent home
materialization", and this library cannot close it: it checkpoints the root it
is given. What it does instead is refuse to hide the question — §14 lists it as
open, and the Writer takes exactly one root so that a caller who needs two has
to come back and change the format rather than quietly getting half a session.

### 3.2 Out, by construction

**Process memory and PTY state.** There is nowhere to put them. The content
object is a tar stream of a filesystem tree; the format has no frame type, no
manifest field, and no code path that could carry a memory image. This is not a
policy the library enforces, it is a shape it does not have — which is the only
version of that promise worth making, given that ADR §2.2 calls a memory image
a secret-bearing artifact.

**The agent home.** It is a different mount (ADR §2.3: "Workspace and agent home
are separate block devices"), so a source rooted at the workspace cannot reach
it. The exclusion list is defense in depth on top of that, not the mechanism.

**Credentials.** Rainier-delivered credentials are not in the workspace to begin
with: the git credential reaches git through the in-sandbox helper's pipe, the
`gh` token is an environment variable on one process, the agent credential set
lives in the agent home, and environment secrets reach a microVM session over
the session RPC and never touch host disk (bootstrap-token note §3). So
exclusion is not what keeps credentials out of a checkpoint; *the credential
never being a file under the workspace root* is. The exclusion list exists for
Rainier-owned paths inside the workspace (`/workspace/.rainier`) and for
whatever a caller's policy adds.

That list is applied **by construction at the API, not by opting in**. Every
`Write` unions `DefaultExclusions()` with the caller's `Source.AlsoExclude`
before the walk begins, and the field is named for the union: there is no
spelling of a `Source` — zero value, struct literal, `DirSource` with no extra
arguments — that checkpoints `/workspace/.rainier`, and no flag or escape hatch
that produces the raw tree. An opt-in default is a default a caller in a hurry
does not get, and "the Rainier-owned control directory is in this checkpoint"
is not a mistake that announces itself at the time it is made. Extra arguments
to `DirSource` *add* to the set; they cannot narrow it.

What the exclusion list is emphatically **not** is a redaction pass. Tenancy
§8.2 is explicit that a workload may deliberately write a credential into its
own workspace, that those bytes are customer content, and that "Rainier does not
represent redaction as a security boundary". A checkpoint of a tree containing a
user-written token contains that token; it is protected by being encrypted under
the session's context and by the session ACL, exactly like the live disk. §18
item 38 asks a snapshot to prove *Rainier-delivered* material is excluded, and
that is the claim made here — no wider one.

## 4. The format

### 4.1 Two objects, one commit

A checkpoint is two objects in a blob store:

```text
<prefix>/rainier.checkpoint.v1/ws/<workspace>/sess/<session>/gen/<0000000000000000000N>/manifest.json
<prefix>/rainier.checkpoint.v1/ws/<workspace>/sess/<session>/gen/<0000000000000000000N>/content.<attempt>
```

The **content** object is the encrypted tree. Its key carries a 16-byte random
`attempt` suffix, so two writers at the same generation — a retry after a host
died mid-upload, a reconciler racing the original — never collide, never
overwrite, and never have to reason about whether a half-written object was
theirs.

The **manifest** object is small, plaintext JSON, and *is the checkpoint*. Its
key has no attempt suffix, and it is written with put-if-absent. That single
conditional write is the atomic manifest commit ADR §4.4 rests on: before it
succeeds there is no checkpoint at generation N, after it succeeds there is
exactly one, forever, and the loser of the race is told `ErrExists` rather than
being allowed to believe it committed. Orphaned content objects from lost
attempts are garbage the retention sweeper collects; they are unreadable
without their manifest's wrapped key, so leaving one lying around costs storage
and discloses nothing.

Generation is zero-padded to 20 digits so that a lexical listing of the `gen/`
prefix is generation order — the retention sweeper and the "latest verified
checkpoint time" view both want that and neither should have to sort.

Workspace and session identifiers are validated against
`[A-Za-z0-9][A-Za-z0-9._-]{0,127}` before they are used in a key or in an AAD.
This is not fussiness: an identifier containing `/` would place one session's
checkpoint under another's prefix, and an identifier containing a NUL would make
the NUL-separated authenticated context ambiguous.

### 4.2 One framed stream, not content-addressed chunks

The requirement leaves the choice open, so here is the argument.

**Chunked and content-addressed** — split the tree into content-defined chunks,
name each by its digest, upload each separately, and let the manifest list them
— buys three real things: deduplication across generations (a checkpoint after
an edit re-uploads only what changed), resumable upload for free (a failed
upload retries the chunks it lost), and parallel upload. For a workspace that
is checkpointed every ten idle minutes and is mostly `node_modules`, dedup is
not a small win.

It costs: a chunk store needs reference counting and a garbage collector, which
means deletion stops being a delete and becomes a distributed refcount problem
— and §14.2 requires deletion to be *provable*, per storage class, in a ledger.
It costs a second atomicity problem, because a manifest is now only valid if
every chunk it names is still present, and "still present" is exactly what a GC
gets wrong. It leaks: per-chunk object sizes and counts are metadata about the
tree, visible to anyone who can list the bucket, and a shared chunk store leaks
*across* contexts by construction, which is precisely what §11 forbids
ciphertext to do. And it makes the restore test expensive: verifying a
chunk-addressed checkpoint means a GET per chunk.

**One framed stream** — one content object, encrypted as a sequence of
independently authenticated frames — costs the dedup and pays for it with: one
object to commit, one object to verify with one sequential read, one object to
delete, no GC, no refcounts, and no cross-context sharing anywhere in the
storage layout.

**Chosen: one framed stream.** The deciding argument is §14.2 and §4.4 rather
than efficiency. The checkpoint's job is to be the thing the control plane can
say a true sentence about — "this exists, it verified, it is newer than the
disk's last write, and deleting it is one operation that either happened or
did not". Dedup can come back later as a *tier below* the format (a blob store
that happens to chunk internally, or a differential second format at a new
version string) without the durability barrier having to change. A garbage
collector standing between a customer's workspace and its only durable copy
cannot be added later and then removed.

Note that "one stream" cannot mean one AEAD operation. `crypto/cipher`'s GCM is
one-shot over a `[]byte`, so sealing a 40 GiB tree in one call needs 40 GiB of
memory twice. Framing is what makes a single logical stream implementable at
all, and it is also what gives per-frame integrity, random-access verification,
and detectable truncation. So the honest description of the choice is: one
content object, framed internally, versus many objects addressed by content.

### 4.3 The content object

Plaintext is a tar stream (`archive/tar`), entries in `fs.WalkDir` order:
depth-first, lexical within each directory, and therefore deterministic. That
order is a **rule of the format, not an accident of the writer**, and the reader
enforces it — every entry's parent must be a directory that appeared earlier in
the stream and that the walk has not yet left. §7 explains why that rule is the
one that makes containment sound. `Uname`, `Gname`, `Uid` and `Gid` are
zeroed for the reason `protocol/workspace`'s `TarGz` zeroes them: they are the
packer's local account names, meaningless in a guest and needlessly
identifying. Modes are masked to `0o777`; setuid, setgid and sticky bits are
dropped, because a durability artifact that can carry a setuid bit into a fresh
guest is an escalation primitive the moment any restore target runs as a
different uid, and no part of the product promises that bit survives a cold
suspend. Sockets, device nodes and FIFOs are **skipped and counted** rather than
refused — this diverges from `TarGz`, which refuses them, and deliberately: a
transfer the user asked for should fail on a surprise, but a stray dev-server
socket in a workspace must not be able to defeat the durability barrier. The
skipped count is in the manifest and in the report, so "not the tree the user
named" is at least never silent.

Three details about what a header carries, each of which is a decision rather
than a default.

**Modification times are whole seconds.** tar's header field is seconds;
sub-second precision exists only as a PAX extended record, which costs a 1 KiB
block *per entry* — a million-file tree would pay a gigabyte for nanoseconds no
build tool asks for. The truncation happens in this package rather than in
`archive/tar` (which would do it silently) so that the tree digest can commit to
the same value a restore reproduces.

**A directory's and a symlink's modification time are not carried at all**, and
are pinned to the Unix epoch in the header. Neither is restored — a directory's
mtime is a function of the order its children were written, and `os.Chtimes` on
a symlink is not in the standard library — and carrying a value the format does
not preserve would cost the property that makes the rest of this note work: with
them pinned, *the plaintext stream is a function of exactly what the tree digest
commits to*. Two checkpoints of the same tree have byte-identical plaintext
(their ciphertext differs, because the data key is fresh per checkpoint),
re-checkpointing a restored tree reproduces the same tree digest and the same
plaintext length, and a future differential format has something stable to diff
against.

**A hard link travels as an independent regular file.** An `fs.FS` does not
expose link counts, so this is what falls out naturally rather than a choice
that was available to make differently; the fidelity loss (two names that shared
an inode no longer do) is recorded here because it is real, and the alternative —
refusing a tree that contains one — would be a workspace that cannot be
checkpointed, which is strictly worse.

The plaintext stream is cut into fixed-size frames (default 1 MiB, every frame
but the last exactly that size) and each frame is sealed with AES-256-GCM:

```text
content object = frame[0] || frame[1] || … || frame[n-1]
frame[i]       = AES-256-GCM(k_content, nonce(i), plaintext[i], aad(i))
nonce(i)       = prefix[8 bytes, random per checkpoint] || uint32be(i)
aad(i)         = context || 0x00 || "frame" || 0x00 || dec(i) || 0x00 || final
```

There is no length prefix, no magic number, and no header — not for elegance but
because every one of those would be an unauthenticated byte in an object whose
whole purpose is to be authenticated. Frame lengths are known (`frame_size + 16`
for all but the last), the frame count is in the manifest, and the manifest is
authenticated, so the reader reads exactly the frames it was told about.

That construction gives, in order of how likely each failure is:

- **A flipped bit anywhere** fails one frame's GCM tag, and the reader stops.
- **Truncation** — the failure mode of an upload that died — is caught twice: the
  stream ends before frame `n-1`, and the ciphertext length and digest recorded
  in the manifest do not match. A truncated-but-plausible stream cannot be made
  to look complete, because the last frame is the only one whose AAD says
  `final=1`.
- **Reordering or splicing within a checkpoint** fails, because `i` is in the
  AAD.
- **Splicing across attempts at the same generation** fails, because the data
  key is fresh per checkpoint, so a frame from another attempt is under another
  key even though its context is identical.
- **Nonce reuse** is impossible without a `crypto/rand` failure: the key is used
  for exactly one stream, `i` is unique within that stream, and the 8-byte
  random prefix means that even a hypothetical key-reuse bug would need a
  2⁻⁶⁴ collision before it became a GCM break rather than a decryption failure.

`frame_size` is bounded (64 KiB to 16 MiB) and the frame count is bounded by
`uint32`, which at the 1 MiB default is a 4 PiB ceiling on one checkpoint and at
the 64 KiB floor is still 256 TiB. Both bounds are enforced on parse, so a
manifest cannot ask a reader to allocate a gigabyte-sized frame buffer.

### 4.4 The manifest

Plaintext JSON, one flat object, every field required, unknown fields rejected.
It holds: the format version string; the workspace, session and generation it
belongs to; the creation time; the algorithm names (`AES-256-GCM`,
`HKDF-SHA256`, `none` for compression); the versioned key reference and the
wrapped data key; the content object's key, byte length, and SHA-256; the frame
size, frame count and nonce prefix; the plaintext byte length; the entry count,
file-byte total and skipped count; the tree digest; and its own authentication
nonce and tag.

It holds **no path, no file name, no symlink target, and no file content**.
Those are all inside the encrypted stream. Tenancy §4.2 counts paths, branch
names and diffs as session content and §15.1 forbids logging them, so a
plaintext sidecar listing them would be a leak that no amount of careful
logging downstream could undo. The manifest is a thing an operator can read;
that is a property it has to earn by containing nothing.

It is authenticated even though it is plaintext. `manifest_auth` is the
16-byte GCM tag of an empty plaintext under `k_manifest`, with the AAD being the
authenticated context followed by a canonical, NUL-separated rendering of every
other manifest field:

```text
manifest_auth = tag of AES-256-GCM(k_manifest, manifest_nonce, ∅, aad)
aad           = context || 0x00 || "manifest" || 0x00 || canonical(fields)
```

Two consequences worth stating. First, the tag cannot be checked without the
data key, which cannot be unwrapped without the key service agreeing — so
"is this manifest authentic" and "am I allowed to read this checkpoint" are the
same question, answered once. Second, the canonical rendering is written by
hand, field by field, in a fixed order, rather than by re-serializing JSON. A
MAC over `json.Marshal` output is a MAC over whatever the encoder does this
year; a MAC over an explicit list is a MAC over a list. The cost is that a
field added to the struct and forgotten in the canonical rendering would be
unauthenticated metadata — the worst bug this format could have — so a test
reflects over the struct's JSON tags and fails if any tag but the two excluded
by definition (`manifest_nonce`, `manifest_auth`) is absent from the canonical
input.

`content_key` is in the manifest and therefore authenticated, and is also
checked to live under the checkpoint's own prefix on parse. The first check is
the security one; the second keeps the layout honest.

### 4.5 Versioning

The version is one string, `rainier.checkpoint.v1`, and it appears in three
places that must agree: the `version` field, the storage key prefix, and the
authenticated context. A reader that does not implement a version refuses it
with a typed error before it looks at anything else, and it refuses rather than
guesses: there is no "minor version" and no field that a v1 reader is allowed
to ignore, because "ignore what you don't understand" is how a format acquires
an unauthenticated extension point.

A v2 is a new string, a new key prefix, and a reader that implements both. The
manifest's algorithm and compression fields exist so that v2 can be a one-field
change with an obvious diff rather than an archaeology exercise — but they are
strict today: `compression` must be `none`, and a reader seeing `zstd` refuses
it as an unimplemented format rather than as a bad field.

### 4.6 Compression: deliberately absent

A workspace tree compresses well and the ADR's economics care about dormant
storage and transfer. Compression is still out of v1, for reasons that are
mostly not cryptographic: it adds a decompression-bomb bound that the Verify
pass would have to enforce (a 40 GiB manifest claim from a 4 MiB object), it
adds a second failure layer between the ciphertext digest and the tar reader,
and most of a real workspace's bulk is already compressed (git packfiles,
downloaded tarballs, images, binaries). The cryptographic reason is thinner but
real: compressed length is a function of content, so a compressed-then-encrypted
object leaks an entropy estimate of the tree it holds.

The field is in the manifest so that adding `zstd` later is a declared,
strictly-parsed format change rather than a silent one. Whether it is worth it
is §14's first open question, and it is a measurement, not an argument.

### 4.7 Resumable upload: a non-goal that the store gets for free

The format has one content object, so a failed upload retries from the
beginning under a new attempt id. That is the honest cost of §4.2's choice.

It is smaller than it looks, because resumability at the *format* layer and at
the *store* layer are different things: GCS's own resumable-upload protocol makes
a single-object write resumable inside the blob-store implementation, invisibly
to the format, and `PutIfAbsent` is expressed as "write this object from this
writer" precisely so that an implementation may do that. Cheap resumability is
therefore available and is the store's business; expensive resumability —
chunk-level, with a manifest that can name a partially-uploaded set — is the
non-goal.

## 5. Keys and authenticated context

### 5.1 The context

```text
context = "rainier.checkpoint.v1" 0x00 workspace 0x00 session 0x00 dec(generation)
```

NUL separators, for the reason `agentCredentialAAD` uses them: they keep
`("a","bc")` and `("ab","c")` apart, and every component is validated to contain
no NUL before it goes in. Tenancy §11 asks a checkpoint's context to be
"workspace, session, checkpoint generation"; the format version is added because
a v2 that reuses the same key hierarchy must not be able to open a v1 object's
data key, and because the version is the one piece of the interpretation of the
bytes that the bytes themselves cannot safely assert.

The context is not used raw. Three purpose-separated AADs derive from it — `key`
for the wrap, `manifest` for the manifest tag, `frame` for each frame — so that
no ciphertext produced for one purpose can be presented as another.

### 5.2 The key hierarchy

Envelope encryption, two levels, which is what §11 asks for:

1. A **data key** (32 bytes from `crypto/rand`) is generated per checkpoint,
   used for exactly one content object and one manifest, and never stored.
2. From it, two purpose-separated subkeys, so that the manifest tag's nonce can
   never collide with a frame nonce:
   `k_content = HKDF-SHA256(dek, salt=context, info="…v1 content")`,
   `k_manifest = HKDF-SHA256(dek, salt=context, info="…v1 manifest")`.
   The context as HKDF salt is a fourth binding and costs nothing.
3. The data key is **wrapped** by a key service under a *versioned key
   reference*, with the context as AAD, and only the wrapped form is stored.

The key-reference model is the one this repository already has, generalized by
exactly one step. `internal/controld` has a single fleet key
(`RAINIER_SECRETS_KEY`, one `[32]byte`, `seal.go`'s deliberate "no key
derivation and no versioning byte") and a vault that binds identity into the
AAD (`agentvault.go`'s `(user, provider, version)`). This library keeps the
binding idea verbatim and adds versioning where §11 requires it:

```go
type KeyRef string

type Wrapper interface {
    Wrap(ctx context.Context, ref KeyRef, dek, aad []byte) (wrapped []byte, used KeyRef, err error)
    Unwrap(ctx context.Context, ref KeyRef, wrapped, aad []byte) (dek []byte, err error)
}
```

`Wrap` takes the reference the caller *asked for* — which may be an alias like
`workspace/alpha/checkpoint` — and returns the concrete reference it actually
used, which is what goes in the manifest. That one return value is the whole
versioned-key story: rotation changes what the alias resolves to, old
checkpoints keep naming the old version, and `Unwrap` is never asked to guess.
A `KeyRef` is opaque to this package; self-hosted spells it
`fleet/secrets/v1` and a hosted cell spells it a Cloud KMS resource name, and
neither spelling reaches the format's logic.

The library ships one implementation, `StaticKeyWrapper`: one `[32]byte`, one
reference, AES-256-GCM with a fresh 12-byte nonce and the AAD bound — the same
construction as `seal.go`, in the same shape, with the same flat
authentication error. It is what self-hosted Rainier and every test use. It is
deliberately a *copy* of that construction rather than a call into it: a public
package may not import `internal/` (`scripts/check-public-control.sh`), and
inverting the dependency so that `controld` seals through this package would
change the credential path in a change that is supposed to be additive. The
copy is annotated in both directions so the next person sees two spellings of
one construction and not two constructions.

`crypto/kms`-shaped services, HSMs, and per-workspace KEK hierarchies are all
expressible behind `Wrapper`, and none of them are in this repository.

### 5.3 Three bindings, not one

Every layer binds the same context independently:

| Layer | Bound by | What a context swap looks like |
|---|---|---|
| Data key | `Wrap`/`Unwrap` AAD | the key service refuses to unwrap; there is no key to try |
| Manifest | `manifest_auth` tag | the tag fails; nothing in the manifest is trusted |
| Every frame | per-frame AAD | the frame fails; no plaintext is returned |

This is redundant on purpose. Any one of the three would satisfy §18 item 39 on
paper. Three means that a mistake in one of them — a wrapper implementation that
ignores its AAD, a reader that checks the manifest tag after using a manifest
field, a refactor that drops the frame index — degrades the property instead of
deleting it. The library's own tests reach past the cheap identity check and
exercise the cryptographic path directly for exactly this reason: a context-swap
test that only proves a string comparison failed has tested nothing.

The cheap check exists too: a manifest that says it belongs to another
workspace, session or generation is refused with `ErrContextMismatch` before any
key is touched. It leaks nothing new — the manifest already says so in plaintext
— and it turns the common operational mistake (restoring the wrong checkpoint)
into a sentence instead of an authentication failure.

## 6. The durability barrier

Against an abstract blob store with put-if-absent:

```go
type BlobStore interface {
    PutIfAbsent(ctx context.Context, key string, write func(io.Writer) error) error
    Open(ctx context.Context, key string) (io.ReadCloser, error)
    Delete(ctx context.Context, key string) error
}
```

`PutIfAbsent` creates the object at `key` only if nothing is there, returning
`ErrExists` otherwise, and it either creates the whole object or leaves nothing
— a failed `write` must not leave a partial object visible. GCS implements it
with `x-goog-if-generation-match: 0`; S3 with `If-None-Match: *`; the in-memory
store by buffering and inserting on success only. The callback shape, rather
than an `io.Reader` parameter, is what lets the producer be a streaming tar
writer without an `io.Pipe` and a goroutine between it and the network, and
lets a real implementation use its own resumable-upload writer (§4.7).

The barrier is then four steps, in this order, and the order is the design:

1. **Quiesce.** The caller's job, not the library's; ADR §4.4 says "quiesce
   writes" and the library detects a violation rather than preventing it (a file
   whose size changed between stat and read fails the write, because a tar
   header has already promised a length).
2. **Upload content.** `PutIfAbsent` at the attempt-suffixed key, streaming.
   Aggregates — frame count, ciphertext digest, entry count, tree digest — are
   computed during this single pass because they are inputs to the manifest.
3. **Commit the manifest.** `PutIfAbsent` at the generation key. This is the
   atomic commit. Before it, nothing; after it, exactly one checkpoint at
   generation N.
4. **Verify by reading back through the store** (§7), and only then report
   suspend successful and release the workspace slot.

Step 4 reads through the blob store rather than verifying the bytes it just
had in memory, which is the only version of a restore test that means anything:
the claim being tested is "the committed object is restorable", and an
in-memory check tests the encoder against itself. A host that dies between any
two steps leaves either no checkpoint or a verified-but-unreported one; the
disk is still there, and reconciliation re-runs from step 2 under a new attempt
id, as ADR §4.4 describes.

Deep-dormant (disk deletion) needs "a verified checkpoint newer than the disk's
last write". The library supplies the two halves of that sentence it can — the
manifest's authenticated `created_at`, and a verification report — and does not
supply the comparison, which needs the disk's last-write time and is the
control plane's.

## 7. What "verified" means

`Verify` is the restore test, and it is defined by what it does *not* do: it
does not write a tree. It:

1. Reads and strictly parses the manifest from the store.
2. Refuses an unimplemented version, a mismatched context, an out-of-range frame
   size or count, a content key outside the checkpoint's prefix.
3. Unwraps the data key — which is where a destination's authorization and key
   readiness are actually proven — and checks the manifest tag.
4. Streams the content object once, hashing the ciphertext as it goes, opening
   each frame with its own AAD, and feeding the plaintext to a tar reader.
5. **Walks the structure**: every entry's name is re-validated under the restore
   path rules, every entry's kind and mode are checked, file bytes are hashed
   and discarded, and the per-entry records are folded into a tree digest.
6. Compares, at the end: ciphertext length and digest, plaintext length, frame
   count, entry count, file-byte total, and tree digest, each against the
   authenticated manifest.

That is "decrypt, integrity, and a structural walk". It costs one sequential
read and a constant amount of memory, and it proves the properties a restore
depends on — that every byte decrypts, that the tree it decodes to is
well-formed and lands inside a target directory, and that it is the tree the
writer said it was. It does not prove that the destination filesystem has room,
which is why `Restore` re-checks the same aggregates while writing.

**The structural walk runs the restore path's code, not a copy of it.** `Verify`
and `Restore` are one function with the writes switched off. That is not a
tidiness preference: the durability barrier releases the workspace — and,
eventually, deletes the disk — on `Verify`'s word, so any rule `Restore` applies
that `Verify` does not is a rule that turns a verified checkpoint into a failed
cold resume. Two rules in particular are only visible because of it:

- **Every entry's parent must be a directory this walk created.** The symlink
  check is lexical — it resolves a link's target against the link's own *name* —
  and that is sound only while the link is created where its name says. It is
  not, if an ancestor component of the name is itself a link. `a/d -> ..` is
  contained (it resolves to the root); `a/d/up -> ..` is contained (it resolves
  to `a`); but `a/d/up` is physically created inside whatever `a/d` points at,
  which is the root, so its `..` leaves the target and `a/d/up/pwned` lands
  outside it. Two individually contained links compose into an escape. Requiring
  the parent to be a directory the walk created closes it, because a link is
  never one — and it is also what makes §4.3's depth-first rule enforceable
  rather than assumed.
- **A directory's recorded mode is applied when the walk LEAVES it**, not when it
  is created. Directories are created `0o700` and chmodded on the way out. A
  `0555` directory — which `go mod download` produces for every module it
  extracts, and a vendored or Cargo tree produces too — otherwise packs cleanly,
  verifies cleanly, and fails at restore on its first child. This is the failure
  mode the barrier is least able to survive, because nothing about it is visible
  until the disk is gone.

Both need the walk to remember something, which §9 otherwise forbids. What they
need is a *stack*, not a list: entries arrive depth-first, so the state is the
chain of directories currently open — O(tree depth), a few hundred at worst.
That is the one exception, and it is the whole of it.

The tree digest is what makes PRD §19's "checksum-equal restore" checkable
without a second copy:

```text
tree = SHA-256 over, in stream order, for each entry:
       kind 0x00 name 0x00 mode 0x00 size 0x00 mtime 0x00 (sha256(bytes) | linkname) 0x0a
```

Directory and symlink modification times are excluded, because the restorer does
not set them and §4.3 does not even carry them. Excluding what cannot be restored
is what keeps the digest an equality and not an aspiration.

## 8. How a destination proves authorization and key readiness

As a contract the library enforces, in three parts:

**The caller supplies the context; the manifest never gets to.** `Verify`,
`Restore` and `Preflight` all take the `Context` the caller *expects*, and that
is the value used to build every AAD. The manifest's own identity fields are
compared against it and are otherwise not consulted. A library that read the
context out of the manifest and then used it to open the manifest would make
§18 item 39 vacuous — the ciphertext would always be in "its own" context — and
this is the single easiest way to get this format wrong.

**Authorization is a required argument, not an assumption.** `Options.Authorize`
is a `func(context.Context, Context, Manifest) error` that runs after the
manifest is parsed and identity-checked and *before* the data key is unwrapped,
before a content byte is read, and before the target directory is touched. A nil
hook is refused at `NewReader`, not at the first restore, so that "we forgot to
authorize the restore" is not a thing this library can be used to do. The library
cannot judge the policy; tenancy §4 and §8.2 define it and it lives in the cell.
What the library can do is guarantee the step exists and runs first.

**The manifest that hook receives is not authenticated, and it says so.** It
cannot be: the tag is checkable only with the data key, and unwrapping that key
is the step being authorized. So by the time the hook runs, exactly three things
are known — the manifest parsed strictly, its workspace/session/generation equal
the caller's context, and its content key is under this checkpoint's own prefix.
Every other field is whatever was in the object, which for an attacker with
write access to the bucket and no key means *attacker-chosen*. §5.3 lists "a
reader that checks the manifest tag after using a manifest field" as exactly the
degradation the three bindings exist to survive, and this reader is one of them
by construction. The hook's documentation therefore states the rule outright:
**decide with it, do not record from it.** Branching on the key reference to
check readiness is what it is for; writing the creation time into a freshness
ledger before `Verify` has returned is recording a forgery. The authenticated
values are the ones in a `Report` or a `Preflight`, both returned only after the
tag has been checked.

`Delete` is the one operation that authorizes against an unauthenticated
manifest and then acts. That is deliberate and unavoidable: deletion has to keep
working after the key version behind it is destroyed, which is the other half of
§10. The only field it acts on is the content key, already pinned to this
checkpoint's own generation directory, so the worst a forged manifest achieves
is deleting a sibling attempt object in the generation the caller asked to
delete anyway.

**Key readiness is provable without content.** `Preflight` does steps 1–3 of
§7 — parse, identity, authorize, unwrap, manifest tag — and returns what a
restore would do, without reading one byte of the content object. That is the
call a placement decision makes when PRD §4.3 filters capacity by "storage and
key readiness": a destination that cannot reach the key version in its region
fails `Preflight` in one small request, before anything is scheduled. `Restore`
begins by doing exactly the same thing, so preflighting is an optimization for
the scheduler and never a check the restore path skips.

`Restore` additionally refuses a target directory that exists and is not empty,
and creates every regular file with `O_CREATE|O_EXCL`, which both refuses to
follow a symlink into place and makes a duplicated entry name an error instead
of an overwrite.

## 9. Bounded memory

The rule is that no structure in the library is O(entries) or O(tree bytes).
Concretely, at any instant a write or a restore holds: one frame's plaintext and
one frame's ciphertext (`frame_size + 16`), a 32 KiB copy buffer, the tar
reader's or writer's own fixed buffers, one entry's header, the O(tree depth)
directory stack §7 describes, and a handful of hash states. That is under 3 MiB
at the default frame size, for any tree.

The things that would break it, and are therefore not in the API: a manifest
that lists entries; a `[]Entry` return anywhere; a map of path to digest for
comparing trees; a two-pass extractor like `protocol/workspace`'s `UntarGz`
(which reads the archive twice so it can refuse a hostile tar whole — correct
there, where the archive comes from the other end of a transfer, and
unaffordable here, where the stream is tens of gigabytes and is authenticated
under the tenant's own key before a single entry is looked at); directory mtime
restoration, which would need a list of directories; and `io.ReadAll` anywhere
at all except the manifest, which is capped at 64 KiB on read.

Two tests prove it rather than asserting it, because the claim has two halves
and a single ceiling test expresses neither well.

**The bytes:** a synthetic 2 GiB sparse tree is written through a discarding
store while the heap is sampled, failing if the peak crosses a 12 MiB ceiling
(measured: 4.5 MiB). It skips only if the environment cannot create the sparse
file.

**The entries:** the same write is measured at two thousand entries and at forty
thousand, and the *difference* must be under 2 MiB (measured: 0.67 MiB for
twenty times the entries). That is the property actually being claimed — the
peak is a function of the frame size, not of the tree — and a ceiling cannot
express it: "under N bytes at this fixture size" passes for any structure whose
constant factor the test's author underestimated, and a map of path to digest is
exactly the kind of thing that gets underestimated. Measuring the slope needs no
guess, and costs seconds rather than a minute of cryptography.

The read side is measured against a store that streams from disk, because a
store holding the ciphertext in the heap would put its own allocation inside the
measurement.

## 10. Deletion

Tenancy §14.2 requires final deletion to enumerate "checkpoints, snapshots,
environment artifacts containing workspace data, exports, caches, signed
capabilities, credentials/grants, provider resources, and encryption wrappers",
each recording completion or an explicit pending state. §16 names "key-wrapper
deletion" as a required control for "Workspace deletion leaves accessible copy".

This format makes that cheap, and it is one of the reasons for §4.2's choice.
The data key exists only in the manifest, wrapped. So:

- **Deleting the manifest** makes the content object unreadable by anyone,
  including Rainier, immediately and irreversibly. There is no second copy of
  the data key and no path that reconstructs it. The content object becomes
  bytes nobody can interpret, and its own deletion becomes a storage-cost
  question rather than a confidentiality one — which is exactly the property a
  deletion ledger wants, because "pending" on the content object is then not a
  disclosure.
- **Destroying the key version** a manifest names makes every checkpoint under
  that reference unreadable at once, without touching object storage at all.
  That is the workspace-level crypto-shred, and it is the reason `key_ref` is a
  first-class authenticated field rather than an implicit fleet-wide assumption.

The library exposes `Delete` for both objects and orders it manifest-first, so
that an interrupted deletion leaves the unreadable state and never the readable
one. It does not implement retention, ledgers, or soft-deletion windows.

## 11. The package: `github.com/tokencanopy/rainier/checkpoint`

A new top-level package, beside `control`, `controlapp`, `v0wire`,
`attachplane`, `runnerplane` and `protocol/*` — the set `check-public-control.sh`
guards and that rainier-cloud may import. This change adds `checkpoint` to that
script's import-hygiene loop, so the package is held to the same table as the
others from its first commit: no `internal/` path, no SQL, no Docker, no cloud
SDK, no provider-named package, no HTTP.

It belongs there rather than in `internal/` because rainier-cloud is the only
caller that matters for the format. `cell-worker` produces checkpoints,
`cell-worker` and the restore path consume them, and a placement decision
preflights them. It cannot be in `internal/`, and it must not be *duplicated*
in rainier-cloud, because a format implemented twice is a format that diverges
on the day the second copy is patched — and this one's divergence mode is
unreadable customer work.

It belongs beside `protocol/workspace` rather than inside it because they are
different contracts with different threat models. `protocol/workspace` is a wire
protocol between two live endpoints, where the archive is untrusted at both ends
and refusing it whole is the right answer. A checkpoint is a durability artifact
authenticated under the tenant's own key, where refusing it whole means losing
work. The two share rules (path containment, symlink containment, exotic entries)
and this note points at that sharing explicitly; they do not share code, because
`checkLink` and `entryPath` are unexported and widening `protocol/workspace`'s
public surface to serve a second consumer is a bigger change than the one this
note is proposing.

The name is unqualified `checkpoint` rather than `workspacecheckpoint` because
ADR §2.2 removed the only other kind: Rainier does not persist guest memory for
an authenticated session, so there is no process checkpoint for this name to be
confused with, and the doc comment says so in case that ever changes.

The surface, in full:

```go
type Context struct { Workspace, Session string; Generation uint64 }
func (Context) Validate() error
func (Context) Bytes() []byte            // the authenticated context of §5.1

type KeyRef string
type Wrapper interface { Wrap(...); Unwrap(...) }
func NewStaticKeyWrapper(ref KeyRef, key [32]byte) (*StaticKeyWrapper, error)

type BlobStore interface { PutIfAbsent(...); Open(...); Delete(...) }
func NewMemoryStore() *MemoryStore

type Source struct { FS fs.FS; AlsoExclude []string }  // unioned with the defaults
func DirSource(dir string, alsoExclude ...string) Source
func DefaultExclusions() []string        // applied by every Write, §3.2

type Writer struct{ … }
func NewWriter(store BlobStore, keys Wrapper, opts WriterOptions) (*Writer, error)
func (*Writer) Write(ctx context.Context, c Context, src Source) (Result, error)

type Reader struct{ … }
func NewReader(store BlobStore, keys Wrapper, opts ReaderOptions) (*Reader, error)
func (*Reader) Preflight(ctx context.Context, c Context) (Preflight, error)
func (*Reader) Verify(ctx context.Context, c Context) (Report, error)
func (*Reader) Restore(ctx context.Context, c Context, target string) (Report, error)
func (*Reader) Delete(ctx context.Context, c Context) error

type Manifest struct{ … }
func ParseManifest(b []byte) (Manifest, error)
func (Manifest) Encode() ([]byte, error)
func (Manifest) Summary() Summary         // the content-free operational view
```

`Reader.Delete` removes both objects, manifest first, so that an interrupted
deletion leaves the unreadable state and never the readable one (§10). It is
idempotent: a missing manifest is success, because a deletion ledger that cannot
report "already gone" as done never converges.

Storage keys are derived from the prefix and the context, so a restoring caller
needs only the two things a control plane already holds and never plumbs an
object name through its own tables.

Errors are typed sentinels and flat sentences: `ErrAuth` (one error for wrong
key, wrong context and tampered bytes, as `seal.go` does and for the same
reason — the distinction is not actionable and the detail is not safe),
`ErrManifest`, `ErrFormatVersion`, `ErrContextMismatch`, `ErrTruncated`,
`ErrTrailingData`, `ErrExists`, `ErrNotFound`, `ErrNoAuthorization`,
`ErrNotAuthorized`, `ErrKeyUnavailable`, `ErrTargetNotEmpty`, `ErrRestore`,
`ErrSource`, `ErrEntry`, `ErrMismatch`, `ErrTooLarge`, `ErrInvalid`.

Two of those distinctions are load-bearing rather than cosmetic.
`ErrKeyUnavailable` is *not* folded into `ErrAuth`, because "this key version is
not replicated here yet" and "somebody tampered with this checkpoint" have
opposite operator actions, and a key service's transport failure must not be
reportable as tampering. And a `Wrapper`'s error is passed through rather than
flattened, for the same reason — a throttled KMS is not a corrupted checkpoint.

**No error carries a path, a file name, a symlink target, a file byte, a key, a
wrapped key, or a nonce.** §4.2 counts paths as content and §15.1 forbids
logging them, and these errors travel from `sessiond` through `runnerd` to a
session's error column. A source refusal therefore names the entry's *ordinal*
and its kind — "entry 41 is a hard link" — which is enough to find with a local
walk of the same tree and is not a path. This is a real cost, honestly a bad
one for debugging, and §14 lists the alternative (a separate content-scoped
diagnostic channel) as open rather than pretending the trade is free.

## 12. What this library does not do

- **Scheduling, retention, checkpoint-age policy, freshness views.** It records
  an authenticated `created_at` and returns reports; PRD §10's maximum
  checkpoint age and the operational view that flags workspaces outside it are
  the cloud's.
- **Upload retry, backoff, concurrency limits, rate limiting.** One attempt per
  call, a typed error, and an attempt-scoped content key so a retry is safe. A
  call *can* be abandoned: cancelling the context stops a write or a restore
  inside its copy loop rather than at the next object boundary, and returns
  `context.Canceled` rather than one of this package's sentinels, because
  nothing is wrong with the tree or the checkpoint.
- **Storage backends.** One in-memory store, for tests and for the `MemoryStore`
  a reviewer can read in a minute. GCS, S3 and anything else are the host's,
  behind `PutIfAbsent`.
- **Key services.** One static-key wrapper. KMS, rotation policy, per-workspace
  KEK hierarchies and region replication are the host's, behind `Wrapper`.
- **Authorization policy.** A required hook, never a decision.
- **Driver integration.** `internal/driver/` is untouched in this change;
  `Suspend(warm=false)` calling `Writer.Write` behind the §6 barrier is a later
  PR, deliberately, because a driver is being changed in parallel.
- **Incremental checkpoints, deduplication, compression, provider snapshots,
  live migration, disk formats.**
- **Anything about the agent home**, beyond being unable to reach it.

## 13. Test plan

Round trip over a tree with nested directories, an empty directory, an empty
file, a file with the exec bit, a symlink, a large-enough file to cross several
frames, and a Unicode name; equality checked by restoring, then *re-checkpointing
the restored tree* and comparing tree digests and plaintext lengths, which pins
names, modes, sizes, contents and file modification times in one assertion.

Tamper on every manifest field and on every frame. Eleven of the manifest's
twenty-one authenticated fields are refused by a cheaper, earlier check than the
tag — the identity comparison, the enumerations, the length arithmetic, the
wrapper's key-reference check — so a tamper test for those proves nothing about
whether the tag binds them; a separate test changes each field in turn through
reflection and requires the authenticated input to change with it, which is what
actually rules out a field bound to a constant or to its neighbour's value. A
second reflection test requires every JSON tag to appear in that input at all,
so a field added and forgotten cannot become unauthenticated metadata.

Context swap in all three components, both through the public path (copying both
objects into another session's prefix, then rewriting the identity fields to
defeat the cheap check) and past it at each cryptographic layer separately — the
key wrap, the manifest tag, the frame AAD — so the test is not a string
comparison. Truncation at and inside a frame boundary, trailing data, a swapped
frame pair, a zeroed object, a missing content object.

Restore-path structure: the chained-symlink escape, an entry whose parent is a
link or a file or absent, an out-of-order entry, a write-protected directory
round trip that also asserts the restored modes, a sparse record, an entry
claiming more bytes than the manifest allows, a non-empty target, a relative
target. Every one of them asserted in verify mode as well as restore mode.

Strict manifest parsing: unknown field, missing field, unknown version and an
unknown version *carrying unknown fields*, out-of-range frame size and count,
self-inconsistent lengths, content key outside the prefix, oversized manifest, a
key reference carrying a newline or an ANSI escape.

Exclusion: a planted credential-shaped file inside an excluded subtree, asserted
against *every path the walk opened* rather than against the restored tree —
once through a source given NO exclusion argument at all (the default set is
applied by construction), and once through a source that adds one of its own
(the caller's path and the defaults are both pruned, a union and not a swap).
Put-if-absent: a second write at the same generation loses with `ErrExists` and
does not disturb the winner. `Authorize` nil at construction, `Authorize`
failing before any content object is opened and before the target exists.
Cancellation on both a write and a restore. Quiesce violation in both
directions. Setuid dropped. Frame-writer boundaries at, one below and one above
an exact frame multiple, and a single write spanning many frames. Errors that
had a path available to leak — a refused source entry, a destination that cannot
hold the tree — asserted against the fixture's own root rather than a guessed
prefix. Bounded memory in both of §9's senses, and on the read side against a
streaming store. A fuzz target for the manifest parser: never
panic, never return an error outside the package's vocabulary, and anything
accepted must round-trip. A golden manifest, produced with an injected
deterministic random source and clock, so the on-the-wire shape cannot drift
without a diff.

## 14. Open questions

Answered above and not repeated: the format choice (§4.2), compression's absence
(§4.6), resumability (§4.7), the three bindings (§5.3), why the caller supplies
the context (§8), why errors carry no paths (§11).

Genuinely open:

- **Compression.** Worth measuring on real dogfood trees before deciding. The
  decision rule should be dormant-storage cost plus deep-dormant restore latency
  against the decompression-bomb bound, not compression ratio alone.
- **Native agent resume state that lives in the agent home** (§3.1, ADR §9).
  Until that is decided, a cold resume preserves exactly the resume state that
  is a file under the workspace root, and the product statement in ADR §2.6
  ("native agent resume state survives") is true only to that extent. This one
  needs an answer before the deep-dormant tier ships, not after.
- **A diagnostic channel for source refusals** (§11). An entry ordinal is a poor
  substitute for a path. The shape would be a content-scoped diagnostic that
  stays inside the session ACL, which is a tenancy design and not a format one.
- **Whether `Verify` should sample rather than read.** For a 40 GiB checkpoint,
  a full read after every cold suspend is real cost, and the barrier's
  requirement is "verified", not "verified by reading everything". A frame-level
  sampling mode is expressible (frames are independently authenticated, which is
  half of why they exist) but it weakens the meaning of the word, and that is a
  policy call that should be made with measured cost in hand.
- **Maximum checkpoint size**, and what the library should do at it. Today the
  bound is `uint32` frames and the store's own object-size limit (GCS: 5 TiB),
  which is a bound and not a policy.
- **Whether the self-hosted `controld` vault should eventually seal through this
  package's construction** rather than beside it (§5.2). It would remove the
  duplicate; it would also touch the credential path, which is not this change.

# Controller ownership: third-review fixes

A third independent review of the pushed head of `feat/controller-ownership`
found three should-fix defects and three nits. This is what each one is, what
it costs, and what closes it. It is a follow-up to
[`2026-09-10-conditional-controller-ownership.md`](2026-09-10-conditional-controller-ownership.md),
which remains the design; nothing here changes the rule, the wire, or the
compatibility matrix.

## Problem

Every one of the six was verified against the branch before anything was
written. All six are real; none is refuted.

### 1. `NextControllerGeneration` vacates the lease on one adapter only

`internal/controld/memstore.go` clears `ControllerHolder` and
`ControllerLeaseExpiresAt` when it advances the counter. `pgstore` advances
`controller_generation` and nothing else. `control/ports.go` does not say
which is right, and `controlapp/repotest` exercises the primitive (S10) and
the lease (S13) but never together — so both adapters pass the conformance
suite with opposite behaviour.

The adapter that ships to Cloud is the one that leaves the lease. The only
path still using this primitive is a legacy (unnegotiated) attach — the "old
client + new plane" row of the compatibility matrix. On Postgres that attach
advances the generation and leaves the **displaced** controller's holder and
future expiry on the row. For up to `ControllerLeaseTTL` (30s) the session
view reports `controller.held: true` for a holder with no authority, and
`AttachmentService.grant` sees `Live() == true && expected != gen` and admits
the next negotiated controller attach as a **viewer**. On memstore the same
attach gets control.

This is the same divergence class as the branch's own I15 fix, which caught it
for `CreateSession` and missed it here.

### 2. A claim waiting for its sandbox acknowledgement is not protected

`ownership.set` is an unconditional write, and `claim` calls it *after*
`installAndWait`, which blocks for up to `ackTimeout`. `Plane.displace` reads
a peer with `other.get()` and writes it with `other.set()` outside one
critical section.

Interleaving: A claims (CAS 1→2) and blocks in `installAndWait`. B claims from
2 (CAS →3). B's `displace` reads A as still `(view, 1)`, takes the viewer
branch, and sets A to `(view, 3)`. A's wait then returns and A unconditionally
overwrites itself to `(control, 2)` and sends itself `attached mode=control
gen=2`. A is not corrected until its next heartbeat (≤5s). During that window
A's client prints `[you have control]`, the plane forwards and stamps A's
keystrokes as gen 2, and the pty discards every one of them.

The pty fence holds — nothing double-executes — but "at most one controller at
any moment" is violated at the plane and on the user's screen.

### 3. A negotiated `--view` attach is authorized as a controller

`authorizedAs` returns `AttachmentController` for *any* negotiated attach,
including `mode=view`, and `AttachTerminal` hands that to
`AuthorizeAttachment`. A host policy that grants view but not control — the
Cloud collaboration policy this seam exists for — therefore refuses
`rainier attach --view` outright.

Separately, `internal/controld/attach.go` pre-checks with the mode the client
asked for while the service checks controller, so that function's stated
invariant ("the same `ownerOrAdmin` policy adapter, asked the same question")
no longer holds. Under such a policy the client would get a 101 upgrade and
then a policy-violation close instead of a clean 403. There is no in-tree
failure only because `ownerOrAdmin.AuthorizeAttachment` ignores the mode.

The escalation this replaced (I2: a mid-attach `claim` reaching control the
attach was never authorized for) is real and must stay closed.

### 4, 5, 6 — the nits

- A negotiated controller attach that dies at the pairing stage has already
  displaced the incumbent: `grant` claims before `broker.Attach`, and the
  plane displaces before `dial_attach` is even sent. Inherent to the binding
  riding `dial_attach`, and recoverable in one Ctrl-\ .
- Two dead fields: `internal/attachio.ownership.legacy` is written and never
  read, and `fakePlane.speaks` is never consulted, so the flag cannot fail.
- Nothing bounds claim frequency. A client that already has control can loop
  `claim`/`release` on its own stream, each claim advancing the generation and
  serially waiting up to `ackTimeout` per peer.

## Scope

In scope: the three should-fix defects with regression tests that fail without
their fix; the two dead fields; two paragraphs of documentation for the two
behaviours that are being documented rather than changed; and the Cloud draft
re-pinned to the new core head, with the regional store brought to the
corrected contract.

Out of scope: any change to the rule, the wire, the message set, the
compatibility matrix, or the lease parameters. No rate limiter — see
alternatives.

## Approach

### 1. One contract for `NextControllerGeneration`

The port grows one sentence: it advances the generation **and vacates the
lease**, exactly as `CompareAndAdvanceControllerGeneration` does. That is the
answer memstore already gives, it is what the primitive means (a take-over
displaces the holder), and it is what makes the legacy-attach row of the
matrix behave the same on both adapters.

`pgstore` adds `controller_holder = '', controller_lease_expires_at = NULL` to
its `UPDATE`. `controlapp`'s test fake does the same. `repotest` S13 gains a
section that establishes a live lease and then calls
`NextControllerGeneration` over it, asserting the row comes back vacant — the
case that would have caught the divergence.

Cloud's regional store (`internal/cell/store`) gets the same statement change
and runs the extended suite.

### 2. Monotonic ownership transitions

`ownership.set` is removed. Three named transitions replace it, each doing its
read and its write under one hold of `o.mu`:

- `advance(mode, gen) bool` — refuses when `gen < o.gen`. `claim` uses it, and
  when it is refused the claim answers `stale` with the generation that
  actually exists instead of `attached: control`. The claim's own
  `install` left a control binding in the sandbox, so the refused path
  re-installs the viewer binding the peer's displacement gave this attach;
  the pty fence already made the stale binding inert, and this makes the
  sandbox's copy agree with the plane's.
- `demoteTo(gen) uint64` — always view, generation `max(o.gen, gen)`. Losing
  control is never backwards: being wrong about the number is survivable,
  believing you still have control is not.
- `displaceTo(gen) (was string, moved bool)` — the `otherGen >= gen` check and
  the write, together, under the peer's own lock. `Plane.displace` calls it
  and branches on what it returns.

### 3. Authorize at the mode the attach opens in

`authorizedAs` is deleted; `AttachTerminal` authorizes `cmd.Mode`. The edge
pre-check and the service ask the same question again.

The controller check moves to the claim path. `controllerKeeper` carries the
policy, the scope and the authoritative resource, and `Claim` asks
`AuthorizeAttachment(..., AttachmentController)` before it touches the store.
A live check, not a cached one: a Cloud grant revoked mid-attach is honoured
at the next claim.

That makes `AttachTarget.Controller != nil` the wrong carrier for "this client
negotiated", because a view-only principal must still be *told* things while
not being allowed to claim. Two explicit facts go on the target instead:

- `Negotiated bool` — this client understands the ownership messages.
- `MayClaim bool` — this client may take control mid-attach.

The plane reads both. A claim from an attach with `MayClaim` false is answered
`stale` and never reaches the store.

### 2a. One place that says what this attach is

Review found `advance` closes only the FIRST of two waits. A claim also has to
displace every peer before it answers, and that loop waits on each displaced
peer's sandbox, once per peer and up to `ackTimeout` each; a second claim can
win inside it. The attach path does the same thing — the broker reads the
granted mode, displaces, and sends the value it read.

That version of the lie is worse, because nothing corrects it: a client that
believes it is the controller sends no claim, so its take-control key does
nothing, and its heartbeat renews nothing because the plane knows it is a
viewer, so no stale renewal ever demotes it.

Every message that tells a client what it is therefore goes out under one
`announce` hold that also does the state read — `announceClaim`,
`announceOpening`, `announceViewer`, `announceStale`. A claim asks one
question once, at the end: does this attach still hold the generation it won?
That answers both waits. `announce` is only ever taken alone (a claim finishes
displacing its peers before announcing anything about itself, and a
displacement takes the peer's `announce` and no other), so two attaches cannot
each hold one and wait for the other's.

Two more read-then-write pairs went with it: `finish` read the mode and the
generation separately, so this attach's own heartbeat demotion could complete
between them and the release would advance past the generation the NEW
controller holds; and `sendStale` fell back to generation zero, which is a
generation no row is ever at, stranding a client whose take-control key is
then refused for the life of the attach.

### 4, 5, 6

- The pairing-stage displacement is documented in
  `docs/terminal-controller-ownership.md` under operating notes, with its
  remedy.
- `legacy` and its now-empty `settleLegacy` are removed — `settled == false`
  is already the whole of the legacy state, and every behaviour the field
  pretended to record is pinned by `TestNewClientOldPlane`. `fakePlane.speaks`
  is removed and its call sites updated.
- Claim frequency gets a line in the operator doc.

## Alternatives

- **Leave `NextControllerGeneration` alone and change memstore instead.**
  Rejected: a take-over that leaves the displaced holder's lease on the row is
  the bug, not the fix. It reports `controller.held` for a holder with no
  authority and demotes the next negotiated attach for 30 seconds.
- **Make `set` monotonic in place rather than naming three transitions.** A
  single monotonic `set` cannot express the demotion whose generation read
  failed (which must move even at an equal or stale number) without a special
  case that reads as an exception to its own rule. Three named transitions
  each say what they are.
- **Have the refused claim re-read and retry.** Rejected: a client that
  re-claimed whenever it was refused is two devices fighting over a keyboard.
  `stale` carries the generation to claim from; the user decides.
- **Cache the controller authorization on the target and skip the live check.**
  Rejected on its own; kept *alongside* the live check. The cached
  `MayClaim` lets the plane answer a view-only principal without a round trip;
  the live check in `Claim` is the authority.
- **Rate-limit claims (finding 6).** Rejected for this change. It is an
  authorized user's nuisance against their own session, every claim is already
  bounded by one `ackTimeout` per peer serially, and a limiter has its own
  failure mode — a legitimate rapid hand-back after a mis-press answered
  "too fast". Documented instead.

## Edge cases

- A claim whose `advance` is refused must still leave the *store* consistent.
  It does: the CAS already committed, so the generation it won is real and
  somebody has since advanced past it. The client is told the current
  generation; the lease it briefly held is vacated by the winner's own CAS.
- `demote` at a generation the store read could not supply falls back to the
  generation this attach holds. `demoteTo` takes the max, so it can never move
  the number down while still moving the mode.
- `displaceTo` refusing (`o.gen >= gen`) must not send anything: the peer is
  already at or past that generation and a `control_changed` naming an older
  number would walk its client backwards.
- A negotiated view-only attach still gets a keeper, because it needs `State`
  for its own generation reads and `finish` is a no-op for a non-controller.
- An unnegotiated attach is unaffected by all three: it carries no keeper, is
  authorized as the controller it can only be, and never claims.
- `NextControllerGeneration` on a row with no lease is unchanged — clearing an
  already-vacant lease is a no-op, and the returned generation is the same.
- Cloud's regional store must change with the port or the extended conformance
  case fails there. It is in this task.

## Verification

- `controlapp/repotest` S13 extended, run on **memstore and pgstore**, zero
  skips: `NextControllerGeneration` over a live lease leaves the row vacant.
  Fails on `pgstore` without the statement change.
- `attachplane`: two viewers, a non-acking sandbox and a short
  `ControlAckTimeout`; A claims, B claims from A's generation while A is still
  waiting. A must never be told `attached mode=control`. Fails without
  `advance`.
- `attachplane`: the same, with the second claim landing while the first is
  inside its displace loop, on the claim path and on the attach path. Fails
  without the `announce` hold.
- `attachplane`: a disconnect raced against this attach's own heartbeat
  demotion 500 times — no release at a generation it never held, and the live
  controller still holding its lease. Fails without `finish`'s single read.
- `attachplane`: a refused claim whose generation read fails is answered the
  generation this attach holds, never zero.
- `attachplane`: one test per transition — `advance` at the generation already
  held, `demoteTo`'s max, `displaceTo`'s refusal and its atomicity over 2000
  rounds, and the refused claim's re-install. Each was a mutation that
  survived the whole tree before it.
- `attachplane`: a claim from an attach with `MayClaim` false is answered
  `stale` and the keeper is never called. Fails without the flag.
- `controlapp`: under a policy that permits viewer and denies controller, a
  negotiated `mode=view` attach **succeeds** and its target carries
  `MayClaim: false`; the keeper's `Claim` returns `ErrDenied`. The first half
  fails without the `authorizedAs` removal, the second without the keeper
  check.
- `controlapp`: a negotiated `mode=control` attach under the same policy is
  still refused — I2 stays closed.
- Full gates: `make verify`; `go test ./internal/e2e/ -race`;
  `go test -race -count=10` on the repotest race case and on `attachplane`;
  and on Cloud, `make verify` and `make canary` with a real database.

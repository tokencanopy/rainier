# Controller ownership: fifth-review fixes

A fifth independent review of `feat/controller-ownership` ran a 22-mutant
battery against the PR's own suite (20 died) and twelve stress scenarios
through the real plane under `-race`, and concluded that the module has
converged: the state machine and every fourth-round fix hold, the one
surviving semantic mutant is a coverage gap rather than a live bug, and the
single defect it found is not a new path but **one residual instance** of the
class the fourth round named.

This is a follow-up to
[`2026-09-10-conditional-controller-ownership.md`](2026-09-10-conditional-controller-ownership.md),
which remains the design, and to
[`2026-09-10-controller-ownership-review-fixes.md`](2026-09-10-controller-ownership-review-fixes.md)
and
[`2026-09-11-controller-ownership-liveness.md`](2026-09-11-controller-ownership-liveness.md).
Nothing here changes the wire, the message set, or the compatibility matrix.
One observable client behaviour changes, deliberately, and is written down in
[Scope](#scope): under `--view` the take-control key stops sending a claim.

Two further reviews of *this* change — one independent, one adversarial —
found four defects it had introduced and several documentation sentences it
had falsified. Their dispositions are in
[What the reviews of this round found](#what-the-reviews-of-this-round-found);
everything below already describes the fixed version.

## Problem

### 1. A displaced controller can be skipped and never told, while the taker *is* told it has control

`Plane.displace` decides whether to announce by asking whether the peer's
state MOVED:

```go
was, moved := other.displaceTo(gen)
if !moved {
    continue
}
```

`moved == false` means the peer's state is already at or past `gen`. The code
reads that as "already at gen, therefore already told". It is not. Three paths
move an attach's own state to a new generation and then spend real time before
announcing it:

- `demote` (`ownership.go`) runs `demoteTo` and then `installAndWait`, which
  is bounded by one `ControlAckTimeout` and spends all of it against a sandbox
  that never acknowledges — an older `sessiond`, which this PR deliberately
  supports, or merely a slow one;
- `sendStale` runs `displaceTo` and then `announceAs`, which can wait out this
  stream's whole write budget on a client that has stopped reading.

A take-over landing in that gap skips the peer with `continue`, `wg.Wait()`
returns at once, and the taker is answered. The reviewer's probe `S10`
reproduces it deterministically (6/6 runs):

1. B claims: `CompareAndAdvance` 1→2, B holds the lease.
2. B waits on its own sandbox's `control_ack`.
3. A's heartbeat renews generation 1, is refused `ErrStale`, and calls
   `demote(ctx, 1)`: it reads 2 from the store, `demoteTo(1, 2)` moves **A's
   own state** to `view@2`, and A then blocks inside `installAndWait(view, 2)`
   for the whole acknowledgement timeout because A's sandbox does not answer.
4. B's ack lands, B advances, `displace` reaches A, `displaceTo(2)` refuses
   because A is already at 2, and A is skipped entirely.
5. B is told `attached control 2`. **A has been told nothing.**
6. A's `installAndWait` times out and A is finally told `control_changed view
   2` — measured 1.70 s later at the production default `ControlAckTimeout=2s`.

The pty fence holds throughout (A's binding is `control@1`, the session's
`controllerGen` is 2), so nothing double-executes, and A self-heals, so the
damage is bounded. What breaks is the invariant the feature exists for, and
the module's own comment says why that is not cosmetic: *a client that
believes it is the controller sends no claim, and this attach's heartbeat
renews nothing*. For 1.7 s two screens both say "you have control".

This is the fourth round's defect class arriving through the one door the
fourth round did not close. `announceAs` was made state-reading so that no
*announcement* can assert a stale mode; the decision *whether to announce at
all* was left asserting one. The premise is written down in
[`2026-09-10-controller-ownership-review-fixes.md`](2026-09-10-controller-ownership-review-fixes.md):
*"`displaceTo` refusing must not send anything… a `control_changed` naming an
older number would walk its client backwards."* That was true while the notice
carried a caller-asserted number. It stopped being true the moment `announceAs`
began reading the number under the hold — the fourth round's own fix
invalidated the sentence, and nobody went back for it.

### 2. A claim that gives its generation back tells nobody

When `installAndWait` fails, a claim gives the generation back
(`keeper.Release`) rather than becoming a phantom controller. The store then
sits at generation 3 with a **vacant** holder, while the attach that actually
held control is still `control@1` in the plane — still forwarded for, until
its next heartbeat, up to one heartbeat interval later. Every viewer's next
press is refused once too, because nobody was told the new number: the exact
case `displace`'s own documentation says the viewer notice exists for.

### 3. The ownership vocabulary is filtered client→sandbox but not sandbox→client

The client pump drops `control` and `control_ack` explicitly, with a comment
saying why: they are the plane's verbs, and a client's copy of one is a client
asking the sandbox to install a binding nobody granted. The runner pump
consumes `control_ack` and forwards everything else verbatim, so a sandbox's
`{"type":"attached","mode":"control","gen":"99"}` reaches the client outside
`announceAs`. A client told that prints `[you have control]`, stops sending
claims — `internal/attachio`'s `claim()` returns nothing when it believes it
has control — and types into a plane that drops every frame. The only way out
is detaching.

Only a buggy or compromised `sessiond` produces such a frame, and that sandbox
could execute the keystrokes itself, so this is hardening rather than a
vulnerability. It is also one line, and it makes the two pumps say the same
thing about the same vocabulary.

### 4. `--view` is documented as never claiming, and Ctrl-\ still claims

`rainier attach --view` is documented as "watch without ever claiming
control". The attach is genuinely admitted a viewer and never types, but
`internal/attachio`'s `ownership.claim()` never consults `askedView`, so
Ctrl-\ sends a claim — and the service sets `MayClaim` for a view-mode attach
whose principal may drive, so the plane honours it. The flag and the help
disagree.

### 5. The client write budget is a whole-write deadline with an unstated assumption

`wsTerminalStream.Send` gives one message 60 s. The fourth round's concern —
that a courtesy notice under a 2 s caller deadline could close a healthy
client — is correctly handled and pinned by a test, and this round confirmed
it empirically: `context.WithTimeout(ctx, 60s)` under a 2 s parent returns a
child with no timer of its own, so `wctx.Err() != nil && ctx.Err() == nil`
cannot be true on the caller-deadline path.

What remains is that 60 s is a WHOLE-write deadline while `attachReadLimit` is
16 MiB, so the largest frame this stream can carry requires ≈273 KB/s
sustained or a client that is making steady progress is `CloseNow`n. A 2 Mbit/s
link is under that.

### 6. Nothing pins the announce hold under contention

Mutant M10′ — read `(mode, gen)` *before* acquiring the announce hold and
report that value — survives the entire PR suite. That is the literal shape of
the defect the hold exists to prevent. It only shows under contention on one
attach's `announce`, which no test creates deliberately.

### 7. `clientWriteTimeout` is a mutable package var three tests write

Safe today — nothing in the package calls `t.Parallel()` — and a race waiting
for the first test that does.

## Scope

In:

- `attachplane/ownership.go`: `displace` announces to every peer, and a claim
  that gives its generation back fans that generation out.
- `attachplane/splice.go`: the runner pump drops the three ownership messages
  the plane owns.
- `attachplane/stream.go`: the write budget scales with the payload, and the
  two knobs behind it become per-stream fields.
- `internal/attachio/ownership.go`: `claim()` refuses under `--view`.
- Tests for each, in the shapes the review's probes name.

Out:

- Any change to the wire, the message set, the store, or the compatibility
  matrix.
- `controlapp`'s `mayClaim`. It is correct as it stands: a view-mode attach
  whose principal may drive IS allowed to take control mid-attach, which is
  what a reconnecting controller admitted as a viewer depends on. `--view` is
  the user's instruction to their own client, and F4 is fixed where that
  instruction lives.

One observable behaviour changes: **under `--view`, Ctrl-\ does nothing.** It
is swallowed rather than forwarded — a `--view` attach sends no input at all,
so forwarding it would have the same effect with more moving parts — and no
line is printed, which is what the key already does on a device that has
control. The alternative was softening the help text to "attaches as a
viewer"; the flag's promise is the more useful of the two, so the code moves
to the documentation rather than the other way round.

## The fixes

### F1 — announce to every peer, and let the announcement decide what it says

```go
was, moved := other.displaceTo(gen)
wg.Add(1)
go func() {
    defer wg.Done()
    if moved && was == terminal.ModeControl {
        …install or installAndWait…
    }
    nctx, cancel := p.step(ctx)
    defer cancel()
    other.announceAs(nctx, terminal.TypeControlChanged, 0)
}()
```

`moved` keeps its one remaining job — deciding whether this peer's SANDBOX
needs a new binding — and stops deciding whether the peer's CLIENT hears
anything. The extra notice cannot walk anybody backwards, because
`announceAs` reads the mode and the generation under the announce hold and
reports what it read; a peer already at `gen` is told the number it already
has, and `attachio.observe` folds a `control_changed` that changes nothing
into no notice at all. The taker still waits for the fan-out, so the ordering
the contract promises is unchanged: every displaced peer has been told before
the taker is.

The cost is one extra client write per already-current peer per handoff, each
under its own `Plane.step` deadline, on its own goroutine.

### F2 — fan out the generation a claim gave back

`sendStale` already reads the current generation from the store and records it
on this attach; it now returns it, and the give-back path fans it out with
`displace(ctx, o, current, true)`.

It WAITS, like a take-over and unlike a release. This is the only fan-out in
the module that can demote a peer which genuinely holds control — `release`
and `finish` both demote the caller first, so their peers are already viewers
— and `displaceTo` flips that peer to `view` in the plane before its sandbox
has the matching binding. A fire-and-forget install there disarms the NEXT
taker: `displaceTo` hands it `was == view`, it skips `installAndWait`, and it
is answered while the old binding is still on the wire. The pty's own
generation fence still holds either way, so nothing executes twice; what would
not hold is the ordering this module promises.

### F3 — the runner pump drops what the plane owns

`attached`, `stale` and `control_changed` are the plane's to send, exactly as
`control` and `control_ack` are the plane's to write. The runner pump drops
them the way the client pump drops the other two.

### F4 — `claim()` refuses under `--view`

One line in the one place that decides what this client claims. It covers both
callers — the take-control key and `--take`'s single claim — so `--view`
cannot claim by any route, including the flag combination the CLI already
refuses at parse time.

What it reads is a new `Options.NeverClaim`, set only by the flag, and NOT the
existing `askedView`. `askedView` means "this attach is requesting view mode",
which is what `cmd/rainier`'s `reconnectOwnership` asks for on every reconnect
whose previous attach ended as a viewer — so a device superseded while its
network was out does not take control back on a blip. Those `Options` are
otherwise byte-identical to `--view`'s, and that device must keep its
take-control key: the plane promotes an attach only in answer to a claim, so
losing the key would pin it a viewer for the rest of the process with no way
back but detaching.

### F5 and F7 — a budget that scales, on the stream rather than in a package var

`wsTerminalStream` carries a base budget and a minimum sustained rate, and one
message's deadline is `base + wire(payload)/rate`. The rate is measured against
the bytes that reach the SOCKET rather than the payload: `Data` is a `[]byte`,
which JSON carries base64-encoded, so four wire bytes leave for every three of
payload, and budgeting the payload would quietly ask a third more throughput
than the rate promises. At the defaults (60 s, 64 KiB/s) the largest frame the
16 MiB read limit allows gets 60 s + 256 s, so a client progressing at
64 KiB/s can take a full snapshot; a client that has taken NOTHING is still
closed, which is the property the budget is for. Both knobs are fields set by
`ClientStream`, so a test constructs a stream with its own and nothing shared
is mutated.

The trade is explicit: a wedged client carrying the largest frame is held for
about five minutes rather than one, and a new attach's first byte can wait one
`Plane.step` behind a wedged peer's courtesy notice whether or not that peer
moved. Each costs one goroutine, one file descriptor and one table entry;
being disconnected mid-scrollback costs a person their session.

### F6 — a test that holds the announce hold while the state moves under it

Not a code change: the coverage gap is closed with a test that takes one
attach's announce hold, queues a second announcement behind it, moves the
attach's state, and then releases — so the queued announcement must report the
state as of when it acquired the hold, not as of when it was called.

## What the reviews of this round found

Two Opus reviews of this change — one independent, one adversarial, both with
a mutation battery — found four defects it had introduced. All four are fixed
above, each with a regression test that fails without its fix.

| # | Finding | Disposition |
|---|---|---|
| 1 | **HIGH** — F4 read `askedView`, which `reconnectOwnership` also sets, so a plain attach that came back a viewer after a disconnect lost Ctrl-\ for the rest of the process, silently and with no way back | **Fixed.** `Options.NeverClaim` is a separate fact from the mode being requested. `TestAReconnectedViewerKeepsItsTakeControlKey` |
| 2 | **HIGH** — F2's give-back fan-out did not wait, so a peer was flipped to `view` in the plane with its binding still in flight, and the next taker skipped its own `installAndWait` | **Fixed.** `wait=true`, and the test asserts the binding has landed when the claim returns rather than polling for it. `TestAClaimThatGivesItsGenerationBackTellsThePeersToo` |
| 3 | **MEDIUM-HIGH** — F1's unconditional announce reached attaches that are registered but have not been told what they are yet (up to `attachFirstMsgTimeout`), making a courtesy notice their OPENING answer: two `[another device took control]` lines for a plain viewer, and one for a `--view` attach that should hear nothing | **Fixed.** A peer whose state did not move AND which has been told nothing is left to its own opening answer, which says the same thing. A peer that moved is still told. `TestAPeerThatHasNotBeenToldWhatItIsIsNotToldAboutSomebodyElseFirst` |
| 4 | **MEDIUM** — the Send-level budget test passed with the scaling reverted, because `json.Marshal` of a 2 MiB payload under `-race` costs most of a second — vacuous at exactly the `-race -count=30` the gates run | **Fixed.** A 384-byte payload at 1 KiB/s buys the same half second with negligible marshalling, and the test now has an upper bound as well as a lower one. |
| 5 | **MEDIUM-LOW** — the budget scaled on the payload, not the base64 the socket actually carries: ≈8% more throughput demanded than promised | **Fixed.** `wireSize` rounds the payload up to its base64 length. |
| 6 | **MEDIUM/LOW (docs)** — `docs/cli-v0-contract.md` said Ctrl-\ still works under `--view`; `docs/terminal-controller-ownership.md` said the flag's key is answered `stale`; the help text did not mention the key at all; two prior design-doc sentences the change falsifies were left standing | **Fixed.** All four updated, the two prior sentences annotated in place with what superseded them. |
| 7 | **LOW** — the F1 test could pass on the wrong producer if a run stalled past `ControlAckTimeout`, and a comment attributed generation zero to `finish`, which never passes it | **Fixed.** Five seconds of margin plus an explicit guard that fails the run rather than concluding from it; the attribution corrected here and in the test. |
| 8 | **LOW** — `budget`'s largest-frame assertion was a tautology (`P/(base+P/rate) < rate` for any base) | **Fixed.** It now checks the wire rate against the promised floor with the base subtracted. |

Findings deliberately NOT taken:

- **A wedged client is now held ~5 minutes rather than 1 for the largest
  frame, and a new attach's first byte can wait one `Plane.step` behind a
  wedged peer.** Accepted and written into the comment: both are the price of
  not disconnecting a client that is making steady progress, and both cost
  resources rather than correctness.
- **A `control_changed` can still precede a peer's opening `attached` when
  that peer's state DID move, and the client then prints its notice twice.**
  Pre-existing (`TestAnAttachStillReadingItsFirstMessageIsDisplacedLikeAnyOther`
  pins the plane's half), unchanged by this round, and the notice is true
  there — that attach really was granted control and really lost it. Folding
  the duplicate is a change to client notice policy and belongs with its own
  design note.
- **The give-back tells the previous controller `control_changed view` while
  the lease is vacant**, which reads as "another device took control" when
  nobody did. It is the closest word the vocabulary has and the state it
  reports is correct; a new message type for "nobody has it" is a wire change.

## Alternatives considered

- **Keep the `continue` and have `demote`/`sendStale` announce sooner.**
  Rejected: it makes correctness depend on how long two other functions take,
  which is the shape of bug this is. Announcing unconditionally is
  unconditionally right because the announcement reads state.
- **Have `displaceTo` report "already at this generation" separately from
  "already told".** Rejected: that is a fourth field of state to keep
  consistent, to answer a question that does not need asking once the notice
  reads what it says.
- **Suppress the extra notice when the peer is already at `gen` AND in view
  mode.** Rejected for the same reason as the first: it re-introduces the
  assertion, in a narrower form, for a saving of one small frame.
- **Make F5 a progress deadline rather than a size-scaled one.** Rejected:
  `coder/websocket` takes one context for the whole write, so a progress
  deadline means chunking the frame ourselves and re-deriving the framing the
  library owns. Scaling by payload is the same guarantee expressed in the
  units the caller has.
- **Fix F4 by softening the help text.** Rejected; see [Scope](#scope).
- **Fix F4 in `controlapp.mayClaim`.** Rejected: it would also refuse a
  reconnecting controller that was admitted as a viewer, which is a legitimate
  take-over.

## Edge cases

- A peer that is already at `gen` and in `control` mode — an attach that won
  its own claim inside this fan-out. `moved` is false, so no viewer binding is
  installed, and `announceAs` reads `control` and tells it so. That is the
  fourth round's `TestADisplacementNoticeReportsTheModeItReads`, now also
  reached on the `!moved` path.
- A peer whose client has stopped reading. The extra notice is under
  `Plane.step`, so it expires in one acknowledgement timeout and the taker is
  not held. `announceAs` gives up on a contended hold the same way.
- A peer at generation 0 — what a composer that never set
  `ControllerGeneration` hands `broker.Attach`, and what `release` falls back
  to when the store read fails and the attach is itself at zero. (`finish`
  displaces only when its read succeeded, so it never passes zero.) Every
  attach is already at or past 0, so `moved` is false for all of them; each
  peer that has already been told what it is gets one `control_changed` naming
  the generation it already holds, which `attachio.observe` folds to nothing.
  No peer is demoted and no binding is written.
- The give-back fan-out runs after `sendStale` has already answered the
  claimer, so the claimer is never in its own peer list and never sees its own
  notice.
- A sandbox that sends `attached`/`stale`/`control_changed` is now silently
  dropped rather than ending the attach. Ending it would give a buggy sandbox
  a way to disconnect every client; dropping it leaves the client with what
  the plane says, which is the only authority for those three.
- `--view` with `--take` is refused at parse time, so the `claim()` guard is
  belt and braces there; it is the whole fix for Ctrl-\.
- A plain attach reconnecting as a viewer asks for view mode and is NOT
  `--view`. It keeps its take-control key; see F4.
- A peer that has been registered but not yet told what it is. Its opening
  answer is still coming and reads the same state, so a courtesy notice that
  arrived first would only change which sentence its client printed. It is
  skipped when its state did not move, and told when it did.
- A zero-payload message under the scaled budget gets exactly the base, which
  is what every ownership message and every `control_ack` is.

## Verification

- `attachplane`, S10 shape: B claims and waits on a gated sandbox ack; A's
  heartbeat is refused and A blocks in `installAndWait` on a non-acking
  sandbox; the ack is released. A must have been told `control_changed`
  BEFORE B's claim is answered. Fails on `c6d16cb`.
- `attachplane`, S12 shape: a claim whose binding never lands gives the
  generation back, and the previous controller is demoted and told, rather
  than waiting for its own heartbeat.
- `attachplane`, S12b shape: a sandbox that sends `attached control 99` never
  reaches the client, and the splice survives it.
- `attachplane`: the announce hold under contention — the M10′ mutant dies.
- `attachplane/stream`: a large payload gets proportionally more budget than a
  small one — measured through `Send`, with a payload small enough that
  marshalling is not what the clock reads — and a client that has taken
  nothing is still closed.
- `internal/attachio`: `--view` + Ctrl-\ sends no claim, and the attach ends a
  viewer; the same `Options` WITHOUT the flag — which is what a reconnect
  builds — claims and takes control.
- Gates: `make verify`; `controlapp/repotest` on memstore and pgstore with
  zero skips; `go test ./internal/e2e/ -race`; `go test ./attachplane/ -race
  -count=20`; the new tests at `-race -count=30`.

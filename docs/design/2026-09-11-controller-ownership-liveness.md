# Controller ownership: fourth-review fixes

A fourth independent review of `feat/controller-ownership` concluded that the
state machine is correct — the store CAS, the single-step `advance` and
`displaceTo`, the `handoff` serialisation and the pty fence hold under every
interleaving it could construct — and that what remains is the layer around
it: one liveness bug, one recurrence of a defect class the third round fixed
on two of four paths, one missing guard, and one authorization split that is
right at the service and unreachable from the CLI.

This is a follow-up to
[`2026-09-10-conditional-controller-ownership.md`](2026-09-10-conditional-controller-ownership.md),
which remains the design, and to
[`2026-09-10-controller-ownership-review-fixes.md`](2026-09-10-controller-ownership-review-fixes.md).
Nothing here changes the rule, the wire, or the compatibility matrix. One
observable behaviour does change, deliberately, and is written down in
[Scope](#scope): a view-only principal's plain `rainier attach` is admitted as
a viewer instead of refused.

The instruction this round was different from the last three: **fix the class,
not the instance.** Three rounds have now found the same two shapes of defect
— a path that asserts what an attach is instead of reading it, and a
peer-facing step with no bound on it. Both are removed here by construction
rather than case by case: there is one announcement function, and there is no
unbounded peer-facing write left in the module.

## Problem

### 1. One peer that stops reading freezes every handoff on the session

`Plane.displace` walks the session's other attaches **serially** and, for
each, performs two unbounded I/O steps: `install`/`installAndWait`, which
writes to that peer's runner socket, and `announceViewer`, which writes to
that peer's client socket. Only the acknowledgement *wait* is bounded. The
contexts both callers hand down are unbounded — the splice's request context
for a mid-attach claim, the attach request's for an attach — and there is no
write deadline and no ping anywhere in the attach plane.

So one viewer that stops reading its socket (a closed lid, a TCP zero window,
a paused browser tab: no RST, so nothing closes it) is enough. A taker's
`keeper.Claim` **succeeds** — the store generation has already advanced, so
the previous controller is fenced at the pty — and then `displace` blocks
forever in that peer's `Send`. The taker is never told, so its client stays in
`view` and sends nothing, while its own heartbeat renews the lease it does not
know it holds. The session has zero working controllers until somebody
detaches.

It is worse on the attach path, where `displace` runs *before* the client
socket is parked and before the `dial_attach` is sent: one wedged viewer stops
a brand-new controller attach from ever reaching its runner.

This is the only finding of the four rounds that is a liveness bug rather than
a correctness one, and it is the scenario this PR ships for — several devices
on one session.

### 2. Two of the four announcement paths still assert a mode they did not read

`announceClaim` and `announceOpening` read `(mode, gen)` inside the `announce`
hold and report what the attach *is*. `announceViewer` and `announceStale`
read only the generation and hard-code `mode: view` and `stale`. Both can
therefore tell a live controller that it is a viewer.

It is reachable with no race at all. `claim` has no "am I already the
controller" guard, so a controller whose client claims with an `expected` it
no longer holds — any client reading `controller.generation`, which this
branch newly exposes — gets `ErrStale` from the keeper and is answered
`stale`. Its client switches to viewer and stops typing, while the plane still
forwards for it and its heartbeat keeps renewing its generation: for the whole
30s lease nobody can type, and every new negotiated attach is admitted a
viewer. One more press of the take-control key recovers it.

This is the exact defect the third round fixed on the other two paths. Fixing
it a third time, path by path, is what the repeat findings are telling us not
to do.

### 3. `demoteTo` is the one transition that cannot refuse

`advance` and `displaceTo` both refuse a backwards move. `demoteTo` raises the
generation to the max of the two but sets `mode = view` unconditionally. A
heartbeat demotion computed at generation N can spend a full acknowledgement
timeout inside `installAndWait` and then demote an attach that has since
**won** N+1 through its own claim. The comment's justification — being wrong
about the number is survivable, believing you still have control is not — is
about the *number*; the bug is the *mode*. End state: the plane says viewer,
the store says this attach holds the lease, and nobody can type for the rest
of the lease.

### 4. The authorization split is right at the service and unreachable from the CLI

`AttachTerminal` authorizes `cmd.Mode` and asks the controller question
separately in `mayClaim` — correct, and what the third round landed. But
`rainier attach` with no flags asks for `mode=control`, and both the edge's
pre-upgrade check and the service refuse it outright under a policy that
grants viewing without driving. A view-only principal — the exact case
`mayClaim` exists for — gets `not authorized to attach to this session` and
must know to type `--view`. `grant`'s own rule, that every refusal lands the
attach in `AttachmentViewer` and never in an error, is not applied to the
policy refusal.

### 5. `claim` is not idempotent, and it is client-triggered

A claim from the current controller advances the generation, re-installs,
re-displaces every peer (an acknowledgement timeout each) and fences its own
in-flight keystrokes. The first-party CLI already refuses to send one
client-side; nothing at the plane does.

### 6. Benign message reordering on the release path

`release` → `demote` → `displace(..., false)` can send `control_changed
view@G` to a peer that is mid-claim and about to be told `attached control@G`.
The final state is correct; the notice ("another device took control", then
"you have control") is not. Fixed for free by #2 once the mode is re-read at
send time.

### 7. Three test-suite soft spots

`installAndWait`'s stale-acknowledgement drain survives deletion — no test
covers it. `fakeSandbox.order` is written and never read. Two "no stdin
arrived" assertions sleep 100ms after a real synchronisation point, so a
forwarding regression would only fail probabilistically.

## Scope

**In:**

- One announcement function, `announceAs`, holding the `announce` mutex,
  reading `(mode, gen)` under it and sending a message that reports what it
  read. The four per-path variants are deleted. A `stale` that would be sent
  to a live controller is sent as `attached control@gen` instead — the truth,
  in a message every client already decodes.
- `displace` becomes a bounded fan-out: peers in parallel, every peer-facing
  step under its own acknowledgement-timeout deadline, and a peer that cannot
  be reached inside it never holds the taker. `displace` still returns only
  when the fan-out is done, so the ordering guarantee the contract makes — the
  taker is not told it has control until the displaced controllers' sandboxes
  have the new binding — is unchanged; what changes is that the bound is now
  two acknowledgement timeouts in total rather than unbounded, serially, per
  peer.
- `install` bounds its own runner write by the same timeout, so no
  peer-facing write in the module is unbounded.
- `ClientStream` gets a write deadline, so a client that has stopped reading
  is eventually closed rather than held.
- `announceClaim`'s corrective install moves out from under the `announce`
  hold.
- `demoteTo(from, to)` no-ops when the generation has moved past `from`, and
  `demote` installs the viewer binding only when the demotion still stands.
- `claim` returns early for the current controller, answering it with what it
  holds.
- `AttachTerminal` admits a negotiated controller attach that the policy
  refuses **as a viewer**, when the policy grants viewing, and `mayClaim`
  stays false for it. `internal/controld`'s pre-upgrade `mayAttach` mirrors
  it, so the two still ask the same question.
- The three test soft spots.

**Out:** the state machine, the wire, the compatibility matrix, the lease
parameters, rate limiting, and any change to what the sandbox does.

**The one behaviour change.** A negotiated `mode=control` attach by a
principal whose host policy refuses the controller used to be `ErrDenied` at
the door (a clean 403 pre-upgrade). It is now admitted as a viewer, told
`attached view@gen`, and prints the viewing notice. `MayClaim` is false for
it, so its take-control key is answered "you are still a viewer" and the store
is never touched. This is what `grant` does for every other refusal, and it is
what makes `mayClaim` reachable at all from the first-party CLI. The
previously-pinned 403 is now pinned the other way, in the same test.

## Alternatives considered

**Bound `displace` with one deadline for the whole loop, still serially.** It
fixes the taker's liveness and not the other peers': a first peer that eats
the whole deadline leaves every peer behind it unvisited, so a displaced
controller is not told and does not stop forwarding until its heartbeat. The
fan-out costs one goroutine per peer for the length of one handoff.

**One deadline per peer rather than per step.** Simpler, and wrong in a way
that matters: the install wait can legitimately consume the whole
acknowledgement timeout against an old sandbox, and the displacement notice
would then be written on an already-expired context and dropped. The notice is
the thing a person sees.

**Fix `announceViewer` and `announceStale` in place.** That is the third round
again. Two of four paths were fixed in the third round exactly this way and
the other two were missed; one function that cannot be called without reading
the state is the only version of this that does not recur.

**Have the CLI retry once with `mode=view` on a 403.** Cheaper, and it puts
the policy's own rule in the client: every other client (browser, API
consumer) would have to reimplement it, and the server would still be refusing
an attach it means to admit.

**Ping/pong instead of a write deadline on `ClientStream`.** Ping/pong detects
a dead peer; it does not bound a write that is already blocked on a full
send buffer. The deadline is what actually releases the writer, and it is the
smaller change.

## Edge cases

- **A `stale` that would reach a live controller.** Sent as `attached
  control@gen`. The client's `observe` folds that to "no news" rather than
  printing anything, which is right: nothing changed for it.
- **A claim that is superseded while it waits.** Unchanged: `announceAs` reads
  `(control, gen)` under the hold, sees the generation has moved, and answers
  `stale` with what exists now. The corrective install that re-points the
  sandbox's binding now happens *after* that message rather than before it;
  the two have no ordering requirement (the pty fence already made the stale
  binding inert), and this attach cannot claim concurrently with its own
  answer, because claims are handled inline on its client pump.
- **A demotion that no longer stands.** `demoteTo` refuses, no binding is
  installed, and the announcement reports `control_changed` with the mode the
  attach actually holds — `control`. A client that reads that stays the
  controller, which is what it is.
- **A peer that is not spliced yet.** Unchanged: `install` returns
  `errAttachNotSpliced` immediately, the displacement proceeds, and the
  binding rides the frame that opens that attachment.
- **A peer whose deadline expires mid-fan-out.** Its own heartbeat demotes it
  within one interval, and its pty fence was already in force the moment the
  store advanced. The notice is a courtesy; the mechanism is the heartbeat and
  the fence. This is written down in `docs/terminal-controller-ownership.md`.
- **A displaced peer's binding that does not land inside its deadline.** The
  peer is fenced anyway, and not by luck: `internal/session.observeLocked`
  walks the session's controller generation up from *any* binding it is told
  about, and the taker's own binding — installed on its own socket before the
  fan-out on a claim, riding the `dial_attach` on an attach — is one of them.
  The displaced peer's binding only re-points its own attachment so the
  sandbox's copy agrees with the plane's. Installing it is therefore
  best-effort by design, which is what lets it carry a deadline at all.
- **A slow but live client during a large snapshot replay.** The write
  deadline is deliberately far above any legitimate frame time on a slow link;
  it exists to close a socket that is not draining at all.
- **`displace` at generation zero.** Unchanged: no row is ever at zero, so
  every `displaceTo` refuses and no goroutine does any I/O. *(Superseded by
  [`2026-09-11-controller-ownership-fifth-review.md`](2026-09-11-controller-ownership-fifth-review.md):
  every peer is now announced to whether or not its state moved, so a
  generation-zero fan-out does one client write per peer that has already
  been told what it is. It still demotes nobody, and what each notice says is
  that peer's own mode and number.)*
- **A view-only principal on an unnegotiated attach.** Still refused. A legacy
  client cannot be told it is a viewer, so admitting it as one would leave a
  terminal that silently does not type.

## What the review of this change found, and what it changed again

Two independent reviews of the first version of these fixes (one adversarial,
with a 21-mutant battery) found that the bound in finding 1 had been applied
one level too wide, and that two of the other fixes composed into a new
failure. Both are fixed here, and the fixes are worth writing down because
they are the same mistake in opposite directions: **a deadline belongs to the
caller that can afford it.**

- **A deadline that is already there is not written twice.** The install step
  inside the fan-out carried a wrapper of its own, which capped at one
  acknowledgement timeout what `install` and `installAndWait` already cap at
  one between them — a write that fails takes its wait with it. It is gone;
  what is left is one deadline per step, in the step. The notice's deadline is
  the one that has to be at the loop, because the write it bounds is the one
  that reaches a client.
- **A courtesy notice must not close a healthy client.** Bounding every
  ownership message by the acknowledgement timeout, and closing the socket
  whenever any context expired, meant a websocket serialises its writes so a
  handoff's `control_changed` queued behind a snapshot being replayed to that
  same client expired on the write lock — and killed the attach. One take-over
  anywhere on the session disconnected a client that was reading a scrollback
  over a slow link. The caller's context now decides: a message this attach is
  owed carries the socket's write deadline, a message about somebody else's
  handoff carries one acknowledgement timeout, and `ClientStream` closes on
  its own budget and nobody else's. The `announce` hold became a channel so
  that *acquiring* it is bounded too.
- **A claim whose binding never landed is not a claim.** `install` is bounded
  now, and a claim discarded its error: the store CAS had already succeeded,
  so the plane took control, told the client so, and the pty then discarded
  every keystroke — with nothing able to repair it, because the lease really
  was this attach's and the heartbeat renewed it happily. The generation is
  given back instead and the client is told what exists now.
- **The idempotency guard checks its belief before answering from it.** A
  controller displaced from another replica reads `control` here until its own
  heartbeat is refused, and the take-control press is its user's only way out.
  One store read, which advances nothing, keeps that door open.
- **A demotion is decided at the renewal that was refused**, not at a
  generation read later, and a superseded one installs nothing at the sandbox.
- **An attach is registered before its first message is read.** A controller
  attach whose generation the application has already advanced used to be
  invisible to the owner table for the length of that read — up to fifteen
  seconds — so a claim elsewhere never displaced it and both clients were told
  they had control. This one predates this round; it is the last window in
  which the rule this PR exists for could be broken, so it is closed here.
- The client prints `[you have control]` when it learns it from a
  `control_changed` rather than from the answer to its own claim, which is
  what happens when a peer's handoff announces to it first.

## Verification

Every fix lands with a regression test that fails without it:

| Fix | Test | Fails without the fix because |
|---|---|---|
| 1 | `TestAStuckPeerDoesNotStallAClaim`, `TestAStalledExControllerIsFencedEvenThoughItsNoticeCouldNotBeDelivered` | the claim is never answered |
| 1 | `TestAStuckPeerDoesNotStallANewControllerAttach` | the attach never reaches its runner |
| 1 | `TestOneStalledPeerDoesNotHideAnothersDisplacement` | the peer behind the stalled one is never told (with the writes unbounded; bounded-but-serial, it is `TestTheFanOutReachesEveryPeerAtOnce` that fails) |
| 1 | `TestTheFanOutReachesEveryPeerAtOnce` | six peers that stopped reading cost six deadlines instead of one |
| 1 | `TestABindingWriteThatNeverLandsDoesNotHoldTheHandoff` | the handoff waits on a runner socket that stopped draining |
| 1 | `TestACourtesyNoticeNeverClosesAHealthyClient` | a handoff's notice closes a client that is mid-snapshot |
| 1 | `TestAClientThatNeverDrainsIsClosedFromBehindTheWriteLock` | a socket that has taken nothing is held open |
| 2 | `TestAClaimWhoseBindingNeverLandedGivesTheGenerationBack` | the plane takes control its sandbox cannot honour |
| 2 | `TestAControllerDisplacedElsewhereRecoversOnOnePress` | the press is answered from a belief the store contradicts |
| 2 | `TestAnAttachStillReadingItsFirstMessageIsDisplacedLikeAnyOther` | two attaches are each told they have control |
| 2 | `internal/attachio: TestControlWonInsideSomebodyElsesHandoffIsStillAnnounced` | the person takes control and is told nothing |
| 1 | `TestAWedgedClientIsClosedRatherThanHeld` | the write never returns |
| 2 | `TestAControllerThatClaimsFromAStaleGenerationKeepsControl`, `TestAStaleAnswerNeverTellsALiveControllerItIsAViewer` | the controller is told `stale` and stops typing |
| 2 | `TestADisplacementNoticeReportsTheModeItReads` | the notice says `view` for a live controller |
| 3 | `TestADemotionThatWasSupersededDoesNotDemote`, `TestDemoteToRefusesAGenerationThisAttachHasLeft` | the plane demotes an attach that won a newer generation |
| 4 | `TestAViewOnlyPrincipalWatchesAndMayNotClaim` (the plain-attach case) | the attach is `ErrDenied` |
| 5 | `TestAClaimFromTheCurrentControllerNeverAdvancesTheGeneration` | the generation advances and peers are re-displaced |
| 7 | `TestAStaleAcknowledgementNeverCostsTheNextHandoffItsWait` | the second handoff burns the whole timeout |

Plus: the existing `attachplane` and `controlapp` suites, `-race -count=10` on
both, `-race -count=20` on the new interleaving tests, `internal/e2e` under
`-race`, and `repotest` on memstore and pgstore with no skips.

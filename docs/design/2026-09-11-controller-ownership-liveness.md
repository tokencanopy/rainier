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
- **A slow but live client during a large snapshot replay.** The write
  deadline is deliberately far above any legitimate frame time on a slow link;
  it exists to close a socket that is not draining at all.
- **`displace` at generation zero.** Unchanged: no row is ever at zero, so
  every `displaceTo` refuses and no goroutine does any I/O.
- **A view-only principal on an unnegotiated attach.** Still refused. A legacy
  client cannot be told it is a viewer, so admitting it as one would leave a
  terminal that silently does not type.

## Verification

Every fix lands with a regression test that fails without it:

| Fix | Test | Fails without the fix because |
|---|---|---|
| 1 | `TestAStuckPeerDoesNotStallAClaim` | the claim is never answered |
| 1 | `TestAStuckPeerDoesNotStallANewControllerAttach` | the attach never reaches its runner |
| 1 | `TestOnePeersStallDoesNotHideAnothersDisplacement` | the peer behind the stalled one is never told |
| 1 | `TestAWedgedClientIsClosedRatherThanHeld` | the write never returns |
| 2 | `TestAControllerIsNeverToldItIsAViewer` | the controller is told `stale` and stops typing |
| 2 | `TestADisplacementReportsTheModeItReadsAtSendTime` | the notice says `view` for a live controller |
| 3 | `TestADemotionThatWasSupersededDoesNotDemote` | the plane demotes an attach that won a newer generation |
| 4 | `TestAViewOnlyPrincipalsPlainAttachIsAdmittedAsAViewer` | the attach is `ErrDenied` |
| 5 | `TestAClaimFromTheControllerChangesNothing` | the generation advances and peers are re-displaced |
| 7 | `TestAStaleAcknowledgementNeverCostsTheNextHandoffItsWait` | the second handoff burns the whole timeout |

Plus: the existing `attachplane` and `controlapp` suites, `-race -count=10` on
both, `-race -count=20` on the new interleaving tests, `internal/e2e` under
`-race`, and `repotest` on memstore and pgstore with no skips.

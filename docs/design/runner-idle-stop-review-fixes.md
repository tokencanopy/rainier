# Idle auto-stop: the fourth review round

Status: proposed (implemented by this change). An addendum to
[`runner-idle-stop.md`](runner-idle-stop.md), not a replacement: that document still
describes the feature, and this one describes the eight findings a fourth, independent
review raised against it and what each one is answered with — plus a ninth, which this
round's OWN independent review found in the fix for finding 5 and which is written up in
"9. The refusal the control plane could not see" below. All of it ships on the same PR
([#87](https://github.com/tokencanopy/rainier/pull/87)).

## Problem

The feature as reviewed is semantically right — the rule, the race discipline, the wire
additivity and the scope all hold — but three things about it are not:

1. **It is on by default, and its safety depends on the control plane understanding a new
   event state.** `ProtocolVersion` stays 1 and `suspended_cold` is additive, so a runner
   rolled ahead of its controld has its auto-stop events dropped on `runnerplane`'s unknown
   -state arm. The row then reads `running` over a stopped container: `rainier attach`
   dials, finds no hub, and fails; `ResumeSession` refuses a `running` row. That is exactly
   the "no way back into the session" outcome the main design doc's cold-resume-fence
   section spends a round making impossible — reintroduced by deploy order alone. Nothing in
   the runner can detect an old control plane.
2. **The stale-frame guard has a hole at zero.** `nextBoot` starts at 0 and only `resumed()`
   mints from it, so every entry that has never been cold-resumed carries `boot == 0`. A
   session id that is deleted and recreated therefore gets an entry whose epoch is
   indistinguishable from its predecessor's, and a `child_exited`/`stage_failed` goroutine
   already spawned by the old container's hub read loop is accepted against the new entry —
   the precise case `nextBoot`'s own doc comment claims to close.
3. **The capacity counts are most misleading on the box they matter most on.** After a
   runnerd restart, `Recover` rebuilds entries with `childExitedAt` zero, so sixteen
   finished sessions report `active=16, idle_exited=0` — "sixteen working agents" about the
   one machine the sweep will reclaim nothing on.

Plus five smaller ones: a resume that overtakes an in-flight stop, two comments describing a
mechanism round 3 deleted, no operator knob on the hosted launcher, an auto-stop being
indistinguishable from an operator's stop on the session row, and a sentence in the PR body
that contradicts the code it describes.

## Scope

In, one commit each, every code fix with a test that fails without it:

| # | Finding | Fix |
|---|---|---|
| 1 | Mixed-version deploy | Deploy order (controld before runners) in the README and the main design doc; `IDLE_STOP` on the launcher, with `0` named as what an un-rolled fleet passes |
| 2 | `boot == 0` shared by every never-resumed entry | Mint the epoch in `put`/`putIfAbsent`, so zero is never a live entry's epoch |
| 3 | Recovered entries counted as `active` | Carry them in neither count until they report an exit |
| 4 | Two comments describe a deleted mechanism | Delete the stale clauses |
| 5 | Resume overtakes an in-flight stop | `Op`'s resume refuses while a claimed stop is still running (409), and the design-doc table gains the row |
| 6 | An auto-stop is indistinguishable from an operator's stop | Recorded as a follow-up (PR body + "Other known limits") |
| 7 | No idle-stop knob on `fleet-up.sh` | `--idle-stop "${IDLE_STOP:-30m}"` |
| 8 | PR body's follow-up 2 contradicts the code | PR text only |
| 9 | Finding 5's new 409 reaches the person as a 500 `internal` | An additive `conflict` bit on a runner `result`, a `ErrRunnerConflict` out of `controlapp.dispatch`, and the 409 the session handlers already write for `control.ErrConflict` |

Out: everything the main design doc puts out, unchanged. In particular this round does
**not** carry the placement generation on `resume` (still follow-up 2), does not add a
durable activity record (still #85's), and does not raise `--slots`' default.

## Design

### 1. Deploy order, and a knob to stand the feature down

There is no in-band answer. `ProtocolVersion` is the runner protocol's version and bumping
it would refuse the connection outright — a far worse failure than a dropped event — and the
control plane sends the runner nothing that says which event states it understands. So the
answer is operational and has two halves:

- **Say the order.** controld is rolled before its runners. This is already true of every
  additive runner event; it has simply never been written down, and this is the first
  additive event whose loss leaves a session unreachable rather than merely unreported.
- **Give the operator the switch.** `scripts/fleet-up.sh` passes
  `--idle-stop "${IDLE_STOP:-30m}"`, so `IDLE_STOP=0` stands the feature down on a fleet
  whose control plane has not been rolled yet, without editing the script. The README says
  so in the same paragraph as the order.

The window is bounded either way — a runner reconnect reconciles the row — but "until this
runner next reconnects" is not a number an operator can plan around, and reconciliation runs
per connection, not periodically.

### 2. Mint the boot epoch at insertion

`r.nextBoot++; e.boot = r.nextBoot` inside `put` and `putIfAbsent`'s critical sections. Two
consequences, both wanted:

- **Zero stops being a live entry's epoch.** `currentBoot` already returns 0 for a session
  this registry does not hold, so a frame that carries 0 now matches no entry at all rather
  than matching every never-resumed one.
- **A recreated id gets a fresh epoch.** The counter is registry-wide, so the new entry's
  epoch is strictly greater than the dead one's and the old container's in-flight
  `child_exited` is dropped by the guard that already exists.

`register` still *reads* rather than mints — the round-2 finding that a per-registration
epoch drops an exit across a plain sessiond redial is untouched by this, because insertion
happens once per session and a redial is not an insertion.

### 3. Recovered entries are in neither count

A `recovered bool` on `sessionEntry`, set by `Recover` and cleared by the two things that
make the runner able to speak for the child again: a `child_exited` report (the session is
now genuinely idle-exited, and is an auto-stop candidate) and a resume that the driver says
**restarted** the container (a new process tree this runner did watch start).

Preferred over documenting the skew because the consumer of these numbers is a sentence
printed to an operator, and "16 slots, 0 active, 0 idle" followed by `used=16` is legible as
"this runner is not telling me" — which the protocol field's doc comment already names as the
reading for a zero pair — while "16 active" is a confident lie.

### 4, 5, 6, 8 — the small ones

4 and 8 are text. 6 is a follow-up entry: the reason string (`"idle for 30m0s"`) is
deliberately dropped by `runnerplane` because `Detail` is the error column and a session that
parked as asked has no error, so distinguishing the two stops on `rainier info` needs a
non-error field — a change to `control.RunnerEvent` and the session row, which is not this
PR's.

5 is the only one with a race in it, and the finding's own wording ("refuse
`state == "suspending"`") would have introduced a worse bug than it fixes. `"suspending"` is
not only a transient: a stop whose outcome the driver could not report deliberately *parks*
the entry there (`settleFailedColdSuspend`'s last arm), no sweep re-claims it because the
idle rule requires `"running"`, and a resume is the only way back. Refusing that resume
strands the session for the life of the runner — the same lost slot this feature exists to
end.

So the fact the resume asks for is "is a cold suspend RUNNING against this container": a
`stopsInFlight` counter on the entry, incremented inside `claimIdle`'s and
`beginColdSuspend`'s critical sections and paired by `coldSuspend`'s defer. A counter and not
a flag because two stops can be claimed against one session (an operator's, arriving while a
`Delete` owns the entry) and the first to finish must not clear the second's claim.

For the sweep the guard is then total, not merely a narrowing. `Op` takes the `driverOps`
bracket as its very first action, `claimIdle` refuses an entry with `driverOps > 0`, and both
take the registry lock: if `beginOp` won it, the sweep never claimed the session; if
`claimIdle` won it, the count `Op` then reads is already up. What remains is
operator-versus-operator — a `rainier stop` and a `rainier resume` issued at the same moment,
where the resume can get past the check before `beginColdSuspend` marks — which this change
narrows but does not close, and which the edge-case table now names.

### 9. The refusal the control plane could not see

Finding 5's fix gave the runner a refusal it did not have before — `errSuspendInFlight`,
a 409 on the runner's local HTTP front. Nothing carries that fact up the control
connection. `agent.go`'s `suspend`/`resume` arm sends a bare `OK:false`,
`controlapp.dispatch` turns every `OK:false` into `ErrRunnerRefused`, and
`internal/controld`'s `writeSessionErr` answers that with **500 `internal`, "could not
resume session"** — a server-error class, for a condition in which nothing is wrong, the
runner is healthy and answering, and the same command succeeds a second later.

It is reachable from controld, which is what makes it worth a round rather than a note.
`announceState` renders a `"suspending"` entry as `suspended_cold` — deliberately, because
omitting a session controld believes is live is the one direction reconciliation cannot
heal from — so a runner whose control connection redials while the idle sweep's stop is in
flight announces the session as stopped, `reconcileSessions` moves the row from `running`
to `suspended_cold`, and the row is now in exactly the state `ResumeSession` accepts. The
person's `rainier attach` (which resumes on their behalf) is dispatched, the runner refuses
it with the new 409, and the CLI prints an internal error. The auto-stop is what makes that
window ordinary rather than exotic: it is an unattended stop that can be in flight against
any idle session at any moment, including the moment a control plane is rolled.

Two things go wrong with one cause, and the cause is that "the runner said no" and "the
runner said not yet" arrive as the same bit:

- **The class is wrong.** 500 `internal` tells a person, and any automation reading the
  code, that something broke here. Nothing did.
- **The client's own recovery is bypassed.** `cmd/rainier`'s `resumeForAttach` already has
  a bounded convergence loop for a resume that lost a race, and it keys on
  `code == "conflict"`. A conflict dressed as an internal error never reaches it.

The answer is one additive bit, and the vocabulary for it already exists on both ends:

1. **`FromRunner.Conflict`** (`json:"conflict,omitempty"`) on a `result`. Additive in both
   directions and by the same argument the rest of this protocol uses: an old controld
   ignores the field, and a new controld reading `false` from an old runner gets exactly
   today's behaviour, which is what it has always had. No `ProtocolVersion` bump, so
   finding 1's deploy order is not made any heavier — an un-rolled control plane simply
   goes on reporting the refusal as it does now.
2. **One list of which refusals are conflicts**, `opConflicts` in `internal/runnerd`, read
   by the local HTTP front's `mapOpErr` (which answers each of them 409, with its own
   sentence) and by the agent's result arms (which set the bit). One list because the
   alternative is precisely the drift this finding is: two surfaces answering the same
   refusal differently, discovered a round later. It holds `errSessionStarting` — already a
   409 on the local front, and a 500 on the control path today for the same bad reason —
   and `errSuspendInFlight`.
3. **`controlapp.ErrRunnerConflict`**, returned by `dispatch` when a refusal carries the
   bit, wrapping `control.ErrConflict` exactly as `ErrRunnerRefused` wraps
   `control.ErrUnavailable`. The services need no new branch — `SuspendSession`,
   `ResumeSession` and `DeleteSession` already return a dispatch error unchanged — and the
   status and the code are the row conflict's, deliberately, so a client keying on either
   sees one retryable refusal.
4. **One new sentence per handler**, `sessionErrText.RunnerConflict`, taken before the row
   conflict's. The first draft of this change reused the row's sentence and an adversarial
   review caught it: those sentences NAME A STATE. `stop`'s row conflict is "session is not
   running", and a runner refusing a stop is refusing a session that very much is — so the
   answer would have been one the client can disprove by re-reading the row it just read.
   `resume` needs no new sentence ("session cannot be resumed right now" is already about
   the moment, not the state) and falls back to it.

What the person gets afterwards: **409 `conflict`, "session cannot be resumed right now"**,
exit 1. The exit code does not change — every `rainier` failure that is not a usage error
is 1 — and saying so is the point: the fix is in the class, the code and the sentence, not
in the exit status.

Two things are NOT fixed, named here rather than discovered later:

- `resumeForAttach` now converges instead of failing at once, and its loop returns success
  only when the ROW reaches a state it reads as live (`running`, `creating` or `queued`).
  A refused resume moves the row to none of those, so `rainier attach` polls for its
  bounded two seconds and then reports the conflict. Better than today's immediate
  internal error, worse than retrying the resume itself — which is a change to the CLI's
  retry policy and not to this PR's subject.
- **`cmd/rainier`'s stderr rendering for a 409 is poor, and this change does not make it
  good.** `reportError` sends every `*cli.APIError` through `readinessError`, which is a
  CONNECTION diagnosis, so a conflict prints "unexpected HTTP status (409); verify the
  configured server with your administrator" rather than the sentence the server wrote for
  a person. The error VALUE is right (`conflict: session cannot be resumed right now`, which
  is what `rainier attach` surfaces and what a script reads), and the stable code is on the
  second line — but the prose is wrong, and it has been wrong for every 409 this CLI has
  ever produced, not just this one. Fixing it means teaching `reportError` that a 4xx
  carrying a server-written message is not a connectivity problem, which is a CLI-wide
  change and belongs in its own PR. On the PR's follow-up list.

### 10. A resume accepted into the gap between the stop and its report

The adversarial review of finding 9's fix found the defect underneath it, and it is the
more serious of the two: **a stop was releasing its in-flight claim before it had reported
itself.**

`coldSuspend` released the claim on return (`defer s.reg.endStop(id)`), and `stopIdle` then
did three more things — read the capacity counts, make a BOUNDED `drv.Capacity` call, write
its log line — before firing the `suspended_cold` event. A resume arriving in that gap was
accepted: the entry already read `"suspended"`, the driver restarted the container,
`ResumeSession` committed `running`, and then this stop's own event landed and parked the
row on `suspended_cold` over a container that is running again. The event cannot be fenced
away — an auto-stop deliberately carries no placement generation, and zero fences nothing —
so the row is simply wrong until somebody resumes again.

That is the "row reads stopped over a live sandbox" end state this design spends two
sections making impossible, and the window is the widest part of a stop: `idleCapacityTimeout`
is five seconds, and a busy daemon spends them.

The race predates finding 9's fix. What finding 9's fix does is **aim clients at it**:
before, the refusal was a 500 and nobody retried; now it is a 409 that this document, the
CLI's conflict vocabulary and every operator are told means "try again", and a client
retrying a resume until it stops being refused walks straight into the gap. A refusal
advertised as retryable has to be honest about when it ends.

So the claim is now the caller's to release, not `coldSuspend`'s: `Op`'s stop pairs it with
a `defer` (it has nothing to report — controld already knows about a stop it dispatched),
and `stopIdle` holds it until it returns, **with the event fired first** so that nothing a
wedged `Capacity` call does can delay the one thing a waiting client needs. A resume in the
gap is refused with the same 409, and the retry finds the row where the stop left it.

The cost is a resume refused for the length of the reporting, bounded by
`idleCapacityTimeout`. That is the safe direction and a bounded one: a daemon too wedged to
answer `Capacity` in five seconds is a daemon the resume was not going to get anything from
either.

## Alternatives considered

**Bump `ProtocolVersion` for finding 1.** Rejected: the version gates the whole connection,
so a new runner would be refused by an old controld entirely. Losing the fleet is worse than
losing an event, and the event is what the deploy order protects.

**Have the runner probe the control plane's vocabulary.** There is no such handshake, and
inventing one for a single additive state is a protocol change of its own — with the same
deploy-order problem one level down.

**Default `--idle-stop` to `0` and let operators opt in.** Rejected: the incident this
feature exists for is the default behaviour being wrong, and an opt-in that the hosted fleet
would set anyway buys nothing but a second config to forget. The order is a one-line
operational fact; the knob is there for the transition.

**Document the `Recover` count skew instead of fixing it (finding 3's stated alternative).**
Rejected as above: a doc comment does not reach the operator reading `rainier status`.

**Refusing a resume whenever the state reads `"suspending"`**, which is finding 5's own
wording. Rejected: that state is also where a stop whose outcome could not be read parks the
entry, permanently, and the resume it would refuse is the only thing that unparks it. The
fix would have converted a narrow race into a guaranteed lost slot.

**Carrying finding 9's conflict in `Detail` and matching on its text.** Rejected: the
runner's free text is the runner's, the control plane deliberately never reads it into an
error (`transport`'s doc comment says so, and `writeSessionErr` refuses to relay it), and
a sentinel spelled as a string is a coupling that no compiler checks.

**Fixing finding 9 only for `errSuspendInFlight`.** Rejected: `errSessionStarting` is
already a 409 on the runner's own front and a 500 on the control path, which is the same
defect one sentinel over. Two lists would have had to agree forever; one list cannot
disagree.

**A `resuming` state for finding 5**, claimed under the lock like `claimIdle` does. Rejected
for this slice: it adds a fourth transient state that every other path (`hubDied`,
`Delete`'s marker, `Recover`, the `GET /sessions` rendering) would have to learn, to close a
window the `driverOps` interlock already closes for the sweep.

## Edge cases

| Case | Behaviour |
|---|---|
| a new runnerd against an old controld | the auto-stop event is dropped on the unknown-state arm and the row reads `running` over a stopped container until the runner reconnects. Prevented operationally: controld is rolled first, and `IDLE_STOP=0` stands the feature down until it is |
| a session id deleted and recreated | the new entry's epoch is strictly greater than the old one's, so the old container's in-flight `child_exited` is dropped |
| a control frame carrying epoch 0 | matches no entry: 0 is no longer any live entry's epoch, and `currentBoot` returns it only for a session the registry does not hold |
| `Recover` rebuilds 16 finished sessions | `active=0, idle_exited=0, used=16` — the pair a consumer already reads as "this runner is not telling me" |
| a recovered session reports a child exit | it leaves the exemption: counted in `idle_exited`, and an auto-stop candidate from that exit |
| a recovered session is cold-resumed | the driver reports a restart, the entry is one this runner watched start, and it counts as `active` |
| a resume arrives while a stop is in flight | 409 `session is being suspended`. For the sweep's stop this is airtight (the `driverOps` interlock); for an operator's stop it is a narrowing |
| a resume arrives after the container is stopped but before the auto-stop is REPORTED | refused, same 409, because the sweep holds its claim until its event is out. Accepting it would have the control plane commit `running` and then take it back when the stop's own unfenced event landed — a row reading stopped over a container that is running |
| the `Capacity` call the sweep makes after a stop is slow | the event has already gone out; only the refusal window is extended, bounded by `idleCapacityTimeout` |
| a resume arrives for an entry parked on `"suspending"` by a stop that could not be read | allowed, and it is the only way back for that session: the guard is on a running stop, not on the state marker |
| a runner redials while the sweep's stop is in flight | it announces the session as `suspended_cold` (`announceState` renders `"suspending"` that way), reconciliation moves the row there, and a resume dispatched into that window is refused — now as **409 `conflict`, "session cannot be resumed right now"**, not as a 500 |
| an old controld receives a conflict-flagged refusal | it ignores the unknown field and reports the refusal exactly as it does today: additive, so finding 1's deploy order does not grow a second obligation |
| a new controld receives a refusal from an old runner | `Conflict` reads `false`, which is `ErrRunnerRefused` — today's behaviour, unchanged |
| any other command refused as a conflict (`destroy`, `suspend`) | the same 409, with that handler's own sentence. `errSessionStarting` stops being a 500 on the control path, which it never should have been |
| `IDLE_STOP=0` on the launcher | `RunIdleStop` returns immediately and logs that auto-stop is disabled — the documented off switch, now reachable without editing the script |

## Verification

- `internal/runnerd`: a test per code fix, each pinned by mutating the fix back out and
  watching it fail — a recreated id refusing the dead container's `child_exited`; `Recover`'s
  entries in neither count, and both ways out of the exemption; a resume during an in-flight
  stop refused with 409 and the driver never called, and a resume of an entry parked by a
  stop that could not be read still allowed.
- A test that `scripts/fleet-up.sh` passes `--idle-stop` with the `IDLE_STOP` override, in
  the package that owns the flag's meaning.
- For finding 10, a unit test whose driver stalls the capacity reading the sweep takes
  after the stop: the container is already suspended, the auto-stop event has already been
  fired, and a resume is still refused — and once the report is done, the same resume is
  accepted and nothing the stop had left to say undoes it. Fails with the claim released by
  `coldSuspend` again.
- For the runner-conflict sentences, a table over `stop`, `snapshot` and `delete`: each
  answers a runner-reported conflict with its own sentence rather than the row's, which
  names a state the refusal does not claim.
- For finding 9, one test per seam plus one that crosses all of them: the agent's result
  for a refused resume carries the bit; every member of `opConflicts` is a 409 on the local
  front AND a conflict on the wire (the anti-drift pin, which fails the moment the two
  lists part); `controlapp.dispatch` turns the bit into `control.ErrConflict` and
  `ResumeSession` leaves the row where it was; the HTTP front writes 409 `conflict`; and
  an `internal/e2e` scene drives the whole thing — a real runnerd whose cold suspend is
  held open, a real controld restarted underneath it so the announce moves the row, the
  real REST client refused, and the same resume succeeding once the stop lands.
- `make verify`, `go test ./internal/runnerd/ ./internal/driver/ ./protocol/... ./runnerplane/
  -race -count=3`, and the new tests at `-race -count=20`.

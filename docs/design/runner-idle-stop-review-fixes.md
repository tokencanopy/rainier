# Idle auto-stop: the fourth review round

Status: proposed (implemented by this change). An addendum to
[`runner-idle-stop.md`](runner-idle-stop.md), not a replacement: that document still
describes the feature, and this one describes the eight findings a fourth, independent
review raised against it and what each one is answered with. Both ship on the same PR
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
| 5 | Resume overtakes an in-flight stop | `Op`'s resume refuses `state == "suspending"` (409), and the design-doc table gains the row |
| 6 | An auto-stop is indistinguishable from an operator's stop | Recorded as a follow-up (PR body + "Other known limits") |
| 7 | No idle-stop knob on `fleet-up.sh` | `--idle-stop "${IDLE_STOP:-30m}"` |
| 8 | PR body's follow-up 2 contradicts the code | PR text only |

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

5 is the only one with a race in it. `Op` takes the `driverOps` bracket as its very first
action, and `claimIdle` refuses an entry with `driverOps > 0`; both take the registry lock.
So for the sweep the guard is total, not merely a narrowing: if `beginOp` won the lock, the
sweep never claimed the session, and if `claimIdle` won it, the state `Op` then reads is
already `"suspending"` and the resume is refused. What remains is operator-versus-operator —
a `rainier stop` and a `rainier resume` issued at the same moment, where the resume can read
`"running"` before `beginColdSuspend` marks — which this change narrows but does not close,
and which the edge-case table now names.

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
| `IDLE_STOP=0` on the launcher | `RunIdleStop` returns immediately and logs that auto-stop is disabled — the documented off switch, now reachable without editing the script |

## Verification

- `internal/runnerd`: a test per code fix, each pinned by mutating the fix back out and
  watching it fail — a recreated id refusing the dead container's `child_exited`; `Recover`'s
  entries in neither count, and both ways out of the exemption; a resume against a
  `"suspending"` entry refused with 409 and the driver never called.
- A test that `scripts/fleet-up.sh` passes `--idle-stop` with the `IDLE_STOP` override, in
  the package that owns the flag's meaning.
- `make verify`, `go test ./internal/runnerd/ ./internal/driver/ ./protocol/... ./runnerplane/
  -race -count=3`, and the new tests at `-race -count=20`.

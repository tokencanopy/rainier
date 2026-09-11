# Idle auto-stop in runnerd

Status: proposed (implemented by this change)
Supersedes nothing. Superseded by: [#85](https://github.com/tokencanopy/rainier/issues/85),
"Runner capacity: replace the fixed slot count with resource-aware admission and idle
auto-suspend". This is the near-term half that issue itself names as agreed and small.

## Problem

A sandbox holds one of runnerd's `--slots` from the moment it is created until an operator
stops or deletes its session — whether or not its child process has exited, and whether or
not anyone is attached. On a hosted runner, fourteen sessions whose agents had finished
hours earlier filled the box while three agents were actually working; new sessions were
refused for "no free capacity". Stopping the finished sessions by hand freed it at once.

Nothing in the runner notices that a session is finished. `child_exited` already arrives
from `sessiond` and is already forwarded to controld, but the runner treats it as news and
changes nothing.

## Scope

In:

1. **Idle auto-stop.** runnerd stops a session whose child has exited and that has had no
   attachment for `--idle-stop` (default 30m, `0` disables), by exactly the path
   `rainier stop` takes: a cold suspend. Files are kept, the slot is released, and
   `rainier attach` resumes it.
2. **Saying it.** One structured log line per auto-stop, an unsolicited `suspended_cold`
   event to controld so the session row follows the sandbox, and two additive counts
   (`active`, `idle_exited`) on the capacity that already piggybacks every runner message.
3. **Documentation** of `--slots` and `--idle-stop` in `cmd/runnerd`'s help and the README.

Out (all of it is #85's):

- Resource-aware admission (memory/disk headroom instead of a count), queueing instead of
  refusal, `suspended_cold` eviction under disk pressure, the high safety-valve cap.
- Idling a session whose child is still running but that has shown no *activity* — #85 wants
  one shared activity signal, and inventing a second one here would have to be unwound.
- The CLI change that would print "16 slots, 3 active, 13 idle" instead of "no free
  capacity". This change carries the counts on the wire and wires nothing further; the CLI
  and the runner row are a follow-up (see below).
- `--slots`' default. Unchanged at 16; the hosted runner passes its own value.

## What a stop is, confirmed end to end

The brief for this work said `suspended_warm` was the state a stopped-but-resumable session
already has. It is the other one, and the difference is the whole point of the change:

| | `suspended_warm` | `suspended_cold` |
|---|---|---|
| driver | `docker pause` | `docker stop` |
| `control.SessionState.OccupiesSlot()` | **true** | **false** |
| `driver.Docker.Capacity` counts it | yes (`status=paused`) | no |

`rainier stop` sends `warm=false` — `cmd/rainier` has a test that fails if it ever sends
anything else ("the safe stop is always cold") — controld dispatches `suspend` with
`Warm=false`, `runnerd.Op` calls `drv.Suspend(handle, false)`, the registry entry lands on
`"suspended"`, `Announce` renders it `suspended_cold`, and the session row reads
`suspended_cold`, which `rainier ls` prints as `stopped`. `rainier attach` on it calls
`POST /v0/sessions/{id}/resume` first.

So idle auto-stop is a **cold suspend**, and it is the same call, on the same registry
states, with the same rollback, as the operator's stop. That is enforced structurally: both
paths go through one `coldSuspend` helper.

## Design

### The facts the runner keeps

Three fields on `sessionEntry`, all read and written under the registry lock like every
other mutable field:

- `attachments int` — live attachments over this session's hub, incremented and decremented
  around **both** attach fronts (the local `/attach` handler and the agent's
  `dial_attach` dial-back). Any attachment counts; the runner does not know or care whether
  a viewer holds the controller lease.
- `childExitedAt time.Time` — when `sessiond` reported `child_exited` for the current
  sandbox boot. Zero means "the child is running, as far as this runner has been told".
- `boot uint64` — which sandbox boot the entry is on. Every control frame carries the token
  its connection was registered under, and a `child_exited` naming any other boot is dropped.
  A control frame can outlive its boot: a hub read loop stalled writing to a wedged viewer
  drains its buffered frames whenever it comes back, which can be after the sandbox has been
  stopped, resumed and re-registered. Without this, that buffered exit lands on the new boot
  and the runner stops a session whose agent is working. It is the same guard, for the same
  reason, as `hubDied`'s `deadHub` parameter. `resumed()` closes the old epoch too, for the
  window between the resume and the new registration.
- `bootFailed bool` — this session's setup/clone/init chain failed. Such a session is never
  auto-stopped: its child exits with the failing stage, so half an hour later it looks
  exactly like a finished agent — but attaching to a failed session to read the log that
  says why is the whole reason the CLI permits it, and neither a stopped sandbox (no hub)
  nor a `failed` row (not resumable) can serve that. It holds its slot until someone removes
  it. Reclaiming those is #85's, and the trade is deliberate: the incident this change exists
  for was fourteen *finished* sessions, not failed ones.
- `lastDetachAt time.Time` — when the most recent attachment ended.

### The rule

A session is idle at time `now`, for a timeout `d > 0`, when all of:

- its state is `"running"` (not `starting`, `suspending`, `suspended`, `destroying`),
- no driver operation this runner dispatched is in flight (`driverOps == 0`) — a warm
  suspend and a resume have no state marker of their own, and a snapshot runs against a live
  container for minutes, so a session somebody else is mid-operation on is not one to stop,
- its boot chain did not fail (`bootFailed == false`) — see below,
- `attachments == 0`,
- `childExitedAt` is non-zero,
- `now.Sub(max(childExitedAt, lastDetachAt)) >= d`.

`max(childExitedAt, lastDetachAt)` is what makes the timer start when the **last viewer
leaves**, not when the child exited: someone reading the scrollback of a finished agent for
an hour resets nothing while attached, and gets the full `d` after detaching.

### The sweep

`RunIdleStop(ctx, d)` runs a ticker; each tick takes a registry snapshot, and for each
candidate calls `claimIdle`, which **re-checks the whole rule under the registry lock and,
in the same critical section, marks the entry `"suspending"`**. Claim and decision cannot be
separated, so two ticks (or a tick and anything else) cannot both stop one session, and an
entry that a concurrent `Delete`/`Op` has already moved out of `"running"` is never claimed.
`"suspending"` is the marker `Op`'s cold suspend already sets for its own reason: `docker
stop` kills the container's `sessiond`, which closes the `/register` conn, and the register
goroutine must read that as a deliberate stop rather than a crash — otherwise it would
destroy the container.

Interval: `d/10`, clamped to `[1s, 1m]`. A session is therefore stopped at the first tick at
or after its deadline, i.e. up to one interval late. Never early: the deadline is checked
against the clock, not counted in ticks.

### The clock

One `func() time.Time` on the server, `time.Now` in production and a fake in tests. Every
comparison is `now.Sub(stored)` where both came from that clock, so in production both carry
Go's monotonic reading and the subtraction uses it: an NTP step or a timezone change cannot
make a session look idle early or keep it from ever looking idle. Nothing on this path calls
`.UTC()`, `.Round()` or marshals these times, which is what would strip the monotonic
reading; `time.Time{}` carries none, so `IsZero()` stays correct after `resumed()` clears a
field.

Host suspend-to-RAM is the one thing the monotonic clock does *not* see: `CLOCK_MONOTONIC`
does not advance across it, so a runner host that sleeps for eight hours wakes with every
session's idle age eight hours short. That is the safe direction — late, never early — and it
is a property of the clock, not something to correct here.

### Announcing it

On a successful stop:

1. One log line: `runnerd: idle auto-stop session=<id> idle=<duration> slots_free=<n> slots_total=<n> active=<n> idle_exited=<n>`.
2. One unsolicited event, `state: "suspended_cold"`, through the same `fireEvent` path as
   `running`/`dead`/`child_exited`. `runnerplane` maps it to `control.StateSuspendedCold`;
   `controlapp.eventTransitions` already accepts exactly that event
   (`{running, suspended_cold} → suspended_cold`), so the control plane needs no change.

   This is the ONE event that carries no placement generation. See "the cold-resume fence"
   below: stamping the generation this runner holds would guarantee the report is fenced as
   stale from a session's second life onwards, and a fenced auto-stop leaves the row reading
   `running` over a stopped container — a session `rainier attach` then refuses to resume
   (it only resumes a `suspended_*` row) and cannot reach (no hub). Zero fences nothing, which
   is safe here specifically because the report is about the sandbox this runner holds right
   now, and a session re-placed onto a *different* runner is still fenced by the runner
   identity `ApplyRunnerEvent` checks first.
3. Two additive fields on every `FromRunner`, beside the `Used`/`Total` that already ride
   there: `active` (sandboxes up whose child has not exited) and `idle_exited` (sandboxes up
   whose child has exited). Both are counted from the registry, not the driver.
   `active + idle_exited <= used`: a warm-suspended sandbox and one still being created hold
   a slot and are in neither count. An old controld ignores unknown fields; a new controld
   reading a zero from an old runner reads the same "unknown" it has today.

### Where the fact is reset

`childExitedAt` describes *the current sandbox boot*. A cold suspend and resume restarts the
container, so the child is new; a warm pause and unpause does not, so the child is still
whatever it was. The registry therefore records `coldSuspended` when a cold suspend
succeeds, and a successful resume clears the idle bookkeeping **only** for a sandbox that was
cold-suspended. A session that is resumed, works, and finishes again is a candidate again —
by its new child's exit.

## Alternatives considered

**Stop on "no attachment", regardless of the child.** Rejected outright: a long unattended
build is the product. The child having exited is the only thing this change treats as
"finished", and it is `sessiond`'s own report, not an inference.

**An activity signal (no output for N minutes).** This is what #85 wants, and wants shared
with warm/cold suspension. Building a second, private one here would have to be unwound.

**Stop from controld instead of the runner.** controld already knows `child_exited` and
could dispatch a `suspend`. Rejected for this slice: attachment liveness is the runner's
fact (the hub is there), and a control plane that decides to stop a session it cannot see
attachments on would need the attachment feed first. #85 puts admission locally and
cold-migration centrally for the same reason.

**Delete instead of stop.** Never. Files are the user's work; `rainier stop` keeps them and
so does this.

**A `time.Timer` per session instead of a sweep.** More moving parts (cancel on attach,
reset on detach, leak on delete) for a sub-minute improvement in when the stop lands, on a
timeout measured in tens of minutes.

## Edge cases

| Case | Behaviour |
|---|---|
| child exited, no attachment, 29m59s | not stopped |
| child exited, no attachment, 30m | stopped once; slot released; files kept |
| child running, nobody attached for hours | never stopped |
| child exited, one viewer attached | never stopped, however long |
| that viewer detaches | the timer runs from the detach, not from the exit |
| `--idle-stop 0` | the loop never starts; nothing is ever stopped |
| already stopped by the operator | state is not `"running"`; never claimed, never stopped twice |
| resumed, works, child exits again | eligible again, `d` after that exit |
| two sweeps race | `claimIdle` is one locked check-and-mark; the loser sees a non-running state |
| an attach arrives as the stop fires | whoever takes the registry lock first wins. The attachment is counted from the moment the session is known to be attaching — before the websocket upgrade and before the first client frame, which crosses the client's network — so a viewer mid-handshake already prevents the stop. One that lands after the claim rides a container that is being stopped and dies with it; the client's reconnect resumes the session, which is what `rainier attach` does with a stopped session anyway. Narrowing that last window needs the attach path to be able to *cancel* an in-flight stop, which is #85's admission machinery, not this. |
| a warm suspend, a resume or a snapshot is in flight | not idle: `driverOps > 0`. Without this a sweep could turn an operator's `docker pause` — which deliberately KEEPS the slot — into a stop that releases it, or stop a container mid-`docker commit`. |
| a `docker stop` or `docker ps` hangs | bounded (30s and 5s, the same bounds this package already uses for a driver call off a background goroutine), so a wedged daemon cannot park the single sweep goroutine for good. A stop that keeps failing is retried on each sweep, with no backoff: one driver call and one log line per interval per stuck session. |
| a `Delete` overtakes an in-flight stop | the stop's rollback and its landing state are both compare-and-swap on `"suspending"`, so neither can wipe `Delete`'s `"destroying"` marker — the marker that stops the register goroutine from destroying the container a second time and reporting the session dead. |
| a session that failed to boot | never stopped; see `bootFailed` above. |
| `drv.Suspend` fails | the entry rolls back to `"running"`, exactly as `Op` does; no event, no log line, and the next sweep tries again |
| the container dies on its own first | the crash path removes the entry; a claim on a removed entry fails |
| runnerd restarts | `Recover` rebuilds entries from labelled containers with no `childExitedAt` — the exit was only ever in memory. Recovered sessions are treated as "child running" and are not auto-stopped until they report a new exit (they won't). Safe direction, and a durable activity record is #85's. |
| `sessiond`'s conn drops and it redials | the fact is kept, not reset: the redial does not restart the child, and `sessiond` re-sends only events it never delivered. |

## The cold-resume fence, and what this change does about it

A **cold resume opens a new placement generation** on the session row
(`controlapp.ResumeSession` sets `TransitionOpts.RunnerID`, and the repository opens a new
generation), but the `resume` command sent to the runner carries no `PlacementGeneration`, so
the runner keeps echoing the generation its *create* carried, and `ApplyRunnerEvent` fences an
event whose placement generation is not the row's. Every unsolicited event about a
cold-resumed session is therefore already dropped as stale today — `dead` and `child_exited`
included. That gap is not introduced here.

The consequence for *this* change would have been new, and worse than a dropped
observation: a fenced auto-stop leaves the row reading `running` over a stopped container, and
`rainier attach` on a `running` row does not resume, so the client dials, finds no hub, and
gives up. `ResumeSession` refuses a `running` row too. The user would have no way back into
their session until the runner happened to reconnect — reconciliation runs once per runner
connection, not periodically.

So the auto-stop event carries **no** placement generation (see "Announcing it"). The real
fix — carry the generation on `resume` and have the runner adopt it, which repairs `dead` and
`child_exited` for cold-resumed sessions too — is a separate change across the protocol and
the control plane, and is recorded as a follow-up on the PR.

## Other known limits

- **A half-open viewer conn holds a session open.** `internal/relay` has no keepalive and
  `AttachClient` sets no read deadline, so a viewer whose laptop lid closed counts as attached
  until TCP gives up. Safe direction (never stops a watched session) but it is the one input
  this feature trusts absolutely, and a liveness signal on attachments would be worth having.
- **`active + idle_exited` is not an arithmetic partition of `used`.** It is usually less, but
  `register`'s hub-death tail deliberately keeps an entry as `"running"` when the driver
  cannot say whether its container survived, and such an entry is counted while `docker ps`
  no longer counts it. A consumer must read the pair as what this runner believes it holds.
- **`cmd/runnerd`'s wiring is not covered by a test** (the package has none, and `main` is not
  factored for one). Deleting the `go s.RunIdleStop(...)` line would leave the suite green.

## Verification

- Unit, table-driven, on a fake clock: every row of the edge-case table above that is a
  decision (`internal/runnerd`, `idlestop_test.go`).
- End to end through the real runner code path with the fake driver and the existing
  Docker-free sandbox harness: create → register → `child_exited` → sweep → the fake
  container is cold-stopped, its volume is untouched, capacity drops by one, and the
  `suspended_cold` event with the two counts arrives on the controld connection.
- A regression test that an attachment open across the sweep prevents the stop, and that the
  timer runs from the detach.
- `runnerplane`: the new event state reaches the fleet service as `suspended_cold`.
- `make verify`.

No test needs Docker: `internal/driver.Fake` plus `internal/runnerd`'s in-process
`/register` sandbox is the whole path below the driver.

# Idle auto-stop and `rainier exec`: counting a live exec as activity

Status: implemented on `feat/rainier-exec` (PR #90), round 3.
Related: [`rainier-exec.md`](rainier-exec.md) (the `--detach` section),
[`runner-idle-stop.md`](runner-idle-stop.md) (the rule and the facts the runner keeps).

## Problem

Two features landed on one branch without meeting each other.

`runnerd`'s idle auto-stop stops a session whose **agent child has exited** and that has had
**no attachment** for `--idle-stop` (default 30m). Its predicate, `sessionEntry.idleFor`,
reads five facts: `state`, `attachments`, `childExitedAt`, `driverOps`, `bootFailed`.

`rainier exec` runs a command inside that same live sandbox. Nothing in `runnerd` ever
learns that an exec exists. `sandboxexec.Runner.LiveCount` has no consumer outside
`internal/sandboxexec`, and `exec_started`/`exec_exit` are *attachment* messages on an
attachment's own stream — never control events.

An **attached** exec is invisible to `idleFor` too, but it is accidentally covered: it opens
an attachment over the session's hub (it shares `dialAttachBack`), so `attachments > 0` and
the session is not idle. A **detached** exec is covered by nothing at all. So:

> agent child exited → caller of `rainier exec s --detach -- claude --continue` hangs up →
> 30 minutes later the sweep cold-stops the session → `docker stop`'s SIGTERM reaches
> `sessiond` → `onShutdownSignal` → `KillAll()` → the detached run is dead.

That is the feature's own motivating case (`rainier-exec.md`, "Detached exec": *"an
unattended run whose supervisor has to sit on a socket for six hours is not unattended"*),
killed by the other feature's default. It also contradicts `idlestop.go`'s own stated rule —
*"a session whose child is still running is never stopped, however long nobody has been
attached (a long unattended build is the product)"* — because from the user's point of view
a detached exec **is** the long unattended build; it simply is not the process `sessiond`
calls its child.

A reviewer reproduced the stop end to end: detached exec running, `child_exited`,
`RunIdleStop(1ms)` → the session is suspended while the command is live. The process
survived only because the fake driver sends no SIGTERM, which `docker stop` does.

Related: `registry.counts()` reports such a session as `idle_exited`, so the runner's
capacity line tells the control plane that a box running detached commands has no agents in
it.

Neither design doc mentions the other (`runner-idle-stop*.md` contains no "exec";
`rainier-exec.md` contains no "idle auto-stop"), and no test covers the pair.

## Scope

In scope:

1. **Count a live exec as activity.** `sessiond` reports exec liveness to `runnerd`; the
   runner keeps it on the entry; `idleFor` treats it exactly like an attachment; `counts()`
   reports such a session as `active`; the idle clock restarts when the last exec ends.
2. A paragraph in each design doc naming the other feature.

Out of scope, deliberately:

- **Reporting exec liveness to `controld`.** Attachment liveness is already the runner's
  fact and is not sent upstream either (`runner-idle-stop.md`, "Alternatives considered":
  *"Stop from controld instead of the runner"*). The one upstream-visible change is the
  `active`/`idle_exited` split, which already rides every message.
- **A per-exec idle timeout.** "No exec has run for N minutes" is the activity signal
  [#85](https://github.com/tokencanopy/rainier/issues/85) wants, shared with warm suspend.
  A second private one here would have to be unwound.
- **Listing execs.** Still not built, for the reasons `rainier-exec.md` gives.

## The shape

### One control event, `exec_count`

`sessiond` sends `ControlEvent{Kind: "exec_count", Live: N, Seq: S}` up the existing control
channel on every transition of the live-exec count, and once more on every (re)connection.

**An absolute count, not `exec_started`/`exec_ended`.** Either would work if delivery were
perfect. It is not: `offerControl` drops on a full queue and `appendPending` drops the oldest
when the pending queue is at its cap, both by design, because an unbounded queue in a process
that outlives everything else (spec §10) is the worse failure. A *delta* that is dropped is
wrong forever — the runner's counter is permanently off by one, either pinning a finished
session out of auto-stop for the life of the runner or, worse, under-counting and stopping a
session with a live command in it. An *absolute count* is self-correcting: the next
transition, or the next reconnection, states the truth again and overwrites whatever was
lost.

**The report goes to a mailbox of its own, not onto the shared event queue.** `offerControl`
drops the *arriving* payload when its channel is full and `appendPending` drops the *oldest*
when the pending queue is at its cap. Neither rule is wrong for the events they were written
for; both are wrong for an absolute count, where the arriving report is the only true one —
and a dropped `live:1` with no further transition coming is the cold stop this whole change
exists to prevent. So a live-exec report waits in a one-slot mailbox that **replaces** what is
in it, and `serveConn` sends it straight down the connection rather than queueing it: a count
is only ever about the connection in hand. It also keeps exec traffic from competing with
`child_exited` for the pending cap, and that exit has no second chance at all.

**Every connection restates the count, as its first message.** A transition that happened
while there was no connection is genuinely lost — the mailbox holds one report and only a
live conn drains it — so the dial is where the two ends are re-synchronised. That is what
makes "an absolute count is self-correcting" true rather than merely likely.

**`Seq` is a monotonic per-`Runner` counter, assigned under the same lock that changes the
count.** The observer is invoked *outside* that lock — calling a callback under a mutex the
callback could re-enter is the kind of deadlock nobody finds twice — so two transitions can
reach the queue in the wrong order. Without a fence, a `reserve`→1 report landing after a
`release`→0 report would leave the runner believing an exec is live forever. `runnerd`
applies a report only when `Seq` is strictly greater than the last it applied, so a reordered
report is dropped rather than believed.

One thing a refused report still tells the runner, and the registry acts on it: a stale
`live > 0` arriving at an entry that believes nothing is running says *work happened, and a
report already applied says it is over*. `runnerd` routes every control frame on a goroutine
of its own, so a command short enough to start and finish inside that window has its two
reports applied end-first — and the end, applied against a count of zero, would stamp
nothing. The idle clock therefore starts at the refusal: late, never early, which is the
direction every other decision on this path takes.

**The boot-epoch fence is the one every other control event carries.** `routeControl` already
receives the epoch the `/register` conn *read*, and the registry refuses any event naming a
different one. A buffered `exec_count` drained by a stalled hub read loop after the sandbox
has been stopped, resumed and re-registered names the previous epoch and is refused — the
same guard, for the same reason, as `child_exited`'s.

**Additive both ways.** `Live` and `Seq` are new `omitempty` fields on `relay.ControlEvent`;
a peer that sets neither writes the bytes it has always written (`TestControlEventWireShape`
still passes). An old `sessiond` never sends `exec_count`, so `liveExecs` stays 0 and the
runner behaves exactly as it does today — including the attached-exec case, which still
counts through its attachment. A `runnerd` older than the event logs `unknown control kind`
and drops it, which is what that arm has always done.

`Live: 0` is the meaningful "the last exec ended" report and it is `omitempty`: it puts no
`live` on the wire and decodes back to 0, the same trick `child_exited` already uses for
`rc: 0`. `Seq` is always ≥ 1, so it is never omitted on a real report.

### The facts the runner keeps

Three fields added to `sessionEntry`, read and written under the registry lock like every
other mutable field:

- `liveExecs int` — how many commands this session is running right now, detached ones
  included, as of the last report the runner applied.
- `lastExecEndedAt time.Time` — when the count last fell to zero. It is to an exec what
  `lastDetachAt` is to a viewer.
- `execSeq uint64` — the highest `Seq` applied, the reordering fence above.

### The rule

`idleFor` gains one clause and one term:

- not idle while `liveExecs > 0` — exactly the `attachments > 0` clause, for exactly the same
  reason;
- the idle clock starts at `max(childExitedAt, lastDetachAt, lastExecEndedAt)` — so the
  timeout runs from the last exec's end, the same rule as the last detach.

`counts()` reports a session with `liveExecs > 0` as `active` even though its child has
exited: the box is running a command, and "idle_exited" would be a confident lie of exactly
the kind that function's comment already refuses to tell.

### Where the fact is reset

`resumed(id, restarted=true)` on a parked entry clears all three, beside `childExitedAt` and
`lastDetachAt` and for the same reason: `docker start` gives the container a new process
tree, so the old sandbox's execs are gone and its `sessiond` — a new process — starts its
`Seq` at 1 again. Not clearing `execSeq` there would make every report from the new sandbox
look stale and be refused for the life of the entry. A warm resume (`docker unpause`) clears
nothing, because nothing restarted: the same `sessiond` keeps counting from where it was, and
the warm-suspend path has already killed its execs and reported 0.

### The sandbox end

`sandboxexec.Runner` gains one observer, set once before the runner serves
(`Runner.ObserveLive`). `reserve` and `release` are the only two places the live set changes;
each computes the new count and the new sequence under the lock it already takes, and calls
the observer after releasing it. `Runner.LiveReport()` returns the current count under a
fresh sequence, which is what `dialLoop` sends on every connection so that a report lost
while `runnerd` was away is repaired by the reconnection rather than waiting for the next
command.

`sessiond` wires that observer to `offerControl(events, …)`, the same never-blocking queue
`child_exited` uses.

## Alternatives considered

**Have `runnerd` treat an exec attachment as an attachment.** It already does, for the
*attached* case — that is the accident this change stops relying on. It cannot work for the
detached case, because a detached exec's attachment closes as soon as the pid is reported:
that is the whole meaning of the flag.

**Make the sweep ask the sandbox.** A session-RPC "are you busy" at sweep time keeps no state
on the runner. Rejected: it puts a network round trip inside the registry lock's decision
path, it answers nothing for a sandbox whose conn is momentarily down (and "no answer" would
have to mean "not idle", which is the pin we are trying to avoid), and it is a new RPC method
for a fact one bit of existing plumbing already carries.

**Let the exec hold a real attachment for its lifetime.** Would fix the predicate without any
new event, but it is a lie on the hub: a detached exec has no client conn, so the entry would
count an attachment nothing can close, and every `attachments`-based assertion in the runner
and every hub cleanup path would have to learn about a phantom. The bug this change fixes is
exactly the cost of an accidental coupling; another one is not the fix.

**Deltas (`exec_started`/`exec_ended`).** See above: a dropped delta is wrong forever.

## Edge cases

| Case | Behaviour |
|---|---|
| child exited, one **detached** exec running, 10 hours | never stopped |
| child exited, one **attached** exec running | never stopped — by the exec's own count now, and by its attachment as before |
| the last exec ends at T, child exited long before | stopped at T + `--idle-stop`, not earlier |
| two execs, one ends | still not idle; the clock starts when the **second** ends |
| a viewer detaches at T1, the last exec ends at T2 > T1 | the clock starts at T2 |
| `exec_count` naming a previous boot epoch | refused; the current count is untouched |
| an `exec_count` that arrives out of order (lower `Seq`) | refused |
| an old `sessiond` that never sends `exec_count` | exactly today's behaviour |
| `exec_count{live:1}` then the sandbox conn dies for good | the entry keeps `liveExecs = 1` and is never auto-stopped; an operator's stop and a delete are unaffected. Safe direction, and the same shape as the existing "a half-open viewer conn holds a session open" limit |
| a cold resume | all three fields cleared; the new sandbox's `Seq` starts at 1 |
| a warm suspend and resume | the quiesce killed every exec, so there is nothing to clear. The `live:0` report is not *guaranteed* to be on the wire before the freeze — `KillAllAndWait` waits for the live set to empty, and the observer runs after the lock that emptied it — so the runner may learn of it only on the unpause or the next dial. Late, never early, and the restatement on the next connection is what closes it |
| a report dropped while there is no connection | the mailbox holds one report and only a live conn drains it, so the drop is real — and `serveConn` restates the current count as the FIRST message of every connection, which says what is true then rather than replaying what was true before |
| a `--detach` exec accepted between `claimIdle` and `docker stop`'s SIGTERM | killed, after its caller was handed a pid and exited 0. The window is the driver call, and reaching it at all needs the full `--idle-stop` of prior idleness, so the exec was started against a session that had been doing nothing for half an hour. Not closed here: closing it means a reservation the sweep respects, which is #85's admission work |

## Rollout

**Roll the runners before the session image.** A `runnerd` that predates `exec_count` logs
`unknown control kind "exec_count"` once per transition per session and drops it, which is
correct — the session simply behaves as it does today — but it is log noise proportional to
how many commands the fleet runs, and it means idle auto-stop can still cold-stop a detached
run on that runner. It is an operational order, not a check: nothing in a sandbox can detect
an old runner, exactly as the README's "Roll `controld` before the runners" note says of the
layer above.

## Verification

- **The reproduction, first.** `TestADetachedExecKeepsAChildExitedSessionAlive` drives the
  reviewer's probe through the real `/register` control path: `exec_count{live:1}`,
  `child_exited`, `RunIdleStop` on a 1ms timeout, and asserts the session is *not* suspended.
  It fails on `9c20a8e` (the session is suspended) and passes after.
- Table rows in `internal/runnerd/idlestop_test.go` for each decision row above, on the fake
  clock.
- The boot fence and the `Seq` fence, as registry-level tests.
- The old-sessiond row: a session that sends no `exec_count` is stopped exactly when it is
  today, and an attached exec still holds it open through its attachment.
- `counts()` reports a session running a detached exec as `active`.
- The sandbox end: a real `sandboxexec.Runner` reports 1 then 0 across a detached exec, in
  order, with strictly increasing sequences.
- `TestControlEventWireShape` (unchanged bytes for every existing event).
- `make verify`, `go test ./internal/e2e/ -race`, `internal/runnerd` and
  `internal/sandboxexec` under `-race -count=10`.

---

# Two smaller findings on the same branch

## `Hub.readLoop`'s per-client write is unbounded

`internal/relay/runnerd_side.go`'s `readLoop` demultiplexes every frame for every attachment
on one session conn on **one goroutine**, and forwards each to its client with
`client.Write(h.ctx, …)` — a write bounded only by the hub's own context, i.e. by the life of
the session. One stuck client therefore stalls every other attachment on that conn, and the
session RPC with them.

The line is pre-existing, but `exec` is what makes it reachable. A terminal stream never
backpressures for long: `attachplane` force-detaches a viewer that exceeds its write budget.
An exec stream blocks **by design** — that is its flow control — and is bounded downstream
only by `attachplane`'s ~20s per-frame exec budget (`defaultExecWriteBase`). So one slow exec
caller buys ~20 seconds of stalled viewers per frame, with nothing in `internal/relay`
enforcing anything.

**Fix.** Each per-client write in `readLoop` gets a deadline of its own, derived from
`h.ctx`, and a client that exceeds it is dropped from the demux and closed — precisely what
the existing write-error branch already does. The change is that a *slow* client is now
treated like a *failed* one instead of being waited for indefinitely.

The budgets are **kind-aware and sit just ABOVE `attachplane`'s**, and that ordering is the
whole design. A first attempt used a flat five seconds on the grounds that a viewer
`attachplane` would drop anyway is not worth waiting for. That was wrong in both halves:
`defaultClientWriteBase` is **60 seconds** for a terminal viewer (plus 64 KiB/s of byte
allowance, so up to ~316s for the largest frame `attachReadLimit` permits), the 20s figure is
the *exec* base only, and the plane's splice is synchronous — `runner.Read` then
`client.Send` — so a human legitimately inside their 60–316 seconds backs the runner socket
up, and a 5s hub bound would have dropped them mid-scrollback. That is exactly the outcome
the plane spends those seconds to avoid, decided on a hop that cannot see it is deciding it.

So: 70s base for a terminal attachment, 25s for an exec one, both plus the same 64 KiB/s
allowance. Each is strictly above the plane's, so **this hop never overrules the plane** —
`TestTheHubNeverOverrulesThePlane` states that as an invariant, in the same shape
`internal/relay`'s existing `TestTheWaitAndTheWriteHaveSeparateBudgets` already uses.

What the bound buys, then, is not a tighter decision but a decision at all: the local
`/attach` front has no plane below it and was unbounded; a wedged plane, or a host that
mounts a stream without those budgets, can no longer park this runner's demux for the life of
the session; and the number is now stated in the package that pays for the wait.

It remains a **mitigation, not a cure**, exactly as `attachplane/stream.go`'s own comment
says: the cure is a writer per attachment rather than one per conn. That is a change to the
backpressure an exec stream depends on — an exec stream blocks by design, and a per-client
queue either bounds memory by dropping a caller that is merely slow, or does not bound it at
all — so it is its own piece of work and not this one.

**Test.** A stuck client on attach id 1 and a viewer on id 2, on injected millisecond
budgets: the viewer's frame arrives without waiting, and the stuck client is closed and
removed. It fails without the bound. `TestTheProductionHubIsBounded` pins the wiring
separately, because passing a zero through `NewHubWithControl` would restore the old
behaviour while leaving every budget-injecting test green.

## The emit-ordering guarantee is pinned only probabilistically

`attachment.emit` checks `closing` in a select of its own **before** the send/`closing`
select. Once a kill has begun both arms of the second select are ready — the outbox has room
and `closing` is shut — and Go picks uniformly, so without the first check an exec killed by
its session reports the SIGTERM that killed it as an ordinary `exec_exit` about half the
time. The contract is 125 with a sentence naming which end it was, not a 143 a script reads
as the command's own answer.

Deleting that select survives the entire `internal/sandboxexec` suite; it is caught only by
`TestStoppingASessionEndsAnInFlightExec` at e2e, and only 4 times in 10 runs.

**Fix.** A unit test that closes `closing` on an attachment whose outbox still has room and
calls `emit` many times, asserting every call is refused and nothing lands in the outbox. One
call cannot distinguish the two implementations — with both select arms ready the choice is a
fair coin, which is the defect — so the assertion is over a run of them; at 200 iterations the
mutant survives with probability 2⁻²⁰⁰, which is deterministic in every sense that matters.

## Nits

- The doubled doc comment at `internal/runnerd/runnerd.go:1303`.
- `internal/execio/execio.go:467` says "All four" over a six-case list.
- `session_ending` (and `too_many_execs`, `too_many_detached`, `stdin_overrun`) reach
  `docs/cli-v0-contract.md` §3.9 as prose but not as the literal words
  `TestExecReasonsAreClosedBothWays`'s failure message tells a reader to add. Added, with a
  check that reads the document so the next word cannot drift out of it silently.

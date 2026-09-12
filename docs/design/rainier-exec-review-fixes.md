# `rainier exec` — the review fixes

Companion to [`rainier-exec.md`](rainier-exec.md), which is the design of the
feature. This document is the design of the **corrections** an independent
Opus review of that implementation asked for: four blocking or
should-fix-before-merge defects, seventeen should-fixes, and a handful of
one-line nits. It exists because several of the fixes are not one-liners —
three of them change a protocol, a lifetime rule, or a package's public
surface — and because the review's own framing ("this mutant survives the
entire suite") is a statement about what the *tests* have to become, not only
about what the code has to become.

**Status: implemented**, on the same branch as the feature.

## Problem

The reviewer executed every guarantee it could and found the security,
lifecycle, compatibility and controller-ownership stories clean. What it found
instead falls into five groups, and grouping them is what makes the fix set
readable:

1. **The suite does not hold the code down.** Two real-process lifetime tests
   were testing a corpse — the subprocess target was `select {}` in a test
   binary with no timer goroutine, which the runtime's deadlock detector kills
   on some platforms — so the process-*group* rule, the SIGTERM→SIGKILL
   escalation and the detached-exec lifetime had no witness. Four further
   guarantees had mutants that survived the whole tree, three published
   constants were read back out of the code under test, and the one real
   backpressure test turned every regression into a green `SKIP`.
2. **Two resource defects that exec turns from rare into per-command.** A relay
   exit that writes `FrameClose` without closing the client socket leaks one fd
   per exec; `kill()` is documented idempotent and is not, so a stdin overrun
   can arm two thousand kill timers.
3. **A lifetime rule the shipped help text asserts and the code does not.**
   The default `rainier stop` is a *warm* suspend — `docker pause` — and
   `KillAll` was wired only to sessiond's SIGTERM handler, which a paused
   sessiond never receives. A detached exec was therefore frozen and resumed,
   not reaped, while `rainier exec --help` says "it dies with its session when
   it is stopped".
4. **Two unbounded waits on the exec path.** A slow exec caller holds the relay
   conn's single writer and stalls every viewer and the session RPC behind it;
   and `reap.AwaitStatus` can park a waiter forever when the reaped-outcome
   table evicts its entry, which exec is what makes reachable — before exec,
   exactly one pid was ever awaited.
5. **Documentation that drifted from the code it ships with**, in the places
   the compatibility and release stories depend on.

## Scope

In: every fix the review asked for, each with a test that fails without it.

Out: the one finding the review recorded as out of scope (`controllerKeeper.Claim`
asking the policy with the runner's dial-back context, which is `b22b225`/#84
and not this branch), and the two design gaps the feature's own open questions
4–6 already record as deliberate.

## The fixes, and the decisions inside them

Most are mechanical. Six are not.

### A detached exec dies on a warm suspend (finding 3)

The rule the design and the CLI both state is that an exec's lifetime is
bounded by its **session**: it outlives its caller, never its session. The
implementation delivered that for a cold stop and a destroy (both of which
deliver SIGTERM to sessiond) and not for the default `rainier stop`, which is
`docker pause`.

Two options, and the first is taken:

- **Kill execs on the warm transition too.** The rule stays what is written and
  what a user is told, and `--detach` keeps a bound a script can rely on.
- Change the rule to "frozen with the sandbox on a warm stop". Cheaper, but it
  makes `--detach`'s lifetime depend on a flag most callers never pass, and it
  would mean a `claude --continue` resuming hours later inside a session
  somebody stopped on purpose.

Mechanism: runnerd sends a **`suspending` control event** down the session's
relay conn immediately before `drv.Suspend(ctx, handle, warm)`, and the sandbox
answers **twice** — `suspend_ack` at once, `suspend_ready` when the processes
are gone. sessiond answers by calling `Runner.KillAllAndWait`, which closes
every exec attachment; the relay's forwarder then sends the `FrameClose` that
ends each in-flight exec's stream, so the caller sees the connection end with no
exit status — exit 125 — and `execEndedSentence` re-reads the session and says
*"the session was stopped before the command reported an exit status"*, which is
the branch the amendment intended and which was unreachable on the default path.

Three things about that shape are answers to ways a simpler version was wrong,
and each was found by executing it rather than reading it:

- **Two answers rather than one**, so the two waits can be different lengths.
  runnerd cannot tell a sandbox that is working from one that predates the
  notice, and a session keeps the `sessiond` it booted with for life — so a
  single answer made every session created before exec shipped pay the full
  budget on every warm stop, forever. The ack is immediate; hearing none means
  "old sandbox, carry on" after two seconds instead of twelve.
- **A nonce on all three, echoed back.** sessiond's answer can outlast
  runnerd's budget (its own quiesce waits out a kill grace, and the send behind
  it queues on a conn writer an exec may be holding), so a straggler from a
  suspend that already gave up released the NEXT one — freezing a container
  mid-kill and leaving a signal pending in the freezer cgroup, which is the
  failure the handshake exists to prevent.
- **A latch on the runner**, not just a sweep. The relay conn stays up for the
  whole budget, so an exec opened mid-quiesce was spawned, never signalled, and
  frozen alive. `reserve` refuses once the sweep has begun, with
  `exec_error{session_ending}`.

Everything that can go wrong ends in "pause anyway": no hub, an old sandbox, a
conn that died, a cancelled dispatch. The last two end the wait at once rather
than spending it.

Compatibility, both directions, is why this is an event and not a new required
handshake:

| Pairing | Behaviour |
|---|---|
| new runnerd + **old sessiond** | The frame is an unknown control kind; sessiond logs and drops it. runnerd's ack wait expires after its budget and the pause proceeds — exactly today's behaviour, which is the one the fleet has now. |
| old runnerd + **new sessiond** | No frame is ever sent. Nothing changes. |

The wait is bounded rather than best-effort because `docker pause` uses the
freezer cgroup: a SIGTERM delivered and then frozen is a signal that is still
pending on resume, which is the failure this is fixing rather than a different
spelling of it.

Cold suspend and destroy are deliberately untouched: `docker stop` already
delivers the SIGTERM that runs the existing handler, and a destroyed
container's processes are gone by definition.

### The reaper's two new failure modes (finding 7)

`internal/reap` is a drive-by in the exec PR — it bounds a pre-existing orphan
leak — and it stays in the PR rather than being split out, because **exec is
what makes every pid awaited**. Before this branch, exactly one pid (the
agent's) was awaited, once, at boot. After it, every exec awaits by pid for the
life of a session, which is what turns two latent bugs into reachable ones:

- **An evicted entry parks its waiter forever.** `AwaitStatus` loops on
  `cond.Wait()` with no escape, and `sandboxexec` has no fallback — `ok ==
  false` means "no reaper", so an exec whose record was evicted would never
  report a status and would hold one of the eight slots indefinitely. Fix: an
  **eviction tombstone**. A record dropped by the bound leaves a marker; a
  waiter that finds one returns `(Status{}, false)` and the caller's
  `cmd.Wait` takes over, which is the same fallback a host build already uses.
- **A pid wraparound can hand an exec somebody else's status.** `codes[pid]`
  carries no epoch, so a fresh exec whose pid matches a stale *unclaimed*
  orphan entry reads that orphan's outcome. Fix: every record is stamped with a
  **monotonic sequence**, and a caller takes a `reap.Mark()` immediately before
  `cmd.Start()`. `AwaitStatus(pid, mark)` ignores any record older than the
  mark — an entry that existed before this child did cannot be this child's.

This is Linux-only code and the development sandbox is Linux, so both are
tested here rather than reasoned about.

### A slow exec caller no longer stalls the session (finding 8)

The exec forwarder's `write()` takes the relay conn's single writer and uses
the `ServeSession` context, which lives as long as the session. A caller
draining at a couple of kilobytes a second never trips the plane's 20-second
budget and holds that writer indefinitely; behind it, the agent's terminal
output and the session RPC wait.

`connWriter.writeWithin` is the bound — two of them, because they answer
different failures and only one is free:

- **Acquiring the writer**, at thirty seconds. Nothing has been written when it
  expires, so the conn is untouched and the cost is this exec alone. It is
  longer than the plane's own per-frame budget on purpose: an exec must not
  lose its socket merely because some other peer on the conn is being dropped.
- **The write itself**, at sixty seconds. A WebSocket frame cannot be abandoned
  half-written, so the transport's answer to an expired write context is to
  close the conn — which is the right answer only for a conn that is not moving
  at all, where the plane's budget has already come and gone and nothing will
  make `ServeSession`'s `Read` fail. It is set an order of magnitude above what
  a slow peer can reach: one `readChunk` is 58,320 wire bytes after **two**
  base64 hops (43,724 as `ServerMessage` JSON, then base64 again inside
  `relay.Frame`), so sixty seconds is under a kilobyte a second.

A first attempt at this used **one** five-second budget shared between the wait
and the write, and it was worse than the defect: a caller draining at 64 KiB/s
— a rate the plane explicitly blesses — tore the session's conn down every five
seconds, and a frame that spent most of the budget waiting got the remainder to
write, so contention turned directly into teardown.

A dropped exec is TOLD. The forwarder sends its `FrameClose` before returning,
on the acquire path especially: nothing was written, so the conn is alive and
nothing else will ever close that client — the hub's cascade fires only on conn
death and the plane's budget only on a write it never gets to make. Without it
the CLI waits forever for an exit status that is not coming.

What this does **not** do is take the writer away from a peer holding it inside
`conn.Write`; nothing can, short of closing the conn. The wedge itself is
bounded by the plane dropping the slow caller. The cure remains a writer per
attachment, which is the feature's open question 4.

### `MaxDetached` gets its own word (finding 12)

Four concurrent detached execs is a real sub-cap of the eight-exec cap, it was
documented nowhere, and the fifth one was refused with the sentence *"this
session is already running as many commands as it may"* with four of eight
slots free. It gets its own reason word, `too_many_detached`, its own CLI
sentence, and a line in both the design and §3.9. The vocabulary test is
changed to iterate the **code's** table rather than the test's `want`, so the
next word cannot drift out of the documented set the way `no_answer` and
`stdin_overrun` did.

### `exec.v1` can no longer take a runner out of the fleet (finding 2)

`buildCapabilities` appended `exec.v1` unconditionally; `runnerplane` caps an
announce at 32 capabilities and refuses the **whole registration** past it. An
operator already passing 32 `--capability` flags would announce 33 after the
first rollout step and never reconnect — a worse failure than the 501 the
append exists to avoid, for what the design itself calls "a cheap pre-check,
not the fence". It is appended only when there is room, and the drop is logged.

### The isolation claim gets a guard (finding 20)

"Nothing in this package imports `internal/session`" is the load-bearing
sentence of the whole "second kind, not a flag" design, and adding that import
failed no test. A small `go/parser` test over the package's own files asserts
it, in the package where the claim is written.

## Edge cases

- **A suspend frame that arrives while an exec is mid-spawn.** `arm` already
  handles a kill that lands between the slot being taken and the process
  existing; the suspend path reaches the same `kill()`.
- **A suspend frame on a session with no execs.** `KillAll` over an empty table
  is a no-op and the acknowledgement is immediate.
- **A second suspend frame.** `kill()` is now genuinely idempotent (finding 9),
  so a repeated suspend signals nothing twice.
- **An exec whose record is evicted while it is also the child `cmd.Wait` can
  reap.** The tombstone hands the outcome to `cmd.Wait`, which on Linux under a
  reaper returns `ECHILD` — so the status is "no status", exit 125, which is
  honest and is what the caller would have got from a hang, sooner.
- **A pid mark taken before `cmd.Start` on a spawn that fails.** No record is
  ever awaited; the mark is discarded.
- **`writeWithin` on a conn whose context is already cancelled.** Returns at
  once, drops the exec, and the outer read loop's own cleanup runs anyway.
- **Post-EOF stdin (finding 10).** `pumpStdin` returns on EOF and nothing drains
  the queue afterwards, so 8 MiB of late stdin could kill a *finished* command
  as `stdin_overrun`. EOF is recorded and later stdin is dropped, which is what
  a closed pipe does.
- **A `--tty` exec's EOF.** Unchanged: a pty has one stream, so EOF is the
  terminal's own EOT and there is no descriptor to close.

## Verification

Every fix lands with a test that fails without it. The ones that are not
obvious:

| Fix | The test, and what fails without it |
|---|---|
| 1 | The subprocess target sleeps rather than parking every goroutine, and a new `grandchild` case (`sh -c 'sleep 300 & sleep 300'`) gives the **minus sign** a witness: signalling the leader alone leaves the grandchild alive, which the test polls `rec.calls()` for. |
| 2 | `TestAgentAnnouncesItsCapabilities` gains the 32-capability boundary case. |
| 3 | Through the real path: a sessiond fixture receives the suspend frame and answers both times with the right nonce; runnerd's tests pin the ORDER against the driver, that an ack alone does not release the pause, that an old sandbox pays only the short budget, that a stale answer cannot release the next suspend, and that a dead conn or a cancelled dispatch ends the wait at once. e2e drives the real stop route. |
| 4 | `internal/e2e`'s `TestExecLeaksNoDescriptorsOrGoroutines`: fifty cycles of each shape that ends an exec differently — the sandbox closing it, and the CALLER hanging up, which is the exit that leaked. Reverting the line reports **+50 descriptors**; with it, +0. A unit test with a fake catches the missing call; only a census catches "and it really is fd-for-fd clean end to end". |
| 5 | One assertion per surviving mutant: the `--json` stream swap, the `KillAll` wiring, the detached slot release, and `ExecClientStream`'s budget. |
| 7 | Linux-only, executed here: an eviction with a waiter parked on the evicted pid, and a stale unclaimed record a later mark must ignore. |
| 8 | A wedged exec frame with a viewer and a `ControlSender` behind it, asserting both get out. |
| 9 | Two thousand post-overrun messages produce exactly one group signal. |
| 13 | The seeded non-exec runner is dialled, and the route's 501 is pinned on the route rather than on the pure decision alone. |
| 14 | `fakeSessiond` records **every** client frame regardless of kind, so a pre-handshake stdin leak cannot route into a branch the assertion does not watch. |
| 16 | The published constants are literals in the test, and one stdin case writes to just under the bound asserting *no* `stdin_overrun`. |
| 20 | The import guard itself. |

Gates: `make verify`, repotest on both adapters with zero skips,
`go test ./internal/e2e/ -race`, the exec packages at `-race -count=10`, and
`GOOS=darwin go vet ./...` — because the reap tests reach into
`reap_linux.go`'s own table and belong in a linux-tagged file, which is the
kind of break a Linux-only gate cannot see. The census is no longer a gate
somebody has to remember to run; it is a test.

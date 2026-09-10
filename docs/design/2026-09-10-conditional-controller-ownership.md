# Conditional controller ownership for terminal attachments

Status: proposed · 2026-09-10 · core (`github.com/tokencanopy/rainier`)

## Problem

Several clients can already attach to one session and all of them receive
output. Nothing decides who may *type*. Every attach advances the controller
generation (`controlapp.AttachmentService.grantGeneration` calls
`NextControllerGeneration` unconditionally), nothing ever reads that
generation again, and `internal/session.Session.Stdin` writes any viewer's
bytes into the PTY. Control therefore follows whoever attached last, and a
superseded client keeps full write authority — it can type and it can resize.

With one laptop that is invisible. With a laptop and a phone — the browser
story the web plan builds next — it is two people fighting over one keyboard,
with no way to tell who has it.

The goal: **at most one controller per session at any moment**, everyone else
a viewer; handoff explicit and atomic; every input fenced by a generation, so
a late frame from a former controller is discarded where it would execute
rather than typed into somebody's shell.

## Scope

In scope, core only:

- a durable controller lease (generation, holder, expiry) on the session row,
  with an atomic compare-and-advance and an atomic renew in every repository
  adapter, and the conformance case that proves the race;
- a negotiated terminal handshake: a client that advertises the capability is
  told its mode and generation before the first snapshot byte, and can claim
  and release control on the attach stream;
- generation and viewer fences applied **where input executes** — in
  `sessiond`, at the PTY — not only at the plane;
- lease expiry and heartbeat renewal on the stream's existing keepalive;
- CLI defaults, `--view`/`--take`, one take-control key binding, an `info`
  line, and honest conditional reconnect;
- an additive `controller` object on the `v0wire` session view.

Out of scope: the browser transport and attachment tokens (plan task 7),
Cloud's gateway negotiation (task 7), collaboration ACLs and grants (the
tenancy contract's §6.1 — this task implements only §6.3, the lease), and any
change to who may attach at all, which remains `control.Authorizer` plus
`controlapp.AttachmentPolicy`.

## Model

`control.Session` already carries `ControllerGeneration`. It gains the lease:

```go
ControllerHolder         string    // opaque attach identity; "" when vacant
ControllerLeaseExpiresAt time.Time // zero when vacant
```

`ControllerHolder` is 16 hex characters from `crypto/rand`, minted per attach.
It is not a user, device, account, or session identifier and never leaves the
control plane except as the derived boolean `controller.held`.

A lease is **live** when `ControllerHolder != ""` and
`ControllerLeaseExpiresAt` is in the future. `control.ControllerLeaseTTL` is
30s and `control.ControllerHeartbeatInterval` is 5s: six renewals per lease,
so a single lost renewal never drops control.

### The repository port

`control.SessionRepository` gains two methods. Both are one statement.

```go
// CompareAndAdvanceControllerGeneration advances id's controller generation
// from expected to expected+1 and vacates the lease, atomically.
// ErrStale when no row matched. ErrInvalid on an empty workspace.
CompareAndAdvanceControllerGeneration(ctx, ws, id, expected uint64) (uint64, error)

// RenewControllerLease installs or extends l on id, fenced by l.Generation.
// ErrStale when the row's generation has moved or another holder holds it.
RenewControllerLease(ctx, ws, id, l ControllerLease) error
```

```sql
UPDATE sessions
SET controller_generation = controller_generation + 1,
    controller_holder = '', controller_lease_expires_at = NULL, updated_at = now()
WHERE workspace_id = $1 AND id = $2 AND controller_generation = $3
RETURNING controller_generation;
```

Zero rows is one answer, `ErrStale`: the row is gone, or the generation moved.
The caller's remedy is identical in both cases — re-read and decide again —
and distinguishing them would cost a second statement that could only report
a third state that was also true a moment ago. Tenant scoping is unchanged:
`workspace_id` is in the predicate exactly as it is on every other method, and
an empty workspace is `ErrInvalid` before any SQL runs.

`NextControllerGeneration` stays, unchanged, for the unnegotiated path.

**Release is compare-and-advance.** A controller that leaves — a clean detach,
an explicit `release` — advances the generation, which both vacates the lease
and fences everything it had already sent. There is no separate release
primitive to keep consistent with the CAS.

**Expiry is passive.** Nothing sweeps leases. A lease whose expiry has passed
is simply not live, so the next attach claims over it. A dead client cannot
wedge control on a row nobody is reading.

### Claiming

Claiming is CAS plus renew, in that order, and it is safe that they are two
statements because *the generation is the authority and the lease is only a
hint*:

1. read the row;
2. if a live lease is held by somebody else and this attach did not ask to
   take over → viewer at the row's current generation;
3. otherwise `CompareAndAdvance(expected = the generation just read)`;
   - `ErrStale` → somebody else got there first → viewer;
   - success → controller at `expected+1`, then `RenewControllerLease`.

Two devices racing from the same generation both call CAS with the same
`expected`; exactly one row update matches; one gets `expected+1` and one gets
`ErrStale`. A renew that lands after another CAS is itself stale and fails, so
the loser cannot install a lease over the winner's generation.

`control.ControllerLeaseKeeper` is the seam the attach plane drives mid-attach
(`Claim`, `Renew`, `Release`), carried on `control.AttachTarget`. The plane
never sees a repository; `controlapp` binds the keeper to one workspace,
session and holder. A nil keeper means "this host does not negotiate", which
is exactly today's behaviour.

## Wire

`protocol/terminal` grows optional fields; every one is `omitempty`, so a peer
that sets none produces the same bytes it produces today. Generations travel
as **decimal strings** (`terminal.Gen`) because a `uint64` exceeds the
browser's exact integer range and task 7 renders this in a browser.

Client → server: `resize` gains `control:"v1"` (the capability advertisement,
on the first message only), `mode` (`control`|`view`) and `expected`.
`stdin`/`resize` gain `gen`. New types `claim {expected}` and `release`.

Server → client: new types `attached {mode, gen}`, `stale {gen}` (the current
generation, so the client can claim again from it) and
`control_changed {mode, gen}`.

Plane ↔ sandbox, over the existing relay `FrameClient`/`FrameServer` payloads
— no relay frame change: `control {mode, gen}` (client-direction) tells
`sessiond` what an attachment now is, and `control_ack {gen}`
(server-direction) is the sandbox saying it has installed it.

A client that does not advertise `control` receives exactly today's message
set. A client that advertises it receives `attached` **before** the first
snapshot or output byte.

## Fences

The fence lives at the PTY, in `internal/session`. A `Session` keeps the
controller generation it has been told about; each attachment keeps its mode
and the generation it was admitted under. Input, resize, and any bytes a
terminal sends back in answer to a query — all of which arrive as `stdin` —
execute only when:

- the attachment is **negotiated** (a `control` message arrived for it), its
  mode is `control`, and the frame's generation equals the session's current
  controller generation; **or**
- the attachment is **not negotiated** and the frame carries no generation, in
  which case it is treated as the current controller's.

That second rule is the old-plane compatibility rule, and it is safe for the
reason it is written down: under the old message set only one client could be
sending, because the old plane had no way to admit a second one as anything
else. A **new** plane always stamps the frames it forwards, including a legacy
client's, so a zero generation reaching a new sandbox means an old plane and
nothing else.

Viewers are also fenced at the plane, which drops their `stdin` and `resize`
without forwarding. The PTY fence is the one that matters for a frame already
past the relay; the plane fence is what keeps such frames off the wire at all.

**PTY size follows the controller.** `EffectiveSize` is computed over
controller attachments only; a viewer's size is recorded and ignored. A
session with no controller keeps the size it had.

### The in-flight frame

A take-over must not leave a window where the taker believes it has control
and the previous controller's already-sent keystroke still executes. Two
attachments reach the sandbox over one relay conn but on independent paths, so
the plane cannot order a displacement notice against a frame already in
flight. It therefore **does not answer a claim until the sandbox has
acknowledged the new generation**: on a successful CAS the plane injects
`control {gen}` toward the sandbox and waits for `control_ack` before sending
`attached` to the taker. Every frame from the old controller that arrives
after that ack is fenced; every frame that arrives before it arrived while the
handoff had not yet happened for anybody.

An old sandbox never acks. The plane waits `controlAckTimeout` (2s) and
proceeds without it: a new plane must require nothing an old sandbox cannot
supply. That attach is then fenced at the plane only, and says so in no
user-visible way, because there is nothing the user could do about it.

### Being displaced

The displaced controller learns two ways. Locally — both attachments on one
replica — the plane pushes `control_changed {mode:"view"}` immediately. Across
replicas, and as the durable backstop everywhere, its own heartbeat renew
returns `ErrStale` within one interval (≤5s) and the plane pushes the same
message then. Its fencing is immediate either way; only the notice can be
late.

## Read model

`v0wire.SessionView` gains one additive object, present on every session:

```json
"controller": {"generation": "3", "held": true}
```

`held` is derived — the row's lease is live — and so travels in
`v0wire.SessionDerived` beside `reachable`, keeping `RenderSession` a pure
function of its arguments. Nothing else in the JSON changes.

## CLI

`rainier attach <session>` keeps its default and its zero clicks: claim if
control is free, otherwise attach as a viewer and print one line naming that
another device has control and the key that takes it. `--view` never claims;
`--take` claims on attach. Ctrl-] still detaches, and a controller's detach
releases. **Ctrl-\\** takes control, and is intercepted only in a negotiated
attach — against an old plane it is forwarded as an ordinary byte, so nothing
about today's key handling changes for a client that never negotiated.

Reconnect presents the generation it was last acknowledged at. If it still
holds it, it resumes as controller; if somebody took over, the plane answers
`attached {mode:"view"}` and the CLI says so once and stays a viewer. There is
no auto-claim loop anywhere: the CLI claims when a person asks it to, and
never in response to being refused.

`rainier info` gains a `Controller:` line — `this device`, `another device`,
or `none` — resolved from `controller.held` and the generation this device
last recorded for that session. It needs no new wire field and discloses no
other client's identity: if the lease is live and the generation has not moved
since this device was granted it, this device is the holder.

## Alternatives considered

**Keep the lease in process memory.** It is where it used to be, and it is
wrong for the same reason placement fencing is durable: two replicas of a
control plane would each hand out "the" controller, and a restart would grant
authority twice under one generation.

**Read the row, then write the new generation.** Two attaches interleaving
between the read and the write both write generation N+1 and both believe they
are the controller. The whole guarantee is that exactly one caller advances a
generation, which is what one predicated statement gives and a read-then-write
cannot.

**Fence only at the plane.** The plane is not where input executes, and a
frame that is already past it — accepted from a controller who was displaced a
moment later — would still reach the PTY. Fencing at the plane is worth doing
because it keeps traffic off the wire; it cannot be the only fence.

**A separate release/expire primitive, and a sweeper.** Release as its own
statement has to keep its own idea of what a vacated lease looks like
consistent with the CAS's, and would leave a released controller's in-flight
frames unfenced because the generation had not moved. A sweeper adds a
background writer to every host for a fact any reader can derive from a
timestamp.

**A confirmation prompt on take-over.** Rejected, per the plan's parameters:
the other side is told, and one key press takes it back. A prompt on a phone
that a laptop cannot answer is worse than a reversible surprise.

**Generations as JSON numbers.** A `uint64` past 2^53 is silently wrong in a
browser, and the browser is the next consumer.

## Edge cases

- **Two claims from the same generation.** One `expected+1`, one `ErrStale`;
  the stale claim increments nothing. Pinned in `controlapp/repotest`, on
  every adapter, concurrently.
- **A stale renew.** Fails on the generation predicate; it cannot install a
  lease over the winner's generation or extend a lease it no longer holds.
- **Claim on a session with no lease and generation 0.** `expected = 0`
  succeeds and yields generation 1 — the existing-row migration default, so a
  session created before this change behaves like one created after it.
- **A crashed controller.** No renew, no release; after 30s the lease is not
  live and the next attach claims. Control is never stuck on a dead device.
- **Viewer resize.** Recorded, never applied. The PTY tracks the controller.
- **Last controller leaves, viewers remain.** The generation advances, no
  lease is live, and the size stays where the controller left it until
  somebody claims.
- **Session exits under a live lease.** Nothing special: the lease is a row
  column, the session is terminal, and no further attach is admitted.
- **A viewer's terminal answering a DSR query.** Arrives as `stdin`, is
  dropped by both fences, and never reaches the PTY.
- **A negotiated client against an old plane.** Never receives `attached`;
  after the first non-`attached` server message it falls back to today's
  behaviour, stops stamping generations, and stops intercepting Ctrl-\\.
- **A legacy client against a new plane.** Gets the old message set and is
  recorded as a take-over, so a negotiated client attached at the same time is
  notified and fenced. This is deliberate: a legacy client has no way to be a
  viewer, so admitting it as anything else would silently break it.

## Verification

Each of the journey's seven steps is a test, beside the package that owns it:

1. attach to an idle session becomes controller at generation 1
   (`controlapp`);
2. a second attach under a live lease is a viewer and is told so
   (`controlapp`, `attachplane`);
3. two claims from one generation → one winner, one `stale`
   (`controlapp/repotest`, concurrent, on memstore and pgstore);
4. the displaced controller is told and its in-flight frame is discarded
   (`internal/session` for the fence, `attachplane` for the notice);
5. detach releases, a crashed client's lease expires under a fake clock
   (`controlapp`);
6. reconnect within the lease resumes; reconnect after a take-over returns a
   viewer (`internal/attachio`, `cmd/rainier`);
7. a viewer's resize does not move the PTY (`internal/session`).

The four compatibility pairings are tests in both directions: new client + old
plane (`internal/attachio`), old client + new plane (`attachplane`), new plane
+ old sandbox (`attachplane`, ack timeout), new sandbox + old plane
(`internal/relay`, a `FrameClient` with no generation).

Gates: `make verify` (module-path, protocols, control, test, build, vet), the
`controlapp/repotest` conformance suite against memstore and pgstore, and the
`internal/e2e` scenes with `RAINIER_TEST_PG_DSN` set. A green run without that
DSN proves nothing about the Postgres adapter and is not evidence.

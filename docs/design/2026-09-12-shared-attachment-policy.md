# A shared attachment policy: every attached terminal may type

Status: implemented in this change.
Predecessors: [`2026-09-10-conditional-controller-ownership.md`](2026-09-10-conditional-controller-ownership.md)
and its four review rounds; the shipped summary is
[`docs/terminal-controller-ownership.md`](../terminal-controller-ownership.md).

## Shared policy (2026-09-12)

**The decision.** On the hosted plane a laptop terminal and a browser terminal
attached to one session may BOTH type. No claim, no take-over key, no "another
device has control" notice. Shared typing was the accidental behaviour before
the controller work; this makes it the deliberate, defined behaviour and the
**default** for both composers — the embedded cell gateway in Rainier Cloud and
the self-hosted `controld` binary. Exclusive ownership stays available, whole,
as a policy a host selects.

**Nothing is removed.** The controller generation, the 30 s lease, the
compare-and-advance take-over, the acknowledged handoff and the pty fence all
stay exactly as they shipped in v0.0.12. Under the shared policy they are
simply not exercised between peers: every live attach holds the session's
current generation, so the fence is a no-op between them and still bites for
a stale or revoked attachment.

**What is exclusive-only.** These are the behaviours a host gets by selecting
`PolicyExclusive`, and the only ones this change makes conditional:

| Behaviour | Exclusive | Shared |
|---|---|---|
| Generation advanced on a controller attach | yes | **no** |
| Generation advanced by a `claim` | yes | **no** |
| Generation advanced by a `release` | yes | **no** |
| A peer is displaced (`control_changed`) by an attach or a claim | yes | **never** |
| The lease is taken, renewed on a 5 s heartbeat, released on detach | yes | **no lease is taken** |
| `stale` answers a claim that lost a race | yes | only a claim from a non-typer |
| CLI take-over copy, `--take` claiming, `Controller:` in `info` | yes | replaced (below) |
| pty generation fence | yes | **yes, unchanged** |
| `--view` never types; a viewer's input and resize are dropped | yes | **yes, unchanged** |

**The resize rule.** The pty follows the **most recent resize from any
attachment that may type** — the "latest client" rule. A viewer's resize is
remembered and never applied, exactly as before. This replaces the
smallest-of-the-controllers rule, which was written when only one attachment
could ever be entitled at once and which, with several typers, would squeeze
every terminal down to the width of the narrowest one. Under the exclusive
policy exactly one attachment is entitled at any instant, so "latest" and
"smallest" are the same value and nothing changes; the rule differs only where
several attachments may type, which is the shared policy and the legacy
unbound-attachment path.

The cost is stated rather than hidden: with two typers of different sizes the
smaller terminal renders a screen wider than itself until its own next resize.
That is the trade the product decision makes — a person who resizes their
window expects their window to be what the shell sees.

## Problem

The conditional-controller model answers "who may type" with "exactly one
device, and here is how it changes hands". For two terminals belonging to **one
person** — the laptop they started the session from and the browser tab they
opened on the hosted plane — that answer is friction with no benefit: every
switch costs a key press, and a viewer sees a notice about a "device" that is
the machine they are sitting at. The product decision is that the hosted
default should be shared typing, and that exclusivity is the special case a
host asks for.

## Scope

In scope: a host-selected policy enumeration carried from the composition root
to the plane, the grant rule under it, the plane's claim/release/heartbeat
behaviour under it, the resize rule, the CLI's notices and `info` row, the
`v0wire` fact a client reads them from, the `controld` flag, and the docs.

Out of scope (non-goals, unchanged): a second pty per session, per-attach
shells, anything about `rainier exec`, removing the generation, the lease or
the fence, and any schema change. No terminal-protocol message and no
negotiation parameter is added.

## Design

### 1. `control.InputPolicy` — one enumeration, chosen at composition

```go
type InputPolicy string
const (
    PolicyShared    InputPolicy = "shared"
    PolicyExclusive InputPolicy = "exclusive"
)
```

It lives in `control`, the frozen public contract, because the two packages
that read it — `controlapp` (which grants) and `attachplane` (which carries and
fences) — both depend on `control` and neither depends on the other. The empty
value means `PolicyShared` **everywhere**, which is what makes the product
default the default without every composer restating it.

It is deliberately NOT named `AttachmentPolicy`: `controlapp.AttachmentPolicy`
is the mode-aware **authorization** seam ("may this principal drive?"), and two
types a reader would have to disambiguate by package is worse than one type
named for what it decides. This one decides who may type, and `rainier info`
already calls that row `Input`.

The host selects it in **one** place, `controlapp.AttachmentOptions.InputPolicy`,
and the service puts the resolved value on every `control.AttachTarget` it
hands the broker. The plane therefore never has a policy of its own to
disagree with the application about — the alternative, a second knob on
`attachplane.Options`, is two values a composer must keep equal, and the
failure when they drift is the worst one available: an application that
advances the generation on attach while the plane declines to tell the peer it
displaced.

### 2. The grant, under `PolicyShared`

`controlapp.AttachmentService.grant` reads the policy and takes one of two
paths. Under shared:

- an attach whose admitted mode is `AttachmentViewer` is a viewer at the row's
  current generation — unchanged, and the `--view` contract with it;
- **every other attach is `AttachmentController` at the row's current
  generation**, with no compare-and-advance, no lease claim, and no peer
  displaced.

That second rule covers the legacy path too. An unnegotiated attach is
recorded as a take-over under the exclusive policy — a client that cannot be
told it is a viewer cannot be made one — and under shared there is nobody to
displace, so advancing would fence the other typers for nothing. A legacy
attach under shared is just another typer.

A negotiated attach still carries a `ControllerLeaseKeeper`: the invariant that
`Controller` is non-nil exactly when `Negotiated` is set is unchanged, and a
viewer still reads its own generation through it. What changes is that nothing
under the shared policy calls `Claim`, `Renew` or `Release` on it.

### 3. The plane, under `PolicyShared`

`attachplane`'s `ownership` reads the policy off its target. Four call sites
branch, and the branch is always "do less":

- **the opening attach** does not `displace` its peers, because no peer lost
  anything. The negotiated client is still told `attached {mode, generation}`
  before the first snapshot or output byte, exactly as today, from the same
  `announceAs` hold. The wire is unchanged and an old client is unaffected.
- **`claim`** from an attach that is already a typer is answered `attached` at
  the generation it already holds, advancing nothing and displacing nobody — it
  is asking for what it has. From an attach that is NOT a typer (a `--view`
  client that sent one anyway, or a principal the host's policy grants viewing
  and not driving) it is answered `stale`, and the store is not touched.
  Under a shared policy input authority is settled once, at attach time;
  nothing a client sends afterwards changes it.

  The alternative — letting a viewer promote itself, since under shared there
  is no contention to lose — was rejected: it would make a plane-side
  revocation reversible by the revoked client, and revocation is the one
  mechanism left that moves a shared attach out of `control`.
- **`release`** is accepted and demotes the caller alone: it becomes a viewer
  at the same generation, its own attachment's binding is re-installed as
  `view` so the pty stops executing what it sends, and its client is told
  `control_changed {view}`. The generation is NOT advanced — advancing is how
  the exclusive policy fences a departing controller, and here it would fence
  every peer typing under that generation. **It is a no-op for peers.**
- **the heartbeat renews nothing**, because no lease was taken. It still runs,
  and what it does instead is READ the generation: an attach whose session has
  moved past the generation it is typing under demotes itself, tells its client
  `control_changed`, and re-installs its own binding as `view`. `finish`
  releases nothing, for the same reason nothing is renewed.

  The read is not ceremony. `displace` only ever reaches the attaches THIS
  replica is serving, so two things can fence a shared attach with nobody to
  announce it: a plane-side revocation on another replica, and a fleet
  straddling a policy change, where an exclusive replica's take-over advances
  the row. Without the read such a terminal simply stops accepting typing, with
  no notice and no key — the exact outcome conditional ownership was built to
  avoid. Under a uniform shared fleet nothing advances the generation, so the
  read never demotes anybody and costs one store read per attach per interval.

`control_changed` is therefore **never pushed by a peer's attach, claim or
release** under this policy. It is still pushed when THIS attach loses input
authority for a reason that is not a peer: its own `release`, and an
attachment revoked by the plane. That second case is what the hosted browser
transport (Cloud Task 7) relies on, which is why the message, the demotion
path and the binding re-install behind it are kept rather than deleted.

### 4. The pty fence is unchanged

`internal/session` is not told the policy and does not need to be. Its rule is
still: an unbound attachment executes unconditionally; a bound one executes
only while its binding is `control`, its binding's generation equals the
session's current generation, and the frame's own generation equals it too.

Under shared every live attach holds the current generation, so the fence is a
no-op between peers — two typers at N both execute, in the order the pty
receives them. It still bites for a stale or revoked attachment: a frame sent
under N-1 after something advanced the session to N is dropped where it would
have executed. That is tested directly.

### 5. What a client is told, and how `info` answers

The terminal protocol is unchanged, so a client cannot learn the policy from
its attach socket without a new message. It does not need one: the CLI already
reads the session view before it attaches (that is how `attach` resumes a
stopped session), so the policy and the peer count ride the view it already
has.

`v0wire.SessionView` gains one additive, always-present object beside
`controller`:

```json
"input": { "policy": "shared", "attached": 2 }
```

`controller {generation, held}` is untouched — the generation still exists and
still means what it meant. `input.attached` counts the attaches **that may
type**, as the replica answering the request sees them; it is derived from live
state like `reachable` and `controller.held`, so there is no schema change and
no new stored column. On a host with several gateway replicas the count is that
replica's, which is why it is a notice and a status row and never an
authorization input. An older server sends no `input` key at all, which reads
as the empty policy — and the CLI reads an empty or unknown policy as
exclusive, so it prints exactly what it prints today.

The CLI's copy under the shared policy:

- the first typer attaches in **silence**, as it does today;
- when other typers are already attached, ONE opening line:
  `[2 other terminals attached; everyone may type. Ctrl-] detaches.]` —
  bracketed and CRLF-framed like every other notice this package writes, with
  the count pluralized rather than printed as `terminal(s)`;
- the take-over copy — "another device has control", "another device took
  control", "somebody else got there first" — is printed only under the
  exclusive policy;
- a shared-policy attach that is nonetheless a viewer (a principal the host
  grants viewing and not driving) prints `[viewing — this terminal may not
  type]` rather than an offer to press a key that cannot succeed;
- `rainier info`'s `Controller:` row becomes `Input:  shared (2 attached)`
  under the shared policy and keeps today's three answers — `none`, `this
  device`, `another device` — under the exclusive one.

`--view` keeps its meaning exactly: never type, under either policy. `--take`
is **kept and accepted** under the shared policy and does nothing beyond
attaching as a typer, because an installed CLI passes it; its help text says
so. It is already a no-op mechanically — `--take` spends its one claim only if
the attach came back a viewer, and under shared a typer never does.

### 6. Compatibility

| Pairing | Behaviour |
|---|---|
| **new client + shared plane** | Told `attached {control, gen}` before the first byte, like any controller attach. The count in its opening notice comes from the session view it already fetched. |
| **new client + old plane (exclusive)** | Unchanged. The client never assumes which policy it will meet: it reads what `attached` tells it, and an absent `input` object reads as exclusive. |
| **legacy client + shared plane** | A typer, at the current generation, with no generation advance and therefore nothing fenced for any peer. It receives today's message set byte for byte. |
| **shared plane + old sandbox** | The binding is installed and the acknowledgement may never come; the plane waits its bounded wait and proceeds, as it does today. Nothing about a handoff is load-bearing under shared. |
| **shared plane + new sandbox** | Every attachment is bound `control` at the current generation. The fence is inert between peers and live for anything stale. |
| **two replicas running DIFFERENT policies** | Supported but not recommended, and it is the one pairing with an operator constraint: see below. |

**A fleet should run one policy.** The policy is a per-process value with no
fleet coordination, so a host rolling the flag across several replicas straddles
the two rules for the length of the roll. What happens then is defined, not
undefined: an exclusive replica's attach advances the generation and fences the
shared replica's typers at the pty, and each of them learns within one heartbeat
interval from the generation read above, demotes, and tells its client. Two
people can type into one shell for at most that interval, and the shared attach
does NOT see a live lease as a reason to become a viewer — `grant`'s shared
branch deliberately ignores the lease, because under its own policy there is
nothing to hint at.

So: roll the policy deliberately, and expect one heartbeat interval of
inconsistency per session that is attached across the roll. The alternative —
making a shared attach honour a live lease — would mean a uniform shared fleet
behaving differently for a session that happened to have an exclusive-era lease
still ticking, which is worse.

## Edge cases

- **A session at generation 0.** Nothing under the shared policy advances the
  generation, so a session nobody has ever attached to exclusively stays at 0
  for its whole life. `terminal.GenOf(0)` is the empty string, which is absent
  from the JSON — and `frameBinding` reads BOUND off the mode, not off the
  generation, so an attachment at generation 0 is bound, fenced against 0, and
  executes. Verified by test rather than by reading.
- **A `--view` attach under shared.** Still a viewer: its stdin is dropped at
  the plane, its frames are fenced at the pty, and its resize is remembered and
  never applied. That last clause is about a BOUND viewer. An unbound
  attachment — the new-plane + old-sandbox pairing, and a direct-to-sandbox
  debugging attach — executes unconditionally and has always sized the pty,
  which under the latest-client rule means a large viewer on an old sandbox can
  now enlarge it where smallest-per-axis would have ignored it. That pairing is
  fenced at the plane alone by construction, which is what the compatibility
  table already says about it.
- **Nothing re-asserts a typer's size after a peer takes it.** The CLI sends its
  size when it GAINS the ability to type, not when the pty moves under it, so a
  terminal that reconnects often (a browser tab across a hosted lease renewal)
  re-takes the size on every reconnect and the other typer stays wrong until its
  own window changes. Accepted: the fix is a client that re-asserts its size,
  which is a client change and not a policy one.
- **A claim from a viewer under shared.** Answered `stale`, store untouched.
  The CLI renders `[viewing — this terminal may not type]`, NOT the lost-race
  sentence: "somebody else got there first; press Ctrl-\ to try again" is wrong
  twice over here, because nobody got there first and trying again cannot work.
  It must render something — a key that produces nothing at all is the one
  outcome a person cannot tell from a broken connection — and that answer is
  the same sentence the opening viewer notice uses, so no client has to learn a
  new one.
- **A release, then input.** The releasing attach's own binding is `view` at
  the current generation before its client is told, so a keystroke already in
  flight is dropped at the pty rather than executed after the release.
- **Two typers resizing at once.** The pty ends at whichever resize the session
  processed last, under one lock; there is no interleaving that leaves the
  emulator and the pty at different sizes.
- **A host that composes the zero policy.** It gets shared, which is the
  product default and is stated in the type's own documentation, in the deploy
  guide, and in this change's release notes. An invalid non-empty value fails
  `NewAttachmentService` with `control.ErrInvalid` — a composition error, caught
  at composition rather than at the first attach.

## Alternatives considered

1. **A policy on `attachplane.Options` as well.** Rejected: the generation is
   advanced by the application, not the plane, so a plane-only knob cannot
   implement rule 1 at all, and a knob in both places is two values that must
   agree. One value on the target is the single source of truth.
2. **Per-session or per-workspace policy.** The decision is a product-level
   one about a plane, and a per-session policy would have to be stored — a
   schema change this task excludes — and read on a path that is already the
   hottest authorization path in the service.
3. **Deleting the controller machinery.** Rejected by the task and on the
   merits: exclusivity is wanted for the multi-person case, the fence is what
   makes a revoked attachment safe under either policy, and a generation that
   no longer exists cannot come back cheaply.
4. **An additive `peers` count on the `attached` message.** It would make the
   opening notice exact even on a multi-replica host, at the cost of a
   terminal-protocol field for a courtesy line. The session view the CLI
   already fetches is enough, and it keeps this change out of the wire.
5. **Keeping the smallest-size resize rule.** With two typers it is the rule
   that makes both terminals slightly wrong forever, and the person who just
   resized has no way to win. "Latest" is the rule a person can predict.

## Verification

- `controlapp`: two negotiated attaches under shared are both granted
  controller at the same, unadvanced generation; the store's
  compare-and-advance is never called on any shared attach path, legacy
  included; a `--view` attach is still a viewer; every generation and lease
  behaviour under `PolicyExclusive` is unchanged, asserted by the existing
  suite running unmodified.
- `attachplane`: both typers receive `attached {control, N}`; both inputs are
  forwarded, in order; a third `--view` attach receives `attached {view}` and
  its stdin and resize are dropped; no peer receives `control_changed` from
  another attach's arrival, claim, release or departure; a claim advances
  nothing and calls nothing on the keeper; a release demotes only its caller
  and pushes `control_changed` to it alone.
- `internal/session`: two attachments bound `control` at N both execute and a
  frame stamped N-1 is dropped; the pty size equals the last resize from a
  typer and a viewer's resize changes nothing.
- `cmd/rainier`: the opening notices and the `info` row under both policies,
  against a fake plane, including an older server that sends no `input` object.
- Full gates: `make verify`, the `controlapp/repotest` conformance suite
  against every adapter, and the pgstore scenarios with `RAINIER_TEST_PG_DSN`
  set.

## Release

Cloud pins the merged commit and makes **two** changes, not one. They are
separate wires and either alone is a half-shipped feature:

1. **Select the policy** where the cell composes its application —
   `controlapp.AttachmentOptions.InputPolicy`, defaulting to shared. This is
   what makes the plane behave shared.
2. **Report it in the session view** — set `v0wire.SessionDerived.InputPolicy`
   and `InputAttached` (from the attach plane's `Plane.Typers`) where the
   gateway renders a session, exactly as `internal/controld`'s renderer does.
   Without it the view omits `input`, every CLI reads the absent policy as
   exclusive, and the hosted plane runs shared input while telling people
   "another device has control; press Ctrl-\ to take it" about a plane where no
   device holds anything and that key cannot succeed.

Self-hosted operators get shared by default and `controld --input-policy
exclusive` to keep today's behaviour. Rolling order across the three parties
(client, plane, sandbox) is unconstrained — the change is additive on every wire
— but the POLICY itself should be uniform across a host's replicas; see the
compatibility note above.

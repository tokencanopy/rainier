# Controller ownership: the identity a mid-attach claim is authorized against

Conditional controller ownership shipped the rule that taking control
mid-attach is a privilege of its own, asked for on the claim rather than
cached at the door. It asks the right question and asks it live — and it asks
it about **nobody**, because it asks it on the runner's dial-back request
context. Every mid-attach `claim` against a host whose policy reads the
caller's identity out of the context is refused as stale, so
`rainier attach --take` and the in-session take-control key cannot take
control at all.

This is a follow-up to
[`2026-09-10-conditional-controller-ownership.md`](2026-09-10-conditional-controller-ownership.md)
and the three review rounds after it
([third](2026-09-10-controller-ownership-review-fixes.md),
[fourth](2026-09-11-controller-ownership-liveness.md),
[fifth](2026-09-11-controller-ownership-fifth-review.md)). Nothing here
changes the rule, the wire, the compatibility matrix, or the policy question
itself. What changes is which context that question is asked on.

## Problem

The defect was found by an independent review of an unrelated open change and
belongs to the merged ownership work, not to that change.

### The path

1. `AttachmentService.AttachTerminal` runs on the **client's** authorized
   request context. For self-hosted controld that is
   `attachplane.WithSince(withUser(r.Context(), u), since)` — a context
   carrying the authenticated `User` (`internal/controld/attach.go`).
2. `grant` builds the `controllerKeeper` on that context and hands it to the
   plane on the `AttachTarget`. The keeper carries the policy, the scope and
   the authoritative resource, so that a later claim can be authorized
   against the same three things the door was.
3. The plane parks the client, the runner dials back, and
   `Plane.handleAttachBack` splices the two with
   `splice(r.Context(), …)` — **the runner's** request context.
4. A `claim` frame from the client is handled inline on that splice, so
   `ownership.claim` → `controllerKeeper.Claim(ctx, expected)` receives the
   runner's dial-back context.
5. `controllerKeeper.Claim` asks
   `policy.AuthorizeAttachment(ctx, k.scope, k.resource, AttachmentController)`
   with that context.

The runner's dial-back is authenticated as a *runner* — a fleet bearer token,
no user, by construction (spec rule 3: nothing dials into a runner, so the
socket the claim arrives on was opened by the runner). So step 5 asks the
policy about a context with no principal in it.

### What it costs

`internal/controld`'s `ownerOrAdmin.AuthorizeAttachment` resolves the acting
user with `actingUser(ctx, scope)`, which refuses a context with no user
before any rule runs, and returns `control.ErrDenied`. `Claim` maps that to
`ErrDenied`, `ownership.claim` cannot distinguish it from a lost race, and the
client is answered `stale`. The CLI renders
`[somebody else got there first; press Ctrl-\ to try again]`, and pressing
again produces the same answer forever. The session's own creator — an
`admin`, even — cannot take control of their own terminal from a second
device.

Hosted Rainier has the same policy shape and does not reach it yet. Its
authorizer reads the current workspace role with `scope.RoleFrom(ctx)` and
refuses a request that carries none, so it would answer a claim exactly as
`ownerOrAdmin` does — but its gateway still opens every attach
**unnegotiated** (`Mode: control.AttachmentController` with no `Negotiated`),
which `grant` answers with a nil keeper and therefore no claim path at all.
The defect is latent there rather than live: the day that composer negotiates
is the day it inherits this, which is a reason to fix it in the core now and
not a reason to think hosted is unaffected.

Two things kept it from being caught:

- There is **no controld-level test for the claim path at all**. The plane's
  own suites drive a keeper fake, and `controlapp`'s fake policy answers from
  a mode table without ever looking at the context — which is precisely the
  observation the defect needed a test to make.
- Self-hosted and hosted policies both answer "may attach" and "may drive"
  identically, so nothing about *modes* diverges; the divergence is about the
  *context*, which no fixture inspected.

### Why the scope on the keeper was not enough

`controllerKeeper` already carries `control.Scope`, and the scope names the
actor. It is not enough on its own, and deliberately so: `actingUser` refuses
a scope whose actor disagrees with the context's user, because "the scope is
authoritative adapter output and the context is what the token resolved to;
if they ever disagree the call was assembled wrong". An adapter that trusted
the scope alone would be trusting adapter output that no authentication had
confirmed on this call. The fix therefore supplies the **context** the scope
was authoritative for, and leaves the cross-check standing.

## Scope

In scope:

- Capture the authorizing identity when the keeper is built, inside
  `AttachTerminal`, and authorize every policy question the keeper asks
  against that identity rather than against its caller's context.
- A controld-level regression test for the mid-attach claim path, under the
  real `ownerOrAdmin` adapter, which fails before the fix.
- An audit of every other policy or authorizer call reachable from the
  plane's splice, with a test for each conclusion.
- The operator-facing note in `docs/terminal-controller-ownership.md` about
  what "asks the policy live" does and does not mean.

Out of scope:

- The policy question itself. It stays `AttachmentController`, asked live, one
  call per claim.
- Weakening `ownerOrAdmin` or `actingUser`. The bug is the context the
  question is asked on, not the rule.
- `internal/sandboxexec` and anything the open exec change (#90) adds. Its
  exec route asks the policy once, on the client's own request context, and
  has no keeper and no claim.
- The wire, the compatibility matrix, the lease parameters, the CLI's own
  ownership state machine.

## Alternatives

**Widen the policy interface to take the identity explicitly.** Add a
principal (or the authorizing context) as a parameter of
`AttachmentPolicy.AuthorizeAttachment`. It is the most explicit fix and it
makes the mistake unrepresentable — and it breaks every host that composes
`controlapp` the moment they take the update, for a seam that is otherwise
frozen. Rejected for the same reason the exec broker was made optional rather
than required: this struct is composed by Rainier Cloud as well as by
self-hosted controld.

**Have each host RE-RESOLVE the claimant from its store on every claim.**
This is what the sandbox's upward credential request already does
(`internal/controld/srpc.go`'s `agentActorContext` reads the session's creator
out of the store and calls `withUser`), and it is the only shape that keeps a
claim's authorization *live* in the identity as well as in the question: a
host would resolve the keeper's own `scope.Actor.ID` — the claimant, not the
session's creator — to whatever that account is now, so a role revoked or an
account deleted mid-attach would be seen at the next press.

It is not what this change does, and the reason is scope rather than
correctness. Every host would need a principal lookup on the claim path
(`ownerOrAdmin` is a zero-size struct today and would need the store),
attach-time authorization would grow a store read, and "the store cannot
answer" would become a new authorization outcome with its own semantics.
Rainier Cloud would need the matching change against its directory, in its own
repository, on its own review. The captured identity is what makes
`--take` work at all; re-resolution is what would make it honour a
revocation, and it is written down as the follow-up under
[Limitations](#limitations) rather than smuggled in here.

**Let the plane carry the client's context into the splice.** The plane would
have to hold the client's request context alongside the runner's and hand the
right one to each. It puts the identity in the package that must never see
one, and it would still leave `controllerKeeper.Claim` trusting whatever
context it was handed. Rejected: the keeper is the thing that knows which
context authorized this attach, so the keeper is where that fact belongs.

**Cache the attach-time answer and skip the policy on claim.** Cheapest, and
it throws away the property the design bought: a grant revoked mid-attach is
honoured at the next press. Rejected.

## Design

`controllerKeeper` gains one field: the context that **authorized** this
attach, captured with `context.WithoutCancel` at keeper construction —
inside `AttachTerminal`'s call to `grant`, which is the one place in the
module where the context still carries the caller.

Every policy question the keeper asks is asked on a context assembled from
two: the **values** of the authorizing context, and the **deadline and
cancellation** of the call being made now.

```go
func (k controllerKeeper) authorizing(ctx context.Context) (context.Context, func()) {
	if k.identity == nil {
		return ctx, func() {}          // ask as a composer got before this existed
	}
	base, stopTimer := k.identity, context.CancelFunc(func() {})
	if deadline, ok := ctx.Deadline(); ok {
		base, stopTimer = context.WithDeadline(base, deadline)
	}
	out, cancel := context.WithCancelCause(base)
	stop := context.AfterFunc(ctx, func() { cancel(context.Cause(ctx)) })
	if ctx.Err() != nil {
		cancel(context.Cause(ctx))     // already over: answer synchronously
	}
	return out, func() { stop(); cancel(context.Canceled); stopTimer() }
}
```

Values from the authorizing context **only**, never merged with the live
call's — not even as a fallback for a key the authorizing context does not
carry. The live call is the runner's, and a runner must not be able to
contribute a value to an authorization decision about a user; the capture's
`context.WithoutCancel` severs the chain, so nothing on the runner's side is
reachable from the question at all. Deadline and cancellation from the live
call **only**: a policy that is a network call must die with the claim that
asked it, and the captured context must not be able to keep a policy call
alive after the attach is gone.

The two are grafted together with the context package's own machinery rather
than by overriding `Value` on a wrapper around the live call. The override is
three lines shorter and it hides the key `context.WithCancel` looks a
cancellation parent up by, so every cancelable context a host's policy derived
— which is every policy that makes a bounded network call — would spawn a
goroutine to watch its parent instead of registering as a child. A test pins
the cost at derivation, not the implementation.

The store calls the keeper makes — the generation CAS, the lease renew, the
row read — keep running on the caller's context exactly as they do today.
They are not authorization decisions, they are already fenced by the
generation, and a host repository that reads a request-scoped value (a
transaction handle, say) must see the context of the call it is actually
serving. The symmetric case on the policy side is the opposite by
construction: a policy that reads the store sees the captured attach's values,
so it cannot join a transaction the claim opened — which is correct, because
the claim opens none.

`Claim` is the only keeper method that asks the policy. `Renew`, `Release` and
`State` ask nothing, which is what makes the heartbeat survive on a runner's
context — and is now asserted by a test, so that adding a policy question to
one of them without routing it through the authorizing identity fails.

### The audit

Every policy or authorizer call reachable from the plane's splice, which runs
on the runner's dial-back context:

| Call | Reached from the splice? | Verdict |
|---|---|---|
| `controllerKeeper.Claim` → `AuthorizeAttachment(Controller)` | yes — a `claim` frame, inline on the client pump | **the bug**; fixed here |
| `controllerKeeper.Renew` (heartbeat) | yes — the heartbeat goroutine | asks no policy; test asserts it |
| `controllerKeeper.Release` (a `release` frame, a failed install, `finish`) | yes | asks no policy; test asserts it |
| `controllerKeeper.State` (`sendStale`, `demote`, `finish`) | yes | asks no policy; test asserts it |
| `AttachmentService.admit` / `mayClaim` / the legacy take-over's `NextControllerGeneration` | no — all on the client's `AttachTerminal` context, before the broker | correct already |
| `internal/controld`'s pre-upgrade `mayAttach` | no — the client's HTTP request | correct already |
| `AgentCredentialService.answerable` → `Authorize(ActionAttach)` | no — the runner's RPC, not the splice | already solved the other way: `agentActorContext` injects the session's creator |
| the exec route's policy question (open PR #90) | no — the client's request context, and no keeper | nothing to fix; untouched |

## Edge cases

- **A claim on a keeper with no identity captured.** `grant` is the only
  constructor and it always captures, so in production this cannot happen: a
  host that authorizes from the scope alone still gets a captured context,
  just one with no principal in it, and that works because the scope and the
  resource are the keeper's own fields. The nil case is reachable only from a
  keeper assembled by hand, which means a test. It falls through to the
  caller's context — the previous behaviour — rather than refusing, because a
  guard that turned a test's oversight into `ErrDenied` would be diagnosing
  it as a policy decision. It is the only nil tolerated: a keeper with no
  *policy* is a composition error and still panics, as it did before.
- **The attach's context is cancelled while a claim is in flight.** Only the
  live call's cancellation is honoured; the captured one has none. A claim on
  a cancelled splice fails on the store call or the policy's own context, as
  it does today.
- **A grant revoked mid-attach.** The *question* is still asked live, per
  claim, so a policy whose answer depends on state it reads at that moment —
  Cloud's planned session collaboration grants are the case this seam exists
  for — is honoured at the next press. What is frozen is the captured
  context's own values, which on both hosts shipping today means the
  claimant's identity *and their role*: see [Limitations](#limitations),
  because today that is the whole of what either policy reads.
- **A composer that had solved this the other way.** A host that injected the
  claimant onto the runner's dial-back context — the shape
  `agentActorContext` uses for the credential RPC — has working claims before
  this change and refused ones after it, because the question no longer reads
  anything the live call carries. It is a composer-visible narrowing and it is
  deliberate (that is the whole property), but it is worth stating: no such
  host exists, the only two `AttachTerminal` callers in either repository are
  self-hosted controld's attach route and the hosted gateway's, and a host
  doing it that way should capture at the door instead, where the identity is
  the claimant's rather than the session creator's.
- **A runner that tries to influence the decision.** It cannot: the runner
  contributes no value to the context the policy sees, and the claim is
  answered against the resource and scope captured at the door, for the
  session the attach was authorized for.
- **The identity outliving the attach.** The captured context is a field on a
  keeper held by one `ownership`, which the plane drops when the attach
  finishes. It carries no goroutine, no timer and no `Done` channel of its
  own. What it does retain is the value chain of the attach request — for
  self-hosted, a cursor, the `User`, and the standard library's own server
  and local-address values; no request body and no socket. Each context
  `authorizing` derives from it is released before `Claim` returns.
- **An unnegotiated (legacy) attach.** It gets no keeper at all, so it has no
  claim path and nothing here applies to it.

## Limitations

Named rather than implied, because the adversarial review of this change
demonstrated each one and an operator would otherwise have to.

**A revoked role still authorizes a claim on a socket that was already
open.** Both shipped policies read everything they need from frozen inputs —
`ownerOrAdmin` from the captured `User` (including its `Role`) and the
keeper's `resource.CreatorID`, hosted Rainier from the role the edge bound and
`scope.Actor.ID` — so a claim's answer today is a function of state captured
at attach time. Concretely: an admin who attached to another person's session
as a viewer, and was then demoted, is refused a *new* attach with 403 and can
still take control on the attach they already hold. Self-hosted controld has
no mechanism that ends that window: no attach lease, no server idle timeout,
and no token-revocation or role-change endpoint at all, so it is the lifetime
of the connection.

This is a narrowing of authority-over-time compared to asking on a fresh
request, and it is a widening compared to the broken behaviour it replaces,
where every claim was refused. It is consistent with the rest of an attach
rather than an exception to it: a controller who is demoted mid-attach keeps
typing under the same rule, because the plane's own `claim` path short-circuits
for the attach that already holds control and the heartbeat's `Renew` asks
nothing. Nothing about a live attach is re-authenticated today; this change
does not make that better and does not make it worse for anybody who already
had control.

The follow-ups that would close it, in order of cost: re-resolve the claimant
from the host's store on each claim (see [Alternatives](#alternatives)), and
close live attach sockets when a membership, role or token changes — which is
the mechanism a hosted cell has and a self-hosted installation does not.

**A claim from the attach that already holds control asks nothing.** It is
answered from a lease read (`attachplane/ownership.go`), by design and by the
shipped doc, so "the take-control key asks the policy" is true of a viewer's
press and not of a controller's. The tests here say which they cover.

## Verification

- `internal/controld`: the regression test — a negotiated controller attach,
  a second negotiated attach admitted as a viewer under the live lease, and a
  mid-attach `claim` from that viewer, all under the real `ownerOrAdmin`
  policy over the real splice. It must be answered `attached control` at an
  advanced generation; before the fix it is answered `stale`. Plus the same
  path for a principal the policy genuinely refuses the controller (still
  `stale`), so the fix is not a weakening.
- `internal/controld`: the exact frame sequence `rainier attach --take` and
  the in-session take-control key produce, against the real server — both
  halves of `--take`, the query-string claim that was never broken and the
  mid-attach claim that was. The frames are written out in the test rather
  than driven through `internal/attachio`, whose decision to send them is
  unexported and pinned by that package's own tests
  (`TestTheTakeKeyClaimsFromTheGenerationItWasTold`,
  `TestALostClaimSaysSoAndDoesNotRetry`); the two halves meet at one frame
  shape. One of these tests runs under a policy that **records the identity
  it was asked about**, and asserts it was the attaching user rather than
  nobody.
- `controlapp`: unit tests over a keeper with an identity-inspecting fake
  policy — `Claim` on a bare context succeeds and the policy sees the
  authorizing user; `Renew`, `Release` and `State` on a bare context succeed
  and ask no policy at all; a keeper with no captured identity behaves as
  before.
- The controller-ownership suites — `attachplane`, `controlapp`,
  `internal/controld`, `internal/attachio`, `cmd/rainier` — at
  `-race -count=3`. `controlapp` needs `-timeout` raised above the default ten
  minutes at that count, for a pre-existing reason that has nothing to do with
  ownership: `TestPullWorkspaceExactlyMaxBytes` and `TestPullWorkspaceOverflow`
  each stream `workspace.MaxBytes` through a fake transport, which costs five
  minutes a round under the race detector.
- Mutation checks, because a test that cannot fail is not verification: the
  capture removed, the values merged with the live call's, and the live call's
  cancellation ignored each fail a named test and nothing else.
- `make verify`, and `repotest` on both store adapters with
  `RAINIER_TEST_PG_DSN` set at PostgreSQL 17.

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

Hosted Rainier has the same shape and the same outcome:
`internal/cell/authz.Authorizer.Authorize` reads the current workspace role
with `scope.RoleFrom(ctx)` and refuses a request that carries none.

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

**Have each host inject the identity onto the runner's dial-back context.**
This is what the sandbox's upward credential request already does
(`internal/controld/srpc.go`'s `agentActorContext` looks the session's creator
up and calls `withUser`). It works, and it is wrong here: the dial-back
carries an attach id and nothing else, so a host would have to *re-derive* who
the claimant is from the session row — which is not the same person as the
claimant, and would authorize a viewer's claim as the session's creator. The
credential path can do it because the principal it wants genuinely is the
session's creator.

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
type policyContext struct {
	context.Context          // the live call: deadline, cancellation, Err
	identity context.Context // the attach that was authorized: values only
}

func (c policyContext) Value(key any) any { return c.identity.Value(key) }
```

Values from the authorizing context **only**, never merged with the live
call's: the live call is the runner's, and a runner must not be able to
contribute a value to an authorization decision about a user. Deadline and
cancellation from the live call **only**: a policy that is a network call must
die with the claim that asked it, and the captured context must not be able
to keep a store or a policy call alive after the attach is gone — which is
also why the capture is `WithoutCancel`ed, so the stored field carries no
`Done` channel for anything to wait on and no `Err` for anything to read.

The store calls the keeper makes — the generation CAS, the lease renew, the
row read — keep running on the caller's context exactly as they do today.
They are not authorization decisions, they are already fenced by the
generation, and a host repository that reads a request-scoped value (a
transaction handle, say) must see the context of the call it is actually
serving.

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

- **A claim on a context with no identity captured.** A composer that built a
  keeper before this change cannot exist (the field is set in the only
  constructor), but a zero-value keeper — or a host that authorizes from the
  scope alone and puts nothing in the context — must not break. A nil
  authorizing context falls through to the caller's context unchanged, which
  is exactly today's behaviour.
- **The attach's context is cancelled while a claim is in flight.** Only the
  live call's cancellation is honoured; the captured one has none. A claim on
  a cancelled splice fails on the store call or the policy's own context, as
  it does today.
- **A grant revoked mid-attach.** Still honoured: the policy is asked live,
  per claim, and it is asked about a stored grant. What is frozen is *who the
  claimant is*, not *what they may do* — and the claimant cannot change
  mid-attach, because the socket is the one the principal authenticated.
- **A role revoked mid-attach, on a host that resolves the role at the
  edge.** The captured context holds the role the attach was admitted with,
  so a claim after a revocation is authorized against it. That is a real
  narrowing of liveness relative to a question asked on a fresh request, and
  it is the only answer available: the claim does not arrive on a request the
  edge could bind a current role to. The mechanism for a revoked membership
  is closing the live connection, which hosted Rainier already does; a policy
  that reads stored state rather than a context-bound role is honoured live.
  This is written down in `docs/terminal-controller-ownership.md` rather than
  left implicit.
- **A runner that tries to influence the decision.** It cannot: the runner
  contributes no value to the context the policy sees, and the claim is
  answered against the resource and scope captured at the door, for the
  session the attach was authorized for.
- **The identity outliving the attach.** The captured context is a field on a
  keeper held by one `ownership`, which the plane drops when the attach
  finishes. It carries no goroutine, no timer and no `Done` channel.
- **An unnegotiated (legacy) attach.** It gets no keeper at all, so it has no
  claim path and nothing here applies to it.

## Verification

- `internal/controld`: the regression test — a negotiated controller attach,
  a second negotiated attach admitted as a viewer under the live lease, and a
  mid-attach `claim` from that viewer, all under the real `ownerOrAdmin`
  policy over the real splice. It must be answered `attached control` at an
  advanced generation; before the fix it is answered `stale`. Plus the same
  path for a principal the policy genuinely refuses the controller (still
  `stale`), so the fix is not a weakening.
- `internal/controld`: `rainier attach --take`'s exact sequence, and the
  in-session take-control key's, driven through the client library against
  the real server with a policy that **records the identity it was asked
  about** — the assertion being that it was asked about the attaching user
  and not about nobody.
- `controlapp`: unit tests over a keeper with an identity-inspecting fake
  policy — `Claim` on a bare context succeeds and the policy sees the
  authorizing user; `Renew`, `Release` and `State` on a bare context succeed
  and ask no policy at all; a keeper with no captured identity behaves as
  before.
- The controller-ownership suites — `attachplane`, `controlapp`,
  `internal/controld`, `internal/attachio`, `cmd/rainier` — at
  `-race -count=3`.
- `make verify`, and `repotest` on both store adapters with
  `RAINIER_TEST_PG_DSN` set at PostgreSQL 17.

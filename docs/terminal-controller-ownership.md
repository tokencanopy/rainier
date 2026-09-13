# Terminal controller ownership

How Rainier decides who may type into a session that several devices are
attached to, what that costs on the wire, and what happens when the three
parties that implement it are running different builds.

The design that introduced it is
[`docs/design/2026-09-10-conditional-controller-ownership.md`](design/2026-09-10-conditional-controller-ownership.md);
this is the operator- and integrator-facing summary of what shipped.

## The policy comes first

Everything below describes the **exclusive** policy. Since
[`docs/design/2026-09-12-shared-attachment-policy.md`](design/2026-09-12-shared-attachment-policy.md)
a host chooses which of two rules it grants attachments by, and **shared is the
default** for the hosted plane and for `controld`:

| Policy | Who may type |
|---|---|
| `shared` (default) | Every attach that asked to type may type, at the session's current generation. Nothing is claimed, no generation is advanced on an attach, a claim or a release, and no attach is ever displaced by another one. |
| `exclusive` | At most one attached terminal is the controller, taken by compare-and-advance on the generation and held under a lease. The whole of the rest of this document. |

`controld --input-policy shared|exclusive` selects it (`RAINIER_INPUT_POLICY`);
a hosted cell selects it where it composes its application. It is one value,
read where the application is composed, and carried on every attach so the
application and the plane cannot disagree about the rule an attach was granted
under. The session view reports it as `input {policy, attached}`, which is where
`rainier info`'s input row and the CLI's opening notice come from.

Nothing in the machinery below is removed under the shared policy — the
generation, the lease, the acknowledged handoff and the pty fence are all
intact. What differs is that between peers there is nothing to fence: every live
attach holds the current generation, so the fence is a no-op between them and
still bites for a stale or revoked attachment. Four things are worth stating
separately for a shared-policy host:

- **`control_changed` is never pushed by a peer's attach, claim, release or
  departure.** It is still pushed when an attach loses input authority for a
  reason that is not a peer: its own `release`, and an attachment the plane
  revokes.
- **A shared attach still finds out when the generation moves under it.** It
  holds no lease to have refused, so its heartbeat READS the generation instead
  of renewing one, and demotes itself when the session has moved past the one it
  is typing under. That is what covers a revocation on another replica, and a
  fleet straddling a policy change.
- **One policy per fleet.** The choice is per process, so a host rolling it
  across replicas straddles the two rules for the length of the roll: an
  exclusive replica's take-over fences a shared replica's typers, and they learn
  from the read above within one heartbeat interval rather than instantly. Roll
  it deliberately.
- **The pty follows the most recent resize from any terminal that may type**,
  under either policy. With one typer that is the controller's size, which is
  what it always was.

## The rule

At most one attached client is the **controller** at any moment. Everyone else
is a **viewer**: they receive the screen and every byte of output, and nothing
they send is executed — not stdin, not a resize, not the bytes a terminal
writes back in answer to a query.

Control is a **lease** on the session row: a monotonic `controller_generation`,
an opaque per-attach holder, and an expiry. The generation is the authority and
the lease is the scheduling hint.

| Parameter | Value | Why |
|---|---|---|
| Lease TTL | 30s | Long enough that a lid closing and reopening is usually still the same controller. |
| Heartbeat | 5s | Six renewals per lease: one lost renewal never costs anybody control. |
| Handoff acknowledgement | 2s | One round trip on an open socket; bounded because an older sandbox never answers. |
| Take-over confirmation | none | It is reversible in one key press and the other side is told. |

Nothing sweeps expired leases. A lease whose expiry has passed is simply not
live, so the next attach claims over it — which is what keeps control from
being stuck on a device that crashed without releasing.

## The handshake

Negotiation travels on the attach request's query string, so a server settles
it before it upgrades the socket, and a server that predates it ignores three
unknown parameters exactly as it ignores any other.

| Parameter | Meaning |
|---|---|
| `control=v1` | This client understands conditional ownership; tell it its mode and generation. |
| `mode=control\|view` | Claim control when it is free, or never claim. Omitted reads as `control`. |
| `expected=<decimal>` | The generation this client last held. Presenting the generation in force claims it even under a live lease — that is how a device resumes what it had, and how `--take` takes it — while presenting one that has been superseded falls back to the ordinary rule below. |

A client that presents no generation claims only while nobody holds a live
lease. A lease that is vacant or expired is claimed by whoever asks, which is
what makes a reconnect after a dropped connection resume rather than come back
a viewer of a session nobody is using: the departing attach released on its
way out.

A client that sends `control=v1` receives `attached {mode, gen}` **before** the
first snapshot or output byte. A client that sends nothing receives exactly the
message set it always did.

Generations travel as **decimal strings** everywhere — the terminal wire and
the JSON session view alike — because a `uint64` past 2^53 is silently wrong as
a JSON number in a browser.

### Messages

| Direction | Type | Carries |
|---|---|---|
| client → server | `claim` | `expected`: the generation to advance from |
| client → server | `release` | — |
| client → server | `stdin`, `resize` | `gen`: the generation the frame was sent under |
| server → client | `attached` | `mode`, `gen` |
| server → client | `stale` | `gen`: the generation that exists now |
| server → client | `control_changed` | `mode`, `gen` |
| plane → sandbox | `control` | `mode`, `gen` — install a binding on a live attachment |
| sandbox → plane | `control_ack` | `gen` — the binding is installed |

The last two are the plane's, and a plane never carries a client's copy of one
in either direction: `control` names a mode and a generation at the pty, so a
client that could send one would be naming its own authority rather than asking
for it. Behind that, an attachment that was opened *unbound* is never bound
later — a binding rides the frame that opens an attachment or it does not
exist — which is what protects a runner's local debugging attach, where there
is no control plane above the socket at all.

The `gen` a sandbox reads on a forwarded frame is always the **plane's**: the
plane replaces whatever the client put there, including on a legacy client's
frames, which the client cannot stamp itself. A **claim is not answered until the sandbox
has acknowledged the new generation**: two attachments reach one sandbox over
one connection but on independent paths, so waiting for the acknowledgement is
what guarantees that every frame from the displaced controller arriving after
the answer is discarded, and that every frame arriving before it arrived while
the handoff had not happened for anybody.

## The fence

The fence is at the **pty**, in the sandbox, not only at the plane — the plane
is not where input executes, and a keystroke accepted from a controller who was
displaced a moment later can already be past it. The plane drops a viewer's
frames as well, because carrying what will certainly be discarded is waste, but
that is an optimisation and not the guarantee.

An attachment executes input when:

- it is **bound** (a control plane told the sandbox what it is), its mode is
  `control`, and both its binding and the frame's `gen` equal the session's
  current controller generation; **or**
- it is **unbound**, in which case it executes unconditionally.

The frame's `gen` is a separate condition from the binding's, and it is
load-bearing on its own: a controller that was displaced and then took control
back holds a binding at the current generation, so only the frame's own stamp
distinguishes what it is typing now from what it typed two generations ago.

The session's generation only ever goes up, so a late frame carrying a
superseded binding cannot un-displace the controller that superseded it.

## Compatibility

The change reaches three separately deployed parties — the client, the plane
(the relay, embedded in a hosted gateway), and `sessiond` inside the session
image — and they roll on different days. All four pairings are supported and
tested in both directions.

| Pairing | Behaviour |
|---|---|
| **new client + old plane** | No `attached` ever arrives. The client settles as an ordinary attach the moment terminal traffic appears: it types, stamps no generation, and stops intercepting Ctrl-\\. |
| **old client + new plane** | The plane sends the old message set. The attach is recorded as a **take-over** — a generation advance — because a client that cannot be told it is a viewer cannot be made one without breaking it; a negotiated client attached at the same time is fenced by that advance and told. The reverse is the cost of the same rule: a legacy client that is later displaced by a negotiated one is fenced with **no notice it can render and no key that takes control back**, because it has neither. Its terminal simply stops accepting typing; detaching and attaching again takes control back, unconditionally. Tell operators this before a mixed-version week, and note that it is what the CLI being tagged last is for. |
| **new plane + old sandbox** | The plane installs bindings and asks for acknowledgements it may not get. It waits its bounded wait and proceeds. That attach is fenced at the plane alone, which is what the pairing can offer. |
| **new sandbox + old plane** | The plane grants no binding and stamps no generation, so the attachment is unbound and unconditional. **Input with no generation is treated as the current controller's**, which is safe because under the old message set only one client could have been sending — the old plane had no way to admit a second one as anything else. |

That last rule is why a **new plane always stamps the frames it forwards**,
including a legacy client's, which the client cannot stamp itself: an unstamped
frame reaching a current sandbox therefore means an older plane, and nothing
else.

One clause on that, since the shared policy: generation **zero** is the empty
string on the wire (`terminal.GenOf`), and a shared session that no exclusive
attach has ever touched stays at zero for its whole life — so its frames are
literally unstamped. It is safe for the reason the fence is arithmetic rather
than presence: the attachment is BOUND (a sandbox reads that off the mode, not
off the generation), the session's own generation is zero, and zero equals zero.
What an unstamped frame no longer proves on its own is that the plane was old.

## Operating notes

- The lease is durable, in the session row, so a control-plane restart or a
  second replica cannot hand out the same authority twice.
- A controller displaced by an attach on **another replica** is not pushed a
  message; its own heartbeat renewal stops being accepted, within one interval
  (≤5s), and it demotes itself. The **generation** moves the moment the
  take-over commits, wherever it happened; what varies is when the sandbox is
  told. On this replica the plane installs the new binding and waits for the
  acknowledgement before answering the taker, so the fence is in place before
  anybody is told they have control. Across replicas the sandbox learns it
  from the taker's own opening frame, or from the displaced controller's next
  heartbeat, whichever lands first — a few hundred milliseconds in practice,
  and bounded by the heartbeat interval.
- A **viewer** is told when the generation moves too, without any notice being
  printed: it is what lets one press of the take-control key take control
  rather than discovering that the number the viewer saw at attach time is
  gone.
- An attach is authorized for the mode it **opens in**, and taking control
  later is authorized separately, on the claim itself. A host whose policy
  grants viewing without granting driving therefore admits a view-mode attach
  normally, and a claim from one is answered `stale` — refused on this
  replica, without a generation moving anywhere. (The first-party `--view`
  client sends no claim at all, so nothing reaches that check; it is what
  answers a third-party client, and a plain attach reconnecting as a viewer.) A **plain attach** from such
  a principal asks for control, because that is what zero-click on one laptop
  means; it is **admitted as a viewer** rather than refused, told `attached,
  view` before it paints a screen, and prints the viewing notice. Refusing it
  would mean a person who may watch a session being told they may not attach
  to it, and having to know to type `--view`. An **unnegotiated** client
  asking for control is still refused: it cannot be told it is a viewer, so
  admitting it as one would leave a terminal that silently does not type. The claim asks the policy
  live rather than reading an answer cached at attach time, so a grant revoked
  mid-attach is honoured at the next press. Self-hosted Rainier answers both
  questions the same way (a caller who may attach may drive), so none of this
  is visible there.

  What is asked live is the **question**; who it is asked about is the
  identity that **opened the attach**, captured when the attach was
  authorized. It has to be: a claim arrives on the socket the runner dialed
  back, which is authenticated as a runner and carries no person at all, so
  there is nobody on that call to ask about. So a policy whose answer depends
  on stored state — a collaboration grant — is honoured at the next press,
  and a policy whose answer depends on the **role** the request was
  authenticated with is answered from the role this attach was admitted with,
  for as long as the attach lives.

  Operationally that means one thing worth knowing: **an attach that is
  already open keeps the authority it opened with.** A person whose role is
  revoked is refused a new attach at once, and on a socket they already hold
  they can still watch, and can still take control if their old role allowed
  it. It is the same rule that already lets a demoted controller keep typing
  — nothing about a live attach is re-authenticated, and this is the whole of
  what the take-control key adds to it. To end such an attach today, end the
  connection: stop it at the proxy or restart the control plane. Nothing in
  self-hosted Rainier expires a live attach on its own.

  There is **one message for both refusals**, and the CLI renders it
  `[somebody else got there first; press Ctrl-\ to try again]`. For a
  principal the host's policy refuses, that sentence is wrong about the cause
  and its suggestion cannot succeed. A client cannot tell the two apart today:
  distinguishing them means a message the wire does not have, and the
  compatibility matrix is the reason not to add one in a fix. Tell such users
  that they are attached read-only, rather than letting the notice explain
  it.
- A **take-over commits before the terminal exists**. The binding rides the
  `dial_attach`, so a negotiated controller attach advances the generation and
  displaces the incumbent before the runner has been asked to dial back — and
  if it never dials back, the attach ends at its pairing TTL having produced
  no terminal at all. The previous controller is then a viewer of a session
  nobody controls. It is not stuck: the displacement announced the new
  generation, so one press of the take-control key takes it back. Reversing
  the order is not available, because until the runner dials back there is no
  socket to install a binding on.
- Nothing bounds how often a client may claim, and nothing needs to. A claim
  from the attach that already **holds** control costs one lease read and
  nothing else: it never advances the generation and never fences the
  keystrokes that client has already sent. (The read is not ceremony. A
  controller displaced by an attach on another replica still reads as the
  controller here until its own heartbeat is refused, and that press is how
  its user finds out; answering it from memory would confirm control that has
  already gone elsewhere.) A
  client can still loop `claim` and `release` on its own stream; each pair
  advances the generation twice and waits, up to one acknowledgement timeout
  in total, for the other attaches' sandboxes. It is an authorized user's
  nuisance against their own session — no escalation, and nothing another
  account can do — and it is deliberately not rate limited, because a limiter
  would also refuse the legitimate rapid hand-back after a mis-press.

- A handoff is **bounded and parallel**. Every peer a take-over displaces is
  reached on its own path, and every step of it — the binding written to that
  peer's sandbox, the acknowledgement waited for, the notice written to that
  peer's client — carries the acknowledgement timeout. A device that has
  stopped reading its socket (a closed lid, a paused browser tab) therefore
  costs a handoff one timeout and delays nobody else: it is fenced at its pty
  the moment the generation moves, the plane stops carrying what it types
  before any of this runs, and its own heartbeat demotes it within one
  interval. What it loses is the immediate notice. Client sockets carry a
  write deadline for the same reason, so one that has stopped reading
  altogether is eventually closed rather than held open — a deadline far
  longer than a handoff's, because a client that is merely slow must be able
  to take a large snapshot, and because a handoff never waits on it anyway.

- A take-over that cannot be **installed** in the taker's own sandbox is not
  granted. The generation it won is given back at once, and the client is told
  the number that exists now, so one more press takes control. Granting it
  would produce a controller nothing can repair: the pty would discard every
  keystroke, and the lease would be renewed happily by a plane that believed
  the handoff had happened. A sandbox that simply never **acknowledges** is a
  different thing — that is an older `sessiond`, and the handoff proceeds
  fenced at the plane alone, as the compatibility matrix says.
- Nothing about ownership is logged, and no message, byte, or length of one is.
  The holder is an opaque per-attach identity, never a user, device, account or
  browser-session identifier, and never leaves the control plane.

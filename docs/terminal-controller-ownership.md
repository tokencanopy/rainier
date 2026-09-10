# Terminal controller ownership

How Rainier decides who may type into a session that several devices are
attached to, what that costs on the wire, and what happens when the three
parties that implement it are running different builds.

The design that introduced it is
[`docs/design/2026-09-10-conditional-controller-ownership.md`](design/2026-09-10-conditional-controller-ownership.md);
this is the operator- and integrator-facing summary of what shipped.

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
| `expected=<decimal>` | Claim only from this generation. What a reconnecting controller presents. |

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

The last two never reach a client. A **claim is not answered until the sandbox
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
| **old client + new plane** | The plane sends the old message set. The attach is recorded as a **take-over** — a generation advance — because a client that cannot be told it is a viewer cannot be made one without breaking it; a negotiated client attached at the same time is fenced by that advance and told. |
| **new plane + old sandbox** | The plane installs bindings and asks for acknowledgements it may not get. It waits its bounded wait and proceeds. That attach is fenced at the plane alone, which is what the pairing can offer. |
| **new sandbox + old plane** | The plane grants no binding and stamps no generation, so the attachment is unbound and unconditional. **Input with no generation is treated as the current controller's**, which is safe because under the old message set only one client could have been sending — the old plane had no way to admit a second one as anything else. |

That last rule is why a **new plane always stamps the frames it forwards**,
including a legacy client's, which the client cannot stamp itself: an unstamped
frame reaching a current sandbox therefore means an older plane, and nothing
else.

## Operating notes

- The lease is durable, in the session row, so a control-plane restart or a
  second replica cannot hand out the same authority twice.
- A controller displaced by an attach on **another replica** is not pushed a
  message; its own heartbeat renewal stops being accepted, within one interval
  (≤5s), and it demotes itself. Its fencing is immediate either way, because
  the generation moved before either notice was sent.
- Nothing about ownership is logged, and no message, byte, or length of one is.
  The holder is an opaque per-attach identity, never a user, device, account or
  browser-session identifier, and never leaves the control plane.

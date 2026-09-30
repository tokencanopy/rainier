# Runner reconnect authorization callback

This dependent slice follows the shared [RPC contract](2026-09-30-guest-reconnect-rpc.md).
It implements the optional `driver.GuestReconnectHost` port on runnerd. It does
not advertise a capability or enable the guest listener. The next driver handshake
can call `host.AuthorizeGuestReconnect(ctx, sessionID, prove)`; `prove` exchanges
only the validated challenge for a signature on its bounded guest connection.

One call captures the accepted agent connection, local placement generation and
registry boot identity. Both RPCs use that connection's existing outbound queue
and runner-origin pending table. Redial never moves an old proof to the new queue.
Before the guest callback, before accept, and before returning authority, check the
original connection and live local placement again. The control plane remains the
source of durable placement, expiry, membership, policy and single-use decisions.
The driver supplies session identity; guest data cannot choose workload scope.
Unknown/zero placement and unaccepted connections refuse. Current cold-resume
placement bookkeeping is insufficient for this guard and must be reconciled before
enabling recovery; no permissive fallback is introduced.

The whole call has a five-second context budget, at most one in-flight operation
per session and 64 across a runner. Admission fails immediately when full. Slots
and pending RPC entries are released on return. Guest proof callbacks must honor
the context and bound peer/frame resources; the callback runs inline, so a broken
implementation that ignores cancellation retains its slot rather than spawning
unbounded detached work. It must not send configuration or replace a relay.

Strict shared decoders validate challenge, proof and acceptance. The challenge
must match the expected session, placement and original connection incarnation.
Refusals return a zero acceptance and only invalid/expired/fenced/unavailable;
upstream and guest error text is discarded. A failure after committed acceptance
can strand that attempt: start a fresh challenge, never replay or cache authority.
Successful return still requires fresh configuration and old-relay fencing by the
caller. No token, proof or challenge is logged.

The generic guest relay now explicitly refuses begin/accept by method name, for
both Docker and microVM guests, in addition to its existing high-bit request and
response fence. Enrollment remains guest-originated. Legacy bootstrap and other
session RPCs preserve their behavior.

A pair of separately callable begin/accept methods was rejected because callers
could accidentally migrate an attempt across agent reconnection, omit validation
or omit cleanup. A new public RPC/HTTP route was rejected: the existing agent
session-RPC queue and driver host callback already own this boundary.

Verification covers real WebSocket round trips, wrong scope, malformed replies,
fixed refusals, cancellation, deadline, admission, local replacement and control
redial. A built execution probe calls the port through real runnerd/RunAgent with
a synthetic driver and proof provider. No shipping CLI or listener invokes this
port yet, so this is executable port coverage, not VM or guest continuity evidence.

Depends on OSS PR113. Before enablement: guest key custody/enrollment, negotiated
capability, strict 4 KiB/five-second guest handshake, bounded peer acceptance,
current configuration delivery, cold lifecycle ordering, relay fencing and real
PID/PTY/agent qualification. Hosted deployment additionally depends on Cloud
PR146/147/148. No RAM persistence is introduced.

Guest identity and preamble implementation are described in
[the guest-side slice](2026-09-30-guest-reconnect-session.md). Host listener
integration and capability enablement remain separate gates.

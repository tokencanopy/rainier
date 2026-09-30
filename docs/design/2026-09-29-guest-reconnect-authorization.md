# Guest reconnection authorization foundation

This change supplies protocol, application and PostgreSQL primitives for the
first stage of authenticated live-guest reconnection. It does **not** enable
reconnection, advertise a driver capability, expose a new RPC method, recover a
listener, or persist guest RAM. Existing guest and Docker paths keep their current
behavior. The staged production design lives in rainier-cloud PR143.

## Authority and ownership

`runnerplane.Binding.ConnectionGeneration` is taken from the registration that
owns the request's WebSocket. It is not a current-generation lookup or a field in
an announce/request body. Existing hosts may ignore it; new generation-sensitive
operations must compare it with durable fleet state. Propagating it alone does
not add authorization to existing session RPC methods.

`controlapp.GuestReconnect` owns randomness, proof verification and bootstrap token
creation. The optional `control.GuestReconnectStore` owns enrollment, challenge
state and transactional consumption. Hosted adapters must hold current membership
and policy authorization in addition to implementing the placement/runner fences.
The reusable `controlapp/repotest.RunGuestReconnect` contract covers concurrency,
replay, scope, lifecycle, pending replacement and bootstrap preservation. The
PostgreSQL suite additionally exercises adapter reopen and expiry during lock
waits. Hosted qualification must run the same contract against its own adapter.

No guest input selects the workspace, session, runner or placement scope.

Enrollment spends the initial bootstrap token and pins a public Ed25519 key and
boot epoch atomically. Only the guest holds the private key. A normal cold-boot
bootstrap mint invalidates the enrollment and pending attempt. A reconnect token
replacement preserves enrollment. These are deliberately different operations.

Beginning an attempt replaces only the pending challenge. It cannot revoke a live
connection or replace a bootstrap capability. Proof acceptance compares the exact
pinned identity, pending attempt, placement and connection generation, increments
the connection epoch, consumes the attempt and installs a new bootstrap hash in
one transaction. Concurrent consumers produce one success. A lost response cannot
be replayed; the caller must start a new attempt.

The PostgreSQL adapter holds runner, session and bootstrap row locks until commit,
checks connected/live state and uses database time after acquiring those locks
for expiry. Enrollment and challenge issuance explicitly lock the bootstrap row
before evaluating expiry: an UPDATE predicate alone can be evaluated before a
lock-only transaction releases that row. The caller's clock cannot extend a
five-second challenge. Database clock skew can cause a closed refusal; it must never extend validity. No failed
operation returns a token or an epoch. Database errors are fixed sentinels.

The connection epoch is scoped to an enrolled boot. It is not the placement
number, runner connection generation, or terminal controller lease. A fresh boot
invalidates the old identity and starts a new epoch sequence.

## Signed protocol

The transcript is the bytes `rainier-guest-reconnect-v1`, NUL, then four
big-endian uint32-length-prefixed printable ASCII identifiers (session, boot,
host incarnation, attempt), a big-endian uint64 placement generation and a
32-byte random challenge. Identifiers are 1–256 bytes. The host incarnation is
the authenticated runner connection generation in decimal. Public keys, signatures
and nonces use canonical unpadded base64url. Golden-vector tests pin the format.

Signature verification alone is not authorization: the corresponding pending
store row must still exist and be consumed under current authority.

## Integration work required before enabling

- Atomically enroll through the guest bootstrap exchange with capability
  negotiation and strict bounded request decoding.
- Add runner-only challenge/proof RPC routing and hosted store support.
- Retain the guest signing key in sessiond memory; fail closed on reconnect
  authorization failure and apply fresh configuration even without secrets.
- Bound unauthenticated peers and frames; authenticate before sending any boot
  configuration or replacing a live relay.
- Fence old relay input and credential RPC immediately and order accepted
  handoffs by connection epoch.
- Reauthorize recovered placements and verify the VMM, network slot and socket
  ownership before restoring listeners. Do not unlink a live listener.
- Qualify PID, PTY, child and real-agent continuity on a disposable KVM guest.

These gates remain unmet by this foundation. Memory persistence and any change to
credential custody require the separate policy amendments and lifecycle work.

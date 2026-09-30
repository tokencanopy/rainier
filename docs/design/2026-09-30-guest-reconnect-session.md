# Guest signing identity and reconnect preamble

This slice adds the guest side of authenticated live reconnection to `sessiond`.
It depends on the shared [RPC contracts](2026-09-30-guest-reconnect-rpc.md) and
[host authorization port](2026-09-30-guest-reconnect-host.md). It does not enable
host listeners or advertise a supported reconnect capability.

## Fresh boot and retained identity

`BootConfig.guest_reconnect` is an optional uint64. Absent/zero preserves the
existing guest bootstrap behavior. Protocol 1 selects enrollment on a fresh boot
only; unsupported values fail closed. A legacy redial cannot enroll later.
No production host sets this field yet. Before any host sets it, the negotiated
image/runner/control-plane capability, cold lifecycle and relay fencing must be
implemented together.

A fresh opted-in sessiond generates an Ed25519 key and a random boot epoch in
process memory. The public key and boot epoch ride `enroll_guest_reconnect`
with the single-use bootstrap token. Enrollment is required even when no secrets
are declared. The response must contain a non-null environment object and every
declared secret (an empty string is valid). An ambiguous or refused enrollment
cannot be retried or downgraded; a fresh authorized boot is required.

The private key is not serialized, logged, stored on disk or exported through
the environment. Losing sessiond loses its reconnect identity. This is continuity
of an enrolled guest, not protection against root within that same sandbox.

## Stream protocol and readiness

Later connections use existing newline-delimited `FrameControl` envelopes, with
attachment ID zero and exactly `kind` and `payload` in their control event:

| Kind | Payload |
| --- | --- |
| `guest_reconnect_challenge` | Shared `GuestReconnectChallenge` |
| `guest_reconnect_proof` | Shared `GuestReconnectAcceptRequest` |
| `guest_reconnect_accepted` | Shared `GuestReconnectAcceptResponse` |
| `guest_reconnect_refused` | Shared fixed-code refusal |

The encoded outer frame is at most 4096 bytes, excluding its newline. `NetConn`
retains its fixed 64 KiB read buffer but assembles no more than the selected
handshake bound; oversized unterminated frames close the connection. Unknown,
duplicate, missing, null and extra envelope fields fail closed. Shared strict
payload decoders validate challenge and acceptance. Any refusal ends the attempt
without echoing its contents. One guest attempt runs at a time and challenge
through acceptance has one five-second deadline.

The guest checks its retained session and boot before signing the shared binary
transcript. An accepted epoch must exceed the last observed epoch. The epoch is
remembered before configuration delivery: a lost delivery requires a new
challenge and epoch. Nothing caches a signed proof or replays an accepted token.
The host/control plane remain responsible for durable authority, current placement,
policy, single-use attempts, ownership checks and fencing the old relay.

After acceptance the guest requires a boot configuration for the same session,
protocol and accepted token. It redeems that fresh token through
`fetch_session_secrets`, including when zero secrets are declared. Configuration
and exchange waits retain their existing 30-second bounds and are additionally
bounded by the accepted token TTL. Failed proof, configuration, redemption or
validation returns a fixed error; `sessionTransport.connect` closes the stream
and does not make it available to the relay or credential dispatcher.

## Configuration and process continuity

The new path validates all environment names/values before applying any changes.
It preserves existing precedence (secrets, fixed launch settings, explicit env),
updates non-secret settings even for an empty secret response, and removes keys
previously owned by this configuration when they disappear. Unowned inherited
variables remain untouched. The legacy boot path keeps its existing behavior.

This updates sessiond and future children. An already-running child retains its
inherited environment; this is not live credential rotation inside a coding
agent. Reconnect does not execute setup/init, start an agent or replace a PTY.

## Verification and remaining gates

Tests cover secret-free enrollment, sticky refusal, late opt-in rejection,
strict stream envelopes, oversize reads before newline, cross-session/boot proof
refusal, replay epochs, failed/mismatched configuration, missing secrets,
concurrent attempts, connection closure and configuration replacement.

`TestGuestReconnectExecutable` runs a separately executed guest test artifact
against a real TCP protocol fixture, using the production bootstrap/preamble and
PTY implementation. It verifies enrollment, a refused connection, a signed new
connection, fresh token redemption and the same shell PID on the same PTY before
and after, including the child-versus-parent environment distinction. This is the
closest executable check for an unenabled guest path; the shipping host has no
caller yet. It is not AF_VSOCK/KVM or a hosted authorization/relay takeover test.

Still required before enabling support: fresh configuration delivery from the
control plane, original-instance listener ownership, accepted-epoch relay takeover,
cold-resume enrollment/generation ordering, runner recovery integration and real
live-agent PID/PTY/tool-turn qualification. No snapshot or memory persistence is
introduced. User-facing enablement documentation is deferred to that integration.

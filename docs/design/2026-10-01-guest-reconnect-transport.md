# Host guest-proof transport and listener admission

This dependent slice connects the [runner authorization port](2026-09-30-guest-reconnect-host.md)
to the [guest proof frames](2026-09-30-guest-reconnect-session.md). It is a transport
prerequisite, not enablement of live reconnection.

## Admission before work

The existing microVM listener now claims its one boot connection inline before
starting a serving goroutine. Later peers are closed inline, without a worker or
per-peer log entry. A guest that holds the first configuration write open cannot
cause an unbounded number of rejected-peer goroutines or log entries. Teardown
still owns and closes the claimed connection; a failed first write does not
release the single-use claim. Boot configuration and legacy behavior are otherwise
unchanged. The listener still refuses every later connection without sending data.

## One admitted stream, one proof

`driver.AuthorizeGuestConnection` supplies the real bounded stream exchange to
`GuestReconnectHost.AuthorizeGuestReconnect`. The driver supplies the session
assignment; guest bytes cannot select it. The helper validates the challenge and
expected session before writing, sends only that challenge, reads one strict
4096-byte outer frame, requires the proof kind and matching attempt ID, and uses
the shared strict proof decoder before returning its signature to the host.
The control plane, not this transport, verifies the signature and authority.

The entire call has a five-second ceiling. The caller deadline and the host's
proof context can shorten it. Cancellation closes blocked peer I/O; failures
close the connection and return zero acceptance with only a fixed refusal code.
Unknown host/provider text becomes `unavailable`. Duplicate proof callbacks,
acceptance without a completed proof, malformed acceptance and late success are
refused. The call remains synchronous; a host violating its cancellation contract
retains its admitted slot rather than creating detached work.

Success returns committed acceptance to the caller and leaves the same connection
open, including any buffered bytes. It does not send the accepted token, a boot
configuration or a refusal payload, and does not attach or replace a relay. This
separates the proof transport from the guarded installation that still must be
implemented. Rebuilding a reader around the raw socket would lose buffered bytes;
the caller must retain the same `relay.Conn`.

## Integration limits

No shipping listener invokes this helper yet and no boot opt-in/capability is
advertised. Relaxing `guestChannel.served` without the complete handoff would
replay configuration or let an unproven peer evict a healthy guest, so that guard
remains intact.

The next integration needs an instance-bound handoff that retains original runner
connection, local handle/boot, placement and accepted epoch through current
configuration delivery and relay installation. It must fence the old relay before
new input/RPC, bound peer admission before launching work, validate recovered
VMM/jail/socket ownership, and resolve cold enrollment/generation ordering.
Guest configuration redemption/readiness must complete before attachments can
interleave with the guest preamble; authorization success alone is insufficient.
Current launch configuration must come through the authenticated control-plane
path, not the cached initial boot token/configuration. Boot-time artifact invariants
and existing-child environment limits from the guest design still apply.

An all-in-one listener change using cached `g.boot` was rejected because it would
violate fresh configuration and single-use token rules. Spawning a goroutine for
all peers and then checking the claim was rejected because admission no longer
bounds allocated work. Keeping stream encoding in callers was rejected because
it would duplicate strict framing, cancellation and attempt correlation rules.

## Verification

Tests pin admission before the next accept, unchanged first-boot behavior,
challenge/proof round trips, invalid scope/attempt/kind/schema, oversized data
without newline, duplicate callbacks, zero authority on refusal, fixed errors,
caller and host cancellation, deadline, and buffered-reader continuity.

The built runner probe now runs the production host port and this transport over
real agent WebSocket and guest TCP streams. Its synthetic guest signs the
challenge; the control-plane fixture verifies that exact signature. Wrong scope,
revoked authority, wrong attempt and oversized proof are refused. It asserts that
no acceptance/configuration bytes reach the guest from this helper. This is
executable transport coverage, not a shipping listener, AF_VSOCK/KVM, enrolled
guest recovery, relay takeover or real coding-agent qualification result.

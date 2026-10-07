# Recovered cold-resume configuration

B1 qualification exposed a missing connection: a running guest can reconnect
through a runner restart, then cold suspend, but its driver's in-memory boot
configuration was deliberately lost on restart. A subsequent cold resume must
resolve that configuration before launching a fresh VM.

The existing runner-only guest configuration RPC already authorizes current
membership, policy, runner incarnation and the exact resuming placement. Reuse
it through a narrow optional driver-host capability. The driver invokes it only
for a recovered configuration, a negotiated reconnect guest, an enabled host and
a nonzero resume placement. The runner requires an active pending resume, captures
its handle, boot and control connection, and rechecks all of them after the RPC.
Wrong session/placement, changed connection or local boot, cancellation, refusal,
and malformed responses fail before mint or launch. The driver independently
checks session, protocol, reconnect negotiation and the absence of a token.
The existing fresh bootstrap mint then authorizes secret resolution by the guest.

Configuration remains in memory and becomes the driver's live boot configuration
only on successful launch. Durable metadata still contains neither configuration
nor bootstrap tokens. The recorded image and workspace/home devices remain the
resume target; this is not cross-host placement or an image migration. Legacy
and unversioned recovered resumes keep their previous refusal.

Persisting the old configuration would violate the existing metadata boundary.
Caching reconnect configuration for a later resume would detach it from the new
placement. Resolving it at the claimed cold-resume boundary uses existing current
authority without adding a wire method or a second lifecycle mechanism.

Validation covers actual driver reconstruction, fresh boot configuration/token,
invalid replies with no mint/launch, metadata non-persistence, and runner RPC
fencing across session, placement, boot, handle and connection changes. The live
KVM sequence must still pass cold resume followed by runner restart. Full B1
qualification, independent/adversarial review and cleanup remain release gates.

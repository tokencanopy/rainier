# Guest reconnect integration candidate

This connects the shared reconnect protocol, runner admission, guest readiness,
and hosted and standalone authorization. B1 remains unqualified. The runner
opt-in is `--microvm-guest-reconnect` (or `RAINIER_MICROVM_GUEST_RECONNECT=1`),
disabled by default. Only a matching host dispatcher plus both runner capabilities
can negotiate enrollment. Volatile standalone stores never enable it.

## Ownership and delivery

A negotiated fresh boot consumes the initial configuration once and enrolls its
in-memory public key before publishing a relay. Subsequent connections retain a
bounded admission lease through proof, fresh configuration, token redemption,
and ready/ack. Accepted proof consumes a monotonically increasing connection
epoch, closes the old hub, drains admitted callbacks, and prevents queued old
callbacks from forwarding RPC. Replacement control connections do not inherit
an old connection's queue or authority.

The runner-only `guest_reconnect_configuration` RPC returns a current non-secret
launch specification scoped to session and placement. It neither mints nor spends
a token. Request/proof frames remain capped at 4 KiB; the authorized configuration
response is capped at 4 MiB and rejects unknown fields, duplicate keys, nulls and
embedded bootstrap tokens. Hosted resolution checks current membership, policy,
and runner binding. The separately accepted proof supplies the narrow token.

The guest refreshes ordinary environment values and secrets, including an empty
secret response. It refuses changes to the already-running launch (command,
repositories, setup/init, proxy, agent manifest/home layout) before redemption;
those changes require a fresh boot. It never relaunches the current agent or PTY
as part of reconnect.

## Runner restart candidate

Every current-version microVM driver holds an exclusive state-directory lock
before discovery and cleanup, including when reconnect is disabled. Older
binaries do not participate in this lock and must be drained before rollout. Fresh launches persist non-secret process identity: host boot ID,
process start time, and network namespace inode/device. Recovery requires current
control-plane placement authorization before binding a listener. Local checks
verify exact process arguments and UID/GID, persisted identity, jail and VMM
socket ownership, cgroup membership, and the adopted network slot. The host only
replaces an owned, refused Unix control socket. It never unlinks an active
listener or the surviving VMM's device socket, and never reloads a boot token.

The recovery worker is sequential, belongs to an accepted control connection,
and retries failed candidates with fresh authorization. Failed, replaced or
canceled authority cannot install a placement. A recovered listener starts with
its initial delivery consumed, so every peer must prove its enrolled key.

## Interrupted launches

Fresh creation and cold resume durably own their namespace, disks and placement
before launch. An uncertain result retains a blocked record and consumes capacity;
retry and workspace deletion cannot reuse or remove that record's disks.

The engine writes a synced launch marker before starting the jailer and publishes
its PID and process start time atomically. Signal authorization requires exact
native argv and the original process start time before every signal, including
shutdown escalation. Recovered-process waits recheck that lifetime. These checks
do not provide a Linux pidfd guarantee: numeric PID check/signal has an immediate
race window, though the delayed escalation window is closed. A missing or invalid PID on the same host boot is unknown,
not proof of exit. Recovery and teardown retain the jail and network resources
until the child is confirmed exited. If the process identity was lost in that
window, host inspection or a confirmed host reboot is required; an empty cgroup
or missing API socket cannot prove that a jailer has exited. Failed Stop keeps
process ownership and its single reaper for a later teardown attempt.

## Remaining qualification gates

- Qualify cold resume followed by runner restart against the explicit resuming
  lifecycle, durable launch ownership and refreshed recovery identity.
- Exercise the built processes over real transports and review the integrated
  branches independently and adversarially.
- Run the bounded B1 harness on a disposable KVM host: relay loss, gateway
  replacement and runner restart, preserving PID/start time, PTY and waiting
  child, followed by another real coding-agent tool turn.
- Record immutable artifacts, negative controls and independently verified
  teardown. No RAM snapshot is part of this capability.

Unit/race results and cross-compilation do not constitute that live qualification.

## Standalone authority

The PostgreSQL dispatcher locks the accepted runner generation, current placement,
bootstrap record and current owner before checking the configured allowlist and
resume policy. Enrollment/proof/token mutations commit with closed audit events.
Secrets are resolved after commit; resolution failure never restores a capability.
All responses fit a bounded 4 MiB budget, including JSON expansion of secret values.
The authority boundary refuses a caller's existing transaction so a successful
return really commits before delivery.

Begin, accept, configuration and mint are runner-origin only. Legacy guest token
redemption remains guest-origin only at epoch zero; proof-issued redemption is
runner-origin only at a positive epoch. PostgreSQL bootstrap RPCs receive these
current-authority checks even before negotiation; this intentionally prevents a
legacy path from bypassing owner revocation or spending a proof-issued token.
The other legacy RPC methods retain their existing handlers.

### Recovered VM exit evidence

A recovered VMM can exit between a successful signal and the next identity
probe. Linux removes its argv before the parent reaps the zombie, so an empty
command line alone cannot distinguish that exit from an unknown live process.
Teardown accepts a readable process pidfd only when the recorded process birth
matches after opening the descriptor and the launch marker names the current,
known host boot. Unlike a zombie-leader stat, the process pidfd is readable only
after the last thread exits; no PIDFD_THREAD flag is used. An unreadable
or different live lifetime still refuses teardown. Tracked children are Waited
even when exit evidence is already conclusive, so the host does not leak zombies.
Native subprocess regressions cover exit after SIGTERM, an already-exited
recovered process, tracked-child reaping, mismatched-birth rejection, and a
zombie leader with a surviving pthread worker.
The [Linux pidfd contract](https://man7.org/linux/man-pages/man2/pidfd_open.2.html)
is the whole-thread-group exit witness.

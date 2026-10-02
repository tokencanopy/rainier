# Guest reconnect integration candidate

This connects the shared reconnect protocol, runner admission, guest readiness,
and hosted authorization. It remains off by default. B1 is not qualified and
this candidate does not enable the capability in the runner CLI.

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
its PID atomically. A missing or invalid PID on the same host boot is unknown,
not proof of exit. Recovery and teardown retain the jail and network resources
until the child is confirmed exited. If the process identity was lost in that
window, host inspection or a confirmed host reboot is required; an empty cgroup
or missing API socket cannot prove that a jailer has exited. Failed Stop keeps
process ownership and its single reaper for a later teardown attempt.

## Remaining qualification gates

- Complete cold-boot lifecycle ordering and recovery identity refresh before
  exposing the opt-in CLI capability.
- Exercise the built processes over real transports and review the integrated
  branches independently and adversarially.
- Run the bounded B1 harness on a disposable KVM host: relay loss, gateway
  replacement and runner restart, preserving PID/start time, PTY and waiting
  child, followed by another real coding-agent tool turn.
- Record immutable artifacts, negative controls and independently verified
  teardown. No RAM snapshot is part of this capability.

Unit/race results and cross-compilation do not constitute that live qualification.

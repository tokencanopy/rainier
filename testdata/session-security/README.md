# Browser sandbox policy fixture

These files are a public, test-only snapshot used by the session-image
qualification job. They let the core repository exercise the same kind of
Chromium user-namespace boundary as a hosted session without checking out a
Rainier Cloud repository or requiring a cross-repository GitHub token.

The seccomp file starts from Docker Engine 27.5.1's default profile and adds
only the exact syscall shapes needed by Codex's Bubblewrap and Chromium's
namespace sandbox. The AppArmor file keeps Docker's device, procfs, sysfs, and
kernel denials while admitting Bubblewrap's namespace setup and Chromium's
three namespace-map writes. Both policies remain default-deny; this fixture
must never be used as a production host policy.

The runtime policy is owned and qualified by Rainier Cloud independently. If
either policy changes, update this snapshot deliberately, preserve the hashes
in `scripts/session-image-security-policy-test.py`, and make the corresponding
Cloud policy change in its own review. The core workflow must remain able to
run with only this public repository.

Pinned fixture invariants:

- Docker base profile: 27.5.1; canonical JSON SHA-256: `885442dc08f21f8d60f99ea43d59af88b1c529103815fe24bbf9ce998d3a609d`
- Full Rainier seccomp canonical JSON SHA-256: `4fb409bf9925eeaab50f950118682150cafa4868dba3b277554dd3936b333ae0`
- AppArmor profile SHA-256: `1035744cd6dd47f242b4775e15e7c8fae5e38603e748d5111b6e166e26b09a96`

The canonical JSON hash is calculated with sorted keys and compact separators;
the raw file hash is intentionally not part of the contract because harmless
formatting changes should not alter the policy identity.

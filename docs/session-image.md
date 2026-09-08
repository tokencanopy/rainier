# The session image: what a session actually has, and why

This is the image every Rainier session runs. It is built from the `Dockerfile`
at the root of this repository, and on the hosted product it is the *only*
image a Dedicated runner can pull: rainier-cloud's `runner-artifacts` workflow
checks this repository out at the OSS ref, builds that file, pushes it, reads
the digest back from the registry, and prints it as the `session_image` an
operator pastes into the Dedicated root's `runner_artifacts`. A runner holds a
short-lived registry credential only long enough for that pull, so a session
created against any other image fails with an unauthenticated pull.

That is the reason the toolchain is in the image rather than in an
environment's setup script. Baked in, it costs one pull. Installed by a setup
script, it costs an egress allowlist entry per installer, a build container
with the read-only rootfs deliberately dropped, a snapshot per environment, and
a first-session build every developer waits through.

## What is in it

Pinned by version in the `Dockerfile`, and by SHA-256 in
`images/session/toolchain.sh` for everything downloaded.

| | |
|---|---|
| **Agents** | Claude Code (npm, exact version), Codex (upstream `codex-package` archive, kept whole under `/usr/local/lib/codex`) |
| **Source control** | `git`, `gh` (GitHub CLI), OpenSSH client |
| **C/C++** | `build-essential` (gcc, g++, libc6-dev), `make`, `pkg-config`, `zlib1g-dev` and buildpack-deps' standard header set |
| **Go** | the official toolchain tarball, behind a wrapper that creates its caches |
| **Node** | the base image's LTS line, with `npm` |
| **Python** | `python3`, `venv`, `pip`, and `uv`/`uvx` |
| **Shell** | `bash`, GNU coreutils, findutils, grep, sed, gawk, diffutils, `patch` |
| **Search and data** | `ripgrep`, `jq` |
| **Network** | `curl`, `wget`, CA certificates, `openssl`, `nc` |
| **Archives** | `tar`, `gzip`, `bzip2`, `xz-utils`, `zip`, `unzip` |
| **Diagnostics** | `ps` (procps), `ss` (iproute2), `lsof`, `dig` (dnsutils), `file` |
| **Editing and transfer** | `less`, `nano`, `vim-tiny`, `rsync` |

Two manifests are written into the image at build time, because a target list
is not evidence and a digest on its own answers nothing:
`/usr/local/share/rainier-os-packages.txt` (every Debian package and version
actually installed) and `/usr/local/share/rainier-npm-global.json`.

**What "version-pinned" does and does not cover.** Every base image is a
digest; every downloaded artifact is a version in the URL and a SHA-256 checked
before extraction; Claude Code is an exact npm version, which for that package
is one immutable tarball with no dependency tree under it. The Debian package
set is pinned by the base image digest and *recorded* by the manifest, not
pinned by version — apt resolves against a moving archive. Pinning that too
means sourcing from `snapshot.debian.org`, which is a real option and a real
cost (a slow, frequently unavailable host in the build path); it has not been
taken. Rebuilds of one commit are therefore reproducible in every layer except
the apt one, and the manifest is how you find out what the apt layer was.

## Why this base image, and not a Dev Containers one

The choice was between a pinned Debian/Ubuntu **Dev Containers base image** and
a **plain Debian/Ubuntu base with selected Dev Container Features**. The
recommendation, and what is implemented, is neither: a plain pinned Debian
bookworm — which is what `node:22-bookworm` is — with distro packages and
checksum-pinned upstream releases on top.

- **Dev Container Features are applied by the devcontainer CLI**, not by
  `docker build`. The publish path here is a plain `docker build --file
  oss/Dockerfile` inside rainier-cloud's workflow. Adopting Features means
  putting Node, the devcontainer CLI, and a set of third-party OCI artifacts
  into the build path of the platform's own session image — a new build-time
  dependency and a new trust surface, in front of the one image a Dedicated
  runner is allowed to pull. Most Features are also consumed by mutable tag,
  which is the opposite of what a `session_image` digest is for.
- **The Dev Containers base images assume a user model this platform does
  not have.** They ship a `vscode` account at uid 1000 with passwordless sudo.
  Rainier runs every container as uid 1000 with `no-new-privileges` and every
  capability dropped, and `/usr/local/bin` must stay root-owned because
  sessiond and the agents live there. Starting from those images means undoing
  their user setup before doing any of the actual work — more moving parts than
  starting from Debian, for a base whose value is conventions this image does
  not follow.
- **`node:22-bookworm` is a plain Debian bookworm** with the Node LTS line,
  npm, and buildpack-deps' compiler and header set already on it. It is an
  official image, it is the established way to get a supported Node without a
  third-party apt repository, and its digest is one this organization has
  already resolved and qualified for rainier-cloud's own environment image.
  Everything else is an explicit `apt-get install` line and a checksum in
  `images/session/toolchain.sh`, which is where a reviewer can see it.
- **Follow-up, not a blocker:** `node:22-bookworm` inherits buildpack-deps'
  full set, which includes things this image has no use for. Moving to
  `node:22-bookworm-slim` and installing the compiler set explicitly is a size
  reduction worth taking once that digest is qualified; the explicit apt list
  makes it a mechanical change.

The base is an `ARG`, so a developer on an arm64 machine can pass the tag and
build natively (`make session-image BUILD_ARGS='--build-arg
BASE_IMAGE=node:22-bookworm'`). The toolchain script carries checksums for both
architectures and refuses to install one architecture's binaries into the
other's userland. CI never overrides the ARG: what ships is the digest.

For the local fleet on ARM, use
`BUILD_ARGS='--build-arg BASE_IMAGE=node:22-bookworm' make e2e`.
Fleet startup delegates to the same image-build target and forwards that
explicit development override. It never silently substitutes an unpinned base.

This adds no general `devcontainer.json` support and is not a step toward it.

## The runtime contract this image is built against

The driver (`internal/driver.runArgs`) runs every session container as
`1000:1000`, with `no-new-privileges`, every capability dropped, a read-only
rootfs, a noexec tmpfs on `/tmp`, the session's volume at `/workspace`, and the
agent-home volume at `/rainier/agents`. The image exists to make that
survivable:

- **Non-root.** `USER 1000:1000` is the image's own default, not only a flag at
  the call site. The base image's uid-1000 account is renamed `rainier` rather
  than joined by a second one, so there is exactly one.
- **sessiond is protected.** It lives in root-owned `/usr/local/bin` and the
  entrypoint names it by **absolute path**. `/opt/rainier-env/bin` is writable
  by the session user and first on `PATH` — that is what lets an environment's
  setup script install anything — so a `PATH`-resolved entrypoint would let a
  setup script drop its own `sessiond` there and have `docker commit` bake it
  into the image every later session of that environment boots as PID 1. The
  agents are in `/usr/local/bin` for the same reason.
- **`/workspace` ownership.** The image's `/workspace` is owned by 1000:1000,
  which is also what docker copies onto a freshly created volume at that mount
  point. The driver's init job does the same job independently; the two agree.
- **Writable caches.** `$HOME` is on the read-only rootfs and `/tmp` is noexec,
  so `GOCACHE`, `GOMODCACHE`, `GOPATH`, `GOTMPDIR`, `XDG_CACHE_HOME`,
  `npm_config_cache`, `PIP_CACHE_DIR` and `UV_CACHE_DIR` all point onto the
  workspace volume. Go additionally needs `GOTMPDIR` to already exist and needs
  to execute what it builds there, which is why `/usr/local/bin/go` is a small
  root-owned wrapper that creates the directories and then execs the real
  toolchain — a `go test` on a noexec `/tmp` otherwise dies at the last step of
  a green build with `fork/exec /tmp/...: permission denied`.
- **`XDG_CONFIG_HOME` is deliberately left alone.** Configuration is where
  tools write credentials, and `/workspace` is what checkpoints, archives and
  `rainier pull` carry off the runner. A tool that wants to persist a token
  gets a read-only `$HOME` and fails loudly, which is the wanted outcome.
- **Fresh agent homes reconstruct only required non-secret state.** Credential
  custody intentionally carries Claude's `.credentials.json`, not its whole
  `.claude.json`, because that application file can contain workspace-local
  project and MCP state. After a positive credential restore, sessiond creates
  a missing `.claude.json` with only `hasCompletedOnboarding: true`; an existing
  file always wins. This prevents Claude's interactive first-run login wizard
  after runner replacement without copying one workspace's mutable state into
  another. No seed is created when custody has no credential.
- **Credential-sync rollout is versioned.** The agent manifest and upward
  credential RPCs carry their own protocol version. A new session process
  refuses to boot from an old manifest, and a new control plane refuses custody
  traffic from an old session process. An old session process can still read a
  home it already mounted, so deployments must stop old control planes, publish
  the matching session image, and replace existing sessions before accepting
  agent login/logout traffic.
- **No credential, no privilege.** Nothing token-shaped is baked in, `sudo` is
  not installed, no host path is mounted, and no docker socket is anywhere near
  a session.

## Testing it

```sh
go test ./internal/driver/ -run TestSession   # the contract, no docker needed
make session-image                            # build it
make session-image-smoke                      # what --version cannot tell you
```

`internal/driver/image_contract_test.go` reads the `Dockerfile` and the
toolchain script as text and fails the ordinary `go test ./...` if the
entrypoint stops being absolute, a base stops being a digest, a cache moves off
the workspace, `sudo` appears, a download stops being checksum-verified, or a
promised tool leaves the install list. That is the half of the contract that can
be checked on a machine with no docker, which is where Dockerfile edits get made.

`scripts/session-image-smoke.sh` is the other half, and it is deliberately
functional rather than a version parade. It compiles and runs C and C++, builds
a Go program *and runs its tests* (the noexec `/tmp` case), runs Node and
Python, drives `make`, creates venvs and installs a wheel offline, installs an
npm dependency offline and checks the cache it used, serves HTTP from Python
and HTTPS from Node against a certificate `curl` verifies, makes a real git
commit, starts Claude Code and Codex with a read-only `$HOME`, asks Codex where
it resolved its own package and runs the tool host it names, and brings the
real entrypoint up as PID 1. Every probe runs in a container wearing the
driver's own restrictions with **no network at all**, and nothing is relaxed to
make a check pass — a check that cannot pass under the real contract is
reporting a real defect in the image.

## Codex is a package, not a binary

The upstream `codex-package` archive is a manifest, `bin/codex`, the
`bin/codex-code-mode-host` tool host, and bundled ripgrep, bubblewrap and zsh
under `codex-path` and `codex-resources`. Codex finds every companion by walking
up from the **resolved** path of its own executable until it finds
`codex-package.json`.

The image used to install `codex-${TRIPLE}.tar.gz`, which is that one executable
and nothing else. `codex --version` was green and `codex --search` failed closed
on a missing `/usr/local/bin/codex-code-mode-host`. Measured against the real
`rust-v0.153.4` archive, with `codex doctor --json` reporting Codex's own
resolution rather than ours:

| Install shape | `install context` | `runtime.search` |
|---|---|---|
| Complete package | names `package`, `bin`, `resources`, `path` | `bundled`, the package's `codex-path/rg` |
| `bin/codex` alone | bare `other` | `system` |
| `bin/codex` alone, tool host **added to PATH** | **byte-for-byte identical to the row above** | `system` |
| Complete package **minus the tool host** | still names the package — doctor does not notice | `bundled` |
| **Symlink on PATH** → package `bin/codex` | **identical to the complete package** | `bundled` |

So: the tool host goes neither on PATH nor beside sessiond in `/usr/local/bin`
(row 3 is row 2 exactly, so PATH placement is inert); the PATH entry is a
symlink, because Codex resolves symlinks before it looks for the package and a
copy would leave the companions unreachable; and the layout is asserted
separately, because row 4 passes every doctor check with the host deleted.

`images/session/toolchain.sh` extracts the archive to `/opt/toolchain/lib/codex`
and links `$BIN/codex -> ../lib/codex/bin/codex`. The link is **relative** so
that it resolves in the toolchain stage and again after the final image copies
both directories under `/usr/local`. The script rejects an incomplete layout —
`require_codex_package` checks the manifest's version, target and layout fields
and every member — so a build that lost a piece fails rather than shipping.

The package is root-owned under `/usr/local`, like sessiond and the other
agents: a session user who could rewrite the tool host could rewrite what the
agent spawns. `/usr/local/lib/codex` is deliberately **not** the user-writable
`/opt/rainier-env` prefix.

Packaging is not the whole of Codex readiness. If bundled bubblewrap cannot
create a namespace on a particular runner, that is a security boundary to
diagnose and review separately; do not reach for
`--dangerously-bypass-approvals-and-sandbox`, a privileged container,
`seccomp=unconfined`, added capabilities or host sysctl changes to get past it.

## `gh` receives a brokered credential per invocation

`/usr/local/bin/gh` is a root-owned wrapper. It invokes sessiond's
`github-cli` subcommand, which asks the existing session credential socket for
a GitHub credential in a managed session and then execs the root-owned upstream
binary at `/usr/local/libexec/rainier/gh`. The credential is inserted only as
`GH_TOKEN` in that child environment; it is not written to the shell,
configuration, image, or workspace. The wrapper passes normal gh commands
through unchanged, including `gh auth token`.

This narrows accidental persistence, not process isolation: gh descendants
inherit `GH_TOKEN`, same-UID workload processes may be able to inspect it, and
workload code can deliberately copy or print it. A managed invocation whose
credential retrieval fails refuses to start gh rather than falling back to an
inherited token or an interactive/config login. Top-level help and version
forms can run offline without retrieval.

Configuration-write limitations remain unchanged. `$HOME` is read-only and
credential configuration is intentionally not moved to `/workspace`, so gh
configuration writes or extension installation may fail under the normal
session restrictions.

API reachability remains a separate control-plane concern:

1. **API egress.** A session's allowlist is the union of the environment's
   hosts, the providers' hosts, and the git hosts the clone needs
   (`github.com`, `codeload.github.com`, `objects.githubusercontent.com`).
   `api.github.com` is in none of them, and every `gh` command that is not a
   git operation goes there. Verified from inside a live hosted session: a
   `CONNECT api.github.com` gets `403` from the egress proxy while
   `github.com` answers `200`. An environment can add the host today with
   `--egress api.github.com`; making it part of what a cloning session gets by
   default is a control-plane change.
The launcher deliberately does not make an API host reachable. Hosted launch
material must allow `api.github.com` before authenticated gh API or PR commands
can succeed; explicit environment egress can supply it until that independent
control-plane change lands.

## Qualification repairs and rollout gate

The image pre-seeds `/workspace/.rainier` before giving the directory to UID
1000. Docker copies both to a fresh volume. This keeps compatibility with the
deployed initializer, which has only CAP_CHOWN and cannot create a directory
in an empty UID-1000-owned 0755 mount. Broadening initializer privileges or
changing workspace ownership would weaken or alter the existing contract;
seeding avoids both. A Docker-gated test calls the real initializer against the
built candidate and then checks repeated mounts as the session user.

The smoke assertion requires both exit success and the expected output;
timeout, signal, and marker-then-failure regressions guard its trustworthiness.
Claude/Codex version output must match the Dockerfile pins, and for Codex the
version alone is explicitly not enough — see below. The gh smoke checks
offline version behavior and a synthetic socket response; it never uses a real
credential.

The OSS `Session image qualification` workflow builds the exact default base
on native linux/amd64 and runs both the driver regression and functional smoke.
It does not publish images or deploy anything. Keep its successful commit/run
with the artifact being published. ARM builds using BASE_IMAGE overrides are
useful local diagnostics, not qualification of the shipping AMD64 image.
Authenticated agent workloads and cold dependency downloads under hosted
egress policy remain a separate no-setup environment gate on approved canary
capacity; never replace a runner holding active work to obtain that evidence.

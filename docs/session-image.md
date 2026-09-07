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
| **Agents** | Claude Code (npm, exact version), Codex (upstream static release) |
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
commit, starts Claude Code and Codex with a read-only `$HOME`, and brings the
real entrypoint up as PID 1. Every probe runs in a container wearing the
driver's own restrictions with **no network at all**, and nothing is relaxed to
make a check pass — a check that cannot pass under the real contract is
reporting a real defect in the image.

## `gh` is installed. It cannot open a pull request yet.

Installing the GitHub CLI is an image change and it is done. Making it able to
open a PR is not an image property, and two separate things are missing. Both
are outside this image's scope and neither is worked around here.

1. **API egress.** A session's allowlist is the union of the environment's
   hosts, the providers' hosts, and the git hosts the clone needs
   (`github.com`, `codeload.github.com`, `objects.githubusercontent.com`).
   `api.github.com` is in none of them, and every `gh` command that is not a
   git operation goes there. Verified from inside a live hosted session: a
   `CONNECT api.github.com` gets `403` from the egress proxy while
   `github.com` answers `200`. An environment can add the host today with
   `--egress api.github.com`; making it part of what a cloning session gets by
   default is a control-plane change.
2. **Authentication.** A session's GitHub credential is minted per operation by
   sessiond's git credential helper and printed only onto the pipe the asking
   `git` process is reading (`cmd/sessiond/helper.go`). `gh` does not consult
   git credential helpers: it reads `GH_TOKEN`/`GITHUB_TOKEN` from the
   environment, or a token stored in its own config directory. Neither exists
   in a session, and neither should be created by copying one in:
   - a token in the process environment is visible to every process in the
     container for the life of the session, and the end-to-end suite asserts
     that nothing token-shaped is in a session's environment at all;
   - a token in a config file under `/workspace` would ride out on every
     checkpoint, archive and `rainier pull`;
   - a token under `$HOME` cannot be written, because `$HOME` is read-only.

   The shape that fits what already exists is the one the git helper already
   has: a per-invocation mint over the session RPC, handed to `gh` in its own
   process environment and nowhere else, so the token's life is one command.
   That is a control-plane and sessiond change, and it belongs with the GitHub
   connection work, not here.

Until both land, the working path from a session is unchanged and complete for
everything except opening the PR itself: `git push` through the brokered
credential, then open the PR from the compare link GitHub prints, or from a
laptop. The image half of "`gh` works" is done; the integration half is
tracked separately.

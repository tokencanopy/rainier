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
| **Databases** | PostgreSQL 17 client *and server* (`psql`, `initdb`, `pg_ctl`, `pg_dump`, `createdb`, `pg_isready`, …), SQLite 3 (`sqlite3`), Redis (`redis-server`, `redis-cli`) — installed, never started; see [Local services](#local-services-a-developer-starts) |
| **Network** | `curl`, `wget`, CA certificates, `openssl`, `nc` |
| **Archives** | `tar`, `gzip`, `bzip2`, `xz-utils`, `zip`, `unzip` |
| **Diagnostics** | `ps` (procps), `ss` (iproute2), `lsof`, `dig` (dnsutils), `file` |
| **Editing and transfer** | `less`, `nano`, `vim-tiny`, `rsync` |

Three manifests are written into the image at build time, because a target list
is not evidence and a digest on its own answers nothing:
`/usr/local/share/rainier-os-packages.txt` (every Debian package and version
actually installed), `/usr/local/share/rainier-npm-global.json`, and
`/usr/local/share/rainier-apt-sources.txt` (every apt archive the build
resolved against — there are two of them; see [Local services](#local-services-a-developer-starts)).

**What "version-pinned" does and does not cover.** Every base image is a
digest; every downloaded artifact is a version in the URL and a SHA-256 checked
before extraction; Claude Code is an exact npm version, which for that package
is one immutable tarball with no dependency tree under it. The Debian package
set is pinned by the base image digest and *recorded* by the manifest, not
pinned by version — apt resolves against a moving archive, and that is now true
of two archives rather than one: Debian's, and the PostgreSQL project's own
(PGDG), whose *signing key* is pinned here by full fingerprint. Pinning that too
means sourcing from `snapshot.debian.org`, which is a real option and a real
cost (a slow, frequently unavailable host in the build path); it has not been
taken. Rebuilds of one commit are therefore reproducible in every layer except
the apt one, and the manifest is how you find out what the apt layer was.

## Local services a developer starts

An integration test needs a database, and a session has no way to get one after
the fact: `sudo` is not installed, the rootfs is read-only, and the egress
allowlist does not carry a package archive. So PostgreSQL 17 — the **server**,
not only `psql` — SQLite and Redis are in the image, and **nothing starts
them**. There is no service in the entrypoint, no cluster in any layer, and no
port bound until somebody runs one of the commands below. Each session
container has its own network namespace, so the loopback these servers bind is
reachable from that session and from nothing else: not the runner, not another
session, not the host.

**Durable** state lives on `/workspace`, under `/workspace/.services`, because
that is the one writable, persistent path a session has — `$HOME` is on the
read-only rootfs and `/tmp` is a per-container tmpfs a suspend and resume does
not carry. That also means a database on this path is inside what checkpoints,
archives and `rainier pull` carry off the runner. Put test data there, not
anything you would not want in an archive.

**Runtime** state — the unix sockets and the pidfiles — deliberately goes the
other way, under `/tmp/rainier-services`. `protocol/workspace.TarGz` *refuses*
a socket rather than skipping it, on the principle that silently shipping a
tree that is not the tree you named is the worse failure. So a socket under
`/workspace` turns `rainier push` or `rainier pull` of any tree containing it
into an error — and an unclean container exit leaves the socket file behind on
the volume to keep doing so. A socket is per-container state anyway; the server
recreates it on every start.

### PostgreSQL

```sh
rainier-pg init          # initdb a cluster in $PGDATA (UTF8, C.UTF-8, trust auth)
rainier-pg start         # pg_ctl start on 127.0.0.1:$PGPORT and $PGHOST
rainier-pg status        # exit 0 only while it is accepting connections
createdb myapp_test      # ordinary client tools; PGHOST/PGPORT are already set
psql -d myapp_test
rainier-pg url myapp_test    # postgres://rainier@127.0.0.1:5432/myapp_test?sslmode=disable
rainier-pg restart
rainier-pg stop
```

`rainier-pg` is a root-owned shell script in `/usr/local/bin`; it installs
nothing, downloads nothing and needs no privilege. The raw tools are on `PATH`
and the equivalent commands are:

```sh
mkdir -p "$(dirname "$PGDATA")" "$PGHOST"
initdb --pgdata="$PGDATA" --username="$(id -un)" \
       --encoding=UTF8 --locale=C.UTF-8 --auth-local=trust --auth-host=trust
printf "unix_socket_directories = '%s'\nlisten_addresses = 'localhost'\n" \
       "$PGHOST" >> "$PGDATA/postgresql.conf"
pg_ctl --pgdata="$PGDATA" --log="$RAINIER_SERVICES_DIR/postgresql/server.log" \
       --options="-p $PGPORT" -w start
pg_ctl --pgdata="$PGDATA" --mode=fast -W stop   # then wait for postmaster.pid to go
```

Three of those lines are the reason the helper exists.

- **`unix_socket_directories`.** PostgreSQL's compiled-in default is
  `/var/run/postgresql`, which is on the read-only rootfs. Without the
  override the postmaster cannot create its socket and a plain `pg_ctl start`
  fails on a correct image.
- **`--locale=C.UTF-8`.** This image generates no locales, so an `initdb` that
  inherited an unset `LANG` produces an **SQL_ASCII** database that mangles the
  first non-ASCII row a test inserts.
- **`-W` and then waiting for `postmaster.pid`, and never trusting that file
  on its own.** `pg_ctl -w stop` polls `kill(pid, 0)`, which cannot tell a
  shut-down postmaster from an unreaped zombie. A real session's PID 1 is sessiond, which reaps orphans
  (`internal/reap`), but a bare `docker run --entrypoint …` PID 1 does not, and
  a stop that hangs for a minute in one shape and not the other is not worth
  debugging twice. The server removing its own pidfile is its own completion
  signal and is true under either. The same file is also the ordinary state
  after the container this cluster last ran in went away — `$PGDATA` is on the
  volume and survives, the postmaster does not — so `rainier-pg stop` asks the
  server before it signals anything. A pid with nothing behind it may since
  have been reused by something unrelated in the new container. The stale file
  is left in place rather than deleted: `pg_ctl start` already clears one, and
  refuses when the pid really is in use, which is PostgreSQL's call to make.

`RAINIER_PG_TIMEOUT` (seconds, default 60) bounds a start and a stop alike.

`trust` authentication is safe **here and only here**: the server listens on
`localhost` inside the session's own network namespace and its data directory
is the session user's. Do not copy this configuration anywhere a second party
can reach the port.

Environment already set by the image: `RAINIER_SERVICES_DIR`
(`/workspace/.services`, durable) and `RAINIER_SERVICES_RUNTIME_DIR`
(`/tmp/rainier-services`, sockets and pidfiles), and from those `PGDATA`,
`PGHOST` (a socket directory, so bare `psql` connects with no flags), `PGPORT`
and `PGDATABASE`.

For PostgreSQL, **`PGDATA` and `PGPORT` are the knobs** — a second cluster is a
second `PGDATA` on a second `PGPORT`. They are PostgreSQL's own variables and
every client honours them, which is why the image does not add a second,
overlapping one. `RAINIER_SERVICES_DIR` and `RAINIER_SERVICES_RUNTIME_DIR` are
where those defaults come from, and are what `rainier-redis` reads.

### Running rainier-cloud's PostgreSQL-backed tests

```sh
rainier-pg up                       # init if needed, then start
createdb rainier_test
export RAINIER_TEST_DATABASE_URL="$(rainier-pg url rainier_test)"
go test ./...                       # in a rainier-cloud checkout
```

The suite migrates the database itself. `rainier-pg url` prints exactly the DSN
shape those tests parse; `make canary` in that repository takes the same
variable.

### SQLite and Redis

```sh
sqlite3 app.db "select sqlite_version()"

rainier-redis start      # 127.0.0.1:$RAINIER_REDIS_PORT (default 6379), protected mode
rainier-redis ping       # PONG
rainier-redis url        # redis://127.0.0.1:6379/0
rainier-redis stop
```

SQLite needs nothing started — it is a library and a shell, and Python's
`sqlite3` module is the same library.

### What is deliberately not here

Named, because "not supported" and "nobody thought about it" look identical
from inside a session:

- **No MySQL/MariaDB, no MongoDB, no Kafka, no Elasticsearch, no MinIO/S3
  stand-in, no NATS, no RabbitMQ.** Each is a real size and maintenance cost
  and none of them is on the path of the tests this platform actually runs. A
  project that needs one should say so; adding it is a bounded change to this
  file.
- **No Docker, no Docker-in-Docker, no `docker compose`, no `testcontainers`
  substrate.** A session gets no docker socket, no privileged container and no
  nested daemon, so a test suite that reaches for testcontainers will fail —
  the answer is the local server above, not a relaxed container.
- **No `gcloud`, `terraform`, `kubectl` or other cloud-ops tooling.**
- **No PostgreSQL extension outside the standard `contrib` set** that ships
  with `postgresql-17`, and no PostGIS, TimescaleDB or `pgvector`.
- **A 64 MiB `/dev/shm`.** That is docker's default and the driver does not
  change it. It is ample for ordinary work; a deliberately parallel query over
  a large table can exhaust it and report `could not resize shared memory
  segment`. Set `max_parallel_workers_per_gather = 0` in that session's
  `postgresql.conf` rather than asking for a wider container.

### Why PGDG, and what that pin covers

Debian bookworm ships PostgreSQL 15. rainier-cloud's cell is 17 and its store
tests are written against 17, so a session with a 15 cannot run them. There is
no PostgreSQL 17 for bookworm that does not come, directly or transitively,
from **PGDG** — the PostgreSQL project's own Debian archive, and the archive
the official `postgres` image installs from.

Adding a second apt archive to the platform's own image is a real trust
decision, and it is pinned the strongest way an apt archive admits: the signing
key is fetched over HTTPS and its **full 40-hex fingerprint** is checked, as an
`ARG` in the `Dockerfile` a reviewer can read, *before* the archive is
configured at all. A substituted key is a failed build, never a silent install.
What that does **not** pin is package versions, which resolve against a moving
archive exactly as Debian's own do — `rainier-apt-sources.txt` records the
archives and `rainier-os-packages.txt` records the versions that were realized.

Two alternatives were considered and not taken. Copying `/usr/lib/postgresql/17`
out of a digest-pinned `postgres:17-bookworm` stage pins harder, but hand-carries
the dependency set — `libpq5`, the ICU and LLVM JIT libraries — with a missing
one showing up as a runtime failure in somebody's test rather than a failed
build. A Dev Container Feature would install from the same archive while adding
the devcontainer CLI to the build path, which this image already rejected for
[the reasons below](#why-this-base-image-and-not-a-dev-containers-one).

### Size

Measured, not estimated. `scripts/session-image-smoke.sh` reports both numbers
on every qualification run — as workflow notices when it runs in Actions, so
the figure lands on the pull request being approved rather than only in a job
log — and the
[rollout runbook](https://github.com/tokencanopy/rainier-cloud/blob/main/docs/runbooks/default-environment-rollout.md)
step 2 is where the pull cost is reviewed against it.

On the qualified candidate (linux/amd64, the default pinned base; run
[34330830439](https://github.com/tokencanopy/rainier/actions/runs/34330830439)):

| | |
|---|---|
| Whole image | ≈2.42 GB (2,416,401,380 bytes), 22 layers |
| The services layer's installed payload | **14 packages, 244,512 KiB (≈239 MiB)** |

That second figure is measured by the build, not estimated: it diffs its own
package set across the install and writes the total and a per-package
breakdown to `/usr/local/share/rainier-services-size.txt`, which the smoke then
reports. Read it off the run rather than off this table, which is one build old
the moment it is written.

**Most of that 239 MiB is not PostgreSQL.** The five largest of the fourteen:

```
  126303 KiB  libllvm19
   57395 KiB  postgresql-17
   22767 KiB  libz3-4
   15847 KiB  locales
   10427 KiB  postgresql-client-17
```

`libllvm19` and its `libz3-4` are 149 MiB — 61% of the layer — and they are
there for **JIT compilation of queries**, which `postgresql-17` hard-depends
on. A session that never runs a query expensive enough to JIT still pays for
them, because a distribution package's dependencies are not optional.

Two things could remove that and neither was done here. Building PostgreSQL
from source with `--without-llvm` trades a checksum-pinned distribution archive
for a build this repository would then own and have to keep patched — a real
cost, on the security-relevant path, for 149 MiB of a 2.4 GB image. Shipping no
server at all is the thing this change exists to fix. If the pull cost review
decides 239 MiB per runner boot is too much, the source build is the
conversation to have, and the breakdown on the run is where it starts.

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
- **`TMPDIR` is deliberately left alone**, for the same reason
  `XDG_CONFIG_HOME` is. `/tmp` is a per-container tmpfs: agent scratch written
  there is writable, is not in a checkpoint, is not in an archive, and does not
  follow `rainier pull` off the runner. Exactly one build temp is moved off it
  — `GOTMPDIR`, because `go test` has to *execute* what it builds. Setting
  `TMPDIR` globally would move every tool's scratch, Claude Code's and Codex's
  included, onto the volume that leaves the runner, and would quietly change
  the sandbox each agent believes it has. Neither agent is wrapped here and
  neither is given a rewritten temp; `internal/driver.TestSessionImageLeavesScratchOnTheTmpfs`
  fails the ordinary suite if that changes.
- **A service's state is on the volume too.** `PGDATA`, `PGHOST` and
  `RAINIER_SERVICES_DIR` point under `/workspace/.services` for the same reason
  the caches do. See [Local services](#local-services-a-developer-starts).
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
real entrypoint up as PID 1.

It also runs the local services end to end: it checks that a fresh session has
**nothing** listening and no cluster in the image, then `initdb`s a cluster on
the workspace volume, starts it with `pg_ctl`, creates a database, commits one
transaction and rolls another back, connects again over the DSN `rainier-pg
url` prints, asserts the listener is on loopback and not on `0.0.0.0`, stops
and restarts the server with its data intact, writes and reads a SQLite
database from both the shell and Python, and starts Redis, PINGs it, round-trips
a key and stops it — all as uid 1000, with a read-only rootfs and no network at
all. The shell of those two helpers is separately exercised against stub
binaries in `internal/driver/image_services_test.go`, which needs no docker and
catches the behavioural failures (a stop that waits on the wrong thing, a
server bound to the wrong interface, a cluster created with the wrong locale)
that reading the script does not. Every probe runs in a container wearing the
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

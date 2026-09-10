# syntax=docker/dockerfile:1
# Dockerfile (session image) — the default hosted coding environment.
#
# This is the image a hosted Dedicated session actually runs. rainier-cloud's
# runner-artifacts workflow checks this repository out at the OSS ref, builds
# THIS file, pushes it as <prefix>-images/rainier-session, and prints the
# resulting digest as the `session_image` an operator pastes into the Dedicated
# root's `runner_artifacts`. A Dedicated runner pulls that digest at boot with a
# short-lived registry credential and can pull nothing else, so a developer's
# session gets exactly what is here and an environment's setup script is the
# only other thing that can add to it. That is why the toolchain a developer
# needs belongs in this file: baked in, it costs one pull; installed by a setup
# script, it costs an egress allowlist entry per installer, a build container
# with the read-only rootfs dropped, and a snapshot per environment.
#
# BASE IMAGE — why this one, and why not a Dev Containers base or Features.
# See docs/session-image.md for the full comparison; the short version is that
# the publish path is a plain `docker build`, Dev Container Features are applied
# by the devcontainer CLI and would put a new build-time dependency and a new
# trust surface in front of the platform's own image, and the Dev Containers
# base images ship a `vscode` uid-1000 user with passwordless sudo whose whole
# user model this image would have to undo. What is left is a plain pinned
# Debian bookworm, which is what `node:22-bookworm` is: Debian bookworm with
# Node's LTS line and npm already on it, and buildpack-deps' compiler and header
# set under that. The digest below is the one rainier-cloud resolved and
# qualified for its own environment image, reused rather than re-resolved.
#
# Overridable so `make session-image` on an arm64 laptop can pass the tag and
# build natively; the digest is what ships, and CI never overrides it.
ARG BASE_IMAGE=node:22-bookworm@sha256:87a4f951f28b85d189df365d24c479d3bdb70be77c1ff5c9029db2ef67e251ac

# The pinned upstream releases. Each version is paired with a SHA-256 in
# images/session/toolchain.sh, checked before extraction; changing a version
# here without changing the sum there fails the build rather than installing
# something unreviewed.
ARG GO_VERSION=1.27.1
ARG GH_VERSION=2.100.0
ARG CODEX_VERSION=0.153.4
ARG UV_VERSION=0.12.10
ARG RIPGREP_VERSION=15.2.0
ARG JQ_VERSION=1.8.2
# Claude Code comes from npm, where the version IS the pin: the package is
# published as a single bundled artifact with no runtime dependency tree, so an
# exact version resolves to one immutable tarball and npm checks its integrity
# hash on the way in.
ARG CLAUDE_CODE_VERSION=2.1.263

# PostgreSQL comes from the PostgreSQL project's own Debian archive (PGDG),
# because Debian bookworm ships 15 and the platform's own store tests are
# written against 17. The archive is the same one the official `postgres` image
# installs from; what is pinned here is its SIGNING KEY, by full fingerprint,
# checked before the archive is added to apt at all. See the services layer
# below and docs/session-image.md for what that pin does and does not cover.
ARG POSTGRES_MAJOR=17
ARG PGDG_KEY_FINGERPRINT=B97B0AFCAA1A47F044F244A07FCC7D46ACCC4CF8

# The browser baseline. A project runs ITS OWN Playwright — nothing Playwright
# is installed globally in this image, deliberately — so what is pinned here is
# the browser that Playwright launches, and the Playwright version it is the
# right browser for. A project on that version downloads nothing; a project on
# another version installs its own revision into the workspace cache beside it.
# See images/session/browsers.sh, whose checksums are the actual pin, and
# docs/session-image.md for the supported set and for what other versions do.
ARG PLAYWRIGHT_VERSION=1.63.0
ARG CHROMIUM_VERSION=153.0.8010.12
ARG CHROMIUM_REVISION=1243
ARG PLAYWRIGHT_FFMPEG_REVISION=1011

# --- the pinned upstream toolchain, verified before it is extracted ----------
FROM ${BASE_IMAGE} AS toolchain
ARG TARGETARCH
ARG GO_VERSION
ARG GH_VERSION
ARG CODEX_VERSION
ARG UV_VERSION
ARG RIPGREP_VERSION
ARG JQ_VERSION
ENV TARGETARCH=${TARGETARCH} \
    GO_VERSION=${GO_VERSION} \
    GH_VERSION=${GH_VERSION} \
    CODEX_VERSION=${CODEX_VERSION} \
    UV_VERSION=${UV_VERSION} \
    RIPGREP_VERSION=${RIPGREP_VERSION} \
    JQ_VERSION=${JQ_VERSION}
COPY images/session/toolchain.sh /tmp/toolchain.sh
RUN /tmp/toolchain.sh && rm /tmp/toolchain.sh

# --- sessiond, built with the same pinned Go the session gets ----------------
FROM toolchain AS build
ENV PATH="/opt/toolchain/go/bin:${PATH}"
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# CGO off so the binary does not link against this stage's libc and stays the
# same PID 1 whatever the final image's userland turns out to be.
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/sessiond ./cmd/sessiond

# --- the session image -------------------------------------------------------
FROM ${BASE_IMAGE}
ARG CLAUDE_CODE_VERSION

# The Debian half of the environment. Everything here is named explicitly even
# where the base image already carries it: this list is the contract with
# docs/session-image.md and with scripts/session-image-smoke.sh, and a package
# that silently leaves the base image should break the build, not a developer's
# afternoon.
#
# Deliberately absent: sudo (a session user who could escalate would make every
# other boundary in this file decorative) and any credential helper or package
# manager configuration that could hold one. The one keyring this image does
# install (the services layer below) holds a public archive-verification key and
# no secret; nothing token-shaped is baked in anywhere.
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential zlib1g-dev \
      pkg-config \
      make \
      python3 python3-venv python3-pip python3-dev \
      git openssh-client \
      ca-certificates curl wget openssl \
      bash coreutils findutils grep sed gawk diffutils patch \
      tar gzip bzip2 xz-utils zip unzip \
      procps iproute2 lsof dnsutils netcat-openbsd \
      file less nano vim-tiny rsync \
      tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && update-ca-certificates

# --- the local service tools: PostgreSQL 17, SQLite and Redis ----------------
#
# A session that cannot start a database cannot run an integration test, and
# every way of getting one later is worse than baking it in: `sudo` is not
# installed and never will be, the rootfs is read-only, and an environment's
# setup script would cost an egress allowlist entry per installer plus a
# snapshot per environment. So the CLIENT and the SERVER halves are both here.
# Nothing starts them — there is no service in this image's entrypoint, no
# cluster in its layers, and no port bound until a developer runs one of the
# commands in docs/session-image.md.
#
# SQLite and Redis are Debian bookworm's own packages. PostgreSQL is not:
# bookworm ships 15, and rainier-cloud's cell is 17 with store tests written
# against it, so a session with a 15 cannot run them. PGDG is the PostgreSQL
# project's own Debian archive and the same one the official `postgres` image
# installs from. Adding a second archive is a real trust decision and it is
# pinned the strongest way an apt archive can be: the signing key is fetched
# over HTTPS and its FULL fingerprint is checked before the archive is
# configured at all, so a substituted key is a failed build and never a silent
# install. The key COUNT is checked first and separately, because `gpg
# --dearmor` converts every key in the file and `signed-by=` then trusts the
# whole keyring: a file carrying the genuine key followed by somebody else's
# would otherwise satisfy a check that reads only the first fingerprint. Package VERSIONS resolve against a moving archive — exactly like
# Debian's own, and with the same answer — and are recorded by the manifest
# below rather than pinned. See docs/session-image.md.
#
# create_main_cluster=false, because postgresql-common otherwise runs initdb at
# install time. That cluster would live under /var/lib/postgresql — on the
# read-only rootfs at runtime, owned by a `postgres` account no session ever
# becomes — so it would be dead weight in every layer and a misleading thing
# for a developer to find. A session's cluster belongs on the workspace volume,
# created by the developer when they want one.
#
# policy-rc.d, because Debian maintainer scripts start what they install and
# nothing in this image may be running. It refuses every service start for the
# duration of the install and is removed immediately afterwards.
#
# libpq5 and libpq-dev are named explicitly, and their major floored at the
# server's, because the base image carries Debian's 15 of both while
# postgresql-client-17 needs at least a 17. apt is as free to resolve that by
# REMOVING the -dev package as by upgrading it, and a silently dropped C client
# header set is exactly the kind of regression a manifest records after the
# fact instead of preventing. The floor is `>=` and not `==` deliberately: PGDG
# ships ONE libpq for every server major it carries, so an archive that has
# released an 18 hands this image an 18.x libpq beside the 17 server. That is
# the supported arrangement — libpq is compatible with older servers — and an
# equality check here would break the build the day a new major ships.
ARG POSTGRES_MAJOR
ARG PGDG_KEY_FINGERPRINT
RUN set -eu; \
    . /etc/os-release; \
    [ "${VERSION_CODENAME:-}" = bookworm ] || { \
      echo "the archive line below names bookworm; this base is ${VERSION_CODENAME:-unknown}" >&2; exit 1; }; \
    curl --fail --location --retry 3 --retry-delay 2 --max-time 120 \
         --proto '=https' --tlsv1.2 --output /tmp/pgdg.asc \
         https://www.postgresql.org/media/keys/ACCC4CF8.asc; \
    keys="$(gpg --show-keys --with-colons /tmp/pgdg.asc | grep -c '^pub:')"; \
    [ "$keys" = 1 ] || { \
      echo "the PGDG key file carries ${keys} primary keys, not one; --dearmor would trust all of them" >&2; exit 1; }; \
    got="$(gpg --show-keys --with-colons --fingerprint /tmp/pgdg.asc | awk -F: '$1 == "fpr" { print $10; exit }')"; \
    [ "$got" = "$PGDG_KEY_FINGERPRINT" ] || { \
      echo "the PGDG signing key is ${got:-unreadable}, not the pinned $PGDG_KEY_FINGERPRINT" >&2; exit 1; }; \
    gpg --dearmor < /tmp/pgdg.asc > /usr/share/keyrings/rainier-pgdg.gpg; \
    rm -f /tmp/pgdg.asc; \
    chmod 0644 /usr/share/keyrings/rainier-pgdg.gpg; \
    echo "deb [signed-by=/usr/share/keyrings/rainier-pgdg.gpg] https://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" \
      > /etc/apt/sources.list.d/rainier-pgdg.list; \
    mkdir -p /etc/postgresql-common; \
    echo 'create_main_cluster = false' > /etc/postgresql-common/createcluster.conf; \
    printf '#!/bin/sh\nexit 101\n' > /usr/sbin/policy-rc.d; \
    chmod 0755 /usr/sbin/policy-rc.d; \
    apt-get update; \
    dpkg-query -W -f='${Package}\n' | sort > /tmp/packages.before; \
    apt-get install -y --no-install-recommends \
      "postgresql-${POSTGRES_MAJOR}" "postgresql-client-${POSTGRES_MAJOR}" \
      libpq5 libpq-dev \
      sqlite3 libsqlite3-0 \
      redis-server redis-tools; \
    rm -f /usr/sbin/policy-rc.d; \
    rm -rf /var/lib/apt/lists/*; \
    pgbin="/usr/lib/postgresql/${POSTGRES_MAJOR}/bin"; \
    "$pgbin/postgres" --version | grep -q "(PostgreSQL) ${POSTGRES_MAJOR}\."; \
    "$pgbin/psql" --version | grep -q "(PostgreSQL) ${POSTGRES_MAJOR}\."; \
    "$pgbin/initdb" --version >/dev/null; \
    "$pgbin/pg_ctl" --version >/dev/null; \
    for pkg in libpq5 libpq-dev; do \
      have=$(dpkg-query -W -f='${Version}' "$pkg" | sed 's/[^0-9].*//'); \
      [ -n "$have" ] && [ "$have" -ge "${POSTGRES_MAJOR}" ] || { \
        echo "$pkg is $(dpkg-query -W -f='${Version}' "$pkg"), older than ${POSTGRES_MAJOR}" >&2; exit 1; }; \
    done; \
    [ ! -d /var/lib/postgresql/${POSTGRES_MAJOR}/main ] || { \
      echo "an installed cluster is in the image; create_main_cluster did not take" >&2; exit 1; }; \
    for f in "$pgbin"/*; do \
      n="${f##*/}"; \
      [ ! -e "/usr/local/bin/$n" ] || { echo "/usr/local/bin/$n already exists" >&2; exit 1; }; \
      ln -s "$f" "/usr/local/bin/$n"; \
    done; \
    sqlite3 --version >/dev/null; \
    redis-server --version >/dev/null; \
    redis-cli --version >/dev/null; \
    dpkg-query -W -f='${Package}\t${Installed-Size}\n' | sort \
      | awk -F'\t' 'NR==FNR { had[$1] = 1; next } \
                     !($1 in had) { n++; kb += $2; added[$1] = $2 } \
                     END { printf "%d packages, %d KiB installed\n", n, kb; \
                           for (p in added) printf "%8d KiB  %s\n", added[p], p }' \
            /tmp/packages.before - \
      | { read -r first; echo "$first"; sort -rn; } > /usr/local/share/rainier-services-size.txt; \
    rm -f /tmp/packages.before; \
    chmod 0644 /usr/local/share/rainier-services-size.txt

# The two helpers that turn those binaries into a working non-root service.
# Root-owned in /usr/local/bin, like sessiond and the agents: a session user who
# could rewrite them could rewrite what a developer is about to run as a server.
# Neither installs anything, neither needs the network, and neither is invoked
# by the entrypoint — see docs/session-image.md for what they do and for the
# equivalent raw initdb/pg_ctl commands.
COPY images/session/services/ /usr/local/bin/
RUN chmod 0755 /usr/local/bin/rainier-pg /usr/local/bin/rainier-redis

# --- browser testing: the shared libraries, the fonts, and one Chromium ------
#
# `npx playwright install --with-deps chromium` is the line every project's CI
# runs, and its --with-deps half is an `apt-get install` as root. This image
# installs no escalation path and never will, the rootfs is read-only, and the
# egress allowlist carries no package archive — so that half has to be a
# build-time layer or a session cannot run a browser test at all. This is that
# layer.
#
# The package list is Playwright's own `debian12-x64` chromium dependency set
# (packages/playwright-core/src/server/registry/nativeDeps.ts), named here in
# full rather than resolved by the tool, because the tool needs root to read it
# and a session has none. Sixteen of these are missing from the base image and
# each one is a `chrome-headless-shell: error while loading shared libraries`
# at somebody's first test run.
#
# The fonts are not decoration. A Chromium with no fonts renders every glyph as
# a box, which turns a screenshot into a useless artifact and a text-measuring
# assertion into a flake. fonts-liberation is the metric-compatible Arial /
# Times / Courier set Chrome for Testing expects, fonts-dejavu-core covers
# Latin, Greek and Cyrillic, and fonts-noto-color-emoji is what an emoji in a
# product's UI renders as. CJK is deliberately absent — fonts-wqy-zenhei and
# fonts-ipafont-gothic are ~35 MiB for a script most suites never assert on;
# see docs/session-image.md.
#
# Xvfb is deliberately absent too: this image runs headless browsers only, and
# an X server would be dead weight plus a socket in every session.
RUN set -eu; \
    apt-get update; \
    dpkg-query -W -f='${Package}\n' | sort > /tmp/packages.before; \
    apt-get install -y --no-install-recommends \
      libasound2 libatk-bridge2.0-0 libatk1.0-0 libatspi2.0-0 \
      libcairo2 libcups2 libdbus-1-3 libdrm2 libgbm1 libglib2.0-0 \
      libnspr4 libnss3 libpango-1.0-0 \
      libx11-6 libxcb1 libxcomposite1 libxdamage1 libxext6 libxfixes3 \
      libxkbcommon0 libxrandr2 \
      fontconfig libfontconfig1 libfreetype6 \
      fonts-liberation fonts-dejavu-core fonts-noto-color-emoji; \
    rm -rf /var/lib/apt/lists/*; \
    fc-cache -f >/dev/null; \
    dpkg-query -W -f='${Package}\t${Installed-Size}\n' | sort \
      | awk -F'\t' 'NR==FNR { had[$1] = 1; next } \
                     !($1 in had) { n++; kb += $2; added[$1] = $2 } \
                     END { printf "%d packages, %d KiB installed\n", n, kb; \
                           for (p in added) printf "%8d KiB  %s\n", added[p], p }' \
            /tmp/packages.before - \
      | { read -r first; echo "$first"; sort -rn; } > /usr/local/share/rainier-browser-size.txt; \
    rm -f /tmp/packages.before; \
    chmod 0644 /usr/local/share/rainier-browser-size.txt

# The browser itself, checksum-verified before extraction and laid out exactly
# where a project's Playwright looks. Root-owned under /usr/local/lib for the
# same reason the agents are: a session user who could rewrite the browser
# binary could rewrite what every later test run executes.
ARG TARGETARCH
ARG PLAYWRIGHT_VERSION
ARG CHROMIUM_VERSION
ARG CHROMIUM_REVISION
ARG PLAYWRIGHT_FFMPEG_REVISION
COPY images/session/browsers.sh /tmp/browsers.sh
RUN TARGETARCH="${TARGETARCH}" PLAYWRIGHT_VERSION="${PLAYWRIGHT_VERSION}" \
    CHROMIUM_VERSION="${CHROMIUM_VERSION}" CHROMIUM_REVISION="${CHROMIUM_REVISION}" \
    PLAYWRIGHT_FFMPEG_REVISION="${PLAYWRIGHT_FFMPEG_REVISION}" \
    /tmp/browsers.sh && rm /tmp/browsers.sh

# The helper that links that baseline into the cache a project's Playwright
# reads. Root-owned in /usr/local/bin beside rainier-pg and rainier-redis; it
# installs nothing, downloads nothing and needs no privilege.
COPY images/session/browsers/ /usr/local/bin/
RUN chmod 0755 /usr/local/bin/rainier-browsers

# The pinned upstream releases from the toolchain stage. Root-owned, under
# /usr/local, which the session user cannot write — see the prefix note below.
COPY --from=toolchain /opt/toolchain/go /usr/local/go
# Codex's vendor package travels whole, and /usr/local/bin/codex arrives from
# the line below as the relative symlink `../lib/codex/bin/codex` that the
# toolchain stage created — the same link, resolving to the same tree, in both
# stages. Codex locates its tool host and its bundled search and sandbox
# resources by walking up from the resolved path of its own executable, so the
# package has to stay together and the PATH entry has to point into it.
COPY --from=toolchain /opt/toolchain/lib/ /usr/local/lib/
COPY --from=toolchain /opt/toolchain/bin/ /usr/local/bin/

# The upstream gh stays root-owned at an absolute path. Its public command is
# a tiny wrapper that replaces itself with sessiond's per-invocation launcher.
RUN mkdir -p /usr/local/libexec/rainier \
    && mv /usr/local/bin/gh /usr/local/libexec/rainier/gh \
    && printf '%s\n' '#!/bin/sh' 'exec /usr/local/bin/sessiond github-cli "$@"' > /usr/local/bin/gh \
    && chmod 0755 /usr/local/bin/gh /usr/local/libexec/rainier/gh

# Claude Code. Installed globally as root into /usr/local/lib/node_modules so
# the session user cannot rewrite the agent it is about to run, with npm's own
# cache kept out of the image entirely: a cache under /root would be dead weight
# a session can never read, and one under /workspace would be shadowed by the
# session's volume the moment the container starts.
RUN npm_config_cache=/tmp/npm-build npm install --global --no-audit --no-fund \
      "@anthropic-ai/claude-code@${CLAUDE_CODE_VERSION}" \
    && rm -rf /tmp/npm-build

# `go` needs a temporary directory it can execute from, and a session's /tmp is
# a noexec tmpfs (internal/driver.runArgs). GOTMPDIR answers that, but Go
# requires the directory to already exist, and the only writable place it can
# live is the workspace volume — which does not exist until the container runs.
# So the one tool with that requirement gets a wrapper that creates its caches
# on the way through. Root-owned and absolute, like everything else in
# /usr/local/bin; the real toolchain stays at /usr/local/go/bin for anyone who
# wants to bypass it.
RUN printf '%s\n' \
      '#!/bin/sh' \
      '# Installed by the session image. A session'"'"'s $HOME is read-only and its' \
      '# /tmp is noexec, so Go'"'"'s caches and its build temp live on the workspace' \
      '# volume, which is writable and survives detach, suspend and resume.' \
      'mkdir -p "$GOCACHE" "$GOMODCACHE" "$GOTMPDIR" "$GOPATH" 2>/dev/null' \
      'exec /usr/local/go/bin/go "$@"' \
      > /usr/local/bin/go \
    && chmod 0755 /usr/local/bin/go \
    && ln -s /usr/local/go/bin/gofmt /usr/local/bin/gofmt

# The session user, by uid — the driver runs every container as 1000:1000
# (internal/driver.sessionUser). The base image already defines uid 1000 as
# `node`; it is renamed rather than replaced so there is exactly one uid-1000
# account and its home is where this repository's docs say it is.
#
# Without a passwd entry for that uid docker gives it HOME=/ on a root-owned
# rootfs, which means an environment's setup script has nowhere outside
# /workspace it can write, and /workspace is a volume that `docker commit`
# excludes. So an image with no session user is an image whose environments can
# never cache anything they install.
#
# /opt/rainier-env is the install prefix that goes with it: a setup script needs
# somewhere outside /workspace it can write, since the snapshot keeps the rootfs
# and excludes the volume. It is a DEDICATED prefix, not /usr/local, and that
# distinction is the security boundary. /usr/local/bin holds sessiond — the
# session's PID 1 — and the agents a session runs, and stays root-owned, so even
# during the one writable-rootfs window (a container carrying a setup script;
# see runArgs) the session user cannot rewrite any of them.
#
# A setup script is untrusted in exactly the way design §10 means: an agent,
# possibly prompt-injected, runs inside these containers, and a PID 1 it could
# replace would be baked into the cached image every later session of that
# environment boots. Cache poisoning of USER-level binaries under this prefix
# remains possible and is inherent to any shared build cache — it is the same
# class of trust a malicious npm package already has. What must not be reachable
# is the platform's own agent, and it isn't.
RUN groupmod --new-name rainier node \
    && usermod --login rainier --home /home/rainier --move-home node \
    && mkdir -p /opt/rainier-env/bin /workspace/.rainier /rainier/agents \
    && chown -R 1000:1000 /opt/rainier-env /workspace \
    && chmod 0755 /rainier /rainier/agents

# Pre-seed the workspace before Docker copies its uid-1000 ownership to a
# fresh volume. The deployed initializer has CAP_CHOWN, not DAC_OVERRIDE: it
# can chown the seed, but cannot mkdir in an empty user-owned 0755 mount.

# The realized inventory, for whoever has to answer "what was in the image we
# ran on the 6th" from a digest alone. A target list is not evidence; this is.
RUN { dpkg-query -W -f='${Package}\t${Version}\n' | sort; } > /usr/local/share/rainier-os-packages.txt \
    && npm ls --global --depth=0 --json > /usr/local/share/rainier-npm-global.json \
    && { for f in /etc/apt/sources.list /etc/apt/sources.list.d/*; do \
           [ -f "$f" ] || continue; \
           echo "== $f"; \
           grep -vE '^[[:space:]]*(#|$)' "$f" || true; \
         done; } > /usr/local/share/rainier-apt-sources.txt \
    && chmod 0644 /usr/local/share/rainier-os-packages.txt /usr/local/share/rainier-npm-global.json \
                  /usr/local/share/rainier-apt-sources.txt

# Caches, all on the workspace volume. A session's $HOME is on the read-only
# rootfs, so a tool that caches under it fails its first write; /workspace is
# the one writable, persistent path a session has and the only place a cache can
# both work and survive a suspend.
#
# XDG_CONFIG_HOME is deliberately NOT moved. Configuration is where tools put
# credentials, /workspace is what checkpoints, archives and `rainier pull`
# carry off the runner, and the two must not meet. A tool that wants to write a
# token gets a read-only $HOME and fails loudly, which is the outcome this
# system wants.
#
# A service's DURABLE state goes on the volume, for the same reason the caches
# do; its RUNTIME state — the unix sockets, the pidfiles — deliberately does
# not. protocol/workspace.TarGz refuses a socket rather than skipping it, so a
# socket under /workspace turns `rainier push`/`pull` of any tree containing it
# into an error, and an unclean exit leaves the socket behind to keep doing so.
# /tmp is the right home for it: writable, per container, and gone when the
# container is.
ENV PATH="/opt/rainier-env/bin:${PATH}" \
    RAINIER_SERVICES_DIR=/workspace/.services \
    RAINIER_SERVICES_RUNTIME_DIR=/tmp/rainier-services \
    PGDATA=/workspace/.services/postgresql/data \
    PGHOST=/tmp/rainier-services/postgresql \
    PGPORT=5432 \
    PGDATABASE=postgres \
    GOCACHE=/workspace/.cache/go-build \
    GOMODCACHE=/workspace/.cache/go-mod \
    GOPATH=/workspace/.gopath \
    GOTMPDIR=/workspace/.cache/go-tmp \
    XDG_CACHE_HOME=/workspace/.cache \
    PLAYWRIGHT_BROWSERS_PATH=/workspace/.cache/ms-playwright \
    npm_config_cache=/workspace/.cache/npm \
    npm_config_update_notifier=false \
    PIP_CACHE_DIR=/workspace/.cache/pip \
    UV_CACHE_DIR=/workspace/.cache/uv \
    UV_PYTHON_DOWNLOADS=never \
    PYTHONDONTWRITEBYTECODE=1 \
    DISABLE_AUTOUPDATER=1

# The browser baseline, linked into the cache a project's Playwright reads.
#
# PLAYWRIGHT_BROWSERS_PATH above is /workspace/.cache/ms-playwright, which is
# both writable and exactly where Playwright would have looked anyway
# ($XDG_CACHE_HOME/ms-playwright). Building the links HERE, into the image's
# own /workspace, means docker copies them onto a freshly created workspace
# volume at session creation: no entrypoint work, no first-run copy of a
# quarter of a gigabyte, and nothing on the volume but symlinks and two empty
# marker files. The payload stays on the read-only rootfs, out of checkpoints,
# archives and `rainier pull`.
#
# The seed's chown is -h, and the layout it produces is why the driver's own
# volume initializer is still correct. GNU chown -R traverses -P by default and
# lchown()s a symlink rather than its target (verified against coreutils 9.1),
# so `chown -R 1000:1000 /workspace` — which is exactly what
# internal/driver.initVolumeScript runs, as root with CAP_CHOWN and a READ-ONLY
# rootfs — walks over these links without touching the browser they point at
# and without failing on a filesystem it cannot write. A -L or --dereference
# there would do both: fail the init job with EROFS, and, on any host where it
# did not, hand the session user the root-owned binary it is about to execute.
# -h here says that out loud, and the assertions below are what actually holds
# it: the binary is still root's, and the cache still reaches it.
RUN set -eu; \
    /usr/local/bin/rainier-browsers link; \
    chown -Rh 1000:1000 /workspace/.cache; \
    bin=$(find /usr/local/lib/rainier-browsers -name chrome-headless-shell -type f); \
    [ -n "$bin" ] || { echo "no browser baseline was installed" >&2; exit 1; }; \
    [ "$(stat -c %u "$bin")" = 0 ] || { \
      echo "the browser baseline is owned by $(stat -c %U "$bin"), not root; the workspace chown followed a symlink" >&2; exit 1; }; \
    link=/workspace/.cache/ms-playwright/chromium_headless_shell-${CHROMIUM_REVISION}/$(basename "$(dirname "$bin")"); \
    [ -L "$link" ] && [ -x "$link/chrome-headless-shell" ] || { \
      echo "the workspace cache does not resolve to the baseline through $link" >&2; exit 1; }

COPY --from=build /out/sessiond /usr/local/bin/sessiond

# sessiond as PID 1; RAINIER_DIAL/RAINIER_SESSION injected by the driver select
# dial (relay) mode. With no env, it falls back to listen mode (dev).
#
# The path is ABSOLUTE. /opt/rainier-env/bin is writable by the session user and
# first on PATH, so a PATH-resolved entrypoint would let a setup script — or an
# agent in a container that has one — drop a `sessiond` there and have the next
# boot of that environment execute it as PID 1.
ENTRYPOINT ["/usr/local/bin/sessiond"]
CMD ["--", "bash", "-i"]
# The driver passes --user 1000:1000 on every create; this is the same answer
# for anything that runs the image without it. The volume-init job overrides it
# with --user 0:0 and a fixed `sh -c`, which is unaffected.
USER 1000:1000
WORKDIR /workspace

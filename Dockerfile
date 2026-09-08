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
# other boundary in this file decorative) and any credential helper, keyring, or
# package manager configuration that could hold one.
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
    && chmod 0644 /usr/local/share/rainier-os-packages.txt /usr/local/share/rainier-npm-global.json

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
ENV PATH="/opt/rainier-env/bin:${PATH}" \
    GOCACHE=/workspace/.cache/go-build \
    GOMODCACHE=/workspace/.cache/go-mod \
    GOPATH=/workspace/.gopath \
    GOTMPDIR=/workspace/.cache/go-tmp \
    XDG_CACHE_HOME=/workspace/.cache \
    npm_config_cache=/workspace/.cache/npm \
    npm_config_update_notifier=false \
    PIP_CACHE_DIR=/workspace/.cache/pip \
    UV_CACHE_DIR=/workspace/.cache/uv \
    UV_PYTHON_DOWNLOADS=never \
    PYTHONDONTWRITEBYTECODE=1 \
    DISABLE_AUTOUPDATER=1

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

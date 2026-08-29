# Dockerfile (session image)
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/sessiond ./cmd/sessiond

FROM alpine:3.20
# curl alongside bash: BusyBox wget on this image ignores https_proxy/-Y
# entirely for https:// URLs (connects direct, verified in Task 13's spike)
# — it cannot exercise egressd's CONNECT proxy at all, only curl can. Kept
# even where egress isn't currently enforced (VM-backed dev docker, R4
# amendment) so the same session image works identically everywhere.
RUN apk add --no-cache bash curl
# The session user, by uid — the driver runs every container as 1000:1000
# (internal/driver.sessionUser). Without a passwd entry for that uid docker
# gives it HOME=/ on a root-owned rootfs, which means an environment's setup
# script has nowhere outside /workspace it can write, and /workspace is a
# volume that `docker commit` excludes. So an image with no session user is an
# image whose environments can never cache anything they install. Mainstream
# language images (node, python) already ship such a user; this is that same
# contract, made explicit for rainier's own.
#
# /opt/rainier-env is the install prefix that goes with it: a setup script
# needs somewhere outside /workspace it can write, since the snapshot keeps the
# rootfs and excludes the volume. It is a DEDICATED prefix, not /usr/local,
# and that distinction is the security boundary. /usr/local/bin holds sessiond
# — the session's PID 1 — and stays root-owned, so even during the one
# writable-rootfs window (a container carrying a setup script; see runArgs) the
# session user cannot rewrite it. A setup script is untrusted in exactly the
# way design §10 means: an agent, possibly prompt-injected, runs inside these
# containers, and a PID 1 it could replace would be baked into the cached image
# every later session of that environment boots.
#
# Cache poisoning of USER-level binaries under this prefix remains possible and
# is inherent to any shared build cache — it is the same class of trust a
# malicious npm package already has. What must not be reachable is the
# platform's own agent, and it isn't.
RUN adduser -D -u 1000 rainier \
 && mkdir -p /opt/rainier-env/bin \
 && chown -R 1000:1000 /opt/rainier-env
ENV PATH="/opt/rainier-env/bin:${PATH}"
COPY --from=build /out/sessiond /usr/local/bin/sessiond
# sessiond as PID 1; RAINIER_DIAL/RAINIER_SESSION injected by the driver select
# dial (relay) mode. With no env, it falls back to listen mode (dev).
ENTRYPOINT ["sessiond"]
CMD ["--", "bash", "-i"]

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
# /usr/local goes with it: it is the conventional install prefix a setup script
# reaches for, and it is root-owned by default. Handing it to the session user
# is what makes `--setup-file` able to put a toolchain somewhere the snapshot
# keeps. The window is narrow — only a container that carries a setup script
# has a writable rootfs at all (see runArgs), and that script is the
# environment admin's own, running in an image the same admin chose.
RUN adduser -D -u 1000 rainier && chown -R 1000:1000 /usr/local
COPY --from=build /out/sessiond /usr/local/bin/sessiond
# sessiond as PID 1; RAINIER_DIAL/RAINIER_SESSION injected by the driver select
# dial (relay) mode. With no env, it falls back to listen mode (dev).
ENTRYPOINT ["sessiond"]
CMD ["--", "bash", "-i"]

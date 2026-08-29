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
COPY --from=build /out/sessiond /usr/local/bin/sessiond
# sessiond as PID 1; RAINIER_DIAL/RAINIER_SESSION injected by the driver select
# dial (relay) mode. With no env, it falls back to listen mode (dev).
ENTRYPOINT ["sessiond"]
CMD ["--", "bash", "-i"]

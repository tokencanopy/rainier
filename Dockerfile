# Dockerfile (session image)
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/sessiond ./cmd/sessiond

FROM alpine:3.20
RUN apk add --no-cache bash
COPY --from=build /out/sessiond /usr/local/bin/sessiond
# sessiond as PID 1; RAINIER_DIAL/RAINIER_SESSION injected by the driver select
# dial (relay) mode. With no env, it falls back to listen mode (dev).
ENTRYPOINT ["sessiond"]
CMD ["--", "bash", "-i"]

# Dockerfile
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/sessiond ./cmd/sessiond

FROM alpine:3.20
RUN apk add --no-cache bash
COPY --from=build /out/sessiond /usr/local/bin/sessiond
# sessiond as PID 1: the spec's rule-1 shape.
ENTRYPOINT ["sessiond", "--listen", "0.0.0.0:7070", "--log", "/tmp/session.log", "--"]
CMD ["bash", "-i"]

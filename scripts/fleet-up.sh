#!/usr/bin/env bash
set -euo pipefail
command -v docker >/dev/null || export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"
command -v docker >/dev/null || { echo "docker CLI not found" >&2; exit 1; }

docker network inspect rainier-internal >/dev/null 2>&1 || docker network create rainier-internal >/dev/null
docker build -q -t rainier-session:latest . >/dev/null
go build -o bin/runnerd ./cmd/runnerd
go build -o bin/egressd ./cmd/egressd
go build -o bin/rattach ./cmd/rattach
go build -o bin/runnerctl ./cmd/runnerctl

# How session containers reach host-run runnerd/egressd. On a VM-backed
# docker (Docker Desktop, colima) dockerd runs inside a Linux VM, not on
# this machine — the internal network's own bridge gateway IP is local to
# that VM and does NOT route back to this host (confirmed: connects and
# gets refused). Docker's host.docker.internal DNS name is what actually
# resolves to a host-reachable address there (the VM's own upstream
# gateway, NATed back out to the real host). On a plain Linux box running
# dockerd locally, dockerd IS this host, so the bridge gateway reaches it
# directly and host.docker.internal may not resolve. Probe from a
# throwaway container on the network and prefer host.docker.internal;
# fall back to the bridge gateway when it doesn't resolve.
BRIDGE_GW=$(docker network inspect rainier-internal -f '{{ (index .IPAM.Config 0).Gateway }}')
# `|| true`: under `set -o pipefail`, a plain Linux dockerd (no
# host.docker.internal) makes getent exit non-zero inside the container,
# which docker run propagates as its own exit status — without this guard
# that failure would abort the whole script right here (set -e) instead of
# falling through to the bridge-gateway fallback below.
HDI=$(docker run --rm --network rainier-internal alpine:3.20 getent hosts host.docker.internal 2>/dev/null | awk '{print $1}') || true
GW="${HDI:-$BRIDGE_GW}"
echo "internal network bridge gateway: $BRIDGE_GW; host reachable via: $GW"

./bin/egressd --listen 0.0.0.0:3128 --admin 127.0.0.1:3129 >/tmp/egressd.log 2>&1 &
echo $! > /tmp/rainier-egressd.pid
# runnerd dials-base uses the gateway so containers can reach host runnerd.
./bin/runnerd --listen 0.0.0.0:8080 --dial-base "ws://$GW:8080" \
  --image rainier-session:latest --network rainier-internal \
  --egress-admin http://127.0.0.1:3129 >/tmp/runnerd.log 2>&1 &
echo $! > /tmp/rainier-runnerd.pid
sleep 1
echo "runnerd on :8080, egressd on :3128. Try:"
echo "  ./bin/runnerctl create            # → {\"session_id\":\"sess-1\"}"
echo "  ./bin/runnerctl ls"
echo "  ./bin/runnerctl attach sess-1      # live terminal through the relay"

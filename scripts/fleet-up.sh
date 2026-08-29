#!/usr/bin/env bash
set -euo pipefail
command -v docker >/dev/null || export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"
command -v docker >/dev/null || { echo "docker CLI not found" >&2; exit 1; }

docker build -q -t rainier-session:latest . >/dev/null
go build -o bin/runnerd ./cmd/runnerd
go build -o bin/egressd ./cmd/egressd
go build -o bin/rattach ./cmd/rattach
go build -o bin/runnerctl ./cmd/runnerctl

# egressd starts FIRST — before the session network is even decided — both
# because R4 wants it up before any session can be created, and because its
# real listener doubles as the platform probe's target below (no separate
# throwaway listener needed).
./bin/egressd --listen 0.0.0.0:3128 --admin 127.0.0.1:3129 >/tmp/egressd.log 2>&1 &
echo $! > /tmp/rainier-egressd.pid
sleep 0.3

# --- R4 platform probe (Task 13 spike; see docker-compose.fleet.yml's
# comment and docs/superpowers/specs/2026-08-28-plan3-controld-design.md §3
# Amendment for the full writeup) ---
#
# `docker network create --internal` removes a container's default route
# entirely, leaving only a local-link route to its own subnet. On native
# Linux dockerd (the GCE production target) the bridge gateway address IS
# the host — dockerd runs there directly — so that local-link route is
# still enough to reach it. On VM-backed docker (Docker Desktop, colima,
# OrbStack — the common macOS dev setup) the real host is reachable only via
# host.docker.internal's off-subnet NAT address, and the missing default
# route blocks exactly that: an --internal network on VM-backed docker
# cannot reach the host AT ALL, not runnerd's register listener, not
# egressd. Confirmed against the container's own routing table (only a
# local-link route, no default route) and a raw-IP connection attempt to
# host.docker.internal's address ("Network unreachable", vs "Connection
# refused" for the always-local bridge gateway).
#
# We don't infer which platform this is from the docker context name — not
# every VM-backed setup names its context predictably, and someone could
# rename one — we prove it directly, the same way the spike did: spin up a
# throwaway --internal network, and from a container on it, try to reach
# the egressd listener just started above, via host.docker.internal (forced
# through --add-host so the test doesn't depend on embedded DNS separately
# omitting that name for --internal networks — a second, compounding
# finding from the same spike) and via the probe network's own bridge
# gateway. `nc -z` proves an actual listening service answered — not just
# that some address was routable to.
PROBE_NET=rainier-internal-probe
docker network rm "$PROBE_NET" >/dev/null 2>&1 || true
docker network create --internal "$PROBE_NET" >/dev/null
PROBE_GW=$(docker network inspect "$PROBE_NET" -f '{{ (index .IPAM.Config 0).Gateway }}')
ENFORCE_INTERNAL=0
if docker run --rm --network "$PROBE_NET" --add-host=host.docker.internal:host-gateway alpine:3.20 \
     sh -c "nc -z -w2 host.docker.internal 3128 || nc -z -w2 $PROBE_GW 3128" >/dev/null 2>&1; then
  ENFORCE_INTERNAL=1
fi
docker network rm "$PROBE_NET" >/dev/null 2>&1 || true

# Create (or fix up) the real session network to match what the probe found.
# Docker can't flip an existing network's --internal flag in place, so a
# stale network from a prior run — or a platform change since the last
# run — has to be recreated, not just inspected-and-skipped like before
# Task 13.
WANT_FLAG=""
WANT_BOOL="false"
if [ "$ENFORCE_INTERNAL" = "1" ]; then
  WANT_FLAG="--internal"
  WANT_BOOL="true"
fi
if docker network inspect rainier-internal >/dev/null 2>&1; then
  HAVE_BOOL=$(docker network inspect rainier-internal -f '{{.Internal}}')
  if [ "$HAVE_BOOL" != "$WANT_BOOL" ]; then
    echo "rainier-internal exists with Internal=$HAVE_BOOL, want $WANT_BOOL — recreating"
    docker network rm rainier-internal >/dev/null 2>&1 || \
      echo "could not recreate rainier-internal (containers still attached?) — leaving it as-is" >&2
  fi
fi
docker network inspect rainier-internal >/dev/null 2>&1 || \
  docker network create $WANT_FLAG rainier-internal >/dev/null

if [ "$ENFORCE_INTERNAL" = "1" ]; then
  echo "R4 egress enforcement: ON (internal:true — host reachable from an internal network on this platform)"
else
  cat >&2 <<'EOF'
================================================================================
 R4 EGRESS ENFORCEMENT IS OFF ON THIS MACHINE (dev override, VM-backed docker)
 Session containers on rainier-internal have UNRESTRICTED direct internet
 egress here — the proxy/allowlist path still works end to end (identity,
 allow/deny, audit log), but nothing blocks bypassing it entirely. This is
 expected on Docker Desktop/colima/OrbStack (see docker-compose.fleet.yml's
 comment) — enforcement itself is verified on Linux dockerd instead (CI, and
 the GCE deploy's acceptance run). scripts/egress-check.sh detects this same
 condition and SKIPs (exit 3) rather than asserting enforcement that cannot
 hold on this platform.
================================================================================
EOF
fi

# How session containers reach host-run runnerd/egressd. Same fallback chain
# as before Task 13 — it's still correct on whichever branch above we landed
# on: host.docker.internal when it resolves (VM-backed, enforcement off),
# this network's own bridge gateway otherwise (Linux, enforcement on).
# `|| true`: under `set -o pipefail`, getent failing inside the container
# propagates as docker run's own exit status — without this guard that
# failure would abort the script here instead of falling through to the
# bridge-gateway fallback below.
BRIDGE_GW=$(docker network inspect rainier-internal -f '{{ (index .IPAM.Config 0).Gateway }}')
HDI=$(docker run --rm --network rainier-internal alpine:3.20 getent hosts host.docker.internal 2>/dev/null | awk '{print $1}') || true
GW="${HDI:-$BRIDGE_GW}"
echo "internal network bridge gateway: $BRIDGE_GW; host reachable via: $GW"

# runnerd dials-base uses the gateway so containers can reach host runnerd.
# --proxy-url is the BASE proxy URL only (no userinfo) — the driver embeds
# each session's own id as URL userinfo per container (Task 13, egress R4),
# so every session gets a distinct, correctly-scoped Proxy-Authorization
# identity from this one shared flag value.
./bin/runnerd --listen 0.0.0.0:8080 --dial-base "ws://$GW:8080" \
  --image rainier-session:latest --network rainier-internal \
  --egress-admin http://127.0.0.1:3129 --proxy-url "http://$GW:3128" \
  >/tmp/runnerd.log 2>&1 &
echo $! > /tmp/rainier-runnerd.pid
sleep 1
echo "runnerd on :8080, egressd on :3128. Try:"
echo "  ./bin/runnerctl create            # → {\"session_id\":\"sess-1\"}"
echo "  ./bin/runnerctl ls"
echo "  ./bin/runnerctl attach sess-1      # live terminal through the relay"
echo "  ./scripts/egress-check.sh          # R4 acceptance (enforcement asserted only where ON)"

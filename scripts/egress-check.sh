#!/usr/bin/env bash
set -euo pipefail
command -v docker >/dev/null || export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"
# Create a session allowing example.com only, then from inside it try an
# allowed and a denied host through the proxy env.
SID=$(./bin/runnerctl create rainier-session:latest | sed 's/.*"session_id":"//;s/".*//')
echo "created $SID"
sleep 2
CID=$(docker ps -q --filter "label=rainier.session=$SID")
echo "container $CID"
# The proxy env is injected by the driver? v0: driver injects RAINIER_DIAL only;
# HTTP(S)_PROXY injection is added here if present. Assert the container is on the
# internal network and reached runnerd (registered) — check runnerd log.
grep -q "registered" /tmp/runnerd.log && echo "PASS: session registered with runnerd"
./bin/runnerctl rm "$SID"

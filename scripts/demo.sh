#!/usr/bin/env bash
# scripts/demo.sh — proves the session outlives its clients.
set -euo pipefail

command -v docker >/dev/null || export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"
command -v docker >/dev/null || { echo "docker CLI not found" >&2; exit 1; }

docker build -q -t rainier-sessiond .
docker rm -f rainier-demo >/dev/null 2>&1 || true
docker run -d --name rainier-demo -p 7070:7070 rainier-sessiond
sleep 1
echo "── attach #1: writing a marker into the live shell"
printf 'MARKER=hello-from-attach-1\r' | ./bin/rattach --url ws://127.0.0.1:7070 >/tmp/attach1.out 2>&1 &
sleep 2
kill %1 2>/dev/null || true
echo "── client #1 gone (simulated laptop sleep). Reattaching…"
echo "── attach #2: interactive — you should see the prior screen. Ctrl-] to exit."
./bin/rattach --url ws://127.0.0.1:7070
docker rm -f rainier-demo >/dev/null

#!/usr/bin/env bash
command -v docker >/dev/null || export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"
for pidf in /tmp/rainier-runnerd.pid /tmp/rainier-egressd.pid; do
  [ -f "$pidf" ] && kill "$(cat "$pidf")" 2>/dev/null; rm -f "$pidf"
done
# Remove any rainier-managed session containers.
ids=$(docker ps -aq --filter label=rainier.session 2>/dev/null || true)
[ -n "$ids" ] && docker rm -f $ids >/dev/null 2>&1 || true
echo "fleet down."

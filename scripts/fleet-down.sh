#!/usr/bin/env bash
command -v docker >/dev/null || export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"
for pidf in /tmp/rainier-runnerd.pid /tmp/rainier-egressd.pid; do
  [ -f "$pidf" ] && kill "$(cat "$pidf")" 2>/dev/null; rm -f "$pidf"
done
# Remove any rainier-managed session containers.
ids=$(docker ps -aq --filter label=rainier.session 2>/dev/null || true)
[ -n "$ids" ] && docker rm -f $ids >/dev/null 2>&1 || true
# ...and the per-session /workspace volumes behind them. The driver's Destroy
# removes a session's volume with its container, but this script bypasses the
# driver entirely, so without this a torn-down fleet leaves one volume per
# session it ever ran — with nothing left naming them. Volumes only, after the
# containers: docker refuses to remove a volume any container still holds.
vols=$(docker volume ls -q --filter name=rainier-ws- 2>/dev/null || true)
[ -n "$vols" ] && docker volume rm -f $vols >/dev/null 2>&1 || true
echo "fleet down."

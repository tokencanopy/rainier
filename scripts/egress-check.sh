#!/usr/bin/env bash
# Verifies R4: sessions have no direct egress; egressd's per-session
# allowlist — reached via the env-var proxy flow, identity carried as
# URL-userinfo Basic auth (Task 13) — is the only path out.
#
# Platform split (see docker-compose.fleet.yml's comment and
# docs/superpowers/specs/2026-08-28-plan3-controld-design.md §3 Amendment):
# actual network-level enforcement only holds on native Linux dockerd.
# fleet-up.sh already auto-detected that and left rainier-internal
# non-internal on a platform that can't enforce it (VM-backed docker —
# Docker Desktop, colima, OrbStack). This script detects the same condition
# by inspecting the network fleet-up.sh actually created, and SKIPs rather
# than asserting enforcement this platform cannot provide.
#
# Exit codes: 0 = pass, 1 = fail, 2 = usage/setup error, 3 = SKIPPED
# (platform can't enforce network isolation — distinct from pass or fail so
# CI can tell "didn't run here" apart from "ran and broke").
set -euo pipefail
command -v docker >/dev/null || export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"
command -v docker >/dev/null || { echo "docker CLI not found" >&2; exit 2; }

BASE=${RUNNERD:-http://127.0.0.1:8080}

# Minor fix (review round 1): distinguish "the fleet was never started" from
# the VM-backed SKIP — a missing network is a setup error (exit 2), not a
# platform-capability finding (exit 3), and the two messages point at
# different fixes.
if ! docker network inspect rainier-internal >/dev/null 2>&1; then
  echo "ERROR: rainier-internal network not found — is the fleet running? Try ./scripts/fleet-up.sh first." >&2
  exit 2
fi
INTERNAL=$(docker network inspect rainier-internal -f '{{.Internal}}')
if [ "$INTERNAL" != "true" ]; then
  echo "SKIPPED: enforcement not verifiable on VM-backed docker — run on Linux (CI or the GCE VM)"
  exit 3
fi

# Minor fix (review round 1): sed's s/// leaves non-matching input
# UNCHANGED (and default-prints it), so without `-n ... p` a malformed
# create response would make $sid the entire raw JSON body instead of
# empty — silently passing the `[ -n "$sid" ]` guard with garbage. `-n`
# suppresses that auto-print; `p` only fires on an actual match, so a
# non-matching body correctly yields an empty $sid here.
create_resp=$(curl -sf -X POST "$BASE/sessions" -d '{"image":"rainier-session:latest","egress_allow":["example.com"],"cmd":["--","sleep","600"]}')
sid=$(printf '%s' "$create_resp" | sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
[ -n "$sid" ] || { echo "FAIL: create failed — response: $create_resp"; exit 1; }
cleanup() { curl -sf -X DELETE "$BASE/sessions/$sid" >/dev/null 2>&1 || true; }
trap cleanup EXIT

cid=$(docker ps -q --filter "label=rainier.session=$sid")
[ -n "$cid" ] || { echo "FAIL: no container for session $sid"; exit 1; }
sleep 1 # let sessiond register and the container's network settle

# 1. Direct egress must FAIL — internal:true means no default route out of
# the container at all, so this must fail at the network level. The proxy
# env vars are explicitly unset for this one check: the container has them
# set (Task 13 injects a proxy URL into every session), and BusyBox wget
# — discovered live while first running this script — does NOT simply
# ignore a configured https_proxy the way the Task 13 spike assumed from an
# unreachable-proxy probe; it sends a plain (non-CONNECT) request straight
# to the proxy, which egressd correctly 405s. Left as-is, that 405 would
# make this check pass on ANY platform where egressd is merely reachable —
# including one where internal:true is NOT actually blocking the route —
# which defeats the entire point of the check (it must fail because there
# is no route, not because the proxy rejected an unsupported method).
# Unsetting the proxy vars makes this a true test of the underlying route.
if docker exec "$cid" env -u HTTP_PROXY -u http_proxy -u HTTPS_PROXY -u https_proxy \
     wget -q -T 5 -O /dev/null https://example.com; then
  echo "FAIL: direct egress worked (internal:true is not actually enforcing)"
  exit 1
fi
echo "PASS: direct egress blocked"

# 2. Allowlisted host through the proxy must succeed. curl, not wget here:
# BusyBox wget can't tunnel HTTPS through a proxy at all in this image
# (verified in the spike — it silently ignores https_proxy and connects
# direct, which step 1 above relies on but step 2 can't use). curl honors
# the lowercase https_proxy the driver injects and automatically sends the
# session id as HTTP Basic auth from the proxy URL's userinfo
# (http://<session-id>:@host:port) — the only way a plain env-var proxy
# flow can carry identity to egressd's allowlist at all.
if ! docker exec "$cid" sh -c 'curl -sf -m 10 -o /dev/null https://example.com'; then
  echo "FAIL: allowlisted egress via proxy blocked"
  exit 1
fi
echo "PASS: allowlisted egress via proxy succeeded"

# 3. Non-allowlisted host through the proxy must FAIL — egressd answers 403,
# curl -f turns that into a non-zero exit.
if docker exec "$cid" sh -c 'curl -sf -m 5 -o /dev/null https://anthropic.com'; then
  echo "FAIL: deny leaked (non-allowlisted host reached anthropic.com through the proxy)"
  exit 1
fi
echo "PASS: non-allowlisted egress via proxy denied"

# Both eyeballs on the audit log: an allow line for example.com and a deny
# line for anthropic.com, each scoped to this exact session id — not just
# any allow/deny line anywhere in a log shared across every session that's
# ever run against this egressd.
if ! grep -F "\"session\":\"$sid\"" /tmp/egressd.log | grep -F '"host":"example.com"' | grep -qF '"decision":"allow"'; then
  echo "FAIL: no allow audit line for session $sid host example.com in /tmp/egressd.log"
  exit 1
fi
if ! grep -F "\"session\":\"$sid\"" /tmp/egressd.log | grep -F '"host":"anthropic.com"' | grep -qF '"decision":"deny"'; then
  echo "FAIL: no deny audit line for session $sid host anthropic.com in /tmp/egressd.log"
  exit 1
fi
echo "PASS: audit log shows both the allow and deny decisions for session $sid"

echo "egress R4 OK"

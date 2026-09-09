#!/usr/bin/env bash
# scripts/session-image-browser-e2e.sh — can a developer run their project's
# Playwright tests in a fresh session, with nothing set up first?
#
# scripts/session-image-smoke.sh answers the image half of that offline: the
# browser is present, root-owned, linked into the cache and able to render.
# This answers the whole of it, through the tool a developer actually uses, on
# a project that has never been in this image: install locked dependencies,
# start a loopback web server, drive a real Chromium at a desktop and a phone
# viewport, assert on what it laid out, write a screenshot, leave a trace and a
# video behind when a test fails, and leave no process behind when it does not.
# Then do it again, because "works once" and "works" are different claims.
#
# The restrictions are the driver's, copied from internal/driver.runArgs: uid
# 1000, no-new-privileges, a read-only rootfs, a noexec tmpfs on /tmp, docker's
# 64 MiB /dev/shm, a workspace volume, and no host mount of any kind. Nothing
# here is relaxed to make a step pass.
#
# The ONE difference from the smoke, and it is deliberate: `npm ci` gets a
# network, because installing a project's locked dependencies IS a network
# operation and pretending otherwise would test nothing. Every step after it —
# both test runs and the failure run — is back on --network none, which is what
# makes "the preinstalled browser needed no download" an observation rather
# than a hope.
#
# Usage:  scripts/session-image-browser-e2e.sh [image]
# Env:    DOCKER=<docker executable>  STEP_TIMEOUT=<seconds>  KEEP=1
# Exit:   0 every check passed, 1 a check failed, 2 setup or usage error.
set -uo pipefail

IMAGE=${1:-rainier-session:smoke}
DOCKER=${DOCKER:-docker}
STEP_TIMEOUT=${STEP_TIMEOUT:-600}

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/session-image-checks.sh"

SAMPLE_DIR="$SCRIPT_DIR/../images/session/browser-sample"
[ -f "$SAMPLE_DIR/package-lock.json" ] \
  || { echo "no sample project at $SAMPLE_DIR" >&2; exit 2; }
command -v "$DOCKER" >/dev/null 2>&1 || { echo "no docker executable ($DOCKER); set DOCKER=" >&2; exit 2; }
"$DOCKER" image inspect "$IMAGE" >/dev/null 2>&1 \
  || { echo "image $IMAGE is not present; build it first (make session-image)" >&2; exit 2; }

SUFFIX=$(od -An -tx1 -N6 /dev/urandom | tr -d ' \n')
WS_VOL="rainier-browser-ws-$SUFFIX"

cleanup() {
  [ "${KEEP:-0}" = 1 ] && return 0
  "$DOCKER" volume rm -f "$WS_VOL" >/dev/null 2>&1
  return 0
}
trap cleanup EXIT

# The driver's own volume preparation, unchanged: root with CAP_CHOWN and
# nothing else, no network, the image's entrypoint never running. This is also
# the step that copies the image's /workspace — the seeded browser cache
# included — onto the fresh volume, which is the mechanism the whole
# no-download claim rests on.
"$DOCKER" volume create "$WS_VOL" >/dev/null || { echo "could not create the workspace volume" >&2; exit 2; }
"$DOCKER" run --rm --network none --user 0:0 \
  --security-opt no-new-privileges --cap-drop ALL --cap-add CHOWN --read-only \
  -v "$WS_VOL:/workspace" --entrypoint sh "$IMAGE" \
  -c 'mkdir -p /workspace/.rainier && chown -R 1000:1000 /workspace' >/dev/null \
  || { echo "could not initialize the workspace volume" >&2; exit 2; }

# step <network> <program> — one session-shaped container. The network argument
# is the only thing that varies between the install step and every other one,
# and it is spelled out at each call site rather than defaulted, because which
# steps are allowed to reach the internet is the interesting part of this file.
step() {
  local network=$1 program=$2
  "$DOCKER" run --rm \
    --network "$network" \
    --user 1000:1000 \
    --security-opt no-new-privileges \
    --cap-drop ALL \
    --read-only \
    --tmpfs /tmp \
    --memory 3g --pids-limit 1024 \
    -v "$WS_VOL:/workspace" \
    -w /workspace/browser-sample \
    -e CI=1 \
    --entrypoint timeout "$IMAGE" -k 10 "$STEP_TIMEOUT" /bin/bash -c "set -uo pipefail
$program" 2>&1
}
offline() { step none "$1"; }
online()  { step bridge "$1"; }

echo "== $IMAGE"
echo
echo "-- a project that has never been in this image"

# Staged as the session user through a pipe rather than a bind mount: a host
# mount is exactly what a session does not get, and a qualification that used
# one would be qualifying a different container.
if ! tar -C "$SCRIPT_DIR/../images/session" -c browser-sample \
     | "$DOCKER" run --rm -i --user 1000:1000 --network none \
         --security-opt no-new-privileges --cap-drop ALL \
         -v "$WS_VOL:/workspace" -w /workspace \
         --entrypoint tar "$IMAGE" -x; then
  echo "could not stage the sample project onto the workspace volume" >&2
  exit 2
fi

check "the sample project arrived with a lockfile and no node_modules" "staged" '
  test -f package-lock.json || { echo "no lockfile"; exit 1; }
  test -d node_modules && { echo "NODE_MODULES-PRESENT"; exit 1; }
  echo staged' offline

check "npm ci installs the project locked dependencies" "playwright-installed" '
  npm ci --no-audit --no-fund >/dev/null 2>&1 || { echo "npm ci failed"; exit 1; }
  test -x node_modules/.bin/playwright || { echo "no playwright in node_modules"; exit 1; }
  # The version that runs has to be the one the lockfile pins, not one the
  # image supplied: `npx playwright` resolves the project binary first, and
  # there is no global Playwright in this image for it to fall back to.
  node -p "require(\"playwright-core/package.json\").version"
  echo playwright-installed' online

# From here on: no network at all. A step that needed a download would fail,
# which is the whole point of preinstalling the browser.
# The default reporters from the sample project own configuration, so the HTML
# report a developer opens is written by the same run that is being checked.
check "the suite runs offline, at a desktop and a phone viewport, with nothing downloaded" "6 passed" '
  npx playwright test 2>&1 | tail -30' offline

check "it runs a second time in the same workspace, still offline" "6 passed" '
  npx playwright test --reporter=list 2>&1 | tail -30' offline

check "nothing was downloaded into the browser cache" "cache-unchanged" '
  # Every entry the cache carries is still one of the baseline links. A real
  # directory here would mean the project had to fetch a browser, which is a
  # legitimate thing for a project on another Playwright to do and exactly what
  # this image exists to make unnecessary for the pinned one.
  for dir in "$PLAYWRIGHT_BROWSERS_PATH"/*/; do
    [ -f "$dir/.rainier-baseline" ] || { echo "UNEXPECTED $dir"; exit 1; }
  done
  echo cache-unchanged' offline

check "the browser and the web server both exit with the suite" "nothing-left" '
  browsers_alive() {
    for p in /proc/[0-9]*; do
      case "$(readlink "$p/exe" 2>/dev/null)" in *chrome-headless-shell*) return 0 ;; esac
    done
    return 1
  }
  npx playwright test --reporter=line >/dev/null 2>&1 || { echo "the suite failed"; exit 1; }
  sleep 1
  browsers_alive && { echo BROWSER-STILL-RUNNING; exit 1; }
  # Playwright started the web server and owns stopping it. A listener left on
  # loopback would collide with the next run in the same workspace, which is
  # the failure a suspend-and-resume workflow hits first.
  ss -ltn 2>/dev/null | grep -q ":8973" && { echo SERVER-STILL-LISTENING; ss -ltnp; exit 1; }
  echo nothing-left' offline

check "a failing test leaves a trace, a screenshot and a video behind" "artifacts-ok" '
  rm -rf test-results
  RAINIER_BROWSER_SMOKE_FAIL=1 npx playwright test artifacts.spec.js --reporter=line >/dev/null 2>&1
  test -d test-results || { echo "no test-results directory"; exit 1; }
  for want in "trace.zip" "test-failed-1.png" ".webm"; do
    find test-results -name "*$want*" -size +0 | grep -q . || {
      echo "MISSING $want"; find test-results -type f | head -20; exit 1; }
  done
  echo artifacts-ok' offline

check "the report a developer opens was written to the workspace" "report-ok" '
  test -s playwright-report/index.html || { echo "no HTML report"; exit 1; }
  echo report-ok' offline

# What the run cost, on the record beside what it proved.
FOOTPRINT=$(offline '
  du -sh node_modules 2>/dev/null | cut -f1 | tr -d "\n"
  printf " node_modules; "
  du -sh "$PLAYWRIGHT_BROWSERS_PATH" 2>/dev/null | cut -f1 | tr -d "\n"
  printf " browser cache on the volume\n"' | tail -1)
printf 'note  workspace footprint: %s\n' "$FOOTPRINT"
note "browser workspace footprint" "$FOOTPRINT"

printf '\n%d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ] || exit 1

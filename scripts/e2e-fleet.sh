#!/usr/bin/env bash
# scripts/e2e-fleet.sh — the local dress rehearsal for the GCE milestone
# (docs/deploy-gce.md). It brings up the whole Plan 3 stack on this machine —
# Postgres in docker, controld on the host, egressd + a DIAL-MODE runnerd via
# fleet-up.sh — and then drives the REAL `rainier` CLI against it end to end:
# login, new, ls, attach (non-tty, piped stdin), suspend, resume, rm. It
# finishes with scripts/egress-check.sh, the R4 acceptance.
#
# Where internal/e2e's Go suite fakes the container runtime to make the chaos
# scenes deterministic, this one fakes nothing: real docker containers, real
# Postgres, real websockets, the real CLI binary, one host.
#
# Exit codes: 0 = every executed check passed, 1 = a check failed,
# 2 = setup/usage error, 3 = the CLI half was skipped (no GitHub auth) but
# everything that did run passed.
#
# Env:
#   GITHUB_USER   GitHub login to allowlist as admin (default: `gh api user`)
#   GH_TOKEN      a GitHub token to log in with instead of `gh auth token`
#   KEEP=1        leave the stack running afterwards (default: tear it down)
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

command -v docker >/dev/null || export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"
command -v docker >/dev/null || { echo "docker CLI not found" >&2; exit 2; }
command -v curl >/dev/null || { echo "curl not found" >&2; exit 2; }

# NOT "rainier-pg": that is the name docs/deploy-gce.md gives the real,
# `--restart unless-stopped`, named-volume Postgres on the dogfood VM, and
# this script does `docker rm -f` on its own container by name before
# starting it. A rehearsal run on the VM must not delete the fleet's actual
# database.
PG_CONTAINER=${PG_CONTAINER:-rainier-pg-e2e}
# 5433 by default so this never collides with a local Postgres on 5432. It is
# env-overridable because a dev box often has several throwaway databases
# mapped into the low 54xx range already — and docker's own "port is already
# allocated" is decided by the daemon, so nothing this script can probe from
# the host would reliably predict it.
PG_PORT=${PG_PORT:-5433}
PG_PASS=rainier
PG_DB=rainier
DSN="postgres://postgres:${PG_PASS}@127.0.0.1:${PG_PORT}/${PG_DB}?sslmode=disable"

# Everything in this rehearsal runs on one host, so 127.0.0.1 is both what
# the CLI dials and what the runner dials — and, critically, what controld
# hands back as the attach dial-back target (--external-url). The runner
# refuses a dial-back whose origin isn't its own controld, so those two must
# name the same host:port. (The GCE deploy substitutes the tailnet MagicDNS
# name for both; see docs/deploy-gce.md.)
CONTROLD_HOST=${CONTROLD_HOST:-127.0.0.1}
CONTROLD_PORT=${CONTROLD_PORT:-9090}
CONTROLD_HTTP="http://${CONTROLD_HOST}:${CONTROLD_PORT}"
CONTROLD_WS="ws://${CONTROLD_HOST}:${CONTROLD_PORT}"

# The CLI's config carries a bearer token; keep this rehearsal out of the
# developer's real ~/.config/rainier/config.json.
export RAINIER_CONFIG=/tmp/rainier-e2e-config.json

MARK="rainier-e2e-$$"
SESSION_NAME="e2e-$$"
CONTROLD_LOG=/tmp/controld.log
CONTROLD_PID=/tmp/rainier-controld.pid

step()  { printf '\n=== %s\n' "$*"; }
ok()    { printf 'PASS: %s\n' "$*"; }
fail()  { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
setup_error() { printf 'ERROR: %s\n' "$*" >&2; exit 2; }

# Ownership flags. Teardown destroys state, and this script must only ever
# destroy state IT created: an abort before we own anything — the preflight
# finding another fleet on these ports, a build failure, a missing binary —
# has to leave that other fleet, its session containers, its controld and its
# database exactly as they were. Each flag flips only once the corresponding
# thing is actually ours.
PG_STARTED=0
CONTROLD_STARTED=0
FLEET_STARTED=0

cleanup() {
  local rc=$?
  if [ "$PG_STARTED$CONTROLD_STARTED$FLEET_STARTED" = "000" ]; then
    return $rc # nothing of ours ever started; destroy nothing
  fi
  if [ "${KEEP:-0}" = "1" ]; then
    echo
    echo "KEEP=1: leaving the stack up (controld $CONTROLD_HTTP, postgres 127.0.0.1:$PG_PORT)."
    echo "Tear down with: ./scripts/fleet-down.sh; kill \$(cat $CONTROLD_PID); docker rm -f $PG_CONTAINER"
    return $rc
  fi
  echo
  echo "--- tearing the stack down (logs kept: $CONTROLD_LOG /tmp/runnerd.log /tmp/egressd.log)"
  [ "$FLEET_STARTED" = "1" ] && { ./scripts/fleet-down.sh >/dev/null 2>&1 || true; }
  if [ "$CONTROLD_STARTED" = "1" ] && [ -f "$CONTROLD_PID" ]; then
    kill "$(cat "$CONTROLD_PID")" 2>/dev/null || true
    rm -f "$CONTROLD_PID"
  fi
  [ "$PG_STARTED" = "1" ] && { docker rm -f "$PG_CONTAINER" >/dev/null 2>&1 || true; }
  return $rc
}
trap cleanup EXIT

# waitfor CONDITION_CMD SECONDS DESCRIPTION — bounded poll, not a fixed sleep.
waitfor() {
  local cmd=$1 secs=$2 what=$3 i
  for ((i = 0; i < secs * 5; i++)); do
    if eval "$cmd" >/dev/null 2>&1; then return 0; fi
    sleep 0.2
  done
  return 1
}

# port_busy PORT — true when something on this host already listens there.
# bash's /dev/tcp needs no nc/lsof dependency; this script always runs under
# bash (see the shebang), the same trick fleet-up.sh uses for its egressd
# readiness poll.
port_busy() {
  (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null && { exec 3>&- 3<&-; return 0; }
  return 1
}

# ---------------------------------------------------------------------------
step "preflight"
# ---------------------------------------------------------------------------
# A leftover fleet from an earlier run is the single most common way this
# script fails, and it fails LATE and cryptically (runnerd log.Fatals on
# "address already in use" seconds after fleet-up.sh reports success). Catch
# it here, where the fix is one line.
RUNNERD_PORT="${RUNNERD_PORT:-8080}"
export RUNNERD_PORT  # fleet-up.sh reads it; egress-check.sh gets RUNNERD below
for port in "$CONTROLD_PORT" "$RUNNERD_PORT" 3128 3129; do
  if port_busy "$port"; then
    setup_error "127.0.0.1:$port is already in use — a previous fleet is probably still running. Try ./scripts/fleet-down.sh (it kills the pid files), or 'lsof -nP -iTCP:$port -sTCP:LISTEN' to find the process."
  fi
done
echo "ports $CONTROLD_PORT, $RUNNERD_PORT, 3128, 3129 are free"

# ---------------------------------------------------------------------------
step "build"
# ---------------------------------------------------------------------------
CGO_ENABLED=0 go build -o bin/ ./cmd/...
echo "built: $(ls bin | tr '\n' ' ')"

# ---------------------------------------------------------------------------
step "postgres ($PG_CONTAINER on 127.0.0.1:$PG_PORT)"
# ---------------------------------------------------------------------------
# Removing a stale container of OUR OWN name (see PG_CONTAINER's comment —
# never the deploy's "rainier-pg") is the first thing this run does that
# touches shared state, so ownership starts here.
docker rm -f "$PG_CONTAINER" >/dev/null 2>&1 || true
PG_STARTED=1
if ! PG_ERR=$(docker run -d --name "$PG_CONTAINER" \
  -e POSTGRES_PASSWORD="$PG_PASS" -e POSTGRES_DB="$PG_DB" \
  -p "127.0.0.1:${PG_PORT}:5432" postgres:16-alpine 2>&1); then
  echo "$PG_ERR" >&2
  case "$PG_ERR" in
    *"already allocated"*|*"address already in use"*)
      setup_error "127.0.0.1:${PG_PORT} is already taken by another container — re-run with PG_PORT=<free port>." ;;
    *) setup_error "could not start postgres" ;;
  esac
fi
waitfor "docker exec $PG_CONTAINER pg_isready -U postgres -d $PG_DB" 60 "postgres" \
  || setup_error "postgres never became ready; docker logs $PG_CONTAINER"
ok "postgres ready"

# ---------------------------------------------------------------------------
step "identity"
# ---------------------------------------------------------------------------
# controld's allowlist is fail-closed: an empty --admins means nobody can log
# in at all. The CLI half of this script therefore needs a GitHub login to
# allowlist AND a GitHub token to exchange for a controld bearer.
GH_LOGIN="${GITHUB_USER:-}"
GH_TOKEN_ARG=()
RUN_CLI=1
SKIP_REASON=""
if [ -n "${GH_TOKEN:-}" ]; then
  GH_TOKEN_ARG=(--token "$GH_TOKEN")
elif command -v gh >/dev/null 2>&1 && gh auth status >/dev/null 2>&1; then
  GH_TOKEN_ARG=(--from-gh)
  [ -n "$GH_LOGIN" ] || GH_LOGIN=$(gh api user --jq .login 2>/dev/null || true)
else
  RUN_CLI=0
  SKIP_REASON="\`gh\` is not installed or not authenticated, and GH_TOKEN is unset — run 'gh auth login', or re-run with GH_TOKEN=<token> GITHUB_USER=<login>."
fi
if [ "$RUN_CLI" = "1" ] && [ -z "$GH_LOGIN" ]; then
  RUN_CLI=0
  SKIP_REASON="could not determine the GitHub login to allowlist — re-run with GITHUB_USER=<login>."
fi
ADMIN="${GH_LOGIN:-nobody}"
if [ "$RUN_CLI" = "1" ]; then
  echo "allowlisting GitHub login \"$ADMIN\" as admin"
else
  echo "SKIPPING the CLI steps: $SKIP_REASON"
  echo "(the fleet still comes up, and the egress acceptance still runs)"
fi

# ---------------------------------------------------------------------------
step "controld ($CONTROLD_HTTP)"
# ---------------------------------------------------------------------------
RUNNER_TOKEN="dev-$(openssl rand -hex 8)"
# A fresh secrets key per run: controld requires one (team secrets are
# AES-GCM-sealed at rest under it), and a throwaway fleet has no secrets
# worth carrying across runs.
SECRETS_KEY="$(openssl rand -hex 32)"
./bin/controld --listen "${CONTROLD_HOST}:${CONTROLD_PORT}" --db "$DSN" \
  --runner-token "$RUNNER_TOKEN" --external-url "$CONTROLD_HTTP" \
  --secrets-key "$SECRETS_KEY" \
  --admins "$ADMIN" >"$CONTROLD_LOG" 2>&1 &
echo $! > "$CONTROLD_PID"
CONTROLD_STARTED=1
# Off the job table: the teardown kills it by pid, and without this bash
# prints a "Terminated" notice over the final summary when it reaps the job.
disown %% 2>/dev/null || true
waitfor "curl -sf $CONTROLD_HTTP/healthz" 30 "controld" \
  || setup_error "controld never answered /healthz; see $CONTROLD_LOG"
ok "controld healthy (migrations applied against $PG_CONTAINER)"

# ---------------------------------------------------------------------------
step "fleet (egressd + dial-mode runnerd)"
# ---------------------------------------------------------------------------
FLEET_STARTED=1
CONTROLD_URL="$CONTROLD_WS" RAINIER_RUNNER_TOKEN="$RUNNER_TOKEN" RUNNER_NAME="${RUNNER_NAME:-vm-local}" \
  ./scripts/fleet-up.sh
waitfor "grep -q 'runner .* connected' $CONTROLD_LOG" 30 "runner join" \
  || fail "the runner never joined the fleet; see $CONTROLD_LOG and /tmp/runnerd.log"
ok "runner joined: $(grep -m1 'runner .* connected' "$CONTROLD_LOG" | sed 's/.*controld: //')"

if [ "$RUN_CLI" = "0" ]; then
  step "egress R4 acceptance"
  set +e; RUNNERD="http://127.0.0.1:$RUNNERD_PORT" ./scripts/egress-check.sh; EGRESS_RC=$?; set -e
  case "$EGRESS_RC" in
    0) ok "egress R4 enforced and verified" ;;
    3) echo "SKIPPED: egress enforcement is not verifiable on this platform (see the message above)" ;;
    *) fail "egress-check.sh exited $EGRESS_RC" ;;
  esac
  echo
  echo "e2e-fleet: fleet OK, CLI steps SKIPPED ($SKIP_REASON)"
  exit 3
fi

# ---------------------------------------------------------------------------
step "rainier login"
# ---------------------------------------------------------------------------
rm -f "$RAINIER_CONFIG"
./bin/rainier login "${GH_TOKEN_ARG[@]}" --server "$CONTROLD_HTTP" \
  || fail "rainier login (is \"$ADMIN\" the login your GitHub token belongs to?)"
[ -f "$RAINIER_CONFIG" ] || fail "login did not write $RAINIER_CONFIG"
PERM=$(stat -f '%Lp' "$RAINIER_CONFIG" 2>/dev/null || stat -c '%a' "$RAINIER_CONFIG")
[ "$PERM" = "600" ] || fail "config file mode is $PERM, want 600 (it holds a bearer token)"
ok "logged in; config is 0600"

# ---------------------------------------------------------------------------
step "rainier new"
# ---------------------------------------------------------------------------
# --detach: this script drives the attach itself, below, with piped stdin.
# No `-- cmd`: the session image's own CMD (`-- bash -i`) is what makes the
# attach echo check meaningful.
SID=$(./bin/rainier new --detach --name "$SESSION_NAME" --image rainier-session:latest --egress example.com)
case "$SID" in
  sess_*) ok "created $SID" ;;
  *) fail "new printed \"$SID\", want a sess_ id" ;;
esac

# state column of `rainier ls`: ID NAME STATE RUNNER REACHABLE AGE
session_state() { ./bin/rainier ls | awk -v id="$SID" '$1 == id { print $3 }'; }
waitfor '[ "$(session_state)" = running ]' 90 "session running" \
  || fail "$SID never reached running (state: $(session_state)); see /tmp/runnerd.log"
ok "session is running: $(./bin/rainier ls | awk -v id="$SID" '$1 == id')"

# ---------------------------------------------------------------------------
step "rainier ls"
# ---------------------------------------------------------------------------
./bin/rainier ls | tee /tmp/rainier-e2e-ls.txt
grep -q "$SESSION_NAME" /tmp/rainier-e2e-ls.txt || fail "ls did not list $SESSION_NAME"
ok "ls lists the session"

# ---------------------------------------------------------------------------
step "rainier attach (non-tty, piped stdin)"
# ---------------------------------------------------------------------------
# attachio's non-tty path skips raw mode and announces a fixed 80x24, so the
# resize-first contract still holds. \r is what a real terminal sends for
# Enter; \035 is Ctrl-], the detach key. The PTY echoes the typed line AND
# bash prints the command's output, so a working end-to-end terminal plane
# shows the marker at least twice.
#
# stdin is a FIFO rather than a `{ …; sleep 5; … } |` pipeline so the detach
# key is sent when the marker has actually come back, not after a fixed wait:
# on a fast machine that wait was four wasted seconds, and on a loaded one it
# was a flake waiting to happen.
ATTACH_OUT=/tmp/rainier-e2e-attach.txt
ATTACH_FIFO=/tmp/rainier-e2e-attach.fifo
rm -f "$ATTACH_FIFO"; mkfifo "$ATTACH_FIFO"
marker_hits() { grep -c "$MARK" "$ATTACH_OUT" 2>/dev/null || true; }

: > "$ATTACH_OUT"
./bin/rainier attach "$SID" <"$ATTACH_FIFO" >"$ATTACH_OUT" 2>&1 &
ATTACH_JOB=$!
exec 9>"$ATTACH_FIFO"          # holds the write end open for the whole attach
printf 'echo %s\r' "$MARK" >&9

if ! waitfor '[ "$(marker_hits)" -ge 2 ]' 30 "the shell to echo the marker"; then
  printf '\035' >&9; exec 9>&-; wait "$ATTACH_JOB" 2>/dev/null || true
  echo "--- attach output ---"; cat -v "$ATTACH_OUT"; echo "---------------------"
  fail "attach echoed the marker $(marker_hits) time(s) in 30s, want >= 2 (typed line + command output)"
fi
HITS=$(marker_hits)
printf '\035' >&9                # Ctrl-]: detach
exec 9>&-
wait "$ATTACH_JOB" 2>/dev/null || true
rm -f "$ATTACH_FIFO"

grep -q "detached at seq" "$ATTACH_OUT" || fail "attach did not print the detach status line"
ok "attach relayed a live shell and detached cleanly (marker seen $HITS times)"

# ---------------------------------------------------------------------------
step "rainier suspend / resume"
# ---------------------------------------------------------------------------
./bin/rainier suspend "$SID" | grep -q "suspended_warm" || fail "suspend did not report suspended_warm"
[ "$(session_state)" = "suspended_warm" ] || fail "state after suspend = $(session_state), want suspended_warm"
ok "suspended (warm)"
./bin/rainier resume "$SID" | grep -q "running" || fail "resume did not report running"
[ "$(session_state)" = "running" ] || fail "state after resume = $(session_state), want running"
ok "resumed"

# ---------------------------------------------------------------------------
step "rainier rm"
# ---------------------------------------------------------------------------
./bin/rainier rm "$SID" | grep -q "removed" || fail "rm did not report removal"
waitfor '[ -z "$(session_state)" ]' 30 "session gone from ls" \
  || fail "$SID still listed after rm (state: $(session_state))"
# Scoped to THIS session's row: a bare `grep -q destroyed` over the whole
# --all listing passes on any leftover destroyed session from an earlier run
# — including one this run's rm never touched.
session_state_all() { ./bin/rainier ls --all | awk -v id="$SID" '$1 == id { print $3 }'; }
[ "$(session_state_all)" = "destroyed" ] \
  || fail "ls --all shows $SID as \"$(session_state_all)\", want destroyed"
ok "removed; $SID's terminal row is still visible under ls --all"

# ---------------------------------------------------------------------------
step "egress R4 acceptance"
# ---------------------------------------------------------------------------
set +e; RUNNERD="http://127.0.0.1:$RUNNERD_PORT" ./scripts/egress-check.sh; EGRESS_RC=$?; set -e
case "$EGRESS_RC" in
  0) ok "egress R4 enforced and verified" ;;
  3) echo "SKIPPED: egress enforcement is not verifiable on this platform (see the message above)" ;;
  *) fail "egress-check.sh exited $EGRESS_RC" ;;
esac

echo
echo "e2e-fleet: ALL CHECKS PASSED (login, new, ls, attach, suspend, resume, rm$([ "$EGRESS_RC" = 0 ] && echo ", egress R4"))"

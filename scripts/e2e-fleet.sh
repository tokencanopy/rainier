#!/usr/bin/env bash
# scripts/e2e-fleet.sh — the local dress rehearsal for the GCE milestone
# (docs/deploy-gce.md). It brings up the whole Plan 3 stack on this machine —
# Postgres in docker, controld on the host, egressd + a DIAL-MODE runnerd via
# fleet-up.sh — and then drives the REAL `rainier` CLI against it end to end:
# login, new, ls, attach (non-tty, piped stdin), suspend, resume, rm, and
# Plan 4's environments (secret set from stdin → env create with a setup
# script → a first session that runs it → the snapshot controld commits → a
# second session that boots the cache). Then Plan 5's GITHUB REHEARSAL against
# a throwaway private repo this script creates and deletes — clone at boot,
# the init hook, a real commit and push through the in-sandbox credential
# helper, the attribution GitHub records for it, `rainier diff`, `rainier
# creds`, and a push/pull round trip. It finishes with scripts/egress-check.sh,
# the R4 acceptance.
#
# Where internal/e2e's Go suite fakes the container runtime to make the chaos
# scenes deterministic, this one fakes nothing: real docker containers, real
# Postgres, real websockets, the real CLI binary, one host — and, in the github
# phase, real GitHub.
#
# Exit codes: 0 = every executed check passed, 1 = a check failed,
# 2 = setup/usage error, 3 = the CLI half was skipped (no GitHub auth) but
# everything that did run passed. A 0 can still carry FINDINGs — defects this
# run PROVED in the product, recorded rather than aborted on; the summary line
# names them (see `finding`).
#
# Env:
#   GITHUB_USER   GitHub login to allowlist as admin (default: `gh api user`)
#   GH_TOKEN      a GitHub token to log in with instead of `gh auth token`
#   SKIP_GITHUB=1 skip the github rehearsal phase even when it could run
#   KEEP=1        leave the stack running afterwards (default: tear it down).
#                 The throwaway GitHub repo is deleted regardless — see cleanup.
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

# finding TEXT — a defect this rehearsal PROVED in the system under test,
# recorded with its evidence instead of aborting. `fail` means the rehearsal
# is broken and nothing after it can be trusted; a finding means the product
# is broken in a way that is already known and written down (see the
# acceptance table in docs/deploy-gce.md), and the rest of the run is still
# worth having. The summary repeats them, so a green run with findings can
# never be mistaken for a clean one.
FINDINGS=0
finding() { FINDINGS=$((FINDINGS + 1)); printf 'FINDING: %s\n' "$*"; }

# Ownership flags. Teardown destroys state, and this script must only ever
# destroy state IT created: an abort before we own anything — the preflight
# finding another fleet on these ports, a build failure, a missing binary —
# has to leave that other fleet, its session containers, its controld and its
# database exactly as they were. Each flag flips only once the corresponding
# thing is actually ours.
PG_STARTED=0
CONTROLD_STARTED=0
FLEET_STARTED=0
# SCRATCH_SLUG is the throwaway GitHub repository the github phase creates,
# owner/name, empty until it exists. It is the one piece of state this script
# creates OUTSIDE this machine, so it is torn down first, unconditionally, and
# before any early return below: a leaked repository on somebody's account is
# the one failure here that a re-run does not clean up.
SCRATCH_SLUG=""

cleanup() {
  local rc=$?
  if [ -n "$SCRATCH_SLUG" ]; then
    echo
    echo "--- deleting the throwaway repository $SCRATCH_SLUG"
    if gh repo delete "$SCRATCH_SLUG" --yes >/dev/null 2>&1; then
      SCRATCH_SLUG=""
    else
      # Loud, and on stderr: this is the one thing a failed run can leave
      # behind that the next run will not tidy up.
      echo "WARNING: could not delete $SCRATCH_SLUG — delete it by hand: gh repo delete $SCRATCH_SLUG --yes" >&2
    fi
  fi
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

# cell ROW_PREFIX HEADER NEXT_HEADER — read one cell out of a `rainier` table
# on stdin, addressed by the CHARACTER OFFSET of its column header rather than
# by awk field number.
#
# The tables are tabwriter-aligned and a cell can be legitimately empty — ENV
# is, on every scratch session — which collapses under awk's whitespace
# splitting and shifts every field after it. `$3` in this script meant STATE
# until sessions grew an env column, and then meant STATE on scratch rows and
# the env name on rows that had one: a silent, row-dependent wrong answer.
# Header offsets don't shift, because tabwriter pads every row of a block to
# the same widths. An empty NEXT_HEADER means "to the end of the line", for
# the last column.
cell() {
  awk -v id="$1" -v h="$2" -v n="$3" '
    NR == 1 { s = index($0, h); e = (n == "" ? 0 : index($0, n)); next }
    index($0, id) == 1 {
      v = (e > 0 ? substr($0, s, e - s) : substr($0, s))
      gsub(/^[ \t]+|[ \t]+$/, "", v)
      print v
    }'
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
# pg_isready is not enough: postgres:16-alpine runs initdb under a temporary
# server, answers "ready" from it, then restarts, and a connection that lands
# in that window is reset — controld's store open pings exactly once and
# dies. Require two real connections two seconds apart before anyone opens it.
for _i in $(seq 1 30); do
  if docker exec "$PG_CONTAINER" psql -U postgres -d "$PG_DB" -Atc "select 1" >/dev/null 2>&1; then
    sleep 2
    docker exec "$PG_CONTAINER" psql -U postgres -d "$PG_DB" -Atc "select 1" >/dev/null 2>&1 && break
  fi
  sleep 1
done
ok "postgres accepted two connections two seconds apart"

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
# The DSN, runner token, and secrets key travel in the environment, which
# controld reads as the default of each flag; on the command line they would
# be readable in ps by every user on the host.
RAINIER_DB="$DSN" RAINIER_RUNNER_TOKEN="$RUNNER_TOKEN" RAINIER_SECRETS_KEY="$SECRETS_KEY" \
RAINIER_E2E_TEST_AGENT=1 \
./bin/controld --listen "${CONTROLD_HOST}:${CONTROLD_PORT}" --external-url "$CONTROLD_HTTP" \
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
# The runner's name is hoisted out of the call so the capability scene below
# can assert WHICH runner a session landed on; fleet-up.sh reads it from the
# environment exactly as it did when it was a command prefix.
RUNNER_NAME="${RUNNER_NAME:-vm-local}"
export RUNNER_NAME
# One portable capability, announced by this runner and required by an
# environment further down (D19). It travels as RAINIER_RUNNER_CAPABILITIES
# because fleet-up.sh owns runnerd's argv; runnerd reads the variable as the
# default of its repeatable --capability flag.
FLEET_CAPABILITY=e2e.gpu
CONTROLD_URL="$CONTROLD_WS" RAINIER_RUNNER_TOKEN="$RUNNER_TOKEN" \
  RAINIER_RUNNER_CAPABILITIES="$FLEET_CAPABILITY" \
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
# GNU stat first: on Linux, `stat -f` SUCCEEDS with filesystem status, so the
# fallback would never run; on macOS, `stat -c` fails and the BSD form runs.
PERM=$(stat -c '%a' "$RAINIER_CONFIG" 2>/dev/null || stat -f '%Lp' "$RAINIER_CONFIG")
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

# `rainier ls`: ID NAME ENV STATE RUNNER REACHABLE AGE — read by header
# offset, since ENV is empty on a scratch session (see `cell`).
#
# TWO readers, and which one a check needs is not a detail. A plain `ls`
# EXCLUDES the terminal states (canceled/failed/dead/destroyed) — that is what
# `--all` is for — so state_of answers "" for a session that failed exactly as
# it does for one that never existed. It is therefore the right reader for "is
# it gone", and the WRONG one for "did it fail": `state_of X = failed` is a
# condition that can never be true, so a waitfor on it always burns its whole
# bound and then reports the state as "".
#
# That is not hypothetical. The stale-credential scene below waited 120s for a
# session that had already failed in about a second, and blamed a boot chain
# that was working (Plan 5, first live rehearsal). Anything asserting on a
# terminal state reads state_all_of.
#
# Both take the FIRST WORD of the cell, because the STATE column is a rendered
# sentence and not a bare state: cmd/rainier's sessionStateCell annotates it
# with whatever the state alone leaves unanswered — "failed (exited 128)",
# "running (exited 0)", "queued (waiting for runner rainier-gpu)". Comparing
# the whole cell to "failed" is therefore false for every session whose agent
# exited, which is all of them; the annotation is for a human reading the
# table, and a check wants the state it decorates.
state_of()      { ./bin/rainier ls | cell "$1" STATE RUNNER | awk '{print $1}'; }
state_all_of()  { ./bin/rainier ls --all | cell "$1" STATE RUNNER | awk '{print $1}'; }
session_state() { state_of "$SID"; }
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
session_state_all() { state_all_of "$SID"; }
[ "$(session_state_all)" = "destroyed" ] \
  || fail "ls --all shows $SID as \"$(session_state_all)\", want destroyed"
ok "removed; $SID's terminal row is still visible under ls --all"

# ---------------------------------------------------------------------------
step "environments (secret → setup → snapshot cache)"
# ---------------------------------------------------------------------------
# Plan 4's whole pipeline against real containers: a team secret stored from
# stdin, an environment whose setup script proves it can see that secret, a
# first session that runs the script live, the snapshot controld commits from
# it, and a second session that boots the cached image instead.
ENV_NAME="e2e-env-$$"
SECRET_NAME=E2E_TOKEN
SECRET_VALUE="e2e-token-value-$$"
SECRET_LEN=${#SECRET_VALUE}
SETUP_FILE=/tmp/rainier-e2e-setup.sh
SECRET_OUT=/tmp/rainier-e2e-secret.txt

# attach_probe SID COMMAND OUTFILE ERE SECONDS — attach with piped stdin, run
# one command in the session's shell, and wait until ERE shows up in the
# session's output; the output file is left behind for the caller to grep
# further. Same FIFO shape as the attach step above, and for the same reason:
# the detach key goes out when the answer has actually arrived, not after a
# fixed sleep.
#
# The PTY echoes the typed line, so ERE must be something the COMMAND TEXT
# does not itself contain — otherwise the echo satisfies the wait and the
# probe proves nothing. Every caller below picks a pattern that only the
# command's OUTPUT can produce.
attach_probe() {
  local sid=$1 cmd=$2 out=$3 ere=$4 secs=$5 rc=0
  local fifo=/tmp/rainier-e2e-probe.fifo
  rm -f "$fifo"; mkfifo "$fifo"
  : > "$out"
  ./bin/rainier attach "$sid" <"$fifo" >"$out" 2>&1 &
  local job=$!
  exec 8>"$fifo"
  printf '%s\r' "$cmd" >&8
  waitfor "grep -qE '$ere' '$out'" "$secs" "$ere" || rc=1
  printf '\035' >&8
  exec 8>&-
  wait "$job" 2>/dev/null || true
  rm -f "$fifo"
  return $rc
}

# --- the secret. Piped on stdin, which is the documented way: it keeps the
# value out of the shell history and out of the process table.
printf '%s' "$SECRET_VALUE" | ./bin/rainier secret set "$SECRET_NAME" >"$SECRET_OUT" 2>&1 \
  || { cat "$SECRET_OUT" >&2; fail "secret set $SECRET_NAME"; }
grep -qx "set $SECRET_NAME" "$SECRET_OUT" \
  || fail "secret set printed \"$(cat "$SECRET_OUT")\", want \"set $SECRET_NAME\""
! grep -q "$SECRET_VALUE" "$SECRET_OUT" || fail "secret set echoed the value back"
./bin/rainier secret ls > /tmp/rainier-e2e-secrets.txt
grep -q "$SECRET_NAME" /tmp/rainier-e2e-secrets.txt || fail "secret ls does not list $SECRET_NAME"
! grep -q "$SECRET_VALUE" /tmp/rainier-e2e-secrets.txt || fail "secret ls printed the secret's VALUE"
ok "secret $SECRET_NAME stored from stdin; neither set nor ls echoed its value"

# --- the environment. The setup script writes to BOTH kinds of path, because
# the difference between them is the whole cache story: /opt/rainier-env is on
# the container's own filesystem and `docker commit` keeps it, so the SECOND
# session must still see that marker; /workspace is a volume the commit
# excludes, so the second session must NOT see that one (and its absence is
# also how we prove the setup did not simply run again).
# No `set -e`: a write that fails should let the session come up anyway, so the
# attach probes below can say exactly WHICH marker is missing. A script that
# died here would only produce "the environment never cached a snapshot".
cat > "$SETUP_FILE" <<'EOF'
#!/bin/sh
# Image-visible: this is what the snapshot is supposed to carry forward. The
# stock session image gives the session user this prefix (and puts its bin/ on
# PATH); /usr/local stays root-owned, because sessiond lives there.
echo installed > /opt/rainier-env/rainier-setup-marker
# Volume-backed: per-session by construction, never in the cache.
echo setup-ran > /workspace/setup-marker
if [ -z "${E2E_TOKEN:-}" ]; then
  echo "E2E_TOKEN is not set in this container" >&2
  exit 17
fi
printf 'secret-len=%s\n' "$(printf %s "$E2E_TOKEN" | wc -c | tr -d ' ')" > /workspace/secret-check
echo "e2e setup complete"
EOF

ENV_ID=$(./bin/rainier env create "$ENV_NAME" \
  --image rainier-session:latest --setup-file "$SETUP_FILE" \
  --secret-ref "$SECRET_NAME" --egress example.com)
case "$ENV_ID" in
  env_*) ok "created environment $ENV_NAME ($ENV_ID)" ;;
  *) fail "env create printed \"$ENV_ID\", want an env_ id" ;;
esac
env_cached() { ./bin/rainier env ls | cell "$ENV_NAME" CACHED ""; }
[ "$(env_cached)" = "no" ] || fail "a brand-new environment reads CACHED=$(env_cached), want no"

# --- the FIRST session: runs the setup script, then becomes the snapshot.
FIRST_START=$(date +%s)
SID1=$(./bin/rainier new --detach --name "$ENV_NAME-first" --env "$ENV_NAME")
case "$SID1" in sess_*) ;; *) fail "new --env printed \"$SID1\", want a sess_ id" ;; esac
waitfor '[ "$(state_of "$SID1")" = running ]' 120 "the first env session" \
  || fail "$SID1 never reached running (state: $(state_of "$SID1")); see /tmp/runnerd.log"
# "running" arrives when sessiond registers, which is BEFORE the setup script
# finishes — the cache landing is what says the first session is actually
# usable, and it is the number criterion 2 compares the second create against.
waitfor '[ "$(env_cached)" = yes ]' 300 "the environment to cache its snapshot" \
  || fail "$ENV_NAME never cached a snapshot; see $CONTROLD_LOG"
FIRST_SECS=$(( $(date +%s) - FIRST_START ))

grep -q "controld: environment $ENV_ID cached as " "$CONTROLD_LOG" \
  || fail "controld never logged a cache for $ENV_ID; see $CONTROLD_LOG"
SNAP_REF=$(grep -m1 "controld: environment $ENV_ID cached as " "$CONTROLD_LOG" | sed 's/.* cached as \([^ ]*\) on .*/\1/')
case "$SNAP_REF" in
  "rainier-env:$ENV_ID-"*) ok "snapshot is content-addressed: $SNAP_REF (cached in ${FIRST_SECS}s)" ;;
  *) fail "snapshot ref \"$SNAP_REF\" is not rainier-env:$ENV_ID-<hash>" ;;
esac
SHOW_REF=$(./bin/rainier env show "$ENV_NAME" | sed -n 's/.*"snapshot_ref": "\([^"]*\)".*/\1/p')
[ "$SHOW_REF" = "$SNAP_REF" ] || fail "env show reports snapshot_ref \"$SHOW_REF\"; the log says \"$SNAP_REF\""

CID1=$(docker ps -q --filter "label=rainier.session=$SID1")
[ -n "$CID1" ] || fail "no container is labeled rainier.session=$SID1"
IMAGE1=$(docker inspect -f '{{.Config.Image}}' "$CID1")
[ "$IMAGE1" = "rainier-session:latest" ] \
  || fail "the first session runs image \"$IMAGE1\", want the environment's own rainier-session:latest"
docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$CID1" | grep -q '^RAINIER_SETUP_B64=' \
  || fail "the first create carried no setup script"
ok "first create dispatched the environment's own image plus its setup script"

ATTACH1_OUT=/tmp/rainier-e2e-env-attach1.txt
attach_probe "$SID1" 'cat /opt/rainier-env/rainier-setup-marker /workspace/setup-marker /workspace/secret-check' \
  "$ATTACH1_OUT" 'secret-len=' 60 \
  || { echo "--- attach output ---"; cat -v "$ATTACH1_OUT"; echo "---------------------"; \
       fail "the first session never answered the setup-marker probe"; }
grep -q 'installed' "$ATTACH1_OUT" || fail "/opt/rainier-env/rainier-setup-marker is missing: the setup script could not write outside /workspace"
grep -q 'setup-ran' "$ATTACH1_OUT" || fail "/workspace/setup-marker is missing: the setup script did not run"
grep -q "secret-len=$SECRET_LEN" "$ATTACH1_OUT" \
  || fail "the setup script saw $SECRET_NAME as \"$(grep -o 'secret-len=[0-9]*' "$ATTACH1_OUT" | head -1)\", want secret-len=$SECRET_LEN"
ok "the setup script ran in the session, installed to /opt/rainier-env, and read the environment's secret ($SECRET_LEN bytes)"

# The boundary that makes the writable build acceptable, against the REAL
# session image: the session user owns its install prefix and cannot touch
# /usr/local/bin, where sessiond — this session's PID 1 — lives. An agent that
# could rewrite PID 1 during a build would have it baked into the cached image
# every later session of the environment boots (design §10).
docker exec -u 1000:1000 "$CID1" sh -c 'touch /opt/rainier-env/writable-probe' >/dev/null 2>&1 \
  || fail "the session user cannot write /opt/rainier-env in a setup build; nothing an environment installs can be cached"
if docker exec -u 1000:1000 "$CID1" sh -c 'touch /usr/local/bin/probe' >/dev/null 2>&1; then
  fail "the session user can write /usr/local/bin during a setup build — sessiond (PID 1) must stay root-owned even while the rootfs is writable"
fi
ok "during a setup build the session user owns /opt/rainier-env and still cannot touch sessiond in /usr/local/bin"
grep -q 'e2e setup complete' "$ATTACH1_OUT" \
  && ok "the setup script's own output is in the session's scrollback — an attached viewer watches provisioning" \
  || finding "the setup script's output is not in the session's scrollback; design §4.3 says setup is streamed to the attached terminal like any other output"

# --- the SECOND session: must boot the cache, with no setup dispatched.
SECOND_START=$(date +%s)
SID2=$(./bin/rainier new --detach --name "$ENV_NAME-second" --env "$ENV_NAME")
case "$SID2" in sess_*) ;; *) fail "the second new --env printed \"$SID2\"" ;; esac
waitfor '[ "$(state_of "$SID2")" = running ]' 120 "the second env session" \
  || fail "$SID2 never reached running (state: $(state_of "$SID2")); see /tmp/runnerd.log"
SECOND_SECS=$(( $(date +%s) - SECOND_START ))

CID2=$(docker ps -q --filter "label=rainier.session=$SID2")
[ -n "$CID2" ] || fail "no container is labeled rainier.session=$SID2"
IMAGE2=$(docker inspect -f '{{.Config.Image}}' "$CID2")
# The image IS the proof that no setup was dispatched: controld resolves a
# session to the snapshot ref only when the cache is usable, and createSpec
# sends `setup` only when it did NOT resolve to the snapshot (internal/controld
# — resolveImage and createSpec are the same branch, read the other way). The
# container's own RAINIER_SETUP_B64 cannot answer this, for the reason the
# finding below spells out.
[ "$IMAGE2" = "$SNAP_REF" ] \
  || fail "the second session runs image \"$IMAGE2\", want the cached snapshot \"$SNAP_REF\" — it was dispatched with setup, not from the cache"
ok "second create dispatched NO setup: it booted $SNAP_REF (${SECOND_SECS}s vs ${FIRST_SECS}s for the first)"

# The strong form of the cache proof, from inside the second session: the
# /opt/rainier-env marker the setup script installed must be there (the commit
# carried it forward — this is what "cached" has to MEAN), and the /workspace
# marker must not (the volume is per-session, and its absence is also how a
# silent setup re-run would show itself).
ATTACH2_OUT=/tmp/rainier-e2e-env-attach2.txt
attach_probe "$SID2" \
  'cat /opt/rainier-env/rainier-setup-marker; test -f /workspace/setup-marker; echo "rerun-check:$?"' \
  "$ATTACH2_OUT" 'rerun-check:[01]' 60 \
  || { echo "--- attach output ---"; cat -v "$ATTACH2_OUT"; echo "---------------------"; \
       fail "the second session never answered the cache probe"; }
grep -q 'installed' "$ATTACH2_OUT" \
  || { echo "--- attach output ---"; cat -v "$ATTACH2_OUT"; echo "---------------------"; \
       fail "the cache-booted session has no /opt/rainier-env/rainier-setup-marker: the snapshot did not carry the setup's installs, so the cache holds nothing"; }
grep -q 'rerun-check:1' "$ATTACH2_OUT" \
  || fail "the cache-booted session found /workspace/setup-marker — it re-ran its setup script, which the cached image exists to skip"
ok "the cached image carries the setup's install, and the session did not re-run setup"

# Read into a variable first: `docker image inspect | grep -q` in an `if`
# condition turns an inspect FAILURE into the same exit status as "no match"
# under pipefail, which would report the reassuring answer for the wrong
# reason.
SNAP_ENV=$(docker image inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$SNAP_REF") \
  || fail "docker image inspect $SNAP_REF: the cached image is not on this runner"
! printf '%s\n' "$SNAP_ENV" | grep -q "^$SECRET_NAME=$SECRET_VALUE\$" \
  || fail "the cached image's config carries $SECRET_NAME's decrypted VALUE — readable by anyone with docker on this runner"
! printf '%s\n' "$SNAP_ENV" | grep -q '^RAINIER_SETUP_B64=.' \
  || fail "the cached image's config carries RAINIER_SETUP_B64, which makes every session booted from it re-run setup"
# The build session's own identity is per-session too, and the proxy vars are
# worse than stale: the driver embeds the session id in them as userinfo, which
# is how egressd identifies the caller.
! printf '%s\n' "$SNAP_ENV" | grep -q '^RAINIER_DIAL=.' \
  || fail "the cached image's config carries RAINIER_DIAL from the build session"
! printf '%s\n' "$SNAP_ENV" | grep -qE "^(RAINIER_SESSION|HTTPS?_PROXY|https?_proxy)=." \
  || fail "the cached image's config carries the build session's identity or its egress proxy URLs (which embed that session's id)"
ok "the cached image's config carries no secret value, no setup script, and nothing naming the build session"

# --- teardown, which is also the env-delete guard (design §5).
if ./bin/rainier env rm "$ENV_NAME" >/tmp/rainier-e2e-envrm.txt 2>&1; then
  fail "env rm removed $ENV_NAME while $SID1 and $SID2 still reference it"
fi
grep -q conflict /tmp/rainier-e2e-envrm.txt \
  || fail "env rm while referenced said \"$(cat /tmp/rainier-e2e-envrm.txt)\", want a conflict"
ok "env rm is refused while live sessions reference the environment"

./bin/rainier rm "$SID1" >/dev/null
./bin/rainier rm "$SID2" >/dev/null
waitfor '[ -z "$(state_of "$SID1")" ] && [ -z "$(state_of "$SID2")" ]' 60 "both env sessions to go" \
  || fail "the environment's sessions are still listed after rm"
./bin/rainier env rm "$ENV_NAME" | grep -q removed || fail "env rm did not report removal"
./bin/rainier secret rm "$SECRET_NAME" >/dev/null || fail "secret rm"
rm -f "$SETUP_FILE"
ok "environment and secret deleted once nothing referenced them"

# --- capability negotiation, both directions (D18/D19). The fleet's one
# runner announced "$FLEET_CAPABILITY" on its dial, controld accepted it and
# persisted it beside the two capabilities it spells for the runner's own
# name, and the scheduler matches an environment's requirements against that
# set. Two environments prove both answers: one requiring what the fleet has,
# whose session lands on that runner, and one requiring what nothing
# advertises, whose session stays queued and SAYS SO.
CAP_ENV="e2e-cap-$$"
NOCAP_ENV="e2e-nocap-$$"
MISSING_CAPABILITY=e2e.none

CAP_ENV_ID=$(./bin/rainier env create "$CAP_ENV" \
  --image rainier-session:latest --capability "$FLEET_CAPABILITY")
case "$CAP_ENV_ID" in
  env_*) ok "created environment $CAP_ENV requiring $FLEET_CAPABILITY ($CAP_ENV_ID)" ;;
  *) fail "env create --capability printed \"$CAP_ENV_ID\", want an env_ id" ;;
esac
./bin/rainier env show "$CAP_ENV" > /tmp/rainier-e2e-cap-env.json
grep -q "\"$FLEET_CAPABILITY\"" /tmp/rainier-e2e-cap-env.json \
  || fail "env show does not carry capabilities: $(cat /tmp/rainier-e2e-cap-env.json)"

CAP_SID=$(./bin/rainier new --detach --name "$CAP_ENV-session" --env "$CAP_ENV")
case "$CAP_SID" in sess_*) ;; *) fail "new --env $CAP_ENV printed \"$CAP_SID\"" ;; esac
waitfor '[ "$(state_of "$CAP_SID")" = running ]' 120 "the capability-matched session" \
  || fail "$CAP_SID never reached running (state: $(state_of "$CAP_SID")); the runner advertises $FLEET_CAPABILITY — see $CONTROLD_LOG"
CAP_RUNNER=$(./bin/rainier ls | cell "$CAP_SID" RUNNER REACHABLE)
[ "$CAP_RUNNER" = "$RUNNER_NAME" ] \
  || fail "$CAP_SID is running on \"$CAP_RUNNER\", want $RUNNER_NAME (the runner that announced $FLEET_CAPABILITY)"
ok "a session from an environment requiring $FLEET_CAPABILITY landed on $RUNNER_NAME"

./bin/rainier env create "$NOCAP_ENV" \
  --image rainier-session:latest --capability "$MISSING_CAPABILITY" >/dev/null \
  || fail "env create --capability $MISSING_CAPABILITY"
NOCAP_SID=$(./bin/rainier new --detach --name "$NOCAP_ENV-session" --env "$NOCAP_ENV")
case "$NOCAP_SID" in sess_*) ;; *) fail "new --env $NOCAP_ENV printed \"$NOCAP_SID\"" ;; esac
# The STATE column carries the queue reason in parentheses (sessionStateCell),
# which is where a human meets it.
nocap_state() { ./bin/rainier ls | cell "$NOCAP_SID" STATE RUNNER; }
waitfor '[ "$(nocap_state)" = "queued (waiting for a runner with capability '"$MISSING_CAPABILITY"')" ]' \
  30 "the queue reason naming $MISSING_CAPABILITY" \
  || fail "$NOCAP_SID reads \"$(nocap_state)\", want queued naming the missing capability $MISSING_CAPABILITY"
ok "a session requiring $MISSING_CAPABILITY stays queued and names the capability nothing advertises"

./bin/rainier rm "$CAP_SID" >/dev/null
./bin/rainier rm "$NOCAP_SID" >/dev/null
waitfor '[ -z "$(state_of "$CAP_SID")" ] && [ -z "$(state_of "$NOCAP_SID")" ]' 60 "both capability sessions to go" \
  || fail "the capability scene's sessions are still listed after rm"
./bin/rainier env rm "$CAP_ENV" | grep -q removed || fail "env rm $CAP_ENV did not report removal"
./bin/rainier env rm "$NOCAP_ENV" | grep -q removed || fail "env rm $NOCAP_ENV did not report removal"
rm -f /tmp/rainier-e2e-cap-env.json
ok "the capability scene's environments and sessions are cleaned up"

# ---------------------------------------------------------------------------
step "agent credential home (login → custody → a fresh home → refresh → snapshot → logout)"
# ---------------------------------------------------------------------------
# OSS plan #17: a coding agent's login, made once, reaches every later session.
# The e2e's controld offers the synthetic "test" provider (RAINIER_E2E_TEST_AGENT
# on both the server and the CLI): its "login" is a local write of the literal
# credential_example into the provider's own file, so the whole path — the
# mount, sessiond's fetch and put, custody, the downward revoke — is proven with
# no real account and no real credential anywhere near this script.
#
# The proof that the SECOND session got its file from custody rather than from
# the runner's volume is the volume's removal in between: this fleet has one
# runner, so "boots on the other runner" is replaced by "boots with no home
# volume at all", which is the same fact (the file can only have come down the
# RPC) and a stricter one.
export RAINIER_E2E_TEST_AGENT=1
AGENT_LOGIN_OUT=/tmp/rainier-e2e-agent-login.txt
AGENT_PROBE_OUT=/tmp/rainier-e2e-agent-probe.txt
AGENT_CRED_PATH=/rainier/agents/test/credential.json

# agent_row FIELD — one cell of `rainier agent ls` for the test provider.
agent_row() { ./bin/rainier agent ls | cell test "$1" ""; }
agent_status()  { ./bin/rainier agent ls | cell test STATUS SINCE; }
agent_version() { ./bin/rainier agent ls | cell test VERSION WORKSPACES; }

[ "$(agent_status)" = "none" ] || fail "before any login, agent ls shows test as \"$(agent_status)\", want none"
ok "agent ls lists the test provider with status none before any login"

# --- the login. `agent login` creates a session running the provider's own
# login command and attaches to it, the way a person would; the synthetic
# command writes the file and exits at once, and sessiond's sync puts the set
# within its two-second tick. The attach is driven through a FIFO like every
# attach in this script, so the detach key goes out when custody has actually
# moved and not after a fixed sleep; the CLI then removes the login session and
# reports what it found.
AGENT_FIFO=/tmp/rainier-e2e-agent.fifo
rm -f "$AGENT_FIFO"; mkfifo "$AGENT_FIFO"
: > "$AGENT_LOGIN_OUT"
./bin/rainier agent login test --env "$ENV_NAME" <"$AGENT_FIFO" >"$AGENT_LOGIN_OUT" 2>&1 &
AGENT_LOGIN_JOB=$!
exec 7>"$AGENT_FIFO"
waitfor '[ "$(agent_version)" = "1" ]' 90 "custody to reach version 1 after the synthetic login" \
  || { printf '\035' >&7; exec 7>&-; wait "$AGENT_LOGIN_JOB" 2>/dev/null || true; cat "$AGENT_LOGIN_OUT" >&2; fail "custody never reached version 1; agent ls: $(./bin/rainier agent ls | tr '\n' '|')"; }
printf '\035' >&7                # Ctrl-]: detach; the CLI removes the session and reports
exec 7>&-
wait "$AGENT_LOGIN_JOB" 2>/dev/null || true
rm -f "$AGENT_FIFO"
grep -q "logged in as of" "$AGENT_LOGIN_OUT" \
  || { cat "$AGENT_LOGIN_OUT" >&2; fail "agent login did not report a completed login"; }
! grep -q credential_example "$AGENT_LOGIN_OUT" || fail "agent login echoed the credential"
[ "$(agent_status)" = "logged_in" ] || fail "after the login, agent ls shows \"$(agent_status)\", want logged_in"
ok "agent login test completed: custody at v1, the CLI reported it, nothing echoed the credential"
./bin/rainier ls | grep -q "agent-login-test" && fail "the login session was not removed after the login"
ok "the login session was removed once the login was reported"

# --- the home volume goes away, so the next session can only get the file
# from custody. The volume's name is a hash the control plane minted (never an
# account id); this fleet has exactly one person and one workspace, so there is
# exactly one.
AGENT_VOL=$(docker volume ls -q | grep '^rainier-agents-' | head -1)
[ -n "$AGENT_VOL" ] || fail "no rainier-agents-* volume exists after a login session ran"
docker volume rm "$AGENT_VOL" >/dev/null || fail "could not remove the home volume $AGENT_VOL (still mounted?)"
ok "removed the home volume $AGENT_VOL so the next boot has nothing local to read"

# --- the second session: a plain session from the same environment. Its agents
# stage fetches the set at boot and writes it into a freshly prepared home.
AGENT_SID=$(./bin/rainier new --detach --name "$ENV_NAME-agent" --env "$ENV_NAME")
case "$AGENT_SID" in sess_*) ;; *) fail "new --env printed \"$AGENT_SID\", want a sess_ id" ;; esac
waitfor '[ "$(state_of "$AGENT_SID")" = running ]' 120 "the second agent session" \
  || fail "$AGENT_SID never reached running (state: $(state_of "$AGENT_SID"))"
attach_probe "$AGENT_SID" "cat $AGENT_CRED_PATH; echo; echo cred-probe-done" "$AGENT_PROBE_OUT" 'cred-probe-done' 60 \
  || { cat -v "$AGENT_PROBE_OUT" >&2; fail "the second session never answered the credential probe"; }
grep -q '^credential_example' "$AGENT_PROBE_OUT" \
  || { cat -v "$AGENT_PROBE_OUT" >&2; fail "the second session's home does not hold the credential custody handed down"; }
ok "a session booted after the volume was gone holds the credential: it came from custody, not the runner"
docker volume ls -q | grep -q '^rainier-agents-' || fail "the second session did not recreate a home volume"
attach_probe "$AGENT_SID" "stat -c %a $AGENT_CRED_PATH; echo mode-probe-done" "$AGENT_PROBE_OUT" 'mode-probe-done' 30 \
  || fail "the mode probe never answered"
grep -q '^600' "$AGENT_PROBE_OUT" || { cat -v "$AGENT_PROBE_OUT" >&2; fail "the credential file is not mode 0600"; }
ok "the fetched credential file is 0600"

# --- a refresh: the agent (here, the shell) rewrites its credential; the sync
# notices within its tick and custody moves to v2.
attach_probe "$AGENT_SID" "printf credential_example2 > $AGENT_CRED_PATH; echo rewrite-done" "$AGENT_PROBE_OUT" 'rewrite-done' 30 \
  || fail "the rewrite probe never answered"
waitfor '[ "$(agent_version)" = "2" ]' 15 "custody to reach version 2 after the rewrite" \
  || fail "custody stayed at v$(agent_version) after the credential was rewritten"
ok "a rewritten credential reached custody as v2 within the sync interval"

# --- the snapshot: the environment's cached image was committed from a session
# that had the home mounted, and docker commit excludes volumes, so nothing
# under the mount point can be in it. SNAP_REF is the environments phase's.
docker run --rm --entrypoint sh "$SNAP_REF" -c "test ! -e $AGENT_CRED_PATH && test -z \"\$(ls -A /rainier/agents 2>/dev/null)\"" \
  || fail "the environment snapshot $SNAP_REF carries something under /rainier/agents"
ok "the environment snapshot carries nothing under the agent home"

# --- logout: custody is destroyed and the live session's copy is removed by
# the downward revoke within the sync interval.
./bin/rainier agent logout test --yes >"$AGENT_LOGIN_OUT" 2>&1 || { cat "$AGENT_LOGIN_OUT" >&2; fail "agent logout failed"; }
grep -q "logged out of test" "$AGENT_LOGIN_OUT" || fail "agent logout printed \"$(cat "$AGENT_LOGIN_OUT")\""
[ "$(agent_status)" = "none" ] || fail "after logout, agent ls shows \"$(agent_status)\", want none"
waitfor "attach_probe '$AGENT_SID' 'test -e $AGENT_CRED_PATH && echo still-present || echo revoke-probe-absent' '$AGENT_PROBE_OUT' 'revoke-probe-(absent|still)' 20 && grep -q revoke-probe-absent '$AGENT_PROBE_OUT'" 15 "the live session to drop the credential" \
  || { cat -v "$AGENT_PROBE_OUT" >&2; fail "the live session still holds the credential after logout"; }
ok "agent logout: custody gone, and the live session's copy removed by the downward revoke"

# --- hygiene: no credential byte in any log the stack wrote.
for logf in "$CONTROLD_LOG" /tmp/runnerd.log /tmp/egressd.log; do
  [ -f "$logf" ] || continue
  ! grep -q 'credential_example' "$logf" || fail "$logf contains the credential"
done
ok "no log line holds the credential"

./bin/rainier rm "$AGENT_SID" >/dev/null 2>&1 || true
unset RAINIER_E2E_TEST_AGENT

# ---------------------------------------------------------------------------
step "github rehearsal (real clone, commit and push against a throwaway repo)"
# ---------------------------------------------------------------------------
# Plan 5's whole delivery path against real GitHub: an environment with a
# github connector and an init hook, a session that clones at boot onto its own
# branch, a commit and a push authenticated by the in-sandbox credential helper
# (which mints from controld's vault, per operation, and writes the token
# nowhere), the attribution GitHub itself records for that commit, the diff
# endpoint, `rainier creds`, and a push/pull round trip.
#
# Everything in it happens on a repository this script creates and deletes.
# That is why the gate below is stricter than "is gh installed": we refuse to
# CREATE a repository we would not be able to REMOVE, because the cost of
# getting that wrong is a private repo left on somebody's account forever, and
# no later run of this script would ever find it.
GH_PHASE=1
GH_SKIP=""
GH_ACCOUNT=""
GH_ID=""
if [ "${SKIP_GITHUB:-0}" = "1" ]; then
  GH_PHASE=0; GH_SKIP="SKIP_GITHUB=1"
elif ! command -v gh >/dev/null 2>&1; then
  GH_PHASE=0; GH_SKIP="the \`gh\` CLI is not installed — this phase creates and deletes its own repository through it"
else
  # The token's own scopes, read from GitHub rather than from `gh auth status`,
  # whose wording is not a contract. gh honors GH_TOKEN here exactly as it does
  # for the login above, so this is the scope set of whichever token this run
  # is actually using.
  GH_SCOPES=$(gh api --include user 2>/dev/null | tr -d '\r' | sed -n 's/^[Xx]-[Oo][Aa]uth-[Ss]copes:[[:space:]]*//p' | tr -d ' ' || true)
  case ",$GH_SCOPES," in
    *,repo,*) ;;
    *) GH_PHASE=0; GH_SKIP="the GitHub token has scopes [$GH_SCOPES] and needs \`repo\` to clone and push a private repository — run: gh auth refresh -h github.com -s repo" ;;
  esac
  if [ "$GH_PHASE" = "1" ]; then
    case ",$GH_SCOPES," in
      *,delete_repo,*) ;;
      *) GH_PHASE=0; GH_SKIP="the GitHub token has scopes [$GH_SCOPES] and cannot DELETE a repository, so this phase will not create one — run: gh auth refresh -h github.com -s delete_repo" ;;
    esac
  fi
  if [ "$GH_PHASE" = "1" ]; then
    GH_ACCOUNT=$(gh api user --jq .login 2>/dev/null || true)
    GH_ID=$(gh api user --jq .id 2>/dev/null || true)
    if [ -z "$GH_ACCOUNT" ] || [ -z "$GH_ID" ]; then
      GH_PHASE=0; GH_SKIP="could not read the authenticated GitHub account from \`gh api user\`"
    fi
  fi
fi

if [ "$GH_PHASE" = "0" ]; then
  echo "SKIPPED: $GH_SKIP"
  echo "(nothing was created on GitHub; every other phase of this run still applies)"
else

SCRATCH_REPO="rainier-e2e-scratch-$$"
GH_ENV_NAME="e2e-gh-$$"
GH_SESSION_NAME="e2e-gh-$$"
GH_BRANCH="rainier/$GH_SESSION_NAME"
GH_INIT_FILE=/tmp/rainier-e2e-init.sh
# The GitHub noreply address controld derives for this account, and therefore
# the author every in-session commit must carry (design §1 criterion 2). Built
# from what `gh api user` just said, never hardcoded.
GH_NOREPLY="$GH_ID+$GH_ACCOUNT@users.noreply.github.com"

# --add-readme, not a bare create: the clone stage asks for a base branch, and
# a repository with no commits has none — an empty repo would fail the clone
# for a reason that has nothing to do with what this phase is testing.
gh repo create "$SCRATCH_REPO" --private --add-readme \
  --description "throwaway; created and deleted by rainier scripts/e2e-fleet.sh" >/dev/null \
  || setup_error "gh repo create $SCRATCH_REPO failed"
SCRATCH_SLUG="$GH_ACCOUNT/$SCRATCH_REPO"   # armed for cleanup from here on
# The base branch is READ, not assumed: a github connector defaults to "main",
# but GitHub's default-branch name is an account setting, and a rehearsal that
# hardcoded it would fail on somebody's account for a reason that has nothing
# to do with rainier. Naming it explicitly also exercises the connector's
# base_branch field rather than only its default.
GH_BASE=$(gh api "repos/$SCRATCH_SLUG" --jq .default_branch 2>/dev/null || true)
[ -n "$GH_BASE" ] || fail "could not read the default branch of $SCRATCH_SLUG"
ok "created the throwaway private repository $SCRATCH_SLUG on $GH_BASE (deleted at teardown, including on failure)"

# The init hook. It runs on EVERY boot and AFTER the clone stage, which is what
# lets it do something no setup script could: read the repository's own git
# history. A marker that carries a commit hash out of the working tree is proof
# of the ordering, not just of the hook.
#
# Its LAST act is to publish that marker inside a directory, which is what the
# readiness gate below waits on: a directory is something the host can ask for
# over `rainier pull` without attaching, and copying it last means its
# existence says "the init stage finished", not "the init stage started".
cat > "$GH_INIT_FILE" <<EOF
#!/bin/sh
set -eu
git -C /workspace/$SCRATCH_REPO log -1 --format='init-sees-commit=%h' > /workspace/init-marker
git -C /workspace/$SCRATCH_REPO rev-parse --abbrev-ref HEAD >> /workspace/init-marker
echo init-ran >> /workspace/init-marker
mkdir -p /workspace/init-done
cp /workspace/init-marker /workspace/init-done/marker
EOF

GH_ENV_ID=$(./bin/rainier env create "$GH_ENV_NAME" \
  --image rainier-session:latest --init-file "$GH_INIT_FILE" \
  --connector-json "{\"type\":\"github\",\"repo\":\"$SCRATCH_SLUG\",\"base_branch\":\"$GH_BASE\"}")
case "$GH_ENV_ID" in
  env_*) ok "created environment $GH_ENV_NAME with a github connector and an init hook" ;;
  *) fail "env create printed \"$GH_ENV_ID\", want an env_ id" ;;
esac

GH_SID=$(./bin/rainier new --detach --name "$GH_SESSION_NAME" --env "$GH_ENV_NAME")
case "$GH_SID" in sess_*) ;; *) fail "new --env printed \"$GH_SID\", want a sess_ id" ;; esac

# The bearer the CLI just wrote, so a failure can be explained with the
# session's own error column — `rainier ls` has no room for it.
BEARER=$(sed -n 's/.*"token"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$RAINIER_CONFIG")
session_error() {
  curl -sf -H "Authorization: Bearer $BEARER" "$CONTROLD_HTTP/v0/sessions/$1" \
    | sed -n 's/.*"error":[[:space:]]*"\([^"]*\)".*/\1/p'
}

# "running" fires when sessiond REGISTERS, which is before the boot chain has
# run — the clone and the init hook both happen ahead of the agent. So the wait
# is for the chain to have FINISHED, or for the session to have failed one of
# its stages.
#
# Finished means INIT, not clone, and the difference is the next assertion:
# it reads /workspace/init-marker, which the init stage writes. This gate used
# to be `rainier diff`, which answers as soon as the CLONE is on disk — a stage
# too early. It never went wrong only because the agent is exec'd after init,
# so the attach probe could not get a shell prompt any sooner and the typed
# line sat in the tty buffer meanwhile; that is an accident of the boot chain,
# not a guarantee, and anything that read stdin during init would swallow the
# command and time the probe out instead.
#
# A pull of the directory the hook publishes last is the version with no race
# in it, and it needs no attach.
GH_INIT_PULL=/tmp/rainier-e2e-gh-init
gh_boot_settled() {
  [ "$(state_all_of "$GH_SID")" = failed ] && return 0
  rm -rf "$GH_INIT_PULL"
  ./bin/rainier pull "$GH_SID:/workspace/init-done" "$GH_INIT_PULL" >/dev/null 2>&1
}
waitfor '[ "$(state_of "$GH_SID")" = running ] || [ "$(state_all_of "$GH_SID")" = failed ]' 120 "the session to be placed" \
  || fail "$GH_SID never left queued/creating; see $CONTROLD_LOG and /tmp/runnerd.log"
waitfor gh_boot_settled 300 "the boot chain (clone, then init)" \
  || fail "$GH_SID never finished its boot chain (state: $(state_all_of "$GH_SID")); see /tmp/runnerd.log"
if [ "$(state_all_of "$GH_SID")" = failed ]; then
  fail "the session failed its boot chain: $(session_error "$GH_SID")"
fi
grep -q 'init-sees-commit=' "$GH_INIT_PULL/marker" \
  || fail "the init marker pulled out of the session does not carry a commit hash: $(cat "$GH_INIT_PULL/marker" 2>&1)"
ok "session $GH_SID booted with the repository cloned and the init stage finished"

# --- what the clone and the init hook actually did, from inside the session.
GH_ATTACH1=/tmp/rainier-e2e-gh-attach1.txt
attach_probe "$GH_SID" \
  "cat /workspace/init-marker; git -C /workspace/$SCRATCH_REPO rev-parse --abbrev-ref HEAD" \
  "$GH_ATTACH1" 'init-ran' 120 \
  || { echo "--- attach output ---"; cat -v "$GH_ATTACH1"; echo "---------------------"; \
       fail "the session never answered the clone/init probe"; }
grep -q 'init-sees-commit=' "$GH_ATTACH1" \
  || fail "the init hook could not read the repository's history: it did not run after the clone"
grep -q "$GH_BRANCH" "$GH_ATTACH1" \
  || { echo "--- attach output ---"; cat -v "$GH_ATTACH1"; echo "---------------------"; \
       fail "the clone is not on branch $GH_BRANCH"; }
ok "cloned to /workspace/$SCRATCH_REPO on $GH_BRANCH, and the init hook ran after the clone (it read the git log)"

# --- the commit and the push. This is the credential path end to end: git asks
# the helper, the helper asks sessiond, sessiond asks controld, controld mints
# from the vault, and the token exists only on the helper's stdout.
GH_ATTACH2=/tmp/rainier-e2e-gh-attach2.txt
attach_probe "$GH_SID" \
  "cd /workspace/$SCRATCH_REPO && date > agent-note.txt && git add -A && git commit -q -m 'rainier rehearsal note' && git push -q; echo \"pushed:\$?\"; git log -1 --format='author=%an <%ae>'" \
  "$GH_ATTACH2" 'pushed:[0-9]' 180 \
  || { echo "--- attach output ---"; cat -v "$GH_ATTACH2"; echo "---------------------"; \
       fail "the session never answered the commit/push probe"; }
grep -q 'pushed:0' "$GH_ATTACH2" \
  || { echo "--- attach output ---"; cat -v "$GH_ATTACH2"; echo "---------------------"; \
       fail "the in-session git push failed; the credential helper could not authenticate"; }
grep -q "author=$GH_ACCOUNT <$GH_NOREPLY>" "$GH_ATTACH2" \
  || fail "the commit's local author is not \"$GH_ACCOUNT <$GH_NOREPLY>\": $(grep -o 'author=.*' "$GH_ATTACH2" | head -1)"
ok "committed and pushed from inside the session over the minted credential"

# The authoritative half of criterion 2: what GITHUB recorded for that commit,
# read back from the API rather than from the container that made it.
GH_REMOTE_AUTHOR=$(gh api "repos/$SCRATCH_SLUG/commits?sha=$GH_BRANCH&per_page=1" \
  --jq '.[0].commit.author | .name + " <" + .email + ">"' 2>/dev/null || true)
[ "$GH_REMOTE_AUTHOR" = "$GH_ACCOUNT <$GH_NOREPLY>" ] \
  || fail "GitHub records the pushed commit's author as \"$GH_REMOTE_AUTHOR\", want \"$GH_ACCOUNT <$GH_NOREPLY>\""
ok "GitHub attributes the pushed commit to $GH_ACCOUNT <$GH_NOREPLY> on $GH_BRANCH"

# --- and the token is nowhere. The scan is for token SHAPES, never for the
# value: putting the real token into a command line would write it into this
# session's scrollback and into a file under /tmp, which is exactly the leak
# the design exists to prevent.
#
# `find -type f | xargs grep`, not `grep -r`, for the workspace sweep. Two
# reasons, and the first is that `grep -r /workspace` walks
# /workspace/.rainier/agent.sock and open()s it: the count stays 0 because -l
# prints nothing for a file it could not read, which is the right answer
# arrived at by luck. The second is coverage — the alternative fix,
# --exclude-dir=.rainier, is a GNU option BusyBox grep does not have (this
# image is alpine, so `grep --exclude-dir` exits 2 with a usage message that
# 2>/dev/null would hide, turning the whole probe into a silent 0), and it
# would skip the workspace gitconfig, which is precisely the file a leaked
# token would be written into.
GH_ATTACH3=/tmp/rainier-e2e-gh-attach3.txt
attach_probe "$GH_SID" \
  "printf 'cfg-hits=%s ws-hits=%s env-hits=%s\n' \"\$(grep -r -l -E 'gh[pousr]_[A-Za-z0-9]|github_pat_' /workspace/$SCRATCH_REPO/.git 2>/dev/null | wc -l | tr -d ' ')\" \"\$(find /workspace -type f | xargs grep -l -E 'gh[pousr]_[A-Za-z0-9]|github_pat_' 2>/dev/null | wc -l | tr -d ' ')\" \"\$(env | grep -c -E 'gh[pousr]_[A-Za-z0-9]|github_pat_' || true)\"" \
  "$GH_ATTACH3" 'cfg-hits=[0-9]+ ws-hits=[0-9]+ env-hits=[0-9]+' 60 \
  || { echo "--- attach output ---"; cat -v "$GH_ATTACH3"; echo "---------------------"; \
       fail "the session never answered the token-hygiene probe"; }
grep -q 'cfg-hits=0 ws-hits=0 env-hits=0' "$GH_ATTACH3" \
  || fail "something token-shaped is on disk or in the environment of a session that just pushed: $(grep -o 'cfg-hits=.*' "$GH_ATTACH3" | head -1)"
ok "after a successful push, nothing token-shaped is in .git, anywhere under /workspace, or in the process environment"

# --- the diff endpoint, against the commit that was just made.
./bin/rainier diff "$GH_SID" > /tmp/rainier-e2e-gh-diff.txt 2>&1 \
  || { cat /tmp/rainier-e2e-gh-diff.txt >&2; fail "rainier diff $GH_SID"; }
grep -q "$SCRATCH_SLUG  $GH_BRANCH vs origin/$GH_BASE" /tmp/rainier-e2e-gh-diff.txt \
  || fail "diff did not name the repository and both branches: $(head -1 /tmp/rainier-e2e-gh-diff.txt)"
grep -q 'agent-note.txt' /tmp/rainier-e2e-gh-diff.txt \
  || { cat /tmp/rainier-e2e-gh-diff.txt; fail "diff does not show the file the session added"; }
ok "rainier diff reports the session's change against the merge-base with origin/$GH_BASE"

# --- `rainier creds`: the credential this whole phase used, still valid.
./bin/rainier creds > /tmp/rainier-e2e-gh-creds.txt
GH_CRED_STATUS=$(cell github STATUS SCOPES < /tmp/rainier-e2e-gh-creds.txt)
[ "$GH_CRED_STATUS" = "valid" ] \
  || fail "rainier creds reports the github credential as \"$GH_CRED_STATUS\", want valid after a successful push"
! grep -qE 'gh[pousr]_[A-Za-z0-9]|github_pat_' /tmp/rainier-e2e-gh-creds.txt \
  || fail "rainier creds printed something token-shaped; the vault is write-only at that API"
ok "rainier creds shows github valid, with no credential material in the table"

# --- push/pull a directory, laptop↔session.
PP_SRC=/tmp/rainier-e2e-xfer-src
PP_DST=/tmp/rainier-e2e-xfer-dst
rm -rf "$PP_SRC" "$PP_DST"
mkdir -p "$PP_SRC/nested/deeper" "$PP_SRC/empty"
printf '%s\n' "$MARK" > "$PP_SRC/note.txt"
head -c 65536 /dev/urandom > "$PP_SRC/nested/blob.bin"
printf '#!/bin/sh\nexit 0\n' > "$PP_SRC/nested/deeper/run.sh"
chmod 755 "$PP_SRC/nested/deeper/run.sh"

./bin/rainier push "$PP_SRC" "$GH_SID:/workspace/incoming" >/dev/null \
  || fail "rainier push $PP_SRC $GH_SID:/workspace/incoming"
./bin/rainier pull "$GH_SID:/workspace/incoming" "$PP_DST" >/dev/null \
  || fail "rainier pull $GH_SID:/workspace/incoming $PP_DST"
diff -r "$PP_SRC" "$PP_DST" >/tmp/rainier-e2e-xfer-diff.txt 2>&1 \
  || { cat /tmp/rainier-e2e-xfer-diff.txt >&2; fail "the directory did not survive the push/pull round trip"; }
[ -x "$PP_DST/nested/deeper/run.sh" ] \
  || fail "run.sh came back without its executable bit"
ok "push/pull round-tripped a directory (nested, binary, empty dir, executable bit) laptop↔session"

# --- the failure half: design criterion 3, which is the one the vault exists
# for and the only one every other assertion in this phase skips.
#
# "A revoked or expired GitHub credential surfaces as a clear, NAMED ACTION
# within ONE FAILED OPERATION." Both halves are asserted below, and the second
# is the one with teeth: a refused mint makes the credential helper print
# controld's sentence and exit 1, and git's own answer to a helper that
# produced no credential is to fall through and prompt — on the session's PTY,
# where it blocks. That failure looks like the clone stage running for its
# whole 600s-per-repo bound and then reporting "clone timed out", with the
# named action nowhere in the tail. So the wait below is deliberately far
# shorter than that bound: a slow failure here is a FAILED assertion, not a
# slow test.
#
# The credential is flipped in the database rather than revoked on GitHub.
# Revoking the operator's own token is not this script's to do — and `status`
# is exactly what a revocation observed in the wild sets (the
# credential_rejected path), so the session under test cannot tell the
# difference.
psql_e2e() { docker exec "$PG_CONTAINER" psql -U postgres -d "$PG_DB" -qtAc "$1"; }
cred_status_row() { psql_e2e "SELECT status FROM credentials WHERE provider='github'" | tr -d ' \r'; }

psql_e2e "UPDATE credentials SET status='needs_refresh' WHERE provider='github'" >/dev/null \
  || setup_error "could not flip the stored github credential to needs_refresh"
[ "$(cred_status_row)" = "needs_refresh" ] \
  || setup_error "the stored credential is \"$(cred_status_row)\" after the flip; the rest of this scene would prove nothing"
./bin/rainier creds > /tmp/rainier-e2e-gh-creds2.txt
GH_CRED_STALE=$(cell github STATUS SCOPES < /tmp/rainier-e2e-gh-creds2.txt)
[ "$GH_CRED_STALE" = "needs_refresh" ] \
  || fail "rainier creds reports \"$GH_CRED_STALE\" for a credential the vault has flipped, want needs_refresh"

# The create gate lets a STALE credential through on purpose (a missing one is
# a 409; a stale one is not). The session is where the user finds out.
GH_SID2=$(./bin/rainier new --detach --name "$GH_SESSION_NAME-stale" --env "$GH_ENV_NAME")
case "$GH_SID2" in
  sess_*) ;;
  *) fail "new --env with a stale credential printed \"$GH_SID2\", want a sess_ id — a stale credential must not be a create-time refusal" ;;
esac
GH_STALE_START=$(date +%s)
waitfor '[ "$(state_all_of "$GH_SID2")" = failed ]' 120 "the stale-credential session to fail" \
  || fail "$GH_SID2 is \"$(state_all_of "$GH_SID2")\" 120s after a refused mint. A refused mint must fail the clone in seconds; if git is instead sitting on a terminal prompt it burns the whole 600s-per-repo clone bound and reports a timeout with no named action (check that the boot chain exports GIT_TERMINAL_PROMPT=0)."
GH_STALE_SECS=$(( $(date +%s) - GH_STALE_START ))
# Grepped out of the raw body rather than the extracted error: the error
# column carries a 2KB tail of the session's own output, quotes and newlines
# included, and session_error's sed stops at the first of either.
GH_STALE_BODY=$(curl -sf -H "Authorization: Bearer $BEARER" "$CONTROLD_HTTP/v0/sessions/$GH_SID2")
case "$GH_STALE_BODY" in
  *"rainier login --refresh github"*) ;;
  *) fail "the failed session's error does not name the action to run — the sentence must survive every hop verbatim: $(session_error "$GH_SID2")" ;;
esac
ok "a session created against a stale credential failed in ${GH_STALE_SECS}s, naming \`rainier login --refresh github\`"

# rm of a FAILED session is not the terminal no-op (canceled, dead, and
# destroyed are): the delete carves failed out of that case and destroys it —
# the container is torn down, the volume reclaimed — exactly as api_test pins
# ("a failed session still present on its runner is destroyed"). The verdict
# that must survive the rm is the error column: a user who removes a failed
# session must not lose the record of why it failed.
./bin/rainier rm "$GH_SID2" >/dev/null || fail "rm of the failed session $GH_SID2"
[ "$(state_all_of "$GH_SID2")" = destroyed ] \
  || fail "after rm, $GH_SID2 reads \"$(state_all_of "$GH_SID2")\" under ls --all, want destroyed"
case "$(session_error "$GH_SID2")" in
  *"rainier login --refresh github"*) ;;
  *) fail "after rm, the failed session's error no longer names the action — removing a failed session must not erase its verdict" ;;
esac

# Put the credential back the way this scene found it: a KEEP=1 stack, and
# anything after this phase, should see the fleet it started with.
psql_e2e "UPDATE credentials SET status='valid' WHERE provider='github'" >/dev/null \
  || setup_error "could not restore the github credential to valid"
[ "$(cred_status_row)" = "valid" ] || fail "the github credential was left as \"$(cred_status_row)\""

# --- teardown of this phase's own state. The repository goes in cleanup, so it
# is deleted whether or not everything above passed.
./bin/rainier rm "$GH_SID" >/dev/null
waitfor '[ -z "$(state_of "$GH_SID")" ]' 60 "the github session to go" \
  || fail "$GH_SID is still listed after rm"
./bin/rainier env rm "$GH_ENV_NAME" | grep -q removed || fail "env rm $GH_ENV_NAME"
rm -f "$GH_INIT_FILE"
rm -rf "$PP_SRC" "$PP_DST" "$GH_INIT_PULL"
ok "github rehearsal session and environment removed"

fi  # GH_PHASE

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
echo "e2e-fleet: ALL CHECKS PASSED (login, new, ls, attach, suspend, resume, rm, environments, agent credential home$([ "$GH_PHASE" = 1 ] && echo ", github rehearsal")$([ "$EGRESS_RC" = 0 ] && echo ", egress R4"))"
if [ "$GH_PHASE" = "0" ]; then
  echo "e2e-fleet: the github rehearsal was SKIPPED — $GH_SKIP"
fi
if [ "$FINDINGS" -gt 0 ]; then
  echo "e2e-fleet: $FINDINGS FINDING(S) recorded above — the flow works, the product has defects; see the acceptance table in docs/deploy-gce.md"
fi

#!/usr/bin/env bash
# scripts/e2e-fleet.sh — the local dress rehearsal for the GCE milestone
# (docs/deploy-gce.md). It brings up the whole Plan 3 stack on this machine —
# Postgres in docker, controld on the host, egressd + a DIAL-MODE runnerd via
# fleet-up.sh — and then drives the REAL `rainier` CLI against it end to end:
# login, new, ls, attach (non-tty, piped stdin), suspend, resume, rm, and
# Plan 4's environments (secret set from stdin → env create with a setup
# script → a first session that runs it → the snapshot controld commits → a
# second session that boots the cache). It finishes with
# scripts/egress-check.sh, the R4 acceptance.
#
# Where internal/e2e's Go suite fakes the container runtime to make the chaos
# scenes deterministic, this one fakes nothing: real docker containers, real
# Postgres, real websockets, the real CLI binary, one host.
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

# `rainier ls`: ID NAME ENV STATE RUNNER REACHABLE AGE — read by header
# offset, since ENV is empty on a scratch session (see `cell`).
state_of()      { ./bin/rainier ls | cell "$1" STATE RUNNER; }
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
session_state_all() { ./bin/rainier ls --all | cell "$SID" STATE RUNNER; }
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
ok "the cached image's config carries neither the secret's value nor the setup script"

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
echo "e2e-fleet: ALL CHECKS PASSED (login, new, ls, attach, suspend, resume, rm, environments$([ "$EGRESS_RC" = 0 ] && echo ", egress R4"))"
if [ "$FINDINGS" -gt 0 ]; then
  echo "e2e-fleet: $FINDINGS FINDING(S) recorded above — the flow works, the product has defects; see the acceptance table in docs/deploy-gce.md"
fi

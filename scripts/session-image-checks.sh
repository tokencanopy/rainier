#!/usr/bin/env bash
# Small assertion boundary shared by the image qualification scripts and their
# shell tests. The caller supplies probe (the execution boundary); reporting is
# here, because two qualification scripts annotating a pull request in two
# slightly different ways is how one of them quietly stops annotating at all.
# A caller that wants different reporting redefines ok and bad after sourcing.

PASS=0 FAIL=0

# In GitHub Actions the job log is the only record of a failed qualification,
# and it is not always reachable from wherever the fix is being made — a
# session's egress allowlist does not carry the Actions log host, for one.
# Emitting each failure as a workflow annotation puts the check's name and its
# detail on the pull request itself, where the check status already is. Inert
# outside Actions, and it reports; it never changes what passes.
# A workflow command's PROPERTIES are comma-separated and colon-terminated, so
# a title carrying either has to be escaped or it truncates the annotation —
# and most check names contain a comma. The message half only has to survive
# the newline.
wf_title() { printf '%s' "$1" | sed 's/%/%25/g; s/\r/%0D/g; s/:/%3A/g; s/,/%2C/g'; }
wf_body()  { printf '%s' "${1:-}" | cut -c1-2000 | sed 's/%/%25/g; s/\r/ /g' | awk '{printf "%s%%0A", $0}'; }
note() {
  [ "${GITHUB_ACTIONS:-}" = true ] || return 0
  printf '::notice title=%s::%s\n' "$(wf_title "$1")" "$(wf_body "$2")"
}
annotate() {
  [ "${GITHUB_ACTIONS:-}" = true ] || return 0
  printf '::error title=%s::%s\n' "$(wf_title "$1")" "$(wf_body "${2:-}")"
}
ok()  { PASS=$((PASS+1)); printf 'ok    %s\n' "$1"; }
bad() { FAIL=$((FAIL+1)); printf 'FAIL  %s\n' "$1"; [ $# -gt 1 ] && printf '      %s\n' "$2"; annotate "$1" "${2:-}"; return 0; }

check() {
  local name=$1 want=$2 prog=$3 runner=${4:-probe} match=${5:-contains} out status=0 matched=1
  out=$("$runner" "$prog") || status=$?
  case "$match" in
    contains) [ "${out#*"$want"}" != "$out" ] && matched=0 ;;
    exact) [ "$out" = "$want" ] && matched=0 ;;
  esac
  if [ "$status" -eq 0 ] && [ -n "$want" ] && [ "$matched" -eq 0 ]; then
    ok "$name"
  else
    bad "$name" "exit=$status; wanted \"$want\", got: $(printf '%s' "$out" | tr '\n' '|' | tail -c 1500)"
  fi
}

# Accept only the specified refusal, not a timeout, crash or unrelated error.
# This function is serialized into the isolated container probe.
expect_refusal() {
  local expected=$1 message=$2 out status=0
  shift 2
  out=$("$@" 2>&1) || status=$?
  printf '%s\n' "$out"
  [ "$expected" -ne 0 ] && [ "$status" -eq "$expected" ] &&
    [ -n "$message" ] && [ "${out#*"$message"}" != "$out" ]
}

# brokered_gh_probe is the image smoke's synthetic credential exchange. It
# requires successful completion from both the wrapped gh child and its local
# socket fixture before it prints the marker. The fixture knobs are used only
# by the host-side regression test; image qualification uses their zero-value.
brokered_gh_probe() {
  local socket=${RAINIER_SMOKE_AGENT_SOCKET:-/workspace/.rainier/agent.sock}
  local out gh_status=0 server_status=0 p
  rm -f "$socket"
  python3 -c 'import json,os,socket,sys; p=os.environ.get("RAINIER_SMOKE_AGENT_SOCKET", "/workspace/.rainier/agent.sock"); s=socket.socket(socket.AF_UNIX); s.settimeout(5); s.bind(p); s.listen(1); c,_=s.accept(); c.settimeout(5); req=json.loads(c.makefile("r").readline()); req == {"method":"mint_git_credential","payload":{}} or sys.exit(1); c.sendall(b"""{"ok":true,"payload":{"token":"synthetic-gh-token"}}\n"""); c.close(); s.close(); sys.exit(int(os.environ.get("RAINIER_SMOKE_FIXTURE_EXIT", "0")))' &
  p=$!
  for _ in $(seq 40); do test -S "$socket" && break; sleep 0.05; done
  if ! test -S "$socket"; then
    wait "$p" || server_status=$?
    return 1
  fi
  out=$(RAINIER_SESSION=sess_test GH_TOKEN=old GITHUB_TOKEN=older gh auth token) || gh_status=$?
  wait "$p" || server_status=$?
  [ "$gh_status" -eq 0 ] && [ "$server_status" -eq 0 ] && [ "$out" = synthetic-gh-token ] || return 1
  printf '%s\n' "$out"
}

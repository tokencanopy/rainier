#!/usr/bin/env bash
# Small assertion boundary shared by the container smoke and its shell tests.
# The caller supplies probe (the execution boundary), ok and bad (reporting).
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

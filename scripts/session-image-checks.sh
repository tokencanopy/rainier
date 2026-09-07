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
    bad "$name" "exit=$status; wanted \"$want\", got: $(printf '%s' "$out" | tr '\n' '|' | tail -c 400)"
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

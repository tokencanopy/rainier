#!/usr/bin/env bash
# Small assertion boundary shared by the container smoke and its shell tests.
# The caller supplies probe (the execution boundary), ok and bad (reporting).
check() {
  local name=$1 want=$2 prog=$3 out status=0
  out=$(probe "$prog") || status=$?
  if [ "$status" -eq 0 ] && [ -n "$want" ] && [ "${out#*"$want"}" != "$out" ]; then
    ok "$name"
  else
    bad "$name" "exit=$status; wanted \"$want\", got: $(printf '%s' "$out" | tr '\n' '|' | tail -c 400)"
  fi
}

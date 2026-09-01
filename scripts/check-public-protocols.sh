#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

for pkg in runner terminal workspace; do
  test -f "protocol/$pkg/messages.go"
  go list "./protocol/$pkg" >/dev/null
done

if rg -n 'github.com/tokencanopy/rainier/internal/(rwire|wire|xfer)' \
  cmd internal protocol -g '*.go'; then
  echo "active Go imports must use the public protocol packages" >&2
  exit 1
fi

for retired in internal/rwire internal/wire internal/xfer; do
  if [[ -e "$retired" ]]; then
    echo "retired internal protocol package is still present" >&2
    exit 1
  fi
done

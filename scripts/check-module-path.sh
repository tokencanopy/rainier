#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
canonical="github.com/tokencanopy/rainier"

actual="$(cd "$repo_root" && go list -m -f '{{.Path}}')"
if [[ "$actual" != "$canonical" ]]; then
  echo "module path must be $canonical" >&2
  exit 1
fi

if rg -n '"rainier/internal/' "$repo_root/cmd" "$repo_root/internal" -g '*.go'; then
  echo "active Go imports must use the canonical module path" >&2
  exit 1
fi

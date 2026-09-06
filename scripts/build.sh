#!/usr/bin/env bash
# Stamp the source tree, not an enclosing repository discovered by the Go tool.
set -euo pipefail
build_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$build_root"
build_revision=''
build_dirty=false
build_git_root="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [[ "$build_git_root" == "$build_root" ]] &&
   build_revision="$(git rev-parse --verify HEAD 2>/dev/null)" &&
   build_status="$(git status --porcelain --untracked-files=normal)"; then
  if [[ -n "$build_status" ]]; then build_dirty=true; fi
else
  # Source archives (including ones inside another repo) are not that repo's HEAD.
  build_revision=''
fi
CGO_ENABLED="${CGO_ENABLED:-0}" go build -buildvcs=false \
  -ldflags="-X main.sourceRevision=$build_revision -X main.sourceDirty=$build_dirty" "$@"

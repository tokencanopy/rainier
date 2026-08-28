#!/usr/bin/env bash
set -euo pipefail

command -v docker >/dev/null || export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"
command -v docker >/dev/null || { echo "docker CLI not found" >&2; exit 1; }

docker compose -f docker-compose.fleet.yml down

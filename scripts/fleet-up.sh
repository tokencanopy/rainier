#!/usr/bin/env bash
set -euo pipefail

command -v docker >/dev/null || export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"
command -v docker >/dev/null || { echo "docker CLI not found" >&2; exit 1; }

docker build -q -t rainier-sessiond .
docker compose -f docker-compose.fleet.yml up -d
echo "Fleet up. Attach from any terminal (one per cmux pane):"
for p in 7071 7072 7073 7074 7075; do echo "  ./bin/rattach --url ws://127.0.0.1:$p"; done

#!/usr/bin/env bash
# scripts/session-image-smoke.sh — does the session image actually work?
#
# `--version` proves a file is on PATH. It does not prove a compiler has its
# headers, that `go test` can execute a binary when /tmp is noexec, that npm can
# find a cache it is allowed to write, or that an agent can start when $HOME is
# read-only. Every one of those has been a real session failure and none of them
# is visible to a version check — so the checks below build, run, install and
# serve, inside containers wearing the same restrictions the driver puts on a
# real session.
#
# Those restrictions are copied from internal/driver.runArgs and are the point
# of the exercise, not an obstacle to it: uid 1000, no-new-privileges, every
# capability dropped, a read-only rootfs, a noexec tmpfs on /tmp, a workspace
# volume, an agent-home volume, and NO NETWORK AT ALL. Nothing here relaxes any
# of that to make a check pass; a check that cannot pass under the real contract
# is reporting a real defect in the image.
#
# No credential of any kind enters these containers, nothing is mounted from the
# host, and no probe reaches the internet, signs in, or spends an inference
# call. `gh auth status` is expected to report that it has no token: this script
# tests an IMAGE, and authentication is not an image property — see
# docs/session-image.md for what `gh` still needs.
#
# Usage:  scripts/session-image-smoke.sh [image]     (default rainier-session:smoke)
# Env:    DOCKER=<docker executable>  PROBE_TIMEOUT=<seconds>  KEEP=1
#         SECCOMP=<host profile path>  APPARMOR=<loaded host profile name>
# Exit:   0 every check passed, 1 a check failed, 2 setup or usage error.
set -uo pipefail

IMAGE=${1:-rainier-session:smoke}
DOCKER=${DOCKER:-docker}
PROBE_TIMEOUT=${PROBE_TIMEOUT:-240}
SECCOMP=${SECCOMP:-}
APPARMOR=${APPARMOR:-}

SECURITY_OPTS=()
if [ -n "$SECCOMP" ]; then
  [ -r "$SECCOMP" ] || { echo "no seccomp profile at $SECCOMP" >&2; exit 2; }
  SECURITY_OPTS+=(--security-opt "seccomp=$SECCOMP")
fi
if [ -n "$APPARMOR" ]; then
  SECURITY_OPTS+=(--security-opt "apparmor=$APPARMOR")
fi

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/session-image-checks.sh"

command -v "$DOCKER" >/dev/null 2>&1 || { echo "no docker executable ($DOCKER); set DOCKER=" >&2; exit 2; }
"$DOCKER" image inspect "$IMAGE" >/dev/null 2>&1 \
  || { echo "image $IMAGE is not present; build it first (make session-image)" >&2; exit 2; }

SUFFIX=$(od -An -tx1 -N6 /dev/urandom | tr -d ' \n')
WS_VOL="rainier-smoke-ws-$SUFFIX"
HOME_VOL="rainier-smoke-agents-$SUFFIX"
PID1=""

cleanup() {
  [ -n "$PID1" ] && "$DOCKER" rm -f "$PID1" >/dev/null 2>&1
  [ "${KEEP:-0}" = 1 ] && return 0
  "$DOCKER" volume rm -f "$WS_VOL" "$HOME_VOL" >/dev/null 2>&1
  return 0
}
trap cleanup EXIT

# The driver's own volume preparation, in the same shape: a new named volume's
# root is root:root, so a container running as 1000:1000 would get a directory
# it can only read. Root, with CAP_CHOWN and nothing else, no network, and the
# image's entrypoint never running (internal/driver.volumeInitArgs).
prepare_volume() {
  local vol=$1 mount=$2
  "$DOCKER" volume create "$vol" >/dev/null || return 1
  "$DOCKER" run --rm --network none --user 0:0 \
    --security-opt no-new-privileges --cap-drop ALL --cap-add CHOWN --read-only \
    -v "$vol:$mount" --entrypoint sh "$IMAGE" \
    -c "mkdir -p $mount/.rainier && chown -R 1000:1000 $mount" >/dev/null
}
prepare_volume "$WS_VOL" /workspace     || { echo "could not prepare the workspace volume" >&2; exit 2; }
prepare_volume "$HOME_VOL" /rainier/agents || { echo "could not prepare the agent-home volume" >&2; exit 2; }

# probe runs one bash program inside a session-shaped container and prints its
# combined output. The time bound is GNU coreutils' `timeout` INSIDE the
# container rather than a watchdog beside the docker client: a probe that hangs
# is then killed with its container, on a host that may not have `timeout`
# itself, and there is no stray killer process left to fire at a recycled pid
# later in the run. --network none is not overridable by a caller — an image
# check that needed the internet would be testing something other than the
# image.
probe() {
  "$DOCKER" run --rm \
    --network none \
    --user 1000:1000 \
    --security-opt no-new-privileges \
    "${SECURITY_OPTS[@]}" \
    --cap-drop ALL \
    --read-only \
    --tmpfs /tmp \
    --memory 3g --pids-limit 1024 \
    -v "$WS_VOL:/workspace" -v "$HOME_VOL:/rainier/agents" \
    -w /workspace \
    -e CLAUDE_CONFIG_DIR=/rainier/agents/claude \
    -e CODEX_HOME=/rainier/agents/codex \
    --entrypoint timeout "$IMAGE" -k 10 "$PROBE_TIMEOUT" /bin/bash -c "set -uo pipefail
$(declare -f expect_refusal)
$(declare -f brokered_gh_probe)
$1" 2>&1
}


# probe_setup is the same container without --read-only: the one shape the
# driver also creates, for a session carrying an environment's setup script.
# That build's whole job is to install things, `docker commit` excludes the
# workspace volume, and a read-only rootfs would make the install impossible —
# so the flag comes off for that container and only that one (see
# internal/driver.runArgs). Nothing else is relaxed here either.
probe_setup() {
  "$DOCKER" run --rm \
    --network none \
    --user 1000:1000 \
    --security-opt no-new-privileges \
    "${SECURITY_OPTS[@]}" \
    --cap-drop ALL \
    --tmpfs /tmp \
    --memory 3g --pids-limit 1024 \
    -v "$WS_VOL:/workspace" -v "$HOME_VOL:/rainier/agents" \
    -w /workspace \
    --entrypoint timeout "$IMAGE" -k 10 "$PROBE_TIMEOUT" /bin/bash -c "set -uo pipefail
$(declare -f expect_refusal)
$(declare -f brokered_gh_probe)
$1" 2>&1
}

CLAUDE_VERSION=$(sed -n 's/^ARG CLAUDE_CODE_VERSION=//p' "$SCRIPT_DIR/../Dockerfile")
CODEX_VERSION=$(sed -n 's/^ARG CODEX_VERSION=//p' "$SCRIPT_DIR/../Dockerfile")
# Read, not hardcoded: a check that names 17 while the image builds an 18 does
# not fail, it stops asking the question.
PG_MAJOR=$(sed -n 's/^ARG POSTGRES_MAJOR=//p' "$SCRIPT_DIR/../Dockerfile")
[[ "$PG_MAJOR" =~ ^[0-9]+$ ]] || { echo "missing or invalid POSTGRES_MAJOR pin" >&2; exit 2; }
CHROMIUM_VERSION=$(sed -n 's/^ARG CHROMIUM_VERSION=//p' "$SCRIPT_DIR/../Dockerfile")
CHROMIUM_REVISION=$(sed -n 's/^ARG CHROMIUM_REVISION=//p' "$SCRIPT_DIR/../Dockerfile")
PLAYWRIGHT_PIN=$(sed -n 's/^ARG PLAYWRIGHT_VERSION=//p' "$SCRIPT_DIR/../Dockerfile")
[[ "$CHROMIUM_VERSION" =~ ^[0-9]+(\.[0-9]+)+$ ]] || { echo "missing or invalid CHROMIUM_VERSION pin" >&2; exit 2; }
[[ "$CHROMIUM_REVISION" =~ ^[0-9]+$ ]] || { echo "missing or invalid CHROMIUM_REVISION pin" >&2; exit 2; }
[[ "$PLAYWRIGHT_PIN" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "missing or invalid PLAYWRIGHT_VERSION pin" >&2; exit 2; }
for version in "$CLAUDE_VERSION" "$CODEX_VERSION"; do
  [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "missing or invalid agent version pin" >&2; exit 2; }
done

echo "== $IMAGE"
echo
echo "-- the runtime contract"

# The image's own configuration, read from the daemon: an entrypoint and a
# default user are properties of the image, and a container started with
# explicit flags would hide both.
CFG=$("$DOCKER" image inspect -f '{{json .Config}}' "$IMAGE")
case "$CFG" in
  *'"/usr/local/bin/sessiond"'*) ok "the entrypoint is the absolute /usr/local/bin/sessiond" ;;
  *) bad "the entrypoint is the absolute /usr/local/bin/sessiond" \
         "a PATH-resolved entrypoint lets a setup script drop a sessiond into the user-writable /opt/rainier-env/bin and own PID 1" ;;
esac
case "$CFG" in
  *'"User":"1000:1000"'*) ok "the image's default user is 1000:1000, not root" ;;
  *) bad "the image's default user is 1000:1000, not root" "Config.User is not 1000:1000" ;;
esac
# A credential baked into an image is readable by anyone who can pull it,
# forever, and no later revocation reaches a layer.
if printf '%s' "$CFG" | grep -qE 'gh[pousr]_[A-Za-z0-9]|github_pat_|sk-ant-|AKIA[0-9A-Z]{12}'; then
  bad "no credential is baked into the image configuration"
else
  ok "no credential is baked into the image configuration"
fi

# Size is a rollout gate, not a curiosity: a Dedicated runner pulls this image
# at boot and somebody has to approve what that costs
# (rainier-cloud docs/runbooks/default-environment-rollout.md, step 2). Report
# it where the approval happens rather than only in a job log.
SIZE=$("$DOCKER" image inspect -f '{{.Size}} bytes, {{.Architecture}}/{{.Os}}, {{len .RootFS.Layers}} layers' "$IMAGE")
printf 'note  image: %s\n' "$SIZE"
note "session image size" "$SIZE"

check "the session runs as uid 1000" "uid=1000" 'id'
check "the rootfs is read-only" "ro-ok" \
  'if echo x > /usr/local/bin/probe 2>/dev/null; then echo ro-BROKEN; else echo ro-ok; fi'
check "the session user cannot rewrite sessiond" "sessiond-protected" \
  'if echo x > /usr/local/bin/sessiond 2>/dev/null; then echo sessiond-WRITABLE; else echo sessiond-protected; fi'
check "sessiond is root-owned" "root root" 'stat -c "%U %G" /usr/local/bin/sessiond'
check "the gh wrapper and real binary are root-owned" "root root|root root" '
  printf "%s|%s" "$(stat -c "%U %G" /usr/local/bin/gh)" "$(stat -c "%U %G" /usr/local/libexec/rainier/gh)"'
check "the agents a session runs are root-owned too" "agents-root-owned" '
  bad=0
  for f in claude codex gh go node npm python3 uv; do
    p=$(command -v "$f") || { echo "missing $f"; bad=1; continue; }
    o=$(stat -Lc %U "$p"); [ "$o" = root ] || { echo "$p is owned by $o"; bad=1; }
  done
  [ "$bad" = 0 ] && echo agents-root-owned'
check "\$HOME is read-only, as a session's is" "home-readonly" \
  'if touch "$HOME/.probe" 2>/dev/null; then echo home-WRITABLE; else echo home-readonly; fi'
check "/tmp is noexec, as a session's is" "tmp-noexec" \
  'printf "#!/bin/sh\necho x\n" > /tmp/p && chmod +x /tmp/p
   if /tmp/p >/dev/null 2>&1; then echo tmp-EXECUTABLE; else echo tmp-noexec; fi'
check "/workspace is writable and owned by the session user" "ws-ok 1000" \
  'touch /workspace/.probe && echo "ws-ok $(stat -c %u /workspace)"'
check "the agent home is writable" "agents-ok" 'touch /rainier/agents/.probe && echo agents-ok'
check "the environment install prefix belongs to the session user" "prefix-owned 1000" \
  'echo "prefix-owned $(stat -c %u /opt/rainier-env)"'
# Writability of the prefix is a property of the SETUP container, not of an
# ordinary session: an ordinary session's rootfs is read-only and this
# directory is on it. Asserting it in the read-only probe would either fail on
# a correct image or, worse, pass on a broken one.
check "an environment's setup script can install into the prefix" "prefix-writable" \
  'mkdir -p /opt/rainier-env/bin && printf "#!/bin/sh\n" > /opt/rainier-env/bin/probe && chmod +x /opt/rainier-env/bin/probe && echo prefix-writable' probe_setup
# And the boundary the prefix exists to draw: even with the rootfs writable,
# the platform's own PID 1 and the agents beside it are not the setup script's
# to replace.
check "a setup script still cannot rewrite sessiond" "sessiond-held" \
  'if echo x > /usr/local/bin/sessiond 2>/dev/null; then echo SESSIOND-WRITABLE; else echo sessiond-held; fi' probe_setup
check "no sudo is installed" "no-sudo" \
  'if command -v sudo >/dev/null 2>&1; then echo SUDO-PRESENT; else echo no-sudo; fi'
# Debian ships su and mount setuid; no-new-privileges is what makes that
# harmless, and this asserts the outcome rather than the file mode.
check "a setuid binary refuses escalation to root" "Authentication failure" \
  'expect_refusal 1 "Authentication failure" timeout 10 su root -c id </dev/null'

echo
echo "-- sessiond as PID 1"
PID1=$("$DOCKER" run -d \
  --network none --user 1000:1000 --security-opt no-new-privileges --cap-drop ALL \
  --read-only --tmpfs /tmp --memory 1g --pids-limit 256 \
  -v "$WS_VOL:/workspace" -w /workspace "$IMAGE" \
  -- bash -c 'echo pid1-child-ran > /workspace/.rainier/pid1' 2>/dev/null)
if [ -n "$PID1" ]; then
  # sessiond deliberately outlives its child so viewers keep the scrollback;
  # that is what makes it inspectable here at all.
  for _ in $(seq 20); do
    "$DOCKER" exec "$PID1" test -f /workspace/.rainier/pid1 >/dev/null 2>&1 && break
    sleep 0.5
  done
  OUT=$("$DOCKER" exec "$PID1" sh -c 'tr "\0" " " < /proc/1/cmdline; cat /workspace/.rainier/pid1' 2>&1)
  case "$OUT" in
    */usr/local/bin/sessiond*pid1-child-ran*) ok "the real entrypoint comes up as PID 1 and runs the child" ;;
    *) bad "the real entrypoint comes up as PID 1 and runs the child" "got: $(printf '%s' "$OUT" | tr '\n' '|')" ;;
  esac
  "$DOCKER" rm -f "$PID1" >/dev/null 2>&1; PID1=""
else
  bad "the real entrypoint comes up as PID 1 and runs the child" "the container never started"
fi

echo
echo "-- the agents start"
# --version says a file exists. --help makes the program parse its own
# configuration, which is what actually fails when $HOME is read-only.
check "claude reports its pinned version" "$CLAUDE_VERSION (Claude Code)" 'timeout 90 claude --version' probe exact
check "claude starts with a read-only \$HOME" "claude-started" \
  'timeout 90 claude --help >/dev/null 2>&1 && echo claude-started'
check "claude's config directory is redirected onto a writable mount" "claude-config-writable" \
  'case "$CLAUDE_CONFIG_DIR" in /rainier/agents/*) ;; *) echo "NOT-ON-MOUNT $CLAUDE_CONFIG_DIR"; exit 1;; esac
   mkdir -p "$CLAUDE_CONFIG_DIR" && touch "$CLAUDE_CONFIG_DIR/.probe" && echo claude-config-writable'
check "codex reports its pinned version" "codex-cli $CODEX_VERSION" \
  'mkdir -p "$CODEX_HOME" && timeout 90 codex --version' probe exact
check "codex starts with a read-only \$HOME" "codex-started" \
  'timeout 90 codex --help >/dev/null 2>&1 && echo codex-started'
check "codex's home is redirected onto a writable mount" "codex-home-writable" \
  'case "$CODEX_HOME" in /rainier/agents/*) ;; *) echo "NOT-ON-MOUNT $CODEX_HOME"; exit 1;; esac
   mkdir -p "$CODEX_HOME" && touch "$CODEX_HOME/.probe" && echo codex-home-writable'

# Codex is a package, not a binary, and `codex --version` above is green either
# way. These three are the difference between an image whose Codex can run a
# turn and one that fails closed on a missing tool host the first time it needs
# one. They ask CODEX where it resolved its own package rather than stat-ing the
# paths we hoped it would use, and then run the tool host it named. No
# credential and no network: doctor reports the layout offline, and its auth and
# connectivity checks are expected to fail in here and are not read.
check "the codex on PATH is a link into its package, not a lifted-out binary" "codex-linked" \
  'p=$(command -v codex); [ -L "$p" ] || { echo "NOT-A-LINK $p"; exit 1; }
   r=$(readlink -f "$p"); case "$r" in /usr/local/lib/codex/bin/codex) echo codex-linked ;;
     *) echo "RESOLVES-ELSEWHERE $r"; exit 1 ;; esac'
check "codex resolves its complete vendor package, with its bundled search tool" "codex-package-resolved" '
   mkdir -p "$CODEX_HOME"
   r=$(timeout 120 codex doctor --json 2>/dev/null) || true
   for want in "(package /usr/local/lib/codex, bin /usr/local/lib/codex/bin," \
               "resources /usr/local/lib/codex/codex-resources," \
               "path /usr/local/lib/codex/codex-path)" \
               "\"search provider\": \"bundled\"" \
               "\"search command\": \"/usr/local/lib/codex/codex-path/rg\""; do
     case "$r" in *"$want"*) ;; *) echo "DOCTOR-MISSING $want"; exit 1 ;; esac
   done
   echo codex-package-resolved'
check "the tool host codex spawns is present and actually runs" "codex-host-runs" '
   h=/usr/local/lib/codex/bin/codex-code-mode-host
   [ -x "$h" ] || { echo "NO-TOOL-HOST $h"; exit 1; }
   timeout 90 "$h" --help 2>&1 | grep -q "Usage: codex-code-mode-host" && echo codex-host-runs'
check "the codex package is root-owned and the session user cannot rewrite it" "codex-package-protected" '
   [ "$(stat -Lc %U /usr/local/lib/codex/bin/codex-code-mode-host)" = root ] || { echo NOT-ROOT; exit 1; }
   if echo x > /usr/local/lib/codex/bin/codex-code-mode-host 2>/dev/null
   then echo codex-package-WRITABLE; else echo codex-package-protected; fi'

echo
echo "-- compilers, runtimes and make"
check "a C program compiles against the standard headers and runs" "c-ok 42" '
  d=$(mktemp -d -p /workspace) && cd "$d"
  cat > hello.c <<"EOP"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
int main(void) { char b[8]; snprintf(b, sizeof b, "%d", 6 * 7); printf("c-ok %s\n", b); return 0; }
EOP
  cc -Wall -Werror -O2 -o hello hello.c && ./hello'
check "a C++ program compiles and runs" "cxx-ok 1" '
  d=$(mktemp -d -p /workspace) && cd "$d"
  cat > h.cc <<"EOP"
#include <string>
#include <iostream>
int main() { std::cout << "cxx-ok " << std::string("y").size() << std::endl; }
EOP
  g++ -O2 -o h h.cc && ./h'
check "pkg-config resolves an installed development library" "pkgconfig-ok" '
  pkg-config --exists zlib && test -n "$(pkg-config --cflags --libs zlib)" && echo pkgconfig-ok'
check "make builds a target" "make-ok" '
  d=$(mktemp -d -p /workspace) && cd "$d"
  printf "#include <stdio.h>\nint main(void){printf(\"make-ok\\\\n\");return 0;}\n" > m.c
  printf "all: m\nm: m.c\n\t\$(CC) -o \$@ \$<\n" > Makefile
  make >/dev/null && ./m'
# The Go check is the one that used to fail in a real session: `go test` builds
# a binary and then EXECUTES it, and a session's /tmp is noexec, so a toolchain
# whose GOTMPDIR was not moved onto the workspace dies with
# "fork/exec /tmp/...: permission denied" at the last step of a green build.
check "a Go program builds, runs, and its tests execute" "go-ok" '
  d=$(mktemp -d -p /workspace) && cd "$d"
  go mod init smoke >/dev/null 2>&1
  printf "package main\nimport \"fmt\"\nfunc main(){fmt.Println(\"go-built\")}\n" > main.go
  printf "package main\nimport \"testing\"\nfunc TestSmoke(t *testing.T){}\n" > main_test.go
  go build -o app . && ./app >/dev/null && go test ./... >/dev/null 2>&1 && echo go-ok'
check "Go's caches and build temp live on the workspace volume" "gocache-ok" '
  for v in GOCACHE GOMODCACHE GOTMPDIR GOPATH; do
    p=$(go env "$v")
    case "$p" in /workspace/*) ;; *) echo "$v is $p, not on the workspace"; exit 1;; esac
  done
  echo gocache-ok'
check "a Node program runs" "node-ok" 'node -e "console.log(\"node-ok\", process.version)"'
check "node is on an LTS major" "node-lts-ok" '
  major=$(node -p "process.versions.node.split(\".\")[0]")
  if [ $((major % 2)) -eq 0 ]; then echo "node-lts-ok $major"; else echo "node-ODD-major $major"; fi'
check "a Python program runs" "py-ok 3" 'python3 -c "import sys; print(\"py-ok\", sys.version_info[0])"'

echo
echo "-- package installs land in writable caches"
# A venv is a real offline package install: ensurepip unpacks and installs pip
# into it. If a read-only $HOME or a cache pointing somewhere unwritable broke
# installs, this is where it shows.
check "python3 -m venv creates a working venv with pip" "venv-ok" '
  d=$(mktemp -d -p /workspace) && cd "$d"
  python3 -m venv .venv && .venv/bin/python -c "import sys" && .venv/bin/pip --version >/dev/null && echo venv-ok'
# A wheel is a zip; building one here keeps the install entirely offline while
# still exercising pip's real install path into a real environment.
check "pip installs a wheel offline into a venv" "pipinstall-ok" '
  d=$(mktemp -d -p /workspace) && cd "$d"
  python3 -m venv .venv || exit 1
  mkdir -p b/smokepkg b/smokepkg-0.1.dist-info
  printf "def hello():\n    return \"pipinstall-ok\"\n" > b/smokepkg/__init__.py
  printf "Metadata-Version: 2.1\nName: smokepkg\nVersion: 0.1\n" > b/smokepkg-0.1.dist-info/METADATA
  printf "Wheel-Version: 1.0\nGenerator: smoke\nRoot-Is-Purelib: true\nTag: py3-none-any\n" > b/smokepkg-0.1.dist-info/WHEEL
  : > b/smokepkg-0.1.dist-info/RECORD
  ( cd b && zip -qr ../smokepkg-0.1-py3-none-any.whl . ) || exit 1
  .venv/bin/pip install --no-index smokepkg-0.1-py3-none-any.whl >/dev/null 2>&1 || exit 1
  .venv/bin/python -c "import smokepkg; print(smokepkg.hello())"'
check "pip's cache directory is on the workspace volume" "pipcache-ok" '
  d=$(python3 -m pip cache dir 2>/dev/null); [ -n "$d" ] || d=$PIP_CACHE_DIR
  case "$d" in /workspace/*) mkdir -p "$d" && touch "$d/.probe" && echo "pipcache-ok $d";; *) echo "pipcache-ELSEWHERE $d";; esac'
check "uv creates a venv and caches on the workspace volume" "uv-ok" '
  d=$(mktemp -d -p /workspace) && cd "$d"
  uv venv --python "$(command -v python3)" .venv >/dev/null 2>&1 || { echo "uv venv failed"; exit 1; }
  c=$(uv cache dir)
  case "$c" in /workspace/*) mkdir -p "$c" && echo "uv-ok $c";; *) echo "uv-cache-ELSEWHERE $c";; esac'
# npm with a file: dependency is a complete install — resolution, linking and a
# cache write — with no registry involved, which is what makes it runnable in a
# container that has no network at all.
check "npm installs a dependency offline into a writable cache" "npm-ok" '
  case "$(npm config get cache)" in /workspace/*) ;; *) echo "npmcache-ELSEWHERE $(npm config get cache)"; exit 1;; esac
  d=$(mktemp -d -p /workspace) && cd "$d"
  mkdir -p dep
  printf "{\"name\":\"dep\",\"version\":\"1.0.0\",\"main\":\"i.js\"}\n" > dep/package.json
  printf "module.exports=\"npm-ok\";\n" > dep/i.js
  printf "{\"name\":\"app\",\"version\":\"1.0.0\",\"dependencies\":{\"dep\":\"file:./dep\"}}\n" > package.json
  npm install --offline --no-audit --no-fund >/dev/null 2>&1 || { echo "npm install failed"; exit 1; }
  node -e "console.log(require(\"dep\"))"'

echo
echo "-- a local HTTP and TLS server"
check "a Python HTTP server answers over loopback" "http-ok" '
  d=$(mktemp -d -p /workspace) && cd "$d" && echo http-ok > index.html
  python3 -m http.server 8111 --bind 127.0.0.1 >/dev/null 2>&1 &
  for _ in $(seq 60); do
    curl -fsS -m 1 http://127.0.0.1:8111/index.html 2>/dev/null && break
    sleep 0.25
  done'
check "a Node TLS server serves a certificate curl verifies" "tls-ok" '
  d=$(mktemp -d -p /workspace) && cd "$d"
  openssl req -x509 -newkey rsa:2048 -nodes -keyout k.pem -out c.pem -days 1 \
    -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost" >/dev/null 2>&1 || exit 1
  cat > s.js <<"EOP"
const https = require("https"), fs = require("fs");
https.createServer({ key: fs.readFileSync("k.pem"), cert: fs.readFileSync("c.pem") },
  (_q, s) => s.end("tls-ok")).listen(8443, "127.0.0.1");
EOP
  node s.js >/dev/null 2>&1 &
  for _ in $(seq 60); do
    curl -fsS -m 1 --cacert c.pem https://localhost:8443/ 2>/dev/null && break
    sleep 0.25
  done'
check "ss reports the listening socket" "ss-ok" '
  python3 -m http.server 8112 --bind 127.0.0.1 >/dev/null 2>&1 &
  for _ in $(seq 40); do ss -ltn 2>/dev/null | grep -q ":8112" && { echo ss-ok; break; }; sleep 0.25; done'

echo
echo "-- the local services a developer starts"

# A probe is one container, so a running server does not outlive the check that
# started it: each check below starts what it needs and stops it again. That is
# the honest shape anyway — the thing being asserted is that a developer can
# bring a database up and take it down, repeatedly, with no privilege at all.
#
# Quoting note: a check program is a single-quoted shell word and cannot contain
# an apostrophe, so SQL string literals use PostgreSQL dollar quoting inside a
# QUOTED heredoc, where neither shell expands anything.

# The negative half first, and before anything in this section has run. A
# session gets a database because a developer starts one; an always-on
# trust-authenticated server, or a cluster baked into a layer, is exactly what
# the rest of this image's boundaries exist to make impossible to arrange.
check "a fresh session has no database running and no cluster in the image" "no-service-running" '
  ss -ltn 2>/dev/null | grep -Eq ":(5432|6379)\b" && { echo A-SERVER-IS-LISTENING; exit 1; }
  test -e /workspace/.services && { echo SERVICE-STATE-ALREADY-EXISTS; exit 1; }
  test -e /var/lib/postgresql/'"$PG_MAJOR"'/main && { echo A-CLUSTER-IS-BAKED-IN; exit 1; }
  echo no-service-running'

check "the PostgreSQL on PATH is the root-owned $PG_MAJOR the docs promise" "pg-major-ok" '
  for t in initdb pg_ctl postgres psql pg_isready createdb pg_dump; do
    command -v "$t" >/dev/null || { echo "missing $t"; exit 1; }
    [ "$(stat -Lc %U "$(command -v "$t")")" = root ] || { echo "$t is not root-owned"; exit 1; }
  done
  for t in initdb pg_ctl psql; do
    "$t" --version | grep -q "(PostgreSQL) '"$PG_MAJOR"'\." || { echo "$t: $("$t" --version)"; exit 1; }
  done
  echo pg-major-ok'

# The acceptance case the whole services layer exists for: a database, on the
# session'"'"'s own writable storage, with no sudo, no download and nothing about
# the runtime contract relaxed to get it.
check "initdb and pg_ctl bring PostgreSQL up on the workspace volume and run a transaction" "pg-transaction-ok" '
  rainier-pg init >/dev/null || { echo "init failed"; exit 1; }
  case "$PGDATA" in /workspace/*) ;; *) echo "PGDATA-ELSEWHERE $PGDATA"; exit 1;; esac
  rainier-pg start >/dev/null || { echo "start failed"; cat /workspace/.services/postgresql/server.log 2>/dev/null; exit 1; }
  rainier-pg status >/dev/null || { echo "status reports stopped"; exit 1; }
  test -S "$PGHOST/.s.PGSQL.$PGPORT" || { echo "no socket in $PGHOST"; exit 1; }
  createdb smokedb || { echo "createdb failed"; exit 1; }
  psql -v ON_ERROR_STOP=1 -q -d smokedb <<"SQL" || { echo "the transaction failed"; exit 1; }
begin;
create table ledger (id integer primary key, note text not null);
insert into ledger values (1, $q$committed$q$);
commit;
begin;
insert into ledger values (2, $q$rolled back$q$);
rollback;
SQL
  rows=$(psql -tAq -d smokedb -c "select count(*) from ledger")
  [ "$rows" = 1 ] || { echo "ROLLBACK-NOT-HONOURED rows=$rows"; exit 1; }
  rainier-pg stop >/dev/null || { echo "stop failed"; exit 1; }
  echo pg-transaction-ok'

# The DSN is the deliverable: this is the exact string a rainier-cloud checkout
# puts in RAINIER_TEST_DATABASE_URL, parsed by a real client against the running
# server. And the interface it is bound to is the security half of it — a
# trust-authenticated server is safe only while it cannot leave this session.
check "the printed DSN reaches the data, over loopback and nothing wider" "pg-dsn-ok" '
  rainier-pg start >/dev/null || { echo "start failed"; exit 1; }
  url=$(rainier-pg url smokedb)
  case "$url" in postgres://*@127.0.0.1:$PGPORT/smokedb[?]sslmode=disable) ;;
    *) echo "UNEXPECTED-DSN $url"; exit 1 ;; esac
  [ "$(psql -tAq "$url" -c "select count(*) from ledger")" = 1 ] || { echo "the DSN did not reach the data"; exit 1; }
  grep -q "listen_addresses = " "$PGDATA/postgresql.conf" || { echo "no listen_addresses"; exit 1; }
  # The LOCAL address column only. `ss` prints a peer column too, and for a
  # listening socket that column is literally 0.0.0.0:* — grepping the whole
  # line for 0.0.0.0 calls every correctly-bound server a leak.
  local_addrs=$(ss -ltnH 2>/dev/null | tr -s " " | cut -d" " -f4)
  case "$local_addrs" in *"0.0.0.0:$PGPORT"*|*"[::]:$PGPORT"*) echo LISTENING-ON-ALL-INTERFACES; exit 1;; esac
  case "$local_addrs" in *"127.0.0.1:$PGPORT"*) ;; *) echo "not listening on loopback: $local_addrs"; exit 1;; esac
  rainier-pg stop >/dev/null
  echo pg-dsn-ok'

check "PostgreSQL stops, restarts and still has its data" "pg-restart-ok" '
  rainier-pg start >/dev/null || { echo "start failed"; exit 1; }
  [ "$(psql -tAq -d smokedb -c "select note from ledger where id = 1")" = committed ] ||
    { echo "the data did not survive the first stop"; exit 1; }
  rainier-pg restart >/dev/null || { echo "restart failed"; exit 1; }
  [ "$(psql -tAq -d smokedb -c "select count(*) from ledger")" = 1 ] || { echo "lost data across a restart"; exit 1; }
  rainier-pg stop >/dev/null || { echo "stop failed"; exit 1; }
  rainier-pg status >/dev/null 2>&1 && { echo STILL-RUNNING-AFTER-STOP; exit 1; }
  echo pg-restart-ok'

check "SQLite writes and reads a database, from the shell and from Python" "sqlite-ok 2" '
  d=$(mktemp -d -p /workspace) && cd "$d"
  sqlite3 s.db "create table t (id integer primary key, v text)" || exit 1
  sqlite3 s.db "insert into t (v) values (char(97)), (char(98))" || exit 1
  python3 -c "import sqlite3; c=sqlite3.connect(\"s.db\"); print(\"sqlite-ok\", c.execute(\"select count(*) from t\").fetchone()[0])"'

check "Redis starts on loopback, answers PING without root, and stops" "redis-ok" '
  command -v redis-server >/dev/null || { echo "no redis-server"; exit 1; }
  rainier-redis start >/dev/null || { echo "start failed"; cat /workspace/.services/redis/redis.log 2>/dev/null; exit 1; }
  [ "$(rainier-redis ping)" = PONG ] || { echo "no PONG"; exit 1; }
  case "$(rainier-redis url)" in redis://127.0.0.1:*/0) ;; *) echo UNEXPECTED-URL; exit 1;; esac
  local_addrs=$(ss -ltnH 2>/dev/null | tr -s " " | cut -d" " -f4)
  case "$local_addrs" in *"0.0.0.0:6379"*|*"[::]:6379"*) echo "REDIS-ON-ALL-INTERFACES $local_addrs"; exit 1;; esac
  case "$local_addrs" in *"127.0.0.1:6379"*) ;; *) echo "redis not on loopback: $local_addrs"; exit 1;; esac
  redis-cli -h 127.0.0.1 set smoke redis-ok >/dev/null || { echo "SET failed"; exit 1; }
  v=$(redis-cli -h 127.0.0.1 get smoke)
  rainier-redis stop >/dev/null || { echo "stop failed"; exit 1; }
  rainier-redis status >/dev/null 2>&1 && { echo STILL-RUNNING-AFTER-STOP; exit 1; }
  [ "$v" = redis-ok ] && echo redis-ok'

# The services layer's own contribution, so the size review has a number for
# the thing this change added and not only a total to diff by hand. The build
# takes it by DIFFING the package set across its own apt install, so it counts
# the Debian-sourced dependencies PostgreSQL pulled in (the JIT and ICU
# libraries and the like) as well as the packages named on the command line.
# Summing by version string, or by a hand-written list, undercounts exactly
# there — and undercounting is the direction that gets a rollout approved.
SERVICES_SIZE=$(probe 'head -6 /usr/local/share/rainier-services-size.txt' | tr -d '\r')
printf 'note  services layer: %s\n' "$SERVICES_SIZE"
note "services layer installed size" "$SERVICES_SIZE"

# The half that is easy to get wrong in the other direction. A service's
# DURABLE state has to be on the volume, or it does not survive a suspend; its
# SOCKETS must not be, because protocol/workspace.TarGz refuses a socket
# outright rather than skipping it — so one under /workspace fails a `rainier
# push` or `pull` of any tree containing it, and an unclean exit leaves it
# there to keep failing. Both servers are running for this check, which is the
# only time the sockets exist.
check "a running server puts no socket on the volume that push and pull carry" "no-socket-on-the-volume" '
  rainier-pg start >/dev/null || { echo "postgres start failed"; exit 1; }
  rainier-redis start >/dev/null || { echo "redis start failed"; exit 1; }
  found=$(find /workspace -xdev -type s 2>/dev/null | head -5)
  rainier-redis stop >/dev/null; rainier-pg stop >/dev/null
  [ -z "$found" ] || { echo "SOCKET-ON-THE-VOLUME $found"; exit 1; }
  echo no-socket-on-the-volume'

check "every service kept its durable state on the workspace volume, and none on the rootfs" "service-state-ok" '
  for p in /workspace/.services/postgresql/data /workspace/.services/redis/data; do
    test -d "$p" || { echo "missing $p"; exit 1; }
  done
  case "$PGHOST" in /workspace/*) echo "PGHOST-ON-THE-VOLUME $PGHOST"; exit 1;; esac
  test -e /var/lib/postgresql/'"$PG_MAJOR"'/main && { echo CLUSTER-ON-ROOTFS; exit 1; }
  echo service-state-ok'

echo
echo "-- browser testing"

# `npx playwright install --with-deps` is what every project's CI runs and what
# a session cannot: its --with-deps half is an apt install as root, and a
# session has no escalation path, a read-only rootfs and no package archive on
# its allowlist. So the shared libraries are a build-time layer and the browser
# is a checksum-pinned artifact, and if either is wrong there is no in-session
# repair — which is why these are checks rather than documentation.
#
# Everything below runs in the ordinary probe: uid 1000, read-only rootfs,
# noexec /tmp, docker's 64 MiB /dev/shm, and NO NETWORK AT ALL. A browser that
# needed to download anything on first use would fail here, which is the point:
# a fresh session has to be able to run a test suite offline.
#
# The direct invocations below intentionally omit --no-sandbox. The sample
# Playwright project requires chromiumSandbox=true, so these probes must prove
# the packaged browser can use its own sandbox under the session policy.

BROWSER_FIXTURE='
  b=$(find -L "$PLAYWRIGHT_BROWSERS_PATH" -maxdepth 3 -type f -name chrome-headless-shell 2>/dev/null | head -1)
  [ -n "$b" ] || { echo "no chrome-headless-shell under $PLAYWRIGHT_BROWSERS_PATH"; exit 1; }
  d=$(mktemp -d -p /workspace) || exit 1
  cat > "$d/page.html" <<HTML
<!doctype html><html><head><meta charset="utf-8"><title>pending</title></head>
<body style="margin:0;background:#ffffff">
<h1 style="font-family:Arial,sans-serif;font-size:32px">Rainier browser smoke</h1>
<span id="m" style="font-family:Arial;font-size:100px">MMMMMMMMMM</span>
<script>
document.title = String(Math.round(document.getElementById("m").getBoundingClientRect().width));
</script>
</body></html>
HTML
  # The flags Playwright passes, and nothing else: --disable-dev-shm-usage is
  # in chromiumSwitches for every launch, which is why docker default 64 MiB
  # /dev/shm is enough for a Playwright suite.
  render() { "$b" --disable-dev-shm-usage --disable-gpu --disable-breakpad \
      --user-data-dir="$d/profile" "$@" 2>&1; }
  png_size() { python3 -c "import struct,sys; d=open(sys.argv[1],\"rb\").read(24); w,h=struct.unpack(\">II\", d[16:24]); print(w,h)" "$1"; }
'

check "no Playwright is installed globally, so a project's own pin is the one that runs" "no-global-playwright" '
  if command -v playwright >/dev/null 2>&1; then echo "GLOBAL-PLAYWRIGHT $(command -v playwright)"; exit 1; fi
  if command -v playwright-core >/dev/null 2>&1; then echo GLOBAL-PLAYWRIGHT-CORE; exit 1; fi
  if grep -qi "playwright" /usr/local/share/rainier-npm-global.json; then echo GLOBAL-PLAYWRIGHT-PACKAGE; exit 1; fi
  echo no-global-playwright'

check "the browser baseline is root-owned and the session user cannot rewrite it" "baseline-held" '
  b=$(find /usr/local/lib/rainier-browsers -type f -name chrome-headless-shell | head -1)
  [ -n "$b" ] || { echo "no browser baseline in the image"; exit 1; }
  [ "$(stat -c %U "$b")" = root ] || { echo "the browser is owned by $(stat -c %U "$b")"; exit 1; }
  if echo x > "$b" 2>/dev/null; then echo BROWSER-WRITABLE; exit 1; fi
  echo baseline-held'

check "a fresh workspace volume already carries the browser cache Playwright reads" "cache-linked" '
  [ "$PLAYWRIGHT_BROWSERS_PATH" = /workspace/.cache/ms-playwright ] \
    || { echo "PLAYWRIGHT_BROWSERS_PATH=$PLAYWRIGHT_BROWSERS_PATH"; exit 1; }
  dir=$PLAYWRIGHT_BROWSERS_PATH/chromium_headless_shell-'"$CHROMIUM_REVISION"'
  # The two files Playwright reads before it decides a browser is installed and
  # whether its dependencies still need checking. Both have to be writable
  # files on the volume, not links onto the read-only rootfs.
  for m in INSTALLATION_COMPLETE DEPENDENCIES_VALIDATED; do
    [ -f "$dir/$m" ] || { echo "no $m in $dir"; exit 1; }
    [ -L "$dir/$m" ] && { echo "$m is a link onto the read-only rootfs"; exit 1; }
    : > "$dir/$m" || { echo "$m is not writable"; exit 1; }
  done
  exe=$(find -L "$dir" -type f -name chrome-headless-shell | head -1)
  [ -x "$exe" ] || { echo "the cache does not resolve to an executable browser"; exit 1; }
  echo cache-linked'

check "the preinstalled browser is exactly the build the Dockerfile pins" "$CHROMIUM_VERSION" '
  '"$BROWSER_FIXTURE"'
  "$b" --version'

check "every shared library the browser needs resolves in this image" "libs-resolved" '
  '"$BROWSER_FIXTURE"'
  missing=$(ldd "$b" 2>/dev/null | awk "/not found/ { print \$1 }" | sort -u)
  [ -z "$missing" ] || { echo "MISSING $missing"; exit 1; }
  echo libs-resolved'

check "the browser renders a page and writes a desktop-viewport screenshot, offline" "1280 800" '
  '"$BROWSER_FIXTURE"'
  render --screenshot="$d/desktop.png" --window-size=1280,800 "file://$d/page.html" >/dev/null
  [ -s "$d/desktop.png" ] || { echo "no screenshot was written"; exit 1; }
  file "$d/desktop.png" | grep -q "PNG image" || { echo "not a PNG"; exit 1; }
  png_size "$d/desktop.png"'

check "the same page at a phone viewport produces a phone-sized screenshot" "390 844" '
  '"$BROWSER_FIXTURE"'
  render --screenshot="$d/phone.png" --window-size=390,844 "file://$d/page.html" >/dev/null
  [ -s "$d/phone.png" ] || { echo "no screenshot was written"; exit 1; }
  png_size "$d/phone.png"'

# A browser with no fonts still renders: it falls back to whatever it can find
# and lays text out with the wrong metrics, so a screenshot is boxes and a
# width assertion is a flake. Ten Arial capital Ms at 100px are 833px wide by
# the font, and Liberation Sans is metric-compatible with Arial by design —
# DejaVu, the usual fallback, gives 791. So the number below is a check that
# fontconfig resolved Arial to the font this image installed FOR that, not
# merely that some font exists.
check "Arial resolves to a metric-compatible font and text lays out at its real width" "font-metrics-ok" '
  '"$BROWSER_FIXTURE"'
  fc-match Arial | grep -qi liberation || { echo "fc-match Arial = $(fc-match Arial)"; exit 1; }
  fc-list | grep -qi emoji || { echo "no emoji font"; exit 1; }
  w=$(render --dump-dom "file://$d/page.html" | sed -n "s/.*<title>\([0-9]*\)<\/title>.*/\1/p" | head -1)
  [ -n "$w" ] || { echo "the page did not report a measured width"; exit 1; }
  [ "$w" -ge 800 ] && [ "$w" -le 870 ] || { echo "ten 100px Arial Ms measured ${w}px, not ~833"; exit 1; }
  echo font-metrics-ok'

check "the browser leaves no process behind after it exits" "no-browser-left" '
  '"$BROWSER_FIXTURE"'
  # By resolved executable rather than by command line: the shell running this
  # check has the string "chrome-headless-shell" in its own argv, so a pgrep -f
  # would match itself and never pass. A zombie has no /proc/pid/exe, which is
  # the right answer too: sessiond is PID 1 in a real session, and it reaps.
  browsers_alive() {
    for p in /proc/[0-9]*; do
      case "$(readlink "$p/exe" 2>/dev/null)" in *chrome-headless-shell*) return 0 ;; esac
    done
    return 1
  }
  render --screenshot="$d/x.png" --window-size=800,600 "file://$d/page.html" >/dev/null
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    browsers_alive || break
    sleep 0.5
  done
  if browsers_alive; then
    echo "BROWSER-STILL-RUNNING"; ps -eo pid,ppid,comm | head -20; exit 1
  fi
  echo no-browser-left'

check "rainier-browsers reports the cache a project's Playwright will read" "linked" '
  rainier-browsers path | grep -qx /workspace/.cache/ms-playwright || { echo "path = $(rainier-browsers path)"; exit 1; }
  rainier-browsers status'

# The sandbox-enabled launch is a required check; a browser that cannot
# initialize its own sandbox exits nonzero. The observed status is recorded for
# diagnostics after the functional checks.
SANDBOX_OUTPUT=$(probe '
  '"$BROWSER_FIXTURE"'
  out=$("$b" --disable-dev-shm-usage --disable-gpu --disable-breakpad --enable-logging=stderr --v=1 --user-data-dir="$d/p2" --dump-dom "file://$d/page.html" 2>&1)
  st=$?
  diagnostic=chromium-no-diagnostic
  case "$out" in
    *"No usable sandbox"*) diagnostic=chromium-no-usable-sandbox ;;
    *"Failed to move to new namespace"*) diagnostic=chromium-namespace-setup-failed ;;
    *"SIGSYS"*|*"seccomp-bpf"*) diagnostic=chromium-seccomp-failure ;;
  esac
  printf "exit=%s %s\n" "$st" "$diagnostic"
  exit "$st"
')
SANDBOX_STATUS=$?
SANDBOX_DIAGNOSTIC=$(printf '%s\n' "$SANDBOX_OUTPUT" | tail -1)
printf 'note  chromium own-sandbox under the driver restrictions: exit=%s %s\n' "$SANDBOX_STATUS" "$SANDBOX_DIAGNOSTIC"
note "chromium own-sandbox status" "exit=$SANDBOX_STATUS $SANDBOX_DIAGNOSTIC"
if [ "$SANDBOX_STATUS" -eq 0 ]; then
  ok "Chromium starts with its own sandbox"
else
  bad "Chromium starts with its own sandbox" "exit=$SANDBOX_STATUS; $SANDBOX_DIAGNOSTIC"
fi

BROWSER_SIZE=$(probe 'cat /usr/local/share/rainier-browser-size.txt 2>/dev/null | head -1; grep -h "browser payload" /usr/local/share/rainier-browser-size.txt 2>/dev/null' | tr '\n' '; ')
printf 'note  browser layer: %s\n' "$BROWSER_SIZE"
note "browser layer size" "$BROWSER_SIZE"

echo
echo "-- git, gh and the rest of the shell toolkit"
check "git makes a commit" "git-ok" '
  d=$(mktemp -d -p /workspace) && cd "$d" && git init -q . && echo a > a
  git -c user.email=smoke@example.invalid -c user.name=smoke add a
  git -c user.email=smoke@example.invalid -c user.name=smoke commit -qm x
  [ "$(git rev-list --count HEAD)" = 1 ] && echo git-ok'
check "gh is executable and reports its version" "gh version" 'gh --version'
check "managed gh version runs offline without a socket" "gh version" 'RAINIER_SESSION=sess_test gh --version'
check "wrapped gh receives only a synthetic socket credential" "synthetic-gh-token" 'brokered_gh_probe'
# Deliberately an "it runs" check and not an "it is signed in" one: nothing in
# an image is authenticated. The preceding fixture covers sessiond's one-shot
# brokered path without letting a real credential enter the image smoke.
check "gh reports unauthenticated without timing out" "not logged into any GitHub hosts" '
  expect_refusal 1 "not logged into any GitHub hosts" timeout 30 gh auth status'
check "an OpenSSH client is present" "OpenSSH" 'ssh -V 2>&1'
check "ripgrep, jq and the GNU text tools are the real ones" "tools-ok" '
  rg --version >/dev/null || { echo "no rg"; exit 1; }
  echo "{\"a\":1}" | jq -e ".a" >/dev/null || { echo "no jq"; exit 1; }
  for t in grep sed find; do "$t" --version 2>&1 | head -1 | grep -q GNU || { echo "$t is not GNU"; exit 1; }; done
  awk --version 2>&1 | head -1 | grep -qi gnu || { echo "awk is not GNU awk"; exit 1; }
  ls --version 2>&1 | head -1 | grep -q coreutils || { echo "ls is not GNU coreutils"; exit 1; }
  echo tools-ok'
check "archives round-trip through tar, gzip, xz, zip and unzip" "archive-ok" '
  d=$(mktemp -d -p /workspace) && cd "$d" && echo payload > f
  tar czf f.tgz f && tar xzf f.tgz -O f >/dev/null &&
  xz -k f && xz -t f.xz && zip -q f.zip f && unzip -qo f.zip -d out &&
  [ "$(cat out/f)" = payload ] && echo archive-ok'
check "diff and patch round-trip" "patch-ok" '
  d=$(mktemp -d -p /workspace) && cd "$d" && echo one > a && echo two > b
  diff -u a b > p.diff
  patch -s a < p.diff && [ "$(cat a)" = two ] && echo patch-ok'
check "rsync copies a tree" "rsync-ok" '
  d=$(mktemp -d -p /workspace) && cd "$d" && mkdir -p src && echo x > src/f
  rsync -a src/ dst/ && test -f dst/f && echo rsync-ok'
check "ps, lsof, dig, file, less and an editor are present" "utils-ok" '
  ps -o pid= -p 1 >/dev/null || { echo "no ps"; exit 1; }
  for t in lsof dig nano editor less; do command -v "$t" >/dev/null || { echo "no $t"; exit 1; }; done
  file /bin/sh >/dev/null || { echo "no file"; exit 1; }
  echo utils-ok'
check "curl trusts the system CA bundle" "ca-ok" '
  test -s /etc/ssl/certs/ca-certificates.crt && curl --version | grep -qi ssl && echo ca-ok'
check "the image records what it actually contains, and where it came from" "manifest-ok" '
  test -s /usr/local/share/rainier-os-packages.txt || { echo "no package manifest"; exit 1; }
  test -s /usr/local/share/rainier-npm-global.json || { echo "no npm manifest"; exit 1; }
  # Two archives resolve this image now, and a reviewer holding only a digest
  # has to be able to see both.
  test -s /usr/local/share/rainier-apt-sources.txt || { echo "no apt-source manifest"; exit 1; }
  test -s /usr/local/share/rainier-services-size.txt || { echo "no services size manifest"; exit 1; }
  grep -q "apt.postgresql.org" /usr/local/share/rainier-apt-sources.txt || { echo "the PGDG archive is unrecorded"; exit 1; }
  grep -q "deb.debian.org" /usr/local/share/rainier-apt-sources.txt || { echo "the Debian archive is unrecorded"; exit 1; }
  grep -q "^postgresql-'"$PG_MAJOR"'" /usr/local/share/rainier-os-packages.txt || { echo "the PostgreSQL server package is unrecorded"; exit 1; }
  echo manifest-ok'

printf '\n%d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ] || exit 1

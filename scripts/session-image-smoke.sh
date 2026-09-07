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
# Exit:   0 every check passed, 1 a check failed, 2 setup or usage error.
set -uo pipefail

IMAGE=${1:-rainier-session:smoke}
DOCKER=${DOCKER:-docker}
PROBE_TIMEOUT=${PROBE_TIMEOUT:-240}

command -v "$DOCKER" >/dev/null 2>&1 || { echo "no docker executable ($DOCKER); set DOCKER=" >&2; exit 2; }
"$DOCKER" image inspect "$IMAGE" >/dev/null 2>&1 \
  || { echo "image $IMAGE is not present; build it first (make session-image)" >&2; exit 2; }

PASS=0 FAIL=0
ok()  { PASS=$((PASS+1)); printf 'ok    %s\n' "$1"; }
bad() { FAIL=$((FAIL+1)); printf 'FAIL  %s\n' "$1"; [ $# -gt 1 ] && printf '      %s\n' "$2"; return 0; }

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

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/session-image-checks.sh"
CLAUDE_VERSION=$(sed -n 's/^ARG CLAUDE_CODE_VERSION=//p' "$SCRIPT_DIR/../Dockerfile")
CODEX_VERSION=$(sed -n 's/^ARG CODEX_VERSION=//p' "$SCRIPT_DIR/../Dockerfile")
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
check "the image records what it actually contains" "manifest-ok" '
  test -s /usr/local/share/rainier-os-packages.txt &&
  test -s /usr/local/share/rainier-npm-global.json && echo manifest-ok'

printf '\n%d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ] || exit 1

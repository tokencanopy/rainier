#!/usr/bin/env bash
# Downloads the session image's pinned upstream toolchain releases into
# /opt/toolchain, verifying every archive against a checksum recorded here
# before a single byte of it is extracted.
#
# It exists as a script rather than a run of RUN lines because the checksums
# ARE the pin: one table, one place to read it, one place to update it, and a
# `sha256sum --check` that runs before `tar` on every entry rather than on the
# ones somebody remembered. A download whose sum does not match is a failed
# build, never a warning.
#
# Every URL below is an upstream release artifact addressed by version. None of
# them is a bootstrap script that installs "the latest": a session image that
# resolved its own contents at build time would be a different image on every
# rebuild, and nothing downstream — a digest in the Dedicated root, a smoke
# result recorded beside it — would mean anything.
#
# amd64 is what the hosted Dedicated runners run and what CI publishes; arm64
# is here so `make session-image` on a developer's arm64 laptop builds the same
# image natively instead of under emulation. An architecture with no row fails
# loudly at the lookup rather than silently installing the wrong binary.
set -euo pipefail

# Codex is a package, not a binary. The upstream `codex-package` archive is a
# manifest, `bin/codex`, the `bin/codex-code-mode-host` tool host, and bundled
# ripgrep, bubblewrap and zsh under `codex-path` and `codex-resources`, and
# Codex finds every companion by walking up from the RESOLVED path of its own
# executable until it finds `codex-package.json`. Installing only the
# executable — which is what this script used to do — leaves `codex --version`
# green and `codex --search` failing closed on a missing tool host, so the
# layout is asserted here and a build that lost a piece of it fails.
#
# The list is written out rather than read from the manifest so that an
# upstream archive that quietly stops shipping one of these fails the build
# instead of silently shrinking the image.
CODEX_PACKAGE_MEMBERS=(
  bin/codex
  bin/codex-code-mode-host
  codex-path/rg
  codex-resources/bwrap
  codex-resources/zsh/bin/zsh
)

# require_codex_package <root> <version> <target triple>
require_codex_package() {
  local root=$1 version=$2 triple=$3 manifest="$1/codex-package.json" member
  if [ ! -s "$manifest" ]; then
    echo "codex: incomplete package: codex-package.json is missing or empty in ${root}" >&2
    return 1
  fi
  # Field-by-field with grep so the check needs no JSON parser in a stage that
  # has none yet, and so a manifest for another version or another CPU cannot
  # pass by being well-formed. Only the whitespace after the colon is a pattern;
  # the values are escaped so a dot cannot match some other character.
  local field literal
  for field in "version:${version//./\\.}" "target:${triple//./\\.}" \
               "layoutVersion:1" "entrypoint:bin/codex" \
               "resourcesDir:codex-resources" "pathDir:codex-path"; do
    literal=${field#*:}
    case "$field" in layoutVersion:*) ;; *) literal="\"${literal}\"" ;; esac
    if ! grep -Eq "\"${field%%:*}\": *${literal}" "$manifest"; then
      echo "codex: codex-package.json does not declare ${field%%:*} = ${field#*:}" >&2
      return 1
    fi
  done
  for member in "${CODEX_PACKAGE_MEMBERS[@]}"; do
    if [ ! -f "${root}/${member}" ] || [ ! -s "${root}/${member}" ]; then
      echo "codex: incomplete package: ${member} is missing from ${root}" >&2
      return 1
    fi
    if [ ! -x "${root}/${member}" ]; then
      echo "codex: incomplete package: ${member} is not executable" >&2
      return 1
    fi
  done
}

# Sourcing exposes the validator to the offline contract tests and installs
# nothing; only an execution reaches the downloads below.
if [ "${BASH_SOURCE[0]}" != "$0" ]; then
  return 0
fi

: "${TARGETARCH:?TARGETARCH must be set (docker sets it from the build platform)}"

# The base image is pinned by a single-platform digest, so a build on another
# host without overriding BASE_IMAGE would pull an amd64 userland and then have
# this script install arm64 binaries into it. Docker reports TARGETARCH from the
# BUILD platform while dpkg reports the truth about the IMAGE; when the two
# disagree the answer is to fail here, not to hand somebody a container whose
# every binary raises "exec format error" at first use.
image_arch=$(dpkg --print-architecture)
if [ "$image_arch" != "$TARGETARCH" ]; then
  echo "the base image is ${image_arch} but the build targets ${TARGETARCH}: pass --build-arg BASE_IMAGE=<a ${TARGETARCH} image>, or build with --platform linux/${image_arch}" >&2
  exit 1
fi
: "${GO_VERSION:?}" "${GH_VERSION:?}" "${CODEX_VERSION:?}"
: "${UV_VERSION:?}" "${RIPGREP_VERSION:?}" "${JQ_VERSION:?}"

PREFIX=/opt/toolchain
BIN="$PREFIX/bin"
mkdir -p "$BIN"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# fetch <url> <sha256> <destination file>
fetch() {
  local url=$1 want=$2 out=$3
  curl --fail --location --retry 3 --retry-delay 2 --max-time 600 \
       --proto '=https' --tlsv1.2 --output "$out" "$url"
  echo "${want}  ${out}" | sha256sum --check --status \
    || { echo "checksum mismatch for ${url}" >&2; exit 1; }
}

case "$TARGETARCH" in
amd64)
  GO_ARCH=amd64;         GO_SHA=63d339f0da5ab53635a56f2490a7984dfe12dfcff22ad749f63edaf590168445
  GH_ARCH=amd64;         GH_SHA=e4d4bb4498e8d007abe545b6568926793ace1b6447da598294a610018cb164be
  CODEX_TRIPLE=x86_64-unknown-linux-musl
  CODEX_SHA=a822187e1a2420c61c5926721bfbd878701ed95547c9bb0d4de4498a16ba1821
  UV_TRIPLE=x86_64-unknown-linux-gnu
  UV_SHA=173d95a0c32d18c896c46ba6fafbf3cf9c14ab74b033f81b76c883ef492a976b
  RG_TRIPLE=x86_64-unknown-linux-musl
  RG_SHA=33e15bcf1624b25cdd2a55813a47a2f95dbe126268203e76aa6a585d1e7b149c
  JQ_ARCH=amd64;         JQ_SHA=b1c22172dd303f3be49e935aa56aa48a8b7a46e0bc838b4997d3bb451495870f
  ;;
arm64)
  GO_ARCH=arm64;         GO_SHA=3450b45a3f9ee8568792736a5c5e70a1f2e9b36c35a8f74958c03e51d7d92bec
  GH_ARCH=arm64;         GH_SHA=ea4e7a581a32ccad6cc7923cb1576ac5859ba4b9a16ab22eb8f8a96e78e2e961
  CODEX_TRIPLE=aarch64-unknown-linux-musl
  CODEX_SHA=fc395cb043a1093ab0db34f44aba3199bfaa9ce640cd9be7fd588f44b0da64a4
  UV_TRIPLE=aarch64-unknown-linux-gnu
  UV_SHA=9ff6b9d4665edcdd3a88dcc73cd1eb641754deb927f14e8c62ebfde6bf4f5f5e
  # ripgrep publishes no musl build for aarch64; the gnu one matches this
  # image's Debian userland.
  RG_TRIPLE=aarch64-unknown-linux-gnu
  RG_SHA=a740b91c82eaf9914cfedd353572f2791cbe0162c84101ee0951058f4dcbc90d
  JQ_ARCH=arm64;         JQ_SHA=8b85c817833814ddca00a144c33705546355afccf0cf39b188f3cdb48b852309
  ;;
*)
  echo "no pinned toolchain for TARGETARCH=${TARGETARCH}; the session image supports amd64 and arm64" >&2
  exit 1
  ;;
esac

# Go, from the project's own download host. The whole tree lands under the
# prefix because the build stage compiles sessiond with this exact toolchain:
# the Go that builds the platform's PID 1 and the Go a developer runs in the
# session are then one pinned artifact rather than two that can drift.
fetch "https://dl.google.com/go/go${GO_VERSION}.linux-${GO_ARCH}.tar.gz" "$GO_SHA" "$work/go.tar.gz"
tar --extract --gzip --file "$work/go.tar.gz" --no-same-owner -C "$PREFIX"

# The GitHub CLI. Installed, and deliberately not authenticated: see
# docs/session-image.md on what `gh` still needs before it can open a PR.
fetch "https://github.com/cli/cli/releases/download/v${GH_VERSION}/gh_${GH_VERSION}_linux_${GH_ARCH}.tar.gz" "$GH_SHA" "$work/gh.tar.gz"
tar --extract --gzip --file "$work/gh.tar.gz" --no-same-owner --strip-components=2 \
    -C "$BIN" "gh_${GH_VERSION}_linux_${GH_ARCH}/bin/gh"

# Codex: the vendor's complete package for this platform, kept whole. The tree
# lands under $PREFIX/lib and the PATH entry is a RELATIVE symlink into it, so
# the same link resolves in this stage and again after the final image copies
# both directories under /usr/local. A symlink and not a copy: Codex resolves
# symlinks before it looks for `codex-package.json`, so the link keeps the tool
# host and the bundled resources reachable, and lifting the executable out of
# the package is exactly the defect this replaced.
mkdir -p "$PREFIX/lib"
fetch "https://github.com/openai/codex/releases/download/rust-v${CODEX_VERSION}/codex-package-${CODEX_TRIPLE}.tar.gz" "$CODEX_SHA" "$work/codex.tar.gz"
mkdir "$PREFIX/lib/codex"
tar --extract --gzip --file "$work/codex.tar.gz" --no-same-owner -C "$PREFIX/lib/codex"
require_codex_package "$PREFIX/lib/codex" "$CODEX_VERSION" "$CODEX_TRIPLE"
ln -s ../lib/codex/bin/codex "$BIN/codex"

fetch "https://github.com/astral-sh/uv/releases/download/${UV_VERSION}/uv-${UV_TRIPLE}.tar.gz" "$UV_SHA" "$work/uv.tar.gz"
tar --extract --gzip --file "$work/uv.tar.gz" --no-same-owner --strip-components=1 -C "$BIN" \
    "uv-${UV_TRIPLE}/uv" "uv-${UV_TRIPLE}/uvx"

fetch "https://github.com/BurntSushi/ripgrep/releases/download/${RIPGREP_VERSION}/ripgrep-${RIPGREP_VERSION}-${RG_TRIPLE}.tar.gz" "$RG_SHA" "$work/rg.tar.gz"
tar --extract --gzip --file "$work/rg.tar.gz" --no-same-owner --strip-components=1 -C "$BIN" \
    "ripgrep-${RIPGREP_VERSION}-${RG_TRIPLE}/rg"

fetch "https://github.com/jqlang/jq/releases/download/jq-${JQ_VERSION}/jq-linux-${JQ_ARCH}" "$JQ_SHA" "$work/jq"
install -m 0755 "$work/jq" "$BIN/jq"

# -type f so the codex symlink is not followed into the vendor package. GNU
# chmod has no --no-dereference, and the archive already ships its members 0755;
# this line is about the files installed here.
find "$BIN" -maxdepth 1 -type f -exec chmod 0755 {} +

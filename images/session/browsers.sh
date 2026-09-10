#!/usr/bin/env bash
# Installs the session image's pinned browser baseline: the Chromium build that
# a project's own Playwright launches for `headless: true`, plus Playwright's
# ffmpeg for video recording.
#
# WHY A BASELINE AT ALL. A session has no sudo, a read-only rootfs, and an
# egress allowlist that does not carry a package archive, so `npx playwright
# install --with-deps` — the line every project's CI uses — cannot work here:
# its apt half needs root. The shared-library half of that command is therefore
# in the Dockerfile as an ordinary build-time apt layer, and the browser half is
# here. Between them a fresh session runs a project's Playwright tests with no
# setup step, and with no download at all when the project's Playwright agrees
# with the pin below.
#
# WHY ONLY THE HEADLESS SHELL. Playwright launches `chromium-headless-shell`
# for `headless: true` and the full Chrome for Testing build only for
# `headless: false` or an explicit `channel: 'chromium'`. This image has no X
# server and no Xvfb, so `headless: false` cannot run in it whatever is
# installed, and the full build's extra 393 MiB would buy one channel setting.
# A project that wants it runs `npx playwright install chromium`, which writes
# into the workspace cache and needs only cdn.playwright.dev. See
# docs/session-image.md.
#
# WHAT IS PINNED. The Chrome for Testing version and the two Playwright
# revisions are ARGs in the Dockerfile; the SHA-256 of every archive is in the
# table below and is checked before a single byte is extracted. Every URL names
# its version — none of them resolves "the latest" at build time — so one
# commit builds one browser, and a version bump that forgets its checksum fails
# the build rather than installing something unreviewed.
#
# WHAT IS NOT INSTALLED. No `playwright` or `@playwright/test` npm package,
# globally or otherwise. A global Playwright would shadow nothing at require()
# time but would absolutely be picked up by a bare `npx playwright`, and the
# version that drives a project's tests has to be the one in the project's own
# lockfile. This script installs browsers; the project installs Playwright.
set -euo pipefail

: "${TARGETARCH:?TARGETARCH must be set (docker sets it from the build platform)}"
: "${CHROMIUM_VERSION:?}" "${CHROMIUM_REVISION:?}" "${PLAYWRIGHT_FFMPEG_REVISION:?}"
: "${PLAYWRIGHT_VERSION:?}"

# Same guard as the toolchain script: docker reports TARGETARCH from the BUILD
# platform while dpkg reports the truth about the IMAGE, and a browser for the
# other architecture is an "exec format error" at somebody's first test run
# rather than a failed build.
image_arch=$(dpkg --print-architecture)
if [ "$image_arch" != "$TARGETARCH" ]; then
  echo "the base image is ${image_arch} but the build targets ${TARGETARCH}: pass --build-arg BASE_IMAGE=<a ${TARGETARCH} image>, or build with --platform linux/${image_arch}" >&2
  exit 1
fi

# Playwright's own registry layout, which is the whole point: these directory
# and executable names are what packages/playwright-core/src/server/registry
# computes from the browser name and revision, so a project's Playwright finds
# the baseline by looking where it always looks. They are asserted after the
# extraction rather than trusted.
case "$TARGETARCH" in
amd64)
  SHELL_DIR=chrome-headless-shell-linux64
  SHELL_ZIP="chrome-headless-shell-linux64.zip"
  SHELL_SHA=a9da028861a0cf789ff25c2fed45f5f1aaf969ed9247835b6a7821a4f7af9d1d
  CFT_PLATFORM=linux64
  FFMPEG_ZIP=ffmpeg-linux.zip
  FFMPEG_SHA=ebc74fc5b94830176a3c2914ae96bd8bc7f6a91f4f33890230f84a172ee61ccc
  ;;
arm64)
  SHELL_DIR=chrome-headless-shell-linux-arm64
  SHELL_ZIP="chrome-headless-shell-linux-arm64.zip"
  SHELL_SHA=d433c45172c7836e38124fe545f767b02210bfb43a6262f08a297473a8e91c99
  CFT_PLATFORM=linux-arm64
  FFMPEG_ZIP=ffmpeg-linux-arm64.zip
  FFMPEG_SHA=2628c03f05318ff812c8c9baaf207dea2ddf53e818c0dc936714b0fbe3afb009
  ;;
*)
  echo "no pinned browser baseline for TARGETARCH=${TARGETARCH}; the session image supports amd64 and arm64" >&2
  exit 1
  ;;
esac

PREFIX=${RAINIER_BROWSERS_PREFIX:-/usr/local/lib/rainier-browsers}
SHELL_HOME="$PREFIX/chromium_headless_shell-${CHROMIUM_REVISION}"
FFMPEG_HOME="$PREFIX/ffmpeg-${PLAYWRIGHT_FFMPEG_REVISION}"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# fetch <url> <sha256> <destination file>
fetch() {
  local url=$1 want=$2 out=$3
  curl --fail --location --retry 3 --retry-delay 2 --max-time 900 \
       --proto '=https' --tlsv1.2 --output "$out" "$url"
  echo "${want}  ${out}" | sha256sum --check --status \
    || { echo "checksum mismatch for ${url}" >&2; exit 1; }
}

mkdir -p "$SHELL_HOME" "$FFMPEG_HOME"

# Chrome for Testing, from Playwright's CDN and at the exact build Playwright
# ${PLAYWRIGHT_VERSION} pins. cdn.playwright.dev serves the Chrome for Testing
# builds under a plain /builds/cft path and Playwright's own artifacts under
# /dbazure/download/playwright — one host for both, which is the only host a
# session needs allowlisted to install a different browser later.
fetch "https://cdn.playwright.dev/builds/cft/${CHROMIUM_VERSION}/${CFT_PLATFORM}/${SHELL_ZIP}" \
      "$SHELL_SHA" "$work/shell.zip"
unzip -q "$work/shell.zip" -d "$SHELL_HOME"

fetch "https://cdn.playwright.dev/dbazure/download/playwright/builds/ffmpeg/${PLAYWRIGHT_FFMPEG_REVISION}/${FFMPEG_ZIP}" \
      "$FFMPEG_SHA" "$work/ffmpeg.zip"
unzip -q "$work/ffmpeg.zip" -d "$FFMPEG_HOME"

# The two markers Playwright writes after a successful install and a successful
# host-requirements check. Both are created here because this baseline is on
# the read-only rootfs at runtime: without INSTALLATION_COMPLETE a project's
# `playwright install` would decide the browser is absent and download it
# again, and without DEPENDENCIES_VALIDATED every launch would re-run the ldd
# sweep and then silently fail to record that it passed.
#
# DEPENDENCIES_VALIDATED is honest here and nowhere else: the shared libraries
# it stands for are installed in the layer above this one, by the same build,
# and are asserted by ldd below.
for home in "$SHELL_HOME" "$FFMPEG_HOME"; do
  : > "$home/INSTALLATION_COMPLETE"
  : > "$home/DEPENDENCIES_VALIDATED"
done

SHELL_BIN="$SHELL_HOME/$SHELL_DIR/chrome-headless-shell"
FFMPEG_BIN="$FFMPEG_HOME/ffmpeg-linux"

# The layout Playwright will look for, asserted rather than assumed: an
# upstream archive that renamed its top-level directory would otherwise ship an
# image whose baseline is invisible to the thing that is supposed to find it.
[ -x "$SHELL_BIN" ] || { echo "the Chromium archive did not contain ${SHELL_DIR}/chrome-headless-shell" >&2; exit 1; }
[ -x "$FFMPEG_BIN" ] || { echo "the ffmpeg archive did not contain ffmpeg-linux" >&2; exit 1; }

# Every shared library the browser needs has to resolve in THIS image. ldd is
# the same check `playwright install` runs, and running it here means a missing
# apt line is a failed build rather than a developer's first test run failing
# with "error while loading shared libraries". Playwright's own version of this
# check reported "validation passed" against an image with sixteen of them
# missing, so this one reads ldd directly.
missing=$(ldd "$SHELL_BIN" 2>/dev/null | awk '/not found/ { print $1 }' | sort -u)
if [ -n "$missing" ]; then
  echo "the browser baseline is missing shared libraries this image does not install:" >&2
  echo "$missing" >&2
  exit 1
fi

# It also has to actually start. --version is the cheapest execution that
# proves the dynamic linker, the CPU baseline and the file mode all agree, and
# it must report the Chrome for Testing build this script was told to install.
got=$("$SHELL_BIN" --version 2>&1 || true)
case "$got" in
  *"$CHROMIUM_VERSION"*) ;;
  *) echo "the installed browser reports '${got}', not ${CHROMIUM_VERSION}" >&2; exit 1 ;;
esac

# What a reader holding only a digest needs in order to answer "which browser,
# which Playwright, and where". Read by /usr/local/bin/rainier-browsers and by
# scripts/session-image-smoke.sh; a target list is not evidence.
cat > /usr/local/share/rainier-browsers.json <<JSON
{
  "playwrightVersion": "${PLAYWRIGHT_VERSION}",
  "prefix": "${PREFIX}",
  "browsers": [
    {
      "name": "chromium-headless-shell",
      "directory": "chromium_headless_shell-${CHROMIUM_REVISION}",
      "revision": "${CHROMIUM_REVISION}",
      "browserVersion": "${CHROMIUM_VERSION}",
      "executable": "${SHELL_DIR}/chrome-headless-shell"
    },
    {
      "name": "ffmpeg",
      "directory": "ffmpeg-${PLAYWRIGHT_FFMPEG_REVISION}",
      "revision": "${PLAYWRIGHT_FFMPEG_REVISION}",
      "executable": "ffmpeg-linux"
    }
  ]
}
JSON
chmod 0644 /usr/local/share/rainier-browsers.json

# Appended to the apt layer's own breakdown so one file answers the whole
# question a pull-cost review asks: what the shared libraries cost, and what
# the browser itself costs.
du -sk "$PREFIX" | awk '{ printf "browser payload: %d KiB under '"$PREFIX"'\n", $1 }' \
  >> /usr/local/share/rainier-browser-size.txt
chmod 0644 /usr/local/share/rainier-browser-size.txt

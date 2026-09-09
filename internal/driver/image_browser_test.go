package driver

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Browser testing is the one developer capability whose usual installation
// instruction — `npx playwright install --with-deps` — cannot run in a Rainier
// session at all: its --with-deps half is `sudo apt-get install`, and there is
// no sudo, no writable rootfs and no package archive on the egress allowlist.
// So the shared libraries are a build-time layer, the browser is a
// checksum-pinned artifact beside the rest of the toolchain, and neither is
// something a session can be asked to acquire later.
//
// These read the Dockerfile and images/session/browsers.sh as text, for the
// same reason the rest of image_contract_test.go does: this is the half of the
// contract that can be checked on a machine with no docker, which is where
// Dockerfile edits get made. scripts/session-image-smoke.sh runs the browser.

func sessionBrowsersScript(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("../../images/session/browsers.sh")
	if err != nil {
		t.Fatalf("reading the session image's browser install script: %v", err)
	}
	return string(b)
}

// TestSessionImageInstallsTheBrowserDependencies: Playwright's own
// debian12-x64 chromium dependency list, named in the Dockerfile because the
// tool that knows it needs root to act on it. Sixteen of these are absent from
// the base image, and each absence is the same failure — "error while loading
// shared libraries" on somebody's first test run — which no version check and,
// measured against this base, not even Playwright's own host-requirements
// validation reports.
func TestSessionImageInstallsTheBrowserDependencies(t *testing.T) {
	df := sessionDockerfile(t)
	// packages/playwright-core/src/server/registry/nativeDeps.ts, deps
	// ["debian12-x64"].chromium, in full.
	for _, pkg := range []string{
		"libasound2", "libatk-bridge2.0-0", "libatk1.0-0", "libatspi2.0-0",
		"libcairo2", "libcups2", "libdbus-1-3", "libdrm2", "libgbm1", "libglib2.0-0",
		"libnspr4", "libnss3", "libpango-1.0-0",
		"libx11-6", "libxcb1", "libxcomposite1", "libxdamage1", "libxext6",
		"libxfixes3", "libxkbcommon0", "libxrandr2",
	} {
		if !regexp.MustCompile(`(?m)(^|\s)` + regexp.QuoteMeta(pkg) + `(\s|\\|$)`).MatchString(df) {
			t.Errorf("the session image no longer installs %q; Chromium will not load", pkg)
		}
	}
	// A browser with no fonts renders every glyph as a box, which makes a
	// screenshot artifact useless and a text-measuring assertion a flake. This
	// is a rendering dependency, not a nicety.
	for _, font := range []string{"fontconfig", "fonts-liberation", "fonts-dejavu-core", "fonts-noto-color-emoji"} {
		if !strings.Contains(df, font) {
			t.Errorf("the session image installs no %q; a Chromium with no fonts renders tofu and its screenshots are worthless", font)
		}
	}
}

// TestSessionImageInstallsNoGlobalPlaywright is the version-matching rule, and
// it is a prohibition rather than a pin.
//
// The version that drives a project's tests has to be the version in that
// project's lockfile: Playwright's client and its browser revision are one
// artifact, and a `playwright` on PATH from somewhere else is picked up by a
// bare `npx playwright` and silently drives the wrong one. The image therefore
// installs BROWSERS and no Playwright at all.
func TestSessionImageInstallsNoGlobalPlaywright(t *testing.T) {
	df := sessionDockerfile(t)
	for _, bad := range []string{
		"npm install --global playwright", "npm install -g playwright",
		"@playwright/test", "npm install --global --no-audit --no-fund \"playwright",
	} {
		if strings.Contains(df, bad) {
			t.Errorf("the Dockerfile contains %q; a globally installed Playwright hijacks `npx playwright` from the project's own pinned one", bad)
		}
	}
	if regexp.MustCompile(`npm install [^\n]*\bplaywright\b`).MatchString(df) {
		t.Error("the Dockerfile npm-installs Playwright; only the browsers belong in the image")
	}
}

// TestSessionImagePinsOneBrowserBaseline: the browser is an upstream download
// like Go or Codex, and it is pinned the same way — a version in the URL and a
// SHA-256 in the repository, checked before extraction. A baseline that
// resolved "the latest Chromium" would change under a digest that is supposed
// to describe it, and would stop matching the Playwright version it is the
// right browser for.
func TestSessionImagePinsOneBrowserBaseline(t *testing.T) {
	df := sessionDockerfile(t)
	sh := sessionBrowsersScript(t)

	for _, arg := range []string{
		"PLAYWRIGHT_VERSION", "CHROMIUM_VERSION", "CHROMIUM_REVISION", "PLAYWRIGHT_FFMPEG_REVISION",
	} {
		m := regexp.MustCompile(`(?m)^ARG ` + arg + `=(\S+)$`).FindStringSubmatch(df)
		if m == nil {
			t.Errorf("the Dockerfile declares no %s; the browser baseline would not be pinned to anything", arg)
			continue
		}
		if strings.Contains(m[1], "latest") {
			t.Errorf("%s is %q", arg, m[1])
		}
	}

	if !strings.Contains(sh, "sha256sum --check") {
		t.Error("the browser install script never verifies a checksum")
	}
	// Two artifacts for each of two architectures.
	if sums := regexp.MustCompile(`[A-Z_]+SHA=([0-9a-f]{64})`).FindAllString(sh, -1); len(sums) < 4 {
		t.Errorf("the browser script records %d checksums; it fetches two artifacts for each of two architectures", len(sums))
	}
	if regexp.MustCompile(`\|\s*(ba)?sh(\s|$)`).MatchString(sh) {
		t.Error("the browser script pipes a download into a shell")
	}
	for _, moving := range []string{"/latest/download", "releases/latest", "@latest"} {
		if strings.Contains(sh, moving) {
			t.Errorf("the browser script resolves %q at build time instead of naming a version", moving)
		}
	}
	// The check that costs nothing at build time and saves the failure that is
	// hardest to act on from inside a session: a browser whose libraries are
	// not in this image.
	if !strings.Contains(sh, "not found") || !strings.Contains(sh, "ldd ") {
		t.Error("the browser script does not ldd the installed browser; a missing apt line would ship as a runtime failure instead of a failed build")
	}
	// And that it starts, reporting the build it was told to install.
	if !strings.Contains(sh, "--version") || !strings.Contains(sh, "$CHROMIUM_VERSION") {
		t.Error("the browser script never runs the browser it installed or checks which build it is")
	}
	for _, arch := range []string{"amd64", "arm64"} {
		if !strings.Contains(sh, arch) {
			t.Errorf("the browser script has no %s row", arch)
		}
	}
}

// TestSessionImageBrowserCacheLivesOnTheWorkspace: the same rule as every
// other cache, and one extra that is a security boundary rather than a
// convenience.
//
// Playwright reads PLAYWRIGHT_BROWSERS_PATH, which has to be writable — a
// project pinned to a different Playwright installs its own revision there and
// must win. The BASELINE that path points into is root-owned on the read-only
// rootfs, because a session user who could rewrite the browser binary could
// rewrite what every later test run executes. The two meet through symlinks,
// and a `chown -R` that dereferenced them would hand the browser to the
// session user, which is why the seed's chown is -h.
func TestSessionImageBrowserCacheLivesOnTheWorkspace(t *testing.T) {
	df := sessionDockerfile(t)

	m := regexp.MustCompile(`(?m)^\s*PLAYWRIGHT_BROWSERS_PATH=(\S+)`).FindStringSubmatch(df)
	if m == nil {
		t.Fatal("the image sets no PLAYWRIGHT_BROWSERS_PATH; Playwright would fall back to a path this image has not qualified")
	}
	if !strings.HasPrefix(m[1], workspaceMount+"/") {
		t.Errorf("PLAYWRIGHT_BROWSERS_PATH is %s, which is not on the writable %s volume; a project could not install its own browser revision", m[1], workspaceMount)
	}

	if !strings.Contains(df, "/usr/local/lib/rainier-browsers") {
		t.Error("the browser baseline is not under /usr/local/lib; it must be root-owned like sessiond and the agents")
	}
	if strings.Contains(df, "/opt/rainier-env/browsers") || strings.Contains(df, "chown -R 1000:1000 /usr/local/lib") {
		t.Error("the browser baseline is in, or given to, the session-writable prefix")
	}
	if !strings.Contains(df, "chown -Rh 1000:1000 /workspace/.cache") {
		t.Error("the browser cache seed is chowned without -h; a recursive chown that dereferences the seed's symlinks gives the session user the root-owned browser it is about to execute")
	}
	// The other half of the same fact, and the one that would take the fleet
	// down rather than merely weaken it. The initializer chowns the whole
	// freshly copied volume as root with a READ-ONLY rootfs; the volume now
	// contains symlinks onto that rootfs. GNU chown -R does not dereference
	// (it traverses -P and lchown()s the link), so this works. -L or
	// --dereference would fail the init job with EROFS on every session
	// create, and would give the session user the browser binary on any host
	// where it did not.
	if strings.Contains(initWorkspaceScript, "-L") || strings.Contains(initWorkspaceScript, "--dereference") {
		t.Errorf("the volume initializer dereferences symlinks (%q); the workspace seed points at the read-only rootfs", initWorkspaceScript)
	}
	if !strings.Contains(initWorkspaceScript, "chown -R "+sessionUser) {
		t.Errorf("the volume initializer no longer chowns the workspace as expected: %q", initWorkspaceScript)
	}
	if !strings.Contains(df, "rainier-browsers link") {
		t.Error("the image never links the baseline into the browser cache; a fresh session would download a browser it already has")
	}
	if !strings.Contains(df, "chmod 0755 /usr/local/bin/rainier-browsers") {
		t.Error("rainier-browsers is not installed root-owned and executable in /usr/local/bin")
	}
}

// TestSessionImageDoesNotDisableBrowserSafety: nothing in the image may turn
// off a check on the user's behalf. PLAYWRIGHT_SKIP_VALIDATE_HOST_REQUIREMENTS
// would hide exactly the missing-library failure this image exists to prevent,
// PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD would take away a project's ability to
// install the version it actually pins, and a download host baked into the
// image would silently redirect where a browser comes from.
func TestSessionImageDoesNotDisableBrowserSafety(t *testing.T) {
	df := sessionDockerfile(t)
	for _, v := range []string{
		"PLAYWRIGHT_SKIP_VALIDATE_HOST_REQUIREMENTS",
		"PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD",
		"PLAYWRIGHT_CHROMIUM_DOWNLOAD_HOST",
		"PLAYWRIGHT_DOWNLOAD_HOST",
	} {
		if regexp.MustCompile(`(?m)^\s*` + v + `=`).MatchString(df) {
			t.Errorf("the image sets %s; a session must keep both the check and the choice", v)
		}
	}
	// The image ships no launch flags at all, and in particular does not ship
	// the one that turns Chromium's own sandbox off for everybody. Whether a
	// suite enables Chromium's sandbox is the suite's call (Playwright's own
	// default is chromiumSandbox: false); it is not the image's to make.
	if strings.Contains(df, "--no-sandbox") {
		t.Error("the Dockerfile names --no-sandbox; the image does not choose a project's browser launch flags")
	}
}

// TestDockerGrantsNoBrowserPrivilege: the isolation half of the same change.
// The usual answers to "Chromium will not start in a container" are a
// privileged container, an added capability, seccomp=unconfined, the host's
// network, or a wider host mount. None of them is here, and a browser baseline
// in the image is not a reason for any of them to arrive later: the driver
// runs the container that runs the browser with exactly the restrictions it
// ran before.
func TestDockerGrantsNoBrowserPrivilege(t *testing.T) {
	d := &Docker{opts: DockerOpts{Label: "rainier.session", Network: "rainier-int"}}
	args := strings.Join(d.runArgs(Spec{SessionID: "sess_browser"}, "img"), " ")
	for _, forbidden := range []string{
		"--privileged",
		"--cap-add",
		"seccomp=unconfined",
		"apparmor=unconfined",
		"--network host",
		"--device",
		"--ipc=host",
		"--ipc host",
		"/dev/shm:",
	} {
		if strings.Contains(args, forbidden) {
			t.Errorf("runArgs contains %q; nothing about running a browser justifies relaxing the session's isolation", forbidden)
		}
	}
	// The restrictions runArgs actually applies to a session with no setup
	// script. --cap-drop is deliberately NOT in this list: the driver does not
	// pass it to the session container today (only to the volume initializer),
	// and a test that asserted it would be asserting the documentation rather
	// than the code. See docs/session-image.md.
	for _, required := range []string{"--user " + sessionUser, "no-new-privileges", "--read-only", "--tmpfs /tmp"} {
		if !strings.Contains(args, required) {
			t.Errorf("runArgs no longer contains %q", required)
		}
	}
}

// --- the helper, run rather than read ---------------------------------------

func browsersHelper(t *testing.T) string {
	t.Helper()
	p, err := filepath.Abs("../../images/session/browsers/rainier-browsers")
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// fakeBaseline is a browser baseline as far as the helper can see it: one
// browser directory holding a payload directory, and Playwright's two markers.
func fakeBaseline(t *testing.T) string {
	t.Helper()
	prefix := t.TempDir()
	payload := filepath.Join(prefix, "chromium_headless_shell-1243", "chrome-headless-shell-linux64")
	if err := os.MkdirAll(payload, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(payload, "chrome-headless-shell"), []byte("#!/bin/sh\necho 'Chromium 153.0.8010.12'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, marker := range []string{"INSTALLATION_COMPLETE", "DEPENDENCIES_VALIDATED"} {
		if err := os.WriteFile(filepath.Join(prefix, "chromium_headless_shell-1243", marker), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return prefix
}

func TestSessionImageBrowserHelperLinksTheBaselineIntoTheCache(t *testing.T) {
	prefix := fakeBaseline(t)
	cache := filepath.Join(t.TempDir(), "ms-playwright")
	run := runHelper(t, browsersHelper(t), t.TempDir(), []string{
		"RAINIER_BROWSERS_PREFIX=" + prefix,
		"PLAYWRIGHT_BROWSERS_PATH=" + cache,
	}, "link")
	if run.status != 0 {
		t.Fatalf("link exited %d: %s", run.status, run.out)
	}
	dir := filepath.Join(cache, "chromium_headless_shell-1243")
	// Playwright decides a browser is installed by the presence of this file,
	// and re-validates its dependencies every thirty days by rewriting the
	// other. Both are real files in the writable cache rather than symlinks
	// into the read-only baseline, or that rewrite fails and every launch pays
	// for an ldd sweep it cannot record the result of.
	for _, marker := range []string{"INSTALLATION_COMPLETE", "DEPENDENCIES_VALIDATED"} {
		info, err := os.Lstat(filepath.Join(dir, marker))
		if err != nil {
			t.Fatalf("%s: %v", marker, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			t.Errorf("%s is a symlink into the read-only baseline; Playwright has to be able to rewrite it", marker)
		}
	}
	// And the payload is a link, not a copy: a quarter of a gigabyte per
	// session volume is the thing this arrangement exists to avoid.
	payload := filepath.Join(dir, "chrome-headless-shell-linux64")
	info, err := os.Lstat(payload)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatal("the browser payload was copied into the workspace cache instead of linked")
	}
	if _, err := os.Stat(filepath.Join(payload, "chrome-headless-shell")); err != nil {
		t.Fatalf("the linked payload does not resolve to an executable: %v", err)
	}
}

func TestSessionImageBrowserHelperIsIdempotent(t *testing.T) {
	prefix := fakeBaseline(t)
	cache := filepath.Join(t.TempDir(), "ms-playwright")
	env := []string{"RAINIER_BROWSERS_PREFIX=" + prefix, "PLAYWRIGHT_BROWSERS_PATH=" + cache}
	for i := 0; i < 2; i++ {
		if run := runHelper(t, browsersHelper(t), t.TempDir(), env, "link"); run.status != 0 {
			t.Fatalf("link %d exited %d: %s", i, run.status, run.out)
		}
	}
	entries, err := os.ReadDir(filepath.Join(cache, "chromium_headless_shell-1243"))
	if err != nil {
		t.Fatal(err)
	}
	// The marker, Playwright's two, and one payload link. A second run that
	// stacked links or nested a link inside its own target would show here.
	if len(entries) != 4 {
		names := []string{}
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("a second link produced %v", names)
	}
}

// A project that installs its own browser owns that directory. `link` has to
// leave it alone even though the name collides: overwriting it would delete a
// download the project's own lockfile asked for, and would put the baseline's
// revision behind a name that means a different one.
func TestSessionImageBrowserHelperLeavesAProjectInstallAlone(t *testing.T) {
	prefix := fakeBaseline(t)
	cache := filepath.Join(t.TempDir(), "ms-playwright")
	mine := filepath.Join(cache, "chromium_headless_shell-1243", "chrome-headless-shell-linux64")
	if err := os.MkdirAll(mine, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mine, "chrome-headless-shell"), []byte("mine"), 0o755); err != nil {
		t.Fatal(err)
	}
	run := runHelper(t, browsersHelper(t), t.TempDir(), []string{
		"RAINIER_BROWSERS_PREFIX=" + prefix,
		"PLAYWRIGHT_BROWSERS_PATH=" + cache,
	}, "link")
	if run.status != 0 {
		t.Fatalf("link exited %d: %s", run.status, run.out)
	}
	b, err := os.ReadFile(filepath.Join(mine, "chrome-headless-shell"))
	if err != nil || string(b) != "mine" {
		t.Fatalf("the project's own install was replaced: %q, %v", string(b), err)
	}
	if !strings.Contains(run.out, "skip") {
		t.Errorf("link did not report that it skipped the project's own install: %s", run.out)
	}
}

// The path a project's Playwright will read has to be the path this helper
// reports, or the helper is describing a different machine.
func TestSessionImageBrowserHelperReportsThePathPlaywrightReads(t *testing.T) {
	cache := filepath.Join(t.TempDir(), "ms-playwright")
	run := runHelper(t, browsersHelper(t), t.TempDir(), []string{"PLAYWRIGHT_BROWSERS_PATH=" + cache}, "path")
	if got := strings.TrimSpace(run.out); got != cache {
		t.Fatalf("path = %q, want %q", got, cache)
	}
	// With no override, Playwright computes $XDG_CACHE_HOME/ms-playwright.
	run = runHelper(t, browsersHelper(t), t.TempDir(), []string{"XDG_CACHE_HOME=/workspace/.cache"}, "path")
	if got := strings.TrimSpace(run.out); got != "/workspace/.cache/ms-playwright" {
		t.Fatalf("path with only XDG_CACHE_HOME = %q", got)
	}
}

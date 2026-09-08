package driver

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Codex is a package, not a binary.
//
// The upstream `codex-package` archive is a manifest, `bin/codex`, the
// `bin/codex-code-mode-host` tool host, and bundled ripgrep, bubblewrap and zsh
// under `codex-path` and `codex-resources`. Codex locates every companion by
// walking up from the RESOLVED path of its own executable until it finds
// `codex-package.json`. The image used to install `codex-${TRIPLE}.tar.gz`,
// which is one executable and nothing else, so a session answered
// `codex --version` with the right number and then failed closed the first time
// a turn needed the tool host — reported as a missing
// /usr/local/bin/codex-code-mode-host.
//
// Measured against the real rust-v0.153.4 archive with `codex doctor --json`,
// which reports Codex's own resolution rather than ours:
//
//	complete package             install context names package/bin/resources/path
//	bin/codex alone              bare "other"; ripgrep resolves to the OS one
//	bin/codex + host on PATH     byte-for-byte identical to the row above
//	package minus the tool host  doctor still passes; nothing notices
//	symlink on PATH -> bin/codex identical to the complete package
//
// So the tool host belongs neither on PATH nor beside sessiond in
// /usr/local/bin, a symlink is how the PATH entry reaches the package, and the
// layout has to be asserted separately because doctor does not catch a deleted
// host. The tests below hold those three decisions in place. The functional
// half — that the installed codex actually resolves the package and that the
// host actually runs — is scripts/session-image-smoke.sh, against a real image.

// codexPackageMembers is every file Codex spawns or execs out of its own
// package. Spelled out here rather than scraped from the script so that
// deleting one from the script fails this test instead of quietly shrinking
// what the image ships.
var codexPackageMembers = []string{
	"bin/codex",
	"bin/codex-code-mode-host",
	"codex-path/rg",
	"codex-resources/bwrap",
	"codex-resources/zsh/bin/zsh",
}

// TestSessionToolchainInstallsTheWholeCodexPackage: the archive the script
// fetches is the package one, for every architecture it supports, and the
// executable is never lifted out of it.
func TestSessionToolchainInstallsTheWholeCodexPackage(t *testing.T) {
	sh := sessionToolchainScript(t)

	if !strings.Contains(sh, "codex-package-${CODEX_TRIPLE}.tar.gz") {
		t.Error("the toolchain script does not fetch the codex-package archive; the single-binary release carries no tool host, and an image built from it answers `codex --version` and then fails closed on the first turn that needs one")
	}
	if regexp.MustCompile(`download/rust-v\$\{CODEX_VERSION\}/codex-\$\{CODEX_TRIPLE\}\.tar\.gz`).MatchString(sh) {
		t.Error("the toolchain script still fetches the single-binary codex release")
	}
	if regexp.MustCompile(`install\s+-m\s+0755\s+"\$work/codex`).MatchString(sh) {
		t.Error("the toolchain script installs the codex executable out of its package; Codex resolves its tool host and bundled resources relative to its own executable, so the executable has to stay in the package and the PATH entry has to be a link into it")
	}

	// A relative link, because the same link has to resolve in the toolchain
	// stage and again after the final image copies both directories under
	// /usr/local. An absolute /opt/toolchain link would dangle there.
	if !strings.Contains(sh, `ln -s ../lib/codex/bin/codex "$BIN/codex"`) {
		t.Error("the PATH entry for codex is not a relative symlink into the installed package")
	}
	for _, member := range codexPackageMembers {
		if !strings.Contains(sh, member) {
			t.Errorf("the toolchain script never requires %q; a package missing it installs silently and fails in a session", member)
		}
	}

	// Both architectures move together or neither does: an arm64 laptop that
	// quietly kept the old single-binary sum would build a different image.
	sums := regexp.MustCompile(`CODEX_SHA=([0-9a-f]{64})`).FindAllStringSubmatch(sh, -1)
	if len(sums) != 2 {
		t.Fatalf("the toolchain script records %d Codex checksums; it supports amd64 and arm64", len(sums))
	}
	for _, stale := range []string{
		"f479424eca092484dc40d87ae28c44f4cc40234a60045d6131e493800d814a30", // amd64 single binary
		"5cda6182bd94c3a30f2eb63a495489ebf7f691fddb14d70f48c6c1a5071b6cde", // arm64 single binary
	} {
		if strings.Contains(sh, stale) {
			t.Errorf("the toolchain script still pins %s, the sum of a single-binary codex release", stale[:12])
		}
	}
	if sums[0][1] == sums[1][1] {
		t.Error("both architectures pin the same Codex checksum; one of them is fetching the wrong package")
	}
}

// TestSessionImageCarriesTheCodexPackageWhole: the final image copies the tree,
// not just the bin directory the link points out of.
func TestSessionImageCarriesTheCodexPackageWhole(t *testing.T) {
	df := sessionDockerfile(t)
	if !strings.Contains(df, "COPY --from=toolchain /opt/toolchain/lib/ /usr/local/lib/") {
		t.Error("the final image never copies /opt/toolchain/lib, so the codex symlink in /usr/local/bin would dangle and no companion would be reachable")
	}
	lib := strings.Index(df, "COPY --from=toolchain /opt/toolchain/lib/")
	bin := strings.Index(df, "COPY --from=toolchain /opt/toolchain/bin/")
	if lib < 0 || bin < 0 || lib > bin {
		t.Error("the package tree must be copied before the bin directory whose symlink points into it")
	}
	// The package is root-owned under /usr/local for the same reason sessiond
	// and the agents are: the session user must not be able to rewrite the
	// agent it is about to run, and that includes the tool host it spawns.
	if strings.Contains(df, "/opt/rainier-env/lib/codex") {
		t.Error("the Codex package is installed under the user-writable environment prefix; it belongs in root-owned /usr/local")
	}
}

// TestCodexPackageValidatorRejectsAnIncompletePackage is the half that cannot
// be read off the script. Each case builds a real synthetic package on disk —
// fictional contents, real paths and modes — and runs the toolchain script's
// own validator against it as a process, so what is asserted is the exit status
// a build would get and the message a reader would have to act on.
func TestCodexPackageValidatorRejectsAnIncompletePackage(t *testing.T) {
	const (
		version = "1.2.3"
		triple  = "x86_64-unknown-linux-musl"
	)
	manifest := fmt.Sprintf(`{
  "layoutVersion": 1,
  "version": %q,
  "target": %q,
  "variant": "codex",
  "entrypoint": "bin/codex",
  "resourcesDir": "codex-resources",
  "pathDir": "codex-path"
}
`, version, triple)

	script, err := filepath.Abs("../../images/session/toolchain.sh")
	if err != nil {
		t.Fatal(err)
	}

	// build lays down a complete package and then applies one defect. body is
	// always the manifest that gets written, so "" really means an empty file.
	build := func(t *testing.T, omit, unreadable, body string) string {
		t.Helper()
		root := t.TempDir()
		if omit != "codex-package.json" {
			if err := os.WriteFile(filepath.Join(root, "codex-package.json"), []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		for _, member := range codexPackageMembers {
			if member == omit {
				continue
			}
			path := filepath.Join(root, filepath.FromSlash(member))
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatal(err)
			}
			mode := os.FileMode(0o755)
			if member == unreadable {
				mode = 0o644
			}
			if err := os.WriteFile(path, []byte("#!/bin/sh\n# fictional "+member+"\nexit 0\n"), mode); err != nil {
				t.Fatal(err)
			}
		}
		return root
	}

	validate := func(t *testing.T, root string) (string, int) {
		t.Helper()
		// Sourcing must expose the validator and install nothing: a test that
		// had to run the downloads would not be a test anybody could run.
		cmd := exec.Command("/bin/bash", "-c",
			`source "$1"; require_codex_package "$2" "$3" "$4"`,
			"codex-package-validator", script, root, version, triple)
		cmd.Env = append(os.Environ(), "TARGETARCH=", "CODEX_VERSION=")
		out, err := cmd.CombinedOutput()
		code := 0
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		} else if err != nil {
			t.Fatalf("running the validator: %v", err)
		}
		return string(out), code
	}

	t.Run("a complete package is accepted", func(t *testing.T) {
		out, code := validate(t, build(t, "", "", manifest))
		if code != 0 {
			t.Fatalf("a complete package was rejected (exit %d): %s", code, out)
		}
	})

	// Every member, named on its own, because each absence is a different
	// failure in a session and the build has to say which one it is.
	for _, missing := range append([]string{"codex-package.json"}, codexPackageMembers...) {
		t.Run("without "+missing, func(t *testing.T) {
			out, code := validate(t, build(t, missing, "", manifest))
			if code == 0 {
				t.Fatalf("a package with no %s was accepted", missing)
			}
			if !strings.Contains(out, missing) {
				t.Errorf("the failure never names %s: %s", missing, out)
			}
		})
	}

	t.Run("a tool host that cannot be executed is not an install", func(t *testing.T) {
		out, code := validate(t, build(t, "", "bin/codex-code-mode-host", manifest))
		if code == 0 {
			t.Fatalf("a package whose tool host cannot be executed was accepted: %s", out)
		}
		if !strings.Contains(out, "not executable") {
			t.Errorf("the failure does not say the member cannot be executed: %s", out)
		}
	})

	// A manifest describing another version or another CPU means the archive
	// pinned by CODEX_SHA is not the one this build asked for.
	for name, body := range map[string]string{
		"another version":      strings.Replace(manifest, `"version": "1.2.3"`, `"version": "9.9.9"`, 1),
		"another architecture": strings.Replace(manifest, triple, "aarch64-unknown-linux-musl", 1),
		"another layout":       strings.Replace(manifest, `"layoutVersion": 1`, `"layoutVersion": 2`, 1),
		"a relocated pathDir":  strings.Replace(manifest, `"pathDir": "codex-path"`, `"pathDir": "elsewhere"`, 1),
		"an empty manifest":    "",
		"not json at all":      "this is not a manifest\n",
	} {
		t.Run("with "+name, func(t *testing.T) {
			out, code := validate(t, build(t, "", "", body))
			if code == 0 {
				t.Fatalf("a package with %s was accepted: %s", name, out)
			}
		})
	}
}

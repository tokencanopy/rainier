package driver

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestSessionImageSeedsWorkspace(t *testing.T) {
	if !strings.Contains(sessionDockerfile(t), "mkdir -p /opt/rainier-env/bin /workspace/.rainier") {
		t.Fatal("seed /workspace/.rainier before chown: the deployed CAP_CHOWN-only initializer cannot mkdir in an empty uid-1000-owned mount")
	}
}

// Run against the built candidate, not Alpine: ownership copied from this
// image is precisely the boundary that a generic Docker fixture cannot test.
func TestSessionImageInitializesFreshWorkspace(t *testing.T) {
	image := os.Getenv("RAINIER_SESSION_IMAGE")
	if image == "" {
		t.Skip("set RAINIER_SESSION_IMAGE to exercise the built candidate with Docker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	volume := fmt.Sprintf("rainier-image-contract-%d", time.Now().UnixNano())
	d := NewDocker(DockerOpts{Image: image})
	created, err := d.ensureVolume(ctx, volume, workspaceMount, image)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("expected a fresh volume")
	}
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		if _, err := dockerRun(cleanupCtx, "volume", "rm", volume); err != nil {
			t.Error(err)
		}
	})
	// Two mounts prove the seed survives and ownership is not reset on reuse.
	for i := 0; i < 2; i++ {
		out, err := dockerRun(ctx, "run", "--rm", "--network", "none",
			"--user", sessionUser, "--security-opt", "no-new-privileges",
			"--cap-drop", "ALL", "--read-only", "-v", volume+":"+workspaceMount,
			"--entrypoint", "/bin/sh", image, "-c",
			"test -d /workspace/.rainier && touch /workspace/.rainier/probe && echo workspace-ready")
		if err != nil || strings.TrimSpace(out) != "workspace-ready" {
			t.Fatalf("mount %d: output=%q error=%v", i, out, err)
		}
	}
}

// The session image and this driver are two halves of one contract, and only
// one half is written in Go. The driver runs every container as
// sessionUser with a read-only rootfs, a noexec tmpfs on /tmp, and the
// workspace volume mounted at workspaceMount; the image has to be built so that
// those flags produce a usable session rather than a container that cannot
// write, cannot execute what it builds, or hands its PID 1 to whoever wrote the
// environment's setup script.
//
// docker_test.go already asserts the runtime half against a real daemon, and
// scripts/session-image-smoke.sh asserts the functional half against a real
// build. Neither runs in the ordinary `go test ./...`, which is exactly when a
// Dockerfile edit gets made. These tests read the Dockerfile as text so the
// contract is checked on every commit, on a machine with no docker at all.
//
// Text, and deliberately not a parse: what is being pinned is a handful of
// specific decisions, each with a reason a reader can act on, and a general
// Dockerfile parser would make the failures vaguer rather than sharper.

func sessionDockerfile(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("../../Dockerfile")
	if err != nil {
		t.Fatalf("reading the session image's Dockerfile: %v", err)
	}
	return string(b)
}

func sessionToolchainScript(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("../../images/session/toolchain.sh")
	if err != nil {
		t.Fatalf("reading the session image's toolchain script: %v", err)
	}
	return string(b)
}

// TestSessionImageRunsAsTheSessionUser: the driver passes --user 1000:1000 on
// every create, but nothing stops somebody running the published image without
// it — a `docker run` on a developer's laptop, a registry scanner, a future
// caller. The image's own default has to be the same answer, or "sessions are
// never root" is a property of one call site rather than of the artifact.
func TestSessionImageRunsAsTheSessionUser(t *testing.T) {
	df := sessionDockerfile(t)
	want := "USER " + sessionUser
	if !strings.Contains(df, want) {
		t.Errorf("the Dockerfile does not declare %q; an image whose default user is root is one bad `docker run` away from a root session", want)
	}
	if strings.Contains(df, "USER root") || strings.Contains(df, "USER 0:0") {
		t.Error("the Dockerfile ends on a root USER; the last USER instruction is the one the image ships with")
	}
}

// TestSessionImageEntrypointIsAbsolute is the one that has teeth.
//
// /opt/rainier-env/bin is writable by the session user and FIRST on PATH — that
// is the whole point of the prefix, and it is what makes an environment's setup
// script able to install anything at all. A PATH-resolved `ENTRYPOINT
// ["sessiond"]` therefore resolves through a directory the session user owns:
// a setup script (untrusted in exactly the way design §10 means — an agent,
// possibly prompt-injected, runs in these containers) could drop its own
// `sessiond` there, and `docker commit` would bake it into the cached image
// that every later session of that environment boots as PID 1.
func TestSessionImageEntrypointIsAbsolute(t *testing.T) {
	df := sessionDockerfile(t)
	const want = `ENTRYPOINT ["/usr/local/bin/sessiond"]`
	if !strings.Contains(df, want) {
		t.Errorf("the Dockerfile does not declare %s; a PATH-resolved entrypoint is resolvable through the user-writable %s", want, "/opt/rainier-env/bin")
	}
	if strings.Contains(df, `ENTRYPOINT ["sessiond"]`) {
		t.Error(`ENTRYPOINT ["sessiond"] resolves through PATH, and /opt/rainier-env/bin is on it and writable by the session user`)
	}
}

// TestSessionImagePrefixOwnership: the two halves of the prefix rule. The
// session user owns /opt/rainier-env, because a setup script needs somewhere
// outside /workspace it can write (the snapshot keeps the rootfs and excludes
// the volume). It does NOT own /usr/local, because that is where sessiond and
// the agents live and where the boundary is.
func TestSessionImagePrefixOwnership(t *testing.T) {
	df := sessionDockerfile(t)
	if !strings.Contains(df, "chown -R "+sessionUser+" /opt/rainier-env") {
		t.Errorf("the Dockerfile never gives /opt/rainier-env to %s; an environment's setup script would then have nowhere outside the volume it can install to, and nothing it installs could ever be cached", sessionUser)
	}
	for _, bad := range []string{"chown -R " + sessionUser + " /usr/local", "chown " + sessionUser + " /usr/local"} {
		if strings.Contains(df, bad) {
			t.Errorf("the Dockerfile contains %q; /usr/local/bin holds sessiond and the agents and must stay root-owned", bad)
		}
	}
}

// TestSessionImageWorkspaceMatchesTheDriver: the driver mounts the session's
// volume at workspaceMount and chowns it to sessionUser. The image has to agree
// about both, because docker copies the ownership of the image's directory at a
// mount point onto a freshly created volume (see initVolumeScript): an image
// whose /workspace is root-owned hands every new session a workspace it can
// only read until the driver's init job repairs it.
func TestSessionImageWorkspaceMatchesTheDriver(t *testing.T) {
	df := sessionDockerfile(t)
	if !strings.Contains(df, "WORKDIR "+workspaceMount) {
		t.Errorf("the Dockerfile does not set WORKDIR %s", workspaceMount)
	}
	if !strings.Contains(df, "chown -R "+sessionUser+" /opt/rainier-env "+workspaceMount) &&
		!strings.Contains(df, "chown -R "+sessionUser+" "+workspaceMount) {
		t.Errorf("the Dockerfile never chowns %s to %s", workspaceMount, sessionUser)
	}
}

// TestSessionImageCachesLiveOnTheWorkspace: a session's $HOME is on the
// read-only rootfs and its /tmp is a noexec tmpfs, so a tool whose cache
// defaults under either fails on first write — or, for Go, builds a test binary
// it then cannot execute. Every cache variable the image sets has to name a
// path on the workspace volume, which is the one writable place a session has.
func TestSessionImageCachesLiveOnTheWorkspace(t *testing.T) {
	df := sessionDockerfile(t)
	for _, v := range []string{
		"GOCACHE", "GOMODCACHE", "GOPATH", "GOTMPDIR",
		"XDG_CACHE_HOME", "npm_config_cache", "PIP_CACHE_DIR", "UV_CACHE_DIR",
	} {
		re := regexp.MustCompile(`(?m)^\s*` + regexp.QuoteMeta(v) + `=(\S+)`)
		m := re.FindStringSubmatch(df)
		if m == nil {
			t.Errorf("the image sets no %s; the tool that reads it will try to cache under a read-only $HOME", v)
			continue
		}
		if !strings.HasPrefix(m[1], workspaceMount+"/") {
			t.Errorf("%s is %s, which is not on the writable %s volume", v, m[1], workspaceMount)
		}
	}
}

// TestSessionImageDoesNotRelocateConfiguration: configuration is where tools
// write credentials, and /workspace is what checkpoints, archives and `rainier
// pull` carry off the runner. Moving XDG_CONFIG_HOME onto the workspace would
// route the next tool that decides to persist a token straight into them. A
// read-only $HOME is the wanted outcome: the write fails loudly instead.
func TestSessionImageDoesNotRelocateConfiguration(t *testing.T) {
	df := sessionDockerfile(t)
	for _, v := range []string{"XDG_CONFIG_HOME", "GH_CONFIG_DIR", "GH_TOKEN", "GITHUB_TOKEN"} {
		if regexp.MustCompile(`(?m)^\s*` + regexp.QuoteMeta(v) + `=`).MatchString(df) {
			t.Errorf("the image sets %s; credentials must not be routed onto the workspace volume, and no token belongs in an image at all", v)
		}
	}
}

// TestSessionImageInstallsNoEscalationPath: no-new-privileges already blocks
// setuid escalation at runtime, and every capability is dropped. Shipping sudo
// anyway would mean the image's safety depended entirely on flags at the call
// site, and one create that forgot them would be a root session.
func TestSessionImageInstallsNoEscalationPath(t *testing.T) {
	df := sessionDockerfile(t)
	if regexp.MustCompile(`(?m)^\s+sudo\b|\bsudo \\|\s+sudo$`).MatchString(df) {
		t.Error("the Dockerfile installs sudo; a session user who can escalate makes every other boundary in the image decorative")
	}
}

// TestSessionImageBasesArePinned: every base is a digest, and every version the
// build resolves is written down. An image that resolved `latest` at build time
// would be a different image on every rebuild, and the digest an operator pastes
// into the Dedicated root's runner_artifacts would describe nothing repeatable.
func TestSessionImageBasesArePinned(t *testing.T) {
	df := sessionDockerfile(t)
	stages := map[string]bool{}
	for _, line := range strings.Split(df, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "FROM ") {
			continue
		}
		fields := strings.Fields(line)
		ref := fields[1]
		if len(fields) >= 4 && strings.EqualFold(fields[2], "AS") {
			stages[fields[3]] = true
		}
		switch {
		case strings.Contains(ref, "@sha256:"):
		case strings.HasPrefix(ref, "${") && strings.HasSuffix(ref, "}"):
			// An ARG, which is checked below by its default value.
		case stages[ref]:
			// An earlier stage of this same build.
		default:
			t.Errorf("FROM %s is not pinned by digest; a tag can be moved under a published image", ref)
		}
	}
	base := regexp.MustCompile(`(?m)^ARG BASE_IMAGE=(\S+)`).FindStringSubmatch(df)
	if base == nil {
		t.Fatal("the Dockerfile declares no BASE_IMAGE ARG")
	}
	if !strings.Contains(base[1], "@sha256:") {
		t.Errorf("BASE_IMAGE defaults to %s, which is not a digest; the default is what CI publishes", base[1])
	}
	if strings.Contains(df, ":latest") {
		t.Error("the Dockerfile names a :latest tag somewhere; nothing in a published session image may be resolved at build time")
	}
}

// TestSessionToolchainIsChecksumVerified: every upstream artifact the image
// downloads is checked against a SHA-256 recorded in the repository before it is
// extracted, and none of them is a bootstrap script that installs whatever is
// current. A session image that fetched "the latest" would change under a digest
// that was supposed to describe it.
func TestSessionToolchainIsChecksumVerified(t *testing.T) {
	sh := sessionToolchainScript(t)
	if !strings.Contains(sh, "sha256sum --check") {
		t.Error("the toolchain script never verifies a checksum")
	}
	sums := regexp.MustCompile(`[A-Z_]+SHA=([0-9a-f]{64})`).FindAllString(sh, -1)
	if len(sums) < 12 {
		t.Errorf("the toolchain script records %d checksums; it fetches six artifacts for each of two architectures, so twelve is the floor", len(sums))
	}
	// A whole shell word, so `| sha256sum --check` — which is the opposite of
	// the thing being forbidden — does not read as a pipe into sh.
	if regexp.MustCompile(`\|\s*(ba)?sh(\s|$)`).MatchString(sh) {
		t.Error("the toolchain script pipes a download into a shell; an installer script resolves its own contents at build time and cannot be pinned")
	}
	// A fetch whose URL names no version is a moving target even with a sum:
	// the sum will simply stop matching one day and the build will fail for a
	// reason nobody can act on.
	for _, moving := range []string{"/latest/download", "releases/latest", "?mode=json"} {
		if strings.Contains(sh, moving) {
			t.Errorf("the toolchain script resolves %q at build time instead of naming a version", moving)
		}
	}
}

// TestSessionImageCarriesTheDeveloperToolset: the environment a developer is
// promised, asserted as a list rather than as prose. scripts/session-image-smoke.sh
// proves each one actually works; this proves nobody quietly dropped one from
// the install while editing something else.
func TestSessionImageCarriesTheDeveloperToolset(t *testing.T) {
	df := sessionDockerfile(t)
	sh := sessionToolchainScript(t)
	both := df + "\n" + sh
	for _, want := range []string{
		"build-essential", "pkg-config", "make",
		"python3", "python3-venv", "python3-pip",
		"git", "openssh-client",
		"ca-certificates", "curl",
		"coreutils", "findutils", "grep", "sed", "gawk", "diffutils", "patch",
		"tar", "gzip", "xz-utils", "zip", "unzip",
		"procps", "iproute2", "lsof", "dnsutils",
		"file", "less", "nano", "rsync",
		"GO_VERSION", "GH_VERSION", "CODEX_VERSION", "UV_VERSION",
		"RIPGREP_VERSION", "JQ_VERSION", "CLAUDE_CODE_VERSION",
	} {
		if !strings.Contains(both, want) {
			t.Errorf("the session image no longer provides %q; the quickstart and docs/session-image.md promise it", want)
		}
	}
}

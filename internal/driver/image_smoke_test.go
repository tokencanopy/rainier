package driver

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestImageSmokeRejectsFailedProbes(t *testing.T) {
	lib, err := filepath.Abs("../../scripts/session-image-checks.sh")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, program, marker, want string }{
		{"success", "printf expected", "expected", "PASS"},
		{"wrong output", "printf unexpected", "missing", "FAIL"},
		{"marker then failure", "printf expected; exit 42", "expected", "FAIL"},
		{"marker then timeout", "printf expected; exit 124", "expected", "FAIL"},
		{"marker then signal", "printf expected; kill -TERM $$", "expected", "FAIL"},
		{"wrong agent version", "printf 'codex-cli 0.0.1'", "codex-cli 0.0.2", "FAIL"},
		{"missing agent version", "printf 'codex-cli'", "codex-cli 0.0.2", "FAIL"},
		{"version suffix collision", "printf 'codex-cli 0.0.20'", "codex-cli 0.0.2", "FAIL"},
		{"version prefix collision", "printf 'unexpected codex-cli 0.0.2'", "codex-cli 0.0.2", "FAIL"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command("/bin/bash", "-c", `
source "$1"
ok() { printf 'PASS\n'; }
bad() { printf 'FAIL\n'; }
probe() { /bin/bash -c "$1"; }
check example "$3" "$2" probe exact
`, "smoke-test", lib, tc.program, tc.marker)
			out, err := cmd.CombinedOutput()
			if err != nil || strings.TrimSpace(string(out)) != tc.want {
				t.Fatalf("want %s, got %q (error %v)", tc.want, out, err)
			}
		})
	}
}

// TestBrokeredGHProbeRequiresChildAndFixtureSuccess exercises the exact inner
// smoke probe instead of reimplementing its status handling here. The fake gh
// emits the expected marker so each failure proves the probe carries that
// process status through to the outer check.
func TestBrokeredGHProbeRequiresChildAndFixtureSuccess(t *testing.T) {
	lib, err := filepath.Abs("../../scripts/session-image-checks.sh")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name       string
		ghStatus   string
		serverExit string
		pass       bool
	}{
		{"success", "0", "0", true},
		{"marker then gh exit 42", "42", "0", false},
		{"marker then fixture failure", "0", "7", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			// macOS's AF_UNIX pathname limit is shorter than t.TempDir's full
			// test-name path. Keep all artifacts in t.TempDir while passing the
			// fixture a short symlinked socket path.
			shortDir := filepath.Join("/tmp", "ghp-"+strconv.Itoa(os.Getpid()))
			if err := os.Symlink(dir, shortDir); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.Remove(shortDir) })
			bin := filepath.Join(dir, "bin")
			if err := os.Mkdir(bin, 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(bin, "gh"), []byte("#!/bin/sh\nexec \"$RAINIER_GH_SMOKE_TEST_BINARY\" -test.run '^TestBrokeredGHProbeFakeGHHelper$'\n"), 0755); err != nil {
				t.Fatal(err)
			}
			testBinary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("/bin/bash", "-c", `
source "$1"
brokered_gh_probe
`, "brokered-gh-probe", lib)
			cmd.Env = append(os.Environ(),
				"PATH="+bin+":"+os.Getenv("PATH"),
				"RAINIER_SMOKE_AGENT_SOCKET="+filepath.Join(shortDir, "agent.sock"),
				"RAINIER_SMOKE_GH_EXIT="+tc.ghStatus,
				"RAINIER_SMOKE_FIXTURE_EXIT="+tc.serverExit,
				"RAINIER_GH_SMOKE_TEST_BINARY="+testBinary,
				"RAINIER_GH_SMOKE_HELPER=1",
			)
			out, err := cmd.CombinedOutput()
			if (err == nil) != tc.pass {
				t.Fatalf("pass=%t, output=%q, err=%v", tc.pass, out, err)
			}
			if tc.pass && strings.TrimSpace(string(out)) != "synthetic-gh-token" {
				t.Fatalf("success output=%q, want marker", out)
			}
		})
	}
}

// TestBrokeredGHProbeFakeGHHelper is the synthetic gh process used by the
// probe regression. It performs the actual socket request, prints the fixture
// token, then takes the requested status so the test exercises child failure
// masking at the probe boundary.
func TestBrokeredGHProbeFakeGHHelper(t *testing.T) {
	if os.Getenv("RAINIER_GH_SMOKE_HELPER") == "" {
		return
	}
	c, err := net.Dial("unix", os.Getenv("RAINIER_SMOKE_AGENT_SOCKET"))
	if err != nil {
		os.Exit(97)
	}
	defer c.Close()
	if err := json.NewEncoder(c).Encode(map[string]any{"method": "mint_git_credential", "payload": map[string]any{}}); err != nil {
		os.Exit(98)
	}
	var response struct {
		Payload struct {
			Token string `json:"token"`
		} `json:"payload"`
	}
	if err := json.NewDecoder(c).Decode(&response); err != nil || response.Payload.Token != "synthetic-gh-token" {
		os.Exit(99)
	}
	fmt.Fprintln(os.Stdout, response.Payload.Token)
	status, err := strconv.Atoi(os.Getenv("RAINIER_SMOKE_GH_EXIT"))
	if err != nil {
		os.Exit(96)
	}
	os.Exit(status)
}

func TestImageSmokeSetupExecution(t *testing.T) {
	lib, _ := filepath.Abs("../../scripts/session-image-checks.sh")
	for _, status := range []string{"0", "42", "124"} {
		cmd := exec.Command("/bin/bash", "-c", `
source "$1"
setup_status=$2
ok() { echo PASS; }
bad() { echo FAIL; }
probe() { echo wrong-executor; }
probe_setup() { echo expected; return "$setup_status"; }
check setup expected ignored probe_setup
`, "test", lib, status)
		out, err := cmd.CombinedOutput()
		want := "FAIL"
		if status == "0" {
			want = "PASS"
		}
		if err != nil || strings.TrimSpace(string(out)) != want {
			t.Fatalf("status %s: want %s, got %q (%v)", status, want, out, err)
		}
	}
}

func TestImageSmokeExpectedRefusal(t *testing.T) {
	lib, _ := filepath.Abs("../../scripts/session-image-checks.sh")
	for _, tc := range []struct {
		program string
		pass    bool
	}{
		{"echo refused; exit 1", true},
		{"echo refused; exit 0", false},
		{"echo refused; exit 124", false},
		{"echo refused; exit 127", false},
		{"echo refused; kill -TERM $$", false},
		{"echo different; exit 1", false},
	} {
		cmd := exec.Command("/bin/bash", "-c", `source "$1"; expect_refusal 1 refused /bin/bash -c "$2"`, "test", lib, tc.program)
		out, err := cmd.CombinedOutput()
		if (err == nil) != tc.pass {
			t.Errorf("%s: pass=%t, got %q (%v)", tc.program, tc.pass, out, err)
		}
	}
}

func TestImageFleetUsesSharedBuild(t *testing.T) {
	// Stop at the build boundary: this must never start a local fleet.
	bin := t.TempDir()
	for name, script := range map[string]string{
		"docker": "#!/bin/sh\nexit 73\n",
		"make":   "#!/bin/sh\nprintf '%s\\n' \"$*\" \"$BUILD_ARGS\"\nexit 72\n",
	} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(script), 0755); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command("/bin/bash", "../../scripts/fleet-up.sh")
	cmd.Env = []string{"PATH=" + bin, "BUILD_ARGS=--build-arg BASE_IMAGE=node:22-bookworm"}
	out, err := cmd.CombinedOutput()
	if err == nil || cmd.ProcessState.ExitCode() != 72 || !strings.Contains(string(out), "session-image SESSION_IMAGE=rainier-session:latest") || !strings.Contains(string(out), "--build-arg BASE_IMAGE=node:22-bookworm") {
		t.Fatalf("fleet did not delegate image build with override: %q (%v)", out, err)
	}
}

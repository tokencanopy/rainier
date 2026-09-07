package driver

import (
	"os/exec"
	"path/filepath"
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command("/bin/bash", "-c", `
source "$1"
ok() { printf 'PASS\n'; }
bad() { printf 'FAIL\n'; }
probe() { /bin/bash -c "$1"; }
check example "$3" "$2"
`, "smoke-test", lib, tc.program, tc.marker)
			out, err := cmd.CombinedOutput()
			if err != nil || strings.TrimSpace(string(out)) != tc.want {
				t.Fatalf("want %s, got %q (error %v)", tc.want, out, err)
			}
		})
	}
}

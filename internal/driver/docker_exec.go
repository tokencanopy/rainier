// internal/driver/docker_exec.go
package driver

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func dockerPath() (string, error) {
	if p, err := exec.LookPath("docker"); err == nil {
		return p, nil
	}
	fallback := "/Applications/Docker.app/Contents/Resources/bin/docker"
	if _, err := os.Stat(fallback); err == nil {
		return fallback, nil
	}
	return "", fmt.Errorf("docker CLI not found on PATH or at %s", fallback)
}

func dockerRun(ctx context.Context, args ...string) (string, error) {
	bin, err := dockerPath()
	if err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, bin, args...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(errb.String()))
	}
	return strings.TrimSpace(out.String()), nil
}

// used by the test helper
func mustDockerPath(t interface{ Fatal(...any) }) string {
	p, err := dockerPath()
	if err != nil {
		t.Fatal(err)
	}
	return p
}

var _ = filepath.Base

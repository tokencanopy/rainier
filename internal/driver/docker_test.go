// internal/driver/docker_test.go
package driver

import (
	"context"
	"os/exec"
	"testing"
)

func dockerAvailable(t *testing.T) {
	t.Helper()
	if _, err := dockerPath(); err != nil {
		t.Skip("docker CLI not found; skipping docker driver test")
	}
	if err := exec.Command(mustDockerPath(t), "info").Run(); err != nil {
		t.Skip("docker daemon not responding; skipping")
	}
}

func TestDockerDriverContract(t *testing.T) {
	dockerAvailable(t)
	RunContract(t, func(t *testing.T) (Driver, func()) {
		d := NewDocker(DockerOpts{
			Image:      "alpine:3.20",
			Network:    "bridge", // plain bridge for the driver test; internal net is a fleet concern
			TotalSlots: 8,
			Label:      "rainier.test",
		})
		// The contract creates containers with Cmd empty; alpine's default cmd
		// exits immediately, so override to a sleeper for lifecycle assertions.
		d.defaultCmd = []string{"sleep", "3600"}
		return d, func() { d.destroyAllLabeled(context.Background()) }
	})
}

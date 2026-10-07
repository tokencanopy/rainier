package controld

import (
	"strings"
	"testing"

	"github.com/tokencanopy/rainier/protocol/runner"
)

func TestReconnectSecretResponseCannotBreakRunnerFrame(t *testing.T) {
	// JSON escaping can exceed the wire budget even below the source byte count.
	for _, value := range []string{strings.Repeat("x", runner.GuestReconnectConfigurationLimit), strings.Repeat("\x00", runner.GuestReconnectConfigurationLimit/5)} {
		if payload, err := encodeReconnectAnswer(runner.GuestReconnectEnrollResponse{Env: map[string]string{"SYNTHETIC_TEST": value}}); err == nil || len(payload) != 0 {
			t.Fatal("oversized environment escaped response bound")
		}
	}
	if _, err := encodeReconnectAnswer(runner.GuestReconnectEnrollResponse{Env: map[string]string{"SYNTHETIC_TEST": "value"}}); err != nil {
		t.Fatal(err)
	}
}

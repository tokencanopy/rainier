package driver

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tokencanopy/rainier/protocol/runner"
)

type resumeConfigurationHost struct {
	*stubMicrovmHost
	config runner.BootConfig
	err    error
}

func (h *resumeConfigurationHost) GuestResumeConfiguration(context.Context, string, uint64) (runner.BootConfig, error) {
	return h.config, h.err
}

func TestRecoveredColdResumeResolvesGuestConfiguration(t *testing.T) {
	for _, fault := range []string{"none", "refused", "session", "token", "protocol"} {
		t.Run(fault, func(t *testing.T) {
			ctx := context.Background()
			dir := shortTempDir(t)
			engine := &coldIdentityEngine{SimulatedEngine: NewSimulatedEngineWithDir(dir), identity: guestHostIdentity{BootID: "boot_test", StartTime: 1, NamespaceDevice: 1, NamespaceInode: 1}}
			m, _ := testMicrovm(t, MicrovmOpts{StateDir: dir, Engine: engine, GuestReconnect: true})
			m.SetHost(&stubMicrovmHost{})
			h, err := m.Create(ctx, Spec{SessionID: "session_test", GuestReconnect: 1, PlacementGeneration: 1})
			if err != nil {
				t.Fatal(err)
			}
			if err = m.Suspend(ctx, h.ID, false); err != nil {
				t.Fatal(err)
			}
			stopTestMicrovmRunner(t, m)
			recovered, _ := testMicrovm(t, m.opts)
			host := &resumeConfigurationHost{stubMicrovmHost: &stubMicrovmHost{}, config: runner.BootConfig{Protocol: runner.SessionBootstrapProtocolVersion, SessionID: "session_test", GuestReconnect: 1, Cmd: []string{"synthetic-agent"}, Env: map[string]string{"CONFIG_TEST": "fresh_configuration_test"}}}
			switch fault {
			case "refused":
				host.err = errors.New("synthetic refusal")
			case "session":
				host.config.SessionID = "other_test"
			case "token":
				host.config.BootstrapToken = "old_token_test"
			case "protocol":
				host.config.GuestReconnect = 0
			}
			recovered.SetHost(host)
			defer recovered.Destroy(ctx, h.ID)
			restarted, err := recovered.ResumePlacement(ctx, h.ID, 2)
			if fault != "none" {
				if err == nil || restarted || host.mintCount() != 0 {
					t.Fatal("invalid configuration launched or minted")
				}
				if state, _ := engine.State(ctx, h.ID); state == VMMStateRunning {
					t.Fatal("invalid configuration booted")
				}
				return
			}
			if err != nil || !restarted {
				t.Fatalf("recovered cold resume: %v", err)
			}
			cfg := readBootConfig(t, dialGuest(t, recovered, h.ID, recovered.instances[h.ID].boots))
			if cfg.SessionID != "session_test" || cfg.Env["CONFIG_TEST"] != "fresh_configuration_test" || len(cfg.Cmd) != 1 || cfg.Cmd[0] != "synthetic-agent" || cfg.BootstrapToken == "" || host.mintCount() != 1 {
				t.Fatal("fresh configuration/token missing")
			}
			data, err := os.ReadFile(filepath.Join(dir, "instances", h.ID, "instance.json"))
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(data), "fresh_configuration_test") || strings.Contains(string(data), cfg.BootstrapToken) {
				t.Fatal("guest configuration persisted to metadata")
			}
		})
	}
}

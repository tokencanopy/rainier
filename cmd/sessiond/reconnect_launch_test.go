package main

import (
	"github.com/tokencanopy/rainier/protocol/runner"
	"testing"
)

func TestReconnectRejectsChangedBootArtifactsBeforeRedemption(t *testing.T) {
	for _, field := range []string{"command", "repository", "git", "setup", "proxy", "agent-manifest"} {
		t.Run(field, func(t *testing.T) {
			b, ch := reconnectFixture(t)
			guest, host, ctx := reconnectPair(t)
			done := make(chan error, 1)
			go func() { done <- reBootstrap(ctx, guest, b); guest.Close() }()
			accepted := acceptProof(t, ctx, host, b, ch, 1)
			cfg := runner.BootConfig{Protocol: 1, GuestReconnect: 1, SessionID: "session-test", BootstrapToken: accepted.Token}
			switch field {
			case "command":
				cfg.Cmd = []string{"different_test"}
			case "repository":
				cfg.Repos = []runner.RepoSpec{{Owner: "example", Name: "changed"}}
			case "git":
				cfg.GitAuthorName = "Changed Test"
			case "setup":
				cfg.Setup = "echo changed_test"
			case "proxy":
				cfg.ProxyURL = "http://proxy.example.invalid:80"
			case "agent-manifest":
				cfg.Env = map[string]string{"RAINIER_AGENTS_B64": "changed_test"}
			}
			sendReconnectConfig(t, ctx, host, cfg)
			if raw, err := host.Read(ctx); err == nil || len(raw) != 0 {
				t.Fatal("changed launch artifacts reached token redemption")
			}
			if err := <-done; err != errGuestReconnect {
				t.Fatal("changed launch artifacts accepted")
			}
		})
	}
}

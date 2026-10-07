package runnerd

import (
	"context"
	"encoding/json"

	"github.com/tokencanopy/rainier/protocol/runner"
	"testing"
	"time"
)

func TestResumeConfigurationRequiresCurrentClaim(t *testing.T) {
	for _, fault := range []string{"none", "session", "placement", "handle", "boot", "control", "claim", "refused", "canceled"} {
		t.Run(fault, func(t *testing.T) {
			s, _, control := reconnectPeer(t)
			s.reg.mu.Lock()
			row := s.reg.items["session_test"]
			row.state = "resuming"
			row.resumePending = true
			row.guestReconnect = true
			s.reg.mu.Unlock()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			type result struct {
				config runner.BootConfig
				err    error
			}
			done := make(chan result, 1)
			go func() { cfg, err := s.GuestResumeConfiguration(ctx, "session_test", 3); done <- result{cfg, err} }()
			req := control.readMsg(t)
			if req.RPC == nil || req.RPC.Method != runner.MethodGuestReconnectConfiguration || !isRunnerOriginated(req.RPC.ID) {
				t.Fatal("resume did not request current runner-only configuration")
			}
			cfg := runner.GuestReconnectConfiguration{Protocol: 1, SessionID: "session_test", PlacementGeneration: 3, Spec: &runner.Spec{Cmd: []string{"synthetic-agent"}, Env: map[string]string{"CONFIG_TEST": "fresh"}}}
			switch fault {
			case "session":
				cfg.SessionID = "another_test"
			case "placement":
				cfg.PlacementGeneration = 4
			case "control":
				s.reconnectControl.Load().state.generation.Add(1)
			case "canceled":
				cancel()
			case "handle", "boot", "claim":
				s.reg.mu.Lock()
				row := s.reg.items["session_test"]
				if fault == "handle" {
					row.handle = "replacement_test"
				}
				if fault == "boot" {
					row.boot++
				}
				if fault == "claim" {
					row.resumePending = false
				}
				s.reg.mu.Unlock()
			}
			body, _ := json.Marshal(cfg)
			replyReconnect(t, control, req, fault != "refused", body)
			got := <-done
			if fault == "none" {
				if got.err != nil || got.config.SessionID != "session_test" || got.config.Env["CONFIG_TEST"] != "fresh" || got.config.GuestReconnect != 1 || got.config.BootstrapToken != "" {
					t.Fatalf("fresh configuration unavailable: %v", got.err)
				}
			} else if got.err == nil || got.config.SessionID != "" {
				t.Fatal("stale or refused configuration escaped")
			}
		})
	}
}

func TestResumeConfigurationRejectsUnclaimedSession(t *testing.T) {
	s, _, _ := reconnectPeer(t)
	if _, err := s.GuestResumeConfiguration(context.Background(), "session_test", 3); err == nil {
		t.Fatal("running session resolved cold configuration")
	}
	if _, err := s.GuestResumeConfiguration(context.Background(), "session_test", 0); err == nil {
		t.Fatal("unversioned resume resolved configuration")
	}
}

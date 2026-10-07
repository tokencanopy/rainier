package runnerd

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/tokencanopy/rainier/protocol/runner"
)

func TestRecoveredGuestPlacementRequiresCurrentAuthorization(t *testing.T) {
	for _, fault := range []string{"none", "scope", "replaced", "control", "refused"} {
		t.Run(fault, func(t *testing.T) {
			s, _, control := reconnectPeer(t)
			s.reg.mu.Lock()
			entry := s.reg.items["session_test"]
			entry.placementGen = 0
			entry.recovered = true
			entry.guestReconnect = true
			row := *entry
			s.reg.mu.Unlock()
			rc := s.reconnectControl.Load()
			done := make(chan error, 1)
			go func() { done <- s.authorizeRecoveredGuest(context.Background(), rc, row) }()
			req := control.readMsg(t)
			if req.RPC.Method != runner.MethodGuestReconnectConfiguration {
				t.Fatal("recovery did not reauthorize current placement")
			}
			cfg := runner.GuestReconnectConfiguration{Protocol: 1, SessionID: "session_test", PlacementGeneration: 3, Spec: &runner.Spec{}}
			switch fault {
			case "scope":
				cfg.SessionID = "another_test"
			case "replaced":
				s.reg.mu.Lock()
				s.reg.items[row.id].handle = "replacement_vm"
				s.reg.mu.Unlock()
			case "control":
				rc.state.generation.Add(1)
			}
			payload, _ := json.Marshal(cfg)
			replyReconnect(t, control, req, fault != "refused", payload)
			err := <-done
			after, _ := s.reg.snapshot(row.id)
			if fault == "none" {
				if err != nil || after.placementGen != 3 {
					t.Fatalf("current authorization not installed: %v", err)
				}
			} else if err == nil || after.placementGen != 0 {
				t.Fatal("stale authorization installed")
			}
		})
	}
}

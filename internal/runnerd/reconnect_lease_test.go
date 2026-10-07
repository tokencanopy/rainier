package runnerd

import (
	"context"
	"testing"
)

func TestReconnectLeaseRetainsAdmissionThroughDelivery(t *testing.T) {
	s, _, _ := reconnectPeer(t)
	lease, err := s.acquireGuestReconnect("session_test")
	if err != nil {
		t.Fatal(err)
	}
	defer lease.close()
	if !lease.valid(context.Background()) {
		t.Fatal("new lease is invalid")
	}
	if _, err := s.acquireGuestReconnect("session_test"); err != errReconnectUnavailable {
		t.Fatalf("parallel delivery admitted: %v", err)
	}
	lease.close()
	next, err := s.acquireGuestReconnect("session_test")
	if err != nil {
		t.Fatal(err)
	}
	defer next.close()
	lease.close()
	if _, err := s.acquireGuestReconnect("session_test"); err != errReconnectUnavailable {
		t.Fatalf("old close released new lease: %v", err)
	}
	if lease.valid(context.Background()) {
		t.Fatal("closed lease retained authority")
	}
}

func TestReconnectLeaseRejectsOwnershipChanges(t *testing.T) {
	for _, change := range []string{"handle", "boot", "placement", "state", "deleted", "control", "generation", "caller"} {
		t.Run(change, func(t *testing.T) {
			s, _, _ := reconnectPeer(t)
			lease, err := s.acquireGuestReconnect("session_test")
			if err != nil {
				t.Fatal(err)
			}
			defer lease.close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s.reg.mu.Lock()
			row := s.reg.items["session_test"]
			switch change {
			case "handle":
				row.handle = "replacement_test"
			case "boot":
				row.boot++
			case "placement":
				row.placementGen++
			case "state":
				row.state = "suspending"
			case "deleted":
				delete(s.reg.items, "session_test")
			case "control":
				s.reconnectControl.Store(nil)
			case "generation":
				lease.control.state.generation.Add(1)
			case "caller":
				cancel()
			}
			s.reg.mu.Unlock()
			if lease.valid(ctx) {
				t.Fatal("changed ownership retained authority")
			}
		})
	}
}

package runnerd

import (
	"context"
	"encoding/json"
	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/runner"
	"net"
	"strings"
	"testing"
	"time"
)

func TestGuestEnrollmentRejectsLegacyExchangeBeforePublication(t *testing.T) {
	s, _, _ := reconnectPeer(t)
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.enrollGuestConnection(ctx, "session_test", relay.NetConn(a)) }()
	event := relay.ControlEvent{Kind: "req:" + runner.MethodFetchSessionSecrets, ID: 1, Payload: json.RawMessage(`{"protocol":1,"token":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}`)}
	if err := writeGuestControl(ctx, relay.NetConn(b), event); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err == nil {
		t.Fatal("legacy exchange accepted for enrolled boot")
	}
	if _, ok := s.reg.hub("session_test"); ok {
		t.Fatal("legacy guest published")
	}
}

func TestGuestEnrollmentPublishesOnlyAfterCommittedExchange(t *testing.T) {
	s, _, control := reconnectPeer(t)
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.enrollGuestConnection(ctx, "session_test", relay.NetConn(a)) }()
	guest := relay.NetConn(b)
	payload, _ := json.Marshal(runner.GuestReconnectEnrollRequest{Protocol: 1, Token: strings.Repeat("A", 43), BootEpoch: "boot_test", PublicKey: strings.Repeat("A", 43)})
	if err := writeGuestControl(ctx, guest, relay.ControlEvent{Kind: "req:" + runner.MethodEnrollGuestReconnect, ID: 1, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	req := control.readMsg(t)
	if req.RPC.Method != runner.MethodEnrollGuestReconnect || !isRunnerOriginated(req.RPC.ID) {
		t.Fatal("enrollment not scoped to host")
	}
	if _, ok := s.reg.hub("session_test"); ok {
		t.Fatal("guest published before enrollment committed")
	}
	replyReconnect(t, control, req, true, []byte(`{"env":{}}`))
	raw, err := guest.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	frame, _ := relay.Decode(raw)
	var event relay.ControlEvent
	json.Unmarshal(frame.Payload, &event)
	if event.Kind != "resp" || event.ID != 1 || !event.OK {
		t.Fatal("enrollment answer missing")
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	hub, ok := s.reg.hub("session_test")
	if !ok {
		t.Fatal("enrolled guest not published")
	}
	hub.Close()
}

func TestColdGuestEnrollmentWaitsForClaimedDriverResult(t *testing.T) {
	s, _, control := reconnectPeer(t)
	s.reg.mu.Lock()
	row := s.reg.items["session_test"]
	row.state = "suspended"
	generation := row.placementGen + 1
	s.reg.mu.Unlock()
	if _, err := s.claimResumePlacement("session_test", row.handle, generation); err != nil {
		t.Fatal(err)
	}
	claimed, _ := s.reg.snapshot("session_test")
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.enrollGuestConnection(ctx, "session_test", relay.NetConn(a)) }()
	guest := relay.NetConn(b)
	payload, _ := json.Marshal(runner.GuestReconnectEnrollRequest{Protocol: 1, Token: strings.Repeat("A", 43), BootEpoch: "boot_new", PublicKey: strings.Repeat("A", 43)})
	written := make(chan error, 1)
	go func() {
		written <- writeGuestControl(ctx, guest, relay.ControlEvent{Kind: "req:" + runner.MethodEnrollGuestReconnect, ID: 1, Payload: payload})
	}()
	select {
	case err := <-written:
		t.Fatalf("pending launch read enrollment: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	s.reg.resumed("session_test", true)
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	req := control.readMsg(t)
	if req.RPC == nil || req.RPC.Method != runner.MethodEnrollGuestReconnect {
		t.Fatal("fresh guest did not enroll")
	}
	replyReconnect(t, control, req, true, []byte(`{"env":{}}`))
	if _, err := guest.Read(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	current, _ := s.reg.snapshot("session_test")
	if current.boot != claimed.boot || current.placementGen != generation || current.hub == nil || current.guestEpoch != 0 {
		t.Fatal("enrollment published outside claimed fresh boot")
	}
	current.hub.Close()
}

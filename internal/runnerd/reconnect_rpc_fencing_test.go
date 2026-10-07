package runnerd

import (
	"context"
	"encoding/json"
	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/runner"
	"testing"
	"time"
)

type reconnectRPCCapture struct{ writes chan []byte }

func (c *reconnectRPCCapture) Read(ctx context.Context) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (c *reconnectRPCCapture) Write(ctx context.Context, b []byte) error { c.writes <- b; return nil }
func (c *reconnectRPCCapture) Close() error                              { return nil }
func TestReconnectRejectsCanceledControlCommand(t *testing.T) {
	s, _ := testMicrovmServer(t)
	s.reg.put("session_test", &sessionEntry{id: "session_test", handle: "vm_test", state: "running", placementGen: 3, guestReconnect: true})
	c := &reconnectRPCCapture{make(chan []byte, 1)}
	h := relay.NewHub(context.Background(), c)
	defer h.Close()
	s.reg.setHub("session_test", h)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.execute(ctx, runner.ToRunner{Type: "session_rpc", Session: "session_test", RPC: &runner.RPCEnvelope{ID: 17, Method: "exec", Payload: []byte(`{"command":"synthetic"}`)}}, func(runner.FromRunner) {}, AgentConfig{}, &agentSessionState{})
	select {
	case <-c.writes:
		t.Fatal("canceled superseded control command reached current guest hub")
	case <-time.After(50 * time.Millisecond):
	}
}
func TestReconnectRejectsUncorrelatedResponse(t *testing.T) {
	s, _ := testMicrovmServer(t)
	s.reg.put("session_test", &sessionEntry{id: "session_test", handle: "vm_test", state: "running", placementGen: 3, guestReconnect: true})
	c := &reconnectRPCCapture{make(chan []byte, 1)}
	h := relay.NewHub(context.Background(), c)
	defer h.Close()
	s.reg.setHub("session_test", h)
	s.forwardSessionRPC(runner.ToRunner{Type: "session_rpc", Session: "session_test", RPC: &runner.RPCEnvelope{ID: 17, Method: "resp", OK: true, Payload: []byte(`{"token":"synthetic"}`)}}, func(runner.FromRunner) {})
	select {
	case <-c.writes:
		t.Fatal("uncorrelated old response reached replacement guest hub")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestReconnectRPCReplyKeepsOriginalHubAndGuestID(t *testing.T) {
	s, _, control := reconnectPeer(t)
	oldConn := &reconnectRPCCapture{writes: make(chan []byte, 2)}
	oldHub := relay.NewHub(context.Background(), oldConn)
	defer oldHub.Close()
	s.reg.mu.Lock()
	s.reg.items["session_test"].guestReconnect = true
	s.reg.mu.Unlock()
	s.reg.setHub("session_test", oldHub)
	lease, err := s.acquireGuestReconnect("session_test")
	if err != nil {
		t.Fatal(err)
	}
	defer lease.close()
	sendRequest := func(hub *relay.Hub) runner.FromRunner {
		s.sendOwnedGuestMessage(lease, hub, runner.FromRunner{Type: "session_req", Session: "session_test", RPC: &runner.RPCEnvelope{ID: 1, Method: "credential_test"}})
		return control.readMsg(t)
	}
	first := sendRequest(oldHub)
	// A same-owner answer retains the original guest ID.
	s.forwardSessionRPC(runner.ToRunner{Session: "session_test", RPC: &runner.RPCEnvelope{ID: first.RPC.ID, Method: "resp", OK: true}}, func(runner.FromRunner) {})
	select {
	case raw := <-oldConn.writes:
		frame, _ := relay.Decode(raw)
		var event relay.ControlEvent
		if json.Unmarshal(frame.Payload, &event) != nil || event.ID != 1 || event.Kind != "resp" {
			t.Fatal("guest correlation lost")
		}
	case <-time.After(time.Second):
		t.Fatal("current reply dropped")
	}
	late := sendRequest(oldHub)
	nextConn := &reconnectRPCCapture{writes: make(chan []byte, 2)}
	nextHub := relay.NewHub(context.Background(), nextConn)
	defer nextHub.Close()
	s.reg.setHub("session_test", nextHub)
	current := sendRequest(nextHub)
	if current.RPC.ID == late.RPC.ID {
		t.Fatal("reused forward ID across guest muxes")
	}
	s.forwardSessionRPC(runner.ToRunner{Session: "session_test", RPC: &runner.RPCEnvelope{ID: late.RPC.ID, Method: "resp", OK: true}}, func(runner.FromRunner) {})
	select {
	case <-nextConn.writes:
		t.Fatal("old reply reached replacement")
	case <-time.After(50 * time.Millisecond):
	}
	s.forwardSessionRPC(runner.ToRunner{Session: "session_test", RPC: &runner.RPCEnvelope{ID: current.RPC.ID, Method: "resp", OK: true}}, func(runner.FromRunner) {})
	select {
	case <-nextConn.writes:
	case <-time.After(time.Second):
		t.Fatal("new mux reply dropped")
	}
}

func TestReconnectCommandCannotAdoptHubAfterReceipt(t *testing.T) {
	s, _, _ := reconnectPeer(t)
	s.reg.mu.Lock()
	s.reg.items["session_test"].guestReconnect = true
	s.reg.mu.Unlock()
	rc := s.reconnectControl.Load()
	target := s.guestRPCTarget(rc.ctx, "session_test", rc.state)
	conn := &reconnectRPCCapture{writes: make(chan []byte, 1)}
	hub := relay.NewHub(context.Background(), conn)
	defer hub.Close()
	s.reg.setHub("session_test", hub)
	s.execute(rc.ctx, runner.ToRunner{Type: "session_rpc", Session: "session_test", RPC: &runner.RPCEnvelope{ID: 17, Method: "exec", Payload: []byte(`{"command":"synthetic"}`)}}, func(runner.FromRunner) {}, AgentConfig{}, rc.state, target)
	select {
	case <-conn.writes:
		t.Fatal("queued command adopted later hub")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestReconnectForwardedRequestsAreBoundedAndConnectionScoped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	target := guestRPCTarget{ctx: ctx, row: sessionEntry{id: "session_test"}, control: &reconnectControl{ctx: ctx}}
	var forwards guestRPCForwards
	var first uint64
	for i := 0; i < 1024; i++ {
		id, ok := forwards.begin(target, 1)
		if !ok {
			t.Fatal("refused within bounded capacity")
		}
		if i == 0 {
			first = id
		}
	}
	if _, ok := forwards.begin(target, 1); ok {
		t.Fatal("unbounded forwarding table")
	}
	if _, ok := forwards.take(first, "another_test"); ok {
		t.Fatal("cross-session correlation")
	}
	if _, ok := forwards.take(first, "session_test"); !ok {
		t.Fatal("wrong-session response consumed real waiter")
	}
	if _, ok := forwards.take(first, "session_test"); ok {
		t.Fatal("response replay accepted")
	}
	cancel()
	replacement := context.Background()
	target.ctx = replacement
	target.control = &reconnectControl{ctx: replacement}
	id, ok := forwards.begin(target, 1)
	if !ok || id <= 1024 {
		t.Fatal("connection loss leaked capacity or reused correlation IDs")
	}
}

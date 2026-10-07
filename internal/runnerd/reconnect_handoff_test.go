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

func TestReconnectHandoffFencesBeforePublication(t *testing.T) {
	s, _, _ := reconnectPeer(t)
	old := relay.NewHub(context.Background(), &handoffIdleConn{done: make(chan struct{})})
	defer old.Close()
	s.reg.setHub("session_test", old)
	lease, err := s.acquireGuestReconnect("session_test")
	if err != nil {
		t.Fatal(err)
	}
	defer lease.close()
	if err = lease.fence(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	select {
	case <-old.Done():
	default:
		t.Fatal("old relay remained usable")
	}
	if _, ok := s.reg.hub("session_test"); ok {
		t.Fatal("old relay still published")
	}
	if err = lease.fence(context.Background(), 2); err != errReconnectFenced {
		t.Fatal("same epoch accepted twice")
	}
}

type handoffIdleConn struct{ done chan struct{} }

func (c *handoffIdleConn) Read(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, context.Canceled
	}
}
func (c *handoffIdleConn) Write(ctx context.Context, _ []byte) error { return ctx.Err() }
func (c *handoffIdleConn) Close() error                              { return nil }

func TestReconnectGuestDeliversConfigurationBeforeReadyPublication(t *testing.T) {
	s, _, control := reconnectPeer(t)
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	host, guest := relay.NetConn(a), relay.NetConn(b)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.ReconnectGuest(ctx, "session_test", host) }()
	req := control.readMsg(t)
	replyReconnect(t, control, req, true, reconnectChallengeJSON())
	event, err := relay.ReadGuestReconnectFrame(ctx, guest)
	if err != nil || event.Kind != relay.KindGuestReconnectChallenge {
		t.Fatal("challenge missing")
	}
	body, _ := json.Marshal(runner.GuestReconnectAcceptRequest{Protocol: 1, AttemptID: "attempt_test", Signature: strings.Repeat("A", 86)})
	if err = relay.WriteGuestReconnectFrame(ctx, guest, relay.KindGuestReconnectProof, body); err != nil {
		t.Fatal(err)
	}
	req = control.readMsg(t)
	replyReconnect(t, control, req, true, []byte(`{"epoch":2,"token":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","expires_in_sec":120}`))
	req = control.readMsg(t)
	if req.RPC.Method != runner.MethodGuestReconnectConfiguration {
		t.Fatal("no fresh configuration request")
	}
	body, _ = json.Marshal(runner.GuestReconnectConfiguration{Protocol: 1, SessionID: "session_test", PlacementGeneration: 3, Spec: &runner.Spec{Env: map[string]string{"CONFIG_TEST": "fresh"}}})
	replyReconnect(t, control, req, true, body)
	event, err = relay.ReadGuestReconnectFrame(ctx, guest)
	if err != nil || event.Kind != relay.KindGuestReconnectAccepted {
		t.Fatal("acceptance missing")
	}
	raw, err := guest.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	frame, _ := relay.Decode(raw)
	if json.Unmarshal(frame.Payload, &event) != nil || event.Kind != relay.KindBootConfig {
		t.Fatal("config missing")
	}
	var cfg runner.BootConfig
	if json.Unmarshal(event.Payload, &cfg) != nil || cfg.Env["CONFIG_TEST"] != "fresh" || cfg.BootstrapToken != strings.Repeat("A", 43) {
		t.Fatal("wrong configuration")
	}
	body, _ = json.Marshal(relay.ControlEvent{Kind: "req:" + runner.MethodFetchSessionSecrets, ID: 9, Payload: json.RawMessage(`{"protocol":1,"token":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}`)})
	raw, _ = relay.Encode(relay.Frame{Type: relay.FrameControl, Payload: body})
	if err = guest.Write(ctx, raw); err != nil {
		t.Fatal(err)
	}
	req = control.readMsg(t)
	if req.RPC.Method != runner.MethodFetchSessionSecrets || !isRunnerOriginated(req.RPC.ID) {
		t.Fatal("redemption not scoped to host request")
	}
	replyReconnect(t, control, req, true, []byte(`{"env":{}}`))
	raw, err = guest.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	frame, _ = relay.Decode(raw)
	json.Unmarshal(frame.Payload, &event)
	if event.ID != 9 || event.Kind != "resp" || !event.OK {
		t.Fatal("redemption correlation lost")
	}
	if _, ok := s.reg.hub("session_test"); ok {
		t.Fatal("relay published before ready")
	}
	body = []byte(`{"protocol":1,"epoch":2}`)
	if err = relay.WriteGuestReconnectFrame(ctx, guest, relay.KindGuestReconnectReady, body); err != nil {
		t.Fatal(err)
	}
	event, err = relay.ReadGuestReconnectFrame(ctx, guest)
	if err != nil || event.Kind != relay.KindGuestReconnectReadyAck {
		t.Fatal("ack missing")
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	hub, ok := s.reg.hub("session_test")
	if !ok {
		t.Fatal("ready relay not published")
	}
	defer hub.Close()
}

func TestReconnectGuestConfigurationFailureNeverPublishes(t *testing.T) {
	for _, fault := range []string{"scope", "placement", "token", "refusal", "deleted", "canceled"} {
		t.Run(fault, func(t *testing.T) {
			s, _, control := reconnectPeer(t)
			a, b := net.Pipe()
			defer a.Close()
			defer b.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			guest := relay.NetConn(b)
			done := make(chan error, 1)
			go func() { done <- s.ReconnectGuest(ctx, "session_test", relay.NetConn(a)) }()
			replyReconnect(t, control, control.readMsg(t), true, reconnectChallengeJSON())
			if _, err := relay.ReadGuestReconnectFrame(ctx, guest); err != nil {
				t.Fatal(err)
			}
			proof, _ := json.Marshal(runner.GuestReconnectAcceptRequest{Protocol: 1, AttemptID: "attempt_test", Signature: strings.Repeat("A", 86)})
			if err := relay.WriteGuestReconnectFrame(ctx, guest, relay.KindGuestReconnectProof, proof); err != nil {
				t.Fatal(err)
			}
			replyReconnect(t, control, control.readMsg(t), true, []byte(`{"epoch":2,"token":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","expires_in_sec":120}`))
			req := control.readMsg(t)
			cfg := runner.GuestReconnectConfiguration{Protocol: 1, SessionID: "session_test", PlacementGeneration: 3, Spec: &runner.Spec{Env: map[string]string{"CONFIG_TEST": "must_not_deliver"}}}
			switch fault {
			case "scope":
				cfg.SessionID = "other_test"
			case "placement":
				cfg.PlacementGeneration++
			case "token":
				cfg.Spec.BootstrapToken = "replacement_test"
			case "deleted":
				s.reg.remove("session_test")
			case "canceled":
				cancel()
			}
			if fault == "refusal" {
				replyReconnect(t, control, req, false, []byte(`{"error":"fenced"}`))
			} else {
				body, _ := json.Marshal(cfg)
				replyReconnect(t, control, req, true, body)
			}
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("failed handoff succeeded")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("handoff stuck")
			}
			if _, ok := s.reg.hub("session_test"); ok {
				t.Fatal("failed handoff published a relay")
			}
			readCtx, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			if raw, err := guest.Read(readCtx); err == nil || len(raw) != 0 {
				t.Fatal("failed resolution disclosed configuration")
			}
		})
	}
}

func TestReconnectRelayFenceRejectsQueuedCallbacks(t *testing.T) {
	authority := &guestRelayAuthority{}
	admitted := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	go authority.run(func() { close(admitted); <-release })
	<-admitted
	go func() { authority.fence(); close(finished) }()
	close(release)
	<-finished
	ran := false
	authority.run(func() { ran = true })
	if ran {
		t.Fatal("fenced callback ran")
	}
}

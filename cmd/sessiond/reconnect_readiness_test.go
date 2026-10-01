package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/runner"
)

func TestReconnectWaitsForReadyAcknowledgment(t *testing.T) {
	b, ch := reconnectFixture(t)
	guest, host, ctx := reconnectPair(t)
	t.Setenv("RAINIER_SESSION", "")
	done := make(chan error, 1)
	go func() { done <- reBootstrap(ctx, guest, b) }()
	accepted := acceptProof(t, ctx, host, b, ch, 1)
	sendReconnectConfig(t, ctx, host, runner.BootConfig{Protocol: 1, GuestReconnect: 1, SessionID: "session-test", BootstrapToken: accepted.Token})
	if _, err := host.Read(ctx); err != nil {
		t.Fatal(err)
	}
	controlWrite(t, ctx, host, relay.ControlEvent{Kind: "resp", ID: secretsRequestID, OK: true, Payload: json.RawMessage(`{"env":{}}`)})
	raw, err := host.Read(ctx)
	if err != nil {
		t.Fatalf("guest became ready without notifying host: %v", err)
	}
	frame, err := relay.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	var event relay.ControlEvent
	if json.Unmarshal(frame.Payload, &event) != nil || event.Kind != "guest_reconnect_ready" {
		t.Fatal("missing readiness notice")
	}
	select {
	case err := <-done:
		t.Fatalf("guest served before acknowledgment: %v", err)
	default:
	}
	controlWrite(t, ctx, host, relay.ControlEvent{Kind: "guest_reconnect_ready_ack", Payload: json.RawMessage(`{"protocol":1,"epoch":1}`)})
	if err = <-done; err != nil {
		t.Fatal(err)
	}
}

func acknowledgeGuestReady(t *testing.T, ctx context.Context, c relay.Conn, epoch uint64) {
	t.Helper()
	event, err := relay.ReadGuestReconnectFrame(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	ready, err := runner.DecodeGuestReconnectReady(event.Payload)
	if err != nil || event.Kind != relay.KindGuestReconnectReady || ready.Epoch != epoch {
		t.Fatal("invalid guest readiness")
	}
	writeReconnect(t, ctx, c, relay.KindGuestReconnectReadyAck, ready)
}

func TestReconnectReadyAckFailsClosed(t *testing.T) {
	for _, which := range []string{"wrong-epoch", "zero", "protocol", "duplicate", "unknown", "null", "kind", "oversize", "lost", "cancel", "timeout"} {
		t.Run(which, func(t *testing.T) {
			b, ch := reconnectFixture(t)
			guest, host, parent := reconnectPair(t)
			ctx, cancel := context.WithCancel(parent)
			defer cancel()
			t.Setenv("RAINIER_SESSION", "")
			tr := sessionTransport{dial: func(context.Context) (relay.Conn, error) { return guest, nil }, preamble: func(ctx context.Context, c relay.Conn) error { return reBootstrap(ctx, c, b) }}
			done := make(chan error, 1)
			go func() {
				c, err := tr.connect(ctx)
				if c != nil {
					done <- nil
					return
				}
				done <- err
			}()
			accepted := acceptProof(t, ctx, host, b, ch, 1)
			sendReconnectConfig(t, ctx, host, runner.BootConfig{Protocol: 1, GuestReconnect: 1, SessionID: "session-test", BootstrapToken: accepted.Token})
			if _, err := host.Read(ctx); err != nil {
				t.Fatal(err)
			}
			controlWrite(t, ctx, host, relay.ControlEvent{Kind: "resp", ID: secretsRequestID, OK: true, Payload: json.RawMessage(`{"env":{}}`)})
			event, err := relay.ReadGuestReconnectFrame(ctx, host)
			if err != nil || event.Kind != relay.KindGuestReconnectReady {
				t.Fatal("missing ready notice")
			}
			switch which {
			case "lost":
				host.Close()
			case "cancel":
				cancel()
			case "timeout": // Parent timeout is shorter than the separate five-second ready bound.
			default:
				body := json.RawMessage(`{"protocol":1,"epoch":1}`)
				kind := relay.KindGuestReconnectReadyAck
				switch which {
				case "wrong-epoch":
					body = json.RawMessage(`{"protocol":1,"epoch":2}`)
				case "zero":
					body = json.RawMessage(`{"protocol":1,"epoch":0}`)
				case "protocol":
					body = json.RawMessage(`{"protocol":2,"epoch":1}`)
				case "duplicate":
					body = json.RawMessage(`{"protocol":1,"epoch":1,"epoch":1}`)
				case "unknown":
					body = json.RawMessage(`{"protocol":1,"epoch":1,"extra":true}`)
				case "null":
					body = json.RawMessage(`null`)
				case "kind":
					kind = relay.KindGuestReconnectReady
				case "oversize":
					body = json.RawMessage(`{"protocol":1,"epoch":1,"extra":"` + strings.Repeat("x", 4096) + `"}`)
				}
				// Ordinary writer intentionally bypasses the helper's outbound size limit.
				controlWrite(t, ctx, host, relay.ControlEvent{Kind: kind, Payload: body})
			}
			select {
			case err := <-done:
				if err != errGuestReconnect {
					t.Fatalf("invalid ack made transport ready: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("ready wait not bounded")
			}
			if b.guest.epoch != 1 || b.reconnecting {
				t.Fatal("lost accepted epoch or attempt slot")
			}
			if which != "lost" {
				readCtx, stop := context.WithTimeout(context.Background(), time.Second)
				defer stop()
				if raw, err := host.Read(readCtx); err == nil || len(raw) > 0 {
					t.Fatal("failed preamble remained open or emitted more data")
				}
			}
		})
	}
}

func TestReconnectDoesNotAnnounceReadyBeforeConfiguration(t *testing.T) {
	b, ch := reconnectFixture(t)
	guest, host, ctx := reconnectPair(t)
	done := make(chan error, 1)
	go func() { done <- reBootstrap(ctx, guest, b); guest.Close() }()
	accepted := acceptProof(t, ctx, host, b, ch, 1)
	sendReconnectConfig(t, ctx, host, runner.BootConfig{Protocol: 1, GuestReconnect: 1, SessionID: "session-test", BootstrapToken: accepted.Token, SecretNames: []string{"REQUIRED_TEST"}})
	if _, err := host.Read(ctx); err != nil {
		t.Fatal(err)
	}
	controlWrite(t, ctx, host, relay.ControlEvent{Kind: "resp", ID: secretsRequestID, OK: true, Payload: json.RawMessage(`{"env":{}}`)})
	if err := <-done; err != errGuestReconnect {
		t.Fatal("missing secret accepted")
	}
	if raw, err := host.Read(ctx); err == nil || len(raw) > 0 {
		t.Fatal("configuration failure emitted ready")
	}
}

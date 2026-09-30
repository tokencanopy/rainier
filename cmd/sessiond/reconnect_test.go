package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/runner"
)

func TestReconnectEnrollmentWithoutSecrets(t *testing.T) {
	a, z := net.Pipe()
	defer a.Close()
	defer z.Close()
	b := &bootstrapper{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := b.exchange(ctx, relay.NetConn(a), runner.BootConfig{Protocol: 1, GuestReconnect: 1, SessionID: "session-test", BootstrapToken: base64.RawURLEncoding.EncodeToString(make([]byte, 32))})
		done <- err
	}()
	host := relay.NetConn(z)
	raw, err := host.Read(ctx)
	if err != nil {
		t.Fatalf("no enrollment for secret-free boot: %v", err)
	}
	f, err := relay.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	var ev relay.ControlEvent
	if err = json.Unmarshal(f.Payload, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Kind != "req:"+runner.MethodEnrollGuestReconnect {
		t.Fatalf("method=%s", ev.Kind)
	}
	enroll, err := runner.DecodeGuestReconnectEnrollRequest(ev.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if enroll.BootEpoch == "" || enroll.PublicKey == "" {
		t.Fatal("missing guest identity")
	}
	body, _ := json.Marshal(relay.ControlEvent{Kind: "resp", ID: ev.ID, OK: true, Payload: json.RawMessage(`{"env":{}}`)})
	frame, _ := relay.Encode(relay.Frame{Type: relay.FrameControl, Payload: body})
	if err = host.Write(ctx, frame); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
}

func reconnectPair(t *testing.T) (relay.Conn, relay.Conn, context.Context) {
	t.Helper()
	a, z := net.Pipe()
	t.Cleanup(func() { a.Close(); z.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	return relay.NetConn(a), relay.NetConn(z), ctx
}
func reconnectFixture(t *testing.T) (*bootstrapper, runner.GuestReconnectChallenge) {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return &bootstrapper{guest: &guestReconnectIdentity{session: "session-test", boot: "boot-test", key: key, enrolled: true}}, runner.GuestReconnectChallenge{Protocol: 1, SessionID: "session-test", BootEpoch: "boot-test", HostIncarnation: "host-test", AttemptID: "attempt-test", PlacementGeneration: 1, Challenge: base64.RawURLEncoding.EncodeToString(make([]byte, 32))}
}
func writeReconnect(t *testing.T, ctx context.Context, c relay.Conn, kind string, v any) {
	t.Helper()
	body, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if err = relay.WriteGuestReconnectFrame(ctx, c, kind, body); err != nil {
		t.Fatal(err)
	}
}
func controlWrite(t *testing.T, ctx context.Context, c relay.Conn, event relay.ControlEvent) {
	t.Helper()
	body, _ := json.Marshal(event)
	raw, _ := relay.Encode(relay.Frame{Type: relay.FrameControl, Payload: body})
	if err := c.Write(ctx, raw); err != nil {
		t.Fatal(err)
	}
}
func acceptProof(t *testing.T, ctx context.Context, host relay.Conn, b *bootstrapper, ch runner.GuestReconnectChallenge, epoch uint64) runner.GuestReconnectAcceptResponse {
	t.Helper()
	writeReconnect(t, ctx, host, relay.KindGuestReconnectChallenge, ch)
	ev, err := relay.ReadGuestReconnectFrame(ctx, host)
	if err != nil {
		t.Fatal(err)
	}
	if ev.Kind != relay.KindGuestReconnectProof {
		t.Fatalf("kind=%s", ev.Kind)
	}
	proof, err := runner.DecodeGuestReconnectAcceptRequest(ev.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if proof.AttemptID != ch.AttemptID {
		t.Fatal("wrong attempt")
	}
	if err = ch.VerifyProof(base64.RawURLEncoding.EncodeToString(b.guest.key.Public().(ed25519.PublicKey)), proof.Signature); err != nil {
		t.Fatal(err)
	}
	tokenBytes := make([]byte, 32)
	tokenBytes[0] = byte(epoch)
	accepted := runner.GuestReconnectAcceptResponse{Epoch: epoch, Token: base64.RawURLEncoding.EncodeToString(tokenBytes), ExpiresInSec: 120}
	writeReconnect(t, ctx, host, relay.KindGuestReconnectAccepted, accepted)
	return accepted
}
func sendReconnectConfig(t *testing.T, ctx context.Context, host relay.Conn, cfg runner.BootConfig) {
	t.Helper()
	body, _ := json.Marshal(cfg)
	controlWrite(t, ctx, host, relay.ControlEvent{Kind: relay.KindBootConfig, Payload: body})
}

func TestReconnectRefreshEmptySecretsRemovesOldConfiguration(t *testing.T) {
	b, ch := reconnectFixture(t)
	for _, k := range []string{"RAINIER_SESSION", "HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy", "NO_PROXY", "no_proxy", "OLD_TEST_SECRET", "OLD_TEST_CONFIG", "NEW_TEST_CONFIG", "UNOWNED_TEST"} {
		t.Setenv(k, "")
	}
	t.Setenv("UNOWNED_TEST", "keep")
	if err := b.refreshConfiguration(runner.BootConfig{SessionID: "session-test", ProxyURL: "http://proxy.invalid", NoProxy: "localhost", Env: map[string]string{"OLD_TEST_CONFIG": "old"}}, map[string]string{"OLD_TEST_SECRET": "synthetic"}); err != nil {
		t.Fatal(err)
	}
	guest, host, ctx := reconnectPair(t)
	done := make(chan error, 1)
	go func() { done <- reBootstrap(ctx, guest, b) }()
	accepted := acceptProof(t, ctx, host, b, ch, 1)
	sendReconnectConfig(t, ctx, host, runner.BootConfig{Protocol: 1, GuestReconnect: 1, SessionID: "session-test", BootstrapToken: accepted.Token, Env: map[string]string{"NEW_TEST_CONFIG": "new"}})
	raw, err := host.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	f, _ := relay.Decode(raw)
	var request relay.ControlEvent
	if json.Unmarshal(f.Payload, &request) != nil || request.Kind != "req:"+runner.MethodFetchSessionSecrets {
		t.Fatalf("no fresh redemption: %s", f.Payload)
	}
	var body struct {
		Token string `json:"token"`
	}
	_ = json.Unmarshal(request.Payload, &body)
	if body.Token != accepted.Token {
		t.Fatal("stale token")
	}
	controlWrite(t, ctx, host, relay.ControlEvent{Kind: "resp", ID: request.ID, OK: true, Payload: json.RawMessage(`{"env":{}}`)})
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"OLD_TEST_SECRET", "OLD_TEST_CONFIG", "HTTP_PROXY", "NO_PROXY"} {
		if _, ok := os.LookupEnv(k); ok {
			t.Fatalf("stale %s", k)
		}
	}
	if os.Getenv("NEW_TEST_CONFIG") != "new" || os.Getenv("UNOWNED_TEST") != "keep" {
		t.Fatal("configuration not refreshed")
	}
	if b.guest.epoch != 1 {
		t.Fatal("epoch not retained")
	}
}

func TestReconnectRefusesUntrustedChallenge(t *testing.T) {
	for _, which := range []string{"session", "boot", "protocol", "configuration", "oversize", "cancel"} {
		t.Run(which, func(t *testing.T) {
			b, ch := reconnectFixture(t)
			guest, host, ctx := reconnectPair(t)
			if which == "cancel" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			done := make(chan error, 1)
			go func() { done <- reBootstrap(ctx, guest, b); guest.Close() }()
			switch which {
			case "session":
				ch.SessionID = "other-test"
			case "boot":
				ch.BootEpoch = "other-boot"
			case "protocol":
				ch.Protocol = 2
			}
			if which == "oversize" {
				_ = host.Write(ctx, []byte(strings.Repeat("x", 4097)))
			} else if which != "cancel" {
				body, _ := json.Marshal(ch)
				kind := relay.KindGuestReconnectChallenge
				if which == "configuration" {
					kind = relay.KindBootConfig
				}
				payload, _ := json.Marshal(relay.ControlEvent{Kind: kind, Payload: body})
				frame, _ := relay.Encode(relay.Frame{Type: relay.FrameControl, Payload: payload})
				_ = host.Write(ctx, frame)
			}
			if err := <-done; err != errGuestReconnect {
				t.Fatalf("failure=%v", err)
			}
			raw, err := host.Read(ctx)
			if err == nil || len(raw) != 0 {
				t.Fatal("refused peer received a proof")
			}
			if b.guest.epoch != 0 {
				t.Fatal("refusal changed epoch")
			}
		})
	}
}

func TestReconnectDeliveryFailsClosed(t *testing.T) {
	for _, which := range []string{"replay", "refused", "bad-token", "wrong-session", "downgrade", "exchange-refusal", "null-env", "missing-secret", "bad-env"} {
		t.Run(which, func(t *testing.T) {
			b, ch := reconnectFixture(t)
			guest, host, ctx := reconnectPair(t)
			if which == "replay" {
				b.guest.epoch = 1
			}
			done := make(chan error, 1)
			go func() { done <- reBootstrap(ctx, guest, b); guest.Close() }()
			if which == "refused" {
				writeReconnect(t, ctx, host, relay.KindGuestReconnectRefused, runner.GuestReconnectErrorResponse{Error: "fenced"})
			} else {
				accepted := acceptProof(t, ctx, host, b, ch, 1)
				if which != "replay" {
					cfg := runner.BootConfig{Protocol: 1, GuestReconnect: 1, SessionID: "session-test", BootstrapToken: accepted.Token}
					switch which {
					case "bad-token":
						cfg.BootstrapToken = "stale"
					case "wrong-session":
						cfg.SessionID = "other-test"
					case "downgrade":
						cfg.GuestReconnect = 0
					case "missing-secret":
						cfg.SecretNames = []string{"ABSENT_TEST"}
					}
					sendReconnectConfig(t, ctx, host, cfg)
					if which == "exchange-refusal" || which == "null-env" || which == "missing-secret" || which == "bad-env" {
						if _, err := host.Read(ctx); err != nil {
							t.Fatal(err)
						}
						payload := json.RawMessage(`{"env":{}}`)
						if which == "null-env" {
							payload = json.RawMessage(`{"env":null}`)
						}
						if which == "bad-env" {
							payload = json.RawMessage(`{"env":{"BAD=KEY":"synthetic"}}`)
						}
						controlWrite(t, ctx, host, relay.ControlEvent{Kind: "resp", ID: secretsRequestID, OK: which != "exchange-refusal", Payload: payload})
					}
				}
			}
			if err := <-done; err != errGuestReconnect {
				t.Fatalf("accepted invalid delivery: %v", err)
			}
			if b.reconnecting {
				t.Fatal("attempt slot leaked")
			}
			if which != "refused" && b.guest.epoch != 1 {
				t.Fatal("accepted epoch lost after failed delivery")
			}
		})
	}
}

func TestGuestEnvironmentStrict(t *testing.T) {
	for _, raw := range []string{`null`, `{}`, `{"env":null}`, `{"env":{},"env":{}}`, `{"Env":{}}`, `{"env":{"A":null}}`, `{"env":{"A":"1","A":"2"}}`, `{"env":{}} {}`, `{"env":{},"extra":1}`} {
		if _, err := decodeGuestEnvironment([]byte(raw)); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
	env, err := decodeGuestEnvironment([]byte(`{"env":{"EMPTY":""}}`))
	if err != nil || len(env) != 1 {
		t.Fatal("empty string must be valid")
	}
}

func TestGuestConfigurationValidationBeforeMutation(t *testing.T) {
	b, _ := reconnectFixture(t)
	t.Setenv("RETAIN_TEST", "original")
	b.configured = []string{"RETAIN_TEST"}
	if err := b.refreshConfiguration(runner.BootConfig{Env: map[string]string{"INVALID=KEY": "x"}}, nil); err == nil {
		t.Fatal("accepted invalid environment")
	}
	if os.Getenv("RETAIN_TEST") != "original" {
		t.Fatal("failed validation mutated environment")
	}
}

func TestLegacyReconnectCannotEnrollLater(t *testing.T) {
	guest, host, ctx := reconnectPair(t)
	b := &bootstrapper{}
	done := make(chan error, 1)
	go func() { done <- reBootstrap(ctx, guest, b); guest.Close() }()
	sendReconnectConfig(t, ctx, host, runner.BootConfig{Protocol: 1, GuestReconnect: 1, SessionID: "session-test", BootstrapToken: base64.RawURLEncoding.EncodeToString(make([]byte, 32))})
	if raw, err := host.Read(ctx); err == nil || len(raw) != 0 {
		t.Fatal("legacy reconnect attempted enrollment outside a fresh boot")
	}
	if err := <-done; err == nil {
		t.Fatal("late opt-in accepted")
	}
}

func TestEnrollmentFailureIsSticky(t *testing.T) {
	guest, host, ctx := reconnectPair(t)
	b := &bootstrapper{}
	done := make(chan error, 1)
	cfg := runner.BootConfig{Protocol: 1, GuestReconnect: 1, SessionID: "session-test", BootstrapToken: base64.RawURLEncoding.EncodeToString(make([]byte, 32))}
	go func() { _, err := b.exchange(ctx, guest, cfg); done <- err }()
	if _, err := host.Read(ctx); err != nil {
		t.Fatal(err)
	}
	controlWrite(t, ctx, host, relay.ControlEvent{Kind: "resp", ID: secretsRequestID, Payload: json.RawMessage(`{"error":"unavailable"}`)})
	if err := <-done; err == nil {
		t.Fatal("refused enrollment succeeded")
	}
	if _, err := b.exchange(ctx, guest, cfg); err == nil {
		t.Fatal("ambiguous enrollment retried")
	}
	if err := reBootstrap(ctx, guest, b); err == nil {
		t.Fatal("failed enrollment reconnected")
	}
	if b.guest.enrolled || len(b.guest.key) != 0 {
		t.Fatal("failed enrollment retained usable identity")
	}
}

func TestReconnectAttemptBudgetAndTransportClosure(t *testing.T) {
	b, ch := reconnectFixture(t)
	guest, host, ctx := reconnectPair(t)
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
	writeReconnect(t, ctx, host, relay.KindGuestReconnectChallenge, ch)
	if _, err := relay.ReadGuestReconnectFrame(ctx, host); err != nil {
		t.Fatal(err)
	}
	other, _, otherCtx := reconnectPair(t)
	if err := reBootstrap(otherCtx, other, b); err != errGuestReconnect {
		t.Fatal("parallel attempt not refused")
	}
	writeReconnect(t, ctx, host, relay.KindGuestReconnectRefused, runner.GuestReconnectErrorResponse{Error: "fenced"})
	if err := <-done; err != errGuestReconnect {
		t.Fatalf("unready transport: %v", err)
	}
	if _, err := host.Read(ctx); err == nil {
		t.Fatal("failed transport remained open")
	}
	if b.reconnecting {
		t.Fatal("slot leaked")
	}
}

func TestReconnectDeadlineReleasesAttempt(t *testing.T) {
	b, ch := reconnectFixture(t)
	guest, host, _ := reconnectPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- reBootstrap(ctx, guest, b) }()
	writeReconnect(t, ctx, host, relay.KindGuestReconnectChallenge, ch)
	if _, err := relay.ReadGuestReconnectFrame(ctx, host); err != nil {
		t.Fatal(err)
	}
	// The peer never supplies acceptance. This same deadline covers both legs.
	if err := <-done; err != errGuestReconnect {
		t.Fatal("deadline did not fail closed")
	}
	if b.reconnecting || b.guest.epoch != 0 {
		t.Fatal("timeout retained authority or attempt")
	}
}

func TestEnrollmentEnvironmentAboveHandshakeLimit(t *testing.T) {
	guest, host, ctx := reconnectPair(t)
	b := &bootstrapper{}
	done := make(chan error, 1)
	cfg := runner.BootConfig{Protocol: 1, GuestReconnect: 1, SessionID: "session-test", BootstrapToken: base64.RawURLEncoding.EncodeToString(make([]byte, 32)), SecretNames: []string{"LARGE_TEST"}}
	want := strings.Repeat("synthetic", 1024)
	go func() {
		env, err := b.exchange(ctx, guest, cfg)
		if err == nil && env["LARGE_TEST"] != want {
			err = errGuestReconnect
		}
		done <- err
	}()
	if _, err := host.Read(ctx); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(runner.GuestReconnectEnrollResponse{Env: map[string]string{"LARGE_TEST": want}})
	controlWrite(t, ctx, host, relay.ControlEvent{Kind: "resp", ID: secretsRequestID, OK: true, Payload: payload})
	if err := <-done; err != nil {
		t.Fatal("authorized environment incorrectly bounded by handshake limit")
	}
}

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/internal/session"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// A separately executed guest uses the production boot/preamble and PTY code
// over real TCP streams. The host is a protocol fixture: shipping host opt-in,
// AF_VSOCK/KVM, policy authorization and relay takeover remain separate gates.
func TestGuestReconnectExecutable(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestGuestReconnectProcessGuest$", "-test.v")
	cmd.Env = append(os.Environ(), "RAINIER_TEST_GUEST_ADDRESS="+ln.Addr().String())
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	accept := func() relay.Conn {
		t.Helper()
		_ = ln.(*net.TCPListener).SetDeadline(time.Now().Add(5 * time.Second))
		c, err := ln.Accept()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		return relay.NetConn(c)
	}
	initial := accept()
	token := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	cfg := runner.BootConfig{Protocol: 1, GuestReconnect: 1, SessionID: "process-test", BootstrapToken: token, Env: map[string]string{"RECONNECT_PROCESS_TEST": "initial"}}
	sendReconnectConfig(t, ctx, initial, cfg)
	raw, err := initial.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	f, _ := relay.Decode(raw)
	var event relay.ControlEvent
	if json.Unmarshal(f.Payload, &event) != nil || event.Kind != "req:"+runner.MethodEnrollGuestReconnect {
		t.Fatal("missing initial enrollment")
	}
	enrolled, err := runner.DecodeGuestReconnectEnrollRequest(event.Payload)
	if err != nil {
		t.Fatal(err)
	}
	controlWrite(t, ctx, initial, relay.ControlEvent{Kind: "resp", ID: event.ID, OK: true, Payload: json.RawMessage(`{"env":{}}`)})
	initial.Close()
	// First reconnect is explicitly fenced. A subsequent fresh challenge must
	// still be usable, and the child shell must not be restarted in either case.
	refused := accept()
	writeReconnect(t, ctx, refused, relay.KindGuestReconnectRefused, runner.GuestReconnectErrorResponse{Error: "fenced"})
	refused.Close()
	host := accept()
	challenge := runner.GuestReconnectChallenge{Protocol: 1, SessionID: cfg.SessionID, BootEpoch: enrolled.BootEpoch, HostIncarnation: "host-test", AttemptID: "process-attempt", PlacementGeneration: 1, Challenge: token}
	writeReconnect(t, ctx, host, relay.KindGuestReconnectChallenge, challenge)
	event, err = relay.ReadGuestReconnectFrame(ctx, host)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := runner.DecodeGuestReconnectAcceptRequest(event.Payload)
	if err != nil || event.Kind != relay.KindGuestReconnectProof {
		t.Fatal("invalid process proof")
	}
	if err = challenge.VerifyProof(enrolled.PublicKey, proof.Signature); err != nil {
		t.Fatal(err)
	}
	tokenBytes := make([]byte, 32)
	tokenBytes[0] = 7
	fresh := base64.RawURLEncoding.EncodeToString(tokenBytes)
	writeReconnect(t, ctx, host, relay.KindGuestReconnectAccepted, runner.GuestReconnectAcceptResponse{Epoch: 1, Token: fresh, ExpiresInSec: 120})
	cfg.BootstrapToken = fresh
	cfg.Env = map[string]string{"RECONNECT_PROCESS_TEST": "refreshed"}
	sendReconnectConfig(t, ctx, host, cfg)
	raw, err = host.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	f, _ = relay.Decode(raw)
	if json.Unmarshal(f.Payload, &event) != nil || event.Kind != "req:"+runner.MethodFetchSessionSecrets {
		t.Fatal("missing fresh redemption")
	}
	var request struct {
		Token string `json:"token"`
	}
	_ = json.Unmarshal(event.Payload, &request)
	if request.Token != fresh {
		t.Fatal("wrong process token")
	}
	controlWrite(t, ctx, host, relay.ControlEvent{Kind: "resp", ID: event.ID, OK: true, Payload: json.RawMessage(`{"env":{}}`)})
	err = cmd.Wait()
	waited = true
	if err != nil {
		t.Fatalf("guest process: %v\n%s", err, output.String())
	}
	if !strings.Contains(output.String(), "retained PTY shell and refreshed guest configuration") {
		t.Fatalf("missing process evidence: %s", output.String())
	}
	if strings.Contains(output.String(), enrolled.PublicKey) || strings.Contains(output.String(), fresh) {
		t.Fatal("guest logged credentials")
	}
	t.Log("separate guest process: enrollment, refused reconnect, verified proof, fresh redemption, same PTY shell across reconnect; no credential output")
}

func TestGuestReconnectProcessGuest(t *testing.T) {
	address := os.Getenv("RAINIER_TEST_GUEST_ADDRESS")
	if address == "" {
		t.Skip("subprocess helper")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	dial := func(ctx context.Context) (relay.Conn, error) {
		c, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
		if err != nil {
			return nil, err
		}
		return relay.NetConn(c), nil
	}
	b := &bootstrapper{}
	c, _, failure, err := bootOverVsock(ctx, dial, b)
	if err != nil || failure != nil {
		t.Fatalf("boot: %v %v", err, failure)
	}
	c.Close()
	chunks := make(chan string, 32)
	p, err := session.StartProc([]string{"/bin/sh", "-c", `stty -echo; while read line; do printf 'SHELL:%s:%s:%s\n' "$$" "$RECONNECT_PROCESS_TEST" "$line"; done`}, 80, 24, func(raw []byte) { chunks <- string(raw) })
	if err != nil {
		t.Fatal(err)
	}
	defer func() { p.Stop(); p.Wait() }()
	turn := func(label string) string {
		t.Helper()
		if _, err := p.Write([]byte(label + "\n")); err != nil {
			t.Fatal(err)
		}
		var output string
		for !strings.Contains(output, ":"+label) {
			select {
			case s := <-chunks:
				output += s
			case <-ctx.Done():
				t.Fatal("PTY turn timeout")
			}
		}
		for _, line := range strings.Split(output, "\n") {
			if strings.HasPrefix(line, "SHELL:") {
				return strings.TrimSpace(line)
			}
		}
		t.Fatalf("missing shell response: %s", output)
		return ""
	}
	before := turn("before")
	tr := sessionTransport{dial: dial, preamble: func(ctx context.Context, c relay.Conn) error { return reBootstrap(ctx, c, b) }}
	if c, err := tr.connect(ctx); err == nil || c != nil {
		t.Fatal("refused connection became ready")
	}
	c, err = tr.connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	after := turn("after")
	if strings.TrimSuffix(before, ":before") != strings.TrimSuffix(after, ":after") {
		t.Fatalf("shell restarted or inherited environment changed: %s / %s", before, after)
	}
	if !strings.Contains(after, ":initial:after") || os.Getenv("RECONNECT_PROCESS_TEST") != "refreshed" {
		t.Fatal("guest/child environment contract violated")
	}
	fmt.Println("retained PTY shell and refreshed guest configuration")
}

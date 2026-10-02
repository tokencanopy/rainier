package pgstore

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/internal/controld"
	"github.com/tokencanopy/rainier/protocol/runner"
)

func TestStandaloneReconnectRoundTrip(t *testing.T) { standaloneReconnectRoundTrip(t, false) }
func TestStandaloneReconnectShippingProcess(t *testing.T) {
	if testing.Short() {
		t.Skip("shipping process integration")
	}
	standaloneReconnectRoundTrip(t, true)
}
func standaloneReconnectRoundTrip(t *testing.T, shipping bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	st := freshStore(t, startPostgres(t), t.Name())
	user, err := st.UpsertUser(ctx, 42, "member_test", "member")
	if err != nil {
		t.Fatal(err)
	}
	endpoint := standaloneReconnectEndpoint(t, st, shipping)
	var conn *websocket.Conn
	for {
		conn, _, err = websocket.Dial(ctx, endpoint+"/v0/runners/connect", &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": []string{"Bearer runner_token_test"}}})
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("standalone process did not accept runner")
		case <-time.After(20 * time.Millisecond):
		}
	}
	defer conn.CloseNow()
	if err := wsjson.Write(ctx, conn, runner.FromRunner{Type: "announce", Proto: runner.ProtocolVersion, Runner: "runner_test", Total: 4, Capabilities: []string{runner.CapabilityMicrovmV1, runner.CapabilityGuestReconnectV1}}); err != nil {
		t.Fatal(err)
	}
	var accepted runner.ToRunner
	if err := wsjson.Read(ctx, conn, &accepted); err != nil || accepted.Type != "accept" {
		t.Fatal("runner was not accepted")
	}
	var sequence uint64
	ask := func(method string, payload any, host bool) runner.RPCEnvelope {
		t.Helper()
		sequence++
		id := sequence
		if host {
			id |= 1 << 63
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		if err := wsjson.Write(ctx, conn, runner.FromRunner{Type: "session_req", Session: "session_host_test", Generation: accepted.Generation, Total: 4, RPC: &runner.RPCEnvelope{ID: id, Method: method, Payload: raw}}); err != nil {
			t.Fatal(err)
		}
		for {
			var msg runner.ToRunner
			if err := wsjson.Read(ctx, conn, &msg); err != nil {
				t.Fatal(err)
			}
			if msg.Type == "session_rpc" && msg.RPC != nil && msg.RPC.ID == id {
				return *msg.RPC
			}
		}
	}
	// The response follows initial inventory reconciliation.
	ask("unknown_test", map[string]int{}, true)
	if _, err := st.Sessions().CreateSession(ctx, "ws_self_hosted", control.Session{ID: "session_host_test", CreatorID: control.ActorID(user.ID), PoolID: "pool_self_hosted", RunnerID: "runner_test", State: control.StateRunning}); err != nil {
		t.Fatal(err)
	}
	protocol := map[string]uint64{"protocol": 1}
	mint := ask(runner.MethodMintSessionBootstrap, protocol, true)
	var token struct {
		Token string `json:"token"`
	}
	if !mint.OK || json.Unmarshal(mint.Payload, &token) != nil || token.Token == "" {
		t.Fatal("mint failed")
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx, `ALTER TABLE events ADD CONSTRAINT reject_enrollment_test CHECK (action <> 'guest_enroll')`); err != nil {
		t.Fatal(err)
	}
	enrollment := runner.GuestReconnectEnrollRequest{Protocol: 1, Token: token.Token, BootEpoch: "boot_test", PublicKey: base64.RawURLEncoding.EncodeToString(pub)}
	if ask(runner.MethodEnrollGuestReconnect, enrollment, false).OK {
		t.Fatal("enrollment escaped failed audit")
	}
	if _, err := st.pool.Exec(ctx, `ALTER TABLE events DROP CONSTRAINT reject_enrollment_test`); err != nil {
		t.Fatal(err)
	}
	enroll := ask(runner.MethodEnrollGuestReconnect, enrollment, false)
	if !enroll.OK {
		t.Fatal("enrollment refused")
	}
	if ask(runner.MethodBeginGuestReconnect, protocol, false).OK {
		t.Fatal("guest originated challenge")
	}
	if ask(runner.MethodBeginGuestReconnect, json.RawMessage(`{"protocol":1,"protocol":1}`), true).OK {
		t.Fatal("duplicate protocol accepted")
	}
	begin := ask(runner.MethodBeginGuestReconnect, protocol, true)
	var challenge runner.GuestReconnectChallenge
	if !begin.OK || json.Unmarshal(begin.Payload, &challenge) != nil {
		t.Fatal("challenge refused")
	}
	message, err := challenge.SigningMessage()
	if err != nil {
		t.Fatal(err)
	}
	proof := runner.GuestReconnectAcceptRequest{Protocol: 1, AttemptID: challenge.AttemptID, Signature: base64.RawURLEncoding.EncodeToString(ed25519.Sign(priv, message))}
	wrong := proof
	_, otherKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	wrong.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(otherKey, message))
	if ask(runner.MethodAcceptGuestReconnect, wrong, true).OK {
		t.Fatal("wrong proof accepted")
	}
	answer := ask(runner.MethodAcceptGuestReconnect, proof, true)
	var capability runner.GuestReconnectAcceptResponse
	if !answer.OK || json.Unmarshal(answer.Payload, &capability) != nil || capability.Epoch != 1 {
		t.Fatal("proof refused")
	}
	if ask(runner.MethodAcceptGuestReconnect, proof, true).OK {
		t.Fatal("proof replay accepted")
	}
	if !ask(runner.MethodGuestReconnectConfiguration, protocol, true).OK {
		t.Fatal("configuration refused")
	}
	redeem := map[string]any{"protocol": 1, "token": capability.Token}
	if ask(runner.MethodFetchSessionSecrets, redeem, false).OK {
		t.Fatal("guest spent proof-issued token")
	}
	if _, err := st.pool.Exec(ctx, `UPDATE users SET login='revoked_test' WHERE id=$1`, user.ID); err != nil {
		t.Fatal(err)
	}
	if ask(runner.MethodFetchSessionSecrets, redeem, true).OK {
		t.Fatal("revoked owner redeemed")
	}
	if _, err := st.pool.Exec(ctx, `UPDATE users SET login='member_test' WHERE id=$1`, user.ID); err != nil {
		t.Fatal(err)
	}
	if !ask(runner.MethodFetchSessionSecrets, redeem, true).OK {
		t.Fatal("authorized redemption refused")
	}
	if ask(runner.MethodFetchSessionSecrets, redeem, true).OK {
		t.Fatal("token replay accepted")
	}
	if _, err := st.Sessions().CreateSession(ctx, "ws_self_hosted", control.Session{ID: "session_negotiated_test", CreatorID: control.ActorID(user.ID), PoolID: "pool_self_hosted", State: control.StateQueued}); err != nil {
		t.Fatal(err)
	}
	for {
		var message runner.ToRunner
		if err := wsjson.Read(ctx, conn, &message); err != nil {
			t.Fatal("negotiated placement was not dispatched")
		}
		if message.Type != "create" || message.Session != "session_negotiated_test" {
			continue
		}
		if message.Spec == nil || message.Spec.GuestReconnect != runner.GuestReconnectProtocol {
			t.Fatal("capable host did not negotiate recovery")
		}
		if err := wsjson.Write(ctx, conn, runner.FromRunner{Type: "result", ReqID: message.ReqID, OK: true, Generation: accepted.Generation, Total: 4}); err != nil {
			t.Fatal(err)
		}
		break
	}

}

func standaloneReconnectEndpoint(t *testing.T, st *Store, shipping bool) string {
	t.Helper()
	if !shipping {
		server, err := controld.New(st, controld.Config{RunnerToken: "runner_token_test", SecretsKey: [32]byte{1}, Members: []string{"member_test"}, ExternalURL: "http://127.0.0.1:1"})
		if err != nil {
			t.Fatal(err)
		}
		loopCtx, stop := context.WithCancel(context.Background())
		t.Cleanup(stop)
		go server.Run(loopCtx)
		serverHTTP := httptest.NewServer(server.Handler())
		t.Cleanup(serverHTTP.Close)
		return serverHTTP.URL
	}
	dir := t.TempDir()
	binary := filepath.Join(dir, "controld")
	build := exec.Command("go", "build", "-o", binary, "../../../cmd/controld")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build controld: %v\n%s", err, output)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	endpoint := "http://" + address
	child := exec.Command(binary, "--listen", address, "--external-url", endpoint, "--members", "member_test")
	child.Env = append(os.Environ(), "RAINIER_DB="+st.pool.Config().ConnString(), "RAINIER_RUNNER_TOKEN=runner_token_test", "RAINIER_SECRETS_KEY=0100000000000000000000000000000000000000000000000000000000000000")
	logFile, err := os.OpenFile(filepath.Join(dir, "controld.log"), os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	child.Stdout = logFile
	child.Stderr = logFile
	if err := child.Start(); err != nil {
		logFile.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = child.Process.Kill(); _ = child.Wait(); _ = logFile.Close() })
	return endpoint
}

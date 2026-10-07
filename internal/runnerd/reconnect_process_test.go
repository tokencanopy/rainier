package runnerd

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"github.com/tokencanopy/rainier/protocol/runner"
	"io"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// No shipping command invokes the optional callback yet. Build its public call
// site and bounded guest transport across real agent WebSocket and guest TCP
// streams. No live VM, enrolled guest key owner or relay takeover is exercised.
func TestReconnectHostBuiltExecution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	binary := filepath.Join(t.TempDir(), "reconnect-host")
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, "./testdata/reconnect-host")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build probe: %v %s", err, output)
	}
	fc := newFakeControld(t, testToken)
	cmd := exec.CommandContext(ctx, binary, fc.wsURL())
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	cmd.Stderr = &logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	answers := make(chan string, 8)
	go func() {
		scan := bufio.NewScanner(output)
		for scan.Scan() {
			answers <- scan.Text()
		}
		close(answers)
	}()
	waited := make(chan error, 1)
	go func() { waited <- cmd.Wait() }()
	t.Cleanup(func() {
		input.Close()
		select {
		case err := <-waited:
			if err != nil {
				t.Error("probe failed")
			}
		case <-time.After(3 * time.Second):
			cancel()
			<-waited
			t.Error("probe did not stop")
		}
		if strings.Contains(logs.String(), "panic:") || strings.Contains(logs.String(), strings.Repeat("A", 43)) {
			t.Error("probe log failure or cryptographic disclosure")
		}
	})
	conn := fc.nextConn(t)
	conn.readAnnounce(t)
	acceptReconnectPeer(t, conn, 7)
	conn.send(t, runner.ToRunner{Type: "create", ReqID: 1, Session: "session_test", PlacementGeneration: 3, Spec: &runner.Spec{Image: "synthetic.invalid/session:test"}})
	if result := conn.readMsg(t); result.Type != "result" || !result.OK {
		t.Fatal("probe placement failed")
	}
	for _, tc := range []struct{ name, want string }{{"valid", "accepted"}, {"wrong_scope", "invalid"}, {"revoked", "fenced"}, {"wrong_attempt", "unavailable"}, {"oversize", "unavailable"}} {
		t.Run(tc.name, func(t *testing.T) {
			command := "authorize"
			if tc.name == "wrong_attempt" || tc.name == "oversize" {
				command = tc.name
			}
			if _, err := io.WriteString(input, command+"\n"); err != nil {
				t.Fatal(err)
			}
			req := conn.readMsg(t)
			if req.RPC == nil || req.RPC.Method != runner.MethodBeginGuestReconnect || !isRunnerOriginated(req.RPC.ID) {
				t.Fatal("invalid begin")
			}
			switch tc.name {
			case "revoked":
				replyReconnect(t, conn, req, false, []byte(`{"error":"fenced"}`))
			case "wrong_attempt", "oversize":
				replyReconnect(t, conn, req, true, reconnectChallengeJSON())
			case "wrong_scope":
				replyReconnect(t, conn, req, true, []byte(strings.Replace(string(reconnectChallengeJSON()), "session_test", "wrong_test", 1)))
			default:
				replyReconnect(t, conn, req, true, reconnectChallengeJSON())
				req = conn.readMsg(t)
				proof, err := runner.DecodeGuestReconnectAcceptRequest(req.RPC.Payload)
				challenge, _ := runner.DecodeGuestReconnectChallenge(reconnectChallengeJSON())
				public := ed25519.NewKeyFromSeed(make([]byte, 32)).Public().(ed25519.PublicKey)
				if err != nil || req.RPC.Method != runner.MethodAcceptGuestReconnect || challenge.VerifyProof(base64.RawURLEncoding.EncodeToString(public), proof.Signature) != nil {
					t.Fatal("invalid built-process proof")
				}
				replyReconnect(t, conn, req, true, []byte(`{"epoch":2,"token":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","expires_in_sec":120}`))
			}
			select {
			case got := <-answers:
				if got != tc.want {
					t.Fatalf("result=%s want %s", got, tc.want)
				}
			case <-time.After(6 * time.Second):
				t.Fatal("no probe result")
			}
		})
	}
	t.Log("built host/stream bridge: accepted proof, wrong-scope, revoked, wrong-attempt and oversized guest paths passed; synthetic driver/guest, real WebSocket and TCP streams")
}

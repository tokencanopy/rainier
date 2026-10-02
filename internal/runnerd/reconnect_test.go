package runnerd

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/protocol/runner"
)

func TestReconnectHostRoundTrip(t *testing.T) {
	s, _ := testMicrovmServer(t)
	s.reg.put("session_test", &sessionEntry{id: "session_test", state: "running", placementGen: 3})
	fc := newFakeControld(t, testToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.RunAgent(ctx, AgentConfig{ControldURL: fc.wsURL(), Token: testToken, RunnerName: "runner_test"})
	conn := fc.nextConn(t)
	conn.readAnnounce(t)
	conn.send(t, runner.ToRunner{Type: "accept", Generation: 7})
	// A command/result barrier proves the accept has run before the host call.
	conn.send(t, runner.ToRunner{Type: "destroy", ReqID: 99, Session: "absent_test"})
	conn.readMsg(t)
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		accepted, err := s.AuthorizeGuestReconnect(ctx, "session_test", func(ctx context.Context, c runner.GuestReconnectChallenge) (string, error) {
			msg, err := c.SigningMessage()
			if err != nil {
				return "", err
			}
			return base64.RawURLEncoding.EncodeToString(ed25519.Sign(private, msg)), nil
		})
		if err == nil && (accepted.Epoch != 2 || accepted.ExpiresInSec != 120) {
			err = context.Canceled
		}
		done <- err
	}()
	req := conn.readMsg(t)
	if req.Total != 4 {
		t.Fatalf("host-originated reconnect RPC lost capacity snapshot: total=%d", req.Total)
	}
	if req.Type != "session_req" || req.Session != "session_test" || req.RPC == nil || req.RPC.Method != runner.MethodBeginGuestReconnect || !isRunnerOriginated(req.RPC.ID) {
		t.Fatal("wrong begin request")
	}
	c := runner.GuestReconnectChallenge{Protocol: 1, SessionID: "session_test", BootEpoch: "boot_test", HostIncarnation: "7", AttemptID: "attempt_test", PlacementGeneration: 3, Challenge: base64.RawURLEncoding.EncodeToString(make([]byte, 32))}
	body, _ := json.Marshal(c)
	conn.send(t, runner.ToRunner{Type: "session_rpc", Session: req.Session, RPC: &runner.RPCEnvelope{ID: req.RPC.ID, Method: "resp", OK: true, Payload: body}})
	req = conn.readMsg(t)
	if req.RPC == nil || req.RPC.Method != runner.MethodAcceptGuestReconnect || !isRunnerOriginated(req.RPC.ID) {
		t.Fatal("wrong accept request")
	}
	proof, err := runner.DecodeGuestReconnectAcceptRequest(req.RPC.Payload)
	if err != nil || proof.AttemptID != c.AttemptID || c.VerifyProof(base64.RawURLEncoding.EncodeToString(public), proof.Signature) != nil {
		t.Fatal("proof was not preserved")
	}
	body = []byte(`{"epoch":2,"token":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","expires_in_sec":120}`)
	conn.send(t, runner.ToRunner{Type: "session_rpc", Session: req.Session, RPC: &runner.RPCEnvelope{ID: req.RPC.ID, Method: "resp", OK: true, Payload: body}})
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("authorization failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("no authorization result")
	}
}

func reconnectPeer(t *testing.T) (*Server, *fakeControld, *fakeConn) {
	t.Helper()
	s, _ := testMicrovmServer(t)
	s.reg.put("session_test", &sessionEntry{id: "session_test", handle: "vm_test", state: "running", placementGen: 3})
	fc := newFakeControld(t, testToken)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = s.RunAgent(ctx, AgentConfig{ControldURL: fc.wsURL(), Token: testToken, RunnerName: "runner_test", RedialBackoffMin: time.Millisecond})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("agent did not stop")
		}
	})
	conn := fc.nextConn(t)
	conn.readAnnounce(t)
	acceptReconnectPeer(t, conn, 7)
	return s, fc, conn
}
func acceptReconnectPeer(t *testing.T, c *fakeConn, generation uint64) {
	t.Helper()
	c.send(t, runner.ToRunner{Type: "accept", Generation: generation})
	c.send(t, runner.ToRunner{Type: "destroy", ReqID: 99, Session: "absent_test"})
	if c.readMsg(t).Type != "result" {
		t.Fatal("accept barrier failed")
	}
}
func reconnectChallengeJSON() []byte {
	return []byte(`{"protocol":1,"session_id":"session_test","boot_epoch":"boot_test","host_incarnation":"7","attempt_id":"attempt_test","placement_generation":3,"challenge":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}`)
}
func replyReconnect(t *testing.T, c *fakeConn, r runner.FromRunner, ok bool, body []byte) {
	t.Helper()
	if r.RPC == nil {
		t.Fatal("missing request")
	}
	c.send(t, runner.ToRunner{Type: "session_rpc", Session: r.Session, RPC: &runner.RPCEnvelope{ID: r.RPC.ID, Method: "resp", OK: ok, Payload: body}})
}
func waitReconnectError(t *testing.T, done <-chan error, want string) {
	t.Helper()
	select {
	case err := <-done:
		if err == nil || err.Error() != want {
			t.Fatalf("refusal=%v want %s", err, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("authorization did not refuse")
	}
}
func TestReconnectHostRejectsWrongChallengeBeforeProof(t *testing.T) {
	for _, tc := range []struct{ name, from, to string }{
		{"session", "session_test", "other_test"}, {"host", `"7"`, `"8"`}, {"placement", `"placement_generation":3`, `"placement_generation":4`},
		{"unknown", "{", `{"extra":true,`}, {"duplicate", "{", `{"protocol":1,`}, {"oversize", "boot_test", strings.Repeat("b", 4096)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, c := reconnectPeer(t)
			var called atomic.Bool
			done := make(chan error, 1)
			go func() {
				out, err := s.AuthorizeGuestReconnect(context.Background(), "session_test", func(context.Context, runner.GuestReconnectChallenge) (string, error) {
					called.Store(true)
					return strings.Repeat("A", 86), nil
				})
				if out != (runner.GuestReconnectAcceptResponse{}) {
					t.Error("failure returned authority")
				}
				done <- err
			}()
			req := c.readMsg(t)
			replyReconnect(t, c, req, true, []byte(strings.Replace(string(reconnectChallengeJSON()), tc.from, tc.to, 1)))
			waitReconnectError(t, done, "invalid")
			if called.Load() {
				t.Fatal("unvalidated challenge reached guest")
			}
		})
	}
}
func TestReconnectHostChecksProofAndAcceptance(t *testing.T) {
	for _, tc := range []struct {
		name, signature, body, want string
		ok                          bool
	}{
		{"bad_signature", "secret.test", "", "invalid", false},
		{"raw_error", strings.Repeat("A", 86), `{"error":"secret.test"}`, "unavailable", false},
		{"fenced", strings.Repeat("A", 86), `{"error":"fenced"}`, "fenced", false},
		{"expired", strings.Repeat("A", 86), `{"error":"expired"}`, "expired", false},
		{"invalid", strings.Repeat("A", 86), `{"error":"invalid"}`, "invalid", false},
		{"bad_success", strings.Repeat("A", 86), `{"epoch":1,"token":"secret.test","expires_in_sec":120}`, "invalid", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, c := reconnectPeer(t)
			done := make(chan error, 1)
			go func() {
				out, err := s.AuthorizeGuestReconnect(context.Background(), "session_test", func(context.Context, runner.GuestReconnectChallenge) (string, error) { return tc.signature, nil })
				if out != (runner.GuestReconnectAcceptResponse{}) {
					t.Error("failure returned authority")
				}
				done <- err
			}()
			replyReconnect(t, c, c.readMsg(t), true, reconnectChallengeJSON())
			if tc.body != "" {
				replyReconnect(t, c, c.readMsg(t), tc.ok, []byte(tc.body))
			}
			waitReconnectError(t, done, tc.want)
		})
	}
}
func TestReconnectHostDoesNotMigrateProofToReplacementSocket(t *testing.T) {
	s, fc, c := reconnectPeer(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	done := make(chan error, 1)
	go func() {
		out, err := s.AuthorizeGuestReconnect(context.Background(), "session_test", func(ctx context.Context, _ runner.GuestReconnectChallenge) (string, error) {
			close(entered)
			select {
			case <-ctx.Done():
			case <-release:
			}
			return strings.Repeat("A", 86), nil
		})
		if out != (runner.GuestReconnectAcceptResponse{}) {
			t.Error("stale proof returned authority")
		}
		done <- err
	}()
	replyReconnect(t, c, c.readMsg(t), true, reconnectChallengeJSON())
	<-entered
	c.c.CloseNow()
	replacement := fc.nextConn(t)
	replacement.readAnnounce(t)
	acceptReconnectPeer(t, replacement, 8)
	waitReconnectError(t, done, "fenced")
	// A current call still works; its first request must be begin, never the old proof.
	retry := make(chan error, 1)
	go func() {
		_, err := s.AuthorizeGuestReconnect(context.Background(), "session_test", func(context.Context, runner.GuestReconnectChallenge) (string, error) {
			return "", errors.New("guest_secret.test")
		})
		retry <- err
	}()
	req := replacement.readMsg(t)
	if req.RPC == nil || req.RPC.Method != runner.MethodBeginGuestReconnect {
		t.Fatal("old proof moved to new socket")
	}
	body := []byte(strings.Replace(string(reconnectChallengeJSON()), `"7"`, `"8"`, 1))
	replyReconnect(t, replacement, req, true, body)
	waitReconnectError(t, retry, "unavailable")
}
func TestReconnectHostBoundsAttemptsAndCleansUp(t *testing.T) {
	s, _, c := reconnectPeer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	prove := func(context.Context, runner.GuestReconnectChallenge) (string, error) {
		return "", errors.New("guest_error.test")
	}
	go func() { _, err := s.AuthorizeGuestReconnect(ctx, "session_test", prove); done <- err }()
	c.readMsg(t)
	if out, err := s.AuthorizeGuestReconnect(ctx, "session_test", prove); err == nil || err.Error() != "unavailable" || out != (runner.GuestReconnectAcceptResponse{}) {
		t.Fatal("parallel attempt admitted")
	}
	cancel()
	waitReconnectError(t, done, "unavailable")
	retry := make(chan error, 1)
	go func() { _, err := s.AuthorizeGuestReconnect(context.Background(), "session_test", prove); retry <- err }()
	replyReconnect(t, c, c.readMsg(t), false, []byte(`{"error":"expired"}`))
	waitReconnectError(t, retry, "expired")
	s.runnerRPC.mu.Lock()
	pending := len(s.runnerRPC.waiting)
	s.runnerRPC.mu.Unlock()
	if pending != 0 {
		t.Fatal("pending RPC leaked")
	}
}
func TestReconnectHostRefusesReplacedInstance(t *testing.T) {
	s, _, c := reconnectPeer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := s.AuthorizeGuestReconnect(ctx, "session_test", func(context.Context, runner.GuestReconnectChallenge) (string, error) {
			s.reg.put("session_test", &sessionEntry{id: "session_test", handle: "replacement_test", state: "running", placementGen: 3})
			return strings.Repeat("A", 86), nil
		})
		done <- err
	}()
	replyReconnect(t, c, c.readMsg(t), true, reconnectChallengeJSON())
	waitReconnectError(t, done, "fenced")
}

func TestReconnectHostGlobalAdmissionBound(t *testing.T) {
	s, _, c := reconnectPeer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 64)
	prove := func(context.Context, runner.GuestReconnectChallenge) (string, error) { return "", nil }
	// Pin the documented production limit independently of its constant.
	for i := 0; i < 64; i++ {
		id := fmt.Sprintf("session_%d_test", i)
		s.reg.put(id, &sessionEntry{id: id, state: "running", placementGen: 3})
		go func() { _, err := s.AuthorizeGuestReconnect(ctx, id, prove); done <- err }()
		req := c.readMsg(t)
		if req.RPC == nil || req.RPC.Method != runner.MethodBeginGuestReconnect {
			t.Fatal("expected pending begin")
		}
	}
	if out, err := s.AuthorizeGuestReconnect(ctx, "session_test", prove); err == nil || err.Error() != "unavailable" || out != (runner.GuestReconnectAcceptResponse{}) {
		t.Fatal("global admission limit exceeded")
	}
	cancel()
	for i := 0; i < 64; i++ {
		waitReconnectError(t, done, "unavailable")
	}
	s.reconnectMu.Lock()
	active := len(s.reconnecting)
	s.reconnectMu.Unlock()
	if active != 0 {
		t.Fatal("admission slots leaked")
	}
}
func TestReconnectHostDeadlineAndLateProof(t *testing.T) {
	s, _, c := reconnectPeer(t)
	done := make(chan error, 1)
	go func() {
		out, err := s.AuthorizeGuestReconnect(context.Background(), "session_test", func(ctx context.Context, _ runner.GuestReconnectChallenge) (string, error) {
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) > 5*time.Second {
				t.Error("unbounded proof context")
			}
			<-ctx.Done()
			return strings.Repeat("A", 86), nil
		})
		if out != (runner.GuestReconnectAcceptResponse{}) {
			t.Error("late proof returned authority")
		}
		done <- err
	}()
	replyReconnect(t, c, c.readMsg(t), true, reconnectChallengeJSON())
	select {
	case err := <-done:
		if err == nil || err.Error() != "fenced" {
			t.Fatalf("late proof error=%v", err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("handshake exceeded budget")
	}
}
func TestReconnectHostRequiresPlacementAndAcceptedConnection(t *testing.T) {
	s, _ := testMicrovmServer(t)
	prove := func(context.Context, runner.GuestReconnectChallenge) (string, error) {
		t.Error("unauthorized proof callback")
		return "", nil
	}
	if out, err := s.AuthorizeGuestReconnect(context.Background(), "absent_test", prove); err == nil || err.Error() != "fenced" || out != (runner.GuestReconnectAcceptResponse{}) {
		t.Fatal("missing session admitted")
	}
	s.reg.put("session_test", &sessionEntry{id: "session_test", state: "running", placementGen: 3})
	if _, err := s.AuthorizeGuestReconnect(context.Background(), "session_test", prove); err == nil || err.Error() != "unavailable" {
		t.Fatal("missing connection admitted")
	}
	if _, err := s.AuthorizeGuestReconnect(context.Background(), "session_test", nil); err == nil || err.Error() != "invalid" {
		t.Fatal("nil proof accepted")
	}
}

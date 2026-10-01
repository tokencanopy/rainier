package driver

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/tokencanopy/rainier/internal/relay"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/protocol/runner"
)

type admissionListener struct {
	g       *guestChannel
	conn    net.Conn
	called  bool
	claimed bool
}

func (l *admissionListener) Accept() (net.Conn, error) {
	if !l.called {
		l.called = true
		return l.conn, nil
	}
	l.g.mu.Lock()
	l.claimed = l.g.served
	l.g.mu.Unlock()
	return nil, net.ErrClosed
}
func (*admissionListener) Close() error   { return nil }
func (*admissionListener) Addr() net.Addr { return &net.UnixAddr{Name: "admission-test", Net: "unix"} }

func TestGuestAdmissionPrecedesWorker(t *testing.T) {
	// The next Accept observes admission synchronously, before the worker can
	// run. A busy listener must not spawn one goroutine per refused peer.
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	host, peer := net.Pipe()
	defer peer.Close()
	g := &guestChannel{boot: runner.BootConfig{Protocol: 1, SessionID: "admission-test"}}
	listener := &admissionListener{g: g, conn: host}
	g.listener = listener
	defer g.close()
	m := &Microvm{}
	m.acceptGuests("admission-test", g)
	if !listener.claimed {
		t.Fatal("accept loop admitted another peer before claiming the worker's connection")
	}
}

type streamAuthorizer func(context.Context, string, GuestReconnectProof) (runner.GuestReconnectAcceptResponse, error)

func (f streamAuthorizer) AuthorizeGuestReconnect(ctx context.Context, id string, prove GuestReconnectProof) (runner.GuestReconnectAcceptResponse, error) {
	return f(ctx, id, prove)
}
func streamChallenge() runner.GuestReconnectChallenge {
	return runner.GuestReconnectChallenge{Protocol: 1, SessionID: "stream-test", BootEpoch: "boot-test", HostIncarnation: "host-test", AttemptID: "attempt-test", PlacementGeneration: 1, Challenge: base64.RawURLEncoding.EncodeToString(make([]byte, 32))}
}
func streamAcceptance() runner.GuestReconnectAcceptResponse {
	return runner.GuestReconnectAcceptResponse{Epoch: 1, Token: base64.RawURLEncoding.EncodeToString(make([]byte, 32)), ExpiresInSec: 120}
}
func TestGuestStreamProofReachesHostAuthority(t *testing.T) {
	a, z := net.Pipe()
	defer a.Close()
	defer z.Close()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	challenge := streamChallenge()
	accepted := streamAcceptance()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	authority := streamAuthorizer(func(ctx context.Context, id string, prove GuestReconnectProof) (runner.GuestReconnectAcceptResponse, error) {
		if id != "stream-test" {
			return runner.GuestReconnectAcceptResponse{}, net.ErrClosed
		}
		signature, err := prove(ctx, challenge)
		if err != nil {
			return runner.GuestReconnectAcceptResponse{}, err
		}
		if err = challenge.VerifyProof(base64.RawURLEncoding.EncodeToString(pub), signature); err != nil {
			return runner.GuestReconnectAcceptResponse{}, err
		}
		return accepted, nil
	})
	done := make(chan error, 1)
	go func() {
		got, err := AuthorizeGuestConnection(ctx, authority, "stream-test", relay.NetConn(a))
		if err == nil && got != accepted {
			err = net.ErrClosed
		}
		done <- err
	}()
	guest := relay.NetConn(z)
	event, err := relay.ReadGuestReconnectFrame(ctx, guest)
	if err != nil {
		t.Fatalf("host did not deliver a bounded challenge: %v", err)
	}
	if event.Kind != relay.KindGuestReconnectChallenge {
		t.Fatal("host sent authority or configuration before proof")
	}
	decoded, err := runner.DecodeGuestReconnectChallenge(event.Payload)
	if err != nil || decoded != challenge {
		t.Fatal("wrong challenge")
	}
	message, _ := decoded.SigningMessage()
	body, _ := json.Marshal(runner.GuestReconnectAcceptRequest{Protocol: 1, AttemptID: decoded.AttemptID, Signature: base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, message))})
	if err = relay.WriteGuestReconnectFrame(ctx, guest, relay.KindGuestReconnectProof, body); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	// Authorization alone must not publish accepted/config or transfer a relay.
	// The caller still owns that guarded handoff. The stream remains usable.
	written := make(chan error, 1)
	go func() { written <- relay.NetConn(a).Write(ctx, []byte("caller-owned")) }()
	raw, err := guest.Read(ctx)
	if err != nil || string(raw) != "caller-owned" {
		t.Fatal("stream was closed or helper published authority")
	}
	if err = <-written; err != nil {
		t.Fatal(err)
	}
}

func TestGuestStreamRejectsInvalidProof(t *testing.T) {
	for _, which := range []string{"attempt", "kind", "unknown", "duplicate", "oversize", "null", "bad-signature", "malformed-acceptance"} {
		t.Run(which, func(t *testing.T) {
			a, z := net.Pipe()
			defer a.Close()
			defer z.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			challenge := streamChallenge()
			authority := streamAuthorizer(func(ctx context.Context, _ string, prove GuestReconnectProof) (runner.GuestReconnectAcceptResponse, error) {
				_, err := prove(ctx, challenge)
				if err != nil {
					return runner.GuestReconnectAcceptResponse{}, err
				}
				accepted := streamAcceptance()
				if which == "malformed-acceptance" {
					accepted.Token = "invalid"
				}
				return accepted, nil
			})
			done := make(chan error, 1)
			go func() {
				got, err := AuthorizeGuestConnection(ctx, authority, "stream-test", relay.NetConn(a))
				if got != (runner.GuestReconnectAcceptResponse{}) {
					done <- errors.New("nonzero refused authority")
					return
				}
				done <- err
			}()
			guest := relay.NetConn(z)
			if _, err := relay.ReadGuestReconnectFrame(ctx, guest); err != nil {
				t.Fatal(err)
			}
			if which == "oversize" {
				_, _ = z.Write([]byte(strings.Repeat("x", 4097)))
			} else {
				proof := runner.GuestReconnectAcceptRequest{Protocol: 1, AttemptID: challenge.AttemptID, Signature: base64.RawURLEncoding.EncodeToString(make([]byte, 64))}
				if which == "attempt" {
					proof.AttemptID = "other-attempt"
				}
				if which == "bad-signature" {
					proof.Signature = "not-canonical"
				}
				body, _ := json.Marshal(proof)
				kind := relay.KindGuestReconnectProof
				switch which {
				case "kind":
					kind = relay.KindGuestReconnectAccepted
				case "unknown":
					body = append(body[:len(body)-1], []byte(`,"extra":1}`)...)
				case "duplicate":
					body = append(body[:len(body)-1], []byte(`,"protocol":1}`)...)
				case "null":
					body = []byte(`null`)
				}
				_ = relay.WriteGuestReconnectFrame(ctx, guest, kind, body)
			}
			if err := <-done; err == nil || err.Error() != "unavailable" {
				t.Fatalf("refusal=%v", err)
			}
			if raw, err := guest.Read(ctx); err == nil || len(raw) > 0 {
				t.Fatal("failed peer received authority or remained connected")
			}
		})
	}
}

func TestGuestStreamHostRefusalSendsNothing(t *testing.T) {
	for _, reason := range []string{"invalid", "expired", "fenced", "provider-secret-test"} {
		t.Run(reason, func(t *testing.T) {
			a, z := net.Pipe()
			defer a.Close()
			defer z.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			authority := streamAuthorizer(func(context.Context, string, GuestReconnectProof) (runner.GuestReconnectAcceptResponse, error) {
				return streamAcceptance(), errors.New(reason)
			})
			got, err := AuthorizeGuestConnection(ctx, authority, "stream-test", relay.NetConn(a))
			want := reason
			if reason == "provider-secret-test" {
				want = "unavailable"
			}
			if got != (runner.GuestReconnectAcceptResponse{}) || err == nil || err.Error() != want {
				t.Fatalf("bad refusal %v", err)
			}
			if raw, err := relay.NetConn(z).Read(ctx); err == nil || len(raw) > 0 {
				t.Fatal("refused host sent guest data")
			}
		})
	}
}

func TestGuestStreamRequiresProofAndCorrectScope(t *testing.T) {
	for _, which := range []string{"no-proof", "wrong-session", "invalid-challenge", "second-proof"} {
		t.Run(which, func(t *testing.T) {
			a, z := net.Pipe()
			defer a.Close()
			defer z.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			authority := streamAuthorizer(func(ctx context.Context, _ string, prove GuestReconnectProof) (runner.GuestReconnectAcceptResponse, error) {
				if which != "no-proof" {
					ch := streamChallenge()
					if which == "wrong-session" {
						ch.SessionID = "other-test"
					}
					if which == "invalid-challenge" {
						ch.Protocol = 2
					}
					_, _ = prove(ctx, ch)
					if which == "second-proof" {
						_, _ = prove(ctx, ch)
					}
				}
				// Even a misbehaving host cannot report acceptance without a valid exchange.
				return streamAcceptance(), nil
			})
			done := make(chan error, 1)
			go func() {
				_, err := AuthorizeGuestConnection(ctx, authority, "stream-test", relay.NetConn(a))
				done <- err
			}()
			if which == "second-proof" {
				guest := relay.NetConn(z)
				ev, err := relay.ReadGuestReconnectFrame(ctx, guest)
				if err != nil {
					t.Fatal(err)
				}
				ch, _ := runner.DecodeGuestReconnectChallenge(ev.Payload)
				body, _ := json.Marshal(runner.GuestReconnectAcceptRequest{Protocol: 1, AttemptID: ch.AttemptID, Signature: base64.RawURLEncoding.EncodeToString(make([]byte, 64))})
				_ = relay.WriteGuestReconnectFrame(ctx, guest, relay.KindGuestReconnectProof, body)
			}
			if err := <-done; err == nil {
				t.Fatal("accepted invalid authority choreography")
			}
			if raw, err := relay.NetConn(z).Read(ctx); err == nil || len(raw) > 0 {
				t.Fatal("invalid challenge or acceptance escaped to guest")
			}
		})
	}
}

func TestGuestStreamCancellationClosesIO(t *testing.T) {
	for _, which := range []string{"outer", "host", "deadline"} {
		t.Run(which, func(t *testing.T) {
			a, z := net.Pipe()
			defer a.Close()
			defer z.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if which == "deadline" {
				var timed context.CancelFunc
				ctx, timed = context.WithTimeout(ctx, 150*time.Millisecond)
				defer timed()
			}
			cancelProof := make(chan context.CancelFunc, 1)
			authority := streamAuthorizer(func(ctx context.Context, _ string, prove GuestReconnectProof) (runner.GuestReconnectAcceptResponse, error) {
				pctx, pcancel := context.WithCancel(ctx)
				defer pcancel()
				cancelProof <- pcancel
				_, err := prove(pctx, streamChallenge())
				return runner.GuestReconnectAcceptResponse{}, err
			})
			done := make(chan error, 1)
			go func() {
				_, err := AuthorizeGuestConnection(ctx, authority, "stream-test", relay.NetConn(a))
				done <- err
			}()
			guest := relay.NetConn(z)
			readCtx, readCancel := context.WithTimeout(context.Background(), time.Second)
			defer readCancel()
			if _, err := relay.ReadGuestReconnectFrame(readCtx, guest); err != nil {
				t.Fatal(err)
			}
			hostCancel := <-cancelProof
			if which == "outer" {
				cancel()
			}
			if which == "host" {
				hostCancel()
			}
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("canceled proof succeeded")
				}
			case <-readCtx.Done():
				t.Fatal("cancellation left proof read blocked")
			}
			if raw, err := guest.Read(readCtx); err == nil || len(raw) > 0 {
				t.Fatal("canceled peer received data")
			}
		})
	}
}

func TestGuestStreamPreservesBufferedBytesForCaller(t *testing.T) {
	a, z := net.Pipe()
	defer a.Close()
	defer z.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	hostConn := relay.NetConn(a)
	guest := relay.NetConn(z)
	authority := streamAuthorizer(func(ctx context.Context, _ string, prove GuestReconnectProof) (runner.GuestReconnectAcceptResponse, error) {
		_, err := prove(ctx, streamChallenge())
		return streamAcceptance(), err
	})
	done := make(chan error, 1)
	go func() { _, err := AuthorizeGuestConnection(ctx, authority, "stream-test", hostConn); done <- err }()
	event, err := relay.ReadGuestReconnectFrame(ctx, guest)
	if err != nil {
		t.Fatal(err)
	}
	challenge, _ := runner.DecodeGuestReconnectChallenge(event.Payload)
	body, _ := json.Marshal(runner.GuestReconnectAcceptRequest{Protocol: 1, AttemptID: challenge.AttemptID, Signature: base64.RawURLEncoding.EncodeToString(make([]byte, 64))})
	payload, _ := json.Marshal(relay.ControlEvent{Kind: relay.KindGuestReconnectProof, Payload: body})
	frame, _ := relay.Encode(relay.Frame{Type: relay.FrameControl, Payload: payload})
	written := make(chan error, 1)
	go func() { _, err := z.Write(append(frame, []byte("\nnext-frame\n")...)); written <- err }()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	raw, err := hostConn.Read(ctx)
	if err != nil || string(raw) != "next-frame" {
		t.Fatal("proof reader lost subsequent bytes")
	}
	if err = <-written; err != nil {
		t.Fatal(err)
	}
}

func TestGuestStreamRefusesLateHostSuccess(t *testing.T) {
	a, z := net.Pipe()
	defer a.Close()
	defer z.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	authority := streamAuthorizer(func(ctx context.Context, _ string, prove GuestReconnectProof) (runner.GuestReconnectAcceptResponse, error) {
		_, err := prove(ctx, streamChallenge())
		if err != nil {
			return runner.GuestReconnectAcceptResponse{}, err
		}
		cancel()
		return streamAcceptance(), nil
	})
	done := make(chan error, 1)
	go func() {
		got, err := AuthorizeGuestConnection(ctx, authority, "stream-test", relay.NetConn(a))
		if got != (runner.GuestReconnectAcceptResponse{}) {
			done <- errors.New("late nonzero authority")
			return
		}
		done <- err
	}()
	guest := relay.NetConn(z)
	readCtx, readCancel := context.WithTimeout(context.Background(), time.Second)
	defer readCancel()
	event, err := relay.ReadGuestReconnectFrame(readCtx, guest)
	if err != nil {
		t.Fatal(err)
	}
	ch, _ := runner.DecodeGuestReconnectChallenge(event.Payload)
	body, _ := json.Marshal(runner.GuestReconnectAcceptRequest{Protocol: 1, AttemptID: ch.AttemptID, Signature: base64.RawURLEncoding.EncodeToString(make([]byte, 64))})
	_ = relay.WriteGuestReconnectFrame(readCtx, guest, relay.KindGuestReconnectProof, body)
	if err = <-done; err == nil || err.Error() != "unavailable" {
		t.Fatalf("late acceptance: %v", err)
	}
	if raw, err := guest.Read(readCtx); err == nil || len(raw) > 0 {
		t.Fatal("late authority reached guest")
	}
}

// internal/controld/srpc_bootstrap_test.go
package controld

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/controlapp"
)

// The two bootstrap methods, driven end to end through the runner plane: a
// scripted fake runner sends the session_req exactly as runnerd forwards one
// out of a sandbox, and the assertions are on the session_rpc that comes back
// down.
//
// The fixture secret is spelled "value_must_not_appear_anywhere" on purpose.
// It is the string the log test greps for, and having every case use it means
// any case that starts leaking it fails that one too.

const bootstrapFixtureSecret = "value_must_not_appear_anywhere"

// seedBootstrapSession seeds a running session on runner, with an environment
// declaring one secret ref whose sealed value is in the vault, and returns the
// session id. It is the state a microVM create leaves behind, minus the token.
func seedBootstrapSession(t *testing.T, st MemStore, id, runnerName string) string {
	t.Helper()
	ctx := context.Background()
	ciphertext, nonce, err := Seal(testSecretsKey, []byte(bootstrapFixtureSecret))
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := st.PutSecret(ctx, "DEPLOY_KEY", ciphertext, nonce); err != nil {
		t.Fatalf("PutSecret: %v", err)
	}
	if _, err := st.Environments().CreateEnvironment(ctx, installWorkspace, control.Environment{
		ID: "env_example", WorkspaceID: installWorkspace, Name: "example",
		SecretRefs: []string{"DEPLOY_KEY"},
	}); err != nil {
		t.Fatalf("CreateEnvironment: %v", err)
	}
	seedSession(t, st, control.Session{
		ID: control.SessionID(id), State: control.StateRunning,
		RunnerID: control.RunnerID(runnerName), EnvironmentID: "env_example",
	})
	return id
}

// mintFor records a token for id at the placement generation the row
// currently holds, the way createSpec does, and returns the plaintext.
func mintFor(t *testing.T, s *Server, st MemStore, id string) string {
	t.Helper()
	row, err := st.Sessions().GetSession(context.Background(), installWorkspace, control.SessionID(id))
	if err != nil {
		t.Fatalf("read %s: %v", id, err)
	}
	minter := controlapp.SessionBootstrapMinter{Store: st.Bootstraps(), Clock: s.clock}
	tok, err := minter.Mint(context.Background(), installWorkspace, control.SessionID(id), row.PlacementGeneration)
	if err != nil {
		t.Fatalf("mint for %s: %v", id, err)
	}
	return tok
}

// bootstrapRequest is the body a sandbox sends: the protocol and the token.
func bootstrapRequest(token string) string {
	b, _ := json.Marshal(map[string]any{"protocol": 1, "token": token})
	return string(b)
}

// refusalText reads the {"error": ...} sentence off an ok:false answer,
// failing the test when the answer was a success.
func refusalText(t *testing.T, payload json.RawMessage) string {
	t.Helper()
	var body struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		t.Fatalf("decoding a refusal payload %s: %v", payload, err)
	}
	return body.Error
}

// TestFetchSessionSecretsTable is the exchange in its five outcomes: one
// success and four refusals with distinct reasons, over the live plane.
//
// Each row drives the same method against a differently prepared session, so
// what distinguishes them is only ever the state of the token — which is the
// claim §3's "three independent refusals, all in the control plane" makes.
func TestFetchSessionSecretsTable(t *testing.T) {
	for _, tc := range []struct {
		name string
		// prepare returns the token the sandbox will present.
		prepare func(t *testing.T, s *Server, st MemStore, id string) string
		wantOK  bool
		// wantReason is a distinguishing fragment of the refusal sentence.
		wantReason string
	}{
		{
			name: "a fresh token is exchanged",
			prepare: func(t *testing.T, s *Server, st MemStore, id string) string {
				return mintFor(t, s, st, id)
			},
			wantOK: true,
		},
		{
			name: "a replayed token is refused",
			prepare: func(t *testing.T, s *Server, st MemStore, id string) string {
				tok := mintFor(t, s, st, id)
				row, _ := st.Sessions().GetSession(context.Background(), installWorkspace, control.SessionID(id))
				if err := st.Bootstraps().ConsumeSessionBootstrap(context.Background(), installWorkspace,
					control.SessionID(id), controlapp.HashSessionBootstrapToken(tok),
					row.PlacementGeneration, s.clock.Now()); err != nil {
					t.Fatalf("first spend: %v", err)
				}
				return tok
			},
			wantReason: "already been exchanged",
		},
		{
			name: "an expired token is refused",
			prepare: func(t *testing.T, s *Server, st MemStore, id string) string {
				row, _ := st.Sessions().GetSession(context.Background(), installWorkspace, control.SessionID(id))
				// Minted as if two hours ago: the mint's own clock is this
				// replica's, so an expiry in the past is written directly.
				tok := "expired_token_example"
				if err := st.Bootstraps().PutSessionBootstrap(context.Background(), installWorkspace,
					control.SessionID(id), control.SessionBootstrap{
						Hash:                controlapp.HashSessionBootstrapToken(tok),
						PlacementGeneration: row.PlacementGeneration,
						ExpiresAt:           s.clock.Now().Add(-2 * time.Hour),
					}); err != nil {
					t.Fatalf("put an expired token: %v", err)
				}
				return tok
			},
			wantReason: "expired",
		},
		{
			name: "a token from a superseded placement is refused",
			prepare: func(t *testing.T, s *Server, st MemStore, id string) string {
				tok := mintFor(t, s, st, id)
				// The session is placed again, which is exactly what a
				// re-placement onto another runner does to the row — and the
				// token minted for the sandbox that no longer exists must
				// stop being an answer. It is placed back onto the SAME
				// runner so the placement guard still admits the request and
				// the fence is what refuses it.
				holder := control.RunnerID("vm1")
				if err := st.Sessions().Transition(context.Background(), installWorkspace, control.SessionID(id),
					control.NonTerminal, control.StateRunning,
					control.TransitionOpts{RunnerID: &holder}); err != nil {
					t.Fatalf("re-place the session: %v", err)
				}
				return tok
			},
			wantReason: "placed again",
		},
		{
			name: "another session's token is refused",
			prepare: func(t *testing.T, s *Server, st MemStore, id string) string {
				seedSession(t, st, control.Session{ID: "sess_neighbour", State: control.StateRunning,
					RunnerID: "vm1", EnvironmentID: "env_example"})
				return mintFor(t, s, st, "sess_neighbour")
			},
			wantReason: "no bootstrap token matching",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, st, ts := newTestControld(t)
			f := joinRunner(t, s, ts, runnerScript{Name: "vm1"})
			id := seedBootstrapSession(t, st, "sess_bootstrap", "vm1")
			token := tc.prepare(t, s, st, id)

			f.sandboxRequest(t, id, 11, "fetch_session_secrets", bootstrapRequest(token))

			cmd := nextSessionRPC(t, f)
			if cmd.Session != id || cmd.RPC.ID != 11 || cmd.RPC.Method != "resp" {
				t.Fatalf("answer = %+v, want a resp for id 11 on %s", cmd.RPC, id)
			}
			if !tc.wantOK {
				if cmd.RPC.OK {
					t.Fatalf("a %s was accepted: %s", tc.name, cmd.RPC.Payload)
				}
				reason := refusalText(t, cmd.RPC.Payload)
				if !strings.Contains(reason, tc.wantReason) {
					t.Fatalf("refusal = %q, want it to name %q", reason, tc.wantReason)
				}
				if strings.Contains(reason, bootstrapFixtureSecret) || strings.Contains(reason, token) {
					t.Fatalf("a refusal carried the secret or the token: %q", reason)
				}
				return
			}
			if !cmd.RPC.OK {
				t.Fatalf("a fresh token was refused: %q", refusalText(t, cmd.RPC.Payload))
			}
			var answer struct {
				Env map[string]string `json:"env"`
			}
			if err := json.Unmarshal(cmd.RPC.Payload, &answer); err != nil {
				t.Fatalf("decoding the answer: %v", err)
			}
			if answer.Env["DEPLOY_KEY"] != bootstrapFixtureSecret {
				t.Fatalf("the answer delivered %d name(s) and not the environment's secret", len(answer.Env))
			}
		})
	}
}

// TestFetchSessionSecretsIsSingleUseOverTheWire is the replay row again, but
// driven the way a replay actually happens: the same token presented twice
// over the plane, with nothing else touching the store in between.
func TestFetchSessionSecretsIsSingleUseOverTheWire(t *testing.T) {
	s, st, ts := newTestControld(t)
	f := joinRunner(t, s, ts, runnerScript{Name: "vm1"})
	id := seedBootstrapSession(t, st, "sess_single_use", "vm1")
	token := mintFor(t, s, st, id)

	f.sandboxRequest(t, id, 1, "fetch_session_secrets", bootstrapRequest(token))
	if first := nextSessionRPC(t, f); !first.RPC.OK {
		t.Fatalf("the first exchange was refused: %q", refusalText(t, first.RPC.Payload))
	}

	f.sandboxRequest(t, id, 2, "fetch_session_secrets", bootstrapRequest(token))
	second := nextSessionRPC(t, f)
	if second.RPC.OK {
		t.Fatalf("the second exchange succeeded: %s", second.RPC.Payload)
	}
	if reason := refusalText(t, second.RPC.Payload); !strings.Contains(reason, "already been exchanged") {
		t.Fatalf("the replay's refusal = %q", reason)
	}
}

// TestFetchSessionSecretsRefusesAnEmptyOrMisversionedRequest pins the two
// pre-checks that run before the store is touched at all.
func TestFetchSessionSecretsRefusesAnEmptyOrMisversionedRequest(t *testing.T) {
	s, st, ts := newTestControld(t)
	f := joinRunner(t, s, ts, runnerScript{Name: "vm1"})
	id := seedBootstrapSession(t, st, "sess_prechecks", "vm1")
	mintFor(t, s, st, id)

	for i, tc := range []struct {
		body       string
		wantReason string
	}{
		{`{"protocol":1}`, "carried no bootstrap token"},
		{`{"protocol":2,"token":"x"}`, "must be replaced"},
		{`{"protocol":"one"}`, "could not be decoded"},
	} {
		f.sandboxRequest(t, id, uint64(20+i), "fetch_session_secrets", tc.body)
		cmd := nextSessionRPC(t, f)
		if cmd.RPC.OK {
			t.Fatalf("%q was accepted", tc.body)
		}
		if reason := refusalText(t, cmd.RPC.Payload); !strings.Contains(reason, tc.wantReason) {
			t.Fatalf("%q refused with %q, want it to name %q", tc.body, reason, tc.wantReason)
		}
	}
}

// TestMintSessionBootstrapAnswersARunner is the cold-resume half: runnerd
// originates the request on a session the guard has placed on it, and gets a
// token that works exactly once.
func TestMintSessionBootstrapAnswersARunner(t *testing.T) {
	s, st, ts := newTestControld(t)
	f := joinRunner(t, s, ts, runnerScript{Name: "vm1"})
	id := seedBootstrapSession(t, st, "sess_mint", "vm1")

	f.sandboxRequest(t, id, 31, "mint_session_bootstrap", `{"protocol":1}`)

	cmd := nextSessionRPC(t, f)
	if cmd.RPC.ID != 31 || !cmd.RPC.OK {
		t.Fatalf("mint answer = %+v", cmd.RPC)
	}
	var answer struct {
		Token        string `json:"token"`
		ExpiresInSec int    `json:"expires_in_sec"`
	}
	if err := json.Unmarshal(cmd.RPC.Payload, &answer); err != nil {
		t.Fatalf("decoding the mint answer: %v", err)
	}
	if answer.Token == "" {
		t.Fatal("the mint answered with no token")
	}
	if answer.ExpiresInSec != int(controlapp.SessionBootstrapTTL.Seconds()) {
		t.Fatalf("expires_in_sec = %d, want %d", answer.ExpiresInSec, int(controlapp.SessionBootstrapTTL.Seconds()))
	}

	// And the token it minted is the one that works.
	f.sandboxRequest(t, id, 32, "fetch_session_secrets", bootstrapRequest(answer.Token))
	used := nextSessionRPC(t, f)
	if !used.RPC.OK {
		t.Fatalf("the freshly minted token was refused: %q", refusalText(t, used.RPC.Payload))
	}
}

// TestMintSessionBootstrapRetiresThePreviousToken is what makes a cold resume
// safe: the token the last boot was handed stops working the moment a new one
// is minted, whether or not it was ever spent.
func TestMintSessionBootstrapRetiresThePreviousToken(t *testing.T) {
	s, st, ts := newTestControld(t)
	f := joinRunner(t, s, ts, runnerScript{Name: "vm1"})
	id := seedBootstrapSession(t, st, "sess_remint", "vm1")
	stale := mintFor(t, s, st, id)

	f.sandboxRequest(t, id, 41, "mint_session_bootstrap", `{"protocol":1}`)
	if cmd := nextSessionRPC(t, f); !cmd.RPC.OK {
		t.Fatalf("the re-mint was refused: %q", refusalText(t, cmd.RPC.Payload))
	}

	f.sandboxRequest(t, id, 42, "fetch_session_secrets", bootstrapRequest(stale))
	cmd := nextSessionRPC(t, f)
	if cmd.RPC.OK {
		t.Fatalf("the retired token still worked: %s", cmd.RPC.Payload)
	}
	if reason := refusalText(t, cmd.RPC.Payload); !strings.Contains(reason, "no bootstrap token matching") {
		t.Fatalf("the retired token's refusal = %q", reason)
	}
}

// TestBootstrapMethodsAreRefusedForAnotherRunnersSession pins that the guard
// above these arms is the one that decides who may ask. It is the same guard
// the three existing methods keep, and it is what makes "no session id in the
// mint request" a design and not an omission: the id is the one the store
// placed on the asking runner.
func TestBootstrapMethodsAreRefusedForAnotherRunnersSession(t *testing.T) {
	s, st, ts := newTestControld(t)
	f := joinRunner(t, s, ts, runnerScript{Name: "vm1"})
	id := seedBootstrapSession(t, st, "sess_elsewhere", "vm2")
	// A token really does exist for it, so the refusal below is the guard's
	// and not an accident of there being nothing to find.
	token := mintFor(t, s, st, id)

	for i, method := range []string{"fetch_session_secrets", "mint_session_bootstrap"} {
		f.sandboxRequest(t, id, uint64(50+i), method, bootstrapRequest(token))
		cmd := nextSessionRPC(t, f)
		if cmd.RPC.OK {
			t.Fatalf("%s was answered for a session placed on another runner: %s", method, cmd.RPC.Payload)
		}
		if reason := refusalText(t, cmd.RPC.Payload); !strings.Contains(reason, "not placed on the runner that asked") {
			t.Fatalf("%s refused with %q, want the placement guard's sentence", method, reason)
		}
	}
}

// TestTheBootstrapExchangeLeavesNothingInTheLog is §15.1, checked rather than
// claimed: this test captures the package's own log output across a whole
// successful exchange — a mint, a fetch, and a refusal — and greps it for the
// fixture secret and for every token that crossed the wire.
//
// A log line is the easiest place for a value to end up and the hardest place
// to get it back out of, which is why this is a test and not a review note.
func TestTheBootstrapExchangeLeavesNothingInTheLog(t *testing.T) {
	var captured bytes.Buffer
	prevOut, prevFlags := log.Writer(), log.Flags()
	log.SetOutput(&captured)
	log.SetFlags(0)
	t.Cleanup(func() { log.SetOutput(prevOut); log.SetFlags(prevFlags) })

	s, st, ts := newTestControld(t)
	f := joinRunner(t, s, ts, runnerScript{Name: "vm1"})
	id := seedBootstrapSession(t, st, "sess_log", "vm1")

	// A mint, whose answer carries a token.
	f.sandboxRequest(t, id, 61, "mint_session_bootstrap", `{"protocol":1}`)
	mintCmd := nextSessionRPC(t, f)
	var minted struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(mintCmd.RPC.Payload, &minted); err != nil {
		t.Fatalf("decoding the mint answer: %v", err)
	}

	// A successful fetch, which delivers the fixture secret...
	f.sandboxRequest(t, id, 62, "fetch_session_secrets", bootstrapRequest(minted.Token))
	if cmd := nextSessionRPC(t, f); !cmd.RPC.OK {
		t.Fatalf("the fetch was refused: %q", refusalText(t, cmd.RPC.Payload))
	}
	// ...and a replay, which is refused.
	f.sandboxRequest(t, id, 63, "fetch_session_secrets", bootstrapRequest(minted.Token))
	if cmd := nextSessionRPC(t, f); cmd.RPC.OK {
		t.Fatal("the replay succeeded")
	}

	out := captured.String()
	for _, forbidden := range []string{bootstrapFixtureSecret, minted.Token} {
		if forbidden == "" {
			t.Fatal("the test's own fixture is empty; it would grep for nothing")
		}
		if strings.Contains(out, forbidden) {
			t.Fatalf("the log carries a value it must never carry:\n%s", out)
		}
	}
	// And the log DID say something about this session, so the grep above is
	// over real output rather than over an empty buffer.
	if !strings.Contains(out, string(id)) {
		t.Fatalf("no log line mentions the session at all; the grep proves nothing:\n%s", out)
	}
}

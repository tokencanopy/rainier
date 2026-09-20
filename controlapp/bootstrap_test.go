package controlapp

import (
	"context"
	"encoding/base64"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// bootstrapRow is the session every case here dispatches: a creator (so the
// agent-home block runs and Spec.Env is non-empty even with nothing secret in
// it), an environment, and an image.
func bootstrapRow() control.Session {
	return control.Session{
		ID: "sess_example", WorkspaceID: "ws_example", CreatorID: "user_example",
		EnvironmentID: "env_example",
		Spec:          control.PortableSpec{Image: "registry.example.invalid/base@sha256:0000"},
	}
}

// TestCreateSpecWithholdsOnlyForMicrovm is the whole of §3's "either the
// secrets or the token, never both", over the matrix of capabilities a
// placement's runner can announce.
//
// The Docker rows are the compatibility floor and are asserted as strictly as
// the microVM one: a runner that does not announce microvm.v1 — which is
// every runner in the fleet today — must be dispatched the values in
// Spec.Env, with no token and no names, byte for byte what it has always
// received.
func TestCreateSpecWithholdsOnlyForMicrovm(t *testing.T) {
	for _, tc := range []struct {
		name     string
		caps     []string
		withheld bool
	}{
		{"a runner announcing nothing", nil, false},
		{"a runner announcing exec.v1", []string{runner.CapabilityExecV1}, false},
		{"a runner announcing microvm.v1", []string{runner.CapabilityMicrovmV1}, true},
		{"a runner announcing both", []string{runner.CapabilityExecV1, runner.CapabilityMicrovmV1}, true},
		{"a runner announcing something that merely looks like it", []string{"microvm"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFleetFixture(t)
			fx.resolver.material = LaunchMaterial{Environment: map[string]string{
				"DEPLOY_KEY": "value_must_not_travel",
				"NPM_TOKEN":  "value_must_not_travel_either",
			}}

			spec, fail := fx.service.createSpec(fleetCtx, bootstrapRow(), nil, tc.caps, 7)
			if fail != "" {
				t.Fatalf("createSpec failed: %s", fail)
			}

			if !tc.withheld {
				if spec.BootstrapToken != "" || len(spec.SecretNames) != 0 {
					t.Fatalf("a runner that announced no microvm.v1 was sent a token or names: %+v", spec)
				}
				if spec.Env["DEPLOY_KEY"] != "value_must_not_travel" ||
					spec.Env["NPM_TOKEN"] != "value_must_not_travel_either" {
					t.Fatalf("the docker path lost its secret values: %v", slices.Sorted(mapKeys(spec.Env)))
				}
				if _, minted := fx.bootstraps.minted("sess_example"); minted {
					t.Fatal("a docker create minted a bootstrap token")
				}
				return
			}

			// Withheld: the names travel, the values do not, and a token does.
			if !slices.Equal(spec.SecretNames, []string{"DEPLOY_KEY", "NPM_TOKEN"}) {
				t.Fatalf("secret names = %v, want both, sorted", spec.SecretNames)
			}
			for _, name := range spec.SecretNames {
				if v, present := spec.Env[name]; present {
					t.Fatalf("a withheld create carried %s = %q in Spec.Env", name, v)
				}
			}
			if spec.BootstrapToken == "" {
				t.Fatal("a withheld create carries no bootstrap token; its guest can never get its secrets")
			}
			// The agent home is configuration and stays, which is what makes
			// the boot config a useful message at all.
			for k, v := range AgentsEnv(AgentProviders()) {
				if spec.Env[k] != v {
					t.Fatalf("a withheld create lost the agent-home key %s: %v",
						k, slices.Sorted(mapKeys(spec.Env)))
				}
			}

			// The token is 32 bytes of base64url, and what was RECORDED is its
			// hash against this placement — never the token.
			raw, err := base64.RawURLEncoding.DecodeString(spec.BootstrapToken)
			if err != nil || len(raw) != 32 {
				t.Fatalf("token decodes to %d bytes (%v), want 32 base64url bytes", len(raw), err)
			}
			rec, minted := fx.bootstraps.minted("sess_example")
			if !minted {
				t.Fatal("nothing was recorded for the token that was handed out")
			}
			if rec.hash != HashSessionBootstrapToken(spec.BootstrapToken) {
				t.Fatal("the recorded hash is not this token's")
			}
			if rec.hash == spec.BootstrapToken || strings.Contains(rec.hash, spec.BootstrapToken) {
				t.Fatal("the plaintext token reached the store")
			}
			if rec.gen != 7 {
				t.Fatalf("recorded placement generation = %d, want the create's 7", rec.gen)
			}
			want := fx.clock.Now().Add(SessionBootstrapTTL)
			if !rec.expiresAt.Equal(want) {
				t.Fatalf("expiry = %s, want %s (%s from the mint)", rec.expiresAt, want, SessionBootstrapTTL)
			}
		})
	}
}

// TestCreateSpecMintsEvenWithNoSecrets pins the rule that makes the driver's
// own refusal safe: a microVM create ALWAYS carries a token, even when the
// environment declares no secret_refs at all.
//
// The alternative — mint only when there is something to fetch — looks
// tidier and breaks the compatibility row it is supposed to serve. A session
// with a creator and no secrets still has a non-empty Spec.Env (the agent
// manifest), and the microVM driver refuses a create with values and no
// token; so "mint only when needed" would refuse every scratch session on a
// microVM runner.
func TestCreateSpecMintsEvenWithNoSecrets(t *testing.T) {
	fx := newFleetFixture(t)
	fx.resolver.material = LaunchMaterial{}

	spec, fail := fx.service.createSpec(fleetCtx, bootstrapRow(), nil, []string{runner.CapabilityMicrovmV1}, 3)
	if fail != "" {
		t.Fatalf("createSpec failed: %s", fail)
	}
	if spec.BootstrapToken == "" {
		t.Fatal("a microVM create with no secrets carries no token, so its driver will refuse it")
	}
	if len(spec.SecretNames) != 0 {
		t.Fatalf("secret names = %v, want none declared", spec.SecretNames)
	}
	if len(spec.Env) == 0 {
		t.Fatal("the create lost the agent manifest, which is configuration and must stay")
	}
}

// TestCreateSpecFailsClosedWhenTheTokenCannotBeRecorded is the fail-closed
// half. A store that will not record the hash leaves nothing able to verify
// the token, so the alternatives are refusing the create or dispatching the
// secrets after all — and the second is the exposure this design removes.
func TestCreateSpecFailsClosedWhenTheTokenCannotBeRecorded(t *testing.T) {
	fx := newFleetFixture(t)
	fx.resolver.material = LaunchMaterial{Environment: map[string]string{"DEPLOY_KEY": "value_must_not_travel"}}
	fx.bootstraps.putErr = errors.New("the store is down")

	spec, fail := fx.service.createSpec(fleetCtx, bootstrapRow(), nil, []string{runner.CapabilityMicrovmV1}, 1)
	if fail == "" {
		t.Fatalf("createSpec succeeded with an unrecordable token: %+v", spec)
	}
	if spec != nil {
		t.Fatalf("a failed create still produced a spec: %+v", spec)
	}
	if strings.Contains(fail, "value_must_not_travel") || strings.Contains(fail, "DEPLOY_KEY") {
		t.Fatalf("the failure reason quotes the material: %q", fail)
	}
}

// TestAnUnreadablePlacementFailsAWithholdingCreateOnly is finding 6 of the
// branch's own review, and the reason placedGeneration now reports whether
// it could read anything.
//
// Zero has always been "not carried", and on an EVENT it fences nothing. The
// same number is now also the generation a bootstrap token is minted
// against, and there it is not inert: a token recorded at 0 against a row at
// 3 is refused on its one and only exchange, as superseded by the very
// placement that minted it — a store blip becoming a dead session with a
// misleading reason and no way back, because a guest cannot re-mint.
//
// So a withholding create refuses, and every other create is dispatched
// exactly as it always was. Both halves are asserted, because failing the
// second would be a regression for the whole fleet.
func TestAnUnreadablePlacementFailsAWithholdingCreateOnly(t *testing.T) {
	// The row is deliberately never seeded, so the read-back GetSession
	// makes cannot answer — the same shape as a store that is down.
	row := control.Session{
		ID: "sess_unplaced", WorkspaceID: "ws_example", State: control.StateCreating,
		PoolID: "pool_example", RunnerID: "vm1",
		Spec: control.PortableSpec{Image: "img:latest"},
	}

	t.Run("a withholding create refuses", func(t *testing.T) {
		fx := newFleetFixture(t)
		fx.st.seedRunner(fleetSeededRunner("vm1", 2, 0, true))
		fx.service.dispatchCreate(fleetCtx, "pool_example", row, "vm1",
			[]string{runner.CapabilityMicrovmV1}, nil)

		if got := fx.transport.dispatchedCommands(); len(got) != 0 {
			t.Fatalf("dispatched %d command(s) with a placement it could not read: %+v", len(got), got)
		}
		if _, minted := fx.bootstraps.minted("sess_unplaced"); minted {
			t.Fatal("a token was minted against a placement generation nobody could read")
		}
	})

	t.Run("every other create is dispatched", func(t *testing.T) {
		fx := newFleetFixture(t)
		fx.st.seedRunner(fleetSeededRunner("vm1", 2, 0, true))
		fx.service.dispatchCreate(fleetCtx, "pool_example", row, "vm1", nil, nil)

		got := fx.transport.dispatchedCommands()
		if len(got) != 1 {
			t.Fatalf("dispatched %d command(s), want 1 — an unreadable placement fences nothing here", len(got))
		}
		if got[0].PlacementGeneration != 0 {
			t.Fatalf("placement generation = %d, want 0 (not carried)", got[0].PlacementGeneration)
		}
	})
}

// TestWithholdableNamesRespectsTheAgentHomeReservation pins that a
// secret_ref spelled like an agent-home variable is dropped on BOTH paths.
//
// On the Docker path the reservation already wins — AgentsEnv's keys are
// launch invariants a workspace's configuration may not replace — and a
// microVM guest that was handed the same name as a name to fetch would apply
// the workspace's value over its own credential-custody path after boot,
// which is the reservation defeated one hop later.
func TestWithholdableNamesRespectsTheAgentHomeReservation(t *testing.T) {
	fx := newFleetFixture(t)
	reserved := AgentsEnv(AgentProviders())
	var anyReserved string
	for k := range reserved {
		if anyReserved == "" || k < anyReserved {
			anyReserved = k
		}
	}
	fx.resolver.material = LaunchMaterial{Environment: map[string]string{
		anyReserved:  "value_must_not_travel",
		"DEPLOY_KEY": "value_must_not_travel_either",
	}}

	spec, fail := fx.service.createSpec(fleetCtx, bootstrapRow(), nil, []string{runner.CapabilityMicrovmV1}, 1)
	if fail != "" {
		t.Fatalf("createSpec failed: %s", fail)
	}
	if slices.Contains(spec.SecretNames, anyReserved) {
		t.Fatalf("the reserved key %q was promised to the guest as a secret name: %v", anyReserved, spec.SecretNames)
	}
	if !slices.Contains(spec.SecretNames, "DEPLOY_KEY") {
		t.Fatalf("the ordinary secret name was dropped too: %v", spec.SecretNames)
	}
}

// TestSessionBootstrapMinterRecordsAndNeverRepeats pins the two properties of
// the mint that no store can supply: the token is fresh every time, and what
// leaves the minter is the only copy.
func TestSessionBootstrapMinterRecordsAndNeverRepeats(t *testing.T) {
	store := newBootstrapStub()
	clock := &fleetFakeClock{now: time.Unix(1_700_000_000, 0)}
	m := SessionBootstrapMinter{Store: store, Clock: clock}

	seen := map[string]struct{}{}
	for i := 0; i < 64; i++ {
		tok, err := m.Mint(context.Background(), "ws_example", "sess_example", uint64(i))
		if err != nil {
			t.Fatalf("mint %d: %v", i, err)
		}
		if _, dup := seen[tok]; dup {
			t.Fatalf("mint %d repeated a token", i)
		}
		seen[tok] = struct{}{}
	}

	// And a minter with no store refuses rather than handing out a
	// capability nothing can ever check.
	if _, err := (SessionBootstrapMinter{Clock: clock}).Mint(context.Background(), "ws_example", "sess_example", 1); err == nil {
		t.Fatal("a minter with no store handed out a token")
	}
}

// TestSessionBootstrapRefusalNamesTheConditionAndNothingElse pins §15.1 at
// the one place these sentences are written: four distinguishable answers,
// none of which carries a token, a value, or another session's id.
func TestSessionBootstrapRefusalNamesTheConditionAndNothingElse(t *testing.T) {
	seen := map[string]struct{}{}
	for _, err := range []error{
		control.ErrBootstrapUnknown, control.ErrBootstrapSpent,
		control.ErrBootstrapExpired, control.ErrBootstrapFenced,
	} {
		sentence, ok := SessionBootstrapRefusal(err)
		if !ok || sentence == "" {
			t.Fatalf("%v produced no sentence", err)
		}
		if _, dup := seen[sentence]; dup {
			t.Fatalf("%v repeats another refusal's sentence: %q", err, sentence)
		}
		seen[sentence] = struct{}{}
	}
	if _, ok := SessionBootstrapRefusal(errors.New("the store is down")); ok {
		t.Fatal("an error nobody wrote to be shown was rendered as a refusal")
	}
	if _, ok := SessionBootstrapRefusal(nil); ok {
		t.Fatal("a nil error was rendered as a refusal")
	}
}

// mapKeys is slices.Sorted's input for a map whose keys a failure message
// names. It exists so a failure prints key names and never values.
func mapKeys(m map[string]string) func(func(string) bool) {
	return func(yield func(string) bool) {
		for k := range m {
			if !yield(k) {
				return
			}
		}
	}
}

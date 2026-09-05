// internal/controld/agentvault_test.go
package controld

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/controlapp"
	"github.com/tokencanopy/rainier/controlapp/repotest"
)

// agentTestProviders returns the first two rows of the provider table. The
// names are read off controlapp rather than spelled: this package must never
// contain one (plan §Global Constraints), and a test is not an exception —
// it is the place the rule is easiest to break and hardest to notice.
func agentTestProviders(t *testing.T) (string, string) {
	t.Helper()
	rows := controlapp.AgentProviders()
	if len(rows) < 2 {
		t.Fatalf("the provider table has %d rows; these tests need two", len(rows))
	}
	return rows[0].Name, rows[1].Name
}

// TestAgentVaultStoreContract runs the public custody contract against the
// self-hosted vault over the in-memory store. The suite is the same one
// pgstore runs (pgstore_test.go) and the one a hosted store will run: passing
// it is what "implements controlapp.AgentCredentialStore" means.
func TestAgentVaultStoreContract(t *testing.T) {
	repotest.RunAgentCredentialStore(t, func(t *testing.T) controlapp.AgentCredentialStore {
		return NewAgentVault(NewMemStore(), testSecretsKey)
	})
}

// TestAgentVaultBindsUserProviderVersion is the seal's whole reason for
// carrying additional authenticated data.
//
// A sealed row is three columns in a table an operator can reach and a
// replica can copy. Without binding, moving those bytes to another person's
// row would hand that person somebody else's login, and rewriting the version
// column would let a replayed set claim to be current. With it, each of those
// edits produces bytes that do not decrypt — indistinguishable from a wrong
// key, which is exactly right, because a row that was moved is not a
// credential.
//
// Every case here copies a REAL sealed row rather than a synthetic one, so
// what is proven is that the vault's own output fails in the vault's own
// reader.
func TestAgentVaultBindsUserProviderVersion(t *testing.T) {
	ctx := context.Background()
	first, second := agentTestProviders(t)
	const (
		userA = control.ActorID("user_example")
		userB = control.ActorID("user_other")
	)

	// One sealed row, produced normally, and read back out of the store so
	// the test holds exactly the bytes the database holds.
	seed := func(t *testing.T) (MemStore, *AgentVault, AgentCredential) {
		t.Helper()
		st := NewMemStore()
		v := NewAgentVault(st, testSecretsKey)
		if _, err := v.PutAgentCredentials(ctx, userA, first,
			map[string][]byte{"file_example": []byte("credential_example")}); err != nil {
			t.Fatalf("put: %v", err)
		}
		row, err := st.GetAgentCredential(ctx, string(userA), first)
		if err != nil {
			t.Fatalf("read back the sealed row: %v", err)
		}
		if row.Version != 1 {
			t.Fatalf("seeded version = %d, want 1", row.Version)
		}
		// It opens where it belongs. Everything below is the same bytes
		// somewhere else.
		if set, err := v.FetchAgentCredentials(ctx, userA, first); err != nil || set.Version != 1 {
			t.Fatalf("the seeded row did not open in its own place: %+v, %v", set, err)
		}
		return st, v, row
	}

	for _, tc := range []struct {
		name  string
		place func(row AgentCredential) (AgentCredential, control.ActorID, string)
	}{
		{
			name: "copied to another person",
			place: func(row AgentCredential) (AgentCredential, control.ActorID, string) {
				row.UserID, row.Version = string(userB), 1
				return row, userB, first
			},
		},
		{
			name: "copied to another provider",
			place: func(row AgentCredential) (AgentCredential, control.ActorID, string) {
				row.Provider, row.Version = second, 1
				return row, userA, second
			},
		},
		{
			name: "replayed at another version",
			place: func(row AgentCredential) (AgentCredential, control.ActorID, string) {
				// The row is already at version 1, so a compare-and-set to 2
				// is exactly what a replay of the same bytes looks like.
				row.Version = 2
				return row, userA, first
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, v, row := seed(t)
			moved, user, provider := tc.place(row)
			if _, err := st.PutAgentCredential(ctx, moved); err != nil {
				t.Fatalf("placing the copied row: %v", err)
			}
			set, err := v.FetchAgentCredentials(ctx, user, provider)
			if err == nil {
				t.Fatalf("a row %s opened anyway: version %d, %d files", tc.name, set.Version, len(set.Files))
			}
			if len(set.Files) != 0 || set.Version != 0 {
				t.Fatalf("a failed open still returned %d files at version %d", len(set.Files), set.Version)
			}
			// The failure says what happened and never what it happened to.
			if strings.Contains(err.Error(), "credential_example") || strings.Contains(err.Error(), "file_example") {
				t.Fatalf("the failure named the value: %v", err)
			}
		})
	}
}

// TestAgentVaultKeepsTheVersionAndTheCiphertextTogether: the version column
// is part of the credential's identity, so the vault must never store bytes
// sealed for a version the row does not end up holding. The compare-and-set
// is what guarantees it — here it is watched directly, by racing a put
// against a write that lands between the vault's read and its own.
func TestAgentVaultKeepsTheVersionAndTheCiphertextTogether(t *testing.T) {
	ctx := context.Background()
	first, _ := agentTestProviders(t)
	const user = control.ActorID("user_example")

	st := NewMemStore()
	v := NewAgentVault(st, testSecretsKey)
	if _, err := v.PutAgentCredentials(ctx, user, first, map[string][]byte{"file_example": []byte("credential_example")}); err != nil {
		t.Fatalf("first put: %v", err)
	}

	// A store that lets one write slip in between the vault's read and its
	// own, exactly once — the interleaving the CAS exists for.
	raced := &racingAgentRows{AgentCredentialRows: st, onRead: func() {
		other := NewAgentVault(st, testSecretsKey)
		if _, err := other.PutAgentCredentials(ctx, user, first, map[string][]byte{"file_example": []byte("auth_example")}); err != nil {
			t.Errorf("the racing put failed: %v", err)
		}
	}}
	racedVault := NewAgentVault(raced, testSecretsKey)
	version, err := racedVault.PutAgentCredentials(ctx, user, first, map[string][]byte{"file_example": []byte("credential_example")})
	if err != nil {
		t.Fatalf("put under contention: %v", err)
	}
	// The loser re-read, re-sealed, and landed on the version after the
	// winner's — never on the winner's own.
	if version != 3 {
		t.Fatalf("version after contention = %d, want 3 (1 seeded, 2 raced in, 3 retried)", version)
	}
	set, err := racedVault.FetchAgentCredentials(ctx, user, first)
	if err != nil {
		t.Fatalf("fetch after contention: %v", err)
	}
	if set.Version != 3 || !bytes.Equal(set.Files["file_example"], []byte("credential_example")) {
		t.Fatalf("fetch after contention = version %d with %d files, want the retried put's bytes at version 3",
			set.Version, len(set.Files))
	}
}

// racingAgentRows fires onRead once, on the first GetAgentCredential, so a
// test can place a competing write exactly in the compare-and-set's window.
type racingAgentRows struct {
	AgentCredentialRows
	onRead func()
	fired  bool
}

func (r *racingAgentRows) GetAgentCredential(ctx context.Context, userID, provider string) (AgentCredential, error) {
	c, err := r.AgentCredentialRows.GetAgentCredential(ctx, userID, provider)
	if !r.fired {
		r.fired = true
		r.onRead()
	}
	return c, err
}

// TestAgentCredentialRowsRejectAVersionOutOfStep pins the store-side half of
// the same guarantee, at the memstore. A write whose version is not one past
// what is stored changes nothing and reports control.ErrConflict — the
// vault's cue to re-seal rather than to store a blob that will never open.
func TestAgentCredentialRowsRejectAVersionOutOfStep(t *testing.T) {
	ctx := context.Background()
	first, _ := agentTestProviders(t)
	st := NewMemStore()
	row := AgentCredential{UserID: "user_example", Provider: first,
		Ciphertext: []byte("sealed"), Nonce: []byte("nonce"), Version: 1}

	if v, err := st.PutAgentCredential(ctx, row); err != nil || v != 1 {
		t.Fatalf("first put = %d, %v; want 1", v, err)
	}
	for _, version := range []uint64{1, 3, 7} {
		out := row
		out.Version = version
		if _, err := st.PutAgentCredential(ctx, out); !errors.Is(err, control.ErrConflict) {
			t.Fatalf("put at version %d = %v, want ErrConflict", version, err)
		}
	}
	if _, err := st.PutAgentCredential(ctx, AgentCredential{UserID: "user_example", Provider: first, Version: 0}); !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("put at version 0 = %v, want ErrInvalid", err)
	}
	// The row is untouched by every rejection.
	got, err := st.GetAgentCredential(ctx, "user_example", first)
	if err != nil || got.Version != 1 {
		t.Fatalf("row after the rejections = version %d, %v; want 1", got.Version, err)
	}

	// A listing never reads the sealed columns.
	list, err := st.ListAgentCredentials(ctx, "user_example")
	if err != nil || len(list) != 1 {
		t.Fatalf("list = %v, %v", list, err)
	}
	if list[0].Ciphertext != nil || list[0].Nonce != nil {
		t.Fatal("a listing carried the sealed bytes")
	}
}

// internal/controld/vault_test.go
package controld

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// vaultToken is the fake GitHub access token every vault test seals. It is
// deliberately distinctive so an assertion that some byte slice, error, or
// response "does not contain the token" is a real assertion and not a
// coincidence.
const vaultToken = "gho_vault_token_do_not_leak"

// newVaultServer returns a Server over a fresh memstore with the test
// secrets key, plus that store — the vault is pure over (Store, SecretsKey),
// so its unit tests need no HTTP surface at all.
func newVaultServer(t *testing.T) (*Server, Store) {
	t.Helper()
	st := NewMemStore()
	s, err := New(st, Config{
		RunnerToken: "tok",
		ExternalURL: "http://controld.test",
		SecretsKey:  testSecretsKey,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s, st
}

// seedVaultUser makes a user row to hang credentials off, the way the auth
// exchange would.
func seedVaultUser(t *testing.T, st Store, githubID int64, login string) User {
	t.Helper()
	u, err := st.UpsertUser(context.Background(), githubID, login, "member")
	if err != nil {
		t.Fatalf("UpsertUser(%s): %v", login, err)
	}
	return u
}

// seedGitHubCredential stores userID's GitHub credential the way a login
// does, then backdates its last-used stamp by an hour and returns that stamp.
// A mint is a USE, so the tests that drive one through the session RPC prove
// it by watching last_used_at leave a value it could not have written itself.
func seedGitHubCredential(t *testing.T, s *Server, st Store, userID string) time.Time {
	t.Helper()
	ctx := context.Background()
	if err := s.storeGitHubCredential(ctx, userID, vaultToken, "repo, read:user"); err != nil {
		t.Fatalf("storeGitHubCredential: %v", err)
	}
	c, err := st.GetCredential(ctx, userID, githubProvider)
	if err != nil {
		t.Fatalf("GetCredential: %v", err)
	}
	stale := time.Now().Add(-time.Hour)
	c.LastUsedAt = stale
	if err := st.UpsertCredential(ctx, c); err != nil {
		t.Fatalf("backdating the credential: %v", err)
	}
	return stale
}

func getCredential(t *testing.T, st Store, userID string) Credential {
	t.Helper()
	c, err := st.GetCredential(context.Background(), userID, githubProvider)
	if err != nil {
		t.Fatalf("GetCredential(%s): %v", userID, err)
	}
	return c
}

// wantCredentialStatus polls until userID's GitHub credential reaches want —
// the store-side assertion for the events controld applies asynchronously.
func wantCredentialStatus(t *testing.T, st Store, userID, want string) Credential {
	t.Helper()
	var got Credential
	eventually(t, 3*time.Second, func() error {
		got = getCredential(t, st, userID)
		if got.Status != want {
			return fmt.Errorf("credential status = %q, want %q", got.Status, want)
		}
		return nil
	})
	return got
}

// ---------------------------------------------------------------------------
// storeGitHubCredential
// ---------------------------------------------------------------------------

// storeGitHubCredential seals the token before it reaches the store: what is
// at rest must not be the token's bytes, and must come back out through Open
// with the real key. Scopes ride along informationally and the row lands
// valid.
func TestStoreGitHubCredentialSeals(t *testing.T) {
	s, st := newVaultServer(t)
	u := seedVaultUser(t, st, 42, "alice")
	ctx := context.Background()

	if err := s.storeGitHubCredential(ctx, u.ID, vaultToken, "repo, read:user"); err != nil {
		t.Fatalf("storeGitHubCredential: %v", err)
	}

	c, err := st.GetCredential(ctx, u.ID, "github")
	if err != nil {
		t.Fatalf("GetCredential: %v", err)
	}
	if c.Status != CredentialValid {
		t.Errorf("status = %q, want %q", c.Status, CredentialValid)
	}
	if c.Scopes != "repo, read:user" {
		t.Errorf("scopes = %q, want %q", c.Scopes, "repo, read:user")
	}
	if string(c.Ciphertext) == vaultToken || strings.Contains(string(c.Ciphertext), vaultToken) {
		t.Fatal("the stored ciphertext is (or contains) the token in the clear")
	}
	if len(c.Nonce) == 0 {
		t.Fatal("the stored credential carries no nonce")
	}
	plain, err := Open(s.cfg.SecretsKey, c.Ciphertext, c.Nonce)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if string(plain) != vaultToken {
		t.Errorf("Open round-trip = %q, want the token back", plain)
	}
	if c.ObtainedAt.IsZero() || c.LastVerifiedAt.IsZero() {
		t.Errorf("timestamps = obtained %v verified %v, want both stamped", c.ObtainedAt, c.LastVerifiedAt)
	}
}

// A second login replaces the row wholly and re-verifies it: the status a
// rejection left behind must not survive a refresh, which is the entire
// point of `rainier login --refresh github`.
func TestStoreGitHubCredentialRefreshClearsNeedsRefresh(t *testing.T) {
	s, st := newVaultServer(t)
	u := seedVaultUser(t, st, 42, "alice")
	ctx := context.Background()

	if err := s.storeGitHubCredential(ctx, u.ID, vaultToken, "read:user"); err != nil {
		t.Fatalf("storeGitHubCredential: %v", err)
	}
	s.rejectCredential(ctx, u.ID, "github")

	if err := s.storeGitHubCredential(ctx, u.ID, vaultToken+"_new", "repo, read:user"); err != nil {
		t.Fatalf("storeGitHubCredential (refresh): %v", err)
	}
	c, err := st.GetCredential(ctx, u.ID, "github")
	if err != nil {
		t.Fatalf("GetCredential: %v", err)
	}
	if c.Status != CredentialValid {
		t.Errorf("status after refresh = %q, want %q", c.Status, CredentialValid)
	}
	if c.Scopes != "repo, read:user" {
		t.Errorf("scopes after refresh = %q, want the new scopes", c.Scopes)
	}
	plain, err := Open(s.cfg.SecretsKey, c.Ciphertext, c.Nonce)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if string(plain) != vaultToken+"_new" {
		t.Errorf("Open round-trip = %q, want the refreshed token", plain)
	}
}

// ---------------------------------------------------------------------------
// mintGitCredential
// ---------------------------------------------------------------------------

// The happy path: a valid credential mints its token back and stamps
// last_used_at. updated_at must NOT move — a mint is a read, and the edit
// clock is how `rainier creds` distinguishes "used at" from "changed at".
func TestMintGitCredentialValid(t *testing.T) {
	s, st := newVaultServer(t)
	u := seedVaultUser(t, st, 42, "alice")
	ctx := context.Background()

	if err := s.storeGitHubCredential(ctx, u.ID, vaultToken, "repo, read:user"); err != nil {
		t.Fatalf("storeGitHubCredential: %v", err)
	}
	// Rewind the clocks so "was it bumped?" is decidable without sleeping.
	before, err := st.GetCredential(ctx, u.ID, "github")
	if err != nil {
		t.Fatalf("GetCredential: %v", err)
	}
	past := time.Now().Add(-time.Hour)
	before.LastUsedAt = past
	before.UpdatedAt = past
	if err := st.UpsertCredential(ctx, before); err != nil {
		t.Fatalf("UpsertCredential (rewind): %v", err)
	}

	tok, err := s.mintGitCredential(ctx, u.ID)
	if err != nil {
		t.Fatalf("mintGitCredential: %v", err)
	}
	if tok != vaultToken {
		t.Errorf("minted token = %q, want the sealed token back", tok)
	}

	after, err := st.GetCredential(ctx, u.ID, "github")
	if err != nil {
		t.Fatalf("GetCredential after mint: %v", err)
	}
	if !after.LastUsedAt.After(past) {
		t.Errorf("last_used_at = %v, want it bumped past %v", after.LastUsedAt, past)
	}
	if !after.UpdatedAt.Equal(past) {
		t.Errorf("updated_at = %v, want it left at %v — a mint is a read, not an edit", after.UpdatedAt, past)
	}
	if after.Status != CredentialValid {
		t.Errorf("status after mint = %q, want it unchanged at %q", after.Status, CredentialValid)
	}
}

// A rejected credential refuses with the named-action error, and refuses it
// as a sentinel callers can branch on with errors.Is.
func TestMintGitCredentialNeedsRefresh(t *testing.T) {
	s, st := newVaultServer(t)
	u := seedVaultUser(t, st, 42, "alice")
	ctx := context.Background()

	if err := s.storeGitHubCredential(ctx, u.ID, vaultToken, "repo"); err != nil {
		t.Fatalf("storeGitHubCredential: %v", err)
	}
	if err := st.SetCredentialStatus(ctx, u.ID, "github", CredentialNeedsRefresh); err != nil {
		t.Fatalf("SetCredentialStatus: %v", err)
	}

	tok, err := s.mintGitCredential(ctx, u.ID)
	if !errors.Is(err, ErrCredentialNeedsRefresh) {
		t.Fatalf("mintGitCredential err = %v, want ErrCredentialNeedsRefresh", err)
	}
	if tok != "" {
		t.Errorf("minted token = %q on the refusal path, want empty", tok)
	}
	if !strings.Contains(err.Error(), "rainier login --refresh github") {
		t.Errorf("error = %q, want it to name the action verbatim", err)
	}
}

// No row at all is a different refusal from a rejected one: the action that
// fixes it is a first login, not a refresh.
func TestMintGitCredentialMissing(t *testing.T) {
	s, st := newVaultServer(t)
	u := seedVaultUser(t, st, 42, "alice")

	tok, err := s.mintGitCredential(context.Background(), u.ID)
	if !errors.Is(err, ErrCredentialMissing) {
		t.Fatalf("mintGitCredential err = %v, want ErrCredentialMissing", err)
	}
	if errors.Is(err, ErrCredentialNeedsRefresh) {
		t.Error("a missing credential must not read as one needing a refresh")
	}
	if tok != "" {
		t.Errorf("minted token = %q with no credential stored, want empty", tok)
	}
	if !strings.Contains(err.Error(), "rainier login") {
		t.Errorf("error = %q, want it to name the action", err)
	}
}

// Every vault refusal is an error a caller may log or hand to a user, so no
// path may put token material in one — including the unseal failure, which
// is the one place plaintext is closest at hand.
func TestMintGitCredentialErrorsCarryNoTokenMaterial(t *testing.T) {
	s, st := newVaultServer(t)
	u := seedVaultUser(t, st, 42, "alice")
	ctx := context.Background()

	if err := s.storeGitHubCredential(ctx, u.ID, vaultToken, "repo"); err != nil {
		t.Fatalf("storeGitHubCredential: %v", err)
	}
	// Corrupt the sealed bytes so Open fails inside the mint.
	c, err := st.GetCredential(ctx, u.ID, "github")
	if err != nil {
		t.Fatalf("GetCredential: %v", err)
	}
	c.Ciphertext[0] ^= 0xff
	if err := st.UpsertCredential(ctx, c); err != nil {
		t.Fatalf("UpsertCredential (corrupt): %v", err)
	}

	tok, err := s.mintGitCredential(ctx, u.ID)
	if err == nil {
		t.Fatal("mintGitCredential over a corrupted credential returned no error")
	}
	if tok != "" {
		t.Errorf("minted token = %q on the failure path, want empty", tok)
	}
	if strings.Contains(err.Error(), vaultToken) {
		t.Errorf("error carries token material: %q", err)
	}
}

// ---------------------------------------------------------------------------
// rejectCredential
// ---------------------------------------------------------------------------

// An observed auth failure flips the row to needs_refresh so the NEXT
// operation refuses clearly and `rainier creds` shows it immediately.
func TestRejectCredentialFlipsStatus(t *testing.T) {
	s, st := newVaultServer(t)
	u := seedVaultUser(t, st, 42, "alice")
	ctx := context.Background()

	if err := s.storeGitHubCredential(ctx, u.ID, vaultToken, "repo"); err != nil {
		t.Fatalf("storeGitHubCredential: %v", err)
	}

	s.rejectCredential(ctx, u.ID, "github")

	c, err := st.GetCredential(ctx, u.ID, "github")
	if err != nil {
		t.Fatalf("GetCredential: %v", err)
	}
	if c.Status != CredentialNeedsRefresh {
		t.Fatalf("status = %q, want %q", c.Status, CredentialNeedsRefresh)
	}
	// The sealed value survives the flip: a rejected token is still the
	// token a refresh replaces, and dropping it here would turn a recoverable
	// state into a missing credential.
	if len(c.Ciphertext) == 0 {
		t.Error("rejecting a credential dropped its sealed value")
	}

	if _, err := s.mintGitCredential(ctx, u.ID); !errors.Is(err, ErrCredentialNeedsRefresh) {
		t.Fatalf("mint after reject = %v, want ErrCredentialNeedsRefresh", err)
	}
}

// rejectCredential is best-effort by contract: it is called from a failure
// path that has nothing useful to do with an error of its own, so an unknown
// (user, provider) must be a quiet no-op rather than a panic.
func TestRejectCredentialUnknownRowIsQuiet(t *testing.T) {
	s, st := newVaultServer(t)
	u := seedVaultUser(t, st, 42, "alice")
	ctx := context.Background()

	s.rejectCredential(ctx, u.ID, "github")
	s.rejectCredential(ctx, "usr_nobody", "github")

	if _, err := st.GetCredential(ctx, u.ID, "github"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetCredential = %v, want ErrNotFound — rejecting must not create a row", err)
	}
}

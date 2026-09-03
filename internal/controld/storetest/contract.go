package storetest

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/internal/controld"
)

// The synthetic scope the host-lookup cases run in. They are the same two
// workspaces and two pools controlapp/repotest uses, so a store's host
// lookups and its repositories are proved over the same shapes.
const (
	hostWorkspace control.WorkspaceID = "ws_alpha"
	hostOtherWS   control.WorkspaceID = "ws_beta"
	hostPoolA     control.PoolID      = "pool_a"
	hostPoolB     control.PoolID      = "pool_b"
)

// RunHost is the contract of controld.HostStore: the persistence the
// self-hosted host owns beside the control repositories — identity (users and
// bearer tokens), the vault (secrets and credentials), and the four lookups
// the control ports deliberately have no method for.
//
// Every case answers with the control sentinel set: a lookup that finds
// nothing is control.ErrNotFound, and a name already held is
// control.ErrConflict. There is one sentinel set in the codebase, and this is
// it.
//
// A lookup case needs rows HostStore itself cannot create, so it asks the
// store for its repository accessors. Every store that has host lookups has
// them; one that does not is failed here rather than silently skipped.
func RunHost(t *testing.T, open func(t *testing.T) controld.HostStore) {
	ctx := context.Background()
	mkUser := func(t *testing.T, st controld.HostStore) controld.User {
		u, err := st.UpsertUser(ctx, 42, "alice", "admin")
		if err != nil {
			t.Fatal(err)
		}
		return u
	}
	ports := func(t *testing.T, st controld.HostStore) controld.Repositories {
		t.Helper()
		r, ok := st.(controld.Repositories)
		if !ok {
			t.Fatalf("a host store must also carry the repository accessors; %T does not", st)
		}
		return r
	}
	provision := func(t *testing.T, st controld.HostStore, wss ...control.WorkspaceID) {
		t.Helper()
		for _, ws := range wss {
			if err := st.EnsureWorkspace(ctx, ws); err != nil {
				t.Fatalf("EnsureWorkspace(%s): %v", ws, err)
			}
		}
	}
	// A store records events as well as it keeps rows: the two are one
	// write, so the recorder is the same object the lookups are on. A store
	// that does not carry it is failed here rather than silently skipped.
	recorder := func(t *testing.T, st controld.HostStore) control.EventRecorder {
		t.Helper()
		r, ok := st.(control.EventRecorder)
		if !ok {
			t.Fatalf("a host store must also record events; %T does not", st)
		}
		return r
	}

	t.Run("user upsert is stable by github id", func(t *testing.T) {
		st := open(t)
		u1 := mkUser(t, st)
		u2, err := st.UpsertUser(ctx, 42, "alice-renamed", "admin")
		if err != nil {
			t.Fatal(err)
		}
		if u1.ID != u2.ID {
			t.Fatalf("same github id must keep user id: %s vs %s", u1.ID, u2.ID)
		}
		if u2.Login != "alice-renamed" {
			t.Fatalf("login should update")
		}
	})

	t.Run("token round trip and unknown token", func(t *testing.T) {
		st := open(t)
		u := mkUser(t, st)
		tok, hash := controld.NewToken()
		if err := st.InsertToken(ctx, u.ID, hash); err != nil {
			t.Fatal(err)
		}
		got, err := st.UserByToken(ctx, controld.HashToken(tok))
		if err != nil || got.ID != u.ID {
			t.Fatalf("lookup: %v %+v", err, got)
		}
		if _, err := st.UserByToken(ctx, controld.HashToken("rnr_bogus")); !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
	})

	t.Run("user lookup by id", func(t *testing.T) {
		st := open(t)
		u := mkUser(t, st)

		got, err := st.GetUser(ctx, u.ID)
		if err != nil {
			t.Fatalf("GetUser: %v", err)
		}
		if got.ID != u.ID || got.GitHubID != u.GitHubID || got.Login != u.Login || got.Role != u.Role {
			t.Fatalf("GetUser = %+v, want %+v", got, u)
		}
		if _, err := st.GetUser(ctx, "usr_nosuch"); !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("GetUser of an unknown id = %v, want ErrNotFound", err)
		}
	})

	t.Run("secrets round trip", func(t *testing.T) {
		st := open(t)
		ct1, nonce1 := []byte{0x00, 0x01, 0xfe, 0xff}, []byte("nonce-aaaaaa")
		if err := st.PutSecret(ctx, "GITHUB_TOKEN", ct1, nonce1); err != nil {
			t.Fatal(err)
		}
		gotCT, gotNonce, err := st.GetSecret(ctx, "GITHUB_TOKEN")
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(gotCT, ct1) || !bytes.Equal(gotNonce, nonce1) {
			t.Fatalf("ciphertext/nonce round trip: %x %x", gotCT, gotNonce)
		}
		if _, _, err := st.GetSecret(ctx, "NOSUCH"); !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("unknown secret: want ErrNotFound, got %v", err)
		}

		if err := st.PutSecret(ctx, "ANTHROPIC_KEY", []byte{0x42}, []byte("nonce-bbbbbb")); err != nil {
			t.Fatal(err)
		}
		// SecretMeta carries name and timestamps only — no listing path ever
		// hands back ciphertext.
		metas, err := st.ListSecrets(ctx)
		if err != nil || len(metas) != 2 {
			t.Fatalf("list: %v %+v", err, metas)
		}
		if metas[0].Name != "ANTHROPIC_KEY" || metas[1].Name != "GITHUB_TOKEN" {
			t.Fatalf("list must be name asc: %+v", metas)
		}
		before := metas[1]
		if before.CreatedAt.IsZero() || before.UpdatedAt.IsZero() {
			t.Fatalf("put must stamp timestamps: %+v", before)
		}

		// Put again upserts: same row, new bytes, new updated_at.
		time.Sleep(2 * time.Millisecond)
		ct2, nonce2 := []byte{0x09}, []byte("nonce-cccccc")
		if err := st.PutSecret(ctx, "GITHUB_TOKEN", ct2, nonce2); err != nil {
			t.Fatal(err)
		}
		gotCT, gotNonce, err = st.GetSecret(ctx, "GITHUB_TOKEN")
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(gotCT, ct2) || !bytes.Equal(gotNonce, nonce2) {
			t.Fatalf("upsert must replace bytes: %x %x", gotCT, gotNonce)
		}
		metas, err = st.ListSecrets(ctx)
		if err != nil || len(metas) != 2 {
			t.Fatalf("list after upsert: %v %+v", err, metas)
		}
		after := metas[1]
		if !after.CreatedAt.Equal(before.CreatedAt) {
			t.Fatalf("upsert must keep created_at: %v vs %v", after.CreatedAt, before.CreatedAt)
		}
		if !after.UpdatedAt.After(before.UpdatedAt) {
			t.Fatalf("upsert must bump updated_at: %v vs %v", after.UpdatedAt, before.UpdatedAt)
		}

		if err := st.DeleteSecret(ctx, "GITHUB_TOKEN"); err != nil {
			t.Fatal(err)
		}
		if _, _, err := st.GetSecret(ctx, "GITHUB_TOKEN"); !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("deleted secret: want ErrNotFound, got %v", err)
		}
		if err := st.DeleteSecret(ctx, "GITHUB_TOKEN"); !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("delete twice: want ErrNotFound, got %v", err)
		}
		if metas, err := st.ListSecrets(ctx); err != nil || len(metas) != 1 || metas[0].Name != "ANTHROPIC_KEY" {
			t.Fatalf("after delete: %v %+v", err, metas)
		}
	})

	t.Run("credential upsert, get, status, touch, and list", func(t *testing.T) {
		st := open(t)
		alice := mkUser(t, st)
		bob, err := st.UpsertUser(ctx, 43, "bob", "member")
		if err != nil {
			t.Fatal(err)
		}

		ct1, nonce1 := []byte{0x00, 0x01, 0xfe, 0xff}, []byte("nonce-aaaaaa")
		if err := st.UpsertCredential(ctx, controld.Credential{
			UserID: alice.ID, Provider: "github",
			Ciphertext: ct1, Nonce: nonce1,
			Scopes: "repo read:user",
		}); err != nil {
			t.Fatal(err)
		}

		first, err := st.GetCredential(ctx, alice.ID, "github")
		if err != nil {
			t.Fatal(err)
		}
		if first.UserID != alice.ID || first.Provider != "github" {
			t.Fatalf("identity round trip: %+v", first)
		}
		if !bytes.Equal(first.Ciphertext, ct1) || !bytes.Equal(first.Nonce, nonce1) {
			t.Fatalf("sealed bytes round trip: %x %x", first.Ciphertext, first.Nonce)
		}
		if first.Scopes != "repo read:user" {
			t.Fatalf("scopes = %q", first.Scopes)
		}
		// An upsert that names no status stores the healthy one, matching the
		// column's own default.
		if first.Status != controld.CredentialValid {
			t.Fatalf("status = %q, want %q", first.Status, controld.CredentialValid)
		}
		if first.ObtainedAt.IsZero() || first.LastVerifiedAt.IsZero() ||
			first.LastUsedAt.IsZero() || first.UpdatedAt.IsZero() {
			t.Fatalf("upsert must stamp timestamps: %+v", first)
		}
		// v0 stores no refresh token and no expiry: both stay absent, not zero
		// values dressed up as real ones.
		if first.RefreshCiphertext != nil || first.RefreshNonce != nil {
			t.Fatalf("refresh columns must stay unset: %x %x", first.RefreshCiphertext, first.RefreshNonce)
		}
		if first.ExpiresAt != nil {
			t.Fatalf("expires_at must stay NULL, got %v", *first.ExpiresAt)
		}

		// The returned bytes are the caller's own copy: writing through them
		// must not reach back into the store.
		first.Ciphertext[0] ^= 0xff
		if again, err := st.GetCredential(ctx, alice.ID, "github"); err != nil || !bytes.Equal(again.Ciphertext, ct1) {
			t.Fatalf("stored ciphertext aliased the caller's slice: %v %x", err, again.Ciphertext)
		}

		// Upserting again replaces the whole credential: a fresh login is a
		// new token, so its bytes, scopes, and clocks all move.
		time.Sleep(2 * time.Millisecond)
		ct2, nonce2 := []byte{0x09}, []byte("nonce-cccccc")
		rct, rnonce := []byte{0x11, 0x22}, []byte("nonce-dddddd")
		exp := time.Now().Add(time.Hour).UTC().Truncate(time.Millisecond)
		if err := st.UpsertCredential(ctx, controld.Credential{
			UserID: alice.ID, Provider: "github",
			Ciphertext: ct2, Nonce: nonce2,
			RefreshCiphertext: rct, RefreshNonce: rnonce,
			Status: controld.CredentialValid, Scopes: "repo",
			ExpiresAt: &exp,
		}); err != nil {
			t.Fatal(err)
		}
		second, err := st.GetCredential(ctx, alice.ID, "github")
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(second.Ciphertext, ct2) || !bytes.Equal(second.Nonce, nonce2) {
			t.Fatalf("upsert must replace the sealed bytes: %x %x", second.Ciphertext, second.Nonce)
		}
		if !bytes.Equal(second.RefreshCiphertext, rct) || !bytes.Equal(second.RefreshNonce, rnonce) {
			t.Fatalf("upsert must store the refresh bytes: %x %x", second.RefreshCiphertext, second.RefreshNonce)
		}
		if second.Scopes != "repo" {
			t.Fatalf("scopes = %q, want repo", second.Scopes)
		}
		if second.ExpiresAt == nil || !second.ExpiresAt.Equal(exp) {
			t.Fatalf("expires_at = %v, want %v", second.ExpiresAt, exp)
		}
		if !second.ObtainedAt.After(first.ObtainedAt) {
			t.Fatalf("re-upsert must restamp obtained_at: %v vs %v", second.ObtainedAt, first.ObtainedAt)
		}

		// A status flip is a small write: it moves status and updated_at, and
		// nothing else.
		time.Sleep(2 * time.Millisecond)
		if err := st.SetCredentialStatus(ctx, alice.ID, "github", controld.CredentialNeedsRefresh); err != nil {
			t.Fatal(err)
		}
		flipped, err := st.GetCredential(ctx, alice.ID, "github")
		if err != nil {
			t.Fatal(err)
		}
		if flipped.Status != controld.CredentialNeedsRefresh {
			t.Fatalf("status = %q, want %q", flipped.Status, controld.CredentialNeedsRefresh)
		}
		if !flipped.UpdatedAt.After(second.UpdatedAt) {
			t.Fatalf("status change must bump updated_at: %v vs %v", flipped.UpdatedAt, second.UpdatedAt)
		}
		if !bytes.Equal(flipped.Ciphertext, ct2) || !flipped.ObtainedAt.Equal(second.ObtainedAt) {
			t.Fatalf("status change must touch nothing else: %+v", flipped)
		}

		// A use touches last_used_at only. It is a read-path stamp, so it must
		// not move updated_at — that clock belongs to edits.
		time.Sleep(2 * time.Millisecond)
		if err := st.TouchCredentialUsed(ctx, alice.ID, "github"); err != nil {
			t.Fatal(err)
		}
		used, err := st.GetCredential(ctx, alice.ID, "github")
		if err != nil {
			t.Fatal(err)
		}
		if !used.LastUsedAt.After(flipped.LastUsedAt) {
			t.Fatalf("touch must bump last_used_at: %v vs %v", used.LastUsedAt, flipped.LastUsedAt)
		}
		if !used.UpdatedAt.Equal(flipped.UpdatedAt) {
			t.Fatalf("touch must not move updated_at: %v vs %v", used.UpdatedAt, flipped.UpdatedAt)
		}
		if used.Status != controld.CredentialNeedsRefresh || !used.LastVerifiedAt.Equal(flipped.LastVerifiedAt) {
			t.Fatalf("touch must not change status or last_verified_at: %+v", used)
		}

		// A listing is per-user and provider-ordered; another user's rows are
		// invisible, in both directions.
		if err := st.UpsertCredential(ctx, controld.Credential{
			UserID: alice.ID, Provider: "aprovider", Ciphertext: []byte{0x1}, Nonce: []byte("n1")}); err != nil {
			t.Fatal(err)
		}
		if err := st.UpsertCredential(ctx, controld.Credential{
			UserID: bob.ID, Provider: "github", Ciphertext: []byte{0x2}, Nonce: []byte("n2")}); err != nil {
			t.Fatal(err)
		}

		mine, err := st.ListCredentials(ctx, alice.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(mine) != 2 {
			t.Fatalf("list: want alice's 2 rows, got %+v", mine)
		}
		if mine[0].Provider != "aprovider" || mine[1].Provider != "github" {
			t.Fatalf("list must be provider asc: %q %q", mine[0].Provider, mine[1].Provider)
		}
		for _, c := range mine {
			if c.UserID != alice.ID {
				t.Fatalf("another user's credential leaked into the listing: %+v", c)
			}
		}
		his, err := st.ListCredentials(ctx, bob.ID)
		if err != nil || len(his) != 1 || his[0].UserID != bob.ID || !bytes.Equal(his[0].Ciphertext, []byte{0x2}) {
			t.Fatalf("bob's listing: %v %+v", err, his)
		}
		if none, err := st.ListCredentials(ctx, "usr_nosuch"); err != nil || len(none) != 0 {
			t.Fatalf("unknown user: want an empty listing, got %v %+v", err, none)
		}

		// Every lookup by an identity nothing answers to is ErrNotFound —
		// including a provider this user has but another user's row holds.
		if _, err := st.GetCredential(ctx, alice.ID, "nosuch"); !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("unknown provider: want ErrNotFound, got %v", err)
		}
		if _, err := st.GetCredential(ctx, "usr_nosuch", "github"); !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("unknown user: want ErrNotFound, got %v", err)
		}
		if _, err := st.GetCredential(ctx, bob.ID, "aprovider"); !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("another user's provider: want ErrNotFound, got %v", err)
		}
		if err := st.SetCredentialStatus(ctx, alice.ID, "nosuch", controld.CredentialNeedsRefresh); !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("SetCredentialStatus on an unknown provider: want ErrNotFound, got %v", err)
		}
		if err := st.SetCredentialStatus(ctx, "usr_nosuch", "github", controld.CredentialNeedsRefresh); !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("SetCredentialStatus on an unknown user: want ErrNotFound, got %v", err)
		}
		if err := st.TouchCredentialUsed(ctx, alice.ID, "nosuch"); !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("TouchCredentialUsed on an unknown provider: want ErrNotFound, got %v", err)
		}
		if err := st.TouchCredentialUsed(ctx, "usr_nosuch", "github"); !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("TouchCredentialUsed on an unknown user: want ErrNotFound, got %v", err)
		}
	})

	// EnsureWorkspace is what makes a store from any source usable: New calls
	// it for the installation workspace on every start, so it has to be a
	// statement of fact rather than a create that can fail the second time.
	t.Run("workspace provisioning is idempotent", func(t *testing.T) {
		st := open(t)
		for i := 1; i <= 2; i++ {
			if err := st.EnsureWorkspace(ctx, hostWorkspace); err != nil {
				t.Fatalf("EnsureWorkspace call %d: %v", i, err)
			}
		}
	})

	// The name index is a locator, never authority: it resolves a name INSIDE
	// a workspace, and a name another workspace holds is simply not there.
	t.Run("environment lookup by name is workspace-scoped", func(t *testing.T) {
		st := open(t)
		envs := ports(t, st).Environments()
		provision(t, st, hostWorkspace, hostOtherWS)

		row, err := envs.CreateEnvironment(ctx, hostWorkspace, control.Environment{
			ID: "env_a", Name: "dev", Image: "img:1", SetupHash: "h1"})
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		got, err := st.EnvironmentByName(ctx, hostWorkspace, "dev")
		if err != nil || got != row.ID {
			t.Fatalf("EnvironmentByName = %q, %v; want %q", got, err, row.ID)
		}
		if _, err := st.EnvironmentByName(ctx, hostOtherWS, "dev"); !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("another workspace's name: err = %v, want control.ErrNotFound", err)
		}
		if _, err := st.EnvironmentByName(ctx, hostWorkspace, "nosuch"); !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("unknown name: err = %v, want control.ErrNotFound", err)
		}
	})

	// SnapshotRunner decides nothing — it reports which runner built the
	// cached snapshot, and it keeps reporting it after the setup hash moves
	// on, because the wire has always shown that column stale or not.
	t.Run("snapshot runner names the holder, stale or not", func(t *testing.T) {
		st := open(t)
		envs := ports(t, st).Environments()
		provision(t, st, hostWorkspace)

		env, err := envs.CreateEnvironment(ctx, hostWorkspace, control.Environment{
			ID: "env_a", Name: "dev", Image: "img:1", Setup: "make deps", SetupHash: "h1"})
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		if holder, err := st.SnapshotRunner(ctx, hostWorkspace, env.ID); err != nil || holder != "" {
			t.Fatalf("a fresh environment's holder = %q, %v; want no holder", holder, err)
		}
		if err := envs.SetEnvironmentSnapshot(ctx, hostWorkspace, env.ID, "h1", "snap:1", "runner_a"); err != nil {
			t.Fatalf("snapshot: %v", err)
		}
		if holder, err := st.SnapshotRunner(ctx, hostWorkspace, env.ID); err != nil || holder != "runner_a" {
			t.Fatalf("holder = %q, %v; want runner_a", holder, err)
		}

		moved := env
		moved.SetupHash = "h2"
		moved.Setup = "make deps && make build"
		if _, err := envs.UpdateEnvironment(ctx, hostWorkspace, moved); err != nil {
			t.Fatalf("update: %v", err)
		}
		if holder, err := st.SnapshotRunner(ctx, hostWorkspace, env.ID); err != nil || holder != "runner_a" {
			t.Fatalf("holder after the hash moved = %q, %v; want runner_a still", holder, err)
		}
	})

	// SnapshotHolder is the direct answer to "where can this ref boot": the
	// runner holding it while the cache is still current, and nobody at all
	// once the environment's setup hash moves on. A stale cache pins a
	// session to no runner — that is the difference between this lookup and
	// SnapshotRunner, which reports the column stale or not.
	t.Run("snapshot holder answers for a current cache only", func(t *testing.T) {
		st := open(t)
		envs := ports(t, st).Environments()
		provision(t, st, hostWorkspace, hostOtherWS)

		env, err := envs.CreateEnvironment(ctx, hostWorkspace, control.Environment{
			ID: "env_a", Name: "dev", Image: "img:1", Setup: "make deps", SetupHash: "h1"})
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		if holder, err := st.SnapshotHolder(ctx, hostWorkspace, "snap:1"); err != nil || holder != "" {
			t.Fatalf("holder before any snapshot = %q, %v; want no holder", holder, err)
		}

		if err := envs.SetEnvironmentSnapshot(ctx, hostWorkspace, env.ID, "h1", "snap:1", "runner_a"); err != nil {
			t.Fatalf("snapshot: %v", err)
		}
		if holder, err := st.SnapshotHolder(ctx, hostWorkspace, "snap:1"); err != nil || holder != "runner_a" {
			t.Fatalf("holder = %q, %v; want runner_a", holder, err)
		}
		// A ref nobody built, and another workspace's view of one that was
		// built, are both "no holder" rather than an error.
		if holder, err := st.SnapshotHolder(ctx, hostWorkspace, "snap:other"); err != nil || holder != "" {
			t.Fatalf("holder of an unknown ref = %q, %v; want no holder", holder, err)
		}
		if holder, err := st.SnapshotHolder(ctx, hostOtherWS, "snap:1"); err != nil || holder != "" {
			t.Fatalf("another workspace's holder = %q, %v; want no holder", holder, err)
		}

		// The setup hash moves on: the cache is stale, and a stale cache
		// holds nothing.
		moved := env
		moved.SetupHash = "h2"
		moved.Setup = "make deps && make build"
		if _, err := envs.UpdateEnvironment(ctx, hostWorkspace, moved); err != nil {
			t.Fatalf("update: %v", err)
		}
		if holder, err := st.SnapshotHolder(ctx, hostWorkspace, "snap:1"); err != nil || holder != "" {
			t.Fatalf("holder after the hash moved = %q, %v; want no holder", holder, err)
		}

		// Neither an unscoped question nor an empty ref is a question this
		// lookup can answer.
		if _, err := st.SnapshotHolder(ctx, "", "snap:1"); !errors.Is(err, control.ErrInvalid) {
			t.Fatalf("empty workspace: err = %v, want control.ErrInvalid", err)
		}
		if _, err := st.SnapshotHolder(ctx, hostWorkspace, ""); !errors.Is(err, control.ErrInvalid) {
			t.Fatalf("empty ref: err = %v, want control.ErrInvalid", err)
		}
	})

	// Recording an event is a write like any other: its id is an identity
	// somebody either holds or does not, and it lands in a workspace that
	// has to exist. What a store then does with the row is its own business
	// — this suite pins the answers, not the storage.
	t.Run("recording an event", func(t *testing.T) {
		st := open(t)
		rec := recorder(t, st)
		provision(t, st, hostWorkspace, hostOtherWS)

		at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
		ev := func(id string, ws control.WorkspaceID, resource string) control.Event {
			return control.Event{
				ID: control.EventID(id), WorkspaceID: ws, ActorID: "act_a", Action: control.ActionCreate,
				Resource: control.Resource{Kind: control.ResourceSession, WorkspaceID: ws, ID: resource, CreatorID: "act_a"},
				At:       at, PlacementGeneration: 2,
				Usage: control.Usage{CPUTimeSeconds: 1.5, MemoryByteSeconds: 2, StorageBytes: 3, NetworkBytes: 4, AgentTokenCount: 5},
			}
		}
		for _, e := range []control.Event{
			ev("evt_example", hostWorkspace, "sess_example"),
			ev("evt_second", hostWorkspace, "sess_second"),
			ev("evt_beta", hostOtherWS, "sess_beta"),
		} {
			if err := rec.Record(ctx, e); err != nil {
				t.Fatalf("Record(%s): %v", e.ID, err)
			}
		}

		// An id is an identity: the same one twice is somebody else already
		// holding it, not a second fact.
		if err := rec.Record(ctx, ev("evt_example", hostWorkspace, "sess_example")); !errors.Is(err, control.ErrConflict) {
			t.Fatalf("duplicate event id: err = %v, want control.ErrConflict", err)
		}
		// A workspace that does not exist is somewhere the event cannot land.
		if err := rec.Record(ctx, ev("evt_nowhere", "ws_nosuch", "sess_example")); !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("unknown workspace: err = %v, want control.ErrNotFound", err)
		}
	})

	// The generation the fleet repository fences on has exactly one writer.
	// It is keyed by pool, so the same runner name in two pools counts
	// separately, and it only ever goes up.
	t.Run("runner generations are per pool and monotonic", func(t *testing.T) {
		st := open(t)
		fleet := ports(t, st).Fleet()

		for want := uint64(1); want <= 3; want++ {
			got, err := st.NextRunnerGeneration(ctx, hostPoolA, "runner_a")
			if err != nil || got != want {
				t.Fatalf("NextRunnerGeneration(pool_a) = %d, %v; want %d", got, err, want)
			}
		}
		if got, err := st.NextRunnerGeneration(ctx, hostPoolB, "runner_a"); err != nil || got != 1 {
			t.Fatalf("NextRunnerGeneration(pool_b) = %d, %v; want 1", got, err)
		}

		for _, tc := range []struct {
			pool control.PoolID
			want uint64
		}{{hostPoolA, 3}, {hostPoolB, 1}} {
			rows, err := fleet.ListRunners(ctx, tc.pool)
			if err != nil || len(rows) != 1 {
				t.Fatalf("ListRunners(%s) = %+v, %v; want one row", tc.pool, rows, err)
			}
			if rows[0].ID != "runner_a" || rows[0].Generation != tc.want {
				t.Fatalf("%s's runner_a generation = %d, want %d", tc.pool, rows[0].Generation, tc.want)
			}
		}
	})
}

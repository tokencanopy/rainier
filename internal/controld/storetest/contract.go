package storetest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"rainier/internal/controld"
)

// sameJSON fails the test unless got carries the same JSON value as want.
// Connector bytes are stored verbatim as a value, not as a byte string:
// Postgres's jsonb re-renders whitespace and member order on the way back
// out, so the contract pins the value every store must preserve — no member
// added, dropped, or rewritten.
func sameJSON(t *testing.T, want string, got []byte) {
	t.Helper()
	var w, g any
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatalf("want is not valid JSON (%s): %v", want, err)
	}
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatalf("stored JSON is invalid (%s): %v", got, err)
	}
	if !reflect.DeepEqual(w, g) {
		t.Fatalf("json value: want %s, got %s", want, got)
	}
}

func RunContract(t *testing.T, open func(t *testing.T) controld.Store) {
	ctx := context.Background()
	mkUser := func(t *testing.T, st controld.Store) controld.User {
		u, err := st.UpsertUser(ctx, 42, "alice", "admin")
		if err != nil {
			t.Fatal(err)
		}
		return u
	}
	mkSess := func(t *testing.T, st controld.Store, owner, name string) controld.Session {
		s, err := st.CreateSession(ctx, controld.Session{
			ID: controld.NewSessionID(), OwnerID: owner, Name: name,
			Image: "img", State: controld.StateQueued})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	const connectorJSON = `{"type":"github","repo":"acme/app","base_branch":"main"}`
	mkEnv := func(t *testing.T, st controld.Store, name string) controld.Environment {
		e, err := st.CreateEnvironment(ctx, controld.Environment{
			ID: controld.NewEnvironmentID(), Name: name,
			Image: "img:1", Setup: "make deps",
			Init: "make dev-server &", InitTimeoutSec: 120,
			EgressAllow: []string{"github.com"},
			SecretRefs:  []string{"GITHUB_TOKEN"},
			Connectors:  []controld.Connector{{Type: "github", Raw: json.RawMessage(connectorJSON)}},
			Placement:   "vm1", SetupTimeoutSec: 600,
		})
		if err != nil {
			t.Fatal(err)
		}
		return e
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
		if _, err := st.UserByToken(ctx, controld.HashToken("rnr_bogus")); !errors.Is(err, controld.ErrNotFound) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
	})

	t.Run("guarded transition: wrong from-state loses with ErrConflict", func(t *testing.T) {
		st := open(t)
		u := mkUser(t, st)
		s := mkSess(t, st, u.ID, "")
		r := "vm1"
		if err := st.Transition(ctx, s.ID, []controld.SessionState{controld.StateQueued}, controld.StateCreating, controld.TransitionOpts{Runner: &r}); err != nil {
			t.Fatal(err)
		}
		err := st.Transition(ctx, s.ID, []controld.SessionState{controld.StateQueued}, controld.StateCanceled, controld.TransitionOpts{})
		if !errors.Is(err, controld.ErrConflict) {
			t.Fatalf("want ErrConflict, got %v", err)
		}
		got, _ := st.GetSession(ctx, s.ID)
		if got.State != controld.StateCreating || got.Runner != "vm1" {
			t.Fatalf("state clobbered: %+v", got)
		}
	})

	t.Run("active name unique per owner; freed by terminal state", func(t *testing.T) {
		st := open(t)
		u := mkUser(t, st)
		mkSess(t, st, u.ID, "dev")
		_, err := st.CreateSession(ctx, controld.Session{ID: controld.NewSessionID(), OwnerID: u.ID, Name: "dev", State: controld.StateQueued})
		if !errors.Is(err, controld.ErrConflict) {
			t.Fatalf("want ErrConflict, got %v", err)
		}
		// terminal frees the name
		first, _ := st.SessionByName(ctx, u.ID, "dev")
		if err := st.Transition(ctx, first.ID, controld.NonTerminal, controld.StateCanceled, controld.TransitionOpts{}); err != nil {
			t.Fatal(err)
		}
		if _, err := st.CreateSession(ctx, controld.Session{ID: controld.NewSessionID(), OwnerID: u.ID, Name: "dev", State: controld.StateQueued}); err != nil {
			t.Fatalf("terminal session must free the name: %v", err)
		}
	})

	t.Run("idempotency key replays", func(t *testing.T) {
		st := open(t)
		u := mkUser(t, st)
		s1, err := st.CreateSession(ctx, controld.Session{ID: controld.NewSessionID(), OwnerID: u.ID, IdempotencyKey: "k1", State: controld.StateQueued})
		if err != nil {
			t.Fatal(err)
		}
		_, err = st.CreateSession(ctx, controld.Session{ID: controld.NewSessionID(), OwnerID: u.ID, IdempotencyKey: "k1", State: controld.StateQueued})
		if !errors.Is(err, controld.ErrIdemReplay) {
			t.Fatalf("want ErrIdemReplay, got %v", err)
		}
		got, err := st.SessionByIdem(ctx, u.ID, "k1")
		if err != nil || got.ID != s1.ID {
			t.Fatalf("replay lookup: %v %+v", err, got)
		}
	})

	t.Run("list pagination is stable and cursor resumes", func(t *testing.T) {
		st := open(t)
		u := mkUser(t, st)
		var ids []string
		for i := 0; i < 5; i++ {
			s := mkSess(t, st, u.ID, "")
			ids = append(ids, s.ID)
			time.Sleep(2 * time.Millisecond) // distinct created_at
		}
		page1, next, err := st.ListSessions(ctx, controld.SessionQuery{Limit: 3})
		if err != nil || len(page1) != 3 || next == "" {
			t.Fatalf("page1: %v n=%d next=%q", err, len(page1), next)
		}
		page2, next2, err := st.ListSessions(ctx, controld.SessionQuery{Limit: 3, Cursor: next})
		if err != nil || len(page2) != 2 || next2 != "" {
			t.Fatalf("page2: %v n=%d", err, len(page2))
		}
		if page1[0].ID != ids[4] {
			t.Fatalf("newest first: got %s want %s", page1[0].ID, ids[4])
		}
		seen := map[string]bool{}
		for _, s := range append(page1, page2...) {
			seen[s.ID] = true
		}
		if len(seen) != 5 {
			t.Fatalf("pages overlap or drop: %v", seen)
		}
	})

	t.Run("terminal sessions hidden unless IncludeTerminal", func(t *testing.T) {
		st := open(t)
		u := mkUser(t, st)
		s := mkSess(t, st, u.ID, "")
		st.Transition(ctx, s.ID, controld.NonTerminal, controld.StateDead, controld.TransitionOpts{})
		rows, _, _ := st.ListSessions(ctx, controld.SessionQuery{Limit: 10})
		if len(rows) != 0 {
			t.Fatalf("terminal leaked into default list")
		}
		rows, _, _ = st.ListSessions(ctx, controld.SessionQuery{Limit: 10, IncludeTerminal: true})
		if len(rows) != 1 {
			t.Fatalf("IncludeTerminal missing row")
		}
	})

	t.Run("runners upsert and sessions-on-runner filter", func(t *testing.T) {
		st := open(t)
		u := mkUser(t, st)
		if err := st.UpsertRunner(ctx, controld.Runner{Name: "vm1", CapacityUsed: 1, CapacityTotal: 4, Connected: true, LastSeenAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
		s := mkSess(t, st, u.ID, "")
		r := "vm1"
		st.Transition(ctx, s.ID, controld.NonTerminal, controld.StateCreating, controld.TransitionOpts{Runner: &r})
		on, err := st.SessionsOnRunner(ctx, "vm1", []controld.SessionState{controld.StateCreating})
		if err != nil || len(on) != 1 || on[0].ID != s.ID {
			t.Fatalf("on-runner: %v %+v", err, on)
		}
		runners, _ := st.ListRunners(ctx)
		if len(runners) != 1 || runners[0].CapacityTotal != 4 {
			t.Fatalf("runners: %+v", runners)
		}
	})

	t.Run("oldest queued ordering", func(t *testing.T) {
		st := open(t)
		u := mkUser(t, st)
		a := mkSess(t, st, u.ID, "")
		time.Sleep(2 * time.Millisecond)
		mkSess(t, st, u.ID, "")
		q, err := st.OldestQueued(ctx)
		if err != nil || len(q) != 2 || q[0].ID != a.ID {
			t.Fatalf("fifo order: %v %+v", err, q)
		}
	})

	t.Run("environment CRUD and name uniqueness", func(t *testing.T) {
		st := open(t)
		e := mkEnv(t, st, "dev")
		if e.SetupHash != controld.SetupHash("img:1", "make deps") {
			t.Fatalf("create must compute setup_hash, got %q", e.SetupHash)
		}
		if e.CreatedAt.IsZero() || e.UpdatedAt.IsZero() {
			t.Fatalf("create must stamp timestamps: %+v", e)
		}
		if e.SnapshotRef != "" || e.SnapshotRunner != "" || e.SnapshotHash != "" {
			t.Fatalf("a fresh environment has no snapshot: %+v", e)
		}

		byID, err := st.GetEnvironment(ctx, e.ID)
		if err != nil {
			t.Fatal(err)
		}
		if byID.Name != "dev" || byID.Image != "img:1" || byID.Setup != "make deps" ||
			byID.Placement != "vm1" || byID.SetupTimeoutSec != 600 || byID.SetupHash != e.SetupHash {
			t.Fatalf("get by id: %+v", byID)
		}
		if !slices.Equal(byID.EgressAllow, []string{"github.com"}) {
			t.Fatalf("egress_allow round trip: %+v", byID.EgressAllow)
		}
		if !slices.Equal(byID.SecretRefs, []string{"GITHUB_TOKEN"}) {
			t.Fatalf("secret_refs round trip: %+v", byID.SecretRefs)
		}
		if len(byID.Connectors) != 1 || byID.Connectors[0].Type != "github" {
			t.Fatalf("connectors round trip: %+v", byID.Connectors)
		}
		sameJSON(t, connectorJSON, byID.Connectors[0].Raw)

		byName, err := st.GetEnvironmentByName(ctx, "dev")
		if err != nil || byName.ID != e.ID {
			t.Fatalf("get by name: %v %+v", err, byName)
		}
		if _, err := st.GetEnvironment(ctx, "env_nosuch"); !errors.Is(err, controld.ErrNotFound) {
			t.Fatalf("unknown id: want ErrNotFound, got %v", err)
		}
		if _, err := st.GetEnvironmentByName(ctx, "nosuch"); !errors.Is(err, controld.ErrNotFound) {
			t.Fatalf("unknown name: want ErrNotFound, got %v", err)
		}

		_, err = st.CreateEnvironment(ctx, controld.Environment{
			ID: controld.NewEnvironmentID(), Name: "dev", Image: "img:2"})
		if !errors.Is(err, controld.ErrConflict) {
			t.Fatalf("duplicate name: want ErrConflict, got %v", err)
		}

		// A create ignores whatever snapshot the caller made up — only
		// SetEnvironmentSnapshot writes those columns.
		alpha, err := st.CreateEnvironment(ctx, controld.Environment{
			ID: controld.NewEnvironmentID(), Name: "alpha", Image: "img:1",
			SnapshotRef: "made-up", SnapshotRunner: "vm9", SnapshotHash: "deadbeef"})
		if err != nil {
			t.Fatal(err)
		}
		if alpha.SnapshotRef != "" || alpha.SnapshotRunner != "" || alpha.SnapshotHash != "" {
			t.Fatalf("create must ignore caller-supplied snapshot columns: %+v", alpha)
		}

		envs, err := st.ListEnvironments(ctx)
		if err != nil || len(envs) != 2 {
			t.Fatalf("list: %v %+v", err, envs)
		}
		if envs[0].Name != "alpha" || envs[1].Name != "dev" {
			t.Fatalf("list must be name asc: %q %q", envs[0].Name, envs[1].Name)
		}

		// A setup change moves setup_hash; created_at stays put.
		upd := byID
		upd.Setup = "make deps && make build"
		moved, err := st.UpdateEnvironment(ctx, upd)
		if err != nil {
			t.Fatal(err)
		}
		if moved.SetupHash == e.SetupHash {
			t.Fatalf("setup change must move setup_hash, still %q", moved.SetupHash)
		}
		if moved.SetupHash != controld.SetupHash(moved.Image, moved.Setup) {
			t.Fatalf("update must recompute setup_hash from image+setup: %+v", moved)
		}
		if !moved.CreatedAt.Equal(e.CreatedAt) {
			t.Fatalf("update must not move created_at: %v vs %v", moved.CreatedAt, e.CreatedAt)
		}

		// An egress-only change leaves setup_hash alone: the build inputs
		// didn't change, so a cached snapshot stays valid.
		upd2 := moved
		upd2.EgressAllow = []string{"github.com", "proxy.golang.org"}
		got2, err := st.UpdateEnvironment(ctx, upd2)
		if err != nil {
			t.Fatal(err)
		}
		if got2.SetupHash != moved.SetupHash {
			t.Fatalf("egress-only change must not move setup_hash: %q vs %q", got2.SetupHash, moved.SetupHash)
		}
		if !slices.Equal(got2.EgressAllow, []string{"github.com", "proxy.golang.org"}) {
			t.Fatalf("update must persist egress_allow: %+v", got2.EgressAllow)
		}
		if reread, err := st.GetEnvironment(ctx, e.ID); err != nil || reread.Setup != upd.Setup ||
			!slices.Equal(reread.EgressAllow, got2.EgressAllow) || reread.SetupHash != got2.SetupHash {
			t.Fatalf("update must persist: %v %+v", err, reread)
		}

		// Renaming onto a name another environment holds conflicts.
		alpha.Name = "dev"
		if _, err := st.UpdateEnvironment(ctx, alpha); !errors.Is(err, controld.ErrConflict) {
			t.Fatalf("rename onto a taken name: want ErrConflict, got %v", err)
		}
		if _, err := st.UpdateEnvironment(ctx, controld.Environment{ID: "env_nosuch", Name: "ghost"}); !errors.Is(err, controld.ErrNotFound) {
			t.Fatalf("update unknown id: want ErrNotFound, got %v", err)
		}

		if err := st.DeleteEnvironment(ctx, alpha.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := st.GetEnvironment(ctx, alpha.ID); !errors.Is(err, controld.ErrNotFound) {
			t.Fatalf("deleted environment: want ErrNotFound, got %v", err)
		}
		if err := st.DeleteEnvironment(ctx, alpha.ID); !errors.Is(err, controld.ErrNotFound) {
			t.Fatalf("delete twice: want ErrNotFound, got %v", err)
		}
		if envs, err := st.ListEnvironments(ctx); err != nil || len(envs) != 1 || envs[0].ID != e.ID {
			t.Fatalf("after delete: %v %+v", err, envs)
		}
	})

	t.Run("guarded snapshot update", func(t *testing.T) {
		st := open(t)
		e := mkEnv(t, st, "dev")

		if err := st.SetEnvironmentSnapshot(ctx, e.ID, e.SetupHash, "rainier-env:dev-aaaa", "vm1"); err != nil {
			t.Fatal(err)
		}
		cached, err := st.GetEnvironment(ctx, e.ID)
		if err != nil {
			t.Fatal(err)
		}
		if cached.SnapshotRef != "rainier-env:dev-aaaa" || cached.SnapshotRunner != "vm1" || cached.SnapshotHash != e.SetupHash {
			t.Fatalf("snapshot not recorded: %+v", cached)
		}

		// Editing setup moves setup_hash and leaves the (now stale) snapshot
		// columns exactly as they were — only SetEnvironmentSnapshot writes
		// them, so the snapshot fields carried in here are ignored too.
		upd := cached
		upd.Setup = "make deps && make build"
		upd.SnapshotRef, upd.SnapshotRunner, upd.SnapshotHash = "hijacked", "vm9", "deadbeef"
		moved, err := st.UpdateEnvironment(ctx, upd)
		if err != nil {
			t.Fatal(err)
		}
		if moved.SetupHash == e.SetupHash {
			t.Fatalf("setup change must move setup_hash, still %q", moved.SetupHash)
		}
		if moved.SnapshotRef != "rainier-env:dev-aaaa" || moved.SnapshotRunner != "vm1" || moved.SnapshotHash != e.SetupHash {
			t.Fatalf("update must not touch snapshot columns: %+v", moved)
		}

		// A snapshot built from the OLD hash must not land.
		err = st.SetEnvironmentSnapshot(ctx, e.ID, e.SetupHash, "rainier-env:dev-stale", "vm2")
		if !errors.Is(err, controld.ErrConflict) {
			t.Fatalf("stale snapshot: want ErrConflict, got %v", err)
		}
		after, err := st.GetEnvironment(ctx, e.ID)
		if err != nil {
			t.Fatal(err)
		}
		if after.SnapshotRef != "rainier-env:dev-aaaa" || after.SnapshotRunner != "vm1" ||
			after.SnapshotHash != e.SetupHash || after.SetupHash != moved.SetupHash {
			t.Fatalf("losing set must change nothing: %+v", after)
		}

		// The matching hash still lands.
		if err := st.SetEnvironmentSnapshot(ctx, e.ID, moved.SetupHash, "rainier-env:dev-bbbb", "vm2"); err != nil {
			t.Fatal(err)
		}
		fresh, err := st.GetEnvironment(ctx, e.ID)
		if err != nil {
			t.Fatal(err)
		}
		if fresh.SnapshotRef != "rainier-env:dev-bbbb" || fresh.SnapshotRunner != "vm2" || fresh.SnapshotHash != moved.SetupHash {
			t.Fatalf("matching hash must land: %+v", fresh)
		}

		// An environment that no longer exists has nothing to guard, and the
		// snapshot must not land there either.
		if err := st.SetEnvironmentSnapshot(ctx, "env_nosuch", moved.SetupHash, "rainier-env:ghost", "vm1"); !errors.Is(err, controld.ErrConflict) {
			t.Fatalf("unknown environment: want ErrConflict, got %v", err)
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
		if _, _, err := st.GetSecret(ctx, "NOSUCH"); !errors.Is(err, controld.ErrNotFound) {
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
		if _, _, err := st.GetSecret(ctx, "GITHUB_TOKEN"); !errors.Is(err, controld.ErrNotFound) {
			t.Fatalf("deleted secret: want ErrNotFound, got %v", err)
		}
		if err := st.DeleteSecret(ctx, "GITHUB_TOKEN"); !errors.Is(err, controld.ErrNotFound) {
			t.Fatalf("delete twice: want ErrNotFound, got %v", err)
		}
		if metas, err := st.ListSecrets(ctx); err != nil || len(metas) != 1 || metas[0].Name != "ANTHROPIC_KEY" {
			t.Fatalf("after delete: %v %+v", err, metas)
		}
	})

	t.Run("count sessions by environment", func(t *testing.T) {
		st := open(t)
		u := mkUser(t, st)
		e := mkEnv(t, st, "dev")
		other := mkEnv(t, st, "other")

		mkOn := func(env string) controld.Session {
			s, err := st.CreateSession(ctx, controld.Session{
				ID: controld.NewSessionID(), OwnerID: u.ID, EnvironmentID: env,
				Image: "img", State: controld.StateQueued})
			if err != nil {
				t.Fatal(err)
			}
			return s
		}
		mkOn(e.ID)
		gone := mkOn(e.ID)
		mkOn(other.ID)
		mkSess(t, st, u.ID, "") // scratch: no environment at all
		if err := st.Transition(ctx, gone.ID, controld.NonTerminal, controld.StateDead, controld.TransitionOpts{}); err != nil {
			t.Fatal(err)
		}

		n, err := st.CountSessionsByEnvironment(ctx, e.ID, controld.NonTerminal)
		if err != nil || n != 1 {
			t.Fatalf("live count: %v n=%d", err, n)
		}
		all, err := st.CountSessionsByEnvironment(ctx, e.ID, nil)
		if err != nil || all != 2 {
			t.Fatalf("no state filter counts every session on the env: %v n=%d", err, all)
		}
		if n, err := st.CountSessionsByEnvironment(ctx, "env_nosuch", controld.NonTerminal); err != nil || n != 0 {
			t.Fatalf("unknown env: %v n=%d", err, n)
		}
	})

	t.Run("session env columns persist", func(t *testing.T) {
		st := open(t)
		u := mkUser(t, st)
		e := mkEnv(t, st, "dev")

		s, err := st.CreateSession(ctx, controld.Session{
			ID: controld.NewSessionID(), OwnerID: u.ID, Name: "work",
			EnvironmentID: e.ID, ResolvedImage: "rainier-env:dev-aaaa",
			Image: "img", State: controld.StateQueued})
		if err != nil {
			t.Fatal(err)
		}
		if s.EnvironmentID != e.ID || s.ResolvedImage != "rainier-env:dev-aaaa" {
			t.Fatalf("create must return the env columns: %+v", s)
		}
		got, err := st.GetSession(ctx, s.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.EnvironmentID != e.ID || got.ResolvedImage != "rainier-env:dev-aaaa" {
			t.Fatalf("get must return the env columns: %+v", got)
		}
		rows, _, err := st.ListSessions(ctx, controld.SessionQuery{Limit: 10})
		if err != nil || len(rows) != 1 || rows[0].ID != s.ID {
			t.Fatalf("list: %v %+v", err, rows)
		}
		if rows[0].EnvironmentID != e.ID || rows[0].ResolvedImage != "rainier-env:dev-aaaa" {
			t.Fatalf("list must return the env columns: %+v", rows[0])
		}

		// A scratch session carries neither.
		scratch := mkSess(t, st, u.ID, "scratch")
		if scratch.EnvironmentID != "" || scratch.ResolvedImage != "" {
			t.Fatalf("scratch session: %+v", scratch)
		}
		gotScratch, err := st.GetSession(ctx, scratch.ID)
		if err != nil {
			t.Fatal(err)
		}
		if gotScratch.EnvironmentID != "" || gotScratch.ResolvedImage != "" {
			t.Fatalf("scratch session after get: %+v", gotScratch)
		}
	})

	// A session's setup_hash is the provenance of the script it was dispatched
	// with: written once by the create dispatch, read when that setup finishes
	// to decide whether the container may become the environment's cache.
	t.Run("session setup hash persists and is settable", func(t *testing.T) {
		st := open(t)
		u := mkUser(t, st)
		e := mkEnv(t, st, "dev")

		// It round-trips through create like any other column...
		const atCreate = "aaaa1111"
		s, err := st.CreateSession(ctx, controld.Session{
			ID: controld.NewSessionID(), OwnerID: u.ID, Name: "work",
			EnvironmentID: e.ID, ResolvedImage: e.Image, SetupHash: atCreate,
			Image: "img", State: controld.StateQueued})
		if err != nil {
			t.Fatal(err)
		}
		if s.SetupHash != atCreate {
			t.Fatalf("create must return setup_hash: %+v", s)
		}

		// ...and the dispatch-time write replaces it, visible through every
		// read path.
		const atDispatch = "bbbb2222"
		if err := st.SetSessionSetupHash(ctx, s.ID, atDispatch); err != nil {
			t.Fatalf("SetSessionSetupHash: %v", err)
		}
		got, err := st.GetSession(ctx, s.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.SetupHash != atDispatch {
			t.Fatalf("get setup_hash = %q, want %q", got.SetupHash, atDispatch)
		}
		rows, _, err := st.ListSessions(ctx, controld.SessionQuery{Limit: 10})
		if err != nil || len(rows) != 1 {
			t.Fatalf("list: %v %+v", err, rows)
		}
		if rows[0].SetupHash != atDispatch {
			t.Fatalf("list setup_hash = %q, want %q", rows[0].SetupHash, atDispatch)
		}
		onRunner, err := st.SessionsOnRunner(ctx, "", []controld.SessionState{controld.StateQueued})
		if err != nil || len(onRunner) != 1 || onRunner[0].SetupHash != atDispatch {
			t.Fatalf("sessions-on-runner setup_hash: %v %+v", err, onRunner)
		}

		// A session dispatched without a script keeps the column empty.
		scratch := mkSess(t, st, u.ID, "scratch")
		if scratch.SetupHash != "" {
			t.Fatalf("scratch session setup_hash = %q, want empty", scratch.SetupHash)
		}

		if err := st.SetSessionSetupHash(ctx, "sess_nosuch", atDispatch); !errors.Is(err, controld.ErrNotFound) {
			t.Fatalf("SetSessionSetupHash on an unknown id = %v, want ErrNotFound", err)
		}
	})

	// An environment's init hook runs on every session boot, not once at build
	// time, so it is deliberately NOT part of setup_hash: editing init must
	// leave a cached snapshot usable. That is the whole point of the column,
	// and the assertion below is what stops a future refactor from folding it
	// into the hash and silently invalidating every team's cache.
	t.Run("environment init round-trips and stays out of setup_hash", func(t *testing.T) {
		st := open(t)
		e := mkEnv(t, st, "dev")

		if e.Init != "make dev-server &" || e.InitTimeoutSec != 120 {
			t.Fatalf("create must return init columns: %+v", e)
		}
		if e.SetupHash != controld.SetupHash(e.Image, e.Setup) {
			t.Fatalf("setup_hash must come from image+setup alone, got %q", e.SetupHash)
		}

		byID, err := st.GetEnvironment(ctx, e.ID)
		if err != nil {
			t.Fatal(err)
		}
		if byID.Init != "make dev-server &" || byID.InitTimeoutSec != 120 {
			t.Fatalf("get must return init columns: %+v", byID)
		}
		byName, err := st.GetEnvironmentByName(ctx, "dev")
		if err != nil {
			t.Fatal(err)
		}
		if byName.Init != e.Init || byName.InitTimeoutSec != e.InitTimeoutSec {
			t.Fatalf("get by name must return init columns: %+v", byName)
		}
		rows, err := st.ListEnvironments(ctx)
		if err != nil || len(rows) != 1 {
			t.Fatalf("list: %v %+v", err, rows)
		}
		if rows[0].Init != e.Init || rows[0].InitTimeoutSec != e.InitTimeoutSec {
			t.Fatalf("list must return init columns: %+v", rows[0])
		}

		// UpdateEnvironment carries init exactly like setup...
		upd := byID
		upd.Init = "make dev-server --port 8080 &"
		upd.InitTimeoutSec = 300
		moved, err := st.UpdateEnvironment(ctx, upd)
		if err != nil {
			t.Fatal(err)
		}
		if moved.Init != upd.Init || moved.InitTimeoutSec != 300 {
			t.Fatalf("update must persist init columns: %+v", moved)
		}
		// ...but an init-only change must NOT move setup_hash: the build
		// inputs are unchanged, so a cached snapshot stays valid.
		if moved.SetupHash != e.SetupHash {
			t.Fatalf("init-only change must not move setup_hash: %q vs %q", moved.SetupHash, e.SetupHash)
		}
		if reread, err := st.GetEnvironment(ctx, e.ID); err != nil ||
			reread.Init != upd.Init || reread.InitTimeoutSec != 300 || reread.SetupHash != e.SetupHash {
			t.Fatalf("update must persist: %v %+v", err, reread)
		}

		// An environment with no init at all keeps the columns empty.
		bare, err := st.CreateEnvironment(ctx, controld.Environment{
			ID: controld.NewEnvironmentID(), Name: "bare", Image: "img:1"})
		if err != nil {
			t.Fatal(err)
		}
		if bare.Init != "" || bare.InitTimeoutSec != 0 {
			t.Fatalf("an environment with no init: %+v", bare)
		}
	})

	// child_exit_code is the agent process's own verdict, recorded when the
	// child exits while the session itself stays up (the operator still has a
	// shell). It is nullable because "no exit yet" and "exited 0" are
	// different facts — a plain int column could not tell them apart.
	t.Run("session child exit code is nullable and settable", func(t *testing.T) {
		st := open(t)
		u := mkUser(t, st)

		// A caller-supplied value at create is ignored: nothing has exited yet.
		code := 3
		s, err := st.CreateSession(ctx, controld.Session{
			ID: controld.NewSessionID(), OwnerID: u.ID, Name: "work",
			Image: "img", State: controld.StateQueued, ChildExitCode: &code})
		if err != nil {
			t.Fatal(err)
		}
		if s.ChildExitCode != nil {
			t.Fatalf("create must ignore a caller-supplied child_exit_code, got %d", *s.ChildExitCode)
		}
		got, err := st.GetSession(ctx, s.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.ChildExitCode != nil {
			t.Fatalf("a fresh session has no child exit code, got %d", *got.ChildExitCode)
		}

		// Zero is a real exit code, and must not read as "never exited".
		if err := st.SetChildExitCode(ctx, s.ID, 0); err != nil {
			t.Fatalf("SetChildExitCode: %v", err)
		}
		got, err = st.GetSession(ctx, s.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.ChildExitCode == nil || *got.ChildExitCode != 0 {
			t.Fatalf("exit 0 must persist as a set value, got %v", got.ChildExitCode)
		}

		if err := st.SetChildExitCode(ctx, s.ID, 137); err != nil {
			t.Fatal(err)
		}
		got, err = st.GetSession(ctx, s.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.ChildExitCode == nil || *got.ChildExitCode != 137 {
			t.Fatalf("get child_exit_code = %v, want 137", got.ChildExitCode)
		}
		rows, _, err := st.ListSessions(ctx, controld.SessionQuery{Limit: 10})
		if err != nil || len(rows) != 1 {
			t.Fatalf("list: %v %+v", err, rows)
		}
		if rows[0].ChildExitCode == nil || *rows[0].ChildExitCode != 137 {
			t.Fatalf("list child_exit_code = %v, want 137", rows[0].ChildExitCode)
		}
		onRunner, err := st.SessionsOnRunner(ctx, "", []controld.SessionState{controld.StateQueued})
		if err != nil || len(onRunner) != 1 {
			t.Fatalf("sessions on runner: %v %+v", err, onRunner)
		}
		if onRunner[0].ChildExitCode == nil || *onRunner[0].ChildExitCode != 137 {
			t.Fatalf("sessions-on-runner child_exit_code = %v, want 137", onRunner[0].ChildExitCode)
		}

		if err := st.SetChildExitCode(ctx, "sess_nosuch", 1); !errors.Is(err, controld.ErrNotFound) {
			t.Fatalf("SetChildExitCode on an unknown id = %v, want ErrNotFound", err)
		}
	})

	// The credential vault: one row per (user, provider), holding sealed bytes
	// the store never interprets. Every assertion here is about the row's
	// lifecycle — the seal itself lives above the store.
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
		if _, err := st.GetCredential(ctx, alice.ID, "nosuch"); !errors.Is(err, controld.ErrNotFound) {
			t.Fatalf("unknown provider: want ErrNotFound, got %v", err)
		}
		if _, err := st.GetCredential(ctx, "usr_nosuch", "github"); !errors.Is(err, controld.ErrNotFound) {
			t.Fatalf("unknown user: want ErrNotFound, got %v", err)
		}
		if _, err := st.GetCredential(ctx, bob.ID, "aprovider"); !errors.Is(err, controld.ErrNotFound) {
			t.Fatalf("another user's provider: want ErrNotFound, got %v", err)
		}
		if err := st.SetCredentialStatus(ctx, alice.ID, "nosuch", controld.CredentialNeedsRefresh); !errors.Is(err, controld.ErrNotFound) {
			t.Fatalf("SetCredentialStatus on an unknown provider: want ErrNotFound, got %v", err)
		}
		if err := st.SetCredentialStatus(ctx, "usr_nosuch", "github", controld.CredentialNeedsRefresh); !errors.Is(err, controld.ErrNotFound) {
			t.Fatalf("SetCredentialStatus on an unknown user: want ErrNotFound, got %v", err)
		}
		if err := st.TouchCredentialUsed(ctx, alice.ID, "nosuch"); !errors.Is(err, controld.ErrNotFound) {
			t.Fatalf("TouchCredentialUsed on an unknown provider: want ErrNotFound, got %v", err)
		}
		if err := st.TouchCredentialUsed(ctx, "usr_nosuch", "github"); !errors.Is(err, controld.ErrNotFound) {
			t.Fatalf("TouchCredentialUsed on an unknown user: want ErrNotFound, got %v", err)
		}
	})
}

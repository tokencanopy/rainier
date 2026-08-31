// internal/controld/pgstore/pgstore_test.go
package pgstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tokencanopy/rainier/internal/controld"
	"github.com/tokencanopy/rainier/internal/controld/storetest"
)

// startPostgres runs a disposable postgres:16-alpine container on an
// ephemeral host port and returns a DSN. Skips when docker is unavailable
// (mirrors the docker driver's mustDockerPath discipline).
//
// RAINIER_TEST_PG_DSN overrides all of that with a server the caller already
// has, so these tests can run on a machine with a Postgres but no container
// runtime. Point it at a THROWAWAY server: every test here creates databases
// of its own on it and never drops them.
func startPostgres(t *testing.T) string {
	t.Helper()
	if dsn := os.Getenv("RAINIER_TEST_PG_DSN"); dsn != "" {
		return dsn
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available and RAINIER_TEST_PG_DSN unset")
	}
	out, err := exec.Command("docker", "run", "-d", "--rm",
		"-e", "POSTGRES_PASSWORD=test", "-p", "127.0.0.1:0:5432", "postgres:16-alpine").Output()
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	id := strings.TrimSpace(string(out))
	t.Cleanup(func() { exec.Command("docker", "rm", "-f", id).Run() })
	port, err := exec.Command("docker", "port", id, "5432/tcp").Output()
	if err != nil {
		t.Fatal(err)
	}
	// "127.0.0.1:49213\n" → port
	hp := strings.TrimSpace(strings.Split(string(port), "\n")[0])
	dsn := fmt.Sprintf("postgres://postgres:test@%s/postgres?sslmode=disable", hp)
	// wait for readiness: retry Open until it succeeds or 30s passes
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, err := Open(context.Background(), dsn); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("postgres never became ready")
		}
		time.Sleep(300 * time.Millisecond)
	}
	return dsn
}

func TestPGStoreContract(t *testing.T) {
	dsn := startPostgres(t)
	n := 0
	storetest.RunContract(t, func(t *testing.T) controld.Store {
		// fresh schema per subtest: unique search_path-free approach — create
		// a numbered database so subtests don't see each other's rows.
		n++
		name := fmt.Sprintf("contract_%d", n)
		admin, err := Open(context.Background(), dsn)
		if err != nil {
			t.Fatal(err)
		}
		if err := admin.execAdmin(context.Background(), "CREATE DATABASE "+name); err != nil {
			t.Fatal(err)
		}
		st, err := Open(context.Background(), strings.Replace(dsn, "/postgres?", "/"+name+"?", 1))
		if err != nil {
			t.Fatal(err)
		}
		return st
	})
}

// freshStore opens a brand-new, empty database on dsn's server so a test can
// run in isolation from every other test on the same throwaway postgres
// container. Mirrors the per-subtest database logic inlined in
// TestPGStoreContract above.
func freshStore(t *testing.T, dsn, name string) *Store {
	t.Helper()
	ctx := context.Background()
	admin, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.execAdmin(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	st, err := Open(ctx, strings.Replace(dsn, "/postgres?", "/"+name+"?", 1))
	if err != nil {
		t.Fatal(err)
	}
	return st
}

// TestListSessionsExactMultiplePagination pins the behavior the shared
// contract suite doesn't itself exercise: when a page comes back exactly
// full, ListSessions must return a non-empty next cursor even if no further
// rows exist — no look-ahead query — and the follow-up call on that cursor
// must then come back with an empty page and an empty next.
func TestListSessionsExactMultiplePagination(t *testing.T) {
	dsn := startPostgres(t)
	st := freshStore(t, dsn, "exactpage")
	ctx := context.Background()

	u, err := st.UpsertUser(ctx, 1, "alice", "admin")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if _, err := st.CreateSession(ctx, controld.Session{
			ID: controld.NewSessionID(), OwnerID: u.ID, Image: "img", State: controld.StateQueued,
		}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond) // distinct created_at
	}

	page, next, err := st.ListSessions(ctx, controld.SessionQuery{Limit: 4})
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 4 {
		t.Fatalf("want 4 rows, got %d", len(page))
	}
	if next == "" {
		t.Fatal("want non-empty next cursor when the page comes back exactly full, even though no further rows exist")
	}

	page2, next2, err := st.ListSessions(ctx, controld.SessionQuery{Limit: 4, Cursor: next})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) != 0 {
		t.Fatalf("follow-up page: want 0 rows, got %d", len(page2))
	}
	if next2 != "" {
		t.Fatalf("follow-up page: want empty next, got %q", next2)
	}
}

// TestUserByTokenTouchesLastUsedAt pins that UserByToken updates
// api_tokens.last_used_at, the column that exists for exactly this.
func TestUserByTokenTouchesLastUsedAt(t *testing.T) {
	dsn := startPostgres(t)
	st := freshStore(t, dsn, "lastused")
	ctx := context.Background()

	u, err := st.UpsertUser(ctx, 1, "alice", "admin")
	if err != nil {
		t.Fatal(err)
	}
	tok, hash := controld.NewToken()
	if err := st.InsertToken(ctx, u.ID, hash); err != nil {
		t.Fatal(err)
	}

	var before *time.Time
	if err := st.pool.QueryRow(ctx, `SELECT last_used_at FROM api_tokens WHERE token_hash = $1`, hash).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if before != nil {
		t.Fatalf("want last_used_at NULL before first use, got %v", *before)
	}

	if _, err := st.UserByToken(ctx, controld.HashToken(tok)); err != nil {
		t.Fatal(err)
	}

	var after *time.Time
	if err := st.pool.QueryRow(ctx, `SELECT last_used_at FROM api_tokens WHERE token_hash = $1`, hash).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after == nil {
		t.Fatal("want last_used_at set after UserByToken, got NULL")
	}

	if _, err := st.UserByToken(ctx, controld.HashToken("rnr_bogus")); !errors.Is(err, controld.ErrNotFound) {
		t.Fatalf("want ErrNotFound for unknown token, got %v", err)
	}
}

// embeddedMigrationVersions returns the version of every embedded migration,
// ascending — exactly the set Open() brings a database up to, derived from the
// same migrations/ listing Migrate walks rather than spelled out here.
//
// Spelling it out is what made this file's upgrade assertion go stale the
// moment a migration was added (it read "want 1..4" and got 1..5). A count
// nobody has to remember to bump cannot go stale again.
func embeddedMigrationVersions(t *testing.T) []int {
	t.Helper()
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		t.Fatalf("read embedded migrations: %v", err)
	}
	var versions []int
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		v, err := migrationVersion(e.Name())
		if err != nil {
			t.Fatalf("embedded migration %s: %v", e.Name(), err)
		}
		versions = append(versions, v)
	}
	slices.Sort(versions)
	return versions
}

// TestMigrate0003To0004AddsColumnsToLegacyRows pins the upgrade path a
// deployed controld actually takes: a database that stopped at 0003 and
// already holds sessions and environments gets every later migration applied
// to live tables. Each is additive with a default (or nullable), so the rows
// that were there before must still read back — and must read back as "no
// init", "no child exit" and "no repo override", not as zeros dressed up as
// real values.
//
// It is named for 0004 because that is the migration that first bolted
// columns onto populated tables; it upgrades to HEAD, and every migration
// after it is covered by the same assertions.
func TestMigrate0003To0004AddsColumnsToLegacyRows(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()

	// Build a database that stops at 0003, the way a deployed controld from
	// the previous plan would have it.
	admin, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.execAdmin(ctx, "CREATE DATABASE upgrade_0004"); err != nil {
		t.Fatal(err)
	}
	legacyDSN := strings.Replace(dsn, "/postgres?", "/upgrade_0004?", 1)
	pool, err := pgxpool.New(ctx, legacyDSN)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version int PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"0001_init.sql", "0002_environments.sql", "0003_session_setup_hash.sql"} {
		sql, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		version, err := migrationVersion(name)
		if err != nil {
			t.Fatal(err)
		}
		if err := applyMigration(ctx, pool, version, string(sql)); err != nil {
			t.Fatalf("apply %s: %v", name, err)
		}
	}

	// Populate it the way a running deployment would have.
	if _, err := pool.Exec(ctx, `INSERT INTO users (id, github_id, login, role) VALUES ('usr_legacy', 7, 'alice', 'admin')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO sessions (id, owner_id, name, image, state, runner) VALUES ('sess_legacy', 'usr_legacy', 'old', 'img:0', 'running', 'vm1')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO environments (id, name, image, setup, setup_hash) VALUES ('env_legacy', 'legacy', 'img:0', 'make deps', $1)`,
		controld.SetupHash("img:0", "make deps")); err != nil {
		t.Fatal(err)
	}
	pool.Close()

	// Open() runs every pending migration — 0004 and everything after it.
	st, err := Open(ctx, legacyDSN)
	if err != nil {
		t.Fatalf("upgrading a 0003 database to head: %v", err)
	}
	var applied []int
	rows, err := st.pool.Query(ctx, `SELECT version FROM schema_migrations ORDER BY version`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		applied = append(applied, v)
	}
	rows.Close()
	if want := embeddedMigrationVersions(t); !slices.Equal(applied, want) {
		t.Fatalf("schema_migrations = %v, want every embedded migration in order %v", applied, want)
	}

	// The legacy session survived, and its new columns read as "never exited"
	// rather than "exited 0", and as "named no repo override" rather than
	// "asked for no repositories" — a pre-0005 row stores SQL NULL there, and
	// a nil Repos is what lets its environment's connectors still decide.
	sess, err := st.GetSession(ctx, "sess_legacy")
	if err != nil {
		t.Fatalf("legacy session after upgrade: %v", err)
	}
	if sess.ChildExitCode != nil {
		t.Fatalf("legacy session child_exit_code = %d, want NULL", *sess.ChildExitCode)
	}
	if sess.Repos != nil {
		t.Fatalf("legacy session repos = %#v, want nil (no override), not an empty list", sess.Repos)
	}
	if sess.Name != "old" || sess.Image != "img:0" || sess.State != controld.StateRunning {
		t.Fatalf("legacy session lost data across the migration: %+v", sess)
	}
	if err := st.SetChildExitCode(ctx, "sess_legacy", 0); err != nil {
		t.Fatal(err)
	}
	if sess, err = st.GetSession(ctx, "sess_legacy"); err != nil || sess.ChildExitCode == nil || *sess.ChildExitCode != 0 {
		t.Fatalf("after SetChildExitCode(0): %v %+v", err, sess.ChildExitCode)
	}

	// The legacy environment survived with an empty init hook, and its
	// setup_hash — the identity a cached snapshot is keyed by — did not move.
	env, err := st.GetEnvironment(ctx, "env_legacy")
	if err != nil {
		t.Fatalf("legacy environment after upgrade: %v", err)
	}
	if env.Init != "" || env.InitTimeoutSec != 0 {
		t.Fatalf("legacy environment init columns = %q/%d, want empty", env.Init, env.InitTimeoutSec)
	}
	if env.SetupHash != controld.SetupHash("img:0", "make deps") {
		t.Fatalf("the migration must not move setup_hash: %q", env.SetupHash)
	}

	// And the new table is there and usable for the user that already existed.
	if err := st.UpsertCredential(ctx, controld.Credential{
		UserID: "usr_legacy", Provider: "github", Ciphertext: []byte{0x1}, Nonce: []byte("n")}); err != nil {
		t.Fatalf("credentials table unusable after upgrade: %v", err)
	}
	if got, err := st.GetCredential(ctx, "usr_legacy", "github"); err != nil || got.Status != controld.CredentialValid {
		t.Fatalf("credential after upgrade: %v %+v", err, got)
	}

	// The foreign key is real: a departing user's credentials go with them,
	// rather than lingering as sealed tokens nobody owns. (A separate user,
	// because the legacy one still has a session referencing it.)
	gone, err := st.UpsertUser(ctx, 8, "bob", "member")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertCredential(ctx, controld.Credential{
		UserID: gone.ID, Provider: "github", Ciphertext: []byte{0x2}, Nonce: []byte("n")}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx, `DELETE FROM users WHERE id = $1`, gone.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.GetCredential(ctx, gone.ID, "github"); !errors.Is(err, controld.ErrNotFound) {
		t.Fatalf("ON DELETE CASCADE must take the credential with the user, got %v", err)
	}
}

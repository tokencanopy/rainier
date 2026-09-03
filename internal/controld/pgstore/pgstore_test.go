// internal/controld/pgstore/pgstore_test.go
package pgstore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/controlapp/repotest"
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

// freshStore opens a brand-new, empty database on dsn's server so a test can
// run in isolation from every other test on the same throwaway postgres
// container. name is a label, not the database name: freshDB derives a legal,
// unique one from it, so a caller may pass t.Name() straight through however
// the subtest is called and however often the test runs.
func freshStore(t *testing.T, dsn, name string) *Store {
	t.Helper()
	return reopen(t, freshDB(t, dsn, name))
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
		if _, err := st.Sessions().CreateSession(ctx, "ws_self_hosted", control.Session{
			ID:        control.SessionID(controld.NewSessionID()),
			CreatorID: control.ActorID(u.ID),
			Spec:      control.PortableSpec{Image: "img"},
			State:     control.StateQueued,
			PoolID:    "pool_self_hosted",
		}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond) // distinct created_at
	}

	page, next, err := st.Sessions().ListSessions(ctx, "ws_self_hosted", control.SessionQuery{Limit: 4})
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 4 {
		t.Fatalf("want 4 rows, got %d", len(page))
	}
	if next == "" {
		t.Fatal("want non-empty next cursor when the page comes back exactly full, even though no further rows exist")
	}

	page2, next2, err := st.Sessions().ListSessions(ctx, "ws_self_hosted", control.SessionQuery{Limit: 4, Cursor: next})
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

	if _, err := st.UserByToken(ctx, controld.HashToken("rnr_bogus")); !errors.Is(err, control.ErrNotFound) {
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
	legacyDSN := freshDB(t, dsn, t.Name())
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
	// This release's head is 9: a database that stopped at 0003 runs the
	// expand step (0007), the contract step (0008), and the events table
	// (0009) in the same start.
	if head := applied[len(applied)-1]; head != 9 {
		t.Fatalf("head migration = %d, want 9", head)
	}

	// The legacy session survived, and its new columns read as "never exited"
	// rather than "exited 0", and as "named no repo override" rather than
	// "asked for no repositories" — a pre-0005 row stores SQL NULL there, and
	// a nil Repos is what lets its environment's connectors still decide.
	sess, err := st.Sessions().GetSession(ctx, "ws_self_hosted", "sess_legacy")
	if err != nil {
		t.Fatalf("legacy session after upgrade: %v", err)
	}
	if sess.ChildExitCode != nil {
		t.Fatalf("legacy session child_exit_code = %d, want NULL", *sess.ChildExitCode)
	}
	if sess.Spec.Repos != nil {
		t.Fatalf("legacy session repos = %#v, want nil (no override), not an empty list", sess.Spec.Repos)
	}
	if sess.Name != "old" || sess.Spec.Image != "img:0" || sess.State != control.StateRunning {
		t.Fatalf("legacy session lost data across the migration: %+v", sess)
	}
	if err := st.Sessions().SetChildExitCode(ctx, "ws_self_hosted", "sess_legacy", 0); err != nil {
		t.Fatal(err)
	}
	if sess, err = st.Sessions().GetSession(ctx, "ws_self_hosted", "sess_legacy"); err != nil || sess.ChildExitCode == nil || *sess.ChildExitCode != 0 {
		t.Fatalf("after SetChildExitCode(0): %v %+v", err, sess.ChildExitCode)
	}

	// The legacy environment survived with an empty init hook, and its
	// setup_hash — the identity a cached snapshot is keyed by — did not move.
	env, err := st.Environments().GetEnvironment(ctx, "ws_self_hosted", "env_legacy")
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
	if _, err := st.GetCredential(ctx, gone.ID, "github"); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("ON DELETE CASCADE must take the credential with the user, got %v", err)
	}

	// Replay: a controld that restarts against an already-migrated database
	// runs Migrate again, and it must do nothing at all. Every statement in
	// the expand step (0007) and the contract step (0008) is a one-way ALTER
	// or DROP, so a second application would fail loudly rather than
	// silently — the assertion is that the version set is unchanged (through
	// 9) and the rows still read.
	if err := Migrate(ctx, st.pool); err != nil {
		t.Fatalf("Migrate twice: %v", err)
	}
	var replayed []int
	rows, err = st.pool.Query(ctx, `SELECT version FROM schema_migrations ORDER BY version`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		replayed = append(replayed, v)
	}
	rows.Close()
	if !slices.Equal(replayed, applied) {
		t.Fatalf("a second Migrate changed schema_migrations: %v, want %v", replayed, applied)
	}
	if _, err := st.Sessions().GetSession(ctx, "ws_self_hosted", "sess_legacy"); err != nil {
		t.Fatalf("the legacy session must still read after a replayed Migrate: %v", err)
	}
}

// ---------------------------------------------------------------------------
// the native repositories (O9)
// ---------------------------------------------------------------------------

// dbName turns a test name into a legal, unique Postgres database name:
// lowercased, every character an identifier cannot hold replaced, truncated
// to leave room for a random suffix. The suffix is what lets the same test
// run twice against the same throwaway server (`-count=2`, or a second `go
// test`) without colliding with the database its first run created.
func dbName(name string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(name) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	trimmed := strings.Trim(b.String(), "_")
	if len(trimmed) > 40 {
		trimmed = trimmed[:40]
	}
	suffix := make([]byte, 4)
	if _, err := rand.Read(suffix); err != nil {
		panic("pgstore test: crypto/rand: " + err.Error())
	}
	return "t_" + trimmed + "_" + hex.EncodeToString(suffix)
}

// freshDB creates a brand-new, empty database on dsn's server and returns the
// DSN that reaches it. The database is dropped when the test ends, so a
// throwaway server does not accumulate one per case forever.
func freshDB(t *testing.T, dsn, name string) string {
	t.Helper()
	ctx := context.Background()
	db := dbName(name)
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+db, pgx.QueryExecModeSimpleProtocol); err != nil {
		admin.Close()
		t.Fatalf("create database %s: %v", db, err)
	}
	admin.Close()
	t.Cleanup(func() {
		cleanup, err := pgxpool.New(context.Background(), dsn)
		if err != nil {
			return
		}
		defer cleanup.Close()
		cleanup.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db+" WITH (FORCE)", pgx.QueryExecModeSimpleProtocol)
	})
	return strings.Replace(dsn, "/postgres?", "/"+db+"?", 1)
}

// reopen opens a second Store over a database that already exists — the
// restart case, where the process is new and the rows are not.
func reopen(t *testing.T, dbDSN string) *Store {
	t.Helper()
	st, err := Open(context.Background(), dbDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	return st
}

// rawPoolAt creates a fresh database, migrates it to exactly version, and
// hands back the raw pool — the way a deployment that stopped at that
// version looks, before the migration under test runs.
func rawPoolAt(t *testing.T, dsn string, version int) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, freshDB(t, dsn, t.Name()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := migrateTo(ctx, pool, version); err != nil {
		t.Fatalf("migrate to %d: %v", version, err)
	}
	return pool
}

// mustExec runs one raw statement, the way the pre-O9 code wrote its rows.
func mustExec(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("exec: %v", err)
	}
}

// TestPGStoreRepositories runs the public repository contract every host's
// store must pass, over a fresh database per case.
func TestPGStoreRepositories(t *testing.T) {
	dsn := startPostgres(t)
	repotest.Run(t, func(t *testing.T) repotest.Stores {
		st := freshStore(t, dsn, t.Name())
		return repotest.Stores{
			Sessions:     st.Sessions(),
			Environments: st.Environments(),
			Fleet:        st.Fleet(),
			Provision:    st.EnsureWorkspace,
		}
	})
}

// TestPGStoreHost runs the host-side contract: identity, the vault, and the
// four lookups the control ports deliberately lack.
func TestPGStoreHost(t *testing.T) {
	dsn := startPostgres(t)
	storetest.RunHost(t, func(t *testing.T) controld.HostStore {
		return freshStore(t, dsn, t.Name())
	})
}

// TestMigration0007BackfillsExistingRows proves the expand step against rows
// the pre-O9 code wrote: they come out scoped, their resolved image is their
// image, and an operator's pin is a capability.
func TestMigration0007BackfillsExistingRows(t *testing.T) {
	ctx := context.Background()
	pool := rawPoolAt(t, startPostgres(t), 6)
	mustExec(t, pool, `INSERT INTO users (id, github_id, login, role) VALUES ('usr_example', 1, 'octocat-example', 'admin')`)
	mustExec(t, pool, `INSERT INTO environments (id, name, image, setup_hash, placement) VALUES ('env_example', 'py', 'img:1', 'h1', 'vm1')`)
	mustExec(t, pool, `INSERT INTO sessions (id, owner_id, name, image, resolved_image, state, runner, environment_id) VALUES ('sess_example', 'usr_example', 'dev', '', 'rainier-env:env_example-abc', 'running', 'vm1', 'env_example')`)
	mustExec(t, pool, `INSERT INTO runners (name, capacity_total, connected) VALUES ('vm1', 4, true)`)

	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	st := &Store{pool: pool}

	row, err := st.Sessions().GetSession(ctx, "ws_self_hosted", "sess_example")
	if err != nil || row.Spec.Image != "rainier-env:env_example-abc" || row.PoolID != "pool_self_hosted" ||
		row.PlacementGeneration != 1 || row.RunnerID != "vm1" {
		t.Fatalf("session after 0007: %+v, %v", row, err)
	}
	if row.WorkspaceID != "ws_self_hosted" || row.CreatorID != "usr_example" || row.ControllerGeneration != 0 {
		t.Fatalf("session scope after 0007: %+v", row)
	}
	env, err := st.Environments().GetEnvironment(ctx, "ws_self_hosted", "env_example")
	if err != nil || !slices.Equal(env.Requirements.Capabilities, []string{"placement:vm1"}) {
		t.Fatalf("environment after 0007: %+v, %v", env, err)
	}
	runners, err := st.Fleet().ListRunners(ctx, "pool_self_hosted")
	if err != nil || len(runners) != 1 || runners[0].ID != "vm1" || runners[0].Generation != 0 {
		t.Fatalf("runners after 0007: %+v, %v", runners, err)
	}
	if runners[0].PoolID != "pool_self_hosted" || runners[0].CapacityTotal != 4 || !runners[0].Connected {
		t.Fatalf("runner fields after 0007: %+v", runners[0])
	}

	// And the contract step (0008) took the two columns the expand step
	// replaced, and the scope defaults with them: after this release a row
	// written without a workspace is a database error, not a silent write
	// into the installation's own workspace.
	for _, gone := range []struct{ table, column string }{
		{"sessions", "resolved_image"},
		{"environments", "placement"},
	} {
		var n int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM information_schema.columns WHERE table_name = $1 AND column_name = $2`,
			gone.table, gone.column).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Fatalf("%s.%s survived 0008", gone.table, gone.column)
		}
	}
	for _, scoped := range []struct{ table, column string }{
		{"sessions", "workspace_id"},
		{"sessions", "pool_id"},
		{"environments", "workspace_id"},
		{"runners", "pool_id"},
	} {
		var def *string
		if err := pool.QueryRow(ctx,
			`SELECT column_default FROM information_schema.columns WHERE table_name = $1 AND column_name = $2`,
			scoped.table, scoped.column).Scan(&def); err != nil {
			t.Fatalf("%s.%s: %v", scoped.table, scoped.column, err)
		}
		if def != nil {
			t.Fatalf("%s.%s still defaults to %q after 0008", scoped.table, scoped.column, *def)
		}
	}
}

// TestNextRunnerGenerationSurvivesReopen is the restart case: a second Store
// over the same database continues the sequence, because the generation the
// fleet fences on is a row and not a process's memory.
func TestNextRunnerGenerationSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	dbDSN := freshDB(t, startPostgres(t), t.Name())
	a := reopen(t, dbDSN)
	for want := uint64(1); want <= 2; want++ {
		got, err := a.NextRunnerGeneration(ctx, "pool_a", "runner_a")
		if err != nil || got != want {
			t.Fatalf("gen = %d, %v; want %d", got, err, want)
		}
	}
	a.Close()

	b := reopen(t, dbDSN)
	if got, err := b.NextRunnerGeneration(ctx, "pool_a", "runner_a"); err != nil || got != 3 {
		t.Fatalf("after reopen gen = %d, %v; want 3", got, err)
	}
}

// ---------------------------------------------------------------------------
// the unit of work and the events table (O10)
// ---------------------------------------------------------------------------

// TestRunIsAtomic: a unit that creates a session, records its event, and
// then fails leaves neither the row nor the event.
func TestRunIsAtomic(t *testing.T) {
	st := freshStore(t, startPostgres(t), t.Name())
	ctx := context.Background()
	if err := st.EnsureWorkspace(ctx, "ws_alpha"); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("boom")
	err := st.Run(ctx, func(ctx context.Context) error {
		if _, err := st.Sessions().CreateSession(ctx, "ws_alpha", control.Session{ID: "sess_example", CreatorID: "act_a", State: control.StateQueued, PoolID: "pool_a"}); err != nil {
			return err
		}
		if err := st.Record(ctx, control.Event{ID: "evt_example", WorkspaceID: "ws_alpha", ActorID: "act_a", Action: control.ActionCreate,
			Resource: control.Resource{Kind: control.ResourceSession, WorkspaceID: "ws_alpha", ID: "sess_example", CreatorID: "act_a"}, At: time.Now()}); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Run returned %v, want fn's own error", err)
	}
	if _, err := st.Sessions().GetSession(ctx, "ws_alpha", "sess_example"); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("the row survived the rollback: %v", err)
	}
	var n int
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM events`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("%d events survived the rollback", n)
	}
}

// TestRunNestsAndCommitsOnce: an inner Run joins the outer one; the write
// is visible only after the outer commit.
func TestRunNestsAndCommitsOnce(t *testing.T) {
	st := freshStore(t, startPostgres(t), t.Name())
	ctx := context.Background()
	if err := st.EnsureWorkspace(ctx, "ws_alpha"); err != nil {
		t.Fatal(err)
	}
	err := st.Run(ctx, func(outer context.Context) error {
		if err := st.Run(outer, func(inner context.Context) error {
			_, err := st.Sessions().CreateSession(inner, "ws_alpha", control.Session{ID: "sess_example", CreatorID: "act_a", State: control.StateQueued, PoolID: "pool_a"})
			return err
		}); err != nil {
			return err
		}
		// Not yet visible outside the unit.
		if _, err := st.Sessions().GetSession(ctx, "ws_alpha", "sess_example"); !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("uncommitted row visible outside: %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Sessions().GetSession(ctx, "ws_alpha", "sess_example"); err != nil {
		t.Fatalf("committed row missing: %v", err)
	}
}

// TestRecordedEventsLandInTheirWorkspace reads the rows back the one way the
// portable host contract cannot: in SQL. storetest.RunHost pins what Record
// answers; this pins that the row is actually there, in the workspace the
// event named, with the fixed fields the outbox consumer will read.
func TestRecordedEventsLandInTheirWorkspace(t *testing.T) {
	st := freshStore(t, startPostgres(t), t.Name())
	ctx := context.Background()
	for _, ws := range []control.WorkspaceID{"ws_alpha", "ws_beta"} {
		if err := st.EnsureWorkspace(ctx, ws); err != nil {
			t.Fatal(err)
		}
	}

	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, e := range []control.Event{
		{ID: "evt_example", WorkspaceID: "ws_alpha", ActorID: "act_a", Action: control.ActionCreate,
			Resource: control.Resource{Kind: control.ResourceSession, WorkspaceID: "ws_alpha", ID: "sess_example", CreatorID: "act_a"},
			At:       at, PlacementGeneration: 2,
			Usage: control.Usage{CPUTimeSeconds: 1.5, MemoryByteSeconds: 2, StorageBytes: 3, NetworkBytes: 4, AgentTokenCount: 5}},
		{ID: "evt_second", WorkspaceID: "ws_alpha", ActorID: "act_a", Action: control.ActionDelete,
			Resource: control.Resource{Kind: control.ResourceSession, WorkspaceID: "ws_alpha", ID: "sess_example", CreatorID: "act_a"},
			At:       at},
		{ID: "evt_beta", WorkspaceID: "ws_beta", ActorID: "act_b", Action: control.ActionCreate,
			Resource: control.Resource{Kind: control.ResourceEnvironment, WorkspaceID: "ws_beta", ID: "env_example", CreatorID: "act_b"},
			At:       at},
	} {
		if err := st.Record(ctx, e); err != nil {
			t.Fatalf("Record(%s): %v", e.ID, err)
		}
	}

	for ws, want := range map[string]int{"ws_alpha": 2, "ws_beta": 1} {
		var n int
		if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE workspace_id = $1`, ws).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != want {
			t.Fatalf("%s holds %d events, want %d", ws, n, want)
		}
	}

	var (
		actor, action, kind, resource, creator string
		gen                                    int64
		cpu                                    float64
		mem, storage, network, tokens          int64
	)
	if err := st.pool.QueryRow(ctx, `SELECT actor_id, action, resource_kind, resource_id, resource_creator_id,
		placement_generation, cpu_time_seconds, memory_byte_seconds, storage_bytes, network_bytes, agent_token_count
		FROM events WHERE id = $1`, "evt_example").Scan(&actor, &action, &kind, &resource, &creator,
		&gen, &cpu, &mem, &storage, &network, &tokens); err != nil {
		t.Fatal(err)
	}
	if actor != "act_a" || action != string(control.ActionCreate) || kind != string(control.ResourceSession) ||
		resource != "sess_example" || creator != "act_a" || gen != 2 {
		t.Fatalf("fixed fields: %q %q %q %q %q %d", actor, action, kind, resource, creator, gen)
	}
	if cpu != 1.5 || mem != 2 || storage != 3 || network != 4 || tokens != 5 {
		t.Fatalf("usage columns: %v %d %d %d %d", cpu, mem, storage, network, tokens)
	}
}

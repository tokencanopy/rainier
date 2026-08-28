// internal/controld/pgstore/pgstore_test.go
package pgstore

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"rainier/internal/controld"
	"rainier/internal/controld/storetest"
)

// startPostgres runs a disposable postgres:16-alpine container on an
// ephemeral host port and returns a DSN. Skips when docker is unavailable
// (mirrors the docker driver's mustDockerPath discipline).
func startPostgres(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available")
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

// internal/controld/pgstore/migrate.go
package pgstore

import (
	"context"
	"embed"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// Migrate applies every embedded migration in migrations/ whose leading
// version number is greater than the highest version already recorded in
// schema_migrations, in ascending order. Each migration file runs and its
// version is recorded in a single transaction, so a failed migration leaves
// schema_migrations untouched for that version.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	return migrateTo(ctx, pool, math.MaxInt)
}

// migrateTo is Migrate with a ceiling: it stops after maxVersion instead of
// running to head. Nothing in production wants a half-migrated schema — this
// exists so a migration test can build the database exactly as the release
// before it left it, and then watch the next migration run against real rows.
func migrateTo(ctx context.Context, pool *pgxpool.Pool, maxVersion int) error {
	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version int PRIMARY KEY,
		applied_at timestamptz NOT NULL DEFAULT now()
	)`); err != nil {
		return fmt.Errorf("pgstore: create schema_migrations: %w", err)
	}

	var maxApplied int
	if err := pool.QueryRow(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&maxApplied); err != nil {
		return fmt.Errorf("pgstore: read schema_migrations: %w", err)
	}

	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("pgstore: read migrations dir: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)

	for _, name := range names {
		version, err := migrationVersion(name)
		if err != nil {
			return fmt.Errorf("pgstore: %s: %w", name, err)
		}
		if version <= maxApplied || version > maxVersion {
			continue
		}
		sql, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("pgstore: read %s: %w", name, err)
		}
		if err := applyMigration(ctx, pool, version, string(sql)); err != nil {
			return fmt.Errorf("pgstore: apply %s: %w", name, err)
		}
	}
	return nil
}

func applyMigration(ctx context.Context, pool *pgxpool.Pool, version int, sql string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, sql); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, version); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// migrationVersion returns the leading integer of a migration filename, e.g.
// "0001_init.sql" -> 1.
func migrationVersion(name string) (int, error) {
	prefix, _, ok := strings.Cut(name, "_")
	if !ok {
		return 0, fmt.Errorf("migration filename %q has no version prefix", name)
	}
	v, err := strconv.Atoi(prefix)
	if err != nil {
		return 0, fmt.Errorf("migration filename %q has invalid version prefix: %w", name, err)
	}
	return v, nil
}

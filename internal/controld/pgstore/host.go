// internal/controld/pgstore/host.go
package pgstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tokencanopy/rainier/control"
)

// The four host lookups the control ports deliberately have no method for
// (controld.HostStore), plus the error discipline every native path in this
// package follows.

// unavailable is what an unexpected database failure becomes on the native
// paths: control.ErrUnavailable, naming the operation and — when PostgreSQL
// gave one — its SQLSTATE, and nothing else. A pgx error carries the
// statement it failed on, often the value that broke it, and on a connection
// failure the host it was dialing; wrapping one would hand a caller the SQL,
// the row, and the DSN when all it is allowed to learn is that the store
// could not answer. The SQLSTATE is the exception: it is a five-character
// class code with no content in it, and it is the difference between a
// legible incident and a shrug.
func unavailable(op string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return fmt.Errorf("pgstore: %s: SQLSTATE %s: %w", op, pgErr.Code, control.ErrUnavailable)
	}
	return fmt.Errorf("pgstore: %s: %w", op, control.ErrUnavailable)
}

// constraintViolation reports the SQLSTATE and constraint name of err when it
// is a PostgreSQL integrity error, so a caller can tell a duplicate name from
// a duplicate idempotency key from a workspace that does not exist.
func constraintViolation(err error) (code, constraint string) {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code, pgErr.ConstraintName
	}
	return "", ""
}

// SQLSTATE classes the native paths answer with a sentinel rather than with
// ErrUnavailable: a unique violation is somebody else already holding the
// identity, and a foreign key violation here can only be the workspace
// column, because it is the only foreign key a tenant row has left.
const (
	sqlstateUniqueViolation     = "23505"
	sqlstateForeignKeyViolation = "23503"
)

// EnsureWorkspace makes ws exist. It is a statement of fact rather than a
// create: New calls it on every start for the installation workspace, and the
// repository contract suite for its two, so a second call must be a no-op
// rather than a conflict.
func (s *Store) EnsureWorkspace(ctx context.Context, ws control.WorkspaceID) error {
	if ws == "" {
		return control.ErrInvalid
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO workspaces (id) VALUES ($1) ON CONFLICT (id) DO NOTHING`, string(ws)); err != nil {
		return unavailable("ensure workspace", err)
	}
	return nil
}

// EnvironmentByName resolves a name inside ws to an id. The index is a
// locator, never authority: a name another workspace holds is simply not
// here, which is the same answer as a name nobody holds.
func (s *Store) EnvironmentByName(ctx context.Context, ws control.WorkspaceID, name string) (control.EnvironmentID, error) {
	if ws == "" {
		return "", control.ErrInvalid
	}
	var id string
	err := s.pool.QueryRow(ctx,
		`SELECT id FROM environments WHERE workspace_id = $1 AND name = $2`, string(ws), name).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", control.ErrNotFound
		}
		return "", unavailable("environment by name", err)
	}
	return control.EnvironmentID(id), nil
}

// SnapshotRunner names the runner that built id's cached snapshot, "" when
// there is none — stale or not, because the wire has always shown the column.
func (s *Store) SnapshotRunner(ctx context.Context, ws control.WorkspaceID, id control.EnvironmentID) (control.RunnerID, error) {
	if ws == "" {
		return "", control.ErrInvalid
	}
	var holder string
	err := s.pool.QueryRow(ctx,
		`SELECT snapshot_runner FROM environments WHERE workspace_id = $1 AND id = $2`, string(ws), string(id)).Scan(&holder)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", control.ErrNotFound
		}
		return "", unavailable("snapshot runner", err)
	}
	return control.RunnerID(holder), nil
}

// NextRunnerGeneration opens a new generation for id in pool and returns it:
// 1 for a runner never seen, else one more than stored. One statement, so two
// controld replicas racing a reconnecting runner cannot mint the same
// generation twice — the row, not either process, is the authority the fleet
// repository then fences on.
func (s *Store) NextRunnerGeneration(ctx context.Context, pool control.PoolID, id control.RunnerID) (uint64, error) {
	if pool == "" || id == "" {
		return 0, control.ErrInvalid
	}
	var generation int64
	err := s.pool.QueryRow(ctx, `
		INSERT INTO runners (pool_id, name, generation) VALUES ($1, $2, 1)
		ON CONFLICT (pool_id, name) DO UPDATE SET generation = runners.generation + 1
		RETURNING generation`, string(pool), string(id)).Scan(&generation)
	if err != nil {
		return 0, unavailable("next runner generation", err)
	}
	return uint64(generation), nil
}

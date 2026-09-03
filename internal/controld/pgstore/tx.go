// internal/controld/pgstore/tx.go
package pgstore

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tokencanopy/rainier/control"
)

// The store's atomicity (control.UnitOfWork). The transaction rides in the
// context and never crosses the port: a caller says "these writes commit
// together" and hands back a context, and no repository method here knows
// whether it is inside a unit or not — it asks q(ctx) what to run on.

// querier is what every statement runs on: the transaction the context
// carries when a Run is open, else the pool.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type txKey struct{}

func (s *Store) q(ctx context.Context) querier {
	if tx, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
		return tx
	}
	return s.pool
}

// Run opens one transaction, hands fn a context that carries it, and commits
// when fn returns nil. A context already carrying a transaction joins it:
// fn runs inside the enclosing unit and the outer Run commits. A failure to
// begin or commit is control.ErrUnavailable; fn's own error is returned as
// is after the rollback.
func (s *Store) Run(ctx context.Context, fn func(context.Context) error) error {
	if _, nested := ctx.Value(txKey{}).(pgx.Tx); nested {
		return fn(ctx)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return control.ErrUnavailable
	}
	defer tx.Rollback(ctx)
	if err := fn(context.WithValue(ctx, txKey{}, tx)); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return control.ErrUnavailable
	}
	return nil
}

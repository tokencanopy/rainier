// internal/controld/pgstore/bootstrap.go
package pgstore

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tokencanopy/rainier/control"
)

// pgBootstraps is the durable half of the session bootstrap token: one row
// per session, holding a hash and never a token.
type pgBootstraps struct{ s *Store }

var _ control.SessionBootstrapStore = pgBootstraps{}

// Bootstraps is the fourth control repository port, a view over this store's
// own rows exactly as the other three are.
func (s *Store) Bootstraps() control.SessionBootstrapStore { return pgBootstraps{s} }

// PutSessionBootstrap records rec as id's only acceptable token.
//
// It is an upsert onto the session's own primary key, which is what makes a
// fresh mint retire its predecessor: consumed_at resets to NULL along with
// the hash, so the new token is unspent and the old one is simply not there
// to be presented any more.
func (r pgBootstraps) PutSessionBootstrap(ctx context.Context, ws control.WorkspaceID, id control.SessionID, rec control.SessionBootstrap) error {
	if ws == "" || id == "" || rec.Hash == "" {
		return control.ErrInvalid
	}
	_, err := r.s.q(ctx).Exec(ctx, `
		INSERT INTO session_bootstraps
			(session_id, workspace_id, token_hash, placement_generation, expires_at, consumed_at)
		VALUES ($1, $2, $3, $4, $5, NULL)
		ON CONFLICT (session_id) DO UPDATE SET
			workspace_id = EXCLUDED.workspace_id,
			token_hash = EXCLUDED.token_hash,
			placement_generation = EXCLUDED.placement_generation,
			expires_at = EXCLUDED.expires_at,
			consumed_at = NULL,
			created_at = now()`,
		string(id), string(ws), rec.Hash, int64(rec.PlacementGeneration), rec.ExpiresAt)
	if err != nil {
		// The error text is dropped rather than wrapped: a constraint
		// violation here quotes the row, and this row's business is a
		// capability. The caller fails the create either way.
		return unavailable("put session bootstrap", err)
	}
	return nil
}

// ConsumeSessionBootstrap spends the token in ONE predicated statement.
//
// The UPDATE is the whole of single-use: every condition a success requires
// is in its WHERE clause, so two exchanges racing on one token produce
// exactly one row affected. A read-then-write would produce two, which is
// the bug this method exists not to have.
//
// The second query runs only on the miss, and only to say WHY. It reads a row
// that no longer has a race to lose — nothing can turn an unspent token into
// a spendable one — so a diagnosis taken after the fact is as true as one
// taken during. It reports nothing it did not already know about a token the
// caller presented.
func (r pgBootstraps) ConsumeSessionBootstrap(ctx context.Context, ws control.WorkspaceID, id control.SessionID, hash string, gen uint64, now time.Time) error {
	if ws == "" || id == "" || hash == "" {
		return control.ErrInvalid
	}
	ct, err := r.s.q(ctx).Exec(ctx, `
		UPDATE session_bootstraps SET consumed_at = $1
		WHERE workspace_id = $2 AND session_id = $3 AND token_hash = $4
		  AND placement_generation = $5 AND expires_at > $1 AND consumed_at IS NULL`,
		now, string(ws), string(id), hash, int64(gen))
	if err != nil {
		return unavailable("consume session bootstrap", err)
	}
	if ct.RowsAffected() == 1 {
		return nil
	}

	var (
		rowGen    int64
		expiresAt time.Time
		consumed  *time.Time
	)
	qerr := r.s.q(ctx).QueryRow(ctx, `
		SELECT placement_generation, expires_at, consumed_at FROM session_bootstraps
		WHERE workspace_id = $1 AND session_id = $2 AND token_hash = $3`,
		string(ws), string(id), hash).Scan(&rowGen, &expiresAt, &consumed)
	switch {
	case errors.Is(qerr, pgx.ErrNoRows):
		// No row, or a row under a different hash: one answer, because a
		// caller holding no valid token must not learn the state of the one
		// that exists.
		return control.ErrBootstrapUnknown
	case qerr != nil:
		return unavailable("read session bootstrap", qerr)
	case uint64(rowGen) != gen:
		return control.ErrBootstrapFenced
	case !now.Before(expiresAt):
		return control.ErrBootstrapExpired
	case consumed != nil:
		return control.ErrBootstrapSpent
	}
	// The row satisfies every condition the UPDATE tested and the UPDATE
	// still matched nothing: a concurrent exchange spent it between the two
	// statements, which is exactly the race single-use exists to settle, and
	// this caller is the one that lost.
	return control.ErrBootstrapSpent
}

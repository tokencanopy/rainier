package pgstore

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/tokencanopy/rainier/control"
)

type pgGuestReconnects struct{ s *Store }

var _ control.GuestReconnectStore = pgGuestReconnects{}

// GuestReconnects exposes durable authorization without enabling any wire path.
func (s *Store) GuestReconnects() control.GuestReconnectStore { return pgGuestReconnects{s} }

func validReconnectScope(b control.GuestReconnectScope) bool {
	return b.WorkspaceID != "" && b.PoolID != "" && b.SessionID != "" && b.RunnerID != "" && b.PlacementGeneration > 0 && b.PlacementGeneration <= math.MaxInt64 && b.ConnectionGeneration > 0 && b.ConnectionGeneration <= math.MaxInt64
}

// lockScope holds authority stable until the enclosing transaction commits.
// A read outside this transaction would authorize a takeover after re-placement
// or a newer runner registration. The row locks also serialize those changes.
func (r pgGuestReconnects) lockScope(ctx context.Context, b control.GuestReconnectScope) error {
	if !validReconnectScope(b) {
		return control.ErrReconnectInvalid
	}
	var found int
	err := r.s.q(ctx).QueryRow(ctx, `SELECT 1 FROM runners WHERE pool_id=$1 AND name=$2 AND generation=$3 AND connected FOR SHARE`, string(b.PoolID), string(b.RunnerID), int64(b.ConnectionGeneration)).Scan(&found)
	if errors.Is(err, pgx.ErrNoRows) {
		return control.ErrReconnectFenced
	}
	if err != nil {
		return control.ErrUnavailable
	}
	err = r.s.q(ctx).QueryRow(ctx, `SELECT 1 FROM sessions WHERE workspace_id=$1 AND id=$2 AND pool_id=$3 AND runner=$4 AND placement_generation=$5 AND state IN ('creating','running') FOR UPDATE`, string(b.WorkspaceID), string(b.SessionID), string(b.PoolID), string(b.RunnerID), int64(b.PlacementGeneration)).Scan(&found)
	if errors.Is(err, pgx.ErrNoRows) {
		return control.ErrReconnectFenced
	}
	if err != nil {
		return control.ErrUnavailable
	}
	return nil
}

// lockBootstrap must precede expiry evaluation. UPDATE predicates can be
// evaluated before a lock-only transaction releases the tuple, so a timestamp
// predicate in that UPDATE alone does not enforce expiry after a lock wait.
func (r pgGuestReconnects) lockBootstrap(ctx context.Context, b control.GuestReconnectScope) error {
	var found int
	err := r.s.q(ctx).QueryRow(ctx, `SELECT 1 FROM session_bootstraps WHERE workspace_id=$1 AND session_id=$2 FOR UPDATE`, string(b.WorkspaceID), string(b.SessionID)).Scan(&found)
	if errors.Is(err, pgx.ErrNoRows) {
		return control.ErrReconnectInvalid
	}
	if err != nil {
		return control.ErrUnavailable
	}
	return nil
}

func (r pgGuestReconnects) EnrollGuest(ctx context.Context, b control.GuestReconnectScope, hash string, id control.GuestReconnectIdentity, now time.Time) error {
	if hash == "" || len(id.BootEpoch) == 0 || len(id.BootEpoch) > 256 || len(id.PublicKey) == 0 || len(id.PublicKey) > 256 || now.IsZero() {
		return control.ErrReconnectInvalid
	}
	return r.s.Run(ctx, func(ctx context.Context) error {
		if err := r.lockScope(ctx, b); err != nil {
			return err
		}
		if err := r.lockBootstrap(ctx, b); err != nil {
			return err
		}
		ct, err := r.s.q(ctx).Exec(ctx, `UPDATE session_bootstraps SET consumed_at=$1,guest_boot_epoch=$2,guest_public_key=$3 WHERE workspace_id=$4 AND session_id=$5 AND token_hash=$6 AND placement_generation=$7 AND expires_at>$1 AND expires_at>clock_timestamp() AND consumed_at IS NULL AND guest_public_key IS NULL`, now, id.BootEpoch, id.PublicKey, string(b.WorkspaceID), string(b.SessionID), hash, int64(b.PlacementGeneration))
		if err != nil {
			return control.ErrUnavailable
		}
		if ct.RowsAffected() != 1 {
			return control.ErrReconnectInvalid
		}
		return nil
	})
}

func validReconnectAttempt(a control.GuestReconnectAttempt, now time.Time) bool {
	return len(a.ID) > 0 && len(a.ID) <= 256 && len(a.Challenge) > 0 && len(a.Challenge) <= 256 && !now.IsZero() && a.ExpiresAt.After(now) && !a.ExpiresAt.After(now.Add(5*time.Second))
}

func (r pgGuestReconnects) BeginGuestReconnect(ctx context.Context, b control.GuestReconnectScope, a control.GuestReconnectAttempt, now time.Time) (control.GuestReconnectIdentity, error) {
	var id control.GuestReconnectIdentity
	if !validReconnectAttempt(a, now) {
		return id, control.ErrReconnectInvalid
	}
	err := r.s.Run(ctx, func(ctx context.Context) error {
		if err := r.lockScope(ctx, b); err != nil {
			return err
		}
		if err := r.lockBootstrap(ctx, b); err != nil {
			return err
		}
		var databaseNow time.Time
		if err := r.s.q(ctx).QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&databaseNow); err != nil {
			return control.ErrUnavailable
		}
		if !databaseNow.Before(a.ExpiresAt) {
			return control.ErrReconnectExpired
		}
		if a.ExpiresAt.After(databaseNow.Add(5 * time.Second)) {
			return control.ErrReconnectInvalid
		}
		err := r.s.q(ctx).QueryRow(ctx, `UPDATE session_bootstraps SET reconnect_attempt=$1,reconnect_challenge=$2,reconnect_expires_at=$3,reconnect_runner_generation=$4 WHERE workspace_id=$5 AND session_id=$6 AND placement_generation=$7 AND guest_public_key IS NOT NULL RETURNING guest_boot_epoch,guest_public_key`, a.ID, a.Challenge, a.ExpiresAt, int64(b.ConnectionGeneration), string(b.WorkspaceID), string(b.SessionID), int64(b.PlacementGeneration)).Scan(&id.BootEpoch, &id.PublicKey)
		if errors.Is(err, pgx.ErrNoRows) {
			return control.ErrReconnectInvalid
		}
		if err != nil {
			return control.ErrUnavailable
		}
		return nil
	})
	if err != nil {
		return control.GuestReconnectIdentity{}, err
	}
	return id, nil
}

func (r pgGuestReconnects) ConsumeGuestReconnect(ctx context.Context, b control.GuestReconnectScope, id control.GuestReconnectIdentity, a control.GuestReconnectAttempt, token control.SessionBootstrap, now time.Time) (uint64, error) {
	var epoch uint64
	if token.Hash == "" || token.PlacementGeneration != b.PlacementGeneration || !token.ExpiresAt.After(now) || token.ExpiresAt.After(now.Add(120*time.Second)) || now.IsZero() {
		return 0, control.ErrReconnectInvalid
	}
	err := r.s.Run(ctx, func(ctx context.Context) error {
		if err := r.lockScope(ctx, b); err != nil {
			return err
		}
		var expiry time.Time
		var current int64
		err := r.s.q(ctx).QueryRow(ctx, `SELECT reconnect_expires_at,guest_connection_epoch FROM session_bootstraps WHERE workspace_id=$1 AND session_id=$2 AND placement_generation=$3 AND guest_boot_epoch=$4 AND guest_public_key=$5 AND reconnect_attempt=$6 AND reconnect_challenge=$7 AND reconnect_runner_generation=$8 FOR UPDATE`, string(b.WorkspaceID), string(b.SessionID), int64(b.PlacementGeneration), id.BootEpoch, id.PublicKey, a.ID, a.Challenge, int64(b.ConnectionGeneration)).Scan(&expiry, &current)
		if errors.Is(err, pgx.ErrNoRows) {
			return control.ErrReconnectInvalid
		}
		if err != nil {
			return control.ErrUnavailable
		}
		var databaseNow time.Time
		if err := r.s.q(ctx).QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&databaseNow); err != nil {
			return control.ErrUnavailable
		}
		if !now.Before(expiry) || !databaseNow.Before(expiry) {
			return control.ErrReconnectExpired
		}
		if !token.ExpiresAt.After(databaseNow) || token.ExpiresAt.After(databaseNow.Add(120*time.Second)) {
			return control.ErrReconnectInvalid
		}
		if !expiry.Equal(a.ExpiresAt) || current == math.MaxInt64 {
			return control.ErrReconnectInvalid
		}
		_, err = r.s.q(ctx).Exec(ctx, `UPDATE session_bootstraps SET guest_connection_epoch=guest_connection_epoch+1,token_hash=$1,expires_at=$2,consumed_at=NULL,reconnect_attempt=NULL,reconnect_challenge=NULL,reconnect_expires_at=NULL,reconnect_runner_generation=NULL WHERE workspace_id=$3 AND session_id=$4`, token.Hash, token.ExpiresAt, string(b.WorkspaceID), string(b.SessionID))
		if err != nil {
			return control.ErrUnavailable
		}
		epoch = uint64(current + 1)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return epoch, nil
}

func (r pgGuestReconnects) ReadGuestReconnect(ctx context.Context, b control.GuestReconnectScope, attemptID string) (control.GuestReconnectIdentity, control.GuestReconnectAttempt, error) {
	var id control.GuestReconnectIdentity
	var a control.GuestReconnectAttempt
	if !validReconnectScope(b) || len(attemptID) == 0 || len(attemptID) > 256 {
		return id, a, control.ErrReconnectInvalid
	}
	err := r.s.q(ctx).QueryRow(ctx, `SELECT guest_boot_epoch,guest_public_key,reconnect_attempt,reconnect_challenge,reconnect_expires_at FROM session_bootstraps WHERE workspace_id=$1 AND session_id=$2 AND placement_generation=$3 AND reconnect_runner_generation=$4 AND reconnect_attempt=$5 AND guest_public_key IS NOT NULL`, string(b.WorkspaceID), string(b.SessionID), int64(b.PlacementGeneration), int64(b.ConnectionGeneration), attemptID).Scan(&id.BootEpoch, &id.PublicKey, &a.ID, &a.Challenge, &a.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return control.GuestReconnectIdentity{}, control.GuestReconnectAttempt{}, control.ErrReconnectInvalid
	}
	if err != nil {
		return control.GuestReconnectIdentity{}, control.GuestReconnectAttempt{}, control.ErrUnavailable
	}
	return id, a, nil
}

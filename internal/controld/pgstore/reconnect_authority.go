package pgstore

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/internal/controld"
)

var _ controld.GuestReconnectAuthorityStore = (*Store)(nil)

// WithGuestReconnectAuthority holds runner, placement, bootstrap and owner rows
// stable while the host checks current policy and mutates its capability.
func (s *Store) WithGuestReconnectAuthority(ctx context.Context, b control.GuestReconnectScope, fn func(context.Context, controld.GuestReconnectAuthority) error) error {
	if _, nested := ctx.Value(txKey{}).(pgx.Tx); nested {
		return control.ErrReconnectInvalid
	}
	if fn == nil {
		return control.ErrReconnectInvalid
	}
	return s.Run(ctx, func(ctx context.Context) error {
		if err := (pgGuestReconnects{s}).lockScope(ctx, b); err != nil {
			return err
		}
		a := controld.GuestReconnectAuthority{}
		var err error
		a.Session, err = s.Sessions().GetSession(ctx, b.WorkspaceID, b.SessionID)
		if err != nil {
			return control.ErrUnavailable
		}
		if a.Session.CreatorID == "" {
			return control.ErrReconnectFenced
		}
		if _, err = s.q(ctx).Exec(ctx, `LOCK TABLE session_bootstraps IN ROW EXCLUSIVE MODE`); err != nil {
			return control.ErrUnavailable
		}
		err = s.q(ctx).QueryRow(ctx, `SELECT guest_connection_epoch FROM session_bootstraps WHERE workspace_id=$1 AND session_id=$2 FOR UPDATE`, string(b.WorkspaceID), string(b.SessionID)).Scan(&a.Epoch)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return control.ErrUnavailable
		}
		err = s.q(ctx).QueryRow(ctx, `SELECT id,github_id,login,role,created_at FROM users WHERE id=$1 FOR SHARE`, string(a.Session.CreatorID)).Scan(&a.User.ID, &a.User.GitHubID, &a.User.Login, &a.User.Role, &a.User.CreatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return control.ErrReconnectFenced
		}
		if err != nil {
			return control.ErrUnavailable
		}
		if err = s.q(ctx).QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&a.Now); err != nil {
			return control.ErrUnavailable
		}
		return fn(ctx, a)
	})
}

package pgstore

import (
	"context"
	"errors"
	"testing"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/internal/controld"
)

func TestReconnectAuthorityUsesCurrentOwnerAndSocket(t *testing.T) {
	ctx := context.Background()
	st := freshStore(t, startPostgres(t), t.Name())
	u, err := st.UpsertUser(ctx, 42, "member_test", "member")
	if err != nil {
		t.Fatal(err)
	}
	b := control.GuestReconnectScope{WorkspaceID: "ws_self_hosted", PoolID: "pool_self_hosted", SessionID: "session_authority_test", RunnerID: "runner_test", PlacementGeneration: 1, ConnectionGeneration: 1}
	if _, err := st.Sessions().CreateSession(ctx, b.WorkspaceID, control.Session{ID: b.SessionID, CreatorID: control.ActorID(u.ID), PoolID: b.PoolID, RunnerID: b.RunnerID, State: control.StateRunning}); err != nil {
		t.Fatal(err)
	}
	if err := st.Fleet().UpsertRunner(ctx, b.PoolID, control.Runner{ID: b.RunnerID, Generation: 1, Connected: true}); err != nil {
		t.Fatal(err)
	}
	if err := st.Run(ctx, func(inner context.Context) error {
		err := st.WithGuestReconnectAuthority(inner, b, func(context.Context, controld.GuestReconnectAuthority) error {
			t.Fatal("nested transaction reached delivery authority")
			return nil
		})
		if !errors.Is(err, control.ErrReconnectInvalid) {
			t.Fatalf("nested authority: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	called := false
	err = st.WithGuestReconnectAuthority(ctx, b, func(_ context.Context, a controld.GuestReconnectAuthority) error {
		called = true
		if a.User.ID != u.ID || a.Session.ID != b.SessionID || a.Epoch != 0 || a.Now.IsZero() {
			t.Fatal("incorrect authority")
		}
		return nil
	})
	if err != nil || !called {
		t.Fatalf("current authority refused: %v", err)
	}
	stale := b
	stale.ConnectionGeneration = 2
	if err := st.WithGuestReconnectAuthority(ctx, stale, func(context.Context, controld.GuestReconnectAuthority) error {
		t.Fatal("stale socket authorized")
		return nil
	}); !errors.Is(err, control.ErrReconnectFenced) {
		t.Fatalf("stale socket: %v", err)
	}
	if _, err := st.pool.Exec(ctx, `UPDATE users SET login='revoked_test' WHERE id=$1`, u.ID); err != nil {
		t.Fatal(err)
	}
	err = st.WithGuestReconnectAuthority(ctx, b, func(_ context.Context, a controld.GuestReconnectAuthority) error {
		if a.User.Login != "revoked_test" {
			t.Fatal("cached owner authority")
		}
		return control.ErrReconnectFenced
	})
	if !errors.Is(err, control.ErrReconnectFenced) {
		t.Fatalf("current owner refusal: %v", err)
	}
}

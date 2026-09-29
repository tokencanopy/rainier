package pgstore

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/tokencanopy/rainier/controlapp"
	"github.com/tokencanopy/rainier/controlapp/repotest"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/control"
)

func TestGuestReconnectStoreContract(t *testing.T) {
	dsn := startPostgres(t)
	repotest.RunGuestReconnect(t, func(t *testing.T) repotest.GuestReconnectStores {
		st := freshStore(t, dsn, t.Name())
		return repotest.GuestReconnectStores{Stores: repotest.Stores{Sessions: st.Sessions(), Fleet: st.Fleet(), Bootstraps: st.Bootstraps(), Provision: st.EnsureWorkspace}, Reconnects: st.GuestReconnects()}
	})
}

func TestGuestReconnectProofWithPostgres(t *testing.T) {
	ctx := context.Background()
	st := freshStore(t, startPostgres(t), t.Name())
	b := control.GuestReconnectScope{WorkspaceID: "ws_self_hosted", PoolID: "pool_self_hosted", SessionID: "proof.test", RunnerID: "runner.test", PlacementGeneration: 1, ConnectionGeneration: 7}
	if _, err := st.Sessions().CreateSession(ctx, b.WorkspaceID, control.Session{ID: b.SessionID, CreatorID: "actor.test", PoolID: b.PoolID, RunnerID: b.RunnerID, State: control.StateRunning}); err != nil {
		t.Fatal(err)
	}
	if err := st.Fleet().UpsertRunner(ctx, b.PoolID, control.Runner{ID: b.RunnerID, Generation: 7, Connected: true}); err != nil {
		t.Fatal(err)
	}
	clock := reconnectClock{time.Now().UTC().Truncate(time.Microsecond)}
	mint := controlapp.SessionBootstrapMinter{Store: st.Bootstraps(), Clock: clock}
	token, err := mint.Mint(ctx, b.WorkspaceID, b.SessionID, 1)
	if err != nil {
		t.Fatal(err)
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	service := controlapp.GuestReconnect{Store: st.GuestReconnects(), Clock: clock}
	if err := service.Enroll(ctx, b, token, "boot.test", base64.RawURLEncoding.EncodeToString(pub)); err != nil {
		t.Fatal(err)
	}
	challenge, err := service.Begin(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	message, err := challenge.SigningMessage()
	if err != nil {
		t.Fatal(err)
	}
	signature := base64.RawURLEncoding.EncodeToString(ed25519.Sign(priv, message))
	wrong := b
	wrong.SessionID = "another.test"
	if _, _, err := service.Accept(ctx, wrong, challenge.AttemptID, signature); err == nil {
		t.Fatal("cross-session proof accepted")
	}
	_, wrongPriv, _ := ed25519.GenerateKey(rand.Reader)
	if _, _, err := service.Accept(ctx, b, challenge.AttemptID, base64.RawURLEncoding.EncodeToString(ed25519.Sign(wrongPriv, message))); err == nil {
		t.Fatal("wrong key accepted")
	}
	epoch, fresh, err := service.Accept(ctx, b, challenge.AttemptID, signature)
	if err != nil || epoch != 1 || fresh == "" || fresh == token {
		t.Fatalf("accept: epoch=%d token-present=%t err=%v", epoch, fresh != "", err)
	}
	// Reopening the durable adapter cannot make a lost acceptance replayable.
	reopened := reopen(t, st.pool.Config().ConnString())
	service.Store = reopened.GuestReconnects()
	if epoch, token, err := service.Accept(ctx, b, challenge.AttemptID, signature); err == nil || epoch != 0 || token != "" {
		t.Fatal("replay returned authority")
	}
	if err := st.Bootstraps().ConsumeSessionBootstrap(ctx, b.WorkspaceID, b.SessionID, controlapp.HashSessionBootstrapToken(fresh), 1, clock.Now()); err != nil {
		t.Fatal(err)
	}
}

type reconnectClock struct{ now time.Time }

func (c reconnectClock) Now() time.Time { return c.now }

// A request can wait behind a lifecycle transaction longer than its challenge
// lives. The store must check database time AFTER that wait, not reuse the
// caller's timestamp or PostgreSQL's transaction-start timestamp.
func TestGuestReconnectExpiresWhileWaitingForAuthorityLock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st := freshStore(t, startPostgres(t), t.Name())
	b := control.GuestReconnectScope{WorkspaceID: "ws_self_hosted", PoolID: "pool_self_hosted", SessionID: "lock.test", RunnerID: "runner.test", PlacementGeneration: 1, ConnectionGeneration: 1}
	if _, err := st.Sessions().CreateSession(ctx, b.WorkspaceID, control.Session{ID: b.SessionID, CreatorID: "actor.test", PoolID: b.PoolID, RunnerID: b.RunnerID, State: control.StateRunning}); err != nil {
		t.Fatal(err)
	}
	if err := st.Fleet().UpsertRunner(ctx, b.PoolID, control.Runner{ID: b.RunnerID, Generation: 1, Connected: true}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	token := control.SessionBootstrap{Hash: "old-hash.test", PlacementGeneration: 1, ExpiresAt: now.Add(time.Minute)}
	if err := st.Bootstraps().PutSessionBootstrap(ctx, b.WorkspaceID, b.SessionID, token); err != nil {
		t.Fatal(err)
	}
	id := control.GuestReconnectIdentity{BootEpoch: "boot.test", PublicKey: "public-key.test"}
	r := st.GuestReconnects()
	if err := r.EnrollGuest(ctx, b, token.Hash, id, now); err != nil {
		t.Fatal(err)
	}
	now = time.Now().UTC().Truncate(time.Microsecond)
	a := control.GuestReconnectAttempt{ID: "attempt.test", Challenge: "nonce.test", ExpiresAt: now.Add(2 * time.Second)}
	if _, err := r.BeginGuestReconnect(ctx, b, a, now); err != nil {
		t.Fatal(err)
	}
	tx, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, `SELECT 1 FROM sessions WHERE workspace_id=$1 AND id=$2 FOR UPDATE`, string(b.WorkspaceID), string(b.SessionID)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		fresh := token
		fresh.Hash = "new-hash.test"
		epoch, err := r.ConsumeGuestReconnect(ctx, b, id, a, fresh, now)
		if epoch != 0 {
			err = fmt.Errorf("unexpected epoch %d", epoch)
		}
		done <- err
	}()
	deadline := time.Now().Add(3 * time.Second)
	for {
		var waiting bool
		err := st.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT 1 FROM sessions%')`).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("consume did not wait on authority lock")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if wait := time.Until(a.ExpiresAt.Add(20 * time.Millisecond)); wait > 0 {
		time.Sleep(wait)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, control.ErrReconnectExpired) {
		t.Fatalf("expired during lock wait: %v", err)
	}
	var hash string
	var epoch int64
	if err := st.pool.QueryRow(ctx, `SELECT token_hash,guest_connection_epoch FROM session_bootstraps WHERE workspace_id=$1 AND session_id=$2`, string(b.WorkspaceID), string(b.SessionID)).Scan(&hash, &epoch); err != nil {
		t.Fatal(err)
	}
	if hash != token.Hash || epoch != 0 {
		t.Fatal("expired request changed capability or epoch")
	}
}

package pgstore

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"github.com/tokencanopy/rainier/controlapp"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/control"
)

func TestGuestReconnectDurableAuthorization(t *testing.T) {
	ctx := context.Background()
	dsn := startPostgres(t)
	st := freshStore(t, dsn, t.Name())
	scope := control.GuestReconnectScope{WorkspaceID: "ws_self_hosted", PoolID: "pool_self_hosted", SessionID: "session.test", RunnerID: "runner.test", PlacementGeneration: 1, ConnectionGeneration: 7}
	_, err := st.Sessions().CreateSession(ctx, scope.WorkspaceID, control.Session{ID: scope.SessionID, CreatorID: "actor.test", PoolID: scope.PoolID, RunnerID: scope.RunnerID, State: control.StateRunning, Spec: control.PortableSpec{Image: "image.test"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Fleet().UpsertRunner(ctx, scope.PoolID, control.Runner{ID: scope.RunnerID, Generation: 7, Connected: true}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	token := control.SessionBootstrap{Hash: "bootstrap.test", PlacementGeneration: 1, ExpiresAt: now.Add(time.Minute)}
	if err := st.Bootstraps().PutSessionBootstrap(ctx, scope.WorkspaceID, scope.SessionID, token); err != nil {
		t.Fatal(err)
	}
	identity := control.GuestReconnectIdentity{BootEpoch: "boot.test", PublicKey: "public-key.test"}
	r := st.GuestReconnects()
	bad := scope
	bad.ConnectionGeneration = 6
	if err := r.EnrollGuest(ctx, bad, token.Hash, identity, now); !errors.Is(err, control.ErrReconnectFenced) {
		t.Fatalf("stale enrollment: %v", err)
	}
	if err := r.EnrollGuest(ctx, scope, token.Hash, identity, now); err != nil {
		t.Fatal(err)
	}
	if err := r.EnrollGuest(ctx, scope, token.Hash, identity, now); err == nil {
		t.Fatal("bootstrap replay enrolled")
	}
	attempt := control.GuestReconnectAttempt{ID: "attempt.test", Challenge: "nonce.test", ExpiresAt: now.Add(5 * time.Second)}
	enrolled, err := r.BeginGuestReconnect(ctx, scope, attempt, now)
	if err != nil || enrolled != identity {
		t.Fatalf("begin: %+v %v", enrolled, err)
	}
	fresh := control.SessionBootstrap{Hash: "fresh.test", PlacementGeneration: 1, ExpiresAt: now.Add(time.Minute)}
	for name, changed := range map[string]control.GuestReconnectScope{
		"workspace": {WorkspaceID: "ws_other", PoolID: scope.PoolID, SessionID: scope.SessionID, RunnerID: scope.RunnerID, PlacementGeneration: 1, ConnectionGeneration: 7},
		"pool":      {WorkspaceID: scope.WorkspaceID, PoolID: "pool_other", SessionID: scope.SessionID, RunnerID: scope.RunnerID, PlacementGeneration: 1, ConnectionGeneration: 7},
		"runner":    {WorkspaceID: scope.WorkspaceID, PoolID: scope.PoolID, SessionID: scope.SessionID, RunnerID: "other.test", PlacementGeneration: 1, ConnectionGeneration: 7},
		"placement": {WorkspaceID: scope.WorkspaceID, PoolID: scope.PoolID, SessionID: scope.SessionID, RunnerID: scope.RunnerID, PlacementGeneration: 2, ConnectionGeneration: 7},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := r.ConsumeGuestReconnect(ctx, changed, identity, attempt, fresh, now); err == nil {
				t.Fatal("foreign authority accepted")
			}
		})
	}
	wrongIdentity := identity
	wrongIdentity.BootEpoch = "other-boot.test"
	if _, err := r.ConsumeGuestReconnect(ctx, scope, wrongIdentity, attempt, fresh, now); err == nil {
		t.Fatal("wrong boot accepted")
	}
	wrongIdentity = identity
	wrongIdentity.PublicKey = "other-key.test"
	if _, err := r.ConsumeGuestReconnect(ctx, scope, wrongIdentity, attempt, fresh, now); err == nil {
		t.Fatal("wrong pinned key accepted")
	}
	wrong := attempt
	wrong.Challenge = "wrong.test"
	if _, err := r.ConsumeGuestReconnect(ctx, scope, identity, wrong, fresh, now); err == nil {
		t.Fatal("wrong challenge accepted")
	}
	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			epoch, err := r.ConsumeGuestReconnect(ctx, scope, identity, attempt, fresh, now)
			if err == nil {
				if epoch != 1 {
					t.Errorf("epoch %d", epoch)
				}
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("concurrent successes: %d", wins.Load())
	}
	if err := st.Bootstraps().ConsumeSessionBootstrap(ctx, scope.WorkspaceID, scope.SessionID, fresh.Hash, 1, now); err != nil {
		t.Fatal(err)
	}
	// Reopen the durable adapter: neither a process restart nor a lost reply
	// makes the accepted proof spendable again.
	reopened := reopen(t, st.pool.Config().ConnString())
	if _, err := reopened.GuestReconnects().ConsumeGuestReconnect(ctx, scope, identity, attempt, fresh, now); err == nil {
		t.Fatal("replay after restart")
	}
	attempt.ID = "second.test"
	if _, err := r.BeginGuestReconnect(ctx, scope, attempt, now); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ConsumeGuestReconnect(ctx, scope, identity, attempt, fresh, attempt.ExpiresAt); !errors.Is(err, control.ErrReconnectExpired) {
		t.Fatalf("expiry boundary: %v", err)
	}
	attempt.ID = "third.test"
	if _, err := r.BeginGuestReconnect(ctx, scope, attempt, now); err != nil {
		t.Fatal(err)
	}
	if err := st.Fleet().UpsertRunner(ctx, scope.PoolID, control.Runner{ID: scope.RunnerID, Generation: 8, Connected: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ConsumeGuestReconnect(ctx, scope, identity, attempt, fresh, now); !errors.Is(err, control.ErrReconnectFenced) {
		t.Fatalf("superseded runner: %v", err)
	}
	scope.ConnectionGeneration = 8
	attempt.ID = "fourth.test"
	if _, err := r.BeginGuestReconnect(ctx, scope, attempt, now); err != nil {
		t.Fatal(err)
	}
	if epoch, err := r.ConsumeGuestReconnect(ctx, scope, identity, attempt, fresh, now); err != nil || epoch != 2 {
		t.Fatalf("next epoch: %d %v", epoch, err)
	}
	// A stale caller clock cannot stretch a five-second attempt indefinitely.
	expiredAttempt := control.GuestReconnectAttempt{ID: "old-clock.test", Challenge: "nonce.test", ExpiresAt: now.Add(-time.Minute)}
	if _, err := r.BeginGuestReconnect(ctx, scope, expiredAttempt, now.Add(-time.Minute-5*time.Second)); !errors.Is(err, control.ErrReconnectExpired) {
		t.Fatalf("stale issuance clock: %v", err)
	}
	// A cold boot invalidates enrollment, pending attempts, and old proof state.
	if err := st.Bootstraps().PutSessionBootstrap(ctx, scope.WorkspaceID, scope.SessionID, token); err != nil {
		t.Fatal(err)
	}
	if _, err := r.BeginGuestReconnect(ctx, scope, attempt, now); err == nil {
		t.Fatal("cold mint retained enrollment")
	}
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
	if epoch, token, err := service.Accept(ctx, b, challenge.AttemptID, signature); err == nil || epoch != 0 || token != "" {
		t.Fatal("replay returned authority")
	}
	if err := st.Bootstraps().ConsumeSessionBootstrap(ctx, b.WorkspaceID, b.SessionID, controlapp.HashSessionBootstrapToken(fresh), 1, clock.Now()); err != nil {
		t.Fatal(err)
	}
}

type reconnectClock struct{ now time.Time }

func (c reconnectClock) Now() time.Time { return c.now }

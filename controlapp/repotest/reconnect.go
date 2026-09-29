package repotest

import (
	"context"
	"errors"
	"github.com/tokencanopy/rainier/control"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// GuestReconnectStores extends the ordinary repository contract with optional
// guest authorization over the SAME durable state. No wire capability is implied.
type GuestReconnectStores struct {
	Stores
	Reconnects control.GuestReconnectStore
}

// RunGuestReconnect is the adapter-neutral authorization contract. The factory
// supplies an empty store for each case, with live wall-clock expiry enforcement.
// Both self-hosted and hosted implementations must pass before reconnect is enabled.
func RunGuestReconnect(t *testing.T, open func(*testing.T) GuestReconnectStores) {
	for _, tc := range []struct {
		name string
		run  func(*testing.T, GuestReconnectStores)
	}{
		{"durable single-use authorization", reconnectDurableAuthorization},
		{"disconnected and terminal authority", reconnectLifecycle},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := open(t)
			for _, ws := range []control.WorkspaceID{Alpha, Beta} {
				if err := s.Provision(context.Background(), ws); err != nil {
					t.Fatal(err)
				}
			}
			tc.run(t, s)
		})
	}
}

func reconnectDurableAuthorization(t *testing.T, st GuestReconnectStores) {
	ctx := context.Background()
	scope := control.GuestReconnectScope{WorkspaceID: Alpha, PoolID: PoolA, SessionID: "session.test", RunnerID: "runner.test", PlacementGeneration: 1, ConnectionGeneration: 7}
	_, err := st.Sessions.CreateSession(ctx, scope.WorkspaceID, control.Session{ID: scope.SessionID, CreatorID: "actor.test", PoolID: scope.PoolID, RunnerID: scope.RunnerID, State: control.StateRunning, Spec: control.PortableSpec{Image: "image.test"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Fleet.UpsertRunner(ctx, scope.PoolID, control.Runner{ID: scope.RunnerID, Generation: 7, Connected: true}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	token := control.SessionBootstrap{Hash: strings.Repeat("a", 64), PlacementGeneration: 1, ExpiresAt: now.Add(time.Minute)}
	if err := st.Bootstraps.PutSessionBootstrap(ctx, scope.WorkspaceID, scope.SessionID, token); err != nil {
		t.Fatal(err)
	}
	identity := control.GuestReconnectIdentity{BootEpoch: "boot.test", PublicKey: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}
	r := st.Reconnects
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
	fresh := control.SessionBootstrap{Hash: strings.Repeat("b", 64), PlacementGeneration: 1, ExpiresAt: now.Add(time.Minute)}
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
	if err := st.Bootstraps.ConsumeSessionBootstrap(ctx, scope.WorkspaceID, scope.SessionID, fresh.Hash, 1, now); err != nil {
		t.Fatal(err)
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
	if err := st.Fleet.UpsertRunner(ctx, scope.PoolID, control.Runner{ID: scope.RunnerID, Generation: 8, Connected: true}); err != nil {
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
	previous := attempt
	previous.ID = "pending-a.test"
	if _, err := r.BeginGuestReconnect(ctx, scope, previous, now); err != nil {
		t.Fatal(err)
	}
	attempt.ID = "pending-b.test"
	if _, err := r.BeginGuestReconnect(ctx, scope, attempt, now); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ConsumeGuestReconnect(ctx, scope, identity, previous, fresh, now); err == nil {
		t.Fatal("replaced attempt accepted")
	}
	// Beginning either attempt and rejecting A cannot alter the accepted token.
	if err := st.Bootstraps.ConsumeSessionBootstrap(ctx, scope.WorkspaceID, scope.SessionID, fresh.Hash, 1, now); err != nil {
		t.Fatal("begin/refusal changed current bootstrap", err)
	}
	if epoch, err := r.ConsumeGuestReconnect(ctx, scope, identity, attempt, fresh, now); err != nil || epoch != 3 {
		t.Fatalf("refusal changed epoch: %d %v", epoch, err)
	}
	attempt.ID = "pending-cold.test"
	if _, err := r.BeginGuestReconnect(ctx, scope, attempt, now); err != nil {
		t.Fatal(err)
	}
	// A stale caller clock cannot stretch a five-second attempt indefinitely.
	expiredAttempt := control.GuestReconnectAttempt{ID: "old-clock.test", Challenge: "nonce.test", ExpiresAt: now.Add(-time.Minute)}
	if _, err := r.BeginGuestReconnect(ctx, scope, expiredAttempt, now.Add(-time.Minute-5*time.Second)); !errors.Is(err, control.ErrReconnectExpired) {
		t.Fatalf("stale issuance clock: %v", err)
	}
	// A cold boot invalidates enrollment, pending attempts, and old proof state.
	if err := st.Bootstraps.PutSessionBootstrap(ctx, scope.WorkspaceID, scope.SessionID, token); err != nil {
		t.Fatal(err)
	}
	if _, err := r.BeginGuestReconnect(ctx, scope, attempt, now); err == nil {
		t.Fatal("cold mint retained enrollment")
	}
	if _, err := r.ConsumeGuestReconnect(ctx, scope, identity, attempt, fresh, now); err == nil {
		t.Fatal("cold mint retained pending proof")
	}
	if err := st.Bootstraps.ConsumeSessionBootstrap(ctx, scope.WorkspaceID, scope.SessionID, token.Hash, 1, now); err != nil {
		t.Fatal("rejected proof changed cold token", err)
	}
}

func reconnectLifecycle(t *testing.T, s GuestReconnectStores) {
	ctx := context.Background()
	b := control.GuestReconnectScope{WorkspaceID: Alpha, PoolID: PoolA, SessionID: "lifecycle.test", RunnerID: "runner.test", PlacementGeneration: 1, ConnectionGeneration: 7}
	if _, err := s.Sessions.CreateSession(ctx, Alpha, control.Session{ID: b.SessionID, CreatorID: "actor.test", PoolID: PoolA, RunnerID: b.RunnerID, State: control.StateRunning}); err != nil {
		t.Fatal(err)
	}
	if err := s.Fleet.UpsertRunner(ctx, PoolA, control.Runner{ID: b.RunnerID, Generation: 7, Connected: true}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	token := control.SessionBootstrap{Hash: strings.Repeat("a", 64), PlacementGeneration: 1, ExpiresAt: now.Add(time.Minute)}
	if err := s.Bootstraps.PutSessionBootstrap(ctx, Alpha, b.SessionID, token); err != nil {
		t.Fatal(err)
	}
	id := control.GuestReconnectIdentity{BootEpoch: "boot.test", PublicKey: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}
	if err := s.Reconnects.EnrollGuest(ctx, b, token.Hash, id, now); err != nil {
		t.Fatal(err)
	}
	a := control.GuestReconnectAttempt{ID: "first.test", Challenge: "nonce.test", ExpiresAt: now.Add(5 * time.Second)}
	if _, err := s.Reconnects.BeginGuestReconnect(ctx, b, a, now); err != nil {
		t.Fatal(err)
	}
	fresh := token
	fresh.Hash = strings.Repeat("b", 64)
	if epoch, err := s.Reconnects.ConsumeGuestReconnect(ctx, b, id, a, fresh, now); err != nil || epoch != 1 {
		t.Fatalf("initial accept: %d %v", epoch, err)
	}
	a.ID = "next.test"
	if _, err := s.Reconnects.BeginGuestReconnect(ctx, b, a, now); err != nil {
		t.Fatal(err)
	}
	if err := s.Fleet.SetRunnerConnected(ctx, PoolA, b.RunnerID, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Reconnects.BeginGuestReconnect(ctx, b, a, now); !errors.Is(err, control.ErrReconnectFenced) {
		t.Fatalf("disconnected begin: %v", err)
	}
	if _, err := s.Reconnects.ConsumeGuestReconnect(ctx, b, id, a, token, now); !errors.Is(err, control.ErrReconnectFenced) {
		t.Fatalf("disconnected consume: %v", err)
	}
	if err := s.Fleet.SetRunnerConnected(ctx, PoolA, b.RunnerID, true); err != nil {
		t.Fatal(err)
	}
	if epoch, err := s.Reconnects.ConsumeGuestReconnect(ctx, b, id, a, fresh, now); err != nil || epoch != 2 {
		t.Fatalf("refusal advanced epoch: %d %v", epoch, err)
	}
	a.ID = "terminal.test"
	if _, err := s.Reconnects.BeginGuestReconnect(ctx, b, a, now); err != nil {
		t.Fatal(err)
	}
	if err := s.Sessions.Transition(ctx, Alpha, b.SessionID, []control.SessionState{control.StateRunning}, control.StateDestroyed, control.TransitionOpts{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Reconnects.ConsumeGuestReconnect(ctx, b, id, a, token, now); !errors.Is(err, control.ErrReconnectFenced) {
		t.Fatalf("terminal consume: %v", err)
	}
	if err := s.Bootstraps.ConsumeSessionBootstrap(ctx, Alpha, b.SessionID, fresh.Hash, 1, now); err != nil {
		t.Fatal("refusal changed capability", err)
	}
}

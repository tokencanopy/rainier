package repotest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/control"
)

// The bootstrap-token half of the contract. It is short because the port is
// two methods, and it is here rather than in either store's own tests because
// it is the one place where "the in-memory store and the Postgres store agree"
// is checked rather than assumed — and because a hosted cell's store has to
// pass exactly the same cases.
//
// Every case creates its session first: a durable store may (and this repo's
// does) key the token to the session row, so a token minted against a session
// that does not exist is not a case the contract has an opinion about.

// bootstrapSession is the session every case below mints against, placed on a
// runner at a known generation so the fence has something to fence.
func bootstrapSession(t *testing.T, s Stores, id control.SessionID) control.Session {
	t.Helper()
	return mustCreate(t, s, Alpha, control.Session{
		ID: id, CreatorID: "act_a", State: control.StateQueued, PoolID: PoolA,
	})
}

// caseSessionBootstrap (B1) is the whole of single-use and the whole of the
// fence, in the five outcomes the design note's test plan names: one success
// and four refusals, each distinguishable from the others.
func caseSessionBootstrap(t *testing.T, s Stores) {
	ctx := context.Background()
	bootstrapSession(t, s, "sess_boot")
	bootstrapSession(t, s, "sess_other")

	now := baseTime()
	const hash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	rec := control.SessionBootstrap{Hash: hash, PlacementGeneration: 4, ExpiresAt: now.Add(120 * time.Second)}
	if err := s.Bootstraps.PutSessionBootstrap(ctx, Alpha, "sess_boot", rec); err != nil {
		t.Fatalf("put: %v", err)
	}

	// A token nobody minted — including, and especially, ANOTHER session's —
	// is unknown. The lookup is keyed by the session, so "sess_other presents
	// sess_boot's token" and "sess_boot presents a token out of thin air" are
	// deliberately the same answer: neither caller learns anything about the
	// token that does exist.
	if err := s.Bootstraps.ConsumeSessionBootstrap(ctx, Alpha, "sess_other", hash, 4, now); !errors.Is(err, control.ErrBootstrapUnknown) {
		t.Fatalf("another session's token: err = %v, want ErrBootstrapUnknown", err)
	}
	const otherHash = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	if err := s.Bootstraps.ConsumeSessionBootstrap(ctx, Alpha, "sess_boot", otherHash, 4, now); !errors.Is(err, control.ErrBootstrapUnknown) {
		t.Fatalf("a token that was never minted: err = %v, want ErrBootstrapUnknown", err)
	}

	// A placement that has moved past the one the token was minted under.
	if err := s.Bootstraps.ConsumeSessionBootstrap(ctx, Alpha, "sess_boot", hash, 5, now); !errors.Is(err, control.ErrBootstrapFenced) {
		t.Fatalf("a superseded generation: err = %v, want ErrBootstrapFenced", err)
	}

	// Expiry is checked at the boundary: at exactly ExpiresAt the token is
	// already gone, so a 120-second TTL is 120 seconds and not 121.
	if err := s.Bootstraps.ConsumeSessionBootstrap(ctx, Alpha, "sess_boot", hash, 4, rec.ExpiresAt); !errors.Is(err, control.ErrBootstrapExpired) {
		t.Fatalf("at the expiry instant: err = %v, want ErrBootstrapExpired", err)
	}

	// The one success, and then the replay.
	if err := s.Bootstraps.ConsumeSessionBootstrap(ctx, Alpha, "sess_boot", hash, 4, now); err != nil {
		t.Fatalf("a fresh token: err = %v, want success", err)
	}
	if err := s.Bootstraps.ConsumeSessionBootstrap(ctx, Alpha, "sess_boot", hash, 4, now); !errors.Is(err, control.ErrBootstrapSpent) {
		t.Fatalf("a replayed token: err = %v, want ErrBootstrapSpent", err)
	}

	// And the token is not the other workspace's to spend, whatever it holds.
	if err := s.Bootstraps.ConsumeSessionBootstrap(ctx, Beta, "sess_boot", hash, 4, now); !errors.Is(err, control.ErrBootstrapUnknown) {
		t.Fatalf("another workspace: err = %v, want ErrBootstrapUnknown", err)
	}
}

// caseSessionBootstrapRemint (B2) is what makes a cold resume safe: the token
// runnerd fetches before it boots a new VM is the only one that works, and the
// one the previous boot was handed stops being an answer — including when that
// one was never spent.
func caseSessionBootstrapRemint(t *testing.T, s Stores) {
	ctx := context.Background()
	bootstrapSession(t, s, "sess_remint")
	now := baseTime()
	const first = "1111111111111111111111111111111111111111111111111111111111111111"
	const second = "2222222222222222222222222222222222222222222222222222222222222222"

	if err := s.Bootstraps.PutSessionBootstrap(ctx, Alpha, "sess_remint", control.SessionBootstrap{
		Hash: first, PlacementGeneration: 1, ExpiresAt: now.Add(time.Minute)}); err != nil {
		t.Fatalf("first put: %v", err)
	}
	if err := s.Bootstraps.PutSessionBootstrap(ctx, Alpha, "sess_remint", control.SessionBootstrap{
		Hash: second, PlacementGeneration: 2, ExpiresAt: now.Add(time.Minute)}); err != nil {
		t.Fatalf("second put: %v", err)
	}
	if err := s.Bootstraps.ConsumeSessionBootstrap(ctx, Alpha, "sess_remint", first, 1, now); !errors.Is(err, control.ErrBootstrapUnknown) {
		t.Fatalf("the retired token: err = %v, want ErrBootstrapUnknown", err)
	}
	if err := s.Bootstraps.ConsumeSessionBootstrap(ctx, Alpha, "sess_remint", second, 2, now); err != nil {
		t.Fatalf("the current token: err = %v, want success", err)
	}

	// A re-mint after a spend is unspent again: that is the resume path, and
	// a store that kept the consumed mark would refuse every session that
	// was ever cold-parked twice.
	if err := s.Bootstraps.PutSessionBootstrap(ctx, Alpha, "sess_remint", control.SessionBootstrap{
		Hash: first, PlacementGeneration: 3, ExpiresAt: now.Add(time.Minute)}); err != nil {
		t.Fatalf("third put: %v", err)
	}
	if err := s.Bootstraps.ConsumeSessionBootstrap(ctx, Alpha, "sess_remint", first, 3, now); err != nil {
		t.Fatalf("a re-minted token: err = %v, want success", err)
	}
}

// caseSessionBootstrapEmpty (B3) pins that neither method accepts an unscoped
// call, the same rule every other port here keeps.
func caseSessionBootstrapEmpty(t *testing.T, s Stores) {
	ctx := context.Background()
	now := baseTime()
	const hash = "3333333333333333333333333333333333333333333333333333333333333333"
	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"Put with no workspace", func() error {
			return s.Bootstraps.PutSessionBootstrap(ctx, "", "sess_example", control.SessionBootstrap{Hash: hash, ExpiresAt: now})
		}},
		{"Put with no session", func() error {
			return s.Bootstraps.PutSessionBootstrap(ctx, Alpha, "", control.SessionBootstrap{Hash: hash, ExpiresAt: now})
		}},
		{"Put with no hash", func() error {
			return s.Bootstraps.PutSessionBootstrap(ctx, Alpha, "sess_example", control.SessionBootstrap{ExpiresAt: now})
		}},
		{"Consume with no workspace", func() error {
			return s.Bootstraps.ConsumeSessionBootstrap(ctx, "", "sess_example", hash, 1, now)
		}},
		{"Consume with no session", func() error {
			return s.Bootstraps.ConsumeSessionBootstrap(ctx, Alpha, "", hash, 1, now)
		}},
		{"Consume with no hash", func() error {
			return s.Bootstraps.ConsumeSessionBootstrap(ctx, Alpha, "sess_example", "", 1, now)
		}},
	} {
		if err := tc.call(); !errors.Is(err, control.ErrInvalid) {
			t.Errorf("%s: err = %v, want ErrInvalid", tc.name, err)
		}
	}
}

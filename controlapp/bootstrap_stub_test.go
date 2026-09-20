package controlapp

import (
	"context"
	"sync"
	"time"

	"github.com/tokencanopy/rainier/control"
)

// bootstrapStub is control.SessionBootstrapStore for every fixture in this
// package: it records what a mint wrote and answers a consume with the same
// four sentinels a real store does.
//
// It is a real (if small) implementation rather than a recorder that always
// says yes, because single use is a rule about the STORE — a fixture that
// accepted every token would let a change that spends one twice pass every
// test here that is not specifically about the exchange.
var _ control.SessionBootstrapStore = (*bootstrapStub)(nil)

type bootstrapStub struct {
	mu   sync.Mutex
	rows map[control.SessionID]*stubBootstrapRow
	// putErr, when set, fails every mint. It is how a test drives the one
	// path createSpec must fail closed on.
	putErr error
}

type stubBootstrapRow struct {
	hash      string
	gen       uint64
	expiresAt time.Time
	spent     bool
}

func newBootstrapStub() *bootstrapStub {
	return &bootstrapStub{rows: map[control.SessionID]*stubBootstrapRow{}}
}

func (b *bootstrapStub) PutSessionBootstrap(_ context.Context, ws control.WorkspaceID, id control.SessionID, rec control.SessionBootstrap) error {
	if ws == "" || id == "" || rec.Hash == "" {
		return control.ErrInvalid
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.putErr != nil {
		return b.putErr
	}
	b.rows[id] = &stubBootstrapRow{hash: rec.Hash, gen: rec.PlacementGeneration, expiresAt: rec.ExpiresAt}
	return nil
}

func (b *bootstrapStub) ConsumeSessionBootstrap(_ context.Context, ws control.WorkspaceID, id control.SessionID, hash string, gen uint64, now time.Time) error {
	if ws == "" || id == "" || hash == "" {
		return control.ErrInvalid
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	row, ok := b.rows[id]
	switch {
	case !ok || row.hash != hash:
		return control.ErrBootstrapUnknown
	case row.gen != gen:
		return control.ErrBootstrapFenced
	case !now.Before(row.expiresAt):
		return control.ErrBootstrapExpired
	case row.spent:
		return control.ErrBootstrapSpent
	}
	row.spent = true
	return nil
}

// minted returns the record held for id, if any.
func (b *bootstrapStub) minted(id control.SessionID) (stubBootstrapRow, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	row, ok := b.rows[id]
	if !ok {
		return stubBootstrapRow{}, false
	}
	return *row, true
}

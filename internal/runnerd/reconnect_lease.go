package runnerd

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/tokencanopy/rainier/internal/relay"
)

// guestReconnectLease retains admission and original local/control ownership
// through proof, current configuration delivery, and guarded relay publication.
// It is process-local authority only; durable proof authorization is still
// required. Never reconstruct one from a guest payload or acceptance alone.
type guestReconnectLease struct {
	server     *Server
	row        sessionEntry
	control    *reconnectControl
	generation uint64
	once       sync.Once
	closed     atomic.Bool
}

func (s *Server) acquireGuestReconnect(id string) (*guestReconnectLease, error) {
	row, ok := s.reg.snapshot(id)
	if !ok || row.placementGen == 0 || (row.state != "running" && row.state != "starting") {
		return nil, errReconnectFenced
	}
	rc := s.reconnectControl.Load()
	if rc == nil || rc.ctx.Err() != nil || rc.state.generation.Load() == 0 {
		return nil, errReconnectUnavailable
	}
	if !s.claimReconnect(id) {
		return nil, errReconnectUnavailable
	}
	lease := &guestReconnectLease{server: s, row: row, control: rc, generation: rc.state.generation.Load()}
	if !lease.valid(context.Background()) {
		lease.close()
		return nil, errReconnectFenced
	}
	return lease, nil
}

func (lease *guestReconnectLease) valid(ctx context.Context) bool {
	current, exists := lease.server.reg.snapshot(lease.row.id)
	return exists && lease.matches(ctx, current)
}

func (lease *guestReconnectLease) close() {
	lease.once.Do(func() { lease.closed.Store(true); lease.server.releaseReconnect(lease.row.id) })
}

// fence consumes the observed epoch and removes the old relay before any new
// configuration or traffic is delivered. Previously started callbacks finish
// before fencing completes; queued callbacks cannot regain authority afterwards.
func (lease *guestReconnectLease) fence(ctx context.Context, epoch uint64) error {
	s := lease.server
	s.reg.mu.Lock()
	row, ok := s.reg.items[lease.row.id]
	if !ok || !lease.matches(ctx, *row) || epoch == 0 || epoch <= row.guestEpoch {
		s.reg.mu.Unlock()
		return errReconnectFenced
	}
	old, authority := row.hub, row.relayAuthority
	row.guestEpoch = epoch
	row.hub = nil
	row.relayAuthority = nil
	s.reg.mu.Unlock()
	if old != nil {
		s.guestForwards.discard(old)
		old.Close()
	}
	if authority != nil {
		authority.fence()
	}
	if !lease.valid(ctx) {
		return errReconnectFenced
	}
	return nil
}

// guestRelayAuthority drains already-admitted callbacks and rejects queued ones.
// It owns no network or registry lock, so closing a relay cannot deadlock a
// callback that must inspect the registry before it enqueues a request.
type guestRelayAuthority struct {
	mu     sync.RWMutex
	closed bool
}

func (a *guestRelayAuthority) run(f func()) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if !a.closed {
		f()
	}
}
func (a *guestRelayAuthority) fence() {
	a.mu.Lock()
	a.closed = true
	a.mu.Unlock()
}

func (lease *guestReconnectLease) matches(ctx context.Context, current sessionEntry) bool {
	return !lease.closed.Load() && ctx.Err() == nil && lease.control.ctx.Err() == nil &&
		lease.server.reconnectControl.Load() == lease.control && lease.control.state.generation.Load() == lease.generation &&
		current.placementGen == lease.row.placementGen && current.boot == lease.row.boot && current.handle == lease.row.handle &&
		(current.hub == nil || current.hub == lease.row.hub) && (current.state == "running" || current.state == "starting")
}

func (lease *guestReconnectLease) publish(ctx context.Context, epoch uint64, hub *relay.Hub, authority *guestRelayAuthority) bool {
	s := lease.server
	s.reg.mu.Lock()
	defer s.reg.mu.Unlock()
	row, ok := s.reg.items[lease.row.id]
	if !ok || !lease.matches(ctx, *row) || row.guestEpoch != epoch || row.hub != nil {
		return false
	}
	row.hub = hub
	row.relayAuthority = authority
	return true
}

package runnerd

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// Captured on the control reader, before a command can queue behind a handoff.
type guestRPCTarget struct {
	ctx        context.Context
	row        sessionEntry
	control    *reconnectControl
	generation uint64
}

func (s *Server) guestRPCTarget(ctx context.Context, id string, ag *agentSessionState) *guestRPCTarget {
	row, ok := s.reg.snapshot(id)
	if !ok || !row.guestReconnect {
		return nil
	}
	rc := s.reconnectControl.Load()
	target := &guestRPCTarget{ctx: ctx, row: row, control: rc}
	if rc != nil && (ag == nil || rc.state == ag) {
		target.generation = rc.state.generation.Load()
	}
	return target
}
func (s *Server) sendGuestRPC(target *guestRPCTarget, env runner.RPCEnvelope) error {
	if target == nil || target.control == nil || target.generation == 0 || target.row.hub == nil {
		return errReconnectFenced
	}
	row, ok := s.reg.snapshot(target.row.id)
	if !ok || target.ctx.Err() != nil || target.control.ctx.Err() != nil || s.reconnectControl.Load() != target.control || target.control.state.generation.Load() != target.generation || row.hub != target.row.hub || row.boot != target.row.boot || row.handle != target.row.handle || row.placementGen != target.row.placementGen {
		return errReconnectFenced
	}
	event := relay.ControlEvent{Kind: "req:" + env.Method, ID: env.ID, Payload: env.Payload}
	if env.Method == "resp" {
		event.Kind = "resp"
		event.OK = env.OK
	}
	body, err := json.Marshal(event)
	if err != nil {
		return errReconnectInvalid
	}
	// Always the captured hub. A concurrent fence closes this hub; it cannot
	// redirect this write to a newly published guest connection.
	return target.row.hub.SendControl(body)
}

// Guest muxes may restart their low ID sequence at each reconnect. Translate
// requests to a runner-lifetime sequence so an old reply cannot alias a new
// guest request, even when both guests used ID 1.
type guestRPCPending struct {
	target  guestRPCTarget
	guestID uint64
	expires time.Time
}
type guestRPCForwards struct {
	mu      sync.Mutex
	seq     uint64
	pending map[uint64]guestRPCPending
}

func (f *guestRPCForwards) begin(target guestRPCTarget, id uint64) (uint64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now()
	for key, entry := range f.pending {
		if !now.Before(entry.expires) || entry.target.control.ctx.Err() != nil {
			delete(f.pending, key)
		}
	}
	if id == 0 || isRunnerOriginated(id) || len(f.pending) >= 1024 || f.seq >= runnerOriginatedIDBase-1 {
		return 0, false
	}
	if f.pending == nil {
		f.pending = make(map[uint64]guestRPCPending)
	}
	f.seq++
	f.pending[f.seq] = guestRPCPending{target: target, guestID: id, expires: now.Add(2 * time.Minute)}
	return f.seq, true
}
func (f *guestRPCForwards) take(id uint64, session string) (guestRPCPending, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	entry, ok := f.pending[id]
	if !ok || entry.target.row.id != session {
		return guestRPCPending{}, false
	}
	delete(f.pending, id)
	return entry, time.Now().Before(entry.expires)
}
func (f *guestRPCForwards) discard(hub *relay.Hub) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for id, entry := range f.pending {
		if entry.target.row.hub == hub {
			delete(f.pending, id)
		}
	}
}

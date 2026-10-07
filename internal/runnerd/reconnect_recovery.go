package runnerd

import (
	"context"
	"time"

	"github.com/tokencanopy/rainier/protocol/runner"
)

func (s *Server) authorizeRecoveredGuest(ctx context.Context, rc *reconnectControl, row sessionEntry) error {
	if rc == nil || !row.recovered || !row.guestReconnect || row.handle == "" || row.state != "running" || row.hub != nil {
		return errReconnectFenced
	}
	generation := rc.state.generation.Load()
	if generation == 0 || rc.ctx.Err() != nil || s.reconnectControl.Load() != rc {
		return errReconnectFenced
	}
	ctx, cancel := context.WithTimeout(ctx, reconnectBudget)
	defer cancel()
	stop := context.AfterFunc(rc.ctx, cancel)
	defer stop()
	payload, err := s.reconnectCall(ctx, rc, row.id, runner.MethodGuestReconnectConfiguration, runner.GuestReconnectBeginRequest{Protocol: 1})
	if err != nil {
		return err
	}
	fresh, err := runner.DecodeGuestReconnectConfiguration(payload)
	if err != nil || fresh.SessionID != row.id || row.placementGen != 0 && fresh.PlacementGeneration != row.placementGen {
		return errReconnectFenced
	}
	s.reg.mu.Lock()
	defer s.reg.mu.Unlock()
	current, ok := s.reg.items[row.id]
	if !ok || ctx.Err() != nil || rc.ctx.Err() != nil || s.reconnectControl.Load() != rc || rc.state.generation.Load() != generation || current.boot != row.boot || current.handle != row.handle || current.placementGen != row.placementGen || !current.recovered || !current.guestReconnect || current.state != "running" || current.hub != nil {
		return errReconnectFenced
	}
	current.placementGen = fresh.PlacementGeneration
	return nil
}

// One sequential worker per accepted connection bounds recovery independently
// of guest traffic. Failed candidates retry under the same current authority;
// cancellation discards that worker and the replacement registration starts anew.
func (s *Server) recoverGuests(rc *reconnectControl) {
	drv, ok := s.drv.(interface {
		RecoverGuest(context.Context, string, string) error
	})
	if !ok {
		return
	}
	recovered := map[string]string{}
	for rc.ctx.Err() == nil && s.reconnectControl.Load() == rc {
		s.reg.mu.Lock()
		var rows []sessionEntry
		for _, row := range s.reg.items {
			if row.recovered && row.guestReconnect && row.state == "running" && row.hub == nil && recovered[row.id] != row.handle {
				rows = append(rows, *row)
			}
		}
		s.reg.mu.Unlock()
		for _, row := range rows {
			if rc.ctx.Err() != nil || s.reconnectControl.Load() != rc {
				return
			}
			ctx, cancel := context.WithTimeout(rc.ctx, 2*reconnectBudget)
			err := s.authorizeRecoveredGuest(ctx, rc, row)
			if err == nil {
				err = drv.RecoverGuest(ctx, row.handle, row.id)
			}
			cancel()
			if err == nil {
				recovered[row.id] = row.handle
			}
		}
		timer := time.NewTimer(5 * time.Second)
		select {
		case <-rc.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

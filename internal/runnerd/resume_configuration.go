package runnerd

import (
	"context"

	"github.com/tokencanopy/rainier/internal/driver"
	"github.com/tokencanopy/rainier/protocol/runner"
)

var _ driver.GuestResumeConfigurationHost = (*Server)(nil)

// GuestResumeConfiguration binds fresh in-memory configuration to an already
// claimed cold resume. It neither mints bootstrap authority nor changes the
// recorded image, workspace, or guest-home devices.
func (s *Server) GuestResumeConfiguration(ctx context.Context, id string, placement uint64) (runner.BootConfig, error) {
	var zero runner.BootConfig
	row, ok := s.reg.snapshot(id)
	rc := s.reconnectControl.Load()
	if !ok || placement == 0 || row.placementGen != placement || !row.resumePending || row.state != "resuming" || !row.guestReconnect || row.handle == "" || rc == nil {
		return zero, errReconnectFenced
	}
	generation := rc.state.generation.Load()
	if generation == 0 || rc.ctx.Err() != nil {
		return zero, errReconnectFenced
	}
	ctx, cancel := context.WithTimeout(ctx, reconnectBudget)
	defer cancel()
	stop := context.AfterFunc(rc.ctx, cancel)
	defer stop()
	payload, err := s.reconnectCall(ctx, rc, id, runner.MethodGuestReconnectConfiguration, runner.GuestReconnectBeginRequest{Protocol: runner.GuestReconnectProtocol})
	if err != nil {
		return zero, errReconnectUnavailable
	}
	fresh, err := runner.DecodeGuestReconnectConfiguration(payload)
	if err != nil || fresh.SessionID != id || fresh.PlacementGeneration != placement {
		return zero, errReconnectFenced
	}
	s.reg.mu.Lock()
	defer s.reg.mu.Unlock()
	current, ok := s.reg.items[id]
	if !ok || ctx.Err() != nil || rc.ctx.Err() != nil || s.reconnectControl.Load() != rc || rc.state.generation.Load() != generation || current.boot != row.boot || current.handle != row.handle || current.placementGen != placement || !current.resumePending || current.state != "resuming" || !current.guestReconnect {
		return zero, errReconnectFenced
	}
	spec := guestDriverSpec(*fresh.Spec)
	spec.SessionID = id
	spec.ProxyURL = s.proxyURL
	spec.GuestReconnect = runner.GuestReconnectProtocol
	return driver.GuestBootConfig(spec), nil
}

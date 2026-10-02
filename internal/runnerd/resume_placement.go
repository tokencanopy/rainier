package runnerd

import (
	"context"
	"errors"
	"time"

	"github.com/tokencanopy/rainier/internal/driver"
	"github.com/tokencanopy/rainier/protocol/runner"
)

var errResumePlacement = errors.New("resume placement is not current")

// claimResumePlacement opens the local boot before driver launch can expose a
// fresh guest. Only the authenticated control command supplies generation.
func (s *Server) claimResumePlacement(id string, generation uint64) error {
	s.reg.mu.Lock()
	row, ok := s.reg.items[id]
	if !ok || generation == 0 || generation <= row.placementGen || row.resumePending || row.state == "running" || row.state == "starting" || row.state == "destroying" || row.stopsInFlight > 0 {
		s.reg.mu.Unlock()
		return errResumePlacement
	}
	old, authority := row.hub, row.relayAuthority
	row.hub, row.relayAuthority = nil, nil
	row.guestEpoch = 0
	row.placementGen = generation
	row.resumePending = true
	row.state = "resuming"
	s.reg.nextBoot++
	row.boot = s.reg.nextBoot
	s.reg.mu.Unlock()
	if old != nil {
		s.guestForwards.discard(old)
		old.Close()
	}
	if authority != nil {
		authority.fence()
	}
	return nil
}

func (s *Server) failResumePlacement(ctx context.Context, id, handle string, generation uint64) {
	// Only observed suspension proves a failed boot can be retried. Inspection
	// errors or a live VM retain an unresolved state and all resource ownership.
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	observed, err := s.drv.Inspect(ctx, handle)
	s.reg.mu.Lock()
	defer s.reg.mu.Unlock()
	row, ok := s.reg.items[id]
	if !ok || row.handle != handle || row.placementGen != generation {
		return
	}
	row.resumePending = false
	if err == nil && observed.State == driver.StateSuspended {
		row.state = "suspended"
	}
}

func (s *Server) resumeStatus(ctx context.Context, m runner.ToRunner) runner.FromRunner {
	refused := runner.FromRunner{Type: "result", ReqID: m.ReqID}
	row, ok := s.reg.snapshot(m.Session)
	if !ok || ctx.Err() != nil || m.PlacementGeneration == 0 || m.PlacementGeneration < row.placementGen {
		return refused
	}
	state := "resuming"
	generation := row.placementGen
	if !row.resumePending {
		if drv, ok := s.drv.(interface {
			ResumeStatus(context.Context, string, uint64) (string, uint64, error)
		}); ok {
			var err error
			state, generation, err = drv.ResumeStatus(ctx, row.handle, m.PlacementGeneration)
			if err != nil {
				return refused
			}
		} else if generation < m.PlacementGeneration && row.state == "suspended" && row.hub == nil && row.stopsInFlight == 0 {
			// Drivers without durable placement metadata can cancel a command
			// only while the existing sandbox is observably stopped. The
			// registry comparison below serializes this fence with any launch.
			observed, err := s.drv.Inspect(ctx, row.handle)
			if err != nil || observed.State != driver.StateSuspended {
				return refused
			}
			state, generation = "suspended_cold", m.PlacementGeneration
		} else if generation == m.PlacementGeneration {
			if row.state == "running" {
				state = "running"
			} else if row.state == "suspended" && row.hub == nil {
				state = "suspended_cold"
			}
		}
	}
	if generation != m.PlacementGeneration || ctx.Err() != nil {
		return refused
	}
	s.reg.mu.Lock()
	defer s.reg.mu.Unlock()
	current, ok := s.reg.items[m.Session]
	if !ok || current.boot != row.boot || current.handle != row.handle || current.placementGen != row.placementGen || current.resumePending != row.resumePending || current.state != row.state || current.stopsInFlight != 0 {
		return refused
	}
	if generation > current.placementGen {
		if state != "suspended_cold" {
			return refused
		}
		current.placementGen = generation // driver's durable cancellation tombstone
	}
	if state == "running" && current.guestReconnect && current.hub == nil {
		state = "resuming"
	}
	return runner.FromRunner{Type: "result", ReqID: m.ReqID, OK: true, State: state, PlacementGeneration: generation}
}

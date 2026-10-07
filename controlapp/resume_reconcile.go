package controlapp

import (
	"context"
	"errors"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// Reuse the scheduler safety pass to settle lost resume replies. One bounded,
// sequential pass cannot launch VMs or create another placement.
func (s *FleetService) reconcileResumes(parent context.Context, pool control.PoolID) {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	hosts, err := s.fleet.ListRunners(ctx, pool)
	if err != nil {
		return
	}
	remaining := 64
	for _, host := range hosts {
		if !host.Connected {
			continue
		}
		rows, err := s.fleet.SessionsOnRunner(ctx, pool, host.ID, []control.SessionState{control.StateResuming})
		if err != nil {
			return
		}
		for _, row := range rows {
			// Let the normal dispatch round trip finish before attempting to
			// cancel an unreceived command. A live launch still reports pending.
			if !row.UpdatedAt.IsZero() && s.clock.Now().Sub(row.UpdatedAt) < 2*time.Minute {
				continue
			}
			if remaining == 0 || ctx.Err() != nil {
				return
			}
			remaining--
			response, err := s.transport.Dispatch(ctx, pool, host.ID, runner.ToRunner{Type: "resume_status", Session: string(row.ID), PlacementGeneration: row.PlacementGeneration})
			if err != nil || !response.OK || response.PlacementGeneration != row.PlacementGeneration || row.PlacementGeneration == 0 {
				continue
			}
			target := control.SessionState(response.State)
			if target != control.StateRunning && target != control.StateSuspendedCold {
				continue
			}
			_ = s.uow.Run(ctx, func(ctx context.Context) error {
				err := s.sessions.Transition(ctx, row.WorkspaceID, row.ID, []control.SessionState{control.StateResuming}, target, control.TransitionOpts{ExpectedPlacementGeneration: &row.PlacementGeneration})
				if errors.Is(err, control.ErrConflict) || errors.Is(err, control.ErrNotFound) {
					return nil
				}
				if err != nil {
					return err
				}
				eventID := s.ids.NewEventID()
				if eventID == "" {
					return control.ErrUnavailable
				}
				return s.recordLifecycleEvent(ctx, control.RunnerEvent{WorkspaceID: row.WorkspaceID, RunnerID: host.ID, SessionID: row.ID}, row, eventID)
			})
		}
	}
}

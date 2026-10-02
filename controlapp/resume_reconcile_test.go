package controlapp

import (
	"context"
	"testing"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
)

func TestSchedulerReconcilesOnlyExactResumePlacement(t *testing.T) {
	for _, tc := range []struct {
		name, state string
		generation  uint64
		want        control.SessionState
	}{
		{"completed", "running", 2, control.StateRunning},
		{"failed_before_launch", "suspended_cold", 2, control.StateSuspendedCold},
		{"old_reply", "running", 1, control.StateResuming},
		{"unversioned", "running", 0, control.StateResuming},
		{"still_launching", "resuming", 2, control.StateResuming},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFleetFixture(t)
			fx.st.seedRunner(fleetReconcileRunnerRow())
			fx.st.seedSession(control.Session{ID: "sess_example", WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example", State: control.StateResuming, PlacementGeneration: 2})
			fx.transport.dispatchReplies = []runner.FromRunner{{OK: true, State: tc.state, PlacementGeneration: tc.generation}}
			fx.service.drainPool(context.Background(), "pool_example")
			got := fleetGetSessionState(t, fx, "ws_example", "sess_example")
			if got.State != tc.want || got.PlacementGeneration != 2 {
				t.Fatalf("state=%s generation=%d", got.State, got.PlacementGeneration)
			}
			if len(fx.transport.dispatched) != 1 || fx.transport.dispatched[0].Type != "resume_status" || fx.transport.dispatched[0].PlacementGeneration != 2 {
				t.Fatal("pending claim was not reconciled against exact runner placement")
			}
		})
	}
}

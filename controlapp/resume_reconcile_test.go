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

func TestResumingAcceptsOnlyCurrentFailureEvidence(t *testing.T) {
	for _, target := range []control.SessionState{control.StateFailed, control.StateDead} {
		for _, generation := range []uint64{0, 1, 2} {
			fx := newFleetFixture(t)
			fx.st.seedRunner(fleetEventRunner(1))
			fx.st.seedSession(control.Session{ID: "sess_example", WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example", State: control.StateResuming, PlacementGeneration: 2})
			err := fx.service.ApplyRunnerEvent(context.Background(), control.RunnerEvent{WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example", SessionID: "sess_example", Generation: 1, PlacementGeneration: generation, State: target})
			row := fleetGetSessionState(t, fx, "ws_example", "sess_example")
			if generation == 2 {
				if err != nil || row.State != target {
					t.Fatalf("current %s lost: %v state=%s", target, err, row.State)
				}
			} else if err == nil || row.State != control.StateResuming {
				t.Fatal("stale failure changed pending boot")
			}
		}
	}
}

func TestPendingResumeCapacityCountsEachPlacementOnce(t *testing.T) {
	for _, tc := range []struct {
		name     string
		observed uint64
		want     int
	}{{"same", 2, 1}, {"old", 1, 0}, {"absent", 0, 0}} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFleetFixture(t)
			host := fleetReconcileRunnerRow()
			host.CapacityTotal = 2
			host.CapacityUsed = 1
			if tc.observed != 0 {
				host.CapacityPlacements = map[control.SessionID]uint64{"sess_example": tc.observed}
			}
			fx.st.seedRunner(host)
			fx.st.seedSession(control.Session{ID: "sess_example", WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example", State: control.StateResuming, PlacementGeneration: 2})
			views, err := fx.service.freeCapacity(context.Background(), "pool_example")
			if err != nil || len(views) != 1 || views[0].free != tc.want {
				t.Fatalf("capacity=%+v err=%v want free=%d", views, err, tc.want)
			}
		})
	}
}

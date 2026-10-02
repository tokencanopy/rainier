package controlapp

import (
	"context"
	"errors"
	"testing"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
)

func TestColdResumeClaimsPlacementBeforeDispatch(t *testing.T) {
	f := newSessionFixtureFull(t)
	f.fleet.runners = []control.Runner{{ID: "runner_a", PoolID: "pool_a", CapacityTotal: 4, Connected: true}}
	f.repo.put(sessionInState(control.StateSuspendedCold))
	f.transport.onDispatch = func(m runner.ToRunner) {
		row, err := f.repo.GetSession(context.Background(), "ws_example", "sess_example")
		if err != nil || row.State != control.SessionState("resuming") || row.PlacementGeneration != 2 || m.PlacementGeneration != 2 {
			t.Fatal("cold boot dispatched before its new placement was claimed")
		}
		_, err = f.svc.ResumeSession(context.Background(), sessionTestScope(), control.ResumeSession{ID: row.ID})
		if !errors.Is(err, control.ErrConflict) {
			t.Fatal("duplicate resume opened another claim")
		}
	}
	got, err := f.svc.ResumeSession(context.Background(), sessionTestScope(), control.ResumeSession{ID: "sess_example"})
	if err != nil || got.State != control.StateRunning || got.PlacementGeneration != 2 {
		t.Fatalf("resume outcome state=%s generation=%d err=%v", got.State, got.PlacementGeneration, err)
	}
}

func TestColdResumeTimeoutRetainsClaim(t *testing.T) {
	f := newSessionFixtureFull(t)
	f.fleet.runners = []control.Runner{{ID: "runner_a", PoolID: "pool_a", CapacityTotal: 4, Connected: true}}
	f.repo.put(sessionInState(control.StateSuspendedCold))
	f.transport.err = context.DeadlineExceeded
	_, err := f.svc.ResumeSession(context.Background(), sessionTestScope(), control.ResumeSession{ID: "sess_example"})
	if !errors.Is(err, control.ErrUnavailable) {
		t.Fatalf("error=%v", err)
	}
	row, _ := f.repo.GetSession(context.Background(), "ws_example", "sess_example")
	if row.State != control.SessionState("resuming") || row.PlacementGeneration != 2 {
		t.Fatal("uncertain launch lost its claimed placement")
	}
}

func TestColdResumeCompletionCannotChangeNewerPlacement(t *testing.T) {
	f := newSessionFixtureFull(t)
	f.fleet.runners = []control.Runner{{ID: "runner_a", PoolID: "pool_a", CapacityTotal: 4, Connected: true}}
	f.repo.put(sessionInState(control.StateSuspendedCold))
	f.transport.onDispatch = func(runner.ToRunner) {
		row := f.repo.rows["sess_example"]
		row.State = control.SessionState("resuming")
		row.PlacementGeneration = 3
		f.repo.put(row)
	}
	got, err := f.svc.ResumeSession(context.Background(), sessionTestScope(), control.ResumeSession{ID: "sess_example"})
	if err != nil || got.State != control.SessionState("resuming") || got.PlacementGeneration != 3 {
		t.Fatal("late completion changed newer placement")
	}
	if len(f.events.events) != 0 {
		t.Fatal("late completion recorded a resume event")
	}
}

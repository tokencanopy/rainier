package controlapp

import (
	"errors"
	"testing"

	"github.com/tokencanopy/rainier/control"
)

func TestPoolReconcileUsesEachAssignedWorkspace(t *testing.T) {
	fx := newFleetFixture(t)
	fx.st.seedRunner(fleetReconcileRunnerRow())
	for _, ws := range []control.WorkspaceID{"ws_alpha", "ws_beta"} {
		fx.st.seedSession(control.Session{ID: control.SessionID("sess_" + string(ws)), WorkspaceID: ws, PoolID: "pool_example", RunnerID: "runner_example", State: control.StateRunning})
	}
	result, err := fx.service.ReconcilePoolRunner(fleetCtx, control.RunnerSnapshot{PoolID: "pool_example", RunnerID: "runner_example", Generation: 1, CapacityTotal: 4, Sessions: []control.RunnerSession{{SessionID: "sess_ws_alpha", State: control.StateRunning}, {SessionID: "sess_unknown", State: control.StateRunning}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Destroy) != 1 || result.Destroy[0] != "sess_unknown" {
		t.Fatalf("destroy = %v", result.Destroy)
	}
	if got := fleetGetSessionState(t, fx, "ws_alpha", "sess_ws_alpha").State; got != control.StateRunning {
		t.Fatalf("present state = %s", got)
	}
	if got := fleetGetSessionState(t, fx, "ws_beta", "sess_ws_beta").State; got != control.StateDead {
		t.Fatalf("missing state = %s", got)
	}
}

func TestPoolReconcileRefusesAmbiguousIDsBeforeSessionMutation(t *testing.T) {
	fx := newFleetFixture(t)
	fx.st.seedRunner(fleetReconcileRunnerRow())
	for _, ws := range []control.WorkspaceID{"ws_alpha", "ws_beta"} {
		fx.st.seedSession(control.Session{ID: "sess_collision", WorkspaceID: ws, PoolID: "pool_example", RunnerID: "runner_example", State: control.StateRunning})
	}
	_, err := fx.service.ReconcilePoolRunner(fleetCtx, control.RunnerSnapshot{PoolID: "pool_example", RunnerID: "runner_example", Generation: 1, CapacityTotal: 4})
	if !errors.Is(err, control.ErrConflict) {
		t.Fatalf("ambiguous inventory error = %v", err)
	}
	if fx.st.transitionCalls != 0 {
		t.Fatal("ambiguous inventory mutated sessions")
	}
}

func TestPoolReconcileRequiresExplicitPoolScopeAndKeepsFence(t *testing.T) {
	fx := newFleetFixture(t)
	fx.st.seedRunner(fleetReconcileRunnerRow())
	snap := control.RunnerSnapshot{WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example", Generation: 1, CapacityTotal: 4}
	if _, err := fx.service.ReconcilePoolRunner(fleetCtx, snap); !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("workspace scope accepted: %v", err)
	}
	snap.WorkspaceID = ""
	if _, err := fx.service.ReconcileRunner(fleetCtx, snap); !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("legacy empty scope accepted: %v", err)
	}
	row := fleetReconcileRunnerRow()
	row.Generation = 2
	fx.st.seedRunner(row)
	result, err := fx.service.ReconcilePoolRunner(fleetCtx, snap)
	if err != nil || !result.Fenced || result.Generation != 2 {
		t.Fatalf("stale pool snapshot = %+v, %v", result, err)
	}
}

func TestPoolReconcileDoesNotAdoptClaimsOutsideAssignedInventory(t *testing.T) {
	for _, tc := range []struct {
		name   string
		pool   control.PoolID
		runner control.RunnerID
		state  control.SessionState
	}{
		{"unassigned", "pool_example", "", control.StateQueued},
		{"other runner", "pool_example", "runner_other", control.StateRunning},
		{"other pool", "pool_other", "runner_example", control.StateRunning},
		{"terminal", "pool_example", "runner_example", control.StateDead},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFleetFixture(t)
			fx.st.seedRunner(fleetReconcileRunnerRow())
			fx.st.seedSession(control.Session{ID: "sess_claim", WorkspaceID: "ws_beta", PoolID: tc.pool, RunnerID: tc.runner, State: tc.state})
			got, err := fx.service.ReconcilePoolRunner(fleetCtx, control.RunnerSnapshot{PoolID: "pool_example", RunnerID: "runner_example", Generation: 1, CapacityTotal: 4, Sessions: []control.RunnerSession{{SessionID: "sess_claim", State: control.StateRunning}}})
			if err != nil {
				t.Fatal(err)
			}
			if len(got.Destroy) != 1 || got.Destroy[0] != "sess_claim" {
				t.Fatalf("destroy = %v", got.Destroy)
			}
			row := fleetGetSessionState(t, fx, "ws_beta", "sess_claim")
			if row.State != tc.state || row.PoolID != tc.pool || row.RunnerID != tc.runner {
				t.Fatalf("claim changed authoritative placement: %+v", row)
			}
		})
	}
}

func TestPoolReconcileRequeuesMissingCreateInEachWorkspace(t *testing.T) {
	fx := newFleetFixture(t)
	fx.st.seedRunner(fleetReconcileRunnerRow())
	for _, ws := range []control.WorkspaceID{"ws_alpha", "ws_beta"} {
		fx.st.seedSession(control.Session{ID: control.SessionID("sess_" + string(ws)), WorkspaceID: ws, PoolID: "pool_example", RunnerID: "runner_example", State: control.StateCreating})
	}
	if _, err := fx.service.ReconcilePoolRunner(fleetCtx, control.RunnerSnapshot{PoolID: "pool_example", RunnerID: "runner_example", Generation: 1, CapacityTotal: 4}); err != nil {
		t.Fatal(err)
	}
	for _, ws := range []control.WorkspaceID{"ws_alpha", "ws_beta"} {
		row := fleetGetSessionState(t, fx, ws, control.SessionID("sess_"+string(ws)))
		if row.State != control.StateQueued || row.RunnerID != "" {
			t.Fatalf("missing create not requeued: %+v", row)
		}
	}
}

func TestPoolRegistrationRequiresExplicitScope(t *testing.T) {
	fx := newFleetFixture(t)
	r := control.RunnerRegistration{PoolID: "pool_example", RunnerID: "runner_example", Generation: 1, CapacityTotal: 4}
	if _, err := fx.service.RegisterRunner(fleetCtx, r); !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("legacy blank workspace accepted: %v", err)
	}
	got, err := fx.service.RegisterPoolRunner(fleetCtx, r)
	if err != nil || !got.Accepted {
		t.Fatalf("shared registration: %+v %v", got, err)
	}
	r.WorkspaceID = "ws_alpha"
	if _, err := fx.service.RegisterPoolRunner(fleetCtx, r); !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("workspace in pool registration accepted: %v", err)
	}
}

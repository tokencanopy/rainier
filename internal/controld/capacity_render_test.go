package controld

import (
	"context"
	"github.com/tokencanopy/rainier/control"
	"testing"
)

func TestRenderedCapacityUsesExactPendingPlacements(t *testing.T) {
	for _, tc := range []struct {
		name        string
		state       control.SessionState
		used, total int
		observed    uint64
		want        int
	}{
		{"unobserved_resume", control.StateResuming, 0, 1, 0, 0},
		{"counted_resume", control.StateResuming, 1, 2, 1, 1},
		{"stale_resume", control.StateResuming, 1, 2, 2, 0},
		{"legacy_resume", control.StateResuming, 1, 2, 0, 0},
		{"counted_create", control.StateCreating, 1, 2, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, st, _ := newTestControld(t)
			ctx := context.Background()
			row, err := st.Sessions().CreateSession(ctx, installWorkspace, control.Session{ID: "capacity_test", PoolID: installPool, RunnerID: "runner_test", State: tc.state})
			if err != nil {
				t.Fatal(err)
			}
			placements := map[control.SessionID]uint64{}
			if tc.observed > 0 {
				placements[row.ID] = tc.observed
			}
			if err := st.Fleet().UpsertRunner(ctx, installPool, control.Runner{ID: row.RunnerID, Connected: true, Generation: 1, CapacityUsed: tc.used, CapacityTotal: tc.total, CapacityPlacements: placements}); err != nil {
				t.Fatal(err)
			}
			r := sessionRenderer{srv: srv, ctx: ctx}
			if got := r.freeSlots()[string(row.RunnerID)]; got != tc.want {
				t.Fatalf("free=%d, want %d", got, tc.want)
			}
		})
	}
}

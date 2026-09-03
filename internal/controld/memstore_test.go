package controld_test

import (
	"context"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/controlapp/repotest"
	"github.com/tokencanopy/rainier/internal/controld"
	"github.com/tokencanopy/rainier/internal/controld/storetest"
)

// TestMemStoreRepositories runs the public repository contract suite against
// the in-memory store's three native ports. They share one backing store, so
// a session created through Sessions is visible to Fleet.SessionsOnRunner —
// which is exactly what the suite checks a host for.
func TestMemStoreRepositories(t *testing.T) {
	repotest.Run(t, func(t *testing.T) repotest.Stores {
		st := controld.NewMemStore()
		return repotest.Stores{
			Sessions:     st.Sessions(),
			Environments: st.Environments(),
			Fleet:        st.Fleet(),
			Provision:    st.EnsureWorkspace,
		}
	})
}

// TestMemStoreHost runs the host-persistence suite: identity, the vault, and
// the four lookups the control ports deliberately have no method for.
func TestMemStoreHost(t *testing.T) {
	storetest.RunHost(t, func(t *testing.T) controld.HostStore { return controld.NewMemStore() })
}

// The in-memory store keeps every event it is handed, in the order it was
// handed them, so a test above it can read back what a command recorded. It
// is the memstore's whole reason for holding them: nothing in the process
// consumes an event, and this store is the durable home of nothing.
func TestMemStoreEventsReadBackInOrder(t *testing.T) {
	ctx := context.Background()
	st := controld.NewMemStore()
	for _, ws := range []control.WorkspaceID{"ws_alpha", "ws_beta"} {
		if err := st.EnsureWorkspace(ctx, ws); err != nil {
			t.Fatal(err)
		}
	}
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	ev := func(id string, ws control.WorkspaceID) control.Event {
		return control.Event{
			ID: control.EventID(id), WorkspaceID: ws, ActorID: "act_a", Action: control.ActionCreate,
			Resource: control.Resource{Kind: control.ResourceSession, WorkspaceID: ws, ID: "sess_example", CreatorID: "act_a"},
			At:       at,
		}
	}
	for _, e := range []control.Event{ev("evt_example", "ws_alpha"), ev("evt_second", "ws_alpha"), ev("evt_beta", "ws_beta")} {
		if err := st.Record(ctx, e); err != nil {
			t.Fatalf("Record(%s): %v", e.ID, err)
		}
	}

	got := st.Events()
	if len(got) != 3 {
		t.Fatalf("Events() = %d rows, want 3", len(got))
	}
	for i, want := range []struct {
		id control.EventID
		ws control.WorkspaceID
	}{{"evt_example", "ws_alpha"}, {"evt_second", "ws_alpha"}, {"evt_beta", "ws_beta"}} {
		if got[i].ID != want.id || got[i].WorkspaceID != want.ws {
			t.Fatalf("event %d = %s in %s, want %s in %s", i, got[i].ID, got[i].WorkspaceID, want.id, want.ws)
		}
	}

	// The slice is the caller's own: writing through it must not reach back
	// into the store.
	got[0].ID = "evt_rewritten"
	if again := st.Events(); again[0].ID != "evt_example" {
		t.Fatalf("Events() handed out the store's own rows: %s", again[0].ID)
	}
}

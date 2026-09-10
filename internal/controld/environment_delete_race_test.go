// internal/controld/environment_delete_race_test.go
package controld

import (
	"context"
	"net/http"
	"testing"

	"github.com/tokencanopy/rainier/control"
)

// raceCreateSessionStore wraps MemStore to seed a session against an
// environment the moment its own delete call runs, the latest point a
// concurrent CreateSession could still land.
type raceCreateSessionStore struct {
	MemStore
	envID     control.EnvironmentID
	triggered bool
}

func (r *raceCreateSessionStore) Environments() control.EnvironmentRepository {
	return raceCreateSessionEnvironments{EnvironmentRepository: r.MemStore.Environments(), owner: r}
}

type raceCreateSessionEnvironments struct {
	control.EnvironmentRepository
	owner *raceCreateSessionStore
}

func (r raceCreateSessionEnvironments) seedRacer(ctx context.Context, ws control.WorkspaceID, id control.EnvironmentID) {
	if o := r.owner; !o.triggered && id == o.envID {
		o.triggered = true
		if _, err := o.MemStore.Sessions().CreateSession(ctx, ws, control.Session{
			ID: "sess_racer", CreatorID: "usr_test", State: control.StateQueued,
			PoolID: installPool, EnvironmentID: id,
		}); err != nil {
			panic("raceCreateSessionStore: seeding the racer session: " + err.Error())
		}
	}
}

func (r raceCreateSessionEnvironments) DeleteEnvironment(ctx context.Context, ws control.WorkspaceID, id control.EnvironmentID) error {
	r.seedRacer(ctx, ws, id)
	return r.EnvironmentRepository.DeleteEnvironment(ctx, ws, id)
}

func (r raceCreateSessionEnvironments) DeleteEnvironmentUnlessReferenced(ctx context.Context, ws control.WorkspaceID, id control.EnvironmentID, states []control.SessionState) error {
	r.seedRacer(ctx, ws, id)
	return r.EnvironmentRepository.DeleteEnvironmentUnlessReferenced(ctx, ws, id, states)
}

// TestDeleteEnvironmentSeesASessionCreatedRightBeforeItsOwnDelete pins a
// session created immediately before the delete's own repository call:
// the guard must still see it, whichever delete path is live.
func TestDeleteEnvironmentSeesASessionCreatedRightBeforeItsOwnDelete(t *testing.T) {
	race := &raceCreateSessionStore{MemStore: NewMemStore()}
	_, ts := newTestControldOver(t, race)
	_, adminTok := loginUser(t, race, "root", "admin")
	created := createEnv(t, ts, adminTok, envCreateBody("dev", nil))
	race.envID = control.EnvironmentID(created.ID)

	resp := doRequest(t, ts, http.MethodDelete, "/v0/environments/"+created.ID, adminTok, nil, nil)
	raw := readBody(t, resp)
	if !race.triggered {
		t.Fatalf("the racer session was never seeded; this test proves nothing")
	}
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409 (sess_racer was created immediately before the delete); body=%s", resp.StatusCode, raw)
	}
	if _, err := race.Environments().GetEnvironment(context.Background(), installWorkspace,
		control.EnvironmentID(created.ID)); err != nil {
		t.Errorf("environment removed despite the racer session: %v", err)
	}
}

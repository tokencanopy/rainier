package controld

import (
	"context"
	"log"
	"slices"
	"sort"
	"time"

	"github.com/tokencanopy/rainier/control"
)

// The host ports that are not persistence: which pools a session may run in,
// where an application event goes, what time it is, and where new identities
// come from. Each is the smallest thing that satisfies its contract for a
// single-tenant installation.

// installationPools is control.PoolResolver for an installation whose whole
// fleet is one pool.
type installationPools struct{ st Store }

// EligiblePools returns the installation's single pool, with capacity summed
// over the runners that currently have a control connection and capabilities
// unioned over the same set. The pool comes back even when that sum is zero:
// the session service refuses a pool with no free capacity itself, and a
// session with nowhere to go should be queued waiting for a runner, which is
// today's behavior, rather than refused for having no eligible pool at all.
func (p installationPools) EligiblePools(ctx context.Context, scope control.Scope, req control.Requirements) ([]control.Pool, error) {
	if scope.WorkspaceID != installWorkspace {
		return nil, control.ErrNotFound
	}
	rows, err := p.st.ListRunners(ctx)
	if err != nil {
		return nil, storeErr(err)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })

	pool := control.Pool{ID: installPool}
	for _, r := range rows {
		if !r.Connected {
			continue
		}
		pool.CapacityUsed += r.CapacityUsed
		pool.CapacityTotal += r.CapacityTotal
		for _, c := range runnerCapabilities(r.Name) {
			if !slices.Contains(pool.Capabilities, c) {
				pool.Capabilities = append(pool.Capabilities, c)
			}
		}
	}
	return []control.Pool{pool}, nil
}

// logRecorder is control.EventRecorder against the process log. O8 has no
// event table, and an event is a fact worth seeing rather than one worth
// keeping, so it is logged and dropped.
type logRecorder struct{}

// Record logs the action, the resource kind, and the resource id — three
// opaque values — and nothing else from the event. A session's name, image,
// error tail, and usage never reach a log line from here. It never fails: an
// unrecorded event must not fail the operation that produced it.
func (logRecorder) Record(_ context.Context, e control.Event) error {
	log.Printf("controld: event %s %s %s", e.Action, e.Resource.Kind, e.Resource.ID)
	return nil
}

// systemClock is control.Clock against the wall clock.
type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// idGenerator is control.IDGenerator over the store's id constructors, so
// every identity the application mints has the same shape as one minted by
// any other path (store.go).
type idGenerator struct{}

func (idGenerator) NewSessionID() control.SessionID { return control.SessionID(NewSessionID()) }

func (idGenerator) NewEnvironmentID() control.EnvironmentID {
	return control.EnvironmentID(NewEnvironmentID())
}

// NewEventID mints "evt_" + 32 hex chars. The store has no event table and
// so no constructor of its own for this one.
func (idGenerator) NewEventID() control.EventID { return control.EventID("evt_" + randHex(16)) }

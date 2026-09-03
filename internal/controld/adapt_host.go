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
// single-tenant installation. The three repository ports are not here: the
// store implements them itself, and compose() reads them off it.
var (
	_ control.PoolResolver      = installationPools{}
	_ control.EventRecorder     = logRecorder{}
	_ control.UnitOfWork        = directUnitOfWork{}
	_ control.CheckpointLocator = pinnedCheckpoints{}
	_ control.Clock             = systemClock{}
	_ control.IDGenerator       = idGenerator{}
)

// installationPools is control.PoolResolver for an installation whose whole
// fleet is one pool.
type installationPools struct{ st Store }

// EligiblePools returns the installation's single pool, with capacity summed
// over the runners that currently have a control connection and capabilities
// unioned over the same set. The pool comes back even when that sum is zero:
// the session service refuses a pool with no free capacity itself, and a
// session with nowhere to go should be queued waiting for a runner, which is
// today's behavior, rather than refused for having no eligible pool at all.
//
// The capabilities are the rows' OWN, not a list synthesized from each name:
// a runner advertises them when it registers and the fleet repository stores
// them, so the pool a placement is matched against describes the fleet as it
// actually announced itself.
func (p installationPools) EligiblePools(ctx context.Context, scope control.Scope, req control.Requirements) ([]control.Pool, error) {
	if scope.WorkspaceID != installWorkspace {
		return nil, control.ErrNotFound
	}
	rows, err := p.st.Fleet().ListRunners(ctx, installPool)
	if err != nil {
		return nil, err
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })

	pool := control.Pool{ID: installPool}
	for _, r := range rows {
		if !r.Connected {
			continue
		}
		pool.CapacityUsed += r.CapacityUsed
		pool.CapacityTotal += r.CapacityTotal
		for _, c := range r.Capabilities {
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

// directUnitOfWork is control.UnitOfWork for a host that has no transactions
// to open. Run calls fn with the context it was handed and returns fn's error
// unchanged, which is exactly what a command does today: the port permits a
// host without transactions to run fn directly, and this installation's
// stores gain a real unit of work in a later plan.
type directUnitOfWork struct{}

func (directUnitOfWork) Run(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

// pinnedCheckpoints is control.CheckpointLocator for an installation whose
// checkpoints are container images built by, and living on, one runner: a
// snapshot boots on its holder and nowhere else. It is the locator spelling
// of the affinity the stores derive today, and the honest answer for this
// build — nothing here can pull a snapshot to a second runner.
type pinnedCheckpoints struct{ st Store }

// LocateCheckpoint names the runner holding cp, or nowhere. It finds the
// environment whose current cache carries cp.Ref and asks the store which
// runner built it; an environment with no holder, a ref no environment of ws
// carries, and an empty ref are all "nowhere", which tells the caller to boot
// without the checkpoint rather than to fail. A store that cannot answer is
// the error, because "nowhere" would silently rebuild what is already cached.
func (p pinnedCheckpoints) LocateCheckpoint(ctx context.Context, ws control.WorkspaceID, cp control.Checkpoint) (control.CheckpointLocation, error) {
	if cp.Ref == "" {
		return control.CheckpointLocation{}, nil
	}
	// A listing, until the store gains the direct lookup: an installation has
	// a handful of environments, and the scheduler asks once per queued row.
	rows, _, err := p.st.Environments().ListEnvironments(ctx, ws, control.EnvironmentQuery{})
	if err != nil {
		return control.CheckpointLocation{}, err
	}
	for _, env := range rows {
		if env.Snapshot.Ref != cp.Ref {
			continue
		}
		holder, err := p.st.SnapshotRunner(ctx, ws, env.ID)
		if err != nil {
			return control.CheckpointLocation{}, err
		}
		if holder == "" {
			return control.CheckpointLocation{}, nil
		}
		return control.CheckpointLocation{Runners: []control.RunnerID{holder}}, nil
	}
	return control.CheckpointLocation{}, nil
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

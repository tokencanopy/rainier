package controld

import (
	"context"
	"slices"
	"sort"
	"time"

	"github.com/tokencanopy/rainier/control"
)

// The host ports that are not persistence: which pools a session may run in,
// where a checkpoint can boot, what time it is, and where new identities come
// from. Each is the smallest thing that satisfies its contract for a
// single-tenant installation. The repository ports are not here, and neither
// are the unit of work and the event record: the store implements all of
// them itself, and compose() reads them off it — an event has to commit with
// the row it describes, which only the store can do.
var (
	_ control.PoolResolver      = installationPools{}
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

// pinnedCheckpoints is control.CheckpointLocator for an installation whose
// checkpoints are container images built by, and living on, one runner: a
// snapshot boots on its holder and nowhere else. It is the locator spelling
// of the affinity the stores derive today, and the honest answer for this
// build — nothing here can pull a snapshot to a second runner.
type pinnedCheckpoints struct{ st Store }

// LocateCheckpoint names the runner holding cp, or nowhere. It is one store
// lookup by ref: a cache nobody holds, a cache whose environment's setup has
// moved on, a ref no environment of ws carries, and an empty ref are all
// "nowhere", which tells the caller to boot without the checkpoint rather
// than to fail. A store that cannot answer is the error, because "nowhere"
// would silently rebuild what is already cached.
func (p pinnedCheckpoints) LocateCheckpoint(ctx context.Context, ws control.WorkspaceID, cp control.Checkpoint) (control.CheckpointLocation, error) {
	if cp.Ref == "" {
		return control.CheckpointLocation{}, nil
	}
	holder, err := p.st.SnapshotHolder(ctx, ws, cp.Ref)
	if err != nil {
		return control.CheckpointLocation{}, err
	}
	if holder == "" {
		return control.CheckpointLocation{}, nil
	}
	return control.CheckpointLocation{Runners: []control.RunnerID{holder}}, nil
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

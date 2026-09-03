package runnerplane

import (
	"context"
	"net/http"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// Binding is the authoritative scope a connection acts in. The host derives
// it from the connection's credentials; the plane never decodes it from the
// announce, with the single exception noted on Host.Identify.
type Binding struct {
	WorkspaceID control.WorkspaceID
	PoolID      control.PoolID
	RunnerID    control.RunnerID
}

// Host is what a plane needs from its host. Every method is one dependency.
type Host interface {
	// Identify authenticates an inbound runner connection and names the scope
	// it acts in. It runs BEFORE the WebSocket upgrade — a refusal answers the
	// HTTP request with 401 and the announce is never read — so name is the
	// runner's name only when the request itself carries one, and empty on a
	// protocol like this one whose name arrives in the announce. A host that
	// binds identity to a credential returns its own RunnerID and the plane
	// refuses an announce naming anything else; a host whose credential is
	// fleet-wide leaves RunnerID empty and the plane fills it from the
	// announce.
	Identify(ctx context.Context, r *http.Request, name string) (Binding, error)
	// NextGeneration opens a new generation for the runner (the store's).
	NextGeneration(ctx context.Context, b Binding) (uint64, error)
	// Fleet is the application's fleet service: Register/Reconcile/ApplyRunnerEvent.
	Fleet() control.Fleet
	// FleetRepository is the heartbeat's and the disconnect's port.
	FleetRepository() control.FleetRepository
	// Wake asks the scheduler for a placement pass on the pool.
	Wake(pool control.PoolID)
	// Aside receives the events that transition no session — a finished setup
	// is news about the ENVIRONMENT, a rejected credential news about its
	// OWNER — after the plane has fenced them. gen is the generation the
	// message was produced under.
	Aside(ctx context.Context, b Binding, gen uint64, m runner.FromRunner)
	// SessionRequest answers an upward session RPC (a session_req) for the
	// session named; the answer is sent back down by the plane, which sets
	// its id and method. It runs off the connection's reader, so it may read
	// a store.
	SessionRequest(ctx context.Context, b Binding, sessionID control.SessionID, env runner.RPCEnvelope) runner.RPCEnvelope
}

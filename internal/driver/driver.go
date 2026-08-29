package driver

import "context"

type Spec struct {
	Name        string   // human label
	Image       string   // OCI ref (v0 default a bash image)
	Cmd         []string // entrypoint override; empty = image default
	DialURL     string   // runnerd URL the container's sessiond dials (relay)
	SessionID   string   // stable id runnerd assigns; sessiond registers with it
	EgressAllow []string // hostnames the session may reach
	// ProxyURL, when non-empty, is the egress proxy the session's outbound
	// traffic must route through. Injected as both cases of HTTP_PROXY/
	// HTTPS_PROXY (tools disagree on which they read: BusyBox wget and curl
	// read lowercase, many Go/Node tools read uppercase) plus NO_PROXY/
	// no_proxy for loopback and the docker-desktop host alias.
	ProxyURL string
	// Env is extra environment for the session container, injected as
	// `docker run -e K=V`. Resolved by the layer above (an environment's
	// declared env plus its resolved secret refs, Plan 4) — the driver only
	// carries it through, in sorted-key order, after the proxy vars.
	Env map[string]string
}

type State string

const (
	StateRunning   State = "running"
	StateSuspended State = "suspended" // warm (paused) or cold (stopped, volume kept)
	StateGone      State = "gone"
)

type Handle struct {
	ID    string // driver's own resource id
	State State
}

type Snapshot struct {
	Ref string // OCI image ref or tar path
}

// Listed pairs a driver handle with the session id it belongs to, for List's
// bulk view of every rainier-managed resource.
type Listed struct {
	SessionID string
	Handle    Handle
}

type Driver interface {
	Create(ctx context.Context, spec Spec) (Handle, error)
	Suspend(ctx context.Context, id string, warm bool) error // warm=pause, cold=stop
	Resume(ctx context.Context, id string) error
	Snapshot(ctx context.Context, id string) (Snapshot, error)
	Destroy(ctx context.Context, id string) error
	Inspect(ctx context.Context, id string) (Handle, error)
	Capacity(ctx context.Context) (used, total int, err error)
	// List returns every rainier-labeled resource, in any state (running,
	// suspended, or otherwise still present) — unlike Capacity, which counts
	// only slot-occupying ones. Used by runnerd.Recover to rebuild its
	// registry from what the driver actually has after a restart.
	List(ctx context.Context) ([]Listed, error)
}

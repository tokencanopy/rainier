package driver

import "context"

type Spec struct {
	Name        string   // human label
	Image       string   // OCI ref (v0 default a bash image)
	Cmd         []string // entrypoint override; empty = image default
	DialURL     string   // runnerd URL the container's sessiond dials (relay)
	SessionID   string   // stable id runnerd assigns; sessiond registers with it
	EgressAllow []string // hostnames the session may reach
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

type Driver interface {
	Create(ctx context.Context, spec Spec) (Handle, error)
	Suspend(ctx context.Context, id string, warm bool) error // warm=pause, cold=stop
	Resume(ctx context.Context, id string) error
	Snapshot(ctx context.Context, id string) (Snapshot, error)
	Destroy(ctx context.Context, id string) error
	Inspect(ctx context.Context, id string) (Handle, error)
	Capacity(ctx context.Context) (used, total int, err error)
}

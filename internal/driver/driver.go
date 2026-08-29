package driver

import (
	"context"
	"fmt"
)

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
	// Setup is the environment's setup script, run once inside the fresh
	// container before the agent starts. It reaches the container as
	// RAINIER_SETUP_B64 (base64 of these bytes): a setup script is
	// multi-line by nature and `docker run -e K=V` carries one line per var,
	// so the encoding is what makes an env var a viable channel for a file
	// at all. sessiond decodes it back to /workspace/.rainier/setup.sh and
	// wraps the agent behind it; see cmd/sessiond.
	//
	// Empty means "no setup" — which is the normal case for a session whose
	// environment was already snapshot-cached, since that image IS the
	// finished setup. Capped at MaxSetupBytes.
	Setup string
	// SetupTimeoutSec bounds that run, injected as RAINIER_SETUP_TIMEOUT
	// alongside the script. It is carried, not policed, here: controld owns
	// the default (design §4.3 — it sends 900 when an environment declares
	// none) and sessiond treats anything non-positive as an unbounded run.
	SetupTimeoutSec int
}

// MaxSetupBytes caps Spec.Setup before encoding. The script rides to the
// container in its environment block, which is not an unbounded place to put
// a file: an oversized one fails deep inside `docker run` with an errno that
// names nothing useful, or is silently truncated into a script that does
// something other than what the environment declared. Rejecting it in Create,
// before anything with a side effect runs, is what turns that into an error
// naming the limit and the input that broke it.
const MaxSetupBytes = 512 << 10

// checkSetupSize enforces MaxSetupBytes. Shared by both drivers so the fake
// can never accept a spec the real one refuses — a test that passed against
// one and not the other would be worse than no test.
func checkSetupSize(spec Spec) error {
	if len(spec.Setup) > MaxSetupBytes {
		return fmt.Errorf("setup script is %d bytes, over the %d-byte limit", len(spec.Setup), MaxSetupBytes)
	}
	return nil
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
	// Snapshot commits a session's current filesystem as an image.
	//
	// ref, when non-empty, is the exact tag to commit to, and comes back in
	// the returned Snapshot verbatim. It is opaque here on purpose: the
	// content-addressed environment refs Plan 4 caches
	// (rainier-env:<envID>-<setupHash>) are minted by CONTROLD, which is the
	// only component that knows an environment's setup hash and the only one
	// that can make the same environment resolve to the same ref on every
	// runner. A driver that decorated or re-derived the name would break that
	// addressing: controld would record one ref while the image lived under
	// another.
	//
	// An empty ref asks the driver to mint one of its own. That is the local
	// dev surface's case (POST /sessions/{id}/snapshot names no environment),
	// and the only caller that has no ref to give.
	Snapshot(ctx context.Context, id, ref string) (Snapshot, error)
	// Prepull fetches ref into this runner's local image store ahead of a
	// create that will need it, so the create isn't the thing paying for the
	// pull. Advisory by design — controld dispatches it without waiting on
	// the answer, and a failure costs nothing worse than the slow create the
	// prepull was trying to avoid.
	Prepull(ctx context.Context, ref string) error
	Destroy(ctx context.Context, id string) error
	Inspect(ctx context.Context, id string) (Handle, error)
	Capacity(ctx context.Context) (used, total int, err error)
	// List returns every rainier-labeled resource, in any state (running,
	// suspended, or otherwise still present) — unlike Capacity, which counts
	// only slot-occupying ones. Used by runnerd.Recover to rebuild its
	// registry from what the driver actually has after a restart.
	List(ctx context.Context) ([]Listed, error)
}

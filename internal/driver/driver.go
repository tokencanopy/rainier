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
	// Repos are the repositories this session clones at boot, resolved by
	// controld. They reach the container as RAINIER_REPOS_B64: base64 of the
	// JSON array, for the same reason the setup script is encoded — an env
	// var carries one line, and JSON quoting through an argv is a parser
	// nobody should have to write twice.
	Repos []RepoSpec
	// Init is the environment's per-boot hook, run after the clones. It rides
	// as RAINIER_INIT_B64 with its bound in RAINIER_INIT_TIMEOUT, a channel
	// separate from the setup one because a cache-hit create carries an init
	// and no setup — the image IS the finished setup, and init is the part
	// that could not be baked into it.
	Init           string
	InitTimeoutSec int
	// GitAuthorName and GitAuthorEmail are the identity the session's commits
	// carry, injected as RAINIER_GIT_AUTHOR_NAME / RAINIER_GIT_AUTHOR_EMAIL
	// for sessiond to write into the workspace gitconfig. Neither is a
	// credential; the token never rides the environment block at all.
	GitAuthorName  string
	GitAuthorEmail string
	// Home is the agent home this session mounts: one writable volume per
	// (creator, workspace), landing at Path, inside which each coding agent
	// keeps its own configuration under its own subdirectory. It is the only
	// writable place a session has outside its workspace, and it is what makes
	// a login survive the session that performed it.
	//
	// The volume is NOT the session's. It outlives every session mounted on
	// it, is shared by every concurrent session the same person runs in the
	// same workspace on this runner, and no teardown here removes it — see
	// Driver.Destroy.
	//
	// nil means "mount nothing", which is the honest state for a session with
	// no creator and for every create a control plane older than the field
	// sent. The session's agents then simply ask for a login.
	Home *HomeMount
}

// HomeMount names the agent home volume and where it lands in the container.
// The driver keeps its own copy of the wire's type for the same reason it
// keeps its own Spec and RepoSpec: this package is the sandbox boundary and
// must not depend on the control-plane vocabulary.
//
// Volume is opaque here on purpose. It is a hash the control plane minted
// (controlapp.AgentHomeVolume) precisely so a volume name — readable by anyone
// with a shell on the runner — does not spell out whose account it holds. The
// driver mounts it and never parses it.
type HomeMount struct {
	Volume string
	Path   string
}

// mount returns the `docker run -v` value for the home, and whether there is
// one to emit. A nil or half-specified home yields no mount: `-v :/path` and
// `-v vol:` are daemon-side syntax errors whose messages name nothing a reader
// could act on, so the argv builder stays total and Create is where a
// half-specified home becomes an error naming the field (see checkHome).
func (h *HomeMount) mount() (string, bool) {
	if h == nil || h.Volume == "" || h.Path == "" {
		return "", false
	}
	return h.Volume + ":" + h.Path, true
}

// checkHome rejects a HomeMount missing either half, before Create touches
// anything with a side effect — the same bargain checkScriptSizes makes. A
// present-but-incomplete home is a defect upstream, and the alternative to
// naming it here is a session that boots with no home at all and an agent
// that asks for a login the person already performed.
func checkHome(h *HomeMount) error {
	if h == nil {
		return nil
	}
	if h.Volume == "" || h.Path == "" {
		return fmt.Errorf("agent home is incomplete: volume %q, path %q — both are required", h.Volume, h.Path)
	}
	return nil
}

// RepoSpec is one repository a session clones, mirroring rwire.RepoSpec field
// for field — the driver layer keeps its own type for the same reason it keeps
// its own Spec: it is the boundary the fake and the docker driver share, and
// it must not depend on the control-plane wire package. The JSON tags are the
// ones sessiond decodes, so the two spellings have to stay identical.
type RepoSpec struct {
	Owner         string `json:"owner"`
	Name          string `json:"name"`
	BaseBranch    string `json:"base_branch"`
	SessionBranch string `json:"session_branch"`
	Dir           string `json:"dir"`
}

// MaxSetupBytes caps Spec.Setup (and Spec.Init, which rides the same channel)
// before encoding. A script rides to the container in its environment block,
// which is not an unbounded place to put a file: an oversized one fails deep
// inside `docker run` with an errno that names nothing useful, or is silently
// truncated into a script that does something other than what the environment
// declared. Rejecting it in Create, before anything with a side effect runs,
// is what turns that into an error naming the limit and the input that broke
// it.
const MaxSetupBytes = 512 << 10

// checkScriptSizes enforces MaxSetupBytes on both scripts a spec can carry.
// Shared by both drivers so the fake can never accept a spec the real one
// refuses — a test that passed against one and not the other would be worse
// than no test.
func checkScriptSizes(spec Spec) error {
	for _, s := range []struct {
		what   string
		script string
	}{{"setup", spec.Setup}, {"init", spec.Init}} {
		if len(s.script) > MaxSetupBytes {
			return fmt.Errorf("%s script is %d bytes, over the %d-byte limit", s.what, len(s.script), MaxSetupBytes)
		}
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
	//
	// stripEnv names environment variables that must NOT survive into the
	// committed image's configuration. A commit captures the container's whole
	// config, environment block included, and two classes of variable in there
	// are actively harmful once they are baked into an image every later
	// session boots:
	//
	//   - the setup channel (RAINIER_SETUP_B64 and its timeout). sessiond runs
	//     a setup script whenever that variable is present, so an image
	//     carrying it makes every cache-booted session re-run the setup its
	//     cache exists to skip — the control plane deliberately dispatches no
	//     script, and the container runs one anyway.
	//   - every value the layer above injected as Spec.Env, which is where an
	//     environment's DECRYPTED secrets live. Baked into an image they are
	//     readable by anyone with a docker socket on that runner, and would be
	//     published outright by any registry-backed distribution of the cache.
	//
	// The keys are stripped, not deleted: a driver sets each to the empty
	// string, which every consumer treats as absent (sessiond gates on
	// `RAINIER_SETUP_B64 != ""`; a credential read as "" fails closed rather
	// than silently wrong). A later `docker run -e K=V` still overrides it, so
	// a session booted from the cache is handed its secrets fresh, as it must
	// be — the environment's secret_refs are resolved per create, not per
	// image.
	Snapshot(ctx context.Context, id, ref string, stripEnv []string) (Snapshot, error)
	// Prepull fetches ref into this runner's local image store ahead of a
	// create that will need it, so the create isn't the thing paying for the
	// pull. Advisory by design — controld dispatches it without waiting on
	// the answer, and a failure costs nothing worse than the slow create the
	// prepull was trying to avoid.
	Prepull(ctx context.Context, ref string) error
	// Destroy removes the container AND the session's workspace volume: the
	// whole teardown, for the path where a user asked for the session to stop
	// existing (`rainier rm`). It is DestroyContainer followed by
	// RemoveWorkspace, and it is the only one of the three that takes a
	// workspace away as a side effect of removing a container.
	//
	// The agent home (Spec.Home) is NOT taken. It belongs to the (creator,
	// workspace), not to this session: other sessions of the same person in
	// the same workspace may be mounted on it right now, and removing it
	// would log them all out of every agent they had signed in.
	Destroy(ctx context.Context, id string) error
	// DestroyContainer removes the container and NOTHING else — the session's
	// workspace volume survives.
	//
	// This is the crash path. When a container dies on its own, runnerd
	// removes what is left of it to reclaim the capacity slot, and that
	// removal is not a user asking for their files to be thrown away: a
	// crashed session is precisely the case someone most wants their work
	// back from, and /workspace is where every hour of it lives. Before the
	// split, that path called Destroy — the same call `rainier rm` makes —
	// so a container dying quietly took the workspace with it.
	//
	// The two are separate METHODS rather than a flag because the difference
	// is not a parameter of one operation, it is which of two operations the
	// caller means; a boolean would put "delete the user's work" one inverted
	// condition away from "reclaim a slot".
	DestroyContainer(ctx context.Context, id string) error
	// RemoveWorkspace removes sessionID's workspace volume
	// (rainier-ws-<sessionID>), the second act of an explicit teardown after
	// a crash deliberately kept it. It takes the SESSION id, not a driver
	// handle, because by the time anything wants a kept workspace gone the
	// container that could have named it is long gone.
	//
	// An absent volume is success, not an error: every caller is a teardown
	// path that may be running second (a full Destroy already took it, a
	// reconcile got there first, an rm was retried), and failing there would
	// turn a completed teardown into a reported failure. An empty sessionID
	// is a no-op — "rainier-ws-" alone is a real volume name, and no driver
	// may be talked into removing whatever is under it.
	RemoveWorkspace(ctx context.Context, sessionID string) error
	Inspect(ctx context.Context, id string) (Handle, error)
	Capacity(ctx context.Context) (used, total int, err error)
	// List returns every rainier-labeled resource, in any state (running,
	// suspended, or otherwise still present) — unlike Capacity, which counts
	// only slot-occupying ones. Used by runnerd.Recover to rebuild its
	// registry from what the driver actually has after a restart.
	List(ctx context.Context) ([]Listed, error)
}

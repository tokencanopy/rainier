package main

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/tokencanopy/rainier/internal/cli"
)

// This file is the whole of the CLI's dependence on Rainier Cloud's workspace
// and catalog contracts (docs/cli-v0-contract.md §5). Isolating them here is
// the point: when a route ships, this file changes and nothing else does.
//
// Two of the three are LIVE as of rainier-cloud 81d1ad3:
//
//   - Compute enrollment. GET /v0/workspaces/{id}/compute answers the
//     workspace's own compute status and health, forwarded to bearer clients
//     by the edge. It is the authoritative source for `rainier status`'s
//     Compute row, and it replaced this CLI's earlier guess at deriving one
//     from /v0/runners.
//   - workspace_not_ready. The cell refuses session create AND resume with
//     that code for an enrolled workspace that is not ready, so `new`'s
//     branch on it is real rather than defensive.
//
// One is still outstanding, and one is proposed:
//
//   - The onboarding destination. It exists server-side only as a relative
//     path computed for the cookie-authenticated browser bootstrap. Until a
//     bearer-reachable route publishes it, `status` prints no Continue line —
//     it will not compose a console URL it was not given.
//   - The agent launch catalog. GET /v0/environments/{id}/agents is where a
//     qualified launch argv belongs: launch capability is a property of the
//     environment's image, which is regional, tenant-scoped state. It is NOT
//     /v0/agents, which is account-scoped credential custody and stays that
//     way.

// Route paths. A 404 or 405 on any of them means this server does not publish
// that contract; anything else is a real error.
const (
	computePathPrefix = "/v0/workspaces/"
	computePathSuffix = "/compute"

	// onboardingPath is where a bearer client reads the console's own
	// entry points. Not yet served by any deployment.
	onboardingPath = "/v0/web/onboarding"
)

// readinessTimeout bounds each contract read. `status` is a command people run
// when something is already wrong, so it must answer even when the server is
// the thing that is wrong.
const readinessTimeout = 6 * time.Second

// The compute vocabulary, verbatim from rainier-cloud's workspacecompute
// package. The CLI matches these exactly and treats anything else as unknown
// rather than guessing which known state an unrecognized one resembles.
const (
	computeNeedsPlan       = "needs_plan"
	computeAwaitingPayment = "awaiting_payment"
	computeProvisioning    = "provisioning"
	computeReady           = "ready"
	computeFailed          = "failed"
	computeCancelling      = "cancelling"
	computeCancelled       = "cancelled"
)

// Health is orthogonal to status, and that separation is the same one this
// CLI draws for sessions: `ready` is what the workspace is entitled to and
// `health` is what is currently reachable. A runner going away makes a ready
// workspace unavailable; it does not make it unentitled.
const (
	healthAvailable   = "available"
	healthUnavailable = "unavailable"
	healthUnknown     = "unknown"
)

const (
	githubStatusConnected    = "connected"
	githubStatusNotConnected = "not_connected"
	githubStatusAttention    = "needs_attention"
)

// computeState is the workspace compute representation this CLI reads. It
// decodes the fields it acts on and ignores the rest — the operation, the
// revision and the plan terms belong to the web, which is where a plan is
// chosen and paid for.
type computeState struct {
	// Published is false when the server does not serve the route: an older
	// cell, a self-hosted controld, or a cell composed to sell no compute.
	// All three mean "derive what you can", not "unavailable".
	Published bool `json:"-"`

	Status string `json:"status"`
	Health string `json:"health"`
}

// ready reports whether this workspace can actually run a session.
//
// BOTH fields have to say so. `ready` with health `unavailable` is capacity
// that exists and cannot be reached — the entitlement is intact and the
// session will not start — so reporting it as ready would send somebody to
// `rainier new` to be refused. That pairing is the one the cloud's own
// vocabulary makes expressible, and this is the CLI honoring it.
func (c computeState) ready() bool {
	return c.Published && c.Status == computeReady && c.Health == healthAvailable
}

// fetchCompute reads the workspace's compute enrollment. An unpublished route
// comes back as a zero value rather than an error.
func fetchCompute(ctx context.Context, c *cli.Client, workspaceID string) (computeState, error) {
	if workspaceID == "" {
		// A self-hosted context has no workspace to name in the path, and no
		// compute enrollment behind it either.
		return computeState{}, nil
	}
	ctx, cancel := context.WithTimeout(ctx, readinessTimeout)
	defer cancel()

	path := computePathPrefix + url.PathEscape(workspaceID) + computePathSuffix
	var out computeState
	if err := c.DoContext(ctx, http.MethodGet, path, nil, &out); err != nil {
		if unpublishedRoute(err) {
			return computeState{}, nil
		}
		return computeState{}, err
	}
	out.Published = true
	return out, nil
}

// ---------------------------------------------------------------------------
// the onboarding destination (contract §5.2) — not served by any deployment yet
// ---------------------------------------------------------------------------

// onboardingDestinations is the console's own entry points: where to send a
// person whose compute is in a given state. The map is the SERVER's, keyed by
// the same compute vocabulary it hands back on the compute route, so the CLI
// performs a lookup rather than a policy decision.
type onboardingDestinations struct {
	Published bool `json:"-"`

	ConsoleURL   string            `json:"console_url"`
	Destinations map[string]string `json:"destinations"`
}

func fetchOnboarding(ctx context.Context, c *cli.Client) (onboardingDestinations, error) {
	ctx, cancel := context.WithTimeout(ctx, readinessTimeout)
	defer cancel()

	var out onboardingDestinations
	if err := c.DoContext(ctx, http.MethodGet, onboardingPath, nil, &out); err != nil {
		if unpublishedRoute(err) {
			return onboardingDestinations{}, nil
		}
		return onboardingDestinations{}, err
	}
	out.Published = true
	return out, nil
}

// destinationFor is the address to send a person to for a compute status, or
// "" when the server named none.
//
// It refuses anything that is not an absolute https URL. The value is
// server-supplied and ends up in a message telling somebody where to go, so a
// relative path — which is what the browser bootstrap serves today — or a
// scheme this CLI cannot vouch for is dropped rather than printed. Callers
// print the line only when this is non-empty: a "Continue:" with no URL is
// worse than no line, and one the CLI composed itself would be a readiness
// claim made by the wrong side of the wire.
func (o onboardingDestinations) destinationFor(status string) string {
	if !o.Published || status == "" {
		return ""
	}
	raw, ok := o.Destinations[status]
	if !ok {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return ""
	}
	return raw
}

// unpublishedRoute reports whether err says the server does not serve a route,
// as opposed to serving it and refusing.
func unpublishedRoute(err error) bool {
	return isHTTPStatus(err, http.StatusNotFound) || isHTTPStatus(err, http.StatusMethodNotAllowed)
}

// ---------------------------------------------------------------------------
// default environment (contract §5.3)
// ---------------------------------------------------------------------------

// errNoDefaultEnvironment is what resolveDefaultEnvironment reports when the
// server marks no default and its catalog cannot answer for it. It is not a
// failure on its own — `rainier new` falls back to a scratch session, exactly
// as it did before this flag existed — but it IS a failure for anything that
// needs an image with an agent CLI in it.
var errNoDefaultEnvironment = errors.New("no default environment")

// resolveDefaultEnvironment names the environment a session starts from when
// the caller passed no --env.
//
// Order, and why: an environment the server MARKS default is the server
// saying which one it is, and nothing beats that. A catalog holding exactly
// one environment is the same answer read a longer way round — there is
// nothing else it could mean. More than one, with no marker, is genuinely
// unknown, and the CLI says so rather than picking. It never matches on a
// name, an image, or a setup script: the catalog is the server's, and a CLI
// that carried its own copy would be wrong the first time the server's
// changed.
func resolveDefaultEnvironment(ctx context.Context, c *cli.Client) (environment, error) {
	envs, err := fetchEnvironments(ctx, c)
	if err != nil {
		return environment{}, err
	}
	for _, env := range envs {
		if env.Default {
			return env, nil
		}
	}
	if len(envs) == 1 {
		return envs[0], nil
	}
	return environment{}, errNoDefaultEnvironment
}

func fetchEnvironments(ctx context.Context, c *cli.Client) ([]environment, error) {
	ctx, cancel := context.WithTimeout(ctx, readinessTimeout)
	defer cancel()
	var resp environmentsEnvelope
	if err := c.DoContext(ctx, http.MethodGet, "/v0/environments", nil, &resp); err != nil {
		return nil, err
	}
	return resp.Environments, nil
}

// ---------------------------------------------------------------------------
// agent launch catalog (contract §5.4)
// ---------------------------------------------------------------------------

// agentLauncher is one entry of GET /v0/environments/{id}/agents: an agent
// this environment's image can start, and the argv that starts it.
//
// Argv is a structured array from a closed server catalog and never a shell
// string. It is the value that becomes a session's command, so it is the one
// place a server string turns into something executed — an array cannot be
// re-split by a shell, and the CLI adds nothing to it.
type agentLauncher struct {
	ID          string   `json:"id"`
	DisplayName string   `json:"display_name"`
	Argv        []string `json:"argv"`
	// RequiresLogin says the agent needs `rainier agent login` before it can
	// do anything. It is the only join to custody, and it is a property of
	// the agent rather than of the caller — which is why it lives here and
	// the caller's own credential status stays on /v0/agents.
	RequiresLogin bool `json:"requires_login"`
}

type environmentAgentsEnvelope struct {
	CatalogVersion string          `json:"catalog_version"`
	Agents         []agentLauncher `json:"agents"`
}

// errNoLaunchCatalog names the missing dependency in the one sentence a
// person can act on. It says what is absent and shows the remaining move,
// because there is nothing the user can do at the CLI to supply it.
var errNoLaunchCatalog = errors.New(
	"this server does not publish which agents its environments can start, so --agent cannot resolve one " +
		"(GET /v0/environments/{id}/agents; see docs/cli-v0-contract.md §5.4). " +
		"Start the agent yourself instead: rainier new --name NAME -- claude")

// fetchEnvironmentAgents reads one environment's launch catalog. An
// unpublished route is reported as such, so the caller can name the
// dependency rather than say the environment carries no agents.
func fetchEnvironmentAgents(ctx context.Context, c *cli.Client, envID string) ([]agentLauncher, bool, error) {
	if envID == "" {
		return nil, false, nil
	}
	ctx, cancel := context.WithTimeout(ctx, readinessTimeout)
	defer cancel()

	var resp environmentAgentsEnvelope
	path := "/v0/environments/" + url.PathEscape(envID) + "/agents"
	if err := c.DoContext(ctx, http.MethodGet, path, nil, &resp); err != nil {
		if unpublishedRoute(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return resp.Agents, true, nil
}

// agentLaunchCommand is the argv that starts provider's agent in env.
//
// Three refusals, and they are different, because the recovery differs:
// a server that publishes no catalog is the missing contract above; an
// environment whose image carries nothing is a readiness problem with the
// workspace; a provider the environment does not carry is the caller naming
// one that is not there. Blurring them would send someone hunting for a
// spelling mistake that isn't there.
func agentLaunchCommand(ctx context.Context, c *cli.Client, env environment, provider string) ([]string, error) {
	launchers, published, err := fetchEnvironmentAgents(ctx, c, env.ID)
	if err != nil {
		return nil, err
	}
	if !published {
		return nil, errNoLaunchCatalog
	}
	if len(launchers) == 0 {
		return nil, errors.New("environment " + safeField(env.Name) +
			" carries no coding agent this server can start; run `rainier status`, or name a command with -- CMD")
	}
	for _, launcher := range launchers {
		if launcher.ID != provider {
			continue
		}
		if len(launcher.Argv) == 0 {
			return nil, errNoLaunchCatalog
		}
		return launcher.Argv, nil
	}
	names := make([]string, 0, len(launchers))
	for _, launcher := range launchers {
		names = append(names, launcher.ID)
	}
	return nil, errors.New("environment " + safeField(env.Name) + " cannot start " + safeField(provider) +
		"; it carries: " + safeField(strings.Join(names, ", ")))
}

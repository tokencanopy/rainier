package controld

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/tokencanopy/rainier/control"
)

// A self-hosted installation is exactly one workspace running exactly one
// pool. The control contract keys every repository call by workspace and
// every fleet call by pool, so the adapters below give this installation a
// fixed identity for each and refuse any other — isolation is enforced even
// though there is one tenant. Making scope mandatory in the schema is O9.
const (
	installWorkspace control.WorkspaceID = "ws_self_hosted"
	installPool      control.PoolID      = "pool_self_hosted"
)

// placementCapabilityPrefix is the capability spelling of an explicit runner
// pin (environment.placement), which control.Environment cannot name
// directly. Its sibling, the affinity of a current snapshot to the runner
// that built it, lives in store.go beside the helpers that write and strip
// it. Runners advertise both for their own name.
const placementCapabilityPrefix = "placement:"

// runnerCapabilities is the pair every runner advertises for its own name: an
// environment pinned to it by placement, and one held to it by the snapshot it
// built, both match. It is the host's spelling of a runner's identity as a
// capability, which is why it lives here with the other scope constants rather
// than in either store.
func runnerCapabilities(name string) []string {
	return []string{placementCapabilityPrefix + name, SnapshotCapability(control.RunnerID(name))}
}

// The capability token rule, and the one this installation applies to every
// capability that is not the host's own: a lowercase token, at most 32 of
// them on any one claim, and never a host prefix. It is deliberately narrow —
// a capability is matched by exact string equality across a fleet of runners
// nobody re-deploys at once, so a spelling that varies by case or whitespace
// is a placement that silently never happens.
const maxCapabilities = 32

var capabilityToken = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

// validateCapabilities applies that rule to caps, returning a client-facing
// sentence naming the first thing wrong with it. what names the list in that
// sentence, so the same rule can answer for an environment's field and for a
// runner's announce without either message claiming to be the other.
//
// Host prefixes are refused rather than ignored: the two capabilities
// controld spells for a runner's own name are the HOST's claims about it
// (adapt_scope.go, store.go), and a runner or an operator that could write
// one could pin any environment to any runner.
func validateCapabilities(what string, caps []string) error {
	if len(caps) > maxCapabilities {
		return fmt.Errorf("%s: at most %d are allowed, got %d", what, maxCapabilities, len(caps))
	}
	seen := make(map[string]bool, len(caps))
	for _, c := range caps {
		switch {
		case strings.Contains(c, ":"):
			return fmt.Errorf("%s: %q carries a host prefix, which only controld may claim", what, clip(c))
		case !capabilityToken.MatchString(c):
			return fmt.Errorf("%s: %q must match [a-z0-9][a-z0-9._-]{0,63}", what, clip(c))
		case seen[c]:
			return fmt.Errorf("%s: %q is listed twice", what, clip(c))
		}
		seen[c] = true
	}
	return nil
}

// portableCapabilities returns the capabilities of caps that are claims about
// what a runner can DO, dropping the host's own spellings of where something
// is (placement:, snapshot:). The colon is the whole test, and it is exact:
// validateCapabilities refuses a colon in anything a runner or an operator
// supplies, so every capability carrying one is the host's.
func portableCapabilities(caps []string) []string {
	out := make([]string, 0, len(caps))
	for _, c := range caps {
		if strings.Contains(c, ":") {
			continue
		}
		out = append(out, c)
	}
	return out
}

// capabilityValue returns the tail of the first capability carrying prefix,
// or "" when none does — the read half of the two prefixes above.
func capabilityValue(caps []string, prefix string) string {
	for _, c := range caps {
		if after, ok := strings.CutPrefix(c, prefix); ok {
			return after
		}
	}
	return ""
}

// installPlacement is the documented installation-local placement scope
// (control/scope.go): not a real cloud region, just the self-hosted values.
func installPlacement() control.PlacementScope {
	return control.PlacementScope{
		ProductRegion: "self-hosted",
		HomeCell:      "default",
		Mode:          control.ExecutionSelfHosted,
	}
}

// userScope is the scope every HTTP handler passes for an authenticated user.
func userScope(u User) control.Scope {
	return control.Scope{
		WorkspaceID: installWorkspace,
		Actor:       control.Actor{ID: control.ActorID(u.ID), Kind: control.ActorUser},
		Placement:   installPlacement(),
	}
}

// userContextKey carries the authenticated User from the request wrapper to
// the authorization adapter. The Scope deliberately carries no role
// (control/scope.go), so the adapter reads the role from here and requires
// the scope's actor to agree with it.
type userContextKey struct{}

func withUser(ctx context.Context, u User) context.Context {
	return context.WithValue(ctx, userContextKey{}, u)
}

func userFromContext(ctx context.Context) (User, bool) {
	u, ok := ctx.Value(userContextKey{}).(User)
	return u, ok
}

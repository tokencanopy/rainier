package controld

import (
	"context"

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

// The two capability spellings the store adapter uses to express things
// control.Environment cannot name directly: an explicit runner pin
// (environment.placement) and the affinity of a current snapshot to the
// runner that built it. Runners advertise both for their own name.
const (
	placementCapabilityPrefix = "placement:"
	snapshotCapabilityPrefix  = "snapshot:"
)

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

// serviceScope is the scope the runner plane acts under: a narrowly scoped
// service principal named for the runner, never a user.
func serviceScope(runner string) control.Scope {
	return control.Scope{
		WorkspaceID: installWorkspace,
		Actor:       control.Actor{ID: control.ActorID("runner:" + runner), Kind: control.ActorService},
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

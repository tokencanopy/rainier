package controld

import (
	"context"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/controlapp"
)

// ownerOrAdmin is the self-hosted authorization adapter, and from here on the
// single source of truth for the matrix the HTTP handlers used to enforce one
// route wrapper at a time: reads are team-wide, session mutations are
// owner-or-admin (once api.go's authorizeOwnerOrAdmin), and the fleet-wide writes —
// environments — are admin-only (auth.go's requireAdmin), because a resource
// the whole team shares has no owner to fall back on.
//
// It answers control.ErrDenied and nothing else. A denial never says whether
// the resource exists, whose it is, or which of the three rules refused it:
// the sentinel is the whole answer, and the handler maps it to one status.
type ownerOrAdmin struct{}

var (
	_ control.Authorizer          = ownerOrAdmin{}
	_ controlapp.AttachmentPolicy = ownerOrAdmin{}
)

// adminRole is the one role that carries fleet-wide authority. Every other
// role value — including "" — is a member.
const adminRole = "admin"

// Authorize answers the (action, resource kind) matrix for the authenticated
// user the request wrapper put in ctx.
func (ownerOrAdmin) Authorize(ctx context.Context, scope control.Scope, a control.Action, r control.Resource) error {
	u, err := actingUser(ctx, scope)
	if err != nil {
		return err
	}
	if r.WorkspaceID != installWorkspace {
		// A resource outside this installation's one workspace is refused
		// before its kind is even considered, so a cross-workspace probe
		// learns nothing an in-workspace one would not.
		return control.ErrDenied
	}
	switch r.Kind {
	case control.ResourceSession:
		switch a {
		case control.ActionCreate, control.ActionGet, control.ActionList, control.ActionDiff:
			return nil
		case control.ActionDelete, control.ActionSuspend, control.ActionResume,
			control.ActionSnapshot, control.ActionAttach, control.ActionPush, control.ActionPull:
			return ownsOrAdmins(u, r)
		}
	case control.ResourceEnvironment:
		switch a {
		case control.ActionGet, control.ActionList:
			return nil
		case control.ActionCreate, control.ActionUpdate, control.ActionDelete:
			if u.Role == adminRole {
				return nil
			}
		}
	case control.ResourceRunner:
		if a == control.ActionList {
			return nil
		}
	}
	return control.ErrDenied
}

// AuthorizeAttachment is the mode-aware half of the same policy. Today's
// attach has no viewer/controller distinction — a caller who may attach may
// drive — so both modes get the session mutation rule, and the mode is
// deliberately not consulted. Cloud is where the two answers diverge.
func (ownerOrAdmin) AuthorizeAttachment(ctx context.Context, scope control.Scope, r control.Resource, _ control.AttachmentMode) error {
	u, err := actingUser(ctx, scope)
	if err != nil {
		return err
	}
	if r.WorkspaceID != installWorkspace {
		return control.ErrDenied
	}
	return ownsOrAdmins(u, r)
}

// actingUser returns the authenticated user a decision is about, refusing two
// things before any rule runs: a context with no user at all (nothing
// authenticated this call, so nothing may be authorized), and a scope whose
// actor disagrees with that user. The scope is authoritative adapter output
// and the context is what the token resolved to; if they ever disagree the
// call was assembled wrong, and guessing which one meant it is exactly the
// mistake an authorization adapter must not make.
func actingUser(ctx context.Context, scope control.Scope) (User, error) {
	u, ok := userFromContext(ctx)
	if !ok {
		return User{}, control.ErrDenied
	}
	if scope.Actor.Kind != control.ActorUser || scope.Actor.ID != control.ActorID(u.ID) {
		return User{}, control.ErrDenied
	}
	return u, nil
}

// ownsOrAdmins is the object-level rule itself: the resource's own creator, or
// an admin. A resource with no creator (there is no such session) is therefore
// admin-only rather than open.
func ownsOrAdmins(u User, r control.Resource) error {
	if u.Role == adminRole || (r.CreatorID != "" && u.ID == string(r.CreatorID)) {
		return nil
	}
	return control.ErrDenied
}

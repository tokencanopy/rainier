package controld

import (
	"context"
	"errors"
	"testing"

	"github.com/tokencanopy/rainier/control"
)

// TestOwnerOrAdminMatrix pins the whole authorization matrix the adapter is
// now the single source of truth for: reads are team-wide, session mutations
// are owner-or-admin, environment writes are admin-only, and nothing outside
// the installation's own workspace is ever authorized.
func TestOwnerOrAdminMatrix(t *testing.T) {
	owner := User{ID: "usr_owner", Role: "member"}
	other := User{ID: "usr_other", Role: "member"}
	admin := User{ID: "usr_admin", Role: "admin"}
	sess := control.Resource{Kind: control.ResourceSession, WorkspaceID: installWorkspace, ID: "sess_example", CreatorID: "usr_owner"}
	env := control.Resource{Kind: control.ResourceEnvironment, WorkspaceID: installWorkspace, ID: "env_example"}
	runners := control.Resource{Kind: control.ResourceRunner, WorkspaceID: installWorkspace}
	cases := []struct {
		name   string
		u      User
		action control.Action
		res    control.Resource
		want   error
	}{
		{"anyone creates a session", other, control.ActionCreate, sess, nil},
		{"owner deletes own", owner, control.ActionDelete, sess, nil},
		{"other cannot delete", other, control.ActionDelete, sess, control.ErrDenied},
		{"admin deletes any", admin, control.ActionDelete, sess, nil},
		{"anyone gets", other, control.ActionGet, sess, nil},
		{"anyone lists", other, control.ActionList, sess, nil},
		{"anyone diffs", other, control.ActionDiff, sess, nil},
		{"owner suspends own", owner, control.ActionSuspend, sess, nil},
		{"owner resumes own", owner, control.ActionResume, sess, nil},
		{"owner snapshots own", owner, control.ActionSnapshot, sess, nil},
		{"owner attaches own", owner, control.ActionAttach, sess, nil},
		{"owner pushes own", owner, control.ActionPush, sess, nil},
		{"owner pulls own", owner, control.ActionPull, sess, nil},
		{"other cannot push", other, control.ActionPush, sess, control.ErrDenied},
		{"other cannot pull", other, control.ActionPull, sess, control.ErrDenied},
		{"other cannot attach", other, control.ActionAttach, sess, control.ErrDenied},
		{"other cannot suspend", other, control.ActionSuspend, sess, control.ErrDenied},
		{"member cannot create env", owner, control.ActionCreate, env, control.ErrDenied},
		{"member cannot update env", owner, control.ActionUpdate, env, control.ErrDenied},
		{"member cannot delete env", owner, control.ActionDelete, env, control.ErrDenied},
		{"admin creates env", admin, control.ActionCreate, env, nil},
		{"admin updates env", admin, control.ActionUpdate, env, nil},
		{"admin deletes env", admin, control.ActionDelete, env, nil},
		{"anyone gets env", other, control.ActionGet, env, nil},
		{"anyone lists env", other, control.ActionList, env, nil},
		{"anyone lists runners", other, control.ActionList, runners, nil},
		{"nobody creates a runner", admin, control.ActionCreate, runners, control.ErrDenied},
		{"other workspace denied", admin, control.ActionGet, control.Resource{Kind: control.ResourceSession, WorkspaceID: "ws_other", ID: "sess_example"}, control.ErrDenied},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := withUser(context.Background(), tc.u)
			err := ownerOrAdmin{}.Authorize(ctx, userScope(tc.u), tc.action, tc.res)
			if !errors.Is(err, tc.want) && !(err == nil && tc.want == nil) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
	if err := (ownerOrAdmin{}).Authorize(context.Background(), userScope(owner), control.ActionGet, sess); !errors.Is(err, control.ErrDenied) {
		t.Fatalf("no user in context: got %v", err)
	}
	if err := (ownerOrAdmin{}).Authorize(withUser(context.Background(), other), userScope(owner), control.ActionGet, sess); !errors.Is(err, control.ErrDenied) {
		t.Fatalf("scope/context disagreement: got %v", err)
	}
	if err := (ownerOrAdmin{}).Authorize(withUser(context.Background(), owner), control.Scope{WorkspaceID: installWorkspace, Actor: control.Actor{ID: "runner:runner-a", Kind: control.ActorService}, Placement: installPlacement()}, control.ActionGet, sess); !errors.Is(err, control.ErrDenied) {
		t.Fatalf("service scope carrying a user context: got %v", err)
	}
}

// TestOwnerOrAdminAttachment pins the attachment policy: today's attach has
// no viewer/controller distinction, so both modes get the same owner-or-admin
// answer the generic matrix gives ActionAttach.
func TestOwnerOrAdminAttachmentAppliesOwnerOrAdminToBothModes(t *testing.T) {
	owner := User{ID: "usr_owner", Role: "member"}
	other := User{ID: "usr_other", Role: "member"}
	admin := User{ID: "usr_admin", Role: "admin"}
	sess := control.Resource{Kind: control.ResourceSession, WorkspaceID: installWorkspace, ID: "sess_example", CreatorID: "usr_owner"}

	for _, mode := range []control.AttachmentMode{control.AttachmentViewer, control.AttachmentController} {
		t.Run(string(mode), func(t *testing.T) {
			for _, u := range []User{owner, admin} {
				ctx := withUser(context.Background(), u)
				if err := (ownerOrAdmin{}).AuthorizeAttachment(ctx, userScope(u), sess, mode); err != nil {
					t.Fatalf("%s: got %v, want nil", u.ID, err)
				}
			}
			ctx := withUser(context.Background(), other)
			if err := (ownerOrAdmin{}).AuthorizeAttachment(ctx, userScope(other), sess, mode); !errors.Is(err, control.ErrDenied) {
				t.Fatalf("non-owner: got %v, want ErrDenied", err)
			}
			if err := (ownerOrAdmin{}).AuthorizeAttachment(context.Background(), userScope(owner), sess, mode); !errors.Is(err, control.ErrDenied) {
				t.Fatalf("no user in context: got %v, want ErrDenied", err)
			}
			foreign := control.Resource{Kind: control.ResourceSession, WorkspaceID: "ws_other", ID: "sess_example", CreatorID: "usr_admin"}
			if err := (ownerOrAdmin{}).AuthorizeAttachment(withUser(context.Background(), admin), userScope(admin), foreign, mode); !errors.Is(err, control.ErrDenied) {
				t.Fatalf("other workspace: got %v, want ErrDenied", err)
			}
		})
	}
}

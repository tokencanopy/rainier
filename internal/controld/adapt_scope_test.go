package controld

import (
	"context"
	"testing"

	"github.com/tokencanopy/rainier/control"
)

func TestInstallationScopesValidate(t *testing.T) {
	u := User{ID: "usr_example", Login: "octo-example", Role: "member"}
	if err := userScope(u).Validate(); err != nil {
		t.Fatalf("user scope invalid: %v", err)
	}
	if s := userScope(u); s.WorkspaceID != installWorkspace || s.Actor.ID != "usr_example" || s.Actor.Kind != control.ActorUser {
		t.Fatalf("user scope = %+v", s)
	}
	if s := userScope(u); s.Placement.Mode != control.ExecutionSelfHosted || s.Placement.ProductRegion != "self-hosted" || s.Placement.HomeCell != "default" {
		t.Fatalf("placement = %+v", s.Placement)
	}
}

func TestUserRoundTripsThroughContext(t *testing.T) {
	if _, ok := userFromContext(context.Background()); ok {
		t.Fatal("an empty context must carry no user")
	}
	u := User{ID: "usr_example", Role: "admin"}
	got, ok := userFromContext(withUser(context.Background(), u))
	if !ok || got != u {
		t.Fatalf("got %+v ok=%v", got, ok)
	}
}

package controlapp

import (
	"context"
	"errors"
	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
	"slices"
	"testing"
)

func TestGuestReconnectSpecResolvesCurrentMaterialWithoutMinting(t *testing.T) {
	fx := newFleetFixture(t)
	row := bootstrapRow()
	fx.resolver.material = LaunchMaterial{Environment: map[string]string{"OLD_TEST": "secret_test"}}
	first, err := fx.service.ResolveGuestReconnectSpec(fleetCtx, row, nil)
	if err != nil {
		t.Fatal(err)
	}
	fx.resolver.material = LaunchMaterial{Environment: map[string]string{"NEW_TEST": "other_secret_test"}, Repos: []runner.RepoSpec{{Owner: "example", Name: "synthetic"}}}
	next, err := fx.service.ResolveGuestReconnectSpec(fleetCtx, row, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(first.SecretNames, []string{"OLD_TEST"}) || !slices.Equal(next.SecretNames, []string{"NEW_TEST"}) {
		t.Fatal("configuration was cached")
	}
	if len(next.Repos) != 1 || next.Repos[0].Name != "synthetic" {
		t.Fatal("current repositories omitted")
	}
	if first.BootstrapToken != "" || next.BootstrapToken != "" {
		t.Fatal("resolver minted authority")
	}
	if _, ok := fx.bootstraps.minted(row.ID); ok {
		t.Fatal("resolver replaced the accepted token")
	}
	if _, ok := next.Env["NEW_TEST"]; ok {
		t.Fatal("secret reached runner configuration")
	}
	if len(next.Env) == 0 {
		t.Fatal("agent configuration omitted")
	}
	next.Env["MUTATION_TEST"] = "test"
	again, err := fx.service.ResolveGuestReconnectSpec(fleetCtx, row, nil)
	if err != nil || again.Env["MUTATION_TEST"] != "" {
		t.Fatal("configuration aliases prior result")
	}
}

func TestGuestReconnectSpecFailureReturnsNoConfiguration(t *testing.T) {
	fx := newFleetFixture(t)
	fx.resolver.err = errors.New("private_resolver_test")
	spec, err := fx.service.ResolveGuestReconnectSpec(fleetCtx, bootstrapRow(), nil)
	if spec != nil || !errors.Is(err, control.ErrUnavailable) {
		t.Fatalf("failed resolution: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fx.resolver.err = nil
	spec, err = fx.service.ResolveGuestReconnectSpec(ctx, bootstrapRow(), nil)
	if spec != nil || !errors.Is(err, control.ErrUnavailable) {
		t.Fatal("canceled resolution returned configuration")
	}
}

func TestGuestReconnectCreateRequiresNegotiatedCapability(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		fx := newFleetFixture(t)
		fx.service.guestReconnect = true
		caps := []string{runner.CapabilityMicrovmV1}
		if enabled {
			caps = append(caps, runner.CapabilityGuestReconnectV1)
		}
		spec, fail := fx.service.createSpec(fleetCtx, bootstrapRow(), nil, caps, 3)
		if fail != "" {
			t.Fatal(fail)
		}
		if (spec.GuestReconnect == 1) != enabled {
			t.Fatal("guest opt-in does not match negotiation")
		}
	}
}

func TestGuestReconnectCreateRequiresHostSupport(t *testing.T) {
	fx := newFleetFixture(t)
	spec, fail := fx.service.createSpec(fleetCtx, bootstrapRow(), nil, []string{runner.CapabilityMicrovmV1, runner.CapabilityGuestReconnectV1}, 3)
	if fail != "" {
		t.Fatal(fail)
	}
	if spec.GuestReconnect != 0 {
		t.Fatal("negotiated reconnect without host authority support")
	}
}

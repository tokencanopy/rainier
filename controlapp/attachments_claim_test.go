package controlapp

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/tokencanopy/rainier/control"
)

// ---------------------------------------------------------------------------
// the identity a keeper's policy questions are asked about
// ---------------------------------------------------------------------------

// testPrincipalKey is a host's "who authenticated this call" context value,
// standing in for internal/controld's User and a hosted cell's workspace
// role. Every real attachment policy resolves its caller from a value like
// this one — which is the whole reason a claim authorized on the runner's
// dial-back context is authorized about nobody.
type testPrincipalKey struct{}

func withTestPrincipal(ctx context.Context, who string) context.Context {
	return context.WithValue(ctx, testPrincipalKey{}, who)
}

func testPrincipalFrom(ctx context.Context) (string, bool) {
	who, ok := ctx.Value(testPrincipalKey{}).(string)
	return who, ok
}

// identityPolicy is the shape of every host policy that matters here: it
// answers about the principal the CONTEXT carries and refuses a call that
// carries none, exactly as internal/controld's actingUser does before any
// rule runs.
type identityPolicy struct {
	allow          string   // the one principal that may do anything
	denyController bool     // grant viewing, refuse driving
	asked          []string // the principal of each question, in order
	modes          []control.AttachmentMode
}

func (p *identityPolicy) AuthorizeAttachment(ctx context.Context, _ control.Scope,
	_ control.Resource, mode control.AttachmentMode) error {
	who, ok := testPrincipalFrom(ctx)
	p.asked = append(p.asked, who)
	p.modes = append(p.modes, mode)
	switch {
	case !ok || who != p.allow:
		return control.ErrDenied
	case p.denyController && mode == control.AttachmentController:
		return control.ErrDenied
	}
	return nil
}

// claimKeeper builds one attach's keeper the way grant does, over the
// fixture's store and clock, with identity as the context that authorized it.
func claimKeeper(fx *attachmentFixture, policy AttachmentPolicy, identity context.Context) controllerKeeper {
	row := fx.sessions.row
	return controllerKeeper{
		sessions: fx.sessions, clock: fx.svc.clock, policy: policy,
		scope: control.Scope{WorkspaceID: row.WorkspaceID, Actor: control.Actor{ID: row.CreatorID, Kind: control.ActorUser}},
		resource: control.Resource{Kind: control.ResourceSession, WorkspaceID: row.WorkspaceID,
			ID: string(row.ID), CreatorID: row.CreatorID},
		identity: identity,
		ws:       row.WorkspaceID, id: row.ID, holder: "hold_example",
	}
}

// TestAClaimIsAuthorizedAgainstTheIdentityThatOpenedTheAttach is the unit
// statement of the defect. A mid-attach claim is made on the runner's
// dial-back context, which carries no principal; the question is asked about
// the identity captured where the keeper was built instead.
func TestAClaimIsAuthorizedAgainstTheIdentityThatOpenedTheAttach(t *testing.T) {
	fx := newAttachmentFixture(t)
	policy := &identityPolicy{allow: "usr_example"}
	k := claimKeeper(fx, policy, withTestPrincipal(context.Background(), "usr_example"))

	// The runner's context: authenticated as a runner, carrying nobody.
	gen, err := k.Claim(context.Background(), fx.sessions.row.ControllerGeneration)
	if err != nil {
		t.Fatalf("Claim on a context with no principal: %v", err)
	}
	if gen == 0 {
		t.Fatal("Claim returned generation 0")
	}
	if !slices.Equal(policy.asked, []string{"usr_example"}) {
		t.Fatalf("the policy was asked about %v, want one question about usr_example", policy.asked)
	}
	if !slices.Equal(policy.modes, []control.AttachmentMode{control.AttachmentController}) {
		t.Fatalf("the policy was asked about %v, want the controller", policy.modes)
	}
}

// TestACallersContextCannotSupplyTheIdentity is the reason the values are the
// authorizing context's ALONE rather than merged with the caller's. The
// caller is the runner's dial-back request; a runner that could contribute a
// principal to this decision would be naming the identity its session's
// claims are authorized as.
func TestACallersContextCannotSupplyTheIdentity(t *testing.T) {
	fx := newAttachmentFixture(t)
	policy := &identityPolicy{allow: "usr_example"}
	k := claimKeeper(fx, policy, withTestPrincipal(context.Background(), "usr_other"))

	// The live call names the principal the policy would allow. It must not
	// be consulted: this keeper's attach was opened by somebody else.
	if _, err := k.Claim(withTestPrincipal(context.Background(), "usr_example"),
		fx.sessions.row.ControllerGeneration); !errors.Is(err, control.ErrDenied) {
		t.Fatalf("Claim with an identity supplied by the caller: err = %v, want ErrDenied", err)
	}
	if !slices.Equal(policy.asked, []string{"usr_other"}) {
		t.Fatalf("the policy was asked about %v, want the attach's own usr_other", policy.asked)
	}
	if got := fx.sessions.compareAndAdvanceCalls(); got != 0 {
		t.Fatalf("a refused claim reached the store %d times, want 0", got)
	}
}

// TestAClaimHonoursTheLiveCallsCancellation is the other half of that split:
// the values come from the attach, and the deadline and cancellation come
// from the call being made now. A captured context must not be able to keep a
// policy's network call alive after the claim that asked it is gone.
func TestAClaimHonoursTheLiveCallsCancellation(t *testing.T) {
	fx := newAttachmentFixture(t)
	var seen error
	policy := policyFunc(func(ctx context.Context, mode control.AttachmentMode) error {
		seen = ctx.Err()
		return ctx.Err()
	})
	k := claimKeeper(fx, policy, withTestPrincipal(context.Background(), "usr_example"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := k.Claim(ctx, fx.sessions.row.ControllerGeneration); !errors.Is(err, control.ErrDenied) {
		t.Fatalf("Claim on a cancelled call: err = %v, want ErrDenied", err)
	}
	if !errors.Is(seen, context.Canceled) {
		t.Fatalf("the policy saw ctx.Err() = %v, want context.Canceled from the live call", seen)
	}
	if _, ok := k.authorizing(ctx).Deadline(); ok {
		t.Fatal("an authorizing context reported a deadline the live call did not have")
	}
}

// TestAViewOnlyPrincipalsClaimIsRefusedWithTheIdentityCarried pins that
// carrying the identity is not a weakening. The policy now gets to answer the
// question it was always asked, about the person it was always about, and a
// host that grants viewing without driving is honoured.
func TestAViewOnlyPrincipalsClaimIsRefusedWithTheIdentityCarried(t *testing.T) {
	fx := newAttachmentFixture(t)
	policy := &identityPolicy{allow: "usr_example", denyController: true}
	k := claimKeeper(fx, policy, withTestPrincipal(context.Background(), "usr_example"))

	if _, err := k.Claim(context.Background(), fx.sessions.row.ControllerGeneration); !errors.Is(err, control.ErrDenied) {
		t.Fatalf("a view-only principal's claim: err = %v, want ErrDenied", err)
	}
	if !slices.Equal(policy.asked, []string{"usr_example"}) {
		t.Fatalf("the policy was asked about %v, want usr_example", policy.asked)
	}
	if got := fx.sessions.compareAndAdvanceCalls(); got != 0 {
		t.Fatalf("a refused claim reached the store %d times, want 0", got)
	}
}

// TestRenewReleaseAndStateAskNoPolicy is the audit as a test. These three run
// on the runner's dial-back context for the whole life of an attach — the
// heartbeat every five seconds, the release on the way out, the read behind a
// refusal — and none of them is a second authorization decision. A policy
// question added to one of them without routing it through the authorizing
// identity would refuse every heartbeat on every host that reads its caller
// from the context, which is what this fails on.
func TestRenewReleaseAndStateAskNoPolicy(t *testing.T) {
	fx := newAttachmentFixture(t)
	// A policy that refuses everything, including the identity it is given:
	// if any of the three asks it anything at all, the answer is no.
	policy := &identityPolicy{allow: "nobody_at_all"}
	k := claimKeeper(fx, policy, withTestPrincipal(context.Background(), "usr_example"))

	gen, err := fx.sessions.NextControllerGeneration(context.Background(), k.ws, k.id)
	if err != nil {
		t.Fatalf("advancing the store's generation: %v", err)
	}
	if err := k.Renew(context.Background(), gen); err != nil {
		t.Fatalf("Renew on the runner's context: %v", err)
	}
	if _, _, err := k.State(context.Background()); err != nil {
		t.Fatalf("State on the runner's context: %v", err)
	}
	if err := k.Release(context.Background(), gen); err != nil {
		t.Fatalf("Release on the runner's context: %v", err)
	}
	if len(policy.asked) != 0 {
		t.Fatalf("renew/state/release asked the policy %v; none of them is an authorization decision", policy.asked)
	}
}

// TestAKeeperWithNoCapturedIdentityAsksOnTheCallersContext keeps the
// fall-through honest: a keeper built without an authorizing context — a
// zero value, or a host that authorizes from the scope alone — asks exactly
// as it did before this existed.
func TestAKeeperWithNoCapturedIdentityAsksOnTheCallersContext(t *testing.T) {
	fx := newAttachmentFixture(t)
	policy := &identityPolicy{allow: "usr_example"}
	k := claimKeeper(fx, policy, nil)

	if _, err := k.Claim(withTestPrincipal(context.Background(), "usr_example"),
		fx.sessions.row.ControllerGeneration); err != nil {
		t.Fatalf("Claim with no captured identity: %v", err)
	}
	if !slices.Equal(policy.asked, []string{"usr_example"}) {
		t.Fatalf("the policy was asked about %v, want the caller's usr_example", policy.asked)
	}
}

// TestAttachTerminalCapturesTheAuthorizingIdentity is the seam itself: the
// keeper the service hands the plane must carry the identity the attach was
// authorized under, because the plane has no way to supply one — it is
// spliced onto a socket the runner opened.
func TestAttachTerminalCapturesTheAuthorizingIdentity(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.transport.connected = true
	ctx := withTestPrincipal(context.Background(), "usr_example")
	cmd := control.AttachTerminal{SessionID: "sess_example",
		Mode: control.AttachmentController, Negotiated: true}
	if err := fx.svc.AttachTerminal(ctx, attachmentTestScope(), cmd, &attachmentRecordingTerminalStream{}); err != nil {
		t.Fatalf("AttachTerminal: %v", err)
	}
	keeper := fx.broker.lastTarget.Controller
	if keeper == nil {
		t.Fatal("a negotiated attach reached the broker with no keeper")
	}
	// The door's own question was asked about the attaching user.
	if got := fx.policy.principalsAsked(); !slices.Equal(got, []string{"usr_example"}) {
		t.Fatalf("the attach's policy questions were about %v, want usr_example", got)
	}
	// And so is a claim made later, on a context that carries nobody.
	if _, err := keeper.Claim(context.Background(), fx.sessions.row.ControllerGeneration); err != nil {
		t.Fatalf("a mid-attach claim on the runner's context: %v", err)
	}
	if got := fx.policy.principalsAsked(); !slices.Equal(got, []string{"usr_example", "usr_example"}) {
		t.Fatalf("the policy was asked about %v, want the claim to be about usr_example too", got)
	}
}

// policyFunc is an AttachmentPolicy from a function, for the tests that care
// about the context rather than the answer.
type policyFunc func(context.Context, control.AttachmentMode) error

func (f policyFunc) AuthorizeAttachment(ctx context.Context, _ control.Scope,
	_ control.Resource, mode control.AttachmentMode) error {
	return f(ctx, mode)
}

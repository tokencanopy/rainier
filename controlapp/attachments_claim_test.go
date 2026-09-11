package controlapp

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"testing"
	"time"

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

// TestARunnersValueCannotFillAGapInTheIdentity is the sharp edge of "values
// from the authorizing context ALONE". A merge that fell back to the live call
// for a key the identity did not carry would look harmless and would hand the
// runner's dial-back context the casting vote on every question the attach
// itself left unanswered.
func TestARunnersValueCannotFillAGapInTheIdentity(t *testing.T) {
	fx := newAttachmentFixture(t)
	policy := &identityPolicy{allow: "usr_example"}
	// An authorizing context that names nobody — a host that authorizes from
	// the scope alone — and a live call that names the principal the policy
	// would allow.
	k := claimKeeper(fx, policy, context.WithValue(context.Background(), struct{ unrelated int }{}, 1))

	if _, err := k.Claim(withTestPrincipal(context.Background(), "usr_example"),
		fx.sessions.row.ControllerGeneration); !errors.Is(err, control.ErrDenied) {
		t.Fatalf("Claim whose identity carries nobody: err = %v, want ErrDenied", err)
	}
	if !slices.Equal(policy.asked, []string{""}) {
		t.Fatalf("the policy was asked about %v, want one question about nobody", policy.asked)
	}
}

// TestAClaimIsCancelledWithTheCallThatAskedIt pins the other half of the
// split: a policy that is a network call must not outlive the claim. Both the
// already-over case and the cancelled-while-asking case, because they take
// different paths — one is answered synchronously and one arrives through the
// context package's own callback.
func TestAClaimIsCancelledWithTheCallThatAskedIt(t *testing.T) {
	fx := newAttachmentFixture(t)

	t.Run("already over", func(t *testing.T) {
		var seen error
		k := claimKeeper(fx, policyFunc(func(ctx context.Context, _ control.AttachmentMode) error {
			seen = ctx.Err()
			return ctx.Err()
		}), withTestPrincipal(context.Background(), "usr_example"))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := k.Claim(ctx, fx.sessions.row.ControllerGeneration); !errors.Is(err, control.ErrDenied) {
			t.Fatalf("Claim on a cancelled call: err = %v, want ErrDenied", err)
		}
		if !errors.Is(seen, context.Canceled) {
			t.Fatalf("the policy saw ctx.Err() = %v, want context.Canceled", seen)
		}
	})

	t.Run("cancelled while asking", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var seen error
		k := claimKeeper(fx, policyFunc(func(asked context.Context, _ control.AttachmentMode) error {
			// The claim is abandoned mid-question, which is what a client
			// hanging up looks like from in here.
			cancel()
			select {
			case <-asked.Done():
				seen = asked.Err()
			case <-time.After(5 * time.Second):
				seen = errors.New("the authorizing context never followed the live call")
			}
			return seen
		}), withTestPrincipal(context.Background(), "usr_example"))
		if _, err := k.Claim(ctx, fx.sessions.row.ControllerGeneration); !errors.Is(err, control.ErrDenied) {
			t.Fatalf("Claim abandoned mid-question: err = %v, want ErrDenied", err)
		}
		// ErrDenied alone would also be the answer to a policy that timed out
		// on its own; what this pins is that the live call's cancellation
		// reached the question.
		if !errors.Is(seen, context.Canceled) {
			t.Fatalf("the policy saw %v, want context.Canceled from the live call", seen)
		}
	})

	t.Run("deadline", func(t *testing.T) {
		var got time.Time
		k := claimKeeper(fx, policyFunc(func(asked context.Context, _ control.AttachmentMode) error {
			got, _ = asked.Deadline()
			return nil
		}), withTestPrincipal(context.Background(), "usr_example"))
		want := time.Now().Add(time.Hour)
		ctx, cancel := context.WithDeadline(context.Background(), want)
		defer cancel()
		if _, err := k.Claim(ctx, fx.sessions.row.ControllerGeneration); err != nil {
			t.Fatalf("Claim under a deadline: %v", err)
		}
		if !got.Equal(want) {
			t.Fatalf("the policy saw deadline %v, want the live call's %v", got, want)
		}
	})
}

// TestAnAuthorizingContextCostsNoGoroutinePerDerivation is the cost of the
// shape, pinned. A host's policy derives its own cancelable context — every
// one that makes a network call does — and an authorizing context that hid
// the cancellation parent would make each of those spawn a goroutine to watch
// it instead of registering as a child.
func TestAnAuthorizingContextCostsNoGoroutinePerDerivation(t *testing.T) {
	fx := newAttachmentFixture(t)
	k := claimKeeper(fx, &identityPolicy{allow: "usr_example"},
		withTestPrincipal(context.Background(), "usr_example"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	authCtx, release := k.authorizing(ctx)
	defer release()

	const derivations = 1000
	before := runtime.NumGoroutine()
	cancels := make([]context.CancelFunc, 0, derivations)
	for range derivations {
		_, c := context.WithCancel(authCtx)
		cancels = append(cancels, c)
	}
	grew := runtime.NumGoroutine() - before
	for _, c := range cancels {
		c()
	}
	// Generous: what this is looking for is one goroutine PER derivation, and
	// the noise from other tests in this binary is two orders smaller.
	if grew > derivations/5 {
		t.Fatalf("%d cancelable contexts derived from an authorizing context grew %d goroutines; "+
			"they must register as children rather than each watch a parent", derivations, grew)
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

// storeCallKey marks the live call's context so a test can tell which context
// a store call was made on.
type storeCallKey struct{}

// TestAClaimsStoreCallRunsOnTheCallThatMadeIt pins the half of authorizing
// that the mutants did not reach: the policy question carries the captured
// identity's values, but the STORE call carries the live call's context. A
// host repository that reads a request-scoped value (a transaction handle, a
// trace id) must see the call it is serving, not the attach that opened the
// keeper hours ago.
func TestAClaimsStoreCallRunsOnTheCallThatMadeIt(t *testing.T) {
	fx := newAttachmentFixture(t)
	var askedOn context.Context
	k := claimKeeper(fx, policyFunc(func(asked context.Context, _ control.AttachmentMode) error {
		askedOn = asked
		return nil
	}), withTestPrincipal(context.Background(), "usr_example"))

	live := context.WithValue(context.Background(), storeCallKey{}, "this call")
	if _, err := k.Claim(live, fx.sessions.row.ControllerGeneration); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	fx.sessions.mu.Lock()
	storeSaw := fx.sessions.casCtx
	fx.sessions.mu.Unlock()
	if storeSaw == nil || storeSaw.Value(storeCallKey{}) != "this call" {
		t.Fatal("the store's conditional claim did not run on the live call's context")
	}
	if askedOn == nil || askedOn.Value(storeCallKey{}) != nil {
		t.Fatal("the policy question carried the live call's value; it must carry the captured identity's alone")
	}
	if askedOn.Value(testPrincipalKey{}) != "usr_example" {
		t.Fatalf("the policy was asked about %v, want the captured usr_example", askedOn.Value(testPrincipalKey{}))
	}
}

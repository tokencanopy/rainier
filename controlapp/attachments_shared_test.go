package controlapp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// The shared attachment policy, at the grant: every attach that asked to type
// is a typer at the generation the row already carries, and nothing advances
// it. The exclusive rule is tested by the rest of this package, which composes
// control.PolicyExclusive explicitly for that reason.

// TestSharedPolicyGrantsEveryTyperTheCurrentGeneration is acceptance rule 1:
// two negotiated attaches both come back `control`, at the same generation,
// and the store is never asked to advance one.
func TestSharedPolicyGrantsEveryTyperTheCurrentGeneration(t *testing.T) {
	fx := newSharedAttachmentFixture(t)
	fx.sessions.row.ControllerGeneration = 9

	for i := 0; i < 2; i++ {
		if err := fx.svc.AttachTerminal(context.Background(), attachmentTestScope(), control.AttachTerminal{
			SessionID: "sess_example", Mode: control.AttachmentController, Negotiated: true,
		}, &attachmentRecordingTerminalStream{}); err != nil {
			t.Fatalf("attach %d: %v", i, err)
		}
	}
	if len(fx.broker.targets) != 2 {
		t.Fatalf("broker saw %d attaches, want 2", len(fx.broker.targets))
	}
	for i, got := range fx.broker.targets {
		if got.Mode != control.AttachmentController {
			t.Fatalf("attach %d granted %q, want controller", i, got.Mode)
		}
		if got.ControllerGeneration != 9 {
			t.Fatalf("attach %d granted generation %d, want the row's 9", i, got.ControllerGeneration)
		}
		if got.Policy != control.PolicyShared {
			t.Fatalf("attach %d carried policy %q, want shared", i, got.Policy)
		}
		if got.Controller == nil || !got.Negotiated {
			t.Fatalf("attach %d: a negotiated attach must carry a keeper: %+v", i, got)
		}
	}
	if n := fx.sessions.compareAndAdvanceCalls(); n != 0 {
		t.Fatalf("the generation was advanced %d times under a shared policy", n)
	}
	if fx.sessions.nextCalls != 0 {
		t.Fatalf("NextControllerGeneration was called %d times under a shared policy", fx.sessions.nextCalls)
	}
	if fx.sessions.renewCalls != 0 {
		t.Fatalf("a lease was renewed %d times under a shared policy", fx.sessions.renewCalls)
	}
}

// TestSharedPolicyDoesNotAdvanceForALegacyAttach is the compatibility half of
// the same rule. Under the exclusive policy an unnegotiated attach is recorded
// as a take-over, because a client that cannot be told it is a viewer cannot be
// made one; under shared there is nobody to displace, so advancing would fence
// the other typers for nothing.
func TestSharedPolicyDoesNotAdvanceForALegacyAttach(t *testing.T) {
	fx := newSharedAttachmentFixture(t)
	fx.sessions.row.ControllerGeneration = 4

	if err := fx.svc.AttachTerminal(context.Background(), attachmentTestScope(), control.AttachTerminal{
		SessionID: "sess_example", Mode: control.AttachmentController,
	}, &attachmentRecordingTerminalStream{}); err != nil {
		t.Fatalf("legacy attach: %v", err)
	}
	got := fx.broker.lastTarget
	switch {
	case got.Mode != control.AttachmentController:
		t.Fatalf("a legacy attach was granted %q, want controller", got.Mode)
	case got.ControllerGeneration != 4:
		t.Fatalf("granted generation %d, want the row's 4", got.ControllerGeneration)
	case got.Negotiated || got.Controller != nil || got.MayClaim:
		t.Fatalf("a legacy attach must carry no keeper and no privilege: %+v", got)
	}
	if fx.sessions.nextCalls != 0 || fx.sessions.compareAndAdvanceCalls() != 0 {
		t.Fatalf("a legacy attach advanced the generation: next=%d cas=%d",
			fx.sessions.nextCalls, fx.sessions.compareAndAdvanceCalls())
	}
}

// TestSharedPolicyKeepsAViewerAViewer: --view means never type, under either
// policy, and a viewer still reads the current generation.
func TestSharedPolicyKeepsAViewerAViewer(t *testing.T) {
	fx := newSharedAttachmentFixture(t)
	fx.sessions.row.ControllerGeneration = 3

	if err := fx.svc.AttachTerminal(context.Background(), attachmentTestScope(), control.AttachTerminal{
		SessionID: "sess_example", Mode: control.AttachmentViewer, Negotiated: true,
	}, &attachmentRecordingTerminalStream{}); err != nil {
		t.Fatalf("view attach: %v", err)
	}
	got := fx.broker.lastTarget
	if got.Mode != control.AttachmentViewer || got.ControllerGeneration != 3 {
		t.Fatalf("view attach granted %q at %d, want viewer at 3", got.Mode, got.ControllerGeneration)
	}
	if got.Policy != control.PolicyShared {
		t.Fatalf("view attach carried policy %q, want shared", got.Policy)
	}
}

// TestSharedPolicyIgnoresALiveLease: a lease left live by an earlier exclusive
// attach — or by a host that changed its policy while a session was up — does
// not turn a shared attach into a viewer. The lease is the exclusivity hint,
// and under this policy there is nothing to hint at.
func TestSharedPolicyIgnoresALiveLease(t *testing.T) {
	fx := newSharedAttachmentFixture(t)
	fx.sessions.row.ControllerGeneration = 11
	fx.sessions.row.ControllerHolder = "att_example"
	fx.sessions.row.ControllerLeaseExpiresAt = fx.now.Add(30 * time.Second)

	if err := fx.svc.AttachTerminal(context.Background(), attachmentTestScope(), control.AttachTerminal{
		SessionID: "sess_example", Mode: control.AttachmentController, Negotiated: true,
	}, &attachmentRecordingTerminalStream{}); err != nil {
		t.Fatalf("attach: %v", err)
	}
	if got := fx.broker.lastTarget; got.Mode != control.AttachmentController || got.ControllerGeneration != 11 {
		t.Fatalf("granted %q at %d, want controller at 11", got.Mode, got.ControllerGeneration)
	}
	if n := fx.sessions.compareAndAdvanceCalls(); n != 0 {
		t.Fatalf("the generation was advanced %d times over a live lease", n)
	}
}

// TestExclusivePolicyStillAdvancesOnAttach states the other side of the switch
// in one place: the same fixture, the same command, the other policy, and
// today's take-over.
func TestExclusivePolicyStillAdvancesOnAttach(t *testing.T) {
	fx := newAttachmentFixture(t) // control.PolicyExclusive
	fx.sessions.row.ControllerGeneration = 4

	if err := fx.svc.AttachTerminal(context.Background(), attachmentTestScope(), control.AttachTerminal{
		SessionID: "sess_example", Mode: control.AttachmentController, Negotiated: true,
	}, &attachmentRecordingTerminalStream{}); err != nil {
		t.Fatalf("attach: %v", err)
	}
	if got := fx.broker.lastTarget; got.ControllerGeneration != 5 || got.Policy != control.PolicyExclusive {
		t.Fatalf("granted generation %d under policy %q, want 5 under exclusive",
			got.ControllerGeneration, got.Policy)
	}
	if n := fx.sessions.compareAndAdvanceCalls(); n != 1 {
		t.Fatalf("the generation was advanced %d times, want once", n)
	}
}

// TestUnnamedPolicyIsShared pins the default itself: a host that composes the
// service without naming a policy gets shared, and the target says so rather
// than carrying an empty word onward.
func TestUnnamedPolicyIsShared(t *testing.T) {
	fx := newAttachmentFixturePolicy(t, "")
	fx.sessions.row.ControllerGeneration = 2

	if err := fx.svc.AttachTerminal(context.Background(), attachmentTestScope(), control.AttachTerminal{
		SessionID: "sess_example", Mode: control.AttachmentController, Negotiated: true,
	}, &attachmentRecordingTerminalStream{}); err != nil {
		t.Fatalf("attach: %v", err)
	}
	got := fx.broker.lastTarget
	if got.Policy != control.PolicyShared || got.ControllerGeneration != 2 {
		t.Fatalf("granted policy %q at generation %d, want shared at 2", got.Policy, got.ControllerGeneration)
	}
}

// TestNewAttachmentServiceRejectsAnUnknownPolicy: a misspelled policy is a
// composition error, caught where the application is built rather than at the
// first attach, exactly as a missing dependency is.
func TestNewAttachmentServiceRejectsAnUnknownPolicy(t *testing.T) {
	_, err := NewAttachmentService(AttachmentOptions{
		Authorizer: &attachmentFakeAuthorizer{},
		Policy:     &attachmentFakePolicy{},
		Sessions:   &attachmentFakeSessions{},
		Transport:  &attachmentFakeTransport{},
		Broker:     &attachmentFakeBroker{},
		Events:     &attachmentFakeEvents{},
		Clock:      attachmentFakeClock(func() time.Time { return time.Unix(0, 0) }),
		IDs:        attachmentFakeIDs{eventID: "evt_example"},
		UnitOfWork: directUOW{},
		// Not "shared", not "exclusive", not empty.
		InputPolicy: control.InputPolicy("Shared"),
	})
	if !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("got %v, want ErrInvalid", err)
	}
}

// TestSharedPolicyStillRefusesAnUnnegotiatedControllerThePolicyDenies: the
// attachment policy — the AUTHORIZATION seam — is untouched by the input
// policy. A principal a host grants viewing and not driving is still admitted
// as a viewer when it negotiated, and still refused when it did not.
func TestSharedPolicyStillRefusesAnUnnegotiatedControllerThePolicyDenies(t *testing.T) {
	fx := newSharedAttachmentFixture(t)
	fx.policy.deny = map[control.AttachmentMode]bool{control.AttachmentController: true}

	err := fx.svc.AttachTerminal(context.Background(), attachmentTestScope(), control.AttachTerminal{
		SessionID: "sess_example", Mode: control.AttachmentController,
	}, &attachmentRecordingTerminalStream{})
	if !errors.Is(err, control.ErrDenied) {
		t.Fatalf("got %v, want ErrDenied", err)
	}

	fx2 := newSharedAttachmentFixture(t)
	fx2.policy.deny = map[control.AttachmentMode]bool{control.AttachmentController: true}
	if err := fx2.svc.AttachTerminal(context.Background(), attachmentTestScope(), control.AttachTerminal{
		SessionID: "sess_example", Mode: control.AttachmentController, Negotiated: true,
	}, &attachmentRecordingTerminalStream{}); err != nil {
		t.Fatalf("negotiated attach: %v", err)
	}
	if got := fx2.broker.lastTarget; got.Mode != control.AttachmentViewer || got.MayClaim {
		t.Fatalf("granted %+v, want a viewer that may not claim", got)
	}
}

// TestInputPolicyReadings pins the contract type's three questions, which is
// where the default lives for every consumer.
func TestInputPolicyReadings(t *testing.T) {
	for _, tc := range []struct {
		p        control.InputPolicy
		shared   bool
		valid    bool
		resolved control.InputPolicy
	}{
		{"", true, true, control.PolicyShared},
		{control.PolicyShared, true, true, control.PolicyShared},
		{control.PolicyExclusive, false, true, control.PolicyExclusive},
		{control.InputPolicy("nonsense"), true, false, control.InputPolicy("nonsense")},
	} {
		if got := tc.p.Shared(); got != tc.shared {
			t.Fatalf("%q.Shared() = %v, want %v", tc.p, got, tc.shared)
		}
		if got := tc.p.Valid(); got != tc.valid {
			t.Fatalf("%q.Valid() = %v, want %v", tc.p, got, tc.valid)
		}
		if got := tc.p.Resolved(); got != tc.resolved {
			t.Fatalf("%q.Resolved() = %q, want %q", tc.p, got, tc.resolved)
		}
	}
	// Spelled out so a reader of the wire and the flag sees the same words.
	if control.PolicyShared != "shared" || control.PolicyExclusive != "exclusive" {
		t.Fatalf("the policy words changed: %q, %q", control.PolicyShared, control.PolicyExclusive)
	}
	// And a viewer mode is still a viewer mode: the two enumerations are
	// different questions and must not collide on a value.
	if string(control.PolicyShared) == string(terminal.ModeControl) {
		t.Fatal("a policy and a mode share a word")
	}
}

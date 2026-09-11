package session

import (
	"testing"
	"time"

	"github.com/tokencanopy/rainier/protocol/terminal"
)

func controllerAt(gen uint64) Binding {
	return Binding{Bound: true, Mode: terminal.ModeControl, Generation: gen}
}

func viewerAt(gen uint64) Binding {
	return Binding{Bound: true, Mode: terminal.ModeView, Generation: gen}
}

// TestViewerInputNeverReachesTheProcess is the other half of "everyone else is
// a viewer": a viewer's keystrokes are dropped at the process, not merely
// discouraged at the plane. Bytes a terminal sends back in answer to a query
// arrive as stdin like anything else, so they are covered by the same rule
// and need none of their own.
func TestViewerInputNeverReachesTheProcess(t *testing.T) {
	s, fp := newFakeSession(t)
	controller, err := s.Attach(0, Size{80, 24}, controllerAt(1))
	if err != nil {
		t.Fatal(err)
	}
	viewer, err := s.Attach(0, Size{80, 24}, viewerAt(1))
	if err != nil {
		t.Fatal(err)
	}
	recv(t, controller.Msgs)
	recv(t, viewer.Msgs)

	if s.Stdin(viewer.ID, 1, []byte("rm -rf /\r")) {
		t.Fatal("a viewer's keystroke was executed")
	}
	// A terminal query response — the same door, the same answer.
	if s.Stdin(viewer.ID, 1, []byte("\x1b[24;80R")) {
		t.Fatal("a viewer's terminal query response was executed")
	}
	if !s.Stdin(controller.ID, 1, []byte("ls\r")) {
		t.Fatal("the controller's keystroke was fenced")
	}
	select {
	case got := <-fp.stdin:
		if string(got) != "ls\r" {
			t.Fatalf("the process received %q; the viewer's bytes got through", got)
		}
	case <-time.After(time.Second):
		t.Fatal("the controller's keystroke never reached the process")
	}
}

// TestADisplacedControllersInFlightFrameIsDiscarded is journey step 4, at the
// only place that can actually make the promise. The frame was sent under
// generation 1, was already past the relay when the take-over landed, and
// arrives at a session that is now at generation 2. It must not execute.
func TestADisplacedControllersInFlightFrameIsDiscarded(t *testing.T) {
	s, fp := newFakeSession(t)
	laptop, err := s.Attach(0, Size{80, 24}, controllerAt(1))
	if err != nil {
		t.Fatal(err)
	}
	phone, err := s.Attach(0, Size{80, 24}, viewerAt(1))
	if err != nil {
		t.Fatal(err)
	}
	recv(t, laptop.Msgs)
	recv(t, phone.Msgs)

	// The phone takes control. The laptop has not been told anything yet —
	// its own binding still says it is the controller at generation 1 — and
	// that is exactly the window this test is about.
	if !s.Bind(phone.ID, controllerAt(2)) {
		t.Fatal("the take-over did not install")
	}
	if s.Stdin(laptop.ID, 1, []byte("y\r")) {
		t.Fatal("a keystroke sent under the superseded generation executed")
	}
	// And the laptop may take it back the same way.
	if !s.Bind(laptop.ID, controllerAt(3)) {
		t.Fatal("taking control back did not install")
	}
	if !s.Stdin(laptop.ID, 3, []byte("y\r")) {
		t.Fatal("the laptop could not take control back")
	}
	select {
	case got := <-fp.stdin:
		if string(got) != "y\r" {
			t.Fatalf("the process received %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("nothing reached the process")
	}
	// The phone, now displaced in its turn, is fenced without being told.
	if s.Stdin(phone.ID, 2, []byte("n\r")) {
		t.Fatal("the phone kept writing after it was displaced")
	}
}

// TestAFrameStampedWithNoGenerationIsFencedOnABoundAttachment pins the
// stamping contract from the sandbox's side: a plane that binds an attachment
// stamps every frame it forwards for it, so an unstamped frame on a bound
// attachment is not a legacy client — it is a frame that lost its provenance,
// and it does not execute.
func TestAFrameStampedWithNoGenerationIsFencedOnABoundAttachment(t *testing.T) {
	s, _ := newFakeSession(t)
	a, err := s.Attach(0, Size{80, 24}, controllerAt(1))
	if err != nil {
		t.Fatal(err)
	}
	recv(t, a.Msgs)
	if s.Stdin(a.ID, 0, []byte("x")) {
		t.Fatal("an unstamped frame executed on a bound attachment")
	}
}

// TestAnUnboundAttachmentIsUnconditional is the new-sandbox + old-plane
// pairing: a plane that grants no binding gets today's behaviour, because
// under the old message set only one client could have been sending.
func TestAnUnboundAttachmentIsUnconditional(t *testing.T) {
	s, fp := newFakeSession(t)
	a, err := s.Attach(0, Size{80, 24}, Binding{})
	if err != nil {
		t.Fatal(err)
	}
	recv(t, a.Msgs)
	if !s.Stdin(a.ID, 0, []byte("ls\r")) {
		t.Fatal("an old plane's input was fenced; it carries no generation and never will")
	}
	select {
	case <-fp.stdin:
	case <-time.After(time.Second):
		t.Fatal("nothing reached the process")
	}
	if !s.SetSize(a.ID, 0, Size{100, 30}) {
		t.Fatal("an old plane's resize was fenced")
	}
}

// TestThePtySizeFollowsTheController is journey step 7: a phone attaching as
// a viewer must not squeeze the laptop's terminal down to phone width, and a
// viewer that later takes control must bring its own size with it.
func TestThePtySizeFollowsTheController(t *testing.T) {
	s, fp := newFakeSession(t)
	laptop, err := s.Attach(0, Size{120, 40}, controllerAt(1))
	if err != nil {
		t.Fatal(err)
	}
	recv(t, laptop.Msgs)
	if got := <-fp.resizes; got != (Size{120, 40}) {
		t.Fatalf("the controller's attach sized the pty %+v, want {120 40}", got)
	}

	phone, err := s.Attach(0, Size{40, 20}, viewerAt(1))
	if err != nil {
		t.Fatal(err)
	}
	recv(t, phone.Msgs)
	if s.SetSize(phone.ID, 1, Size{40, 20}) {
		t.Fatal("a viewer's resize was applied")
	}
	select {
	case got := <-fp.resizes:
		t.Fatalf("a viewer moved the pty to %+v", got)
	case <-time.After(50 * time.Millisecond):
	}

	// The phone takes control, and the pty follows it.
	if !s.Bind(phone.ID, controllerAt(2)) {
		t.Fatal("the take-over did not install")
	}
	select {
	case got := <-fp.resizes:
		if got != (Size{40, 20}) {
			t.Fatalf("after the handoff the pty is %+v, want the new controller's {40 20}", got)
		}
	case <-time.After(time.Second):
		t.Fatal("the pty did not follow the new controller")
	}
}

// TestTheFenceOnlyEverGoesUp pins that a late frame carrying a superseded
// binding cannot un-displace the controller that superseded it — which is the
// one way a monotonic fence could be walked backwards.
func TestTheFenceOnlyEverGoesUp(t *testing.T) {
	s, _ := newFakeSession(t)
	a, err := s.Attach(0, Size{80, 24}, controllerAt(5))
	if err != nil {
		t.Fatal(err)
	}
	recv(t, a.Msgs)
	b, err := s.Attach(0, Size{80, 24}, controllerAt(2)) // a stale binding, arriving late
	if err != nil {
		t.Fatal(err)
	}
	recv(t, b.Msgs)
	if s.Stdin(b.ID, 2, []byte("x")) {
		t.Fatal("a stale binding took control by arriving late")
	}
	if !s.Stdin(a.ID, 5, []byte("x")) {
		t.Fatal("the live controller was displaced by a stale binding")
	}
}

// TestAnUnboundAttachmentCannotBindItself is the defence at the pty behind
// the plane's own refusal to carry a client's `control`. A binding travels on
// the frame that OPENS an attachment; an attachment that was opened without
// one is talking to something that grants no bindings at all — an older
// plane, or a runner's local debugging attach, which has no control plane
// above it. A handoff arriving on such an attachment did not come from a
// plane, and honouring it would let whatever is on the other end name its own
// mode and its own generation.
//
// The generation below is the one that makes this more than tidiness: a fence
// only ever goes up, so accepting it once would leave every attachment on the
// session — including the one that was typing — unable to write for the life
// of the process.
func TestAnUnboundAttachmentCannotBindItself(t *testing.T) {
	s, fp := newFakeSession(t)
	a, err := s.Attach(0, Size{80, 24}, Binding{})
	if err != nil {
		t.Fatal(err)
	}
	recv(t, a.Msgs)

	if s.Bind(a.ID, controllerAt(^uint64(0))) {
		t.Fatal("an attachment nobody bound bound itself")
	}
	// The fence did not move, so the attachment still writes exactly as an
	// old plane's attachment always has.
	if !s.Stdin(a.ID, 0, []byte("ls\r")) {
		t.Fatal("a forged binding wedged an unconditional attachment")
	}
	select {
	case got := <-fp.stdin:
		if string(got) != "ls\r" {
			t.Fatalf("the process received %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("nothing reached the process")
	}
}

// TestARePromotedControllersOlderFrameIsStillDiscarded is the clause of the
// fence that nothing else reaches. A controller that was displaced and then
// took control back holds a binding at the CURRENT generation, so the rule
// about its binding says yes; the only thing standing between the shell and a
// keystroke it typed two generations ago — one that was in flight while
// somebody else had control — is that the frame carries the generation it was
// sent under and that generation is gone.
func TestARePromotedControllersOlderFrameIsStillDiscarded(t *testing.T) {
	s, fp := newFakeSession(t)
	laptop, err := s.Attach(0, Size{80, 24}, controllerAt(1))
	if err != nil {
		t.Fatal(err)
	}
	recv(t, laptop.Msgs)

	// Displaced, then back: the laptop is the controller again, at 3.
	if !s.Bind(laptop.ID, viewerAt(2)) {
		t.Fatal("the demotion did not install")
	}
	if !s.Bind(laptop.ID, controllerAt(3)) {
		t.Fatal("taking control back did not install")
	}

	if s.Stdin(laptop.ID, 1, []byte("y\r")) {
		t.Fatal("a keystroke typed two generations ago executed on its sender's return")
	}
	if s.SetSize(laptop.ID, 1, Size{40, 20}) {
		t.Fatal("a resize sent two generations ago moved the pty on its sender's return")
	}
	if !s.Stdin(laptop.ID, 3, []byte("ls\r")) {
		t.Fatal("the re-promoted controller cannot type under the generation it holds")
	}
	select {
	case got := <-fp.stdin:
		if string(got) != "ls\r" {
			t.Fatalf("the process received %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("nothing reached the process")
	}
}

package session

import (
	"testing"
	"time"
)

// The pty under a shared attachment policy: several attachments bound
// `control` at the same generation. The session is not told the policy and
// does not need to be — its rule is the same one it has always had — so these
// tests pin what that rule DOES when more than one attachment is entitled at
// once. See docs/design/2026-09-12-shared-attachment-policy.md.

// TestTwoTypersAtOneGenerationBothExecuteAndOlderFramesAreDropped is the
// fence's whole behaviour under the shared policy: peers at the current
// generation are unfenced from each other, and an attachment left behind at
// N-1 — one the plane revoked, or whose frame was in flight across a
// generation move — is still discarded where it would have executed.
func TestTwoTypersAtOneGenerationBothExecuteAndOlderFramesAreDropped(t *testing.T) {
	s, fp := newFakeSession(t)

	laptop, err := s.Attach(0, Size{80, 24}, controllerAt(7))
	if err != nil {
		t.Fatal(err)
	}
	recv(t, laptop.Msgs)
	browser, err := s.Attach(0, Size{80, 24}, controllerAt(7))
	if err != nil {
		t.Fatal(err)
	}
	recv(t, browser.Msgs)

	// Both are the controller at 7, and the fence is a no-op between them.
	if !s.Stdin(laptop.ID, 7, []byte("one\r")) {
		t.Fatal("the laptop could not type at the generation it holds")
	}
	if !s.Stdin(browser.ID, 7, []byte("two\r")) {
		t.Fatal("the browser could not type at the generation it holds")
	}
	assertProcRead(t, fp, "one\r")
	assertProcRead(t, fp, "two\r")

	// A third attachment the plane revoked: its binding was re-installed as a
	// viewer, and the session's fence moved to 8 with it. That is the "reason
	// that is not a peer" the shared policy keeps control_changed for.
	revoked, err := s.Attach(0, Size{80, 24}, controllerAt(7))
	if err != nil {
		t.Fatal(err)
	}
	recv(t, revoked.Msgs)
	if !s.Bind(revoked.ID, viewerAt(8)) {
		t.Fatal("the revocation did not install")
	}

	// Its frame, sent under 7 and arriving after the move, is dropped.
	if s.Stdin(revoked.ID, 7, []byte("rm -rf /\r")) {
		t.Fatal("a revoked attachment's frame executed at the superseded generation")
	}
	// And so is a frame at 7 from an attachment that still believes 7 is
	// current: the session has moved on, and the fence only ever goes up.
	if s.Stdin(laptop.ID, 7, []byte("stale\r")) {
		t.Fatal("a frame at the superseded generation executed")
	}
	assertNothingReachedProc(t, fp)

	// The two live typers, re-bound at the generation that exists now, are
	// unfenced from each other again.
	if !s.Bind(laptop.ID, controllerAt(8)) || !s.Bind(browser.ID, controllerAt(8)) {
		t.Fatal("re-binding the live typers did not install")
	}
	if !s.Stdin(laptop.ID, 8, []byte("three\r")) || !s.Stdin(browser.ID, 8, []byte("four\r")) {
		t.Fatal("a live typer was fenced at the current generation")
	}
	assertProcRead(t, fp, "three\r")
	assertProcRead(t, fp, "four\r")
}

// TestThePtyFollowsTheLatestTyperAndIgnoresViewers is the resize rule, with the
// two typers and the viewer the acceptance criteria name.
func TestThePtyFollowsTheLatestTyperAndIgnoresViewers(t *testing.T) {
	s, fp := newFakeSession(t)

	laptop, err := s.Attach(0, Size{120, 40}, controllerAt(1))
	if err != nil {
		t.Fatal(err)
	}
	recv(t, laptop.Msgs)
	if got := <-fp.resizes; got != (Size{120, 40}) {
		t.Fatalf("the first typer's attach sized the pty %+v, want {120 40}", got)
	}

	// A second typer, narrower. Its attach is a size report like any other, so
	// the pty follows it rather than intersecting the two.
	browser, err := s.Attach(0, Size{80, 24}, controllerAt(1))
	if err != nil {
		t.Fatal(err)
	}
	recv(t, browser.Msgs)
	if got := <-fp.resizes; got != (Size{80, 24}) {
		t.Fatalf("the pty is %+v, want the latest typer's {80 24}", got)
	}

	// The first typer resizes: latest wins, and it is the WHOLE size, not the
	// smaller of each axis.
	if !s.SetSize(laptop.ID, 1, Size{200, 60}) {
		t.Fatal("a typer's resize was fenced")
	}
	if got := <-fp.resizes; got != (Size{200, 60}) {
		t.Fatalf("the pty is %+v, want the last resize received {200 60}", got)
	}

	// A viewer changes nothing, however small it is and however recently it
	// said so.
	phone, err := s.Attach(0, Size{40, 20}, viewerAt(1))
	if err != nil {
		t.Fatal(err)
	}
	recv(t, phone.Msgs)
	if s.SetSize(phone.ID, 1, Size{40, 20}) {
		t.Fatal("a viewer's resize was applied")
	}
	assertNoResize(t, fp)

	// And the typer that resized last still owns the size after the viewer's
	// arrival and departure.
	s.Detach(phone.ID)
	assertNoResize(t, fp)
}

// TestTheSizeFollowsTheRemainingTyperWhenOneLeaves: "latest" is a rule about
// live typers, so a departure hands the pty to the most recent resize among the
// attachments that are still there.
func TestTheSizeFollowsTheRemainingTyperWhenOneLeaves(t *testing.T) {
	s, fp := newFakeSession(t)

	laptop, err := s.Attach(0, Size{120, 40}, controllerAt(1))
	if err != nil {
		t.Fatal(err)
	}
	recv(t, laptop.Msgs)
	<-fp.resizes
	browser, err := s.Attach(0, Size{80, 24}, controllerAt(1))
	if err != nil {
		t.Fatal(err)
	}
	recv(t, browser.Msgs)
	if got := <-fp.resizes; got != (Size{80, 24}) {
		t.Fatalf("the pty is %+v, want {80 24}", got)
	}

	s.Detach(browser.ID)
	if got := <-fp.resizes; got != (Size{120, 40}) {
		t.Fatalf("after the latest typer left the pty is %+v, want the remaining typer's {120 40}", got)
	}
}

func assertProcRead(t *testing.T, fp *fakeProc, want string) {
	t.Helper()
	select {
	case got := <-fp.stdin:
		if string(got) != want {
			t.Fatalf("the process received %q, want %q", got, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("%q never reached the process", want)
	}
}

func assertNothingReachedProc(t *testing.T, fp *fakeProc) {
	t.Helper()
	select {
	case got := <-fp.stdin:
		t.Fatalf("%q reached the process", got)
	case <-time.After(50 * time.Millisecond):
	}
}

func assertNoResize(t *testing.T, fp *fakeProc) {
	t.Helper()
	select {
	case got := <-fp.resizes:
		t.Fatalf("the pty moved to %+v", got)
	case <-time.After(50 * time.Millisecond):
	}
}

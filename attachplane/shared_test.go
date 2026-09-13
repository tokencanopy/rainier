package attachplane

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// The shared attachment policy at the plane: several typers, one generation,
// and nothing that displaces anybody. Every other test in this package names
// control.PolicyExclusive, which is the model these are the counterpart to.

// startSharedAttach is startAttachWith over a target that names
// control.PolicyShared. It is spelled out here rather than threaded through
// that helper's six existing parameters so the exclusive suite keeps the
// signature it was written against and this file states its own premise in
// one place.
func startSharedAttach(t *testing.T, p *Plane, h *fakeHost, ts *httptest.Server,
	mode control.AttachmentMode, gen uint64, keeper control.ControllerLeaseKeeper, acks bool) *attachFixture {
	t.Helper()
	sandbox := newFakeSandbox(acks)
	h.dialBack = func(at *runner.Attach) { sandbox.serve(t, ts, at) }

	stream := newScriptedStream()
	stream.in <- terminal.ClientMessage{Type: "resize", Cols: 80, Rows: 24}
	target := brokerTarget("sess_example", "vm1")
	target.Mode = mode
	target.ControllerGeneration = gen
	target.Negotiated = keeper != nil
	target.MayClaim = keeper != nil && mode == control.AttachmentController
	target.Controller = keeper
	target.Policy = control.PolicyShared

	done := make(chan error, 1)
	go func() { done <- p.Broker().Attach(context.Background(), target, stream) }()
	f := &attachFixture{stream: stream, sandbox: sandbox, done: done}
	t.Cleanup(f.close)
	return f
}

// awaitSharedPumpCaughtUp is awaitPumpCaughtUp for a shared-policy typer,
// which answers a claim `attached` rather than `stale`: it is already a typer,
// so its claim is a question about what it has. Same purpose — make "nothing I
// typed was carried" an assertion about what happened rather than about how
// long the test waited.
func awaitSharedPumpCaughtUp(t *testing.T, f *attachFixture) {
	t.Helper()
	f.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(0)}
	awaitType(t, f.stream, terminal.TypeAttached)
}

// noOwnershipMessage fails the test if anything in the ownership vocabulary
// reaches this client within d. It is how "nobody was told they lost control"
// is asserted: the absence has to be bounded, and every other message type
// (output, snapshot) is allowed through.
func noOwnershipMessage(t *testing.T, f *attachFixture, d time.Duration) {
	t.Helper()
	deadline := time.After(d)
	for {
		select {
		case m := <-f.stream.out:
			switch m.Type {
			case terminal.TypeControlChanged, terminal.TypeStale:
				t.Fatalf("this client was sent %q (%s at %q) under a shared policy",
					m.Type, m.Mode, m.Generation)
			case terminal.TypeAttached:
				t.Fatalf("this client was sent a second %q (%s at %q)", m.Type, m.Mode, m.Generation)
			}
		case <-deadline:
			return
		}
	}
}

// TestSharedPolicyAdmitsTwoTypersAtOneGeneration is the acceptance rule: both
// attaches are told `control` at the SAME generation, both of their keystrokes
// are carried, in order, and neither is ever told it lost anything.
func TestSharedPolicyAdmitsTwoTypersAtOneGeneration(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{})
	lease := &fakeLease{gen: 5}

	laptop := startSharedAttach(t, p, h, ts, control.AttachmentController, 5, fakeKeeper{lease, "att_laptop"}, true)
	first, before := awaitType(t, laptop.stream, terminal.TypeAttached)
	if len(before) != 0 {
		t.Fatalf("%v reached the client before it was told its mode", before)
	}
	if first.Mode != terminal.ModeControl || first.Generation.Value() != 5 {
		t.Fatalf("the laptop was told %s at %q, want control at 5", first.Mode, first.Generation)
	}
	// The binding rode the command that opens the attachment, at the current
	// generation and not a new one.
	if cmd := h.nextCmd(t); cmd.Attach.Mode != terminal.ModeControl || cmd.Attach.Generation != 5 {
		t.Fatalf("dial_attach carried %s at %d, want control at 5", cmd.Attach.Mode, cmd.Attach.Generation)
	}
	awaitSpliced(t, laptop)

	browser := startSharedAttach(t, p, h, ts, control.AttachmentController, 5, fakeKeeper{lease, "att_browser"}, true)
	second, _ := awaitType(t, browser.stream, terminal.TypeAttached)
	if second.Mode != terminal.ModeControl || second.Generation.Value() != 5 {
		t.Fatalf("the browser was told %s at %q, want control at 5", second.Mode, second.Generation)
	}
	awaitSpliced(t, browser)

	// Both type, and both sandboxes execute what they sent — stamped with the
	// one generation they share, which is what makes the pty's fence a no-op
	// between them.
	laptop.stream.in <- terminal.ClientMessage{Type: "stdin", Data: []byte("a")}
	awaitSharedPumpCaughtUp(t, laptop)
	browser.stream.in <- terminal.ClientMessage{Type: "stdin", Data: []byte("b")}
	awaitSharedPumpCaughtUp(t, browser)

	assertTypedInOrder(t, laptop, 5, "a")
	assertTypedInOrder(t, browser, 5, "b")

	// Neither was told anything about the other's arrival, and the lease was
	// never touched: no claim, no renewal, no release.
	noOwnershipMessage(t, laptop, 200*time.Millisecond)
	noOwnershipMessage(t, browser, 200*time.Millisecond)
	lease.mu.Lock()
	gen, holder := lease.gen, lease.holder
	lease.mu.Unlock()
	if gen != 5 || holder != "" {
		t.Fatalf("the lease moved to generation %d held by %q under a shared policy", gen, holder)
	}
	if n := p.Typers("sess_example"); n != 2 {
		t.Fatalf("Typers = %d, want 2", n)
	}
}

// assertTypedInOrder pins what one attach's sandbox received: the stdin frames
// it sent, in the order it sent them, each stamped with the plane's generation.
//
// It WAITS for them rather than reading once. A claim answered on the client
// stream is not evidence that a keystroke forwarded before it has been read off
// the runner socket yet — the two travel on independent paths, which is the
// same fact control_ack exists for — so a single read here would fail
// probabilistically. Absence is the assertion that needs the pump's own
// catch-up; presence needs this.
func assertTypedInOrder(t *testing.T, f *attachFixture, gen uint64, want ...string) {
	t.Helper()
	awaitSandbox(t, f.sandbox, func(got []terminal.ClientMessage) bool {
		return len(stdinOf(got)) >= len(want)
	}, fmt.Sprintf("%d forwarded keystroke(s)", len(want)))
	frames := f.sandbox.received()
	var got []string
	for _, m := range frames {
		if m.Type != "stdin" {
			continue
		}
		if m.Generation.Value() != gen {
			t.Fatalf("a forwarded keystroke was stamped %q, want %d", m.Generation, gen)
		}
		got = append(got, string(m.Data))
	}
	if len(got) != len(want) {
		t.Fatalf("the sandbox executed %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("the sandbox executed %v, want %v", got, want)
		}
	}
}

// stdinOf is the keystroke frames out of everything one sandbox received.
func stdinOf(got []terminal.ClientMessage) []terminal.ClientMessage {
	var out []terminal.ClientMessage
	for _, m := range got {
		if m.Type == "stdin" {
			out = append(out, m)
		}
	}
	return out
}

// TestSharedPolicyStillDropsAViewersInputAndResize: --view means never type,
// under either policy. The third attach in the acceptance matrix.
func TestSharedPolicyStillDropsAViewersInputAndResize(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{})
	lease := &fakeLease{gen: 2}

	typer := startSharedAttach(t, p, h, ts, control.AttachmentController, 2, fakeKeeper{lease, "att_typer"}, true)
	awaitType(t, typer.stream, terminal.TypeAttached)
	awaitSpliced(t, typer)

	viewer := startSharedAttach(t, p, h, ts, control.AttachmentViewer, 2, fakeKeeper{lease, "att_viewer"}, true)
	m, _ := awaitType(t, viewer.stream, terminal.TypeAttached)
	if m.Mode != terminal.ModeView || m.Generation.Value() != 2 {
		t.Fatalf("the viewer was told %s at %q, want view at 2", m.Mode, m.Generation)
	}
	awaitViewerSpliced(t, viewer)

	viewer.stream.in <- terminal.ClientMessage{Type: "stdin", Data: []byte("rm -rf /\r")}
	viewer.stream.in <- terminal.ClientMessage{Type: "resize", Cols: 20, Rows: 5}
	// A viewer's claim is answered `stale`, which is also this pump's
	// catch-up: everything queued before it has been processed by then.
	viewer.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(2)}
	awaitType(t, viewer.stream, terminal.TypeStale)
	for _, got := range viewer.sandbox.received() {
		if got.Type == "stdin" || got.Type == "resize" {
			t.Fatalf("a viewer's %q was carried under a shared policy", got.Type)
		}
	}
	// And the typer heard nothing about it.
	noOwnershipMessage(t, typer, 200*time.Millisecond)
	if n := p.Typers("sess_example"); n != 1 {
		t.Fatalf("Typers = %d, want 1 (the viewer is not one)", n)
	}
}

// TestSharedPolicyClaimAnswersWithoutAdvancing is acceptance rule 2: a claim
// from a client that is already a typer is answered at the generation it holds,
// the store is never asked to advance, and no peer hears about it.
func TestSharedPolicyClaimAnswersWithoutAdvancing(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{})
	lease := &fakeLease{gen: 7}

	a := startSharedAttach(t, p, h, ts, control.AttachmentController, 7, fakeKeeper{lease, "att_a"}, true)
	awaitType(t, a.stream, terminal.TypeAttached)
	awaitSpliced(t, a)
	b := startSharedAttach(t, p, h, ts, control.AttachmentController, 7, fakeKeeper{lease, "att_b"}, true)
	awaitType(t, b.stream, terminal.TypeAttached)
	awaitSpliced(t, b)

	a.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(7)}
	m, _ := awaitType(t, a.stream, terminal.TypeAttached)
	if m.Mode != terminal.ModeControl || m.Generation.Value() != 7 {
		t.Fatalf("the claim was answered %s at %q, want control at 7", m.Mode, m.Generation)
	}
	lease.mu.Lock()
	gen := lease.gen
	lease.mu.Unlock()
	if gen != 7 {
		t.Fatalf("a claim advanced the generation to %d under a shared policy", gen)
	}
	noOwnershipMessage(t, b, 200*time.Millisecond)
	// The peer is still typing, which is the whole point of not advancing.
	b.stream.in <- terminal.ClientMessage{Type: "stdin", Data: []byte("still here")}
	awaitSharedPumpCaughtUp(t, b)
	assertTypedInOrder(t, b, 7, "still here")
}

// TestSharedPolicyReleaseIsANoOpForPeers is acceptance rule 2's other half: a
// release demotes its caller — which is the one case `control_changed` is
// still pushed for, and the case the hosted browser transport relies on — and
// changes nothing for anybody else.
func TestSharedPolicyReleaseIsANoOpForPeers(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{})
	lease := &fakeLease{gen: 3}

	leaving := startSharedAttach(t, p, h, ts, control.AttachmentController, 3, fakeKeeper{lease, "att_leaving"}, true)
	awaitType(t, leaving.stream, terminal.TypeAttached)
	awaitSpliced(t, leaving)
	staying := startSharedAttach(t, p, h, ts, control.AttachmentController, 3, fakeKeeper{lease, "att_staying"}, true)
	awaitType(t, staying.stream, terminal.TypeAttached)
	awaitSpliced(t, staying)

	leaving.stream.in <- terminal.ClientMessage{Type: terminal.TypeRelease}
	m, _ := awaitType(t, leaving.stream, terminal.TypeControlChanged)
	if m.Mode != terminal.ModeView || m.Generation.Value() != 3 {
		t.Fatalf("the releasing client was told %s at %q, want view at 3", m.Mode, m.Generation)
	}
	// Its own sandbox was told too, so a keystroke already in flight is
	// dropped at the pty rather than executed after the release.
	awaitSandbox(t, leaving.sandbox, func(got []terminal.ClientMessage) bool {
		for _, c := range got {
			if c.Type == terminal.TypeControl && c.Mode == terminal.ModeView {
				return true
			}
		}
		return false
	}, "the releasing attachment's view binding")

	lease.mu.Lock()
	gen := lease.gen
	lease.mu.Unlock()
	if gen != 3 {
		t.Fatalf("a release advanced the generation to %d under a shared policy", gen)
	}
	noOwnershipMessage(t, staying, 200*time.Millisecond)
	staying.stream.in <- terminal.ClientMessage{Type: "stdin", Data: []byte("unaffected")}
	awaitSharedPumpCaughtUp(t, staying)
	assertTypedInOrder(t, staying, 3, "unaffected")
	if n := p.Typers("sess_example"); n != 1 {
		t.Fatalf("Typers = %d, want 1 after a release", n)
	}
}

// TestSharedPolicyDepartureTellsNobodyAndReleasesNothing: a terminal closing
// is not a generation change. Under the exclusive policy the departing
// controller releases on its way out and every viewer is told the new number;
// under shared there is nothing to release and nobody to tell.
func TestSharedPolicyDepartureTellsNobodyAndReleasesNothing(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{})
	lease := &fakeLease{gen: 6}

	staying := startSharedAttach(t, p, h, ts, control.AttachmentController, 6, fakeKeeper{lease, "att_staying"}, true)
	awaitType(t, staying.stream, terminal.TypeAttached)
	awaitSpliced(t, staying)
	going := startSharedAttach(t, p, h, ts, control.AttachmentController, 6, fakeKeeper{lease, "att_going"}, true)
	awaitType(t, going.stream, terminal.TypeAttached)
	awaitSpliced(t, going)

	going.close()
	select {
	case <-going.done:
	case <-time.After(testDeadline):
		t.Fatal("the departing attach never finished")
	}

	lease.mu.Lock()
	gen := lease.gen
	lease.mu.Unlock()
	if gen != 6 {
		t.Fatalf("a departure advanced the generation to %d under a shared policy", gen)
	}
	noOwnershipMessage(t, staying, 200*time.Millisecond)
	staying.stream.in <- terminal.ClientMessage{Type: "stdin", Data: []byte("mine")}
	awaitSharedPumpCaughtUp(t, staying)
	assertTypedInOrder(t, staying, 6, "mine")
}

// TestSharedPolicyHeartbeatTakesNoLease: no lease was taken, so nothing renews
// one. Under the exclusive policy this same attach renews a lease six times a
// lease-length; here a renewal would install a holder nobody asked for and make
// `controller.held` true for a session with no controller. The loop still runs
// — it reads the generation, which is the next test — and what it must never do
// is write.
func TestSharedPolicyHeartbeatTakesNoLease(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{HeartbeatInterval: 5 * time.Millisecond})
	lease := &fakeLease{gen: 8}

	f := startSharedAttach(t, p, h, ts, control.AttachmentController, 8, fakeKeeper{lease, "att_only"}, true)
	awaitType(t, f.stream, terminal.TypeAttached)
	awaitSpliced(t, f)

	// Many heartbeat intervals.
	time.Sleep(150 * time.Millisecond)
	lease.mu.Lock()
	gen, holder := lease.gen, lease.holder
	lease.mu.Unlock()
	if gen != 8 || holder != "" {
		t.Fatalf("the lease reads generation %d held by %q; no heartbeat should have run", gen, holder)
	}
}

// TestSharedPolicyAttachWhoseGenerationMovesIsToldAndFenced is the case
// `displace` cannot reach: the generation moved somewhere this replica is not
// party to — a plane-side revocation, or an exclusive replica's take-over in a
// fleet straddling a policy change — and the only thing that can notice is this
// attach's own heartbeat.
//
// Without the read, such a terminal stops accepting typing with no notice and no
// key: its binding is dead at the pty, no peer list contains it, and under a
// shared policy nothing else asks the store anything.
func TestSharedPolicyAttachWhoseGenerationMovesIsToldAndFenced(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{HeartbeatInterval: 5 * time.Millisecond})
	lease := &fakeLease{gen: 5}

	f := startSharedAttach(t, p, h, ts, control.AttachmentController, 5, fakeKeeper{lease, "att_shared"}, true)
	if m, _ := awaitType(t, f.stream, terminal.TypeAttached); m.Generation.Value() != 5 {
		t.Fatalf("attached at %q, want 5", m.Generation)
	}
	awaitSpliced(t, f)

	// Something else advances the session: another replica's exclusive
	// take-over, or a revocation. This attach is told nothing by anybody.
	if _, err := (fakeKeeper{lease, "att_elsewhere"}).Claim(context.Background(), 5); err != nil {
		t.Fatal(err)
	}

	m, _ := awaitType(t, f.stream, terminal.TypeControlChanged)
	if m.Mode != terminal.ModeView || m.Generation.Value() != 6 {
		t.Fatalf("the fenced attach was told %s at %q, want view at 6", m.Mode, m.Generation)
	}
	// Its sandbox was told too, so the pty agrees with the plane.
	awaitSandbox(t, f.sandbox, func(got []terminal.ClientMessage) bool {
		for _, c := range got {
			if c.Type == terminal.TypeControl && c.Mode == terminal.ModeView {
				return true
			}
		}
		return false
	}, "the fenced attachment's view binding")
	// And the plane stops carrying what it types.
	f.stream.in <- terminal.ClientMessage{Type: "stdin", Data: []byte("gone")}
	f.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(6)}
	awaitType(t, f.stream, terminal.TypeStale)
	for _, got := range f.sandbox.received() {
		if got.Type == "stdin" {
			t.Fatal("a fenced attach's keystroke was still carried")
		}
	}
	// The demotion read the store and wrote nothing: the generation is the one
	// the OTHER claim won, not one this attach advanced.
	lease.mu.Lock()
	gen := lease.gen
	lease.mu.Unlock()
	if gen != 6 {
		t.Fatalf("the generation is %d, want the 6 somebody else won", gen)
	}
}

// TestSharedPolicyRefusesAClaimFromAViewerThatMayDrive is the guard the design
// doc's revocation argument rests on, and the target it needs is the one the
// application really builds for `rainier attach` from a principal a host grants
// viewing and not driving: AttachmentViewer with MayClaim SET, because the two
// questions are asked separately.
//
// Under the exclusive policy that client's claim is honoured — it is how a
// reconnecting controller admitted as a viewer takes its session back. Under
// shared it must not be, or a revoked attachment could re-promote itself by
// pressing one key, and revocation is the only thing left that moves a shared
// attach out of `control`.
func TestSharedPolicyRefusesAClaimFromAViewerThatMayDrive(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{})
	lease := &fakeLease{gen: 5}

	typer := startSharedAttach(t, p, h, ts, control.AttachmentController, 5, fakeKeeper{lease, "att_typer"}, true)
	awaitType(t, typer.stream, terminal.TypeAttached)
	awaitSpliced(t, typer)

	viewer := startSharedViewerThatMayDrive(t, p, h, ts, 5, fakeKeeper{lease, "att_viewer"})
	if m, _ := awaitType(t, viewer.stream, terminal.TypeAttached); m.Mode != terminal.ModeView {
		t.Fatalf("the view-only attach was told %s, want view", m.Mode)
	}
	awaitViewerSpliced(t, viewer)

	viewer.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(5)}
	m, _ := awaitType(t, viewer.stream, terminal.TypeStale)
	if m.Mode != "" || m.Generation.Value() != 5 {
		t.Fatalf("the claim was answered %s at %q, want a stale at 5", m.Mode, m.Generation)
	}
	lease.mu.Lock()
	gen, holder := lease.gen, lease.holder
	lease.mu.Unlock()
	if gen != 5 || holder != "" {
		t.Fatalf("a view-only claim moved the lease to generation %d held by %q", gen, holder)
	}
	// It still may not type, and the typer beside it heard nothing.
	viewer.stream.in <- terminal.ClientMessage{Type: "stdin", Data: []byte("no")}
	viewer.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(5)}
	awaitType(t, viewer.stream, terminal.TypeStale)
	for _, got := range viewer.sandbox.received() {
		if got.Type == "stdin" {
			t.Fatal("a refused claimant's keystroke was carried")
		}
	}
	noOwnershipMessage(t, typer, 200*time.Millisecond)
}

// startSharedViewerThatMayDrive is the target controlapp builds for a plain
// `attach` from a principal the host's policy admits as a viewer and would
// authorize as a controller: AttachmentViewer, negotiated, MayClaim true.
func startSharedViewerThatMayDrive(t *testing.T, p *Plane, h *fakeHost, ts *httptest.Server,
	gen uint64, keeper control.ControllerLeaseKeeper) *attachFixture {
	t.Helper()
	sandbox := newFakeSandbox(true)
	h.dialBack = func(at *runner.Attach) { sandbox.serve(t, ts, at) }

	stream := newScriptedStream()
	stream.in <- terminal.ClientMessage{Type: "resize", Cols: 80, Rows: 24}
	target := brokerTarget("sess_example", "vm1")
	target.Mode = control.AttachmentViewer
	target.ControllerGeneration = gen
	target.Negotiated = true
	target.MayClaim = true
	target.Controller = keeper
	target.Policy = control.PolicyShared

	done := make(chan error, 1)
	go func() { done <- p.Broker().Attach(context.Background(), target, stream) }()
	f := &attachFixture{stream: stream, sandbox: sandbox, done: done}
	t.Cleanup(f.close)
	return f
}

// TestSharedPolicyAttachesALegacyClientWithoutAdvancing is the compatibility
// case: an unnegotiated attach types, is stamped like every other attach so an
// unstamped frame at a sandbox still means an older plane, and takes nothing
// from the negotiated client typing beside it.
func TestSharedPolicyAttachesALegacyClientWithoutAdvancing(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{})
	lease := &fakeLease{gen: 4}

	modern := startSharedAttach(t, p, h, ts, control.AttachmentController, 4, fakeKeeper{lease, "att_modern"}, true)
	awaitType(t, modern.stream, terminal.TypeAttached)
	awaitSpliced(t, modern)

	// A legacy attach: no keeper, therefore not negotiated, and the
	// application granted it the row's own generation.
	legacy := startSharedAttach(t, p, h, ts, control.AttachmentController, 4, nil, true)
	awaitSpliced(t, legacy)
	legacy.stream.in <- terminal.ClientMessage{Type: "stdin", Data: []byte("legacy")}
	assertTypedInOrder(t, legacy, 4, "legacy")

	lease.mu.Lock()
	gen := lease.gen
	lease.mu.Unlock()
	if gen != 4 {
		t.Fatalf("a legacy attach advanced the generation to %d under a shared policy", gen)
	}
	// And the negotiated client was told nothing and still types.
	noOwnershipMessage(t, modern, 200*time.Millisecond)
	modern.stream.in <- terminal.ClientMessage{Type: "stdin", Data: []byte("modern")}
	awaitSharedPumpCaughtUp(t, modern)
	assertTypedInOrder(t, modern, 4, "modern")
}

// TestSharedPolicyAtGenerationZero: nothing under this policy advances the
// generation, so a session nobody has ever attached to exclusively stays at
// zero. GenOf(0) is the empty string and disappears from the wire, and a
// sandbox reads BOUND off the mode — so the attachment is bound, fenced
// against zero, and types.
func TestSharedPolicyAtGenerationZero(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{})
	lease := &fakeLease{}

	f := startSharedAttach(t, p, h, ts, control.AttachmentController, 0, fakeKeeper{lease, "att_zero"}, true)
	m, _ := awaitType(t, f.stream, terminal.TypeAttached)
	if m.Mode != terminal.ModeControl || m.Generation != "" {
		t.Fatalf("attached = %s at %q, want control at the empty generation", m.Mode, m.Generation)
	}
	cmd := h.nextCmd(t)
	if cmd.Attach.Mode != terminal.ModeControl || cmd.Attach.Generation != 0 {
		t.Fatalf("dial_attach carried %s at %d, want control at 0", cmd.Attach.Mode, cmd.Attach.Generation)
	}
	awaitSpliced(t, f)
	f.stream.in <- terminal.ClientMessage{Type: "stdin", Data: []byte("z")}
	awaitSharedPumpCaughtUp(t, f)
	assertTypedInOrder(t, f, 0, "z")
}

// TestTypersCountsOnlyThisReplicasTypers pins what the count IS, because it is
// rendered to people: the attaches this replica is serving that may type, and
// nothing about a session nobody is attached to.
func TestTypersCountsOnlyThisReplicasTypers(t *testing.T) {
	p, _, _ := newTestPlane(t, Options{})
	if n := p.Typers("sess_nobody"); n != 0 {
		t.Fatalf("Typers on an unattached session = %d, want 0", n)
	}
}

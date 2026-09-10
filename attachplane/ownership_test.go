package attachplane

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// ---------------------------------------------------------------------------
// a session's controller lease, as the application would keep it
// ---------------------------------------------------------------------------

// fakeKeeper is control.ControllerLeaseKeeper over one shared generation, the
// way a repository holds it. Several keepers over one fakeLease stand in for
// several attaches to one session — which is the whole subject here.
type fakeLease struct {
	mu     sync.Mutex
	gen    uint64
	holder string
}

type fakeKeeper struct {
	lease  *fakeLease
	holder string
}

func (k fakeKeeper) Claim(_ context.Context, expected uint64) (uint64, error) {
	k.lease.mu.Lock()
	defer k.lease.mu.Unlock()
	if k.lease.gen != expected {
		return 0, control.ErrStale
	}
	k.lease.gen++
	k.lease.holder = k.holder
	return k.lease.gen, nil
}

func (k fakeKeeper) Renew(_ context.Context, generation uint64) error {
	k.lease.mu.Lock()
	defer k.lease.mu.Unlock()
	if k.lease.gen != generation || (k.lease.holder != "" && k.lease.holder != k.holder) {
		return control.ErrStale
	}
	k.lease.holder = k.holder
	return nil
}

func (k fakeKeeper) Release(_ context.Context, generation uint64) error {
	k.lease.mu.Lock()
	defer k.lease.mu.Unlock()
	if k.lease.gen != generation {
		return nil
	}
	k.lease.gen++
	k.lease.holder = ""
	return nil
}

func (k fakeKeeper) State(context.Context) (uint64, bool, error) {
	k.lease.mu.Lock()
	defer k.lease.mu.Unlock()
	return k.lease.gen, k.lease.holder != "", nil
}

// ---------------------------------------------------------------------------
// a sandbox on the other end of the dial-back
// ---------------------------------------------------------------------------

// fakeSandbox is the runner half of one splice: it records every client frame
// that reaches it and answers a control message with the acknowledgement a
// current sessiond sends. acks: false is a sandbox that predates this
// protocol and never answers.
type fakeSandbox struct {
	acks bool

	mu    sync.Mutex
	got   []terminal.ClientMessage
	order []string

	ready chan struct{}
	conn  relay.Conn
	once  sync.Once
}

func newFakeSandbox(acks bool) *fakeSandbox {
	return &fakeSandbox{acks: acks, ready: make(chan struct{})}
}

// serve is the sandbox's whole life: dial back, then read until the splice
// ends, acknowledging bindings and recording everything.
func (s *fakeSandbox) serve(t *testing.T, ts *httptest.Server, at *runner.Attach) {
	t.Helper()
	c, _, err := dialAttachBack(t, ts, at.AttachID, testRunnerToken)
	if err != nil {
		return
	}
	defer c.CloseNow()
	conn := relay.WSConn(c)
	s.conn = conn
	s.once.Do(func() { close(s.ready) })
	for {
		raw, err := conn.Read(context.Background())
		if err != nil {
			return
		}
		var m terminal.ClientMessage
		if json.Unmarshal(raw, &m) != nil {
			continue
		}
		s.mu.Lock()
		s.got = append(s.got, m)
		s.order = append(s.order, m.Type)
		s.mu.Unlock()
		if m.Type == terminal.TypeControl && s.acks {
			ack, _ := json.Marshal(terminal.ServerMessage{
				Type: terminal.TypeControlAck, Mode: m.Mode, Generation: m.Generation})
			if conn.Write(context.Background(), ack) != nil {
				return
			}
		}
	}
}

func (s *fakeSandbox) received() []terminal.ClientMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]terminal.ClientMessage(nil), s.got...)
}

// write pushes one server message from the sandbox to the client.
func (s *fakeSandbox) write(t *testing.T, m terminal.ServerMessage) {
	t.Helper()
	<-s.ready
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.conn.Write(context.Background(), raw); err != nil {
		t.Fatalf("sandbox write: %v", err)
	}
}

// ---------------------------------------------------------------------------
// one negotiated attach, end to end through the plane
// ---------------------------------------------------------------------------

// attachFixture is one live attach: the client stream a test drives, the
// sandbox on the other end, and the goroutine running the broker.
type attachFixture struct {
	stream  *scriptedStream
	sandbox *fakeSandbox
	done    chan error
}

func (f *attachFixture) close() { f.stream.Close(errAttachEnded) }

// startAttach runs one attach against p: mode and generation are what the
// application granted, keeper is how this attach reaches the lease, and acks
// says whether the sandbox understands bindings. A keeper stands for a
// negotiated attach that may also claim, which is the ordinary case; the
// view-only principal that may not is startViewOnlyAttach.
func startAttach(t *testing.T, p *Plane, h *fakeHost, ts *httptest.Server,
	mode control.AttachmentMode, gen uint64, keeper control.ControllerLeaseKeeper, acks bool) *attachFixture {
	t.Helper()
	return startAttachAs(t, p, h, ts, mode, gen, keeper, acks, keeper != nil)
}

// startViewOnlyAttach runs an attach the host authorized for viewing and not
// for driving: it is told everything a negotiated attach is told, and it may
// not take control.
func startViewOnlyAttach(t *testing.T, p *Plane, h *fakeHost, ts *httptest.Server,
	gen uint64, keeper control.ControllerLeaseKeeper, acks bool) *attachFixture {
	t.Helper()
	return startAttachAs(t, p, h, ts, control.AttachmentViewer, gen, keeper, acks, false)
}

func startAttachAs(t *testing.T, p *Plane, h *fakeHost, ts *httptest.Server,
	mode control.AttachmentMode, gen uint64, keeper control.ControllerLeaseKeeper, acks, mayClaim bool) *attachFixture {
	t.Helper()
	sandbox := newFakeSandbox(acks)
	h.dialBack = func(at *runner.Attach) { sandbox.serve(t, ts, at) }

	stream := newScriptedStream()
	stream.in <- terminal.ClientMessage{Type: "resize", Cols: 80, Rows: 24}
	target := brokerTarget("sess_example", "vm1")
	target.Mode = mode
	target.ControllerGeneration = gen
	target.Negotiated = keeper != nil
	target.MayClaim = mayClaim
	target.Controller = keeper

	done := make(chan error, 1)
	go func() { done <- p.Broker().Attach(context.Background(), target, stream) }()
	f := &attachFixture{stream: stream, sandbox: sandbox, done: done}
	t.Cleanup(f.close)
	return f
}

// awaitType reads the client stream until a message of type kind arrives, and
// returns everything that came before it as well.
func awaitType(t *testing.T, s *scriptedStream, kind string) (terminal.ServerMessage, []string) {
	t.Helper()
	var before []string
	deadline := time.After(5 * time.Second)
	for {
		select {
		case m := <-s.out:
			if m.Type == kind {
				return m, before
			}
			before = append(before, m.Type)
		case <-deadline:
			t.Fatalf("no %q message reached the client; saw %v", kind, before)
			return terminal.ServerMessage{}, nil
		}
	}
}

// TestJourney1And2ANegotiatedAttachIsToldItsModeFirst pins the ordering the
// contract promises: a negotiated client learns its mode and generation
// BEFORE the first snapshot or output byte, so it never paints a screen
// without knowing whether it may type into it.
func TestJourney1And2ANegotiatedAttachIsToldItsModeFirst(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{})
	lease := &fakeLease{gen: 1}
	f := startAttach(t, p, h, ts, control.AttachmentController, 1, fakeKeeper{lease, "att_a"}, true)

	m, before := awaitType(t, f.stream, terminal.TypeAttached)
	if len(before) != 0 {
		t.Fatalf("%v reached the client before it was told its mode", before)
	}
	if m.Mode != terminal.ModeControl || m.Generation.Value() != 1 {
		t.Fatalf("attached = %s at %q, want control at 1", m.Mode, m.Generation)
	}

	// And the binding rode the command that opens the attachment, so the
	// sandbox has it before a byte of screen is queued.
	cmd := h.nextCmd(t)
	if cmd.Attach.Mode != terminal.ModeControl || cmd.Attach.Generation != 1 {
		t.Fatalf("dial_attach carried %s at %d, want control at 1", cmd.Attach.Mode, cmd.Attach.Generation)
	}
}

// TestAViewerIsToldSoAndItsInputNeverLeavesThePlane is journey step 2 from
// the plane's side: a viewer gets the screen and the output, and what it
// types is not carried.
func TestAViewerIsToldSoAndItsInputNeverLeavesThePlane(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{})
	lease := &fakeLease{gen: 4, holder: "att_other"}
	f := startAttach(t, p, h, ts, control.AttachmentViewer, 4, fakeKeeper{lease, "att_b"}, true)

	m, _ := awaitType(t, f.stream, terminal.TypeAttached)
	if m.Mode != terminal.ModeView || m.Generation.Value() != 4 {
		t.Fatalf("attached = %s at %q, want view at 4", m.Mode, m.Generation)
	}
	<-f.sandbox.ready

	f.stream.in <- terminal.ClientMessage{Type: "stdin", Data: []byte("rm -rf /\r")}
	f.stream.in <- terminal.ClientMessage{Type: "resize", Cols: 40, Rows: 20}
	// A round trip the sandbox DOES answer, so the assertion below is about
	// what arrived rather than about how long we waited.
	f.sandbox.write(t, terminal.ServerMessage{Type: "output", Seq: 1, Data: []byte("x")})
	awaitType(t, f.stream, "output")

	for _, got := range f.sandbox.received() {
		if got.Type == "stdin" || got.Type == "resize" {
			t.Fatalf("a viewer's %s frame was carried to the sandbox", got.Type)
		}
	}
}

// TestAnUnnegotiatedAttachGetsTodaysMessageSetAndStampedFrames is the old
// client + new plane pairing. The client is told nothing it could not decode,
// and the plane stamps its frames on its behalf — which is what makes an
// unstamped frame arriving at a sandbox mean an older PLANE and nothing else.
func TestAnUnnegotiatedAttachGetsTodaysMessageSetAndStampedFrames(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{})
	f := startAttach(t, p, h, ts, control.AttachmentController, 7, nil, true)
	<-f.sandbox.ready

	f.stream.in <- terminal.ClientMessage{Type: "stdin", Data: []byte("ls\r")}
	f.sandbox.write(t, terminal.ServerMessage{Type: "snapshot", Seq: 1, Data: []byte("screen")})

	m, before := awaitType(t, f.stream, "snapshot")
	if len(before) != 0 {
		t.Fatalf("an unnegotiated client received %v; it can decode none of it", before)
	}
	if m.Mode != "" || m.Generation != "" {
		t.Fatalf("a legacy client's snapshot carried ownership fields: %+v", m)
	}

	deadline := time.After(5 * time.Second)
	for {
		var stamped bool
		for _, got := range f.sandbox.received() {
			if got.Type == "stdin" {
				if got.Generation.Value() != 7 {
					t.Fatalf("a legacy client's stdin reached the sandbox stamped %q, want 7", got.Generation)
				}
				stamped = true
			}
		}
		if stamped {
			return
		}
		select {
		case <-deadline:
			t.Fatal("the legacy client's stdin never reached the sandbox")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestJourney3And4AClaimIsAnsweredOnlyAfterTheSandboxHasTheFence is the
// handoff guarantee at its sharpest. The taker is not told it has control
// until the sandbox has confirmed the new generation, so there is no window
// in which one device believes it is typing and another one still can.
func TestJourney3And4AClaimIsAnsweredOnlyAfterTheSandboxHasTheFence(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{})
	lease := &fakeLease{gen: 1, holder: "att_laptop"}
	f := startAttach(t, p, h, ts, control.AttachmentViewer, 1, fakeKeeper{lease, "att_phone"}, true)
	awaitType(t, f.stream, terminal.TypeAttached)
	<-f.sandbox.ready

	f.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(1)}
	m, _ := awaitType(t, f.stream, terminal.TypeAttached)
	if m.Mode != terminal.ModeControl || m.Generation.Value() != 2 {
		t.Fatalf("the claim was answered %s at %q, want control at 2", m.Mode, m.Generation)
	}

	// The sandbox had the binding before the client was told. Because the
	// answer came after the acknowledgement, this is guaranteed rather than
	// merely likely.
	var installed bool
	for _, got := range f.sandbox.received() {
		if got.Type == terminal.TypeControl && got.Mode == terminal.ModeControl && got.Generation.Value() == 2 {
			installed = true
		}
	}
	if !installed {
		t.Fatal("the client was told it has control before the sandbox was told to fence for it")
	}

	// And what it types now goes out under the generation it holds.
	f.stream.in <- terminal.ClientMessage{Type: "stdin", Data: []byte("ls\r")}
	deadline := time.After(5 * time.Second)
	for {
		for _, got := range f.sandbox.received() {
			if got.Type == "stdin" {
				if got.Generation.Value() != 2 {
					t.Fatalf("the new controller's stdin was stamped %q, want 2", got.Generation)
				}
				return
			}
		}
		select {
		case <-deadline:
			t.Fatal("the new controller's stdin never reached the sandbox")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestALostClaimIsAnsweredStaleWithTheCurrentGeneration is journey step 3's
// loser: it stays a viewer, and it is told the number it would have to claim
// from to try again rather than a bare refusal.
func TestALostClaimIsAnsweredStaleWithTheCurrentGeneration(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{})
	lease := &fakeLease{gen: 9, holder: "att_other"}
	f := startAttach(t, p, h, ts, control.AttachmentViewer, 9, fakeKeeper{lease, "att_late"}, true)
	awaitType(t, f.stream, terminal.TypeAttached)
	<-f.sandbox.ready

	// Somebody else claimed in the meantime, from the generation this client
	// is about to present.
	if _, err := (fakeKeeper{lease, "att_winner"}).Claim(context.Background(), 9); err != nil {
		t.Fatal(err)
	}
	f.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(9)}
	m, _ := awaitType(t, f.stream, terminal.TypeStale)
	if m.Generation.Value() != 10 {
		t.Fatalf("stale carried generation %q, want the current 10", m.Generation)
	}
	select {
	case extra := <-f.stream.out:
		if extra.Type == terminal.TypeAttached {
			t.Fatal("a losing claim was granted control anyway")
		}
	default:
	}
}

// TestADisplacedControllerIsToldAtOnce is journey step 4's notice. Both
// attaches are on this replica, so the plane can say it immediately; a
// controller displaced from another replica learns at its next heartbeat
// instead, which the next test covers.
func TestADisplacedControllerIsToldAtOnce(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{})
	lease := &fakeLease{gen: 1, holder: "att_laptop"}

	laptop := startAttach(t, p, h, ts, control.AttachmentController, 1, fakeKeeper{lease, "att_laptop"}, true)
	awaitType(t, laptop.stream, terminal.TypeAttached)
	<-laptop.sandbox.ready

	phone := startAttach(t, p, h, ts, control.AttachmentViewer, 1, fakeKeeper{lease, "att_phone"}, true)
	awaitType(t, phone.stream, terminal.TypeAttached)
	<-phone.sandbox.ready

	phone.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(1)}
	if m, _ := awaitType(t, phone.stream, terminal.TypeAttached); m.Generation.Value() != 2 {
		t.Fatalf("the phone was granted %q, want 2", m.Generation)
	}

	m, _ := awaitType(t, laptop.stream, terminal.TypeControlChanged)
	if m.Mode != terminal.ModeView || m.Generation.Value() != 2 {
		t.Fatalf("the displaced controller was told %s at %q, want view at 2", m.Mode, m.Generation)
	}

	// And it is fenced at the plane too: what it types now is not carried.
	laptop.stream.in <- terminal.ClientMessage{Type: "stdin", Data: []byte("y\r")}
	time.Sleep(100 * time.Millisecond)
	for _, got := range laptop.sandbox.received() {
		if got.Type == "stdin" {
			t.Fatal("a displaced controller's keystroke was still carried")
		}
	}
}

// TestAStaleHeartbeatDemotesAControllerFromAnotherReplica is the durable
// half. Nothing pushed this attach a message — the take-over happened
// somewhere else entirely — and its own renewal stopping is what tells it.
func TestAStaleHeartbeatDemotesAControllerFromAnotherReplica(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{HeartbeatInterval: 10 * time.Millisecond})
	lease := &fakeLease{gen: 1, holder: "att_here"}
	f := startAttach(t, p, h, ts, control.AttachmentController, 1, fakeKeeper{lease, "att_here"}, true)
	awaitType(t, f.stream, terminal.TypeAttached)
	<-f.sandbox.ready

	// Another replica's attach takes control. This one is told nothing.
	if _, err := (fakeKeeper{lease, "att_elsewhere"}).Claim(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	m, _ := awaitType(t, f.stream, terminal.TypeControlChanged)
	if m.Mode != terminal.ModeView || m.Generation.Value() != 2 {
		t.Fatalf("the heartbeat demoted it to %s at %q, want view at 2", m.Mode, m.Generation)
	}
}

// TestReleasingAdvancesTheGenerationAndDemotes is journey step 5's first
// half, at the plane: a release that only vacated a lease would leave this
// attach's binding still matching the sandbox's generation, and its next
// keystroke would still execute.
func TestReleasingAdvancesTheGenerationAndDemotes(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{})
	lease := &fakeLease{gen: 1, holder: "att_a"}
	f := startAttach(t, p, h, ts, control.AttachmentController, 1, fakeKeeper{lease, "att_a"}, true)
	awaitType(t, f.stream, terminal.TypeAttached)
	<-f.sandbox.ready

	f.stream.in <- terminal.ClientMessage{Type: terminal.TypeRelease}
	m, _ := awaitType(t, f.stream, terminal.TypeControlChanged)
	if m.Mode != terminal.ModeView {
		t.Fatalf("after releasing, mode = %s, want view", m.Mode)
	}
	lease.mu.Lock()
	gen, holder := lease.gen, lease.holder
	lease.mu.Unlock()
	if gen != 2 || holder != "" {
		t.Fatalf("after a release the lease is generation %d held by %q, want 2 and vacant", gen, holder)
	}
	f.stream.in <- terminal.ClientMessage{Type: "stdin", Data: []byte("y\r")}
	time.Sleep(100 * time.Millisecond)
	for _, got := range f.sandbox.received() {
		if got.Type == "stdin" {
			t.Fatal("a released controller's keystroke was still carried")
		}
	}
}

// TestDetachingReleasesControl is journey step 5's other half: a controller
// that walks away frees control immediately, so the next attach claims with
// no click rather than waiting out a lease nobody is using.
func TestDetachingReleasesControl(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{})
	lease := &fakeLease{gen: 1, holder: "att_a"}
	f := startAttach(t, p, h, ts, control.AttachmentController, 1, fakeKeeper{lease, "att_a"}, true)
	awaitType(t, f.stream, terminal.TypeAttached)
	<-f.sandbox.ready

	f.close()
	select {
	case <-f.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the attach never ended")
	}
	lease.mu.Lock()
	gen, holder := lease.gen, lease.holder
	lease.mu.Unlock()
	if gen != 2 || holder != "" {
		t.Fatalf("after a detach the lease is generation %d held by %q, want 2 and vacant", gen, holder)
	}
}

// TestAnOldSandboxNeverAcksAndTheHandoffStillHappens is the new plane + old
// sandbox pairing. The plane asks for an acknowledgement it may not get, and
// requires nothing: it waits its bounded wait and proceeds, fenced at the
// plane alone, which is what that pairing can offer.
func TestAnOldSandboxNeverAcksAndTheHandoffStillHappens(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{ControlAckTimeout: 50 * time.Millisecond})
	lease := &fakeLease{gen: 1, holder: "att_other"}
	f := startAttach(t, p, h, ts, control.AttachmentViewer, 1, fakeKeeper{lease, "att_phone"}, false)
	awaitType(t, f.stream, terminal.TypeAttached)
	<-f.sandbox.ready

	started := time.Now()
	f.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(1)}
	m, _ := awaitType(t, f.stream, terminal.TypeAttached)
	if m.Mode != terminal.ModeControl || m.Generation.Value() != 2 {
		t.Fatalf("the claim was answered %s at %q, want control at 2", m.Mode, m.Generation)
	}
	if elapsed := time.Since(started); elapsed < 50*time.Millisecond {
		t.Fatalf("the handoff answered in %s without waiting for an acknowledgement at all", elapsed)
	}
}

// TestRequestedOwnershipReadsTheAttachURL pins the negotiation's one
// direction of forgiveness: anything a host cannot make sense of reads as
// "not asked for", because refusing an attach over a query string the client
// does not know it got wrong helps nobody.
func TestRequestedOwnershipReadsTheAttachURL(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query string
		want  Ownership
	}{
		{"nothing", "", Ownership{Mode: control.AttachmentController}},
		{"an older capability", "control=v0&mode=view", Ownership{Mode: control.AttachmentController}},
		{"claim if free", "control=v1", Ownership{Negotiated: true, Mode: control.AttachmentController}},
		{"view only", "control=v1&mode=view", Ownership{Negotiated: true, Mode: control.AttachmentViewer}},
		{"an unknown mode", "control=v1&mode=admin", Ownership{Negotiated: true, Mode: control.AttachmentController}},
		{"a reconnect", "control=v1&expected=42",
			Ownership{Negotiated: true, Mode: control.AttachmentController, Expected: 42}},
		{"an unparseable generation", "control=v1&expected=soon",
			Ownership{Negotiated: true, Mode: control.AttachmentController}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, err := url.ParseQuery(tc.query)
			if err != nil {
				t.Fatal(err)
			}
			if got := RequestedOwnership(q); got != tc.want {
				t.Fatalf("RequestedOwnership(%q) = %+v, want %+v", tc.query, got, tc.want)
			}
		})
	}
}

// TestAClientsControlVerbNeverReachesTheSandbox is the fence around the
// privileged half of the protocol. `control` is the PLANE's word to a
// sandbox: it installs a mode and a generation at the pty. A client that
// writes one itself is not performing a handoff — it is naming its own
// authority — so the plane must not carry it, however the client dresses it
// up.
//
// The generation below is what makes this more than tidiness. The pty's fence
// only ever goes up, so one accepted `control` at a generation past every
// generation this session will ever reach would leave nobody able to type for
// the life of the process — a session wedged by any attach that can send
// JSON.
func TestAClientsControlVerbNeverReachesTheSandbox(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{})
	lease := &fakeLease{gen: 1, holder: "att_laptop"}
	a := startAttach(t, p, h, ts, control.AttachmentController, 1,
		fakeKeeper{lease, "att_laptop"}, true)
	awaitType(t, a.stream, terminal.TypeAttached)
	<-a.sandbox.ready

	a.stream.in <- terminal.ClientMessage{
		Type: terminal.TypeControl, Mode: terminal.ModeView, Generation: terminal.GenOf(2)}
	a.stream.in <- terminal.ClientMessage{
		Type: terminal.TypeControl, Mode: terminal.ModeControl, Generation: terminal.GenOf(^uint64(0))}
	a.stream.in <- terminal.ClientMessage{Type: terminal.TypeControlAck, Generation: terminal.GenOf(9)}
	// A message the plane DOES carry, sent after them, so that observing it
	// arrive proves the three above were dropped rather than merely slow.
	a.stream.in <- terminal.ClientMessage{Type: "stdin", Data: []byte("ls\r")}
	awaitSandbox(t, a.sandbox, func(got []terminal.ClientMessage) bool {
		return len(got) > 0 && got[len(got)-1].Type == "stdin"
	}, "the trailing keystroke")

	for _, got := range a.sandbox.received() {
		if got.Type == terminal.TypeControl || got.Type == terminal.TypeControlAck {
			t.Fatalf("a client's %q reached the sandbox at generation %q", got.Type, got.Generation)
		}
	}
}

// awaitSandbox waits until the sandbox's record satisfies want, so a test can
// say "everything up to here has arrived" without sleeping for a fixed time.
func awaitSandbox(t *testing.T, s *fakeSandbox, want func([]terminal.ClientMessage) bool, what string) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		if want(s.received()) {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("%s never reached the sandbox", what)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// TestAViewerLearnsWhenControlIsFreed is the other half of "one key press
// takes control". A viewer that is never told the generation moved is left
// holding the number it saw at attach time, so its first press claims from a
// generation that no longer exists and is answered "somebody else got there
// first" — about a session nobody is using. The notice a viewer gets is
// silent; the number it carries is the point.
func TestAViewerLearnsWhenControlIsFreed(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{})
	lease := &fakeLease{gen: 1, holder: "att_laptop"}
	laptop := startAttach(t, p, h, ts, control.AttachmentController, 1, fakeKeeper{lease, "att_laptop"}, true)
	awaitType(t, laptop.stream, terminal.TypeAttached)
	<-laptop.sandbox.ready

	phone := startAttach(t, p, h, ts, control.AttachmentViewer, 1, fakeKeeper{lease, "att_phone"}, true)
	awaitType(t, phone.stream, terminal.TypeAttached)
	<-phone.sandbox.ready

	// The laptop gives control up without leaving.
	laptop.stream.in <- terminal.ClientMessage{Type: terminal.TypeRelease}

	m, _ := awaitType(t, phone.stream, terminal.TypeControlChanged)
	if m.Generation.Value() != 2 {
		t.Fatalf("the viewer was told generation %q, want the 2 the release left behind", m.Generation)
	}
	// And one press of the key takes it, from the generation it was told.
	phone.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(2)}
	got, before := awaitType(t, phone.stream, terminal.TypeAttached)
	for _, kind := range before {
		if kind == terminal.TypeStale {
			t.Fatal("the viewer's first press was refused; it claimed from a generation nobody told it about")
		}
	}
	if got.Mode != terminal.ModeControl || got.Generation.Value() != 3 {
		t.Fatalf("the viewer's claim = %s at %q, want control at 3", got.Mode, got.Generation)
	}
}

// TestALegacyAttachDisplacesANegotiatedControllerAndSaysSo is the old client
// + new plane pairing at the plane, which is where the compatibility rule
// says the negotiated client is "notified and fenced". The application admits
// a legacy attach unconditionally, as a take-over; this is the half that
// makes that safe for the device it took control from.
func TestALegacyAttachDisplacesANegotiatedControllerAndSaysSo(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{})
	lease := &fakeLease{gen: 1, holder: "att_laptop"}
	laptop := startAttach(t, p, h, ts, control.AttachmentController, 1, fakeKeeper{lease, "att_laptop"}, true)
	awaitType(t, laptop.stream, terminal.TypeAttached)
	<-laptop.sandbox.ready

	// A client that negotiates nothing: no keeper, and the generation the
	// application advanced unconditionally on its behalf.
	legacy := startAttach(t, p, h, ts, control.AttachmentController, 2, nil, true)
	<-legacy.sandbox.ready

	m, _ := awaitType(t, laptop.stream, terminal.TypeControlChanged)
	if m.Mode != terminal.ModeView || m.Generation.Value() != 2 {
		t.Fatalf("displaced by a legacy attach: told %s at %q, want view at 2", m.Mode, m.Generation)
	}
	laptop.stream.in <- terminal.ClientMessage{Type: "stdin", Data: []byte("y\r")}
	legacy.stream.in <- terminal.ClientMessage{Type: "stdin", Data: []byte("n\r")}
	awaitSandbox(t, legacy.sandbox, func(got []terminal.ClientMessage) bool {
		for _, m := range got {
			if m.Type == "stdin" {
				return true
			}
		}
		return false
	}, "the legacy client's keystroke")
	for _, got := range laptop.sandbox.received() {
		if got.Type == "stdin" {
			t.Fatal("a controller displaced by a legacy attach was still carried")
		}
	}
}

// TestATakeOverAtAttachTimeWaitsForTheSandboxToo pins the ordering that makes
// the in-flight keystroke harmless when the take-over IS the attach rather
// than a claim on an existing one. The taker is not told it has control until
// the displaced controller's sandbox has confirmed the new binding, because
// until then a keystroke the old controller has already sent still executes.
func TestATakeOverAtAttachTimeWaitsForTheSandboxToo(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{})
	lease := &fakeLease{gen: 1, holder: "att_laptop"}
	laptop := startAttach(t, p, h, ts, control.AttachmentController, 1, fakeKeeper{lease, "att_laptop"}, true)
	awaitType(t, laptop.stream, terminal.TypeAttached)
	awaitSpliced(t, laptop)

	phone := startAttach(t, p, h, ts, control.AttachmentController, 2, fakeKeeper{lease, "att_phone"}, true)
	awaitType(t, phone.stream, terminal.TypeAttached)

	// By the time the taker was told, the fence protecting it was already in
	// the displaced controller's sandbox.
	var fenced bool
	for _, got := range laptop.sandbox.received() {
		if got.Type == terminal.TypeControl && got.Mode == terminal.ModeView && got.Generation.Value() == 2 {
			fenced = true
		}
	}
	if !fenced {
		t.Fatal("the taker was told it had control before the displaced controller's sandbox knew")
	}
}

// TestThePlaneStampsItsOwnViewOverTheClients pins which of the two
// generations on a frame is the one the sandbox reads. The client's is a
// claim about itself; the plane's is what the application granted, and it is
// the fresher of the two. A fence a client could write its own value into
// would be no fence at all.
func TestThePlaneStampsItsOwnViewOverTheClients(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{})
	lease := &fakeLease{gen: 1, holder: "att_laptop"}
	a := startAttach(t, p, h, ts, control.AttachmentController, 1, fakeKeeper{lease, "att_laptop"}, true)
	awaitType(t, a.stream, terminal.TypeAttached)
	<-a.sandbox.ready

	// It takes control, so the plane now holds generation 2 — and then sends
	// a keystroke stamped with the one it used to hold.
	a.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(1)}
	if m, _ := awaitType(t, a.stream, terminal.TypeAttached); m.Generation.Value() != 2 {
		t.Fatalf("the claim landed at %q, want 2", m.Generation)
	}
	a.stream.in <- terminal.ClientMessage{
		Type: "stdin", Data: []byte("ls\r"), Generation: terminal.GenOf(1)}

	awaitSandbox(t, a.sandbox, func(got []terminal.ClientMessage) bool {
		for _, m := range got {
			if m.Type == "stdin" {
				return true
			}
		}
		return false
	}, "the keystroke")
	for _, m := range a.sandbox.received() {
		if m.Type == "stdin" && m.Generation.Value() != 2 {
			t.Fatalf("the sandbox read generation %q off a keystroke, want the plane's 2", m.Generation)
		}
	}
}

// awaitSpliced waits until f's client pump is actually carrying frames to its
// sandbox, which is the moment the plane has the socket it installs bindings
// on. Until then a displacement has nowhere to go — harmlessly, because the
// attachment's binding rides its own opening frame — but a test about the
// ORDER of a handoff has to start after it.
func awaitSpliced(t *testing.T, f *attachFixture) {
	t.Helper()
	<-f.sandbox.ready
	f.stream.in <- terminal.ClientMessage{Type: "resize", Cols: 80, Rows: 24}
	awaitSandbox(t, f.sandbox, func(got []terminal.ClientMessage) bool {
		for _, m := range got {
			if m.Type == "resize" {
				return true
			}
		}
		return false
	}, "the attach's first forwarded frame")
}

// awaitViewerSpliced proves this attach's splice is running when the ordinary
// probe cannot: a viewer's own frames are dropped at the plane, so the frame
// that proves it has to travel the other way. It matters because a handoff
// needs the sandbox's socket, and an attach that has not been spliced yet
// installs nothing and therefore waits for nothing.
func awaitViewerSpliced(t *testing.T, f *attachFixture) {
	t.Helper()
	f.sandbox.write(t, terminal.ServerMessage{Type: "output", Seq: 1, Data: []byte("x")})
	awaitType(t, f.stream, "output")
}

// awaitGeneration blocks until the shared lease reaches want, so a test can
// sequence one claim behind another's store write without sleeping for it.
func awaitGeneration(t *testing.T, l *fakeLease, want uint64) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		l.mu.Lock()
		got := l.gen
		l.mu.Unlock()
		if got >= want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("the lease never reached generation %d (it is at %d)", want, got)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// TestAClaimSupersededWhileItWaitedIsNeverToldItHasControl closes the window
// a claim opens on itself. A claim advances the generation in the store and
// then waits — for as long as the acknowledgement timeout allows — for the
// sandbox to confirm the binding. A second claim can win inside that wait, so
// by the time the first one wakes up the generation it holds has already been
// superseded.
//
// Announcing "attached, control" anyway breaks the one rule this design has:
// at most one controller at any moment. Nothing double-executes — the fence
// is at the pty and holds — but until the next heartbeat the user's screen
// says they have control while every keystroke they type is discarded, and a
// second device has been told exactly the same thing.
//
// The interleaving is made deterministic rather than raced: the first
// attach's sandbox predates the protocol and never acknowledges, so its claim
// waits out the whole timeout, while the second attach's sandbox answers at
// once.
func TestAClaimSupersededWhileItWaitedIsNeverToldItHasControl(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{ControlAckTimeout: 500 * time.Millisecond})
	lease := &fakeLease{gen: 1}

	a := startAttach(t, p, h, ts, control.AttachmentViewer, 1, fakeKeeper{lease, "att_a"}, false)
	awaitType(t, a.stream, terminal.TypeAttached)
	awaitViewerSpliced(t, a)
	b := startAttach(t, p, h, ts, control.AttachmentViewer, 1, fakeKeeper{lease, "att_b"}, true)
	awaitType(t, b.stream, terminal.TypeAttached)
	awaitViewerSpliced(t, b)

	a.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(1)}
	// a has won generation 2 in the store and is now blocked on an
	// acknowledgement that will never come. b claims over it from there.
	awaitGeneration(t, lease, 2)
	b.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(2)}

	deadline := time.After(5 * time.Second)
	for done := false; !done; {
		select {
		case m := <-a.stream.out:
			if m.Type == terminal.TypeAttached && m.Mode == terminal.ModeControl {
				t.Fatalf("a claim superseded while it waited was told it has control at generation %q; "+
					"two devices are now printing [you have control]", m.Generation)
			}
			if m.Type == terminal.TypeStale {
				if m.Generation.Value() != 3 {
					t.Fatalf("the superseded claim was told generation %q, want the 3 that exists", m.Generation)
				}
				done = true
			}
		case <-deadline:
			t.Fatal("the superseded claim was never answered at all")
		}
	}

	// And the winner is told what it actually won.
	got, _ := awaitType(t, b.stream, terminal.TypeAttached)
	if got.Mode != terminal.ModeControl || got.Generation.Value() != 3 {
		t.Fatalf("the winning claim = %s at %q, want control at 3", got.Mode, got.Generation)
	}
}

// countingKeeper counts the claims that actually reach the application, so a
// test can say that a refusal never left this replica.
type countingKeeper struct {
	fakeKeeper
	n atomic.Int64
}

func (k *countingKeeper) Claim(ctx context.Context, expected uint64) (uint64, error) {
	k.n.Add(1)
	return k.fakeKeeper.Claim(ctx, expected)
}

// TestAViewOnlyAttachIsToldEverythingAndClaimsNothing is the plane's half of
// the mode-aware attachment policy. A host can grant viewing without granting
// driving, and such an attach is still NEGOTIATED — its client is told its
// mode and its generation, and reads every `control_changed` that follows,
// because being unable to take control is not a reason to be told nothing.
//
// Its take-control key is answered "you are still a viewer", and the answer
// is reached without the application being asked at all: an unauthorized
// claim must not advance a generation on its way to being refused.
func TestAViewOnlyAttachIsToldEverythingAndClaimsNothing(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{})
	lease := &fakeLease{gen: 4}
	keeper := &countingKeeper{fakeKeeper: fakeKeeper{lease, "att_guest"}}

	f := startViewOnlyAttach(t, p, h, ts, 4, keeper, true)
	opening, _ := awaitType(t, f.stream, terminal.TypeAttached)
	if opening.Mode != terminal.ModeView || opening.Generation.Value() != 4 {
		t.Fatalf("a view-only attach was told %s at %q, want view at 4", opening.Mode, opening.Generation)
	}
	awaitViewerSpliced(t, f)

	f.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(4)}
	deadline := time.After(5 * time.Second)
	for done := false; !done; {
		select {
		case m := <-f.stream.out:
			if m.Type == terminal.TypeAttached && m.Mode == terminal.ModeControl {
				t.Fatalf("a client the host authorized for viewing alone was granted control at %q", m.Generation)
			}
			if m.Type == terminal.TypeStale {
				if m.Generation.Value() != 4 {
					t.Fatalf("the refused claim was told generation %q, want the 4 that stands", m.Generation)
				}
				done = true
			}
		case <-deadline:
			t.Fatal("a view-only client's claim was never answered at all")
		}
	}
	if n := keeper.n.Load(); n != 0 {
		t.Fatalf("%d unauthorized claim(s) reached the application", n)
	}
	lease.mu.Lock()
	gen := lease.gen
	lease.mu.Unlock()
	if gen != 4 {
		t.Fatalf("an unauthorized claim moved the generation to %d", gen)
	}
}

// TestAClaimSupersededWhileItDisplacedIsNeverToldItHasControl is the second
// half of the same rule, and the longer window by far. A claim answers itself
// only after it has displaced every peer on this replica, and that loop waits
// on each displaced peer's SANDBOX — for as long as the acknowledgement
// timeout allows, per peer. Another claim can win inside that wait.
//
// Reporting the mode read before the loop would tell a client it holds
// control that has already moved, and nothing would ever correct it: a client
// that believes it is the controller sends no claim of its own, and this
// attach's heartbeat renews nothing because the plane knows it is a viewer.
func TestAClaimSupersededWhileItDisplacedIsNeverToldItHasControl(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{ControlAckTimeout: 700 * time.Millisecond})
	lease := &fakeLease{gen: 1, holder: "att_aaaa"}

	// The incumbent's sandbox predates the protocol, so displacing it costs
	// the whole timeout — which is the window under test.
	laptop := startAttach(t, p, h, ts, control.AttachmentController, 1, fakeKeeper{lease, "att_aaaa"}, false)
	awaitType(t, laptop.stream, terminal.TypeAttached)
	<-laptop.sandbox.ready

	phone := startAttach(t, p, h, ts, control.AttachmentViewer, 1, fakeKeeper{lease, "att_bbbb"}, true)
	awaitType(t, phone.stream, terminal.TypeAttached)
	awaitViewerSpliced(t, phone)
	tablet := startAttach(t, p, h, ts, control.AttachmentViewer, 1, fakeKeeper{lease, "att_cccc"}, true)
	awaitType(t, tablet.stream, terminal.TypeAttached)
	awaitViewerSpliced(t, tablet)

	phone.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(1)}
	// The phone has won generation 2 and is inside its displace loop, waiting
	// on the laptop's sandbox for an acknowledgement that will never come.
	awaitSandbox(t, laptop.sandbox, func(got []terminal.ClientMessage) bool {
		for _, m := range got {
			if m.Type == terminal.TypeControl && m.Mode == terminal.ModeView && m.Generation.Value() == 2 {
				return true
			}
		}
		return false
	}, "the displaced incumbent's new binding")
	tablet.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(2)}

	deadline := time.After(5 * time.Second)
	for done := false; !done; {
		select {
		case m := <-phone.stream.out:
			if m.Type == terminal.TypeAttached && m.Mode == terminal.ModeControl {
				t.Fatalf("a claim superseded while it displaced its peers was told it has control at %q; "+
					"nothing will ever correct it", m.Generation)
			}
			if m.Type == terminal.TypeStale {
				if m.Generation.Value() != 3 {
					t.Fatalf("the superseded claim was told generation %q, want the 3 that exists", m.Generation)
				}
				done = true
			}
		case <-deadline:
			t.Fatal("the superseded claim was never answered at all")
		}
	}
	if got, _ := awaitType(t, tablet.stream, terminal.TypeAttached); got.Generation.Value() != 3 {
		t.Fatalf("the winning claim = %s at %q, want control at 3", got.Mode, got.Generation)
	}
}

// TestATakeOverAtAttachTimeIsToldWhatItIsAfterItDisplaced is the same rule on
// the attach path. A take-over that IS an attach displaces every peer before
// it answers, and that loop waits on each displaced peer's sandbox; a claim
// on an existing attach can win inside it. What the opening `attached` says
// has to be what this attach holds when the message is written, not what the
// application granted before the loop began.
func TestATakeOverAtAttachTimeIsToldWhatItIsAfterItDisplaced(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{ControlAckTimeout: 700 * time.Millisecond})
	lease := &fakeLease{gen: 1, holder: "att_aaaa"}

	laptop := startAttach(t, p, h, ts, control.AttachmentController, 1, fakeKeeper{lease, "att_aaaa"}, false)
	awaitType(t, laptop.stream, terminal.TypeAttached)
	<-laptop.sandbox.ready
	tablet := startAttach(t, p, h, ts, control.AttachmentViewer, 1, fakeKeeper{lease, "att_cccc"}, true)
	awaitType(t, tablet.stream, terminal.TypeAttached)
	awaitViewerSpliced(t, tablet)

	// The application admits a new controller attach: it has already advanced
	// the generation before the broker is called at all.
	gen, err := (fakeKeeper{lease, "att_dddd"}).Claim(context.Background(), 1)
	if err != nil || gen != 2 {
		t.Fatalf("the application's own claim = %d, %v; want 2, nil", gen, err)
	}
	phone := startAttach(t, p, h, ts, control.AttachmentController, 2, fakeKeeper{lease, "att_dddd"}, true)

	// It is now inside its displace loop, waiting on the incumbent's sandbox.
	awaitSandbox(t, laptop.sandbox, func(got []terminal.ClientMessage) bool {
		for _, m := range got {
			if m.Type == terminal.TypeControl && m.Mode == terminal.ModeView && m.Generation.Value() == 2 {
				return true
			}
		}
		return false
	}, "the displaced incumbent's new binding")
	tablet.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(2)}
	if m, _ := awaitType(t, tablet.stream, terminal.TypeAttached); m.Generation.Value() != 3 {
		t.Fatalf("the claim that won = %s at %q, want control at 3", m.Mode, m.Generation)
	}

	m, _ := awaitType(t, phone.stream, terminal.TypeAttached)
	if m.Mode == terminal.ModeControl {
		t.Fatalf("an attach displaced before it was ever told anything was told it has control at %q", m.Generation)
	}
	if m.Generation.Value() != 3 {
		t.Fatalf("the displaced attach opened at generation %q, want the 3 that exists", m.Generation)
	}
}

// recordingKeeper records every generation a release was attempted at, so a
// test can say which generation a departing attach actually gave up.
type recordingKeeper struct {
	fakeKeeper
	mu       sync.Mutex
	releases []uint64
	stateErr error
}

func (k *recordingKeeper) Release(ctx context.Context, generation uint64) error {
	k.mu.Lock()
	k.releases = append(k.releases, generation)
	k.mu.Unlock()
	return k.fakeKeeper.Release(ctx, generation)
}

func (k *recordingKeeper) State(ctx context.Context) (uint64, bool, error) {
	if k.stateErr != nil {
		return 0, false, k.stateErr
	}
	return k.fakeKeeper.State(ctx)
}

func (k *recordingKeeper) released() []uint64 {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]uint64(nil), k.releases...)
}

// TestFinishReleasesOnlyTheGenerationItActuallyHeld closes the last place a
// mode and a generation were read separately. A departing attach gives up
// control on its way out, and its own heartbeat can be demoting it at the
// same moment — a demotion holds `control` for the whole of its bounded wait
// on the sandbox, and only then writes the CURRENT generation, which belongs
// to whoever took over.
//
// Read in two steps, the mode says "still the controller" and the generation
// says "the winner's", and the release advances past a device that
// legitimately has control, from an attach that is walking out of the door.
// Its screen says [you have control] and its keystrokes are fenced until its
// next heartbeat.
func TestFinishReleasesOnlyTheGenerationItActuallyHeld(t *testing.T) {
	p, _, _ := newTestPlane(t, Options{})
	for range 500 {
		// This attach holds control at 1; another replica has just taken it.
		lease := &fakeLease{gen: 2, holder: "att_bbbb"}
		keeper := &recordingKeeper{fakeKeeper: fakeKeeper{lease, "att_aaaa"}}
		o := &ownership{plane: p, session: "sess_example", negotiated: true, mayClaim: true,
			keeper: keeper, mode: terminal.ModeControl, gen: 1, ack: make(chan uint64, 1)}
		p.owners.add(o)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); o.demoteTo(2) }() // the heartbeat, finishing
		go func() { defer wg.Done(); o.finish() }()    // the client, disconnecting
		wg.Wait()

		for _, gen := range keeper.released() {
			if gen != 1 {
				t.Fatalf("a departing attach released generation %d, which it never held", gen)
			}
		}
		lease.mu.Lock()
		gen, holder := lease.gen, lease.holder
		lease.mu.Unlock()
		if gen != 2 || holder != "att_bbbb" {
			t.Fatalf("a departing attach advanced past the live controller: generation %d, holder %q; want 2, att_bbbb",
				gen, holder)
		}
	}
}

// TestARefusedClaimIsNeverToldGenerationZero pins the fallback a refused
// claim answers with when the store cannot be read. Zero is a generation no
// row is ever at, so a client told it presents zero on every later claim and
// is refused every time — stranded by one read that timed out, until it
// detaches and attaches again.
func TestARefusedClaimIsNeverToldGenerationZero(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{})
	lease := &fakeLease{gen: 7, holder: "att_aaaa"}
	keeper := &recordingKeeper{fakeKeeper: fakeKeeper{lease, "att_bbbb"},
		stateErr: control.ErrUnavailable}

	f := startAttach(t, p, h, ts, control.AttachmentViewer, 7, keeper, true)
	awaitType(t, f.stream, terminal.TypeAttached)
	awaitViewerSpliced(t, f)

	// A claim from a generation that has been superseded, answered while the
	// store is briefly unusable.
	f.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(5)}
	m, _ := awaitType(t, f.stream, terminal.TypeStale)
	if m.Generation.Value() != 7 {
		t.Fatalf("a refused claim whose generation read failed was told %q, want the 7 this attach holds", m.Generation)
	}
}

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

// testDeadline is how long any of the helpers below waits for something the
// plane should do in milliseconds. It is generous on purpose: every one of
// them fails the test when it expires, so the only thing a tight bound buys
// is a suite that reports a busy machine as a bug. The handoff timings these
// tests actually measure carry bounds of their own.
const testDeadline = 15 * time.Second

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
	// ackGate, when non-nil, holds every acknowledgement until the test
	// closes it. A real sandbox answers whenever it answers; a test that
	// needs something else to happen INSIDE a handoff's wait has to decide
	// when that is, and this is how it says so.
	ackGate chan struct{}

	mu  sync.Mutex
	got []terminal.ClientMessage

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
		s.mu.Unlock()
		if m.Type == terminal.TypeControl && s.acks {
			if s.ackGate != nil {
				<-s.ackGate
			}
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
	return startAttachOn(t, p, h, ts, mode, gen, keeper, acks, keeper != nil, mayClaim)
}

// startAttachOn is the same with the target's two ownership facts set
// independently of the keeper, so a test can build the incoherent targets the
// contract says cannot exist and pin what a broker does with one anyway.
func startAttachOn(t *testing.T, p *Plane, h *fakeHost, ts *httptest.Server,
	mode control.AttachmentMode, gen uint64, keeper control.ControllerLeaseKeeper,
	acks, negotiated, mayClaim bool) *attachFixture {
	t.Helper()
	return startAttachWith(t, p, h, ts, mode, gen, keeper, newFakeSandbox(acks), negotiated, mayClaim)
}

// startAttachWith is the same over a sandbox the test built itself, for the
// tests that need to decide WHEN that sandbox acknowledges a binding. The
// sandbox must be fully configured before this is called: it is served on a
// goroutine of the plane's making from here on.
func startAttachWith(t *testing.T, p *Plane, h *fakeHost, ts *httptest.Server,
	mode control.AttachmentMode, gen uint64, keeper control.ControllerLeaseKeeper,
	sandbox *fakeSandbox, negotiated, mayClaim bool) *attachFixture {
	t.Helper()
	h.dialBack = func(at *runner.Attach) { sandbox.serve(t, ts, at) }

	stream := newScriptedStream()
	stream.in <- terminal.ClientMessage{Type: "resize", Cols: 80, Rows: 24}
	target := brokerTarget("sess_example", "vm1")
	target.Mode = mode
	target.ControllerGeneration = gen
	target.Negotiated = negotiated
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
	deadline := time.After(testDeadline)
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
	f := startAttach(t, p, h, ts, control.AttachmentController, 1, fakeKeeper{lease, "att_aaaa"}, true)

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
	f := startAttach(t, p, h, ts, control.AttachmentViewer, 4, fakeKeeper{lease, "att_bbbb"}, true)

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
	awaitSpliced(t, f)

	f.stream.in <- terminal.ClientMessage{Type: "stdin", Data: []byte("ls\r")}
	f.sandbox.write(t, terminal.ServerMessage{Type: "snapshot", Seq: 1, Data: []byte("screen")})

	m, before := awaitType(t, f.stream, "snapshot")
	if len(before) != 0 {
		t.Fatalf("an unnegotiated client received %v; it can decode none of it", before)
	}
	if m.Mode != "" || m.Generation != "" {
		t.Fatalf("a legacy client's snapshot carried ownership fields: %+v", m)
	}

	deadline := time.After(testDeadline)
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
	awaitViewerSpliced(t, f)

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
	deadline := time.After(testDeadline)
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
	awaitViewerSpliced(t, f)

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
	awaitSpliced(t, laptop)

	phone := startAttach(t, p, h, ts, control.AttachmentViewer, 1, fakeKeeper{lease, "att_phone"}, true)
	awaitType(t, phone.stream, terminal.TypeAttached)
	awaitViewerSpliced(t, phone)

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
	awaitPumpCaughtUp(t, laptop)
	for _, got := range laptop.sandbox.received() {
		if got.Type == "stdin" {
			t.Fatal("a displaced controller's keystroke was still carried")
		}
	}
}

// awaitPumpCaughtUp blocks until this attach's client pump has processed
// everything queued before it, by sending one message the pump must ANSWER
// and waiting for the answer: a claim from generation zero, which no row is
// ever at, so the store refuses it, no generation moves anywhere and nothing
// about the attach changes.
//
// It is what makes "nothing I typed was carried" an assertion about what
// happened rather than about how long the test waited. A sleep in its place
// leaves a forwarding regression failing only probabilistically.
func awaitPumpCaughtUp(t *testing.T, f *attachFixture) {
	t.Helper()
	f.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(0)}
	awaitType(t, f.stream, terminal.TypeStale)
}

// TestAStaleHeartbeatDemotesAControllerFromAnotherReplica is the durable
// half. Nothing pushed this attach a message — the take-over happened
// somewhere else entirely — and its own renewal stopping is what tells it.
func TestAStaleHeartbeatDemotesAControllerFromAnotherReplica(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{HeartbeatInterval: 10 * time.Millisecond})
	lease := &fakeLease{gen: 1, holder: "att_here"}
	f := startAttach(t, p, h, ts, control.AttachmentController, 1, fakeKeeper{lease, "att_here"}, true)
	awaitType(t, f.stream, terminal.TypeAttached)
	awaitSpliced(t, f)

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
	lease := &fakeLease{gen: 1, holder: "att_aaaa"}
	f := startAttach(t, p, h, ts, control.AttachmentController, 1, fakeKeeper{lease, "att_aaaa"}, true)
	awaitType(t, f.stream, terminal.TypeAttached)
	awaitSpliced(t, f)

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
	awaitPumpCaughtUp(t, f)
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
	lease := &fakeLease{gen: 1, holder: "att_aaaa"}
	f := startAttach(t, p, h, ts, control.AttachmentController, 1, fakeKeeper{lease, "att_aaaa"}, true)
	awaitType(t, f.stream, terminal.TypeAttached)
	awaitSpliced(t, f)

	f.close()
	select {
	case <-f.done:
	case <-time.After(testDeadline):
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
	awaitViewerSpliced(t, f)

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
	awaitSpliced(t, a)

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
	deadline := time.After(testDeadline)
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
	awaitSpliced(t, laptop)

	phone := startAttach(t, p, h, ts, control.AttachmentViewer, 1, fakeKeeper{lease, "att_phone"}, true)
	awaitType(t, phone.stream, terminal.TypeAttached)
	awaitViewerSpliced(t, phone)

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
	awaitSpliced(t, laptop)

	// A client that negotiates nothing: no keeper, and the generation the
	// application advanced unconditionally on its behalf.
	legacy := startAttach(t, p, h, ts, control.AttachmentController, 2, nil, true)
	awaitSpliced(t, legacy)

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
	// A viewer, because a claim from the CONTROLLER is answered without
	// touching the store: a client that already has control has nothing to
	// win, and re-claiming would fence its own in-flight keystrokes.
	a := startAttach(t, p, h, ts, control.AttachmentViewer, 1, fakeKeeper{lease, "att_phone"}, true)
	awaitType(t, a.stream, terminal.TypeAttached)
	awaitViewerSpliced(t, a)

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
	deadline := time.After(testDeadline)
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
	p, h, ts := newTestPlane(t, Options{ControlAckTimeout: 1500 * time.Millisecond})
	lease := &fakeLease{gen: 1}

	a := startAttach(t, p, h, ts, control.AttachmentViewer, 1, fakeKeeper{lease, "att_aaaa"}, false)
	awaitType(t, a.stream, terminal.TypeAttached)
	awaitViewerSpliced(t, a)
	b := startAttach(t, p, h, ts, control.AttachmentViewer, 1, fakeKeeper{lease, "att_bbbb"}, true)
	awaitType(t, b.stream, terminal.TypeAttached)
	awaitViewerSpliced(t, b)

	a.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(1)}
	// a has won generation 2 in the store and is now blocked on an
	// acknowledgement that will never come. b claims over it from there.
	awaitGeneration(t, lease, 2)
	b.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(2)}

	deadline := time.After(testDeadline)
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

	// The loser's own claim installed a CONTROL binding in its sandbox on the
	// way in, and nothing else will replace it — a peer displacing a viewer
	// installs nothing, because a viewer has nothing to fence. The refused
	// answer is what re-points it, so the sandbox's copy of what this
	// attachment is agrees with the plane's. The pty fence had already made
	// the stale binding inert; this is what stops it lingering until the next
	// handoff.
	awaitSandbox(t, a.sandbox, func(got []terminal.ClientMessage) bool {
		last := ""
		for _, m := range got {
			if m.Type == terminal.TypeControl {
				last = m.Mode + "@" + string(m.Generation)
			}
		}
		return last == terminal.ModeView+"@3"
	}, "the refused claimant's binding, re-pointed at the viewer it is")
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
	deadline := time.After(testDeadline)
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
	p, h, ts := newTestPlane(t, Options{ControlAckTimeout: 1500 * time.Millisecond})
	lease := &fakeLease{gen: 1, holder: "att_aaaa"}

	// The incumbent's sandbox predates the protocol, so displacing it costs
	// the whole timeout — which is the window under test.
	laptop := startAttach(t, p, h, ts, control.AttachmentController, 1, fakeKeeper{lease, "att_aaaa"}, false)
	awaitType(t, laptop.stream, terminal.TypeAttached)
	awaitSpliced(t, laptop)

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

	deadline := time.After(testDeadline)
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
	p, h, ts := newTestPlane(t, Options{ControlAckTimeout: 1500 * time.Millisecond})
	lease := &fakeLease{gen: 1, holder: "att_aaaa"}

	laptop := startAttach(t, p, h, ts, control.AttachmentController, 1, fakeKeeper{lease, "att_aaaa"}, false)
	awaitType(t, laptop.stream, terminal.TypeAttached)
	awaitSpliced(t, laptop)
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
			keeper: keeper, mode: terminal.ModeControl, gen: 1, announce: make(chan struct{}, 1), ack: make(chan uint64, 1)}
		p.owners.add(o)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); o.demoteTo(1, 2) }() // the heartbeat, finishing
		go func() { defer wg.Done(); o.finish() }()       // the client, disconnecting
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

// TestATargetThatPredatesTheNegotiatedFlagIsStillToldEverything pins how a
// broker reads a target built before Negotiated existed. Such a composer sets
// the keeper and nothing else, and a keeper has always meant exactly this
// attach negotiated — so it is read that way. Read the other way, an
// installed client would stop being told its mode, its generation and every
// handoff, and from its end that is indistinguishable from an older plane.
func TestATargetThatPredatesTheNegotiatedFlagIsStillToldEverything(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{})
	lease := &fakeLease{gen: 6}
	f := startAttachOn(t, p, h, ts, control.AttachmentViewer, 6,
		fakeKeeper{lease, "att_aaaa"}, true, false /* negotiated unset */, true)

	m, _ := awaitType(t, f.stream, terminal.TypeAttached)
	if m.Mode != terminal.ModeView || m.Generation.Value() != 6 {
		t.Fatalf("a target with a keeper and no flag was told %s at %q, want view at 6", m.Mode, m.Generation)
	}
}

// TestANegotiatedClaimIsAlwaysAnswered pins the other half. A target with the
// flag and no keeper is the incoherent case in the other direction — a
// composer bug the contract forbids — and the honest answer is that the claim
// did not land, not silence: a take-control key that produces nothing at all
// is the one outcome a client cannot tell from a broken connection.
func TestANegotiatedClaimIsAlwaysAnswered(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{})
	f := startAttachOn(t, p, h, ts, control.AttachmentViewer, 6,
		nil /* no keeper */, true, true /* negotiated */, true /* and told it may claim */)

	awaitType(t, f.stream, terminal.TypeAttached)
	awaitViewerSpliced(t, f)
	f.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(6)}
	if m, _ := awaitType(t, f.stream, terminal.TypeStale); m.Generation.Value() != 6 {
		t.Fatalf("the claim was answered %q, want the 6 this attach holds", m.Generation)
	}
}

// TestDisplaceToRefusesAGenerationThisAttachHasPassed pins the check itself:
// an attach already at or past the generation a peer is announcing is left
// alone, because naming it a number it has passed would walk its client
// backwards.
func TestDisplaceToRefusesAGenerationThisAttachHasPassed(t *testing.T) {
	o := &ownership{mode: terminal.ModeControl, gen: 3, announce: make(chan struct{}, 1)}
	switch was, moved := o.displaceTo(2); {
	case moved:
		t.Fatal("a displacement to an older generation moved an attach that has passed it")
	case was != terminal.ModeControl:
		t.Fatalf("reported the previous mode as %q, want control", was)
	}
	if mode, gen := o.get(); mode != terminal.ModeControl || gen != 3 {
		t.Fatalf("after a refused displacement: %s at %d, want control at 3", mode, gen)
	}
}

// TestDisplaceToAndAdvanceAreEachOneStep is the atomicity, asserted as an
// invariant rather than as one reproduced interleaving. A displacement that
// reads an attach and then writes it can have a claim land between the two:
// the write then puts back the mode it read, at a generation OLDER than the
// one the claim won, and the client has already been told it has control.
//
// Whatever order these two run in, the answer is the same and there is only
// one: the claim at 3 either happens before the displacement to 2 (which then
// refuses) or after it (which then advances over it).
func TestDisplaceToAndAdvanceAreEachOneStep(t *testing.T) {
	for range 2000 {
		o := &ownership{mode: terminal.ModeView, gen: 1, announce: make(chan struct{}, 1)}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); o.advance(terminal.ModeControl, 3) }()
		go func() { defer wg.Done(); o.displaceTo(2) }()
		wg.Wait()
		if mode, gen := o.get(); mode != terminal.ModeControl || gen != 3 {
			t.Fatalf("a claim that won generation 3 came out %s at %d: "+
				"a displacement to 2 clobbered it between its own read and write", mode, gen)
		}
	}
}

// TestDemoteToNeverWalksTheGenerationBack pins the max. A demotion reads the
// current generation and can be overtaken by a peer's claim before it writes;
// moving the number down would leave this attach — and its client — claiming
// from a generation that has been superseded, refused every time.
func TestDemoteToNeverWalksTheGenerationBack(t *testing.T) {
	o := &ownership{mode: terminal.ModeControl, gen: 5, announce: make(chan struct{}, 1)}
	if got, moved := o.demoteTo(5, 3); got != 5 || !moved {
		t.Fatalf("demoteTo(5, 3) at generation 5 returned %d, %v; want 5, true", got, moved)
	}
	if mode, gen := o.get(); mode != terminal.ModeView || gen != 5 {
		t.Fatalf("after demoteTo(5, 3): %s at %d, want view at 5", mode, gen)
	}
	// And it still demotes at a generation it cannot read: being wrong about
	// the number is survivable, believing you still have control is not.
	o = &ownership{mode: terminal.ModeControl, gen: 5, announce: make(chan struct{}, 1)}
	if got, moved := o.demoteTo(5, 5); got != 5 || !moved {
		t.Fatalf("demoteTo(5, 5) returned %d, %v; want 5, true", got, moved)
	}
	if mode, _ := o.get(); mode != terminal.ModeView {
		t.Fatalf("after demoteTo at its own generation: mode %q, want view", mode)
	}
}

// TestDemoteToRefusesAGenerationThisAttachHasLeft is the guard every other
// transition already had. A demotion is an answer about ONE generation — the
// one this attach held when the heartbeat's renewal was refused — and it can
// be in flight across a store read and a bounded sandbox wait. An attach that
// won a newer generation inside that window has left the question behind, and
// demoting it anyway leaves the plane saying viewer while the store says this
// attach holds the lease: nobody types until the lease expires.
func TestDemoteToRefusesAGenerationThisAttachHasLeft(t *testing.T) {
	o := &ownership{mode: terminal.ModeControl, gen: 3, announce: make(chan struct{}, 1)}
	if got, moved := o.demoteTo(1, 3); moved {
		t.Fatalf("a demotion decided at generation 1 demoted an attach at 3 (to %d)", got)
	}
	if mode, gen := o.get(); mode != terminal.ModeControl || gen != 3 {
		t.Fatalf("after a refused demotion: %s at %d, want control at 3", mode, gen)
	}
}

// TestAdvanceAcceptsTheGenerationThisAttachAlreadyHolds pins the boundary.
// A departing peer's release announces the CURRENT generation to everybody
// behind it, which can set a still-waiting claimant to view at exactly the
// generation it has just won; the honest advance that follows must be allowed
// through, or the winner is told "somebody else got there first" about a
// generation it owns and holds the lease on.
func TestAdvanceAcceptsTheGenerationThisAttachAlreadyHolds(t *testing.T) {
	o := &ownership{mode: terminal.ModeView, gen: 4, announce: make(chan struct{}, 1)}
	if !o.advance(terminal.ModeControl, 4) {
		t.Fatal("a claim was refused its own generation")
	}
	if mode, gen := o.get(); mode != terminal.ModeControl || gen != 4 {
		t.Fatalf("after advance at the held generation: %s at %d, want control at 4", mode, gen)
	}
	if o.advance(terminal.ModeControl, 3) {
		t.Fatal("a generation older than the one this attach holds was accepted")
	}
}

// TestDisplaceAtGenerationZeroTouchesNobody pins the guard displaceTo is.
// Zero is not a generation any row is ever at — it is what `finish` passes
// when its store read failed — so it must demote nobody and leave every
// attach on the session at the generation it holds.
//
// Every peer is still ANNOUNCED to, because the fan-out no longer decides
// what a client hears from whether its state moved. What the announcement
// says is read under the announce hold, so a peer at control@2 is told
// control@2: its own state, never the zero that reached displace.
func TestDisplaceAtGenerationZeroTouchesNobody(t *testing.T) {
	p, _, _ := newTestPlane(t, Options{})
	stream := newScriptedStream()
	winner := &ownership{plane: p, session: "sess_example", announce: make(chan struct{}, 1)}
	peer := &ownership{plane: p, session: "sess_example", negotiated: true, stream: stream,
		mode: terminal.ModeControl, gen: 2, announce: make(chan struct{}, 1)}
	p.owners.add(winner)
	p.owners.add(peer)

	p.displace(context.Background(), winner, 0, false)

	if mode, gen := peer.get(); mode != terminal.ModeControl || gen != 2 {
		t.Fatalf("a displacement at generation zero moved a peer to %s at %d, want control at 2", mode, gen)
	}
	m := stream.nextServerMsg(t)
	if m.Type != terminal.TypeControlChanged || m.Mode != terminal.ModeControl ||
		m.Generation.Value() != 2 {
		t.Fatalf("the peer was sent %q %s at %q; a notice out of a displacement at generation "+
			"zero must still name what that attach IS, which is control at 2",
			m.Type, m.Mode, m.Generation)
	}
}

// ---------------------------------------------------------------------------
// what a client is told is what the plane READ (fourth review, findings 2/5/6)
// ---------------------------------------------------------------------------

// TestAControllerThatClaimsFromAStaleGenerationKeepsControl is the fourth
// review's second finding end to end, and it needs no race at all. A client
// that reads `controller.generation` — which this branch newly exposes — and
// claims from a value it no longer holds used to reach the store, be refused
// ErrStale, and be answered `stale`: its client switched to viewer and stopped
// typing while the plane still forwarded for it and its own heartbeat kept
// renewing its lease. Nobody could type for the rest of the 30s lease, and
// every new negotiated attach was admitted a viewer behind it.
//
// Two things close it, and this pins both from the client's side: a claim from
// the current controller never reaches the store at all, and no announcement
// asserts a mode it did not read.
func TestAControllerThatClaimsFromAStaleGenerationKeepsControl(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{})
	lease := &fakeLease{gen: 4, holder: "att_aaaa"}
	keeper := &countingKeeper{fakeKeeper: fakeKeeper{lease, "att_aaaa"}}
	f := startAttach(t, p, h, ts, control.AttachmentController, 4, keeper, true)
	if m, _ := awaitType(t, f.stream, terminal.TypeAttached); m.Mode != terminal.ModeControl {
		t.Fatalf("the controller opened as %s, want control", m.Mode)
	}
	awaitSpliced(t, f)

	// The generation it presents is one this session left long ago.
	f.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(1)}

	deadline := time.After(testDeadline)
	for answered := false; !answered; {
		select {
		case m := <-f.stream.out:
			if m.Type == terminal.TypeStale {
				t.Fatalf("the session's own controller was told stale@%q; its client stops typing "+
					"while its heartbeat keeps renewing the lease", m.Generation)
			}
			if m.Type == terminal.TypeAttached {
				if m.Mode != terminal.ModeControl || m.Generation.Value() != 4 {
					t.Fatalf("the controller's claim was answered %s at %q, want control at 4",
						m.Mode, m.Generation)
				}
				answered = true
			}
		case <-deadline:
			t.Fatal("the controller's claim was never answered at all")
		}
	}
	// The press cost the store one READ and no claim at all: the generation
	// the client named is not the question, what this attach still holds is.
	if n := keeper.n.Load(); n != 0 {
		t.Fatalf("%d claim(s) from the current controller reached the store", n)
	}
	lease.mu.Lock()
	gen, holder := lease.gen, lease.holder
	lease.mu.Unlock()
	if gen != 4 || holder != "att_aaaa" {
		t.Fatalf("a refused claim left the lease at generation %d held by %q, want 4 and att_aaaa", gen, holder)
	}

	// And it is still typing, under the generation it never left.
	f.stream.in <- terminal.ClientMessage{Type: "stdin", Data: []byte("ls\r")}
	awaitSandbox(t, f.sandbox, func(got []terminal.ClientMessage) bool {
		for _, m := range got {
			if m.Type == "stdin" {
				if m.Generation.Value() != 4 {
					t.Fatalf("the controller's keystroke was stamped %q, want the 4 it holds", m.Generation)
				}
				return true
			}
		}
		return false
	}, "the controller's keystroke")
}

// TestAClaimFromTheCurrentControllerNeverAdvancesTheGeneration is the same
// rule at the store. A claim is client-triggered and deliberately unlimited,
// and one from the attach that already holds control advances the generation,
// re-installs a binding the sandbox already has, fences the keystrokes this
// client has already sent, and spends an acknowledgement timeout on every
// peer — all to reach an answer the client already had.
func TestAClaimFromTheCurrentControllerNeverAdvancesTheGeneration(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{})
	lease := &fakeLease{gen: 1, holder: "att_aaaa"}
	keeper := &countingKeeper{fakeKeeper: fakeKeeper{lease, "att_aaaa"}}
	f := startAttach(t, p, h, ts, control.AttachmentController, 1, keeper, true)
	awaitType(t, f.stream, terminal.TypeAttached)
	awaitSpliced(t, f)

	// The honest version: the generation it actually holds.
	f.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(1)}
	if m, _ := awaitType(t, f.stream, terminal.TypeAttached); m.Generation.Value() != 1 {
		t.Fatalf("a claim from the controller answered generation %q, want the 1 it holds", m.Generation)
	}
	if n := keeper.n.Load(); n != 0 {
		t.Fatalf("%d claim(s) from the current controller reached the store", n)
	}
	lease.mu.Lock()
	gen, holder := lease.gen, lease.holder
	lease.mu.Unlock()
	if gen != 1 || holder != "att_aaaa" {
		t.Fatalf("after a self-claim the lease is generation %d held by %q, want 1 and att_aaaa", gen, holder)
	}
}

// TestAControllerDisplacedElsewhereRecoversOnOnePress is why the guard above
// is about AGREEMENT rather than about being the controller. A controller
// displaced by an attach on another replica is told nothing: it reads
// `control` at this plane until its own heartbeat renewal is refused, up to
// one interval later. Its user cannot type and presses the take-control key,
// which is the one thing they can do — and answering that from memory would
// tell them they have control, at a generation somebody else's lease has
// passed, with the store never consulted.
func TestAControllerDisplacedElsewhereRecoversOnOnePress(t *testing.T) {
	// The heartbeat is long, so the press is the only thing that can correct
	// this attach — which is the window under test.
	p, h, ts := newTestPlane(t, Options{HeartbeatInterval: time.Hour})
	lease := &fakeLease{gen: 1, holder: "att_here"}
	f := startAttach(t, p, h, ts, control.AttachmentController, 1, fakeKeeper{lease, "att_here"}, true)
	awaitType(t, f.stream, terminal.TypeAttached)
	awaitSpliced(t, f)

	// Another replica's attach takes control. Nothing tells this one.
	if _, err := (fakeKeeper{lease, "att_elsewhere"}).Claim(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	f.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(1)}

	m, _ := awaitType(t, f.stream, terminal.TypeStale)
	if m.Generation.Value() != 2 {
		t.Fatalf("the press was answered stale@%q, want the 2 that exists now", m.Generation)
	}
	// And the client now holds a generation one more press can take.
	f.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(2)}
	got, _ := awaitType(t, f.stream, terminal.TypeAttached)
	if got.Mode != terminal.ModeControl || got.Generation.Value() != 3 {
		t.Fatalf("the second press = %s at %q, want control at 3", got.Mode, got.Generation)
	}
}

// blockedConn is a sandbox socket that has stopped draining: every write
// blocks until its context runs out. It is not a broken socket — nothing
// fails, nothing closes — which is what makes it the hard case.
type blockedConn struct{}

func (blockedConn) Read(ctx context.Context) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (blockedConn) Write(ctx context.Context, _ []byte) error {
	<-ctx.Done()
	return ctx.Err()
}

func (blockedConn) Close() error { return nil }

// TestAClaimWhoseBindingNeverLandedGivesTheGenerationBack is the phantom
// controller, and it is unrecoverable if the claim takes control anyway. The
// store CAS succeeds, so the generation and the lease are this attach's; the
// binding write to its sandbox does not land, so the pty keeps discarding
// everything it types; and nothing corrects it — the heartbeat renews happily
// because the lease genuinely is this attach's, and the client's next press
// is answered out of the same wrong state.
//
// A claim that cannot be fenced at the sandbox is therefore not a claim. The
// generation goes back, and the client is told what exists now.
func TestAClaimWhoseBindingNeverLandedGivesTheGenerationBack(t *testing.T) {
	p, _, _ := newTestPlane(t, Options{ControlAckTimeout: 100 * time.Millisecond})
	lease := &fakeLease{gen: 1}
	stream := newScriptedStream()
	o := &ownership{plane: p, session: "sess_example", negotiated: true, mayClaim: true,
		keeper: fakeKeeper{lease, "att_aaaa"}, stream: stream, mode: terminal.ModeView, gen: 1,
		announce: make(chan struct{}, 1), ack: make(chan uint64, 1)}
	o.bindRunner(blockedConn{})

	o.claim(context.Background(), 1)

	if mode, gen := o.get(); mode != terminal.ModeView {
		t.Fatalf("an attach whose binding never reached its sandbox took control at %d; "+
			"the pty discards everything it types and nothing will correct it", gen)
	}
	m := stream.nextServerMsg(t)
	if m.Type != terminal.TypeStale {
		t.Fatalf("the claim was answered %q %s at %q, want stale", m.Type, m.Mode, m.Generation)
	}
	lease.mu.Lock()
	gen, holder := lease.gen, lease.holder
	lease.mu.Unlock()
	if holder != "" {
		t.Fatalf("the generation it could not use is still held by %q", holder)
	}
	if m.Generation.Value() != gen {
		t.Fatalf("the client was told generation %q while the store is at %d; "+
			"its next press would be refused too", m.Generation, gen)
	}
}

// TestAStaleAnswerNeverTellsALiveControllerItIsAViewer is the announcement
// half on its own, one level below the client pump — because the pump's own
// guard is not the only way in, and because one function deciding this under
// the hold that writes the message is the whole point of announceAs.
//
// The mirror case is in the same test: an attach BEHIND the store really did
// lose, and is told the generation it would have to claim from.
func TestAStaleAnswerNeverTellsALiveControllerItIsAViewer(t *testing.T) {
	p, _, _ := newTestPlane(t, Options{})
	for _, tc := range []struct {
		name     string
		mode     string
		held     uint64
		store    uint64
		wantType string
		wantMode string
		wantGen  uint64
	}{
		{"the live controller", terminal.ModeControl, 4, 4, terminal.TypeAttached, terminal.ModeControl, 4},
		{"an attach the session left behind", terminal.ModeView, 9, 10, terminal.TypeStale, "", 10},
		{"a controller the session left behind", terminal.ModeControl, 5, 7, terminal.TypeStale, "", 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lease := &fakeLease{gen: tc.store, holder: "att_holder"}
			stream := newScriptedStream()
			o := &ownership{plane: p, session: "sess_example", negotiated: true, mayClaim: true,
				keeper: fakeKeeper{lease, "att_aaaa"}, stream: stream,
				mode: tc.mode, gen: tc.held, announce: make(chan struct{}, 1), ack: make(chan uint64, 1)}

			o.sendStale(context.Background())

			m := stream.nextServerMsg(t)
			if m.Type != tc.wantType || m.Mode != tc.wantMode || m.Generation.Value() != tc.wantGen {
				t.Fatalf("a refused claim from %s at %d (store at %d) was answered %q %s at %q; "+
					"want %q %s at %d", tc.mode, tc.held, tc.store,
					m.Type, m.Mode, m.Generation, tc.wantType, tc.wantMode, tc.wantGen)
			}
		})
	}
}

// TestADisplacementNoticeReportsTheModeItReads pins the other half-path the
// fold closed. The notice a displacement and a demotion send used to hard-code
// `view`, so it could tell a live controller — one that won its own claim
// inside the loop that is displacing it — that it is a viewer, at a generation
// it holds the lease on.
func TestADisplacementNoticeReportsTheModeItReads(t *testing.T) {
	p, _, _ := newTestPlane(t, Options{})
	for _, mode := range []string{terminal.ModeControl, terminal.ModeView} {
		t.Run(mode, func(t *testing.T) {
			stream := newScriptedStream()
			o := &ownership{plane: p, negotiated: true, stream: stream, mode: mode, gen: 5,
				announce: make(chan struct{}, 1), ack: make(chan uint64, 1)}

			if got, gen := o.announceAs(context.Background(), terminal.TypeControlChanged, 0); got != mode || gen != 5 {
				t.Fatalf("announceAs reported %s at %d, want %s at 5", got, gen, mode)
			}
			m := stream.nextServerMsg(t)
			if m.Type != terminal.TypeControlChanged || m.Mode != mode || m.Generation.Value() != 5 {
				t.Fatalf("a %s attach was sent %q %s at %q, want control_changed %s at 5",
					mode, m.Type, m.Mode, m.Generation, mode)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// a peer that stopped reading holds nobody (fourth review, finding 1)
// ---------------------------------------------------------------------------

// stalledPeer starts a viewer attach and wedges its client socket: it is
// attached, nothing about it is broken, and it has stopped reading. No RST
// ever arrives for such a client, so nothing closes it and nothing times it
// out at the transport — which is precisely why every write the plane makes
// to a peer has to carry its own deadline.
func stalledPeer(t *testing.T, p *Plane, h *fakeHost, ts *httptest.Server,
	gen uint64, keeper control.ControllerLeaseKeeper) *attachFixture {
	t.Helper()
	f := startAttach(t, p, h, ts, control.AttachmentViewer, gen, keeper, true)
	awaitType(t, f.stream, terminal.TypeAttached)
	awaitViewerSpliced(t, f)
	f.stream.stall()
	return f
}

// TestAStuckPeerDoesNotStallAClaim is the fourth review's blocker, and it
// needs no concurrency at all — one viewer that stopped reading is the whole
// setup.
//
// The taker's claim has already advanced the store by the time the plane
// walks its peers, so the previous controller is fenced and nobody may type.
// Walked serially with an unbounded write, the notice to the stuck viewer
// never returned: the taker was never told it had won, its client stayed in
// `view` and sent nothing, and its own heartbeat renewed a lease it did not
// know it held. Zero working controllers for the life of the attach.
func TestAStuckPeerDoesNotStallAClaim(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{ControlAckTimeout: 250 * time.Millisecond})
	lease := &fakeLease{gen: 1, holder: "att_laptop"}

	laptop := startAttach(t, p, h, ts, control.AttachmentController, 1, fakeKeeper{lease, "att_laptop"}, true)
	awaitType(t, laptop.stream, terminal.TypeAttached)
	awaitSpliced(t, laptop)
	stuck := stalledPeer(t, p, h, ts, 1, fakeKeeper{lease, "att_stuck"})

	phone := startAttach(t, p, h, ts, control.AttachmentViewer, 1, fakeKeeper{lease, "att_phone"}, true)
	awaitType(t, phone.stream, terminal.TypeAttached)
	awaitViewerSpliced(t, phone)

	phone.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(1)}
	m, _ := awaitType(t, phone.stream, terminal.TypeAttached)
	if m.Mode != terminal.ModeControl || m.Generation.Value() != 2 {
		t.Fatalf("the winning claim was answered %s at %q, wanted control at 2", m.Mode, m.Generation)
	}

	// The peer that could not be reached cost the taker nothing: what it
	// missed is the notice, and its own heartbeat and its pty fence are what
	// the design leans on for it. (That it is fenced at the plane regardless
	// is TestAStalledExControllerIsFencedEvenThoughItsNoticeCouldNotBeDelivered.)
	stuck.stream.drain()
}

// TestAStuckPeerDoesNotStallANewControllerAttach is the same wedge on the
// attach path, where it was worse: the displacement runs BEFORE the client
// socket is parked and before the dial_attach is sent, so one stuck viewer
// stopped a brand-new controller attach from ever reaching its runner. The
// new client sat on an upgraded socket with no output and no close.
func TestAStuckPeerDoesNotStallANewControllerAttach(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{ControlAckTimeout: 250 * time.Millisecond})
	lease := &fakeLease{gen: 1, holder: "att_laptop"}

	laptop := startAttach(t, p, h, ts, control.AttachmentController, 1, fakeKeeper{lease, "att_laptop"}, true)
	awaitType(t, laptop.stream, terminal.TypeAttached)
	awaitSpliced(t, laptop)
	h.nextCmd(t) // the laptop's own dial_attach, so the next one read is the taker's
	stuck := stalledPeer(t, p, h, ts, 1, fakeKeeper{lease, "att_stuck"})
	h.nextCmd(t)

	// The application admits a new controller attach: it advanced the
	// generation before the broker was called at all.
	if gen, err := (fakeKeeper{lease, "att_new"}).Claim(context.Background(), 1); err != nil || gen != 2 {
		t.Fatalf("the application's own claim = %d, %v; want 2, nil", gen, err)
	}
	newer := startAttach(t, p, h, ts, control.AttachmentController, 2, fakeKeeper{lease, "att_new"}, true)

	if cmd := h.nextCmd(t); cmd.Type != "dial_attach" || cmd.Attach == nil {
		t.Fatalf("the new controller attach sent %+v, want a dial_attach", cmd)
	}
	if m, _ := awaitType(t, newer.stream, terminal.TypeAttached); m.Mode != terminal.ModeControl {
		t.Fatalf("the new controller attach opened as %s at %q, want control", m.Mode, m.Generation)
	}
	stuck.stream.drain()
}

// TestOneStalledPeerDoesNotHideAnothersDisplacement is the other half of the
// fan-out: peers are reached in parallel, so a peer that cannot be reached is
// never IN FRONT of one that can. Walked serially, everything behind the
// stalled socket — including the controller that has to be told it was
// displaced, and fenced at its sandbox — was simply never visited.
//
// The serial version fails this whenever a stalled peer is walked first,
// which is two times in three here; the fan-out passes it every time, and the
// suite runs it under -race -count=20.
func TestOneStalledPeerDoesNotHideAnothersDisplacement(t *testing.T) {
	// The heartbeat is turned off for the length of this test, because it is
	// the fallback being measured against: a displaced controller learns from
	// its own renewal being refused within one interval whatever the plane
	// does. What is under test is the notice the plane owes it AT ONCE.
	p, h, ts := newTestPlane(t, Options{ControlAckTimeout: 250 * time.Millisecond,
		HeartbeatInterval: time.Hour})
	lease := &fakeLease{gen: 1, holder: "att_laptop"}

	laptop := startAttach(t, p, h, ts, control.AttachmentController, 1, fakeKeeper{lease, "att_laptop"}, true)
	awaitType(t, laptop.stream, terminal.TypeAttached)
	awaitSpliced(t, laptop)
	stuckA := stalledPeer(t, p, h, ts, 1, fakeKeeper{lease, "att_stuck_a"})
	stuckB := stalledPeer(t, p, h, ts, 1, fakeKeeper{lease, "att_stuck_b"})

	phone := startAttach(t, p, h, ts, control.AttachmentViewer, 1, fakeKeeper{lease, "att_phone"}, true)
	awaitType(t, phone.stream, terminal.TypeAttached)
	awaitViewerSpliced(t, phone)

	phone.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(1)}

	m, _ := awaitType(t, laptop.stream, terminal.TypeControlChanged)
	if m.Mode != terminal.ModeView || m.Generation.Value() != 2 {
		t.Fatalf("the displaced controller was told %s at %q, want view at 2", m.Mode, m.Generation)
	}
	// And its sandbox was told, which is the fence the taker's answer depends
	// on — behind two sockets that are not draining.
	awaitSandbox(t, laptop.sandbox, func(got []terminal.ClientMessage) bool {
		for _, m := range got {
			if m.Type == terminal.TypeControl && m.Mode == terminal.ModeView && m.Generation.Value() == 2 {
				return true
			}
		}
		return false
	}, "the displaced controller's new binding")
	stuckA.stream.drain()
	stuckB.stream.drain()
}

// TestAStalledExControllerIsFencedEvenThoughItsNoticeCouldNotBeDelivered is
// the case the design's own tradeoff rests on. The notice a displaced peer
// gets is a courtesy; its fence is not, and neither is the plane's refusal to
// carry what it types. A controller whose client stopped reading is fenced at
// its sandbox and at the plane before the taker is told anything, and the
// message it never received costs it nothing but the notice.
func TestAStalledExControllerIsFencedEvenThoughItsNoticeCouldNotBeDelivered(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{ControlAckTimeout: 250 * time.Millisecond})
	lease := &fakeLease{gen: 1, holder: "att_laptop"}

	laptop := startAttach(t, p, h, ts, control.AttachmentController, 1, fakeKeeper{lease, "att_laptop"}, true)
	awaitType(t, laptop.stream, terminal.TypeAttached)
	awaitSpliced(t, laptop)
	laptop.stream.stall()

	phone := startAttach(t, p, h, ts, control.AttachmentViewer, 1, fakeKeeper{lease, "att_phone"}, true)
	awaitType(t, phone.stream, terminal.TypeAttached)
	awaitViewerSpliced(t, phone)

	phone.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(1)}
	if m, _ := awaitType(t, phone.stream, terminal.TypeAttached); m.Generation.Value() != 2 {
		t.Fatalf("the winning claim was answered %s at %q, want control at 2", m.Mode, m.Generation)
	}

	// Its sandbox has the viewer binding at the new generation...
	awaitSandbox(t, laptop.sandbox, func(got []terminal.ClientMessage) bool {
		for _, m := range got {
			if m.Type == terminal.TypeControl && m.Mode == terminal.ModeView && m.Generation.Value() == 2 {
				return true
			}
		}
		return false
	}, "the stalled ex-controller's new binding")
	// ...and the plane carries nothing more from it. The round trip is the
	// synchronisation point: the phone's keystroke is behind the laptop's on
	// no queue at all, so it is a fresh assertion rather than a sleep.
	laptop.stream.drain()
	laptop.stream.in <- terminal.ClientMessage{Type: "stdin", Data: []byte("y\r")}
	phone.stream.in <- terminal.ClientMessage{Type: "stdin", Data: []byte("n\r")}
	awaitSandbox(t, phone.sandbox, func(got []terminal.ClientMessage) bool {
		for _, m := range got {
			if m.Type == "stdin" {
				return true
			}
		}
		return false
	}, "the new controller's keystroke")
	for _, got := range laptop.sandbox.received() {
		if got.Type == "stdin" {
			t.Fatal("a displaced controller's keystroke was still carried")
		}
	}
}

// gatedKeeper holds a demotion still at the moment it is DECIDED: the renewal
// that is about to be refused. The test opens the gate once it has arranged
// the interleaving it wants to see, so the whole window — from the refusal to
// the write that acts on it — is deterministic rather than raced.
type gatedKeeper struct {
	fakeKeeper
	stale   atomic.Bool // renewals are refused while this is armed
	gate    chan struct{}
	entered chan struct{}
	once    sync.Once
}

func (k *gatedKeeper) Renew(ctx context.Context, generation uint64) error {
	if !k.stale.Load() {
		return k.fakeKeeper.Renew(ctx, generation)
	}
	k.once.Do(func() { close(k.entered) })
	<-k.gate
	return control.ErrStale
}

// TestADemotionThatWasSupersededDoesNotDemote is the fourth review's third
// finding, driven through the real plane. The interleaving is the one the
// heartbeat makes possible and nothing else corrects:
//
//  1. the laptop holds control at generation 1, and its renewal at that
//     generation is about to be refused — something on another replica took
//     over — which is the moment the demotion is decided;
//  2. that refusal is held in flight;
//  3. the phone claims and takes generation 2, displacing the laptop;
//  4. the laptop's own client takes it back at generation 3;
//  5. the demotion decided at generation 1 wakes up.
//
// Demoting there leaves the plane calling the laptop a viewer while the store
// says the laptop holds the lease at 3: it cannot type, its heartbeat renews
// a lease nobody can use, and every new negotiated attach behind it is
// admitted a viewer — for the rest of the 30s lease.
func TestADemotionThatWasSupersededDoesNotDemote(t *testing.T) {
	p, h, ts := newTestPlane(t, Options{HeartbeatInterval: 10 * time.Millisecond,
		ControlAckTimeout: 250 * time.Millisecond})
	lease := &fakeLease{gen: 1, holder: "att_aaaa"}
	keeper := &gatedKeeper{fakeKeeper: fakeKeeper{lease, "att_aaaa"},
		gate: make(chan struct{}), entered: make(chan struct{})}

	laptop := startAttach(t, p, h, ts, control.AttachmentController, 1, keeper, true)
	awaitType(t, laptop.stream, terminal.TypeAttached)
	awaitSpliced(t, laptop)
	phone := startAttach(t, p, h, ts, control.AttachmentViewer, 1, fakeKeeper{lease, "att_bbbb"}, true)
	awaitType(t, phone.stream, terminal.TypeAttached)
	awaitViewerSpliced(t, phone)

	// (1) and (2): the renewal is refused and the demotion parks in the store.
	keeper.stale.Store(true)
	<-keeper.entered

	// (3) the phone takes control...
	phone.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(1)}
	if m, _ := awaitType(t, phone.stream, terminal.TypeAttached); m.Generation.Value() != 2 {
		t.Fatalf("the phone's claim landed at %q, want 2", m.Generation)
	}
	if m, _ := awaitType(t, laptop.stream, terminal.TypeControlChanged); m.Mode != terminal.ModeView {
		t.Fatalf("the displaced laptop was told %s, want view", m.Mode)
	}
	// (4) ...and the laptop takes it straight back.
	laptop.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(2)}
	if m, _ := awaitType(t, laptop.stream, terminal.TypeAttached); m.Mode != terminal.ModeControl ||
		m.Generation.Value() != 3 {
		t.Fatalf("the laptop took control back as %s at %q, want control at 3", m.Mode, m.Generation)
	}

	// (5) the demotion decided at generation 1 finishes. Renewals are honest
	// again from here: this attach really does hold the lease at 3.
	keeper.stale.Store(false)
	close(keeper.gate)

	// The notice it sends says what the attach IS, which is the controller.
	m, _ := awaitType(t, laptop.stream, terminal.TypeControlChanged)
	if m.Mode != terminal.ModeControl || m.Generation.Value() != 3 {
		t.Fatalf("a superseded demotion told the session's controller it is %s at %q; "+
			"the store says it holds generation 3", m.Mode, m.Generation)
	}
	// And it installed NOTHING in the sandbox. A viewer binding written for
	// an attach that has since won a newer generation fences the controller
	// it just became: session.mayWriteLocked refuses every keystroke, and the
	// heartbeat renews happily because the lease really is this attach's. The
	// notice above is the demotion's last step, so by now any binding it was
	// going to write has been written.
	last := ""
	for _, got := range laptop.sandbox.received() {
		if got.Type == terminal.TypeControl {
			last = got.Mode + "@" + string(got.Generation)
		}
	}
	if last != terminal.ModeControl+"@3" {
		t.Fatalf("the laptop's sandbox was last told %s; it holds control at generation 3, "+
			"so the pty would refuse every keystroke it sends", last)
	}
	// And it is still typing, under the generation it won.
	laptop.stream.in <- terminal.ClientMessage{Type: "stdin", Data: []byte("ls\r")}
	awaitSandbox(t, laptop.sandbox, func(got []terminal.ClientMessage) bool {
		for _, m := range got {
			if m.Type == "stdin" {
				if m.Generation.Value() != 3 {
					t.Fatalf("the controller's keystroke was stamped %q, want the 3 it holds", m.Generation)
				}
				return true
			}
		}
		return false
	}, "the controller's keystroke")
}

// ackingConn is a sandbox that answers a binding the INSTANT the plane writes
// it — before the plane has parked on the acknowledgement channel at all,
// which is what a fast socket does and what makes the drain in installAndWait
// load-bearing rather than tidy.
type ackingConn struct{ o *ownership }

func (c ackingConn) Read(ctx context.Context) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (c ackingConn) Write(_ context.Context, b []byte) error {
	var m terminal.ClientMessage
	if json.Unmarshal(b, &m) == nil && m.Type == terminal.TypeControl {
		c.o.acked(m.Generation.Value())
	}
	return nil
}

func (c ackingConn) Close() error { return nil }

// TestAStaleAcknowledgementNeverCostsTheNextHandoffItsWait covers the drain at
// the top of installAndWait, which had no test at all and survived deletion.
//
// A handoff that gave up on an acknowledgement leaves the sandbox's answer to
// arrive afterwards, into a channel that holds exactly one. The NEXT handoff
// installs its binding and the sandbox answers at once — and that answer is
// dropped, because the previous handoff's is still sitting in the channel. The
// handoff then waits out the entire acknowledgement timeout for an answer it
// has already been given, which on a take-over is time the person pressing the
// key spends looking at a terminal that will not type.
func TestAStaleAcknowledgementNeverCostsTheNextHandoffItsWait(t *testing.T) {
	p, _, _ := newTestPlane(t, Options{ControlAckTimeout: 2 * time.Second})
	o := &ownership{plane: p, session: "sess_example", mode: terminal.ModeControl, gen: 2,
		announce: make(chan struct{}, 1), ack: make(chan uint64, 1)}
	o.bindRunner(ackingConn{o})
	// The previous handoff's acknowledgement, arriving after it gave up.
	o.acked(2)

	started := time.Now()
	if err := o.installAndWait(context.Background(), terminal.ModeView, 3); err != nil {
		t.Fatalf("installAndWait: %v", err)
	}
	if elapsed := time.Since(started); elapsed > p.ackTimeout/4 {
		t.Fatalf("a handoff whose sandbox answered at once took %s: it spent the wait on the "+
			"PREVIOUS handoff's acknowledgement", elapsed)
	}
}

// awaitOwners blocks until the plane is serving n attaches for session, read
// under the owner table's own lock. It is how a test says "this attach is
// registered" without reaching for a sleep — and what it proves is the point
// of the test below: registration does not wait for the client to speak.
func awaitOwners(t *testing.T, p *Plane, session control.SessionID, n int) {
	t.Helper()
	deadline := time.After(testDeadline)
	for {
		p.owners.mu.Lock()
		got := len(p.owners.m[session])
		p.owners.mu.Unlock()
		if got >= n {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("the plane is serving %d attaches for %s, want %d: an attach that has not "+
				"spoken yet is invisible to every peer", got, session, n)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// TestAnAttachStillReadingItsFirstMessageIsDisplacedLikeAnyOther closes the
// last window in which two devices are each told they have control.
//
// A controller attach is granted before the broker is called — the
// application advanced the generation already — and the client then has up to
// attachFirstMsgTimeout to send the resize the protocol opens with. An attach
// registered only after that read is, for the whole of it, a controller no
// peer can see: a claim on another attach takes its peer list without it, so
// nothing displaces it, and its own opening announcement then tells it it has
// control at a generation somebody else has passed. Both clients print
// [you have control]; nothing corrects the loser until its next heartbeat.
func TestAnAttachStillReadingItsFirstMessageIsDisplacedLikeAnyOther(t *testing.T) {
	// No heartbeat: the plane's own notice is what is under test, not the
	// fallback that eventually repairs it.
	p, h, ts := newTestPlane(t, Options{HeartbeatInterval: time.Hour})
	lease := &fakeLease{gen: 1}
	phone := startAttach(t, p, h, ts, control.AttachmentViewer, 1, fakeKeeper{lease, "att_phone"}, true)
	awaitType(t, phone.stream, terminal.TypeAttached)
	awaitViewerSpliced(t, phone)

	// The application admits the laptop as a controller: generation 2 is
	// already its own when the broker is called.
	if gen, err := (fakeKeeper{lease, "att_laptop"}).Claim(context.Background(), 1); err != nil || gen != 2 {
		t.Fatalf("the application's own claim = %d, %v; want 2, nil", gen, err)
	}
	sandbox := newFakeSandbox(true)
	h.dialBack = func(at *runner.Attach) { sandbox.serve(t, ts, at) }
	// A client that has not sent its opening resize yet. Nothing is wrong
	// with it; it is one round trip slower than the phone.
	stream := newScriptedStream()
	t.Cleanup(func() { stream.Close(errAttachEnded) })
	target := brokerTarget("sess_example", "vm1")
	target.Mode, target.ControllerGeneration = control.AttachmentController, 2
	target.Negotiated, target.MayClaim = true, true
	target.Controller = fakeKeeper{lease, "att_laptop"}
	go func() { _ = p.Broker().Attach(context.Background(), target, stream) }()

	// It is registered before it has said anything, so the phone can see it.
	awaitOwners(t, p, "sess_example", 2)
	phone.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(2)}
	if m, _ := awaitType(t, phone.stream, terminal.TypeAttached); m.Generation.Value() != 3 {
		t.Fatalf("the phone's claim landed at %q, want 3", m.Generation)
	}

	// Only now does the laptop finish opening — and it is told what it IS.
	stream.in <- terminal.ClientMessage{Type: "resize", Cols: 80, Rows: 24}
	m, _ := awaitType(t, stream, terminal.TypeAttached)
	if m.Mode == terminal.ModeControl {
		t.Fatalf("an attach displaced while it was still opening was told it has control at %q; "+
			"two devices are now printing [you have control]", m.Generation)
	}
	if m.Generation.Value() != 3 {
		t.Fatalf("it opened at generation %q, want the 3 that exists", m.Generation)
	}
}

// TestTheFanOutReachesEveryPeerAtOnce is the parallelism itself, which the
// per-step deadlines alone do not give. Bounded but serial, a take-over on a
// session with several devices that have stopped reading pays one deadline per
// device before it answers anybody — and a session with several such devices
// is exactly the session this feature ships for. Reached at once, it pays one.
func TestTheFanOutReachesEveryPeerAtOnce(t *testing.T) {
	// Six, not more: the fake host queues the dial_attach commands it is
	// handed and this test reads none of them, so the peers and the taker
	// have to fit inside that queue.
	const (
		peers = 6
		ack   = 250 * time.Millisecond
	)
	p, h, ts := newTestPlane(t, Options{ControlAckTimeout: ack, HeartbeatInterval: time.Hour})
	lease := &fakeLease{gen: 1}
	for i := range peers {
		stuck := stalledPeer(t, p, h, ts, 1, fakeKeeper{lease, "att_stuck_" + string(rune('a'+i))})
		t.Cleanup(stuck.stream.drain)
	}
	taker := startAttach(t, p, h, ts, control.AttachmentViewer, 1, fakeKeeper{lease, "att_taker"}, true)
	awaitType(t, taker.stream, terminal.TypeAttached)
	awaitViewerSpliced(t, taker)

	started := time.Now()
	taker.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(1)}
	if m, _ := awaitType(t, taker.stream, terminal.TypeAttached); m.Generation.Value() != 2 {
		t.Fatalf("the claim was answered %s at %q, want control at 2", m.Mode, m.Generation)
	}
	// Serial, this is peers × ack — two full seconds of a person waiting for
	// a key press to do anything. In parallel it is one ack, plus whatever
	// the machine is busy with.
	if elapsed := time.Since(started); elapsed > peers*ack/2 {
		t.Fatalf("a claim behind %d peers that had stopped reading took %s: they were walked "+
			"one after another, not at once", peers, elapsed)
	}
}

// TestABindingWriteThatNeverLandsDoesNotHoldTheHandoff is the deadline on the
// SANDBOX-facing half. A runner socket that has stopped draining is rarer than
// a client that has, and it holds exactly as much: the handoff waits on a
// write that will never complete, while the generation it is fencing moved in
// the store long ago.
func TestABindingWriteThatNeverLandsDoesNotHoldTheHandoff(t *testing.T) {
	p, _, _ := newTestPlane(t, Options{ControlAckTimeout: 200 * time.Millisecond})
	o := &ownership{plane: p, session: "sess_example", mode: terminal.ModeControl, gen: 2,
		announce: make(chan struct{}, 1), ack: make(chan uint64, 1)}
	o.bindRunner(blockedConn{})

	// A generous outer bound, so an unbounded write fails this as an
	// assertion rather than as a hung test.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	started := time.Now()
	if err := o.installAndWait(ctx, terminal.ModeView, 3); err == nil {
		t.Fatal("a binding write that never landed was reported as installed")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("the handoff waited %s on a runner socket that had stopped draining", elapsed)
	}
}

// TestAPeerAlreadyAtTheNewGenerationIsStillToldAboutIt is the fifth review's
// finding, and the last instance of the class the fourth round named: a path
// that decides what an attach is instead of reading it. The fourth round made
// every announcement read the state it reports; the decision whether to
// announce AT ALL was left asserting that a peer whose state is already at
// the new generation has already been told about it.
//
// It has not. Two paths move an attach's own state and then spend real time
// before announcing it, and a take-over landing in that gap skipped the peer
// entirely:
//
//  1. B claims: the store goes 1→2 and B waits on its own sandbox's
//     acknowledgement, which this test holds.
//  2. A's heartbeat renews generation 1, is refused, and A demotes itself: it
//     reads 2 from the store, moves its OWN state to view@2, and then blocks
//     in installAndWait against a sandbox that never answers — an older
//     sessiond, which this PR supports.
//  3. B's acknowledgement lands, B advances, and the fan-out reaches A —
//     already at generation 2, and so skipped.
//
// B is then told it has control while A has been told nothing at all, and A
// finds out only when its own wait times out: one full ControlAckTimeout in
// which two screens both say "you have control". Nothing corrects it in the
// meantime, because a client that believes it is the controller sends no
// claim and this attach's heartbeat renews nothing.
func TestAPeerAlreadyAtTheNewGenerationIsStillToldAboutIt(t *testing.T) {
	const ackTimeout = 2 * time.Second
	p, h, ts := newTestPlane(t, Options{
		HeartbeatInterval: 20 * time.Millisecond, ControlAckTimeout: ackTimeout})
	lease := &fakeLease{gen: 1, holder: "att_aaaa"}

	// A holds control, and its sandbox predates the protocol: every binding
	// it is sent costs the whole acknowledgement timeout.
	a := startAttach(t, p, h, ts, control.AttachmentController, 1, fakeKeeper{lease, "att_aaaa"}, false)
	awaitType(t, a.stream, terminal.TypeAttached)
	awaitSpliced(t, a)

	// B watches, and its sandbox answers only when this test says so.
	gate := make(chan struct{})
	sandbox := newFakeSandbox(true)
	sandbox.ackGate = gate
	b := startAttachWith(t, p, h, ts, control.AttachmentViewer, 1,
		fakeKeeper{lease, "att_bbbb"}, sandbox, true, true)
	awaitType(t, b.stream, terminal.TypeAttached)
	awaitViewerSpliced(t, b)

	// (1) B claims. The store moves, and B parks on the gated acknowledgement.
	b.stream.in <- terminal.ClientMessage{Type: terminal.TypeClaim, Expected: terminal.GenOf(1)}
	awaitGeneration(t, lease, 2)

	// (2) A's renewal at generation 1 is refused, so A demotes itself to
	//     view@2 and then blocks installing that binding. The binding
	//     reaching A's sandbox is the signal that A's own state has already
	//     moved — which is the whole precondition of this bug.
	awaitSandbox(t, a.sandbox, func(got []terminal.ClientMessage) bool {
		for _, m := range got {
			if m.Type == terminal.TypeControl && m.Mode == terminal.ModeView &&
				m.Generation.Value() == 2 {
				return true
			}
		}
		return false
	}, "the demoted controller's viewer binding")

	// (3) B's sandbox acknowledges, so B advances and displaces its peers.
	close(gate)

	if m, _ := awaitType(t, b.stream, terminal.TypeAttached); m.Mode != terminal.ModeControl ||
		m.Generation.Value() != 2 {
		t.Fatalf("B's claim was answered %s at %q, want control at 2", m.Mode, m.Generation)
	}
	// A must already have been told. The taker is not answered until the
	// fan-out is done, so by the time B's `attached` is readable A's notice
	// is in A's queue — and a peer that is skipped has nothing in it until
	// its own wait times out, a whole acknowledgement timeout later.
	select {
	case m := <-a.stream.out:
		if m.Type != terminal.TypeControlChanged || m.Mode != terminal.ModeView ||
			m.Generation.Value() != 2 {
			t.Fatalf("the displaced controller was sent %q %s at %q, want control_changed view at 2",
				m.Type, m.Mode, m.Generation)
		}
	default:
		t.Fatalf("B was told it has control while A, which still believed it had control, was told "+
			"nothing: A was skipped because its own demotion had already moved it to generation 2, "+
			"and it finds out only when its binding wait times out %s from now", ackTimeout)
	}
}

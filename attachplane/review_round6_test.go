package attachplane

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// ---------------------------------------------------------------------------
// helpers: one ownership in a plane's owner table, with a stream we can read
// ---------------------------------------------------------------------------

type fixPeer struct {
	o  *ownership
	st *scriptedStream
}

func mkFixPeer(t *testing.T, p *Plane, mode control.AttachmentMode, gen uint64,
	keeper control.ControllerLeaseKeeper, mayClaim bool) *fixPeer {
	t.Helper()
	st := newScriptedStream()
	o := newOwnership(p, control.AttachTarget{SessionID: "sess_example", Mode: mode,
		ControllerGeneration: gen, Negotiated: true, MayClaim: mayClaim, Controller: keeper,
		Policy: control.PolicyExclusive})
	o.stream = st
	p.owners.add(o)
	t.Cleanup(func() { p.owners.remove(o) })
	return &fixPeer{o: o, st: st}
}

// drain collects ownership messages this peer was sent, for d.
func (f *fixPeer) drain(d time.Duration) []terminal.ServerMessage {
	var out []terminal.ServerMessage
	deadline := time.After(d)
	for {
		select {
		case m := <-f.st.out:
			out = append(out, m)
		case <-deadline:
			return out
		}
	}
}

func fmtMsgs(ms []terminal.ServerMessage) string {
	var b strings.Builder
	for _, m := range ms {
		fmt.Fprintf(&b, "%s/%s/%d ", m.Type, m.Mode, m.Generation.Value())
	}
	if b.Len() == 0 {
		return "(nothing)"
	}
	return b.String()
}

// a runner socket that takes every write and never acknowledges anything.
type silentRunner struct{ writes chan []byte }

func newSilentRunner() silentRunner { return silentRunner{writes: make(chan []byte, 32)} }

func (s silentRunner) Read(ctx context.Context) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (s silentRunner) Write(_ context.Context, b []byte) error {
	select {
	case s.writes <- append([]byte(nil), b...):
	default:
	}
	return nil
}
func (s silentRunner) Close() error { return nil }

// ---------------------------------------------------------------------------
// FIX (3) — displace no longer equates "state did not move" with "client was
// told", and skips only a peer that has been told NOTHING and did not move.
// ---------------------------------------------------------------------------

func TestFix3TheThreeDisplaceCases(t *testing.T) {
	p := New(&probeHost{base: "ws://127.0.0.1:1", born: make(chan *probeSandbox, 1),
		mk: func() *probeSandbox { return newProbeSandbox(ackNow, 0) }},
		Options{HeartbeatInterval: time.Hour, ControlAckTimeout: 100 * time.Millisecond,
			Logf: func(string, ...any) {}})
	winner := mkFixPeer(t, p, control.AttachmentController, 9, nil, false)

	t.Run("moved-but-never-told", func(t *testing.T) {
		// The case the prompt asks about: a peer whose state this handoff DID
		// move and that has not been told anything yet. It must be told, and
		// the question is whether the first word is the right one.
		peer := mkFixPeer(t, p, control.AttachmentController, 1, nil, false)
		peer.o.bindRunner(newSilentRunner())
		if peer.o.spokenTo() {
			t.Fatal("a fresh ownership is already marked told")
		}
		p.displace(context.Background(), winner.o, 2, true)
		if got := peer.drain(200 * time.Millisecond); len(got) != 0 {
			t.Fatalf("moved-but-never-told peer was sent %s before its opening answer", fmtMsgs(got))
		}
		if m, g := peer.o.get(); m != terminal.ModeView || g != 2 {
			t.Fatalf("state after displacement: %s@%d — the fence must not wait for the client", m, g)
		}
		// Its opening answer reads the state at send time: the first word is
		// `attached view 2`, never "somebody took control from you".
		peer.o.announceAs(context.Background(), terminal.TypeAttached, 0)
		got := peer.drain(200 * time.Millisecond)
		if len(got) != 1 || got[0].Type != terminal.TypeAttached || got[0].Mode != terminal.ModeView ||
			got[0].Generation.Value() != 2 {
			t.Fatalf("first word to a moved-but-never-told peer: %s, want attached view 2", fmtMsgs(got))
		}
	})

	t.Run("not-moved-and-never-told", func(t *testing.T) {
		// The skip the fix deliberately kept: nothing has been said and
		// nothing moved, so the opening `attached` is still this client's
		// first word.
		peer := mkFixPeer(t, p, control.AttachmentViewer, 5, nil, false)
		p.displace(context.Background(), winner.o, 2, true)
		if got := peer.drain(200 * time.Millisecond); len(got) != 0 {
			t.Fatalf("a never-told peer at a newer generation was sent %s", fmtMsgs(got))
		}
		// And its own opening announce still works and is the first word.
		mode, gen := peer.o.announceAs(context.Background(), terminal.TypeAttached, 0)
		got := peer.drain(200 * time.Millisecond)
		if len(got) != 1 || got[0].Type != terminal.TypeAttached {
			t.Fatalf("opening word after a skipped displacement: %s", fmtMsgs(got))
		}
		t.Logf("FIX3/b: skipped, then opened with %s (announceAs reported %s@%d)", fmtMsgs(got), mode, gen)
	})

	t.Run("not-moved-but-already-told", func(t *testing.T) {
		// The skip the fix REMOVED: a peer already at or past gen whose
		// client has heard something must still be told, and must be told its
		// real state rather than `view @ gen`.
		peer := mkFixPeer(t, p, control.AttachmentController, 5, nil, false)
		peer.o.announceAs(context.Background(), terminal.TypeAttached, 0)
		if got := peer.drain(200 * time.Millisecond); len(got) != 1 {
			t.Fatalf("opening announce: %s", fmtMsgs(got))
		}
		if !peer.o.spokenTo() {
			t.Fatal("spokenTo is false after an announce")
		}
		p.displace(context.Background(), winner.o, 2, true)
		got := peer.drain(200 * time.Millisecond)
		if len(got) != 1 || got[0].Type != terminal.TypeControlChanged {
			t.Fatalf("an already-told peer at a newer generation was sent %s", fmtMsgs(got))
		}
		if got[0].Mode != terminal.ModeControl || got[0].Generation.Value() != 5 {
			t.Fatalf("the notice walked the client backwards: %s (peer really is control@5)", fmtMsgs(got))
		}
		t.Logf("FIX3/c: told %s — its own real state, not the handoff's", fmtMsgs(got))
	})
}

// TestFix3ADisplacedControllerMidOpeningIsToldBeforeTheTaker is the F1
// reproduction at its narrowest: a peer whose own demotion is parked in
// installAndWait (so its state has already moved and its client has not heard)
// must still be reached by a take-over's fan-out.
func TestFix3AParkedDemotionDoesNotHideThePeer(t *testing.T) {
	p := New(&probeHost{base: "ws://127.0.0.1:1", born: make(chan *probeSandbox, 1),
		mk: func() *probeSandbox { return newProbeSandbox(ackNever, 0) }},
		Options{HeartbeatInterval: time.Hour, ControlAckTimeout: 400 * time.Millisecond,
			Logf: func(string, ...any) {}})
	lease := &fakeLease{gen: 1, holder: "att_a"}
	winner := mkFixPeer(t, p, control.AttachmentViewer, 1, fakeKeeper{lease, "att_b"}, true)
	a := mkFixPeer(t, p, control.AttachmentController, 1, fakeKeeper{lease, "att_a"}, true)
	a.o.bindRunner(newSilentRunner())
	a.o.announceAs(context.Background(), terminal.TypeAttached, 0) // its opening word
	a.drain(100 * time.Millisecond)

	// A's own heartbeat demotion: moves A to view@1 then parks for the whole
	// ack timeout on a sandbox that never answers.
	lease.mu.Lock()
	lease.gen, lease.holder = 2, "att_b"
	lease.mu.Unlock()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); a.o.demote(context.Background(), 1) }()
	time.Sleep(60 * time.Millisecond) // A is now view@2 in the plane, client told nothing new

	if m, _ := a.o.get(); m != terminal.ModeView {
		t.Fatalf("A did not move itself: %s", m)
	}
	start := time.Now()
	p.displace(context.Background(), winner.o, 2, true)
	told := a.drain(50 * time.Millisecond)
	if len(told) == 0 {
		t.Fatalf("F1: the take-over's fan-out skipped a peer that had moved itself and said nothing; "+
			"the taker would be answered while A's screen still said `control` "+
			"(A state %s, %s into its parked demotion)", a.o.believesMode(), time.Since(start))
	}
	t.Logf("FIX3/F1: the parked-demotion peer was told %s inside the fan-out", fmtMsgs(told))
	wg.Wait()
}

func (o *ownership) believesMode() string { m, _ := o.get(); return m }

// ---------------------------------------------------------------------------
// FIX (2) — the give-back fan-out waits, and is still bounded
// ---------------------------------------------------------------------------

// TestFix2TheGiveBackWaitsForThePeersSandbox is the defect the fix removed:
// the give-back's fan-out returned before the displaced controller's sandbox
// had the view binding, so the next taker's displaceTo saw `was == view`,
// skipped installAndWait, and was answered with the old binding in flight.
func TestFix2TheGiveBackWaitsForThePeersSandbox(t *testing.T) {
	p := New(&probeHost{base: "ws://127.0.0.1:1", born: make(chan *probeSandbox, 1),
		mk: func() *probeSandbox { return newProbeSandbox(ackNow, 0) }},
		Options{HeartbeatInterval: time.Hour, ControlAckTimeout: 300 * time.Millisecond,
			Logf: func(string, ...any) {}})
	lease := &fakeLease{gen: 1, holder: "att_a"}

	a := mkFixPeer(t, p, control.AttachmentController, 1, fakeKeeper{lease, "att_a"}, true)
	aRunner := newSilentRunner() // takes the write, never acks
	a.o.bindRunner(aRunner)
	a.o.announceAs(context.Background(), terminal.TypeAttached, 0)
	a.drain(50 * time.Millisecond)
	go func() { a.drain(5 * time.Second) }()

	b := mkFixPeer(t, p, control.AttachmentViewer, 1, fakeKeeper{lease, "att_b"}, true)
	b.o.bindRunner(failRunner{blocked: make(chan struct{})}) // its install never lands
	go func() { b.drain(5 * time.Second) }()

	b.o.claim(context.Background(), 1) // the give-back path

	// By the time claim returns, A's sandbox must already have the new
	// binding: that is what "the give-back waits" buys the NEXT taker.
	select {
	case raw := <-aRunner.writes:
		s := string(raw)
		if !strings.Contains(s, `"view"`) {
			t.Fatalf("A's sandbox got %q, not a view binding", s)
		}
		t.Logf("FIX2/a: the give-back had already installed %q in A's sandbox when it returned", s)
	default:
		t.Fatal("FIX2/a: the give-back returned before the displaced controller's sandbox " +
			"had the view binding; the next taker will skip installAndWait for it")
	}
	if m, _ := a.o.get(); m == terminal.ModeControl {
		t.Fatal("FIX2/a: the previous controller is still `control` in the plane")
	}
}

// TestFix2TheGiveBackIsStillBounded is the adjacent interleaving the fix could
// plausibly break: wait=true against sandboxes that NEVER acknowledge. The
// give-back must cost at most a couple of acknowledgement timeouts in total,
// not one per peer, and must never be unbounded.
func TestFix2TheGiveBackIsStillBounded(t *testing.T) {
	const ack = 250 * time.Millisecond
	const peers = 6
	p := New(&probeHost{base: "ws://127.0.0.1:1", born: make(chan *probeSandbox, 1),
		mk: func() *probeSandbox { return newProbeSandbox(ackNever, 0) }},
		Options{HeartbeatInterval: time.Hour, ControlAckTimeout: ack, Logf: func(string, ...any) {}})
	lease := &fakeLease{gen: 1, holder: "att_a"}

	// Every peer is a controller in the plane at generation 1 (so each one
	// moves and each one needs installAndWait) onto a sandbox that never acks.
	for i := 0; i < peers; i++ {
		pe := mkFixPeer(t, p, control.AttachmentController, 1, fakeKeeper{lease, "att_a"}, true)
		pe.o.bindRunner(newSilentRunner())
		pe.o.announceAs(context.Background(), terminal.TypeAttached, 0)
		go func() { pe.drain(10 * time.Second) }()
	}
	b := mkFixPeer(t, p, control.AttachmentViewer, 1, fakeKeeper{lease, "att_b"}, true)
	b.o.bindRunner(failRunner{blocked: make(chan struct{})})
	go func() { b.drain(10 * time.Second) }()

	start := time.Now()
	done := make(chan struct{})
	go func() { b.o.claim(context.Background(), 1); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("FIX2/b: the give-back never returned against sandboxes that never acknowledge")
	}
	took := time.Since(start)
	// One ack timeout for the fan-out (all peers in parallel), plus slack.
	if took > 3*ack {
		t.Fatalf("FIX2/b: the give-back took %s with %d never-acking peers and one ack timeout of %s: "+
			"the fan-out is serialising", took, peers, ack)
	}
	t.Logf("FIX2/b: the give-back over %d never-acking sandboxes took %s (one ack timeout is %s)",
		peers, took, ack)
}

// ---------------------------------------------------------------------------
// FIX (4) — the write budget is against the bytes that reach the socket
// ---------------------------------------------------------------------------

func TestFix4TheBudgetScalesWithTheWireSize(t *testing.T) {
	s := wsTerminalStream{base: 10 * time.Second, rate: 1000}
	if got := s.budget(0); got != 10*time.Second {
		t.Fatalf("an empty frame gets %s, want the base", got)
	}
	// 3000 payload bytes are 4000 wire bytes at 1000 B/s = 4s on top of base.
	if got := s.budget(3000); got != 14*time.Second {
		t.Fatalf("budget(3000) = %s, want 14s (4000 wire bytes at 1000 B/s)", got)
	}
	// Rounding is up to the 4-byte base64 group.
	for payload, wire := range map[int]int{0: 0, 1: 4, 2: 4, 3: 4, 4: 8, 5: 8, 6: 8, 7: 12} {
		if got := wireSize(payload); got != wire {
			t.Fatalf("wireSize(%d) = %d, want %d", payload, got, wire)
		}
	}
	// A zero or negative rate degrades to the base rather than dividing by it.
	if got := (wsTerminalStream{base: time.Second, rate: 0}).budget(1 << 20); got != time.Second {
		t.Fatalf("rate 0: %s", got)
	}
	// What the shipped constants give the largest frame the read limit allows.
	ship := wsTerminalStream{base: defaultClientWriteBase, rate: defaultClientWriteRate}
	t.Logf("FIX4: the largest frame (%d payload, %d wire) gets %s; the doc comment says 60s+256s",
		attachReadLimit, wireSize(attachReadLimit), ship.budget(attachReadLimit))
	if ship.budget(attachReadLimit) < defaultClientWriteBase+250*time.Second {
		t.Fatalf("the largest frame gets only %s", ship.budget(attachReadLimit))
	}
}

// ---------------------------------------------------------------------------
// adjacent to FIX (3): the OTHER way a displaced controller ends up believing
// it still has control — the courtesy notice gives up on the announce hold
// ---------------------------------------------------------------------------

// TestAdjDisplacedWhileItsOpeningAnnounceIsMidWrite holds a peer's opening
// `attached` inside its announce hold (a client slow to take one small frame),
// displaces it from under that hold, and asks what its client ends up
// believing. announceAs reads the state when it takes the hold; displace's
// courtesy notice carries p.step and gives up on the hold rather than waiting.
func TestAdjDisplacedWhileItsOpeningAnnounceIsMidWrite(t *testing.T) {
	const ack = 200 * time.Millisecond
	p := New(&probeHost{base: "ws://127.0.0.1:1", born: make(chan *probeSandbox, 1),
		mk: func() *probeSandbox { return newProbeSandbox(ackNow, 0) }},
		Options{HeartbeatInterval: 20 * time.Millisecond, ControlAckTimeout: ack,
			Logf: func(string, ...any) {}})
	lease := &fakeLease{gen: 1, holder: "att_a"}
	winner := mkFixPeer(t, p, control.AttachmentViewer, 1, fakeKeeper{lease, "att_b"}, true)
	a := mkFixPeer(t, p, control.AttachmentController, 1, fakeKeeper{lease, "att_a"}, true)
	a.o.bindRunner(newSilentRunner())

	// A's client has stopped draining: its opening `attached` will sit in the
	// write for as long as the test wants, holding A's announce.
	a.st.stalled.Store(true)
	opened := make(chan struct{})
	go func() { defer close(opened); a.o.announceAs(context.Background(), terminal.TypeAttached, 0) }()
	time.Sleep(40 * time.Millisecond) // the hold is taken and the state is read

	// Somebody else wins generation 2 and fans out.
	lease.mu.Lock()
	lease.gen, lease.holder = 2, "att_b"
	lease.mu.Unlock()
	p.displace(context.Background(), winner.o, 2, true)

	// The socket drains again, so the held write lands.
	a.st.stalled.Store(false)
	close(a.st.draining)
	<-opened
	got := a.drain(3 * ack)
	if m, g := a.o.get(); m != terminal.ModeView || g != 2 {
		t.Fatalf("the plane does not think A was displaced: %s@%d", m, g)
	}
	last := terminal.ServerMessage{}
	if len(got) > 0 {
		last = got[len(got)-1]
	}
	t.Logf("A was sent: %s; the plane says view@2", fmtMsgs(got))
	if last.Type == terminal.TypeAttached && last.Mode == terminal.ModeControl {
		t.Errorf("ADJ: A's client was told `attached control %d` and nothing after it, while the plane "+
			"has A at view@2. The courtesy notice gave up on A's announce hold after one ack timeout "+
			"(p.step), A's own announceAs had already read `control` before the displacement, and no "+
			"heartbeat corrects a viewer — so this client believes it has control for the rest of the "+
			"attach, types into a plane that drops every frame, and its take-control key sends no claim "+
			"(internal/attachio claim() returns false when mode == control)", last.Generation.Value())
	}
}

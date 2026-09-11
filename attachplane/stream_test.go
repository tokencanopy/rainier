package attachplane

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// clientPair returns a live client stream and the socket on the other end of
// it, with the peer reading nothing until drain is closed. It is a client that
// is alive and slow, which is a different thing from one that is wedged — and
// the difference is what these two tests are about. base and rate are this
// stream's write budget, so a test spends milliseconds where production
// spends a minute without any of them sharing a variable.
func clientPair(t *testing.T, drain <-chan struct{}, base time.Duration, rate int) (control.TerminalStream, *websocket.Conn) {
	t.Helper()
	served := make(chan struct{})
	accepted := make(chan *websocket.Conn, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		accepted <- c
		<-served
	}))
	t.Cleanup(func() { close(served); ts.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	client, _, err := websocket.Dial(ctx, "ws"+ts.URL[len("http"):], nil)
	if err != nil {
		t.Fatalf("dialing the test client: %v", err)
	}
	t.Cleanup(func() { client.CloseNow() })
	client.SetReadLimit(attachReadLimit)
	go func() {
		<-drain
		for {
			if _, _, err := client.Read(context.Background()); err != nil {
				return
			}
		}
	}()
	c := <-accepted
	return clientStream(c, base, rate), c
}

// TestACourtesyNoticeNeverClosesAHealthyClient is the other side of the write
// deadline, and the thing it must not do. A websocket serialises its writes,
// so a handoff's `control_changed` — which carries ONE acknowledgement
// timeout, because a handoff must never wait on a client — queues behind a
// snapshot the plane is already replaying to that same client and expires
// waiting for the write lock. The socket has failed at nothing. Ending it
// there would let one take-over anywhere on the session disconnect a client
// that is merely reading a large scrollback over a slow link.
//
// The rule is that this stream closes on ITS OWN budget and on nobody else's.
func TestACourtesyNoticeNeverClosesAHealthyClient(t *testing.T) {
	drain := make(chan struct{})
	defer close(drain)
	stream, conn := clientPair(t, drain, defaultClientWriteBase, defaultClientWriteRate)

	// The snapshot, as the splice's output pump has it: a writer that owns
	// the socket and has not finished. Holding the lock explicitly is what
	// makes the queue below a fact rather than a hope.
	w, err := conn.Writer(context.Background(), websocket.MessageText)
	if err != nil {
		t.Fatalf("taking the write lock: %v", err)
	}

	// And now a handoff's courtesy notice, with a handoff's deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if err := stream.Send(ctx, terminal.ServerMessage{
		Type: terminal.TypeControlChanged, Mode: terminal.ModeView, Generation: "2"}); err == nil {
		t.Fatal("the notice was written while another writer held the socket")
	}

	// The snapshot finishes, and the client is still there.
	if _, err := w.Write([]byte(`{"type":"snapshot"}`)); err != nil {
		t.Fatalf("a courtesy notice timing out killed a healthy client mid-snapshot: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("finishing the snapshot: %v", err)
	}
	if err := stream.Send(context.Background(), terminal.ServerMessage{Type: "output", Seq: 2}); err != nil {
		t.Fatalf("the client was closed by somebody else's deadline: %v", err)
	}
}

// TestAClientThatNeverDrainsIsClosedFromBehindTheWriteLock is the case the
// explicit close IS for. A write that never acquires the conn's write lock
// never reaches the socket, so the websocket library's own teardown — which
// fires when a write in FLIGHT runs out of time — never happens. Without a
// close of our own, a client that has taken nothing for the whole of this
// stream's budget would be left open with writers parked behind it.
func TestAClientThatNeverDrainsIsClosedFromBehindTheWriteLock(t *testing.T) {
	never := make(chan struct{}) // a peer that never reads a byte
	defer close(never)
	stream, conn := clientPair(t, never, 300*time.Millisecond, defaultClientWriteRate)

	// Somebody else owns the socket and is not giving it back.
	if _, err := conn.Writer(context.Background(), websocket.MessageText); err != nil {
		t.Fatalf("taking the write lock: %v", err)
	}
	// The socket is closed when this fires, so a read that would otherwise
	// block forever is how the test sees it happen.
	closed := make(chan error, 1)
	go func() {
		_, _, err := conn.Read(context.Background())
		closed <- err
	}()

	started := time.Now()
	if err := stream.Send(context.Background(), terminal.ServerMessage{Type: "output", Seq: 2}); err == nil {
		t.Fatal("a write that never got the socket reported success")
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("the queued write took %s to give up", elapsed)
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("the socket was left open behind a client that had taken nothing for its whole budget")
	}
}

// TestAWedgedClientIsClosedRatherThanHeld is the transport half of the
// fourth review's blocker. A client that stops READING its socket — a closed
// lid, a TCP zero window, a paused browser tab — accepts a little and then
// accepts nothing, and no RST ever arrives to end it. An unbounded write to
// such a socket is an unbounded wait in whatever goroutine is carrying it, and
// this stream is what the plane and the application above it both write
// through.
//
// Without the deadline this test does not fail, it HANGS: the write never
// returns at all, which is exactly the production symptom.
func TestAWedgedClientIsClosedRatherThanHeld(t *testing.T) {
	const base = 250 * time.Millisecond
	never := make(chan struct{}) // the client that never reads
	defer close(never)
	// A rate high enough that the frames below buy milliseconds rather than
	// minutes: what this test is about is the base, and the frames are big
	// only because the kernel's own buffers take the first megabytes.
	stream, _ := clientPair(t, never, base, 64<<20)
	// Big frames, because the kernel's own buffers take the first megabytes
	// whatever the peer does. One of these writes blocks with nothing left to
	// unblock it.
	big := terminal.ServerMessage{Type: "output", Seq: 1, Data: make([]byte, 8<<20)}
	started := time.Now()
	var sendErr error
	for i := 0; i < 8 && sendErr == nil; i++ {
		sendErr = stream.Send(context.Background(), big)
	}
	if sendErr == nil {
		t.Fatal("a client that never read its socket absorbed 64MB of writes; the test cannot say anything")
	}
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Fatalf("the write to a wedged client took %s to give up", elapsed)
	}

	// And the socket is CLOSED rather than left open with a writer parked on
	// it: the next write fails at once instead of waiting out another
	// deadline.
	started = time.Now()
	if err := stream.Send(context.Background(), terminal.ServerMessage{Type: "output", Seq: 2}); err == nil {
		t.Fatal("the wedged socket was still open after its write deadline expired")
	} else if elapsed := time.Since(started); elapsed >= base {
		t.Fatalf("a write to the wedged socket took %s; it was held, not closed", elapsed)
	}
}

// TestTheWriteBudgetScalesWithTheFrame is the arithmetic on its own. A budget
// that did not scale would be a WHOLE-write deadline, and the largest frame
// attachReadLimit allows would then demand ≈273 KB/s of a client that is
// making perfectly steady progress — more than a 2 Mbit/s link has, and the
// answer to falling short is CloseNow.
func TestTheWriteBudgetScalesWithTheFrame(t *testing.T) {
	s := wsTerminalStream{base: defaultClientWriteBase, rate: defaultClientWriteRate}
	if got := s.budget(0); got != defaultClientWriteBase {
		t.Fatalf("a message with no payload gets %s, want the base %s", got, defaultClientWriteBase)
	}
	// The biggest payload this stream can carry: the read limit bounds the
	// WIRE frame, and Data travels base64, so the payload inside it is three
	// quarters of that.
	const biggest = attachReadLimit / 4 * 3
	got := s.budget(biggest)
	if want := defaultClientWriteBase +
		time.Duration(wireSize(biggest))*time.Second/defaultClientWriteRate; got != want {
		t.Fatalf("the largest frame gets %s, want %s", got, want)
	}
	// And the rate it demands is the rate this stream promises, measured
	// against the bytes that actually reach the socket. Budgeting the
	// payload instead would ask a third more of the client than the comment
	// and the doc say.
	if rate := float64(wireSize(biggest)) / (got - defaultClientWriteBase).Seconds(); rate >
		defaultClientWriteRate+1 {
		t.Fatalf("the largest frame demands %.0f B/s on the wire, which is more than the "+
			"%d B/s this stream promises to keep a client for", rate, defaultClientWriteRate)
	}
	// A stream with no rate is the base and nothing else, rather than a
	// division by zero.
	if got := (wsTerminalStream{base: time.Second}).budget(1 << 20); got != time.Second {
		t.Fatalf("a stream with no rate gave a 1MiB frame %s, want its base", got)
	}
}

// TestALargeFrameIsGivenTimeInProportionToItself is the same property through
// Send, so that the arithmetic above is the arithmetic the socket actually
// gets. Holding the conn's write lock is what makes it measurable: the send
// never reaches the socket at all, so what it spends is exactly its budget.
//
// The rate is absurdly slow and the payload correspondingly tiny on purpose.
// A test that bought its extra budget with a megabyte would measure
// json.Marshal under -race instead — which is most of a second for 2MiB, and
// enough to make this test pass with the scaling removed, at exactly the
// -count and the -race the gates run it under.
func TestALargeFrameIsGivenTimeInProportionToItself(t *testing.T) {
	const (
		base    = 100 * time.Millisecond
		rate    = 1024 // B/s: slow enough that half a kilobyte is half a second
		payload = 384  // and so 512 wire bytes, and so 500ms on top of the base
		scaled  = base + payload*4/3*time.Second/rate
	)
	elapsed := func(m terminal.ServerMessage) time.Duration {
		t.Helper()
		never := make(chan struct{}) // a peer that never reads a byte
		defer close(never)
		stream, conn := clientPair(t, never, base, rate)
		if _, err := conn.Writer(context.Background(), websocket.MessageText); err != nil {
			t.Fatalf("taking the write lock: %v", err)
		}
		started := time.Now()
		if err := stream.Send(context.Background(), m); err == nil {
			t.Fatal("a write that never got the socket reported success")
		}
		return time.Since(started)
	}

	small := elapsed(terminal.ServerMessage{Type: terminal.TypeControlChanged,
		Mode: terminal.ModeView, Generation: "2"})
	if small < base || small > base*3 {
		t.Fatalf("a message with no payload spent %s, want about its %s base", small, base)
	}
	big := elapsed(terminal.ServerMessage{Type: "snapshot", Seq: 1, Data: make([]byte, payload)})
	if big < scaled {
		t.Fatalf("a %dB frame spent %s, want at least %s: a whole-write deadline is what "+
			"disconnects a client that is making steady progress through a large snapshot",
			payload, big, scaled)
	}
	// And an UPPER bound, because the lower one alone is satisfied by
	// anything slow — including the marshalling this test deliberately keeps
	// negligible.
	if big > scaled+base*3 {
		t.Fatalf("a %dB frame spent %s, want about %s; the budget is not what bounded it",
			payload, big, scaled)
	}
}

// TestExecClientStreamIsOnTheExecBudget is the mitigation finding 8's plane
// half rests on, and until now `grep -rn 'ExecClientStream|defaultExecWriteBase'`
// found zero test references: swapping ExecClientStream for ClientStream
// survived the whole tree.
//
// The budgets differ for a reason about the SESSION rather than about the
// exec. Every attachment on one session shares one relay conn and one writer,
// so a peer that has stopped reading backs that writer up and the agent's
// terminal waits behind it. A minute is right for a person watching a screen —
// being disconnected mid-scrollback costs them their session — and twenty
// seconds is right for a script that can simply run the command again.
func TestExecClientStreamIsOnTheExecBudget(t *testing.T) {
	drain := make(chan struct{})
	defer close(drain)
	_, conn := clientPair(t, drain, defaultClientWriteBase, defaultClientWriteRate)

	es, ok := ExecClientStream(conn).(wsTerminalStream)
	if !ok {
		t.Fatal("ExecClientStream did not return this package's own stream")
	}
	as, ok := ClientStream(conn).(wsTerminalStream)
	if !ok {
		t.Fatal("ClientStream did not return this package's own stream")
	}
	if es.base != defaultExecWriteBase {
		t.Fatalf("an exec caller's write base is %s, want %s", es.base, defaultExecWriteBase)
	}
	if as.base != defaultClientWriteBase {
		t.Fatalf("an attach client's write base is %s, want %s", as.base, defaultClientWriteBase)
	}
	if es.base == as.base {
		t.Fatal("an exec caller and a terminal viewer are on the same write budget; " +
			"the whole point of the exec stream is that they are not")
	}
	if es.rate != as.rate {
		t.Fatalf("the per-byte rate differs (%d vs %d); only the BASE is shorter, so a "+
			"caller making steady progress on a large frame is unaffected", es.rate, as.rate)
	}
}

package relay

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/protocol/runner"
)

// ---------------------------------------------------------------------------
// one stuck client must not stall the rest
//
// readLoop demultiplexes every frame for every attachment on one session conn
// on ONE goroutine. See clientWriteBudget.
// ---------------------------------------------------------------------------

// stuckConn is a client that never takes a frame: its Write blocks until the
// caller's context gives up, which is what a client whose socket buffer is
// full and whose reader has stopped reading looks like from this side.
type stuckConn struct {
	mu      sync.Mutex
	closed  bool
	entered chan struct{}
	once    sync.Once
}

func newStuckConn() *stuckConn { return &stuckConn{entered: make(chan struct{})} }

func (c *stuckConn) Read(ctx context.Context) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (c *stuckConn) Write(ctx context.Context, _ []byte) error {
	c.once.Do(func() { close(c.entered) })
	<-ctx.Done()
	return ctx.Err()
}

func (c *stuckConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

func (c *stuckConn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// frameFor encodes one server frame for an attachment, carrying payload as its
// body.
func frameFor(t *testing.T, id uint64, body string) []byte {
	t.Helper()
	raw, err := Encode(Frame{Type: FrameServer, AttachID: id, Payload: json.RawMessage(`"` + body + `"`)})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestAStuckClientDoesNotStallTheOtherAttachments is the finding: a client
// that will not take its frame used to hold readLoop — and therefore every
// other attachment on the same session conn, and the session RPC with them —
// for as long as whatever was below was willing to wait, which for an exec
// caller is attachplane's twenty-second per-frame budget.
//
// With the bound, the stuck client is dropped and closed on its own budget and
// the viewer behind it gets its frame.
func TestAStuckClientDoesNotStallTheOtherAttachments(t *testing.T) {
	sessionEnd, runnerEnd := newPipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const budget = 150 * time.Millisecond
	h := newHub(ctx, runnerEnd, nil, budget, budget)
	defer h.Close()

	// Two attachments over the one session conn: a stuck exec caller and a
	// viewer. AttachClient assigns the ids in the order it is called.
	// Attached one at a time, because AttachClient assigns the ids and two
	// racing goroutines would not agree on which is which.
	stuck := newStuckConn()
	go h.AttachClient(ctx, stuck, Open{Kind: runner.KindExec})
	waitForClients(t, h, 1)
	viewer, viewerEnd := newPipe()
	go h.AttachClient(ctx, viewer, Open{})
	waitForClients(t, h, 2)

	// The session writes to the stuck attachment and then to the viewer.
	if err := sessionEnd.Write(ctx, frameFor(t, 1, "for-the-stuck-one")); err != nil {
		t.Fatalf("write to the stuck attachment: %v", err)
	}
	select {
	case <-stuck.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("readLoop never reached the stuck client")
	}
	if err := sessionEnd.Write(ctx, frameFor(t, 2, "for-the-viewer")); err != nil {
		t.Fatalf("write to the viewer: %v", err)
	}

	// The viewer's frame arrives. Comfortably more than the budget and
	// comfortably less than the twenty seconds an exec caller could hold this
	// loop for without it — a deadline that a regression fails on rather than
	// hangs on.
	readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
	defer readCancel()
	got, err := viewerEnd.Read(readCtx)
	if err != nil {
		t.Fatalf("the viewer's frame never arrived behind a stuck client: %v", err)
	}
	if string(got) != `"for-the-viewer"` {
		t.Fatalf("the viewer got %s", got)
	}

	// And the client that would not take its frame is gone: dropped from the
	// demux and closed, exactly as one whose write FAILED already was.
	deadline := time.Now().Add(5 * time.Second)
	for {
		h.mu.Lock()
		_, still := h.clients[1]
		h.mu.Unlock()
		if !still && stuck.isClosed() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the stuck client is still registered (%v) / open (%v)", still, !stuck.isClosed())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitForClients polls until the hub has registered n attachments. AttachClient
// registers on its own goroutine, so the alternative is a sleep.
func waitForClients(t *testing.T, h *Hub, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		h.mu.Lock()
		got := len(h.clients)
		h.mu.Unlock()
		if got == n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("the hub never registered %d attachments", n)
}

// TestAHealthyClientIsNotDroppedByTheBudget: the bound is on a client that has
// STOPPED reading, not on one that is merely taking its time. A frame taken
// well inside the budget keeps its attachment.
func TestAHealthyClientIsNotDroppedByTheBudget(t *testing.T) {
	sessionEnd, runnerEnd := newPipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := newHub(ctx, runnerEnd, nil, 2*time.Second, 2*time.Second)
	defer h.Close()
	client, clientEnd := newPipe()
	go h.AttachClient(ctx, client, Open{})
	waitForClients(t, h, 1)

	for i := 0; i < 5; i++ {
		if err := sessionEnd.Write(ctx, frameFor(t, 1, "tick")); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
		got, err := clientEnd.Read(readCtx)
		readCancel()
		if err != nil || string(got) != `"tick"` {
			t.Fatalf("frame %d: %s (%v)", i, got, err)
		}
	}
	h.mu.Lock()
	still := len(h.clients)
	h.mu.Unlock()
	if still != 1 {
		t.Fatalf("a healthy client was dropped; %d attachment(s) left", still)
	}
}

// TestTheProductionHubIsBounded pins the wiring, not just the mechanism.
// NewHubWithControl is what every host actually calls, and passing a zero here
// — which writeClient reads as "no bound at all" — would restore the very
// behaviour this change removes while leaving every test above green, because
// they all name their own budgets.
func TestTheProductionHubIsBounded(t *testing.T) {
	_, runnerEnd := newPipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := NewHubWithControl(ctx, runnerEnd, nil)
	defer h.Close()
	if h.termWrite <= 0 || h.execWrite <= 0 {
		t.Fatalf("the production hub writes clients unbounded: term=%s exec=%s",
			h.termWrite, h.execWrite)
	}
}

// TestTheHubNeverOverrulesThePlane is the ordering the two budgets exist
// inside, stated as an invariant rather than left in a comment.
//
// attachplane gives a TERMINAL viewer sixty seconds plus a byte allowance and
// an EXEC caller twenty plus the same allowance, and it made both trades
// deliberately — a person disconnected mid-scrollback loses their session,
// where a script has nothing to lose by re-running its command. A shorter
// budget on THIS hop would silently overrule those decisions on a hop that
// cannot see it is making them, and the human's half of that is the expensive
// one. internal/relay's own execWriterWait carries the same rule for the same
// reason (see TestTheWaitAndTheWriteHaveSeparateBudgets).
func TestTheHubNeverOverrulesThePlane(t *testing.T) {
	// attachplane's defaultClientWriteBase and defaultExecWriteBase. Spelled
	// as literals because they are unexported there, exactly as
	// TestTheWaitAndTheWriteHaveSeparateBudgets spells the exec one.
	const planeTerminalBase, planeExecBase = 60 * time.Second, 20 * time.Second
	if clientWriteBase <= planeTerminalBase {
		t.Fatalf("the hub gives a terminal viewer %s where the plane gives it %s; "+
			"this hop would drop a person the plane meant to keep",
			clientWriteBase, planeTerminalBase)
	}
	if execWriteBase <= planeExecBase {
		t.Fatalf("the hub gives an exec caller %s where the plane gives it %s",
			execWriteBase, planeExecBase)
	}
	// And the byte allowance has to be at least as generous as the plane's,
	// or the ordering above holds only for an empty frame. The plane budgets
	// the BASE64 size of a payload against the same rate, so budgeting the
	// raw wire bytes here — which is what this hop forwards — is the more
	// generous of the two for every size.
	if got, want := writeBudget(clientWriteBase, 16<<20),
		planeTerminalBase+time.Duration(16<<20)*time.Second/(64<<10); got <= want {
		t.Fatalf("on the largest frame the hub gives %s and the plane gives at least %s", got, want)
	}
}

// TestAnExecAttachmentGetsTheExecBudget: the kind reaches the budget. Without
// it every attachment would take the terminal one, and the case this bound was
// added for — an exec caller that has stopped reading — would wait out a
// person's budget instead of a script's.
func TestAnExecAttachmentGetsTheExecBudget(t *testing.T) {
	_, runnerEnd := newPipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := newHub(ctx, runnerEnd, nil, time.Minute, 10*time.Millisecond)
	defer h.Close()

	stuck := newStuckConn()
	go h.AttachClient(ctx, stuck, Open{Kind: runner.KindExec})
	waitForClients(t, h, 1)
	h.mu.Lock()
	cl := h.clients[1]
	h.mu.Unlock()
	if !cl.exec {
		t.Fatal("an exec attachment was not recorded as one")
	}
	// The write returns on the EXEC budget (10ms), not the terminal one (1m).
	done := make(chan error, 1)
	go func() { done <- h.writeClient(cl, []byte(`"x"`)) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a stuck client's write succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the write took the terminal budget, not the exec one")
	}
}

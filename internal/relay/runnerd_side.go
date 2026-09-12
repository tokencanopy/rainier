// internal/relay/runnerd_side.go
package relay

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/tokencanopy/rainier/protocol/runner"
)

// Conn is the minimal transport interface relay logic runs against, so it's
// testable with in-memory pipes instead of a real network. A *websocket.Conn
// adapter (read/write text frames) is added in Task 9/10 where the real
// transports are wired.
type Conn interface {
	Read(ctx context.Context) ([]byte, error)
	Write(ctx context.Context, b []byte) error
	Close() error
}

// Hub runs on the runnerd side: one Hub wraps one registered session conn
// (the single outbound WebSocket that session opened to runnerd) and lets
// many clients attach over it, each multiplexed by its own AttachID.
type Hub struct {
	conn    Conn
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.Mutex
	next    uint64
	clients map[uint64]*hubClient // attachID → client
	// onControl is wired by the constructor and never written again — see
	// NewHubWithControl for why it isn't a settable field.
	onControl func(payload []byte)
	// termWrite and execWrite are the per-write budgets below, named on the
	// hub so a test can drive the drop in milliseconds. Set by the constructor
	// and never written again, for the same reason onControl is: readLoop is
	// already running by the time a caller could assign one.
	termWrite, execWrite time.Duration
}

// hubClient is one attached client: its conn, and whether the attachment is an
// EXEC one. The kind is kept because the two get different write budgets, for
// the reason attachplane already gives them different ones.
type hubClient struct {
	conn Conn
	exec bool
}

// The per-write budgets readLoop gives one client for one frame.
//
// readLoop demultiplexes every frame for every attachment on this session's
// conn — terminal output, exec output and the session RPC alike — on ONE
// goroutine. A write with no bound of its own therefore makes any single stuck
// client a stall on all of them, for as long as whatever is below is willing
// to wait, and this package enforced nothing at all: the bound was the life of
// the session.
//
// The numbers are chosen to sit just ABOVE attachplane's own per-frame
// budgets, not below them, and that ordering is the whole point. The plane
// already decides which clients are worth keeping, and it has already made the
// trade this hop would otherwise re-make worse: sixty seconds for a terminal
// viewer, because "being disconnected mid-scrollback costs a person their
// session", and twenty for an exec caller, because a script has nothing to
// lose by re-running the command and everybody else on the session is waiting
// behind it. A shorter budget here would silently overrule both — dropping a
// human mid-scrollback on a hop that cannot see that is what it is doing.
//
// So what this bound buys is not a tighter decision, it is a decision at all:
//   - the local /attach front has NO plane below it, and was unbounded;
//   - a plane that is wedged, or a host that mounts a stream of its own
//     without the budgets, can no longer park this runner's demux for the life
//     of the session;
//   - the number is now stated in the package that pays for the wait.
//
// It is a mitigation and not a cure, exactly as attachplane's own comment
// says: the cure is a writer per attachment rather than one per conn, which
// changes the backpressure an exec stream depends on and is its own piece of
// work. See docs/design/exec-idle-stop.md.
const (
	// clientWriteBase is the part that does not depend on size: what a client
	// that has taken NOTHING gets. Ten seconds above the plane's sixty.
	clientWriteBase = 70 * time.Second
	// execWriteBase is the same for an exec attachment, five above the
	// plane's twenty — and above internal/relay's own execWriterWait rule
	// that a healthy exec caller must never be dropped by one hop merely
	// because another peer is being dropped at the next.
	execWriteBase = 25 * time.Second
	// clientWriteRate buys the rest of the budget for a large frame, at the
	// same floor the plane uses. Without it the base would be a whole-write
	// deadline and the largest frame attachReadLimit allows would demand
	// throughput of a client that is making perfectly steady progress.
	clientWriteRate = 64 << 10 // bytes per second
)

// writeBudget is how long ONE frame of n bytes may take to reach one client.
// n is the bytes that actually go on the wire — this hop forwards the payload
// verbatim — so unlike the plane's own budget there is no base64 adjustment to
// make.
func writeBudget(base time.Duration, n int) time.Duration {
	return base + time.Duration(n)*time.Second/clientWriteRate
}

func NewHub(ctx context.Context, sessionConn Conn) *Hub {
	return NewHubWithControl(ctx, sessionConn, nil)
}

// NewHubWithControl is NewHub plus a handler for the session's control
// events — the FrameControls that belong to the session itself (a setup
// outcome, a session-RPC request the sandbox originated, the response to one
// this hub sent with SendControl) rather than to any attachment, which is why
// they carry no meaningful AttachID and bypass the client demux in readLoop.
//
// The handler is wired here, in the constructor, rather than assigned to a
// field afterwards, and that is deliberate on two counts. Correctness: the
// constructor starts readLoop, so a later assignment would be a plain write
// racing that goroutine's read under the Go memory model — the same shape
// runnerd's OnEvent had to fix in Plan 3. Delivery: wiring it before the
// goroutine starts means the handler exists from the hub's very first frame,
// so a control event that arrives the instant a session registers cannot land
// in the window before a caller got around to installing a handler.
//
// onControl runs ON readLoop's goroutine, so it must hand off and return —
// the same contract as runnerd's OnEvent — because blocking in it stalls
// every attachment multiplexed over this conn, not just the control channel.
// nil means control frames are read and dropped.
func NewHubWithControl(ctx context.Context, sessionConn Conn, onControl func(payload []byte)) *Hub {
	return newHub(ctx, sessionConn, onControl, clientWriteBase, execWriteBase)
}

// newHub is NewHubWithControl with the two per-write budgets named, so a test
// can drive the drop in milliseconds instead of in a minute. It is unexported
// because the budgets are not a host's choice: they are a property of what
// this hop owes the OTHER attachments on the same conn, and of where they sit
// relative to the plane's.
func newHub(ctx context.Context, sessionConn Conn, onControl func(payload []byte),
	termWrite, execWrite time.Duration) *Hub {
	hctx, cancel := context.WithCancel(ctx)
	h := &Hub{conn: sessionConn, ctx: hctx, cancel: cancel, clients: map[uint64]*hubClient{},
		onControl: onControl, termWrite: termWrite, execWrite: execWrite}
	go h.readLoop()
	return h
}

// readLoop demultiplexes FrameServer/FrameClose from the session to the right
// client, and hands FrameControl to onControl instead (it belongs to the
// session, not to an attachment). Its deferred cleanup is what makes
// session-conn death cascade to every attached client: once h.conn.Read
// errors for good (conn dead), close every remaining client conn rather than
// leaving each one parked forever with no one left to ever write it another
// byte or a close.
func (h *Hub) readLoop() {
	defer func() {
		h.mu.Lock()
		for id, cl := range h.clients {
			cl.conn.Close()
			delete(h.clients, id)
		}
		h.mu.Unlock()
	}()
	for {
		raw, err := h.conn.Read(h.ctx)
		if err != nil {
			h.cancel()
			return
		}
		f, err := Decode(raw)
		if err != nil {
			continue
		}
		if f.Type == FrameControl {
			// Handled before the client lookup: a control frame carries
			// AttachID 0, which is never a client id (ids start at 1), so
			// the demux below would drop it as "unknown attachment".
			if h.onControl != nil {
				h.onControl(f.Payload)
			}
			continue
		}
		h.mu.Lock()
		client := h.clients[f.AttachID]
		h.mu.Unlock()
		if client == nil {
			continue
		}
		switch f.Type {
		case FrameServer:
			// Forward the terminal.ServerMessage payload verbatim to the
			// client, under a bound of its own: a client that cannot take a
			// frame in its budget is treated exactly as one whose write
			// FAILED, because from this loop's point of view — the one
			// goroutine every other attachment on this conn is also waiting on
			// — there is no useful difference between the two.
			if err := h.writeClient(client, f.Payload); err != nil {
				log.Printf("relay: dropping attachment %d: %v", f.AttachID, err)
				h.mu.Lock()
				delete(h.clients, f.AttachID)
				h.mu.Unlock()
				client.conn.Close()
			}
		case FrameClose:
			client.conn.Close()
			h.mu.Lock()
			delete(h.clients, f.AttachID)
			h.mu.Unlock()
		}
	}
}

// writeClient is one bounded write to one client, on the budget its KIND is
// owed. The deadline is derived from h.ctx, so a hub whose session conn has
// died fails immediately rather than spending the budget on a write that
// cannot land.
//
// A base of zero or less means "no bound of its own", which is what this wrote
// before the bound existed; no production path sets it, and it is here so the
// shape is explicit rather than accidental.
func (h *Hub) writeClient(client *hubClient, payload []byte) error {
	base := h.termWrite
	if client.exec {
		base = h.execWrite
	}
	if base <= 0 {
		return client.conn.Write(h.ctx, payload)
	}
	ctx, cancel := context.WithTimeout(h.ctx, writeBudget(base, len(payload)))
	defer cancel()
	return client.conn.Write(ctx, payload)
}

// SendControl writes payload to the session as a FrameControl on AttachID 0:
// the runnerd → sessiond direction of the control channel, the mirror of
// ControlSender.Send on the other end. It is what carries a session-RPC
// request down into a sandbox (a diff, a push) and a response back to one the
// sandbox originated. Like Send, it returns the conn's write error rather
// than dropping the frame silently — a caller with a pending request needs to
// know its request never left, or it waits for a response no one will send.
//
// Concurrency: this writes h.conn directly, with no mutex, unlike the
// sessiond side's connWriter. That is not an oversight — the hub already
// writes this conn from every concurrent AttachClient goroutine, and this
// write is sound for the same reason those are: coder/websocket serializes
// writers itself. Conn.Write takes the conn's message-writer lock before the
// first byte and holds it until the message is on the wire (v1.8.15,
// write.go: c.write → msgWriter.reset → mu.lock, released by the deferred
// unlock), so a second writer waits its turn instead of interleaving its
// frame's bytes into another's. A mutex here would re-serialize what the
// transport already serializes, and would not cover the AttachClient writes
// anyway.
//
// It takes h.ctx, so a write on a dead hub fails at once rather than parking
// on that lock: readLoop cancels that context when the session conn dies, and
// coder/websocket's lock is context-aware.
func (h *Hub) SendControl(payload []byte) error {
	b, err := Encode(Frame{Type: FrameControl, AttachID: 0, Payload: payload})
	if err != nil {
		return err
	}
	return h.conn.Write(h.ctx, b)
}

// Open is what one attachment opens with: the client's cursor and terminal
// size, plus the controller binding the control plane granted it. Mode is
// empty — and Generation zero — when no plane granted one, which leaves the
// attachment unbound and unconditional at the session, exactly as it is
// today.
//
// It is a struct rather than five parameters because the binding travels with
// the cursor and the size or it travels not at all: an attachment that
// installed its size and then its binding would have a window between them,
// and this hop is the last place that window could open.
type Open struct {
	Since      uint64
	Cols, Rows int
	Mode       string
	Generation uint64
	// Kind and Exec are the attachment's KIND and, for runner.KindExec, the
	// command it runs. Empty is the terminal, which is what every control
	// plane older than the field sends; the runner forwards both verbatim and
	// interprets neither, exactly as it forwards the binding.
	Kind string
	Exec *runner.ExecSpec
}

// AttachClient bridges a client conn to a new attachment over the session
// conn: it opens the attachment (FrameOpen), then pumps client → session as
// FrameClient until the client disconnects, at which point it tells the
// session to close the attachment too. Blocks until the client conn errors.
func (h *Hub) AttachClient(ctx context.Context, client Conn, o Open) error {
	h.mu.Lock()
	h.next++
	id := h.next
	h.clients[id] = &hubClient{conn: client, exec: o.Kind == runner.KindExec}
	h.mu.Unlock()

	open, _ := Encode(Frame{Type: FrameOpen, AttachID: id, Since: o.Since, Cols: o.Cols, Rows: o.Rows,
		Mode: o.Mode, Gen: o.Generation, Kind: o.Kind, Exec: o.Exec})
	if err := h.conn.Write(h.ctx, open); err != nil {
		// Session conn is already dead: this client would otherwise stay
		// registered in h.clients (and its caller left hanging with no
		// FrameOpen ever having reached the session) forever.
		h.mu.Lock()
		delete(h.clients, id)
		h.mu.Unlock()
		client.Close()
		return err
	}

	// Pump client → session as FrameClient until the client disconnects.
	for {
		raw, err := client.Read(ctx)
		if err != nil {
			cl, _ := Encode(Frame{Type: FrameClose, AttachID: id})
			h.conn.Write(h.ctx, cl)
			h.mu.Lock()
			delete(h.clients, id)
			h.mu.Unlock()
			// And the socket itself, which was the one exit of the five in
			// this file that did not. A read error is not a closed conn: a
			// websocket whose peer half-closed, or whose read simply failed,
			// still holds an fd — and the attach-back dial has no deferred
			// CloseNow of its own, so it sat in CLOSE_WAIT for runnerd's
			// life. With `rainier exec` that is one descriptor per COMMAND
			// rather than per human detach, which walks a CI loop to EMFILE.
			client.Close()
			return err
		}
		fr, _ := Encode(Frame{Type: FrameClient, AttachID: id, Payload: raw})
		if err := h.conn.Write(h.ctx, fr); err != nil {
			// Same cleanup as the client-read-error branch above, applied to
			// a session-conn write failure instead: readLoop is likely
			// already gone (its own h.conn.Read errored too), so nothing
			// will ever close this client conn or notify it otherwise.
			h.mu.Lock()
			delete(h.clients, id)
			h.mu.Unlock()
			client.Close()
			return err
		}
	}
}

// Done reports when the session conn has died — either readLoop noticed
// h.conn.Read fail on its own (container/network death) and called
// h.cancel(), or a caller invoked Close() directly. Both converge on the
// same h.ctx cancellation, so this is the one signal a caller needs to learn
// "this hub is no longer serving a live session conn" without duplicating
// readLoop's own liveness detection.
func (h *Hub) Done() <-chan struct{} { return h.ctx.Done() }

// Close is idempotent and safe to call more than once, including
// concurrently with itself or with readLoop's own h.cancel() call: cancel
// (from context.WithCancel) tolerates repeat calls by contract, and
// coder/websocket's CloseNow is a best-effort immediate close that likewise
// tolerates being called again on an already-closed conn. Callers relying on
// this: runnerd's /register handler calls it after Done() fires, and
// sessionOp's DELETE branch calls it directly to force that same Done() —
// both can legitimately race to close the same hub.
func (h *Hub) Close() { h.cancel(); h.conn.Close() }

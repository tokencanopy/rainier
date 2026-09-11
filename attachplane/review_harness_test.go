package attachplane

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// ---------------------------------------------------------------------------
// a sandbox whose acknowledgement behaviour a test picks
// ---------------------------------------------------------------------------

const (
	ackNow   = "now"
	ackLate  = "late"
	ackNever = "never"
	ackPrev  = "previous"
)

type probeSandbox struct {
	ack    string
	lateBy time.Duration

	mu    sync.Mutex
	got   []terminal.ClientMessage
	ready chan struct{}
	conn  relay.Conn
	once  sync.Once
}

func newProbeSandbox(ack string, lateBy time.Duration) *probeSandbox {
	return &probeSandbox{ack: ack, lateBy: lateBy, ready: make(chan struct{})}
}

func (s *probeSandbox) serve(ts *httptest.Server, at *runner.Attach) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	hdr := http.Header{}
	hdr.Set("Authorization", "Bearer "+testRunnerToken)
	c, _, err := websocket.Dial(ctx, wsBase(ts)+"/v0/runners/attach-back?attach_id="+at.AttachID,
		&websocket.DialOptions{HTTPHeader: hdr})
	cancel()
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
		if m.Type != terminal.TypeControl {
			continue
		}
		switch s.ack {
		case ackNever:
		case ackLate:
			go func(g terminal.Gen, mode string) {
				time.Sleep(s.lateBy)
				b, _ := json.Marshal(terminal.ServerMessage{
					Type: terminal.TypeControlAck, Mode: mode, Generation: g})
				_ = conn.Write(context.Background(), b)
			}(m.Generation, m.Mode)
		case ackPrev:
			prev := m.Generation.Value()
			if prev > 0 {
				prev--
			}
			b, _ := json.Marshal(terminal.ServerMessage{
				Type: terminal.TypeControlAck, Mode: m.Mode, Generation: terminal.GenOf(prev)})
			_ = conn.Write(context.Background(), b)
		default:
			b, _ := json.Marshal(terminal.ServerMessage{
				Type: terminal.TypeControlAck, Mode: m.Mode, Generation: m.Generation})
			_ = conn.Write(context.Background(), b)
		}
	}
}

func (s *probeSandbox) received() []terminal.ClientMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]terminal.ClientMessage(nil), s.got...)
}

// ---------------------------------------------------------------------------
// a host that serves many attaches at once
// ---------------------------------------------------------------------------

type probeHost struct {
	base string
	born chan *probeSandbox
	mk   func() *probeSandbox

	mu sync.Mutex
	ts *httptest.Server
}

func (h *probeHost) IdentifyRunner(_ context.Context, r *http.Request) (control.PoolID, control.RunnerID, error) {
	if r.Header.Get("Authorization") != "Bearer "+testRunnerToken {
		return "", "", control.ErrDenied
	}
	return "pool_test", "vm1", nil
}

func (h *probeHost) Send(_ control.PoolID, _ control.RunnerID, m runner.ToRunner) error {
	sb := h.mk()
	h.mu.Lock()
	ts := h.ts
	h.mu.Unlock()
	go sb.serve(ts, m.Attach)
	select {
	case h.born <- sb:
	default:
	}
	return nil
}

func (h *probeHost) BackURL(attachID string) string {
	return h.base + "/v0/runners/attach-back?attach_id=" + attachID
}

func newProbePlane(t *testing.T, o Options, mk func() *probeSandbox) (*Plane, *probeHost) {
	t.Helper()
	ts := httptest.NewUnstartedServer(nil)
	h := &probeHost{base: "ws" + "://" + ts.Listener.Addr().String(), born: make(chan *probeSandbox, 64), mk: mk}
	o.Logf = func(string, ...any) {}
	p := New(h, o)
	mux := http.NewServeMux()
	mux.Handle("GET /v0/runners/attach-back", p.BackHandler())
	ts.Config.Handler = mux
	ts.Start()
	h.mu.Lock()
	h.ts = ts
	h.mu.Unlock()
	t.Cleanup(ts.Close)
	return p, h
}

// ---------------------------------------------------------------------------
// one probed attach, with every ownership message it was told, timestamped
// ---------------------------------------------------------------------------

type told struct {
	at      time.Time
	typ     string
	mode    string
	gen     uint64
	dropped bool // the plane tried to say this and the write did not land
}

type probeAttach struct {
	name    string
	stream  *scriptedStream
	sandbox *probeSandbox
	done    chan error

	mu    sync.Mutex
	log   []told
	mode  atomic.Value // string: the mode this client currently BELIEVES
	gen   atomic.Uint64
	snaps atomic.Int64 // snapshots the client actually took
}

func (a *probeAttach) believes() string {
	v, _ := a.mode.Load().(string)
	return v
}

// probeStream records every ownership message AT THE MOMENT THE PLANE
// SUCCEEDS IN SENDING IT, which is the only timestamp the cross-client
// ordering invariant can be checked against: a buffered client channel lets
// two readers dequeue in an order the plane never wrote in.
type probeStream struct {
	*scriptedStream
	a *probeAttach
}

func (s probeStream) Send(ctx context.Context, m terminal.ServerMessage) error {
	err := s.scriptedStream.Send(ctx, m)
	if err != nil {
		switch m.Type {
		case terminal.TypeAttached, terminal.TypeStale, terminal.TypeControlChanged:
			s.a.mu.Lock()
			s.a.log = append(s.a.log, told{time.Now(), m.Type, m.Mode, m.Generation.Value(), true})
			s.a.mu.Unlock()
		}
		return err
	}
	switch m.Type {
	case terminal.TypeAttached, terminal.TypeStale, terminal.TypeControlChanged:
		s.a.mu.Lock()
		s.a.log = append(s.a.log, told{time.Now(), m.Type, m.Mode, m.Generation.Value(), false})
		s.a.mu.Unlock()
		if m.Type == terminal.TypeStale {
			s.a.mode.Store(terminal.ModeView)
		} else {
			s.a.mode.Store(m.Mode)
		}
		s.a.gen.Store(m.Generation.Value())
	}
	return nil
}

// watch drains what the plane wrote, so a buffered channel never becomes the
// back-pressure the test is not trying to model.
func (a *probeAttach) watch() {
	for {
		select {
		case m := <-a.stream.out:
			if m.Type == "snapshot" {
				a.snaps.Add(1)
			}
		case <-a.stream.dead:
			return
		}
	}
}

func (a *probeAttach) history() []told {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]told(nil), a.log...)
}

// startProbeAttach runs one negotiated attach and waits until its sandbox is
// spliced, so the test starts from a state where a handoff has somewhere to go.
func startProbeAttach(t *testing.T, p *Plane, h *probeHost, name string,
	mode control.AttachmentMode, gen uint64, keeper control.ControllerLeaseKeeper, mayClaim bool) *probeAttach {
	t.Helper()
	stream := newScriptedStream()
	stream.in <- terminal.ClientMessage{Type: "resize", Cols: 80, Rows: 24}
	target := brokerTarget("sess_example", "vm1")
	target.Mode = mode
	target.ControllerGeneration = gen
	target.Negotiated = keeper != nil
	target.MayClaim = mayClaim
	target.Controller = keeper

	done := make(chan error, 1)
	a := &probeAttach{name: name, stream: stream, done: done}
	a.mode.Store("")
	go a.watch()
	go func() { done <- p.Broker().Attach(context.Background(), target, probeStream{stream, a}) }()
	select {
	case sb := <-h.born:
		a.sandbox = sb
	case <-time.After(testDeadline):
		t.Fatalf("%s: no dial_attach reached the runner", name)
	}
	select {
	case <-a.sandbox.ready:
	case <-time.After(testDeadline):
		t.Fatalf("%s: the sandbox never dialled back", name)
	}
	// And the splice is actually pumping: the first frame the sandbox pushes
	// reaches this client.
	deadline := time.Now().Add(testDeadline)
	for {
		b, _ := json.Marshal(terminal.ServerMessage{Type: "output", Seq: 1, Data: []byte("x")})
		if a.sandbox.conn.Write(context.Background(), b) == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: the splice never started", name)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Cleanup(func() { stream.Close(errAttachEnded) })
	return a
}

// controllersInPlane counts the attaches this plane currently believes are
// controllers, read under the owner table's own lock.
func controllersInPlane(p *Plane, session control.SessionID) (n int, gen uint64) {
	p.owners.mu.Lock()
	owners := make([]*ownership, 0, len(p.owners.m[session]))
	for o := range p.owners.m[session] {
		owners = append(owners, o)
	}
	p.owners.mu.Unlock()
	for _, o := range owners {
		if m, g := o.get(); m == terminal.ModeControl {
			n++
			gen = g
		}
	}
	return n, gen
}

// ---------------------------------------------------------------------------
// S1 — N attaches racing claims
// ---------------------------------------------------------------------------

type failRunner struct{ blocked chan struct{} }

func (f failRunner) Read(ctx context.Context) ([]byte, error) {
	select {
	case <-f.blocked:
	case <-ctx.Done():
	}
	return nil, context.Canceled
}
func (f failRunner) Write(context.Context, []byte) error { return errProbeRunnerDown }
func (f failRunner) Close() error                        { return nil }

var errProbeRunnerDown = errorsNew("probe: the runner socket is down")

func errorsNew(s string) error { return &probeErr{s} }

type probeErr struct{ s string }

func (e *probeErr) Error() string { return e.s }

// TestProbeAGiveBackLeavesThePreviousControllerUndisplaced drives the claim
// path whose install never lands: the generation is given back to the store,
// and the attach that HELD control is left saying `control` at the plane,

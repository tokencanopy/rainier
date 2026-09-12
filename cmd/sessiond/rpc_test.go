package main

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/internal/relay"
)

// lastEvent decodes what a stubSender was last asked to send, which for the
// dispatcher is always one control event.
func lastEvent(t *testing.T, s *stubSender) relay.ControlEvent {
	t.Helper()
	sent := s.sentStrings()
	if len(sent) == 0 {
		t.Fatal("nothing was sent")
	}
	var ev relay.ControlEvent
	if err := json.Unmarshal([]byte(sent[len(sent)-1]), &ev); err != nil {
		t.Fatalf("decoding %q: %v", sent[len(sent)-1], err)
	}
	return ev
}

func errorText(t *testing.T, payload json.RawMessage) string {
	t.Helper()
	var body struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		t.Fatalf("decoding error payload %s: %v", payload, err)
	}
	return body.Error
}

// TestRPCServesInboundRequests covers the four outcomes a request arriving
// from controld can have. Every one of them answers: a request with no
// response leaves the far end holding a pending entry until its timeout, so
// "answer something" is the contract, not "answer when convenient".
func TestRPCServesInboundRequests(t *testing.T) {
	d := newRPCDispatcher()
	d.RegisterRPCHandler("echo", func(payload []byte) (any, error) {
		var in struct {
			Say string `json:"say"`
		}
		if err := json.Unmarshal(payload, &in); err != nil {
			return nil, err
		}
		return map[string]string{"said": in.Say}, nil
	})
	d.RegisterRPCHandler("boom", func([]byte) (any, error) { return nil, errors.New("it did not work") })
	d.RegisterRPCHandler("panics", func([]byte) (any, error) { panic("handler bug") })

	stub := &stubSender{}
	d.online(stub)

	t.Run("a registered handler's result", func(t *testing.T) {
		d.OnControl([]byte(`{"kind":"req:echo","id":1,"payload":{"say":"hi"}}`))
		ev := lastEvent(t, stub)
		if ev.Kind != "resp" || ev.ID != 1 || !ev.OK {
			t.Fatalf("event = %+v, want an ok resp for id 1", ev)
		}
		if string(ev.Payload) != `{"said":"hi"}` {
			t.Fatalf("payload = %s, want the handler's own result", ev.Payload)
		}
	})

	t.Run("a handler's error", func(t *testing.T) {
		d.OnControl([]byte(`{"kind":"req:boom","id":2}`))
		ev := lastEvent(t, stub)
		if ev.Kind != "resp" || ev.ID != 2 || ev.OK {
			t.Fatalf("event = %+v, want an ok:false resp for id 2", ev)
		}
		if got := errorText(t, ev.Payload); got != "it did not work" {
			t.Fatalf("error = %q, want the handler's own message", got)
		}
	})

	t.Run("an unregistered method", func(t *testing.T) {
		d.OnControl([]byte(`{"kind":"req:nope","id":3}`))
		ev := lastEvent(t, stub)
		if ev.OK || ev.ID != 3 {
			t.Fatalf("event = %+v, want an ok:false resp for id 3", ev)
		}
		if got := errorText(t, ev.Payload); !strings.Contains(got, "nope") {
			t.Fatalf("error = %q, want it to name the method", got)
		}
	})

	// A panicking handler must become a failed response, not a dead sessiond:
	// this process outlives the agent, the connection and every viewer, and a
	// panic on a handler goroutine would take the whole session down with it.
	t.Run("a panicking handler", func(t *testing.T) {
		d.OnControl([]byte(`{"kind":"req:panics","id":4}`))
		ev := lastEvent(t, stub)
		if ev.OK || ev.ID != 4 {
			t.Fatalf("event = %+v, want an ok:false resp for id 4", ev)
		}
		if got := errorText(t, ev.Payload); !strings.Contains(got, "panic") {
			t.Fatalf("error = %q, want it to say the handler panicked", got)
		}
	})
}

// TestRPCIgnoresUnroutableFrames: garbage, a request with no id to answer, and
// a response nobody is waiting for are each dropped without a reply and
// without a crash — the same discipline runnerd applies at its end of this
// channel.
func TestRPCIgnoresUnroutableFrames(t *testing.T) {
	d := newRPCDispatcher()
	stub := &stubSender{}
	d.online(stub)

	for _, payload := range []string{
		`not json`,
		`{"kind":"req:echo"}`,            // no id
		`{"kind":"resp","id":404}`,       // correlates to nothing
		`{"kind":"child_exited","rc":0}`, // an event, not an RPC
	} {
		d.OnControl([]byte(payload))
	}
	if n := stub.count(); n != 0 {
		t.Fatalf("%d frames sent, want none — nothing above is answerable", n)
	}
}

// TestRPCCallRoundTrips is the upstream direction: sessiond asks controld
// something (the credential mint, in production) and gets the answer back.
func TestRPCCallRoundTrips(t *testing.T) {
	d := newRPCDispatcher()
	stub := &stubSender{}
	d.online(stub)

	type result struct {
		payload json.RawMessage
		err     error
	}
	out := make(chan result, 1)
	go func() {
		p, err := d.Call("mint_git_credential", map[string]string{"host": "github.com"}, 3*time.Second)
		out <- result{p, err}
	}()

	waitFor(t, "the request to be sent", func() bool { return stub.count() > 0 })
	req := lastEvent(t, stub)
	if req.Kind != "req:mint_git_credential" {
		t.Fatalf("kind = %q, want \"req:mint_git_credential\"", req.Kind)
	}
	if req.ID == 0 {
		t.Fatal("id = 0; an upstream request with no id can never be answered")
	}
	if string(req.Payload) != `{"host":"github.com"}` {
		t.Fatalf("payload = %s, want the caller's own object", req.Payload)
	}
	if n := d.pendingCount(); n != 1 {
		t.Fatalf("%d pending calls while one is in flight, want 1", n)
	}

	answer, err := json.Marshal(relay.ControlEvent{Kind: "resp", ID: req.ID, OK: true,
		Payload: json.RawMessage(`{"token":"ghs_x"}`)})
	if err != nil {
		t.Fatal(err)
	}
	d.OnControl(answer)

	got := <-out
	if got.err != nil {
		t.Fatalf("Call: %v", got.err)
	}
	if string(got.payload) != `{"token":"ghs_x"}` {
		t.Fatalf("payload = %s, want the answer's own body", got.payload)
	}
	if n := d.pendingCount(); n != 0 {
		t.Fatalf("%d pending calls after the answer, want 0", n)
	}
}

// TestRPCWaitsForItsFirstConnection covers the boot race the git credential
// helper can lose. The chain's clone stage starts within milliseconds of the
// process, while dialLoop's websocket dial to runnerd is still in flight — and
// a Call with no connection fails at once by design (nothing is re-sent across
// a reconnect). Without a wait, a session whose runnerd happened to be
// restarting would fail its clone stage with "sessiond is not connected"
// instead of cloning.
func TestRPCWaitsForItsFirstConnection(t *testing.T) {
	d := newRPCDispatcher()
	if d.waitConn(20 * time.Millisecond) {
		t.Fatal("waitConn reported a connection that does not exist")
	}

	go func() {
		time.Sleep(30 * time.Millisecond)
		d.online(&stubSender{})
	}()
	if !d.waitConn(2 * time.Second) {
		t.Fatal("waitConn missed a connection that arrived while it was waiting")
	}

	// A live connection is not waited on at all: the common case pays nothing.
	start := time.Now()
	if !d.waitConn(time.Minute) {
		t.Fatal("waitConn missed the live connection")
	}
	if took := time.Since(start); took > 100*time.Millisecond {
		t.Fatalf("waitConn took %s for a connection that was already up", took)
	}

	// And a connection that goes away is not one to wait on either.
	d.offline()
	if d.waitConn(20 * time.Millisecond) {
		t.Fatal("waitConn reported a connection that had ended")
	}
}

// TestAgentSocketCallRidesOutABootRace: the in-sandbox socket's call is the
// only caller that can legitimately arrive before the relay exists, so it — and
// not Call itself — is where the wait lives.
func TestAgentSocketCallRidesOutABootRace(t *testing.T) {
	d := newRPCDispatcher()
	stub := &stubSender{}

	out := make(chan error, 1)
	go func() {
		_, err := agentSocketCall(d, nil)("mint_git_credential", nil)
		out <- err
	}()

	time.Sleep(20 * time.Millisecond) // the helper is waiting; nothing has been sent
	if stub.count() != 0 {
		t.Fatal("the request went out before there was a connection to send it on")
	}
	d.online(stub)

	waitFor(t, "the request to be sent once the relay came up", func() bool { return stub.count() > 0 })
	answer, err := json.Marshal(relay.ControlEvent{Kind: "resp", ID: lastEvent(t, stub).ID, OK: true,
		Payload: json.RawMessage(`{"token":"ghs_x"}`)})
	if err != nil {
		t.Fatal(err)
	}
	d.OnControl(answer)
	if err := <-out; err != nil {
		t.Fatalf("the call failed across the boot race: %v", err)
	}
}

// TestRPCCallSurfacesRefusals: an ok:false answer is not a transport failure.
// Its message is what the credential helper prints on stderr, which is how a
// user learns the named action they have to run, so it must survive verbatim.
func TestRPCCallSurfacesRefusals(t *testing.T) {
	d := newRPCDispatcher()
	stub := &stubSender{}
	d.online(stub)

	const msg = "github credentials need a refresh: run `rainier login --refresh github`"
	out := make(chan error, 1)
	go func() {
		_, err := d.Call("mint_git_credential", nil, 3*time.Second)
		out <- err
	}()
	waitFor(t, "the request to be sent", func() bool { return stub.count() > 0 })
	req := lastEvent(t, stub)
	body, err := json.Marshal(map[string]string{"error": msg})
	if err != nil {
		t.Fatal(err)
	}
	answer, err := json.Marshal(relay.ControlEvent{Kind: "resp", ID: req.ID, Payload: body})
	if err != nil {
		t.Fatal(err)
	}
	d.OnControl(answer)

	got := <-out
	if got == nil {
		t.Fatal("Call after an ok:false answer = nil, want the refusal")
	}
	if got.Error() != msg {
		t.Fatalf("error = %q, want the server's own message %q", got.Error(), msg)
	}
	if n := d.pendingCount(); n != 0 {
		t.Fatalf("%d pending calls after a refusal, want 0", n)
	}
}

// TestRPCCallFailsOnConnDeath: an upstream request is never re-sent across a
// reconnect (unlike the fire-and-forget events, which queue). The caller — git,
// through the credential helper — has to learn now, because retrying the git
// command is the natural and safe recovery.
func TestRPCCallFailsOnConnDeath(t *testing.T) {
	d := newRPCDispatcher()
	stub := &stubSender{}
	d.online(stub)

	out := make(chan error, 1)
	go func() {
		_, err := d.Call("mint_git_credential", nil, 30*time.Second)
		out <- err
	}()
	waitFor(t, "the request to be sent", func() bool { return stub.count() > 0 })
	d.offline()

	select {
	case err := <-out:
		if err == nil {
			t.Fatal("Call across a dead connection = nil, want an error")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Call did not fail when the connection died (it waited for its timeout)")
	}
	if n := d.pendingCount(); n != 0 {
		t.Fatalf("%d pending calls after the connection died, want 0", n)
	}
}

// TestRPCCallTimesOut bounds a call whose answer never comes, and proves the
// pending entry goes with it: a timeout is the exit with no delivery to clean
// up after it, so it has to clean up after itself.
func TestRPCCallTimesOut(t *testing.T) {
	d := newRPCDispatcher()
	stub := &stubSender{}
	d.online(stub)

	start := time.Now()
	if _, err := d.Call("diff", nil, 50*time.Millisecond); err == nil {
		t.Fatal("Call with no answer = nil, want a timeout")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("waited %s, want ~the 50ms timeout", elapsed)
	}
	if n := d.pendingCount(); n != 0 {
		t.Fatalf("%d pending calls after a timeout, want 0", n)
	}
}

// TestRPCCallWithoutAConnection: the helper can be invoked before sessiond has
// ever registered (or while it is between conns). There is nothing to wait for
// then, so the call fails immediately rather than parking a git process.
func TestRPCCallWithoutAConnection(t *testing.T) {
	d := newRPCDispatcher()
	if _, err := d.Call("mint_git_credential", nil, time.Second); err == nil {
		t.Fatal("Call with no connection = nil, want an error")
	}
	d.online(&stubSender{})
	d.offline()
	if _, err := d.Call("mint_git_credential", nil, time.Second); err == nil {
		t.Fatal("Call after the connection ended = nil, want an error")
	}
}

// TestRPCRepliesGoBackOnTheArrivingConnection: a handler can outlive the
// connection its request arrived on (a diff runs for seconds; a conn dies in
// milliseconds). Its answer still goes back over THAT connection, never
// whichever one happens to be live when it finishes — ids are per-connection,
// so a late answer written to a fresh connection could correlate against a
// request that merely reused the number. A write to the dead conn fails
// harmlessly; the initiator's own pending entry died with it.
func TestRPCRepliesGoBackOnTheArrivingConnection(t *testing.T) {
	d := newRPCDispatcher()
	release := make(chan struct{})
	entered := make(chan struct{})
	d.RegisterRPCHandler("slow", func([]byte) (any, error) {
		close(entered)
		<-release
		return map[string]bool{"done": true}, nil
	})

	first := &stubSender{}
	d.online(first)
	go d.OnControl([]byte(`{"kind":"req:slow","id":5}`))
	<-entered

	d.offline()
	second := &stubSender{}
	d.online(second)
	close(release)

	waitFor(t, "the answer on the connection the request arrived on", func() bool { return first.tries() > 0 })
	if ev := lastEvent(t, first); ev.ID != 5 || !ev.OK {
		t.Fatalf("event = %+v, want an ok resp for id 5", ev)
	}
	if n := second.tries(); n != 0 {
		t.Fatalf("%d frames written to the connection that replaced it, want 0", n)
	}
}

// ---------------------------------------------------------------------------
// the suspend notice
// ---------------------------------------------------------------------------

// recordingExecs is the exec runner as the suspend path sees it.
type recordingExecs struct {
	mu       sync.Mutex
	budget   time.Duration
	calls    int
	killAlls int
	left     int
}

func (r *recordingExecs) KillAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.killAlls++
}

func (r *recordingExecs) KillAllAndWait(budget time.Duration) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	r.budget = budget
	return r.left
}

func (r *recordingExecs) state() (int, time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls, r.budget
}

// recordingSender records the payloads a dispatcher sends upstream.
type recordingSender struct {
	mu   sync.Mutex
	sent [][]byte
	err  error
}

func (s *recordingSender) Send(p []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.sent = append(s.sent, append([]byte(nil), p...))
	return nil
}

func (s *recordingSender) events() []relay.ControlEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]relay.ControlEvent, 0, len(s.sent))
	for _, p := range s.sent {
		var ev relay.ControlEvent
		if json.Unmarshal(p, &ev) == nil {
			out = append(out, ev)
		}
	}
	return out
}

// suspendWired is the dispatcher wired the way main wires it, so these tests
// exercise the frame's whole journey through sessiond — OnControl's kind
// dispatch, the event handler, the kill, and the acknowledgement — rather than
// calling quiesceExecs directly.
func suspendWired(execs execKiller) (*rpcDispatcher, *recordingSender) {
	d := newRPCDispatcher()
	sender := &recordingSender{}
	d.online(sender)
	d.RegisterEventHandler(relay.KindSuspending, func(relay.ControlEvent) {
		quiesceExecs(execs, d, 77)
	})
	return d, sender
}

func suspendFrame(t *testing.T) []byte {
	t.Helper()
	b, err := json.Marshal(relay.ControlEvent{Kind: relay.KindSuspending})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestASuspendNoticeEndsEveryExec is the fix for the lifetime rule the CLI's
// help text states and the code did not have.
//
// The default `rainier stop` is a WARM suspend — docker pause — which freezes
// this process without ever delivering it a SIGTERM, so the shutdown handler
// that calls KillAll never runs. A detached `claude --continue` was therefore
// frozen and resumed rather than reaped. runnerd now sends this notice first,
// and the sandbox answers it.
func TestASuspendNoticeEndsEveryExec(t *testing.T) {
	execs := &recordingExecs{}
	d, sender := suspendWired(execs)

	d.OnControl(suspendFrame(t))

	calls, budget := execs.state()
	if calls != 1 {
		t.Fatalf("a suspend notice killed the execs %d times, want exactly once", calls)
	}
	if budget != execQuiesceBudget {
		t.Fatalf("the kill was given %s, want the quiesce budget %s", budget, execQuiesceBudget)
	}
	got := sender.events()
	if len(got) != 2 {
		t.Fatalf("the sandbox answered %+v, want an ack and then a ready", got)
	}
	if got[0].Kind != relay.KindSuspendAck || got[1].Kind != relay.KindSuspendReady {
		t.Fatalf("the sandbox answered %s then %s, want %s then %s",
			got[0].Kind, got[1].Kind, relay.KindSuspendAck, relay.KindSuspendReady)
	}
	for _, ev := range got {
		if ev.ID != 77 {
			t.Fatalf("a %s answered nonce %d, want the notice's own 77 — without the "+
				"echo a late answer satisfies the NEXT suspend", ev.Kind, ev.ID)
		}
	}
}

// TestTheSandboxSaysItHeardBeforeItStartsKilling is why there are two answers.
// runnerd cannot tell a sandbox that is working from one that predates this
// notice, and a session keeps the sessiond it booted with for life — so
// without an early "heard you" every session created before exec shipped would
// make every warm stop wait out the long budget, forever.
func TestTheSandboxSaysItHeardBeforeItStartsKilling(t *testing.T) {
	killing := make(chan struct{})
	execs := &blockingExecs{enter: killing, release: make(chan struct{})}
	d, sender := suspendWired(execs)

	done := make(chan struct{})
	go func() { defer close(done); d.OnControl(suspendFrame(t)) }()

	<-killing // the kill is under way and has not returned
	got := sender.events()
	if len(got) != 1 || got[0].Kind != relay.KindSuspendAck {
		t.Fatalf("before the kill finished the sandbox had said %+v, want the ack — "+
			"an old sandbox is indistinguishable from a working one without it", got)
	}
	close(execs.release)
	<-done
	if got := sender.events(); len(got) != 2 || got[1].Kind != relay.KindSuspendReady {
		t.Fatalf("after the kill the sandbox had said %+v", got)
	}
}

// blockingExecs parks inside the kill so a test can look at what has been said
// while it is still running.
type blockingExecs struct {
	enter   chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingExecs) KillAll() {}

func (b *blockingExecs) KillAllAndWait(time.Duration) int {
	b.once.Do(func() { close(b.enter) })
	<-b.release
	return 0
}

// TestASuspendNoticeIsAcknowledgedEvenWhenAnExecWillNotDie: runnerd freezes the
// container when it hears nothing, so a sandbox that gave up waiting has every
// reason to say so and none to stay silent.
func TestASuspendNoticeIsAcknowledgedEvenWhenAnExecWillNotDie(t *testing.T) {
	execs := &recordingExecs{left: 2}
	d, sender := suspendWired(execs)

	d.OnControl(suspendFrame(t))

	got := sender.events()
	if len(got) != 2 || got[1].Kind != relay.KindSuspendReady {
		t.Fatalf("a sandbox with a stubborn exec answered %+v, want an ack and a ready",
			got)
	}
}

// TestAnUnknownControlKindIsStillDropped: the event registry must not turn
// every unrecognised kind into something that runs. A sandbox is the far end
// of a channel that also carries a session's terminal traffic.
func TestAnUnknownControlKindIsStillDropped(t *testing.T) {
	execs := &recordingExecs{}
	d, sender := suspendWired(execs)

	b, err := json.Marshal(relay.ControlEvent{Kind: "not_a_kind_this_build_knows"})
	if err != nil {
		t.Fatal(err)
	}
	d.OnControl(b)

	if calls, _ := execs.state(); calls != 0 {
		t.Fatalf("an unknown control kind ran the suspend handler %d times", calls)
	}
	if got := sender.events(); len(got) != 0 {
		t.Fatalf("an unknown control kind produced %+v", got)
	}

}

// TestAPanickingEventHandlerDoesNotTakeTheSessionDown: event handlers run on
// relay's per-frame goroutines, and this process by design outlives its agent,
// its connection and every viewer.
func TestAPanickingEventHandlerDoesNotTakeTheSessionDown(t *testing.T) {
	d := newRPCDispatcher()
	d.online(&recordingSender{})
	d.RegisterEventHandler("boom", func(relay.ControlEvent) { panic("handler bug") })
	b, err := json.Marshal(relay.ControlEvent{Kind: "boom"})
	if err != nil {
		t.Fatal(err)
	}
	d.OnControl(b) // must return rather than unwind the process
}

// TestAShutdownSignalEndsEveryExec is the OTHER half of the lifetime rule, and
// the half nothing was checking: a cold stop and a destroy both arrive as a
// SIGTERM, and deleting execs.KillAll() from that handler left the whole tree
// green — the end-to-end test reached into the runner's KillAll directly and
// never exercised the wiring.
func TestAShutdownSignalEndsEveryExec(t *testing.T) {
	execs := &recordingExecs{}
	var order []string
	onShutdownSignal(
		func() { order = append(order, "stop-watching") },
		func() { order = append(order, "stop-agent") },
		execs,
		func() { order = append(order, "close-agents") },
	)
	execs.mu.Lock()
	killAlls := execs.killAlls
	execs.mu.Unlock()
	if killAlls != 1 {
		t.Fatalf("a shutdown signal killed the execs %d times, want exactly once — "+
			"a detached exec outlives its caller, never its session", killAlls)
	}
	want := []string{"stop-watching", "stop-agent", "close-agents"}
	if len(order) != len(want) {
		t.Fatalf("shutdown ran %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("shutdown ran %v, want %v", order, want)
		}
	}
}

// TestAShutdownSignalWithNoAgentSyncIsStillAShutdown: a session created with
// no agent manifest has no sync at all, and its execs still have to go.
func TestAShutdownSignalWithNoAgentSyncIsStillAShutdown(t *testing.T) {
	execs := &recordingExecs{}
	onShutdownSignal(func() {}, func() {}, execs, nil)
	execs.mu.Lock()
	defer execs.mu.Unlock()
	if execs.killAlls != 1 {
		t.Fatalf("killAlls = %d, want 1", execs.killAlls)
	}
}

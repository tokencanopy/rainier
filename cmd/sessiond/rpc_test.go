package main

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"rainier/internal/relay"
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

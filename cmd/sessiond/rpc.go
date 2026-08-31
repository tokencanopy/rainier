package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tokencanopy/rainier/internal/relay"
)

// This file is sessiond's end of the session RPC — both directions of the
// request/response channel that reaches from controld into this sandbox.
//
// Inbound (OnControl): a "req:<method>" frame arrives on the relay's control
// channel, the registered handler runs, and a "resp" goes back with the same
// id. relay hands each inbound control frame to its handler on a goroutine of
// its own, so a handler may take as long as its method needs — the terminal
// traffic multiplexed over the same conn is not waiting on it.
//
// Outbound (Call): this end asks controld for something — the credential mint
// the in-sandbox git helper needs — and waits for the answer, correlated by an
// id from this end's own counter.
//
// The two id spaces are independent and neither end remaps anything: a
// response always travels opposite its request, so a "resp" arriving here can
// only be answering a request this end sent.

// RPCHandler serves one inbound method. The payload is the request's body
// (nil when it had none) and the returned value becomes the response's, JSON
// encoded; an error becomes an ok:false response carrying its message, which
// is what the far end shows a user.
type RPCHandler func(payload []byte) (any, error)

// rpcConn is one connection's write side, with a channel that closes when that
// connection ends. Pending calls select on it: an upstream request is never
// re-sent across a reconnect (unlike the fire-and-forget events, which queue —
// see serveConn), so its caller has to learn the answer will never come. For
// the credential helper that is exactly right: git surfaces the failure and
// retrying the git command is the natural, safe recovery.
type rpcConn struct {
	sender controlSender
	done   chan struct{}
}

// rpcDispatcher owns both halves: the registry of methods this sandbox serves
// and the table of calls it is waiting on.
type rpcDispatcher struct {
	// conn is the live connection, swapped by dialLoop on every (re)connect.
	// atomic.Pointer rather than a plain field because Call, the handler
	// goroutines and dialLoop all touch it from different goroutines.
	conn atomic.Pointer[rpcConn]
	// seq is this end's id source for the requests it originates.
	seq atomic.Uint64

	mu sync.Mutex
	// handlers is written at boot (RegisterRPCHandler) and read by every
	// inbound frame afterwards. Under the same lock as pending because both
	// are small map touches and one lock is one fewer thing to reason about.
	handlers map[string]RPCHandler
	pending  map[uint64]chan relay.ControlEvent
}

func newRPCDispatcher() *rpcDispatcher {
	return &rpcDispatcher{
		handlers: map[string]RPCHandler{},
		pending:  map[uint64]chan relay.ControlEvent{},
	}
}

// RegisterRPCHandler installs fn as the handler for method. It is called at
// boot, before the relay is serving, so that a request arriving on a
// connection's first frame finds its method already installed.
func (d *rpcDispatcher) RegisterRPCHandler(method string, fn RPCHandler) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.handlers[method] = fn
}

// online installs the sender for a freshly established connection. Any
// connection it replaces is marked done first, which is what fails the calls
// still waiting on it.
func (d *rpcDispatcher) online(sender controlSender) {
	next := &rpcConn{sender: sender, done: make(chan struct{})}
	if old := d.conn.Swap(next); old != nil {
		close(old.done)
	}
}

// offline reports that the current connection has ended: pending calls fail
// (see rpcConn) and a new Call has nowhere to go until the next connection.
func (d *rpcDispatcher) offline() {
	if old := d.conn.Swap(nil); old != nil {
		close(old.done)
	}
}

// waitConn blocks until a connection is live, reporting whether one appeared
// within timeout. A live one returns immediately.
//
// It exists for exactly one caller — the in-sandbox agent socket (see
// agentSocketCall) — and deliberately is NOT folded into Call. Call fails at
// once with no connection because that is right for controld-driven work: the
// far end is already waiting and can retry. The helper's caller is a git
// process that started before this sessiond finished dialing runnerd, or during
// a reconnect, and its failure costs a whole boot chain.
//
// Polling rather than a notification: online/offline are a single atomic swap
// on the hot path, and a condition variable or a broadcast channel would put
// bookkeeping there to save a caller that runs once per git operation a few
// milliseconds.
func (d *rpcDispatcher) waitConn(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if d.conn.Load() != nil {
			return true
		}
		if !time.Now().Before(deadline) {
			return false
		}
		time.Sleep(connPollInterval)
	}
}

// connPollInterval is how often waitConn re-checks. Short enough to be
// invisible next to a git operation, long enough not to spin.
const connPollInterval = 25 * time.Millisecond

// OnControl is the handler relay calls for every control frame arriving from
// runnerd. It is wired into ServeSessionWithControl at construction, so it
// exists from the conn's first frame.
//
// Anything unroutable is logged and dropped rather than answered: a frame with
// no id has nothing to correlate a response against, and a response for an
// unknown id is one whose caller has already given up. Neither can be allowed
// to take down the channel — this runs per frame, and the session outlives
// every connection it ever has.
func (d *rpcDispatcher) OnControl(payload []byte) {
	var ev relay.ControlEvent
	if err := json.Unmarshal(payload, &ev); err != nil {
		log.Printf("undecodable control payload (%d bytes): %v", len(payload), err)
		return
	}
	switch {
	case ev.Kind == "resp":
		if ev.ID == 0 {
			log.Printf("control response with no id; dropping")
			return
		}
		if !d.deliver(ev) {
			log.Printf("control response for unknown request %d (timed out?); dropping", ev.ID)
		}
	case strings.HasPrefix(ev.Kind, "req:"):
		method := strings.TrimPrefix(ev.Kind, "req:")
		if method == "" || ev.ID == 0 {
			log.Printf("control request %q with no method or id; dropping", ev.Kind)
			return
		}
		d.serve(method, ev)
	default:
		log.Printf("control frame of unknown kind %q; ignoring", ev.Kind)
	}
}

// serve runs one inbound request and sends its response back over the
// connection the request ARRIVED on — never merely whichever one is live when
// the handler finishes. Ids are per-connection, so a late answer written to a
// fresh connection could correlate against a request that simply reused the
// number. A write to a dead conn fails harmlessly; the initiator's own pending
// entry died with it.
//
// It runs inline on the goroutine relay gave this frame (see
// ServeSessionWithControl): one goroutine per frame is already the contract, so
// starting a second here would buy nothing.
func (d *rpcDispatcher) serve(method string, ev relay.ControlEvent) {
	conn := d.conn.Load()
	reply := relay.ControlEvent{Kind: "resp", ID: ev.ID}
	out, err := d.invoke(method, ev.Payload)
	switch {
	case err != nil:
		reply.Payload = rpcErrorPayload(err.Error())
	default:
		raw, encErr := rpcPayload(out)
		if encErr != nil {
			log.Printf("encoding the answer to %s: %v", method, encErr)
			reply.Payload = rpcErrorPayload("the answer could not be encoded")
			break
		}
		reply.OK = true
		reply.Payload = raw
	}
	if conn == nil {
		log.Printf("no connection to answer %s (request %d) on; dropping the response", method, ev.ID)
		return
	}
	b, encErr := json.Marshal(reply)
	if encErr != nil {
		log.Printf("encoding the response to %s: %v", method, encErr)
		return
	}
	if err := conn.sender.Send(b); err != nil {
		log.Printf("answering %s (request %d): %v", method, ev.ID, err)
	}
}

// invoke looks up and runs a handler, turning both an unknown method and a
// panicking one into an ordinary error.
//
// The recover is not defensive dressing: handlers run on relay's per-frame
// goroutines, and an unrecovered panic on one of those takes the whole process
// down — this process, which by design outlives its agent, its connection and
// every viewer, and whose death loses the session's terminal scrollback for
// good. A method's bug must cost that method's caller an error, nothing more.
func (d *rpcDispatcher) invoke(method string, payload []byte) (out any, err error) {
	d.mu.Lock()
	fn, ok := d.handlers[method]
	d.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("unknown method %q", method)
	}
	defer func() {
		if r := recover(); r != nil {
			log.Printf("handler for %s panicked: %v", method, r)
			out, err = nil, fmt.Errorf("the handler for %q panicked: %v", method, r)
		}
	}()
	return fn(payload)
}

// deliver hands a response to the call waiting on its id, reporting whether
// anyone was. The channel is buffered by one and every caller removes its own
// entry, so this never blocks the frame's goroutine.
func (d *rpcDispatcher) deliver(ev relay.ControlEvent) bool {
	d.mu.Lock()
	ch, ok := d.pending[ev.ID]
	d.mu.Unlock()
	if !ok {
		return false
	}
	select {
	case ch <- ev:
	default:
	}
	return true
}

// Call asks controld for something and waits up to timeout for the answer,
// returning the response body. An ok:false answer comes back as an error
// carrying the far end's own message — for the credential mint that message is
// the named action the user has to run, and the git helper prints it verbatim.
//
// It fails immediately when there is no connection, and when the connection it
// sent on ends: nothing here is re-sent across a reconnect.
func (d *rpcDispatcher) Call(method string, payload any, timeout time.Duration) (json.RawMessage, error) {
	conn := d.conn.Load()
	if conn == nil {
		return nil, fmt.Errorf("%s: sessiond is not connected to its runner", method)
	}
	raw, err := rpcPayload(payload)
	if err != nil {
		return nil, fmt.Errorf("%s: encoding the request: %w", method, err)
	}

	id := d.seq.Add(1)
	ch := make(chan relay.ControlEvent, 1)
	d.mu.Lock()
	d.pending[id] = ch
	d.mu.Unlock()
	// The one cleanup path for every exit below — answer, conn death, timeout.
	// Nothing sweeps this table because nothing has to: the call that made an
	// entry is always the one that removes it.
	defer func() {
		d.mu.Lock()
		delete(d.pending, id)
		d.mu.Unlock()
	}()

	b, err := json.Marshal(relay.ControlEvent{Kind: "req:" + method, ID: id, Payload: raw})
	if err != nil {
		return nil, fmt.Errorf("%s: encoding the request: %w", method, err)
	}
	if err := conn.sender.Send(b); err != nil {
		return nil, fmt.Errorf("%s: sending the request: %w", method, err)
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case ev := <-ch:
		return rpcResult(method, ev)
	case <-conn.done:
		// select is random among ready cases: prefer an answer that landed in
		// the same instant the connection died.
		if ev, ok := drainRPC(ch); ok {
			return rpcResult(method, ev)
		}
		return nil, fmt.Errorf("%s: the connection to the runner ended before the answer", method)
	case <-timer.C:
		if ev, ok := drainRPC(ch); ok {
			return rpcResult(method, ev)
		}
		return nil, fmt.Errorf("%s: no answer within %s", method, timeout)
	}
}

func drainRPC(ch chan relay.ControlEvent) (relay.ControlEvent, bool) {
	select {
	case ev := <-ch:
		return ev, true
	default:
		return relay.ControlEvent{}, false
	}
}

// rpcResult turns a response into a call's outcome: an ok:false into an error
// carrying the far end's message verbatim, an ok into its payload.
func rpcResult(method string, ev relay.ControlEvent) (json.RawMessage, error) {
	if ev.OK {
		return ev.Payload, nil
	}
	if msg := rpcErrorText(ev.Payload); msg != "" {
		return nil, &upstreamError{Method: method, Msg: msg}
	}
	return nil, &upstreamError{Method: method}
}

// upstreamError is a refusal from controld — a request that arrived, was
// understood, and was declined. Its message is the far end's own, which is why
// Error() renders exactly that and nothing more.
type upstreamError struct {
	Method string
	Msg    string
}

func (e *upstreamError) Error() string {
	if e.Msg == "" {
		return fmt.Sprintf("%s was refused without a reason", e.Method)
	}
	return e.Msg
}

// rpcErrorText reads the {"error": "..."} body a failed response carries.
func rpcErrorText(payload json.RawMessage) string {
	if len(payload) == 0 {
		return ""
	}
	var body struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		return ""
	}
	return body.Error
}

// rpcErrorPayload is the {"error": ...} body every failed response carries —
// the shape controld reads a refusal's message out of, and the same one
// runnerd and controld write.
func rpcErrorPayload(msg string) json.RawMessage {
	b, err := json.Marshal(struct {
		Error string `json:"error"`
	}{msg})
	if err != nil {
		log.Printf("encoding an RPC error payload: %v", err)
		return nil
	}
	return b
}

// rpcPayload encodes a request or response body, rendering nil (and anything
// that encodes to JSON null) as no payload at all rather than the four bytes
// "null" — a method with no arguments puts nothing on the wire.
func rpcPayload(v any) (json.RawMessage, error) {
	switch p := v.(type) {
	case nil:
		return nil, nil
	case json.RawMessage:
		if len(p) == 0 {
			return nil, nil
		}
		return p, nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	if string(b) == "null" {
		return nil, nil
	}
	return b, nil
}

// pendingCount reports how many upstream calls are still waiting. Tests use it
// to prove every exit path takes its entry with it; production code does not
// read it.
func (d *rpcDispatcher) pendingCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.pending)
}

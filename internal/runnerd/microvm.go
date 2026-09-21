// internal/runnerd/microvm.go
//
// The runner's half of the microVM driver's two needs: where a guest's vsock
// connection goes, and where a cold resume's bootstrap token comes from.
// Both are implementations of driver.MicrovmHost, and both are here rather
// than in the driver because both are things only the runner can do — one
// needs the session registry, the other needs the control connection.
package runnerd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tokencanopy/rainier/internal/driver"
	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/runner"
)

var _ driver.MicrovmHost = (*Server)(nil)

// GuestConnected serves one microVM guest's control connection, exactly as
// the /register handler serves a container's WebSocket.
//
// On its own goroutine because the driver calls this from its accept loop and
// serveSessionConn blocks for the life of the conn — a loop that waited would
// never accept the guest's next dial.
//
// The context is this runner's own and not a request's, which is the one
// difference from the WebSocket door: there is no request. The hub's life is
// then bounded by the conn and by hub.Close(), which is what
// serveSessionConn's own teardown already relies on — `register` blocks on
// hub.Done() exactly because r.Context() does not reflect the socket dying.
//
// The session id is the DRIVER's, taken from the socket path Firecracker
// forwarded the connection to, and not anything the guest said. That is the
// whole difference between this door and the WebSocket one, where `register`
// believes a query parameter with no authentication on the hop at all.
func (s *Server) GuestConnected(sessionID string, conn relay.Conn) {
	if _, ok := s.reg.get(sessionID); !ok {
		// A guest for a session this runner no longer holds: a destroy that
		// raced the boot. Closing it is what tells the guest to stop; leaving
		// it open would leak the conn and its goroutine with no registry
		// entry that could ever reap them.
		log.Printf("microvm guest connected for unknown session %s; closing", sessionID)
		_ = conn.Close()
		return
	}
	go s.serveSessionConn(context.Background(), sessionID, conn)
}

// runnerOriginatedIDBase separates the request ids this RUNNER assigns from
// the ones a sandbox assigns.
//
// Both travel up the same connection as a "session_req" for the same session,
// and the control plane echoes whichever id it was given, so the two spaces
// must not collide: a response to the runner's mint carrying id 1 would
// otherwise be delivered into a sandbox that is waiting on its own id 1. The
// high bit is the cheapest disjoint space there is, and a sandbox counting
// from 1 will not reach it.
const runnerOriginatedIDBase uint64 = 1 << 63

// isRunnerOriginated reports whether an id came from this runner's own
// counter. forwardSessionRPC asks before it routes a response into a sandbox.
func isRunnerOriginated(id uint64) bool { return id&runnerOriginatedIDBase != 0 }

// refuseSandboxOrigin reports why an upward message must not be forwarded,
// or "" when it may be. It is the fence on the one door an untrusted peer
// has into the control plane's method table.
//
// It guards RESPONSES as well as requests, and that is not a formality: an
// id space is closed at both ends or it is not closed. A sandbox that may
// not send `req:` numbered 1<<63|n but may send `resp` numbered 1<<63|n has
// put an id from the runner's space on the wire either way — and the `resp`
// travels further, because the runner forwards it to the control plane
// rather than answering it there. `method` is the envelope's method, which
// is "resp" for a response, so both arms consult the same fence.
//
// Two things are refused, and only this hop can refuse either of them.
//
// A sandbox may not mint its own bootstrap token. The design's whole point
// is that the token is SINGLE-USE and lives 120 seconds: a guest that could
// ask for a fresh one whenever it liked would hold an unbounded, self-
// renewing capability to re-read its environment's current secrets, which is
// the property being removed rather than added. The method exists for a COLD
// RESUME, which is a thing the runner does and the guest cannot observe, and
// controld cannot tell the two apart — a "session_req" proves only that some
// runner sent it, and which end of the runner originated it is a fact only
// the runner has. (This applies to a Docker sandbox too, which is why it is
// not conditional on the driver.)
//
// And a sandbox may not use an id from the runner's own space. The high bit
// is how a response is routed back to a caller inside this process rather
// than into the sandbox (see forwardSessionRPC), and an id space the
// untrusted end can write into is not a space: a guest choosing
// 1<<63|n could have controld's echo delivered to a cold resume that is
// waiting on that number.
func refuseSandboxOrigin(method string, id uint64) string {
	switch {
	case method == runner.MethodMintSessionBootstrap:
		return "a sandbox may not mint its own bootstrap token; a fresh one is minted by the runner on a cold resume"
	case isRunnerOriginated(id):
		return "this id is reserved for the runner's own requests"
	}
	return ""
}

// runnerRPCTable is the pending table for requests this runner originated.
//
// It is deliberately tiny and deliberately separate from the sandbox's: the
// runner asks the control plane exactly one thing (a bootstrap token, on a
// cold resume), and a table shared with the forwarding path would make the
// forwarder a participant in the conversation it is supposed to be a pipe
// for.
type runnerRPCTable struct {
	seq atomic.Uint64
	mu  sync.Mutex
	// waiting is id → the one call waiting on it. Every caller removes its
	// own entry, so nothing sweeps this map.
	waiting map[uint64]pendingRunnerCall
}

// pendingRunnerCall is one in-flight request this runner made, and it records
// the SESSION it was made for as well as the channel to answer on.
//
// The session is half of the correlation, not decoration. An id alone says
// "some call is waiting on this number"; a mint for session A answered by a
// message naming session B would otherwise be delivered to A's caller, which
// would boot A's new VM with a token minted against B's row. The id spaces
// are per-process, the sessions are not, so the pair is what identifies a
// call.
type pendingRunnerCall struct {
	session string
	ch      chan runner.RPCEnvelope
}

func newRunnerRPCTable() *runnerRPCTable {
	return &runnerRPCTable{waiting: map[uint64]pendingRunnerCall{}}
}

func (t *runnerRPCTable) begin(session string) (uint64, chan runner.RPCEnvelope) {
	id := runnerOriginatedIDBase | t.seq.Add(1)
	ch := make(chan runner.RPCEnvelope, 1)
	t.mu.Lock()
	t.waiting[id] = pendingRunnerCall{session: session, ch: ch}
	t.mu.Unlock()
	return id, ch
}

func (t *runnerRPCTable) end(id uint64) {
	t.mu.Lock()
	delete(t.waiting, id)
	t.mu.Unlock()
}

// deliver hands a response to the call waiting on its id AND its session,
// reporting whether one was. The channel is buffered by one and every caller
// removes its own entry, so this never blocks the reader that called it.
//
// A mismatched session is not delivered anywhere: the caller learns nothing
// arrived and times out, which is the honest outcome for an answer that does
// not belong to the question.
func (t *runnerRPCTable) deliver(session string, env runner.RPCEnvelope) bool {
	t.mu.Lock()
	p, ok := t.waiting[env.ID]
	t.mu.Unlock()
	if !ok || p.session != session {
		return false
	}
	ch := p.ch
	select {
	case ch <- env:
	default:
	}
	return true
}

// mintBootstrapTimeout bounds one upward mint. It sits well under a cold
// resume's own patience and well over a round trip to a control plane that is
// answering: a resume that waits longer than this is waiting on a plane that
// is not going to answer, and reporting that is better than holding the
// session's resume open.
const mintBootstrapTimeout = 10 * time.Second

// MintSessionBootstrap asks the control plane for a fresh single-use token
// for sessionID, on the runner's own control connection.
//
// It is the one request this runner originates on the session RPC. The
// message carries no session id of its own — FromRunner.Session is the id,
// and the control plane answers from the row its placement guard read — which
// is what stops a runner holding session A from minting for session B.
//
// A runner with no control connection fails immediately rather than waiting:
// there is no queue behind this channel by design, and a cold resume that
// cannot get a token has to report that rather than boot a guest that will
// ask for its secrets and be refused.
func (s *Server) MintSessionBootstrap(ctx context.Context, sessionID string) (string, error) {
	body, err := json.Marshal(struct {
		Protocol int `json:"protocol"`
	}{runner.SessionBootstrapProtocolVersion})
	if err != nil {
		return "", fmt.Errorf("encoding the bootstrap mint request: %w", err)
	}

	id, ch := s.runnerRPC.begin(sessionID)
	defer s.runnerRPC.end(id)

	if !s.fireSessionRPC(sessionID, runner.RPCEnvelope{
		ID: id, Method: runner.MethodMintSessionBootstrap, Payload: body}) {
		return "", errors.New("this runner has no controld connection to mint a bootstrap token on")
	}

	timer := time.NewTimer(mintBootstrapTimeout)
	defer timer.Stop()
	var answer runner.RPCEnvelope
	select {
	case answer = <-ch:
	case <-ctx.Done():
		return "", fmt.Errorf("minting a bootstrap token for %s: %w", sessionID, ctx.Err())
	case <-timer.C:
		return "", fmt.Errorf("minting a bootstrap token for %s: no answer within %s", sessionID, mintBootstrapTimeout)
	}
	if !answer.OK {
		// The control plane's own sentence, relayed: it names the condition
		// a person can act on, and it never carries a token or a value.
		return "", errors.New(rpcErrorText(answer.Payload))
	}
	var minted struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(answer.Payload, &minted); err != nil {
		// Logged without the error and without the payload: a json message
		// quotes what it choked on, and what it choked on is the token.
		return "", errors.New("the control plane's bootstrap token could not be decoded")
	}
	if minted.Token == "" {
		return "", errors.New("the control plane answered a bootstrap mint with no token")
	}
	return minted.Token, nil
}

// defaultFlushWait is how long a guest gets to put what it has written on its
// block devices.
//
// A flush is a sync of the session's disks, and what is outstanding on them is
// whatever a setup script just installed — a node_modules tree, a Rust
// toolchain, a container image layer. Thirty seconds is the cold suspend's own
// budget (defaultColdSuspendReadyWait), which covers the same work plus an
// exec kill and an unmount, so it is the right order of magnitude and the one
// an operator has already seen.
//
// It is finite because the peer is untrusted: a guest that answers nothing
// must not hold a snapshot open forever, and a snapshot that gives up is a
// snapshot refused (the session keeps running, and the next one rebuilds the
// environment from its setup script).
const defaultFlushWait = 30 * time.Second

// FlushGuest asks sessionID's sandbox to put what it has written on its block
// devices, and returns when it says it has.
//
// It is the third of driver.MicrovmHost's three, and it is here for the same
// reason as the other two: the session's control connection belongs to its
// relay hub, and the hub lives in this package.
//
// Every way this can fail is an ERROR rather than a shrug, which is the one
// place it differs from quiesceExecs — and the difference is what is on the
// other side. A suspend that goes ahead unflushed freezes a container that is
// about to be thawed again; a SNAPSHOT that goes ahead unflushed publishes an
// environment image with a setup script's results half in it, which every
// later session of that environment then boots. So a sandbox that never
// registered, a sessiond that predates the kind, a conn that died, and a guest
// that is simply too slow all read the same way here: this guest cannot be
// flushed, so nothing is published for it.
func (s *Server) FlushGuest(ctx context.Context, sessionID string) error {
	hub, ok := s.reg.hub(sessionID)
	if !ok {
		return fmt.Errorf("session %s has no sandbox connection to flush; nothing may be published from its filesystem", sessionID)
	}
	nonce := s.flushNonce.Add(1)
	w := s.armFlushWaiter(sessionID, nonce)
	defer s.disarmFlushWaiter(sessionID, w)

	b, err := json.Marshal(relay.ControlEvent{Kind: relay.KindFlush, ID: nonce})
	if err != nil {
		return fmt.Errorf("encoding the flush request for %s: %w", sessionID, err)
	}
	if err := hub.SendControl(b); err != nil {
		return fmt.Errorf("asking session %s to flush: %w", sessionID, err)
	}

	timer := time.NewTimer(s.flushWait)
	defer timer.Stop()
	select {
	case <-w.done:
		return nil
	case <-hub.Done():
		return fmt.Errorf("session %s lost its sandbox connection before it had flushed", sessionID)
	case <-ctx.Done():
		return fmt.Errorf("waiting for session %s to flush: %w", sessionID, ctx.Err())
	case <-timer.C:
		return fmt.Errorf("session %s did not report a flush within %s "+
			"(a sandbox that predates the flush request never will)", sessionID, s.flushWait)
	}
}

// rpcErrorText reads the {"error": ...} sentence a failed response carries,
// falling back to a flat one so a refusal with no body is still a reason.
func rpcErrorText(payload json.RawMessage) string {
	var body struct {
		Error string `json:"error"`
	}
	if len(payload) > 0 && json.Unmarshal(payload, &body) == nil && body.Error != "" {
		return body.Error
	}
	return "the control plane refused a bootstrap mint without a reason"
}

// driverCapabilities are the capability tokens that are facts about which
// DRIVER this runner was started with, appended to the operator's own claims
// exactly as exec.v1 is and for the same reason: whether a session on this
// runner is a microVM is decided by a flag the operator already passed
// (--driver=microvm), not by a second flag they have to remember beside it.
//
// It is the fence, not merely a pre-check, which is the one way it differs
// from exec.v1: the control plane WITHHOLDS an environment's secret values
// from a runner that announces microvm.v1, so a runner that announced it
// falsely would be dispatched creates it cannot fulfil, and one that failed
// to announce it would be dispatched the values it must not accept — which
// its driver then refuses (driver.refuseUnwithheldEnv).
func (s *Server) driverCapabilities() []string {
	if cd, ok := s.drv.(driver.CapabilityDriver); ok {
		return cd.Capabilities()
	}
	return nil
}

// withholdsSecrets reports whether this runner's driver is one the control
// plane withholds an environment's secret values from.
//
// It is read off the announced capability rather than off the driver's
// concrete type, and that is the point: the two places this runner behaves
// differently for a microVM session — a create carries no dial URL, and a
// cold suspend tells the sandbox it is cold — are consequences of the same
// claim the control plane acts on. Keying them on a second fact would let
// the announcement and the behaviour disagree.
func (s *Server) withholdsSecrets() bool {
	return slices.Contains(s.driverCapabilities(), runner.CapabilityMicrovmV1)
}

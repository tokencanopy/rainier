// internal/controld/srpc.go
package controld

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"rainier/internal/rwire"
	"rainier/internal/xfer"
)

// The session RPC is controld's request/response channel to the inside of a
// sandbox, and this file is controld's end of it — both directions.
//
// Downward (sessionRPC): controld asks a session to do something (a diff, a
// push) and waits for the answer. The request rides a ToRunner "session_rpc";
// the answer comes back as a FromRunner "session_req" whose envelope Method is
// "resp". Correlation is the envelope's own ID, assigned here.
//
// Upward (routeSessionReq): a sandbox asks controld for something (a git
// credential) and waits for the answer. The request arrives as a "session_req"
// with a method; the answer goes back down as a "session_rpc" whose envelope
// Method is "resp", echoing the id the SANDBOX chose.
//
// Those two id spaces never collide, and nothing anywhere remaps an id,
// because a response always travels in the opposite direction to its request:
// a "resp" arriving here can only ever be answering a request this end sent,
// so it is matched against this end's table and no other. runnerd in between
// forwards envelopes verbatim, routing by session name alone.

// srpcTable is one runner connection's pending table for the session RPCs
// CONTROLD originated. It is deliberately separate from runnerConn.pending
// (the runner-dispatch table keyed by ReqID): the two id spaces mean different
// things — a ReqID correlates a command the RUNNER executes and answers, an
// envelope ID correlates a request the SANDBOX answers — and sharing a counter
// between them would make a runner's result and a sandbox's response
// indistinguishable to whichever one happened to be waiting.
type srpcTable struct {
	// seq is this connection's id source. Per-connection like ReqID's, so ids
	// never have to be unique across the fleet, only across one socket.
	seq atomic.Uint64

	mu      sync.Mutex
	pending map[uint64]chan rwire.RPCEnvelope
}

func newSRPCTable() *srpcTable {
	return &srpcTable{pending: map[uint64]chan rwire.RPCEnvelope{}}
}

// add registers a pending call and returns the channel its answer will arrive
// on. Buffered by one, so deliver never blocks the connection's reader even if
// the caller has already stopped waiting.
func (t *srpcTable) add(id uint64) chan rwire.RPCEnvelope {
	ch := make(chan rwire.RPCEnvelope, 1)
	t.mu.Lock()
	t.pending[id] = ch
	t.mu.Unlock()
	return ch
}

func (t *srpcTable) remove(id uint64) {
	t.mu.Lock()
	delete(t.pending, id)
	t.mu.Unlock()
}

// deliver hands a response to whoever is waiting on its id, reporting whether
// anyone was. A duplicate response for the same id is dropped rather than
// stalling the reader (the channel is buffered by one and the caller always
// removes its own entry).
func (t *srpcTable) deliver(env rwire.RPCEnvelope) bool {
	t.mu.Lock()
	ch, ok := t.pending[env.ID]
	t.mu.Unlock()
	if !ok {
		return false
	}
	select {
	case ch <- env:
	default:
	}
	return true
}

// len reports how many calls are still pending on this connection. Tests use
// it to prove every exit path — answer, timeout, conn death — takes its entry
// with it; production code does not read it.
func (t *srpcTable) len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.pending)
}

// sandboxError is a response the far end answered ok:false — a refusal from
// inside the sandbox (or from a method that ran and failed), not a transport
// failure. It is deliberately NOT wrapped in ErrRunnerUnreachable: the request
// arrived, was understood, and was declined, and a caller that retried it
// would get the same answer.
//
// Error() is the sandbox's own message verbatim whenever there is one, because
// that message is frequently the named action a user has to run ("run `rainier
// login --refresh github`") and every layer above here passes it through to
// them unchanged.
type sandboxError struct {
	Session string
	Method  string
	Msg     string
}

func (e *sandboxError) Error() string {
	if e.Msg == "" {
		return fmt.Sprintf("session %s refused %s without saying why", e.Session, e.Method)
	}
	return e.Msg
}

// sessionRPC asks the sandbox running sessionID to perform method and waits
// for its answer, decoding a successful one into out (pass nil to ignore the
// body).
//
// It resolves the session's runner from the store on every call rather than
// caching it: placement changes under reconciliation, and a request sent to
// yesterday's runner would be answered by nobody or, worse, by a stale
// duplicate container.
//
// Every no-answer outcome wraps ErrRunnerUnreachable, exactly as a runner
// dispatch does — from the caller's side "the sandbox did not answer" is one
// fact, whether the runner was disconnected, the connection died, or OpTimeout
// elapsed. Only the timeout additionally wraps ErrDispatchTimeout, which keeps
// the same meaning it has for dispatch: the request was delivered and may well
// have run, controld just did not hear back.
func (s *Server) sessionRPC(ctx context.Context, sessionID, method string, payload any, out any) error {
	row, err := s.st.GetSession(ctx, sessionID)
	if err != nil {
		return fmt.Errorf("session rpc %s for %s: %w", method, clip(sessionID), err)
	}
	if row.Runner == "" {
		return fmt.Errorf("session rpc %s for %s: the session is not placed on a runner: %w",
			method, row.ID, ErrRunnerUnreachable)
	}
	rc := s.conn(row.Runner)
	if rc == nil {
		return fmt.Errorf("session rpc %s for %s: runner %q is not connected: %w",
			method, row.ID, row.Runner, ErrRunnerUnreachable)
	}
	raw, err := rpcPayload(payload)
	if err != nil {
		return fmt.Errorf("session rpc %s for %s: encoding the payload: %w", method, row.ID, err)
	}

	id := rc.srpc.seq.Add(1)
	ch := rc.srpc.add(id)
	// The one cleanup path for all four exits below. A pending entry that
	// outlived its call would never be collected by anything else: nothing
	// sweeps this table, because nothing needs to — the call that made an
	// entry is always the one that removes it.
	defer rc.srpc.remove(id)

	if err := rc.enqueue(rwire.ToRunner{Type: "session_rpc", Session: row.ID,
		RPC: &rwire.RPCEnvelope{ID: id, Method: method, Payload: raw}}); err != nil {
		return fmt.Errorf("session rpc %s for %s: %w", method, row.ID, err)
	}

	timer := time.NewTimer(s.cfg.OpTimeout)
	defer timer.Stop()
	select {
	case env := <-ch:
		return decodeRPCAnswer(row.ID, method, env, out)
	case <-rc.done:
		// select is random among ready cases: prefer an answer that landed in
		// the same instant the connection died.
		if env, ok := drainRPC(ch); ok {
			return decodeRPCAnswer(row.ID, method, env, out)
		}
		return fmt.Errorf("session rpc %s for %s: runner %q disconnected before the answer: %w",
			method, row.ID, rc.name, ErrRunnerUnreachable)
	case <-timer.C:
		if env, ok := drainRPC(ch); ok {
			return decodeRPCAnswer(row.ID, method, env, out)
		}
		// A connection that died in the same instant the timer fired could
		// surface here as a timeout; conn death is the stronger fact, and
		// ErrDispatchTimeout's contract is "the connection was still live".
		select {
		case <-rc.done:
			return fmt.Errorf("session rpc %s for %s: runner %q disconnected before the answer: %w",
				method, row.ID, rc.name, ErrRunnerUnreachable)
		default:
		}
		return fmt.Errorf("session rpc %s for %s: no answer within %s: %w",
			method, row.ID, s.cfg.OpTimeout, ErrDispatchTimeout)
	case <-ctx.Done():
		if env, ok := drainRPC(ch); ok {
			return decodeRPCAnswer(row.ID, method, env, out)
		}
		// The caller went away; the runner is not implicated, so this is not
		// an unreachable-runner error (same rule dispatch follows).
		return fmt.Errorf("session rpc %s for %s: %w", method, row.ID, ctx.Err())
	}
}

func drainRPC(ch chan rwire.RPCEnvelope) (rwire.RPCEnvelope, bool) {
	select {
	case env := <-ch:
		return env, true
	default:
		return rwire.RPCEnvelope{}, false
	}
}

// decodeRPCAnswer turns one response envelope into this call's result: a
// refusal into a sandboxError carrying the far end's own words, a success into
// out.
func decodeRPCAnswer(sessionID, method string, env rwire.RPCEnvelope, out any) error {
	if !env.OK {
		return &sandboxError{Session: sessionID, Method: method, Msg: rpcErrorText(env.Payload)}
	}
	if out == nil {
		return nil
	}
	if len(env.Payload) == 0 {
		return fmt.Errorf("session rpc %s for %s: the sandbox answered ok with no payload", method, sessionID)
	}
	if err := json.Unmarshal(env.Payload, out); err != nil {
		return fmt.Errorf("session rpc %s for %s: decoding the answer: %w", method, sessionID, err)
	}
	return nil
}

// rpcErrorText reads the {"error": "..."} body a failed response carries. An
// unreadable one yields "", which sandboxError renders as its own sentence —
// better than echoing bytes from inside a container into a user's terminal.
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

// rpcPayload encodes a request or response body. A nil payload (and anything
// that encodes to JSON null) travels as no payload at all rather than the
// four bytes "null", which keeps a method with no arguments off the wire
// entirely — see rwire's session_req round-trip pin.
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

// ---------------------------------------------------------------------------
// controld-initiated: workspace inspection
//
// The three methods behind GET /v0/sessions/{id}/diff and the push/pull file
// transfer. Each is a thin wrapper on sessionRPC that does one thing the
// handler must not have to remember: BOUND WHAT THE SANDBOX SENT BACK.
//
// A sandbox is not a trusted peer. It runs a user's own agent, on a runner
// this replica reaches over a socket, and its answers become a client's
// response body — so every field of every answer that a compromised or simply
// broken sandbox could inflate is cut down to the same limit the honest one
// applies inside. The caps exist at both ends on purpose: the one inside keeps
// an honest sandbox from producing an enormous frame, and the one here keeps a
// dishonest one from spending this replica's memory (design §5, "size cap
// enforced client-side AND sessiond-side" — this is the third side).
// ---------------------------------------------------------------------------

// maxDiffRepos bounds how many repositories one diff answer may describe. A
// session's repository list is resolved at create and is small; the number is
// slack, not a working limit, and a sandbox that exceeds it is answering about
// a session it was not asked about.
const maxDiffRepos = 64

// sessionDiff asks a sandbox for its per-repository diff.
func (s *Server) sessionDiff(ctx context.Context, sessionID string) (xfer.DiffAnswer, error) {
	var ans xfer.DiffAnswer
	if err := s.sessionRPC(ctx, sessionID, xfer.MethodDiff, nil, &ans); err != nil {
		return xfer.DiffAnswer{}, err
	}
	return boundDiff(ans), nil
}

// maxDiffLabel bounds the three short fields — a repository slug and two
// branch names. They come from the session's own row by way of the sandbox,
// but "by way of the sandbox" is the part that matters: nothing on a rendered
// answer is trusted to be the length it should be.
const maxDiffLabel = 256

// boundDiff cuts an answer down to what this API is willing to relay.
func boundDiff(ans xfer.DiffAnswer) xfer.DiffAnswer {
	if len(ans.Repos) > maxDiffRepos {
		ans.Repos = ans.Repos[:maxDiffRepos]
	}
	for i := range ans.Repos {
		r := &ans.Repos[i]
		r.Repo = clipTo(r.Repo, maxDiffLabel)
		r.BaseBranch = clipTo(r.BaseBranch, maxDiffLabel)
		r.SessionBranch = clipTo(r.SessionBranch, maxDiffLabel)
		r.Stat = clipTo(r.Stat, xfer.StatBytes)
	}
	return ans
}

// clipTo truncates s to max bytes, keeping the result valid UTF-8 — the cut
// lands mid-rune as often as not, and every one of these strings is about to
// be JSON-encoded into somebody's terminal.
func clipTo(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return strings.ToValidUTF8(s[:max], "")
}

// sessionPushChunk hands one chunk of an upload to a sandbox and returns its
// ack.
func (s *Server) sessionPushChunk(ctx context.Context, sessionID string, chunk xfer.PushChunk) (xfer.PushAck, error) {
	var ack xfer.PushAck
	if err := s.sessionRPC(ctx, sessionID, xfer.MethodPushFiles, chunk, &ack); err != nil {
		return xfer.PushAck{}, err
	}
	// The ack's sequence number is the client's correlation, and a sandbox
	// that answered about a different chunk would have the client believing a
	// chunk landed that never did.
	if ack.Seq != chunk.Seq {
		return xfer.PushAck{}, fmt.Errorf("session %s acked chunk %d for a request about chunk %d",
			sessionID, ack.Seq, chunk.Seq)
	}
	return ack, nil
}

// sessionPullChunk asks a sandbox for one chunk of a download.
func (s *Server) sessionPullChunk(ctx context.Context, sessionID string, req xfer.PullRequest) (xfer.PullChunk, error) {
	var chunk xfer.PullChunk
	if err := s.sessionRPC(ctx, sessionID, xfer.MethodPullFiles, req, &chunk); err != nil {
		return xfer.PullChunk{}, err
	}
	if chunk.Seq != req.Seq {
		return xfer.PullChunk{}, fmt.Errorf("session %s answered chunk %d for a request about chunk %d",
			sessionID, chunk.Seq, req.Seq)
	}
	if len(chunk.Data) > xfer.ChunkBytes {
		return xfer.PullChunk{}, fmt.Errorf("session %s answered a %d-byte chunk; the limit is %d",
			sessionID, len(chunk.Data), xfer.ChunkBytes)
	}
	if len(chunk.Data) == 0 && !chunk.Done {
		// A chunk that carries nothing and does not end the transfer makes no
		// progress, and a sandbox answering those forever would spin the pull
		// loop without ever reaching the byte cap — the cap counts bytes, and
		// there are none. Only the LAST chunk may be empty.
		return xfer.PullChunk{}, fmt.Errorf("session %s answered chunk %d with no data and no end",
			sessionID, chunk.Seq)
	}
	return chunk, nil
}

// ---------------------------------------------------------------------------
// sandbox-initiated requests
// ---------------------------------------------------------------------------

// routeSessionReq handles one "session_req" from a runner: either the response
// to a request this replica sent down, or a fresh request from inside a
// sandbox that controld has to answer.
//
// It runs on the connection's reader, so the response half is delivered inline
// (a buffered channel hand-off) and the request half is not: answering reads
// the store, and this goroutine is the only one delivering every result and
// event that runner sends. Anything unroutable is logged and dropped — this
// message crossed a container boundary, and a malformed one must not be able
// to end the connection every session on that runner depends on.
func (s *Server) routeSessionReq(ctx context.Context, rc *runnerConn, m rwire.FromRunner) {
	if m.RPC == nil {
		log.Printf("controld: runner %s: session_req for %s carried no envelope", rc.name, clip(m.Session))
		return
	}
	env := *m.RPC
	if env.ID == 0 {
		log.Printf("controld: runner %s: session_req for %s carried no id; dropping",
			rc.name, clip(m.Session))
		return
	}
	if env.Method == "resp" {
		if !rc.srpc.deliver(env) {
			log.Printf("controld: runner %s: session-RPC response for unknown id %d (timed out?)", rc.name, env.ID)
		}
		return
	}
	if m.Session == "" {
		// Nothing to authorize the request against, and nowhere to send the
		// answer: a session_req names its session or it is not routable.
		log.Printf("controld: runner %s: session_req %q named no session; dropping", rc.name, clip(env.Method))
		return
	}
	go s.answerSessionRequest(ctx, rc, m.Session, env)
}

// answerSessionRequest authorizes one sandbox-initiated request and sends back
// whatever the method answered — always exactly one response, because the
// sandbox is holding a pending entry (and, for a credential mint, a git
// process) until one arrives.
//
// The placement check is the authorization: the runner token is fleet-wide, so
// a session_req proves only that SOME runner sent it, while every method
// behind this routing acts with the session owner's authority. A request for a
// session the store does not place on the asking runner is refused before any
// method runs — the same guard applyEvent applies to the events that end a
// session, and for the same reason: a stale or misbehaving runner must not be
// able to act on a session that is not its.
func (s *Server) answerSessionRequest(ctx context.Context, rc *runnerConn, sessionID string, env rwire.RPCEnvelope) {
	ans := s.authorizeSessionRequest(ctx, rc.name, sessionID, env)
	// The id and the method are this layer's to set, never the handler's: the
	// id is what the sandbox correlates against, and every answer is a "resp".
	ans.ID = env.ID
	ans.Method = "resp"
	if err := rc.enqueue(rwire.ToRunner{Type: "session_rpc", Session: sessionID, RPC: &ans}); err != nil {
		log.Printf("controld: answering %s for session %s on runner %s: %v",
			clip(env.Method), clip(sessionID), rc.name, err)
	}
}

func (s *Server) authorizeSessionRequest(ctx context.Context, runner, sessionID string, env rwire.RPCEnvelope) rwire.RPCEnvelope {
	row, err := s.st.GetSession(ctx, sessionID)
	switch {
	case errors.Is(err, ErrNotFound):
		log.Printf("controld: runner %s asked %q for unknown session %s; refusing",
			runner, clip(env.Method), clip(sessionID))
		return rpcRefusal(env.ID, "no such session")
	case err != nil:
		log.Printf("controld: runner %s: %s for %s: %v", runner, clip(env.Method), clip(sessionID), err)
		return rpcRefusal(env.ID, "the session could not be read")
	case row.Runner != runner:
		log.Printf("controld: runner %s asked %q for %s, which the store places on %q; refusing",
			runner, clip(env.Method), row.ID, row.Runner)
		return rpcRefusal(env.ID, "this session is not placed on the runner that asked")
	}
	return s.handleSessionRequest(ctx, runner, row, env)
}

// handleSessionRequest performs one authorized sandbox-initiated request. The
// caller has already established that the store places row on runner, so a
// method here may act with the session owner's authority.
//
// It takes the ROW the guard read rather than the id it was asked about: the
// authorization and the work then act on one and the same session — a second
// read could see a different row — and the owner every method needs is already
// on it. Unknown methods are refused by name, which is also what a newer
// sandbox talking to an older controld gets: a clear answer rather than a hang.
func (s *Server) handleSessionRequest(ctx context.Context, runner string, row Session, env rwire.RPCEnvelope) rwire.RPCEnvelope {
	switch env.Method {
	case mintGitCredentialMethod:
		return s.answerMintGitCredential(ctx, runner, row, env)
	default:
		log.Printf("controld: runner %s: session %s asked for unknown method %q",
			runner, row.ID, clip(env.Method))
		return rpcRefusal(env.ID, fmt.Sprintf("unknown method %q", clip(env.Method)))
	}
}

// mintGitCredentialMethod is the method name a sandbox's credential helper
// calls, spelled once here and once in cmd/sessiond/helper.go — the two ends
// of the same wire word (plan §Global Constraints).
const mintGitCredentialMethod = "mint_git_credential"

// mintAnswer is the body of a successful mint, and the ONE place in controld
// where a credential is rendered into anything. The shape is the contract the
// in-sandbox helper reads (cmd/sessiond/helper.go: `{"token": …}`), and the
// tag is why this is a named type rather than an inline literal — a renamed
// field here silently breaks git inside every session.
type mintAnswer struct {
	Token string `json:"token"`
}

// answerMintGitCredential answers one sandbox's request for a git credential:
// the session's owner names whose vault to open, and the vault decides.
//
// The refusals pass through VERBATIM, deliberately. Each vault sentinel is a
// named action ("run: rainier login --refresh github") that travels from here
// into the sandbox, out of the credential helper, through git's stderr and
// onto the user's terminal; a message rewritten at any hop would cost them the
// one sentence that says what to do. Everything else — a store that would not
// read, a fleet key that no longer opens the row — is an internal fault, and
// its text says nothing a sandbox could act on, so it is logged here and
// answered with a flat sentence instead.
//
// The token itself appears in exactly one place: the payload below. Not in the
// log line, not in an error, not in the refusal — see the vault's own note on
// secret hygiene (vault.go).
func (s *Server) answerMintGitCredential(ctx context.Context, runner string, row Session, env rwire.RPCEnvelope) rwire.RPCEnvelope {
	if row.OwnerID == "" {
		// Unreachable through the API (every create records its caller), and
		// refused rather than looked up anyway: the owner IS the authority
		// this mint acts with, and a lookup for the empty user is one stray
		// row away from handing a sandbox a credential nobody granted it.
		log.Printf("controld: runner %s: session %s has no owner; refusing to mint a credential", runner, row.ID)
		return rpcRefusal(env.ID, "this session has no owner to mint a github credential for")
	}

	token, err := s.mintGitCredential(ctx, row.OwnerID)
	switch {
	case errors.Is(err, ErrCredentialNeedsRefresh), errors.Is(err, ErrCredentialMissing):
		log.Printf("controld: session %s: no github credential to mint for user %s: %v", row.ID, row.OwnerID, err)
		return rpcRefusal(env.ID, err.Error())
	case err != nil:
		log.Printf("controld: session %s: minting a github credential for user %s: %v", row.ID, row.OwnerID, err)
		return rpcRefusal(env.ID, "the github credential could not be read")
	}

	body, err := json.Marshal(mintAnswer{Token: token})
	if err != nil {
		// Unreachable (a string always marshals) and logged WITHOUT the error,
		// which is the one error in this package whose text could quote the
		// value it failed on.
		log.Printf("controld: session %s: encoding the github credential answer failed", row.ID)
		return rpcRefusal(env.ID, "the github credential could not be encoded")
	}
	log.Printf("controld: session %s: minted a github credential for user %s on runner %s", row.ID, row.OwnerID, runner)
	return rwire.RPCEnvelope{ID: env.ID, Method: "resp", OK: true, Payload: body}
}

// rpcRefusal builds an ok:false response carrying msg where every consumer
// looks for it — the {"error": ...} body sessiond's Call and controld's own
// sandboxError both read.
func rpcRefusal(id uint64, msg string) rwire.RPCEnvelope {
	body, err := json.Marshal(struct {
		Error string `json:"error"`
	}{msg})
	if err != nil {
		// Unreachable (a string always marshals) but silence here would be a
		// response with no reason at all.
		log.Printf("controld: encoding an RPC refusal: %v", err)
		body = nil
	}
	return rwire.RPCEnvelope{ID: id, Method: "resp", Payload: body}
}

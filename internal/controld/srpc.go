// internal/controld/srpc.go
package controld

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/controlapp"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// The session RPC is controld's request/response channel to the inside of a
// sandbox, and this file is the host's half of it — the half whose requests
// the sandbox originates and controld answers.
//
// The transport is the runner plane's: it carries a request down as a
// ToRunner "session_rpc", correlates the answer by the envelope's own id, and
// routes an inbound "session_req" either into that pending table or, when it
// is a fresh request from a sandbox, to this file through
// runnerplane.Host.SessionRequest (runners.go). What stays here is what
// answering one means for THIS installation: the placement check that
// authorizes it, and the one method behind it.
//
// The placement check is the authorization: the runner token is fleet-wide,
// so a session_req proves only that SOME runner sent it, while every method
// behind this routing acts with the session owner's authority. A request for
// a session the store does not place on the asking runner is refused before
// any method runs — the same guard the events that end a session apply, and
// for the same reason: a stale or misbehaving runner must not be able to act
// on a session that is not its.

func (s *Server) authorizeSessionRequest(ctx context.Context, runnerName, sessionID string, env runner.RPCEnvelope) runner.RPCEnvelope {
	row, err := s.st.Sessions().GetSession(ctx, installWorkspace, control.SessionID(sessionID))
	switch {
	case errors.Is(err, control.ErrNotFound):
		log.Printf("controld: runner %s asked %q for unknown session %s; refusing",
			runnerName, clip(env.Method), clip(sessionID))
		return rpcRefusal(env.ID, "no such session")
	case err != nil:
		log.Printf("controld: runner %s: %s for %s: %v", runnerName, clip(env.Method), clip(sessionID), err)
		return rpcRefusal(env.ID, "the session could not be read")
	case string(row.RunnerID) != runnerName:
		log.Printf("controld: runner %s asked %q for %s, which the store places on %q; refusing",
			runnerName, clip(env.Method), row.ID, row.RunnerID)
		return rpcRefusal(env.ID, "this session is not placed on the runner that asked")
	}
	return s.handleSessionRequest(ctx, runnerName, row, env)
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
func (s *Server) handleSessionRequest(ctx context.Context, runnerName string, row control.Session, env runner.RPCEnvelope) runner.RPCEnvelope {
	switch env.Method {
	case mintGitCredentialMethod:
		return s.answerMintGitCredential(ctx, runnerName, row, env)
	case runner.MethodFetchAgentCredentials:
		return s.answerFetchAgentCredentials(ctx, runnerName, row, env)
	case runner.MethodPutAgentCredentials:
		return s.answerPutAgentCredentials(ctx, runnerName, row, env)
	default:
		log.Printf("controld: runner %s: session %s asked for unknown method %q",
			runnerName, row.ID, clip(env.Method))
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
func (s *Server) answerMintGitCredential(ctx context.Context, runnerName string, row control.Session, env runner.RPCEnvelope) runner.RPCEnvelope {
	if row.CreatorID == "" {
		// Unreachable through the API (every create records its caller), and
		// refused rather than looked up anyway: the owner IS the authority
		// this mint acts with, and a lookup for the empty user is one stray
		// row away from handing a sandbox a credential nobody granted it.
		log.Printf("controld: runner %s: session %s has no owner; refusing to mint a credential", runnerName, row.ID)
		return rpcRefusal(env.ID, "this session has no owner to mint a github credential for")
	}

	token, err := s.mintGitCredential(ctx, string(row.CreatorID))
	switch {
	case errors.Is(err, ErrCredentialNeedsRefresh), errors.Is(err, ErrCredentialMissing):
		log.Printf("controld: session %s: no github credential to mint for user %s: %v", row.ID, row.CreatorID, err)
		return rpcRefusal(env.ID, err.Error())
	case err != nil:
		log.Printf("controld: session %s: minting a github credential for user %s: %v", row.ID, row.CreatorID, err)
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
	log.Printf("controld: session %s: minted a github credential for user %s on runner %s", row.ID, row.CreatorID, runnerName)
	return runner.RPCEnvelope{ID: env.ID, Method: "resp", OK: true, Payload: body}
}

// rpcRefusal builds an ok:false response carrying msg where every consumer
// looks for it — the {"error": ...} body sessiond's Call and controld's own
// sandboxError both read.
func rpcRefusal(id uint64, msg string) runner.RPCEnvelope {
	body, err := json.Marshal(struct {
		Error string `json:"error"`
	}{msg})
	if err != nil {
		// Unreachable (a string always marshals) but silence here would be a
		// response with no reason at all.
		log.Printf("controld: encoding an RPC refusal: %v", err)
		body = nil
	}
	return runner.RPCEnvelope{ID: id, Method: "resp", Payload: body}
}

// ---------------------------------------------------------------------------
// agent credentials
//
// The other two sandbox-initiated methods: the boot-time fetch of a person's
// coding-agent login, and the put that keeps custody equal to what the agent
// wrote. Both are answered by controlapp.AgentCredentialService, which owns
// the authorization for BOTH hosts — a hosted cell answers the same two
// methods through the same service — so this file holds only what answering
// one means here: the wire shapes, the bounds on an untrusted payload, and
// the acting user the self-hosted authorizer reads.
//
// Neither function spells a provider. The name is a string that came off the
// wire, checked against controlapp's table by the service and carried
// verbatim by everything on this side of it.
// ---------------------------------------------------------------------------

// agentRequestMaxBytes bounds one request body before it is decoded. The
// service caps a credential SET at 64 KiB, which is about 87 KiB once
// base64-encoded into JSON; this is comfortably above that and far below
// anything worth allocating for a peer that has decided to misbehave. It is a
// pre-decode bound on purpose — a cap applied after unmarshalling has already
// let the sandbox choose how much memory to spend.
const agentRequestMaxBytes = 128 << 10

// agentCredentialRequest is the union of the two request bodies: a fetch
// sends the provider alone, a put sends the provider, the files, and the
// version it last saw. Decoding both through one type is what makes the two
// answers share their front half; the fields a fetch does not send are simply
// absent.
//
// Version is accepted and deliberately not acted on: v0 custody is
// last-writer-wins, so a stale version is stored exactly as a current one is
// (see controlapp.AgentCredentialService.AnswerPut). It is decoded rather
// than ignored so that a sandbox sending it is not answered with a decode
// error, and so the field's arrival here is visible to whoever adds the
// stricter rule later.
type agentCredentialRequest struct {
	Provider string            `json:"provider"`
	Files    map[string][]byte `json:"files"`
	Version  uint64            `json:"version"`
}

// agentFetchAnswer and agentPutAnswer are the two success bodies, named
// types for the same reason mintAnswer is one: sessiond reads these exact
// keys, so a renamed field here silently breaks the agent home in every
// session rather than failing a build.
type agentFetchAnswer struct {
	Version uint64            `json:"version"`
	Files   map[string][]byte `json:"files"`
}

type agentPutAnswer struct {
	Version uint64 `json:"version"`
}

// answerFetchAgentCredentials answers one sandbox's boot-time fetch:
// {"provider"} → {"version", "files": {name: base64}}.
//
// Version 0 with no files is a SUCCESS, not a refusal — it is the truthful
// answer for a person who has not logged that agent in, and the sandbox
// starts the agent anyway so they can.
//
// The credential appears in exactly one place: the payload below. Not in the
// log line, which names the session, the provider, and the version and
// nothing else.
func (s *Server) answerFetchAgentCredentials(ctx context.Context, runnerName string, row control.Session, env runner.RPCEnvelope) runner.RPCEnvelope {
	req, bad := decodeAgentRequest(env)
	if bad != nil {
		log.Printf("controld: runner %s: session %s sent an undecodable %s request",
			runnerName, row.ID, clip(env.Method))
		return *bad
	}
	set, err := s.agents.AnswerFetch(s.agentActorContext(ctx, row), row, req.Provider)
	if err != nil {
		return s.agentRefusal(env, row, runnerName, req.Provider, err, "the agent credential could not be read")
	}
	files := set.Files
	if files == nil {
		// The key is always present, even when it is empty: sessiond reads
		// "files" and a missing key would make "no credential" and "a
		// malformed answer" look alike on the far side.
		files = map[string][]byte{}
	}
	body, err := json.Marshal(agentFetchAnswer{Version: set.Version, Files: files})
	if err != nil {
		// Logged WITHOUT the error: json's own message quotes the value it
		// failed on, and that value is the credential.
		log.Printf("controld: session %s: encoding an agent credential answer failed", row.ID)
		return rpcRefusal(env.ID, "the agent credential could not be encoded")
	}
	log.Printf("controld: session %s: answered an agent credential fetch for %q at version %d on runner %s",
		row.ID, clip(req.Provider), set.Version, runnerName)
	return runner.RPCEnvelope{ID: env.ID, Method: "resp", OK: true, Payload: body}
}

// answerPutAgentCredentials records what the agent wrote:
// {"provider", "files", "version"} → {"version"}.
//
// The service applies the 64 KiB cap and the provider's file allowlist before
// anything is stored — the sandbox is an untrusted peer, and the bound it
// applies to itself is not a bound.
func (s *Server) answerPutAgentCredentials(ctx context.Context, runnerName string, row control.Session, env runner.RPCEnvelope) runner.RPCEnvelope {
	req, bad := decodeAgentRequest(env)
	if bad != nil {
		log.Printf("controld: runner %s: session %s sent an undecodable %s request",
			runnerName, row.ID, clip(env.Method))
		return *bad
	}
	version, err := s.agents.AnswerPut(s.agentActorContext(ctx, row), row, req.Provider, req.Files)
	if err != nil {
		return s.agentRefusal(env, row, runnerName, req.Provider, err, "the agent credential could not be stored")
	}
	body, err := json.Marshal(agentPutAnswer{Version: version})
	if err != nil {
		log.Printf("controld: session %s: encoding an agent credential version failed", row.ID)
		return rpcRefusal(env.ID, "the agent credential could not be encoded")
	}
	log.Printf("controld: session %s: stored an agent credential for %q at version %d from runner %s",
		row.ID, clip(req.Provider), version, runnerName)
	return runner.RPCEnvelope{ID: env.ID, Method: "resp", OK: true, Payload: body}
}

// decodeAgentRequest bounds and decodes one request body, returning the
// refusal to send when it cannot. The decode error is never relayed and never
// logged: a JSON error quotes the bytes it choked on, and on a put those
// bytes are a credential.
func decodeAgentRequest(env runner.RPCEnvelope) (agentCredentialRequest, *runner.RPCEnvelope) {
	var req agentCredentialRequest
	if len(env.Payload) > agentRequestMaxBytes {
		refusal := rpcRefusal(env.ID, "the agent credential request is too large")
		return req, &refusal
	}
	if err := json.Unmarshal(env.Payload, &req); err != nil {
		refusal := rpcRefusal(env.ID, "the agent credential request could not be decoded")
		return req, &refusal
	}
	return req, nil
}

// agentActorContext puts the session's creator into the context the
// authorization adapter reads (adapt_policy.go: actingUser). A sandbox
// request carries no authenticated user of its own — the runner token is
// fleet-wide, and the placement guard above has established only which runner
// asked — so the authority the answer acts with is the ROW's creator, exactly
// as the git mint's is.
//
// A creator the store cannot resolve leaves the context without a user, and
// the authorizer then denies: "this person is no longer an operator here" and
// "this person is no longer a member" are the same fact to the sandbox, and
// they get the same refusal.
func (s *Server) agentActorContext(ctx context.Context, row control.Session) context.Context {
	if row.CreatorID == "" {
		return ctx
	}
	u, err := s.st.GetUser(ctx, string(row.CreatorID))
	if err != nil {
		return ctx
	}
	return withUser(ctx, u)
}

// agentRefusal turns one service error into the answer the sandbox gets.
//
// ONLY controlapp's own fixed sentences are relayed: AgentRefusalSentence
// recognizes them, and anything else — a context error, a store failure that
// slipped through, a future error nobody thought about — becomes fallback.
// That is the difference between relaying an ACTION a person can take and
// relaying whatever text an error happens to hold, which on this path could
// be a row, a column, or a value.
func (s *Server) agentRefusal(env runner.RPCEnvelope, row control.Session, runnerName, provider string,
	err error, fallback string) runner.RPCEnvelope {
	sentence, ok := controlapp.AgentRefusalSentence(err)
	if !ok {
		// The error itself is logged here and nowhere else, because this is
		// the one arm whose text was not written to be shown.
		log.Printf("controld: session %s: agent credential %q on runner %s: %v",
			row.ID, clip(provider), runnerName, err)
		sentence = fallback
	} else {
		log.Printf("controld: session %s: refused an agent credential request for %q on runner %s: %s",
			row.ID, clip(provider), runnerName, sentence)
	}
	return rpcRefusal(env.ID, sentence)
}

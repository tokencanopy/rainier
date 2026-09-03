// internal/controld/srpc.go
package controld

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/tokencanopy/rainier/control"
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

func (s *Server) authorizeSessionRequest(ctx context.Context, runner, sessionID string, env runner.RPCEnvelope) runner.RPCEnvelope {
	row, err := s.st.Sessions().GetSession(ctx, installWorkspace, control.SessionID(sessionID))
	switch {
	case errors.Is(err, control.ErrNotFound):
		log.Printf("controld: runner %s asked %q for unknown session %s; refusing",
			runner, clip(env.Method), clip(sessionID))
		return rpcRefusal(env.ID, "no such session")
	case err != nil:
		log.Printf("controld: runner %s: %s for %s: %v", runner, clip(env.Method), clip(sessionID), err)
		return rpcRefusal(env.ID, "the session could not be read")
	case string(row.RunnerID) != runner:
		log.Printf("controld: runner %s asked %q for %s, which the store places on %q; refusing",
			runner, clip(env.Method), row.ID, row.RunnerID)
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
func (s *Server) handleSessionRequest(ctx context.Context, runner string, row control.Session, env runner.RPCEnvelope) runner.RPCEnvelope {
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

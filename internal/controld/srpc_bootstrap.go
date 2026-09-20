// internal/controld/srpc_bootstrap.go
package controld

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/controlapp"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// The two methods the microVM bootstrap token rides, answered here beside the
// three that were already on this channel and under the same discipline: the
// placement guard above has established which session asked, every answer is
// derived from the ROW that guard read, and the value appears in exactly one
// place — the payload.
//
// Neither method is gated on the runner having announced microvm.v1, and that
// is deliberate rather than an omission. A runner that did not announce it was
// dispatched the secret values in Spec.Env already, so nothing here is a
// privilege it does not have; adding a capability check would be a second,
// weaker fence in front of the one that actually holds (the placement guard,
// the token hash, the generation, and single use), and two fences that can
// disagree are worse than one that cannot.
//
// See docs/design/2026-09-20-microvm-bootstrap-token-and-vsock.md §3 and §5.

// sessionBootstrapRequest is the body of both requests: the fetch sends the
// protocol and the token, the mint sends the protocol alone. Decoding both
// through one type is the same economy the agent credential pair makes, and
// the field a mint does not send is simply absent.
type sessionBootstrapRequest struct {
	Protocol uint64 `json:"protocol"`
	Token    string `json:"token"`
}

// sessionSecretsAnswer and sessionBootstrapAnswer are the two success bodies,
// named types for the same reason mintAnswer is one: sessiond and runnerd read
// these exact keys, so a renamed field here would silently stop a microVM
// session from ever getting its environment rather than failing a build.
type sessionSecretsAnswer struct {
	Env map[string]string `json:"env"`
}

type sessionBootstrapAnswer struct {
	Token        string `json:"token"`
	ExpiresInSec int    `json:"expires_in_sec"`
}

// sessionBootstrapRequestMaxBytes bounds a request body before it is decoded.
// Both bodies are a version and at most one 43-character token; a kilobyte is
// slack rather than a working limit, and bounding before the decode is what
// keeps a misbehaving peer from choosing how much memory this process spends.
const sessionBootstrapRequestMaxBytes = 4 << 10

// answerFetchSessionSecrets answers one sandbox's boot-time exchange:
// {"protocol", "token"} → {"env": {name: value}}.
//
// This is the only path in this installation that hands an environment's
// decrypted secret_refs to anything, and every refusal below is a closed one.
// The secrets are re-resolved HERE, from the environment as it stands now,
// rather than remembered from the create: the control plane deliberately never
// held them between the two, which is the same rule that keeps them out of a
// session row.
//
// The value appears in exactly one place: the payload. Not in the log line,
// which names the session, the runner, and a COUNT; not in an error, which is
// why a decode failure is never relayed; and not in a refusal, which names a
// condition. §15.1 of the tenancy specification, pinned by a test that greps
// this package's own log output for its fixture secret.
func (s *Server) answerFetchSessionSecrets(ctx context.Context, runnerName string, row control.Session, env runner.RPCEnvelope) runner.RPCEnvelope {
	req, bad := decodeSessionBootstrapRequest(env)
	if bad != nil {
		log.Printf("controld: runner %s: session %s sent an unusable %s request",
			runnerName, row.ID, clip(env.Method))
		return *bad
	}
	if req.Token == "" {
		log.Printf("controld: runner %s: session %s asked for its secrets with no token", runnerName, row.ID)
		return rpcRefusal(env.ID, "this request carried no bootstrap token")
	}

	// The spend comes BEFORE the secrets are resolved, and single use means
	// single use even when what follows fails: a token that was presented has
	// been presented, and re-spendability on a failed resolve would turn one
	// refused exchange into an unbounded number of attempts.
	//
	// The generation is the ROW's, read by the guard that authorized this
	// request, never anything in the request.
	err := s.st.Bootstraps().ConsumeSessionBootstrap(ctx, installWorkspace, row.ID,
		controlapp.HashSessionBootstrapToken(req.Token), row.PlacementGeneration, s.clock.Now())
	if err != nil {
		sentence, known := controlapp.SessionBootstrapRefusal(err)
		if !known {
			log.Printf("controld: session %s: spending a bootstrap token on runner %s: %v",
				row.ID, runnerName, err)
			return rpcRefusal(env.ID, "the bootstrap token could not be checked")
		}
		log.Printf("controld: session %s: refused a secret fetch on runner %s: %s",
			row.ID, runnerName, sentence)
		return rpcRefusal(env.ID, sentence)
	}

	vars, err := s.sessionSecrets(ctx, row)
	if err != nil {
		// The resolver's own sentence may name a secret REFERENCE, which is a
		// name and not a value, and which a person cannot fix without. It is
		// logged and relayed for exactly the reason secretEnvironment says:
		// a dangling reference is unfixable without its name.
		log.Printf("controld: session %s: resolving the environment's secrets on runner %s: %v",
			row.ID, runnerName, err)
		return rpcRefusal(env.ID, err.Error())
	}
	if vars == nil {
		// The key is always present, even when it is empty: the guest reads
		// "env", and a missing key would make "this environment declares no
		// secrets" and "a malformed answer" look alike on the far side.
		vars = map[string]string{}
	}
	body, err := json.Marshal(sessionSecretsAnswer{Env: vars})
	if err != nil {
		// Logged WITHOUT the error: json's own message quotes the value it
		// failed on, and that value is the secret.
		log.Printf("controld: session %s: encoding the session secrets failed", row.ID)
		return rpcRefusal(env.ID, "the session secrets could not be encoded")
	}
	log.Printf("controld: session %s: delivered %d environment secret(s) to runner %s",
		row.ID, len(vars), runnerName)
	return runner.RPCEnvelope{ID: env.ID, Method: "resp", OK: true, Payload: body}
}

// answerMintSessionBootstrap answers a RUNNER's request for a fresh token on
// a cold resume: {"protocol"} → {"token", "expires_in_sec"}.
//
// It is the one method on this channel a sandbox does not originate, and it
// needs no new authorization for it: the request names no session, so the
// session it mints for is FromRunner.Session — the id the placement guard
// already checked — and a runner holding session A cannot mint for B because
// there is nowhere in the message to say B.
//
// The new token retires whatever this session had, which is what makes a
// resume's token the only one that works. The token appears in exactly one
// place: the payload.
func (s *Server) answerMintSessionBootstrap(ctx context.Context, runnerName string, row control.Session, env runner.RPCEnvelope) runner.RPCEnvelope {
	if _, bad := decodeSessionBootstrapRequest(env); bad != nil {
		log.Printf("controld: runner %s: session %s sent an unusable %s request",
			runnerName, row.ID, clip(env.Method))
		return *bad
	}
	minter := controlapp.SessionBootstrapMinter{Store: s.st.Bootstraps(), Clock: s.clock}
	token, err := minter.Mint(ctx, installWorkspace, row.ID, row.PlacementGeneration)
	if err != nil {
		log.Printf("controld: session %s: minting a bootstrap token for runner %s: %v",
			row.ID, runnerName, err)
		return rpcRefusal(env.ID, "a bootstrap token could not be minted for this session")
	}
	body, err := json.Marshal(sessionBootstrapAnswer{
		Token:        token,
		ExpiresInSec: int(controlapp.SessionBootstrapTTL.Seconds()),
	})
	if err != nil {
		// Unreachable (a string and an int always marshal) and logged WITHOUT
		// the error, which is the one error here whose text could quote the
		// value it failed on.
		log.Printf("controld: session %s: encoding a bootstrap token failed", row.ID)
		return rpcRefusal(env.ID, "the bootstrap token could not be encoded")
	}
	log.Printf("controld: session %s: minted a bootstrap token for runner %s at placement generation %d",
		row.ID, runnerName, row.PlacementGeneration)
	return runner.RPCEnvelope{ID: env.ID, Method: "resp", OK: true, Payload: body}
}

// sessionSecrets resolves the environment secrets row's create would have
// carried, through the same resolver the scheduler dispatches with.
//
// It is the resolver and not a second decryption path on purpose: launchMaterial
// is the only place in the self-hosted adapter set that holds the secrets key,
// and a second one would be a second thing to get wrong. A session with no
// environment, or one whose environment declares no secret_refs, resolves to
// nothing and gets an empty map — a truthful answer, not a refusal.
func (s *Server) sessionSecrets(ctx context.Context, row control.Session) (map[string]string, error) {
	env, err := s.sessionEnvironment(ctx, row)
	if err != nil {
		return nil, err
	}
	return launchMaterial{st: s.st, key: s.cfg.SecretsKey}.secretEnvironment(ctx, env)
}

// sessionEnvironment reads the environment row came from, or nil for a
// scratch session. An environment that has been deleted since the create is
// nil too: the session is still running, it simply has no declared secrets to
// be given, and refusing its boot over a row somebody removed would be the
// control plane taking a live session down.
func (s *Server) sessionEnvironment(ctx context.Context, row control.Session) (*control.Environment, error) {
	if row.EnvironmentID == "" {
		return nil, nil
	}
	env, err := s.st.Environments().GetEnvironment(ctx, installWorkspace, row.EnvironmentID)
	switch {
	case errors.Is(err, control.ErrNotFound):
		return nil, nil
	case err != nil:
		return nil, errors.New("this session's environment could not be read")
	}
	return &env, nil
}

// decodeSessionBootstrapRequest bounds and decodes one request body,
// returning the refusal to send when it cannot. The decode error is never
// relayed and never logged: a JSON error quotes the bytes it choked on, and
// on this path those bytes are a capability.
func decodeSessionBootstrapRequest(env runner.RPCEnvelope) (sessionBootstrapRequest, *runner.RPCEnvelope) {
	var req sessionBootstrapRequest
	if len(env.Payload) > sessionBootstrapRequestMaxBytes {
		refusal := rpcRefusal(env.ID, "the bootstrap request is too large")
		return req, &refusal
	}
	if err := json.Unmarshal(env.Payload, &req); err != nil {
		refusal := rpcRefusal(env.ID, "the bootstrap request could not be decoded")
		return req, &refusal
	}
	if req.Protocol != runner.SessionBootstrapProtocolVersion {
		refusal := rpcRefusal(env.ID, "this session must be replaced before it can exchange a bootstrap token")
		return req, &refusal
	}
	return req, nil
}

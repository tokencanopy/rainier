package controld

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/controlapp"
	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/runnerplane"
)

func reconnectMethod(method string) bool {
	switch method {
	case runner.MethodEnrollGuestReconnect, runner.MethodBeginGuestReconnect, runner.MethodAcceptGuestReconnect, runner.MethodGuestReconnectConfiguration, runner.MethodMintSessionBootstrap, runner.MethodFetchSessionSecrets:
		return true
	}
	return false
}

type authorityClock struct{ now time.Time }

func (c authorityClock) Now() time.Time { return c.now }

func reconnectRefusal(id uint64, err error) runner.RPCEnvelope {
	code := "unavailable"
	switch {
	case errors.Is(err, control.ErrReconnectInvalid):
		code = "invalid"
	case errors.Is(err, control.ErrReconnectExpired):
		code = "expired"
	case errors.Is(err, control.ErrReconnectFenced), errors.Is(err, control.ErrDenied):
		code = "fenced"
	}
	return rpcRefusal(id, code)
}

// answerGuestReconnect derives placement from storage and socket generation from
// the accepted connection. No peer payload may nominate either authority.
func (s *Server) answerGuestReconnect(ctx context.Context, host GuestReconnectAuthorityStore, b runnerplane.Binding, id control.SessionID, env runner.RPCEnvelope) runner.RPCEnvelope {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	hostOrigin := env.ID&(uint64(1)<<63) != 0
	if env.Method != runner.MethodEnrollGuestReconnect && env.Method != runner.MethodFetchSessionSecrets && !hostOrigin {
		return reconnectRefusal(env.ID, control.ErrReconnectInvalid)
	}
	row, err := s.st.Sessions().GetSession(ctx, b.WorkspaceID, id)
	if err != nil {
		return reconnectRefusal(env.ID, control.ErrReconnectFenced)
	}
	binding := control.GuestReconnectScope{WorkspaceID: b.WorkspaceID, PoolID: b.PoolID, RunnerID: b.RunnerID, SessionID: id, PlacementGeneration: row.PlacementGeneration, ConnectionGeneration: b.ConnectionGeneration}
	var answer any
	var secrets bool
	err = host.WithGuestReconnectAuthority(ctx, binding, func(ctx context.Context, a GuestReconnectAuthority) error {
		role, ok := s.roleFor(a.User.Login)
		if !ok {
			return control.ErrReconnectFenced
		}
		a.User.Role = role
		resource := control.Resource{Kind: control.ResourceSession, ID: string(a.Session.ID), WorkspaceID: a.Session.WorkspaceID, CreatorID: a.Session.CreatorID}
		if err := (ownerOrAdmin{}).Authorize(withUser(ctx, a.User), userScope(a.User), control.ActionResume, resource); err != nil {
			return control.ErrReconnectFenced
		}
		clock := authorityClock{a.Now}
		service := controlapp.GuestReconnect{Store: host.GuestReconnects(), Clock: clock}
		var action control.Action
		switch env.Method {
		case runner.MethodEnrollGuestReconnect:
			req, err := runner.DecodeGuestReconnectEnrollRequest(env.Payload)
			if err != nil {
				return control.ErrReconnectInvalid
			}
			if err := service.Enroll(ctx, binding, req.Token, req.BootEpoch, req.PublicKey); err != nil {
				return err
			}
			secrets = true
			action = control.ActionGuestEnroll
		case runner.MethodBeginGuestReconnect:
			if _, err := runner.DecodeGuestReconnectBeginRequest(env.Payload); err != nil {
				return control.ErrReconnectInvalid
			}
			challenge, err := service.Begin(ctx, binding)
			if err != nil {
				return err
			}
			answer = challenge
			action = control.ActionGuestReconnectBegin
		case runner.MethodAcceptGuestReconnect:
			req, err := runner.DecodeGuestReconnectAcceptRequest(env.Payload)
			if err != nil {
				return control.ErrReconnectInvalid
			}
			epoch, token, err := service.Accept(ctx, binding, req.AttemptID, req.Signature)
			if err != nil {
				return err
			}
			answer = runner.GuestReconnectAcceptResponse{Epoch: epoch, Token: token, ExpiresInSec: uint32(controlapp.SessionBootstrapTTL.Seconds())}
			action = control.ActionGuestReconnectAccept
		case runner.MethodGuestReconnectConfiguration:
			if _, err := runner.DecodeGuestReconnectBeginRequest(env.Payload); err != nil {
				return control.ErrReconnectInvalid
			}
			var environment *control.Environment
			if a.Session.EnvironmentID != "" {
				current, err := s.st.Environments().GetEnvironment(ctx, a.Session.WorkspaceID, a.Session.EnvironmentID)
				if err != nil {
					return control.ErrUnavailable
				}
				environment = &current
			}
			spec, err := s.fleet.ResolveGuestReconnectSpec(ctx, a.Session, environment)
			if err != nil {
				return control.ErrUnavailable
			}
			config := runner.GuestReconnectConfiguration{Protocol: runner.GuestReconnectProtocol, SessionID: string(a.Session.ID), PlacementGeneration: a.Session.PlacementGeneration, Spec: spec}
			encoded, err := json.Marshal(config)
			if err != nil {
				return control.ErrUnavailable
			}
			if _, err := runner.DecodeGuestReconnectConfiguration(encoded); err != nil {
				return control.ErrUnavailable
			}
			answer = config
			action = control.ActionGuestReconnectConfigure
		case runner.MethodMintSessionBootstrap:
			if _, err := runner.DecodeGuestReconnectBeginRequest(env.Payload); err != nil {
				return control.ErrReconnectInvalid
			}
			token, err := (controlapp.SessionBootstrapMinter{Store: s.st.Bootstraps(), Clock: clock}).Mint(ctx, a.Session.WorkspaceID, a.Session.ID, a.Session.PlacementGeneration)
			if err != nil {
				return err
			}
			answer = sessionBootstrapAnswer{Token: token, ExpiresInSec: int(controlapp.SessionBootstrapTTL.Seconds())}
			action = control.ActionGuestBootstrapMint
		case runner.MethodFetchSessionSecrets:
			req, err := runner.DecodeSessionBootstrapRedeemRequest(env.Payload)
			if err != nil {
				return control.ErrReconnectInvalid
			}
			if hostOrigin != (a.Epoch > 0) {
				return control.ErrReconnectFenced
			}
			if err := s.st.Bootstraps().ConsumeSessionBootstrap(ctx, a.Session.WorkspaceID, a.Session.ID, controlapp.HashSessionBootstrapToken(req.Token), a.Session.PlacementGeneration, a.Now); err != nil {
				return control.ErrReconnectInvalid
			}
			secrets = true
			action = control.ActionGuestBootstrapRedeem
		default:
			return control.ErrReconnectInvalid
		}
		row = a.Session
		return s.st.Record(ctx, control.Event{ID: (idGenerator{}).NewEventID(), WorkspaceID: a.Session.WorkspaceID, ActorID: a.Session.CreatorID, Action: action, Resource: resource, At: a.Now, PlacementGeneration: a.Session.PlacementGeneration})
	})
	if err != nil {
		return reconnectRefusal(env.ID, err)
	}
	// Enrollment and spends are committed before secret resolution. A resolver
	// failure cannot restore a capability, and its error never crosses the wire.
	if secrets {
		vars, err := s.sessionSecrets(ctx, row)
		if err != nil {
			return reconnectRefusal(env.ID, control.ErrUnavailable)
		}
		if vars == nil {
			vars = map[string]string{}
		}
		answer = runner.GuestReconnectEnrollResponse{Env: vars}
	}
	if ctx.Err() != nil {
		return reconnectRefusal(env.ID, control.ErrUnavailable)
	}
	payload, err := encodeReconnectAnswer(answer)
	if err != nil {
		return reconnectRefusal(env.ID, control.ErrUnavailable)
	}
	return runner.RPCEnvelope{ID: env.ID, Method: "resp", OK: true, Payload: payload}
}

// Bound even secret-bearing answers below the shared runner socket frame limit;
// one oversized environment must not disconnect every session on that runner.
func encodeReconnectAnswer(answer any) ([]byte, error) {
	payload, err := json.Marshal(answer)
	if err != nil || len(payload) > runner.GuestReconnectConfigurationLimit {
		return nil, control.ErrUnavailable
	}
	return payload, nil
}

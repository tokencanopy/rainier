package runnerd

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/tokencanopy/rainier/internal/driver"
	"github.com/tokencanopy/rainier/protocol/runner"
)

const reconnectBudget = 5 * time.Second
const maxGuestReconnects = 64

var (
	errReconnectInvalid     = errors.New("invalid")
	errReconnectExpired     = errors.New("expired")
	errReconnectFenced      = errors.New("fenced")
	errReconnectUnavailable = errors.New("unavailable")
)

// A snapshot of one agent connection. out belongs only to this connection;
// a late request can never land on the replacement's queue.
type reconnectControl struct {
	ctx   context.Context
	state *agentSessionState
	out   chan<- runner.FromRunner
}

var _ driver.GuestReconnectHost = (*Server)(nil)

// AuthorizeGuestReconnect implements driver.GuestReconnectHost. It does not
// deliver configuration, attach a guest, or advertise reconnect capability.
func (s *Server) AuthorizeGuestReconnect(ctx context.Context, id string, prove driver.GuestReconnectProof) (runner.GuestReconnectAcceptResponse, error) {
	var zero runner.GuestReconnectAcceptResponse
	if prove == nil {
		return zero, errReconnectInvalid
	}
	lease, err := s.acquireGuestReconnect(id)
	if err != nil {
		return zero, err
	}
	defer lease.close()
	return lease.authorize(ctx, prove)
}

func (lease *guestReconnectLease) authorize(ctx context.Context, prove driver.GuestReconnectProof) (runner.GuestReconnectAcceptResponse, error) {
	var zero runner.GuestReconnectAcceptResponse
	if prove == nil {
		return zero, errReconnectInvalid
	}
	s, id, row, rc, generation := lease.server, lease.row.id, lease.row, lease.control, lease.generation
	ctx, cancel := context.WithTimeout(ctx, reconnectBudget)
	defer cancel()
	stop := context.AfterFunc(rc.ctx, cancel)
	defer stop()
	valid := func() bool { return lease.valid(ctx) }
	if !valid() {
		return zero, errReconnectFenced
	}
	payload, err := s.reconnectCall(ctx, rc, id, runner.MethodBeginGuestReconnect, runner.GuestReconnectBeginRequest{Protocol: runner.GuestReconnectProtocol})
	if err != nil {
		return zero, err
	}
	if !valid() {
		return zero, errReconnectFenced
	}
	challenge, err := runner.DecodeGuestReconnectChallenge(payload)
	if err != nil || challenge.SessionID != id || challenge.PlacementGeneration != row.placementGen || challenge.HostIncarnation != strconv.FormatUint(generation, 10) {
		return zero, errReconnectInvalid
	}
	signature, err := prove(ctx, challenge)
	if err != nil {
		return zero, errReconnectUnavailable
	}
	if !valid() {
		return zero, errReconnectFenced
	}
	request := runner.GuestReconnectAcceptRequest{Protocol: runner.GuestReconnectProtocol, AttemptID: challenge.AttemptID, Signature: signature}
	body, _ := json.Marshal(request)
	if _, err := runner.DecodeGuestReconnectAcceptRequest(body); err != nil {
		return zero, errReconnectInvalid
	}
	payload, err = s.reconnectCall(ctx, rc, id, runner.MethodAcceptGuestReconnect, request)
	if err != nil {
		return zero, err
	}
	if !valid() {
		return zero, errReconnectFenced
	}
	accepted, err := runner.DecodeGuestReconnectAcceptResponse(payload)
	if err != nil {
		return zero, errReconnectInvalid
	}
	return accepted, nil
}

func (s *Server) claimReconnect(id string) bool {
	s.reconnectMu.Lock()
	defer s.reconnectMu.Unlock()
	if _, ok := s.reconnecting[id]; ok || len(s.reconnecting) >= maxGuestReconnects {
		return false
	}
	if s.reconnecting == nil {
		s.reconnecting = make(map[string]struct{})
	}
	s.reconnecting[id] = struct{}{}
	return true
}
func (s *Server) releaseReconnect(id string) {
	s.reconnectMu.Lock()
	defer s.reconnectMu.Unlock()
	delete(s.reconnecting, id)
}

func (s *Server) reconnectCall(ctx context.Context, rc *reconnectControl, session, method string, request any) ([]byte, error) {
	if ctx.Err() != nil || rc.ctx.Err() != nil {
		return nil, errReconnectUnavailable
	}
	body, err := json.Marshal(request)
	if err != nil {
		return nil, errReconnectInvalid
	}
	id, ch := s.runnerRPC.begin(session)
	defer s.runnerRPC.end(id)
	msg := runner.FromRunner{Type: "session_req", Session: session, RPC: &runner.RPCEnvelope{ID: id, Method: method, Payload: body}}
	msg.Used, msg.Total, _ = s.drv.Capacity(ctx)
	msg.Active, msg.IdleExited = s.reg.counts()
	if ctx.Err() != nil || rc.ctx.Err() != nil {
		return nil, errReconnectUnavailable
	}
	select {
	case rc.out <- msg:
	default:
		return nil, errReconnectUnavailable
	}
	select {
	case <-ctx.Done():
		return nil, errReconnectUnavailable
	case answer := <-ch:
		if ctx.Err() != nil || rc.ctx.Err() != nil {
			return nil, errReconnectUnavailable
		}
		if answer.OK {
			return answer.Payload, nil
		}
		refusal, err := runner.DecodeGuestReconnectErrorResponse(answer.Payload)
		if err != nil {
			return nil, errReconnectUnavailable
		}
		switch refusal.Error {
		case "invalid":
			return nil, errReconnectInvalid
		case "expired":
			return nil, errReconnectExpired
		case "fenced":
			return nil, errReconnectFenced
		default:
			return nil, errReconnectUnavailable
		}
	}
}

package controlapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// sessionRPC asks the sandbox running row to perform method and waits for its
// answer, decoding a successful one into out (pass nil to ignore the body).
// It is the one downward request/response path every workspace operation
// shares: a ToRunner "session_rpc" carrying a runner.RPCEnvelope, answered by
// a FromRunner "session_req" whose envelope Method is "resp" and whose ID
// echoes the request.
//
// The sandbox is an untrusted peer, so every answer is validated before it is
// trusted: wrong session, wrong RPC ID, wrong method, a false verdict, or a
// malformed body all become control.ErrUnavailable without relaying any
// sandbox payload text.
func (s *AttachmentService) sessionRPC(ctx context.Context, row control.Session, method string, payload any, out any) error {
	id := s.rpcSeq.Add(1)
	if id == 0 {
		return control.ErrUnavailable
	}
	raw, err := rpcPayload(payload)
	if err != nil {
		return control.ErrInvalid
	}
	msg := runner.ToRunner{Type: "session_rpc", Session: string(row.ID),
		RPC: &runner.RPCEnvelope{ID: id, Method: method, Payload: raw}}
	res, err := s.transport.Dispatch(ctx, row.PoolID, row.RunnerID, msg)
	if err != nil {
		return attachmentPortError(err)
	}
	if res.Type != "session_req" || res.Session != string(row.ID) || res.RPC == nil ||
		res.RPC.ID != id || res.RPC.Method != "resp" {
		return control.ErrUnavailable
	}
	if !res.RPC.OK {
		return control.ErrUnavailable
	}
	return decodeRPCAnswer(res.RPC.Payload, out)
}

// rpcPayload encodes a request body. A nil payload (and anything that encodes
// to JSON null) travels as no payload at all; a caller-supplied
// json.RawMessage is validated as JSON before it is forwarded.
func rpcPayload(v any) (json.RawMessage, error) {
	switch p := v.(type) {
	case nil:
		return nil, nil
	case json.RawMessage:
		if len(p) == 0 {
			return nil, nil
		}
		if !json.Valid(p) {
			return nil, control.ErrInvalid
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

// decodeRPCAnswer turns one successful response body into out, rejecting an
// absent body (unless out is nil), malformed JSON, and trailing JSON.
func decodeRPCAnswer(payload json.RawMessage, out any) error {
	if out == nil {
		return nil
	}
	if len(payload) == 0 {
		return control.ErrUnavailable
	}
	dec := json.NewDecoder(bytes.NewReader(payload))
	if err := dec.Decode(out); err != nil {
		return control.ErrUnavailable
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return control.ErrUnavailable
	}
	return nil
}

package controlapp

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/workspace"
)

// rpcProbe is a synthetic decode target. Its fields are the only thing tests
// may assert; never decode workspace content into it.
type rpcProbe struct {
	N int `json:"n"`
}

func rpcOKReply(id uint64, payload any) runner.FromRunner {
	var raw json.RawMessage
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			panic(err)
		}
		raw = b
	}
	return runner.FromRunner{
		Type:    "session_req",
		Session: "sess_example",
		RPC:     &runner.RPCEnvelope{ID: id, Method: "resp", OK: true, Payload: raw},
	}
}

func TestSessionRPCSuccess(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.transport.replyFn = func(m runner.ToRunner) runner.FromRunner {
		return rpcOKReply(m.RPC.ID, rpcProbe{N: 42})
	}
	var out rpcProbe
	if err := fx.svc.sessionRPC(context.Background(), runningSession(), workspace.MethodDiff, nil, &out); err != nil {
		t.Fatalf("sessionRPC: %v", err)
	}
	if out.N != 42 {
		t.Fatalf("decoded N = %d, want 42", out.N)
	}
	got := fx.transport.dispatched()
	if len(got) != 1 {
		t.Fatalf("dispatched %d messages, want 1", len(got))
	}
	m := got[0]
	if m.Type != "session_rpc" || m.Session != "sess_example" || m.ReqID != 0 ||
		m.RPC == nil || m.RPC.ID == 0 || m.RPC.Method != workspace.MethodDiff {
		t.Fatalf("request = %+v", m)
	}
}

func TestSessionRPCHostileResponses(t *testing.T) {
	tests := []struct {
		name  string
		reply runner.FromRunner
	}{
		{"false ok", runner.FromRunner{Type: "session_req", Session: "sess_example",
			RPC: &runner.RPCEnvelope{ID: 1, Method: "resp", OK: false, Payload: json.RawMessage(`{"error":"synthetic"}`)}}},
		{"wrong type", runner.FromRunner{Type: "result", Session: "sess_example",
			RPC: &runner.RPCEnvelope{ID: 1, Method: "resp", OK: true}}},
		{"wrong session", runner.FromRunner{Type: "session_req", Session: "other_session",
			RPC: &runner.RPCEnvelope{ID: 1, Method: "resp", OK: true}}},
		{"missing rpc", runner.FromRunner{Type: "session_req", Session: "sess_example"}},
		{"wrong envelope id", runner.FromRunner{Type: "session_req", Session: "sess_example",
			RPC: &runner.RPCEnvelope{ID: 99, Method: "resp", OK: true}}},
		{"non-resp method", runner.FromRunner{Type: "session_req", Session: "sess_example",
			RPC: &runner.RPCEnvelope{ID: 1, Method: workspace.MethodDiff, OK: true}}},
		{"malformed json", runner.FromRunner{Type: "session_req", Session: "sess_example",
			RPC: &runner.RPCEnvelope{ID: 1, Method: "resp", OK: true, Payload: json.RawMessage("{not json")}}},
		{"empty successful payload", runner.FromRunner{Type: "session_req", Session: "sess_example",
			RPC: &runner.RPCEnvelope{ID: 1, Method: "resp", OK: true}}},
		{"trailing json", runner.FromRunner{Type: "session_req", Session: "sess_example",
			RPC: &runner.RPCEnvelope{ID: 1, Method: "resp", OK: true, Payload: json.RawMessage(`{"n":1} {"n":2}`)}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fx := newAttachmentFixture(t)
			fx.transport.replies = []runner.FromRunner{tt.reply}
			var out rpcProbe
			err := fx.svc.sessionRPC(context.Background(), runningSession(), workspace.MethodDiff, nil, &out)
			if !errors.Is(err, control.ErrUnavailable) {
				t.Fatalf("got %v, want ErrUnavailable", err)
			}
		})
	}
}

func TestSessionRPCNilOutAcceptsAbsentPayload(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.transport.replies = []runner.FromRunner{runner.FromRunner{
		Type: "session_req", Session: "sess_example",
		RPC: &runner.RPCEnvelope{ID: 1, Method: "resp", OK: true},
	}}
	if err := fx.svc.sessionRPC(context.Background(), runningSession(), workspace.MethodDiff, nil, nil); err != nil {
		t.Fatalf("nil out with absent payload: %v", err)
	}
}

func TestSessionRPCDisconnectedRunner(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.transport.dispatchErr = control.ErrUnavailable
	err := fx.svc.sessionRPC(context.Background(), runningSession(), workspace.MethodDiff, nil, &rpcProbe{})
	if !errors.Is(err, control.ErrUnavailable) {
		t.Fatalf("got %v, want ErrUnavailable", err)
	}
}

func TestSessionRPCContextCancellation(t *testing.T) {
	fx := newAttachmentFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := fx.svc.sessionRPC(ctx, runningSession(), workspace.MethodDiff, nil, &rpcProbe{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}

func TestSessionRPCInvalidRawMessage(t *testing.T) {
	fx := newAttachmentFixture(t)
	err := fx.svc.sessionRPC(context.Background(), runningSession(), workspace.MethodDiff,
		json.RawMessage("{not json"), &rpcProbe{})
	if !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("got %v, want ErrInvalid", err)
	}
}

func TestSessionRPCConcurrentOutOfOrder(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.transport.replyFn = func(m runner.ToRunner) runner.FromRunner {
		return rpcOKReply(m.RPC.ID, rpcProbe{N: int(m.RPC.ID)})
	}
	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var out rpcProbe
			errs[i] = fx.svc.sessionRPC(context.Background(), runningSession(), workspace.MethodDiff, nil, &out)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	got := fx.transport.dispatched()
	if len(got) != n {
		t.Fatalf("dispatched %d messages, want %d", len(got), n)
	}
	seen := map[uint64]bool{}
	for _, m := range got {
		if m.RPC == nil || m.RPC.ID == 0 {
			t.Fatalf("request missing id: %+v", m)
		}
		if seen[m.RPC.ID] {
			t.Fatalf("duplicate request id %d", m.RPC.ID)
		}
		seen[m.RPC.ID] = true
	}
}

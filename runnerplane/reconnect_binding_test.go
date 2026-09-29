package runnerplane

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
)

func TestSessionRequestBindsOriginalConnectionGeneration(t *testing.T) {
	h := newFakeHost()
	var got Binding
	h.answer = func(b Binding, _ control.SessionID, _ runner.RPCEnvelope) runner.RPCEnvelope {
		got = b
		return runner.RPCEnvelope{OK: true}
	}
	p := New(h, Options{})
	rc := newRunnerConn(Binding{WorkspaceID: testWorkspace, PoolID: testPool, RunnerID: "runner.test"}, nil)
	rc.gen = 7
	// A newer store generation must not make this old connection authoritative.
	h.repo.runners["runner.test"] = &control.Runner{Generation: 8}
	p.answerSessionRequest(context.Background(), rc, "session.test", runner.RPCEnvelope{ID: 1, Method: "synthetic"})
	if got.ConnectionGeneration != 7 {
		t.Fatalf("got generation %d, want original connection generation 7", got.ConnectionGeneration)
	}
	if got.WorkspaceID != testWorkspace || got.PoolID != testPool || got.RunnerID != "runner.test" {
		t.Fatal("scope changed")
	}
}

// This drives the real WebSocket registration and session-request path. The
// payload's invented generation cannot replace the plane's connection binding.
func TestSessionRequestConnectionGenerationOverWebSocket(t *testing.T) {
	h := newFakeHost()
	got := make(chan Binding, 1)
	h.answer = func(b Binding, _ control.SessionID, _ runner.RPCEnvelope) runner.RPCEnvelope {
		got <- b
		return runner.RPCEnvelope{OK: true}
	}
	p, ts := newTestPlaneOver(t, h)
	f := startFakeRunner(t, ts, runnerScript{Name: "runner.test", Total: 1})
	waitConnected(t, p, "runner.test")
	drainAccept(t, f)
	f.write(t, runner.FromRunner{Type: "session_req", Session: "session.test", RPC: &runner.RPCEnvelope{ID: 44, Method: "synthetic", Payload: json.RawMessage(`{"connection_generation":999}`)}})
	response := nextSessionRPC(t, f)
	if !response.RPC.OK || response.RPC.ID != 44 {
		t.Fatalf("response: %+v", response)
	}
	b := <-got
	if b.ConnectionGeneration != 1 || b.RunnerID != "runner.test" || b.WorkspaceID != testWorkspace || b.PoolID != testPool {
		t.Fatalf("binding: %+v", b)
	}
}

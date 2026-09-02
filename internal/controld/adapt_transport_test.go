package controld

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// transportFixture is a Server with one registered connection and no socket:
// the test plays the runner by reading rc.out and answering through deliver.
func transportFixture(t *testing.T, name string) (*Server, *runnerConn) {
	t.Helper()
	s := &Server{
		cfg:         Config{OpTimeout: 200 * time.Millisecond},
		runners:     map[string]*runnerConn{},
		runnerLocks: map[string]*sync.Mutex{},
	}
	rc := newRunnerConn(name, nil)
	s.runners[name] = rc
	return s, rc
}

func TestTransportConnectedReflectsTheConnectionMap(t *testing.T) {
	s, _ := transportFixture(t, "runner-a")
	tr := runnerTransport{srv: s}
	if !tr.Connected(installPool, "runner-a") {
		t.Fatal("registered runner reported disconnected")
	}
	if tr.Connected(installPool, "runner-b") {
		t.Fatal("unknown runner reported connected")
	}
	if tr.Connected("pool_other", "runner-a") {
		t.Fatal("foreign pool reported connected")
	}
}

func TestTransportDispatchCorrelatesResultByReqID(t *testing.T) {
	s, rc := transportFixture(t, "runner-a")
	go func() {
		m := <-rc.out
		rc.deliver(runner.FromRunner{Type: "result", ReqID: m.ReqID, OK: true, Detail: "snap:example"})
	}()
	res, err := runnerTransport{srv: s}.Dispatch(context.Background(), installPool, "runner-a", runner.ToRunner{Type: "snapshot", Session: "sess_example"})
	if err != nil {
		t.Fatal(err)
	}
	if !res.OK || res.Detail != "snap:example" {
		t.Fatalf("result = %+v", res)
	}
	rc.mu.Lock()
	pending := len(rc.pending)
	rc.mu.Unlock()
	if pending != 0 {
		t.Fatalf("pending table not cleaned up: %d", pending)
	}
}

func TestTransportDispatchSessionRPCCorrelatesByEnvelopeID(t *testing.T) {
	s, rc := transportFixture(t, "runner-a")
	go func() {
		m := <-rc.out
		if m.Type != "session_rpc" || m.RPC == nil || m.ReqID != 0 {
			return // the test's assertions below will report the timeout
		}
		rc.srpc.deliver(runner.RPCEnvelope{ID: m.RPC.ID, Method: "resp", OK: true, Payload: json.RawMessage(`{"repos":[]}`)})
	}()
	req := runner.ToRunner{Type: "session_rpc", Session: "sess_example",
		RPC: &runner.RPCEnvelope{ID: 77, Method: "diff"}}
	res, err := runnerTransport{srv: s}.Dispatch(context.Background(), installPool, "runner-a", req)
	if err != nil {
		t.Fatal(err)
	}
	if res.Type != "session_req" || res.Session != "sess_example" || res.RPC == nil ||
		res.RPC.ID != 77 || res.RPC.Method != "resp" || !res.RPC.OK {
		t.Fatalf("answer = %+v", res)
	}
	if rc.srpc.len() != 0 {
		t.Fatalf("srpc table not cleaned up: %d", rc.srpc.len())
	}
	tr := runnerTransport{srv: s}
	if _, err := tr.Dispatch(context.Background(), installPool, "runner-a",
		runner.ToRunner{Type: "session_rpc", Session: "sess_example"}); !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("session_rpc without an envelope: got %v, want ErrInvalid", err)
	}
}

func TestTransportDispatchFailuresAreUnavailableWithoutRunnerText(t *testing.T) {
	s, rc := transportFixture(t, "runner-a")
	tr := runnerTransport{srv: s}
	cases := []struct {
		name string
		id   control.RunnerID
		pool control.PoolID
		prep func()
	}{
		{"unknown runner", "runner-b", installPool, func() {}},
		{"foreign pool", "runner-a", "pool_other", func() {}},
		{"no answer before OpTimeout", "runner-a", installPool, func() { go func() { <-rc.out }() }},
		{"connection closed", "runner-a", installPool, func() { go func() { <-rc.out; close(rc.done) }() }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.prep()
			_, err := tr.Dispatch(context.Background(), tc.pool, tc.id, runner.ToRunner{Type: "destroy", Session: "sess_example"})
			if !errors.Is(err, control.ErrUnavailable) {
				t.Fatalf("got %v, want ErrUnavailable", err)
			}
			if strings.Contains(err.Error(), "sess_example") {
				t.Fatalf("error carries the session id: %v", err)
			}
		})
	}
}

func TestTransportDispatchHonorsCallerCancellation(t *testing.T) {
	s, rc := transportFixture(t, "runner-a")
	s.cfg.OpTimeout = time.Minute
	go func() { <-rc.out }()
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	start := time.Now()
	_, err := runnerTransport{srv: s}.Dispatch(ctx, installPool, "runner-a", runner.ToRunner{Type: "suspend", Session: "sess_example"})
	if !errors.Is(err, control.ErrUnavailable) || time.Since(start) > time.Second {
		t.Fatalf("got %v after %s", err, time.Since(start))
	}
}

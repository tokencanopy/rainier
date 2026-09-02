package controld

import (
	"context"
	"fmt"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// runnerTransport is control.RunnerTransport over the Server's connection
// map: the one downward path every application service takes to a runner.
// It owns nothing — the connections are registered and retired by the runner
// plane (runners.go) — and it never reads a runner's free text into an error.
// Every failure a caller sees is control.ErrUnavailable wrapped with our own
// words: the command type and the runner's name, both of which are ours.
type runnerTransport struct{ srv *Server }

var _ control.RunnerTransport = runnerTransport{}

// Connected reports whether id currently holds a control connection here.
func (t runnerTransport) Connected(pool control.PoolID, id control.RunnerID) bool {
	return pool == installPool && t.srv.conn(string(id)) != nil
}

// Dispatch sends m to id and waits for its answer, bounded by OpTimeout, the
// connection's lifetime, and ctx — the same three ways today's dispatch waits.
// Two message kinds correlate their answers differently: an ordinary command
// is answered by a "result" carrying the ReqID assigned here, while a
// "session_rpc" is answered by a "session_req" whose envelope ID the caller
// minted; the latter comes back re-wrapped in the exact FromRunner shape
// controlapp's sessionRPC validates.
func (t runnerTransport) Dispatch(ctx context.Context, pool control.PoolID, id control.RunnerID, m runner.ToRunner) (runner.FromRunner, error) {
	if pool != installPool {
		return runner.FromRunner{}, unreachable(m.Type, id)
	}
	rc := t.srv.conn(string(id))
	if rc == nil {
		return runner.FromRunner{}, unreachable(m.Type, id)
	}
	if m.Type == "session_rpc" {
		return t.sessionRPC(ctx, rc, id, m)
	}

	m.ReqID = rc.seq.Add(1)
	ch := make(chan runner.FromRunner, 1)
	rc.mu.Lock()
	rc.pending[m.ReqID] = ch
	rc.mu.Unlock()
	defer func() {
		rc.mu.Lock()
		delete(rc.pending, m.ReqID)
		rc.mu.Unlock()
	}()
	if err := rc.enqueue(m); err != nil {
		return runner.FromRunner{}, unreachable(m.Type, id)
	}
	res, ok := awaitAnswer(ctx, rc, ch, t.srv.cfg.OpTimeout)
	if !ok {
		return runner.FromRunner{}, unreachable(m.Type, id)
	}
	return res, nil
}

// sessionRPC is the session_rpc half of Dispatch. The envelope ID is the
// caller's correlation key: it is parked in the connection's srpc table, and
// routeSessionReq delivers the runner's "resp" envelope to it.
func (t runnerTransport) sessionRPC(ctx context.Context, rc *runnerConn, id control.RunnerID, m runner.ToRunner) (runner.FromRunner, error) {
	if m.RPC == nil || m.RPC.ID == 0 || m.Session == "" {
		return runner.FromRunner{}, control.ErrInvalid
	}
	ch := rc.srpc.add(m.RPC.ID)
	defer rc.srpc.remove(m.RPC.ID)
	if err := rc.enqueue(m); err != nil {
		return runner.FromRunner{}, unreachable(m.Type, id)
	}
	env, ok := awaitAnswer(ctx, rc, ch, t.srv.cfg.OpTimeout)
	if !ok {
		return runner.FromRunner{}, unreachable(m.Type, id)
	}
	return runner.FromRunner{Type: "session_req", Session: m.Session, RPC: &env}, nil
}

// awaitAnswer waits on ch the way dispatch always has: an answer wins; a
// connection that closes or a timeout that fires still drains an answer that
// raced in; and a caller that goes away takes what arrived, else nothing.
func awaitAnswer[T any](ctx context.Context, rc *runnerConn, ch chan T, timeout time.Duration) (T, bool) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case v := <-ch:
		return v, true
	case <-rc.done:
		return drainAnswer(ch)
	case <-timer.C:
		return drainAnswer(ch)
	case <-ctx.Done():
		return drainAnswer(ch)
	}
}

func drainAnswer[T any](ch chan T) (T, bool) {
	select {
	case v := <-ch:
		return v, true
	default:
		var zero T
		return zero, false
	}
}

// unreachable is the one error Dispatch returns: the closed sentinel, wrapped
// with the command type and the runner name — ours, never the runner's.
func unreachable(typ string, id control.RunnerID) error {
	return fmt.Errorf("dispatch %s to runner %q: %w", typ, id, control.ErrUnavailable)
}

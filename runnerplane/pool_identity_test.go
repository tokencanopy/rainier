package runnerplane

import (
	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
	"testing"
)

func TestSameRunnerNameInDifferentPoolsDoesNotReplaceTransport(t *testing.T) {
	p := New(newFakeHost(), Options{Logf: func(string, ...any) {}})
	first := newRunnerConn(Binding{PoolID: "pool_alpha", RunnerID: "runner_same"}, nil)
	second := newRunnerConn(Binding{PoolID: "pool_beta", RunnerID: "runner_same"}, nil)
	p.registerRunner(first)
	p.registerRunner(second)
	for _, rc := range []*runnerConn{first, second} {
		select {
		case <-rc.done:
			t.Fatal("another pool closed this runner")
		default:
		}
		if !p.Transport().Connected(rc.binding.PoolID, rc.binding.RunnerID) {
			t.Fatal("pool lost its connection")
		}
		if err := p.Send(rc.binding.PoolID, rc.binding.RunnerID, runner.ToRunner{Type: "dial_attach", Session: string(rc.binding.PoolID)}); err != nil {
			t.Fatal(err)
		}
		if got := <-rc.out; got.Session != string(rc.binding.PoolID) {
			t.Fatal("cross-pool dispatch")
		}
	}
	replacement := newRunnerConn(Binding{PoolID: "pool_alpha", RunnerID: control.RunnerID("runner_same")}, nil)
	p.registerRunner(replacement)
	p.retireRunner(first)
	if !p.Transport().Connected("pool_alpha", "runner_same") || !p.Transport().Connected("pool_beta", "runner_same") {
		t.Fatal("retirement affected another authority")
	}
}

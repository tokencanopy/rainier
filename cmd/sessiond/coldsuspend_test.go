package main

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// The cold half of the suspend handshake, wired the way main wires it, so
// what is exercised is the frame's whole journey — OnControl's kind
// dispatch, the `cold` flag, the handler, and the two answers — rather than
// quiesceCold called directly.
// stream is the workspace streamer the handler runs before it answers ready; a
// nil one is a sandbox that does not stream, which is what these two tests
// want — the secrets and the execs are their subject, and the stream has a file
// of its own (workspacestream_test.go).
func coldWired(execs execKiller, boots *bootstrapper, stream workspaceStreamer) (*rpcDispatcher, *recordingSender) {
	d := newRPCDispatcher()
	sender := &recordingSender{}
	d.online(sender)
	d.RegisterEventHandler(relay.KindSuspending, func(ev relay.ControlEvent) {
		if ev.Cold {
			quiesceCold(execs, d, nil, boots, ev.ID, stream)
			return
		}
		quiesceExecs(execs, d, ev.ID)
	})
	return d, sender
}

func suspendingFrame(t *testing.T, cold bool, nonce uint64) []byte {
	t.Helper()
	b, err := json.Marshal(relay.ControlEvent{Kind: relay.KindSuspending, ID: nonce, Cold: cold})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestAColdSuspendForgetsTheDeliveredSecrets is §4.4 from inside the guest:
// a cold notice is not a freeze, so the sandbox ends its execs AND forgets
// every secret the bootstrap exchange delivered before it answers ready.
func TestAColdSuspendForgetsTheDeliveredSecrets(t *testing.T) {
	cleanEnv(t)
	boots := &bootstrapper{}
	if err := applyBootConfig(
		runner.BootConfig{Protocol: runner.SessionBootstrapProtocolVersion, SessionID: "sess_example"},
		map[string]string{"DEPLOY_KEY": "value_example"}); err != nil {
		t.Fatal(err)
	}
	boots.remember([]string{"DEPLOY_KEY"})

	execs := &recordingExecs{}
	d, sender := coldWired(execs, boots, nil)
	d.OnControl(suspendingFrame(t, true, 91))

	if _, set := os.LookupEnv("DEPLOY_KEY"); set {
		t.Error("a cold suspend left a delivered secret in this process's environment")
	}
	if calls, budget := execs.state(); calls != 1 || budget != execQuiesceBudget {
		t.Fatalf("the cold suspend killed the execs %d time(s) with budget %s", calls, budget)
	}
	got := sender.events()
	if len(got) != 2 || got[0].Kind != relay.KindSuspendAck || got[1].Kind != relay.KindSuspendReady {
		t.Fatalf("the sandbox answered %+v, want an ack and then a ready", got)
	}
	for _, ev := range got {
		if ev.ID != 91 {
			t.Fatalf("an answer carries nonce %d, want the notice's 91", ev.ID)
		}
	}
}

// TestAWarmSuspendKeepsTheDeliveredSecrets is the other side of the same
// flag, and the reason it exists: a freeze is not the end of this VM, so the
// session comes back with the same process, the same agent, and the same
// environment. Forgetting there would break a warm resume for nothing.
func TestAWarmSuspendKeepsTheDeliveredSecrets(t *testing.T) {
	cleanEnv(t)
	boots := &bootstrapper{}
	if err := applyBootConfig(
		runner.BootConfig{Protocol: runner.SessionBootstrapProtocolVersion, SessionID: "sess_example"},
		map[string]string{"DEPLOY_KEY": "value_example"}); err != nil {
		t.Fatal(err)
	}
	boots.remember([]string{"DEPLOY_KEY"})

	execs := &recordingExecs{}
	d, sender := coldWired(execs, boots, nil)
	d.OnControl(suspendingFrame(t, false, 92))

	if os.Getenv("DEPLOY_KEY") != "value_example" {
		t.Error("a warm suspend forgot a delivered secret the resumed session still needs")
	}
	if got := sender.events(); len(got) != 2 {
		t.Fatalf("the sandbox answered %+v, want an ack and then a ready", got)
	}
}

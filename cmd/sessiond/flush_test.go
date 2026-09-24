package main

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// recordingAgents stands in for the agent sync: it records that the flush
// asked for the agent's pending write, which is the step a session's own log
// would otherwise be the only evidence of.
type recordingAgents struct{ flushes int }

func (a *recordingAgents) flush() { a.flushes++ }

// flushWired is coldWired's twin for the flush request: the handler wired the
// way main wires it, so what is exercised is the frame's whole journey rather
// than flushDisks called directly.
func flushWired(execs execKiller, boots *bootstrapper, agents agentFlusher) (*rpcDispatcher, *recordingSender) {
	d := newRPCDispatcher()
	sender := &recordingSender{}
	d.online(sender)
	d.RegisterEventHandler(relay.KindFlush, func(ev relay.ControlEvent) {
		flushDisks(d, agents, ev.ID)
	})
	d.RegisterEventHandler(relay.KindSuspending, func(ev relay.ControlEvent) {
		if ev.Cold {
			quiesceCold(execs, d, nil, boots, ev.ID, nil)
			return
		}
		quiesceExecs(execs, d, ev.ID)
	})
	return d, sender
}

func flushFrame(t *testing.T, nonce uint64) []byte {
	t.Helper()
	b, err := json.Marshal(relay.ControlEvent{Kind: relay.KindFlush, ID: nonce})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestAFlushAnswersWithTheNonceAndChangesNothingElse.
//
// The host asks for a flush when it is about to copy this session's root
// filesystem as an environment image, and the session KEEPS RUNNING: the user
// may be attached to it right now. So the answer is the whole of what this
// does — no exec is killed, no secret is forgotten, nothing is unmounted. A
// flush that ended somebody's commands would be a snapshot that stopped their
// work.
func TestAFlushAnswersWithTheNonceAndChangesNothingElse(t *testing.T) {
	cleanEnv(t)
	boots := &bootstrapper{}
	if err := applyBootConfig(
		runner.BootConfig{Protocol: runner.SessionBootstrapProtocolVersion, SessionID: "sess_example"},
		map[string]string{"DEPLOY_KEY": "value_example"}); err != nil {
		t.Fatal(err)
	}
	boots.remember([]string{"DEPLOY_KEY"})

	// Both real steps are watched, because a sessiond that answered "flushed"
	// without doing either would pass a test that only reads the answer — and
	// the host publishes an environment image on the strength of that answer.
	agents := &recordingAgents{}
	synced := 0
	restore := diskSync
	diskSync = func() { synced++ }
	t.Cleanup(func() { diskSync = restore })

	execs := &recordingExecs{}
	d, sender := flushWired(execs, boots, agents)
	d.OnControl(flushFrame(t, 77))

	if agents.flushes != 1 {
		t.Errorf("the agent's pending write was flushed %d time(s), want 1", agents.flushes)
	}
	if synced != 1 {
		t.Errorf("sync(2) was called %d time(s), want 1: the host copies the file as IT sees it, so an unsynced page is a byte that never reaches the image", synced)
	}

	got := sender.events()
	if len(got) != 1 || got[0].Kind != relay.KindFlushed {
		t.Fatalf("the sandbox answered %+v, want one %q", got, relay.KindFlushed)
	}
	if got[0].ID != 77 {
		t.Fatalf("the answer carries nonce %d, want the request's 77", got[0].ID)
	}
	if calls, _ := execs.state(); calls != 0 {
		t.Errorf("a flush killed this session's execs %d time(s); the session is staying", calls)
	}
	if os.Getenv("DEPLOY_KEY") != "value_example" {
		t.Error("a flush forgot a delivered secret; this session is still running and still needs it")
	}
}

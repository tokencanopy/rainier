package main

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/internal/sandboxexec"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// ---------------------------------------------------------------------------
// what runnerd is told about this session's commands
//
// The idle sweep stops a session whose agent child has exited and that has had
// no attachment for the timeout. A detached exec holds neither, so without
// these reports `rainier exec s --detach -- claude --continue` is cold-stopped
// half an hour after the agent finishes — see docs/design/exec-idle-stop.md.
// ---------------------------------------------------------------------------

// drainEvents takes every control payload waiting on the queue, decoded.
func drainEvents(t *testing.T, events <-chan []byte, want int) []relay.ControlEvent {
	t.Helper()
	var out []relay.ControlEvent
	deadline := time.After(5 * time.Second)
	for len(out) < want {
		select {
		case p := <-events:
			var ev relay.ControlEvent
			if err := json.Unmarshal(p, &ev); err != nil {
				t.Fatalf("undecodable control payload %s: %v", p, err)
			}
			out = append(out, ev)
		case <-deadline:
			t.Fatalf("only %d control event(s) arrived, want %d: %+v", len(out), want, out)
		}
	}
	return out
}

// TestALiveExecIsReportedUpstream wires the exec runner exactly as main does
// and runs a real detached command in a temporary workspace: the queue gets
// "one live" when it starts and "none live" when the session ends it, which is
// the pair the runner's idle clock needs.
func TestALiveExecIsReportedUpstream(t *testing.T) {
	root := t.TempDir()
	events := make(chan []byte, pendingCap)
	execs := sandboxexec.NewRunner(root, []string{"PATH=" + os.Getenv("PATH")},
		sandboxexec.NewSpawner().Start)
	execs.ObserveLive(func(live int, seq uint64) {
		offerControl(events, execCountPayload(live, seq))
	})

	execs.OpenExec(runner.ExecSpec{
		Argv: []string{"/bin/sh", "-c", "sleep 30"}, Detach: true, LogPath: "run.log"})
	got := drainEvents(t, events, 1)
	if got[0].Kind != relay.KindExecCount || got[0].Live != 1 || got[0].Seq == 0 {
		t.Fatalf("a started detached exec queued %+v, want one exec_count live=1 with a sequence", got[0])
	}

	// The session ends, which is the bound on a detached run's life — and the
	// report that starts the runner's idle clock.
	execs.KillAllAndWait(5 * time.Second)
	end := drainEvents(t, events, 1)
	if end[0].Kind != relay.KindExecCount || end[0].Live != 0 {
		t.Fatalf("the last exec ending queued %+v, want exec_count live=0", end[0])
	}
	if end[0].Seq <= got[0].Seq {
		t.Fatalf("the ending report carried seq %d, which is not ahead of %d", end[0].Seq, got[0].Seq)
	}
}

// TestEveryConnectionRestatesTheExecCount is what repairs a report that was
// dropped while there was no connection to carry it. The queue drops on
// overflow and drops its oldest at the cap, both deliberately, so an absolute
// count is only self-correcting if something restates it — and a dial is the
// one moment at which the two ends can have drifted with no further command
// coming to fix it.
func TestEveryConnectionRestatesTheExecCount(t *testing.T) {
	root := t.TempDir()
	execs := sandboxexec.NewRunner(root, []string{"PATH=" + os.Getenv("PATH")},
		sandboxexec.NewSpawner().Start)
	execs.OpenExec(runner.ExecSpec{
		Argv: []string{"/bin/sh", "-c", "sleep 30"}, Detach: true, LogPath: "run.log"})
	deadline := time.Now().Add(5 * time.Second)
	for execs.LiveCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	t.Cleanup(func() { execs.KillAllAndWait(5 * time.Second) })

	sender := &recordingSender{}
	reportExecs(sender, execs)
	evs := sender.events()
	if len(evs) != 1 || evs[0].Kind != relay.KindExecCount || evs[0].Live != 1 {
		t.Fatalf("a new connection was told %+v, want one exec_count live=1", evs)
	}

	// And the NEXT connection restates it under a higher sequence, so the
	// restatement can never be refused as a stale report.
	reportExecs(sender, execs)
	evs = sender.events()
	if len(evs) != 2 || evs[1].Seq <= evs[0].Seq {
		t.Fatalf("the second connection's restatement did not advance the sequence: %+v", evs)
	}
}

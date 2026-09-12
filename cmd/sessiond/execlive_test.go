package main

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
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

// nextReport takes the next report out of the mailbox, decoded, under a bound.
func nextReport(t *testing.T, m *execCountMailbox) relay.ControlEvent {
	t.Helper()
	select {
	case p := <-m.c():
		var ev relay.ControlEvent
		if err := json.Unmarshal(p, &ev); err != nil {
			t.Fatalf("undecodable control payload %s: %v", p, err)
		}
		return ev
	case <-time.After(5 * time.Second):
		t.Fatal("no live-exec report arrived")
		return relay.ControlEvent{}
	}
}

// TestALiveExecIsReportedUpstream wires the exec runner exactly as main does
// and runs a real detached command in a temporary workspace: the queue gets
// "one live" when it starts and "none live" when the session ends it, which is
// the pair the runner's idle clock needs.
func TestALiveExecIsReportedUpstream(t *testing.T) {
	root := t.TempDir()
	execs := sandboxexec.NewRunner(root, []string{"PATH=" + os.Getenv("PATH")},
		sandboxexec.NewSpawner().Start)
	// watchExecs, not a hand-rolled copy of it: this is the wiring main
	// installs, so deleting it there is a compile error here rather than a
	// green suite over a feature that reports nothing.
	mailbox := watchExecs(execs)

	execs.OpenExec(runner.ExecSpec{
		Argv: []string{"/bin/sh", "-c", "sleep 30"}, Detach: true, LogPath: "run.log"})
	got := nextReport(t, mailbox)
	if got.Kind != relay.KindExecCount || got.Live != 1 || got.Seq == 0 {
		t.Fatalf("a started detached exec reported %+v, want one exec_count live=1 with a sequence", got)
	}

	// The session ends, which is the bound on a detached run's life — and the
	// report that starts the runner's idle clock.
	execs.KillAllAndWait(5 * time.Second)
	end := nextReport(t, mailbox)
	if end.Kind != relay.KindExecCount || end.Live != 0 {
		t.Fatalf("the last exec ending reported %+v, want exec_count live=0", end)
	}
	if end.Seq <= got.Seq {
		t.Fatalf("the ending report carried seq %d, which is not ahead of %d", end.Seq, got.Seq)
	}
}

// TestTheMailboxKeepsTheNEWESTReport is the difference between a count that is
// self-correcting and one that only looks it. `events` drops the ARRIVING
// payload when it is full — right for an event that has no second chance,
// wrong for an absolute count, where the arriving one is the only true one.
// A dropped "one live" with no further transition coming is the cold stop this
// whole change exists to prevent.
func TestTheMailboxKeepsTheNEWESTReport(t *testing.T) {
	m := newExecCountMailbox()
	for seq := uint64(1); seq <= 5; seq++ {
		m.offer(execCountPayload(int(seq), seq), seq)
	}
	var ev relay.ControlEvent
	select {
	case p := <-m.c():
		if err := json.Unmarshal(p, &ev); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatal("the mailbox is empty after five reports")
	}
	if ev.Seq != 5 || ev.Live != 5 {
		t.Fatalf("the mailbox held live=%d seq=%d, want the newest (5, 5)", ev.Live, ev.Seq)
	}
	select {
	case p := <-m.c():
		t.Fatalf("the mailbox held a second report: %s", p)
	default:
	}
	// And a nil payload — what execCountPayload returns when encoding failed —
	// does not clear it.
	m.offer(execCountPayload(9, 9), 9)
	m.offer(nil, 0)
	select {
	case <-m.c():
	default:
		t.Fatal("a nil offer emptied the mailbox")
	}
}

// TestConcurrentOffersLeaveTheNewestReport: several exec slots can come back
// at once, so the discard and the fill have to be one step against each other.
// Without the lock two offers can both find the slot empty and the older one
// can be the survivor.
func TestConcurrentOffersLeaveTheNewestReport(t *testing.T) {
	for attempt := 0; attempt < 50; attempt++ {
		m := newExecCountMailbox()
		var wg sync.WaitGroup
		const n = 8
		for i := 1; i <= n; i++ {
			wg.Add(1)
			go func(seq uint64) {
				defer wg.Done()
				m.offer(execCountPayload(int(seq), seq), seq)
			}(uint64(i))
		}
		wg.Wait()
		select {
		case p := <-m.c():
			var ev relay.ControlEvent
			if err := json.Unmarshal(p, &ev); err != nil {
				t.Fatal(err)
			}
			if ev.Seq != n {
				t.Fatalf("the mailbox held sequence %d, want the newest, %d: a later arrival evicted a newer report", ev.Seq, n)
			}
		default:
			t.Fatal("every concurrent offer was lost")
		}
	}
}

// TestServeConnRestatesTheExecCountAndNeverQueuesOne pins the two rules
// together, on the function the dial loop actually calls. The restatement is
// what repairs a report lost while there was no connection; not queueing is
// what keeps a count from competing with the child's exit for the pending cap,
// since that exit has no second chance at all.
func TestServeConnRestatesTheExecCountAndNeverQueuesOne(t *testing.T) {
	sender := &recordingSender{}
	errc := make(chan error, 1)
	execCounts := make(chan []byte, 1)
	reporter := &stubReporter{live: 2, seq: 41}

	// A report that the previous connection never delivered, still waiting.
	execCounts <- execCountPayload(1, 40)

	done := make(chan [][]byte, 1)
	go func() {
		p, _ := serveConn(sender, errc, nil, execCounts, reporter, nil)
		done <- p
	}()

	deadline := time.After(5 * time.Second)
	for {
		if len(sender.events()) >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("serveConn sent %+v, want the restatement and the queued report",
				sender.events())
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
	evs := sender.events()
	if evs[0].Kind != relay.KindExecCount || evs[0].Live != 2 || evs[0].Seq != 41 {
		t.Fatalf("the connection's FIRST message was %+v, want the restatement", evs[0])
	}
	if evs[1].Kind != relay.KindExecCount || evs[1].Seq != 40 {
		t.Fatalf("the waiting report was %+v", evs[1])
	}

	errc <- io.EOF
	select {
	case pending := <-done:
		if len(pending) != 0 {
			t.Fatalf("serveConn queued %d exec count(s) for the next connection; a count "+
				"is only ever about the connection in hand, and it would compete with the "+
				"child's exit for the cap", len(pending))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serveConn did not return")
	}
}

// TestAnUndeliverableExecCountNeverDisplacesTheChildsExit is the same rule
// stated where it actually bites, and the reason the test above is not enough
// on its own: with a WORKING sender the drain at the top of serveConn's loop
// empties the queue before anything can be observed in it, so queueing an exec
// count would look identical to sending one. It is only when sends are failing
// — which is exactly when the queue matters — that the difference shows.
//
// The queue drops its OLDEST at pendingCap. The child's exit has no second
// chance at all: if it is dropped, runnerd never learns the child exited,
// childExitedAt stays zero, and the session holds its slot for the life of the
// runner with nothing in the log to say why. A live-exec count, which every
// connection restates by design, must never be the thing that pushes it out.
func TestAnUndeliverableExecCountNeverDisplacesTheChildsExit(t *testing.T) {
	dead := &stubSender{err: errors.New("write: broken pipe")}
	errc := make(chan error, 1)
	events := make(chan []byte, pendingCap)
	execCounts := make(chan []byte, 1)

	// The one event that cannot be repeated, queued first.
	events <- childExitedPayload(0)

	done := make(chan [][]byte, 1)
	go func() {
		p, _ := serveConn(dead, errc, events, execCounts, &stubReporter{live: 1, seq: 1}, nil)
		done <- p
	}()

	// Then more exec counts than the queue can hold, one at a time so each is
	// taken before the next is offered.
	for seq := uint64(2); seq <= uint64(pendingCap)+4; seq++ {
		execCounts <- execCountPayload(1, seq)
		deadline := time.Now().Add(5 * time.Second)
		for len(execCounts) > 0 {
			if time.Now().After(deadline) {
				t.Fatal("serveConn stopped taking exec counts")
			}
			time.Sleep(time.Millisecond)
		}
	}

	errc <- io.EOF
	select {
	case pending := <-done:
		if len(pending) != 1 {
			t.Fatalf("the queue holds %d payload(s), want only the child's exit", len(pending))
		}
		var ev relay.ControlEvent
		if err := json.Unmarshal(pending[0], &ev); err != nil {
			t.Fatal(err)
		}
		if ev.Kind != "child_exited" {
			t.Fatalf("the queue holds %q; the child's exit was pushed out by exec counts, "+
				"and runnerd will never learn the child exited", ev.Kind)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serveConn did not return")
	}
}

// stubReporter is an exec runner that only has a count.
type stubReporter struct {
	live int
	seq  uint64
}

func (s *stubReporter) LiveReport() (int, uint64) { return s.live, s.seq }

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

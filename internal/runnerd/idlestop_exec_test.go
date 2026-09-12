package runnerd

import (
	"context"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/internal/driver"
	"github.com/tokencanopy/rainier/internal/relay"
)

// ---------------------------------------------------------------------------
// idle auto-stop × a live exec
//
// The rule table next door covers the decision; this file covers the two
// fences the report arrives behind, what the capacity counts say about a
// session running one, and the reproduction itself through the real control
// path. See docs/design/exec-idle-stop.md.
// ---------------------------------------------------------------------------

// liveExecsOn reads the count the registry currently holds for id.
func liveExecsOn(rd *Server, id string) int {
	e, ok := rd.reg.snapshot(id)
	if !ok {
		return -1
	}
	return e.liveExecs
}

// waitForLiveExecs polls until the registry holds want for id. routeControl
// runs on a goroutine of its own (a control frame must never stall the demux
// that carries every viewer's terminal traffic), so the alternative to polling
// is a sleep long enough to be flaky.
func waitForLiveExecs(t *testing.T, rd *Server, id string, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if liveExecsOn(rd, id) == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("session %s holds %d live exec(s), want %d", id, liveExecsOn(rd, id), want)
}

// TestADetachedExecKeepsAChildExitedSessionAlive is the reproduction, through
// the whole real path: a registered sandbox reports a live command over the
// control channel, its agent child exits, and the REAL idle loop runs on a
// timeout of a millisecond.
//
// Before this change the runner had no idea an exec existed — `exec_count` was
// an unknown control kind it logged and dropped — so the sweep cold-stopped
// the session within a tick. In production `docker stop`'s SIGTERM then
// reaches sessiond, whose handler kills every exec, and
// `rainier exec s --detach -- claude --continue` is dead: the case the flag
// exists for, killed by the other feature's default.
func TestADetachedExecKeepsAChildExitedSessionAlive(t *testing.T) {
	rd, srv, conn := runnerWithControld(t, func(s *Server) { s.idleSweep = time.Millisecond })
	fd := rd.drv.(*driver.Fake)
	id, sb := dialSandbox(t, rd, srv)
	nextEventOfState(t, conn, "running")

	// The detached command is running in there.
	sb.send(t, relay.ControlEvent{Kind: relay.KindExecCount, Live: 1, Seq: 1})
	waitForLiveExecs(t, rd, id, 1)

	// The agent that started the session finishes underneath it, and its
	// caller is long gone — no attachment, no child, just the command.
	sb.send(t, relay.ControlEvent{Kind: "child_exited", RC: 0})
	nextEventOfState(t, conn, "child_exited")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rd.RunIdleStop(ctx, time.Millisecond)

	// Long enough for hundreds of sweeps at a millisecond apiece.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, state, ok := rd.reg.opTarget(id); ok && state != "running" {
			t.Fatalf("the runner moved a session running a detached exec to %q; "+
				"docker stop's SIGTERM would have killed the command", state)
		}
		time.Sleep(5 * time.Millisecond)
	}
	handle, _, _ := rd.reg.opTarget(id)
	if h, err := fd.Inspect(ctx, handle); err != nil || h.State != driver.StateRunning {
		t.Fatalf("container state = %+v (%v), want running", h, err)
	}

	// And when the command ends, the clock starts: the same sweep that spent
	// half a second refusing to stop this session stops it once there is
	// nothing left in it.
	sb.send(t, relay.ControlEvent{Kind: relay.KindExecCount, Live: 0, Seq: 2})
	waitForLiveExecs(t, rd, id, 0)
	ev := nextEventOfState(t, conn, "suspended_cold")
	if ev.Session != id {
		t.Fatalf("suspended_cold for %q, want %q", ev.Session, id)
	}
}

// TestARunningCommandCountsAsActive: the capacity line a runner sends up.
// A box of sessions running detached commands reporting "no agents in here"
// would be the same confident lie the recovered-session exemption refuses to
// tell, and it is what an operator reads when deciding where the fleet's work
// is.
func TestARunningCommandCountsAsActive(t *testing.T) {
	rd, srv, conn := runnerWithControld(t)
	id, sb := dialSandbox(t, rd, srv)
	nextEventOfState(t, conn, "running")

	sb.send(t, relay.ControlEvent{Kind: relay.KindExecCount, Live: 1, Seq: 1})
	waitForLiveExecs(t, rd, id, 1)
	sb.send(t, relay.ControlEvent{Kind: "child_exited", RC: 0})
	ev := nextEventOfState(t, conn, "child_exited")
	if ev.Active != 1 || ev.IdleExited != 0 {
		t.Fatalf("a session running a command counted (%d active, %d idle-exited), want (1, 0)",
			ev.Active, ev.IdleExited)
	}

	// The command finishes and the honest answer changes with it.
	sb.send(t, relay.ControlEvent{Kind: relay.KindExecCount, Live: 0, Seq: 2})
	waitForLiveExecs(t, rd, id, 0)
	active, idleExited := rd.reg.counts()
	if active != 0 || idleExited != 1 {
		t.Fatalf("after the last command ended: (%d active, %d idle-exited), want (0, 1)",
			active, idleExited)
	}
}

// TestAnExecCountFromAnOldBootIsRefused is the same guard child_exited has,
// for the same reason: a hub read loop stalled on a wedged viewer drains its
// buffered frames whenever it comes back, which can be after the sandbox has
// been stopped, resumed and re-registered. A stale "one live" landing on the
// new boot would pin a finished session out of auto-stop for the life of the
// runner, with nothing in the log to say why.
func TestAnExecCountFromAnOldBootIsRefused(t *testing.T) {
	h := newIdleHarness(t)
	stale := h.boot[h.id]

	// A cold stop and a resume: the container restarts, so the epoch moves and
	// the new sandbox registers on it.
	if err := h.rd.Op(context.Background(), h.id, "suspend", false); err != nil {
		t.Fatalf("cold suspend: %v", err)
	}
	if err := h.rd.Op(context.Background(), h.id, "resume", false); err != nil {
		t.Fatalf("resume: %v", err)
	}
	h.register(h.id)
	if h.boot[h.id] == stale {
		t.Fatal("the cold resume did not open a new boot epoch")
	}

	// The previous boot's buffered report finally drains.
	h.rd.routeControl(h.id, stale, []byte(`{"kind":"exec_count","live":1,"seq":9}`))
	if got := liveExecsOn(h.rd, h.id); got != 0 {
		t.Fatalf("a report from a boot the session has left set live=%d, want 0", got)
	}

	// And the new boot's own reports are not refused behind it: a resume
	// clears the sequence fence with the count, so the new sessiond — a new
	// process, numbering from 1 — is heard.
	h.rd.routeControl(h.id, h.boot[h.id], []byte(`{"kind":"exec_count","live":1,"seq":1}`))
	if got := liveExecsOn(h.rd, h.id); got != 1 {
		t.Fatalf("the new sandbox's first report set live=%d, want 1; "+
			"a stale sequence fence would refuse every report it ever sends", got)
	}
}

// TestAReorderedExecCountIsRefused pins the other fence. The sandbox assigns
// its sequence numbers under the lock that changes its own count and reports
// after releasing it, so two transitions can reach this runner in either
// order. A "one live" believed after a "none live" would leave the session
// holding a slot forever with nothing running in it.
func TestAReorderedExecCountIsRefused(t *testing.T) {
	h := newIdleHarness(t)
	h.rd.routeControl(h.id, h.boot[h.id], []byte(`{"kind":"exec_count","live":1,"seq":1}`))
	h.rd.routeControl(h.id, h.boot[h.id], []byte(`{"kind":"exec_count","seq":2}`))
	if got := liveExecsOn(h.rd, h.id); got != 0 {
		t.Fatalf("live=%d after the command ended, want 0", got)
	}

	// The start report, overtaken on its way here, arrives late.
	h.rd.routeControl(h.id, h.boot[h.id], []byte(`{"kind":"exec_count","live":1,"seq":1}`))
	if got := liveExecsOn(h.rd, h.id); got != 0 {
		t.Fatalf("a report that did not advance the sequence set live=%d, want 0", got)
	}

	// A repeat of the LAST report is refused too, which is what keeps a
	// re-delivered "none live" from pushing the idle deadline out — the same
	// rule childExited applies to its own repeats.
	h.clk.set(time.Hour)
	h.rd.routeControl(h.id, h.boot[h.id], []byte(`{"kind":"exec_count","seq":2}`))
	e := h.entry(h.id)
	if !e.lastExecEndedAt.Equal(idleEpoch) {
		t.Fatalf("a re-delivered report moved the idle clock to %s, want it left at %s",
			e.lastExecEndedAt, idleEpoch)
	}
}

// TestANegativeExecCountReadsAsNone: nothing sends one, and "fewer than no
// commands" would be a count that can never fall back to zero.
func TestANegativeExecCountReadsAsNone(t *testing.T) {
	h := newIdleHarness(t)
	h.rd.routeControl(h.id, h.boot[h.id], []byte(`{"kind":"exec_count","live":-3,"seq":1}`))
	if got := liveExecsOn(h.rd, h.id); got != 0 {
		t.Fatalf("a negative report set live=%d, want 0", got)
	}
	e := h.entry(h.id)
	if _, idle := e.idleFor(h.clk.now()); idle {
		t.Fatal("a session whose child is still running looked idle")
	}
}

// TestACommandAcrossASweepIsNotStoppedAndThenIs walks the whole shape once
// more at the registry's own level, so a regression in idleFor reports here
// rather than only through the loop.
func TestACommandAcrossASweepIsNotStoppedAndThenIs(t *testing.T) {
	h := newIdleHarness(t)
	ctx := context.Background()
	h.rd.routeControl(h.id, h.boot[h.id], []byte(`{"kind":"child_exited","rc":0}`))
	h.rd.routeControl(h.id, h.boot[h.id], []byte(`{"kind":"exec_count","live":1,"seq":1}`))

	for _, at := range []time.Duration{time.Hour, 6 * time.Hour, 24 * time.Hour} {
		h.clk.set(at)
		if stops := h.rd.sweepIdle(ctx, 30*time.Minute); len(stops) != 0 {
			t.Fatalf("stopped %v at %s with a command running", stops, at)
		}
	}

	h.clk.set(24 * time.Hour)
	h.rd.routeControl(h.id, h.boot[h.id], []byte(`{"kind":"exec_count","seq":2}`))
	h.clk.set(24*time.Hour + 29*time.Minute)
	if stops := h.rd.sweepIdle(ctx, 30*time.Minute); len(stops) != 0 {
		t.Fatalf("stopped %v 29 minutes after the command ended", stops)
	}
	h.clk.set(24*time.Hour + 30*time.Minute)
	if stops := h.rd.sweepIdle(ctx, 30*time.Minute); len(stops) != 1 || stops[0] != h.id {
		t.Fatalf("sweep stopped %v, want [%s]", stops, h.id)
	}
}

// TestAColdResumeClearsTheExecBookkeeping. `docker start` gives the container
// a new process tree, so the old sandbox's commands are gone with it — and its
// sessiond is a NEW process whose sequence numbers start again at 1. The count
// and the fence have to be cleared together: leaving the fence behind would
// make every report the new sandbox ever sends look stale, and the session
// would then hold its slot for the life of the runner with nothing in the log
// to say why.
func TestAColdResumeClearsTheExecBookkeeping(t *testing.T) {
	h := newIdleHarness(t)
	ctx := context.Background()
	h.rd.routeControl(h.id, h.boot[h.id], []byte(`{"kind":"exec_count","live":1,"seq":5}`))
	if got := liveExecsOn(h.rd, h.id); got != 1 {
		t.Fatalf("live=%d before the stop, want 1", got)
	}

	if err := h.rd.Op(ctx, h.id, "suspend", false); err != nil {
		t.Fatalf("cold suspend: %v", err)
	}
	if err := h.rd.Op(ctx, h.id, "resume", false); err != nil {
		t.Fatalf("resume: %v", err)
	}
	h.register(h.id)

	if e := h.entry(h.id); e.liveExecs != 0 || !e.lastExecEndedAt.IsZero() {
		t.Fatalf("a restarted container kept live=%d, lastExecEndedAt=%s",
			e.liveExecs, e.lastExecEndedAt)
	}
	// The new sessiond's FIRST report, numbered 1 as every sandbox's is.
	h.rd.routeControl(h.id, h.boot[h.id], []byte(`{"kind":"exec_count","live":1,"seq":1}`))
	if got := liveExecsOn(h.rd, h.id); got != 1 {
		t.Fatalf("the restarted sandbox's first report set live=%d, want 1; "+
			"a sequence fence left over from the previous boot refuses every one of them", got)
	}
}

// TestAWarmResumeKeepsTheExecBookkeeping is the other half, and it is not
// symmetry for its own sake: `docker unpause` restarts nothing, the same
// sessiond keeps counting from where it was, and its numbers keep advancing.
// Clearing the fence here would let a report that was overtaken in flight be
// believed on the way back.
func TestAWarmResumeKeepsTheExecBookkeeping(t *testing.T) {
	h := newIdleHarness(t)
	ctx := context.Background()
	// The warm-suspend handshake kills every exec before the freeze, so the
	// count the sandbox last reported is zero — stamped at the moment the last
	// one ended.
	h.rd.routeControl(h.id, h.boot[h.id], []byte(`{"kind":"exec_count","live":1,"seq":1}`))
	h.clk.set(10 * time.Minute)
	h.rd.routeControl(h.id, h.boot[h.id], []byte(`{"kind":"exec_count","seq":2}`))
	stamped := h.entry(h.id).lastExecEndedAt

	if err := h.rd.Op(ctx, h.id, "suspend", true); err != nil {
		t.Fatalf("warm suspend: %v", err)
	}
	if err := h.rd.Op(ctx, h.id, "resume", false); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if e := h.entry(h.id); !e.lastExecEndedAt.Equal(stamped) {
		t.Fatalf("an unpause moved the exec clock from %s to %s", stamped, e.lastExecEndedAt)
	}
	// And the fence survived with it: a report numbered behind the last one is
	// still refused.
	h.rd.routeControl(h.id, h.boot[h.id], []byte(`{"kind":"exec_count","live":1,"seq":1}`))
	if got := liveExecsOn(h.rd, h.id); got != 0 {
		t.Fatalf("a stale report set live=%d across an unpause, want 0", got)
	}
}

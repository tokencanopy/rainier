// internal/runnerd/suspend_test.go
package runnerd

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/internal/driver"
	"github.com/tokencanopy/rainier/internal/relay"
)

// This file is the runner's half of the exec lifetime rule: a session that is
// STOPPED reaps the commands running in it, and the default stop is a WARM
// suspend — `docker pause` — which delivers the sandbox no signal at all, so
// sessiond's own SIGTERM handler (the only thing wired to KillAll) never runs.

// suspendTrackingFake records WHEN Suspend happened, so a test can assert an
// order rather than merely that two things both occurred.
type suspendTrackingFake struct {
	*driver.Fake
	mu         sync.Mutex
	suspendsAt []time.Time
}

func newSuspendTrackingFake(total int) *suspendTrackingFake {
	return &suspendTrackingFake{Fake: driver.NewFake(total)}
}

func (f *suspendTrackingFake) Suspend(ctx context.Context, id string, warm bool) error {
	f.mu.Lock()
	f.suspendsAt = append(f.suspendsAt, time.Now())
	f.mu.Unlock()
	return f.Fake.Suspend(ctx, id, warm)
}

func (f *suspendTrackingFake) suspendedAt() (time.Time, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.suspendsAt) == 0 {
		return time.Time{}, false
	}
	return f.suspendsAt[0], true
}

// suspendScene is a runner with one created, registered session and a driver
// that remembers when it was asked to suspend.
func suspendScene(t *testing.T) (*Server, *suspendTrackingFake, *sandbox, string) {
	t.Helper()
	fd := newSuspendTrackingFake(4)
	rd := New(fd, "", "", "")
	srv := httptest.NewServer(rd.Handler())
	t.Cleanup(srv.Close)
	id, sb := dialSandbox(t, rd, srv)
	return rd, fd, sb, id
}

// answer sends one of the sandbox's two replies, echoing the notice's nonce.
func (sb *sandbox) answer(t *testing.T, kind string, nonce uint64) {
	t.Helper()
	sb.send(t, relay.ControlEvent{Kind: kind, ID: nonce})
}

// TestAWarmSuspendTellsTheSandboxBeforeItFreezesIt is the ordering the whole
// fix rests on. The freezer cgroup stops a container's clocks, so a signal
// sent AFTER the pause is one that stays pending until somebody resumes the
// session — which is the bug rather than a different spelling of it. The
// notice goes out first, and the pause waits for the answers.
func TestAWarmSuspendTellsTheSandboxBeforeItFreezesIt(t *testing.T) {
	rd, fd, sb, id := suspendScene(t)

	done := make(chan error, 1)
	go func() { done <- rd.Op(context.Background(), id, "suspend", true) }()

	ev := sb.nextControl(t)
	if ev.Kind != relay.KindSuspending {
		t.Fatalf("the sandbox was sent %q, want %q", ev.Kind, relay.KindSuspending)
	}
	if ev.ID == 0 {
		t.Fatal("the suspend notice carries no nonce; a late answer to it would " +
			"satisfy the next suspend")
	}
	if _, suspended := fd.suspendedAt(); suspended {
		t.Fatal("the container was frozen before the sandbox was told to end its execs")
	}
	noticeAt := time.Now()

	// The ack alone must NOT release the pause: it says "heard", not "gone".
	sb.answer(t, relay.KindSuspendAck, ev.ID)
	select {
	case <-done:
		t.Fatal("the container was frozen on the acknowledgement alone; the execs " +
			"had not been reported gone")
	case <-time.After(300 * time.Millisecond):
	}

	sb.answer(t, relay.KindSuspendReady, ev.ID)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("warm suspend: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the ready answer did not release the suspend")
	}
	at, suspended := fd.suspendedAt()
	if !suspended {
		t.Fatal("the container was never suspended")
	}
	if at.Before(noticeAt) {
		t.Fatal("the container was frozen before the notice went out")
	}
}

// TestASandboxThatPredatesTheNoticePaysOnlyTheSHORTBudget is the compatibility
// half, and it is not a corner: a session keeps the sessiond it booted with for
// as long as it lives, so EVERY session created before exec shipped reaches
// this path on every warm stop, forever. It must cost the short wait, not the
// long one.
func TestASandboxThatPredatesTheNoticePaysOnlyTheSHORTBudget(t *testing.T) {
	rd, fd, sb, id := suspendScene(t)
	rd.suspendAckWait = 150 * time.Millisecond
	rd.suspendReadyWait = 30 * time.Second // must not be spent

	started := time.Now()
	done := make(chan error, 1)
	go func() { done <- rd.Op(context.Background(), id, "suspend", true) }()

	// An older sessiond logs an unknown control kind and drops it.
	if ev := sb.nextControl(t); ev.Kind != relay.KindSuspending {
		t.Fatalf("the sandbox was sent %q", ev.Kind)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("warm suspend against a silent sandbox: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a silent sandbox held the suspend open past its ACK budget; every " +
			"session created before exec shipped pays this on every stop")
	}
	if took := time.Since(started); took > 5*time.Second {
		t.Fatalf("a silent sandbox cost %s; it must cost the ack budget, not the "+
			"ready one", took)
	}
	if _, suspended := fd.suspendedAt(); !suspended {
		t.Fatal("a silent sandbox stopped the container from being suspended at all")
	}
}

// TestASandboxThatHeardGetsTheLongBudget is the other side of that: a sandbox
// that answered the ack is working, so it is given the time its kill needs
// rather than the two seconds an old one gets.
func TestASandboxThatHeardGetsTheLongBudget(t *testing.T) {
	rd, _, sb, id := suspendScene(t)
	rd.suspendAckWait = 150 * time.Millisecond
	rd.suspendReadyWait = 5 * time.Second

	done := make(chan error, 1)
	go func() { done <- rd.Op(context.Background(), id, "suspend", true) }()
	ev := sb.nextControl(t)
	sb.answer(t, relay.KindSuspendAck, ev.ID)

	// Well past the ACK budget, and still not frozen: the ack bought the time.
	select {
	case <-done:
		t.Fatal("a sandbox that said it heard was cut off at the ack budget")
	case <-time.After(time.Second):
	}
	sb.answer(t, relay.KindSuspendReady, ev.ID)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("warm suspend: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the ready answer did not release the suspend")
	}
}

// TestAColdSuspendSendsNoNotice: `docker stop` delivers the SIGTERM sessiond's
// own handler already answers, so the cold path keeps exactly the shape it has
// today and pays none of the budgets above.
func TestAColdSuspendSendsNoNotice(t *testing.T) {
	rd, _, sb, id := suspendScene(t)

	if err := rd.Op(context.Background(), id, "suspend", false); err != nil {
		t.Fatalf("cold suspend: %v", err)
	}
	seen := make(chan string, 1)
	go func() {
		ctx, cancel := context.WithTimeout(sb.ctx, 400*time.Millisecond)
		defer cancel()
		for {
			_, raw, err := sb.c.Read(ctx)
			if err != nil {
				return
			}
			f, err := relay.Decode(raw)
			if err != nil || f.Type != relay.FrameControl {
				continue
			}
			var ev relay.ControlEvent
			if json.Unmarshal(f.Payload, &ev) == nil {
				seen <- ev.Kind
				return
			}
		}
	}()
	select {
	case kind := <-seen:
		t.Fatalf("a cold suspend sent the sandbox a %q it has no use for", kind)
	case <-time.After(600 * time.Millisecond):
	}
}

// TestAStaleAnswerCannotReleaseTheNextSuspend is what the nonce is for.
//
// Sessiond's answer can legitimately outlast runnerd's budget — its own
// quiesce waits out a kill grace, and the send behind it queues on a conn
// writer an exec may be holding. Without the nonce, suspend #1's straggler
// releases suspend #2 the instant it arrives: runnerd freezes a container
// while its sandbox is mid-kill, leaving a signal pending in the freezer
// cgroup, which is the exact failure this handshake exists to prevent.
func TestAStaleAnswerCannotReleaseTheNextSuspend(t *testing.T) {
	rd, _, sb, id := suspendScene(t)
	rd.suspendAckWait = 100 * time.Millisecond
	rd.suspendReadyWait = 100 * time.Millisecond

	// Suspend #1: the sandbox says nothing in time and the container freezes.
	if err := rd.Op(context.Background(), id, "suspend", true); err != nil {
		t.Fatalf("first suspend: %v", err)
	}
	first := sb.nextControl(t)
	if first.Kind != relay.KindSuspending {
		t.Fatalf("first notice = %q", first.Kind)
	}

	// Suspend #2, with a budget long enough that only an ANSWER can release
	// it quickly.
	rd.suspendAckWait = 10 * time.Second
	rd.suspendReadyWait = 10 * time.Second
	if err := rd.Op(context.Background(), id, "resume", false); err != nil {
		t.Fatalf("resume: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- rd.Op(context.Background(), id, "suspend", true) }()
	second := sb.nextControl(t)
	if second.ID == first.ID {
		t.Fatal("two suspends used the same nonce; a stale answer is indistinguishable")
	}

	// Suspend #1's stragglers finally arrive.
	started := time.Now()
	sb.answer(t, relay.KindSuspendAck, first.ID)
	sb.answer(t, relay.KindSuspendReady, first.ID)
	select {
	case <-done:
		t.Fatalf("a stale answer released the next suspend after %s; the container "+
			"freezes while the sandbox is mid-kill", time.Since(started))
	case <-time.After(700 * time.Millisecond):
	}

	// The right answers still work.
	sb.answer(t, relay.KindSuspendAck, second.ID)
	sb.answer(t, relay.KindSuspendReady, second.ID)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("second suspend: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the matching answers did not release the suspend")
	}
}

// TestAFinishedSuspendDoesNotUnregisterAConcurrentOne. Deleting the waiter by
// session id alone let the first of two overlapping suspends unregister the
// second's, which then waited out its whole budget with the answer already in
// hand. The doc calls concurrent suspends a caller bug; a controld stop retry
// makes them ordinary.
func TestAFinishedSuspendDoesNotUnregisterAConcurrentOne(t *testing.T) {
	rd, _, _, id := suspendScene(t)

	a := rd.armSuspendWaiter(id, 1)
	b := rd.armSuspendWaiter(id, 2)
	rd.disarmSuspendWaiter(id, a) // a finishes first

	rd.suspendMu.Lock()
	got := rd.suspends[id]
	rd.suspendMu.Unlock()
	if got != b {
		t.Fatal("the first suspend's cleanup unregistered the second's waiter; the " +
			"second now waits out its whole budget with its answer already sent")
	}
	rd.disarmSuspendWaiter(id, b)
	rd.suspendMu.Lock()
	n := len(rd.suspends)
	rd.suspendMu.Unlock()
	if n != 0 {
		t.Fatalf("%d waiter(s) left behind", n)
	}
}

// TestASuspendGivesUpWhenTheConnDies: the registry knows the hub is gone the
// moment it happens, and a sandbox that is not there will not answer. Waiting
// out the budget anyway makes every stop of a session whose container already
// died as slow as one of a session that is merely old.
func TestASuspendGivesUpWhenTheConnDies(t *testing.T) {
	rd, _, sb, id := suspendScene(t)
	rd.suspendAckWait = 30 * time.Second
	rd.suspendReadyWait = 30 * time.Second

	done := make(chan error, 1)
	go func() { done <- rd.Op(context.Background(), id, "suspend", true) }()
	if ev := sb.nextControl(t); ev.Kind != relay.KindSuspending {
		t.Fatalf("the sandbox was sent %q", ev.Kind)
	}
	sb.c.CloseNow()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("suspend after the conn died: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("a suspend waited out its budget for a sandbox whose conn was already " +
			"known to be gone")
	}
}

// TestASuspendGivesUpWhenItsDispatchIsCancelled: a dispatch whose context is
// already cancelled — controld gone, the agent conn dropped — is one whose
// Suspend is about to fail anyway. Burning the budget first helps nobody.
func TestASuspendGivesUpWhenItsDispatchIsCancelled(t *testing.T) {
	rd, _, sb, id := suspendScene(t)
	rd.suspendAckWait = 30 * time.Second
	rd.suspendReadyWait = 30 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rd.Op(ctx, id, "suspend", true) }()
	if ev := sb.nextControl(t); ev.Kind != relay.KindSuspending {
		t.Fatalf("the sandbox was sent %q", ev.Kind)
	}
	cancel()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("a cancelled dispatch waited out the suspend budget")
	}
}

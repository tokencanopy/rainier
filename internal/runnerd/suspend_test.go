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

// TestAWarmSuspendTellsTheSandboxBeforeItFreezesIt is the ordering the whole
// fix rests on. The freezer cgroup stops a container's clocks, so a signal
// sent AFTER the pause is one that stays pending until somebody resumes the
// session — which is the bug rather than a different spelling of it. The
// notice goes out first, and the pause waits for the answer.
func TestAWarmSuspendTellsTheSandboxBeforeItFreezesIt(t *testing.T) {
	rd, fd, sb, id := suspendScene(t)

	done := make(chan error, 1)
	go func() { done <- rd.Op(context.Background(), id, "suspend", true) }()

	ev := sb.nextControl(t)
	if ev.Kind != relay.KindSuspending {
		t.Fatalf("the sandbox was sent %q, want %q", ev.Kind, relay.KindSuspending)
	}
	if ev.ID != 0 {
		t.Fatalf("the suspend notice carries id %d; it is an event, not a request", ev.ID)
	}
	if _, suspended := fd.suspendedAt(); suspended {
		t.Fatal("the container was frozen before the sandbox was told to end its execs")
	}
	noticeAt := time.Now()

	sb.send(t, relay.ControlEvent{Kind: relay.KindSuspendReady})
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("warm suspend: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the acknowledgement did not release the suspend")
	}
	at, suspended := fd.suspendedAt()
	if !suspended {
		t.Fatal("the container was never suspended")
	}
	if at.Before(noticeAt) {
		t.Fatal("the container was frozen before the notice went out")
	}
}

// TestAWarmSuspendProceedsWhenTheSandboxCannotAnswer is the compatibility
// half, and it is not a corner: a session keeps the sessiond it booted with
// for as long as it lives, so every session created before this rolls answers
// nothing, forever. It must cost a bounded wait and never a failed stop.
func TestAWarmSuspendProceedsWhenTheSandboxCannotAnswer(t *testing.T) {
	rd, fd, sb, id := suspendScene(t)
	rd.suspendWait = 150 * time.Millisecond

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
		t.Fatal("a silent sandbox held the suspend open past its budget")
	}
	if _, suspended := fd.suspendedAt(); !suspended {
		t.Fatal("a silent sandbox stopped the container from being suspended at all")
	}
}

// TestAColdSuspendSendsNoNotice: `docker stop` delivers the SIGTERM sessiond's
// own handler already answers, so the cold path keeps exactly the shape it has
// today and pays none of the budget above.
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

// TestALateSuspendAcknowledgementIsDropped: a sandbox that answers after the
// budget expired must not satisfy the NEXT suspend, and must not panic on a
// waiter that is no longer there.
func TestALateSuspendAcknowledgementIsDropped(t *testing.T) {
	rd, _, sb, id := suspendScene(t)
	rd.suspendWait = 100 * time.Millisecond

	if err := rd.Op(context.Background(), id, "suspend", true); err != nil {
		t.Fatalf("warm suspend: %v", err)
	}
	if ev := sb.nextControl(t); ev.Kind != relay.KindSuspending {
		t.Fatalf("the sandbox was sent %q", ev.Kind)
	}
	// Far too late — the suspend already gave up and the container is paused.
	sb.send(t, relay.ControlEvent{Kind: relay.KindSuspendReady})
	time.Sleep(100 * time.Millisecond)

	rd.suspendMu.Lock()
	n := len(rd.suspendAcks)
	rd.suspendMu.Unlock()
	if n != 0 {
		t.Fatalf("%d suspend waiter(s) left behind; a later suspend would be satisfied by a stale answer", n)
	}
}

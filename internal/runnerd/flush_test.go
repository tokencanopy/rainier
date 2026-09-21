// internal/runnerd/flush_test.go
//
// The runner's half of the flush a microVM snapshot takes first: the host asks
// the sandbox to put what it has written on its block devices, and publishes
// nothing until it says it has.
package runnerd

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/internal/relay"
)

// TestFlushGuestWaitsForTheSandbox is the happy path and the ordering: the
// request goes out with a nonce, and the call does not return until the
// sandbox's answer carries that nonce back.
func TestFlushGuestWaitsForTheSandbox(t *testing.T) {
	rd, _, sb, id := suspendScene(t)

	done := make(chan error, 1)
	go func() { done <- rd.FlushGuest(context.Background(), id) }()

	ev := sb.nextControl(t)
	if ev.Kind != relay.KindFlush {
		t.Fatalf("the sandbox was sent %q, want %q", ev.Kind, relay.KindFlush)
	}
	if ev.ID == 0 {
		t.Fatal("the flush request carries no nonce; a late answer to it would release the next one")
	}

	select {
	case <-done:
		t.Fatal("FlushGuest returned before the sandbox reported a flush; a snapshot would copy a filesystem with the guest's writes still in its page cache")
	case <-time.After(200 * time.Millisecond):
	}

	sb.answer(t, relay.KindFlushed, ev.ID)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("FlushGuest: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the flushed answer did not release the flush")
	}
}

// TestFlushGuestRefusesAStaleAnswer: a late answer to a flush that already
// gave up must not release the next one, or a snapshot would copy a filesystem
// on the strength of a sync that finished before the writes it is publishing.
func TestFlushGuestRefusesAStaleAnswer(t *testing.T) {
	rd, _, sb, id := suspendScene(t)
	rd.flushWait = 150 * time.Millisecond

	done := make(chan error, 1)
	go func() { done <- rd.FlushGuest(context.Background(), id) }()
	first := sb.nextControl(t)
	if err := <-done; err == nil {
		t.Fatal("a flush nobody answered succeeded")
	}

	// The second flush gets its own nonce, and the first one's late answer
	// does not satisfy it.
	rd.flushWait = 10 * time.Second
	go func() { done <- rd.FlushGuest(context.Background(), id) }()
	second := sb.nextControl(t)
	if second.ID == first.ID {
		t.Fatal("two flushes of one session shared a nonce")
	}
	sb.answer(t, relay.KindFlushed, first.ID)
	select {
	case <-done:
		t.Fatal("a late answer to the first flush released the second")
	case <-time.After(200 * time.Millisecond):
	}
	sb.answer(t, relay.KindFlushed, second.ID)
	if err := <-done; err != nil {
		t.Fatalf("FlushGuest: %v", err)
	}
}

// TestFlushGuestReportsASandboxThatCannotBeAsked. Every way this fails is an
// error and not a shrug, which is where it differs from the suspend notice: a
// suspend that goes ahead unflushed freezes a container that will be thawed
// again, where a SNAPSHOT that goes ahead unflushed publishes an environment
// image that every later session of that environment boots.
func TestFlushGuestReportsASandboxThatCannotBeAsked(t *testing.T) {
	rd, _, sb, id := suspendScene(t)

	// A session this runner does not hold.
	if err := rd.FlushGuest(context.Background(), "no-such-session"); err == nil {
		t.Fatal("FlushGuest of an unknown session succeeded")
	}

	// A sandbox that predates the kind: it logs one unknown frame and answers
	// nothing, and the budget is what ends the wait.
	rd.flushWait = 150 * time.Millisecond
	err := rd.FlushGuest(context.Background(), id)
	if err == nil {
		t.Fatal("FlushGuest succeeded against a sandbox that never answered")
	}
	if !strings.Contains(err.Error(), "did not report a flush") {
		t.Errorf("error = %q, want it to say the sandbox did not answer", err)
	}
	if ev := sb.nextControl(t); ev.Kind != relay.KindFlush {
		t.Fatalf("the sandbox was sent %q", ev.Kind)
	}

	// And a caller whose own context ends does not wait out the budget.
	rd.flushWait = 30 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := rd.FlushGuest(ctx, id); err == nil {
		t.Fatal("FlushGuest with a cancelled context succeeded")
	}
}

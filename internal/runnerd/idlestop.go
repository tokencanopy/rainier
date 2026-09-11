// internal/runnerd/idlestop.go — idle auto-stop.
//
// A sandbox holds one of this runner's slots from creation until someone
// stops or deletes its session, whether or not its child process finished
// hours ago. This file is the small, safe half of the fix (design
// docs/design/runner-idle-stop.md, issue #85): a session whose child has
// exited and that has had no attachment for the configured timeout is stopped
// by the runner itself — the container stops, the files stay, the slot comes
// back, and `rainier attach` resumes it. It is the same cold suspend an
// operator's `rainier stop` performs, through the same coldSuspend call.
//
// What it never does: stop a session whose child is still running, however
// long nobody has been attached (a long unattended build is the product), and
// delete anything at all.
package runnerd

import (
	"context"
	"log"
	"strconv"
	"time"
)

// idleSweepMin and idleSweepMax bound how often RunIdleStop looks. The
// interval is a tenth of the timeout so that a stop lands within ten percent
// of when it was due, clamped at both ends: never more than once a second
// (a timeout of seconds is a test's or a demo's, and a sweep is a lock and a
// map walk, but there is no reason to spin), and never less than once a
// minute (a long timeout should not mean a finished session goes on holding a
// slot for an extra quarter of an hour).
//
// The sweep decides against the clock, not against a tick count, so a session
// is stopped at the first sweep at or after its deadline: up to one interval
// late, never early.
const (
	idleSweepMin = time.Second
	idleSweepMax = time.Minute
	// idleStopTimeout and idleCapacityTimeout bound the two driver calls one
	// stop makes. They match the bounds the rest of this package already uses
	// for a driver call made off a background goroutine: 30s for an operation
	// on a container, 5s for a capacity reading.
	idleStopTimeout     = 30 * time.Second
	idleCapacityTimeout = 5 * time.Second
)

// idleSweepInterval is how often a timeout of idle is swept for.
func idleSweepInterval(idle time.Duration) time.Duration {
	d := idle / 10
	if d < idleSweepMin {
		return idleSweepMin
	}
	if d > idleSweepMax {
		return idleSweepMax
	}
	return d
}

// RunIdleStop stops idle sessions until ctx is done. idle is the timeout
// (cmd/runnerd's --idle-stop); zero or negative disables the whole loop, which
// is the documented way to turn the behaviour off, and this function then
// returns immediately rather than running a sweep that can never fire.
//
// It is one goroutine for the whole runner rather than a timer per session:
// the state a timer would key off (an attach opening, a detach, a child
// exiting, a delete) changes on four different goroutines, so a timer would
// need cancelling and resetting from all of them, and would leak on the paths
// that forget. A sweep reads the same fact from the registry each time and has
// nothing to leak.
func (s *Server) RunIdleStop(ctx context.Context, idle time.Duration) {
	if idle <= 0 {
		log.Printf("runnerd: idle auto-stop is disabled")
		return
	}
	every := s.idleSweep
	if every <= 0 {
		every = idleSweepInterval(idle)
	}
	log.Printf("runnerd: idle auto-stop after %s, checked every %s", idle, every)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.sweepIdle(ctx, idle)
		}
	}
}

// sweepIdle stops every session that has been idle for at least idle, and
// returns their ids in the order they were stopped. Split from RunIdleStop's
// ticker so the decision can be driven directly, one sweep at a time, against
// a fake clock.
//
// The candidate list is a snapshot; the decision is claimIdle, which re-checks
// the same rule under the registry lock it marks the entry in. A session that
// stops being idle between the two — an attach that lands, a stop that arrives
// from controld, a delete — is simply not claimed.
func (s *Server) sweepIdle(ctx context.Context, idle time.Duration) []string {
	if idle <= 0 {
		// The disabling value, checked here as well as in RunIdleStop so that
		// "off" is a property of the decision and not merely of the loop that
		// calls it: without this, a zero would make every session with an
		// exited child idle "for at least zero", i.e. stop the lot.
		return nil
	}
	var stopped []string
	for _, id := range s.reg.idleSessions(idle, s.now()) {
		if s.stopIdle(ctx, id, idle) {
			stopped = append(stopped, id)
		}
	}
	return stopped
}

// stopIdle claims one session and stops it, reporting whether it did.
//
// Everything after the claim is the operator's stop path verbatim: the same
// driver call, the same landing state, the same rollback. The event is the
// one thing an operator's stop does not need, because controld already knows
// about a stop it dispatched — this one it did not, so the runner says so, in
// the vocabulary the announce already uses for the same condition. A control
// plane that has never heard of it logs an unknown state and moves on; the
// session's row then heals the next time this runner reconnects and
// re-announces, which is the only time reconciliation runs.
func (s *Server) stopIdle(ctx context.Context, id string, idle time.Duration) bool {
	handle, idleFor, ok := s.reg.claimIdle(id, idle, s.now())
	if !ok {
		return false
	}
	// Bounded, like every other driver call this runner makes off a request
	// goroutine (register's post-hub-death Inspect takes 30s, the agent's
	// piggybacked Capacity 5s). cmd/runnerd runs this loop on a background
	// context, and one `docker stop` against a wedged daemon would otherwise
	// park the single sweep goroutine for good — idle auto-stop silently dead
	// for the whole runner until it is restarted.
	stopCtx, cancel := context.WithTimeout(ctx, idleStopTimeout)
	err := s.coldSuspend(stopCtx, id, handle)
	cancel()
	if err != nil {
		// Rolled back to "running" by coldSuspend, so the next sweep tries
		// again. No event and no auto-stop log line: nothing happened.
		log.Printf("session %s: idle auto-stop could not stop the container: %v", id, err)
		return false
	}
	active, idleExited := s.reg.counts()
	// One capacity call per session actually stopped — not per sweep tick —
	// because "what this stop freed" is the number worth logging and it is
	// only meaningful right after the stop. Bounded for the same reason the
	// stop is.
	capCtx, capCancel := context.WithTimeout(ctx, idleCapacityTimeout)
	used, total, capErr := s.drv.Capacity(capCtx)
	capCancel()
	free, slots := strconv.Itoa(total-used), strconv.Itoa(total)
	if capErr != nil {
		// The stop itself succeeded; only the count of what it freed is
		// unavailable. Both numbers go out as "unknown" rather than as
		// plausible-looking ones — a failing Capacity returns a zero used and
		// whatever total the driver happens to hold, and neither is a
		// measurement.
		free, slots = "unknown", "unknown"
	}
	log.Printf("runnerd: idle auto-stop session=%s idle=%s slots_free=%s slots_total=%s active=%d idle_exited=%d",
		id, idleFor.Round(time.Second), free, slots, active, idleExited)
	s.fireEventDetail(id, "suspended_cold", "idle for "+idleFor.Round(time.Second).String())
	return true
}

// internal/controld/sched.go
package controld

import (
	"context"
	"errors"
	"log"
	"time"

	"rainier/internal/rwire"
)

// runnerView is one connected runner's placement-relevant state: just
// enough for pickRunner to choose among them.
type runnerView struct {
	Name string
	Free int
}

// pickRunner returns the name of the runner with the most free capacity,
// breaking ties by lexicographically smaller name so placement is
// deterministic (design §4.7). It reports false when rs is empty or no
// runner has any free capacity — a runner with zero or negative Free
// (over-committed, which should not happen but must never be chosen if it
// does) is never a candidate.
func pickRunner(rs []runnerView) (string, bool) {
	best := -1
	for i, r := range rs {
		if r.Free <= 0 {
			continue
		}
		if best == -1 || r.Free > rs[best].Free || (r.Free == rs[best].Free && r.Name < rs[best].Name) {
			best = i
		}
	}
	if best == -1 {
		return "", false
	}
	return rs[best].Name, true
}

// schedulerLoop wakes on capacity/queue news (the fast path) or a 10s
// safety tick (in case a wake was ever missed or coalesced away) and makes
// one placement pass over the queue each time. It returns when ctx is
// done, which is what lets Run(ctx) host it directly.
func (s *Server) schedulerLoop(ctx context.Context) {
	t := time.NewTicker(10 * time.Second) // safety net; wakes are the fast path
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.schedWake:
		case <-t.C:
		}
		s.drainQueue(ctx)
	}
}

// drainQueue makes one FIFO placement pass over the queue: oldest session
// first, place it on whichever connected runner currently has the most
// free capacity, and start a goroutine to dispatch its create. It stops the
// instant no runner has any room left, leaving the remainder queued for the
// next wake — a burst that outgrows the fleet waits, it doesn't error.
//
// Placement — the free-capacity math and the queued->creating transition —
// runs sequentially in this one goroutine (schedulerLoop never overlaps
// calls), so two rows can never be handed the same last-open slot; free
// capacity is recomputed fresh from the store before each row, and a
// successful Transition is visible to that recomputation immediately, which
// is what keeps the math honest without any separate in-memory tally. Only
// the create dispatch that follows a successful placement runs
// concurrently, so one slow or wedged runner can't stall placement for the
// rest of the queue.
func (s *Server) drainQueue(ctx context.Context) {
	rows, err := s.st.OldestQueued(ctx)
	if err != nil {
		log.Printf("controld: scheduler: listing queued sessions: %v", err)
		return
	}
	for _, row := range rows {
		views, err := s.freeCapacity(ctx)
		if err != nil {
			log.Printf("controld: scheduler: computing free capacity: %v", err)
			return
		}
		runner, ok := pickRunner(views)
		if !ok {
			return // no capacity anywhere right now; leave the rest queued
		}
		if err := s.st.Transition(ctx, row.ID, []SessionState{StateQueued}, StateCreating, TransitionOpts{Runner: &runner}); err != nil {
			if errors.Is(err, ErrConflict) || errors.Is(err, ErrNotFound) {
				continue // canceled or already moved out from under us
			}
			log.Printf("controld: scheduler: placing %s on %s: %v", row.ID, runner, err)
			continue
		}
		go s.dispatchCreate(ctx, row, runner)
	}
}

// freeCapacity returns one runnerView per connected runner. Free is
// CapacityTotal - CapacityUsed - the number of sessions this runner is
// currently `creating`: a row just placed here hasn't necessarily shown up
// in the runner's own reported Used yet (that arrives piggybacked on the
// runner's next message, whenever that is), so without subtracting it a
// burst of placements could overshoot every runner's real capacity before a
// single create has landed.
func (s *Server) freeCapacity(ctx context.Context) ([]runnerView, error) {
	runners, err := s.st.ListRunners(ctx)
	if err != nil {
		return nil, err
	}
	views := make([]runnerView, 0, len(runners))
	for _, r := range runners {
		if !r.Connected {
			continue
		}
		creating, err := s.st.SessionsOnRunner(ctx, r.Name, []SessionState{StateCreating})
		if err != nil {
			return nil, err
		}
		views = append(views, runnerView{Name: r.Name, Free: r.CapacityTotal - r.CapacityUsed - len(creating)})
	}
	return views, nil
}

// dispatchCreate sends row's create to runner and settles the outcome:
// ok:false fails the session with the runner's own detail text, and a
// transport failure — no connection, a dead one, a full send queue
// (ErrRunnerUnreachable) — puts the row back on the queue with its placement
// cleared and wakes the scheduler so it, or once it's back another runner,
// gets another chance.
//
// A timeout on a LIVE connection (ErrDispatchTimeout) is deliberately not
// requeued. The command was delivered, and a create that outlasts OpTimeout
// is usually one still pulling a cold image: requeuing it would place a
// second copy of the same session on another runner, and the first copy
// would survive — invisible to the store — until that runner's next announce
// reconciled it away, or, if it landed back on the same runner, leave a row
// stuck `creating` against a container that already exists. So the row is
// left `creating` and settled by whichever arrives first: the runner's
// "running" event, its next announce (adopt if the create landed, requeue if
// it didn't), or a later reconcile.
//
// The caller's own ctx being canceled (process shutdown) is left alone for
// the same reason — the row stays `creating` and the next announce
// reconciles it.
func (s *Server) dispatchCreate(ctx context.Context, row Session, runner string) {
	res, err := s.dispatch(ctx, runner, rwire.ToRunner{
		Type:    "create",
		Session: row.ID,
		Spec: &rwire.Spec{
			Name:        row.Name,
			Image:       row.Image,
			Cmd:         row.Cmd,
			EgressAllow: row.EgressAllow,
		},
	})
	switch {
	case errors.Is(err, ErrDispatchTimeout):
		log.Printf("controld: create %s on %s: no result before the op timeout; leaving it creating for the runner's event or next announce to settle: %v",
			row.ID, runner, err)
	case errors.Is(err, ErrRunnerUnreachable):
		none := ""
		s.transitionQuiet(ctx, row.ID, []SessionState{StateCreating}, StateQueued, TransitionOpts{Runner: &none})
		s.wakeScheduler()
	case err != nil:
		log.Printf("controld: dispatch create %s to %s: %v", row.ID, runner, err)
	case !res.OK:
		detail := res.Detail
		s.transitionQuiet(ctx, row.ID, []SessionState{StateCreating}, StateFailed, TransitionOpts{Error: &detail})
	}
}

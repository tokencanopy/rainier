// internal/runnerd/workspacestream.go
//
// The runner's half of the cold-suspend checkpoint: it asks a guest to cold
// suspend, reads the workspace the guest streams back, and hands the bytes to
// whoever is checkpointing them.
//
// It lives here rather than in the driver for the same reason FlushGuest does:
// the guest's control connection belongs to the session's relay hub, and the
// hub lives in this package. The driver owns the BARRIER — what is done with
// the bytes, and what may not happen until the manifest commits — and calls
// this for the hop it cannot reach.
package runnerd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tokencanopy/rainier/internal/driver"
	"github.com/tokencanopy/rainier/internal/relay"
)

// The four budgets a cold suspend's stream gets. They replace one stopwatch
// with a shape, because the work behind them has a shape: a fixed amount of
// quiescing, then a copy whose length is the workspace's, then an answer.
//
// See docs/design/2026-09-23-workspace-checkpoint-wiring.md §4. Every one is a
// field on the Server (not a constant read here) so a test can drive it in
// milliseconds, exactly as the suspend and flush budgets are.
const (
	// defaultWorkspaceStartWait is from the sandbox's acknowledgement to the
	// first chunk: the flush, the ten-second exec kill and the unmount that
	// #98's cold budget already bought, unchanged.
	defaultWorkspaceStartWait = 30 * time.Second
	// defaultWorkspaceIdleWait is between chunks, and from the last chunk to
	// the end marker. It bounds PROGRESS rather than throughput, and it
	// deliberately covers this host's own slowness too: the stream is
	// back-pressured by whatever is consuming it, so a blob store that has
	// stopped taking bytes stalls the stream and fails the suspend — which
	// keeps the VM, which is the right answer.
	defaultWorkspaceIdleWait = 60 * time.Second
	// defaultWorkspaceTotalWait is the ceiling on the whole copy. The workspace
	// disk is 10 GiB (driver.workspaceDiskBytes); thirty minutes is that at
	// about 6 MiB/s, which is well under what a vsock hop and a local store
	// manage and well over what a stalled one does.
	defaultWorkspaceTotalWait = 30 * time.Minute
)

// workspaceWaiter is one in-flight stream's end marker, matched by nonce. It is
// separate from the suspend waiter that carries the same nonce because the two
// answer different questions — "the tree is out" and "this VM may go" — and the
// host acts between them.
type workspaceWaiter struct {
	nonce uint64
	done  chan struct{}
	once  sync.Once
	// The end marker's own words, written before done is closed and read only
	// after it, so the channel is the synchronization.
	ok      bool
	entries int64
	bytes   int64
	stage   string
	tail    string
}

func (s *Server) armWorkspaceWaiter(id string, nonce uint64) *workspaceWaiter {
	w := &workspaceWaiter{nonce: nonce, done: make(chan struct{})}
	s.workspaceMu.Lock()
	s.workspaces[id] = w
	s.workspaceMu.Unlock()
	return w
}

func (s *Server) disarmWorkspaceWaiter(id string, w *workspaceWaiter) {
	s.workspaceMu.Lock()
	if s.workspaces[id] == w {
		delete(s.workspaces, id)
	}
	s.workspaceMu.Unlock()
}

// deliverWorkspaceEnd wakes this session's stream waiter, and only when the
// NONCE matches the stream that is actually in flight: a late end marker from a
// suspend that already gave up must not tell the next one that a tree it never
// received is complete.
func (s *Server) deliverWorkspaceEnd(id string, ev relay.ControlEvent) {
	s.workspaceMu.Lock()
	w := s.workspaces[id]
	s.workspaceMu.Unlock()
	switch {
	case w == nil:
		return
	case w.nonce != ev.ID:
		log.Printf("session %s: a workspace end marker for %d arrived while %d is in flight; dropping it", id, ev.ID, w.nonce)
	default:
		w.once.Do(func() {
			w.ok, w.entries, w.bytes = ev.OK, ev.Entries, ev.Bytes
			w.stage, w.tail = ev.Stage, ev.Tail
			close(w.done)
		})
	}
}

// StreamWorkspace runs the cold-suspend handshake with sessionID's sandbox and
// copies the workspace it streams back into dst.
//
// It returns when the guest has said the tree is complete AND said it is ready
// to be terminated, and not before: the counts in the end marker are what turn
// "the connection went quiet" into a refused suspend rather than a checkpoint
// of half a tree.
//
// Every failure is an error rather than a shrug, which is the difference
// between this and quiesceExecs. A warm suspend that goes ahead unquiesced
// freezes a container that will be thawed again; a COLD one that goes ahead
// unstreamed terminates a VM whose workspace nobody has a copy of. So a sandbox
// that never registered, a sessiond that predates this vocabulary, a conn that
// died and a guest that is simply too slow all read the same way here: this
// session cannot be checkpointed, so it is not suspended either.
func (s *Server) StreamWorkspace(ctx context.Context, sessionID string, dst io.Writer) (driver.WorkspaceStream, error) {
	var none driver.WorkspaceStream
	hub, ok := s.reg.hub(sessionID)
	if !ok {
		return none, fmt.Errorf("session %s has no sandbox connection to stream its workspace over", sessionID)
	}
	nonce := s.suspendNonce.Add(1)
	sw := s.armSuspendWaiter(sessionID, nonce)
	defer s.disarmSuspendWaiter(sessionID, sw)
	ww := s.armWorkspaceWaiter(sessionID, nonce)
	defer s.disarmWorkspaceWaiter(sessionID, ww)

	// received is written by the sink, which runs on the hub's read loop, and
	// read here. Atomic rather than "ordered by the end marker": the ordering
	// argument is true on the happy path and this value is also read on the
	// paths where it is not, and a counter that is correct only when nothing
	// went wrong is not a counter worth checking a checkpoint against.
	var received atomic.Int64
	progress := make(chan struct{}, 1)
	failed := make(chan error, 1)
	if err := hub.AddStream(nonce, func(chunk []byte) error {
		n, err := dst.Write(chunk)
		received.Add(int64(n))
		if err != nil {
			select {
			case failed <- err:
			default:
			}
			return err
		}
		select {
		case progress <- struct{}{}:
		default:
		}
		return nil
	}); err != nil {
		return none, fmt.Errorf("session %s: %w", sessionID, err)
	}
	defer hub.RemoveStream(nonce)

	b, err := json.Marshal(relay.ControlEvent{Kind: relay.KindSuspending, ID: nonce, Cold: true})
	if err != nil {
		return none, fmt.Errorf("encoding the cold suspend notice for %s: %w", sessionID, err)
	}
	if err := hub.SendControl(b); err != nil {
		return none, fmt.Errorf("asking session %s to cold suspend: %w", sessionID, err)
	}

	// "Did you hear me." A sandbox that predates this vocabulary answers
	// nothing at all, and there is no checkpoint to be had from one — so unlike
	// the warm path, which shrugs and freezes anyway, this is a refusal.
	ackTimer := time.NewTimer(s.suspendAckWait)
	defer ackTimer.Stop()
	select {
	case <-sw.ack:
	case err := <-failed:
		return none, fmt.Errorf("session %s: the workspace stream could not be taken: %w", sessionID, err)
	case <-hub.Done():
		return none, fmt.Errorf("session %s lost its sandbox connection before it acknowledged the cold suspend", sessionID)
	case <-ctx.Done():
		return none, fmt.Errorf("waiting for session %s to acknowledge the cold suspend: %w", sessionID, ctx.Err())
	case <-ackTimer.C:
		return none, fmt.Errorf("session %s did not acknowledge the cold suspend within %s "+
			"(a sandbox that predates the workspace stream never will, and cannot be checkpointed)",
			sessionID, s.suspendAckWait)
	}

	if err := s.readStream(ctx, sessionID, hub, ww, &received, progress, failed); err != nil {
		return none, err
	}
	if !ww.ok {
		// The guest's own refusal, in the stage_failed shape: a stage and a
		// tail, with counts and no path. Both strings are the SANDBOX's, so
		// both are bounded here before they travel any further — sessiond is
		// careful about what it puts in them (see its streamFailureTail), and
		// this hop is where that stops being something to rely on.
		return none, fmt.Errorf("session %s could not stream its workspace (stage %s, %d entries and %d bytes in): %s",
			sessionID, clampGuestText(ww.stage), ww.entries, ww.bytes, clampGuestText(ww.tail))
	}
	if got := received.Load(); ww.bytes != got {
		// The one check only this hop can make: the guest says how much it
		// wrote, this end knows how much arrived, and a difference is a stream
		// that lost bytes somewhere between them.
		return none, fmt.Errorf("session %s streamed %d bytes of workspace and %d arrived; "+
			"the stream is not complete and nothing may be checkpointed from it", sessionID, ww.bytes, got)
	}

	// And "you may go". It follows the end marker immediately on the same
	// ordered conn, so this is almost always already satisfied.
	readyTimer := time.NewTimer(s.coldSuspendReadyWait)
	defer readyTimer.Stop()
	select {
	case <-sw.ready:
	case <-hub.Done():
		// The conn died after the whole tree and the end marker arrived. The
		// stream is complete and checked, and a guest that cannot say a last
		// word is not a reason to throw away a workspace that is already out of
		// it — the VM is being terminated either way.
		log.Printf("session %s: the sandbox connection ended after its workspace stream but before it reported ready; continuing", sessionID)
	case <-ctx.Done():
		return none, fmt.Errorf("waiting for session %s to report ready after its workspace stream: %w", sessionID, ctx.Err())
	case <-readyTimer.C:
		log.Printf("session %s: the sandbox streamed its whole workspace but did not report ready within %s; continuing",
			sessionID, s.coldSuspendReadyWait)
	}
	return driver.WorkspaceStream{Entries: ww.entries, Bytes: received.Load(), Nonce: nonce}, nil
}

// maxGuestText bounds a string the sandbox chose before this runner puts it in
// an error. The frame limit above it is 16 MiB, which is not a bound for
// something that reaches an operator's log and a session's error column.
const maxGuestText = 512

// clampGuestText truncates a sandbox-supplied string and says that it did.
func clampGuestText(s string) string {
	if len(s) <= maxGuestText {
		return s
	}
	return s[:maxGuestText] + "… (truncated)"
}

// readStream waits for the end marker under the three budgets: one for the
// stream to START, one for it to keep MOVING, and one for the whole of it.
//
// The idle budget is reset by progress rather than by arrival count, so a
// steady copy of a large workspace never touches it and a stalled one always
// does, whichever end stalled.
func (s *Server) readStream(ctx context.Context, sessionID string, hub *relay.Hub, ww *workspaceWaiter,
	received *atomic.Int64, progress <-chan struct{}, failed <-chan error) error {
	total := time.NewTimer(s.workspaceTotalWait)
	defer total.Stop()
	quiet := time.NewTimer(s.workspaceStartWait)
	defer quiet.Stop()
	started := false

	for {
		select {
		case <-ww.done:
			return nil
		case <-progress:
			if !started {
				started = true
			}
			quiet.Reset(s.workspaceIdleWait)
		case err := <-failed:
			// This host stopped taking the bytes: the checkpoint writer failed,
			// or the store did. Named as what it is, so that a store outage is
			// not reported as a guest that would not stream.
			return fmt.Errorf("session %s: the workspace stream could not be taken: %w", sessionID, err)
		case <-hub.Done():
			return fmt.Errorf("session %s lost its sandbox connection part way through its workspace stream "+
				"(%d bytes in); nothing may be checkpointed from it", sessionID, received.Load())
		case <-ctx.Done():
			return fmt.Errorf("streaming session %s's workspace: %w", sessionID, ctx.Err())
		case <-total.C:
			return fmt.Errorf("session %s was still streaming its workspace after %s", sessionID, s.workspaceTotalWait)
		case <-quiet.C:
			if !started {
				return fmt.Errorf("session %s did not begin streaming its workspace within %s of acknowledging the cold suspend",
					sessionID, s.workspaceStartWait)
			}
			return fmt.Errorf("session %s's workspace stream stopped making progress for %s", sessionID, s.workspaceIdleWait)
		}
	}
}

// CheckpointCommitted tells sessionID's guest that its workspace is durable.
//
// Best effort, and deliberately so: the guest acts on nothing and answers
// nothing, the VM is terminated moments later, and the value of the event is
// that the session's own log says whether its work made it out. A failure here
// is not a failed suspend — the manifest is already committed and verified.
//
// nonce is the suspend's, so a session's log ties the line to the stream it
// wrote rather than to "some checkpoint".
func (s *Server) CheckpointCommitted(sessionID string, nonce uint64) {
	hub, ok := s.reg.hub(sessionID)
	if !ok {
		return
	}
	b, err := json.Marshal(relay.ControlEvent{Kind: relay.KindCheckpointCommitted, ID: nonce})
	if err != nil {
		return
	}
	if err := hub.SendControl(b); err != nil {
		log.Printf("session %s: telling the sandbox its checkpoint committed: %v", sessionID, err)
	}
}

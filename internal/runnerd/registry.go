// internal/runnerd/registry.go
package runnerd

import (
	"slices"
	"sync"
	"time"

	"github.com/tokencanopy/rainier/internal/relay"
)

type sessionEntry struct {
	id     string
	handle string // driver handle id
	state  string
	allow  []string
	// envKeys are the NAMES of the variables this session's create injected
	// (Spec.Env) — never their values, which are an environment's decrypted
	// secrets and have no business being held anywhere but the container.
	// Recorded because a snapshot has to name them to keep the commit from
	// baking them into an image (see OpSnapshot and driver.Driver.Snapshot),
	// and by then the Spec is long gone. Sorted at capture, so a snapshot's
	// strip list is the same on every call.
	envKeys []string
	// placementGen is the session placement generation the create that
	// started this sandbox carried (protocol/runner: ToRunner.PlacementGeneration).
	// It is kept beside the sandbox because every event about the session
	// echoes it, which is what lets controld fence a report from a sandbox
	// the session has since been re-placed away from. Zero is "the create
	// carried none" — an old controld — and fences nothing.
	placementGen uint64
	hub          *relay.Hub // set when sessiond registers; nil until then
	// attachments is the number of attachments currently open over this
	// session's hub — viewers and controllers alike, since a runner neither
	// knows nor needs to know which of them holds the controller lease.
	// Maintained around BOTH attach fronts (the local /attach handler and the
	// agent's dial_attach dial-back), because a session with a live viewer is
	// not idle no matter which door that viewer came through.
	attachments int
	// childExitedAt is when sessiond reported child_exited for the sandbox
	// boot this entry is currently on. Zero means "the child is running, as
	// far as this runner has been told" — which is also what a session
	// rebuilt by Recover looks like, since the exit was only ever in memory.
	// Zero is the safe value in both cases: idle auto-stop never stops a
	// session whose child it has not been told has exited.
	childExitedAt time.Time
	// lastDetachAt is when the most recent attachment over this session
	// ended. It is what makes the idle timer run from the moment the last
	// viewer left rather than from the child's exit: someone reading a
	// finished agent's scrollback for an hour resets nothing while attached
	// and gets the full timeout after detaching.
	lastDetachAt time.Time
	// boot identifies the sandbox boot this entry is currently on: a counter
	// bumped by every fresh /register and by every cold resume. It exists
	// because a control frame can outlive the boot that sent it — a hub's
	// read loop that was stalled writing to a wedged viewer drains its
	// buffered frames whenever it comes back, which can be after the sandbox
	// has been stopped, resumed, and re-registered. A child_exited from the
	// PREVIOUS boot landing on the current one would make this runner believe
	// a working agent had finished, and stop the session out from under it —
	// the one thing idle auto-stop must never do. Same guard, same reason, as
	// hubDied's deadHub parameter.
	boot uint64
	// driverOps counts driver operations this runner has dispatched for the
	// session and not yet finished: a warm suspend, a resume, a snapshot.
	// A session with one in flight is never idle, because the entry still
	// reads "running" for the whole of `docker pause` or `docker commit`, and
	// a sweep that claimed it in that window would stop a container somebody
	// else is mid-operation on — turning an operator's pause into a stop, or
	// killing a commit halfway. The cold suspend and Delete don't need this:
	// each marks the state before its driver call.
	driverOps int
	// bootFailed records that this session's boot chain failed (setup, clone
	// or init). Such a session is deliberately never idle-stopped: the whole
	// reason the CLI lets you attach to a failed session is to read the log
	// that says why it failed, and a stopped sandbox has no hub to attach to
	// and a failed row cannot be resumed. It holds its slot until someone
	// removes it — the honest trade, and reclaiming those is part of the
	// resource-aware admission this change's design doc defers to.
	bootFailed bool
}

// idleFor reports how long this session has been idle at now, and whether it
// is idle at all: its container is up, its child has exited, and nothing is
// attached. Callers must hold the registry lock.
//
// The clock is the caller's, and both times come from it, so in production
// both carry Go's monotonic reading and this subtraction uses it — a wall
// clock stepped by NTP can neither make a session look idle early nor keep
// one from ever looking idle.
//
// The idle clock starts at the LATER of the child's exit and the last
// detach, which is the whole reason lastDetachAt is kept.
func (e *sessionEntry) idleFor(now time.Time) (time.Duration, bool) {
	if e.state != "running" || e.attachments > 0 || e.childExitedAt.IsZero() {
		return 0, false
	}
	if e.driverOps > 0 || e.bootFailed {
		return 0, false
	}
	since := e.childExitedAt
	if e.lastDetachAt.After(since) {
		since = e.lastDetachAt
	}
	return now.Sub(since), true
}

type registry struct {
	mu    sync.Mutex
	items map[string]*sessionEntry
	// nextBoot mints sessionEntry.boot values. Monotonic across the whole
	// registry rather than per entry so that a value can never be reused by a
	// session id that was deleted and recreated, which is exactly the case a
	// stale frame would otherwise be accepted in.
	nextBoot uint64
}

func newRegistry() *registry { return &registry{items: map[string]*sessionEntry{}} }

func (r *registry) put(id string, e *sessionEntry) { r.mu.Lock(); r.items[id] = e; r.mu.Unlock() }

// putIfAbsent inserts e under id only if no entry exists yet, as a single
// locked check-and-reserve step. CreateWithID uses this (not a separate
// get()-then-put() pair) as its very first action so a concurrent caller
// racing the same id can only ever find the id already claimed, never
// observe "absent" itself — closing the idempotent-create TOCTOU where two
// racing create commands for the same id (e.g. a retried create the sender
// is unsure landed) could otherwise both pass an existence check and both
// reach drv.Create, producing a duplicate/orphaned container (review round
// 1, finding 2). Reports whether it inserted (true) or found an existing
// entry and left it untouched (false).
func (r *registry) putIfAbsent(id string, e *sessionEntry) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.items[id]; exists {
		return false
	}
	r.items[id] = e
	return true
}

func (r *registry) get(id string) (*sessionEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.items[id]
	return e, ok
}
func (r *registry) remove(id string) { r.mu.Lock(); delete(r.items, id); r.mu.Unlock() }

// snapshot returns a value copy of one entry, taken under the lock, for
// callers that need to read the two mutable fields (state, hub) together and
// consistently — exactly what list() gives, for a single id. get() returns
// the live pointer, so reading those fields off it races setHub/setState; see
// list()'s doc comment for why that is a real race and not a hypothetical.
func (r *registry) snapshot(id string) (sessionEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.items[id]
	if !ok {
		return sessionEntry{}, false
	}
	return *e, true
}

// list snapshots every entry (value copies, not the live pointers) under a
// single critical section. That matters because id/handle/allow are set once
// at put() and never mutated again, but hub and state are: a caller ranging
// over live *sessionEntry pointers after list() has already unlocked would
// read those two fields with no synchronization at all against setHub/
// setState's locked writes — a real data race, not a hypothetical one, since
// register (setHub) and attach's poll loop, or two sessionOp requests and a
// concurrent GET /sessions, run on different goroutines. Copying under the
// lock makes every returned snapshot internally consistent and race-free.
func (r *registry) list() []sessionEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]sessionEntry, 0, len(r.items))
	for _, e := range r.items {
		out = append(out, *e)
	}
	return out
}

// setHub reports whether the entry still existed to receive the hub. It can
// return false when a concurrent DELETE removed the entry between register's
// existence check and this call (session deleted while its container was
// still booting/dialing in) — the caller has a live hub with a running
// readLoop goroutine that will now never be found through the registry, so
// it must close that hub itself instead of leaking it.
func (r *registry) setHub(id string, h *relay.Hub) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.items[id]
	if !ok {
		return false
	}
	e.hub = h
	return true
}

// hub reads an entry's hub field under the registry lock. attach's
// registration-wait loop must go through this (rather than calling get() and
// then dereferencing the returned pointer's .hub field itself) because that
// unlocked field read would race against setHub's locked write from the
// concurrent /register handler.
func (r *registry) hub(id string) (*relay.Hub, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.items[id]
	if !ok || e.hub == nil {
		return nil, false
	}
	return e.hub, true
}

// restoreAfterFailedDestroy puts an entry back where Delete found it when the
// driver teardown failed — but only if the entry is still the one Delete
// marked. The same compare-and-swap discipline as releaseColdSuspend, and for
// the mirror of its reason: a cold suspend that landed while drv.Destroy was in
// flight has already moved the entry to "suspended", and stamping the
// pre-Delete state back over it would claim a container is running that this
// runner has just stopped.
func (r *registry) restoreAfterFailedDestroy(id, previousState string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.items[id]; ok && e.state == "destroying" {
		e.state = previousState
	}
}

// setState updates an entry's state under the registry lock — sessionOp's
// suspend/resume handlers and the GET /sessions listing (via list() above)
// both touch e.state from different request goroutines, so it needs the same
// protection as hub.
func (r *registry) setState(id, state string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.items[id]; ok {
		e.state = state
	}
}

// opTarget returns an entry's driver handle and state under the registry
// lock, for sessionOp's DELETE/suspend/resume/snapshot handlers. Those used
// to call get() and then read the returned pointer's .handle field directly
// (unsynchronized against setHandle's locked write — a real data race once
// handle became a post-put field, not merely a hypothetical one: setHandle
// runs on the POST /sessions goroutine while sessionOp runs on a concurrent
// request goroutine). Callers must also check state: a "starting" entry
// (handle == "") means Create hasn't returned yet, so there is nothing yet
// for a driver call to act on — see sessionOp's http.StatusConflict guard.
func (r *registry) opTarget(id string) (handle, state string, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.items[id]
	if !ok {
		return "", "", false
	}
	return e.handle, e.state, true
}

// envKeys returns the names of the variables a session's create injected, or
// nil for a session that had none (and for one that no longer exists — a
// snapshot of a session that just vanished has nothing to strip, and its
// commit is about to fail on the missing container anyway).
//
// Read under the lock like every other entry field, even though envKeys is set
// once at putIfAbsent and never mutated: the whole registry's rule is that no
// caller dereferences a live *sessionEntry, and one field quietly exempting
// itself is how the next field to become mutable acquires a silent race. The
// clone is for the same reason — a caller must not be able to alias, or sort,
// the registry's own slice.
func (r *registry) envKeys(id string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.items[id]
	if !ok {
		return nil
	}
	return slices.Clone(e.envKeys)
}

// placementGeneration returns the placement generation an entry's create
// carried, or zero for a session this runner does not (or no longer) holds —
// the same answer an old controld's create leaves behind, and zero fences
// nothing either way. Read under the lock like every other entry field.
func (r *registry) placementGeneration(id string) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.items[id]
	if !ok {
		return 0
	}
	return e.placementGen
}

// hubDied is called when a session's relay hub dies (its conn closed). It
// always clears the entry's hub (the connection is dead either way — a stale
// hub left in place only blocks a fresh register from installing its
// replacement) but, unlike the onHubDeath it replaces, never removes the
// entry itself: hub death no longer implies container death now that
// sessiond survives conn loss and redials (see cmd/sessiond's dialLoop), so
// deciding whether the container is actually gone means asking the driver —
// the caller's job, via Inspect, done AFTER this returns. See
// removeIfHubless for the other half of that decision.
//
// The suspend-state normalization is unchanged from onHubDeath: a deliberate
// cold suspend (`docker stop`) kills the container's sessiond too, closing
// this same conn — indistinguishable at the socket level from a crash or a
// conn drop. state "suspending" or "suspended" here means "keep, and land on
// suspended" so a later resume's re-register can setHub a fresh hub onto the
// same entry, rather than falling into the caller's Inspect-then-destroy
// path.
//
// Delete's "destroying" marker is deliberately NOT normalized here: it is
// returned verbatim so the caller can stand down entirely (no Inspect, no
// destroy, no "dead" event) and leave the teardown to the Delete that is
// already running. See Delete and register's hub-death tail.
//
// deadHub guards against a redial that already installed a fresh hub before
// this call runs (this goroutine is for the OLD conn, running after a newer
// one has already re-registered): if the entry's current hub isn't the one
// that just died, this is stale and must not touch the entry at all — ok
// reports that.
func (r *registry) hubDied(id string, deadHub *relay.Hub) (handle, state string, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, exists := r.items[id]
	if !exists || e.hub != deadHub {
		return "", "", false
	}
	e.hub = nil
	if e.state == "suspending" || e.state == "suspended" {
		e.state = "suspended"
	}
	return e.handle, e.state, true
}

// removeIfHubless removes the entry only if it still has no hub installed —
// the guard that makes register()'s inspect-then-remove tail safe against a
// redial that raced ahead of the driver.Inspect call and already set a fresh
// hub on this entry (in which case removing now would delete a session that
// just came back, out from under its new hub). Only call after Inspect has
// confirmed the container is actually gone. Reports whether it actually
// removed the entry, so the caller knows it — not some other, later
// register() call — owns destroying the container.
func (r *registry) removeIfHubless(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.items[id]
	if !ok || e.hub != nil {
		return false
	}
	delete(r.items, id)
	return true
}

// setHandle assigns an entry's driver handle id once its container has
// actually started. POST /sessions now calls put() with a handle-less entry
// before it calls the driver's Create — a real container's sessiond can dial
// /register the instant `docker run -d` returns, which can race ahead of a
// put() that used to follow Create() (that /register would 404 on a session
// it can't find yet, and sessiond treats a non-101 dial response as fatal
// and exits — see cmd/sessiond). setHandle fills in the handle afterward
// under the same lock as every other post-creation mutation.
func (r *registry) setHandle(id, handle string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.items[id]; ok {
		e.handle = handle
	}
}

// attachStarted records that an attachment has opened over id's hub. A
// session with any attachment open is not idle, however long its child has
// been gone. An unknown id is a no-op: the session was deleted out from under
// an attach that was already in flight, and there is nothing left to account
// against.
func (r *registry) attachStarted(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.items[id]; ok {
		e.attachments++
	}
}

// attachEnded records that an attachment over id's hub has closed, at time
// at. The idle clock starts here, not at the child's exit — see
// sessionEntry.lastDetachAt.
//
// The counter floors at zero rather than going negative: every caller pairs
// this with attachStarted through a defer, but an entry that was recreated
// under the same id between the two (a delete and a create of the same
// controld-supplied id) would otherwise leave a permanently negative count on
// the new entry, which reads as "fewer than no attachments" and would be
// indistinguishable from an attached session forever after.
// pumped says whether this attachment ever became one — whether it got as far
// as bridging the client to the session. A handshake that failed (the upgrade,
// or the first client frame that never came) still decrements the count, which
// is what keeps a dropped dial from pinning a session open, but must NOT move
// the idle clock: the local /attach surface has no authentication, so a client
// looping on a failed dial once a minute would otherwise push every deadline on
// this runner out indefinitely, and nothing would say why.
func (r *registry) attachEnded(id string, at time.Time, pumped bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.items[id]
	if !ok {
		return
	}
	if e.attachments > 0 {
		e.attachments--
	}
	if pumped {
		e.lastDetachAt = at
	}
}

// childExited records that sessiond reported this session's child process
// ended, at time at. It is the ONLY thing that makes a session a candidate for
// idle auto-stop: a runner never infers that a child is finished, it is told.
//
// Recorded once per sandbox boot. A repeat report (sessiond re-delivering an
// event it was unsure landed) must not move the idle clock forward, or a
// retried delivery would silently extend the timeout.
func (r *registry) childExited(id string, boot uint64, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.items[id]
	if !ok || e.boot != boot {
		// Not this sandbox's boot: a frame from a connection the session has
		// since moved on from. Dropping it is the safe direction in the only
		// way that matters — the worst case is an auto-stop this runner never
		// makes, against a worst case of stopping a session whose agent is
		// working. See sessionEntry.boot.
		return
	}
	if e.childExitedAt.IsZero() {
		e.childExitedAt = at
	}
}

// currentBoot returns the sandbox-boot epoch id is on, which register READS
// (rather than mints) and every control frame from that connection then
// carries. Zero for a session that has never been cold-resumed, and for one
// this registry does not hold — a frame for a session that has been deleted
// matches nothing either way.
//
// Reading rather than minting is the whole point, and it took a review round
// to get right. A new epoch per REGISTRATION would open one on a plain
// sessiond redial too, where the container never restarted and the child never
// changed — and sessiond re-sends only events it never delivered, so a
// child_exited already in flight across that redial would be dropped here and
// never sent again. That session then holds its slot for the life of the
// runner, silently: the very incident this feature exists to end. Only a
// restart of the container can change the child, only a cold resume restarts
// it, and resumed() is where the epoch therefore moves.
func (r *registry) currentBoot(id string) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.items[id]; ok {
		return e.boot
	}
	return 0
}

// markBootFailed records that this session's boot chain failed, which takes it
// out of idle auto-stop until it is cold-resumed — see sessionEntry.bootFailed.
// Boot-guarded exactly like childExited: a stage failure that outlived its boot
// describes a sandbox this session has already left, and letting it pin a
// healthy one out of auto-stop would be the same lost slot by another route.
func (r *registry) markBootFailed(id string, boot uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.items[id]; ok && e.boot == boot {
		e.bootFailed = true
	}
}

// beginOp and endOp bracket a driver operation dispatched for id, so a sweep
// cannot claim a session that is mid-`docker pause`, mid-`docker start` or
// mid-`docker commit`. Paired through a defer by every caller; an unknown id
// is a no-op at both ends, so a session deleted mid-operation leaves nothing
// behind.
func (r *registry) beginOp(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.items[id]; ok {
		e.driverOps++
	}
}

func (r *registry) endOp(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.items[id]; ok && e.driverOps > 0 {
		e.driverOps--
	}
}

// counts returns the two numbers a runner's capacity line needs beyond
// used/total: how many of its sandboxes are up with a child still running,
// and how many are up with the child gone. Only "running" entries are counted:
// a warm-suspended sandbox and one still being created each hold a slot and
// are in neither count, which is the honest answer rather than a made-up one.
//
// So the two normally sum to less than `used`, not more — but not
// invariably: register's hub-death tail deliberately KEEPS an entry as
// "running" when the driver cannot say whether its container survived
// (destroying on that uncertainty is the catastrophic direction), and such an
// entry is counted here while `docker ps` no longer counts it. A consumer of
// these numbers must treat them as a split of what this runner believes it
// holds, not as an arithmetic partition of `used`.
//
// idleExited also includes a session whose BOOT failed, which idle auto-stop
// deliberately never reclaims (see sessionEntry.bootFailed). The count says
// what is up with no agent in it, which is the truth about the machine; it is
// not a promise about what the sweep will hand back.
func (r *registry) counts() (active, idleExited int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.items {
		if e.state != "running" {
			continue
		}
		if e.childExitedAt.IsZero() {
			active++
			continue
		}
		idleExited++
	}
	return active, idleExited
}

// idleSessions returns the ids that have been idle for at least idle at now,
// in stable (id) order. It is the sweep's candidate list, not its decision:
// the decision is claimIdle, which re-checks the same rule under the lock it
// marks the entry in. Sorted so a sweep that stops several sessions logs them
// in an order that does not depend on map iteration.
func (r *registry) idleSessions(idle time.Duration, now time.Time) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for id, e := range r.items {
		if d, ok := e.idleFor(now); ok && d >= idle {
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return out
}

// claimIdle re-checks the idle rule for id and, in the SAME critical section,
// marks the entry "suspending" and cold — the claim. It returns the driver
// handle to stop and how long the session had been idle when it was claimed.
//
// Check and mark cannot be separated. If they were, two sweeps (or a sweep and
// a stop arriving from controld) could both decide to stop one session, and a
// sweep could stop a session that a concurrent Delete had already begun tearing
// down. One locked call means at most one caller ever gets a handle back.
//
// "suspending" is exactly the marker Op's cold suspend sets, and for the same
// reason: `docker stop` kills the container's sessiond, closing the /register
// conn, and the register goroutine must read that as a deliberate stop rather
// than a crash — otherwise it destroys the container this is only trying to
// park.
func (r *registry) claimIdle(id string, idle time.Duration, now time.Time) (handle string, idleFor time.Duration, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, exists := r.items[id]
	if !exists {
		return "", 0, false
	}
	d, isIdle := e.idleFor(now)
	if !isIdle || d < idle {
		return "", 0, false
	}
	e.state = "suspending"
	return e.handle, d, true
}

// beginColdSuspend marks an entry for the cold suspend Op is about to run:
// the "suspending" state and the cold flag, in one lock, for the reasons
// claimIdle spells out. It is claimIdle's unconditional sibling — an
// operator's stop asks no questions about idleness.
// It leaves a "destroying" entry alone, for the same reason releaseColdSuspend
// does: a Delete that got there first owns the entry, and overwriting its
// marker is what makes the register goroutine destroy the container a second
// time and report the session dead instead of destroyed.
func (r *registry) beginColdSuspend(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.items[id]; ok && e.state != "destroying" {
		e.state = "suspending"
	}
}

// releaseColdSuspend rolls a claimed-but-failed cold suspend back to running:
// the container was never stopped, so the entry must not be left claiming it
// was cold-parked. The next sweep (or the operator's retry) will try again.
//
// It moves the entry ONLY if it is still the one this stop claimed. A Delete
// that arrived while `docker stop` was in flight has already marked the entry
// "destroying" — the marker that makes the register goroutine stand down
// instead of destroying the container a second time and reporting the session
// dead — and an unconditional rollback to "running" would wipe it, which is
// precisely the outcome Delete's marker exists to prevent.
func (r *registry) releaseColdSuspend(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.items[id]; ok && e.state == "suspending" {
		e.state = "running"
	}
}

// finishColdSuspend lands a successful cold suspend on "suspended", with the
// same compare-and-swap discipline and for the same reason as
// releaseColdSuspend: a Delete that overtook this stop owns the entry now.
func (r *registry) finishColdSuspend(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.items[id]; ok && e.state == "suspending" {
		e.state = "suspended"
	}
}

// resumed lands an entry back on "running" after a successful driver resume,
// and clears the idle bookkeeping if — and only if — the driver reports that it
// RESTARTED the container.
//
// That condition is the whole point, and it must come from the driver rather
// than from what this runner intended. `docker start` gives the container a new
// process tree, so the child is a new one and the old child's exit says nothing
// about it; keeping the fact would let the next sweep stop a session whose
// agent is working, which is the one thing idle auto-stop must never do.
// `docker unpause` restarts nothing, so a child that had exited is still exited
// and the fact must survive, or a warm-cycled session would never be
// auto-stopped again.
//
// An earlier version keyed this on a flag set when the runner ASKED for a cold
// suspend, and the two come apart in both directions. A warm-paused container
// that the docker daemon restarts underneath a surviving runnerd (a daemon
// upgrade, an OOM kill) is resumed with `docker start` while the flag reads
// "paused" — a new agent inheriting the old one's exit, stopped half an hour
// later. And an entry left marked cold by a stop whose outcome could not be
// read is resumed by an `unpause` or by nothing at all, while the epoch bump
// deafens the connection that is still live — its child's exit dropped, its
// slot never reclaimed. Only the driver knows which call it made; see
// driver.Driver.Resume.
func (r *registry) resumed(id string, restarted bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.items[id]
	if !ok {
		return
	}
	// Read before it is overwritten: only the resume that actually brings a
	// PARKED entry back can be the one that restarted it. `docker start` on an
	// an already-started container exits 0, so a second resume reports a
	// restart just as honestly as the first — and two are a real shape, since
	// the control plane dispatches the command before it transitions the row,
	// so two racing clients both send one. Without this, the second bumps the
	// epoch again; if the restarted sandbox registered in between, that bump
	// moves the epoch out from under a connection that is already live, and
	// its child's exit is dropped — the session's slot never comes back, and
	// nothing says why. Consuming the transition is what the intent flag used
	// to give for free, and this keeps it while the answer comes from the
	// driver.
	wasParked := e.state != "running"
	e.state = "running"
	if !restarted || !wasParked {
		return
	}
	e.childExitedAt = time.Time{}
	e.lastDetachAt = time.Time{}
	// The new boot runs the whole chain again (setup, clone, init), so a
	// previous boot's failure says nothing about it.
	e.bootFailed = false
	// And close the old boot, so a control frame still in flight from the
	// sandbox that has just been restarted cannot land its child's exit on
	// the new one. The register that is about to arrive opens the next
	// epoch; until it does, frames from either side of the restart match
	// nothing. See sessionEntry.boot.
	r.nextBoot++
	e.boot = r.nextBoot
}

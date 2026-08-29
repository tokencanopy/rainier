// internal/runnerd/registry.go
package runnerd

import (
	"slices"
	"sync"

	"rainier/internal/relay"
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
	hub     *relay.Hub // set when sessiond registers; nil until then
}

type registry struct {
	mu    sync.Mutex
	items map[string]*sessionEntry
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

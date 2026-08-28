// internal/runnerd/registry.go
package runnerd

import (
	"sync"

	"rainier/internal/relay"
)

type sessionEntry struct {
	id     string
	handle string // driver handle id
	state  string
	allow  []string
	hub    *relay.Hub // set when sessiond registers; nil until then
}

type registry struct {
	mu    sync.Mutex
	items map[string]*sessionEntry
}

func newRegistry() *registry { return &registry{items: map[string]*sessionEntry{}} }

func (r *registry) put(id string, e *sessionEntry) { r.mu.Lock(); r.items[id] = e; r.mu.Unlock() }
func (r *registry) get(id string) (*sessionEntry, bool) {
	r.mu.Lock(); defer r.mu.Unlock()
	e, ok := r.items[id]; return e, ok
}
func (r *registry) remove(id string) { r.mu.Lock(); delete(r.items, id); r.mu.Unlock() }

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
	r.mu.Lock(); defer r.mu.Unlock()
	out := make([]sessionEntry, 0, len(r.items))
	for _, e := range r.items { out = append(out, *e) }
	return out
}
// setHub reports whether the entry still existed to receive the hub. It can
// return false when a concurrent DELETE removed the entry between register's
// existence check and this call (session deleted while its container was
// still booting/dialing in) — the caller has a live hub with a running
// readLoop goroutine that will now never be found through the registry, so
// it must close that hub itself instead of leaking it.
func (r *registry) setHub(id string, h *relay.Hub) bool {
	r.mu.Lock(); defer r.mu.Unlock()
	e, ok := r.items[id]
	if !ok { return false }
	e.hub = h
	return true
}

// hub reads an entry's hub field under the registry lock. attach's
// registration-wait loop must go through this (rather than calling get() and
// then dereferencing the returned pointer's .hub field itself) because that
// unlocked field read would race against setHub's locked write from the
// concurrent /register handler.
func (r *registry) hub(id string) (*relay.Hub, bool) {
	r.mu.Lock(); defer r.mu.Unlock()
	e, ok := r.items[id]
	if !ok || e.hub == nil { return nil, false }
	return e.hub, true
}

// setState updates an entry's state under the registry lock — sessionOp's
// suspend/resume handlers and the GET /sessions listing (via list() above)
// both touch e.state from different request goroutines, so it needs the same
// protection as hub.
func (r *registry) setState(id, state string) {
	r.mu.Lock(); defer r.mu.Unlock()
	if e, ok := r.items[id]; ok { e.state = state }
}

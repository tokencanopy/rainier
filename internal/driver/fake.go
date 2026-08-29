// internal/driver/fake.go
package driver

import (
	"context"
	"fmt"
	"sync"
)

// fakeItem tracks one fake container's session id, externally-visible state,
// and — only meaningful while state == StateSuspended — whether that suspend
// was cold (stop) or warm (pause). cold is what lets Capacity count warm
// suspends as slot-occupying while excluding cold ones, mirroring the real
// Docker driver's running+paused vs stopped distinction.
type fakeItem struct {
	sessionID string
	state     State
	cold      bool
}

type Fake struct {
	mu      sync.Mutex
	total   int
	seq     int
	snapSeq int // per-call uniqueness for Snapshot refs — mirrors Docker's snapSeq
	items   map[string]*fakeItem
	// lastSpec is the most recent Spec passed to Create, verbatim — for
	// tests asserting fields (e.g. ProxyURL) that fakeItem doesn't track
	// per-container, since Fake otherwise only models the lifecycle states
	// the contract suite exercises, not every Spec field.
	lastSpec Spec
}

func NewFake(total int) *Fake { return &Fake{total: total, items: map[string]*fakeItem{}} }

// LastSpec returns the Spec passed to the most recent Create call.
func (f *Fake) LastSpec() Spec {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastSpec
}

// usedLocked counts slot-occupying items — running or warm-suspended — the
// same semantic Capacity reports. Callers must hold f.mu.
func (f *Fake) usedLocked() int {
	used := 0
	for _, it := range f.items {
		if it.state == StateRunning || (it.state == StateSuspended && !it.cold) {
			used++
		}
	}
	return used
}

func (f *Fake) Create(_ context.Context, spec Spec) (Handle, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	used := f.usedLocked()
	if used >= f.total {
		return Handle{}, fmt.Errorf("no capacity: %d/%d", used, f.total)
	}
	f.lastSpec = spec
	f.seq++
	id := fmt.Sprintf("fake-%d", f.seq)
	f.items[id] = &fakeItem{sessionID: spec.SessionID, state: StateRunning}
	return Handle{ID: id, State: StateRunning}, nil
}
func (f *Fake) Suspend(_ context.Context, id string, warm bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	it, ok := f.items[id]
	if !ok {
		return fmt.Errorf("no such id %s", id)
	}
	it.state = StateSuspended
	it.cold = !warm
	return nil
}
func (f *Fake) Resume(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	it, ok := f.items[id]
	if !ok {
		return fmt.Errorf("no such id %s", id)
	}
	it.state = StateRunning
	it.cold = false
	return nil
}
func (f *Fake) Snapshot(_ context.Context, id string) (Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.items[id]; !ok {
		return Snapshot{}, fmt.Errorf("no such id %s", id)
	}
	f.snapSeq++
	return Snapshot{Ref: "fake-image:" + id + "-" + fmt.Sprint(f.snapSeq)}, nil
}
func (f *Fake) Destroy(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.items, id)
	return nil
}
func (f *Fake) Inspect(_ context.Context, id string) (Handle, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	it, ok := f.items[id]
	if !ok {
		return Handle{ID: id, State: StateGone}, nil
	}
	return Handle{ID: id, State: it.state}, nil
}
func (f *Fake) Capacity(_ context.Context) (int, int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.usedLocked(), f.total, nil
}
func (f *Fake) List(_ context.Context) ([]Listed, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Listed, 0, len(f.items))
	for id, it := range f.items {
		out = append(out, Listed{SessionID: it.sessionID, Handle: Handle{ID: id, State: it.state}})
	}
	return out, nil
}

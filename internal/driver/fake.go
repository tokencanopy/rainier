// internal/driver/fake.go
package driver

import (
	"context"
	"fmt"
	"sync"
)

type Fake struct {
	mu    sync.Mutex
	total int
	seq   int
	items map[string]State
}

func NewFake(total int) *Fake { return &Fake{total: total, items: map[string]State{}} }

func (f *Fake) Create(_ context.Context, spec Spec) (Handle, error) {
	f.mu.Lock(); defer f.mu.Unlock()
	used := len(f.items)
	if used >= f.total { return Handle{}, fmt.Errorf("no capacity: %d/%d", used, f.total) }
	f.seq++
	id := fmt.Sprintf("fake-%d", f.seq)
	f.items[id] = StateRunning
	return Handle{ID: id, State: StateRunning}, nil
}
func (f *Fake) set(id string, st State) error {
	f.mu.Lock(); defer f.mu.Unlock()
	if _, ok := f.items[id]; !ok { return fmt.Errorf("no such id %s", id) }
	f.items[id] = st
	return nil
}
func (f *Fake) Suspend(_ context.Context, id string, warm bool) error { return f.set(id, StateSuspended) }
func (f *Fake) Resume(_ context.Context, id string) error             { return f.set(id, StateRunning) }
func (f *Fake) Snapshot(_ context.Context, id string) (Snapshot, error) {
	f.mu.Lock(); defer f.mu.Unlock()
	if _, ok := f.items[id]; !ok { return Snapshot{}, fmt.Errorf("no such id %s", id) }
	return Snapshot{Ref: "fake-image:" + id}, nil
}
func (f *Fake) Destroy(_ context.Context, id string) error {
	f.mu.Lock(); defer f.mu.Unlock()
	delete(f.items, id)
	return nil
}
func (f *Fake) Inspect(_ context.Context, id string) (Handle, error) {
	f.mu.Lock(); defer f.mu.Unlock()
	st, ok := f.items[id]
	if !ok { return Handle{ID: id, State: StateGone}, nil }
	return Handle{ID: id, State: st}, nil
}
func (f *Fake) Capacity(_ context.Context) (int, int, error) {
	f.mu.Lock(); defer f.mu.Unlock()
	return len(f.items), f.total, nil
}

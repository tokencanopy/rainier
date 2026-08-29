// internal/driver/fake.go
package driver

import (
	"context"
	"fmt"
	"maps"
	"slices"
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
	// volume is the workspace volume name this item holds, "" for a spec with
	// no session id (the docker driver mounts none in that case either).
	// Recorded so Destroy can release it — the fake's stand-in for `docker
	// volume rm`.
	volume string
	// env is a copy of Spec.Env as passed to Create, so tests can assert what
	// was injected without a docker daemon. A copy, not the caller's map: a
	// test that mutates its own Spec.Env afterwards must not be able to
	// rewrite what the driver "received".
	env map[string]string
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
	// volumes is the set of workspace volumes that currently exist, the
	// fake's model of `docker volume ls`. Kept beside items rather than
	// derived from them because the two have different lifetimes in the real
	// driver: a cold-parked session's container is stopped but its volume is
	// untouched, which is the entire reason the volume is named per session.
	volumes map[string]bool
}

func NewFake(total int) *Fake {
	return &Fake{total: total, items: map[string]*fakeItem{}, volumes: map[string]bool{}}
}

// hasVolume reports whether name is currently a live workspace volume.
func (f *Fake) hasVolume(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.volumes[name]
}

// volumeNames returns every live workspace volume name, sorted.
func (f *Fake) volumeNames() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Sorted(maps.Keys(f.volumes))
}

// envFor returns the Spec.Env recorded for the handle id, or nil if that id
// has no item (or was created with no env).
func (f *Fake) envFor(id string) map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	it, ok := f.items[id]
	if !ok {
		return nil
	}
	return maps.Clone(it.env)
}

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
	// Same name the docker driver would use, and the same "no session id, no
	// volume" rule — a fake that named them differently would let a test pass
	// against a string production never produces.
	volume := ""
	if spec.SessionID != "" {
		volume = workspaceVolume(spec.SessionID)
		f.volumes[volume] = true
	}
	f.items[id] = &fakeItem{
		sessionID: spec.SessionID,
		state:     StateRunning,
		volume:    volume,
		env:       maps.Clone(spec.Env),
	}
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
	// The session's volume goes with the session, exactly as in the docker
	// driver — Suspend/Resume deliberately don't touch it, so this is the only
	// place a workspace disappears.
	if it, ok := f.items[id]; ok && it.volume != "" {
		delete(f.volumes, it.volume)
	}
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

// internal/driver/fake.go
package driver

import (
	"context"
	"errors"
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
	// pulls records every ref handed to Prepull, in order — see Pulls.
	pulls []string
	// strips records the stripEnv list handed to each Snapshot call, in order.
	// The real driver's stripping is invisible to anything but `docker image
	// inspect`, so for every caller above the driver this record IS the
	// observable: it is how a runnerd or e2e test proves the keys that must
	// never reach a committed image were actually named on the command.
	strips [][]string
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

// Volumes returns every live workspace volume name, sorted. Exported — like
// Pulls and Strips, and unlike this fake's other accessors — because the
// packages whose behavior this models live elsewhere: runnerd's crash tests
// assert that a container's death did NOT take a volume, and above the driver
// this record is the only place that fact is observable at all.
func (f *Fake) Volumes() []string {
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
	// Mirrors Docker.Create's own first check, and for the same reason the
	// empty-ref rejection in Prepull mirrors its counterpart: a fake that
	// accepted a spec production refuses would let a runnerd test pass
	// against a create that can never happen.
	if err := checkSetupSize(spec); err != nil {
		return Handle{}, err
	}
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
func (f *Fake) Snapshot(_ context.Context, id, ref string, stripEnv []string) (Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.items[id]; !ok {
		return Snapshot{}, fmt.Errorf("no such id %s", id)
	}
	// Recorded even on the generated-ref path: what a caller asked to be
	// stripped is worth knowing whoever named the tag.
	f.strips = append(f.strips, slices.Clone(stripEnv))
	// A named ref comes back verbatim, exactly as `docker commit <id> <ref>`
	// would — see the Driver interface for why the driver must never rename
	// what controld content-addressed. Only an unnamed snapshot gets a
	// generated tag.
	if ref != "" {
		return Snapshot{Ref: ref}, nil
	}
	f.snapSeq++
	return Snapshot{Ref: "fake-image:" + id + "-" + fmt.Sprint(f.snapSeq)}, nil
}

// Prepull records ref instead of fetching anything. Prepull changes nothing
// else about a driver's state, so the record is the only evidence a caller has
// that the command arrived at all — see Pulls.
//
// The empty-ref rejection mirrors Docker.Prepull's: a fake that accepted one
// would let a runnerd test pass against a call production refuses.
func (f *Fake) Prepull(_ context.Context, ref string) error {
	if ref == "" {
		return errors.New("prepull: empty image ref")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pulls = append(f.pulls, ref)
	return nil
}

// Pulls returns every ref passed to Prepull, in call order. Exported — unlike
// this fake's other accessors, which are package-internal — because runnerd's
// agent tests live in another package and this record is their whole
// assertion.
func (f *Fake) Pulls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.pulls)
}

// Strips returns the stripEnv list of every Snapshot call, in call order.
// Exported for the same reason Pulls is: runnerd's and the e2e suite's tests
// live in other packages, and this record is the only place above the driver
// where "the secrets and the setup channel were named for stripping" is
// observable at all.
func (f *Fake) Strips() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]string, len(f.strips))
	for i, s := range f.strips {
		out[i] = slices.Clone(s)
	}
	return out
}

// Destroy is the whole teardown: the container AND the session's volume, in
// that order, exactly as in the docker driver. Suspend/Resume deliberately
// don't touch the volume, so this and RemoveWorkspace are the only two places
// a workspace disappears.
func (f *Fake) Destroy(ctx context.Context, id string) error {
	f.mu.Lock()
	sessionID := ""
	if it, ok := f.items[id]; ok {
		sessionID = it.sessionID
	}
	f.mu.Unlock()

	if err := f.DestroyContainer(ctx, id); err != nil {
		return err
	}
	return f.RemoveWorkspace(ctx, sessionID)
}

// DestroyContainer removes the item and leaves its volume in place — the
// fake's model of the crash path. Getting this right in the FAKE is what
// makes runnerd's crash tests worth running: they assert on nothing but what
// this records, so a fake that dropped the volume here would go green against
// precisely the bug the split exists to prevent.
func (f *Fake) DestroyContainer(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.items, id)
	return nil
}

// RemoveWorkspace releases the volume named for sessionID, tolerating one
// that was never there — the docker driver's `volume rm -f`, and the same
// no-op for an empty id (see driver.Driver.RemoveWorkspace).
func (f *Fake) RemoveWorkspace(_ context.Context, sessionID string) error {
	if sessionID == "" {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.volumes, workspaceVolume(sessionID))
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

// internal/driver/netslot/pool.go
//
// Portions of this file are derived from E2B's orchestrator:
//
//	https://github.com/e2b-dev/infra
//	packages/orchestrator/pkg/sandbox/network/storage_local.go
//	packages/orchestrator/pkg/sandbox/network/reclaim.go
//	packages/orchestrator/pkg/sandbox/network/network.go
//	commit 926fbd6da6c8bf825d6e62674755ab482b1bba26, read 2026-09-21
//
// Copyright 2023 FoundryLabs, Inc.
// Copyright 2026 Rainier Maintainers
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// What was taken: the pool's three-state index bookkeeping (free, in use,
// leaked), the rule that a namespace that survived a failed teardown is the
// rediscovery anchor and must be deleted LAST, and startup reclaim by
// scanning the host's namespace directory. What changed: every host
// operation goes through the Host interface rather than netlink and iptables
// calls made under runtime.LockOSThread, there is no in-namespace NAT or DSCP
// marking, and Adopt exists because Rainier's driver keeps its own per-session
// records across a restart and re-associates them instead of reclaiming
// everything. See PROVENANCE.md.
package netslot

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
)

// ErrNoSlots is what Allocate returns when this host is full. It is a
// capacity answer, not a failure: the driver above turns it into the same
// "no capacity" the slot accounting already reports.
var ErrNoSlots = errors.New("netslot: no free network slot on this host")

// Pool is the host's slot allocator: at most one live Slot per index, with
// the index recycled on release.
type Pool struct {
	cfg  resolved
	host Host

	mu sync.Mutex
	// inUse is every index this process has handed out and not taken back.
	inUse map[int]*Slot
	// leaked is every index whose teardown failed. Its namespace is still on
	// the host, so the index cannot be handed out clean — but it is not lost
	// either: Allocate takes one when nothing else is left and finishes the
	// teardown first, which is how a transient nft or ip failure heals
	// in-process instead of draining the host until the next restart.
	leaked map[int]struct{}
	// foreign is every index whose namespace this pool tried to create and
	// found already there, having not created it itself. Something else on
	// this host is using our naming scheme, and the one safe thing to do
	// with such an index is never to touch it again — deleting it would
	// delete another program's namespace.
	//
	// It is populated ONLY from that collision, and only when the namespace
	// is confirmed to exist. A create that failed for any other reason (no
	// capability, a cancelled context) marks the index leaked instead, which
	// is recoverable: a leaked index is handed out again once nothing clean
	// is left, and the allocation that takes it tears it down first. An
	// index retired here is not recoverable in this process's lifetime,
	// which is why the bar for it is "the namespace is demonstrably there".
	foreign map[int]struct{}
}

// New builds a pool over host and reconciles nothing. Call Reclaim for that:
// the two are separate because the driver has to re-associate its own
// surviving records with their slots BEFORE the leftovers are torn down, and
// a constructor that reclaimed eagerly would delete the network out from
// under every session that outlived its runnerd.
func New(cfg Config, host Host) (*Pool, error) {
	if host == nil {
		return nil, errors.New("netslot: a Host is required; there is deliberately no implementation that pretends to build a network")
	}
	r, err := cfg.resolve()
	if err != nil {
		return nil, err
	}
	return &Pool{
		cfg:     r,
		host:    host,
		inUse:   make(map[int]*Slot),
		leaked:  make(map[int]struct{}),
		foreign: make(map[int]struct{}),
	}, nil
}

// Slots is the pool's capacity.
func (p *Pool) Slots() int { return p.cfg.slots }

// SlotFor derives the slot for an index without touching the host. It is how
// a record recovered from disk is turned back into addresses.
func (p *Pool) SlotFor(idx int, key string) (*Slot, error) { return p.cfg.newSlot(idx, key) }

// Allocate builds one slot's network and hands it out.
//
// A failure part-way leaves nothing behind that a later Allocate can trip
// over: the partially built slot is torn down before the error is returned,
// and if the teardown itself fails the index is marked leaked rather than
// returned to the free list.
//
// It LOOPS, because one failure is not a failure of the allocation: an index
// whose namespace turns out to belong to something else on this host is
// retired and the next one is tried, the way E2B's Acquire scans past a
// namespace it did not create. The loop is bounded — every turn either
// returns or retires an index — so a host whose whole range has been taken
// over answers ErrNoSlots rather than spinning.
func (p *Pool) Allocate(ctx context.Context, key string) (*Slot, error) {
	for {
		slot, retry, err := p.allocateOnce(ctx, key)
		if retry {
			continue
		}
		return slot, err
	}
}

func (p *Pool) allocateOnce(ctx context.Context, key string) (_ *Slot, retry bool, _ error) {
	idx, wasLeaked, err := p.take()
	if err != nil {
		return nil, false, err
	}
	slot, err := p.cfg.newSlot(idx, key)
	if err != nil {
		p.giveBack(idx, false)
		return nil, false, err
	}

	// A leaked index still has a namespace on the host, and everything keyed
	// to that index under it. Finish the previous teardown before building on
	// top of it; E2B's CreateNetwork does the same, and for the same reason —
	// deleting only the namespace would orphan the host-side state.
	if wasLeaked {
		if err := p.teardown(ctx, slot); err != nil {
			p.giveBack(idx, true)
			return nil, false, fmt.Errorf("netslot: reclaiming leaked slot %d before reuse: %w", idx, err)
		}
	}

	// The namespace is created on its own, before anything that would need
	// undoing, because its failure has to be told apart from every other
	// failure here.
	//
	//   - The name is taken, and this pool did not take it. Something else on
	//     the host uses our naming scheme. Deleting it would delete another
	//     program's namespace, so the index is retired for good and the next
	//     one is tried.
	//   - Anything else — no capability, a cancelled context, ENOMEM. The
	//     index is marked LEAKED, not retired: leaked is recoverable (the
	//     next allocation that reaches it tears it down first, and tearing
	//     down a namespace that was never created succeeds), and a runner
	//     that lost CAP_NET_ADMIN for a minute must not come back with a
	//     permanently smaller host.
	if err := p.host.AddNetns(ctx, slot.Netns); err != nil {
		if p.namespaceExists(ctx, slot.Netns) {
			p.retire(idx)
			return nil, true, nil
		}
		p.giveBack(idx, true)
		return nil, false, fmt.Errorf("netslot: creating namespace %s: %w", slot.Netns, err)
	}

	if err := p.setup(ctx, slot); err != nil {
		if terr := p.teardown(ctx, slot); terr != nil {
			p.giveBack(idx, true)
			return nil, false, errors.Join(err, fmt.Errorf("netslot: cleaning up slot %d after a failed setup: %w", idx, terr))
		}
		p.giveBack(idx, false)
		return nil, false, err
	}

	p.mu.Lock()
	p.inUse[idx] = slot
	p.mu.Unlock()
	return slot, false, nil
}

// namespaceExists reports whether the host currently has a namespace by that
// name. A listing this cannot read answers false: the caller's other branch
// is recoverable and this one is not, so an unreadable host must not be able
// to retire an index permanently.
func (p *Pool) namespaceExists(ctx context.Context, name string) bool {
	names, err := p.host.ListNetns(ctx)
	if err != nil {
		return false
	}
	return slices.Contains(names, name)
}

// retire takes an index out of circulation for the lifetime of this process.
// See Pool.foreign.
func (p *Pool) retire(idx int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.inUse, idx)
	delete(p.leaked, idx)
	p.foreign[idx] = struct{}{}
}

// Adopt records an index as in use WITHOUT building anything, for a slot a
// previous runnerd built and whose session outlived it. An index already in
// use, retired as foreign, or outside the envelope is refused: two sessions
// sharing one slot is two guests on one address.
func (p *Pool) Adopt(idx int, key string) (*Slot, error) {
	slot, err := p.cfg.newSlot(idx, key)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if held, ok := p.inUse[idx]; ok {
		return nil, fmt.Errorf("netslot: slot %d is already held by %q", idx, held.Key)
	}
	if _, theirs := p.foreign[idx]; theirs {
		return nil, fmt.Errorf("netslot: slot %d was retired: its namespace belongs to something this runner did not create", idx)
	}
	delete(p.leaked, idx)
	p.inUse[idx] = slot
	return slot, nil
}

// Release tears a slot's network down and returns its index to the pool.
//
// A teardown that fails marks the index leaked and returns the error. The
// index is NOT handed out clean again: the namespace is still there, and it
// is what a later Allocate or Reclaim uses to find the rest.
func (p *Pool) Release(ctx context.Context, slot *Slot) error {
	if slot == nil {
		return nil
	}
	// Releasing a slot this pool does not hold is not an error — teardown is
	// retried from several places (cold suspend, destroy, crash reconcile)
	// and every one of them has to be able to finish — but a slot whose
	// index a DIFFERENT session now holds is not this caller's to tear down.
	p.mu.Lock()
	held, ok := p.inUse[slot.Index]
	mine := !ok || held.Key == slot.Key
	p.mu.Unlock()
	if !mine {
		return nil
	}

	err := p.teardown(ctx, slot)
	p.giveBack(slot.Index, err != nil)
	if err != nil {
		return fmt.Errorf("netslot: releasing slot %d: %w", slot.Index, err)
	}
	return nil
}

// Reclaim tears down every slot this host still has that nobody holds.
//
// It is startup reconciliation, and it works from the namespace names alone
// because those are the one record that survives: a namespace is created
// before any host-side state that belongs to a slot, and deleted after all of
// it, so a name that is present means "this index may still have state" and
// its absence means it does not. Anything in the namespace directory that is
// not ours by name is left alone.
//
// It reclaims indexes ABOVE this runner's current slot count as well, which
// the allocator itself would never hand out. An operator who shrinks --slots
// is the ordinary way that happens, and a leftover the allocator cannot name
// is a leftover nothing would ever clean up.
//
// What it cannot reconcile is a slot built under DIFFERENT addressing: the
// host-side route it deletes is derived from this run's guest and uplink
// ranges, so changing --microvm-guest-cidr or --microvm-uplink-cidr between
// runs leaves the previous run's host route behind with nothing left to find
// it by. Changing those ranges on a host with sessions on it is a drain, not
// a restart.
//
// It returns how many slots it reclaimed. Callers run it AFTER re-associating
// their own surviving sessions (see Adopt): a slot someone holds is not a
// leftover.
func (p *Pool) Reclaim(ctx context.Context) (int, error) {
	names, err := p.host.ListNetns(ctx)
	if err != nil {
		return 0, fmt.Errorf("netslot: listing network namespaces to reclaim leftovers: %w", err)
	}
	slices.Sort(names)

	var (
		reclaimed int
		errs      []error
	)
	for _, name := range names {
		idx, ours := p.cfg.netnsIndexAny(name)
		if !ours {
			continue
		}
		p.mu.Lock()
		_, held := p.inUse[idx]
		p.mu.Unlock()
		if held {
			continue
		}

		// Unbounded, so an index above this runner's slot count still gets
		// torn down. It is only ever used to name what to REMOVE.
		//
		// The one thing that can fail here is an index whose /30 does not
		// fit the configured ranges — a leftover from a run with different
		// addressing. It is reported and left alone rather than half torn
		// down: this pool cannot name the host route that belongs to it, so
		// deleting its namespace would remove the only handle anything has
		// on the rest.
		slot, err := p.cfg.newSlotUnbounded(idx, fmt.Sprintf("reclaim-%d", idx))
		if err != nil {
			errs = append(errs, fmt.Errorf("netslot: %s is not reconcilable under this runner's addressing and was left alone: %w", name, err))
			continue
		}
		if err := p.teardown(ctx, slot); err != nil {
			if idx <= p.cfg.slots {
				p.mu.Lock()
				p.leaked[idx] = struct{}{}
				p.mu.Unlock()
			}
			errs = append(errs, fmt.Errorf("netslot: reclaiming leftover slot %d: %w", idx, err))
			continue
		}
		p.mu.Lock()
		delete(p.leaked, idx)
		p.mu.Unlock()
		reclaimed++
	}
	return reclaimed, errors.Join(errs...)
}

// take picks the lowest free index, or a leaked one when nothing clean is
// left. It reports whether the index came from the leaked set, because that
// one has to be torn down before it is built.
func (p *Pool) take() (idx int, wasLeaked bool, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := 1; i <= p.cfg.slots; i++ {
		if _, busy := p.inUse[i]; busy {
			continue
		}
		if _, dirty := p.leaked[i]; dirty {
			continue
		}
		if _, theirs := p.foreign[i]; theirs {
			continue
		}
		// Reserved immediately, under the same lock that found it free: two
		// concurrent creates that both saw index 3 would otherwise both build
		// it, and the second would find a namespace it did not create.
		p.inUse[i] = &Slot{Index: i}
		return i, false, nil
	}
	for i := 1; i <= p.cfg.slots; i++ {
		if _, dirty := p.leaked[i]; !dirty {
			continue
		}
		if _, busy := p.inUse[i]; busy {
			continue
		}
		p.inUse[i] = &Slot{Index: i}
		return i, true, nil
	}
	return 0, false, ErrNoSlots
}

// giveBack drops an index's reservation, either to the free list or to the
// leaked set.
func (p *Pool) giveBack(idx int, leaked bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.inUse, idx)
	if leaked {
		p.leaked[idx] = struct{}{}
	} else {
		delete(p.leaked, idx)
	}
}

// setup builds everything a slot needs INSIDE its namespace, and the one
// host-side route that points at it. The namespace itself is created by
// Allocate, before this runs, because its failure mode is different: see
// there.
func (p *Pool) setup(ctx context.Context, s *Slot) error {
	if err := p.host.AddVeth(ctx, s.VethHost, s.VethNS, s.Netns); err != nil {
		return fmt.Errorf("netslot: creating veth pair %s/%s: %w", s.VethHost, s.VethNS, err)
	}
	if err := p.host.AddAddr(ctx, "", s.VethHost, s.VethHostCIDR()); err != nil {
		return fmt.Errorf("netslot: addressing %s: %w", s.VethHost, err)
	}
	if err := p.host.LinkUp(ctx, "", s.VethHost); err != nil {
		return fmt.Errorf("netslot: bringing %s up: %w", s.VethHost, err)
	}
	if err := p.host.AddAddr(ctx, s.Netns, s.VethNS, s.VethNSCIDR()); err != nil {
		return fmt.Errorf("netslot: addressing %s: %w", s.VethNS, err)
	}
	if err := p.host.LinkUp(ctx, s.Netns, s.VethNS); err != nil {
		return fmt.Errorf("netslot: bringing %s up: %w", s.VethNS, err)
	}
	if err := p.host.LinkUp(ctx, s.Netns, "lo"); err != nil {
		return fmt.Errorf("netslot: bringing loopback up in %s: %w", s.Netns, err)
	}
	if err := p.host.AddTap(ctx, s.Netns, s.Tap, s.TapMAC); err != nil {
		return fmt.Errorf("netslot: creating tap %s: %w", s.Tap, err)
	}
	if err := p.host.AddAddr(ctx, s.Netns, s.Tap, s.GatewayCIDR()); err != nil {
		return fmt.Errorf("netslot: addressing %s: %w", s.Tap, err)
	}
	if err := p.host.LinkUp(ctx, s.Netns, s.Tap); err != nil {
		return fmt.Errorf("netslot: bringing %s up: %w", s.Tap, err)
	}
	// A guest's packet arrives on the TAP and leaves on the veth, which is
	// forwarding — and a freshly created network namespace has forwarding
	// OFF. Without this every microVM has a link, an address, a route and a
	// firewall, and cannot send a packet through any of them.
	//
	// It is namespaced, so this turns forwarding on for ONE slot and not for
	// the host.
	if err := p.host.SetSysctl(ctx, s.Netns, "net.ipv4.ip_forward", "1"); err != nil {
		return fmt.Errorf("netslot: enabling forwarding in %s: %w", s.Netns, err)
	}
	// Inside the namespace the way out is the host end of the veth pair;
	// from the host the way in to the guest's /30 is the namespace end.
	if err := p.host.AddRoute(ctx, s.Netns, "default", s.VethHostIP.String()); err != nil {
		return fmt.Errorf("netslot: default route in %s: %w", s.Netns, err)
	}
	if err := p.host.AddRoute(ctx, "", s.GuestNet.String(), s.VethNSIP.String()); err != nil {
		return fmt.Errorf("netslot: host route to %s: %w", s.GuestNet, err)
	}
	return p.applyFirewall(ctx, s)
}

// teardown removes everything setup made, in reverse, and is idempotent.
//
// The namespace is deleted LAST and only if everything before it succeeded.
// That ordering is load-bearing and copied deliberately: the namespace entry
// is the only record that a leftover slot exists, so removing it while
// host-side state remains would orphan that state with nothing left to find
// it by. A teardown that fails leaves the anchor in place for the next one.
func (p *Pool) teardown(ctx context.Context, s *Slot) error {
	var errs []error
	if err := p.removeFirewall(ctx, s); err != nil {
		errs = append(errs, err)
	}
	if err := p.host.DelRoute(ctx, "", s.GuestNet.String(), s.VethNSIP.String()); err != nil {
		errs = append(errs, fmt.Errorf("netslot: removing host route to %s: %w", s.GuestNet, err))
	}
	// Deleting one end of a veth pair deletes both, and deleting the host
	// end explicitly (rather than relying on the namespace going away) is
	// what stops the next slot with the same index from racing a name that
	// the kernel has not finished releasing.
	if err := p.host.DelLink(ctx, "", s.VethHost); err != nil {
		errs = append(errs, fmt.Errorf("netslot: removing %s: %w", s.VethHost, err))
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	if err := p.host.DelNetns(ctx, s.Netns); err != nil {
		return fmt.Errorf("netslot: removing namespace %s: %w", s.Netns, err)
	}
	return nil
}

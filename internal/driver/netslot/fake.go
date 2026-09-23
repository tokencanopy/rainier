// internal/driver/netslot/fake.go
//
// The recording fake Host.
//
// It is in a normal file rather than a _test.go one because the microVM
// driver's own tests, in another package, need it: internal/driver builds a
// Pool and has to do it over something. It is never constructed by production
// code — MicrovmOpts injects the whole host seam together or not at all — and
// nothing in this package's non-test files refers to it.
package netslot

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
)

// Op is one recorded host operation: the verb, the namespace it ran in ("" is
// the host's own), and its arguments.
type Op struct {
	Verb  string
	Netns string
	Args  []string
}

func (o Op) String() string {
	parts := append([]string{o.Verb}, o.Args...)
	if o.Netns != "" {
		return "[" + o.Netns + "] " + strings.Join(parts, " ")
	}
	return strings.Join(parts, " ")
}

// FakeHost records every operation in order and keeps just enough state to
// answer the questions a real host would: a namespace that exists cannot be
// created twice, and ListNetns names what is there.
type FakeHost struct {
	mu sync.Mutex

	ops   []Op
	ns    map[string]bool
	links map[string]bool   // "netns/link"
	rules map[string]string // netns -> the ruleset last applied there
	// sysctl is what a slot's namespace was tuned to. It is state and not
	// just a recorded call, because a second Allocate on a recycled index
	// must find the value its own setup put there rather than the previous
	// session's.
	sysctl map[string]string // "netns key" -> value
	failOn map[string]error
}

var (
	_ Host      = (*FakeHost)(nil)
	_ NftReader = (*FakeHost)(nil)
)

// NewFakeHost builds an empty host.
func NewFakeHost() *FakeHost {
	return &FakeHost{
		ns:     make(map[string]bool),
		links:  make(map[string]bool),
		rules:  make(map[string]string),
		sysctl: make(map[string]string),
		failOn: make(map[string]error),
	}
}

// FailOn makes the next and every later call to verb fail with err. A nil err
// clears it. Verbs are the strings Op.Verb carries.
func (f *FakeHost) FailOn(verb string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err == nil {
		delete(f.failOn, verb)
		return
	}
	f.failOn[verb] = err
}

// Ops returns every operation in call order.
func (f *FakeHost) Ops() []Op {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.ops)
}

// Namespaces returns the namespaces that currently exist, sorted.
func (f *FakeHost) Namespaces() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.namespacesLocked()
}

func (f *FakeHost) namespacesLocked() []string {
	out := make([]string, 0, len(f.ns))
	for name, live := range f.ns {
		if live {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return out
}

// Links returns the live links as "netns/link", sorted; a host-namespace link
// is "/link".
func (f *FakeHost) Links() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.links))
	for key, live := range f.links {
		if live {
			out = append(out, key)
		}
	}
	slices.Sort(out)
	return out
}

// Preexisting plants a namespace as though a previous run had left it, which
// is what Reclaim exists to find.
func (f *FakeHost) Preexisting(names ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, n := range names {
		f.ns[n] = true
	}
}

func (f *FakeHost) record(verb, netns string, args ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ops = append(f.ops, Op{Verb: verb, Netns: netns, Args: slices.Clone(args)})
	if err, ok := f.failOn[verb]; ok {
		return err
	}
	return nil
}

func (f *FakeHost) AddNetns(_ context.Context, name string) error {
	if err := f.record("netns-add", "", name); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ns[name] {
		return fmt.Errorf("fake host: network namespace %q already exists", name)
	}
	f.ns[name] = true
	return nil
}

func (f *FakeHost) DelNetns(_ context.Context, name string) error {
	if err := f.record("netns-del", "", name); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.ns, name)
	for key := range f.links {
		if strings.HasPrefix(key, name+"/") {
			delete(f.links, key)
		}
	}
	return nil
}

func (f *FakeHost) ListNetns(_ context.Context) ([]string, error) {
	if err := f.record("netns-list", ""); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.namespacesLocked(), nil
}

func (f *FakeHost) AddVeth(_ context.Context, hostName, peerName, peerNetns string) error {
	if err := f.record("veth-add", "", hostName, peerName, peerNetns); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.links["/"+hostName] = true
	f.links[peerNetns+"/"+peerName] = true
	return nil
}

func (f *FakeHost) DelLink(_ context.Context, netns, name string) error {
	if err := f.record("link-del", netns, name); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.links, netns+"/"+name)
	return nil
}

func (f *FakeHost) AddTap(_ context.Context, netns, name, mac string) error {
	if err := f.record("tap-add", netns, name, mac); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.links[netns+"/"+name] = true
	return nil
}

func (f *FakeHost) AddAddr(_ context.Context, netns, link, cidr string) error {
	return f.record("addr-add", netns, link, cidr)
}

func (f *FakeHost) LinkUp(_ context.Context, netns, link string) error {
	return f.record("link-up", netns, link)
}

func (f *FakeHost) AddRoute(_ context.Context, netns, dst, via string) error {
	return f.record("route-add", netns, dst, via)
}

func (f *FakeHost) DelRoute(_ context.Context, netns, dst, via string) error {
	return f.record("route-del", netns, dst, via)
}

func (f *FakeHost) SetSysctl(_ context.Context, netns, key, value string) error {
	if err := f.record("sysctl", netns, key, value); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sysctl[netns+" "+key] = value
	return nil
}

func (f *FakeHost) ApplyNft(_ context.Context, netns, ruleset string) error {
	// The ruleset is recorded as one argument rather than folded into the
	// operation line: it is many lines long, and a test that wants it wants
	// all of it (see Ruleset).
	if err := f.record("nft-apply", netns, "<ruleset>"); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rules[netns] = ruleset
	return nil
}

func (f *FakeHost) DeleteNftTable(_ context.Context, netns, table string) error {
	if err := f.record("nft-del-table", netns, table); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.rules, netns)
	return nil
}

// ListNft is the read half, recorded like every other operation.
//
// It answers with what ApplyNft last installed in netns, which is a stronger
// fake than it looks: a Pool that read a slot's firewall back from anywhere
// but the host would still pass a test written against the RENDERER, and would
// fail this one the moment the ruleset it read was not the one the host was
// given.
//
// The table name is checked rather than ignored, because the caller composes
// it from a slot index and a name prefix and a Pool that asked for the wrong
// table would otherwise be indistinguishable from one that asked for the
// right one.
func (f *FakeHost) ListNft(_ context.Context, netns, table string) (string, error) {
	if err := f.record("nft-list", netns, table); err != nil {
		return "", err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	// A namespace that is gone is its own answer, as it is on a Linux host
	// where `ip netns exec` fails before nft runs. Checked first because the
	// two are not ranked the same way by a caller: see ErrNoNetns.
	if netns != "" && !f.ns[netns] {
		return "", fmt.Errorf("%w: %q", ErrNoNetns, netns)
	}
	rules, ok := f.rules[netns]
	if !ok {
		return "", fmt.Errorf("%w: inet %s in %q", ErrNoNftTable, table, netns)
	}
	if !strings.Contains(rules, "table inet "+table+" {") {
		return "", fmt.Errorf("%w: inet %s in %q", ErrNoNftTable, table, netns)
	}
	return rules, nil
}

// Ruleset returns the ruleset last applied in netns, and whether there is
// one. It is how a test asks what a guest is actually behind.
func (f *FakeHost) Ruleset(netns string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.rules[netns]
	return r, ok
}

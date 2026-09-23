// internal/driver/netslot/host.go
package netslot

import (
	"context"
	"errors"
)

// DefaultNetnsDir is where `ip netns` keeps its named namespaces, and
// therefore where a previous runnerd's leftovers are found.
const DefaultNetnsDir = "/var/run/netns"

// ErrNoNftTable is what a read of a slot's firewall reports when the table is
// not there at all — a slot that was never set up, or one whose teardown has
// already taken its rules away.
//
// It is a named error rather than an empty string because the two answers are
// not the same question: "this guest is behind no rules" is the finding a
// security check exists to make, and an empty ruleset that could also mean "the
// table is fine, it just has no rules" would make that check unable to fire.
var ErrNoNftTable = errors.New("netslot: no such nftables table")

// Host is every privileged operation a slot needs, and the only way this
// package reaches the machine.
//
// It exists for three reasons, in order of how much they matter:
//
//  1. Fail closed, testably. A microVM host needs CAP_NET_ADMIN to create a
//     namespace, a veth pair, a TAP device or a firewall rule. Developer
//     machines and CI have none of it. Without this seam the slot allocator
//     would be code nobody could run, and the driver above it would be tested
//     against a network that is never built.
//  2. A privileged helper can own it later. ADR-0003 §4.5 wants `runnerd`
//     unprivileged. Whatever cannot be done with a capability — and the
//     jailer's chroot is the live candidate — has to move behind a helper
//     process eventually. An interface whose implementation is already
//     swappable is the difference between that being a new implementation and
//     it being a refactor of the allocator.
//  3. Order is observable. A fake that records every call in sequence
//     (see fake.go) is how the tests assert that a namespace is created
//     before anything is put in it and deleted after everything is taken out
//     — which is the invariant Reclaim depends on.
//
// Every method takes a netns name; "" means the host's own namespace.
// Implementations must be safe to call concurrently.
type Host interface {
	// AddNetns creates a named network namespace. It must fail if one of
	// that name already exists rather than adopting it: an existing namespace
	// is a previous run's leftover, and Pool tears it down explicitly.
	AddNetns(ctx context.Context, name string) error

	// DelNetns removes a named network namespace. An absent namespace is
	// success — teardown is retried, and a retry must be able to finish.
	DelNetns(ctx context.Context, name string) error

	// ListNetns names every namespace the host currently has. It is the only
	// read in the interface, and the whole of Reclaim's evidence.
	ListNetns(ctx context.Context) ([]string, error)

	// AddVeth creates a veth pair with hostName in the host namespace and
	// peerName moved into peerNetns.
	AddVeth(ctx context.Context, hostName, peerName, peerNetns string) error

	// DelLink removes a link. An absent link is success.
	DelLink(ctx context.Context, netns, name string) error

	// AddTap creates a TAP device inside netns with the given hardware
	// address.
	AddTap(ctx context.Context, netns, name, mac string) error

	// AddAddr assigns an address in CIDR form to a link.
	AddAddr(ctx context.Context, netns, link, cidr string) error

	// LinkUp brings a link up.
	LinkUp(ctx context.Context, netns, link string) error

	// AddRoute adds a route. dst is "default" or a CIDR; via is the gateway
	// address.
	AddRoute(ctx context.Context, netns, dst, via string) error

	// SetSysctl sets a kernel parameter inside netns. The net.* tree is
	// per-namespace, which is the whole reason this is here rather than an
	// os.WriteFile: a slot needs net.ipv4.ip_forward on in ITS namespace and
	// nowhere else.
	SetSysctl(ctx context.Context, netns, key, value string) error

	// DelRoute removes a route. An absent route is success.
	DelRoute(ctx context.Context, netns, dst, via string) error

	// ApplyNft installs a complete nftables ruleset inside netns. The
	// ruleset is the whole text `nft -f` would be given, and it is
	// self-contained: it deletes and recreates its own table, so applying it
	// twice is applying it once.
	ApplyNft(ctx context.Context, netns, ruleset string) error

	// DeleteNftTable removes one inet table. An absent table is success:
	// teardown is retried, and a retry has to be able to finish.
	DeleteNftTable(ctx context.Context, netns, table string) error
}

// NftReader is the read half of ApplyNft: what is installed in a slot's
// namespace right now, as the host has it.
//
// It is an OPTIONAL interface rather than a method on Host for one reason. No
// production path needs it: applying a ruleset is a write the pool makes and
// then relies on, and a driver that read its own firewall back on every create
// would be paying for a check the kernel already enforced. What needs it is
// EVIDENCE — a Phase A operator, or the host-gated harness in internal/driver,
// asking "is this guest actually behind the ruleset ADR-0003 §4.3 requires?"
// and being able to answer from the host rather than from the renderer that
// produced the text. Adding a method to Host would have made every
// implementation of it carry a call nothing in production makes.
//
// An implementation reports ErrNoNftTable when the table is absent, and never
// an empty ruleset for it: see that error for why the distinction matters.
type NftReader interface {
	ListNft(ctx context.Context, netns, table string) (string, error)
}

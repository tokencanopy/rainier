// internal/driver/netslot/firewall.go
//
// Portions of this file are derived from E2B's orchestrator:
//
//	https://github.com/e2b-dev/infra
//	packages/orchestrator/pkg/sandbox/network/firewall.go
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
// What was taken: the shape of the ruleset — one table per slot, a filter
// chain at prerouting that matches on the slot's TAP interface, an
// established/related accept ahead of everything, an always-allow set and an
// always-deny set. What changed: the ruleset is RENDERED AS TEXT from a
// template and applied with `nft -f -`, rather than built as expression trees
// with google/nftables, so the exact bytes the kernel is asked for are
// reviewable and can be pinned by a golden file; there are no user-supplied
// allow or deny sets, because an environment's egress policy is egressd's
// allowlist and not a per-slot nftables set; and the allow list is exactly
// one destination, the egress proxy runnerd was started with. See
// PROVENANCE.md.
package netslot

import (
	"bytes"
	"context"
	"fmt"
	"net/netip"
	"slices"
	"strings"
	"text/template"
)

// alwaysDenied is what ADR-0003 §4.3 requires every guest to be unable to
// reach, whatever it does with its own routes or proxy variables.
//
// It is a constant rather than configuration because none of it is a property
// of a deployment: link-local carries the cloud metadata service on every
// provider Rainier has qualified, loopback and the reserved ranges are not
// routable destinations a session has any business addressing, and RFC1918 is
// where every host network and control-plane network in this architecture
// lives. The ranges that ARE deployment-specific — the regional control
// plane — arrive through Config.ControlPlaneCIDRs, and the slot ranges are
// added per host from the allocator's own configuration.
//
// Sorted, because the rendered ruleset is compared byte for byte.
var alwaysDenied = []string{
	"0.0.0.0/8",      // "this network"
	"10.0.0.0/8",     // RFC1918
	"100.64.0.0/10",  // RFC6598 carrier-grade NAT, where provider fabrics live
	"127.0.0.0/8",    // loopback
	"169.254.0.0/16", // link-local, and the metadata service inside it
	"172.16.0.0/12",  // RFC1918
	"192.0.0.0/24",   // IETF protocol assignments
	"192.168.0.0/16", // RFC1918
	"198.18.0.0/15",  // benchmarking
	"224.0.0.0/4",    // multicast
	"240.0.0.0/4",    // reserved, and 255.255.255.255 with it
}

// metadataAddress is called out as its own rule even though 169.254.0.0/16
// already covers it. Cloud metadata denial is a named control in the hosted
// tenancy specification (§10.2 and §16, "DNS rebinding or SSRF reaches
// metadata"), and a named control that exists only as a side effect of a
// broader range is a control nobody can point at in a ruleset dump.
const metadataAddress = "169.254.169.254"

// slotRulesetTemplate is the ruleset applied inside one slot's network
// namespace.
//
// The chain is at prerouting because that is the first hook a packet
// arriving from the guest crosses, before any routing decision the guest
// could influence. It matches on the TAP interface by name: everything else
// in the namespace — the veth uplink, loopback — is the host's own side and
// is not what this chain is about.
//
// Order is the policy, and it reads top to bottom:
//
//  1. Anything not from the guest returns immediately. This chain has an
//     opinion about one interface.
//  2. IPv6 from the guest is dropped outright. The guest link is configured
//     IPv4-only, so any IPv6 is either link-local autoconfiguration or an
//     attempt to reach a denied destination by a family the deny list below
//     does not name.
//  3. Established traffic is accepted, so later packets of a connection this
//     chain already allowed are not re-evaluated. ESTABLISHED and not
//     RELATED: a RELATED match covers conntrack helper expectations, and a
//     helper active in this namespace would let a guest talking to the
//     allowed proxy conjure an expectation for some other address and walk
//     straight past the deny set. Nothing the guest legitimately sends is
//     RELATED — the answers to its connections arrive on the veth, which
//     rule 1 has already returned on.
//  4. The egress proxy, at exactly one address and one port. This is the
//     ONLY host-side destination a guest may reach (ADR-0003 §4.3). It is
//     above the deny rules deliberately: the proxy lives on the host's own
//     network, which the next rules deny wholesale.
//  5. The metadata address, named.
//  6. Everything else in the deny set, which includes this host's own slot
//     ranges and therefore every neighbouring session's /30 and gateway.
//
// Anything left is public internet, which is where egressd's allowlist takes
// over: this chain is the boundary that holds when the guest ignores the
// proxy, not the egress policy itself.
//
// The default policy is accept for that reason, and the table is flushed
// before it is rebuilt so that applying a ruleset twice is applying it once.
const slotRulesetTemplate = `table inet {{ .Table }}
delete table inet {{ .Table }}
table inet {{ .Table }} {
	set denied4 {
		type ipv4_addr
		flags interval
		elements = {
{{- range $i, $cidr := .Denied }}
			{{ $cidr }}{{ if not (last $i $.Denied) }},{{ end }}
{{- end }}
		}
	}

	chain guest_egress {
		type filter hook prerouting priority -150; policy accept;
		iifname != "{{ .Tap }}" return
		meta nfproto ipv6 drop
		ct state established accept
{{- if .Proxy }}
		ip daddr {{ .Proxy }} tcp dport {{ .ProxyPort }} accept
{{- end }}
		ip daddr {{ .Metadata }} drop
		ip daddr @denied4 drop
	}
}
`

var slotRuleset = template.Must(template.New("slot-nft").Funcs(template.FuncMap{
	"last": func(i int, s []string) bool { return i == len(s)-1 },
}).Parse(slotRulesetTemplate))

// tableName is the nftables table one slot's rules live in. It carries the
// index so a `nft list ruleset` on a busy host says which session each rule
// belongs to.
func (r resolved) tableName(idx int) string {
	return fmt.Sprintf("%s-slot-%d", r.prefix, idx)
}

// deniedFor is the deny set for one slot: the constant ranges, the
// deployment's control-plane ranges, and this host's own slot ranges.
//
// The slot ranges go in WHOLE rather than as "every other slot's /30". That
// is stricter than ADR-0003 §4.3's wording and deliberately so: it denies
// every neighbour's /30 and gateway, it denies the uplink veth addresses the
// guest has no business addressing, and it stays correct when the operator
// resizes --slots. The slot's own gateway is denied with the rest, which
// costs nothing — a guest reaches its gateway by ARP, at layer 2, and the
// only thing it needs to address beyond it is the proxy, which is accepted
// two rules earlier.
func (r resolved) deniedFor() []string {
	denied := slices.Clone(alwaysDenied)
	denied = append(denied, r.guest.String(), r.uplink.String())
	for _, p := range r.controlNet {
		denied = append(denied, p.String())
	}
	denied = dedupeRanges(denied)
	slices.Sort(denied)
	return denied
}

// dedupeRanges returns a non-overlapping set, which is what an nftables
// interval set requires.
//
// CIDR prefixes either nest or are disjoint, so "non-overlapping" here means
// dropping any range another one already covers. Broadest first, so the one
// dropped is always the narrower — a control-plane range inside RFC1918, or a
// slot range inside it, is already denied by the range that contains it.
func dedupeRanges(cidrs []string) []string {
	parsed := make([]netip.Prefix, 0, len(cidrs))
	for _, c := range cidrs {
		p, err := netip.ParsePrefix(c)
		if err != nil {
			continue
		}
		parsed = append(parsed, p.Masked())
	}
	// Broadest first, so a narrower range that is already covered is the one
	// dropped.
	slices.SortFunc(parsed, func(a, b netip.Prefix) int {
		if a.Bits() != b.Bits() {
			return a.Bits() - b.Bits()
		}
		return a.Addr().Compare(b.Addr())
	})
	kept := make([]netip.Prefix, 0, len(parsed))
	for _, p := range parsed {
		covered := false
		for _, k := range kept {
			if k.Overlaps(p) {
				covered = true
				break
			}
		}
		if !covered {
			kept = append(kept, p)
		}
	}
	out := make([]string, len(kept))
	for i, p := range kept {
		out[i] = p.String()
	}
	return out
}

// Ruleset renders the nftables ruleset for one slot.
//
// It is exported so the ruleset a host would be given can be read without a
// host — which is how the golden test pins it, and how an operator can see
// what a session is actually behind.
func (p *Pool) Ruleset(slot *Slot) (string, error) { return p.cfg.ruleset(slot) }

func (r resolved) ruleset(slot *Slot) (string, error) {
	proxy, proxyPort := "", 0
	if r.hasProxy {
		proxy, proxyPort = r.proxyAddr.String(), r.proxyPort
	}
	var buf bytes.Buffer
	err := slotRuleset.Execute(&buf, struct {
		Table     string
		Tap       string
		Denied    []string
		Proxy     string
		ProxyPort int
		Metadata  string
	}{
		Table:     r.tableName(slot.Index),
		Tap:       slot.Tap,
		Denied:    r.deniedFor(),
		Proxy:     proxy,
		ProxyPort: proxyPort,
		Metadata:  metadataAddress,
	})
	if err != nil {
		return "", fmt.Errorf("netslot: rendering the firewall for slot %d: %w", slot.Index, err)
	}
	return buf.String(), nil
}

// applyFirewall installs the slot's ruleset inside its namespace.
//
// It is the last thing setup does, and the first thing teardown undoes, which
// is the wrong way round for a boundary — so note what protects the gap:
// nothing is attached to the TAP device until Firecracker opens it, and
// Firecracker is not started until Allocate has returned. There is no window
// in which a guest is on the link without the ruleset.
func (p *Pool) applyFirewall(ctx context.Context, s *Slot) error {
	ruleset, err := p.cfg.ruleset(s)
	if err != nil {
		return err
	}
	if err := p.host.ApplyNft(ctx, s.Netns, ruleset); err != nil {
		return fmt.Errorf("netslot: applying the firewall for slot %d: %w", s.Index, err)
	}
	return nil
}

// removeFirewall deletes the slot's table.
//
// It runs even though the namespace is about to go — deleting a namespace
// takes its rules with it — because teardown is retried and a retry may be
// happening precisely because the namespace would not go away.
func (p *Pool) removeFirewall(ctx context.Context, s *Slot) error {
	if err := p.host.DeleteNftTable(ctx, s.Netns, p.cfg.tableName(s.Index)); err != nil {
		return fmt.Errorf("netslot: removing the firewall for slot %d: %w", s.Index, err)
	}
	return nil
}

// nftTableSpec is the argument DeleteNftTable implementations need, written
// once so the family cannot drift between the template and the delete.
func nftDeleteCommand(table string) string {
	return strings.Join([]string{"delete", "table", "inet", table}, " ")
}

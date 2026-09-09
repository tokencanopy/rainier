package controlapp

import (
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/protocol/runner"
)

// The baseline is policy, and the things that make it safe are properties of
// the TABLE rather than of any one row: no wildcards, nothing multi-tenant,
// nothing that only exists to be typed once and never qualified. A row is
// added by a human who has just finished proving a host is needed, which is
// exactly the moment these are easiest to forget.

// TestBaselineHasNoWildcards is the one that matters most. Proxy.permitted
// reads a leading "*." as a suffix match, so a single wildcard row here would
// silently hand every session every subdomain under it — and the subdomains
// worth having are, without exception, the multi-tenant ones.
func TestBaselineHasNoWildcards(t *testing.T) {
	for _, row := range DefaultDeveloperEgress() {
		if strings.ContainsAny(row.Host, "*?") {
			t.Errorf("baseline row %q is a pattern, not a host: the default egress list is literal names only", row.Host)
		}
	}
}

// TestBaselineNamesNoMultiTenantHost. Every entry below is a real host some
// ecosystem genuinely redirects to — GitHub Actions job logs land on
// productionresultssa*.blob.core.windows.net, GitHub LFS used to hand out
// github-cloud.s3.amazonaws.com, the Go mirror is fronted by Google's — and
// each is a namespace where anybody can create a bucket. Allowing one turns
// "the session may fetch its dependencies" into "the session may reach a
// destination an attacker can also write to", which is the whole failure mode
// a per-host allowlist exists to prevent. When one of these is genuinely
// needed, it is named on the environment that needs it, not here.
func TestBaselineNamesNoMultiTenantHost(t *testing.T) {
	forbidden := []string{
		"blob.core.windows.net",
		"amazonaws.com",
		"storage.googleapis.com",
		"r2.cloudflarestorage.com",
		"cloudfront.net",
		"azureedge.net",
		"pages.dev",
		"netlify.app",
	}
	for _, row := range DefaultDeveloperEgress() {
		for _, bad := range forbidden {
			if row.Host == bad || strings.HasSuffix(row.Host, "."+bad) {
				t.Errorf("baseline row %q sits in the multi-tenant namespace %q; a default that reaches it reaches every tenant in it", row.Host, bad)
			}
		}
	}
}

// TestBaselineRowsAreWellFormed: a lowercase dotted name, once, with a stated
// ecosystem and a stated reason. The reason is load-bearing — it is what the
// next reviewer checks a proposed row against and what a removal has to
// falsify — so an empty one fails here rather than being noticed a year later.
func TestBaselineRowsAreWellFormed(t *testing.T) {
	seen := map[string]bool{}
	for _, row := range DefaultDeveloperEgress() {
		switch {
		case row.Host == "":
			t.Error("baseline row with no host")
		case row.Host != strings.ToLower(row.Host):
			t.Errorf("baseline row %q is not lowercase; the proxy matches the name a client wrote, byte for byte", row.Host)
		case !strings.Contains(row.Host, "."):
			t.Errorf("baseline row %q is not a fully qualified name", row.Host)
		case strings.ContainsAny(row.Host, "/: "):
			t.Errorf("baseline row %q is a URL or an address, not a host", row.Host)
		case seen[row.Host]:
			t.Errorf("baseline names %q twice", row.Host)
		case row.Ecosystem == "":
			t.Errorf("baseline row %q names no ecosystem", row.Host)
		case row.Reason == "":
			t.Errorf("baseline row %q states no reason; a host nobody can justify is a host nobody can remove", row.Host)
		}
		seen[row.Host] = true
	}
	if len(seen) == 0 {
		t.Fatal("the baseline is empty")
	}
}

// TestBaselineCoversTheShippedToolchain names, as a list, the ecosystems the
// session image ships a tool for. Deleting a row now fails here instead of
// quietly making `npm ci` a thing a developer has to file a ticket about.
func TestBaselineCoversTheShippedToolchain(t *testing.T) {
	want := map[string][]string{
		"npm":   {"registry.npmjs.org", "registry.yarnpkg.com"},
		"pypi":  {"pypi.org", "files.pythonhosted.org"},
		"go":    {"proxy.golang.org", "sum.golang.org"},
		"git":   {"github.com", "codeload.github.com", "objects.githubusercontent.com"},
		"gh":    {"api.github.com"},
		"fetch": {"raw.githubusercontent.com", "release-assets.githubusercontent.com"},
	}
	hosts := DefaultDeveloperEgressHosts()
	for tool, need := range want {
		for _, host := range need {
			if !slices.Contains(hosts, host) {
				t.Errorf("%s cannot work by default: %q is not in the baseline", tool, host)
			}
		}
	}
}

// TestGitHubGitEgressHostsIsTheCloneSubset. The launch resolvers add these for
// a cloning session even when the baseline is switched off, so they have to be
// the clone hosts and only those: an api.github.com that leaked in here would
// hand API reach to every cloning session on a fleet that had deliberately
// turned the baseline off.
func TestGitHubGitEgressHostsIsTheCloneSubset(t *testing.T) {
	got := GitHubGitEgressHosts()
	want := []string{"github.com", "codeload.github.com", "objects.githubusercontent.com"}
	if !slices.Equal(got, want) {
		t.Fatalf("GitHubGitEgressHosts() = %v, want %v", got, want)
	}
	for _, host := range got {
		if !slices.Contains(DefaultDeveloperEgressHosts(), host) {
			t.Errorf("%q is a clone host that the baseline does not carry; the two lists have drifted", host)
		}
	}
}

// TestBaselineAccessorsCopy: the table is package state read on every dispatch,
// and a caller that sorted or appended to it would change what every later
// session gets.
func TestBaselineAccessorsCopy(t *testing.T) {
	rows := DefaultDeveloperEgress()
	rows[0].Host = "mutated.invalid"
	if DefaultDeveloperEgress()[0].Host == "mutated.invalid" {
		t.Error("DefaultDeveloperEgress handed out the package's own table")
	}
	hosts := DefaultDeveloperEgressHosts()
	hosts[0] = "mutated.invalid"
	if slices.Contains(DefaultDeveloperEgressHosts(), "mutated.invalid") {
		t.Error("DefaultDeveloperEgressHosts handed out the package's own slice")
	}
	git := GitHubGitEgressHosts()
	git[0] = "mutated.invalid"
	if slices.Contains(GitHubGitEgressHosts(), "mutated.invalid") {
		t.Error("GitHubGitEgressHosts handed out the package's own slice")
	}
}

// ---------------------------------------------------------------------------
// the seam: what a dispatch actually carries
// ---------------------------------------------------------------------------

// TestDispatchCarriesTheDeveloperBaseline is the whole point of the change: a
// session that declared no egress at all, from no environment, still boots
// able to install a dependency.
func TestDispatchCarriesTheDeveloperBaseline(t *testing.T) {
	fx := newFleetFixtureWithBaseline(t)
	fx.st.seedRunner(fleetSeededRunner("vm1", 4, 0, true))
	fx.st.seedSession(fleetScratchQueued("sess_x", 0))
	fleetRunFixture(t, fx)
	fx.service.Wake("pool_example")

	spec := waitForCreateSpec(t, fx)
	for _, host := range DefaultDeveloperEgressHosts() {
		if !slices.Contains(spec.EgressAllow, host) {
			t.Errorf("dispatched EgressAllow = %v, want it to carry the baseline host %q", spec.EgressAllow, host)
		}
	}
}

// TestDispatchBaselineIsAdditiveAndNotStored pins the two halves of the
// semantics an operator has to be able to rely on: an environment's own hosts
// survive (this is a union, not a replacement) and read first, and the session
// ROW is untouched — what a human sees on `session inspect` is still what a
// human asked for.
func TestDispatchBaselineIsAdditiveAndNotStored(t *testing.T) {
	fx := newFleetFixtureWithBaseline(t)
	fx.st.seedRunner(fleetSeededRunner("vm1", 4, 0, true))
	row := fleetScratchQueued("sess_x", 0)
	// One host nobody else supplies, and one the baseline also carries.
	row.Spec.EgressAllow = []string{"internal.example.test", "pypi.org"}
	fx.st.seedSession(row)
	fleetRunFixture(t, fx)
	fx.service.Wake("pool_example")

	spec := waitForCreateSpec(t, fx)
	if len(spec.EgressAllow) < 2 || spec.EgressAllow[0] != "internal.example.test" || spec.EgressAllow[1] != "pypi.org" {
		t.Errorf("dispatched EgressAllow = %v, want the declared hosts first and in order", spec.EgressAllow)
	}
	for _, host := range spec.EgressAllow {
		if n := slices.Index(spec.EgressAllow, host); slices.Index(spec.EgressAllow[n+1:], host) >= 0 {
			t.Errorf("dispatched EgressAllow = %v carries %q twice", spec.EgressAllow, host)
		}
	}
	stored, err := fx.st.getSession("ws_example", "sess_x")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(stored.Spec.EgressAllow, []string{"internal.example.test", "pypi.org"}) {
		t.Errorf("stored egress_allow = %v, want exactly what the caller declared — the baseline is a dispatch-time union, not a write", stored.Spec.EgressAllow)
	}
}

// TestDefaultEgressOptionReplacesTheBaseline: nil takes the table, an empty
// slice turns it off, and a populated one is the host's own list. An operator
// who cannot turn a default off does not have a default, they have a rule.
func TestDefaultEgressOptionReplacesTheBaseline(t *testing.T) {
	for name, tc := range map[string]struct {
		opt  *[]string
		want []string
		off  bool
	}{
		"unset takes the built-in baseline": {opt: nil, want: DefaultDeveloperEgressHosts()},
		"empty turns it off":                {opt: &[]string{}, off: true},
		"a host's own list replaces it":     {opt: &[]string{"mirror.example.test"}, want: []string{"mirror.example.test"}},
	} {
		t.Run(name, func(t *testing.T) {
			if got := resolveDefaultEgress(tc.opt); tc.off {
				if len(got) != 0 {
					t.Fatalf("resolveDefaultEgress = %v, want nothing", got)
				}
			} else if !slices.Equal(got, tc.want) {
				t.Fatalf("resolveDefaultEgress = %v, want %v", got, tc.want)
			}
		})
	}
	// And the empty case really does reach a dispatch: this is the fixture
	// every other scheduler test uses, which is only sound if it is true.
	fx := newFleetFixtureWithResolver(t, nil)
	fx.st.seedRunner(fleetSeededRunner("vm1", 4, 0, true))
	fx.st.seedSession(fleetScratchQueued("sess_x", 0))
	fleetRunFixture(t, fx)
	fx.service.Wake("pool_example")

	spec := waitForCreateSpec(t, fx)
	for _, host := range DefaultDeveloperEgressHosts() {
		if slices.Contains(spec.EgressAllow, host) {
			t.Fatalf("dispatched EgressAllow = %v with the baseline switched off, want no baseline host (%q)", spec.EgressAllow, host)
		}
	}
}

// waitForCreateSpec returns the spec of the create the fixture dispatched.
func waitForCreateSpec(t *testing.T, fx *fleetFixture) *runner.Spec {
	t.Helper()
	var spec *runner.Spec
	fleetEventually(t, 2*time.Second, func() error {
		for _, cmd := range fx.transport.dispatchedCommands() {
			if cmd.Type == "create" {
				spec = cmd.Spec
			}
		}
		if spec == nil {
			return fmt.Errorf("no create dispatched")
		}
		return nil
	})
	return spec
}

// TestBaselineDocMatchesTheTable. docs/default-egress.md is where an operator
// decides whether to trust this default and where a reviewer checks a proposed
// row, so a table that has drifted from the code is worse than no table: it
// describes a policy the fleet is not running. Both directions are checked —
// an undocumented host and a documented one that no longer exists are the same
// bug seen from either end.
func TestBaselineDocMatchesTheTable(t *testing.T) {
	doc, err := os.ReadFile("../docs/default-egress.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(doc)
	for _, row := range DefaultDeveloperEgress() {
		if !strings.Contains(text, "`"+row.Host+"`") {
			t.Errorf("docs/default-egress.md never names %q, which every session can now reach", row.Host)
		}
	}
	// The reverse: a host the doc presents in its own table row must still be
	// in the table. Only the "| `host` |" shape is read, so the hosts the doc
	// discusses in prose as DELIBERATELY EXCLUDED are not swept in.
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, "| `") {
			continue
		}
		host := strings.TrimPrefix(strings.Split(line, "`")[1], "")
		if !strings.Contains(host, ".") {
			continue
		}
		if !slices.Contains(DefaultDeveloperEgressHosts(), host) {
			t.Errorf("docs/default-egress.md lists %q in its baseline table, but the table in code does not carry it", host)
		}
	}
}

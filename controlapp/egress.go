package controlapp

import "slices"

// The default developer egress baseline: the hosts every session may reach
// without anybody naming them.
//
// WHY THIS EXISTS. Egress is default-deny (spec §8): a session container has
// no route out except the per-VM CONNECT proxy, which permits only the hosts
// the control plane pushed for that session. Until now that list was, in
// full: what the caller typed into `--egress`, what the environment declared,
// the source-control hosts a clone implies, and each agent provider's login
// and inference hosts. Nothing in it made `npm ci` work. So the first honest
// thing a new environment did was fail — a developer hit a 403 from the proxy,
// went and found a hostname, edited the environment, and started over. A
// default environment that cannot install a dependency is not a default
// environment, and a host list that lives in a runbook is a list only the
// person who read the runbook has.
//
// WHAT IT IS NOT. It is not the public internet. Every row is a single literal
// apex reached by a toolchain THIS PROJECT'S SESSION IMAGE actually ships
// (docs/session-image.md), whose need was established by fetching from it and
// following its redirects, not by copying a vendor's firewall page. There are
// no wildcards: Proxy.permitted treats a leading "*." as a suffix match, and
// one "*.blob.core.windows.net" or "*.amazonaws.com" would hand every session
// a writable, multi-tenant destination that happens to also serve the artifact
// somebody wanted. Where a real ecosystem needs exactly that — GitHub Actions
// job logs redirect to a numbered productionresultssa*.blob.core.windows.net —
// the answer here is that the operation is not supported by default and its
// denial is diagnostic, not that a tenant boundary is worth less than the
// convenience. docs/default-egress.md carries that argument in full.
//
// WHAT IT DOES NOT GRANT. Reachability is not authority. A session that can
// reach github.com still holds no GitHub credential unless the broker issued
// one, so it reads public repositories and nothing else. A session that can
// reach registry.npmjs.org installs public packages and cannot publish one,
// because publishing needs a token this grant does not create. A private
// registry is not here and will not be: it is named per environment, beside
// the credential that reaches it.
//
// ADDING A ROW is a change to this table and nowhere else. It is the single
// source of truth — the self-hosted launch resolver's clone hosts are a filter
// over it (GitHubGitEgressHosts), the tests assert against it, and the docs
// table is checked against it. A row no shipped tool reaches is dead policy:
// it cannot be qualified, it cannot be probed, and it silently widens the
// default. A toolchain absent from the image (Rust, a browser-download CDN, a
// container registry) gets its row when the image gets the tool, not before.

// Ecosystems named by the baseline. They exist so a caller can filter the
// table without matching on hostnames, and so a reader can tell a package
// registry from a source host at a glance.
const (
	// EcosystemNPM is the Node package registries the image's npm and yarn
	// resolve against.
	EcosystemNPM = "npm"
	// EcosystemPyPI is the Python index and the file host it links to.
	EcosystemPyPI = "pypi"
	// EcosystemGo is the Go module mirror and its checksum database.
	EcosystemGo = "go"
	// EcosystemGitHubGit is the source-control half of GitHub: what a clone,
	// fetch, push and LFS transfer reach. These are the hosts the launch
	// material has always added for a CLONING session; they are in the
	// baseline because a session that clones nothing still routinely needs to
	// read a public dependency that lives in a repository.
	EcosystemGitHubGit = "github-git"
	// EcosystemGitHubAPI is api.github.com — the REST and GraphQL surface `gh`
	// and an agent's tooling use.
	EcosystemGitHubAPI = "github-api"
	// EcosystemGitHubContent is GitHub's read-only content hosts: raw files
	// and published release assets.
	EcosystemGitHubContent = "github-content"
)

// EgressHost is one row of the baseline: the literal host, the ecosystem that
// reaches it, and the reason it is there. The reason is not decoration — it is
// what a reviewer checks a proposed row against, and what a removal has to
// falsify.
type EgressHost struct {
	Host      string
	Ecosystem string
	Reason    string
}

// defaultDeveloperEgress is the table. Order is stable and grouped by
// ecosystem, because it is read back to humans — a session's rendered
// egress_allow, an audit line, the docs table — far more often than it is
// matched against.
//
// Each Reason records what was actually observed, because the alternative is a
// list nobody can ever safely shorten. Two rows that are NOT here are as
// deliberate as the ones that are:
//
//   - storage.googleapis.com, which the dogfood runbook's host list carries for
//     Go. Three fetches through proxy.golang.org — a 4 MB module zip included —
//     returned 200 with zero redirects, so the mirror serves its own bytes and
//     the entry buys nothing. What it costs is every Google Cloud Storage
//     bucket on the internet, which is the exact shape of multi-tenant
//     destination this table refuses.
//   - github-cloud.s3.amazonaws.com and the Actions result hosts, for the same
//     reason at larger scale.
var defaultDeveloperEgress = []EgressHost{
	{
		Host: "registry.npmjs.org", Ecosystem: EcosystemNPM,
		Reason: "npm/pnpm metadata and tarballs. Every one of the 536 `resolved` " +
			"entries across this project's own lockfiles points here, and a tarball " +
			"fetch returns 200 with no redirect, so the registry serves its own bytes. " +
			"corepack downloads pnpm and yarn berry from here too.",
	},
	{
		Host: "registry.yarnpkg.com", Ecosystem: EcosystemNPM,
		Reason: "Yarn Classic's default registry — `yarn config get registry` on the " +
			"yarn the base image ships answers with this host, not registry.npmjs.org. " +
			"Without it `yarn install` fails on an image that has yarn on PATH.",
	},
	{
		Host: "pypi.org", Ecosystem: EcosystemPyPI,
		Reason: "The PyPI simple index and JSON API that pip and uv resolve against.",
	},
	{
		Host: "files.pythonhosted.org", Ecosystem: EcosystemPyPI,
		Reason: "The sdist/wheel host the simple index links to; served directly, no " +
			"redirect. The image sets UV_PYTHON_DOWNLOADS=never, so uv fetches " +
			"packages from here and never an interpreter from a release host.",
	},
	{
		Host: "proxy.golang.org", Ecosystem: EcosystemGo,
		Reason: "The default GOPROXY, which also serves the toolchain modules " +
			"GOTOOLCHAIN resolves. Module zips come back 200 with no redirect.",
	},
	{
		Host: "sum.golang.org", Ecosystem: EcosystemGo,
		Reason: "The default GOSUMDB. A module fetch that cannot reach it fails " +
			"verification rather than proceeding unverified, so it is not optional.",
	},
	{
		Host: "github.com", Ecosystem: EcosystemGitHubGit,
		Reason: "git over HTTPS: clone, fetch, push, and the LFS batch endpoint. Also " +
			"GOPROXY's `direct` fallback and every `go get` of a module the mirror " +
			"has not cached.",
	},
	{
		Host: "codeload.github.com", Ecosystem: EcosystemGitHubGit,
		Reason: "The archive endpoints. Both github.com/<o>/<r>/archive/... and " +
			"api.github.com/repos/<o>/<r>/tarball redirect here, observed live.",
	},
	{
		Host: "objects.githubusercontent.com", Ecosystem: EcosystemGitHubGit,
		Reason: "git-lfs object transfer, and the release-asset host GitHub used " +
			"before release-assets.githubusercontent.com.",
	},
	{
		Host: "api.github.com", Ecosystem: EcosystemGitHubAPI,
		Reason: "The REST and GraphQL API `gh` uses. Reachability only: the token " +
			"still comes from the credential broker, and its scopes are unchanged.",
	},
	{
		Host: "raw.githubusercontent.com", Ecosystem: EcosystemGitHubContent,
		Reason: "Raw file reads — the host a README link, a schema URL, or a CI " +
			"config reference resolves to. Read-only and served directly.",
	},
	{
		Host: "release-assets.githubusercontent.com", Ecosystem: EcosystemGitHubContent,
		Reason: "Where a release-asset download now lands: fetching a public asset " +
			"from github.com/<o>/<r>/releases/download/... redirects here once. " +
			"`gh release download` fails without it.",
	},
}

// DefaultDeveloperEgress returns a copy of the baseline table.
func DefaultDeveloperEgress() []EgressHost {
	return slices.Clone(defaultDeveloperEgress)
}

// DefaultDeveloperEgressHosts returns just the hostnames, in table order. This
// is what the scheduler unions into a dispatched session's allowlist.
func DefaultDeveloperEgressHosts() []string { return egressHostsOf(nil) }

// GitHubGitEgressHosts returns the source-control hosts a clone, fetch, push
// and LFS transfer reach, in table order. A launch resolver adds these for a
// cloning session even where the baseline is switched off, because they are
// not a convenience there: the connector said "acme/app", not three host
// names, and a clone the caller asked for must not fail on a host the caller
// had no way to know about.
func GitHubGitEgressHosts() []string { return egressHostsOf([]string{EcosystemGitHubGit}) }

// egressHostsOf returns the hosts of the rows whose ecosystem is in keep, or
// every host when keep is nil.
func egressHostsOf(keep []string) []string {
	out := make([]string, 0, len(defaultDeveloperEgress))
	for _, row := range defaultDeveloperEgress {
		if keep == nil || slices.Contains(keep, row.Ecosystem) {
			out = append(out, row.Host)
		}
	}
	return out
}

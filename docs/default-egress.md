# The default developer egress baseline

A session's network is default-deny. Its container sits on an `internal: true`
docker network — no default route, no DNS to the outside, nothing — and the one
way out is `egressd`, a CONNECT proxy on the runner that permits only the hosts
the control plane pushed for that session id. That design is the reason a
Rainier session is safe to hand an agent.

It is also the reason, until this change, that the first thing a new
environment did was fail. `npm ci` in a fresh session returned
`CONNECT tunnel failed, response 403`, and the fix was for a human to go and
find out that npm's tarballs live on `registry.npmjs.org`, edit the
environment, and start again — once per ecosystem, per environment, forever.

This document is the argument for what replaced that, the list itself, and what
the list deliberately does not contain.

## Where the default actually lives

There is one seam. `controlapp.FleetService.createSpec` builds the runner spec
for every dispatch, in both compositions — self-hosted `controld` and hosted
Rainier Cloud — and composes the allowlist there, in this order:

| # | Source | Where it comes from |
|---|---|---|
| 1 | the session row's `egress_allow` | `unionHosts(environment, session override)` at create (`controlapp/sessions.go`) |
| 2 | launch material | the clone hosts a repository connector implies (`LaunchMaterialResolver`) |
| 3 | agent providers | each provider's login/refresh/inference hosts (`controlapp.AgentProviders`) |
| 4 | **the developer baseline** | `controlapp.DefaultDeveloperEgressHosts` — this document |

`unionHosts` dedupes and preserves order, so a host a human named reads first
and appears once however many of the four sources supply it.

A list in a runbook is not a default. `rainier-cloud`'s
`environments/agents-dev/egress-hosts.txt` was pasted into one
`rainier env create --egress ...` command by whoever read the runbook; every
environment created any other way had none of it. The baseline is at the seam
instead, so there is nothing to remember and nothing to paste.

## The decision: a curated list, not the public internet

The obvious alternative was to make egress open by default and rely on the
protections that would remain — no route to RFC1918, no metadata endpoint, no
other tenant. It is a real option and several products in this space take it.
We did not, and the reasons are specific rather than reflexive:

- **The blast radius of an agent is the reason Rainier exists.** An agent that
  can reach any host can exfiltrate a repository to any host. Everything else
  in a session — the read-only rootfs, no sudo, the credential broker that
  keeps a token out of the environment block — is about limiting what a wrong
  turn costs. Open egress is the one that costs the most and is the least
  visible when it goes wrong: the audit line for an exfiltration to a paste
  site and the one for a package download look identical.
- **The allowlist is the audit log.** `decision: allow|deny|deny_private` per
  host, per session, is only meaningful because the allow set is small enough
  that a deny means something. Open egress makes the log a traffic record.
- **A curated list is falsifiable; "the internet" is not.** Each row below
  names a tool that is in the session image and an operation that was run
  against it. That is a thing a reviewer can check and a smoke test can prove.
- **The failure mode is diagnostic and cheap.** A missing host is a 403;
  the proxy audit log identifies the host, and the fix is one line on one environment. A missing
  protection is a postmortem.

What the curated list costs, honestly:

- **It will be wrong at the edges.** Somebody will need a host that is not
  here, and they will lose the first ten minutes to a 403 they did not expect.
  The mitigation is that the common ninety-nine per cent of development —
  install dependencies, clone, read the API — is covered, and the escape hatch
  (`--egress` on the environment) already exists and is unchanged.
- **It has to be maintained.** Ecosystems move their CDNs; GitHub moved
  release assets to `release-assets.githubusercontent.com` while this was
  being written. The mitigation is the live qualification suite in
  `rainier-cloud` (`environments/`, `RAINIER_LIVE_EGRESS=1`), which fails when
  a real fetch stops working through exactly this list.
- **It is not sufficient for arbitrary browsing.** An agent asked to "read the
  docs at example.com" cannot. That is deliberate: package INSTALLATION and
  arbitrary documentation BROWSING are different powers, and only the first is
  a prerequisite for building software. A workspace that wants the second asks
  for it per environment.

If that trade is ever revisited, the decision to revisit is a design change
with its own review, not a row added to a table.

## The list

Every host is a literal name. **There are no wildcard entries and there will
not be**, because `egressd` reads a leading `*.` as a suffix match and every
subdomain wildcard worth having is a multi-tenant namespace.

| Host | Ecosystem | Why |
|---|---|---|
| `registry.npmjs.org` | npm | metadata and tarballs. All 536 `resolved` entries across this project's lockfiles point here; a tarball fetch is a 200 with no redirect. corepack fetches pnpm and yarn berry from here too. |
| `registry.yarnpkg.com` | npm | Yarn Classic's own default — `yarn config get registry` on the yarn the base image ships answers with this, not npmjs.org. |
| `pypi.org` | pypi | the simple index and JSON API pip and uv resolve against. |
| `files.pythonhosted.org` | pypi | the sdist/wheel host the index links to. The image sets `UV_PYTHON_DOWNLOADS=never`, so uv needs no interpreter-download host. |
| `proxy.golang.org` | go | the default `GOPROXY`, which also serves the `golang.org/toolchain` modules `GOTOOLCHAIN` resolves. |
| `sum.golang.org` | go | the default `GOSUMDB`. A module fetch that cannot reach it fails verification rather than proceeding unverified. |
| `github.com` | github-git | git over HTTPS — clone, fetch, push, LFS batch — and `GOPROXY`'s `direct` fallback. |
| `codeload.github.com` | github-git | the archive endpoints; both `github.com/<o>/<r>/archive/...` and `api.github.com/repos/<o>/<r>/tarball` redirect here. |
| `objects.githubusercontent.com` | github-git | git-lfs object transfer. |
| `api.github.com` | github-api | the REST and GraphQL API `gh` uses. |
| `raw.githubusercontent.com` | github-content | raw file reads. |
| `release-assets.githubusercontent.com` | github-content | where a release-asset download lands today; `gh release download` fails without it. |

The table is `controlapp/egress.go` and nothing else. The clone hosts the
launch resolvers add are a filter over it (`GitHubGitEgressHosts`), the tests
assert against it, and this table is checked against it.

### How each row was established

Not from a vendor's firewall page. Each was a fetch through a proxy, with the
redirect chain followed and the final host recorded — which is how
`release-assets.githubusercontent.com` was found and how
`storage.googleapis.com` was ruled out. The derivations that changed the answer:

- **`storage.googleapis.com` is not needed for Go and is not included.** The
  existing runbook host list carries it. Three fetches through
  `proxy.golang.org` — `@v/list`, a small module zip, and a 3.9 MB one —
  returned 200 with zero redirects. The mirror serves its own bytes. What the
  entry would have cost is every Google Cloud Storage bucket on the internet.
- **npm needs one host, not two.** `dist.tarball` for a registry package is a
  `registry.npmjs.org` URL and the fetch does not redirect.
- **Yarn Classic does not use npm's registry.** It is on the image, so its
  default is on the list.
- **`uv` needs no release host** because the image pins
  `UV_PYTHON_DOWNLOADS=never`. Without that pin this table would have needed
  a python-build-standalone release host, and the honest place to fix that is
  the image.

## What is deliberately excluded

**GitHub Actions job logs and artifacts.** `gh run view --log` fetches
`/repos/{o}/{r}/actions/jobs/{id}/logs`, which 302s to
`productionresultssa<N>.blob.core.windows.net` — a numbered, multi-tenant Azure
Blob Storage namespace. Covering it means `*.blob.core.windows.net`, which is a
grant to every Azure storage tenant, on every session, forever, so that one
command can print. It is not worth that and it is not in the baseline. Run-log
zips take a different first hop, `results-receiver.actions.githubusercontent.com`,
whose own continuation could not be observed from a sandbox that (correctly)
denies it. A workspace that genuinely needs CI logs in-session names the hosts
on its own environment, with that trade made explicitly by the person making
it; the default answers 403 and records the host in its audit log.

**Rust, `cargo`, `rustup`** (`static.rust-lang.org`, `index.crates.io`,
`static.crates.io`). There is no Rust toolchain in the session image. A row for
a tool that is not installed cannot be qualified, cannot be probed, and widens
the default for nobody. It arrives with the toolchain.

**Playwright browser downloads.** `cdn.playwright.dev` redirects to
`playwright.download.prss.microsoft.com` (observed: one redirect, 200). Both
hosts, together, are the contract — the CDN name alone gets a redirect the
session cannot follow. Browsers are not in this image and the browser image is
a different task's; that task opts in by adding the pair to the environment
that runs browsers, or by proposing the two rows here once a shipped image has
the browsers.

**devcontainer features and container registries** (`mcr.microsoft.com`,
`ghcr.io`). A session has no container runtime and no docker socket, so
nothing in it can consume them.

**Distribution package archives** (`deb.debian.org`, `security.debian.org`).
The rootfs is read-only and `sudo` is not installed. `apt-get install` cannot
work in a session by design; the answer is a row in the image, not a host here.

**Private registries**, of any kind. A private index, an internal artifact
store, GitHub Packages, a cloud artifact registry: each is named on the
environment that needs it, beside the credential that reaches it. A default
that reached a private registry would be a default that decided a tenancy
question on somebody's behalf.

## What this does not grant

Reachability is not authority, and nothing about credentials changed:

- A session that can reach `github.com` holds **no GitHub token** unless the
  credential broker issued one. Public repositories are readable; that is all.
  Scopes, sharing, and revocation are exactly as they were.
- A session that can reach `registry.npmjs.org` can install public packages and
  **cannot publish** one — publishing needs a token this grant does not create.
- The agent-provider hosts, the credential broker's routing, and tenant
  isolation are untouched. This change adds hosts to one union and nothing
  else.

One thing this does NOT claim, because it would not be true: a session that
already holds a broker-issued GitHub token can use it against any repository
that token's scopes reach, and `github.com` was already on a cloning session's
allowlist before this change. Putting it in the baseline extends that
reachability to sessions that clone nothing. The bound on what a token can do
is the token's scopes and the broker's willingness to issue it, which is where
that bound has always been and where it should be argued — not the egress list.

## Semantics an operator can rely on

- **Additive, never replacing.** An environment's `egress_allow` keeps every
  host it declared and gains these. A session's own `egress_allow` still
  extends its environment's, as before.
- **Not stored.** The baseline is unioned at dispatch and never written to a
  session row. `egress_allow` on the API and in the CLI still means "what a
  human asked for", which is what makes it reviewable.
- **Off is a supported state.** `FleetOptions.DefaultEgress` is a pointer:
  unset takes the baseline, a non-nil empty slice turns it off entirely, and a
  populated one replaces it with the host's own. It is host policy — there is
  no API field and no environment column behind it, so a tenant cannot widen
  its own default.
- **Existing sessions do not change.** `runnerd` pushes a session's allowlist
  to `egressd` exactly once, in `createWithID`. A session that is already
  running keeps the list it was created with; suspend and resume do not
  re-push. The baseline therefore reaches a session when that session is
  created, on a control plane that has this change. Nothing about a running
  session's network changes underneath it, in either direction.

## Rolling it out

1. Deploy the control plane. Sessions created after it get the baseline;
   sessions already running are unaffected (above).
2. Nothing to do per environment. An existing environment's declared hosts are
   preserved and the baseline is added — no `env update`, no new environment,
   no snapshot invalidation (the setup hash covers image and setup, not
   egress).
3. Environments whose `egress_allow` exists only to name package hosts can drop
   those entries at leisure. Leaving them costs nothing: the union dedupes.
4. To roll back, set `FleetOptions.DefaultEgress` to an empty slice and
   redeploy. Sessions created afterwards get the pre-change behaviour; running
   sessions, again, are untouched.

## Adding a row

1. Establish the need from a real failure or a lockfile — not from a vendor
   page. Say which shipped tool reaches it.
2. Fetch through it and follow the redirects. The host that serves the bytes
   is the host that belongs on the list; the one you started at may not be.
3. Check it is not a multi-tenant namespace. `TestBaselineNamesNoMultiTenantHost`
   knows the common ones; your judgement covers the rest.
4. Add the row, with its reason, to `controlapp/egress.go`. Update the table
   above. Run the live qualification in `rainier-cloud`.

## The guard underneath

An allowlisted NAME is not an allowlisted DESTINATION. The allowlist matches
the string a client wrote into its CONNECT line, and what that string resolves
to is a nameserver's answer. With a broader default that matters more, so
`egressd` now resolves the host itself, refuses an answer that is loopback,
RFC1918, link-local (which is where every cloud metadata service lives),
unique-local, CGNAT, or otherwise not publicly routable, and dials the address
it vetted rather than re-resolving the name. A refusal is logged as
`deny_private` and answered 403; an unresolvable host is still an `allow` whose
dial failed, so the audit log's vocabulary keeps meaning what it meant.

This is defence in depth, not the allowlist's replacement — every host in the
baseline is public — and it is what makes "a redirect cannot broaden private
access" true rather than merely likely. A redirect is followed by the CLIENT,
which means a new CONNECT for the new host, through the same allowlist and the
same guard.

`egress.AllowPrivateDestinations()` lifts it for a local fleet whose origins
are on loopback. `egressd` exposes no flag for it.

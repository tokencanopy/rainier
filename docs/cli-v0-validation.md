# CLI v0 finishing review

Reviewed PR #65 against Rainier main
`fa079149bbf45e6b6266484ce1e8722f045068a4`. The work branch started at that
commit and normally merged PR head `5a9346f43a36efd1e728221002e4b21a3a002b1f`.
The shared root checkout stayed clean at the main commit.

Material corrections:

- Missing hosted compute, workspace, and default-environment facts no longer
  imply readiness. Self-hosted readiness still checks observable runner capacity.
  Status checks have a deadline and JSON preserves compute status and health.
- Session lifecycle, child exit, reachability, and last event time remain separate.
  Warm children are paused; cold suspension does not claim a running child.
  Failed sessions appear in the default list; canceled/deleted records need `--all`.
- Failed attachment does not change `current`; remembering a successful session
  pins the original context. Unsafe ID paths and repeated selector pages fail.
  Default environments are sent by opaque ID, avoiding name reuse between reads.
- Create failures retain a recovery key. Delete failures identify the opaque ID
  to retry. Neither automatically repeats an ambiguous mutation. Existing server
  transition guards and advanced connection semantics are preserved.
- API bodies, raw session failure prose, URL transport details, and WebSocket
  close reasons are omitted from errors. Codes and request IDs remain available.
  Incomplete agent login and canceled deletion fail; `/dev/null` is not a terminal.
- Help and quickstart match the three independent session facts. Fleet assertions
  consume JSON instead of obsolete display-column offsets. `resume --json` is
  supported. The default help remains 23 lines, at most 78 columns.

## Validation commands

Run from the isolated worktree with writable, executable Go build directories:

```sh
export GOCACHE="$PWD/.go-cache"
export GOMODCACHE=/tmp/rainier-go-mod
export GOTMPDIR="$PWD/.go-tmp"
make verify
go test -race -count=2 ./cmd/rainier/ ./internal/cli/
go test -race ./internal/e2e/ ./controlapp/ ./v0wire/ ./control/
go test -count=1 -v ./cmd/rainier -run '^TestCompiled(V0|Session)Fixtures$'
bash -n scripts/e2e-fleet.sh
git diff --check
```

Every command in the block above completed with exit 0. Changed Go files are
formatted. The separate live fleet result is recorded below.

`make verify` runs all three public/module static checks, `go test ./...`,
`bash scripts/build.sh -o bin/ ./cmd/...`, and `go vet ./...`.
The image qualification workflow does not apply: no image/build inputs changed.

The fixture tests compile and execute the CLI with synthetic local HTTP responses;
JSON stdout is decoded separately from stderr. Exact invocations and exits:

| Invocation | Exit |
| --- | --- |
| `rainier --help` | 0 |
| `rainier status` (compute ready, health unavailable) | 1 |
| `rainier status --json` (same fixture) | 1 |
| `rainier agent status --json` | 0 |
| `rainier new --agent claude --detach --json` (missing catalog) | 1 |
| `rainier delete sess_example` (no terminal/confirmation) | 2 |
| `rainier info sess_a/../sess_b --json` | 2 |
| `rainier diff` | 2 |
| `rainier ls` | 0 |
| `rainier ls --json` | 0 |
| `rainier info fixture --json` | 0 |
| `rainier stop sess_example --json` | 0 |
| `rainier resume sess_example --json` | 0 |
| `rainier delete sess_example --yes --json` | 0 |

The live fleet rehearsal was attempted with
`SKIP_GITHUB=1 bash scripts/e2e-fleet.sh`: **exit 2, Docker CLI not found**.
It is not a passing live-fleet result. The deterministic Go e2e suite is separate.

## Backend dependencies

Cloud main `05fbe23562292c4b69a12b0fdab2574911cd7dca` confirms the live compute
shape and create/resume readiness admission. It still lacks:

1. A bearer-reachable onboarding destination. The browser bootstrap's relative
   path is cookie-only. CLI status omits `Continue` rather than inventing a URL.
2. A per-environment launch catalog at `/v0/environments/{id}/agents`.
   `new --agent` fails before creating a session and offers an explicit command.

Both are isolated in `readiness.go`; no Cloud implementation is added here.
An authoritative environment default flag and session repository display metadata
are also absent. The CLI supports an unambiguous single environment and omits
repository metadata rather than reconstructing it.


## Completion pass

The CLI work was merged onto core main containing the default developer image
update (#72), retaining the original CLI PR history. Four independent review
findings now have regression coverage in `cmd/rainier/completion_test.go`:

- Explicit environment names are resolved to the same opaque ID for both the
  launch catalog read and session creation.
- Diagnostic attachment by name includes terminal records when `--since` is
  explicit, while ordinary attachment preserves its existing filter.
- Verbose listing uses the same credential and URL redaction as session details
  and JSON output.
- A warm suspension, including a concurrent one, never claims released capacity.

Fresh `make verify`, CLI/client race tests, and all completion regressions pass.
The real local fleet rehearsal also exits 0 using the documented native ARM
base override (`BUILD_ARGS='--build-arg BASE_IMAGE=node:22-bookworm'`). It covers
login, create/list/attach, stop/resume/delete, environment setup/snapshot reuse,
and agent credential restore/revocation. GitHub publishing was explicitly
skipped (`SKIP_GITHUB=1`); network enforcement cannot be qualified on VM-backed
macOS Docker and still requires native Linux. Neither skip is a passing check.
The default pinned amd64 base correctly rejects an implicit ARM target; this
local override is development evidence, not qualification of the shipping amd64
image.

The first full race run passed the previously reported keepalive case but hit
`TestPlacementPinQueuesWithReason` while the image was building. The placement
case then passed three isolated race runs. Record the final full-suite result
separately; an isolated pass does not erase the earlier timeout.

`.github/workflows/verify.yml` now runs general PR verification and CLI/client
race checks. Previously only image-related paths triggered CI.

The two Cloud APIs above remain outside this CLI PR. Releasing the complete
hosted shortcut workflow requires those APIs and an explicitly configured
`main.defaultServer` release linker value (or `RAINIER_SERVER` / `login --cloud`
for source builds). The web destination contract now matches `/app/workspace`
and `/app/sessions`; there is no separate onboarding UI.

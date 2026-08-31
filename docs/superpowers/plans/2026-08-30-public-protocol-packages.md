# Public Protocol Packages Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish Rainier's existing runner-control, terminal-attach, and workspace-transfer contracts as importable Go packages without changing their JSON bytes or self-hosted behavior.

**Architecture:** Three focused packages under `protocol/` become the single source of truth for wire-visible types. The change moves definitions and their contract tests rather than copying them, then rewrites every in-repository caller to consume the public paths; runtime orchestration, transport, persistence, and execution drivers remain internal. A repository guard prevents new imports of the retired internal protocol packages.

**Tech Stack:** Go 1.25, standard-library JSON and archive packages, Bash, existing Rainier test/build gates.

**Spec:** `rainier-cloud/docs/architecture/adr-0001-oss-cloud-composition.md` sections “Responsibility boundary,” “OSS application-service boundary,” “Versioning and rollout compatibility,” and “Migration sequence”; `rainier-cloud/docs/superpowers/plans/2026-08-30-hosted-implementation-program.md` gate O3.

## Global Constraints

- Work only in `.worktrees/public-protocol-packages` on `feat/public-protocol-packages`, created from freshly fetched `origin/main` after this plan is merged.
- Own this plan, `protocol/**`, the retired `internal/rwire/**`, `internal/wire/**`, `internal/xfer/**`, imports that refer to those packages, `scripts/check-public-protocols.sh`, and the `Makefile` protocol guard only.
- Do not export `internal/controld`, `internal/driver`, `internal/relay`, `internal/session`, or `internal/term`; do not define application-service, store, authorization, workspace-scope, capability-negotiation, or provider interfaces in this plan.
- Move each wire definition exactly once. No compatibility aliases, duplicate structs, forwarding packages, copied JSON types, or second protocol versions remain.
- Preserve every JSON field name, `omitempty` decision, numeric constant, unknown-field behavior, size bound, path rule, archive rule, and current call-site behavior byte for byte.
- Public packages are exactly `github.com/tokencanopy/rainier/protocol/runner`, `github.com/tokencanopy/rainier/protocol/terminal`, and `github.com/tokencanopy/rainier/protocol/workspace`.
- `runner.ProtocolVersion` is `1`. Capability negotiation and a rolling-version window are intentionally deferred to the later capabilities plan; this plan does not pretend version 1 already negotiates capabilities.
- Exported symbols have doc comments that state wire shape, invariants, error behavior, and compatibility expectations. Tests use only fictional IDs, paths, repositories, and `.test` or `example.com` values.
- Every task runs affected package tests, the public-protocol guard, `make verify`, and `git diff --check` before commit.

## File structure

```text
protocol/runner/
  messages.go               runner-control and session-RPC JSON vocabulary
  messages_test.go          external-package byte-shape and compatibility tests
protocol/terminal/
  messages.go               viewer input, snapshot/replay/output/exit vocabulary
  messages_test.go          external-package byte-shape and cursor tests
protocol/workspace/
  messages.go               diff and bounded push/pull RPC vocabulary
  path.go                   shared lexical and symlink containment rules
  archive.go                bounded safe tar/gzip pack and extract behavior
  messages_test.go          external-package JSON shape tests
  path_test.go              path and symlink contract tests
  archive_test.go           archive bounds and escape contract tests
protocol/doc.go             package documentation for the protocol namespace
protocol/import_test.go      one external consumer importing all three packages
scripts/check-public-protocols.sh
                              source-of-truth and forbidden-import guard
```

`protocol/*` contains contracts shared across binaries or modules. It contains no HTTP handler, WebSocket connection, scheduler, database, Docker, hosted identity, billing, or provider behavior.

---

### Task 1: Publish the runner-control protocol

**Files:**

- Create: `protocol/runner/messages.go`
- Create: `protocol/runner/messages_test.go`
- Delete: `internal/rwire/rwire.go`
- Delete: `internal/rwire/rwire_test.go`
- Modify: every Go file returned by `rg -l 'internal/rwire' cmd internal -g '*.go'`

**Interfaces:**

- Consumes: the exact JSON contract currently defined by `internal/rwire` and used between `controld` and `runnerd`.
- Produces: `runner.ProtocolVersion`, `runner.RPCEnvelope`, `runner.FromRunner`, `runner.SessionInfo`, `runner.ToRunner`, `runner.RepoSpec`, `runner.Spec`, and `runner.Attach` at the canonical public import path.

- [ ] **Step 1: Write external-package byte-contract tests**

Move the existing round-trip cases into `package runner_test`, import the public package, and add an exact fixture that proves a separate package can construct both directions:

```go
func TestPublicRunnerWireShapes(t *testing.T) {
	create := runner.ToRunner{
		Type: "create", ReqID: 7, Session: "sess_example",
		Spec: &runner.Spec{Image: "example.invalid/agent@sha256:0000", Cmd: []string{"bash"}},
	}
	got, err := json.Marshal(create)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"type":"create","req_id":7,"session":"sess_example","spec":{"image":"example.invalid/agent@sha256:0000","cmd":["bash"]}}`
	if string(got) != want {
		t.Fatalf("create JSON = %s, want %s", got, want)
	}
	if runner.ProtocolVersion != 1 {
		t.Fatalf("protocol version = %d, want 1", runner.ProtocolVersion)
	}
}
```

Keep the existing environment, repository/init, session-RPC, setup-event, and unknown-field cases. Replace `Proto` assertions with `ProtocolVersion`; do not change the JSON `proto` field.

- [ ] **Step 2: Run the new tests and verify the public package is absent**

Run:

```bash
go test ./protocol/runner
```

Expected: FAIL because `protocol/runner` does not exist yet.

- [ ] **Step 3: Move the source of truth and rewrite imports**

Move `internal/rwire/rwire.go` to `protocol/runner/messages.go`, change the package declaration to `package runner`, rename only the Go constant:

```go
const ProtocolVersion = 1
```

Keep every struct field and JSON tag unchanged. Rewrite active imports from:

```go
"github.com/tokencanopy/rainier/internal/rwire"
```

to:

```go
"github.com/tokencanopy/rainier/protocol/runner"
```

and update qualifiers (`rwire.ToRunner` to `runner.ToRunner`, `rwire.Proto` to `runner.ProtocolVersion`) mechanically. Do not change literal message types, states, RPC methods, payloads, or transport code.

- [ ] **Step 4: Verify byte compatibility and all affected callers**

Run:

```bash
gofmt -w protocol/runner $(rg -l 'protocol/runner' cmd internal -g '*.go')
go test ./protocol/runner ./internal/runnerd ./internal/controld ./cmd/sessiond
make verify
git diff --check
```

Expected: all commands pass. Review `git diff --find-renames=50% -- internal/rwire protocol/runner`; aside from package/import-path documentation and the Go constant rename, the contract fixtures prove the moved source has identical JSON behavior.

- [ ] **Step 5: Commit and stop**

```bash
git add protocol/runner internal/rwire cmd internal
git commit -m "feat: publish runner control protocol"
```

Report the commit, the number of rewritten imports, and every verification result, then stop.

---

### Task 2: Publish the terminal-attach protocol

**Files:**

- Create: `protocol/terminal/messages.go`
- Create: `protocol/terminal/messages_test.go`
- Delete: `internal/wire/wire.go`
- Delete: `internal/wire/wire_test.go`
- Modify: every Go file returned by `rg -l 'internal/wire' cmd internal -g '*.go'`

**Interfaces:**

- Consumes: the current viewer/session JSON contract and `SinceAll` cursor semantics from `internal/wire`.
- Produces: `terminal.SinceAll`, `terminal.ClientMessage`, and `terminal.ServerMessage` at the canonical public import path.

- [ ] **Step 1: Write external-package terminal fixtures**

Create `package terminal_test` tests with exact JSON for resize, stdin, snapshot, output, and exit. The cursor contract must include:

```go
func TestSinceAllIsReservedMaximum(t *testing.T) {
	if terminal.SinceAll != ^uint64(0) {
		t.Fatalf("SinceAll = %d, want max uint64", terminal.SinceAll)
	}
}

func TestResizeWireShape(t *testing.T) {
	b, err := json.Marshal(terminal.ClientMessage{Type: "resize", Cols: 80, Rows: 24})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"type":"resize","cols":80,"rows":24}` {
		t.Fatalf("resize JSON = %s", b)
	}
}
```

The snapshot/output fixtures must pin `seq`, base64 encoding of `data`, `cols`, and `rows`; the exit fixture must pin the existing camel-case `exitCode` tag.

- [ ] **Step 2: Run the tests and verify the public names are absent**

Run:

```bash
go test ./protocol/terminal
```

Expected: FAIL because the package and public names do not exist.

- [ ] **Step 3: Move the definitions and rewrite callers**

Move `internal/wire/wire.go` to `protocol/terminal/messages.go`, change the package to `terminal`, and rename the Go types without changing tags:

```go
type ClientMessage struct {
	Type string `json:"type"`
	Data []byte `json:"data,omitempty"`
	Cols int    `json:"cols,omitempty"`
	Rows int    `json:"rows,omitempty"`
}

type ServerMessage struct {
	Type     string `json:"type"`
	Seq      uint64 `json:"seq,omitempty"`
	Data     []byte `json:"data,omitempty"`
	Cols     int    `json:"cols,omitempty"`
	Rows     int    `json:"rows,omitempty"`
	ExitCode int    `json:"exitCode,omitempty"`
}
```

Rewrite imports to `github.com/tokencanopy/rainier/protocol/terminal` and qualifiers mechanically. Keep `Type` as a string in this extraction: adding a typed message vocabulary and validation behavior would be a separate compatibility change, not a package move.

- [ ] **Step 4: Verify attach, replay, and session behavior**

Run:

```bash
gofmt -w protocol/terminal $(rg -l 'protocol/terminal' cmd internal -g '*.go')
go test ./protocol/terminal ./internal/session ./internal/server ./internal/attachio ./internal/controld ./internal/cli ./cmd/rainier
make verify
git diff --check
```

Expected: all commands pass with the same snapshot/replay and late-exit behavior.

- [ ] **Step 5: Commit and stop**

```bash
git add protocol/terminal internal/wire cmd internal
git commit -m "feat: publish terminal attach protocol"
```

Report the commit and verification output, then stop.

---

### Task 3: Publish the workspace inspection and transfer protocol

**Files:**

- Create: `protocol/workspace/messages.go`
- Create: `protocol/workspace/path.go`
- Create: `protocol/workspace/archive.go`
- Create: `protocol/workspace/messages_test.go`
- Create: `protocol/workspace/path_test.go`
- Create: `protocol/workspace/archive_test.go`
- Delete: `internal/xfer/xfer.go`
- Delete: `internal/xfer/xfer_test.go`
- Modify: every Go file returned by `rg -l 'internal/xfer' cmd internal -g '*.go'`

**Interfaces:**

- Consumes: the existing `internal/xfer` methods, bounds, JSON messages, containment rules, and archive behavior.
- Produces: one public `workspace` package with the same exported method constants, bounds, messages, `ErrTooLarge`, `ValidatePath`, `Resolve`, `TarGz`, `UntarGz`, and `HumanBytes` behavior.

- [ ] **Step 1: Split the existing tests by responsibility before moving code**

Move tests without weakening assertions:

```text
messages_test.go  TestChunkWireShape plus a DiffAnswer empty-array fixture
path_test.go      TestValidatePath, TestResolveStaysUnderRoot,
                  TestResolveRefusesASymlinkOutOfTheRoot
archive_test.go   every TarGz/UntarGz round-trip, bound, irregular-file,
                  symlink-escape, decompression-bomb, and garbage test
```

Use `package workspace_test` and import the public path. Add this exact empty-diff contract:

```go
func TestEmptyDiffAnswerIsArray(t *testing.T) {
	b, err := json.Marshal(workspace.DiffAnswer{})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"repos":[]}` {
		t.Fatalf("empty diff JSON = %s", b)
	}
}
```

- [ ] **Step 2: Run the split tests and verify the package is absent**

Run:

```bash
go test ./protocol/workspace
```

Expected: FAIL because `protocol/workspace` does not exist.

- [ ] **Step 3: Move and split the implementation by responsibility**

Move the existing implementation without semantic edits:

```text
messages.go  method/bound constants, ErrTooLarge, message types,
             DiffAnswer.MarshalJSON, HumanBytes
path.go      ValidatePath, Resolve, and private containment helpers
archive.go   TarGz, UntarGz, limit/count helpers, archive entry checks
```

Change the package name to `workspace`. Keep these exact wire method values and limits:

```go
const (
	MethodDiff      = "diff"
	MethodPushFiles = "push_files"
	MethodPullFiles = "pull_files"
	MaxBytes        = int64(256 << 20)
	MaxExtractBytes = 4 * MaxBytes
	ChunkBytes      = 1 << 20
	SyncEvery       = 8
	StatBytes       = 64 << 10
	WorkspaceRoot   = "/workspace"
)
```

Rewrite imports and qualifiers from `xfer` to `workspace`. Do not relax repeated validation at the CLI, control plane, or sandbox hop.

- [ ] **Step 4: Verify security behavior and all consumers**

Run:

```bash
gofmt -w protocol/workspace $(rg -l 'protocol/workspace' cmd internal -g '*.go')
go test ./protocol/workspace ./internal/cli ./internal/controld ./cmd/sessiond ./cmd/rainier
go test ./protocol/workspace -race -count=5
make verify
git diff --check
```

Expected: all commands pass; malicious paths, escaping symlinks, oversized compressed or expanded archives, irregular files, and corrupt archives remain rejected.

- [ ] **Step 5: Commit and stop**

```bash
git add protocol/workspace internal/xfer cmd internal
git commit -m "feat: publish workspace transfer protocol"
```

Report the commit, the moved test count, and verification output, then stop.

---

### Task 4: Guard the public protocol source of truth

**Files:**

- Create: `protocol/doc.go`
- Create: `protocol/import_test.go`
- Create: `scripts/check-public-protocols.sh`
- Modify: `Makefile`

**Interfaces:**

- Consumes: the three public protocol packages completed by Tasks 1–3.
- Produces: an external import smoke test and a deterministic repository guard included in `make verify`.

- [ ] **Step 1: Write one external consumer smoke test**

Create `protocol/doc.go`:

```go
// Package protocol documents Rainier's public wire-contract namespace.
// Concrete contracts live in the runner, terminal, and workspace packages.
package protocol
```

Create `protocol/import_test.go` with `package protocol_test`:

```go
func TestPublicProtocolImports(t *testing.T) {
	_ = runner.ToRunner{Type: "destroy", Session: "sess_example"}
	_ = terminal.ClientMessage{Type: "resize", Cols: 80, Rows: 24}
	_ = workspace.PullRequest{Xfer: "xfer_example", Path: "src", Seq: 0}
}
```

It imports only the three canonical public paths and proves an external package does not need any `internal/` import.

- [ ] **Step 2: Write the repository guard and prove its negative cases**

Create executable `scripts/check-public-protocols.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

for pkg in runner terminal workspace; do
  test -f "protocol/$pkg/messages.go"
  go list "./protocol/$pkg" >/dev/null
done

if rg -n 'github.com/tokencanopy/rainier/internal/(rwire|wire|xfer)' \
  cmd internal protocol -g '*.go'; then
  echo "active Go imports must use the public protocol packages" >&2
  exit 1
fi

for retired in internal/rwire internal/wire internal/xfer; do
  if [[ -e "$retired" ]]; then
    echo "retired internal protocol package is still present" >&2
    exit 1
  fi
done
```

The final script must emit no absolute path and no output on success. Prove its negative case by adding exactly this temporary file with `apply_patch`:

```go
// protocol/forbidden_import_test.go
package protocol_test
import _ "github.com/tokencanopy/rainier/internal/wire"
```

Run `./scripts/check-public-protocols.sh` and expect nonzero with `active Go imports must use the public protocol packages`, then delete only `protocol/forbidden_import_test.go` with `apply_patch` and rerun the script to success.

- [ ] **Step 3: Add the guard to the aggregate gate**

Extend `Makefile`:

```make
.PHONY: protocols

protocols:
	./scripts/check-public-protocols.sh

verify: module-path protocols test build
	go vet ./...
```

Do not change `test`, `build`, `demo`, or `e2e` behavior.

- [ ] **Step 4: Run final O3 verification**

```bash
bash -n scripts/check-public-protocols.sh
chmod +x scripts/check-public-protocols.sh
./scripts/check-public-protocols.sh
go test ./protocol/... -race -count=5
make verify
git diff --check origin/main...HEAD
```

Expected: all commands pass and `rg -n 'internal/(rwire|wire|xfer)' --glob '*.go' cmd internal protocol` returns no imports.

- [ ] **Step 5: Commit and stop**

```bash
git add protocol scripts/check-public-protocols.sh Makefile
git commit -m "test: guard public protocol contracts"
```

Report the commit and every final gate, then stop.

## Final reviewer checks

- The three public packages are the only definitions of their JSON structs and constants; no alias or copied compatibility package remains.
- Exact JSON fixtures prove runner commands/events/RPC, terminal resize/snapshot/output/exit, workspace diff, and push/pull messages retain their bytes.
- Unknown runner fields remain tolerated, terminal cursor semantics are unchanged, and workspace archive/path security tests remain intact.
- No application-service, provider, hosted identity, billing, workspace-scope, or capability type leaked into `protocol/`.
- `go list github.com/tokencanopy/rainier/protocol/...`, `go test ./protocol/... -race -count=5`, `make verify`, and `git diff --check` pass.

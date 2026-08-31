# Canonical Go Module Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Complete one task, commit it, report the commit and verification output, and stop for reviewer approval before starting the next task.

**Goal:** Make Rainier's Go module importable as `github.com/tokencanopy/rainier` without changing runtime behavior or prematurely exporting internal packages.

**Architecture:** This is a repository identity change only. The module declaration and every active Go import move together in one commit, while a small verification script protects the canonical path from regressing. Package locations, interfaces, HTTP routes, protocol types, and behavior remain unchanged for the later extraction plans.

**Tech Stack:** Go 1.25, Bash, existing Rainier test/build gates.

**Spec:** `rainier-cloud/docs/architecture/adr-0001-oss-cloud-composition.md` sections “Versioning and rollout compatibility” and “Migration sequence”; `rainier-cloud/docs/superpowers/plans/2026-08-30-hosted-implementation-program.md` gate O2.

## Global Constraints

- Work only in `.worktrees/canonical-module-path` on `feat/canonical-module-path`, created from freshly fetched `origin/main`.
- Own `go.mod`, active `*.go` files, `Makefile`, `scripts/check-module-path.sh`, and this plan. Do not move packages or edit historical plan/spec examples.
- Change only the Go module/import identity. Do not change HTTP behavior, `/v0/`, exported names, package names, database schema, or protocol payloads.
- The exact canonical module is `github.com/tokencanopy/rainier`; active in-module imports use `github.com/tokencanopy/rainier/...`.
- GitHub-facing source, tests, commit messages, and output contain only synthetic data.
- Every task runs `gofmt`, the module-path guard, affected tests, the complete Go suite, build, vet, and `git diff --check`.

## File structure

```text
go.mod                       canonical module declaration
cmd/**, internal/**          active imports rewritten in place
scripts/check-module-path.sh repository identity contract
Makefile                     one verify target that includes the contract
```

The guard is intentionally a repository script, not a new Go package: module identity is a repository-level invariant and should not create runtime code or a fake public interface.

---

### Task 1: Canonical module declaration and active imports

**Files:**

- Modify: `go.mod`
- Modify: every `*.go` file returned by `rg -l '"rainier/internal/' --glob '*.go'`

**Interfaces:**

- Consumes: the current local module identity `rainier` and imports rooted at `rainier/internal/...`.
- Produces: the exact module identity `github.com/tokencanopy/rainier` and imports rooted at `github.com/tokencanopy/rainier/internal/...`.

- [ ] **Step 1: Capture the failing identity checks**

Run:

```bash
test "$(go list -m -f '{{.Path}}')" = github.com/tokencanopy/rainier
! rg -n '"rainier/internal/' --glob '*.go'
```

Expected: both checks fail on the unmodified branch because `go.mod` declares `module rainier` and active Go files import `rainier/internal/...`.

- [ ] **Step 2: Rewrite the declaration and imports mechanically**

Change the first line of `go.mod` to:

```go
module github.com/tokencanopy/rainier
```

For active Go source only, replace each import prefix:

```go
"rainier/internal/
```

with:

```go
"github.com/tokencanopy/rainier/internal/
```

Do not rewrite domain strings such as `rainier/<session-name>`, filesystem paths such as `.rainier`, comments that name the CLI, or historical Markdown plans.

- [ ] **Step 3: Format and verify the complete source tree**

Run:

```bash
gofmt -w $(rg --files cmd internal -g '*.go')
test "$(go list -m -f '{{.Path}}')" = github.com/tokencanopy/rainier
! rg -n '"rainier/internal/' --glob '*.go'
go test -p 1 ./...
go vet ./...
CGO_ENABLED=0 go build ./cmd/...
git diff --check
```

Expected: all commands pass. Test package labels now begin with `github.com/tokencanopy/rainier/` and behavior is otherwise unchanged.

- [ ] **Step 4: Commit and stop**

```bash
git add go.mod cmd internal
git commit -m "build: use canonical Rainier module path"
```

Report the commit hash, the count of rewritten Go files, and every verification command, then stop.

---

### Task 2: Repository-level module identity guard

**Files:**

- Create: `scripts/check-module-path.sh`
- Modify: `Makefile`

**Interfaces:**

- Consumes: repository root, `go.mod`, and active Go source under `cmd/` and `internal/`.
- Produces: `scripts/check-module-path.sh`, a deterministic zero-argument check; `make verify`, the aggregate local identity/test/build gate.

- [ ] **Step 1: Write the guard and prove it detects both regressions**

Create an executable Bash script with this contract:

```bash
#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
canonical="github.com/tokencanopy/rainier"

actual="$(cd "$repo_root" && go list -m -f '{{.Path}}')"
if [[ "$actual" != "$canonical" ]]; then
  echo "module path must be $canonical" >&2
  exit 1
fi

if rg -n '"rainier/internal/' "$repo_root/cmd" "$repo_root/internal" -g '*.go'; then
  echo "active Go imports must use the canonical module path" >&2
  exit 1
fi
```

The error messages must not print environment variables or local paths beyond the matched source locations emitted by `rg`.

Before keeping the script, temporarily substitute a noncanonical module path in a copy of `go.mod` under a temporary directory and confirm the script logic exits nonzero; do not modify the worktree's `go.mod` for this negative check.

- [ ] **Step 2: Add one aggregate Make target**

Extend `Makefile` without changing `test`, `build`, `demo`, or `e2e` semantics:

```make
.PHONY: verify module-path

module-path:
	./scripts/check-module-path.sh

verify: module-path test build
	go vet ./...
```

- [ ] **Step 3: Run the final contract gates**

```bash
bash -n scripts/check-module-path.sh
chmod +x scripts/check-module-path.sh
./scripts/check-module-path.sh
make verify
git diff --check
```

Expected: every command passes and the script emits no output on success.

- [ ] **Step 4: Commit and stop**

```bash
git add Makefile scripts/check-module-path.sh
git commit -m "test: guard canonical Rainier module identity"
```

Report the commit hash and gate results, then stop.

## Final reviewer checks

- `git diff origin/main...HEAD --stat` contains only the plan, module declaration, import rewrites, guard, and Makefile wiring.
- `rg -n 'module rainier$|"rainier/internal/' go.mod cmd internal` returns no matches.
- No package moved and `go list ./...` reports the same package set under the canonical prefix.
- `go test -p 1 ./...`, `go vet ./...`, `CGO_ENABLED=0 go build ./cmd/...`, `make verify`, and `git diff --check` pass.

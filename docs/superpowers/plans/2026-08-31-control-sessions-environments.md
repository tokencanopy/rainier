# Control Sessions and Environments Extraction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement Rainier's portable session commands, guarded lifecycle operations, and environment behavior behind the frozen `control.Sessions` and `control.Environments` interfaces.

**Architecture:** Add two deep modules in the public `controlapp` package: `SessionService` owns session creation, authorization, lifecycle dispatch, and result recording; `EnvironmentService` owns reusable environment CRUD and setup-checkpoint invalidation. They depend only on the frozen `control` ports and public runner protocol, while HTTP, identity, SQL, Docker, GitHub, provider, and billing behavior remain adapters outside the seam.

**Tech Stack:** Go 1.25, `github.com/tokencanopy/rainier/control`, `github.com/tokencanopy/rainier/protocol/runner`, standard library, existing Rainier verification gates.

**Spec:** `rainier-cloud/docs/architecture/adr-0001-oss-cloud-composition.md`, `rainier-cloud/docs/superpowers/plans/2026-08-30-hosted-implementation-program.md` gates O4-O8 and Wave 4, and `docs/superpowers/plans/2026-08-31-control-application-interfaces.md`.

## Global Constraints

- Work only in `.worktrees/control-sessions-environments` on `feat/control-sessions-environments`, created from freshly fetched `origin/main` after the three Wave 4 plan documents merge.
- Own only `controlapp/sessions.go`, `controlapp/sessions_test.go`, `controlapp/environments.go`, `controlapp/environments_test.go`, and `controlapp/sessions_external_test.go`.
- Do not edit `control/*.go`, `internal/controld/controld.go`, another Wave 4 lane's files, protocols, `go.mod`, `Makefile`, migrations, HTTP handlers, or existing self-hosted tests.
- Do not delete or redirect existing `internal/controld` behavior. The later recomposition plan performs the cutover after all three extracted modules merge.
- The module imports no HTTP/WebSocket, SQL/pgx, Docker, GitHub SDK, cloud SDK, billing package, or `internal/` package.
- Every operation validates `control.Scope`, uses its authoritative `WorkspaceID`, and invokes `control.Authorizer` before returning state or causing an external side effect.
- Cross-workspace lookups remain `control.ErrNotFound`; authorization denials remain `control.ErrDenied`. Do not reveal which condition occurred in free-form text.
- Every collection crossing the seam is copied. Preserve `CreateSession.Repos` nil versus non-nil empty semantics.
- Ordinary callers never choose a provider, VM shape, zone, disk, cluster, CPU size, or memory size. `PoolResolver` and environment requirements select eligible capacity.
- Runner commands use only the public `runner.ToRunner`/`runner.FromRunner` types. A false runner result and an unavailable transport map to a closed `control` sentinel.
- Record one provider-neutral `control.Event` after each successful mutation. Until the later transactional-outbox plan, an event-recording failure returns `control.ErrUnavailable` and the persisted row remains authoritative; tests must re-read rather than fabricate state.
- Use only synthetic `.test`, `.invalid`, `example.com`, `agents.localhost`, and fictional opaque IDs in tests and commits.

## File structure

```text
controlapp/
  sessions.go                SessionService, options, create/query/lifecycle behavior
  sessions_test.go           direct interface tests with lane-owned fakes
  environments.go            EnvironmentService, setup hashing, guarded CRUD
  environments_test.go       environment authorization and lifecycle tests
  sessions_external_test.go  external-package construction and import guard
```

`SessionService` and `EnvironmentService` are independently constructible so this lane compiles before the final aggregate `control.Application` composer exists. The recomposition plan will embed both with the Fleet and Attachment modules and hide their wiring from normal callers.

## Behavior intentionally outside this lane

- Connector-specific JSON validation, secret-value lookup, Git identity, and credential checks remain host-adapter behavior; the portable environment stores only bounded opaque connectors and secret names.
- Queued placement and create dispatch belong to the Fleet lane. This lane selects the eligible pool, commits the queued session, and emits one wake only.
- HTTP defaults and response wording remain self-hosted/Cloud adapter behavior. This module returns only closed `control` sentinels and portable values.

---

### Task 1: Implement authorized session creation and queries

**Files:**
- Create: `controlapp/sessions.go`
- Create: `controlapp/sessions_test.go`

**Interfaces:**
- Consumes: `control.Authorizer`, `control.SessionRepository`, `control.EnvironmentRepository`, `control.PoolResolver`, `control.EventRecorder`, `control.Clock`, and `control.IDGenerator`.
- Produces: `NewSessionService(SessionOptions) (*SessionService, error)` and the `CreateSession`, `GetSession`, and `ListSessions` methods of `control.Sessions`.
- Produces for final composition: `SessionOptions.Wake func(control.PoolID)`, called only after a queued session is durably stored.

- [ ] **Step 1: Write constructor and compile-time interface tests**

Create `controlapp/sessions_test.go` in `package controlapp` with lane-prefixed fakes and this first contract:

```go
func TestNewSessionServiceRequiresEveryDependency(t *testing.T) {
	valid := validSessionOptions()
	if _, err := NewSessionService(valid); err != nil {
		t.Fatalf("NewSessionService(valid): %v", err)
	}
	valid.Sessions = nil
	if _, err := NewSessionService(valid); !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("missing repository: got %v, want ErrInvalid", err)
	}
}

type sessionCreationAndQueries interface {
	CreateSession(context.Context, control.Scope, control.CreateSession) (control.Session, error)
	GetSession(context.Context, control.Scope, control.SessionID) (control.Session, error)
	ListSessions(context.Context, control.Scope, control.SessionQuery) (control.SessionPage, error)
}

var _ sessionCreationAndQueries = (*SessionService)(nil)
```

`validSessionOptions` must use a fixed clock (`2026-01-02T03:04:05Z`), IDs `sess_example`/`evt_example`, a no-op recorder, and a wake function that records pool IDs. Give every fake a call log so tests can prove authorization and durability order.

- [ ] **Step 2: Run the constructor test and observe the expected failure**

Run:

```bash
go test ./controlapp -run TestNewSessionServiceRequiresEveryDependency -count=1
```

Expected: FAIL because `controlapp` and `NewSessionService` do not exist.

- [ ] **Step 3: Add the exact construction seam**

Create `controlapp/sessions.go` with these types:

```go
package controlapp

type SessionOptions struct {
	Authorizer   control.Authorizer
	Sessions     control.SessionRepository
	Environments control.EnvironmentRepository
	Pools        control.PoolResolver
	Events       control.EventRecorder
	Clock        control.Clock
	IDs          control.IDGenerator
	Wake         func(control.PoolID)
}

type SessionService struct {
	auth control.Authorizer
	sessions control.SessionRepository
	environments control.EnvironmentRepository
	pools control.PoolResolver
	events control.EventRecorder
	clock control.Clock
	ids control.IDGenerator
	wake func(control.PoolID)
}

func NewSessionService(o SessionOptions) (*SessionService, error) {
	if o.Authorizer == nil || o.Sessions == nil || o.Environments == nil ||
		o.Pools == nil || o.Events == nil || o.Clock == nil || o.IDs == nil || o.Wake == nil {
		return nil, control.ErrInvalid
	}
	return &SessionService{auth: o.Authorizer, sessions: o.Sessions,
		environments: o.Environments, pools: o.Pools, events: o.Events,
		clock: o.Clock, ids: o.IDs, wake: o.Wake}, nil
}
```

Keep dependencies private. Do not add getters or expose the concrete fields.

- [ ] **Step 4: Pin create authorization, idempotency, environment resolution, and pool selection**

Add table tests proving:

- invalid scope and contradictory environment/scratch input touch no port;
- `ActionCreate` authorization occurs before the idempotency lookup and before storage;
- an existing `(workspace, creator, idempotency key)` returns the existing row without minting an ID, recording another event, or waking the scheduler;
- an environment is read only from `scope.WorkspaceID`, and a missing environment returns `ErrNotFound` without creating a session;
- nil `Repos` stays nil while `[]control.RepoRef{}` stays non-nil and empty;
- environment image, egress list, requirements, and current snapshot (`SnapshotHash == SetupHash`) are resolved into the stored portable session without aliasing the environment's slices;
- scratch requirements are zero and never produce a user-facing size choice;
- pool selection chooses the eligible pool with the greatest `CapacityTotal-CapacityUsed`, breaking ties by ascending `PoolID`; no positive-capacity pool returns `ErrUnavailable`;
- the stored session has the generated ID, authoritative workspace/creator, `StateQueued`, chosen pool, `PlacementGeneration: 1`, and fixed timestamps;
- the store call precedes event recording, which precedes `Wake(pool)`.

Use this core assertion rather than testing private helpers:

```go
got, err := svc.CreateSession(ctx, scope, control.CreateSession{
	Name: "investigate", EnvironmentID: "env_example",
	Repos: []control.RepoRef{}, IdempotencyKey: "idem_example",
})
if err != nil { t.Fatal(err) }
if got.WorkspaceID != "ws_example" || got.CreatorID != "act_example" ||
	got.State != control.StateQueued || got.PoolID != "pool_a" ||
	got.PlacementGeneration != 1 {
	t.Fatalf("created session = %+v", got)
}
if got.Spec.Repos == nil || len(got.Spec.Repos) != 0 {
	t.Fatalf("explicit empty repos lost: %#v", got.Spec.Repos)
}
```

- [ ] **Step 5: Implement create with stable copies and closed errors**

Implement `CreateSession` in this order:

```go
if err := scope.Validate(); err != nil { return control.Session{}, control.ErrInvalid }
if err := cmd.Validate(); err != nil { return control.Session{}, control.ErrInvalid }
createResource := control.Resource{Kind: control.ResourceSession,
	WorkspaceID: scope.WorkspaceID, CreatorID: scope.Actor.ID}
if err := s.auth.Authorize(ctx, scope, control.ActionCreate, createResource); err != nil {
	return control.Session{}, control.ErrDenied
}
```

If `IdempotencyKey` is non-empty, query `SessionByIDem`; return a copied existing row on success, continue only on `ErrNotFound`, and safely propagate another closed sentinel. Resolve an environment only by ID within the authoritative workspace. Select a pool with a private deterministic helper over a copied slice. Create and store the queued row, record `ActionCreate`, then wake its pool. Validate that generated IDs are non-empty; otherwise return `ErrUnavailable` before storage.

Use private `cloneSession`, `cloneEnvironment`, `clonePortableSpec`, and `copyCheckpoint` helpers in this file. Copy `Cmd`, `EgressAllow`, `Repos`, `Capabilities`, connector bytes, and every pointer field.

- [ ] **Step 6: Pin and implement scoped reads**

Write tests for `GetSession` and `ListSessions` proving a denied caller receives no row/page, a cross-workspace repository miss is `ErrNotFound`, list authorization occurs before repository access, invalid limits/cursors remain repository concerns, and returned pages cannot mutate stored slices.

Implement resource construction exactly once:

```go
func sessionResource(row control.Session) control.Resource {
	return control.Resource{Kind: control.ResourceSession, WorkspaceID: row.WorkspaceID,
		ID: string(row.ID), CreatorID: row.CreatorID}
}
```

`GetSession` reads the workspace-keyed row, authorizes it, and only then returns a copy. `ListSessions` authorizes `ActionList` against a workspace-scoped empty-ID session resource before calling the repository, then returns a non-nil empty slice for an empty page.

- [ ] **Step 7: Run the task gate and commit**

Run:

```bash
gofmt -w controlapp/sessions.go controlapp/sessions_test.go
go test ./controlapp -run 'Test(NewSessionService|CreateSession|GetSession|ListSessions)' -race -count=5
go vet ./controlapp
git diff --check
```

Commit:

```bash
git add controlapp/sessions.go controlapp/sessions_test.go
git commit -m "feat: extract session creation and queries"
```

### Task 2: Implement guarded lifecycle commands

**Files:**
- Modify: `controlapp/sessions.go`
- Modify: `controlapp/sessions_test.go`

**Interfaces:**
- Consumes: `control.RunnerTransport` and `control.FleetRepository`, added to `SessionOptions` without changing any frozen control interface.
- Produces: `DeleteSession`, `SuspendSession`, `ResumeSession`, and `SnapshotSession` with the existing guarded state-machine semantics.

- [ ] **Step 1: Extend the constructor and write failing state-table tests**

Add `Fleet control.FleetRepository` and `Transport control.RunnerTransport` to `SessionOptions` and require both. Add table-driven tests covering:

```go
tests := []struct {
	name string
	state control.SessionState
	wantTo control.SessionState
	wantCommand string
	wantErr error
}{
	{"delete queued", control.StateQueued, control.StateCanceled, "", nil},
	{"delete creating", control.StateCreating, "", "", control.ErrConflict},
	{"delete running", control.StateRunning, control.StateDestroyed, "destroy", nil},
	{"delete failed", control.StateFailed, control.StateDestroyed, "destroy", nil},
	{"delete destroyed idempotent", control.StateDestroyed, control.StateDestroyed, "", nil},
}
```

Add `var _ control.Sessions = (*SessionService)(nil)` in this task, after all seven methods exist.

Every mutating case must also prove: get row, authorize exact resource/action, validate state, dispatch if required, guarded transition, record event, wake pool. Denial must prevent dispatch and transition.

- [ ] **Step 2: Run the lifecycle tests and observe method failures**

Run:

```bash
go test ./controlapp -run 'Test(Delete|Suspend|Resume|Snapshot)Session' -count=1
```

Expected: FAIL because the lifecycle methods are not implemented and the compile-time `control.Sessions` assertion is incomplete.

- [ ] **Step 3: Implement runner dispatch and authoritative re-read**

Add one private command helper:

```go
func (s *SessionService) dispatch(ctx context.Context, row control.Session, msg runner.ToRunner) (runner.FromRunner, error) {
	if row.PoolID == "" || row.RunnerID == "" || !s.transport.Connected(row.PoolID, row.RunnerID) {
		return runner.FromRunner{}, control.ErrUnavailable
	}
	res, err := s.transport.Dispatch(ctx, row.PoolID, row.RunnerID, msg)
	if err != nil { return runner.FromRunner{}, control.ErrUnavailable }
	if !res.OK { return runner.FromRunner{}, control.ErrUnavailable }
	return res, nil
}
```

For a transition conflict after a successful runner side effect, re-read the row and return that authoritative state; never mutate the previously read struct and pretend it was committed.

- [ ] **Step 4: Implement delete, suspend, and resume**

Preserve these rules:

- delete of queued transitions only `queued -> canceled`;
- delete of creating returns `ErrConflict` and sends nothing;
- delete of a terminal state other than failed is row-idempotent;
- running, warm/cold suspended, and failed dispatch `destroy` when connected, then transition to destroyed;
- send `remove_workspace` after explicit deletion of a placed session; treat it as idempotent cleanup and do not convert an already successful destroy into a false state;
- suspend accepts only running and dispatches `{Type:"suspend", Warm:cmd.Warm}` before transitioning to the matching warm/cold state;
- resume accepts only warm/cold suspended; cold resume computes free capacity on the same runner as `CapacityTotal-CapacityUsed-len(creating)` and returns `ErrConflict` when no slot exists;
- resume dispatches before the guarded transition to running;
- every successful mutation records the corresponding event and wakes the row's pool.

Use typed IDs only at the seam and convert to strings only in `runner.ToRunner.Session`.

- [ ] **Step 5: Implement snapshots and safe checkpoint metadata**

Allow snapshots only from running, warm-suspended, or cold-suspended sessions. Dispatch:

```go
runner.ToRunner{Type: "snapshot", Session: string(row.ID)}
```

Reject an empty successful `Detail` as `ErrUnavailable`; otherwise return:

```go
control.Checkpoint{Ref: res.Detail, Format: "rainier-runner-v0",
	Capabilities: []string{"workspace"}}
```

Record `ActionSnapshot` without putting the opaque reference in the event. Tests must prove runner error detail and checkpoint references never enter returned free-form errors.

- [ ] **Step 6: Run the lifecycle gate and commit**

Run:

```bash
gofmt -w controlapp/sessions.go controlapp/sessions_test.go
go test ./controlapp -run 'Test(Delete|Suspend|Resume|Snapshot)Session' -race -count=10
go test ./controlapp -race
go vet ./controlapp
git diff --check
```

Commit:

```bash
git add controlapp/sessions.go controlapp/sessions_test.go
git commit -m "feat: extract guarded session lifecycle"
```

### Task 3: Implement environment behavior

**Files:**
- Create: `controlapp/environments.go`
- Create: `controlapp/environments_test.go`

**Interfaces:**
- Consumes: `control.Authorizer`, `control.EnvironmentRepository`, `control.EventRecorder`, `control.Clock`, and `control.IDGenerator`.
- Produces: `NewEnvironmentService(EnvironmentOptions) (*EnvironmentService, error)` implementing all of `control.Environments`.

- [ ] **Step 1: Write the external behavior matrix**

Create constructor and compile-time tests:

```go
var _ control.Environments = (*EnvironmentService)(nil)

func TestCreateEnvironmentPinsSetupHash(t *testing.T) {
	svc := newEnvironmentFixture(t)
	got, err := svc.CreateEnvironment(ctx, scope, control.CreateEnvironment{
		Name: "standard", Image: "registry.example.invalid/rainier@sha256:0000",
		Setup: "make bootstrap", EgressAllow: []string{"example.com"},
	})
	if err != nil { t.Fatal(err) }
	if got.ID != "env_example" || got.WorkspaceID != "ws_example" || got.SetupHash == "" {
		t.Fatalf("environment = %+v", got)
	}
}
```

Add cases for invalid scope; empty name/image; negative timeouts/resources; authorization denial before storage; duplicate-name conflict; workspace-scoped get/list; update pointer optionality; setup-hash change making the existing snapshot stale without erasing its visible metadata; delete refusal while a non-terminal session references the environment; and immutable return copies.

Pin update optionality and delete safety with direct calls:

```go
empty := []string{}
updated, err := svc.UpdateEnvironment(ctx, scope, control.UpdateEnvironment{
	ID: "env_example", EgressAllow: &empty,
})
if err != nil { t.Fatal(err) }
if updated.EgressAllow == nil || len(updated.EgressAllow) != 0 {
	t.Fatalf("explicit clear lost: %#v", updated.EgressAllow)
}

repo.liveSessionCount = 1
if err := svc.DeleteEnvironment(ctx, scope, control.DeleteEnvironment{ID:"env_example"});
	!errors.Is(err, control.ErrConflict) {
	t.Fatalf("delete with live session: got %v, want ErrConflict", err)
}
```

- [ ] **Step 2: Run the tests and observe the missing module**

Run:

```bash
go test ./controlapp -run 'Test(Create|Get|List|Update|Delete)Environment' -count=1
```

Expected: FAIL because `EnvironmentService` does not exist.

- [ ] **Step 3: Implement the construction seam and deterministic setup hash**

Define:

```go
type EnvironmentOptions struct {
	Authorizer control.Authorizer
	Environments control.EnvironmentRepository
	Events control.EventRecorder
	Clock control.Clock
	IDs control.IDGenerator
}

type EnvironmentService struct {
	auth control.Authorizer
	environments control.EnvironmentRepository
	events control.EventRecorder
	clock control.Clock
	ids control.IDGenerator
}
```

`NewEnvironmentService` returns `ErrInvalid` for a missing dependency. Implement the current setup identity exactly:

```go
func setupHash(image, setup string) string {
	h := sha256.New()
	_, _ = io.WriteString(h, image)
	_, _ = h.Write([]byte{0})
	_, _ = io.WriteString(h, setup)
	return hex.EncodeToString(h.Sum(nil))
}
```

Do not include init, egress, connectors, secrets, requirements, or timeouts in this hash: init runs per boot and the snapshot caches only image plus setup.

- [ ] **Step 4: Implement create, get, and list**

`CreateEnvironment` validates all scalar bounds before generating an ID, authorizes `ActionCreate` on a workspace-scoped environment resource, copies every input, computes `SetupHash`, stores, records, and returns a copy. `GetEnvironment` reads by `(workspace,id)`, authorizes `ActionGet`, and returns a copy. `ListEnvironments` authorizes before repository access and normalizes an empty page to `[]`.

Validation is exact and provider-neutral:

- name and image are non-empty;
- setup/init timeout seconds are non-negative;
- CPU, memory, and disk minima are non-negative;
- capability and secret-reference entries are non-empty;
- connector `Type` is non-empty and `Raw` is valid JSON, but connector-specific schemas remain adapter work.

- [ ] **Step 5: Implement patch-style update and guarded delete**

`UpdateEnvironment` reads the current row, authorizes `ActionUpdate`, applies only non-nil fields, validates the complete result, recomputes `SetupHash`, preserves the old `Snapshot` and `SnapshotHash`, stores, records, and returns the repository's row. Preserving mismatched snapshot metadata makes staleness observable; consumers use it only when `SnapshotHash == SetupHash`.

`DeleteEnvironment` reads and authorizes the environment, then calls:

```go
n, err := s.environments.CountSessionsByEnvironment(
	ctx, scope.WorkspaceID, cmd.ID, control.NonTerminal,
)
```

Return `ErrConflict` when `n != 0`; otherwise delete and record `ActionDelete`. A repository `ErrNotFound` remains `ErrNotFound` even if another workspace contains the same opaque ID.

- [ ] **Step 6: Run the environment gate and commit**

Run:

```bash
gofmt -w controlapp/environments.go controlapp/environments_test.go
go test ./controlapp -run 'Test(Create|Get|List|Update|Delete)Environment' -race -count=10
go test ./controlapp -race
go vet ./controlapp
git diff --check
```

Commit:

```bash
git add controlapp/environments.go controlapp/environments_test.go
git commit -m "feat: extract environment behavior"
```

### Task 4: Prove the lane is independently consumable

**Files:**
- Create: `controlapp/sessions_external_test.go`

**Interfaces:**
- Consumes: the two constructors produced in Tasks 1-3.
- Produces: an external-package proof that a separate module can construct and call both deep modules without importing Rainier internals.

- [ ] **Step 1: Add the external-package smoke test**

Use `package controlapp_test`, implement only synthetic fakes of the frozen ports, and compile these assignments:

```go
var _ control.Sessions = mustSessions()
var _ control.Environments = mustEnvironments()
```

Call `CreateSession` and `CreateEnvironment` through their interfaces. Assert only observable calls and results; do not inspect concrete private fields.

- [ ] **Step 2: Run import and repository guards**

Run:

```bash
if rg -n 'internal/|net/http|nhooyr|pgx|docker|github|cloud.google|billing' controlapp/sessions.go controlapp/environments.go; then exit 1; fi
bash scripts/check-public-protocols.sh
bash scripts/check-public-control.sh
go test ./controlapp -race -count=10
make verify
git diff --check origin/main...HEAD
```

Expected: every command succeeds. The public-control guard may continue printing only the pre-existing migration inventory; this lane must add no entry.

- [ ] **Step 3: Review the lane and commit the smoke test**

Confirm the deletion test: removing either module would force authorization, workspace scoping, lifecycle rules, stable copying, event facts, and error mapping back into every caller. Remove any exported helper that does not serve a real caller.

Commit:

```bash
git add controlapp/sessions_external_test.go
git commit -m "test: verify session application seam"
```

## Acceptance checklist

- `SessionService` implements every `control.Sessions` method and `EnvironmentService` implements every `control.Environments` method.
- Authorization precedes disclosure or runner side effects; workspace scope reaches every repository call.
- Create is durable before event/wake, idempotency is creator/workspace scoped, and repository overrides preserve nil versus empty.
- Lifecycle commands preserve guarded transitions and return authoritative persisted state after races.
- Environment setup hashes and stale snapshots retain current semantics.
- No hosted identity, secret value, GitHub credential, provider identifier, pricing field, HTTP behavior, or storage technology appears in `controlapp`.
- Existing self-hosted behavior remains untouched until recomposition.

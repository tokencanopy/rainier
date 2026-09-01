# Control Fleet, Scheduling, and Reconciliation Extraction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement provider-neutral runner registration, generation fencing, reconciliation, event application, and queued-session placement behind the frozen `control.Fleet` interface.

**Architecture:** Add one deep `FleetService` module in the public `controlapp` package. The module owns fleet truth and within-pool scheduling while injected adapters own connections, persistence, eligible-pool policy, sensitive launch material, and provider execution; a small wake channel hides the scheduler loop from callers and lets final composition connect durable session creation to prompt placement.

**Tech Stack:** Go 1.25, `github.com/tokencanopy/rainier/control`, public runner protocol, standard library concurrency primitives, existing Rainier verification gates.

**Spec:** `rainier-cloud/docs/architecture/adr-0001-oss-cloud-composition.md`, `rainier-cloud/docs/superpowers/plans/2026-08-30-hosted-implementation-program.md` gates O4-O8 and Wave 4, and `docs/superpowers/plans/2026-08-31-control-application-interfaces.md`.

## Global Constraints

- Work only in `.worktrees/control-fleet-reconciliation` on `feat/control-fleet-reconciliation`, created from freshly fetched `origin/main` after the three Wave 4 plan documents merge.
- Own only `controlapp/fleet.go`, `controlapp/fleet_test.go`, `controlapp/scheduler.go`, `controlapp/scheduler_test.go`, and `controlapp/fleet_external_test.go`.
- Do not edit `control/*.go`, `internal/controld/controld.go`, another Wave 4 lane's files, protocols, `go.mod`, `Makefile`, migrations, transports, or existing self-hosted tests.
- Do not delete or redirect existing `internal/controld` behavior. The sequential recomposition plan performs the cutover.
- The module imports no HTTP/WebSocket, SQL/pgx, Docker, GitHub SDK, cloud SDK, billing package, or `internal/` package.
- Runner, pool, workspace, and placement generations are opaque Rainier identities. No provider project, zone, machine type, cluster, disk, or provider response crosses this module.
- A stale registration, snapshot, or event never mutates state, dispatches work, records usage, or discloses another workspace's session.
- Scheduling is FIFO per pool, single-threaded per `FleetService`, and recomputes capacity after every successful `queued -> creating` transition. Dispatch may run concurrently only after a durable guarded placement.
- A delivered create with an uncertain result is never duplicated. Requeue only when `RunnerTransport.Connected` is false after the dispatch failure; leave a live-connection timeout in `creating` for an event or reconciliation to settle.
- `LaunchMaterialResolver` is the one real adapter seam introduced by this plan: self-hosted and Cloud resolve secret values and source-control attribution differently. It returns in-memory material only; values are never stored, logged, included in errors, events, or test output.
- Every collection crossing the seam is copied, every output is stably sorted, and all task/event processing is idempotent.
- Use only synthetic `.test`, `.invalid`, `example.com`, `agents.localhost`, and fictional opaque IDs in tests and commits.

## File structure

```text
controlapp/
  fleet.go                FleetService, registration, fencing, reconciliation, events
  fleet_test.go           generation/workspace/reconciliation contract tests
  scheduler.go            wake loop, capacity selection, create dispatch/spec building
  scheduler_test.go       FIFO, capacity, retry, and uncertain-delivery tests
  fleet_external_test.go  external-package construction and import guard
```

`FleetService` implements `control.Fleet` and additionally exposes `Wake(control.PoolID)` and `Run(context.Context) error` for the final composer. Those lifecycle methods are implementation wiring, not additions to the frozen caller-facing interface.

## Behavior intentionally outside this lane

- The public runner event currently carries one runner authority generation, not a separate placement generation. This lane fences what the frozen message represents; the next sequential schema/generation plan adds placement-generation transport and persistence.
- Automatic `setup_done` environment-cache publication is not representable by the frozen `RunnerEvent` state vocabulary. The self-hosted runner adapter retains that translation until the checkpoint/capability plan introduces one portable checkpoint-result contract.
- Source-control connector decoding, secret values, and commit attribution stay in `LaunchMaterialResolver`; scheduling controls when material is requested and guarantees it is never persisted.
- Runner WebSocket authentication, request correlation, connection replacement, and reconnect mechanics remain transport-adapter behavior.

---

### Task 1: Implement runner registration and scoped listing

**Files:**
- Create: `controlapp/fleet.go`
- Create: `controlapp/fleet_test.go`

**Interfaces:**
- Consumes: `control.Authorizer`, `control.SessionRepository`, `control.EnvironmentRepository`, `control.FleetRepository`, `control.PoolResolver`, `control.RunnerTransport`, `control.EventRecorder`, `control.Clock`, and `control.IDGenerator`.
- Produces: `NewFleetService(FleetOptions) (*FleetService, error)`, `RegisterRunner`, `ListRunners`, and a non-blocking `Wake(control.PoolID)`.

- [ ] **Step 1: Write constructor and registration tests**

Create lane-prefixed fakes with call logs and add:

```go
type runnerRegistrationAndListing interface {
	RegisterRunner(context.Context, control.RunnerRegistration) (control.RunnerRegistrationResult, error)
	ListRunners(context.Context, control.Scope, control.RunnerQuery) (control.RunnerPage, error)
}

var _ runnerRegistrationAndListing = (*FleetService)(nil)

func TestRegisterRunnerRejectsStaleGeneration(t *testing.T) {
	fx := newFleetFixture(t)
	fx.fleet.runners["pool_example"] = []control.Runner{{
		ID: "runner_example", PoolID: "pool_example", Generation: 8,
		CapacityTotal: 4, Connected: true,
	}}
	got, err := fx.service.RegisterRunner(ctx, control.RunnerRegistration{
		WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_example",
		Generation: 7, CapacityTotal: 4,
	})
	if err != nil { t.Fatal(err) }
	if got.Accepted || got.Generation != 8 { t.Fatalf("result = %+v", got) }
	if fx.fleet.upsertCalls != 0 { t.Fatal("stale registration mutated the store") }
}
```

Add cases for empty IDs, zero generation, negative/over-total capacity, duplicate capabilities, same-generation idempotent reconnect, newer generation replacement, adapter-level `ErrStale`, copied session/capability slices, and wake only after accepted upsert.

- [ ] **Step 2: Run the tests and observe the expected package failure**

Run:

```bash
go test ./controlapp -run 'Test(NewFleetService|RegisterRunner)' -count=1
```

Expected: FAIL because `FleetService` is absent.

- [ ] **Step 3: Add the exact construction seam**

Define:

```go
type FleetOptions struct {
	Authorizer control.Authorizer
	Sessions control.SessionRepository
	Environments control.EnvironmentRepository
	Fleet control.FleetRepository
	Pools control.PoolResolver
	Transport control.RunnerTransport
	Events control.EventRecorder
	Clock control.Clock
	IDs control.IDGenerator
	SafetyInterval time.Duration
}

type FleetService struct {
	auth control.Authorizer
	sessions control.SessionRepository
	environments control.EnvironmentRepository
	fleet control.FleetRepository
	pools control.PoolResolver
	transport control.RunnerTransport
	events control.EventRecorder
	clock control.Clock
	ids control.IDGenerator
	safetyInterval time.Duration
	wake chan control.PoolID
}
```

`NewFleetService` requires every port, requires a positive safety interval, and creates `wake` with capacity 64. No goroutine starts in the constructor.

- [ ] **Step 4: Implement validation and fenced registration**

Validate a registration before touching a port:

```go
if r.WorkspaceID == "" || r.PoolID == "" || r.RunnerID == "" || r.Generation == 0 ||
	r.CapacityUsed < 0 || r.CapacityTotal < 0 || r.CapacityUsed > r.CapacityTotal {
	return control.RunnerRegistrationResult{}, control.ErrInvalid
}
```

Read the pool's runners and locate the same ID. A lower generation returns `{Accepted:false, Generation:current}`. Convert the accepted claim into a copied `control.Runner` with `Connected:true` and `LastSeenAt: clock.Now()`, then call `UpsertRunner`. Treat repository `ErrStale` as a refused registration; propagate only closed control sentinels. Wake the pool after a successful upsert. Do not apply the claimed session list here; `ReconcileRunner` owns that behavior and may be invoked by the transport adapter immediately after registration.

- [ ] **Step 5: Pin and implement scoped runner listing**

Tests must prove authorization occurs before pool or fleet reads; `PoolResolver.EligiblePools(ctx, scope, control.Requirements{})` supplies every pool visible to the authoritative scope; rows from those pools are merged, copied, sorted by `(RunnerID,PoolID)`, and cursor-paginated without provider filters.

Use an opaque cursor containing the last `(runnerID,poolID)` pair encoded with base64 URL encoding of JSON. Cap limits to 100, default 50, and return `ErrInvalid` for a malformed cursor or negative limit. An empty result is `[]`, never nil.

- [ ] **Step 6: Run the task gate and commit**

Run:

```bash
gofmt -w controlapp/fleet.go controlapp/fleet_test.go
go test ./controlapp -run 'Test(NewFleetService|RegisterRunner|ListRunners)' -race -count=10
go vet ./controlapp
git diff --check
```

Commit:

```bash
git add controlapp/fleet.go controlapp/fleet_test.go
git commit -m "feat: extract runner registration"
```

### Task 2: Implement generation-fenced reconciliation

**Files:**
- Modify: `controlapp/fleet.go`
- Modify: `controlapp/fleet_test.go`

**Interfaces:**
- Consumes: `FleetRepository.SessionsOnRunner` plus workspace-keyed session reads/transitions.
- Produces: `ReconcileRunner(context.Context, control.RunnerSnapshot) (control.ReconcileResult, error)`.

- [ ] **Step 1: Write the reconciliation matrix**

Use a table with these observable outcomes:

```go
tests := []struct {
	name string
	stored control.SessionState
	reported *control.RunnerSession
	want control.SessionState
	wantDestroy bool
}{
	{"creating adopted running", control.StateCreating, &control.RunnerSession{SessionID:"sess_example", State:control.StateRunning}, control.StateRunning, false},
	{"running missing becomes dead", control.StateRunning, nil, control.StateDead, false},
	{"creating missing requeues", control.StateCreating, nil, control.StateQueued, false},
	{"terminal announced is orphan", control.StateDestroyed, &control.RunnerSession{SessionID:"sess_example", State:control.StateRunning}, control.StateDestroyed, true},
}
```

Add cases for lower runner generation fenced with no reads beyond the authoritative runner, unknown announced session returned in `Destroy`, duplicate announced IDs rejected as `ErrInvalid`, a reported session from another workspace returned in `Destroy` without mutation, mismatched stored pool/runner/generation treated as orphan/stale, and deterministic sorted `Destroy` output.

- [ ] **Step 2: Run the reconciliation tests and observe failure**

Run:

```bash
go test ./controlapp -run TestReconcileRunner -count=1
```

Expected: FAIL because `ReconcileRunner` is not implemented.

- [ ] **Step 3: Validate authority and reject stale snapshots**

Find the runner by `(snapshot.PoolID,snapshot.RunnerID)` before session reconciliation. Return `{Generation:current, Fenced:true}` for a lower generation and `ErrStale` for a same-generation identity mismatch. A newer snapshot first upserts the new authoritative runner generation, then reconciles against it.

The result's generation is always the store-authoritative generation, never merely the generation the caller sent.

- [ ] **Step 4: Reconcile stored and reported sets idempotently**

Load stored live sessions with:

```go
states := []control.SessionState{
	control.StateCreating, control.StateRunning,
	control.StateSuspendedWarm, control.StateSuspendedCold,
}
stored, err := s.fleet.SessionsOnRunner(ctx, snap.PoolID, snap.RunnerID, states)
```

Index both sides by session ID. For a present matching row, apply only a valid guarded state transition. Requeue a missing creating session with empty `RunnerID`; mark a missing running/warm/cold session dead. Never adopt an announced session whose workspace, pool, or runner differs from its authoritative row. `RunnerSnapshot` does not carry the separate session placement generation; the next sequential generation/schema plan adds that fence. Unknown, terminal, or mismatched announced sessions go to `Destroy` and are not inserted.

Repeat calls with the same snapshot must produce the same `Destroy` list and no additional state mutation.

- [ ] **Step 5: Run the task gate and commit**

Run:

```bash
gofmt -w controlapp/fleet.go controlapp/fleet_test.go
go test ./controlapp -run TestReconcileRunner -race -count=20
go test ./controlapp -race
go vet ./controlapp
git diff --check
```

Commit:

```bash
git add controlapp/fleet.go controlapp/fleet_test.go
git commit -m "feat: extract runner reconciliation"
```

### Task 3: Implement stale-safe runner events

**Files:**
- Modify: `controlapp/fleet.go`
- Modify: `controlapp/fleet_test.go`

**Interfaces:**
- Consumes: `control.RunnerEvent`, workspace-scoped session transitions, child-exit storage, and event recording.
- Produces: `ApplyRunnerEvent(context.Context, control.RunnerEvent) error`.

- [ ] **Step 1: Write event acceptance and rejection tests**

Cover running, warm/cold suspension, failed, dead, and child-exit observations. The child-exit case is represented by a non-nil `ChildExitCode`; it updates the exit code without moving a running session out of running.

Pin stale checks before mutation:

```go
event := control.RunnerEvent{
	WorkspaceID: "ws_example", PoolID: "pool_example", RunnerID: "runner_old",
	Generation: 6, SessionID: "sess_example", State: control.StateRunning,
}
if err := svc.ApplyRunnerEvent(ctx, event); !errors.Is(err, control.ErrStale) {
	t.Fatalf("got %v, want ErrStale", err)
}
if repo.transitionCalls != 0 || recorder.calls != 0 { t.Fatal("stale event had effects") }
```

Add wrong workspace, pool, runner, and runner-generation cases; duplicate terminal events; invalid state; overlong detail; and a recorder assertion that error detail never enters `control.Event`.

- [ ] **Step 2: Run the event tests and observe failure**

Run:

```bash
go test ./controlapp -run TestApplyRunnerEvent -count=1
```

Expected: FAIL because event application is absent.

- [ ] **Step 3: Implement identity/generation fencing before state logic**

Read the session by `(event.WorkspaceID,event.SessionID)`, then require exact matches for its workspace, pool, and runner. Validate `event.Generation` against the current runner generation from `FleetRepository.ListRunners(event.PoolID)`. `RunnerEvent.Generation` is the runner authority generation; the next sequential generation/schema plan adds a distinct placement-generation field. Any mismatch available in this contract returns `ErrStale` with no event record.

- [ ] **Step 4: Implement the guarded event table**

Use one private table rather than scattered switches:

```go
var eventTransitions = map[control.SessionState][]control.SessionState{
	control.StateRunning:       {control.StateCreating, control.StateRunning},
	control.StateSuspendedWarm: {control.StateRunning, control.StateSuspendedWarm},
	control.StateSuspendedCold: {control.StateRunning, control.StateSuspendedCold},
	control.StateFailed:        {control.StateCreating, control.StateRunning},
	control.StateDead:          {control.StateCreating, control.StateRunning,
		control.StateSuspendedWarm, control.StateSuspendedCold},
}
```

Add `var _ control.Fleet = (*FleetService)(nil)` after `ApplyRunnerEvent` completes the four-method interface.

For failed, bound `Detail` to 2048 valid UTF-8 bytes and pass it in `TransitionOpts.Error`. For a non-nil child exit, call `SetChildExitCode` and do not transition. A transition conflict caused by an already-applied identical event is success; another conflict remains `ErrConflict`. Record a provider-neutral service event only after an accepted mutation.

- [ ] **Step 5: Run the task gate and commit**

Run:

```bash
gofmt -w controlapp/fleet.go controlapp/fleet_test.go
go test ./controlapp -run TestApplyRunnerEvent -race -count=20
go test ./controlapp -race
go vet ./controlapp
git diff --check
```

Commit:

```bash
git add controlapp/fleet.go controlapp/fleet_test.go
git commit -m "feat: fence runner lifecycle events"
```

### Task 4: Extract FIFO scheduling and create dispatch

**Files:**
- Create: `controlapp/scheduler.go`
- Create: `controlapp/scheduler_test.go`

**Interfaces:**
- Consumes: queued sessions already assigned a pool, fleet capacity, environments, runner transport, and sensitive launch material from a host adapter.
- Produces: `FleetService.Wake(control.PoolID)`, `FleetService.Run(context.Context) error`, and deterministic placement/dispatch behavior.

- [ ] **Step 1: Define and test the sensitive-material seam**

Add to `scheduler.go`:

```go
type LaunchMaterial struct {
	Repos []runner.RepoSpec
	GitAuthorName string
	GitAuthorEmail string
	Environment map[string]string
}

type LaunchMaterialResolver interface {
	ResolveLaunchMaterial(context.Context, control.Session, *control.Environment) (LaunchMaterial, error)
}
```

Add `LaunchMaterial LaunchMaterialResolver` to `FleetOptions` and `FleetService`. Require it in the constructor. Tests use two adapters—a self-host-shaped fake and a Cloud-shaped fake—to prove this is a real seam. Inputs and results use fictional data, and fake error strings contain no values.

- [ ] **Step 2: Write pure capacity and selection tests**

Port the behavior of `TestPickRunner`, `TestSchedulerFIFOPlacementAndCapacityFrees`, `TestCreateDispatchFailureRequeues`, and `TestDrainQueueStopsWhenNoRunnerHasCapacity` to tests through `FleetService` plus its frozen ports.

Pin these rules:

- disconnected runners are ineligible;
- free capacity is `CapacityTotal-CapacityUsed-len(creating)`;
- greatest free capacity wins, ties break by ascending `RunnerID`;
- a runner must contain every portable capability requested by the session's current environment;
- queue order from `OldestQueued(pool)` is preserved;
- one blocked session does not block a later compatible session;
- no placement oversubscribes the last slot;
- `Wake` never blocks when 64 distinct or duplicate wakeups are pending.

- [ ] **Step 3: Run scheduling tests and observe failure**

Run:

```bash
go test ./controlapp -run 'Test(FleetRun|Pick|Drain|DispatchCreate)' -count=1
```

Expected: FAIL because the scheduler loop and create dispatch do not exist.

- [ ] **Step 4: Implement wake coalescing and a single scheduler loop**

`Wake` performs a non-blocking send. `Run` owns one timer and drains serially:

```go
func (s *FleetService) Wake(pool control.PoolID) {
	if pool == "" { return }
	select { case s.wake <- pool: default: }
}

func (s *FleetService) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.safetyInterval)
	defer ticker.Stop()
	pending := map[control.PoolID]struct{}{}
	for {
		select {
		case <-ctx.Done(): return ctx.Err()
		case pool := <-s.wake:
			pending[pool] = struct{}{}
		case <-ticker.C:
			// Safety passes cover pools observed through accepted runners.
			for pool := range s.knownPools() { pending[pool] = struct{}{} }
		}
		for pool := range pending { delete(pending, pool); s.drainPool(ctx, pool) }
	}
}
```

Add `knownMu sync.Mutex` and `known map[control.PoolID]struct{}` to `FleetService`. Maintain known pool IDs only from accepted registrations and wake calls, guarded by `knownMu`; `knownPools` returns a copied map.

- [ ] **Step 5: Build a create spec without leaking adapter concerns**

At dispatch time, re-read the session's environment. Build the public runner spec from the session and current environment:

```go
spec := runner.Spec{Name: row.Name, Image: row.Spec.Image,
	Cmd: slices.Clone(row.Spec.Cmd), EgressAllow: slices.Clone(row.Spec.EgressAllow)}
if env != nil {
	if env.Snapshot.Ref != "" && env.SnapshotHash == env.SetupHash {
		spec.Image = env.Snapshot.Ref
	} else {
		spec.Setup, spec.SetupTimeoutSec = env.Setup, env.SetupTimeoutSec
	}
	spec.Init, spec.InitTimeoutSec = env.Init, env.InitTimeoutSec
}
material, err := s.launchMaterial.ResolveLaunchMaterial(ctx, row, env)
```

Copy material into `spec`, pin `SetupHash` before dispatch when setup is non-empty, and never store the material. The resolver owns connector decoding, secret values, and source-control attribution; the application owns when those inputs are needed and how they enter the runner command.

- [ ] **Step 6: Implement placement and uncertain-delivery handling**

For each FIFO row, choose one eligible runner, guard `queued -> creating` with `TransitionOpts{RunnerID:&runnerID}`, and start dispatch only after the transition succeeds. Dispatch:

```go
runner.ToRunner{Type: "create", Session: string(row.ID), Spec: &spec}
```

If material resolution fails or the runner explicitly returns `OK:false`, transition creating to failed with a bounded safe reason. If dispatch errors and `Connected(pool,runner)` is false, requeue and clear `RunnerID`; if it remains connected, leave creating to prevent duplicate execution. Wake after any transition that frees or requeues capacity.

- [ ] **Step 7: Run scheduler gates and commit**

Run:

```bash
gofmt -w controlapp/fleet.go controlapp/fleet_test.go controlapp/scheduler.go controlapp/scheduler_test.go
go test ./controlapp -run 'Test(FleetRun|Pick|Drain|DispatchCreate)' -race -count=20
go test ./controlapp -race
go vet ./controlapp
git diff --check
```

Commit:

```bash
git add controlapp/fleet.go controlapp/fleet_test.go controlapp/scheduler.go controlapp/scheduler_test.go
git commit -m "feat: extract portable fleet scheduler"
```

### Task 5: Prove the lane is independently consumable

**Files:**
- Create: `controlapp/fleet_external_test.go`

**Interfaces:**
- Consumes: `NewFleetService`, `control.Fleet`, and the public runner protocol.
- Produces: an external-package proof that self-hosted and Cloud adapters can host the Fleet module without importing Rainier internals.

- [ ] **Step 1: Add an external-package smoke test**

Use `package controlapp_test` and compile:

```go
func constructFleet(t *testing.T) control.Fleet {
	svc, err := controlapp.NewFleetService(externalFleetOptions())
	if err != nil { t.Fatal(err) }
	return svc
}
```

Call registration, listing, reconciliation, and event application through `control.Fleet`. Run `svc.Run` in a canceled context and assert it returns `context.Canceled` without leaking a goroutine.

- [ ] **Step 2: Run dependency and full-repository gates**

Run:

```bash
if rg -n 'internal/|net/http|nhooyr|pgx|docker|github|cloud.google|billing' controlapp/fleet.go controlapp/scheduler.go; then exit 1; fi
bash scripts/check-public-protocols.sh
bash scripts/check-public-control.sh
go test ./controlapp -race -count=10
make verify
git diff --check origin/main...HEAD
```

Expected: every command succeeds and no existing self-hosted file changes.

- [ ] **Step 3: Review depth and commit**

Confirm tests observe behavior through `control.Fleet`, `Run`, and the frozen ports rather than private maps or helper functions. Remove exported scheduler/capacity helpers; callers need only the deep module's small interface.

Commit:

```bash
git add controlapp/fleet_external_test.go
git commit -m "test: verify fleet application seam"
```

## Acceptance checklist

- `FleetService` implements all four `control.Fleet` methods and runs one cancellable scheduler loop.
- Generation fencing rejects stale runner authority before every mutation or dispatch.
- Reconciliation is workspace-safe, deterministic, duplicate-safe, and returns explicit orphan cleanup.
- Queue placement is FIFO, capacity-correct, capability-aware, and cannot double-place after an uncertain dispatch.
- Sensitive launch material exists only in memory between its adapter and `runner.ToRunner`; it never reaches persistence, logs, events, errors, or fixtures.
- No provider policy or source-control implementation leaks into portable scheduling.
- Existing self-hosted behavior remains untouched until recomposition.

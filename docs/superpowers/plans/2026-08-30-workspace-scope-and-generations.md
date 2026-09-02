# Workspace Scope and Generations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make workspace scope mandatory in self-hosted persistence, make `memstore` and `pgstore` implement the control repository ports natively — retiring `internal/controld`'s private model twins — and persist the three generations the contract names (runner, placement, controller), so that a controld restart, a second replica, or a hosted cell all fence on stored authority instead of on process memory.

**Architecture:** Three moves, in order. (1) One coordinated contract amendment adds the durable controller-lease method and pins the generation semantics in the port docs; the lease moves out of `controlapp`'s memory into the host's repository. (2) The two stores become native, workspace-keyed implementations of `control.SessionRepository`, `control.EnvironmentRepository`, and `control.FleetRepository`, verified by one exported contract suite (`controlapp/repotest`) that a hosted store must pass too; what remains private is `HostStore` — identity, vault, and four host lookups the ports deliberately lack. (3) controld composes over the native stores; `adapt_store.go`'s conversions, the process-local generation table, and the twins are deleted, and the `check-public-control.sh` allowlist empties. The schema moves by expand (0007) then contract (0008), so every intermediate commit is green against both stores.

**Tech Stack:** Go 1.25, PostgreSQL 16 via `jackc/pgx/v5`, `control` (amended once, in Task 1), `controlapp`, `internal/controld` memstore and pgstore.

**Spec:** `rainier-cloud/docs/architecture/adr-0001-oss-cloud-composition.md` — "Workspace scope and authorization composition" (workspace identity mandatory in store methods; a resource ID or name is never authority; cross-workspace lookups fail without revealing existence), "Persistence model" (shared tables with mandatory workspace keys, composite tenant uniqueness), "Placement composition" (session placement generations distinct from provider eligibility), migration step 5; `rainier-cloud/docs/superpowers/plans/2026-08-30-hosted-implementation-program.md` gate **O8 → O9** ("Mandatory multi-workspace scope changes") and Wave 5 ("workspace scope, persistence expansion, repository enforcement, runner generations … are sequential signature/schema changes"); `rainier-cloud/docs/product/hosted-product-prd.md` §9 ("every resume, host recovery, or migration creates a monotonic execution generation" — one sandbox, process tree, placement, lease) and §5.1 (exactly one controller generation may send input); `rainier-cloud/docs/security/hosted-tenancy-and-security.md` §4 (locators are not authority; workspace-keyed shared tables) and §6.2 (one fenced controller generation per session); `docs/superpowers/plans/2026-08-30-self-hosted-recomposition.md` "Not in this plan" (the items it deferred here). This is OSS plan #7 in the program inventory; plan #8 (`2026-08-30-control-outbox-checkpoint-capabilities.md`) builds on the store shape this plan leaves behind.

## Global Constraints

- Every task's changes start in a worktree created from freshly fetched `origin/main` (Task 1, the reviewer's coordinated PR) or from the integration branch `feat/workspace-scope` (every later task, each on top of the previous one — this plan is sequential by design). Nothing is pushed or merged by a worker.
- `control/*.go` changes exactly once, in Task 1, by the reviewer, as its own PR before any worker starts. No later task edits `control/`. `controlapp/*.go` changes in Task 1 (the lease) and gains one new package (`controlapp/repotest`) in Task 2; no exported `controlapp` identifier is renamed or removed.
- One model. After Task 5 no type under `internal/controld` duplicates a `control` type: sessions, environments, runners, states, queries, transition options, repo refs, connectors, and the two sentinels exist once, in `control`. The `check-public-control.sh` allowlist is emptied in Task 5 and stays empty.
- Every repository method is workspace- or pool-keyed and every store enforces it: an empty workspace or pool ID is `control.ErrInvalid`; a workspace the store does not know is `control.ErrNotFound` on a write and simply "no rows" (`ErrNotFound`) on a read; a resource in another workspace is `ErrNotFound`, never `ErrDenied`, and never a different message.
- Every intermediate commit is green: `make verify`, `go test ./internal/controld/... -race -count=3`, `scripts/check-public-control.sh`, `scripts/check-public-protocols.sh`, and the pgstore suite against a real PostgreSQL (`RAINIER_TEST_PG_DSN` or docker). The expand migration (0007) lands in Task 3 while the old store methods still run; the contract migration (0008) lands in Task 5 after the last old method is deleted. No task leaves a migration that a later task has to edit.
- The `/v0/` wire does not change. There is no deviation table in this plan because there is nothing to deviate; the existing response-shape and behavior tests in `internal/controld`, `internal/e2e`, `cmd/rainier`, and `internal/cli` pass unmodified through Task 4. Task 5 rewrites test *setup* (how a row is seeded) and never an assertion's expected value.
- Migrations are tested against rows created by the pre-0007 code (Task 3 seeds them with raw SQL) and the dogfood upgrade is rehearsed on `rainier-1` from a `pg_dump` of its live database before the merge — the one non-synthetic step, and it stays on the VM.
- No secret value, credential, terminal byte, session content, or runner free text enters a store error, a log line added by this plan, a commit message, or test output. Tests use only synthetic `.test`, `.invalid`, `example.com`, `agents.localhost`, and fictional opaque IDs (`ws_alpha`, `ws_beta`, `pool_a`, `sess_example`, …).
- Go gates run serially. Use `GOCACHE=/private/tmp/rainier-scope-gocache`.
- Commit messages follow the repository style (`feat:`/`refactor:` prefix, imperative subject, a body that says why) and end with the attribution trailers the reviewer supplies at commit time. Workers do not commit; they leave the tree ready and report.

## What "mandatory workspace scope" means here

A self-hosted installation is still exactly one workspace (`ws_self_hosted`) running exactly one pool (`pool_self_hosted`), as in O8. What changes is where that fact lives: today it is a constant the adapters *check* on the way into a single-tenant store; after this plan it is a column every tenant row *carries*, a key every query *requires*, and a composite every uniqueness constraint *includes*. The install workspace is provisioned by the migration and by `NewMemStore`, and `controld.New` re-asserts it (`EnsureWorkspace`) so a store from any source is usable. The hosted cell gets the same repository contract with real workspace IDs; it also gets `controlapp/repotest`, which is the proof its own store must pass.

Two things stay deliberately unscoped: users and credentials (identity — the host's, not the tenant's) and secrets (the OSS vault is installation-wide; the hosted product has its own workspace-secret store per the tenancy spec §9). Runner rows carry a pool, not a workspace, because the contract keys the fleet by pool.

## Generations

| Generation | Where it lives after this plan | Who advances it | Who fences on it |
|---|---|---|---|
| Runner (`control.Runner.Generation`) | `runners.generation` | `HostStore.NextRunnerGeneration`, once per connection, before registration | `FleetRepository.UpsertRunner` (`ErrStale` when lower than stored); `RegisterRunner`/`ReconcileRunner`/`ApplyRunnerEvent` as in O8; **new:** the heartbeat (`touchRunner`) — a superseded connection's heartbeat is refused and the connection ends |
| Placement (`control.Session.PlacementGeneration`) | `sessions.placement_generation` | the repository, on every `Transition` whose `RunnerID` option names a runner — a placement by the scheduler, an adoption by reconcile, and (Task 1) a cold resume, which names the row's own runner because it starts a new sandbox; a warm resume unpauses the same sandbox and names none | carried on `AttachTarget`; plan 8 puts it on the runner wire and on events |
| Controller (`control.Session.ControllerGeneration`, new) | `sessions.controller_generation` | `SessionRepository.NextControllerGeneration` (new), called by `AttachmentService` for a controller attach | carried on `AttachTarget`; the relay's controller fencing (later) |

In O8 the runner generation restarted at 1 with every controld process and the controller generation lived in a map inside `controlapp`. Both are now stored, so a restart continues the sequence, and two replicas sharing a store cannot hand out the same authority twice.

## File structure

```text
control/
  session.go                 Task 1: Session.ControllerGeneration; PlacementGeneration doc gains the advancing rule
  ports.go                   Task 1: SessionRepository.NextControllerGeneration; Transition, UpsertRunner,
                             SetEnvironmentSnapshot docs pin the semantics the stores implement
  contract_test.go           Task 1: the fake repository grows the method
controlapp/
  attachments.go             Task 1: the lease map is gone; grantGeneration asks the repository
  attachments_test.go        Task 1: lease tests run against a repository stub
  *_test.go                  Task 1: every SessionRepository fake gains NextControllerGeneration
  repotest/
    doc.go                   Task 2: how a host proves its repositories
    repotest.go              Task 2: Run(t, open) — the three-port contract suite
internal/controld/
  store.go                   Task 2: HostStore, the Store union, SnapshotCheckpoint/SnapshotCapability helpers;
                             Task 5: the twins, ErrIdemReplay, and the old Store methods are gone
  memstore.go                Task 2: rows kept in control types; native ports as accessor views; old methods
                             become conversions over the same rows; Task 5: old methods deleted
  memstore_test.go           Task 2: repotest.Run + storetest.RunHost
  storetest/contract.go      Task 2: RunHost (identity, vault, host lookups); Task 5: RunContract deleted
  pgstore/
    migrations/0007_workspace_scope.sql            Task 3: expand
    migrations/0008_workspace_scope_contract.sql   Task 5: contract
    pgstore.go               Task 3: accessors; UpsertRunner's ON CONFLICT target; Task 5: old methods deleted
    host.go                  Task 3: EnsureWorkspace, EnvironmentByName, SnapshotRunner, NextRunnerGeneration
    sessions.go              Task 3: control.SessionRepository
    environments.go          Task 3: control.EnvironmentRepository (+ requirements jsonb codec)
    fleet.go                 Task 3: control.FleetRepository
    pgstore_test.go          Task 3: repotest.Run, storetest.RunHost, migration replay, pre-0007 backfill
  adapt_store.go             Task 4: DELETED (conversions, the three adapters, runnerGenerations)
  adapt_scope.go             Task 4: runnerCapabilities and capabilityValue move here
  adapt_host.go              Task 4: installationPools over Fleet(); logRecorder/systemClock/idGenerator stay
  controld.go                Task 4: Store is the union; compose() uses the accessors; gens field deleted;
                             New ensures the install workspace
  api.go attach.go runners.go srpc.go   Task 4: every direct store read/write goes through the ports or HostStore
  runners.go                 Task 4: generation minted from the store; heartbeat carries it and is fenced
  *_test.go                  Task 5: seeded through the ports in control types
scripts/check-public-control.sh   Task 2: controlapp/repotest joins the import-hygiene loop; Task 5: allowlist=()
docs/deploy-gce.md           Task 5: upgrade note (0007/0008 run on start; back up first)
```

---

### Task 1: Amend the contract — durable controller generation, pinned generation semantics (reviewer)

This is the one coordinated `control` change of the plan (the pattern of rainier #31). It is additive, non-behavioral on the wire, and lands as its own PR before Task 2 starts.

**Files:**
- Modify: `control/session.go` (the `Session` struct and its `PlacementGeneration` doc)
- Modify: `control/ports.go` (`SessionRepository`, `EnvironmentRepository.SetEnvironmentSnapshot` doc, `FleetRepository.UpsertRunner` doc)
- Modify: `control/contract_test.go` (`fakeSessionRepository`)
- Modify: `controlapp/attachments.go`, `controlapp/attachments_test.go`, and every `controlapp/*_test.go` fake that implements `control.SessionRepository` (`grep -ln "func (.*) SessionByIDem" controlapp/*_test.go`)
- Modify: `controlapp/sessions.go` (`ResumeSession`'s cold path), `controlapp/sessions_test.go`
- Modify: `internal/controld/adapt_store.go` (`storeSessions` gains the method over a process-local table), `internal/controld/controld.go` (`compose` constructs the table)

**Interfaces:**
- Produces, on `control.Session`:
  ```go
  // ControllerGeneration is the monotonic generation of this session's
  // current terminal controller: 0 until a controller has attached, then
  // the value NextControllerGeneration last returned. A viewer attaches
  // under the current value; a controller advances it. Stale input is
  // fenced against it by the attachment plane.
  ControllerGeneration uint64
  ```
  and the `PlacementGeneration` doc gains one sentence: *"A repository advances it by one on every Transition whose RunnerID option names a runner, and leaves it unchanged otherwise. A generation is one sandbox on one runner: the scheduler's placement, reconcile's adoption, and a cold resume — which names the row's own runner, because it starts a new sandbox there — each open one; a warm resume unpauses the sandbox it has and opens none."*
- Produces, on `control.SessionRepository`:
  ```go
  // NextControllerGeneration advances id's controller generation by one and
  // returns the new value, atomically with respect to every other caller.
  // ErrNotFound when id does not exist in ws.
  NextControllerGeneration(ctx context.Context, ws WorkspaceID, id SessionID) (uint64, error)
  ```
  plus three doc sentences: on `Transition` — *"When opts.RunnerID names a runner the repository also advances PlacementGeneration (see Session)."*; on `UpsertRunner` — *"ErrStale when r.Generation is below the runner's stored generation; nothing changes."*; on `SetEnvironmentSnapshot` — *"ErrStale when SetupHash no longer equals expectHash, ErrNotFound when envID does not exist in ws."*
- Consumes: nothing new.

- [ ] **Step 1: Write the failing controlapp test**

In `controlapp/attachments_test.go`, replace the lease tests that inspect the service's map with one that observes the repository:

```go
// TestControllerGenerationIsTheRepositorys pins the lease's home: a viewer
// attaches under the row's current generation, a controller asks the
// repository for the next one, and the service keeps no generation of its
// own.
func TestControllerGenerationIsTheRepositorys(t *testing.T) {
	repo := newAttachStubSessionRepo() // the file's existing stub, extended below
	repo.rows[attachKey{ws: "ws_alpha", id: "sess_example"}] = control.Session{
		ID: "sess_example", WorkspaceID: "ws_alpha", CreatorID: "act_example",
		State: control.StateRunning, PoolID: "pool_a", RunnerID: "runner_a",
		PlacementGeneration: 1, ControllerGeneration: 4,
	}
	svc := newAttachmentServiceOver(t, repo) // the file's existing constructor helper
	broker := &captureBroker{}
	svc.broker = broker

	if err := svc.AttachTerminal(ctx, alphaScope(), control.AttachTerminal{SessionID: "sess_example", Mode: control.AttachmentViewer}, nopStream{}); err != nil {
		t.Fatal(err)
	}
	if broker.last.ControllerGeneration != 4 {
		t.Fatalf("viewer generation = %d, want the row's 4", broker.last.ControllerGeneration)
	}
	if err := svc.AttachTerminal(ctx, alphaScope(), control.AttachTerminal{SessionID: "sess_example", Mode: control.AttachmentController}, nopStream{}); err != nil {
		t.Fatal(err)
	}
	if broker.last.ControllerGeneration != 5 || repo.nextCalls != 1 {
		t.Fatalf("controller generation = %d (repo calls %d), want 5 from one NextControllerGeneration", broker.last.ControllerGeneration, repo.nextCalls)
	}
}
```

The stub's `NextControllerGeneration` increments the stored row's field and counts calls; a missing row returns `control.ErrNotFound`.

In `controlapp/sessions_test.go`, beside the existing resume tests:

```go
// TestColdResumeOpensANewPlacementGeneration pins PRD §9: a cold resume
// starts a new sandbox, so it is a placement and names the row's own runner
// in its transition; a warm resume unpauses the sandbox it has and names
// none. The stub repository counts a generation per transition that names
// a runner, exactly as the contract says a store must.
func TestColdResumeOpensANewPlacementGeneration(t *testing.T) {
	for _, tc := range []struct {
		from    control.SessionState
		wantGen uint64
	}{
		{control.StateSuspendedCold, 2},
		{control.StateSuspendedWarm, 1},
	} {
		repo := newSessionStubSessionRepo() // the file's stub, extended to bump PlacementGeneration when opts.RunnerID names a runner
		repo.rows["sess_example"] = control.Session{ID: "sess_example", WorkspaceID: "ws_alpha", CreatorID: "act_a",
			State: tc.from, PoolID: "pool_a", RunnerID: "runner_a", PlacementGeneration: 1}
		svc := newSessionServiceOver(t, repo) // the file's constructor helper, with a transport whose Dispatch answers OK
		got, err := svc.ResumeSession(ctx, alphaScope(), control.ResumeSession{ID: "sess_example"})
		if err != nil {
			t.Fatalf("%s: %v", tc.from, err)
		}
		if got.PlacementGeneration != tc.wantGen || got.RunnerID != "runner_a" {
			t.Fatalf("%s: generation %d on %q, want %d on runner_a", tc.from, got.PlacementGeneration, got.RunnerID, tc.wantGen)
		}
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./controlapp -run TestControllerGenerationIsTheRepositorys`
Expected: compile failure — `control.Session` has no field `ControllerGeneration`, the stub has no `NextControllerGeneration`.

- [ ] **Step 3: Amend the contract and move the lease**

`control/session.go`, `control/ports.go`: exactly the additions above. `control/contract_test.go`: `fakeSessionRepository` gains `func (fakeSessionRepository) NextControllerGeneration(context.Context, control.WorkspaceID, control.SessionID) (uint64, error) { return 0, nil }`.

`controlapp/attachments.go`: delete `leaseMu`, `controller`, `attachmentLeaseKey`, and the map initialization in `NewAttachmentService`; `grantGeneration` becomes

```go
// grantGeneration returns the controller generation a target carries: a
// viewer attaches under the row's current value; a controller asks the
// repository to advance it. The generation is the repository's — durable,
// shared by every replica — and this service keeps none of its own.
func (s *AttachmentService) grantGeneration(ctx context.Context, row control.Session, mode control.AttachmentMode) (uint64, error) {
	if mode == control.AttachmentViewer {
		return row.ControllerGeneration, nil
	}
	gen, err := s.sessions.NextControllerGeneration(ctx, row.WorkspaceID, row.ID)
	if err != nil {
		if errors.Is(err, control.ErrNotFound) {
			return 0, control.ErrNotFound
		}
		return 0, portError(err)
	}
	return gen, nil
}
```

and its one call site passes `ctx, row, cmd.Mode`.

`controlapp/sessions.go` `ResumeSession` (the transition at `sessions.go:540` on `main`, `control.TransitionOpts{}` for both suspended states): the cold branch passes `control.TransitionOpts{RunnerID: &row.RunnerID}` — the row's own runner, so the store opens a new placement generation for the new sandbox — and the warm branch stays empty. Add the sentence to `ResumeSession`'s doc comment: *"A cold resume is a placement: it names the session's runner again so the repository opens a new generation for the sandbox it starts; a warm resume unpauses the sandbox it has."*

Update the package doc sentence in `controlapp/doc.go` ("grants a fenced controller generation" → "grants the repository's next controller generation"). Every `SessionRepository` fake in `controlapp/*_test.go` gains the method (a counter over its rows, `ErrNotFound` when absent).

`internal/controld/adapt_store.go`: until Task 2 makes it durable, the self-hosted adapter keeps the lease where it always was — in process memory — but on the host's side of the port:

```go
// controllerLeases is the process-local controller generation table the
// self-hosted adapter answers NextControllerGeneration from until the store
// persists it (Task 2 of the workspace-scope plan). Keyed by session; a
// restart starts every session over at 0, which is sound because every
// attachment dies with the process.
type controllerLeases struct {
	mu  sync.Mutex
	cur map[control.SessionID]uint64
}

func (l *controllerLeases) next(id control.SessionID) uint64 { /* lock; cur[id]++; return */ }
func (l *controllerLeases) current(id control.SessionID) uint64 { /* lock; return cur[id] */ }
```

`storeSessions` becomes `struct{ st Store; leases *controllerLeases }`; `GetSession` stamps `c.ControllerGeneration = r.leases.current(id)` after conversion (`ListSessions` rows too); `NextControllerGeneration` checks the workspace, confirms the row exists (`st.GetSession` → `storeErr`), then returns `r.leases.next(id)`. `compose()` constructs one `&controllerLeases{}` and passes it.

- [ ] **Step 4: Run the gates**

Run: `go test ./control ./controlapp -race -count=3 && go test ./internal/controld/... -race -count=2 && scripts/check-public-control.sh && make verify`
Expected: all pass; the attach tests in `internal/controld` (`attach_test.go`, `adapt_attach_test.go`) are unchanged and green — the wire never carried the generation — and the O8 adapter's `Transition` ignores the named runner's effect on a generation it does not yet store (`sessionToControl` still answers 1), which Task 2 makes real.

- [ ] **Step 5: Commit and open the coordinated PR**

```bash
git add control controlapp internal/controld/adapt_store.go internal/controld/controld.go
git commit -m "feat(control): make the controller generation the repository's; a cold resume opens a placement generation"
```

PR body names it as the plan's Task 1 and lists the three doc sentences verbatim. Merge before Task 2's worktree is created.

---

### Task 2: The repository contract suite and the native in-memory store

**Files:**
- Create: `controlapp/repotest/doc.go`, `controlapp/repotest/repotest.go`
- Modify: `internal/controld/store.go` (`HostStore`, the `Store` union, exported snapshot helpers)
- Modify: `internal/controld/memstore.go` (rows in control types; accessor views; old methods as conversions)
- Modify: `internal/controld/memstore_test.go` (run both suites)
- Modify: `internal/controld/storetest/contract.go` (add `RunHost`; `RunContract` untouched)
- Modify: `scripts/check-public-control.sh` (the import-hygiene loop covers `controlapp/repotest`)

**Interfaces:**
- Consumes: Task 1's contract.
- Produces (public):
  ```go
  package repotest // github.com/tokencanopy/rainier/controlapp/repotest

  // The two workspaces and two pools every case is written against. A
  // store that treats either pair as the same thing fails the suite.
  const (
  	Alpha control.WorkspaceID = "ws_alpha"
  	Beta  control.WorkspaceID = "ws_beta"
  	PoolA control.PoolID      = "pool_a"
  	PoolB control.PoolID      = "pool_b"
  )

  // Stores is what a host hands the suite: its three repository ports over
  // ONE backing store (a session created through Sessions must be visible to
  // Fleet.SessionsOnRunner), plus the way to make a workspace exist there.
  type Stores struct {
  	Sessions     control.SessionRepository
  	Environments control.EnvironmentRepository
  	Fleet        control.FleetRepository
  	Provision    func(ctx context.Context, ws control.WorkspaceID) error
  }

  // Run drives the contract. open is called once per case and must return an
  // empty store each time; Run provisions Alpha and Beta before the case body.
  func Run(t *testing.T, open func(t *testing.T) Stores)
  ```
- Produces (private, `internal/controld/store.go`):
  ```go
  // HostStore is the persistence the self-hosted host owns beside the
  // control repositories: identity (users, bearer tokens), the vault
  // (secrets, credentials), and four lookups the control ports deliberately
  // have no method for. Like the ports it returns the control sentinel set
  // and never leaks SQL, a DSN, or a row's contents in an error.
  type HostStore interface {
  	// EnsureWorkspace makes ws exist; idempotent. New calls it for the
  	// installation workspace, the repository contract suite for its two.
  	EnsureWorkspace(ctx context.Context, ws control.WorkspaceID) error

  	UpsertUser(ctx context.Context, githubID int64, login, role string) (User, error)
  	InsertToken(ctx context.Context, userID, tokenHash string) error
  	UserByToken(ctx context.Context, tokenHash string) (User, error)
  	GetUser(ctx context.Context, id string) (User, error)

  	PutSecret(ctx context.Context, name string, ciphertext, nonce []byte) error
  	ListSecrets(ctx context.Context) ([]SecretMeta, error)
  	GetSecret(ctx context.Context, name string) (ciphertext, nonce []byte, err error)
  	DeleteSecret(ctx context.Context, name string) error

  	UpsertCredential(ctx context.Context, c Credential) error
  	GetCredential(ctx context.Context, userID, provider string) (Credential, error)
  	SetCredentialStatus(ctx context.Context, userID, provider, status string) error
  	TouchCredentialUsed(ctx context.Context, userID, provider string) error
  	ListCredentials(ctx context.Context, userID string) ([]Credential, error)

  	// EnvironmentByName resolves a name inside ws to the id the service is
  	// then asked for. The name index is a locator, never authority: the
  	// caller still fetches through the service, which authorizes.
  	EnvironmentByName(ctx context.Context, ws control.WorkspaceID, name string) (control.EnvironmentID, error)
  	// SnapshotRunner names the runner that built id's cached snapshot, ""
  	// when there is none — stale or not, because the wire has always shown
  	// the column. It decides nothing.
  	SnapshotRunner(ctx context.Context, ws control.WorkspaceID, id control.EnvironmentID) (control.RunnerID, error)
  	// NextRunnerGeneration opens a new generation for id in pool and returns
  	// it: 1 for a runner never seen, else one more than stored. It is the
  	// only writer of the generation the fleet repository fences on.
  	NextRunnerGeneration(ctx context.Context, pool control.PoolID, id control.RunnerID) (uint64, error)
  }

  // Store is what controld.New composes over: the host's own persistence
  // plus the three control repositories, all over one backing store.
  type Store interface {
  	HostStore
  	Sessions() control.SessionRepository
  	Environments() control.EnvironmentRepository
  	Fleet() control.FleetRepository
  }

  // SnapshotCheckpoint is the control spelling of a self-hosted environment
  // snapshot: a runner-built image ref, the one format this build has.
  func SnapshotCheckpoint(ref string) control.Checkpoint {
  	return control.Checkpoint{Ref: ref, Format: "rainier-runner-v0", Capabilities: []string{"workspace"}}
  }

  // SnapshotCapability is the self-hosted spelling of a CURRENT snapshot's
  // affinity to the runner that built it: appended to an environment's
  // requirements on the way out of a store, stripped on the way in. Plan 8
  // replaces it with control.CheckpointLocator and deletes it.
  func SnapshotCapability(holder control.RunnerID) string
  // StripSnapshotCapabilities returns caps without any snapshot affinity.
  func StripSnapshotCapabilities(caps []string) []string
  ```
  During Tasks 2–4 the `Store` interface **also** keeps every method it has today (the twin-typed ones), so existing tests and handlers compile; Task 5 removes them. The private sentinels `ErrNotFound`/`ErrConflict`/`ErrIdemReplay` stay until Task 5; the *native* methods and the four new host lookups return `control` sentinels from the start.

#### The contract suite, case by case

Every case provisions `Alpha` and `Beta` and then does exactly what the row says. "Every method" means a table-driven loop over every method of the port. Expected errors are `errors.Is` against the control sentinel and nothing else.

Sessions (`control.SessionRepository`):

| # | Case | Setup | Assertion |
|---|---|---|---|
| S1 | round trip | create in Alpha a session with every field set (Spec with Cmd, EgressAllow, `Repos: []control.RepoRef{}` non-nil empty), `ChildExitCode: &3`, `PlacementGeneration: 0`, `ControllerGeneration: 9` | `GetSession` equals the created row field for field except: `ChildExitCode == nil` (create never stores one), `PlacementGeneration == 1` (zero is stored as one), `ControllerGeneration == 0` (only NextControllerGeneration writes it); `Spec.Repos` is non-nil and empty; a second create with `Repos: nil` reads back nil |
| S2 | workspace isolation | create `sess_same` in Alpha and again in Beta with different names | both succeed; `GetSession(Beta, …)` returns Beta's row; `GetSession(Alpha, sess_beta_only)` is `ErrNotFound`; `ListSessions` per workspace returns only its own |
| S3 | empty workspace | every method with `ws == ""` | `ErrInvalid` |
| S4 | unknown workspace | `CreateSession("ws_nobody", …)` | `ErrNotFound`; a `GetSession` there is `ErrNotFound` |
| S5 | active name unique per creator | create `dev` for `act_a` in Alpha twice | second is `ErrConflict`; after `Transition` of the first to `StateDestroyed`, a third succeeds; `dev` for `act_b` in Alpha and `dev` for `act_a` in Beta both succeed alongside |
| S6 | idempotency replay | create with `IdempotencyKey: "idem_1"`, then create again with a different name and the same key and creator | second returns the first row (same ID) and no error; the same key for another creator is a new row; `SessionByIDem` finds each; `SessionByIDem` with an unknown key, or an empty key, is `ErrNotFound` |
| S7 | listing | create five in Alpha with `CreatedAt` one second apart, one of them `StateDead`; one in Beta | default list hides the dead one and Beta's; `IncludeTerminal` shows the dead one; order is `CreatedAt` desc then `ID` desc; `Limit: 2` returns two and a cursor that resumes exactly after them, empty cursor at the end; a garbage cursor is `ErrInvalid` |
| S8 | guarded transition | queued row | from `[running]` to `creating` is `ErrConflict` and changes nothing; from `[queued]` with `RunnerID: &"runner_a"` sets `RunnerID` and `PlacementGeneration` becomes 2; a further transition with `RunnerID: &""` clears the runner and leaves 2; one with `RunnerID: nil` and `Error: &"x"` leaves the runner, sets `Error`, leaves 2; `UpdatedAt` and `LastEventAt` move forward; unknown id is `ErrNotFound` |
| S9 | provenance writes | queued row | `SetSessionSetupHash` and `SetChildExitCode` write their column and bump `UpdatedAt` but leave `LastEventAt` alone; unknown id is `ErrNotFound`; the exit code reads back as a fresh pointer (mutating it does not change a later read) |
| S10 | controller generation | queued row | `NextControllerGeneration` returns 1, 2, 3; `GetSession` shows 3; unknown id is `ErrNotFound`; Alpha's counter is independent of a same-ID row in Beta |

Environments (`control.EnvironmentRepository`):

| # | Case | Setup | Assertion |
|---|---|---|---|
| E1 | round trip | create in Alpha with Requirements (`Capabilities: ["placement:runner_a", "gpu"]`, `MinCPU: 2`), two Connectors with distinct Raw bytes, `SetupHash: "h1"` | `GetEnvironment` equals it; `Snapshot` is zero and `SnapshotHash` empty on create; `SetupHash` is stored as given (the repository computes nothing) |
| E2 | name unique per workspace | same name twice in Alpha | `ErrConflict`; the same name in Beta succeeds; `UpdateEnvironment` renaming one onto a held name is `ErrConflict`; updating an unknown id is `ErrNotFound` |
| E3 | isolation | one in each workspace | `GetEnvironment(Beta, alphaID)` is `ErrNotFound`; `DeleteEnvironment(Beta, alphaID)` is `ErrNotFound` and Alpha's row survives |
| E4 | listing | three in Alpha named `c`, `a`, `b` | order `(Name, ID)` ascending; `Limit: 2` pages with a cursor that resumes; `Limit: 0` is the whole workspace |
| E5 | update ignores the cache | after E6's snapshot | `UpdateEnvironment` with `Snapshot` and `SnapshotHash` set to other values (and `SetupHash: "h2"`) stores the new `SetupHash`, leaves `Snapshot.Ref` and `SnapshotHash` exactly as they were — the cache is the store's, written only by `SetEnvironmentSnapshot` |
| E6 | guarded snapshot | env with `SetupHash: "h1"` | `SetEnvironmentSnapshot(…, "h1", "snap:1", "runner_a")` succeeds and `GetEnvironment` shows `Snapshot.Ref == "snap:1"`, `Snapshot.Format != ""`, `SnapshotHash == "h1"`, and `Requirements.Capabilities` gains exactly one entry equal to `"snapshot:runner_a"` (appended after the stored ones); with `expectHash: "h9"` it is `ErrStale` and nothing changes; after E5 changed the hash to `h2` the snapshot capability is no longer emitted; an unknown env is `ErrNotFound` |
| E7 | count by environment | env in Alpha; sessions on it: two queued, one destroyed; one queued session on it in Beta (same env ID provisioned there) | `CountSessionsByEnvironment(Alpha, id, nil) == 3`, `(Alpha, id, [queued]) == 2`; Beta counts 1 |
| E8 | empty workspace | every method with `ws == ""` | `ErrInvalid` |

Fleet (`control.FleetRepository`):

| # | Case | Setup | Assertion |
|---|---|---|---|
| F1 | round trip and order | upsert `runner_b` then `runner_a` in PoolA with `Capabilities: ["placement:runner_a", "gpu"]`, `Generation: 1` | `ListRunners(PoolA)` is `[runner_a, runner_b]` with every field back, capabilities in the given order; `ListRunners(PoolB)` is empty |
| F2 | pool isolation | `runner_a` in PoolA (total 4) and in PoolB (total 8) | independent rows; `SetRunnerConnected(PoolB, runner_a, false)` leaves PoolA's connected |
| F3 | generation fence | upsert gen 2, then gen 1 with a different capacity | second is `ErrStale` and the row still shows gen 2 and the first capacity; gen 2 again and gen 3 both succeed |
| F4 | connected flag | | `SetRunnerConnected` on an unknown runner is `ErrNotFound`; on a known one it flips the flag and moves `LastSeenAt` |
| F5 | sessions on a runner | sessions: two running on `runner_a` in PoolA, one creating there, one running on `runner_a` in PoolB, one running on `runner_b` | `SessionsOnRunner(PoolA, runner_a, [running])` is the two; `[running, creating]` is three; `(PoolB, runner_a, nil)` is one |
| F6 | oldest queued | queued sessions in PoolA created at t, t+1, t+2 and one queued in PoolB | `OldestQueued(PoolA)` is the three in ascending `CreatedAt`, `ID`; PoolB's is its one |
| F7 | empty pool | every method with `pool == ""` | `ErrInvalid` |

Two cases written out, so the shape of the rest is unambiguous (the worker writes every row of the tables above as a `t.Run` in this shape):

```go
func caseGenerationFence(t *testing.T, s Stores) {
	ctx := context.Background()
	first := control.Runner{ID: "runner_a", PoolID: PoolA, CapacityTotal: 4, Connected: true, Generation: 2, Capabilities: []string{"gpu"}}
	if err := s.Fleet.UpsertRunner(ctx, PoolA, first); err != nil {
		t.Fatalf("upsert gen 2: %v", err)
	}
	stale := first
	stale.Generation, stale.CapacityTotal = 1, 99
	if err := s.Fleet.UpsertRunner(ctx, PoolA, stale); !errors.Is(err, control.ErrStale) {
		t.Fatalf("upsert gen 1 over 2: err = %v, want ErrStale", err)
	}
	rows, err := s.Fleet.ListRunners(ctx, PoolA)
	if err != nil || len(rows) != 1 || rows[0].Generation != 2 || rows[0].CapacityTotal != 4 {
		t.Fatalf("after a stale upsert: rows = %+v, err = %v; want the gen-2 row untouched", rows, err)
	}
	for _, gen := range []uint64{2, 3} {
		next := first
		next.Generation = gen
		if err := s.Fleet.UpsertRunner(ctx, PoolA, next); err != nil {
			t.Fatalf("upsert gen %d: %v", gen, err)
		}
	}
}

func casePlacementGeneration(t *testing.T, s Stores) {
	ctx := context.Background()
	row := mustCreate(t, s, Alpha, control.Session{ID: "sess_example", CreatorID: "act_a", State: control.StateQueued, PoolID: PoolA})
	if row.PlacementGeneration != 1 {
		t.Fatalf("created generation = %d, want 1", row.PlacementGeneration)
	}
	placed := control.RunnerID("runner_a")
	if err := s.Sessions.Transition(ctx, Alpha, row.ID, []control.SessionState{control.StateQueued}, control.StateCreating, control.TransitionOpts{RunnerID: &placed}); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Sessions.GetSession(ctx, Alpha, row.ID)
	if got.RunnerID != placed || got.PlacementGeneration != 2 {
		t.Fatalf("after placement: runner %q gen %d, want runner_a gen 2", got.RunnerID, got.PlacementGeneration)
	}
	cleared := control.RunnerID("")
	if err := s.Sessions.Transition(ctx, Alpha, row.ID, []control.SessionState{control.StateCreating}, control.StateQueued, control.TransitionOpts{RunnerID: &cleared}); err != nil {
		t.Fatal(err)
	}
	got, _ = s.Sessions.GetSession(ctx, Alpha, row.ID)
	if got.RunnerID != "" || got.PlacementGeneration != 2 {
		t.Fatalf("after requeue: runner %q gen %d, want no runner and gen still 2", got.RunnerID, got.PlacementGeneration)
	}
	reason := "lost"
	if err := s.Sessions.Transition(ctx, Alpha, row.ID, control.NonTerminal, control.StateDead, control.TransitionOpts{Error: &reason}); err != nil {
		t.Fatal(err)
	}
	got, _ = s.Sessions.GetSession(ctx, Alpha, row.ID)
	if got.Error != "lost" || got.PlacementGeneration != 2 {
		t.Fatalf("after an error-only transition: %+v", got)
	}
}
```

`mustCreate` is the suite's own helper (create, fatal on error, return the row). `Run` registers each case as `t.Run(name, func(t) { s := open(t); provision both; case(t, s) })`.

#### memstore

Rows are kept in control types, keyed by their scope:

```go
type sessionKey struct {
	ws control.WorkspaceID
	id control.SessionID
}
type environmentKey struct {
	ws control.WorkspaceID
	id control.EnvironmentID
}
type runnerKey struct {
	pool control.PoolID
	id   control.RunnerID
}

type memStore struct {
	mu           sync.Mutex
	workspaces   map[control.WorkspaceID]struct{}
	sessions     map[sessionKey]*control.Session
	environments map[environmentKey]*control.Environment
	snapshots    map[environmentKey]control.RunnerID // holder of the cached snapshot
	runners      map[runnerKey]*control.Runner
	users, tokens, secrets, credentials …             // unchanged
}
```

`NewMemStore()` provisions `installWorkspace`. The three accessors return `memSessions{m}`, `memEnvironments{m}`, `memFleet{m}` — value types over the same `*memStore` — and each method is: check the key (`ErrInvalid` on empty), lock, look up (`ErrNotFound`), mutate a clone, store, return a clone. Rules the tables above imply and the code must follow:

- `CreateSession`: `PlacementGeneration` stored as `max(1, given)`; `ControllerGeneration` and `ChildExitCode` reset; timestamps defaulted to `time.Now()` when zero; the idempotency check runs before the name check and returns the existing row.
- `Transition`: the from-list guard; `RunnerID` non-nil sets the runner and, when non-empty, `PlacementGeneration++`; `Error` non-nil sets; both clocks bump.
- `ListSessions`: filter, sort by `(CreatedAt desc, ID desc)`, cursor `encodeCursor(createdAt, id)` as today, `ErrInvalid` on a cursor that does not decode.
- Environments: on read, `Requirements.Capabilities` is the stored list plus `SnapshotCapability(holder)` when `Snapshot.Ref != "" && holder != "" && SnapshotHash == SetupHash`; on write (`Create`/`Update`), `StripSnapshotCapabilities` first, and `Update` copies `Snapshot`/`SnapshotHash` forward from the stored row, ignoring the caller's.
- `SetEnvironmentSnapshot`: `ErrNotFound` when absent, `ErrStale` when `SetupHash != expectHash`, else `Snapshot = SnapshotCheckpoint(ref)`, `SnapshotHash = expectHash`, `snapshots[key] = runnerID`.
- Fleet: `UpsertRunner` compares generations (`ErrStale` when lower), stores a clone with capabilities cloned; `ListRunners` sorted by ID.
- Host lookups: `EnvironmentByName` scans the workspace; `SnapshotRunner` reads `snapshots`; `NextRunnerGeneration` inserts `{ID, PoolID, Generation: 1}` when absent, else `Generation++`, and returns it.

The **old** `Store` methods stay as thin conversions over these rows so everything else keeps compiling: `CreateSession(ctx, s Session)` converts with `sessionFromControl`… no — the O8 conversions go the other way. Move `sessionToControl`/`sessionFromControl`/`environmentToControl`/`environmentFromControl`/`runnerToControl` and their helpers from `adapt_store.go` into `memstore.go` unchanged (they are what the old methods now need), then each old method is `convert in → native call under installWorkspace/installPool → convert out`, mapping `control.ErrNotFound`/`ErrConflict` back to the private sentinels and a replayed idempotency key to `ErrIdemReplay` (the old contract). `SessionByName` scans for `(creator, name)` non-terminal. `adapt_store.go` keeps compiling by referring to the moved functions.

- [ ] **Step 1: Write the failing tests**

`internal/controld/memstore_test.go`:

```go
func TestMemStoreRepositories(t *testing.T) {
	repotest.Run(t, func(t *testing.T) repotest.Stores {
		st := controld.NewMemStore()
		return repotest.Stores{
			Sessions: st.Sessions(), Environments: st.Environments(), Fleet: st.Fleet(),
			Provision: st.EnsureWorkspace,
		}
	})
}

func TestMemStoreHost(t *testing.T) {
	storetest.RunHost(t, func(t *testing.T) controld.HostStore { return controld.NewMemStore() })
}
```

`storetest.RunHost` is the existing user, token, secret, and credential cases of `RunContract` copied verbatim (they do not move yet — `RunContract` keeps running the old methods until Task 5) plus four new ones: `EnsureWorkspace` twice is fine; `EnvironmentByName` finds a name only inside its workspace and is `ErrNotFound` otherwise; `SnapshotRunner` is `""` for a fresh environment, the holder after `SetEnvironmentSnapshot`, and still the holder after the setup hash moves on; `NextRunnerGeneration` returns 1, 2, 3 for `(pool_a, runner_a)`, 1 for `(pool_b, runner_a)`, and the fleet repository's `ListRunners` then shows generation 3 and 1.

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/controld -run 'TestMemStore(Repositories|Host)'`
Expected: compile failure — no `repotest`, no `Sessions()`, no `EnsureWorkspace`.

- [ ] **Step 3: Implement**

`controlapp/repotest` (the suite, every row of the three tables), `store.go` (the interfaces and helpers above; the old methods remain on `Store` for now), `memstore.go` (as specified), `storetest.RunHost`. Add `controlapp/repotest` to the `for pkg in control controlapp` loop in `scripts/check-public-control.sh` (the duplicate-model inventory section stays on `./control` only).

- [ ] **Step 4: Run the gates**

Run: `go test ./internal/controld/... ./controlapp/... -race -count=3 && scripts/check-public-control.sh && make verify`
Expected: both new suites and the old `TestMemStoreContract` pass; every existing controld test passes unmodified (they still use the old methods, which now run over control-typed rows).

- [ ] **Step 5: Leave the tree ready**

Report the commit message: `feat(controld): native workspace-keyed memstore and the repository contract suite`, with a body naming the suite's case count and that the old Store methods are conversions until Task 5.

---

### Task 3: pgstore — the expand migration and the native repositories

**Files:**
- Create: `internal/controld/pgstore/migrations/0007_workspace_scope.sql`
- Create: `internal/controld/pgstore/host.go`, `sessions.go`, `environments.go`, `fleet.go`
- Modify: `internal/controld/pgstore/pgstore.go` (accessors; the old `UpsertRunner`'s conflict target; nothing else old changes)
- Modify: `internal/controld/pgstore/pgstore_test.go`

**Interfaces:**
- Consumes: `controld.Store`/`HostStore`, `repotest`, `storetest.RunHost`, `controld.SnapshotCheckpoint`/`SnapshotCapability`/`StripSnapshotCapabilities`.
- Produces: `(*pgstore.Store).Sessions()/Environments()/Fleet()`, the four host lookups, and a schema at version 7.

The migration, verbatim:

```sql
-- 0007_workspace_scope.sql — O9 expand step.
--
-- Every tenant table carries its workspace; sessions carry their pool and
-- their placement and controller generations; runners carry their pool,
-- generation, and capabilities. The defaults name the one self-hosted
-- workspace and pool so this migration alone scopes every existing row and
-- the pre-O9 code paths keep working until the contract step (0008) removes
-- the defaults and the columns they replaced. Uniqueness becomes
-- workspace-composite here, because a constraint that ignores the workspace
-- is a cross-tenant collision waiting to happen.
CREATE TABLE workspaces (
  id text PRIMARY KEY,
  created_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO workspaces (id) VALUES ('ws_self_hosted');

ALTER TABLE sessions
  ADD COLUMN workspace_id text NOT NULL DEFAULT 'ws_self_hosted' REFERENCES workspaces(id),
  ADD COLUMN pool_id text NOT NULL DEFAULT 'pool_self_hosted',
  ADD COLUMN placement_generation bigint NOT NULL DEFAULT 1,
  ADD COLUMN controller_generation bigint NOT NULL DEFAULT 0;
-- The resolved image IS the session's image (control.PortableSpec.Image).
UPDATE sessions SET image = resolved_image WHERE resolved_image <> '';

DROP INDEX sessions_idem;
CREATE UNIQUE INDEX sessions_idem ON sessions(workspace_id, owner_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;
DROP INDEX sessions_owner_name_active;
CREATE UNIQUE INDEX sessions_owner_name_active ON sessions(workspace_id, owner_id, name)
  WHERE name <> '' AND state NOT IN ('canceled','failed','dead','destroyed');
DROP INDEX sessions_list;
CREATE INDEX sessions_list ON sessions(workspace_id, created_at DESC, id DESC);
DROP INDEX sessions_runner;
CREATE INDEX sessions_runner ON sessions(pool_id, runner) WHERE runner IS NOT NULL;
CREATE INDEX sessions_pool_queue ON sessions(pool_id, state, created_at ASC, id ASC);

ALTER TABLE environments
  ADD COLUMN workspace_id text NOT NULL DEFAULT 'ws_self_hosted' REFERENCES workspaces(id),
  ADD COLUMN requirements jsonb NOT NULL DEFAULT '{}';
-- An operator's runner pin becomes the portable capability the scheduler
-- already matches on (adapt_scope.go, placementCapabilityPrefix).
UPDATE environments
  SET requirements = jsonb_build_object('capabilities', jsonb_build_array('placement:' || placement))
  WHERE placement <> '';
ALTER TABLE environments DROP CONSTRAINT environments_name_key;
CREATE UNIQUE INDEX environments_workspace_name ON environments(workspace_id, name);

ALTER TABLE runners
  ADD COLUMN pool_id text NOT NULL DEFAULT 'pool_self_hosted',
  ADD COLUMN generation bigint NOT NULL DEFAULT 0,
  ADD COLUMN capabilities jsonb NOT NULL DEFAULT '[]';
ALTER TABLE runners DROP CONSTRAINT runners_pkey;
ALTER TABLE runners ADD PRIMARY KEY (pool_id, name);
```

The column `owner_id` keeps its name and holds `control.Session.CreatorID`; `name` on `runners` holds `control.Runner.ID`. Renames buy nothing here and would force every old query to change in this task.

The native implementations, by the rules the suite pins:

- `sessions.go`: `selectSessionCols` for the native path is `id, workspace_id, owner_id, name, image, cmd, egress_allow, repos, state, pool_id, runner, placement_generation, controller_generation, idempotency_key, error, environment_id, setup_hash, child_exit_code, created_at, updated_at, last_event_at` (no `resolved_image`). `CreateSession` inserts with `placement_generation = GREATEST(1, $n)`; a `23505` on `sessions_idem` is answered by selecting the row for `(workspace_id, owner_id, idempotency_key)` and returning it; on `sessions_owner_name_active` by `ErrConflict`; a `23503` (the workspace FK) by `ErrNotFound`. `Transition` is one statement:

  ```sql
  UPDATE sessions
  SET state = $1,
      runner = COALESCE($2, runner),
      placement_generation = placement_generation + CASE WHEN $2 IS NOT NULL AND $2 <> '' THEN 1 ELSE 0 END,
      error = COALESCE($3, error),
      updated_at = now(), last_event_at = now()
  WHERE workspace_id = $4 AND id = $5 AND state = ANY($6)
  ```

  with the same 0-rows disambiguation as today (`GetSession` → `ErrNotFound` else `ErrConflict`). `NextControllerGeneration` is `UPDATE sessions SET controller_generation = controller_generation + 1, updated_at = now() WHERE workspace_id = $1 AND id = $2 RETURNING controller_generation`, `ErrNotFound` on no row.
- `environments.go`: `requirements` is `{"capabilities":[…],"min_cpu":n,"min_memory_bytes":n,"min_disk_bytes":n}` with zero fields omitted (a small `requirementsJSON` struct with `omitempty`); on read the stored capabilities are decoded and `SnapshotCapability` appended under the same rule as memstore; on write `StripSnapshotCapabilities` first. `UpdateEnvironment` does not name the three snapshot columns in its `SET` list (as today). `SetEnvironmentSnapshot` distinguishes the two zero-row causes with a follow-up `GetEnvironment`: absent → `ErrNotFound`, present → `ErrStale`.
- `fleet.go`: `UpsertRunner` is

  ```sql
  INSERT INTO runners (pool_id, name, capacity_used, capacity_total, connected, generation, capabilities, last_seen_at)
  VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
  ON CONFLICT (pool_id, name) DO UPDATE SET
    capacity_used = EXCLUDED.capacity_used, capacity_total = EXCLUDED.capacity_total,
    connected = EXCLUDED.connected, generation = EXCLUDED.generation,
    capabilities = EXCLUDED.capabilities, last_seen_at = EXCLUDED.last_seen_at
  WHERE runners.generation <= EXCLUDED.generation
  ```

  and `RowsAffected() == 0` is `ErrStale`. `ListRunners` orders by `name`. `SessionsOnRunner` and `OldestQueued` add `pool_id = $1`.
- `host.go`: `EnsureWorkspace` is `INSERT … ON CONFLICT DO NOTHING`; `NextRunnerGeneration` is `INSERT INTO runners (pool_id, name, generation) VALUES ($1, $2, 1) ON CONFLICT (pool_id, name) DO UPDATE SET generation = runners.generation + 1 RETURNING generation`.
- Every native method checks its workspace or pool argument for emptiness before touching the pool and returns `ErrInvalid`; every unexpected pgx error is returned as `control.ErrUnavailable` — the DSN, the SQL, and the row never reach the caller (the old methods keep their `fmt.Errorf("pgstore: …: %w")` wrapping until Task 5 deletes them).

Old code touched in this task: the old `UpsertRunner`'s `ON CONFLICT (name)` becomes `ON CONFLICT (pool_id, name)` — the only old statement the new primary key breaks. The old `scanSession` and every other old statement name their columns explicitly, so the added columns are invisible to them, and the old inserts take the defaults.

- [ ] **Step 1: Write the failing tests**

`pgstore_test.go` gains, beside the existing `TestPGStoreContract`:

```go
func TestPGStoreRepositories(t *testing.T) {
	repotest.Run(t, func(t *testing.T) repotest.Stores {
		st := freshStore(t, startPostgres(t), t.Name()) // the file's existing helpers: a fresh database, migrated
		return repotest.Stores{Sessions: st.Sessions(), Environments: st.Environments(), Fleet: st.Fleet(), Provision: st.EnsureWorkspace}
	})
}

func TestPGStoreHost(t *testing.T) {
	storetest.RunHost(t, func(t *testing.T) controld.HostStore { return freshStore(t, startPostgres(t), t.Name()) })
}

// TestMigration0007BackfillsExistingRows proves the expand step against rows
// the pre-O9 code wrote: they come out scoped, their resolved image is their
// image, and an operator's pin is a capability.
func TestMigration0007BackfillsExistingRows(t *testing.T) {
	pool := rawPoolAt(t, startPostgres(t), 6) // a new test helper: a fresh database migrated by migrateTo(ctx, pool, 6) — add migrateTo to migrate.go (unexported; Migrate becomes migrateTo(ctx, pool, maxInt)), the way TestMigrate0003To0004AddsColumnsToLegacyRows already seeds legacy rows
	mustExec(t, pool, `INSERT INTO users (id, github_id, login, role) VALUES ('usr_example', 1, 'octocat-example', 'admin')`)
	mustExec(t, pool, `INSERT INTO environments (id, name, image, setup_hash, placement) VALUES ('env_example', 'py', 'img:1', 'h1', 'vm1')`)
	mustExec(t, pool, `INSERT INTO sessions (id, owner_id, name, image, resolved_image, state, runner, environment_id) VALUES ('sess_example', 'usr_example', 'dev', '', 'rainier-env:env_example-abc', 'running', 'vm1', 'env_example')`)
	mustExec(t, pool, `INSERT INTO runners (name, capacity_total, connected) VALUES ('vm1', 4, true)`)
	if err := Migrate(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	st := &Store{pool: pool}
	row, err := st.Sessions().GetSession(context.Background(), "ws_self_hosted", "sess_example")
	if err != nil || row.Spec.Image != "rainier-env:env_example-abc" || row.PoolID != "pool_self_hosted" || row.PlacementGeneration != 1 || row.RunnerID != "vm1" {
		t.Fatalf("session after 0007: %+v, %v", row, err)
	}
	env, err := st.Environments().GetEnvironment(context.Background(), "ws_self_hosted", "env_example")
	if err != nil || !slices.Equal(env.Requirements.Capabilities, []string{"placement:vm1"}) {
		t.Fatalf("environment after 0007: %+v, %v", env, err)
	}
	runners, err := st.Fleet().ListRunners(context.Background(), "pool_self_hosted")
	if err != nil || len(runners) != 1 || runners[0].ID != "vm1" || runners[0].Generation != 0 {
		t.Fatalf("runners after 0007: %+v, %v", runners, err)
	}
}

// TestNextRunnerGenerationSurvivesReopen is the restart case: a second Store
// over the same database continues the sequence.
func TestNextRunnerGenerationSurvivesReopen(t *testing.T) {
	dsn := startPostgres(t)
	a := freshStore(t, dsn, t.Name())
	for want := uint64(1); want <= 2; want++ {
		if got, _ := a.NextRunnerGeneration(ctx, "pool_a", "runner_a"); got != want {
			t.Fatalf("gen = %d, want %d", got, want)
		}
	}
	a.Close()
	b := reopen(t, dsn, t.Name()) // Open on the database freshStore created, without dropping it
	if got, _ := b.NextRunnerGeneration(ctx, "pool_a", "runner_a"); got != 3 {
		t.Fatalf("after reopen gen = %d, want 3", got)
	}
}
```

Migration replay (`Migrate` twice is a no-op at version 7) is asserted by extending the file's existing migration test.

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/controld/pgstore -run 'TestPGStore(Repositories|Host)|TestMigration0007|TestNextRunnerGeneration'`
Expected: compile failure on the accessors; after stubbing, `TestPGStoreContract` (old) fails on `UpsertRunner`'s conflict target once 0007 is present.

- [ ] **Step 3: Implement**

The migration, the four files, `migrateTo`, the accessor methods, the one old-statement fix.

- [ ] **Step 4: Run the gates**

Run: `go test ./internal/controld/pgstore -race -count=2 && go test ./internal/controld/... -race -count=2 && RAINIER_TEST_PG_DSN=… go test ./internal/e2e -count=1 && make verify`
Expected: old contract, new suites, backfill, reopen, e2e-over-pgstore all green.

- [ ] **Step 5: Leave the tree ready**

Commit message: `feat(pgstore): workspace-scoped schema (expand) and native control repositories`.

---

### Task 4: Compose controld over the native store

**Files:**
- Modify: `internal/controld/controld.go` (`Server.st Store` — the union; `compose()`; `New` ensures the workspace; `gens` deleted)
- Delete: `internal/controld/adapt_store.go` (the conversions moved to `memstore.go` in Task 2 stay there; everything else in the file goes: `storeSessions`, `storeEnvironments`, `storeFleet`, `runnerGenerations`, `controllerLeases`, `storeErr`, `snapshotCheckpointFormat`)
- Modify: `internal/controld/adapt_scope.go` (gains `runnerCapabilities`, `capabilityValue`; `snapshotCapabilityPrefix` moves to `store.go` beside `SnapshotCapability`)
- Modify: `internal/controld/adapt_host.go` (`installationPools` over `Fleet()`)
- Modify: `internal/controld/api.go`, `attach.go`, `runners.go`, `srpc.go` (the table below)
- Modify: `internal/controld/adapt_store_test.go` (only the tests of deleted code go; conversion tests move beside the conversions), `adapt_host_test.go`, `compose_test.go`, `runners_test.go` (two new tests)

**Interfaces:**
- Consumes: Task 2/3's `Store`.
- Produces: a controld whose every store access is a port call or a `HostStore` call. The old twin methods are no longer called by production code after this task (`grep -n "s\.st\.\|srv\.st\." internal/controld/*.go` shows only `HostStore` methods and the three accessors); tests still call them.

Every production call site, from `origin/main` at `88cac9b`:

| Site | Today | After |
|---|---|---|
| `controld.go` `compose()` | `storeSessions{st}`, `storeEnvironments{st}`, `&storeFleet{st, gens}` | `s.st.Sessions()`, `s.st.Environments()`, `s.st.Fleet()`; `New` first runs `st.EnsureWorkspace` under a 10s `context.Background()` timeout and fails closed on error |
| `adapt_host.go` `installationPools.EligiblePools` | `st.ListRunners(ctx)`, capabilities synthesized by name | `st.Fleet().ListRunners(ctx, installPool)`; capacity summed and capabilities unioned from the rows' own `Capabilities` |
| `api.go:237,247` `sessionRenderer.freeSlots` | `st.ListRunners`, `st.SessionsOnRunner(ctx, name, []SessionState{StateCreating})` | `Fleet().ListRunners(ctx, installPool)`, `Fleet().SessionsOnRunner(ctx, installPool, r.ID, []control.SessionState{control.StateCreating})`; map keyed by `string(r.ID)` |
| `api.go:1453` `environmentRef` | `st.GetEnvironmentByName(ctx, ref)` then the service | `st.EnvironmentByName(ctx, scope.WorkspaceID, ref)` then the service |
| `api.go:1469` `snapshotRunnerOf` | `st.GetEnvironment` → `.SnapshotRunner` | `st.SnapshotRunner(ctx, installWorkspace, id)` |
| `api.go:1577` (the secret-in-use check) | `st.ListEnvironments(ctx)` | `Environments().ListEnvironments(ctx, installWorkspace, control.EnvironmentQuery{})` |
| `api.go:1751` | `st.CountSessionsByEnvironment(ctx, id, NonTerminal)` | `Environments().CountSessionsByEnvironment(ctx, installWorkspace, env.ID, control.NonTerminal)` |
| `attach.go:210,239` | `st.GetSession(ctx, id)`; `row.OwnerID`, `row.State`, `row.Runner` | `Sessions().GetSession(ctx, installWorkspace, control.SessionID(id))`; `CreatorID`, `State`, `RunnerID` |
| `runners.go:232` `handleRunnerConnect` | `rc.gen = s.gens.next(name)` | `rc.gen, err = s.st.NextRunnerGeneration(connCtx, installPool, control.RunnerID(name))`; on error log and close with `"registration refused"` |
| `runners.go:383` `touchRunner` | `st.UpsertRunner(ctx, Runner{…})`, error logged | `Fleet().UpsertRunner(ctx, installPool, control.Runner{ID, PoolID: installPool, CapacityUsed, CapacityTotal, Connected: true, Generation: rc.gen, Capabilities: runnerCapabilities(rc.name), LastSeenAt: time.Now()})`; `ErrStale` → log `"controld: runner %s: connection at generation %d is superseded"` and **return false** (the read loop ends and the deferred retire closes the socket); any other error logged as today |
| `runners.go:502,567,598,635,681` `applyAdapterArm`, `placedExactlyOn`, `cacheEnvironment`, `snapshotWanted`, `buildSnapshot` | twin `Session`/`Environment` via `st.GetSession`/`st.GetEnvironment` | `control.Session`/`control.Environment` via the ports; field renames (`Runner`→`RunnerID`, `OwnerID`→`CreatorID`, `Placement`→`capabilityValue(…)`, `SnapshotRef`→`Snapshot.Ref`) |
| `runners.go:709` `buildSnapshot` | `st.SetEnvironmentSnapshot(…)`, `ErrConflict` branch | `Environments().SetEnvironmentSnapshot(wctx, installWorkspace, env.ID, hash, ref, control.RunnerID(runnerName))`; the dropped-snapshot branch tests `control.ErrStale` **or** `control.ErrNotFound` (edited, or deleted) |
| `runners.go:932` `retireRunner` | `st.SetRunnerConnected(ctx, name, false)`; ignores private `ErrNotFound` | `Fleet().SetRunnerConnected(ctx, installPool, control.RunnerID(rc.name), false)`; ignores `control.ErrNotFound` |
| `runners.go:870` `connectRunner` | `Capabilities: runnerCapabilities(rc.name)` | unchanged — and now persisted by the native `UpsertRunner` |
| `srpc.go:168` | `st.GetSession(ctx, sessionID)` | `Sessions().GetSession(ctx, installWorkspace, control.SessionID(sessionID))` |
| `adapt_launch.go:55,86`, `auth.go`, `vault.go`, `api.go` secrets/credentials | `HostStore` methods | unchanged |

`errors.Is(err, ErrNotFound)` at the sites above that read through a port becomes `control.ErrNotFound`; the host-method sites keep the private sentinel until Task 5.

- [ ] **Step 1: Write the failing tests**

`runners_test.go`:

```go
// TestRunnerGenerationContinuesAcrossRestart pins that the generation is the
// store's, not the process's: a second Server over the same store registers
// the runner at the next generation, not at 1.
func TestRunnerGenerationContinuesAcrossRestart(t *testing.T) {
	st := NewMemStore()
	s1, ts1 := newTestControldOver(t, st)
	f := joinRunner(t, s1, ts1, runnerScript{name: "vm1", total: 4})
	awaitReconciled(t, f)
	ts1.Close()
	s2, ts2 := newTestControldOver(t, st)
	f2 := joinRunner(t, s2, ts2, runnerScript{name: "vm1", total: 4})
	awaitReconciled(t, f2)
	rows, err := st.Fleet().ListRunners(context.Background(), installPool)
	if err != nil || len(rows) != 1 || rows[0].Generation != 2 {
		t.Fatalf("after restart: %+v, %v; want vm1 at generation 2", rows, err)
	}
}

// TestSupersededConnectionIsFencedOnHeartbeat: once the store holds a newer
// generation for a runner (another replica registered it), this replica's
// connection is refused at its next heartbeat and ends.
func TestSupersededConnectionIsFencedOnHeartbeat(t *testing.T) {
	s, st, ts := newTestControld(t)
	f := joinRunner(t, s, ts, runnerScript{name: "vm1", total: 4})
	awaitReconciled(t, f)
	if _, err := st.NextRunnerGeneration(context.Background(), installPool, "vm1"); err != nil {
		t.Fatal(err)
	}
	f.write(t, runner.FromRunner{Type: "event", Session: "sess_nobody", State: "running", Used: 1, Total: 4}) // any message heartbeats
	eventually(t, 2*time.Second, func() error {
		if s.runnerConnected("vm1") {
			return errors.New("superseded connection still registered")
		}
		return nil
	})
	rows, _ := st.Fleet().ListRunners(context.Background(), installPool)
	if rows[0].Generation != 2 || rows[0].CapacityUsed != 0 {
		t.Fatalf("the stale heartbeat wrote through: %+v", rows[0])
	}
}
```

`compose_test.go` asserts `s.gens` no longer exists by not mentioning it, and adds: `New` over a store whose `EnsureWorkspace` fails returns an error.

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/controld -run 'TestRunnerGeneration|TestSuperseded'`
Expected: the first fails with generation 1 (process-local table restarted); the second times out (no fence on the heartbeat).

- [ ] **Step 3: Rewire**

The table, top to bottom. Delete `adapt_store.go` (moving the conversion tests into `memstore_test.go`'s package-internal companion if they still test something the old methods use; delete the rest with a named disposition in the report). `Server.gens` is gone; `installPlacement`, `userScope` unchanged.

- [ ] **Step 4: Run the gates**

Run: `go test ./internal/controld/... -race -count=3 && go test ./internal/e2e ./cmd/... ./internal/cli -count=1 && scripts/check-public-control.sh && make verify`
Expected: every existing test passes unmodified — they seed through the old methods, production reads through the new ones, and the rows are the same rows. `check-public-control.sh` still lists the migration inventory (the twins exist until Task 5) and reports no new duplicate.

- [ ] **Step 5: Leave the tree ready**

Commit message: `refactor(controld): compose over the native store; runner generations come from the store`.

---

### Task 5: Retire the twins, contract the schema, empty the allowlist

**Files:**
- Modify: every `internal/controld/*_test.go` that seeds or reads the store with a twin type (`api_test.go`, `runners_test.go`, `sched_test.go`, `attach_test.go`, `srpc_test.go`, `files_test.go`, `adapt_*_test.go`, `vault_test.go`, `auth_test.go`) — the seeding helpers first (`seedSession`, `seedEnv`, `cacheEnvSnapshot`, `getSession`, `wantState`, `envRow`, `countSessions`, `seedSetupSession`, `loginUser` stays), then their call sites
- Modify: `internal/controld/store.go` (delete `SessionState` and its constants, `NonTerminal`, `Session`, `RepoRef`, `Connector`, `Environment`, `Runner`, `ErrNotFound`, `ErrConflict`, `ErrIdemReplay`, `TransitionOpts`, `SessionQuery`, `SetupHash`; the old methods leave the `Store` interface; `User`, `SecretMeta`, `Credential`, the ID constructors, `NewToken`/`HashToken` stay)
- Modify: `internal/controld/memstore.go`, `pgstore/pgstore.go` (old methods and conversions deleted; host methods return `control` sentinels)
- Modify: `internal/controld/storetest/contract.go` (`RunContract` deleted; `RunHost` asserts `control.ErrNotFound`/`ErrConflict`)
- Modify: `internal/controld/auth.go`, `vault.go`, `api.go`, `adapt_launch.go` (host-method error checks → `control` sentinels)
- Create: `internal/controld/pgstore/migrations/0008_workspace_scope_contract.sql`
- Modify: `scripts/check-public-control.sh` (`allowlist=()`)
- Modify: `docs/deploy-gce.md`

**Interfaces:**
- Consumes: Task 4.
- Produces: one model. `go doc ./internal/controld | grep -c "type Session\b"` is 0.

The contract migration, verbatim:

```sql
-- 0008_workspace_scope_contract.sql — O9 contract step.
--
-- No code path relies on a default workspace or pool any more: every insert
-- names its scope. Dropping the defaults makes a missing scope a database
-- error instead of a silent write into the installation workspace. The
-- columns the expand step replaced go with them.
ALTER TABLE sessions
  ALTER COLUMN workspace_id DROP DEFAULT,
  ALTER COLUMN pool_id DROP DEFAULT,
  DROP COLUMN resolved_image;
DROP INDEX sessions_name_list;
DROP INDEX sessions_state_list;
ALTER TABLE environments
  ALTER COLUMN workspace_id DROP DEFAULT,
  DROP COLUMN placement;
ALTER TABLE runners
  ALTER COLUMN pool_id DROP DEFAULT;
```

Test-migration rules (the reviewer checks the diff against them):

1. A seeding helper's signature changes type, not meaning: `seedSession(t, st, Session{OwnerID: u.ID, State: StateRunning, Runner: "vm1"})` becomes `seedSession(t, st, control.Session{CreatorID: control.ActorID(u.ID), State: control.StateRunning, RunnerID: "vm1"})`, and the helper calls `st.Sessions().CreateSession(ctx, installWorkspace, s)`. A seeded `Image` for an environment session is the resolved image (`Spec.Image`).
2. `getSession`/`wantState` read through `Sessions().GetSession(ctx, installWorkspace, …)` and return `control.Session`; every field access at a call site is renamed, never dropped.
3. An assertion's expected value never changes. `if got.Runner != "vm1"` becomes `if got.RunnerID != "vm1"`; a state constant becomes the `control` one; nothing else.
4. Tests of the deleted old methods (`TestMemStoreContract`, `TestPGStoreContract`, the conversion tests) are deleted with a named disposition in the report: the `repotest`/`RunHost` case that covers the same behavior, or "behavior no longer exists" (only `SessionByName` and `ErrIdemReplay` qualify).
5. `countSessions` lists with `IncludeTerminal: true` and no limit.

- [ ] **Step 1: Make the guard the failing test**

Set `allowlist=()` in `scripts/check-public-control.sh`.

Run: `scripts/check-public-control.sh`
Expected: fails, naming every twin as a "new duplicate not in the freeze allowlist".

- [ ] **Step 2: Migrate the tests, delete the twins and the old methods, add 0008**

In that order, compiling as you go: helpers, call sites, then `store.go`, then the stores, then `storetest`, then the host-sentinel call sites, then the migration.

- [ ] **Step 3: Run the gates**

Run: `scripts/check-public-control.sh && go test ./internal/controld/... -race -count=3 && go test ./controlapp/... ./control -race -count=2 && RAINIER_TEST_PG_DSN=… go test ./internal/e2e -count=1 && make verify && git diff --check`
Expected: the guard passes with an empty inventory; every test green; `TestMigration0007BackfillsExistingRows` extended to also assert that after 0008 the `resolved_image` and `placement` columns are gone (`SELECT column_name FROM information_schema.columns WHERE table_name = 'sessions'`).

- [ ] **Step 4: Document the upgrade**

`docs/deploy-gce.md`, in the upgrade section that already says controld runs its migrations on start: add that this release applies 0007 and 0008, that 0008 drops two columns, and the two-line rehearsal — `pg_dump` before the upgrade; restore onto a scratch database and start the new controld against it first if the installation has state worth keeping.

- [ ] **Step 5: Leave the tree ready**

Commit message: `refactor(controld): one model — retire the control twins, contract the schema`.

---

## Reviewer procedure (per task)

1. Fresh worktree from the integration branch; apply the worker's tree; read the whole diff.
2. Run every gate in the task serially (never trust the report); for Task 3 and Task 5 also run the pgstore suite against docker PostgreSQL **and** against the e2e's `RAINIER_TEST_PG_DSN` path.
3. Task 5 diff review against the five test-migration rules; any changed expected value is a rejection.
4. Commit with the trailers; cherry-pick onto `feat/workspace-scope`; retire the task worktree.
5. Before the integration PR: on `rainier-1`, `pg_dump` the dogfood database, restore it into a scratch database, run the new `controld --db <scratch DSN>` once to migrate, and run the live-fleet e2e from the branch (`scripts/e2e-fleet.sh`, all scenes). Both must pass. Then the integration PR, then the dogfood upgrade.

## Acceptance

- `scripts/check-public-control.sh` passes with `allowlist=()`; `go list -deps ./control/... ./controlapp/...` shows no `internal/` package.
- `controlapp/repotest.Run` passes against memstore and pgstore; every one of the S/E/F cases exists by name.
- `sessions`, `environments`, and `runners` carry their scope columns without defaults; every uniqueness constraint on a tenant table includes `workspace_id`.
- A runner's generation continues across a controld restart; a superseded connection is fenced at its next heartbeat; a controller attach advances a stored generation that a second `Server` over the same store continues.
- The `/v0/` wire is byte-for-byte what O8 shipped: every response-shape and behavior test in `internal/controld`, `internal/e2e`, `cmd/rainier`, and `internal/cli` passes with only seeding changed.
- The dogfood database upgrades from schema 6 to 8 in one start, and the live-fleet e2e is green on `rainier-1` from the branch.

## Not in this plan

- Row-level security and a per-request workspace setting on the connection: the regional cell's store (rainier-cloud `internal/regional/postgres`) owns that; the OSS store proves the contract, not the defense in depth.
- Workspace-scoped secrets, memberships, roles, or a `pools` table: hosted identity and tenancy policy (rainier-cloud), and the OSS vault stays installation-wide.
- Connection-state authority across replicas (`SetRunnerConnected` is not generation-fenced): a second self-hosted replica is not a supported topology yet.
- Placement generations on the runner wire, the transactional outbox, portable checkpoint location, and capability negotiation: **plan 8**, `2026-08-30-control-outbox-checkpoint-capabilities.md`.
- Controller-lease expiry, handoff, and reclaim: the tenancy plan.

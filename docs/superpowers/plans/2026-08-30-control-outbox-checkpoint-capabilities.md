# Control Outbox, Checkpoint Location, and Capability Negotiation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every application mutation and the event it produces durable together (the transactional outbox), give the application a portable answer to "where can this checkpoint boot" so a cached snapshot stops pinning sessions to one runner, and let runners announce portable capabilities that the fleet persists, the scheduler matches, and controld acknowledges with the runner's generation — through one coordinated, additive contract amendment.

**Architecture:** Three host ports and two wire fields, each the smallest thing that closes a gap O8 documented. `control.UnitOfWork` lets a service say "these writes commit together" without a transaction type ever crossing the seam: hosts carry their transaction in the context, and the OSS pgstore proves the pattern with an `events` table written inside the same transaction as the row it describes. `control.CheckpointLocator` answers which runners can boot a checkpoint; the scheduler prefers them and falls back to a rebuild elsewhere, which restores the placement behavior O8 traded away as deviation D3, and retires the `snapshot:<runner>` capability spelling the stores derive today. The runner protocol gains an announced `capabilities` list and an `accept` message carrying the runner's generation; protocol version stays 1 because every field is additive and an old runner keeps working with an empty set.

**Tech Stack:** Go 1.25, PostgreSQL 16 via `jackc/pgx/v5`, `control` (amended once, in Task 1), `controlapp`, `protocol/runner`, `internal/runnerd`, `internal/controld`, the `rainier` CLI.

**Spec:** `rainier-cloud/docs/architecture/adr-0001-oss-cloud-composition.md` — "OSS application-service boundary" (host ports include checkpoint storage and event delivery), "Persistence model" ("State changes that feed billing, history, search, or operations write a durable regional outbox in the same transaction as the authoritative lifecycle change"), "Placement composition" (the scheduler selects within a pool using capabilities and artifact affinity), "Versioning and rollout compatibility" (runner protocols negotiate capabilities so a cell can roll runners without a flag day), migration step 6; `rainier-cloud/docs/superpowers/plans/2026-08-30-hosted-implementation-program.md` gate O9, fixed decision 3, Wave 5 ("transactional events, checkpoints, and capability negotiation"); `rainier-cloud/docs/superpowers/plans/2026-08-30-regional-durability.md` (the hosted store writes authoritative state plus its outbox row in one transaction through one private helper — the shape `UnitOfWork` composes with); `docs/superpowers/plans/2026-08-30-self-hosted-recomposition.md` deviation **D3** and "Not in this plan"; `docs/superpowers/plans/2026-08-30-workspace-scope-and-generations.md` (plan 7 — this plan starts from its merged store). OSS plan #8, the last before the tagged release (O10).

## Global Constraints

- Every task starts in a worktree from freshly fetched `origin/main` (Task 1, the reviewer's coordinated PR) or from the integration branch `feat/control-outbox` (every later task; Tasks 2 and 5 may run in parallel once Task 1 is merged, Tasks 3 and 4 follow Task 2). Nothing is pushed or merged by a worker.
- `control/*.go` changes exactly once, in Task 1. `protocol/runner/messages.go` changes exactly once, in Task 5, additively (`omitempty` fields and one new `ToRunner.Type`); `runner.ProtocolVersion` stays `1`. No exported identifier in `control`, `controlapp`, or `protocol/*` is renamed or removed.
- The application decides atomicity, the host provides it. After Task 3 no `controlapp` method records an event outside a `UnitOfWork.Run` that also holds the mutation the event describes; `wake` is called only after `Run` returns.
- A runner's announced capabilities are claims about itself and nothing else: a token matching `^[a-z0-9][a-z0-9._-]{0,63}$`, at most 32 of them, none carrying the host's own prefixes (`placement:`). Anything else refuses the registration. The two capabilities controld synthesizes for a runner's own name are the host's; an environment's `capabilities` field is validated by the same rule.
- The deviation table below is the complete list of user-visible changes; every other existing test in `internal/controld`, `internal/e2e`, `cmd/rainier`, and `internal/cli` passes unmodified.
- No secret value, credential, terminal byte, session content, or runner free text enters an event row, a store error, a log line added by this plan, a commit message, or test output. Tests use only synthetic `.test`, `.invalid`, `example.com`, `agents.localhost`, and fictional opaque IDs.
- Go gates run serially. Use `GOCACHE=/private/tmp/rainier-outbox-gocache`.
- Commit messages follow the repository style and end with the attribution trailers the reviewer supplies at commit time. Workers do not commit; they leave the tree ready and report.

## Deliberate deviations (the complete list)

| # | Today (after plan 7) | After this plan | Why | Tests that may change |
|---|---|---|---|---|
| D16 | A session created from an environment with a current snapshot shows the snapshot ref as its `image` from the moment it is created | It shows the environment's image until it is placed; once placed on a runner that holds the snapshot it shows the snapshot ref, and on any other runner it keeps the environment's image (and runs setup) | The image a session runs is decided at placement, where the holder is known — which is where controld decided it before O8 | `api_test.go` cases asserting `image` on a just-created session from a cached environment |
| D17 | Such a session is placed only on the snapshot's holder; if that runner is full or away the session stays queued (O8's D3) | It prefers the holder and, when the holder has no room or is not connected, boots the environment's image with setup on any eligible runner that does | Restores the pre-O8 behavior D3 gave up; a cache is a head start, never a pin | the `api_test.go`/`sched_test.go` cases D3 rewrote (restore their pre-O8 assertions) |
| D18 | `POST/PUT /v0/environments` accept `placement`; the view shows `placement` | They also accept and show `capabilities` (a JSON array of strings, `[]` when none); `placement` is unchanged | An environment can require a portable capability a runner announces | response-shape tests that pin the environment key set (one new key) |
| D19 | An announce carries `proto`, `runner`, `sessions`, `used`, `total` | It may also carry `capabilities`; controld answers a registered runner with `{"type":"accept","generation":N,"capabilities":[…]}` before any command; later `event`/`result` messages may carry `generation` | Capability negotiation and the runner's generation on the wire (ADR "Versioning and rollout compatibility") | `runners_test.go` fixtures that assert the first message a runner receives is a command (they now skip the `accept`); `internal/runnerd` tests |

Everything not in this table is a bug in the task that introduced it.

## File structure

```text
control/
  ports.go                     Task 1: UnitOfWork, CheckpointLocator, CheckpointLocation; TransitionOpts.Image
  contract_test.go             Task 1: fakes for the two ports
controlapp/
  sessions.go environments.go fleet.go attachments.go scheduler.go
                               Task 1: options gain UnitOfWork (and Fleet gains Checkpoints); Task 3: Run wraps;
                               Task 4: locator-aware placement
  uow_test.go                  Task 3: the atomicity contract of every command
  repotest/repotest.go         Task 2: TransitionOpts.Image case; Task 4: no snapshot capability on read
internal/controld/
  store.go                     Task 2: HostStore.SnapshotHolder, UnitOfWork/EventRecorder on Store;
                               Task 4: SnapshotCapability/StripSnapshotCapabilities deleted
  memstore.go                  Task 2: Run (direct), events slice, Events() for tests; Task 4: derivation gone
  pgstore/
    migrations/0009_events.sql Task 2
    tx.go                      Task 2: Run, the context-carried transaction, q(ctx)
    events.go                  Task 2: Record
    host.go                    Task 2: SnapshotHolder
    *.go                       Task 2: every statement runs on q(ctx)
    pgstore_test.go            Task 2: atomicity, nesting, replay
  adapt_host.go                Task 3: logRecorder deleted (the store records); Task 4: installationCheckpoints
  controld.go                  Task 3/4: compose the unit of work, the recorder, the locator
  runners.go                   Task 5: announced capabilities validated and unioned; accept sent; generation read
  api.go                       Task 5: environments.capabilities; queue_reason names a missing capability
protocol/runner/messages.go    Task 5: FromRunner.Capabilities, FromRunner.Generation; ToRunner "accept",
                               ToRunner.Generation, ToRunner.Capabilities
internal/runnerd/runnerd.go agent.go   Task 5: Config.Capabilities; announce; accept handling; generation stamping
cmd/runnerd/main.go            Task 5: --capability (repeatable) / RAINIER_RUNNER_CAPABILITIES
cmd/rainier/main.go            Task 5: --capability on env create/update
scripts/e2e-fleet.sh           Task 5: one capability scene
docs/deploy-gce.md README.md   Task 6
```

---

### Task 1: Amend the contract — unit of work, checkpoint location, placement-resolved image (reviewer)

The one coordinated `control` change of the plan; additive; its own PR before any worker starts. Behavior is unchanged after this task: the new ports are wired with implementations that do exactly what happens today.

**Files:**
- Modify: `control/ports.go`, `control/session.go` (`TransitionOpts`), `control/contract_test.go`
- Modify: `controlapp/sessions.go`, `environments.go`, `fleet.go`, `attachments.go` (options and constructors), every `controlapp/*_test.go` constructor call
- Modify: `internal/controld/controld.go` (`compose`), `adapt_host.go`

**Interfaces:**
- Produces, in `control/ports.go`:

```go
// UnitOfWork is the host's atomicity: Run executes fn so that every
// repository write and event record fn makes through the context it is
// handed commits together or not at all. A nested Run joins the enclosing
// unit rather than starting another. A host without transactions (an
// in-memory store) runs fn directly. Run returns fn's error unchanged, and
// ErrUnavailable when the unit itself cannot be opened or committed; no
// transaction, connection, or driver type crosses this port.
type UnitOfWork interface {
	Run(ctx context.Context, fn func(ctx context.Context) error) error
}

// CheckpointLocation is where a checkpoint can be booted: on any runner of
// the pool (Portable), or only on the named ones. Both empty means nowhere
// — the checkpoint is not usable right now and the caller boots without it.
type CheckpointLocation struct {
	Portable bool
	Runners  []RunnerID
}

// CheckpointLocator is the host's knowledge of where checkpoint artifacts
// live. Self-hosted snapshots are container images on the runner that built
// them; hosted checkpoints are regional objects any capable runner restores.
// The application asks and never assumes.
type CheckpointLocator interface {
	LocateCheckpoint(ctx context.Context, ws WorkspaceID, cp Checkpoint) (CheckpointLocation, error)
}
```

- Produces, in `control/session.go`, one field on `TransitionOpts`:

```go
	// Image, when non-nil, records the image this placement resolved for the
	// session (Spec.Image): the cached checkpoint on a runner that holds it,
	// else the environment's own image. Set only by a placement transition.
	Image *string
```

- Produces, in `controlapp`: `SessionOptions.UnitOfWork`, `EnvironmentOptions.UnitOfWork`, `FleetOptions.UnitOfWork`, `FleetOptions.Checkpoints control.CheckpointLocator`, `AttachmentOptions.UnitOfWork` — all required (`control.ErrInvalid` when nil). The services store them; nothing calls them yet.
- Produces, in `internal/controld/adapt_host.go`: `directUnitOfWork{}` (`Run` calls `fn(ctx)`) and `pinnedCheckpoints{st}` whose `LocateCheckpoint` returns `{Runners: [holder]}` from `HostStore.SnapshotRunner` of the environment whose `Snapshot.Ref` equals `cp.Ref` — until Task 2 adds the direct lookup, it lists the workspace's environments to find it. `compose()` passes both.

- [ ] **Step 1: Write the failing test**

`control/contract_test.go` `TestIdealCallSite` grows two fakes (`fakeUnitOfWork`, `fakeCheckpointLocator`) and asserts the ports are satisfiable from an external package; `controlapp` constructor tests assert `ErrInvalid` for a nil `UnitOfWork`/`Checkpoints`.

- [ ] **Step 2: Run to verify it fails** — compile failure.
- [ ] **Step 3: Implement** the additions above; every `controlapp` test constructor gets `UnitOfWork: directUOW{}` (a test stub in one shared `_test.go` file) and `Checkpoints: locatorStub{}`.
- [ ] **Step 4: Gates** — `go test ./control ./controlapp ./internal/controld/... -race -count=2 && scripts/check-public-control.sh && make verify`.
- [ ] **Step 5: Commit and open the coordinated PR** — `feat(control): unit-of-work and checkpoint-locator ports; placement-resolved image`.

---

### Task 2: The stores — transactions, the events table, the checkpoint holder

**Files:**
- Create: `internal/controld/pgstore/migrations/0009_events.sql`, `pgstore/tx.go`, `pgstore/events.go`
- Modify: `internal/controld/pgstore/*.go` (every statement runs on `s.q(ctx)`), `pgstore/host.go` (`SnapshotHolder`), `pgstore_test.go`
- Modify: `internal/controld/store.go` (`Store` embeds `control.UnitOfWork` and `control.EventRecorder`; `HostStore.SnapshotHolder`), `memstore.go`, `memstore_test.go`, `storetest/contract.go` (`RunHost` cases), `controlapp/repotest/repotest.go` (one case)

**Interfaces:**
- Produces:

```go
// on controld.Store
type Store interface {
	HostStore
	control.UnitOfWork
	control.EventRecorder
	Sessions() control.SessionRepository
	Environments() control.EnvironmentRepository
	Fleet() control.FleetRepository
}
// on HostStore
	// SnapshotHolder names the runner holding the snapshot with ref in ws,
	// "" when no environment's current cache has it.
	SnapshotHolder(ctx context.Context, ws control.WorkspaceID, ref string) (control.RunnerID, error)
```

  and on memstore only, for tests: `Events() []control.Event` (a copy of everything recorded, in order).

The migration, verbatim:

```sql
-- 0009_events.sql — the durable application-event table.
--
-- One row per control.Event: fixed, provider-neutral fields and nothing
-- else — no terminal data, content, secret, raw error, price, or provider
-- resource. It is written inside the same transaction as the mutation it
-- describes (pgstore.Store.Run), which is what makes the event a fact rather
-- than a hope. Nothing in this release reads it back; the hosted cell's
-- outbox dispatcher is the consumer this shape is for.
CREATE TABLE events (
  id text PRIMARY KEY,
  workspace_id text NOT NULL REFERENCES workspaces(id),
  actor_id text NOT NULL,
  action text NOT NULL,
  resource_kind text NOT NULL,
  resource_id text NOT NULL,
  resource_workspace_id text NOT NULL,
  resource_creator_id text NOT NULL DEFAULT '',
  at timestamptz NOT NULL,
  cpu_time_seconds double precision NOT NULL DEFAULT 0,
  memory_byte_seconds bigint NOT NULL DEFAULT 0,
  storage_bytes bigint NOT NULL DEFAULT 0,
  network_bytes bigint NOT NULL DEFAULT 0,
  agent_token_count bigint NOT NULL DEFAULT 0,
  recorded_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX events_workspace_at ON events(workspace_id, at DESC, id DESC);
```

`pgstore/tx.go`:

```go
// querier is what every statement runs on: the transaction the context
// carries when a Run is open, else the pool.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type txKey struct{}

func (s *Store) q(ctx context.Context) querier {
	if tx, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
		return tx
	}
	return s.pool
}

// Run opens one transaction, hands fn a context that carries it, and commits
// when fn returns nil. A context already carrying a transaction joins it:
// fn runs inside the enclosing unit and the outer Run commits. A failure to
// begin or commit is control.ErrUnavailable; fn's own error is returned as
// is after the rollback.
func (s *Store) Run(ctx context.Context, fn func(context.Context) error) error {
	if _, nested := ctx.Value(txKey{}).(pgx.Tx); nested {
		return fn(ctx)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return control.ErrUnavailable
	}
	defer tx.Rollback(ctx)
	if err := fn(context.WithValue(ctx, txKey{}, tx)); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return control.ErrUnavailable
	}
	return nil
}
```

Every `s.pool.Exec/Query/QueryRow` in the package becomes `s.q(ctx).…` (a `grep -c "s.pool\." internal/controld/pgstore/*.go` of zero outside `Open`, `Close`, and `tx.go` is the check). `events.go` `Record` inserts the row on `s.q(ctx)`; a duplicate id is `control.ErrConflict`; the FK on the workspace is `control.ErrNotFound`. `SnapshotHolder` is `SELECT snapshot_runner FROM environments WHERE workspace_id = $1 AND snapshot_ref = $2 AND snapshot_hash = setup_hash LIMIT 1` (`""`, nil when none).

memstore: `Run` calls `fn(ctx)` (the contract allows a host without transactions; a memstore is never the durable store of anything); `Record` appends a clone to `events`; `SnapshotHolder` scans. The `TransitionOpts.Image` rule in both stores: when non-nil, `Spec.Image` (memstore) / `image` (pgstore, `image = COALESCE($n, image)`) is set by the same statement as the state.

`repotest` gains S11: a placement transition with `Image: &"snap:1"` sets `Spec.Image`; one with `Image: nil` leaves it. `storetest.RunHost` gains: `SnapshotHolder` is `""` before a snapshot, the holder after, `""` again once the setup hash moves on; `Record` then reading back (memstore `Events()`; pgstore via a `SELECT count(*)` in the pgstore test).

- [ ] **Step 1: Write the failing tests**

`pgstore_test.go`:

```go
// TestRunIsAtomic: a unit that creates a session, records its event, and
// then fails leaves neither the row nor the event.
func TestRunIsAtomic(t *testing.T) {
	st := freshStore(t, startPostgres(t), t.Name())
	ctx := context.Background()
	st.EnsureWorkspace(ctx, "ws_alpha")
	boom := errors.New("boom")
	err := st.Run(ctx, func(ctx context.Context) error {
		if _, err := st.Sessions().CreateSession(ctx, "ws_alpha", control.Session{ID: "sess_example", CreatorID: "act_a", State: control.StateQueued, PoolID: "pool_a"}); err != nil {
			return err
		}
		if err := st.Record(ctx, control.Event{ID: "evt_example", WorkspaceID: "ws_alpha", ActorID: "act_a", Action: control.ActionCreate,
			Resource: control.Resource{Kind: control.ResourceSession, WorkspaceID: "ws_alpha", ID: "sess_example", CreatorID: "act_a"}, At: time.Now()}); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Run returned %v, want fn's own error", err)
	}
	if _, err := st.Sessions().GetSession(ctx, "ws_alpha", "sess_example"); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("the row survived the rollback: %v", err)
	}
	var n int
	st.pool.QueryRow(ctx, `SELECT count(*) FROM events`).Scan(&n)
	if n != 0 {
		t.Fatalf("%d events survived the rollback", n)
	}
}

// TestRunNestsAndCommitsOnce: an inner Run joins the outer one; the write
// is visible only after the outer commit.
func TestRunNestsAndCommitsOnce(t *testing.T) {
	st := freshStore(t, startPostgres(t), t.Name())
	ctx := context.Background()
	st.EnsureWorkspace(ctx, "ws_alpha")
	err := st.Run(ctx, func(outer context.Context) error {
		if err := st.Run(outer, func(inner context.Context) error {
			_, err := st.Sessions().CreateSession(inner, "ws_alpha", control.Session{ID: "sess_example", CreatorID: "act_a", State: control.StateQueued, PoolID: "pool_a"})
			return err
		}); err != nil {
			return err
		}
		// Not yet visible outside the unit.
		if _, err := st.Sessions().GetSession(ctx, "ws_alpha", "sess_example"); !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("uncommitted row visible outside: %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Sessions().GetSession(ctx, "ws_alpha", "sess_example"); err != nil {
		t.Fatalf("committed row missing: %v", err)
	}
}
```

- [ ] **Step 2: Run to verify they fail** — compile failure (`Run`, `Record`).
- [ ] **Step 3: Implement** as specified.
- [ ] **Step 4: Gates** — `go test ./internal/controld/... ./controlapp/... -race -count=3 && RAINIER_TEST_PG_DSN=… go test ./internal/e2e -count=1 && make verify`.
- [ ] **Step 5: Leave the tree ready** — `feat(controld): transactional unit of work, the events table, and the checkpoint holder`.

---

### Task 3: Atomic commands

**Files:**
- Modify: `controlapp/sessions.go`, `environments.go`, `fleet.go`, `attachments.go`
- Create: `controlapp/uow_test.go`
- Modify: `internal/controld/controld.go` (`compose` passes the store as `UnitOfWork` and `EventRecorder`), `adapt_host.go` (`logRecorder` deleted), `adapt_host_test.go`

**Interfaces:**
- Consumes: Task 1's ports, Task 2's stores.
- Produces: the rule "mutation and its event commit together", held by `controlapp`.

The wrapping, method by method (`grep -n "recordEvent\|s\.record(\|recordLifecycleEvent" controlapp/*.go` is the complete list of event sites; each gets its mutation inside the same `Run`):

| Method | Inside `Run` | After `Run` |
|---|---|---|
| `SessionService.CreateSession` | `CreateSession` (repo) + `recordEvent` | `s.wake(pool)` |
| `DeleteSession` / `SuspendSession` / `ResumeSession` | the guarded `Transition` + `recordEvent` (the runner dispatch that precedes it stays outside — a transport call is not a store write) | any wake |
| `SnapshotSession` | `recordEvent` (its only write) | — |
| `EnvironmentService.Create/Update/DeleteEnvironment` | the repo write + `recordEvent` | — |
| `FleetService.ApplyRunnerEvent` | the `Transition` or `SetChildExitCode` + `recordLifecycleEvent` | `s.Wake(pool)` |
| `AttachmentService.AttachTerminal` and the workspace RPCs | `s.record` (their only write) | — |

Not wrapped, deliberately: `RegisterRunner`, `ReconcileRunner`, and `drainPool`'s placement transition — single writes with no event, retried by the connection or the safety tick.

- [ ] **Step 1: Write the failing test**

`controlapp/uow_test.go`:

```go
// countingUOW counts open units and refuses to commit when told to. Every
// repository stub in this file reports the unit depth it was called at.
type countingUOW struct {
	depth, runs int
	failCommit  bool
}

func (u *countingUOW) Run(ctx context.Context, fn func(context.Context) error) error {
	u.runs++
	u.depth++
	defer func() { u.depth-- }()
	if err := fn(context.WithValue(ctx, depthKey{}, u.depth)); err != nil {
		return err
	}
	if u.failCommit {
		return control.ErrUnavailable
	}
	return nil
}

// TestCreateSessionCommitsRowAndEventTogether: the row write and the event
// record happen at depth 1 of one unit, and a unit that cannot commit fails
// the create with ErrUnavailable without waking the scheduler.
func TestCreateSessionCommitsRowAndEventTogether(t *testing.T) {
	uow := &countingUOW{}
	repo, rec := newDepthRecordingRepo(), newDepthRecordingRecorder()
	woke := 0
	svc := newSessionServiceWith(t, SessionOptions{UnitOfWork: uow, Sessions: repo, Events: rec, Wake: func(control.PoolID) { woke++ } /* remaining stubs */})
	if _, err := svc.CreateSession(ctx, alphaScope(), control.CreateSession{Name: "investigate"}); err != nil {
		t.Fatal(err)
	}
	if uow.runs != 1 || repo.createDepth != 1 || rec.recordDepth != 1 || woke != 1 {
		t.Fatalf("runs %d, create at depth %d, record at depth %d, woke %d; want 1/1/1/1", uow.runs, repo.createDepth, rec.recordDepth, woke)
	}
	uow.failCommit = true
	if _, err := svc.CreateSession(ctx, alphaScope(), control.CreateSession{Name: "again"}); !errors.Is(err, control.ErrUnavailable) {
		t.Fatalf("uncommittable create: err = %v, want ErrUnavailable", err)
	}
	if woke != 1 {
		t.Fatalf("scheduler woken for a create that did not commit")
	}
}
```

One such test per row of the table above (same shape; the stubs report depth per method).

- [ ] **Step 2: Run to verify they fail** — depths are 0 and `runs` is 0.
- [ ] **Step 3: Implement** — wrap; `wake` after; `Run`'s `ErrUnavailable` propagates as `ErrUnavailable`. `compose()` passes `s.st` for both ports; delete `logRecorder` and its test.
- [ ] **Step 4: Gates** — `go test ./controlapp -race -count=5 && go test ./internal/controld/... -race -count=3 && RAINIER_TEST_PG_DSN=… go test ./internal/e2e -count=1 && make verify`. Then, on pgstore: a controld API test (`api_test.go`, run with `RAINIER_TEST_PG_DSN`) that creates a session and asserts one `events` row for it.
- [ ] **Step 5: Leave the tree ready** — `feat(controlapp): every command commits its mutation and event in one unit of work`.

---

### Task 4: Portable checkpoint placement (D16, D17)

**Files:**
- Modify: `controlapp/sessions.go` (`portableSpecFor`), `controlapp/scheduler.go` (`drainPool`, `pickForEnvironment`, `dispatchCreate`, `createSpec`), `controlapp/scheduler_test.go`, `controlapp/sessions_test.go`
- Modify: `controlapp/repotest/repotest.go` (E6: no `snapshot:` capability is emitted)
- Modify: `internal/controld/store.go` (`SnapshotCapability`/`StripSnapshotCapabilities` deleted), `memstore.go`, `pgstore/environments.go` (the derivation and the stripping deleted), `adapt_scope.go` (`snapshotCapabilityPrefix` deleted; `runnerCapabilities` returns only the placement capability), `adapt_host.go` (`pinnedCheckpoints` becomes `installationCheckpoints` over `SnapshotHolder`)
- Modify: `internal/controld/api_test.go`, `sched_test.go` (D16/D17 rows only)

**Interfaces:**
- Consumes: `control.CheckpointLocator`, `TransitionOpts.Image`.
- Produces: placement that prefers a holder and falls back.

The rules:

1. `portableSpecFor`: an environment session with no image override stores `spec.Image = env.Image`, never the snapshot ref. (`runsCachedSnapshot` stays, for the scheduler.)
2. `drainPool`, for each queued row with an environment whose snapshot is current (`runsCachedSnapshot(env)`): `loc, err := s.checkpoints.LocateCheckpoint(ctx, row.WorkspaceID, env.Snapshot)`; an error means "nowhere" (log nothing; treat as `CheckpointLocation{}`). Candidates are the runners with room that hold every required capability (`pickForEnvironment`'s filter). Preferred are the candidates `loc` admits (all of them when `Portable`, else those in `loc.Runners`). Pick from preferred first (most free, then ID); if none, pick from all candidates. The placement transition carries `Image: &env.Snapshot.Ref` when the pick was preferred, else `Image: &env.Image`; `row.Spec.Image` is set to the same value before `dispatchCreate` receives it.
3. `createSpec` keeps its rule verbatim: setup is sent iff `row.Spec.Image != env.Snapshot.Ref` — which is now exactly "this placement did not boot the snapshot".
4. A scratch session, or an environment without a current snapshot, places exactly as today.
5. The stores no longer emit or strip a `snapshot:` capability; `runnerCapabilities(name)` is `[]string{placementCapabilityPrefix + name}`; the self-hosted locator answers from `SnapshotHolder`, `{Runners: [holder]}` when connected-or-not (the scheduler's candidate filter already excludes a disconnected runner), `{}` when there is none.

- [ ] **Step 1: Write the failing tests**

`controlapp/scheduler_test.go`, three cases on the existing scheduler harness (two runners `runner_a` (holder) and `runner_b`, an environment `env_example` with `Snapshot.Ref: "snap:1"`, `SnapshotHash == SetupHash`, a locator stub returning `{Runners: ["runner_a"]}`):

```go
// TestPlacementPrefersTheSnapshotHolder: both runners have room; the session
// lands on runner_a booting the snapshot with no setup.
// TestPlacementFallsBackWhenTheHolderIsFull: runner_a has no room; the
// session lands on runner_b booting env.Image WITH setup, and the row's
// Spec.Image is env.Image.
// TestPortableCheckpointBootsAnywhere: the locator answers {Portable: true};
// runner_b (more free) is chosen and boots the snapshot with no setup.
```

Each asserts the dispatched `runner.ToRunner.Spec.Image`, `Spec.Setup`, and the stored row's `RunnerID`, `Spec.Image`, `PlacementGeneration == 2`. `controlapp/sessions_test.go`: a create from a cached environment stores `Spec.Image == env.Image`.

`internal/controld/api_test.go` / `sched_test.go`: restore the pre-O8 assertions of the cases D3 changed (the busy-holder fallback boots the image with setup on the other runner; the session view shows the environment image while queued and the snapshot ref once placed on the holder).

- [ ] **Step 2: Run to verify they fail** — the holder is pinned; the fallback never happens; the view shows the snapshot ref at create.
- [ ] **Step 3: Implement** the five rules; delete the derivation.
- [ ] **Step 4: Gates** — `go test ./controlapp/... ./internal/controld/... -race -count=3 && go test ./internal/e2e -count=1 && scripts/check-public-control.sh && make verify`.
- [ ] **Step 5: Leave the tree ready** — `feat: place a session where its checkpoint can boot, and fall back to a rebuild`.

---

### Task 5: Capability negotiation (D18, D19)

**Files:**
- Modify: `protocol/runner/messages.go`
- Modify: `internal/runnerd/runnerd.go` (`Config.Capabilities []string`), `internal/runnerd/agent.go` (announce; `accept` handling; `Generation` on every later `FromRunner`), `internal/runnerd/agent_test.go`
- Modify: `cmd/runnerd/main.go` (`--capability`, repeatable; `RAINIER_RUNNER_CAPABILITIES` comma-separated)
- Modify: `internal/controld/runners.go` (`connectRunner`: validate and union; send `accept`; `applyRunnerEvent`: use the message's `Generation` when non-zero), `runners_test.go`
- Modify: `internal/controld/api.go` (`createEnvironmentRequest.Capabilities`, `updateEnvironmentRequest.Capabilities *[]string`, `environmentView.Capabilities`, `queueReason`), `api_test.go`
- Modify: `cmd/rainier/main.go` (`--capability`, repeatable, on the environment create and update subcommands, beside the existing placement flag), `cmd/rainier/main_test.go`, `internal/cli` if the client type carries the environment body
- Modify: `scripts/e2e-fleet.sh` (the environments scene: one runner started with `--capability e2e.gpu`, an environment requiring it, a session that lands there; a second environment requiring `e2e.none` whose session shows `queue_reason` naming it)

**Interfaces:**
- Produces, in `protocol/runner/messages.go`:

```go
// on FromRunner
	// Capabilities are the portable runtime capabilities this runner claims
	// on an announce: lowercase tokens such as "gpu" or "docker.rootless".
	// Absent means none — an old runner is a runner with no capabilities,
	// and every environment that requires one simply never lands on it.
	Capabilities []string `json:"capabilities,omitempty"` // announce
	// Generation is the runner generation controld granted in its accept,
	// echoed on later events and results so a report from a superseded
	// connection can be fenced by the store rather than by the socket it
	// arrived on. Zero means "the connection's" (an old runner).
	Generation uint64 `json:"generation,omitempty"` // event, result
// on ToRunner
	// "accept" is controld's answer to an announce, sent before any command:
	// the generation this connection acts under and the announced
	// capabilities controld will schedule on.
	Type string `json:"type"` // … | "accept"
	Generation   uint64   `json:"generation,omitempty"`   // accept
	Capabilities []string `json:"capabilities,omitempty"` // accept
```

- Produces, on the environment wire: `capabilities` in the create and update bodies and the view.
- Produces, in `internal/controld/runners.go`: `validateRunnerCapabilities(caps []string) error` (the token rule; at most 32; no `placement:` prefix; no duplicates) and the union `append(runnerCapabilities(name), announced...)` registered and persisted.

- [ ] **Step 1: Write the failing tests**

`runners_test.go`: a runner announcing `["gpu", "docker.rootless"]` is registered with `["placement:vm1", "gpu", "docker.rootless"]` (assert via `st.Fleet().ListRunners`) and receives `{"type":"accept","generation":1,"capabilities":["gpu","docker.rootless"]}` as its first message; one announcing `["placement:other"]` or `["GPU"]` is closed with `"registration refused"`; an event carrying `generation: 1` after the store moved to 2 is ignored (`ErrStale` path) while the same event with no generation uses the connection's. `api_test.go`: `capabilities` round-trips on create/get/update, is `[]` when absent, rejects `["GPU"]` with 400, and `queue_reason` for a queued session whose environment requires `gpu` with no such runner connected is `"waiting for a runner with capability gpu"`. `internal/runnerd/agent_test.go`: the announce carries `cfg.Capabilities`; after an `accept` with generation 7, the next event carries `Generation: 7`. `cmd/rainier/main_test.go`: `--capability gpu --capability docker.rootless` sends `"capabilities":["gpu","docker.rootless"]`.

- [ ] **Step 2: Run to verify they fail** — unknown fields; unknown flag; the first message is a command.
- [ ] **Step 3: Implement** — protocol fields; runnerd config/flag/announce/accept (`execute`'s switch gains `case "accept"`: store the generation, log `agent: accepted at generation %d with %d capabilities`); controld validation, union, `accept` send right after `registerRunner` succeeds and before reconcile's destroys; `applyRunnerEvent` generation source; the API field with the same validation; the CLI flag; the e2e scene.
- [ ] **Step 4: Gates** — `go test ./protocol/... ./internal/runnerd ./internal/controld/... ./cmd/... ./internal/cli -race -count=3 && scripts/check-public-protocols.sh && go test ./internal/e2e -count=1 && make verify`; then the live-fleet e2e on `rainier-1` from the branch (the reviewer).
- [ ] **Step 5: Leave the tree ready** — `feat: runners announce portable capabilities; controld accepts with the generation`.

---

### Task 6: Documentation (reviewer)

- `README.md`: the environments section gains `capabilities` beside `placement`; the runner section gains `--capability`.
- `docs/deploy-gce.md`: migration 0009; `RAINIER_RUNNER_CAPABILITIES` in the runnerd unit.
- `controlapp/doc.go`: the atomicity rule and the locator in the package doc.
- `docs/superpowers/plans/2026-08-30-self-hosted-recomposition.md`: a one-line note under D3 that plan 8 restored it.

Commit: `docs: capabilities, the events table, and the restored snapshot fallback`.

---

## Reviewer procedure (per task)

1. Fresh worktree from the integration branch; apply the worker's tree; read the whole diff.
2. Run every gate in the task serially; for Tasks 2–4 also the pgstore-backed API and e2e runs.
3. Task 3: `grep -n "recordEvent\|s\.record(\|recordLifecycleEvent" controlapp/*.go` — every hit is lexically inside a `Run` closure. Task 5: `git diff protocol/` shows only additions.
4. Commit with the trailers; cherry-pick onto `feat/control-outbox`; retire the task worktree.
5. Before the integration PR: the live-fleet e2e on `rainier-1` from the branch, including the new capability scene, against a scratch database restored from the dogfood `pg_dump`.

## Acceptance

- `control.UnitOfWork`, `control.CheckpointLocator`, and `TransitionOpts.Image` exist; `check-public-control.sh` passes with an empty allowlist; `controlapp` imports nothing new.
- On pgstore, a failed unit leaves neither the row nor the event; a created session has exactly one `events` row; a mutation's event is never visible without its mutation.
- A session from a cached environment boots the snapshot on its holder when the holder has room, and the environment's image with setup elsewhere when it does not; O8's D3 rows in `api_test.go`/`sched_test.go` carry their pre-O8 assertions again.
- A runner announcing `gpu` is the only runner an environment requiring `gpu` lands on; an old runnerd (no `capabilities`, no `accept` handling) registers, is scheduled on, and logs the unknown `accept` once without harm.
- `runner.ProtocolVersion == 1`; `git diff origin/main -- protocol/` is additive.
- The live-fleet e2e is green on `rainier-1` from the branch, including the capability scene.

## Not in this plan

- Reading events back over `/v0/`, or an OSS dispatcher that drains them: nothing in the self-hosted product consumes the table yet; the hosted cell's dispatcher (regional-durability plan) is the consumer this shape serves.
- A registry for environment snapshots, so a snapshot could be pulled by a non-holder: `prepull` stays advisory; the fallback is a rebuild.
- Hosted checkpoint storage, encryption, transfer, and relocation: the hosted lifecycle and Dedicated adapter plans.
- Runner protocol version 2 or a rolling-version window: deferred to the compatibility ADR the ADR names.
- Controller-lease expiry, handoff, reclaim: the tenancy plan.

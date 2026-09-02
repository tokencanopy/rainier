# Self-Hosted Controld Recomposition Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Recompose self-hosted `internal/controld` as thin HTTP/WebSocket, store, authorization, launch-material, transport, and attach adapters over the four `controlapp` services, deleting controld's own copies of session lifecycle, placement, reconciliation, runner-event, and workspace-RPC behavior in the same commits that wire their replacements — without changing a byte of the `/v0/` wire that is not listed as a deliberate deviation below.

**Architecture:** `controld.Server` keeps connection ownership, request decoding, JSON rendering, GitHub login, the vault, the upward session-RPC (git-credential mint), and the `setup_done` snapshot arm. Everything else becomes an adapter behind a `control` port: the single-tenant `Store` is wrapped as the three workspace-keyed repository ports under one installation workspace and one installation pool; owner-or-admin becomes `control.Authorizer` and `controlapp.AttachmentPolicy`; repository/git-identity/secret resolution becomes `controlapp.LaunchMaterialResolver`; `runnerConn` becomes `control.RunnerTransport`; the dial-back pairing becomes `control.AttachmentBroker`. Each HTTP handler shrinks to decode → build `control.Scope` → call the service → map the closed sentinel set to today's status/slug → render.

**Tech Stack:** Go 1.25, `control` (frozen), `controlapp`, `protocol/{runner,terminal,workspace}`, `coder/websocket`, `internal/relay`, existing `memstore`/`pgstore`.

**Spec:** `rainier-cloud/docs/architecture/adr-0001-oss-cloud-composition.md` ("OSS application-service boundary", "Workspace scope and authorization composition", migration step 4), `rainier-cloud/docs/superpowers/plans/2026-08-30-hosted-implementation-program.md` gate **O8** and Wave 4's closing line ("one sequential worker recomposes self-hosted `controld`"), `docs/superpowers/plans/2026-08-31-control-application-interfaces.md`, and the three Wave 4 extraction plans. This is OSS plan #6 in the program inventory.

## Global Constraints

- Every task's changes start in a worktree created from freshly fetched `origin/main` (Tasks 1 and 2, which may run in parallel) or from the integration branch `feat/self-hosted-recomposition` (every later task). Nothing is pushed or merged by a worker.
- `control/*.go` is frozen: no edit of any kind. `controlapp` may change only where a task's file list names a `controlapp/` file, and only additively or as the exact behavioral fix that task specifies. No exported `controlapp` identifier is renamed or removed.
- One session lifecycle implementation. After Task 6 no code under `internal/controld` transitions a session's state, selects a runner, applies a runner lifecycle event, reconciles an announce, or performs a downward session RPC except by calling a `controlapp` service. The only direct store writes that remain in controld are the ones this plan names: runner heartbeat capacity, runner disconnect, environment snapshot caching, credential status, secrets, users, and tokens.
- The public `check-public-control.sh` guard must keep passing; its migration allowlist (the `Session`/`Environment`/`Runner`/state twins in `internal/controld`) is **not** emptied by this plan. O9 rewrites the store schema for mandatory workspace scope and retires the twins then; retiring them here would mean rewriting `pgstore` twice.
- Workspace and pool are installation constants in O8 (`installWorkspace`, `installPool`). Every repository adapter method refuses any other workspace with `control.ErrNotFound`, so isolation is enforced even though there is one tenant. Making scope mandatory in the schema is O9.
- Runner generations are process-local and monotonic per runner name in O8. O9 persists them.
- The nine deviations in the table below are the only permitted user-visible behavior changes, and the tests listed beside each are the only existing tests a task may modify. Every other existing test in `internal/controld`, `internal/e2e`, `cmd/rainier`, and `internal/cli` must pass unmodified. White-box tests of deleted internals are handled per the "Existing white-box tests" rule below, not by this table.
- No prompt, terminal output, repository content, path, archive byte, secret value, credential, or raw runner/sandbox error text enters a `control.Event`, a returned error, a log line added by this plan, a commit message, or test output. Tests use only synthetic `.test`, `.invalid`, `example.com`, `agents.localhost`, and fictional opaque IDs.
- Go gates run serially. Use `GOCACHE=/private/tmp/rainier-recomposition-gocache`.
- Commit messages follow the repository style (`feat:`/`refactor:`/`fix:` prefix, imperative subject, a body that says why) and end with the attribution trailers the reviewer supplies at commit time. Workers do not commit; they leave the tree ready and report.

## Deliberate deviations (the complete list)

| # | Today | After this plan | Why | Tests that may change |
|---|---|---|---|---|
| D1 | Runner/sandbox free text reaches HTTP error bodies (`writeSandboxErr`, `runner reported failure: <detail>` in logs) | Handlers map the seven sentinels to fixed status/slug/message; a session's own `Error` column (written by `ApplyRunnerEvent`) still carries the runner's failure tail | A provider-neutral service does not relay runner text (control/errors.go) | `api_test.go`, `files_test.go`, `srpc_test.go` assertions on error *message* text only |
| D2 | `POST /v0/sessions` accepts `image` together with `environment` (override wins) | `400 invalid_request` "an environment session cannot override the image" | `control.CreateSession.Validate` refuses environment + scratch spec | `api_test.go` cases that send both |
| D3 | A session from an environment with a current snapshot boots from the snapshot only if the snapshot runner is connected and has room, else from the plain image anywhere | Such a session is placed only on the snapshot runner (capability `snapshot:<runner>`); if that runner is full it stays queued | `control.Environment` has no snapshot-runner field; the checkpoint plan (O9) restores portable placement | `api_test.go`/`sched_test.go` cases asserting the busy-runner fallback |
| D4 | Cold resume on a full runner answers `409 no_capacity` | `409 conflict` "session cannot be resumed right now" | `controlapp.ResumeSession` reports `ErrConflict`; the slug cannot be recovered without a second capacity computation in the handler | `api_test.go` (3 hits), `cmd/rainier/main_test.go` (2 hits) |
| D5 | `GET /v0/sessions?state=&name=&runner=` filter in the store query | The three filters apply to the page the service returns; `next_cursor` is the service's | `control.SessionQuery` carries only `IncludeTerminal`, `Limit`, `Cursor` | `api_test.go`/`internal/cli/client_test.go` cases asserting filtered pagination across more than one page (none expected) |
| D6 | GitHub egress hosts are written onto the session row at create when repos exist, so `egress_allow` in the session view includes them | Hosts are added at dispatch from `LaunchMaterial.EgressAllow`; the row and view carry only what the caller or environment declared | The service stores the caller's spec verbatim; egress needed by launch material is the resolver's knowledge | `api_test.go` (3 hits on `objects.githubusercontent.com`) |
| D7 | The placement pass `continue`s past a store error on the queued→creating transition | The pass ends (`drainPool` returns); the safety tick retries the whole queue | A store that cannot answer one transition will not answer the next; re-run the burst e2e as the gate rather than assume parity | none expected |
| D8 | (controlapp, not the wire) `controlapp.DeleteSession` on a session whose runner is not connected returns `ErrUnavailable` | It destroys the row without dispatching, exactly as today's handler does; the orphan is destroyed by reconcile when the runner returns | The extraction lost a behavior; reconcile already covers the orphan | `controlapp/sessions_test.go` `TestLifecycleRunnerUnavailable` |
| D9 | `environment.placement` is a column; `snapshot_runner` is a column | Both still render; `placement` round-trips through `Requirements.Capabilities` as `placement:<runner>`; `snapshot_runner` is read from the store for the view only | `control.Environment` names no runner; a capability is the portable spelling of a pin | none — the JSON is unchanged |

Everything not in this table is a bug in the task that introduced it.

## Existing white-box tests

`sched_test.go` (10 tests) calls `createSpec`, `dispatchCreate`, `pickRunner`, `pickForSession`, and `drainQueue` directly; `srpc_test.go` (15 tests) calls `sessionRPC` directly. Those functions are deleted by Tasks 5 and 6. The task that deletes a function must, for **each** test that called it, do exactly one of:

1. Name the `controlapp` test that already covers the same behavior (e.g. `TestDrainPoolPlacesFIFO` for a `drainQueue` FIFO case) and delete the controld test, or
2. Rewrite it against the adapter that replaces the deleted function (e.g. a `createSpec` test about git attribution becomes a `launchMaterial` test in `adapt_launch_test.go`).

The task's report lists every deleted test by name with its disposition. A test dropped without a named replacement fails review.

## File structure

```text
internal/controld/
  adapt_scope.go            installWorkspace, installPool, scopes, User-in-context
  adapt_store.go            storeSessions, storeEnvironments, storeFleet over Store
  adapt_store_test.go       type conversion, sentinel mapping, workspace refusal, capability encoding
  adapt_host.go             installationPools, logRecorder, systemClock, idGenerator
  adapt_host_test.go
  adapt_policy.go           ownerOrAdmin: control.Authorizer + controlapp.AttachmentPolicy
  adapt_policy_test.go      the exact per-action matrix
  adapt_launch.go           launchMaterial: controlapp.LaunchMaterialResolver
  adapt_launch_test.go      repos, attribution, secrets, egress hosts; nothing sensitive in output
  adapt_http.go             controlStatus: sentinel → (status, slug, message); unavailableStatus
  adapt_http_test.go
  adapt_transport.go        runnerTransport: control.RunnerTransport incl. session_rpc correlation
  adapt_transport_test.go
  adapt_attach.go           attachBroker: control.AttachmentBroker; wsTerminalStream
  adapt_attach_test.go
  controld.go               Server gains the four services; New composes them (reviewer-owned)
  api.go                    handlers become decode → scope → service → render
  runners.go                connection ownership only; announce → Register+Reconcile; events → ApplyRunnerEvent
  sched.go                  deleted
  srpc.go                   upward RPC only (routeSessionReq, authorizeSessionRequest, mint)
  attach.go                 HTTP upgrade + readiness wait; pairing moved to adapt_attach.go
controlapp/
  sessions.go               D8 fix (Task 4)
  scheduler.go              LaunchMaterial.EgressAllow union (Task 2)
  attachments.go            AttachmentOptions.MaxTransferBytes (Task 6)
  workspace_rpc.go          uses the injected bound (Task 6)
```

Execution order: **Task 1 ‖ Task 2** (disjoint new files; separate worktrees off `origin/main`, cherry-picked onto the integration branch by the reviewer) → **Task 3** (reviewer) → **Task 4** → **Task 5** → **Task 6** → **Task 7** (reviewer). Tasks 3 and 7 touch the composition root and are reviewer-owned per the program; a worker never edits `controld.go` except where Task 5 explicitly grants `Run` and `wakeScheduler`.

---

### Task 1: Store-backed repository, pool, clock, ID, and event adapters

**Files:**
- Create: `internal/controld/adapt_scope.go`
- Create: `internal/controld/adapt_store.go`
- Create: `internal/controld/adapt_store_test.go`
- Create: `internal/controld/adapt_host.go`
- Create: `internal/controld/adapt_host_test.go`

**Interfaces:**
- Consumes: `Store` (store.go), `control.SessionRepository`, `control.EnvironmentRepository`, `control.FleetRepository`, `control.PoolResolver`, `control.EventRecorder`, `control.Clock`, `control.IDGenerator`.
- Produces (all unexported, used by Tasks 3–6):

```go
const (
	installWorkspace control.WorkspaceID = "ws_self_hosted"
	installPool      control.PoolID      = "pool_self_hosted"

	placementCapabilityPrefix = "placement:"
	snapshotCapabilityPrefix  = "snapshot:"
)

func installPlacement() control.PlacementScope // {ProductRegion:"self-hosted", HomeCell:"default", Mode: control.ExecutionSelfHosted}
func userScope(u User) control.Scope           // Actor{ID: u.ID, Kind: control.ActorUser}
func serviceScope(runner string) control.Scope // Actor{ID: "runner:"+runner, Kind: control.ActorService}
func withUser(ctx context.Context, u User) context.Context
func userFromContext(ctx context.Context) (User, bool)

type storeSessions struct{ st Store }      // control.SessionRepository
type storeEnvironments struct{ st Store }  // control.EnvironmentRepository
type storeFleet struct {                   // control.FleetRepository
	st   Store
	gens *runnerGenerations
}
type runnerGenerations struct { mu sync.Mutex; cur map[string]uint64 }
func (g *runnerGenerations) next(name string) uint64   // 1, 2, 3 … per name
func (g *runnerGenerations) current(name string) uint64 // 0 when never connected

func sessionToControl(s Session) control.Session
func sessionFromControl(c control.Session) Session
func environmentToControl(e Environment) control.Environment
func environmentFromControl(c control.Environment) Environment
func runnerToControl(r Runner, gen uint64) control.Runner
func storeErr(err error) error // ErrNotFound→control.ErrNotFound, ErrConflict→control.ErrConflict, nil→nil, else control.ErrUnavailable

type installationPools struct{ st Store } // control.PoolResolver: one pool, capacity summed over connected runners
type logRecorder struct{}                 // control.EventRecorder: logs action/kind/id only; never fails
type systemClock struct{}                 // control.Clock
type idGenerator struct{}                 // control.IDGenerator: NewSessionID, NewEnvironmentID, "evt_"+randHex(16)
```

Conversion rules (these are the contract for every later task):

- `sessionToControl`: `WorkspaceID = installWorkspace`, `CreatorID = OwnerID`, `PoolID = installPool` always (a queued session is queued *in* the installation pool); `RunnerID = Runner`; `PlacementGeneration = 1`; `Spec = {Image: s.effectiveImage(), Cmd, EgressAllow, Repos}` with slices cloned and nil preserved.
- `sessionFromControl`: `OwnerID = CreatorID`; `Runner = RunnerID`; when `EnvironmentID != ""` write `Spec.Image` to `ResolvedImage` and leave `Image = ""`, otherwise write it to `Image`; `Repos = Spec.Repos` (nil stays nil).
- `environmentToControl`: `WorkspaceID = installWorkspace`; `Snapshot = control.Checkpoint{Ref: SnapshotRef, Format: "rainier-runner-v0", Capabilities: []string{"workspace"}}` when `SnapshotRef != ""` else zero; `SnapshotHash` copied; `Requirements.Capabilities` = (`placement:<Placement>` if `Placement != ""`) + (`snapshot:<SnapshotRunner>` if `SnapshotRef != "" && SnapshotRunner != "" && SnapshotHash == SetupHash`), in that order, nil when both absent.
- `environmentFromControl`: `Placement` = the name after the first `placement:` capability, `""` if none; every `placement:` and `snapshot:` capability is dropped from what is written back; `SnapshotRef`, `SnapshotRunner`, `SnapshotHash` are **never** written by this function (the write path for them is `Store.SetEnvironmentSnapshot`, called from the `setup_done` arm in Task 5) — `UpdateEnvironment` therefore re-reads the current row and copies those three columns forward before calling `Store.UpdateEnvironment`.
- `runnerToControl`: `ID = Name`, `PoolID = installPool`, `Generation = gen`, `Capabilities = []string{"placement:"+Name, "snapshot:"+Name}`.
- Every method of the three repository adapters first checks `ws == installWorkspace` (or `pool == installPool`) and returns `control.ErrNotFound` otherwise, before touching the store.
- `storeSessions.CreateSession`: on `ErrIdemReplay`, call `Store.SessionByIdem(ownerID, key)` and return that row with `nil` error (the contract says a replayed key returns the existing row). `SessionByIDem` maps `ErrNotFound`.
- `storeSessions.ListSessions`: passes `IncludeTerminal`, `Limit`, `Cursor`; an invalid cursor from the store (any error when `Cursor != ""`) maps to `control.ErrInvalid`.
- `storeFleet.UpsertRunner`: writes `Name`, capacity, `Connected`, `LastSeenAt`; **ignores** `Generation` and `Capabilities` (neither has a column in O8); returns `control.ErrStale` never (the store has no generation to lose a race on). `ListRunners` attaches `gens.current(name)` as `Generation` and the two synthesized capabilities.
- `storeFleet.SessionsOnRunner` and `OldestQueued` convert every row with `sessionToControl`.
- `installationPools.EligiblePools`: returns exactly one `control.Pool{ID: installPool, CapacityUsed, CapacityTotal}` summed over runners with `Connected == true`, `Capabilities` = the union of every connected runner's synthesized capabilities. Returns the pool even when capacity is zero (the session service refuses a pool with no free capacity itself; keeping the pool present preserves today's "queued, waiting for a runner" rather than "no eligible pool").

- [ ] **Step 1: Write the conversion and refusal tests**

In `adapt_store_test.go`, using `newMemStore()` (memstore.go) as the backing store:

```go
func TestSessionConversionRoundTrip(t *testing.T) {
	code := 3
	in := Session{
		ID: "sess_example", OwnerID: "usr_example", Name: "investigate",
		Image: "", ResolvedImage: "registry.example.invalid/snap@sha256:0000",
		Cmd: []string{"claude"}, EgressAllow: []string{"example.com"},
		State: StateRunning, Runner: "runner-a", IdempotencyKey: "idem_example",
		EnvironmentID: "env_example", SetupHash: "abc", Repos: []RepoRef{{Repo: "acme/app"}},
		ChildExitCode: &code,
	}
	c := sessionToControl(in)
	if c.WorkspaceID != installWorkspace || c.PoolID != installPool || c.CreatorID != "usr_example" ||
		c.RunnerID != "runner-a" || c.PlacementGeneration != 1 ||
		c.Spec.Image != "registry.example.invalid/snap@sha256:0000" {
		t.Fatalf("toControl = %+v", c)
	}
	back := sessionFromControl(c)
	back.CreatedAt, back.UpdatedAt, back.LastEventAt = in.CreatedAt, in.UpdatedAt, in.LastEventAt
	if !reflect.DeepEqual(back, in) {
		t.Fatalf("round trip drifted:\n got %+v\nwant %+v", back, in)
	}
	c.Spec.Cmd[0] = "mutated"
	if in.Cmd[0] == "mutated" {
		t.Fatal("toControl aliased the store's slice")
	}
}

func TestRepositoriesRefuseOtherWorkspaces(t *testing.T) {
	st := newMemStore()
	st.CreateSession(context.Background(), Session{ID: "sess_example", OwnerID: "usr_example", State: StateQueued})
	sessions := storeSessions{st: st}
	if _, err := sessions.GetSession(context.Background(), "ws_other", "sess_example"); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("other workspace: got %v, want ErrNotFound", err)
	}
	if err := sessions.Transition(context.Background(), "ws_other", "sess_example", NonTerminalControl, control.StateCanceled, control.TransitionOpts{}); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("other workspace transition: got %v", err)
	}
	if row, _ := st.GetSession(context.Background(), "sess_example"); row.State != StateQueued {
		t.Fatal("a refused transition mutated the store")
	}
}

func TestEnvironmentPlacementRoundTripsAsCapability(t *testing.T) {
	e := Environment{ID: "env_example", Name: "std", Image: "img", Placement: "runner-a",
		SnapshotRef: "snap:1", SnapshotRunner: "runner-b", SnapshotHash: "h", SetupHash: "h"}
	c := environmentToControl(e)
	if !reflect.DeepEqual(c.Requirements.Capabilities, []string{"placement:runner-a", "snapshot:runner-b"}) {
		t.Fatalf("capabilities = %v", c.Requirements.Capabilities)
	}
	if c.Snapshot.Ref != "snap:1" || c.SnapshotHash != "h" {
		t.Fatalf("snapshot = %+v / %q", c.Snapshot, c.SnapshotHash)
	}
	back := environmentFromControl(c)
	if back.Placement != "runner-a" {
		t.Fatalf("placement = %q", back.Placement)
	}
	if back.SnapshotRef != "" || back.SnapshotRunner != "" || back.SnapshotHash != "" {
		t.Fatalf("fromControl must never write snapshot columns, got %+v", back)
	}
	stale := e
	stale.SetupHash = "changed"
	if caps := environmentToControl(stale).Requirements.Capabilities; len(caps) != 1 || caps[0] != "placement:runner-a" {
		t.Fatalf("a stale snapshot must not pin placement, got %v", caps)
	}
}

func TestCreateSessionIdempotentReplayReturnsExisting(t *testing.T) { /* seed, create twice with the same key, expect the first row and nil error */ }
func TestStoreErrMapsSentinels(t *testing.T) { /* ErrNotFound, ErrConflict, nil, errors.New("pq: …") */ }
func TestRunnerGenerationsAreMonotonicPerName(t *testing.T) { /* next(a)=1,2; next(b)=1; current(c)=0 */ }
func TestEligiblePoolsSumsConnectedRunners(t *testing.T) { /* two connected + one disconnected; pool present at zero capacity */ }
func TestIDGeneratorMintsDistinctPrefixedIDs(t *testing.T) { /* sess_, env_, evt_; 100 distinct */ }
```

Define `NonTerminalControl` in the test file as `[]control.SessionState{control.StateQueued, control.StateCreating, control.StateRunning, control.StateSuspendedWarm, control.StateSuspendedCold}` (or use `control.NonTerminal`). Fill in every `/* … */` body — the plan shows the shape, the worker writes the assertions.

- [ ] **Step 2: Run the tests and observe the missing symbols**

Run: `go test ./internal/controld -run 'TestSessionConversion|TestRepositoriesRefuse|TestEnvironmentPlacement|TestCreateSessionIdempotent|TestStoreErr|TestRunnerGenerations|TestEligiblePools|TestIDGenerator' -count=1`
Expected: FAIL to compile — `sessionToControl`, `storeSessions`, etc. undefined.

- [ ] **Step 3: Implement `adapt_scope.go` and `adapt_store.go`**

Implement the types and conversion rules above exactly. Keep each adapter method small: workspace check, store call, `storeErr`, conversion. `Transition` maps `control.TransitionOpts{RunnerID *control.RunnerID, Error *string}` to `TransitionOpts{Runner *string, Error *string}`. `SetEnvironmentSnapshot` maps straight through (`runnerID` → `runner` string); it is the one method that does **not** use `storeErr` unchanged — the store reports a hash mismatch or a vanished environment as `ErrConflict` (store.go:339–343, memstore.go:560), and this adapter maps that specific `ErrConflict` to `control.ErrStale`, since the contract names a superseded setup hash as stale.

- [ ] **Step 4: Implement `adapt_host.go`**

`installationPools` as specified; `logRecorder.Record` logs `"controld: event %s %s %s"` with `e.Action`, `e.Resource.Kind`, `e.Resource.ID` — nothing else from the event — and returns nil; `systemClock.Now` returns `time.Now()`; `idGenerator` uses `NewSessionID`, `NewEnvironmentID`, and `"evt_" + randHex(16)`.

- [ ] **Step 5: Run the tests and the package**

Run: `go test ./internal/controld -run 'TestSessionConversion|TestRepositoriesRefuse|TestEnvironmentPlacement|TestCreateSessionIdempotent|TestStoreErr|TestRunnerGenerations|TestEligiblePools|TestIDGenerator' -count=1`
Expected: PASS.
Run: `go vet ./internal/controld && go test ./internal/controld -race -count=1`
Expected: PASS (nothing existing is touched).

- [ ] **Step 6: Compile-time assertions**

Add to `adapt_store.go`:

```go
var (
	_ control.SessionRepository     = storeSessions{}
	_ control.EnvironmentRepository = storeEnvironments{}
	_ control.FleetRepository       = (*storeFleet)(nil)
	_ control.PoolResolver          = installationPools{}
	_ control.EventRecorder         = logRecorder{}
	_ control.Clock                 = systemClock{}
	_ control.IDGenerator           = idGenerator{}
)
```

Report: `git diff --stat`, each command with its verbatim result line.

---

### Task 2: Authorization, attachment policy, and launch material

**Files:**
- Create: `internal/controld/adapt_policy.go`
- Create: `internal/controld/adapt_policy_test.go`
- Create: `internal/controld/adapt_launch.go`
- Create: `internal/controld/adapt_launch_test.go`
- Modify: `controlapp/scheduler.go` (`LaunchMaterial` gains `EgressAllow`; `createSpec` unions it)
- Modify: `controlapp/scheduler_test.go` (one new test)

**Interfaces:**
- Consumes: `userFromContext`, `installWorkspace`, `Store.GetUser`, `Store.GetSecret`, `Open` (seal.go), `decodeGitHubConnector`, `expandRepos`, `sessionBranch`, `uniqueDir`, `noreplyEmail`, `gitEgressHosts` (all currently in sched.go / api.go — Task 2 **moves** `expandRepos`, `sessionBranch`, `uniqueDir`, `noreplyEmail`, `gitEgressHosts`, `withGitHubHosts`, `sessionRepoRefs`, `defaultBaseBranch` into `adapt_launch.go` unchanged, deleting them from `sched.go`; `sched.go` keeps compiling because Task 2 leaves `createSpec`/`applyRepos` calling the moved functions by the same names).
- Produces:

```go
type ownerOrAdmin struct{}
func (ownerOrAdmin) Authorize(ctx context.Context, scope control.Scope, a control.Action, r control.Resource) error
func (ownerOrAdmin) AuthorizeAttachment(ctx context.Context, scope control.Scope, r control.Resource, m control.AttachmentMode) error

type launchMaterial struct {
	st  Store
	key [32]byte
}
func (l launchMaterial) ResolveLaunchMaterial(ctx context.Context, row control.Session, env *control.Environment) (controlapp.LaunchMaterial, error)
```

And in `controlapp/scheduler.go`:

```go
type LaunchMaterial struct {
	Repos          []runner.RepoSpec
	GitAuthorName  string
	GitAuthorEmail string
	Environment    map[string]string
	// EgressAllow lists hosts the resolved material needs reachable — for
	// example the source-control hosts Repos clone from. createSpec unions
	// them into the session's egress list, in order, without duplicates.
	EgressAllow []string
}
```

The authorization matrix `ownerOrAdmin.Authorize` implements — derived from today's handlers and route wrappers, and the single source of truth from now on:

| Kind | Action | Rule |
|---|---|---|
| session | create, get, list, diff | any authenticated user |
| session | delete, suspend, resume, snapshot, attach, push, pull | `u.Role == "admin" \|\| u.ID == string(r.CreatorID)` |
| environment | get, list | any authenticated user |
| environment | create, update, delete | `u.Role == "admin"` |
| runner | list | any authenticated user |

`u` comes from `userFromContext(ctx)`; a missing user is `control.ErrDenied`. A scope whose `Actor.ID != u.ID` is `control.ErrDenied` (the scope and the context must agree). A resource whose `WorkspaceID != installWorkspace` is `control.ErrDenied`. `AuthorizeAttachment` applies the owner-or-admin rule for both viewer and controller (today's attach has no viewer distinction).

`launchMaterial.ResolveLaunchMaterial` reproduces `createSpec`'s material half exactly:

1. Repos: `row.Spec.Repos` when non-nil, else the environment's `github` connectors decoded with `decodeGitHubConnector`, else none. Expanded with `expandRepos` using `sessionBranch` built from `row.Name`/`row.ID`.
2. When repos are non-empty: `GitAuthorName = user.Login`, `GitAuthorEmail = noreplyEmail(user)` for `user := st.GetUser(ctx, string(row.CreatorID))`; `EgressAllow = gitEgressHosts`.
3. `Environment`: the environment's `SecretRefs` opened with `Open(l.key, …)`; a missing secret is an error whose text is `"environment references secret <name>, which no longer exists"` (the name is not a value); an unopenable secret is an error `"could not resolve the environment's secrets"`.
4. No value, token, or repository content appears in any error, log, or test output.

- [ ] **Step 1: Write the policy matrix test**

```go
func TestOwnerOrAdminMatrix(t *testing.T) {
	owner := User{ID: "usr_owner", Role: "member"}
	other := User{ID: "usr_other", Role: "member"}
	admin := User{ID: "usr_admin", Role: "admin"}
	sess := control.Resource{Kind: control.ResourceSession, WorkspaceID: installWorkspace, ID: "sess_example", CreatorID: "usr_owner"}
	env := control.Resource{Kind: control.ResourceEnvironment, WorkspaceID: installWorkspace, ID: "env_example"}
	cases := []struct {
		name string; u User; action control.Action; res control.Resource; want error
	}{
		{"owner deletes own", owner, control.ActionDelete, sess, nil},
		{"other cannot delete", other, control.ActionDelete, sess, control.ErrDenied},
		{"admin deletes any", admin, control.ActionDelete, sess, nil},
		{"anyone gets", other, control.ActionGet, sess, nil},
		{"anyone diffs", other, control.ActionDiff, sess, nil},
		{"other cannot push", other, control.ActionPush, sess, control.ErrDenied},
		{"member cannot create env", owner, control.ActionCreate, env, control.ErrDenied},
		{"admin creates env", admin, control.ActionCreate, env, nil},
		{"anyone lists env", other, control.ActionList, env, nil},
		{"other workspace denied", admin, control.ActionGet, control.Resource{Kind: control.ResourceSession, WorkspaceID: "ws_other", ID: "sess_example"}, control.ErrDenied},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := withUser(context.Background(), tc.u)
			err := ownerOrAdmin{}.Authorize(ctx, userScope(tc.u), tc.action, tc.res)
			if !errors.Is(err, tc.want) && !(err == nil && tc.want == nil) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
	if err := ownerOrAdmin{}.Authorize(context.Background(), userScope(owner), control.ActionGet, sess); !errors.Is(err, control.ErrDenied) {
		t.Fatalf("no user in context: got %v", err)
	}
	if err := ownerOrAdmin{}.Authorize(withUser(context.Background(), other), userScope(owner), control.ActionGet, sess); !errors.Is(err, control.ErrDenied) {
		t.Fatalf("scope/context disagreement: got %v", err)
	}
}
```

- [ ] **Step 2: Write the launch-material tests**

```go
func TestLaunchMaterialResolvesReposAttributionAndSecrets(t *testing.T) {
	st := newMemStore()
	u, _ := st.UpsertUser(ctx, 12345, "octo-example", "member")
	key := testSecretsKey(t) // any [32]byte
	ct, nonce, _ := Seal(key, []byte("s3cr3t-value"))
	st.PutSecret(ctx, "API_TOKEN", ct, nonce)
	env := environmentToControl(Environment{ID: "env_example", SecretRefs: []string{"API_TOKEN"},
		Connectors: []Connector{{Type: "github", Raw: json.RawMessage(`{"type":"github","repo":"acme/app","base_branch":"main"}`)}}})
	row := control.Session{ID: "sess_example", CreatorID: control.ActorID(u.ID), Name: "investigate", EnvironmentID: "env_example"}

	m, err := launchMaterial{st: st, key: key}.ResolveLaunchMaterial(ctx, row, &env)
	if err != nil { t.Fatal(err) }
	if len(m.Repos) != 1 || m.Repos[0].Owner != "acme" || m.Repos[0].Name != "app" || m.Repos[0].SessionBranch != "rainier/investigate" || m.Repos[0].Dir != "app" {
		t.Fatalf("repos = %+v", m.Repos)
	}
	if m.GitAuthorName != "octo-example" || m.GitAuthorEmail != "12345+octo-example@users.noreply.github.com" {
		t.Fatalf("attribution = %q <%s>", m.GitAuthorName, m.GitAuthorEmail)
	}
	if !reflect.DeepEqual(m.EgressAllow, gitEgressHosts) { t.Fatalf("egress = %v", m.EgressAllow) }
	if m.Environment["API_TOKEN"] != "s3cr3t-value" { t.Fatal("secret not opened") }
}

func TestLaunchMaterialSessionReposOverrideConnectors(t *testing.T) { /* row.Spec.Repos = []control.RepoRef{} → no repos, no attribution, no egress */ }
func TestLaunchMaterialMissingSecretNamesTheRefNotTheValue(t *testing.T) { /* error text contains "API_TOKEN" and not any value; fails closed */ }
func TestLaunchMaterialScratchSessionHasNoMaterial(t *testing.T) { /* env == nil, no repos → zero material, nil error */ }
```

Confirm `Seal` exists in seal.go with that shape; if the name differs, use the actual sealing helper and note it in the report.

- [ ] **Step 3: Write the controlapp union test**

In `controlapp/scheduler_test.go`, using the existing `newFleetFixtureWithResolver` and `fleetFakeResolver`:

```go
func TestDispatchCreateUnionsMaterialEgress(t *testing.T) {
	resolver := &fleetFakeResolver{material: LaunchMaterial{EgressAllow: []string{"github.com", "example.com"}}}
	fx := newFleetFixtureWithResolver(t, resolver)
	// seed one connected runner and one queued scratch session whose Spec.EgressAllow is []string{"example.com", "api.example.com"}
	// run one drainPool pass; find the dispatched "create"
	// assert spec.EgressAllow == []string{"example.com", "api.example.com", "github.com"}  (row's order first, then new hosts, no duplicates)
}
```

- [ ] **Step 4: Run all three groups and observe failures**

Run: `go test ./internal/controld -run 'TestOwnerOrAdmin|TestLaunchMaterial' -count=1` → FAIL to compile.
Run: `go test ./controlapp -run TestDispatchCreateUnionsMaterialEgress -count=1` → FAIL (`EgressAllow` undefined / assertion).

- [ ] **Step 5: Implement**

`adapt_policy.go` per the matrix. `adapt_launch.go` per the rules, moving the listed helpers out of `sched.go`/`api.go` verbatim (`withGitHubHosts` stays available because `resolveRepos` in api.go still calls it until Task 4 deletes that caller). In `controlapp/scheduler.go`, add the field and in `createSpec`:

```go
spec.EgressAllow = unionHosts(spec.EgressAllow, material.EgressAllow)
```

with an unexported `unionHosts(base, extra []string) []string` that returns `base` (cloned) plus each host of `extra` not already present, in order; nil in → nil out only when both are nil.

- [ ] **Step 6: Run and gate**

Run: `go test ./internal/controld -run 'TestOwnerOrAdmin|TestLaunchMaterial' -count=1` → PASS.
Run: `go test ./controlapp -race -count=1` → PASS (existing scheduler tests still pass; the new one passes).
Run: `go vet ./internal/controld ./controlapp && go test ./internal/controld -race -count=1` → PASS.
Run: `./scripts/check-public-control.sh` → PASS.
Add `var _ controlapp.LaunchMaterialResolver = launchMaterial{}` and `var _ controlapp.AttachmentPolicy = ownerOrAdmin{}` and `var _ control.Authorizer = ownerOrAdmin{}`.

Report as in Task 1.

---

### Task 3: Composition root (reviewer-owned)

**Files:**
- Modify: `internal/controld/controld.go` (`Server` fields, `New`)
- Create: `internal/controld/adapt_http.go`
- Create: `internal/controld/adapt_http_test.go`

**Interfaces:**
- Consumes: everything Tasks 1–2 produce; `controlapp.NewSessionService`, `NewEnvironmentService`, `NewFleetService`, `NewAttachmentService`.
- Produces on `*Server`:

```go
sessions     *controlapp.SessionService
environments *controlapp.EnvironmentService
fleet        *controlapp.FleetService
attachments  *controlapp.AttachmentService
gens         *runnerGenerations
transport    *runnerTransport // Task 5 creates the type; Task 3 adds the field as an interface placeholder: control.RunnerTransport
broker       control.AttachmentBroker // Task 6
```

and

```go
// controlStatus maps a control sentinel to today's status, slug, and a fixed
// message. It is the one place the wire learns about the closed error set.
func controlStatus(err error) (status int, code, msg string)
// unavailableStatus refines ErrUnavailable for a placed session: 502
// runner_unreachable when its runner has no control connection, else 500.
func (s *Server) unavailableStatus(row control.Session) (int, string, string)
func writeControlErr(w http.ResponseWriter, err error)
```

The mapping table (fixed; later tasks call it and never invent a second):

| sentinel | status | code | message |
|---|---|---|---|
| `ErrInvalid` | 400 | `invalid_request` | `"invalid request"` (handlers that already validated pass their own specific message instead) |
| `ErrDenied` | 403 | `forbidden` | `"not authorized"` |
| `ErrNotFound` | 404 | `not_found` | `"not found"` |
| `ErrConflict` | 409 | `conflict` | `"conflict"` (handlers supply the specific text where today's handler did) |
| `ErrStale` | 409 | `conflict` | `"stale"` |
| `ErrUnavailable` | 500 / 502 | `internal` / `runner_unreachable` | via `unavailableStatus` |
| `ErrUnsupported` | 501 | `unsupported` | `"unsupported"` |
| `context.Canceled`, `DeadlineExceeded` | — | — | write nothing (client went away) |
| anything else | 500 | `internal` | `"internal error"` |

Until Task 5 supplies the real transport, `New` wires a `notYetTransport` whose `Connected` returns false and whose `Dispatch` returns `control.ErrUnavailable`; until Task 6, a `notYetBroker` that returns `control.ErrUnavailable`. Nothing calls the services until Task 4, so the server's behavior is unchanged after Task 3. `New` does **not** start `fleet.Run`; Task 5 does.

Gate: `go build ./... && go test ./internal/controld -race -count=1` unchanged-green; `adapt_http_test.go` pins every row of the table.

---

### Task 4: Session and environment handlers over the services

**Files:**
- Modify: `internal/controld/api.go` — everything from `handleCreateSession` through `handleListRunners`, the `sessionRenderer`, `resolveEnvironment`, `resolveRepos`, `resolveImage`, `cacheUsable`, `reclaimWorkspace`; and `handleCreateEnvironment` through `handleDeleteEnvironment`
- Modify: `internal/controld/api_test.go` — only the cases the deviation table names (D1 message text, D2, D4, D6)
- Modify: `controlapp/sessions.go` (D8) and `controlapp/sessions_test.go` (`TestLifecycleRunnerUnavailable` → the new behavior, plus one new test)
- Modify: `cmd/rainier/main_test.go` — only the two `no_capacity` hits (D4)

**Interfaces:**
- Consumes: `s.sessions`, `s.environments`, `s.fleet.ListRunners`, `userScope`, `withUser`, `controlStatus`, `writeControlErr`, `unavailableStatus`, `environmentToControl` (for the two view-only fields), `installationPools`.
- Produces: none new; every session/environment/runner-list route now reaches the store only through a service, except the three explicitly adapter-owned reads named below.

Handler recipe (every handler in this task follows it):

```go
func (s *Server) handleSuspendSession(w http.ResponseWriter, r *http.Request, u User) {
	var req suspendRequest
	if !decodeJSONBody(w, r, &req) { return }
	warm := req.Warm == nil || *req.Warm
	ctx := withUser(r.Context(), u)
	row, err := s.sessions.SuspendSession(ctx, userScope(u), control.SuspendSession{ID: control.SessionID(r.PathValue("id")), Warm: warm})
	if err != nil {
		s.writeSessionErr(w, ctx, r.PathValue("id"), err, "session is not running")
		return
	}
	writeJSON(w, http.StatusOK, sessionEnvelope{Session: s.renderer(ctx).view(row)})
}
```

where `writeSessionErr(w, ctx, id, err, conflictMsg)` calls `controlStatus`, substitutes `conflictMsg` for `ErrConflict`, and for `ErrUnavailable` re-reads the session through `s.sessions.GetSession` to feed `unavailableStatus` (a failed re-read falls back to 500).

Create is the one handler with preflights. In order: decode; `repoOverrides`; **D2** — if `req.Environment != "" && req.Image != ""` → 400 `invalid_request` "an environment session cannot override the image"; resolve the environment reference through `s.environments.GetEnvironment` by ID, and on `ErrNotFound` by name via `Store.GetEnvironmentByName` then `GetEnvironment` (this by-name lookup is the first adapter-owned read; a miss stays 400 `invalid_request` `environment %q does not exist` as today); **secret preflight** — the vault is host policy, so before calling the service run `s.secretEnv(ctx, storeEnv)` exactly as today and answer 409 `conflict` with `missingSecretMessage` on a missing ref (second adapter-owned read); **credential preflight** — if the resolved repo list (`sessionRepoRefs` over the row-to-be and environment) is non-empty and `Store.GetCredential(u.ID, githubProvider)` is `ErrNotFound`, answer 409 `conflict` `ErrCredentialMissing.Error()` (third adapter-owned read); then `s.sessions.CreateSession(ctx, userScope(u), control.CreateSession{Name, EnvironmentID, Spec: {Image, Cmd, EgressAllow} for scratch only, Repos, IdempotencyKey: r.Header.Get("Idempotency-Key")})`; 202 with `Location` as today. `wakeScheduler` is no longer called here — the service wakes the pool.

The renderer keeps `Reachable`, `Environment` (name), and `QueueReason`. `reachable` uses `s.transport.Connected(installPool, control.RunnerID(row.RunnerID))`. `queueReason` needs "does the pinned runner have room": implement `runnerHasRoom(ctx, name)` in the renderer over `Store.ListRunners` + `Store.SessionsOnRunner(name, creating)` — this is view logic, it makes no decision, and it is the fourth and last adapter-owned read. The environment's pin comes from the `placement:` capability of the environment the service returned.

`handleListSessions`: parse `limit`, `cursor`, `all` as today into `control.SessionQuery`; call `s.sessions.ListSessions`; apply `state`/`name`/`runner` to the returned page (**D5**); render.

`handleListRunners`: `s.fleet.ListRunners(ctx, userScope(u), control.RunnerQuery{Limit: 100})`; render `runnerSummary` from `control.Runner` (`Name = ID`).

Environment handlers: decode as today (`validateEnvironmentBasics`, `validateConnectors`, `missingSecretRef` stay — they are request validation and vault policy); build `control.CreateEnvironment`/`UpdateEnvironment` with `Requirements.Capabilities = []string{"placement:" + req.Placement}` when a placement is given (**D9**), nil pointer semantics preserved for patch; call the service; render with `environmentJSON` — which now takes `(control.Environment, snapshotRunner string)`; `snapshotRunner` is read from `Store.GetEnvironment` for the view (view-only, D9). `GET`/`PATCH`/`DELETE` by name resolve the reference the same way create does.

**D8** in `controlapp/sessions.go` `DeleteSession`: replace the unconditional `dispatch` with

```go
if row.PoolID != "" && row.RunnerID != "" && s.transport.Connected(row.PoolID, row.RunnerID) {
	if _, err := s.dispatch(ctx, row, runner.ToRunner{Type: "destroy", Session: string(row.ID)}); err != nil {
		return control.ErrUnavailable
	}
	s.reclaimWorkspace(ctx, row)
}
```

and keep the guarded transition to destroyed after it. Update `TestLifecycleRunnerUnavailable` so the delete case expects success with the row destroyed and no dispatch; add `TestDeleteSessionOnDisconnectedRunnerDestroysWithoutDispatch`. Suspend/resume/snapshot keep refusing with `ErrUnavailable` (those need the runner).

Delete from api.go once nothing calls them: `resolveEnvironment`, `resolveRepos`, `resolveImage`, `cacheUsable`, `reclaimWorkspace`, `authorizeOwnerOrAdmin` (the adapter owns the rule now; keep it only if `sessionForRPC` in Task 6 still needs it — Task 6 deletes it otherwise).

- [ ] **Step 1: Run the black-box suite before touching anything and save the list of passing tests**

Run: `go test ./internal/controld -run 'Test' -count=1 -json | jq -r 'select(.Action=="pass" and .Test!=null) | .Test' | sort > /private/tmp/rainier-recomposition-baseline.txt`

- [ ] **Step 2: Rewire one handler at a time, running `go test ./internal/controld -run <that handler's tests>` after each**

Order: get → list → delete → suspend → resume → snapshot → runners → environments (create, get, list, update, delete) → create session last (it has the most preflights).

- [ ] **Step 3: D8 in controlapp**

Write `TestDeleteSessionOnDisconnectedRunnerDestroysWithoutDispatch` first; run it (FAIL: `ErrUnavailable`); implement; run (PASS); update `TestLifecycleRunnerUnavailable`.

- [ ] **Step 4: Reconcile the suite against the deviation table**

Run: `go test ./internal/controld -race -count=1`. For every failure, either it is a case the table names for D1/D2/D4/D6 (update it, and cite the deviation in the test's comment) or it is a bug in this task. Run: `go test ./cmd/rainier ./internal/cli -count=1` and apply the two D4 updates.

- [ ] **Step 5: Gate**

`go vet ./... && go test ./internal/controld ./controlapp ./cmd/rainier ./internal/cli -race -count=1 && ./scripts/check-public-control.sh`. Diff the passing-test list against the baseline: the only names missing must be renamed/updated ones the report lists.

Report: the diff of passing test names, every modified existing test with the deviation it cites, `git diff --stat`.

---

### Task 5: Runner transport and the fleet service

**Files:**
- Create: `internal/controld/adapt_transport.go`
- Create: `internal/controld/adapt_transport_test.go`
- Modify: `internal/controld/runners.go` — `handleRunnerConnect`, `readLoop`, `touchRunner`, `connectRunner`, `retireRunner` kept and reshaped; **delete** `applyEvent`, `placedExactlyOn`, `reconcile`, `reconcilePresent`, `reconcileMissing`, `reconcileUnplaced`, `destroyOrphan`, `transitionQuiet`, `announcedState`, `dispatch`, `drain`; **keep** `failStage`/`stageFailure` (detail composition), `cacheEnvironment`, `snapshotWanted`, `buildSnapshot`, `snapshotRef`, `rejectCredential`, `runnerConn` and its loops, `sendToRunner`, `broadcastToRunners`, `conn`, `isCurrentConn`, `nameLock`, `registerRunner`, `runnerTokenOK`, `closeRunner`, `clip`
- Delete: `internal/controld/sched.go` (everything remaining after Task 2's moves)
- Modify: `internal/controld/sched_test.go` — per the white-box rule
- Modify: `internal/controld/runners_test.go` — only if a test asserts a log line or internal call this task removes; report each
- Modify: `internal/controld/controld.go` — `Run` and `wakeScheduler` only (granted)

**Interfaces:**
- Consumes: `s.fleet` (`RegisterRunner`, `ReconcileRunner`, `ApplyRunnerEvent`, `Wake`, `Run`), `s.gens`, `serviceScope`, `runnerToControl`, `storeErr`.
- Produces:

```go
type runnerTransport struct{ srv *Server }
func (t runnerTransport) Connected(pool control.PoolID, id control.RunnerID) bool
func (t runnerTransport) Dispatch(ctx context.Context, pool control.PoolID, id control.RunnerID, m runner.ToRunner) (runner.FromRunner, error)
```

`Dispatch` is the old `dispatch` moved behind the port with one addition. For every `m.Type` except `"session_rpc"` it assigns `m.ReqID`, parks a reply channel in `rc.pending`, enqueues, and waits on `rc.done`/`OpTimeout`/`ctx` exactly as today, returning the `result` `FromRunner`. For `m.Type == "session_rpc"` it parks `m.RPC.ID` in `rc.srpc` (the `srpcTable`), enqueues, waits the same way, and returns the answer wrapped as `runner.FromRunner{Type: "session_req", Session: m.Session, RPC: &answer}` — which is the exact shape `controlapp.sessionRPC` validates. Every failure returns `control.ErrUnavailable` wrapped with no runner text (`fmt.Errorf("dispatch %s to runner %q: %w", m.Type, id, control.ErrUnavailable)` — the type and runner name are ours, not the runner's). `pool != installPool` → `control.ErrUnavailable`.

`handleRunnerConnect` after `readAnnounce`:

```go
gen := s.gens.next(name)
reg, err := s.fleet.RegisterRunner(connCtx, control.RunnerRegistration{
	WorkspaceID: installWorkspace, PoolID: installPool, RunnerID: control.RunnerID(name),
	Generation: gen, CapacityUsed: ann.Used, CapacityTotal: ann.Total,
	Capabilities: []string{placementCapabilityPrefix + name, snapshotCapabilityPrefix + name},
	Sessions: announcedSessions(ann.Sessions),
})
if err != nil || !reg.Accepted { closeRunner(c, websocket.StatusInternalError, "registration refused"); return }
// register the conn (registerRunner) under the name lock, start the writer, then:
res, err := s.fleet.ReconcileRunner(connCtx, control.RunnerSnapshot{ /* same bindings, Generation: gen, Sessions */ })
if err != nil { log; closeRunner(...); return }
for _, id := range res.Destroy { s.destroyOrphan(connCtx, name, string(id)) } // destroyOrphan is kept: destroy + remove_workspace to the runner, no store write
s.fleet.Wake(installPool)
s.readLoop(connCtx, rc)
```

`announcedSessions` maps `runner.SessionInfo{ID, State}` to `control.RunnerSession{SessionID, State: control.SessionState(State)}`.

`readLoop` keeps `touchRunner` (the heartbeat capacity write goes straight to `Store.UpsertRunner` as today — it is transport bookkeeping, not a reconcile; calling `ReconcileRunner` per message would declare every session lost). Then:

- `"result"` → `rc.deliver` as today.
- `"session_req"` → `s.routeSessionReq` as today (unchanged; Task 6 leaves it).
- `"event"` → `s.applyRunnerEvent(ctx, rc.name, m)`:

```go
func (s *Server) applyRunnerEvent(ctx context.Context, name string, m runner.FromRunner) {
	ev := control.RunnerEvent{WorkspaceID: installWorkspace, PoolID: installPool,
		RunnerID: control.RunnerID(name), Generation: s.gens.current(name), SessionID: control.SessionID(m.Session)}
	switch m.State {
	case "running":           ev.State = control.StateRunning
	case "dead":              ev.State = control.StateDead
	case "setup_failed":      ev.State = control.StateFailed; ev.Detail = stageFailure(setupStage, m.Detail)
	case "stage_failed":
		stage, rest, ok := strings.Cut(m.Detail, ": ")   // today's rule, verbatim: the stage rides at the front
		if !ok { stage, rest = unnamedStage, m.Detail }
		ev.State = control.StateFailed; ev.Detail = stageFailure(stage, rest)
	case "child_exited":
		code, err := strconv.Atoi(m.Detail); if err != nil { log unreadable; return }
		ev.State = control.StateRunning; ev.ChildExitCode = &code // State is ignored on a child-exit event; Running is the row's expected state
	case "setup_done", "credential_rejected":
		// The two adapter arms. Both need the row and both keep today's exact-placement guard
		// (placedExactlyOn, kept for this purpose): read it via the store — this is one of the
		// direct store reads the Global Constraints name — then call the kept arm unchanged:
		// cacheEnvironment(ctx, name, row) / rejectCredential(ctx, row.OwnerID, githubProvider).
		s.applyAdapterArm(ctx, name, m); return
	default:                  log unknown; return
	}
	if err := s.fleet.ApplyRunnerEvent(ctx, ev); err != nil {
		// ErrStale and ErrConflict are the races reconciliation and events have always had with
		// each other; they are logged at the same lines today's applyEvent logged its refusals.
		log.Printf("controld: runner %s: event %s for %s not applied: %v", name, m.State, clip(m.Session), err)
	}
}
```

`ev.Generation` is the generation this connection registered with; if the transport's current generation for the name has moved past it (a reconnect raced), the service answers `ErrStale` — which is correct. Note: `ApplyRunnerEvent` with `ChildExitCode` set ignores `State`; the child-exit arm is an observation with no transition, matching today.

`retireRunner`: unchanged except `s.wakeScheduler()` → `s.fleet.Wake(installPool)`. `Store.SetRunnerConnected(false)` stays a direct store write.

`controld.go`: `Run(ctx)` becomes `_ = s.fleet.Run(ctx)`; `wakeScheduler()` becomes `s.fleet.Wake(installPool)` (keep the name; Task 4's handlers no longer call it, but `cacheEnvironment` and `retireRunner` do); delete `schedWake`.

Delete `sched.go`. For each `sched_test.go` test, apply the white-box rule: `createSpec`/`applyRepos` cases → `adapt_launch_test.go` or already covered by `TestLaunchMaterial*`; `pickRunner`/`pickForSession` → `controlapp` `TestPickRunner*`/`TestPickForEnvironment*`; `dispatchCreate` → `controlapp` `TestDispatchCreate*`; `drainQueue` → `controlapp` `TestDrainPool*`. Placement-pin cases become capability cases (`placement:<name>`) — and the busy-snapshot-runner fallback case is **D3** (delete with the deviation cited).

- [ ] **Step 1: Write the transport tests**

`adapt_transport_test.go`: using the existing websocket test harness from `runners_test.go` (a fake runnerd that answers `result` and `session_req`/`resp`): `Dispatch` of a `destroy` returns the result; `Dispatch` of a `session_rpc` returns a `session_req` `FromRunner` whose `RPC.ID` equals the request's and `Method == "resp"`; a disconnected runner → `ErrUnavailable` with no runner text in the error string; a timeout → `ErrUnavailable`; `Connected` reflects the map.

- [ ] **Step 2: Write the generation-fencing test**

In `runners_test.go` (new test, black-box over the websocket): connect runner `runner-a`, drop it, reconnect it; an `event` sent on the **first** (stale) socket after the second announce must not transition the session (assert via the API), while the same event on the second socket does.

- [ ] **Step 3: Implement, delete, migrate tests, run**

Run after each of: transport implemented; announce rewired; events rewired; `sched.go` deleted; `Run` rewired:
`go test ./internal/controld -race -count=1`
Then: `go test ./internal/controld -run 'Reconcile|Runner|Event|Sched|Queue|Placement' -race -count=20` and `go test ./internal/e2e -race -count=1`.

- [ ] **Step 4: Gate**

`go vet ./... && go build ./... && ./scripts/check-public-control.sh && go test ./internal/controld ./internal/e2e -race -count=1`.

Report: the disposition of every deleted `sched_test.go` test by name; any `runners_test.go` change with its reason; `git diff --stat`.

---

### Task 6: Attach broker and workspace transfer

**Files:**
- Create: `internal/controld/adapt_attach.go`
- Create: `internal/controld/adapt_attach_test.go`
- Modify: `internal/controld/attach.go` — `handleClientAttach` becomes: read session + authorize via `s.attachments` path below; `waitRunning`, `failedButAttachable`, `readFirstResize`, `attachBackURL`, `handleAttachBack`, `attachTable`, `closeAttach` kept; `splice` reshaped to pump a `control.TerminalStream` against a `relay.Conn`
- Modify: `internal/controld/srpc.go` — **delete** `sessionRPC`, `drainRPC`, `decodeRPCAnswer`, `rpcErrorText`, `rpcPayload`, `sessionDiff`, `boundDiff`, `clipTo`, `sessionPushChunk`, `sessionPullChunk`, `sandboxError`; **keep** `srpcTable`, `routeSessionReq`, `answerSessionRequest`, `authorizeSessionRequest`, `handleSessionRequest`, `answerMintGitCredential`, `rpcRefusal`
- Modify: `internal/controld/api.go` — `handleSessionDiff`, `handlePushFiles`, `handlePullFiles`, `sessionForRPC`, `writeSandboxErr`, `sandboxMessage`, `validatePushChunk`
- Modify: `internal/controld/srpc_test.go`, `files_test.go`, `attach_test.go` — per the white-box rule and D1
- Modify: `controlapp/attachments.go`, `controlapp/workspace_rpc.go`, `controlapp/attachments_test.go` or `workspace_rpc_test.go` — `MaxTransferBytes`

**Interfaces:**
- Consumes: `s.attachments` (`AttachTerminal`, `WorkspaceDiff`, `PushWorkspace`, `PullWorkspace`), `runnerTransport.Dispatch` (session_rpc correlation from Task 5), `ownerOrAdmin.AuthorizeAttachment`, `controlStatus`.
- Produces:

```go
type attachBroker struct{ srv *Server }               // control.AttachmentBroker
type wsTerminalStream struct{ c *websocket.Conn }     // control.TerminalStream over the client socket
type attachSinceKey struct{}                          // ctx key: the attach cursor, adapter-internal
func withAttachSince(ctx context.Context, since uint64) context.Context
func attachSince(ctx context.Context) uint64
```

and in `controlapp`:

```go
type AttachmentOptions struct {
	// … existing fields …
	// MaxTransferBytes bounds one push or pull's compressed bytes. Zero means
	// workspace.MaxBytes; a negative value is ErrInvalid. Hosts lower it in
	// tests so the overrun path is exercised without streaming the full limit.
	MaxTransferBytes int64
}
```

`PushWorkspace`/`PullWorkspace` compare against `s.maxTransfer` instead of `workspace.MaxBytes`; chunk bounds stay `workspace.ChunkBytes`.

`handleClientAttach` becomes, in order: authenticate (wrapper); `since` parse; readiness — `waitRunning` (kept; it reads the store directly, which is a read the attach plan explicitly leaves to the adapter) answering `404`/`503 session_not_ready`/return-on-cancel as today; reachability — `row.Runner == "" || !s.transport.Connected(...)` → `502 runner_unreachable` as today; **then** `websocket.Accept`; then `s.attachments.AttachTerminal(withAttachSince(withUser(ctx, u), since), userScope(u), control.AttachTerminal{SessionID, Since: since, Mode: control.AttachmentController}, wsTerminalStream{c})`. `AttachTerminal` performs the authorization (generic + policy) and the readiness re-check, then calls the broker. A denial after the upgrade closes the socket with `StatusPolicyViolation` and today's reason text; `ErrConflict` (not attachable any more) closes with `StatusTryAgainLater "session not ready"`; `ErrUnavailable` closes with `StatusTryAgainLater "runner unreachable"`.

`wsTerminalStream` is typed where `relay.Conn` is not: `relay.Conn` is `Read(ctx) ([]byte, error)` / `Write(ctx, []byte) error` / `Close() error` over raw frames (internal/relay/runnerd_side.go:13), while `control.TerminalStream` carries `terminal.ClientMessage` / `terminal.ServerMessage`. So `Receive` reads one frame from the client socket and `json.Unmarshal`s a `ClientMessage`; `Send` `json.Marshal`s a `ServerMessage` and writes one frame; `Close(err)` maps to `closeAttach` with `StatusTryAgainLater` and a fixed reason. The public `protocol/terminal` structs are the protocol, so the re-encode is lossless.

`attachBroker.Attach(ctx, target, stream)`: `first, err := stream.Receive(ctx)` bounded by `attachFirstMsgTimeout`, must be `resize` (today's `readFirstResize` rule); mint `attachID`; park; send `dial_attach` with `Since: attachSince(ctx)`, `Cols`, `Rows`, `TargetURL`; arm the pair TTL; on dial-back, `splice(ctx, stream, relay.WSConn(c))` with two pumps — client→runner: `stream.Receive` → `json.Marshal` → `runner.Write`; runner→client: `runner.Read` → `json.Unmarshal` into `terminal.ServerMessage` → `stream.Send` — returning when either side ends, as today's byte splice does. `TerminalStream.Close(err)` maps to `closeAttach` with `StatusTryAgainLater`. The broker never reads a message after the first resize except to pump it, and logs no message.

Workspace handlers: `sessionForRPC` shrinks to path-ID extraction; the service does the read, authorization, readiness (`ErrConflict` when not running → `503 session_not_ready` "session is not running" — keep today's status for this one `ErrConflict` by mapping it in these three handlers specifically), and unreachable (`ErrUnavailable` → `unavailableStatus`). `handleSessionDiff` → `s.attachments.WorkspaceDiff`. `handlePushFiles` today accepts **one chunk per request** (`workspace.PushChunk` with `Xfer`/`Seq`) and the CLI sends one request per 1 MiB chunk under one `xfer` (`internal/cli/xfer.go:69–84`, which also checks `ack.Seq == seq`), while `controlapp.PushWorkspace` streams a whole archive from an `io.Reader` and mints its own sandbox-side `xfer`. Preserve the wire with a per-transfer pipe:

```go
type pushTransfer struct {
	pw      *io.PipeWriter
	done    chan error      // closed by the PushWorkspace goroutine with its result
	expires time.Time
}
type pushTable struct { mu sync.Mutex; m map[string]*pushTransfer } // keyed by xfer
```

On a chunk with `Seq == 0`: refuse a duplicate `xfer` (409 conflict); create `pr, pw := io.Pipe()`; start `go func() { done <- s.attachments.PushWorkspace(ctx, scope, control.PushWorkspace{SessionID, Path: chunk.Path, Body: pr}); pr.CloseWithError(...) }()` — `ctx` is a background context bounded by `cfg.OpTimeout × (workspace.MaxBytes/workspace.ChunkBytes + 2)` because it must outlive this one request; register the entry with `expires = now + cfg.AttachPairTTL`. On every chunk (including the first): look up `xfer` (404 not_found if absent or expired), require `chunk.Path` to equal the transfer's path and `Seq` to be the next expected (400 invalid_request otherwise), `pw.Write(chunk.Data)` — which blocks until the service has consumed it, giving today's backpressure — then answer `workspace.PushAck{Seq: chunk.Seq, Synced: false}`. On `Done: true`: `pw.Close()`, wait on `done`, remove the entry, and answer `PushAck{Seq, Synced: true}` on nil, else `controlStatus` (a service error mid-stream also surfaces on the next `pw.Write` as the pipe's close error, so a client learns about it on the very next chunk). A sweeper on the attach-pair TTL cadence closes expired pipes with `context.DeadlineExceeded`. The sandbox-side chunk numbering is now the service's own and is independent of the HTTP `seq`; the CLI only ever sees HTTP acks, so the wire is unchanged. `handlePullFiles` → `PullWorkspace{SessionID, Path, Body: w}` after writing the `200`/`Content-Type: application/gzip` headers on the first byte via a `firstWriteHeader` writer; a service error before any byte → `controlStatus`; after bytes → `panic(http.ErrAbortHandler)` as today; the transfer-limit `409 conflict` message is preserved by mapping `ErrInvalid` from pull specifically to `409 conflict "this path is larger than the transfer limit"` (D1 fixes the text, the status stays).

White-box rule for `srpc_test.go` (15): tests of `sessionRPC` correlation/timeout/disconnect → `adapt_transport_test.go` (Task 5 may already cover; name them) or `controlapp` `TestSessionRPC*`; tests of `decodeRPCAnswer`/`boundDiff`/`clipTo` → `controlapp` `TestDecodeRPCAnswer*`/`TestBoundDiff*`; tests of upward RPC stay.

- [ ] **Step 1: `MaxTransferBytes` in controlapp** — write `TestPullRefusesBeyondInjectedBound` (bound 3 KiB, sandbox answers 4 chunks of 1 KiB → `ErrInvalid` before the fourth write) and `TestNewAttachmentServiceRejectsNegativeBound`; run (FAIL); implement; run (PASS); `go test ./controlapp -race -count=1`.
- [ ] **Step 2: Broker tests** — `adapt_attach_test.go` over the existing attach harness in `attach_test.go`: first message not resize → close `PolicyViolation`; no dial-back within TTL → `TryAgainLater`; happy path splices both directions; `since` reaches `dial_attach`.
- [ ] **Step 3: Rewire attach, run `go test ./internal/controld -run Attach -race -count=1`.**
- [ ] **Step 4: Rewire diff/push/pull, run `go test ./internal/controld -run 'Files|Diff|Push|Pull|SRPC|Session' -race -count=1`; apply D1 text updates only.**
- [ ] **Step 5: Delete the listed `srpc.go` functions; migrate `srpc_test.go`; delete `authorizeOwnerOrAdmin` if nothing references it.**
- [ ] **Step 6: Gate** — `go vet ./... && ./scripts/check-public-control.sh && go test ./internal/controld ./controlapp ./internal/cli ./internal/e2e -race -count=1`.

Report: the push-path decision with evidence; every deleted `srpc_test.go` test by name with its disposition; every D1 text change; `git diff --stat`.

---

### Task 7: Sweep, package doc, and the full gate (reviewer-owned)

**Files:**
- Modify: `internal/controld/controld.go` — package doc and `New` cleanup (remove `notYet*` placeholders, `xferMax` moves into `AttachmentOptions.MaxTransferBytes` via `Config`)
- Modify: `internal/controld/api.go`, `runners.go`, `srpc.go`, `attach.go` — delete any helper left unreferenced (`go vet` and `staticcheck -checks U1000 ./internal/controld` if available; otherwise `grep` each deleted name)
- Modify: `README.md` — one paragraph: controld is composed from `controlapp`; what remains host-specific

- [ ] **Step 1:** `grep -rn "func (s \*Server)" internal/controld/*.go | grep -v _test` and confirm every remaining method is transport, rendering, auth, vault, upward RPC, or the two adapter arms. Anything else is a leftover.
- [ ] **Step 2:** Package doc for `internal/controld` states the split in one paragraph and names the four adapter-owned reads from Task 4 and the direct store writes from the Global Constraints.
- [ ] **Step 3:** Full gates, serially:

```bash
gofmt -l internal controlapp control scripts   # empty
make verify
go test ./internal/controld -run 'Reconcile|Runner|Event|Sched|Queue|Placement|Attach' -race -count=20
go test ./internal/e2e -race -count=3
./scripts/e2e-fleet.sh   # if docker and a GitHub login are available; exit 3 = CLI half skipped, acceptable
```

- [ ] **Step 4:** Diff the final passing-test list against the Task 4 baseline; every removed name must appear in a task report with a disposition.

## Acceptance checklist (gate O8)

- No code under `internal/controld` transitions a session, selects a runner, applies a lifecycle event, reconciles, or performs a downward session RPC except through a `controlapp` service; `grep -n "st.Transition\|OldestQueued\|SessionsOnRunner" internal/controld/*.go` shows only the adapter files and the renderer's `runnerHasRoom`.
- `controld.New` composes `SessionService`, `EnvironmentService`, `FleetService`, and `AttachmentService` from one adapter set; `Server.Run` is `FleetService.Run`.
- Every repository adapter refuses a foreign workspace with `ErrNotFound`; runner generation fencing is observable over the websocket (Task 5 Step 2).
- The `/v0/` wire is unchanged except D1–D6 and D9's rendering source; the `rainier` binary is unmodified.
- All black-box suites pass; every white-box test of deleted code has a named disposition; `make verify` and the `-count=20` race runs are green; the e2e suite is green.
- `check-public-control.sh` passes with its allowlist unchanged; `controlapp` imports nothing new.
- No provider identifier, secret value, credential, terminal byte, or runner free text appears in events, errors, added logs, commits, or test output.

## Not in this plan

- Mandatory workspace scope in the schema, retiring the model twins, persisted runner generations, placement generations on the wire, durable controller leases, the transactional outbox, portable checkpoint location: **O9**.
- A Cloud composer: after O10.
- A viewer/controller distinction in the self-hosted attach policy: the tenancy plan.

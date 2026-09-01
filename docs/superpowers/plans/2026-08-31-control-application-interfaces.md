# Public Control Application Interfaces Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use `executing-plans` and implement one task at a time. This plan freezes contracts only; behavior extraction follows in separate worktrees.

**Goal:** Define the minimal public Go contracts that let self-hosted `controld` and Rainier Cloud call the same portable session, environment, fleet, and attach application behavior without importing `internal/controld` or learning provider details.

**Architecture:** Add one public `control` package containing domain vocabulary, caller-facing application interfaces, and narrow host ports. The package is transport-, identity-, persistence-, and provider-neutral. This change does not move behavior: it makes the seam compile, documents every invariant, and pins it with external-package contract tests so three later workers can independently extract session/environment, fleet/reconciliation, and attach/RPC behavior behind a surface they may consume but not edit.

**Tech stack:** Go 1.25, existing public `protocol/{runner,terminal,workspace}` packages, standard library, existing Rainier gates.

**Specs:** `rainier-cloud/docs/architecture/adr-0001-oss-cloud-composition.md`, especially “Responsibility boundary,” “OSS application-service boundary,” “Workspace scope and authorization composition,” “Provider boundary,” and “Migration sequence”; `rainier-cloud/docs/superpowers/plans/2026-08-30-hosted-implementation-program.md`, gates O3–O4 and Wave 4.

## Decisions frozen by this plan

- The canonical import path is `github.com/tokencanopy/rainier/control`.
- The public surface is an application contract, not an exported HTTP server. It imports no HTTP, WebSocket, SQL, Docker, GitHub, billing, or cloud-provider package.
- Every application command/query receives a host-created `Scope`. It contains opaque workspace and actor identities plus Rainier placement context. It contains no role or cached allow/deny decision; current authorization remains an `Authorizer` call.
- IDs are distinct named string types. Provider-native identifiers are never represented.
- Caller-facing behavior is split into `Sessions`, `Environments`, `Fleet`, and `Attachments`. `Application` embeds those four interfaces only; consumers need not depend on a concrete service.
- Host dependencies are capability-sized ports: authorization, session state, environment state, fleet state, eligible-pool resolution, runner transport, attachment brokering, event recording, clock, and IDs. There is no `Host`, `Backend`, or catch-all store interface.
- Persistence ports carry `WorkspaceID` explicitly on every tenant-bearing operation. Self-hosted adapters map all calls to one installation-local workspace. This establishes the safe signature now; later workspace-storage work supplies the durable implementation and enforcement.
- Pool and runner identities are opaque Rainier identities. Eligible-pool resolution owns product/provider policy; the application owns selection among eligible runners.
- Commands use option structs; no positional booleans. List operations are cursor-paginated with stable ordering and capped limits.
- Errors are a closed sentinel set. Ports may wrap a sentinel with safe context but must not expose SQL, credentials, provider responses, filesystem paths, or terminal/session content.
- Application events are fixed-field, provider-neutral facts with stable event IDs. `EventRecorder` is deliberately a semantic port; the later persistence/outbox plan will make mutation plus event atomic without changing event vocabulary.
- Terminal and workspace bytes continue to use the already-public protocol packages. The control package references those contracts but never duplicates their message structs.
- This surface is pre-v1 and may evolve during `/v0/`, but there is one canonical contract at a time: no `V2`, `Legacy`, compatibility aliases, or Cloud fork.

## Ideal consumer shape

The external-package contract test pins the intended dependency direction:

```go
func create(ctx context.Context, app control.Application, scope control.Scope) (control.Session, error) {
	return app.CreateSession(ctx, scope, control.CreateSession{
		Name:           "investigate",
		EnvironmentID: "env_example",
		IdempotencyKey: "idem_example",
	})
}
```

Rainier Cloud constructs `scope` from authenticated regional state, supplies private adapters for the ports, and never supplies a provider project, VM type, cluster, native zone, or disk ID. Self-hosted `controld` constructs the same call with its installation workspace and GitHub actor.

## Public file structure

```text
control/
  doc.go                 package boundary and compatibility contract
  errors.go              closed safe sentinel vocabulary
  scope.go               typed IDs, actor, execution mode, authoritative scope
  session.go             session model, commands, queries, Sessions interface
  environment.go         portable environment model and Environments interface
  fleet.go               pools, runners, requirements, events, Fleet interface
  attach.go              attach/session-RPC commands and Attachments interface
  ports.go               narrow host-supplied ports only
  contract_test.go       external-package construction and semantic shape tests
  import_test.go         standalone consumer and forbidden-dependency smoke test
scripts/
  check-public-control.sh
Makefile
```

## Contract sketch

The implementation must use this vocabulary. Small naming corrections discovered by a failing compile test are allowed only in Task 1, before the freeze commit; after Task 4 no worker edits these signatures.

```go
type WorkspaceID string
type ActorID string
type SessionID string
type EnvironmentID string
type PoolID string
type RunnerID string
type EventID string

type ActorKind string
const (
	ActorUser    ActorKind = "user"
	ActorService ActorKind = "service"
)

type ExecutionMode string
const (
	ExecutionSelfHosted ExecutionMode = "self_hosted"
	ExecutionDedicated  ExecutionMode = "dedicated"
	ExecutionServerless ExecutionMode = "serverless"
)

type Actor struct {
	ID   ActorID
	Kind ActorKind
}

type PlacementScope struct {
	ProductRegion string
	HomeCell      string
	Mode          ExecutionMode
}

type Scope struct {
	WorkspaceID WorkspaceID
	Actor       Actor
	Placement   PlacementScope
}
```

`Scope.Validate` rejects empty IDs, unknown actor kinds/modes, and missing hosted region/cell. `self_hosted` may use documented installation-local region/cell values; zero scope is invalid. Roles are intentionally absent.

The closed errors are:

```go
var (
	ErrInvalid     = errors.New("control: invalid")
	ErrDenied      = errors.New("control: denied")
	ErrNotFound    = errors.New("control: not found")
	ErrConflict    = errors.New("control: conflict")
	ErrStale       = errors.New("control: stale")
	ErrUnavailable = errors.New("control: unavailable")
	ErrUnsupported = errors.New("control: unsupported")
)
```

`ErrNotFound` is also the non-disclosing answer for a resource outside the authoritative workspace. HTTP adapters map these centrally; the package contains no status codes or public error-envelope text.

Caller-facing interfaces are operation-oriented:

```go
type Sessions interface {
	CreateSession(context.Context, Scope, CreateSession) (Session, error)
	GetSession(context.Context, Scope, SessionID) (Session, error)
	ListSessions(context.Context, Scope, SessionQuery) (SessionPage, error)
	DeleteSession(context.Context, Scope, DeleteSession) error
	SuspendSession(context.Context, Scope, SuspendSession) (Session, error)
	ResumeSession(context.Context, Scope, ResumeSession) (Session, error)
	SnapshotSession(context.Context, Scope, SnapshotSession) (Checkpoint, error)
}

type Environments interface {
	CreateEnvironment(context.Context, Scope, CreateEnvironment) (Environment, error)
	GetEnvironment(context.Context, Scope, EnvironmentID) (Environment, error)
	ListEnvironments(context.Context, Scope, EnvironmentQuery) (EnvironmentPage, error)
	UpdateEnvironment(context.Context, Scope, UpdateEnvironment) (Environment, error)
	DeleteEnvironment(context.Context, Scope, DeleteEnvironment) error
}

type Fleet interface {
	RegisterRunner(context.Context, RunnerRegistration) (RunnerRegistrationResult, error)
	ReconcileRunner(context.Context, RunnerSnapshot) (ReconcileResult, error)
	ApplyRunnerEvent(context.Context, RunnerEvent) error
	ListRunners(context.Context, Scope, RunnerQuery) (RunnerPage, error)
}

type Attachments interface {
	AttachTerminal(context.Context, Scope, AttachTerminal, TerminalStream) error
	WorkspaceDiff(context.Context, Scope, WorkspaceDiff) (workspace.DiffAnswer, error)
	PushWorkspace(context.Context, Scope, PushWorkspace) error
	PullWorkspace(context.Context, Scope, PullWorkspace) error
}

type Application interface {
	Sessions
	Environments
	Fleet
	Attachments
}
```

Fleet service-principal calls carry authoritative workspace/pool/session bindings in `RunnerRegistration`, `RunnerSnapshot`, and `RunnerEvent`; they do not accept a user-created `Scope`. Runner generation is present and monotonic so later hosted fencing does not require replacing these event shapes.

`TerminalStream` is a transport adapter over complete terminal protocol messages, not a socket:

```go
type AttachmentMode string
const (
	AttachmentViewer     AttachmentMode = "viewer"
	AttachmentController AttachmentMode = "controller"
)

type AttachTerminal struct {
	SessionID SessionID
	Since     uint64
	Mode      AttachmentMode
}

type AttachTarget struct {
	WorkspaceID         WorkspaceID
	SessionID           SessionID
	PoolID              PoolID
	RunnerID            RunnerID
	PlacementGeneration uint64
	ControllerGeneration uint64
}

type TerminalStream interface {
	Receive(context.Context) (terminal.ClientMessage, error)
	Send(context.Context, terminal.ServerMessage) error
	Close(error) error
}
```

The application authorizes before calling the attachment broker and does not log or persist stream messages. Workspace push/pull use `io.Reader`/`io.Writer` plus the public workspace bounds; they do not introduce a second archive or path type.

The required ports are separate interfaces:

```go
type Authorizer interface {
	Authorize(context.Context, Scope, Action, Resource) error
}

type SessionRepository interface { /* workspace-keyed session operations */ }
type EnvironmentRepository interface { /* workspace-keyed environment operations */ }
type FleetRepository interface { /* pool-keyed runner/capacity operations */ }

type PoolResolver interface {
	EligiblePools(context.Context, Scope, Requirements) ([]Pool, error)
}

type RunnerTransport interface {
	Dispatch(context.Context, PoolID, RunnerID, runner.ToRunner) (runner.FromRunner, error)
	Connected(PoolID, RunnerID) bool
}

type AttachmentBroker interface {
	Attach(context.Context, AttachTarget, TerminalStream) error
}

type EventRecorder interface {
	Record(context.Context, Event) error
}

type Clock interface { Now() time.Time }
type IDGenerator interface {
	NewSessionID() SessionID
	NewEnvironmentID() EnvironmentID
	NewEventID() EventID
}
```

Repository methods mirror only the semantic reads/transitions the existing application behavior needs. They never expose SQL transactions, rows, JSON blobs, credentials, secrets, user records, GitHub identities, or provider resources. `RunnerTransport.Dispatch` carries the public runner protocol as-is; transport authentication and connection ownership stay in adapters.

## Portable models and invariants

- `Session` carries `WorkspaceID`, `CreatorID`, lifecycle state, environment reference, resolved portable execution spec, selected `PoolID`/`RunnerID`, monotonic placement generation, idempotency key, exit code, and timestamps. It contains no role, email, provider, native machine shape, charge, or credential.
- `SessionState` preserves the current vocabulary and `Terminal`/`OccupiesSlot` semantics byte-for-byte.
- `CreateSession` distinguishes an absent repository override from an explicitly empty list. It accepts one `EnvironmentID` or a scratch portable spec, never both. It does not accept CPU, memory, VM shape, or a user-selected size; environment defaults and host policy derive `Requirements` so ordinary sessions work without sizing choices.
- `SessionQuery` has capped limit, opaque cursor, stable `(created_at,id)` order, explicit terminal inclusion, and no provider filter.
- `Environment` preserves image/setup/init, egress, secret references, connectors, portable requirements, checkpoint metadata, and timestamps. Connector payloads remain bounded/validated at adapters; the control model does not know GitHub token storage.
- `Requirements` names portable capabilities and resource minima. It has no provider SKU. Session sizing remains optional/automatic for clients; host policy may derive requirements.
- `Pool` is an opaque eligible scheduling boundary with aggregate capacity and capabilities. The pool resolver may change providers without changing a session request.
- `Runner` and runner events carry pool, generation, portable capacity/capabilities, and session identities. A runner event that does not match the authoritative workspace, pool, runner, or generation is stale.
- `Checkpoint` is a portable opaque reference plus format/capability metadata. Provider object or image references stay inside the checkpoint adapter.
- `AttachTerminal` distinguishes viewer and controller intent. Controller authorization and the monotonic controller-lease generation are application facts; `AttachmentBroker` receives the granted generation out of band rather than changing terminal JSON. This leaves room for named-member shared sessions while preserving one fenced input controller.
- `Event` carries event ID, workspace, actor/service identity, resource identity, lifecycle action, timestamp, and provider-neutral resource/usage facts. Usage may include CPU time, memory byte-seconds, storage/network bytes, and agent token counts when available; it never includes a provider rate or customer charge. It has no terminal data, conversation content, secret, raw error, price, or provider resource ID.
- Every slice/map crossing the boundary is copied by the implementation. Empty collections have documented nil/empty behavior.

---

### Task 1: Pin external caller vocabulary and safe errors

**Files:** create `control/doc.go`, `control/errors.go`, `control/scope.go`, and the first part of `control/contract_test.go`.

- [ ] Write `package control_test` tests that construct every ID, actor kind, execution mode, and scope from outside the package.
- [ ] Pin zero/unknown scope validation failures to `ErrInvalid` without asserting free-form error text.
- [ ] Pin the seven sentinel identities with `errors.Is` and ensure wrapped errors contain no accidental storage/provider detail fixture.
- [ ] Document that actors and placement are authoritative adapter output, never decoded directly from client JSON.
- [ ] Run `go test ./control`; observe the expected compile failure before implementation.
- [ ] Implement the minimum types, validation, and doc comments; run `go test ./control -race`, `go vet ./control`, and `git diff --check`.
- [ ] Do not commit yet; Tasks 1–4 form one interface-freeze commit so no intermediate incomplete surface is consumed.

### Task 2: Freeze portable models and caller-facing interfaces

**Files:** create `control/session.go`, `control/environment.go`, `control/fleet.go`, and `control/attach.go`; extend `control/contract_test.go`.

- [ ] Write the ideal external call-site test shown above and compile-time assertions for all five interfaces.
- [ ] Port the current session-state vocabulary and pure state helpers with table tests proving no semantic drift.
- [ ] Define the command/query/page types with typed IDs, stable pagination contracts, idempotency fields, and explicit optionality; reject contradictory scratch/environment creation.
- [ ] Define portable environment requirements and checkpoint metadata without provider names or sizes that force callers to select a provider SKU.
- [ ] Define fleet registration/snapshot/event shapes with workspace/pool/runner/session binding and monotonic generation.
- [ ] Define `TerminalStream` and workspace operations by referencing public protocol packages, not copying messages.
- [ ] Run `go test ./control -race`, `go vet ./control`, and `git diff --check`.

### Task 3: Freeze narrow host ports

**Files:** create `control/ports.go`; extend `control/contract_test.go`.

- [ ] Derive each repository method from an existing `internal/controld` call site and record the mapping in a test comment. Do not copy the existing mixed identity/secret/session `Store` interface.
- [ ] Make `WorkspaceID` mandatory on every tenant repository method and `PoolID` mandatory on every fleet transport/state method.
- [ ] Define closed `Action` and `ResourceKind` vocabularies for every frozen application operation. `Resource` carries workspace, opaque ID, and creator when relevant; it carries no role.
- [ ] Define pool resolution, runner transport, attachment broker, event recorder, clock, and ID ports exactly once.
- [ ] Add compile-only fakes from `package control_test`, one per port, proving a Cloud module can implement them without internal imports.
- [ ] Add reflection/AST tests rejecting fields or exported names containing provider vocabulary (`gcp`, `aws`, `azure`, `oracle`, `hetzner`, `netcup`, project, instance type, machine type, cluster, native zone, disk ID) and sensitive-host vocabulary (`email`, `role`, `token`, `secret value`, `price`, `charge`). Allow portable `SecretRefs`, opaque `Checkpoint`, and documented Rainier product region/mode.
- [ ] Run `go test ./control -race`, `go vet ./control`, and `git diff --check`.

### Task 4: Add repository guards, freeze, and commit

**Files:** create `control/import_test.go` and `scripts/check-public-control.sh`; modify `Makefile`.

- [ ] Make `import_test.go` a standalone external consumer of `control` plus all three public protocol packages.
- [ ] Make the script fail if `control/**` imports any `internal/` path, HTTP/WebSocket, SQL/pgx, Docker, GitHub SDK, cloud SDK, billing package, or provider-named package. Permit standard `context`, `errors`, `io`, and `time` plus public protocol packages.
- [ ] Make the script fail on duplicate public control model definitions under `internal/controld` only after later extraction moves them; for this freeze commit, it reports duplicates as an explicit migration inventory rather than failing. Record the exact allowlist so new duplicates cannot appear.
- [ ] Add the guard to `make verify` after the public-protocol guard.
- [ ] Run:

```bash
go test ./control -race -count=10
bash scripts/check-public-protocols.sh
bash scripts/check-public-control.sh
go test -p 1 ./...
make verify
git diff --check
```

- [ ] Review every exported name with `go doc github.com/tokencanopy/rainier/control`; remove helpers without a demonstrated external caller.
- [ ] Commit exactly the contract/guard files as `feat: freeze public control application interfaces` and stop. Do not extract behavior in this branch.

## Parallel extraction unlocked by the freeze commit

After Task 4 merges, create three fresh worktrees from that merge. None may edit `control/*.go`, `internal/controld/controld.go`, or another lane's owned files.

| Lane | Owns | Implements behind frozen contracts |
|---|---|---|
| A — sessions/environments | new internal application files plus extracted lifecycle/environment tests | create/get/list/delete/suspend/resume/snapshot, environment resolution/checkpoint semantics |
| B — fleet | new internal scheduler/reconciliation files plus fleet tests | eligible-pool scheduling, runner registration, events, reconciliation, generation fencing |
| C — attach/RPC | new internal attach/workspace-operation files plus attach tests | authorization-before-disclosure, readiness, terminal broker, bounded workspace RPC |

The reviewer integrates those lanes, runs their shared contract suite, and only then assigns one sequential recomposition task that turns self-hosted `internal/controld.Server` into HTTP/auth/store/transport adapters over the application service. Cloud composition begins only after that self-hosted recomposition and a tagged OSS release.

## Review and acceptance checklist

- [ ] A separate Go module can import `control` and implement every port without an `internal/` import.
- [ ] The ideal caller never supplies role, home-cell authority, provider identity, runner choice, or billing data.
- [ ] Every tenant read/write has workspace scope; every fleet operation has pool and generation scope.
- [ ] Authorization precedes state disclosure or side effects by contract.
- [ ] Public protocol structs are referenced, never duplicated.
- [ ] Provider and sensitive hosted vocabulary guards pass.
- [ ] Lists are cursor-paginated and stable; retriable creates carry idempotency.
- [ ] Errors are branchable sentinels and safe to surface through a central adapter mapping.
- [ ] Self-hosted behavior has not changed in the interface-freeze commit.
- [ ] The three extraction lanes have disjoint ownership and require no signature edits.

## Deferred work

- Implementing the interfaces and recomposing self-hosted `controld`.
- Durable workspace-scoped PostgreSQL repositories, RLS, and transactional outbox integration.
- Capability negotiation and supported rolling runner/library version windows.
- Hosted email identity, membership policy, billing/spend controls, history/search, abuse controls, and provider adapters.
- Direct signed data-plane attachment, provider-specific diagnostics, and enterprise isolation modes.

These are consumers or implementations of the boundary; none belongs in the interface-freeze commit.

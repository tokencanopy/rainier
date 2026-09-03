# Hosted Composition Kit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish the last OSS pieces a hosted regional cell needs and cannot get from `control`/`controlapp` today — the runner plane, the attach plane, the `/v0/` wire shapes, and hosted login in the CLI — as public packages that self-hosted `controld` composes exactly as the cell will, and tag `v0.0.2`.

**Architecture:** Everything a cell needs already exists in `internal/controld`, coupled to `Server`. This plan extracts three packages behind small host interfaces and recomposes controld over them: `runnerplane` (the runner WebSocket endpoint, connection registry, dispatch correlation, generation mint and fence, capability acceptance, event translation), `attachplane` (the dial-back pairing, the terminal splice, the WebSocket terminal stream), and `v0wire` (the JSON views, request bodies, validators, and the closed sentinel→status table of `/v0/`). The CLI learns the hosted passwordless login and named contexts so one `rainier` binary talks to a self-hosted controld or a hosted cell. No behavior on the self-hosted wire changes; the live-fleet e2e is the proof.

**Tech Stack:** Go 1.25, `coder/websocket`, `control`, `controlapp`, `protocol/{runner,terminal,workspace}`, `internal/relay` (importable by these packages: they live in the same module).

**Spec:** `rainier-cloud/docs/architecture/adr-0001-oss-cloud-composition.md` — "OSS application-service boundary" ("HTTP routing … remain outside the application service": the routing stays with each host; the wire *shapes* and the runner/attach *protocol handling* are behavior the ADR gives the OSS, since Cloud "operates hosted endpoints" and must not copy protocol handling), "Responsibility boundary" (runner registration/reconciliation: OSS owns contract and behavior; attach orchestration: OSS owns), "Versioning and rollout compatibility"; `rainier-cloud/docs/architecture/adr-0002-gcp-v0-provider-architecture.md` "Cell gateway" (terminates hosted runner streams and terminal attach); `rainier-cloud/docs/superpowers/plans/2026-08-30-hosted-implementation-program.md` Wave 6 (worker C: gateway primitives) and gate O10; PRD §7 (CLI-first login journey), §14 (the CLI is the supported contract). This is OSS plan #10; the hosted lifecycle canary (rainier-cloud) consumes `v0.0.2`.

## Global Constraints

- Every task starts in a worktree from freshly fetched `origin/main` (Tasks 1–3 are independent lanes and may run in parallel; Task 4 is independent of them; Task 5 is the reviewer's). Nothing is pushed or merged by a worker.
- `control/*.go` does not change. `controlapp/*.go` does not change. `protocol/*` does not change.
- The new packages import only the standard library, `coder/websocket`, `control`, `controlapp`, `protocol/*`, and `internal/relay`; `scripts/check-public-control.sh`'s import-hygiene loop gains `runnerplane`, `attachplane`, and `v0wire` with `internal/relay` allowlisted for `attachplane` only. No SQL, HTTP-server routing, GitHub, Docker, provider, or billing import.
- One implementation. After Task 3, `internal/controld` contains no runner connection registry, no dispatch correlation, no attach pairing table, no terminal splice, and no `/v0/` view struct of its own: `grep -n "type runnerConn\|type attachTable\|type sessionView\|func controlStatus" internal/controld/*.go` is empty.
- The self-hosted `/v0/` wire, the runner protocol, and the CLI's existing behavior are unchanged; every existing test in `internal/controld`, `internal/e2e`, `cmd/rainier`, and `internal/cli` passes unmodified except where a test named a moved identifier. The live-fleet e2e on the fleet VM is the final gate.
- No secret value, credential, terminal byte, or session content enters a log line, an error, a commit, or test output. Synthetic identifiers only.
- Go gates run serially. `GOCACHE=/private/tmp/rainier-kit-gocache`.

## File structure

```text
runnerplane/
  doc.go             what the plane is and what a host supplies
  plane.go           Plane, New, Options, Handler, Transport, Send, Broadcast, Connected
  host.go            Host, Binding, the Aside hook and the SessionRequest hook
  conn.go            runnerConn, write loop, delivery, dispatch correlation, session-rpc table
  connect.go         announce, capability validation, generation mint, register, accept, reconcile, read loop
  events.go          FromRunner → control.RunnerEvent translation, generation fence, heartbeat
  plane_test.go      a fake host + fake runner over httptest: registration, accept, fence, dispatch, rpc
attachplane/
  doc.go
  plane.go           Plane, New, Broker (control.AttachmentBroker), BackHandler, DialBackURL, ClientStream
  pairing.go         attachTable, pendingAttach, TTL
  stream.go          wsTerminalStream, attachCloseReason, since-cursor context
  splice.go          splice(client control.TerminalStream, runner relay.Conn)
  plane_test.go
v0wire/
  doc.go
  sessions.go        SessionView, SessionEnvelope, SessionsEnvelope, CreateSessionRequest, RepoRequest,
                     SuspendRequest, RenderSession(control.Session, Derived), DecodeCreateSession
  environments.go    EnvironmentView, envelopes, CreateEnvironmentRequest, PatchEnvironmentRequest,
                     RenderEnvironment, connector validation, capability token rule
  runners.go         RunnerView, RunnersEnvelope, RenderRunner
  errors.go          ErrorBody, StatusFor(err) (status int, code, msg string), WriteError
  decode.go          DecodeJSON(w, r, v, limit) bool
  wire_test.go       golden JSON for every view; the status table; validators
internal/controld/
  runners.go         → host adapter only: identify (fleet token), aside (setup_done → snapshot cache;
                     credential_rejected), session_req (mint_git_credential); the plane does the rest
  attach.go          → the client attach handler over attachplane; mayAttach/waitRunning stay
  srpc.go            → the upward RPC answerer only (table moved)
  api.go             → handlers over v0wire types; no view/request struct definitions remain
  adapt_http.go      DELETED (v0wire/errors.go)
  adapt_attach.go    DELETED (attachplane)
  adapt_transport.go DELETED (runnerplane.Transport)
cmd/rainier/main.go  login --cloud, context subcommand, Rainier-Workspace header
internal/cli/config.go   named contexts
scripts/check-public-control.sh  hygiene loop covers the three packages
docs/deploy-gce.md, README.md    contexts and hosted login
```

---

### Task 1: `runnerplane`

**Files:**
- Create: `runnerplane/{doc,plane,host,conn,connect,events}.go`, `runnerplane/plane_test.go`
- Modify: `internal/controld/runners.go` (shrinks to the host adapter and the setup/credential asides), `internal/controld/controld.go` (compose), `internal/controld/adapt_scope.go` (`runnerCapabilities` stays; `validateCapabilities` moves to `v0wire`/`runnerplane` — the plane owns the runner-side rule, `v0wire` the environment-side rule, both the same token regexp exported once from `runnerplane` as `ValidCapability`)
- Delete: `internal/controld/adapt_transport.go`
- Modify: `internal/controld/runners_test.go`, `generations_test.go` (tests of moved behavior move to `runnerplane/plane_test.go` with a named disposition each; controld keeps the tests of its asides)

**Interfaces (produced):**

```go
package runnerplane

// Binding is the authoritative scope a connection acts in. The host derives it
// from the connection's credentials; the plane never decodes it from the announce.
type Binding struct {
	WorkspaceID control.WorkspaceID
	PoolID      control.PoolID
	RunnerID    control.RunnerID
}

// Host is what a plane needs from its host. Every method is one dependency.
type Host interface {
	// Identify authenticates an inbound runner connection and names the scope
	// it acts in. ErrDenied refuses the socket before the announce is read.
	// name is the runner's announced name; a host that binds identity to a
	// credential returns its own RunnerID and may refuse a mismatch.
	Identify(ctx context.Context, r *http.Request, name string) (Binding, error)
	// NextGeneration opens a new generation for the runner (the store's).
	NextGeneration(ctx context.Context, b Binding) (uint64, error)
	// Fleet is the application's fleet service: Register/Reconcile/ApplyRunnerEvent.
	Fleet() control.Fleet
	// FleetRepository is the heartbeat's and the disconnect's port.
	FleetRepository() control.FleetRepository
	// Wake asks the scheduler for a placement pass on the pool.
	Wake(pool control.PoolID)
	// Aside receives the events that transition no session — setup_done,
	// setup_failed, credential_rejected — after the plane has fenced them.
	Aside(ctx context.Context, b Binding, gen uint64, m runner.FromRunner)
	// SessionRequest answers an upward session RPC (a session_req) for the
	// session named; the answer is sent back down by the plane.
	SessionRequest(ctx context.Context, b Binding, sessionID control.SessionID, env runner.RPCEnvelope) runner.RPCEnvelope
}

type Options struct {
	OpTimeout    time.Duration // one dispatch round trip; default 60s
	ReadLimit    int64         // per-message; default the current runnerReadLimit
	Logf         func(string, ...any)
}

type Plane struct{ /* unexported */ }

func New(h Host, o Options) *Plane
func (p *Plane) Handler() http.Handler              // the runner WebSocket endpoint
func (p *Plane) Transport() control.RunnerTransport // Dispatch (incl. session_rpc correlation) and Connected
func (p *Plane) Send(pool control.PoolID, id control.RunnerID, m runner.ToRunner) error
func (p *Plane) Broadcast(pool control.PoolID, m runner.ToRunner, except control.RunnerID)
func (p *Plane) Close()                             // closes every connection; used at shutdown

// ValidCapability is the one token rule for a portable capability.
func ValidCapability(s string) bool
const MaxCapabilities = 32
```

Behavior moved verbatim from `internal/controld/runners.go` and `adapt_transport.go`: `newRunnerConn`/`shutdown`/`enqueue`/`deliver`/`writeLoop`; `readAnnounce`; the announce → `Identify` → `NextGeneration` → capability validation → `RegisterRunner` (fleet-first, per #36) → install → `accept` → `ReconcileRunner` → destroys → `Wake` → read loop; heartbeat through `FleetRepository().UpsertRunner` with the ErrStale fence; `applyRunnerEvent`'s translation (`eventGeneration`, `PlacementGeneration` copy, `stageFailure`, child-exit parse) with the aside cases routed to `Host.Aside`; `routeSessionReq` and the `srpcTable`; `destroyOrphan` (retries); `sendToRunner`/`broadcastToRunners`; `runnerTransport.Dispatch`/`sessionRPC`/`awaitAnswer`/`unreachable`; `retireRunner` through `FleetRepository().SetRunnerConnected`. The runner token check becomes the self-hosted host's `Identify`. Log lines keep their text.

controld's host adapter (`runners.go`): `type runnerHost struct{ srv *Server }` — `Identify` = `runnerTokenOK` + `Binding{installWorkspace, installPool, RunnerID(name)}`; `NextGeneration` = `st.NextRunnerGeneration`; `Fleet` = `s.fleet`; `FleetRepository` = `st.Fleet()`; `Wake` = `s.fleet.Wake`; `Aside` = today's `applyAdapterArm` (setup_done → `cacheEnvironment`; credential_rejected → `rejectCredential`); `SessionRequest` = `authorizeSessionRequest`/`answerMintGitCredential`. `compose()` builds `s.plane = runnerplane.New(runnerHost{s}, …)`, `s.transport = s.plane.Transport()`, and the mux mounts `s.plane.Handler()` at the runner connect path.

- [ ] **Step 1: Write the failing plane test.** `runnerplane/plane_test.go`: a `fakeHost` (records Identify/NextGeneration calls; a stub `control.Fleet` that accepts registrations and answers reconcile with a destroy list; an in-memory `control.FleetRepository`) and a `fakeRunner` over `httptest` speaking `protocol/runner` (announce with capabilities → expects `accept` first with generation 1 → answers a dispatched `create` with a result → sends an event with generation and placement_generation → sends a `session_req` and expects the host's answer). Cases: registration and accept-first; a refused Identify closes before the announce is read; the generation continues across a second `New` over the same fake repository; a heartbeat from a superseded generation ends the connection; `Dispatch` correlates by ReqID and `session_rpc` by envelope ID; `Connected` flips on retire.
- [ ] **Step 2: Run it** — compile failure.
- [ ] **Step 3: Extract** the code listed above; recompose controld; move the controld tests of moved behavior (`TestRunnerGenerationContinuesAcrossRestart`, `TestSupersededConnectionIsFencedOnHeartbeat`, `TestAnnouncedCapabilities`, `TestConnectRunnerRefusalDoesNotCloseTheWinningConn`, the dispatch/correlation/reconnect tests in `runners_test.go`) to `runnerplane` against the fake host, keeping every assertion's value; controld keeps the aside tests (`TestSetupDone*`, credential rejection) unchanged.
- [ ] **Step 4: Gates** — `gofmt`, `go vet ./...`, `go test ./runnerplane ./internal/controld -race -count=3`, `go test ./internal/e2e -count=1`, `scripts/check-public-control.sh`, `make verify`, `git diff --check`; `grep -n "type runnerConn\|awaitAnswer\|srpcTable" internal/controld/*.go` empty.
- [ ] **Step 5: Leave the tree ready** — `refactor: extract the runner plane into a public package`.

---

### Task 2: `attachplane`

**Files:**
- Create: `attachplane/{doc,plane,pairing,stream,splice}.go`, `attachplane/plane_test.go`
- Modify: `internal/controld/attach.go` (keeps `handleClientAttach`, `mayAttach`, `waitRunning`, `failedButAttachable`; the pairing table, the attach-back handler, the splice, and the URL derivation move), `internal/controld/controld.go`
- Delete: `internal/controld/adapt_attach.go`
- Modify: `internal/controld/attach_test.go`, `adapt_attach_test.go` → moved cases into `attachplane/plane_test.go`

**Interfaces (produced):**

```go
package attachplane

type Host interface {
	// IdentifyRunner authenticates a runner's dial-back and names the pool/runner it is.
	IdentifyRunner(ctx context.Context, r *http.Request) (control.PoolID, control.RunnerID, error)
	// Send delivers the dial_attach to the runner (runnerplane.Send).
	Send(pool control.PoolID, id control.RunnerID, m runner.ToRunner) error
	// BackURL is this replica's attach-back WebSocket URL for an attach id
	// (a host with a gateway fronting it names the gateway's public URL).
	BackURL(attachID string) string
}

type Options struct {
	PairTTL time.Duration // default 15s
	Logf    func(string, ...any)
}

type Plane struct{ /* unexported */ }

func New(h Host, o Options) *Plane
func (p *Plane) Broker() control.AttachmentBroker  // control.AttachTarget + TerminalStream → paired splice
func (p *Plane) BackHandler() http.Handler          // the runner's dial-back endpoint
// ClientStream wraps an accepted client WebSocket as a control.TerminalStream.
func ClientStream(c *websocket.Conn) control.TerminalStream
// WithSince / Since carry the attach cursor through the broker's context.
func WithSince(ctx context.Context, since uint64) context.Context
func Since(ctx context.Context) uint64
```

Behavior moved verbatim: `attachTable`/`pendingAttach`/`park`/`claim`/`has`, `attachBroker.Attach` (mint attach id, park, `dial_attach` via `Send`, await the dial-back within `PairTTL`, `attachFirstResize`), `handleAttachBack` (identify, claim, splice), `splice`, `wsTerminalStream`, `attachCloseReason`, `attachBackURL`'s scheme switch. controld's host: `IdentifyRunner` = runner token check + `installPool` + the runner name from the request; `Send` = `s.plane.Send`; `BackURL` = today's derivation from `cfg.ExternalURL`.

- [ ] **Step 1: Failing test** — `attachplane/plane_test.go`: a fake host whose `Send` records the `dial_attach` and immediately dials the `BackHandler` over `httptest` as the runner, with a fake `relay.Conn` producing a snapshot frame; a client `TerminalStream` stub. Cases: a paired attach splices the first snapshot to the client; an unpaired attach times out at `PairTTL` and closes the client with the documented reason; a dial-back for an unknown attach id is refused; an unauthenticated dial-back is refused before any claim.
- [ ] **Step 2: Run** — compile failure. **Step 3: Extract and recompose.** **Step 4: Gates** as Task 1 plus `go test ./attachplane -race -count=3` and the e2e attach scene. **Step 5:** `refactor: extract the attach plane into a public package`.

---

### Task 3: `v0wire`

**Files:**
- Create: `v0wire/{doc,sessions,environments,runners,errors,decode}.go`, `v0wire/wire_test.go`
- Modify: `internal/controld/api.go` (handlers use `v0wire` types; the definitions and validators leave), `internal/controld/adapt_http.go` deleted, `internal/controld/adapt_scope.go` (`validateCapabilities` → `v0wire.ValidateCapabilities`, which calls `runnerplane.ValidCapability`), `internal/controld/api_test.go` (only import/identifier renames), `cmd/rainier/main.go` (its own view mirrors may stay; a follow-up may switch the CLI to `v0wire` — not this plan)

**Produced (exported, JSON tags identical to today's wire — the response-shape tests are the proof):** `SessionView`, `SessionDerived`, `RenderSession`, `SessionEnvelope`, `SessionsEnvelope`, `CreateSessionRequest`, `RepoRequest`, `SuspendRequest`, `DecodeCreateSession(req) (control.CreateSession, string)` (the validation controld does today, returning the 400 message), `EnvironmentView`, `EnvironmentEnvelope`, `EnvironmentsEnvelope`, `CreateEnvironmentRequest`, `PatchEnvironmentRequest`, `RenderEnvironment(control.Environment, snapshotRunner control.RunnerID)`, `ValidateEnvironmentBasics`, `ValidateConnectors`, `ValidateCapabilities`, `EnvironmentRequirements(placement string, capabilities []string) control.Requirements`, `PlacementOf(control.Requirements) string`, `RunnerView`, `RunnersEnvelope`, `RenderRunner(control.Runner, connected bool)`, `ErrorBody`, `StatusFor(err error) (int, string, string)` (today's `controlStatus` table), `WriteError(w, status, code, msg)`, `WriteControlError(w, err)`, `DecodeJSON(w, r, v any, limit int64) bool`. Secrets and credentials views stay in controld (self-hosted vault only).

- [ ] **Step 1: Failing test** — `v0wire/wire_test.go`: golden JSON for one fully populated value of every view (byte-exact strings, synthetic values), the key-set of each view, `StatusFor` for all seven sentinels plus context cancellation, and the validators' accept/reject tables (moved from `api_test.go` where they exist).
- [ ] **Step 2–3:** compile failure; move. **Step 4: Gates** — the controld response-shape tests are the wire proof; `go test ./v0wire ./internal/controld ./cmd/rainier ./internal/cli -race -count=3`, `make verify`, guard. **Step 5:** `refactor: publish the /v0/ wire shapes as v0wire`.

---

### Task 4: Hosted login and contexts in the CLI

**Files:**
- Modify: `internal/cli/config.go` (named contexts), `internal/cli/client.go` (the `Rainier-Workspace` header when the context names a workspace), `cmd/rainier/main.go` (`login --cloud <edge URL>`, `context list|use|current|remove`, `workspace use`), `cmd/rainier/main_test.go`, `internal/cli/client_test.go`, `README.md`

**Produced:**

```go
// internal/cli
type Context struct {
	Server      string `json:"server_url"`
	Token       string `json:"token"`
	OwnerID     string `json:"owner_id,omitempty"`     // self-hosted
	Workspace   string `json:"workspace,omitempty"`    // hosted: the active workspace id
	RefreshToken string `json:"refresh_token,omitempty"` // hosted only
	AccessExpiresAt string `json:"access_expires_at,omitempty"`
}
type Config struct {
	Current  string             `json:"current"`
	Contexts map[string]Context `json:"contexts"`
	// legacy single-context fields are read once and migrated on Save
}
```

`Load` migrates a legacy `{server_url, token, owner_id}` file into `contexts["default"]`, `Current = "default"`, byte-for-byte compatible for every existing caller. `Client` sends `Rainier-Workspace: <id>` when the current context has a workspace and refreshes an expired hosted access token through `POST /v0/auth/refresh` transparently once, saving the rotated pair (the refresh token is single-use and rotates; a replay answers 401 `credential_replayed`, which the CLI reports as "log in again").

`rainier login --cloud https://edge.example.test`: `POST /v0/auth/login-attempts {device_name}` → prints the browser URL (`server + browser_path`) and opens it when a browser is available → polls `POST /v0/auth/login-attempts/{id}/exchange {poll_token}` at the returned interval while it answers 202 → stores the token pair; then `GET /v0/workspaces` and, with exactly one workspace, sets it as the context's workspace, else prints them and asks for `rainier workspace use <id>`. The hosted wire is the one `rainier-cloud/internal/edge/authhttp` serves today (bodies and codes verbatim from that handler).

- [ ] **Step 1: Failing tests** — `internal/cli`: legacy config migrates and re-saves in the new shape; the header is sent iff a workspace is set; one transparent refresh on 401 then give up. `cmd/rainier/main_test.go`: `login --cloud` against an `httptest` edge scripted from the authhttp contract (201 → 202 ×2 → 200; then the workspaces listing) stores the context; `context use`/`list`; `workspace use`.
- [ ] **Step 2–3:** fail; implement. **Step 4: Gates** — `go test ./cmd/rainier ./internal/cli -race -count=3`, `make verify`. **Step 5:** `feat(cli): hosted login and named contexts`.

---

### Task 5: Docs, the live-fleet proof, and `v0.0.2` (reviewer)

- `README.md`, `docs/deploy-gce.md`: contexts, `login --cloud`; the three packages in the "public Go surface" list.
- `scripts/check-public-control.sh`: hygiene for the three packages.
- Live-fleet e2e on the fleet VM from the integration branch, every scene.
- Tag `v0.0.2` with release notes naming the three packages and the CLI change; the external-import proof extends to `runnerplane`, `attachplane`, `v0wire`.

## Acceptance

- A module outside this repository composes a runner endpoint, an attach broker, and `/v0/` renderers from the tag with no `replace` and no `internal/` import.
- `internal/controld` has no connection registry, pairing table, splice, or view struct of its own; controld's runner and attach behavior is the packages' (the e2e is unchanged and green).
- The CLI logs into a hosted edge from the documented flow and carries the workspace scope; every self-hosted CLI test passes unmodified.

## Not in this plan

- A public HTTP router for `/v0/` (each host keeps its routing, per ADR-0001); a CLI switched to `v0wire` types; multi-instance attach routing (rainier-cloud's gateway, with Redis, per ADR-0002).

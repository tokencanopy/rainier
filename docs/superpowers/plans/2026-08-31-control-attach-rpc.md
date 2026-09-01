# Control Attach and Workspace RPC Extraction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement authorized terminal attachment and bounded workspace diff/push/pull orchestration behind the frozen `control.Attachments` interface.

**Architecture:** Add one deep `AttachmentService` module in the public `controlapp` package. It resolves and authorizes a session, grants a fenced controller generation, and delegates terminal transport to `AttachmentBroker`; workspace operations share one private session-RPC implementation over `RunnerTransport` and enforce every path, chunk, correlation, and aggregate bound before bytes cross the seam.

**Tech Stack:** Go 1.25, frozen `control` contracts, public `protocol/runner`, `protocol/terminal`, and `protocol/workspace` packages, standard library streaming and atomic primitives.

**Spec:** `rainier-cloud/docs/architecture/adr-0001-oss-cloud-composition.md`, `rainier-cloud/docs/security/hosted-tenancy-and-security.md` session collaboration rules, `rainier-cloud/docs/superpowers/plans/2026-08-30-hosted-implementation-program.md` gates O4-O8 and Wave 4, and `docs/superpowers/plans/2026-08-31-control-application-interfaces.md`.

## Global Constraints

- Work only in `.worktrees/control-attach-rpc` on `feat/control-attach-rpc`, created from freshly fetched `origin/main` after the three Wave 4 plan documents merge.
- Own only `controlapp/attachments.go`, `controlapp/attachments_test.go`, `controlapp/workspace_rpc.go`, `controlapp/workspace_rpc_test.go`, and `controlapp/attachments_external_test.go`.
- Do not edit `control/*.go`, `internal/controld/controld.go`, another Wave 4 lane's files, protocols, `go.mod`, `Makefile`, HTTP/WebSocket adapters, or existing self-hosted tests.
- Do not delete or redirect existing `internal/controld` attach/RPC behavior. The later recomposition plan performs the cutover.
- The module imports no HTTP/WebSocket implementation, SQL/pgx, Docker, GitHub SDK, cloud SDK, billing package, or `internal/` package.
- `TerminalStream` carries complete public terminal messages. The module never parses a socket, logs a terminal message, persists terminal bytes, or duplicates the terminal protocol.
- Every operation validates scope, performs a workspace-keyed session read, and authorizes the exact resource/action before calling the broker, runner, reader, or writer.
- Only the creator may resume under the v0 policy adapter, but named active workspace members may view or control a shared session when the generic authorizer and mode-aware attachment policy both grant it; the module contains no roles or membership cache.
- A session has one controller at a time. Each accepted controller attach increments a monotonic generation; viewers never increment it. The in-memory lease store is explicitly extraction-only and is replaced by durable controller generations in the next sequential scope/generation plan.
- Workspace paths use `workspace.ValidatePath`; pushes and pulls cap compressed bytes at `workspace.MaxBytes`; each RPC chunk caps data at `workspace.ChunkBytes`; diffs cap repository count, labels, and stat bytes.
- Runner and sandbox output is untrusted. Reject wrong session, wrong RPC ID, wrong method, wrong sequence, empty-progress chunks, oversized data, malformed JSON, and unsafe error payloads.
- No prompt, response, terminal output, command, repository content, path, archive bytes, credential, or raw sandbox error enters logs, events, commit messages, or test output.
- Use only synthetic `.test`, `.invalid`, `example.com`, `agents.localhost`, and fictional opaque IDs in tests and commits.

## File structure

```text
controlapp/
  attachments.go                AttachmentService and controller generations
  attachments_test.go           authorization, readiness, sharing, fencing tests
  workspace_rpc.go              correlated RPC plus bounded diff/push/pull streams
  workspace_rpc_test.go         hostile-sandbox and transfer contract tests
  attachments_external_test.go  external-package construction and import guard
```

The module is independently constructible. The final composer supplies the same session repository and runner transport used by the other extracted modules, then exposes only `control.Application` to normal callers.

## Behavior intentionally outside this lane

- HTTP/WebSocket upgrade, first-resize parsing, attach-back pairing URLs/tokens, and byte splicing remain `AttachmentBroker` adapter behavior.
- Session-initiated credential mint RPC is not part of the frozen `control.Attachments` interface. The self-hosted credential adapter retains it until a later portable credential-broker contract is accepted; this lane handles only control-initiated workspace RPC.
- Durable collaboration grants, lease expiry/renewal, handoff, reclaim, revocation closure, and stale-input enforcement are completed by the next scope/generation plan. This extraction supplies a monotonic generation to the broker and never treats workspace role as content authority.

---

### Task 1: Implement authorized, generation-fenced terminal attachment

**Files:**
- Create: `controlapp/attachments.go`
- Create: `controlapp/attachments_test.go`

**Interfaces:**
- Consumes: `control.Authorizer`, a mode-aware `AttachmentPolicy`, `control.SessionRepository`, `control.RunnerTransport`, `control.AttachmentBroker`, `control.EventRecorder`, `control.Clock`, and `control.IDGenerator`.
- Produces: `NewAttachmentService(AttachmentOptions) (*AttachmentService, error)` and `AttachTerminal` from `control.Attachments`.

- [ ] **Step 1: Write constructor and attach contract tests**

Create lane-prefixed fakes and compile-time assertions:

```go
type terminalAttachment interface {
	AttachTerminal(context.Context, control.Scope, control.AttachTerminal, control.TerminalStream) error
}

var _ terminalAttachment = (*AttachmentService)(nil)

func TestAttachAuthorizesBeforeBroker(t *testing.T) {
	fx := newAttachmentFixture(t)
	fx.auth.err = control.ErrDenied
	err := fx.service.AttachTerminal(ctx, scope, control.AttachTerminal{
		SessionID: "sess_example", Since: terminal.SinceAll,
		Mode: control.AttachmentViewer,
	}, &recordingTerminalStream{})
	if !errors.Is(err, control.ErrDenied) { t.Fatalf("got %v", err) }
	if fx.broker.calls != 0 { t.Fatal("denied attach reached broker") }
}
```

Add cases for invalid scope, nil stream, unknown mode, cross-workspace not-found, running session, failed-but-still-connected session, dead/queued/suspended refusal, disconnected runner, viewer generation, first/second controller generations, concurrent controller attaches producing distinct generations, broker error, stream-close behavior, and no terminal message read by the module.

Add separate policy cases proving a view grant cannot attach as controller, a control grant can attach as viewer or controller, and generic `ActionAttach` denial wins before the mode policy runs.

- [ ] **Step 2: Run the tests and observe the expected missing package**

Run:

```bash
go test ./controlapp -run 'Test(NewAttachmentService|Attach)' -count=1
```

Expected: FAIL because `AttachmentService` is absent.

- [ ] **Step 3: Add the exact construction seam**

Define:

```go
type AttachmentOptions struct {
	Authorizer control.Authorizer
	Policy AttachmentPolicy
	Sessions control.SessionRepository
	Transport control.RunnerTransport
	Broker control.AttachmentBroker
	Events control.EventRecorder
	Clock control.Clock
	IDs control.IDGenerator
}

type AttachmentPolicy interface {
	AuthorizeAttachment(context.Context, control.Scope, control.Resource, control.AttachmentMode) error
}

type AttachmentService struct {
	auth control.Authorizer
	policy AttachmentPolicy
	sessions control.SessionRepository
	transport control.RunnerTransport
	broker control.AttachmentBroker
	events control.EventRecorder
	clock control.Clock
	ids control.IDGenerator
	rpcSeq atomic.Uint64
	leaseMu sync.Mutex
	controller map[attachmentLeaseKey]uint64
}

type attachmentLeaseKey struct {
	workspace control.WorkspaceID
	session control.SessionID
}
```

`NewAttachmentService` rejects a missing dependency—including `AttachmentPolicy`—with `ErrInvalid`, initializes the controller map keyed by authoritative workspace plus session, and starts no goroutine. The policy is a real seam: self-hosted maps it to creator/installation policy, while Cloud maps it to current session collaboration grants.

- [ ] **Step 4: Resolve, authorize, and validate readiness**

Use one private helper shared by terminal and workspace operations:

```go
func (s *AttachmentService) authorizedSession(ctx context.Context, scope control.Scope,
	id control.SessionID, action control.Action) (control.Session, error) {
	if err := scope.Validate(); err != nil || id == "" { return control.Session{}, control.ErrInvalid }
	row, err := s.sessions.GetSession(ctx, scope.WorkspaceID, id)
	if err != nil { return control.Session{}, attachmentPortError(err) }
	resource := control.Resource{Kind: control.ResourceSession, WorkspaceID: row.WorkspaceID,
		ID: string(row.ID), CreatorID: row.CreatorID}
	if err := s.auth.Authorize(ctx, scope, action, resource); err != nil {
		return control.Session{}, control.ErrDenied
	}
	return cloneAttachmentSession(row), nil
}
```

Implement `attachmentPortError` as a switch over the seven `control` sentinels using `errors.Is`, returning the matching sentinel and mapping every other adapter failure to `control.ErrUnavailable`. `cloneAttachmentSession` copies the session's command, egress, repositories, and child-exit pointer so this lane compiles independently and never aliases repository memory.

Authorization follows the internal read needed to construct the authoritative resource but precedes disclosure and all external effects. Never authorize against actor-supplied creator/workspace values.

After generic `ActionAttach` authorization, call `Policy.AuthorizeAttachment` with the validated mode and the same authoritative resource. Map a refusal to `ErrDenied`. This extra mode-aware check is required because the frozen `control.ActionAttach` value alone cannot distinguish a view grant from a control grant.

- [ ] **Step 5: Grant a controller generation and call the broker**

Accept running sessions. Also accept failed sessions only when they retain a non-empty pool/runner and `Transport.Connected(pool,runner)` is true, preserving setup-failure diagnosis. Reject all other states with `ErrConflict`.

For a controller, increment under `leaseMu` and reject uint64 overflow as `ErrUnavailable`; for a viewer, read the current value without incrementing. Build:

```go
target := control.AttachTarget{
	WorkspaceID: row.WorkspaceID, SessionID: row.ID,
	PoolID: row.PoolID, RunnerID: row.RunnerID,
	PlacementGeneration: row.PlacementGeneration,
	ControllerGeneration: generation,
}
```

Call `Broker.Attach(ctx,target,stream)` synchronously. On error, call `stream.Close(control.ErrUnavailable)` once and return `ErrUnavailable`; on success, return without closing a stream the broker now owns. Record `ActionAttach` without terminal content.

- [ ] **Step 6: Run the attach gate and commit**

Run:

```bash
gofmt -w controlapp/attachments.go controlapp/attachments_test.go
go test ./controlapp -run 'Test(NewAttachmentService|Attach)' -race -count=20
go vet ./controlapp
git diff --check
```

Commit:

```bash
git add controlapp/attachments.go controlapp/attachments_test.go
git commit -m "feat: extract terminal attachment"
```

### Task 2: Implement one correlated session-RPC path

**Files:**
- Create: `controlapp/workspace_rpc.go`
- Create: `controlapp/workspace_rpc_test.go`

**Interfaces:**
- Consumes: `control.RunnerTransport.Dispatch` and public `runner.RPCEnvelope` messages.
- Produces: private `sessionRPC(context.Context, control.Session, string, any, any) error` used by all workspace operations.

- [ ] **Step 1: Write hostile-response tests**

Create tests for success, false `OK`, wrong `FromRunner.Type`, wrong session, missing RPC, wrong envelope ID, non-`resp` method, malformed response JSON, empty successful payload, disconnected runner, context cancellation, and two concurrent out-of-order calls. The fake transport captures exact public runner messages and returns configured replies.

Pin the request shape:

```go
if got.Type != "session_rpc" || got.Session != "sess_example" || got.ReqID != 0 ||
	got.RPC == nil || got.RPC.ID == 0 || got.RPC.Method != workspace.MethodDiff {
	t.Fatalf("request = %+v", got)
}
```

Never print `got.RPC.Payload` in a failure because it can contain workspace-derived data; compare decoded synthetic fields.

- [ ] **Step 2: Run the RPC tests and observe failure**

Run:

```bash
go test ./controlapp -run TestSessionRPC -count=1
```

Expected: FAIL because `sessionRPC` does not exist.

- [ ] **Step 3: Implement request encoding and response validation**

Use `rpcSeq.Add(1)` and reject rollover to zero. Marshal nil as an absent payload and validate a caller-supplied `json.RawMessage` before forwarding it. Dispatch:

```go
msg := runner.ToRunner{Type: "session_rpc", Session: string(row.ID),
	RPC: &runner.RPCEnvelope{ID: id, Method: method, Payload: payload}}
res, err := s.transport.Dispatch(ctx, row.PoolID, row.RunnerID, msg)
```

Require `res.Type == "session_req"`, `res.Session == string(row.ID)`, `res.RPC != nil`, matching ID, and `res.RPC.Method == "resp"`. A false `OK` returns `ErrUnavailable` without relaying the sandbox's payload text. Decode a successful payload into `out` with `json.Decoder`; reject trailing JSON. A nil `out` accepts an absent payload.

- [ ] **Step 4: Run the RPC gate and commit**

Run:

```bash
gofmt -w controlapp/workspace_rpc.go controlapp/workspace_rpc_test.go
go test ./controlapp -run TestSessionRPC -race -count=20
go test ./controlapp -race
go vet ./controlapp
git diff --check
```

Commit:

```bash
git add controlapp/workspace_rpc.go controlapp/workspace_rpc_test.go
git commit -m "feat: extract correlated session rpc"
```

### Task 3: Implement bounded workspace diff and push

**Files:**
- Modify: `controlapp/workspace_rpc.go`
- Modify: `controlapp/workspace_rpc_test.go`

**Interfaces:**
- Consumes: `workspace.MethodDiff`, `workspace.MethodPushFiles`, `workspace.ValidatePath`, and `control.PushWorkspace.Body`.
- Produces: `WorkspaceDiff` and `PushWorkspace` from `control.Attachments`.

- [ ] **Step 1: Write diff-bound and push-stream tests**

For diff, return 65 repositories with 300-byte labels and a `workspace.StatBytes+1` stat; assert the result contains 64 entries and every string is clipped to valid UTF-8 at its byte bound. Assert an empty result serializes with `repos:[]` through the public protocol.

For push, cover unsafe path, nil body, empty archive, one short chunk, exact `ChunkBytes`, multiple chunks, exactly `MaxBytes`, `MaxBytes+1`, wrong ack sequence, unsynced final ack, reader error, and context cancellation. Capture each `workspace.PushChunk` decoded from the runner RPC.

- [ ] **Step 2: Run diff/push tests and observe failure**

Run:

```bash
go test ./controlapp -run 'Test(WorkspaceDiff|PushWorkspace)' -count=1
```

Expected: FAIL because the public methods are absent.

- [ ] **Step 3: Implement bounded diff**

Resolve and authorize with `ActionDiff`, require the session to be running, call `sessionRPC` with `workspace.MethodDiff`, and bound the untrusted answer:

```go
const maxDiffRepos = 64
const maxDiffLabel = 256
```

Clip `Repo`, `BaseBranch`, and `SessionBranch` to 256 valid UTF-8 bytes and `Stat` to `workspace.StatBytes`. Copy before clipping so the decoded transport object cannot alias the returned value.

- [ ] **Step 4: Implement streaming push without buffering the archive**

Resolve and authorize with `ActionPush`, validate `cmd.Body != nil` and `workspace.ValidatePath(cmd.Path)`, and require running. Generate a 128-bit lowercase-hex transfer ID with `crypto/rand`.

Read with a `bufio.Reader` so exact chunk boundaries can detect EOF without sending a spurious empty chunk. For each sequence, send at most `workspace.ChunkBytes`, set `Done` only on the last chunk, and track total compressed bytes. Read one byte beyond `workspace.MaxBytes` to detect overflow and return `ErrInvalid` without forwarding it. Require `ack.Seq == chunk.Seq` and require `ack.Synced` on the final chunk.

Use this shape for every hop:

```go
chunk := workspace.PushChunk{Xfer: xfer, Path: cmd.Path,
	Seq: seq, Data: slices.Clone(data), Done: done}
var ack workspace.PushAck
if err := s.sessionRPC(ctx, row, workspace.MethodPushFiles, chunk, &ack); err != nil {
	return err
}
```

Record `ActionPush` only after the final synced ack. Never include path, transfer ID, data, or ack payload in the event.

- [ ] **Step 5: Run the diff/push gate and commit**

Run:

```bash
gofmt -w controlapp/workspace_rpc.go controlapp/workspace_rpc_test.go
go test ./controlapp -run 'Test(WorkspaceDiff|PushWorkspace)' -race -count=20
go test ./controlapp -race
go vet ./controlapp
git diff --check
```

Commit:

```bash
git add controlapp/workspace_rpc.go controlapp/workspace_rpc_test.go
git commit -m "feat: extract bounded workspace push"
```

### Task 4: Implement bounded workspace pull

**Files:**
- Modify: `controlapp/workspace_rpc.go`
- Modify: `controlapp/workspace_rpc_test.go`

**Interfaces:**
- Consumes: `workspace.MethodPullFiles`, `workspace.PullRequest`, `workspace.PullChunk`, and `control.PullWorkspace.Body`.
- Produces: `PullWorkspace` from `control.Attachments`.

- [ ] **Step 1: Write pull correlation and writer tests**

Cover unsafe path, nil writer, empty final chunk, one/multiple chunks, exact `MaxBytes`, overflow, wrong sequence, oversized single chunk, empty non-final chunk, writer short-write, writer error, duplicate final response, and context cancellation.

Use a writer that records bytes plus call count; failure messages report only synthetic byte counts and sequence numbers, never archive content or paths.

- [ ] **Step 2: Run pull tests and observe failure**

Run:

```bash
go test ./controlapp -run TestPullWorkspace -count=1
```

Expected: FAIL because `PullWorkspace` is absent.

- [ ] **Step 3: Implement sequential bounded pull**

Resolve and authorize with `ActionPull`, validate writer/path, require running, and generate a transfer ID. Starting at sequence zero, request:

```go
workspace.PullRequest{Xfer: xfer, Path: cmd.Path, Seq: seq}
```

Require the response sequence to match, `len(Data) <= workspace.ChunkBytes`, and either data or `Done`. Check `total+len(Data) <= workspace.MaxBytes` before writing. Use a loop around `Body.Write` so a permitted short write makes progress; zero bytes with nil error becomes `io.ErrShortWrite`. Stop at the first valid `Done`, record `ActionPull`, and make no extra RPC.

- [ ] **Step 4: Run the transfer and public-protocol gates**

Run:

```bash
gofmt -w controlapp/workspace_rpc.go controlapp/workspace_rpc_test.go
go test ./controlapp -run 'Test(WorkspaceDiff|PushWorkspace|PullWorkspace|SessionRPC)' -race -count=20
go test ./protocol/workspace -race -count=5
go test ./controlapp -race
go vet ./controlapp
git diff --check
```

- [ ] **Step 5: Commit pull support**

```bash
git add controlapp/workspace_rpc.go controlapp/workspace_rpc_test.go
git commit -m "feat: extract bounded workspace pull"
```

### Task 5: Prove the lane is independently consumable

**Files:**
- Create: `controlapp/attachments_external_test.go`

**Interfaces:**
- Consumes: `NewAttachmentService` and all of `control.Attachments`.
- Produces: an external-package proof that a separate module can provide transport adapters without importing Rainier internals or learning socket details.

- [ ] **Step 1: Add an external-package smoke test**

Use `package controlapp_test` with external fakes and compile:

```go
func constructAttachments(t *testing.T) control.Attachments {
	svc, err := controlapp.NewAttachmentService(externalAttachmentOptions())
	if err != nil { t.Fatal(err) }
	return svc
}
```

Call one viewer attach, empty diff, one-chunk push, and one-chunk pull through the interface. Assert only public messages and returned bytes.

Add `var _ control.Attachments = (*controlapp.AttachmentService)(nil)` here, after all four methods exist.

- [ ] **Step 2: Run dependency and full-repository gates**

Run:

```bash
if rg -n 'internal/|net/http|nhooyr|pgx|docker|github|cloud.google|billing' controlapp/attachments.go controlapp/workspace_rpc.go; then exit 1; fi
bash scripts/check-public-protocols.sh
bash scripts/check-public-control.sh
go test ./controlapp -race -count=10
make verify
git diff --check origin/main...HEAD
```

Expected: every command succeeds. No test emits terminal messages, workspace bytes, or raw sandbox payloads.

- [ ] **Step 3: Review depth and commit**

Confirm callers learn only `control.Attachments`; correlation, bounds, controller fencing, and error sanitization remain inside the module. Remove any exported RPC, transfer, or lease helper.

Commit:

```bash
git add controlapp/attachments_external_test.go
git commit -m "test: verify attachment application seam"
```

## Acceptance checklist

- `AttachmentService` implements all four `control.Attachments` methods.
- Authorization precedes broker, runner, reader, and writer effects; cross-workspace resources remain non-disclosing.
- Named-member collaboration is a policy-adapter decision, while one monotonic controller generation fences input ownership.
- Terminal messages remain opaque and untouched.
- Diff, push, and pull reject every unsafe or unbounded sandbox/client behavior and preserve exact public protocol shapes.
- No terminal or workspace content, path, credential, provider detail, or sandbox error escapes through events, errors, logs, or fixtures.
- Existing self-hosted behavior remains untouched until recomposition.

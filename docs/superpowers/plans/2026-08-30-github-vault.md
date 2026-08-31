# GitHub Connector + Credential Vault Implementation Plan (Rainier v0, Plan 5)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Environments deliver code with working git — clone-at-boot on session branches, an in-sandbox credential helper minting from a lifecycle-aware vault, the `init` hook, bounded push/pull, the diff endpoint — plus the durability riders (crash-preserves-workspace, `child_exited`) and the `--since`/`/v0/me` fixes.

**Architecture:** One new primitive carries everything: the control channel becomes a bidirectional **session RPC** (request/response with correlation, initiated from either end). Credential mint (sandbox→controld), diff and push/pull (controld→sandbox) are methods on it. The vault stores per-user provider credentials sealed under the existing secrets key, mints optimistically, and flips to `needs_refresh` on observed auth failure — never a GitHub call on the hot path.

**Tech Stack:** Go 1.25; existing deps only (`pgx/v5`, `coder/websocket`, stdlib crypto). No new dependencies.

**Spec:** `docs/superpowers/specs/2026-08-29-plan5-github-vault-design.md` (read first; §4.1's RPC contract and §4.2's vault semantics are binding). Parents: Plan 4 design (connector vocabulary), `2026-08-27-rainier-design.md` §8.

## Global Constraints

- Preserve every Plan 1–4 interface except the explicit changes named per task. `rattach`/`runnerctl`, the dev HTTP surface, and every existing test keep working after every task.
- **Wire vocabulary (exact strings).** ControlEvent kinds: existing `setup_done`/`setup_failed`/`child_exited`(new, T4) events keep `ID: 0`; requests are `req:<method>` with `ID > 0`; responses are kind `resp` echoing the request `ID` with `OK bool` and `Payload json.RawMessage`. RPC methods v0: `mint_git_credential`, `diff`, `push_files`, `pull_files`. `stage_failed` (T7) carries `Stage: "setup"|"clone"|"init"`; runnerd accepts legacy `setup_failed` as `stage_failed{Stage:"setup"}` forever (wire compat).
- rwire: `ToRunner{Type:"session_rpc"}` carries `RPC *RPCEnvelope{ID uint64; Method string; Payload json.RawMessage}`; sessiond-initiated requests surface as `FromRunner{Type:"session_req", Session, RPC}` and controld answers with `session_rpc` responses (`Method:"resp"`). Runnerd is a pure forwarder with per-hop 30s timeouts.
- Migration `0004_credentials.sql` (ONE file, T2): `credentials` table (PK `(user_id, provider)`), `environments.init text NOT NULL DEFAULT ''`, `environments.init_timeout_sec int NOT NULL DEFAULT 0`, `sessions.child_exit_code int NULL`.
- Vault: sealed with the EXISTING `seal.go` under `Config.SecretsKey`; values write-only at every API; `status ∈ {valid, needs_refresh}`; every named-action error says exactly `rainier login --refresh github`.
- Session branch: `rainier/<session-name>`, falling back to `rainier/<last 12 hex of session id>` when unnamed. Git author: `user.name = <login>`, `user.email = <github_id>+<login>@users.noreply.github.com`. `GIT_CONFIG_GLOBAL=/workspace/.rainier/gitconfig`. Helper answers ONLY host `github.com` (empty output otherwise — git falls through).
- Caps: clone timeout 600s/repo; diff 30s + 64KB per repo; push/pull 256MiB compressed total, 1MiB chunks, ack every 8 chunks; RPC payloads ≤ 2MiB (chunking, not raised limits).
- Egress auto-append when repos present: `github.com`, `codeload.github.com`, `objects.githubusercontent.com`.
- Login device flow requests scope `repo read:user`; the exchange reads `X-OAuth-Scopes` and WARNS (never fails) when `repo` is absent; scopes stored informationally.
- Envelope error codes unchanged (the nine); credential-missing at create → `409 conflict` naming the action, pre-insert like missing secrets.
- TDD; `-race` green everywhere touched; `go vet`/`CGO_ENABLED=0 go build`/`gofmt -w` clean; conventional commits.

## File Structure

```
internal/relay/frame.go, session_side.go, runnerd_side.go     RPC envelope, SendControl, inbound handler (T1)
internal/rwire/rwire.go                                        session_rpc/session_req + RPCEnvelope (T1)
internal/controld/{store.go,memstore.go,storetest,pgstore}     credentials + init + child_exit_code (T2)
internal/controld/vault.go (+_test)                            mint decision, status transitions (T3)
internal/controld/{auth.go,api.go,controld.go}                 login stores creds; /v0/credentials; routes (T3)
cmd/rainier/main.go, internal/cli                              creds, login --refresh, push/pull, diff (T3,T9)
internal/driver/{driver.go,docker.go,fake.go,contract.go}      DestroyContainer/RemoveWorkspace; RAINIER_REPOS/INIT (T4,T6)
internal/runnerd/{runnerd.go,agent.go}                         crash keeps volume; RPC forwarding both ways (T4,T5)
internal/controld/{runners.go,srpc.go}                         session-RPC pending table + routing (T5,T8)
cmd/sessiond/main.go (+ gitchain.go, helper.go, socket.go)     boot chain, clone/init stages, helper, socket (T7)
internal/controld/sched.go, api.go                             RepoSpec expansion, cred check, attribution (T6)
internal/e2e/e2e_test.go, scripts/e2e-fleet.sh, docs           scenes, gh-gated rehearsal phase, runbook (T11)
internal/attachio / cmd/rainier                                --since fix (T10)
```

Execution order strictly T1→T11. T4 is independent of T3 but keep the order (one migration lands in T2; T4 consumes its column).

---

### Task 1: Session RPC envelope — relay + rwire, both directions

**Files:**
- Modify: `internal/relay/frame.go`, `internal/relay/session_side.go`, `internal/relay/runnerd_side.go`, `internal/rwire/rwire.go`
- Test: `internal/relay/relay_test.go`, `internal/rwire/rwire_test.go` (extend)

**Interfaces:**
- Consumes: `ControlEvent{Kind,RC,Tail}` (extends), `ControlSender`/`connWriter`, `NewHubWithControl(ctx, conn, onControl)`, `ServeSessionWithControl(ctx, conn, s) (*ControlSender, <-chan error)` — all exact current signatures.
- Produces (later tasks compile against these verbatim):

```go
// relay
type ControlEvent struct {
    Kind    string          `json:"kind"`            // events: setup_done|stage_failed|child_exited (ID 0); "req:<method>"; "resp"
    ID      uint64          `json:"id,omitempty"`    // correlation; 0 = fire-and-forget event
    OK      bool            `json:"ok,omitempty"`    // resp only
    Payload json.RawMessage `json:"payload,omitempty"`
    Stage   string          `json:"stage,omitempty"` // stage_failed (T7)
    RC      int             `json:"rc,omitempty"`
    Tail    string          `json:"tail,omitempty"`
}
func (h *Hub) SendControl(payload []byte) error   // runnerd→sessiond, via the hub's conn (concurrent-safe: coder/websocket serializes writers; document)
// ServeSessionWithControl gains a constructor-wired inbound handler:
func ServeSessionWithControl(ctx context.Context, conn Conn, s *session.Session, onControl func(payload []byte)) (*ControlSender, <-chan error)
    // BREAKING for the 2 existing callers (cmd/sessiond dialLoop; relay tests) — update them in this task, nil handler = old behavior.
    // Handler runs on the read goroutine; hand off, never block (same contract as Hub's onControl).
// rwire
type RPCEnvelope struct { ID uint64 `json:"id"`; Method string `json:"method"`; Payload json.RawMessage `json:"payload,omitempty"` }
// ToRunner gains RPC *RPCEnvelope `json:"rpc,omitempty"`; Type vocabulary += "session_rpc"
// FromRunner gains RPC *RPCEnvelope `json:"rpc,omitempty"`; Type vocabulary += "session_req"
```

- [ ] **Step 1: Failing tests.** relay: `TestControlRPCRoundTripBothDirections` — hub sends `req:ping` via SendControl; the session-side handler (registered via the new parameter) replies `resp` with the same ID through its ControlSender; hub's onControl receives it; then the REVERSE: session-side sends `req:pong`, hub replies via SendControl, session handler sees the resp; THEN an attach still streams (mux intact — mirror `TestControlFramesReachHub`'s second half). rwire: round-trip `session_rpc`/`session_req` with RPCEnvelope; empty-spec omitempty pin extended to `rpc`.
- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement. SendControl writes a FrameControl through the hub's conn directly (add a doc comment citing coder/websocket's writer serialization; AttachClient already writes concurrently). ServeSessionWithControl: thread the handler into serveSession's FrameControl case (inbound frames dispatch `go handler(payload)` — hand-off documented); keep the exported `ServeSession` wrapper nil-handler. Update the two callers.
- [ ] **Step 4:** `go test ./internal/relay/ ./internal/rwire/ -race -count=5`; full suite. **Step 5:** Commit `feat: bidirectional session RPC envelope on the control channel`.

---

### Task 2: Store — credentials, init, child_exit_code (migration 0004)

**Files:**
- Modify: `internal/controld/store.go`, `memstore.go`, `storetest/contract.go`, `pgstore/pgstore.go`; Create: `pgstore/migrations/0004_credentials.sql`

**Interfaces (verbatim for T3/T4/T6/T8):**

```go
type Credential struct {
    UserID, Provider string
    Ciphertext, Nonce []byte            // sealed access token
    RefreshCiphertext, RefreshNonce []byte // nullable; unused v0
    Status string                        // "valid" | "needs_refresh"
    Scopes string
    ObtainedAt time.Time; ExpiresAt *time.Time
    LastVerifiedAt, LastUsedAt, UpdatedAt time.Time
}
// Store additions:
UpsertCredential(ctx, c Credential) error                       // by (UserID, Provider); stamps timestamps
GetCredential(ctx, userID, provider string) (Credential, error) // ErrNotFound
SetCredentialStatus(ctx, userID, provider, status string) error // ErrNotFound; bumps UpdatedAt
TouchCredentialUsed(ctx, userID, provider string) error         // last_used_at = now; ErrNotFound
ListCredentials(ctx, userID string) ([]Credential, error)       // that user's only; caller strips ciphertext for views
SetChildExitCode(ctx, id string, code int) error                // sessions; ErrNotFound
// Environment gains Init string; InitTimeoutSec int  (NOT part of SetupHash — add a contract subtest pinning that)
// Session gains ChildExitCode *int
```

Migration `0004_credentials.sql` (verbatim):

```sql
CREATE TABLE credentials (
  user_id text NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  provider text NOT NULL,
  ciphertext bytea NOT NULL,
  nonce bytea NOT NULL,
  refresh_ciphertext bytea,
  refresh_nonce bytea,
  status text NOT NULL DEFAULT 'valid',
  scopes text NOT NULL DEFAULT '',
  obtained_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz,
  last_verified_at timestamptz NOT NULL DEFAULT now(),
  last_used_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, provider)
);
ALTER TABLE environments ADD COLUMN init text NOT NULL DEFAULT '';
ALTER TABLE environments ADD COLUMN init_timeout_sec int NOT NULL DEFAULT 0;
ALTER TABLE sessions ADD COLUMN child_exit_code int;
```

- [ ] **Step 1: Contract subtests first** (complete bodies, storetest style): credential upsert/get/status/touch/list round-trip (list returns only that user's; second user invisible); status transition bumps updated_at; unknown → ErrNotFound everywhere; env Init round-trips through CRUD and `Init` change does NOT move SetupHash (the cache-identity pin); UpdateEnvironment carries Init like Setup; session ChildExitCode nil→set→persists through Get/List. Run memstore contract → FAIL.
- [ ] **Step 2:** Migration + memstore + pgstore to green (mirror existing idioms; nullable bytea/timestamptz scanning like `runners.last_seen_at`). Env API/CLI plumbing for `init` (`--init-file`, PATCH field) rides HERE too — it is two fields on existing paths (api.go createEnvironmentRequest/patch + CLI flags + one route test extension each), not worth its own task.
- [ ] **Step 3:** `go test ./internal/controld/... -race -count=1` (docker pgstore incl. a 0003→0004 upgrade check with a legacy row). Full suite. **Step 4:** Commit `feat: store — credentials vault schema, env init hook, child exit code`.

---

### Task 3: Vault behavior — login stores, /v0/credentials, creds CLI, refresh

**Files:**
- Create: `internal/controld/vault.go`, `vault_test.go`
- Modify: `internal/controld/auth.go` (exchange stores; scope header), `api.go` (+`GET /v0/credentials`), `controld.go` (route), `cmd/rainier/main.go` + `internal/cli` (`creds`, `login --refresh <provider>`), api_test/auth_test extensions

**Interfaces:**

```go
// vault.go (package controld) — pure-ish over Store + SecretsKey:
func (s *Server) storeGitHubCredential(ctx, userID, token, scopes string) error       // Seal + UpsertCredential(status valid, last_verified now)
func (s *Server) mintGitCredential(ctx, userID string) (token string, err error)      // Get → status needs_refresh → ErrCredentialNeedsRefresh; valid → Open + TouchCredentialUsed
func (s *Server) rejectCredential(ctx, userID, provider string)                        // best-effort SetCredentialStatus(needs_refresh) + log
var ErrCredentialNeedsRefresh = errors.New(`github credential needs refresh — run: rainier login --refresh github`)
var ErrCredentialMissing = errors.New(`no github credential — run: rainier login`)
```

- `POST /v0/auth/github`: after the existing verify, read `X-OAuth-Scopes` from the SAME `/user` response; `storeGitHubCredential`; response gains `"scopes"` and, when `repo` absent, `"warning": "token lacks repo scope; git operations will require rainier login --refresh github"` (CLI prints it). Device flow (`githubDeviceFlow`) requests `scope=repo read:user` (currently `read:user` — check the existing request body and change it).
- `GET /v0/credentials` (requireUser): caller's rows as `{provider, status, scopes, obtained_at, last_verified_at, last_used_at}` — NO value fields in the view type (secrets discipline).
- CLI: `rainier creds` (tabwriter PROVIDER STATUS SCOPES LAST_VERIFIED LAST_USED); `rainier login --refresh github` = the existing acquisition paths then the exchange (server upserts — no new endpoint); help text explains when to use it.

- [ ] **Step 1: Failing tests.** vault_test: mint on valid returns the token + touches last_used (memstore-backed, real Seal/Open); mint on needs_refresh → ErrCredentialNeedsRefresh (errors.Is); mint with no row → ErrCredentialMissing; rejectCredential flips status. auth_test: exchange stores a credential (assert via store; ciphertext ≠ token bytes; Open round-trips); scope warning surfaces when the fake GitHub omits repo from X-OAuth-Scopes; /v0/credentials four-kind route tests + a raw-JSON no-token-material assertion; a second user's creds invisible.
- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement (fake GitHub fixture gains the X-OAuth-Scopes header — it's `newTestControld`'s fixture; extend, reuse everywhere). **Step 4:** controld suite `-race`; cli smoke extension (`creds` renders). Full suite. **Step 5:** Commit `feat: credential vault — login stores, status lifecycle, creds CLI`.

---

### Task 4: Durability riders — crash preserves workspace; child_exited

**Files:**
- Modify: `internal/driver/driver.go`, `docker.go`, `fake.go`, `contract.go` (split destroy), `internal/runnerd/runnerd.go` (crash path + event), `cmd/sessiond/main.go` (emit child_exited), `internal/controld/runners.go` (event arm), `api.go` (sessionJSON annotation), `cmd/rainier/main.go` (ls display)

**Interfaces:**

```go
// driver.Driver changes:
Destroy(ctx, id) error                      // UNCHANGED semantics: container + workspace volume (explicit rm path)
DestroyContainer(ctx, id string) error      // NEW: container only; volume survives (crash path)
RemoveWorkspace(ctx, sessionID string) error // NEW: volume by session id (rainier-ws-<id>); tolerate absent
// runnerd crash path (register() hub-death, container gone): DestroyContainer + fireEvent(id, "dead") — volume kept.
// controld handleDeleteSession, runner-disconnected AND dead-session branches: after (or instead of) the container
// destroy dispatch, also dispatch a new ToRunner{Type:"remove_workspace", Session} (fire-and-forget ok:true even if absent).
// Reconcile's terminal-orphan destroy: FULL Destroy (duplicates, not crashes) — unchanged.
// sessiond: on <-s.Exited(), send ControlEvent{Kind:"child_exited", RC: s.ExitCode()} via the ControlSender
// (buffered/pending like the setup verdict — reuse serveConn's pending machinery: generalize it from one pending
// payload to a small FIFO (cap 8, drop-oldest with a log) since setup + child_exited can both be pending).
// runnerd routeControl: "child_exited" → fireEventDetail(id, "child_exited", strconv.Itoa(rc)).
// controld applyEvent: "child_exited" (placement-guarded) → SetChildExitCode; no state change.
// sessionJSON gains "child_exit_code" (nullable); CLI ls STATE renders `running (exited N)` when set.
```

- [ ] **Step 1: Failing tests.** Driver contract: crash-vs-rm split (`DestroyContainer` leaves the volume — fake records; docker-gated: volume listed after, gone after `RemoveWorkspace`); existing destroy-removes-volume subtest re-pinned unchanged. runnerd: crash-path test asserts DestroyContainer (not Destroy) via a recording fake + volume untouched. sessiond: unit — child-exit emits the event through a scripted conn (extend the pending-machinery tests for two queued payloads FIFO). controld: applyEvent child_exited sets the column (placement-guarded subtest); shape-pin gains the key. e2e: extend an existing scene — scripted sessiond sends child_exited(0) → ls annotation via API.
- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement across the five surfaces. **Step 4:** `-race` on driver/runnerd/controld/sessiond + full suite. **Step 5:** Commit `feat: crash preserves the workspace; child exit is visible fleet-wide`.

---

### Task 5: Session RPC plumbing end-to-end (controld ⇄ runnerd ⇄ sessiond)

**Files:**
- Create: `internal/controld/srpc.go`, `srpc_test.go`; `cmd/sessiond/socket.go` (unix listener + local protocol), `cmd/sessiond/rpc.go` (handler registry + upstream requests)
- Modify: `internal/runnerd/agent.go` + `runnerd.go` (forwarding both directions), `internal/controld/runners.go` (session_req arm), `cmd/sessiond/main.go` (wire the handler + socket into boot)

**Interfaces:**

```go
// controld/srpc.go — controld-initiated calls:
func (s *Server) sessionRPC(ctx context.Context, sessionID, method string, payload any, out any) error
    // resolves the session's runner; dispatches ToRunner{Type:"session_rpc", Session, RPC:{ID, Method, Payload}};
    // pending map[uint64]chan (own table, own atomic seq — do NOT reuse the runner-dispatch ReqID space);
    // timeout = Config.OpTimeout; conn-death fail-fast (reuse rc.done); ErrRunnerUnreachable mapping as dispatch.
    // Errors from the sandbox arrive as resp{OK:false, Payload:{"error": "..."}} → returned as a typed sandboxError.
// controld handler registry for sandbox-initiated requests (runners.go "session_req" arm):
func (s *Server) handleSessionRequest(ctx, runner string, sessionID string, env rwire.RPCEnvelope) rwire.RPCEnvelope
    // v0 methods: mint_git_credential (T8 fills the real body; THIS task lands the routing with a stub that
    // answers ok:false "unknown method" for everything — the plumbing task proves transport, not business logic).
// runnerd: agent execute "session_rpc" → hub := waitHub(session) → hub.SendControl(marshaled ControlEvent{Kind:"req:<m>", ID, Payload})
//   responses come back via routeControl: kind "resp" with an ID known to a pending session_rpc → result upstream
//   (runnerd keeps a small pending table mapping control-resp IDs → the rwire ReqID that asked; 30s TTL).
//   Sessiond-initiated: routeControl kind "req:<m>" → forward upstream as FromRunner{Type:"session_req", Session, RPC};
//   controld's answer (ToRunner{Type:"session_rpc", RPC:{ID, Method:"resp",...}}) → hub.SendControl back down. Symmetric.
// sessiond rpc.go: RegisterRPCHandler(method string, fn func(payload []byte) (any, error)) — called at boot before serving;
//   inbound req:<m> dispatches on its own goroutine, replies via ControlSender. Upstream: Call(method, payload, timeout) —
//   used by the helper path (T7); pending table with IDs from its own atomic. A pending upstream
//   call FAILS on conn death (the caller — git via the helper — retries naturally; document); only fire-and-forget
//   EVENTS ride T4's pending FIFO across reconnects. Requests are never re-sent automatically.
// sessiond socket.go: unix listener at /workspace/.rainier/agent.sock (0700; remove stale at boot); line protocol:
//   one JSON request {method, payload} per connection, one JSON response; 5s per-connection deadline.
```

- [ ] **Step 1: Failing tests.** srpc_test (controld pkg, fake runner + scripted sessiond over real websockets — reuse the e2e-style helpers already in the package's tests): controld `sessionRPC("ping")` reaches the scripted sessiond and the answer round-trips; timeout when the sandbox never answers; conn-death fail-fast; sandbox ok:false surfaces typed; two concurrent RPCs to one session correlate (out-of-order replies). Sessiond-initiated: scripted "req:echo" from the sessiond side reaches a controld-side stub and the resp lands back at the sessiond handler (assert at both ends). Runnerd pending-TTL: an orphaned resp (unknown ID) is dropped with a log, no crash. sessiond socket: unit test dials the unix socket in a t.TempDir, round-trips a request against a registered handler, asserts the deadline closes a stalled client.
- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement all four sides. **Step 4:** `-race -count=5` on controld srpc tests + runnerd + relay; full suite. **Step 5:** Commit `feat: session RPC end to end — controld to sandbox and back`.

---

### Task 6: Resolution — RepoSpec expansion, attribution, credential gate, init dispatch

**Files:**
- Modify: `internal/controld/sched.go` (createSpec), `api.go` (create accepts `repos`, credential gate, egress append), `internal/rwire/rwire.go` (Spec.Repos/Init/InitTimeoutSec/GitAuthorName/GitAuthorEmail), `internal/driver/driver.go`+`docker.go`+`fake.go` (Spec.Repos/Init/GitAuthorName/GitAuthorEmail → `RAINIER_REPOS_B64` (json array), `RAINIER_INIT_B64`/`RAINIER_INIT_TIMEOUT`, `RAINIER_GIT_AUTHOR_NAME`/`RAINIER_GIT_AUTHOR_EMAIL` injection; ALL stripped at snapshot — extend `driverEnvKeys` and its committed-config test), agent.go (rwire→driver mapping)

**Interfaces:**

```go
// rwire + driver (same shape both layers):
type RepoSpec struct {
    Owner string `json:"owner"`; Name string `json:"name"`
    BaseBranch string `json:"base_branch"`; SessionBranch string `json:"session_branch"`
    Dir string `json:"dir"`
}
// rwire.Spec += Repos []RepoSpec, Init string, InitTimeoutSec int, GitAuthorName, GitAuthorEmail string (all omitempty)
// POST /v0/sessions body += `repos: [{repo:"owner/name", base_branch?}]` — overrides env github connectors; explicit [] = none
//   (pointer-slice semantics exactly like egress_allow's nil-vs-empty).
// Resolution rules (createSpec/handleCreateSession):
//   repos = session override if non-nil, else env's github connectors (decode stored Raw via the T4-Plan4 strict decoder);
//   SessionBranch = "rainier/"+name or "rainier/"+id[len-12:]; Dir = Name (dedupe: second same-Name gets Owner__Name);
//   creation gate: repos non-empty → owner must HAVE a github credential row (any status) else 409 naming `rainier login`
//     (needs_refresh passes the gate — the clone fails with the named refresh action instead; document why: a stale cred
//      is refreshable mid-flight, a missing one never is);
//   attribution from users row (login + github_id); egress append (the three hosts) deduped;
//   Init/InitTimeoutSec (0⇒900) dispatched on EVERY create (cache-hit included) when env.Init != "".
```

- [ ] **Step 1: Failing tests.** api/sched: create-with-connector dispatches Repos+attribution+appended egress (fake runner captures); session `repos` override wins; `[]` → no Repos; unnamed session branch fallback; dedupe; gate-409 pre-insert (store empty) for missing cred; needs_refresh passes the gate; init dispatched on cache-hit (extend the cache-hit scene from Plan 4's tests); shape pins updated. Driver: env injection docker-gated assertions + strip-list extension (`RAINIER_REPOS_B64`/`RAINIER_INIT_B64`/`RAINIER_INIT_TIMEOUT` absent from a committed config). Agent: mapping test rwire→driver.
- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement. **Step 4:** Suites + full. **Step 5:** Commit `feat: sessions resolve repos, attribution, and the init hook`.

---

### Task 7: sessiond — git boot chain and the credential helper

**Files:**
- Create: `cmd/sessiond/gitchain.go` (+_test), `cmd/sessiond/helper.go` (+_test)
- Modify: `cmd/sessiond/main.go` (boot order; stage_failed), `internal/relay/frame.go` ONLY if a field is missing (T1 defined Stage)

**Interfaces / contract (binding):**

```
Boot order (dial mode): workspace dirs → write gitconfig (when Repos or Init present) → setup wrapper (existing, unchanged,
pre-clone) → CLONE stage → INIT stage → exec agent. Implemented by EXTENDING the wrapper composition: the T8-Plan4 wrapper
becomes a staged chain script (a Go const, unit-tested exactly like setupWrapperFmt) that: runs setup.sh if present (rc file
+ gate, as today); then runs /workspace/.rainier/clones.sh if present (written by sessiond from RAINIER_REPOS_B64: per repo
`git clone --branch <base> -- https://github.com/<o>/<n>.git <dir> && git -C <dir> checkout -b <session-branch>`, echoing
stage markers); then init.sh if present (same rc-file pattern, rc → /workspace/.rainier/init.rc); each stage's failure rc
file names the stage; only full success execs the agent. The watcher (existing) generalizes: it watches an ordered list of
(stage, rcPath) pairs and reports ControlEvent{Kind:"stage_failed", Stage, RC, Tail} on the first failure, or the existing
setup_done semantics for the setup stage (cache orchestration unchanged — clone/init failures never reach snapshotting
because setup_done still fires only on setup rc 0 and caching keys on it alone; document this in the watcher comment).
gitconfig (written at boot, before any git runs):
  [credential] helper = /usr/local/bin/sessiond git-credential-helper
  [user] name / email from RAINIER_GIT_AUTHOR_NAME/EMAIL (driver injects from Spec — add to T6's injection + strip list)
  [push] default = current
  exported as GIT_CONFIG_GLOBAL=/workspace/.rainier/gitconfig in the child chain env.
helper.go: `sessiond git-credential-helper get` — reads git's key=value stdin; host != "github.com" or op != get → exit 0
  no output; else dial the unix socket, Call("mint_git_credential", {}) with 20s timeout; on ok print
  "username=x-access-token\npassword=<token>\n" (the conventional token-auth username; GitHub accepts any non-empty
  username with a token password — pin the exact two lines in the helper test); on refusal print NOTHING and exit 1 with the server's message on stderr
  (git surfaces stderr — the named action reaches the user's terminal). On credential_rejected detection: helper does NOT
  detect; sessiond watches git stage failures? NO — detection is T8's controld-side job via a second RPC: after a clone
  stage failure whose tail matches /authentication failed|401|403/i, sessiond fires event ControlEvent{Kind:
  "credential_rejected"} (ID 0) — one line in the watcher; controld flips status (T8).
```

- [ ] **Step 1: Failing tests.** gitchain: chain-script composition table (setup+clones+init permutations — exact argv/const, quoting-hostile session branch names); real /bin/sh execution in t.TempDir with stub git (a PATH-shimmed fake `git` script recording calls) proving order, rc-file semantics per stage, and exec-through on success; gitconfig content golden test. helper: protocol table (get/store/erase; github/other hosts) against a stub socket server; refusal path prints stderr and exits 1; timeout path. main: boot composes the chain only when the envs are present (absent = byte-identical to Plan 4 behavior — pin with the existing wrapper tests unmodified).
- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement. **Step 4:** `go test ./cmd/sessiond -race -count=5`; full suite. **Step 5:** Commit `feat: sessiond git boot chain — clone and init stages, credential helper`.

---

### Task 8: controld — mint handler, credential_rejected, stage_failed arms

**Files:**
- Modify: `internal/controld/srpc.go` (mint method), `runners.go` (credential_rejected + stage_failed event arms), tests

**Contract:**
- `handleSessionRequest("mint_git_credential")`: session row → owner → `mintGitCredential` (T3); ok → `{token}` payload; ErrCredentialNeedsRefresh/Missing → ok:false with the exact named-action message (assert verbatim).
- Event `credential_rejected` (placement-guarded): `rejectCredential(owner, "github")` — best-effort, logged.
- Event `stage_failed{stage, rc, tail}` (placement-guarded): `Transition(from creating/running → failed, Error: "<stage> failed: rc N: <tail>" prefix-composed exactly like setup)` — the legacy setup_failed arm delegates here with stage "setup". `setup_done` orchestration untouched.

- [ ] **Step 1: Failing tests** (fake runner + scripted sessiond via T5 harness): mint round-trip returns the sealed-then-opened token to the sandbox side; needs_refresh mint → ok:false with the verbatim message; mint touches last_used (store assert); credential_rejected flips status and the NEXT mint refuses; stage_failed{clone} → failed with composed error; legacy setup_failed still lands (wire-compat pin). `-count=5`.
- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement. **Step 4:** Suites + full. **Step 5:** Commit `feat: credential minting over session RPC, lazy revocation, staged failures`.

---

### Task 9: diff endpoint + push/pull

**Files:**
- Modify: `internal/controld/api.go` (+`GET /v0/sessions/{id}/diff`, `POST /v0/sessions/{id}/files` upload + `GET .../files?path=` download — chunked RPC bridging), `srpc.go` (methods), `cmd/sessiond/rpc.go` (diff/push/pull handlers), `cmd/rainier/main.go` + `internal/cli` (`push`/`pull`/`diff` subcommands)

**Contract:**
- Diff: REST → `sessionRPC("diff")` → sessiond per repo: `git -C <dir> fetch -q origin <base>` then `git -C <dir> diff --stat origin/<base>...HEAD`, 30s+64KB caps per repo → `{"repos":[{repo, base_branch, session_branch, stat}]}`; no repos → `{"repos":[]}`; session not running → 503 session_not_ready. CLI `rainier diff <session>` renders.
- push: CLI tars+gzips locally (cap 256MiB compressed, refuse over with the size named), streams `push_files` chunks {Xfer string(id), Seq, Data ≤1MiB b64, Done bool} as sequential sessionRPCs (server acks each; every 8th carries `synced:true` after sessiond fsyncs); sessiond assembles under `/tmp` then untars into the validated destination (must resolve under `/workspace`; reject `..`/absolute-outside; partial failure removes the staging file, never half-extracts). pull mirrors (sessiond tars, controld streams chunks back as the REST response body — `Content-Type: application/gzip`, streamed per chunk). REST: multipart-free — the CLI talks the chunk protocol over plain sequential JSON requests; document the deliberate v0 crudeness + the attach-plane streaming upgrade path in a code comment.
- [ ] **Step 1: Failing tests.** sessiond handlers against a stub-git PATH shim (diff composition; push path-escape refusals; pull tar round-trip in t.TempDir); controld REST tests with scripted sessiond (four-kind per route + a full 3MiB round-trip both directions asserting byte-equality and chunk acks); CLI unit for tar cap + progress; cli smoke extension (push a dir into the in-process stack's scripted sessiond, pull it back, byte-compare).
- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement. **Step 4:** Suites (`-race`), full. **Step 5:** Commit `feat: diff endpoint and bounded push/pull`.

---

### Task 10: Riders B — --since replay fix, /v0/me id, owner-preference

**Files:**
- Investigate + Modify: the `--since` path (`cmd/rainier/main.go` attach URL building, `internal/attachio`, `internal/controld/attach.go` since forwarding into dial_attach — find the actual break: acceptance showed the server log intact but the viewer receiving screen-only); Modify: `internal/controld/auth.go` (userView + id), `cmd/rainier/main.go` (owner-preference reads /v0/me at login; drop the Config.OwnerID cache-from-new)

- [ ] **Step 1: Reproduce first** — an e2e scene: session with 50 scripted output events → detach → reattach `--since 0` → assert all 50 arrive (this test FAILS today; it is the bug's pin). Diagnose from the failing test (the suspects, in order: CLI not putting since into the ws URL; controld's attach handler not parsing it; dial_attach not carrying it; sessiond's Attach(since) fine — Plan 1 tests prove it).
- [ ] **Step 2:** Fix minimally at the actual break; scene green `-count=5`. **Step 3:** `/v0/me` gains `"id"`; shape pins updated; CLI login stores it; `resolveSessionID` owner-preference uses it (delete the `new`-response cache path + its limitation doc); ambiguity tests updated.
- [ ] **Step 4:** Full suite. **Step 5:** Commit `fix: full-history replay reaches the viewer; owner id from /v0/me`.

---

### Task 11: e2e scenes, rehearsal phase, docs, acceptance

**Files:**
- Modify: `internal/e2e/e2e_test.go`, `scripts/e2e-fleet.sh`, `docs/deploy-gce.md`, `README.md`

- [ ] **Step 1: e2e scenes** (fake driver + scripted sessiond speaking the T5 RPC): `TestConnectorSessionMintsAndReportsDiff` (create-with-connector → Repos in spec → scripted mint round-trip → scripted diff answers → REST diff renders it); `TestCredentialLifecycle` (login stores → mint ok → credential_rejected flips status → the creation gate still passes (any-status rule) but the next MINT refuses with the verbatim named action → `--refresh` re-exchange → mint ok again); `TestStageFailedClone` (clone stage fails w/ auth-ish tail → session failed + credential flipped); `TestPushPullRoundTrip`; `TestChildExitAnnotation` (if not landed in T4's scene); since-replay scene from T10 stays. `-race -count=5` stable.
- [ ] **Step 2: Rehearsal phase** (`e2e-fleet.sh`, gated on `gh auth token` presence — skip loudly without): `gh repo create rainier-e2e-scratch-$$ --private` → env with github connector (+init writing a marker + `git log` assertion script) → `rainier new --env` → attach: assert clone on `rainier/<name>`, commit+push works, author name/email match the noreply convention (`git log --format='%an %ae'`), `rainier diff` shows the stat, `rainier creds` valid; push/pull a directory; teardown `gh repo delete --yes`. RUN LIVE (RUNNERD_PORT=8081 PG_PORT=5434), transcript in the report.
- [ ] **Step 3: Docs.** deploy-gce.md: credentials section (login scopes, `rainier creds`, refresh flow), init-hook + idempotency note extended to clone/init, the Plan 5 acceptance table (design §1 criteria 1–9) in the notes section; README quickstart gains the connector flow. Setup-vs-init guidance (`setup` = cacheable toolchain pre-clone; `init` = per-session post-clone).
- [ ] **Step 4:** Full `go test ./... -race`; commits split (scenes; rehearsal+docs). **Step 5 (with Josh):** GCE acceptance — criteria 1–9 on rainier-1, recorded.

---

## Coverage ledger (self-review against the design)

- §4.1 RPC primitive both directions + correlation at edges → T1 (envelope), T5 (plumbing; separate ID spaces per initiator; runnerd pure-forwarder with TTL'd pending). §4.2 vault (schema T2, behavior T3, optimistic mint + lazy flip T3/T8, scope upgrade + warning T3). §4.3 connector execution (resolution/attribution/egress/gate T6; boot chain/gitconfig/helper T7; branch + dedupe rules T6/T7). §4.4 init (schema T2, dispatch T6, execution T7, every-session-incl-cache-hit pinned T6). §4.5 push/pull caps + path safety → T9. §4.6 diff → T9. §4.7 riders → T4 (crash volume + child_exited), T10 (--since, /v0/me). §5 edge cases: no-credential gate (T6, needs_refresh-passes documented), revoked-mid-clone (T7 detection + T8 flip + e2e), helper non-github fallthrough (T7), path escapes (T9), RPC-vs-PTY contention (T1's shared writer + T9's ack pacing), admin-as-owner note (docs T11). §7 verification map → tasks as listed; rehearsal real-git phase T11.
- Deferred per design (do NOT build): auto-push (P6), App mode, GitLab abstraction, PR API, streaming transfer, per-repo minting.
- Type consistency check (done): `RPCEnvelope` identical T1/T5/T8; `RepoSpec` identical rwire/driver T6/T7; `Credential` fields T2/T3; event kinds/stage strings T1/T4/T7/T8; named-action strings defined ONCE (vault.go errors) and asserted verbatim in T3/T8/e2e.
- Placeholder scan (done): none of the banned patterns; canonical-file references name exact existing files.

# Environments Implementation Plan (Rainier v0, Plan 4)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Environments as the primary reusable object — image + setup script with snapshot caching, `/workspace` session volumes, flat team secrets, placement hints, and the typed connector vocabulary reserved for Plans 5–7.

**Architecture:** An `environments` table + CRUD surface; session create resolves an environment into a concrete spec (image/egress/secrets/placement); the first session on an env runs its setup script streamed through the ordinary terminal path and reports the outcome over a new **control frame** on the existing sessiond relay connection (the same channel Plan 5's credential helper will reuse); controld snapshots the result into a content-addressed per-runner cache that later creates boot from.

**Tech Stack:** Go 1.25; existing deps only (`pgx/v5`, `coder/websocket`, stdlib `crypto/cipher` AES-256-GCM for secrets). No new dependencies.

**Spec:** `docs/superpowers/specs/2026-08-29-plan4-environments-design.md` (this plan argues from it — read it first). Parent: `docs/superpowers/specs/2026-08-27-rainier-design.md` §9.

## Global Constraints

- Preserve every Plan 1–3 interface except the explicit changes this plan names (`driver.Snapshot` gains a ref parameter; `driver.Driver` gains `Prepull`; `driver.Spec` gains `Setup`, `SetupTimeoutSec`, `Env`; `rwire` gains the same Spec fields plus a `prepull` command and two event states; `relay` gains `FrameControl`). `rattach`/`runnerctl` keep working against the dev surface after every task.
- IDs: environments `env_` + 32 lowercase hex (16 random bytes; reuse `controld.randHex`). Env names unique, non-empty, `[a-z0-9-]{1,64}`.
- Snapshot refs are content-addressed exactly: `rainier-env:<env_id>-<first 12 hex of setup_hash>`; `setup_hash` = sha256 hex of `image + "\x00" + setup`.
- Error envelope codes stay exactly the nine from Plan 3 (`invalid_request`, `unauthenticated`, `forbidden`, `not_found`, `conflict`, `no_capacity`, `session_not_ready`, `runner_unreachable`, `internal`). Env-delete-while-referenced and secrets-related refusals use `conflict`.
- `RAINIER_SECRETS_KEY` (64 hex chars = 32 bytes) becomes a REQUIRED controld config, validated at startup like `--runner-token` (fail closed, loud). Secret values are write-only at the API — never returned, never logged.
- Setup runs as the session user (uid 1000) inside the sandbox; default timeout 15 minutes (`environments.setup_timeout_sec`, 0 ⇒ 900).
- Env edits never mutate running sessions: sessions persist `resolved_image` at create.
- Session states, authZ rules (mutations owner-or-admin; env + secrets mutations admin-only; reads team-visible), TDD, `-race` green, `go vet`/`CGO_ENABLED=0 go build`/`gofmt -w` clean, conventional commits — all per Plan 3's conventions.
- Connector vocabulary (validated, stored, NOT executed in this plan): `github{repo, base_branch}`, `files{paths[]}`, `tunnel{name, target_host, target_port}`, `browser{tier: "dedicated"|"extension"}`. Unknown types or unknown fields → 400 `invalid_request`.

## File Structure

```
internal/relay/frame.go, session_side.go, runnerd_side.go   FrameControl + SendControl + Hub.OnControl (Task 1)
internal/rwire/rwire.go                                      Spec{Setup,SetupTimeoutSec,Env}; "prepull"; event states (Task 1)
internal/controld/store.go, memstore.go                      Environment/SecretMeta types, Store additions (Task 2)
internal/controld/storetest/contract.go                      env/secrets/guarded-snapshot subtests (Task 2)
internal/controld/pgstore/{pgstore.go,migrations/0002_environments.sql}  (Task 2)
internal/controld/seal.go                                    AES-256-GCM Seal/Open (Task 3)
internal/controld/api.go, controld.go                        secrets + environments routes; SecretsKey config (Tasks 3–4)
internal/cli/…, cmd/rainier/main.go                          env + secret subcommands; --env on new (Tasks 3–4, 7)
internal/driver/{driver.go,docker.go,fake.go,contract.go}    volume, Env injection, Snapshot(ref), Prepull (Tasks 5–6)
internal/runnerd/{runnerd.go,agent.go}                       control-frame routing, snapshot ref pass-through, prepull (Tasks 6, 8)
cmd/sessiond/main.go                                         setup wrapper + outcome watcher (Task 8)
internal/controld/{sched.go,runners.go,api.go}               env resolution, placement pin, snapshot orchestration (Tasks 7, 9)
internal/e2e/e2e_test.go                                     environment scenes (Task 10)
scripts/e2e-fleet.sh, docs/deploy-gce.md, README.md          (Task 10)
```

Execution order is Task 1 → 10, strictly sequential (each consumes the previous).

---

### Task 1: Protocol groundwork — `FrameControl` and the rwire additions

**Files:**
- Modify: `internal/relay/frame.go`, `internal/relay/session_side.go`, `internal/relay/runnerd_side.go`
- Modify: `internal/rwire/rwire.go`
- Test: `internal/relay/relay_test.go` (extend), `internal/rwire/rwire_test.go` (extend)

**Interfaces:**
- Consumes: the existing relay mux (`Frame{Type,AttachID,Since,Cols,Rows,Payload}`, `ServeSession`, `Hub`) and `rwire` vocabulary.
- Produces:

```go
// relay
const FrameControl FrameType = 4 // after FrameServer; explicit value, wire-stable
// Session side: a way for sessiond to emit control events upstream.
// ServeSession gains an optional outbound queue: NewControlSender wraps the
// conn used by ServeSession; safe for concurrent use with it (single writer:
// sender owns a mutex shared with ServeSession's write path — see Step 3).
type ControlSender struct{ /* conn Conn + *sync.Mutex shared with ServeSession */ }
func ServeSessionWithControl(ctx context.Context, conn Conn, s *session.Session) (*ControlSender, <-chan error)
    // starts ServeSession in a goroutine; returned channel yields its error exactly once.
func (c *ControlSender) Send(payload []byte) error // wraps payload in FrameControl, AttachID 0
// Runnerd side:
// Hub gains: OnControl func(payload []byte)   — set before traffic; called from readLoop (must not block)
// rwire
// Spec gains: Setup string `json:"setup,omitempty"`; SetupTimeoutSec int `json:"setup_timeout_sec,omitempty"`; Env map[string]string `json:"env,omitempty"`
// ToRunner.Type gains "prepull"; ToRunner gains Ref string `json:"ref,omitempty"`
// FromRunner event State vocabulary grows: "setup_done", "setup_failed" (Detail carries the failure tail)
```

- [ ] **Step 1: Failing relay test.** In `relay_test.go` (mirror the file's existing in-memory-pipe pattern — read its `pipeConn` helper first): `TestControlFramesReachHub` — build a Hub over one end, `ServeSessionWithControl` over the other with a scripted `session.Session`? No — control frames don't need a live session: use `ServeSessionWithControl` with the test's existing minimal session fixture (the file already constructs sessions for ServeSession tests). Set `hub.OnControl` to capture payloads into a channel; call `sender.Send([]byte(`{"kind":"setup_done"}`))`; assert the exact payload arrives; assert terminal frames still flow (open an attachment through the hub afterwards and echo one stdin round — pinning that the shared-writer change didn't break the mux).
- [ ] **Step 2: Run to verify fail** — `go test ./internal/relay/ -run TestControlFrames` → FAIL (undefined).
- [ ] **Step 3: Implement.** `FrameControl` const with explicit `= 4`. Session side: today `ServeSession` writes via a local `write` closure; extract the conn-write behind a `*sync.Mutex` owned by a small struct both `ServeSession`'s writes and `ControlSender.Send` go through (one writer discipline preserved — the relay's docs comment explains why). `ServeSessionWithControl` composes: create the shared-mutex writer, run `ServeSession` (refactored internally to accept it; the EXPORTED `ServeSession(ctx, conn, s)` keeps its exact signature and behavior by wrapping) in a goroutine, return sender + error channel. Hub side: `readLoop` gets `case FrameControl:` → `if h.OnControl != nil { h.OnControl(f.Payload) }` (document: called on the read goroutine; handlers must hand off, not block — same contract as OnEvent). rwire: add the fields/constants verbatim from the Interfaces block; extend the round-trip test to cover `Spec.Env` and `Ref`.
- [ ] **Step 4: Run to verify pass** — `go test ./internal/relay/ ./internal/rwire/ -race`; then full `go test ./...`.
- [ ] **Step 5: Commit** — `git commit -m "feat: relay control frames and rwire environment vocabulary"`

---

### Task 2: Store — environments, secrets, session columns

**Files:**
- Modify: `internal/controld/store.go`, `internal/controld/memstore.go`, `internal/controld/storetest/contract.go`
- Modify: `internal/controld/pgstore/pgstore.go`; Create: `internal/controld/pgstore/migrations/0002_environments.sql`
- Test: existing contract runners (memstore + pgstore) pick the new subtests up automatically.

**Interfaces:**
- Consumes: Plan 3's Store conventions — guarded updates, ErrNotFound/ErrConflict, copies-not-pointers, `randHex`. Read `store.go` + `memstore.go` + `pgstore.go` before coding; mirror their idioms exactly.
- Produces (exact signatures later tasks compile against):

```go
type Environment struct {
    ID, Name, Image, Setup string
    SetupHash              string            // sha256(image+"\x00"+setup), maintained by the store on create/update
    EgressAllow            []string
    SecretRefs             []string
    Connectors             []Connector       // opaque here; validated at the API (Task 4)
    Placement              string            // runner name or ""
    SetupTimeoutSec        int
    SnapshotRef            string            // "" until cached
    SnapshotRunner         string            // runner that built the cache
    SnapshotHash           string            // setup_hash the snapshot was built from
    CreatedAt, UpdatedAt   time.Time
}
type Connector struct {
    Type string          `json:"type"`
    Raw  json.RawMessage `json:"-"` // full original object, stored verbatim
}
// (stored as the raw jsonb array; Connector is the decoded envelope — see Task 4 for validation)
type SecretMeta struct{ Name string; CreatedAt, UpdatedAt time.Time }

func NewEnvironmentID() string // "env_"+randHex(16)
func SetupHash(image, setup string) string

// Store interface additions:
CreateEnvironment(ctx, e Environment) (Environment, error)          // ErrConflict on name
GetEnvironment(ctx, id string) (Environment, error)
GetEnvironmentByName(ctx, name string) (Environment, error)
ListEnvironments(ctx) ([]Environment, error)                        // name asc; envs are few, no pagination
UpdateEnvironment(ctx, e Environment) (Environment, error)          // by ID; recomputes SetupHash; ErrNotFound
DeleteEnvironment(ctx, id string) error
CountSessionsByEnvironment(ctx, envID string, states []SessionState) (int, error)
SetEnvironmentSnapshot(ctx, envID, expectHash, ref, runner string) error
    // guarded: UPDATE ... WHERE id=$1 AND setup_hash=$2; 0 rows → ErrConflict (stale snapshot must not land)
PutSecret(ctx, name string, ciphertext, nonce []byte) error         // upsert
ListSecrets(ctx) ([]SecretMeta, error)
GetSecret(ctx, name string) (ciphertext, nonce []byte, err error)   // ErrNotFound
DeleteSecret(ctx, name string) error
// Session struct gains: EnvironmentID string; ResolvedImage string  (both "" for scratch)
// CreateSession persists them; sessionJSON exposure comes in Task 7.
```

- [ ] **Step 1: Contract-suite subtests first** (they define the semantics; write them complete, in `storetest/contract.go`, following its existing style): `environment CRUD and name uniqueness` (create → get by id + name; duplicate name → ErrConflict; update recomputes SetupHash — assert it changes when setup changes and not when only egress changes); `guarded snapshot update` (set with matching hash succeeds; then update env (hash changes); set with the OLD hash → ErrConflict and fields unchanged); `secrets round trip` (put → get bytes equal; list shows meta only; delete → ErrNotFound; put again upserts with new UpdatedAt); `count sessions by environment` (two sessions on env, one terminal → count(NonTerminal)==1); `session env columns persist` (CreateSession with EnvironmentID+ResolvedImage → GetSession returns them).
- [ ] **Step 2: Run to verify fail** — `go test ./internal/controld/ -run TestMemStoreContract` → FAIL.
- [ ] **Step 3: Migration** `0002_environments.sql`:

```sql
CREATE TABLE environments (
  id text PRIMARY KEY,
  name text UNIQUE NOT NULL,
  image text NOT NULL,
  setup text NOT NULL DEFAULT '',
  setup_hash text NOT NULL,
  egress_allow jsonb NOT NULL DEFAULT '[]',
  secret_refs jsonb NOT NULL DEFAULT '[]',
  connectors jsonb NOT NULL DEFAULT '[]',
  placement text NOT NULL DEFAULT '',
  setup_timeout_sec int NOT NULL DEFAULT 0,
  snapshot_ref text NOT NULL DEFAULT '',
  snapshot_runner text NOT NULL DEFAULT '',
  snapshot_hash text NOT NULL DEFAULT '',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE secrets (
  name text PRIMARY KEY,
  ciphertext bytea NOT NULL,
  nonce bytea NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE sessions ADD COLUMN environment_id text NOT NULL DEFAULT '';
ALTER TABLE sessions ADD COLUMN resolved_image text NOT NULL DEFAULT '';
```

- [ ] **Step 4: Implement memstore then pgstore** to green (memstore first — it pins semantics fast; pgstore reuses the migration runner from 0001; connectors/egress/secret_refs marshal like `sessions.cmd` does today). `SetEnvironmentSnapshot` in pgstore is one guarded UPDATE; in memstore a locked compare-and-set.
- [ ] **Step 5: Full store suites** — `go test ./internal/controld/... -race -count=1` (pgstore runs against docker postgres as in Plan 3).
- [ ] **Step 6: Commit** — `git commit -m "feat: store — environments, secrets, session env columns"`

---

### Task 3: Secrets — AES-GCM sealing, API, CLI, required key

**Files:**
- Create: `internal/controld/seal.go`; Test: `internal/controld/seal_test.go`
- Modify: `internal/controld/controld.go` (Config.SecretsKey + validation), `internal/controld/api.go` (routes), `cmd/controld/main.go` (flag/env), `cmd/rainier/main.go` + `internal/cli` (secret subcommands)
- Test: `internal/controld/api_test.go` (extend, following its table style)

**Interfaces:**
- Consumes: Store secrets methods (Task 2), `writeErr/writeJSON`, `requireUser`, admin checks (`api.go` has the established authorizeAdmin-style pattern — reuse whatever helper exists; if only owner-or-admin exists, add `requireAdmin` beside `requireUser` following its shape).
- Produces:

```go
// seal.go — package controld
func ParseSecretsKey(hexKey string) ([32]byte, error)                  // exactly 64 hex chars
func Seal(key [32]byte, plaintext []byte) (ciphertext, nonce []byte, err error)  // AES-256-GCM, fresh 12-byte crypto/rand nonce per call
func Open(key [32]byte, ciphertext, nonce []byte) ([]byte, error)
// Config gains SecretsKey [32]byte; New() errors when zero (fail closed, message names RAINIER_SECRETS_KEY).
// Routes: PUT /v0/secrets/{name} (admin; body {"value":"..."} ≤64KB, name [A-Z0-9_]{1,64}) → 204
//         GET /v0/secrets (auth'd) → {"secrets":[{name,created_at,updated_at}]}
//         DELETE /v0/secrets/{name} (admin) → 204; unknown → 404
// CLI: rainier secret set <name> [--value v | reads stdin], secret ls, secret rm <name>
```

- [ ] **Step 1: Failing seal tests** — round-trip; tampered ciphertext → error; tampered nonce → error; two Seals of the same plaintext produce different ciphertexts (nonce freshness); ParseSecretsKey rejects short/odd/non-hex.
- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement seal.go with stdlib `crypto/aes` + `cipher.NewGCM`. Run → PASS.
- [ ] **Step 4: Failing API tests** — per Plan 3's four-kind convention: happy PUT+GET-list+DELETE; validation (bad name, oversized body, unknown field) → 400; authZ (member PUT → 403); shape pin on the list. Plus: GET list never contains a `value` key (assert on raw JSON). And `TestNewRequiresSecretsKey` (Config without key → error).
- [ ] **Step 5:** Implement routes + Config + `cmd/controld` (`--secrets-key`/`RAINIER_SECRETS_KEY`; startup log says "secrets: enabled") + CLI subcommands (client.Do conventions; `secret set` reads stdin when `--value` absent so values stay out of shell history — say so in help text). Run full controld suite `-race`.
- [ ] **Step 6: Commit** — `git commit -m "feat: team secrets — AES-GCM at rest, admin API, CLI"`

---

### Task 4: Environments — API, connector validation, CLI

**Files:**
- Modify: `internal/controld/api.go` (+routes, connector validation), `internal/controld/controld.go` (route registration), `cmd/rainier/main.go` + `internal/cli` (env subcommands)
- Test: `internal/controld/api_test.go` (extend)

**Interfaces:**
- Consumes: Store env methods (Task 2), requireAdmin (Task 3).
- Produces: routes + `validateConnectors(raw json.RawMessage) ([]Connector, error)` used again by Task 7's create path; CLI `rainier env create|ls|show|update|rm`.

Route contract (each row = tests per the four-kind convention):

| Route | Behavior |
|---|---|
| `POST /v0/environments` | admin. Body `{name, image, setup?, egress_allow?, secret_refs?, connectors?, placement?, setup_timeout_sec?}`; name regex + image non-empty; secret_refs must all exist (else 400 naming the missing one); connectors validated (below); → 201 + Location + `{"environment":{...}}` (snapshot fields included, empty). |
| `GET /v0/environments` | auth'd → `{"environments":[...]}` name asc. |
| `GET /v0/environments/{id}` | id or name lookup (ids are `env_`-prefixed — same disambiguation trick as the CLI's session refs) → 200/404. |
| `PATCH /v0/environments/{id}` | admin; partial update of the POST fields; changing image/setup recomputes hash (store does it) — response shows `snapshot_ref` still set but STALE is fine (resolution compares hashes; Task 7). 200. |
| `DELETE /v0/environments/{id}` | admin; `CountSessionsByEnvironment(NonTerminal) > 0` → 409 conflict naming the count; else 204. |

Connector validation — exact v0 rules, `DisallowUnknownFields` per type:

```go
// api.go
type githubConnector struct{ Type, Repo, BaseBranch string }   // repo "owner/name" regexp `^[\w.-]+/[\w.-]+$`; base_branch default "main"
type filesConnector struct{ Type string; Paths []string }       // ≥1 path, each non-empty
type tunnelConnector struct{ Type, Name, TargetHost string; TargetPort int } // port 1..65535
type browserConnector struct{ Type, Tier string }               // tier ∈ {"dedicated","extension"}
// validateConnectors: decode the array loosely to grab each "type"; per type re-decode strictly
// (json.Decoder + DisallowUnknownFields on the element bytes); unknown type → error naming it.
// Returns []Connector{Type, Raw} for storage. A comment states these are VOCABULARY ONLY in
// Plan 4 — Plans 5–7 give them behavior — so reviewers don't hunt for missing execution paths.
```

- [ ] **Step 1: Failing API tests** — the table above (happy/validation/authZ/shape per route) plus connector cases: valid one-of-each accepted and round-tripped verbatim in the response; `{"type":"gitlab"}` → 400 naming gitlab; `{"type":"github","repo":"x/y","extra":1}` → 400; bad tier/port/paths → 400; missing secret_ref → 400 naming it.
- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement (env handlers mirror the sessions handlers' structure; keep the prologue helper pattern the final review asked us not to grow — reuse `writeCurrentSession`-style shapes where sensible). **Step 4:** Run controld suite `-race` → PASS.
- [ ] **Step 5: CLI** — `env create --image ... --setup-file ./setup.sh --egress a,b --secret-ref X --placement rainier-1 --connector-json '<raw json>'` (v0: connectors passed as raw JSON; friendlier flags come with the connectors that work), `env ls` (tabwriter NAME ID IMAGE CACHED — CACHED = `yes` when snapshot_hash==setup_hash), `env show <ref>` (pretty JSON), `env update` (same flags, only-provided-fields patch), `env rm`. `--from-devcontainer [dir]`: read `.devcontainer/devcontainer.json` (or `devcontainer.json`), take `.image` as `--image` default, print exactly which keys were present-but-ignored. Smoke these against the in-process stack in `internal/cli/client_test.go` (extend the existing smoke) for create/ls/show.
- [ ] **Step 6:** Full `go test ./... -race`; commit — `git commit -m "feat: environments — CRUD API, connector vocabulary, CLI"`

---

### Task 5: Driver — `/workspace` session volume + `Spec.Env` injection

**Files:**
- Modify: `internal/driver/driver.go` (Spec.Env), `internal/driver/docker.go`, `internal/driver/fake.go`, `internal/driver/contract.go`
- Test: contract suite (runs on fake + docker), `internal/driver/docker_test.go` for docker-gated specifics

**Interfaces:**
- Consumes: current `Docker.Create` args assembly (read it; Task 13 of Plan 3 reshaped proxy env there).
- Produces: every session container gets a named volume `rainier-ws-<SessionID>` mounted rw at `/workspace`, workdir `/workspace`; `Destroy` removes the volume (`docker volume rm`, tolerate absent); `Spec.Env map[string]string` injected as `-e K=V` (after proxy env; deterministic sorted order for testability). Fake driver records volumes + env per session and simulates cold-park persistence.

- [ ] **Step 1: Extend the contract suite** with `workspace volume survives cold park` (create with a probe: for the DOCKER driver this is an exec-less check — instead assert via the driver surface: `Suspend(id,false)` then `Resume(id)` then `Inspect` shows running, and `List` still shows the session; the actual file-persistence proof is docker-gated in docker_test.go: create with `Cmd` writing a marker file to /workspace then sleeping, cold suspend, resume, `docker exec` reads the marker — the test-harness-may-exec note from Plan 3 applies) and `destroy removes the volume` (docker-gated: `docker volume ls` filtered by name empty after Destroy; fake: recorded). Plus `env vars injected` extends the existing proxy-env docker test with a `Spec.Env{"FOO":"bar"}` assertion via `docker inspect .Config.Env`.
- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement docker (`docker volume create` before run — tolerate exists; `-v rainier-ws-<id>:/workspace -w /workspace`; sorted `-e` loop; volume rm in Destroy after container rm) and fake (maps). **Step 4:** `go test ./internal/driver/ -race` with docker present → PASS; full suite.
- [ ] **Step 5: Commit** — `git commit -m "feat: driver — /workspace session volumes and Spec.Env injection"`

---

### Task 6: Driver — `Snapshot(ctx, id, ref)`, `Prepull`, rwire plumb-through

**Files:**
- Modify: `internal/driver/driver.go`, `docker.go`, `fake.go`, `contract.go`; `internal/runnerd/runnerd.go` (Op snapshot pass-through), `internal/runnerd/agent.go` (snapshot ref + prepull command)
- Test: driver contract + `internal/runnerd/agent_test.go` (extend)

**Interfaces:**
- Consumes: rwire `ToRunner{Type:"snapshot"|"prepull", Ref}` (Task 1).
- Produces:

```go
// driver.Driver changes:
Snapshot(ctx context.Context, id, ref string) (Snapshot, error) // ref "" ⇒ driver generates the legacy rainier-snap:<short>-<n> ref (dev surface compatibility)
Prepull(ctx context.Context, ref string) error                  // docker pull; fake records
// runnerd:
//  Op(ctx, id, "snapshot", warm) becomes Op(ctx, id, op string, warm bool, ref string) — or cleaner:
//  add OpSnapshot(ctx, id, ref string) (string, error) and keep Op for suspend/resume; HTTP dev
//  surface calls OpSnapshot(id, "") — choose this shape; it avoids threading ref through suspend/resume.
//  agent execute: "snapshot" passes m.Ref through; result.Detail carries the final ref (unchanged contract).
//  agent execute gains "prepull": fire-and-forget goroutine drv.Prepull(ctx, m.Ref), result{ok} sent when done
//  (controld treats prepull results as informational; no pending wait — sendToRunner, not dispatch).
```

- [ ] **Step 1: Failing tests.** Contract: `snapshot honors an explicit ref` (create → Snapshot(id, "rainier-env:test-abc") → returned Ref equals it; docker-gated: `docker image inspect` finds it; cleanup rmi) and `snapshot with empty ref generates unique refs` (the Plan 2 behavior, now pinned through the new signature). Agent test: scripted controld sends `{"type":"snapshot","req_id":9,"session":"...","ref":"rainier-env:e-1"}` → result detail is that ref; sends `{"type":"prepull","ref":"x"}` → fake driver records the pull, a result arrives, and no pending-correlation is required for it.
- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement (docker Snapshot: commit to the given ref; keep the atomic counter path for ""); update every Snapshot call site (`runnerd` dev surface, contract suite, tests). **Step 4:** Full `go test ./... -race` → PASS.
- [ ] **Step 5: Commit** — `git commit -m "feat: driver snapshot-to-ref and prepull; runnerd plumb-through"`

---

### Task 7: controld — environment resolution, placement pin, cache tiebreak

**Files:**
- Modify: `internal/controld/api.go` (create resolution + sessionJSON), `internal/controld/sched.go` (placement pin + tiebreak), `internal/controld/runners.go` (prepull broadcast helper `broadcastToRunners(msg)`)
- Test: `internal/controld/api_test.go`, `internal/controld/sched_test.go` (extend)

**Interfaces:**
- Consumes: env store methods, Seal/Open (secret decryption), validateConnectors, rwire Spec fields.
- Produces: `POST /v0/sessions` accepts `"environment": "<name-or-id>"`; resolution rules (exact):

1. Env not found → 400 `invalid_request` naming it. Scratch (no env) unchanged.
2. `resolved_image`: session `image` if set (override wins), else env snapshot when `SnapshotRef != "" && SnapshotHash == SetupHash` (cache hit → `Spec.Setup` empty), else `env.Image` with `Spec.Setup = env.Setup`, `Spec.SetupTimeoutSec` from env (0⇒900).
3. `egress_allow`: session-provided if set, else env's.
4. `Spec.Env`: decrypt each `secret_refs` name (`Open`) → map; missing secret → 409 `conflict` naming it (env references something deleted — fail the create loudly, before the row is created).
5. Placement: env.Placement pins the candidate set (scheduler); session rows keep no copy — the scheduler re-reads the env by `session.EnvironmentID` during placement (env edits move FUTURE placements only, sessions already `creating` unaffected).
6. Cache tiebreak in the scheduler: when the session's resolved image equals its env's SnapshotRef and `SnapshotRunner` is connected-with-free-slot, choose it; otherwise normal `pickRunner`.
7. `sessionJSON` gains `"environment"` (env NAME, "" for scratch — resolve via a per-request env cache to avoid N store reads in list) and ls's queue display: a queued session whose env placement names a disconnected/full runner renders state as `queued` with `queue_reason: "waiting for runner <name>"` in the JSON (derived, not stored).

- [ ] **Step 1: Failing tests.** API: create-with-env resolves image+egress+secrets into the dispatched rwire Spec (fake runner captures it — assert Setup present on cache miss, absent+snapshot-image on cache hit, secrets decrypted in Env, override image wins, missing secret → 409 pre-insert (no row created — assert store empty), unknown env → 400); shape pin gains `environment` + `queue_reason` keys. Sched: placement-pin table (pinned+free → placed there; pinned+full → stays queued; pinned+disconnected → queued; unpinned unchanged) and tiebreak (snapshot runner preferred when tied AND when not-most-free but free; normal pick when snapshot runner full).
- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement. **Step 4:** controld suite `-race` → PASS.
- [ ] **Step 5: CLI** — `rainier new --env <name>` (mutually composable with --image/--egress overrides); `ls` gains an ENV column. Extend the cli smoke.
- [ ] **Step 6:** Full suite; commit — `git commit -m "feat: session create resolves environments; placement pin and cache tiebreak"`

---

### Task 8: Setup execution — driver env, sessiond wrapper, control events

**Files:**
- Modify: `internal/driver/docker.go`+`fake.go` (Setup/SetupTimeoutSec → container env), `cmd/sessiond/main.go` (wrapper + watcher), `internal/runnerd/runnerd.go` (Hub.OnControl wiring → OnEvent)
- Test: `cmd/sessiond/main_test.go` (wrapper composition unit tests), `internal/runnerd/runnerd_test.go` (control→event routing)

**Interfaces:**
- Consumes: FrameControl/ControlSender (Task 1), Spec.Setup (Tasks 6–7 plumb it into driver.Spec — add `Setup string; SetupTimeoutSec int` to driver.Spec in THIS task, injected as `RAINIER_SETUP_B64` (base64) + `RAINIER_SETUP_TIMEOUT` envs).
- Produces the setup execution contract:

```
sessiond boot (dial mode), when RAINIER_SETUP_B64 is set:
 1. decode → write /workspace/.rainier/setup.sh (0755; mkdir -p)
 2. compose the child argv as a wrapper instead of the raw argv:
      sh -c 'sh /workspace/.rainier/setup.sh
             rc=$?
             echo $rc > /workspace/.rainier/setup.rc
             [ "$rc" -eq 0 ] && exec "$@"
             exit $rc' wrapper <real argv...>
    (exact script is a Go const with a unit test asserting the composed argv;
     setup output streams on the PTY like any output — viewers watch it live)
 3. watcher goroutine: poll /workspace/.rainier/setup.rc every 1s.
      rc file with 0        → ControlSender.Send({"kind":"setup_done"})
      rc file with non-0    → Send({"kind":"setup_failed","rc":N,"tail":"<last 2KB of the event log's plain text — read the session log file tail>"})
      timeout exceeded      → s.Stop() (SIGTERM the wrapper), Send setup_failed with rc=-1 and "setup timed out after Ns"
 4. no RAINIER_SETUP_B64 → exactly today's behavior, byte for byte.
runnerd: hub.OnControl parses {"kind":...}; "setup_done" → fireEvent(id, "setup_done");
 "setup_failed" → fireEvent carries the detail: OnEvent's signature is (id, state string) —
 encode as state "setup_failed" and pass detail via a new parallel hook? NO — keep it simple:
 the agent already builds FromRunner events; extend fireEvent to fireEventDetail(id, state, detail string)
 (old fireEvent delegates with ""), agent includes Detail in the event message (rwire already has Detail).
```

- [ ] **Step 1: Failing unit tests** — wrapper argv composition (given argv `["claude","--foo"]` the composed sh -c argv is exactly the const + args; a table case with quoting-hostile args (spaces, `$`) proving `"$@"` passes them intact — execute the wrapper against `/bin/sh` locally with a stub setup.sh in a t.TempDir to prove pass-through and rc semantics for 0 and 7); runnerd routing test — fake control payloads through a registered hub → events observed with detail.
- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement all three sides. sessiond's watcher lives beside dialLoop; the ControlSender comes from switching dialLoop to `ServeSessionWithControl` (behavior identical when no setup). driver: base64 encode Setup; cap at 512KB → larger returns a create error naming the limit.
- [ ] **Step 4:** `go test ./cmd/sessiond ./internal/runnerd ./internal/driver -race` → PASS; full suite.
- [ ] **Step 5: Commit** — `git commit -m "feat: setup execution — sessiond wrapper, control events, driver env"`

---

### Task 9: controld — setup orchestration and snapshot caching

**Files:**
- Modify: `internal/controld/runners.go` (applyEvent arms for setup_done/setup_failed; snapshot orchestration; prepull broadcast)
- Test: `internal/controld/runners_test.go` (extend with the fake-runner script)

**Interfaces:**
- Consumes: everything above.
- Produces the orchestration contract (each clause a subtest):

1. Event `setup_failed` (placement-guarded like `dead`): `Transition(from creating/running → failed, Error: "setup failed (rc N): <tail>")` — note the session may already be `running` (the wrapper only execs the agent on rc 0, so in practice it's `creating`… but the `running` event can race the rc write; accept both from-states).
2. Event `setup_done` (placement-guarded): look up the session's env and decide need-to-cache at event time — recompute `h := SetupHash(env.Image, env.Setup)`; cache is needed iff `env.SnapshotHash != h` (covers both never-cached and stale-cached; a session built from an env that was edited mid-setup produces a snapshot for the OLD hash, which the guarded store update then rejects — correct). When needed → dispatch `snapshot{ref: rainier-env:<envid>-<hash12>}` to THAT runner (ordinary dispatch, OpTimeout); on ok → `SetEnvironmentSnapshot(envID, hash, ref, runner)` (ErrConflict ⇒ env changed meanwhile — log, drop); then `broadcastToRunners(prepull{ref})` to every OTHER connected runner (fire-and-forget).
3. A `setup_done` for a scratch session or an env already freshly cached → no-op beyond the log.
4. The session itself: no state change on setup_done (the `running` event from registration governs, as today).

- [ ] **Step 1: Failing tests** — fake runner script: announce → create arrives (assert Setup non-empty) → send event running → send event setup_done → expect a snapshot command with the exact content-addressed ref → answer ok → assert env row has SnapshotRef/Runner/Hash; a second fake runner connected → assert it receives prepull{ref}. Failure path: setup_failed with detail → session failed with the composed error. Stale path: env updated between setup_done and snapshot-ok (drive via store) → SetEnvironmentSnapshot conflict → env unchanged, no crash.
- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement. **Step 4:** controld `-race -count=1` (and `-count=5` on the new tests) → PASS; full suite.
- [ ] **Step 5: Commit** — `git commit -m "feat: setup orchestration — snapshot caching, prepull broadcast"`

---

### Task 10: e2e scenes, fleet script, docs, acceptance

**Files:**
- Modify: `internal/e2e/e2e_test.go`, `scripts/e2e-fleet.sh`, `docs/deploy-gce.md`, `README.md`
- Test: the e2e suite itself; the fleet script run live

**Interfaces:** consumes everything; produces the Plan 4 acceptance evidence (design §1 criteria).

- [ ] **Step 1: e2e scenes** (in-process, fake driver + scripted sessiond extended to emit control frames; follow the existing `newFleet` isolation pattern):
  - `TestEnvSetupStreamsAndCaches` — env with setup; first create dispatches Setup, scripted sessiond emits setup_done, snapshot lands with the content-addressed ref (criterion 2's automated half); second create dispatches WITHOUT Setup and with the snapshot image.
  - `TestEnvEditInvalidatesCache` — update env setup → third create carries Setup again.
  - `TestSetupFailureLandsFailed` — setup_failed(rc 7, tail "boom") → session failed, error contains both.
  - `TestSecretsReachSpec` — env with secret_ref → fake runner sees decrypted value in Spec.Env (value seeded via the secrets API).
  - `TestPlacementPinQueuesWithReason` — env pinned to an absent runner → session queued, `queue_reason` names it; runner joins → session places (criterion 5).
- [ ] **Step 2:** `-race -count=5` stable, no sync sleeps.
- [ ] **Step 3: e2e-fleet.sh** gains an environments phase: `rainier secret set E2E_TOKEN`, `rainier env create e2e-env --image rainier-session:latest --setup-file <(echo 'echo setup-ran > /workspace/setup-marker; echo "$E2E_TOKEN" | wc -c > /workspace/secret-len')` + secret-ref + `rainier new --env e2e-env`, attach and assert both markers; time first vs second create and PASS when the second dispatched no setup (grep controld log) — run the script live on this machine (RUNNERD_PORT=8081 PG_PORT=5434) and put the transcript in the report.
- [ ] **Step 4: Docs.** deploy-gce.md: `RAINIER_SECRETS_KEY` generation line in step 4 (`openssl rand -hex 32`), an environments quickstart after step 6, and the design-§1 acceptance table appended to the notes section for the GCE run. README: envs in the quickstart. Also note the secrets-key-loss consequence (values only).
- [ ] **Step 5:** Full `go test ./... -race`; commit — `git commit -m "feat: environments e2e, fleet rehearsal, runbook"`
- [ ] **Step 6 (with Josh, on the fleet):** GCE acceptance — criteria 1–7 from the design §1 run on rainier-1 (setup-vs-cache timings recorded in deploy-gce.md notes).

---

## Coverage ledger (self-review against the design)

- §4.1 environment object/schema → Task 2 (all fields, incl. snapshot triple + timeout). §4.2 connector vocabulary, fail-closed unknowns → Task 4. §4.3 setup pipeline steps 1–5 → Tasks 7 (cache-hit resolution), 8 (streamed execution + outcome), 9 (guarded snapshot + prepull); content-addressed refs fixing the Plan 3 snapshot-collision minor → Task 6. §4.4 volume → Task 5. §4.5 API/CLI incl. devcontainer hint + secrets surface → Tasks 3–4, 7. §4.6 placement hint → Task 7. §5 edge cases: setup hang/timeout → Task 8 watcher; bad image → existing failed path (Task 9 test's failure scene covers the composed error); stale snapshot guard → Tasks 2/9; env-delete-while-referenced → Task 4; per-runner cache fallback → Task 7 tiebreak + Task 9 (other runners rebuild — covered by resolution rule 2 when SnapshotRunner absent? NOTE: resolution uses the snapshot IMAGE only when cached; a create placed on a non-holder runner with a cache-hit resolved image will `docker pull` and fail (no registry) → the tiebreak makes this rare, and the honest v0 fallback is: resolution rule 2 only takes the cache when SnapshotRunner is connected-with-capacity, else falls back to image+setup — THIS RULE IS PART OF TASK 7 (add to its Step 1 tests: cache-hit-but-holder-full → dispatched with Setup on another runner). §6 registry-upgrade path documented in code comments (Task 9). §7 verification map → Tasks 1–10 as listed.
- Deferred per design (do NOT build): connector execution, credential vault, broker, browser tiers, continuous sync, registry-backed snapshot distribution, per-env RBAC.
- Type consistency check (done): `Environment` fields identical across Tasks 2/4/7/9; `Spec.Setup/SetupTimeoutSec/Env` identical across rwire (T1), driver (T5/T8), resolution (T7); `Snapshot(ctx,id,ref)` consistent T6 onward; control payload kinds `setup_done`/`setup_failed` identical T1/T8/T9; snapshot ref format identical T6/T9/T10.
- Placeholder scan (done): none of the banned patterns; the one deliberate reference-to-existing-code idiom ("mirror X's pattern, read it first") always names an exact file that exists.

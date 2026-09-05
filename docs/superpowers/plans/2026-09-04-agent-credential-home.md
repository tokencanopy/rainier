# Agent Credential Home and Agent Login Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A person logs a coding agent in once — Claude Code through its own `/login`, Codex through `codex login --device-auth` — inside an ordinary Rainier session, and every later session of theirs, in every workspace they are a member of, on any runner, starts with that agent already authenticated. No credential is ever pasted into a sandbox, written into the workspace, baked into a snapshot, carried in a create, or printed in a log. Tag `v0.0.3`.

**Architecture:** Three pieces. (1) An **agent home**: one writable docker volume per (creator, workspace), mounted at `/rainier/agents` into every session that creator runs in that workspace on that runner, with each agent pointed at its own subdirectory through the variable it already honors (`CLAUDE_CONFIG_DIR`, `CODEX_HOME`). `$HOME` and the read-only rootfs are untouched. (2) A **credential set**: the provider's allowlisted files inside that subdirectory, which `sessiond` keeps equal to the control plane's copy — fetched at boot, put on every change, emptied on a downward revoke — over the session RPC that already carries the git credential. (3) **Custody**: the control plane's sealed copy, keyed per (user, provider), one per person per agent, projected into every workspace the person is in; membership is re-checked at every delivery. Everything provider-specific is one row in a table `controlapp` owns; `sessiond`, the driver, the stores, and the RPC never spell a provider name. Self-hosted `controld` answers the RPC out of its existing sealed store; a hosted cell answers the same RPC out of its own, through the same `runnerplane.Host.SessionRequest` hook it already answers `mint_git_credential` on.

**Tech Stack:** Go 1.25, `protocol/runner` (additive at version 1), `controlapp`, `internal/driver` (docker), `cmd/sessiond`, `internal/controld` (`seal.go`, `pgstore`), `v0wire`, `cmd/rainier`.

**Spec:** `docs/superpowers/specs/2026-08-27-rainier-design.md` §4.3 (read-only rootfs, the boot chain), §8 ("Subscription login", "Agent login flows"); `2026-08-29-plan5-github-vault-design.md` §4.1 (the session RPC), §4.2 (secret hygiene); the hosted product's security requirements the OSS half must satisfy: a credential set never enters workspace files, environment, images, checkpoints, exports, or logs; mutable agent state in one workspace is not visible in another; a runner cannot request another session's credential by substituting an id. The design this plan implements is `rainier-cloud/docs/superpowers/specs/2026-09-04-agent-credential-home-design.md` (decisions of 2026-09-04: mounted home plus control-plane custody; one login reaches every workspace; Claude Code first, Codex second). This is OSS plan #17; the hosted plan consumes `v0.0.3`.

## Global Constraints

- Every task starts in `.worktrees/<task>` branched from the integration branch `feat/agent-credential-home` (created from freshly fetched `origin/main` by the reviewer), so each worker reads this plan and the probe results. Workers do not commit, push, or merge; the reviewer commits each task, re-runs every gate, and cherry-picks.
- **A credential never enters this repository, a log line, an error, a metric, a commit, or test output.** Fixtures are the literal bytes `credential_example` and `auth_example`; the synthetic provider is `test`; identifiers are `user_example`, `ws_example`, `sess_example`, `runner-example-0`. Tests assert absence, not just presence.
- **`sessiond`, `internal/driver`, `internal/runnerd`, the stores, and the RPC dispatch contain no provider name.** `grep -rn "claude\|codex" cmd/sessiond internal/driver internal/runnerd internal/controld/srpc.go internal/controld/pgstore` is empty after every task. The two rows in `controlapp/agents.go` are the only place.
- `protocol/runner` changes are additive at `ProtocolVersion` 1: one new `Spec` field and three new RPC method names. `TestPublicRunnerWireShapes`' golden create is unchanged (the field is `omitempty` and absent there).
- `control/*.go` does not change. `controlapp` gains one file and two small additions to `createSpec`; the frozen service signatures are untouched.
- Gates for a Go task: `gofmt -l cmd internal controlapp protocol v0wire` empty, `go vet ./...`, the task's packages under `-race -count=1`, `make verify` (module path, protocol and control guards, the full suite, build, vet), `git diff --check`. `GOCACHE=/private/tmp/rainier-agents-gocache`. Go gates run serially.
- The fake-driver chaos suite (`go test ./internal/e2e/`) and every existing test in `internal/controld`, `cmd/sessiond`, `cmd/rainier`, and `internal/driver` pass unmodified except where a test named a changed identifier or a wire golden gained the new field.
- The live-fleet e2e on the fleet VM (`make e2e`), with the new phase, is the final gate before the tag.

## Probe results (Task 0 fills these in before any worker starts)

| Probe | Question | Result |
|---|---|---|
| A1 | With `CLAUDE_CONFIG_DIR=/rainier/agents/claude`, does Claude Code write its `.claude.json` (sign-in session, onboarding, project trust) under that directory, or under `$HOME`? | **Under the directory.** `.claude.json`, `.credentials.json`, `settings.json`, `history.jsonl`, `projects/`, `sessions/`, `backups/`, `debug/` all land in `CLAUDE_CONFIG_DIR`; nothing is written under `$HOME` (Claude Code 2.1.197 on the Linux session image; 2.1.261 on macOS). The `HomeVar` fallback is not needed. |
| A2 | Inside a session with a read-only rootfs and the egress proxy, does `claude` `/login` fall back to the paste-a-code prompt, and does the pasted code complete the login? | **Yes.** A person attached from a laptop, ran `claude`, `/login`, chose the subscription, pasted the code back, and `.credentials.json` (0600, ~470 bytes) appeared under the directory. `codex login --device-auth` likewise wrote `auth.json` (0600) under `CODEX_HOME`. The rootfs was confirmed read-only throughout (`$HOME`, `/usr/local`, `/opt/rainier-env` all refuse writes), so without the mount neither login could have landed. |
| A3 | Which hosts do Claude Code login, refresh, and inference reach; which do Codex device login and inference reach? (read off egressd's log) | **Claude Code:** `api.anthropic.com` (API), `platform.claude.com` (OAuth exchange at login), `downloads.claude.ai` (self-update check), `mcp-proxy.anthropic.com` (claude.ai connectors); optional, refused harmlessly: `http-intake.logs.us5.datadoghq.com` (telemetry), `raw.githubusercontent.com` and `registry.npmjs.org` (update check). **Codex:** `auth.openai.com` (device login), `chatgpt.com` — the bare apex, which a `*.chatgpt.com` entry does not match — (inference). See A5. |
| A4 | Two `claude` processes in two sessions sharing one config directory: both work, neither corrupts the other's credential? | **Partial.** Two concurrent `claude -p` runs sharing one config directory both started, both read the same login, and `.credentials.json` was byte-for-byte unchanged afterwards; neither completed a model call because of A5, so "both work" is proven for the credential and not yet for the API. |

If A1 is "under `$HOME`", the `claude` row gains `HomeVar: "HOME"` and `sessiond` sets `HOME` for the agent process only (Task 3, "A1 false"); the mount and every other task are unchanged.

**A5 (found by the probes, not planned) — RESOLVED 2026-09-05.** Under the session's `HTTPS_PROXY`, Claude Code — the native 2.1.261 build and the npm 2.1.197 build alike — completed login and the OAuth exchange but failed every model call with `Connection error` and never opened a socket toward the proxy. The cause was the proxy URL the driver injects: `http://<session-id>:@host:3128`, a username with an EMPTY password. With a bare URL the client reaches egressd and receives its 407 challenge; with `http://<session-id>:x@host:3128` both builds answer a model call in three to five seconds through the proxy, and egressd's `sessionFromProxyAuth` reads the username half and ignores the password. DNS was a red herring: the `ENOTFOUND` came from the native build's first-run connectivity check, which does resolve the host itself, and a DNS answer alone changed nothing. **The fix is one line in `internal/driver/docker.go`** (`withSessionUserinfo` now carries the placeholder password `rainier`), shipped separately from this plan so it reaches `v0.0.3`'s runner artifacts, plus a bug report to Claude Code. The `claude` row's `Egress` stands; its `Env` gains nothing, though a deployment may set `CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC` to skip the claude.ai connectors' retry cost.

## File structure

```text
protocol/runner/messages.go          Spec.Home *HomeMount{Volume, Path}; the three method names as constants
protocol/runner/messages_test.go     round-trip and golden for HomeMount; unknown-field tolerance unchanged
controlapp/agents.go                 NEW. AgentProvider table (claude, codex, test), HomeMountPath,
                                     AgentHomeVolume(ws, creator), AgentsEnv(providers) → RAINIER_AGENTS_B64,
                                     AgentCredentialStore port, AgentCredentialService (fetch/put/revoke/
                                     withdraw/list with the authorization checks and the downward revoke)
controlapp/agents_test.go            the table's invariants; the service against a fake store and transport
controlapp/scheduler.go              createSpec: Spec.Home, the providers' variables and RAINIER_AGENTS_B64
                                     into Spec.Env, their egress hosts unioned — for every create
controlapp/repotest/agentcreds.go    NEW. the store contract suite every AgentCredentialStore passes
internal/driver/driver.go            Spec.Home
internal/driver/docker.go            the volume: create + the chown job + `-v`; destroy leaves it
internal/driver/docker_test.go       args golden; snapshot excludes the mount
internal/runnerd/agent.go            "create": Spec.Home → driver.Spec.Home
cmd/sessiond/agents.go               NEW. the agents stage (fetch → files 0600), the sync loop (put on
                                     change, backoff, exit-time put), the revoke handler, the 64 KiB cap,
                                     O_NOFOLLOW reads, the boot notes
cmd/sessiond/agents_test.go          against the rpc_test fake host
cmd/sessiond/gitchain.go             bootEnv.AgentsB64; the stage ordering (after clone, before init)
internal/controld/store.go           AgentCredential{UserID, Provider, Ciphertext, Nonce, Version, UpdatedAt};
                                     Get/Upsert/Delete/ListAgentCredentials on Store
internal/controld/memstore.go        the in-memory implementation
internal/controld/pgstore/pgstore.go the SQL
internal/controld/pgstore/migrations/0010_agent_credentials.sql
internal/controld/agentvault.go      NEW. the self-hosted AgentCredentialStore over seal.go
internal/controld/srpc.go            fetch_agent_credentials / put_agent_credentials beside the mint
internal/controld/api.go             GET /v0/agents, DELETE /v0/agents/{provider}
internal/controld/controld.go        the two routes; the membership hook (withdraw on role removal)
v0wire/agents.go                     NEW. AgentView, AgentsEnvelope, RenderAgents
v0wire/wire_test.go                  golden JSON
cmd/rainier/main.go                  agent login | ls | logout
scripts/e2e-fleet.sh                 the "agent credential home" phase
README.md, docs/deploy-gce.md        the three verbs, the mount, what a provider row is
```

---

### Task 0: Probes (reviewer and operator, on the fleet VM)

No worker. Half a day. Each probe runs in a throwaway session on rainier-1 from the current tag with an environment whose setup script installs the agent CLI into `/opt/rainier-env` and whose egress list is wide open for the probe only. Record the four answers in the table above and the two provider rows' final `Files` and `Egress` values in Task 1. Nothing from the probe sessions — no transcript, no account name, no token — enters this repository; the table gets "under the directory" / "under $HOME", "yes" / "no", and host names.

---

### Task 1: The wire, the provider table, and the create

**Files:**
- Modify: `protocol/runner/messages.go`, `protocol/runner/messages_test.go`
- Create: `controlapp/agents.go`, `controlapp/agents_test.go`
- Modify: `controlapp/scheduler.go` (`createSpec` only)

**Interfaces:**
- `runner.HomeMount{Volume string; Path string}`; `runner.Spec.Home *HomeMount` (`json:"home,omitempty"`).
- Method names: `runner.MethodFetchAgentCredentials = "fetch_agent_credentials"`, `MethodPutAgentCredentials = "put_agent_credentials"`, `MethodRevokeAgentCredentials = "revoke_agent_credentials"`. Payloads (documented on the constants, as the mint's is): fetch `{"provider"}` → `{"version", "files": {name: base64}}`; put `{"provider", "files", "version"}` → `{"version"}`; revoke (downward) `{"provider"}` → `{}`. Refusals are `{"error": sentence}` on `ok:false`, as everywhere.
- `controlapp.AgentProvider{Name, HomeEnv, HomeVar, Files []string, Egress []string, LoginCmd []string}`; `controlapp.AgentProviders() []AgentProvider` — `claude`, `codex`, and `test` (present only when `controlapp.EnableTestAgentProvider` is set by the host, which only the e2e's controld does).
- `controlapp.HomeMountPath = "/rainier/agents"`; `controlapp.AgentHomeVolume(ws control.WorkspaceID, creator control.ActorID) string` = `"rainier-agents-" + hex(sha256(ws + "\x00" + creator))[:16]`; `controlapp.AgentsEnv(providers) (map[string]string)` — each provider's `HomeEnv` → `HomeMountPath/<name>`, plus `RAINIER_AGENTS_B64` = base64 JSON `[{"provider","dir","files"}]`.
- `createSpec` sets `spec.Home` for every create with a creator, merges `AgentsEnv` into `spec.Env` *before* `material.Environment` (so a resolver's value can still override, the documented last-wins rule), and unions every provider's `Egress` into `spec.EgressAllow`.

The volume key is opaque on purpose: a docker volume name is visible to anyone with `docker` on the runner, and an account id is not something to print there. The provider table is data, not code: adding a third agent is a row plus its Task 0 probes.

- [ ] **Step 1: Write the failing tests**

```go
// protocol/runner
func TestHomeMountRoundTrip(t *testing.T) {
	// {"home":{"volume":"rainier-agents-0123456789abcdef","path":"/rainier/agents"}} pins the tags;
	// a Spec without Home marshals with no "home" key (the golden create is unchanged).
}

// controlapp
func TestAgentProvidersNeverSpellASecretPath(t *testing.T) {
	// every Files entry is a bare file name (no "/" or ".."), every HomeEnv is
	// non-empty and distinct, and "test" is absent unless enabled.
}
func TestAgentHomeVolumeIsOpaqueAndStable(t *testing.T) {
	// same (ws, creator) → same name; a different creator → different; the name
	// contains neither id.
}
func TestCreateSpecCarriesTheHome(t *testing.T) {
	// a create for (ws_example, user_example) has Home{Volume: AgentHomeVolume(...), Path: HomeMountPath},
	// Env[CLAUDE_CONFIG_DIR] == "/rainier/agents/claude", Env[RAINIER_AGENTS_B64] decodes to both rows,
	// and EgressAllow contains every provider's Egress hosts once; a resolver value for
	// CLAUDE_CONFIG_DIR wins over the table's.
}
```

- [ ] **Step 2: Run them** — FAIL. **Step 3: Implement.** **Step 4: PASS.**
- [ ] **Step 5: Gates** — the Go set; `make protocols` and `make control` in particular.

---

### Task 2: The driver mounts it

**Files:**
- Modify: `internal/driver/driver.go`, `internal/driver/docker.go`, `internal/driver/docker_test.go`, `internal/runnerd/agent.go`

**Interfaces:**
- `driver.Spec.Home *driver.HomeMount{Volume, Path}` (the driver's own type; `internal/runnerd/agent.go` copies the runner's into it on `"create"`, as it copies every other field).
- `docker.go`: when `spec.Home != nil`, `docker volume create <volume>` if absent, then the one-shot chown job `workspaceInitArgs` already runs for the workspace volume — same image, same `--cap-drop ALL --cap-add CHOWN --network none --read-only` — against the home volume, then `-v <volume>:<path>` on the run. `destroy` and `remove_workspace` leave it: it belongs to the (creator, workspace), not the session.

- [ ] **Step 1: Write the failing tests**

```go
func TestRunArgsMountTheHome(t *testing.T) {
	// args contain "-v", "rainier-agents-0123456789abcdef:/rainier/agents" exactly once, after the
	// workspace mount; a Spec with Home == nil has no such arg (every existing golden passes).
}
func TestHomeVolumeIsPreparedOnceAndChowned(t *testing.T) {
	// first create: volume create + the init job with CAP_CHOWN only; second create on the same
	// volume: neither runs again.
}
func TestDestroyLeavesTheHome(t *testing.T) {
	// destroy removes the container and the workspace volume; the home volume remains.
}
func TestSnapshotExcludesTheHome(t *testing.T) {
	// docker-backed (skips without docker): a container with the mount and a file under
	// /rainier/agents/test is committed; the image has no /rainier/agents/test.
}
```

- [ ] **Step 2: Run them** — FAIL. **Step 3: Implement.** **Step 4: PASS.**
- [ ] **Step 5: Gates** — the Go set; the docker-backed cases run on the fleet VM by the reviewer.

---

### Task 3: `sessiond` keeps the set equal to custody

**Files:**
- Create: `cmd/sessiond/agents.go`, `cmd/sessiond/agents_test.go`
- Modify: `cmd/sessiond/gitchain.go` (`bootEnv.AgentsB64`, the stage), `cmd/sessiond/main.go` (registration, the loop's lifecycle)

**Interfaces (internal to sessiond, all provider-agnostic):**
- `type agentEntry struct{ Provider, Dir string; Files []string }` decoded from `RAINIER_AGENTS_B64`.
- The **agents stage**, after the clone stage and before init: for each entry, `mkdir -p dir` 0700 (fail the stage with the sentence `"this runner does not mount agent homes; upgrade runnerd to v0.0.3"` if `HomeMountPath` is absent or not writable), then `fetch_agent_credentials`; write each returned file 0600 via temp-and-rename; record the baseline (version, per-file size and mtime). A refusal or a timeout is a **boot note** (`{"kind":"agent_note","provider":…,"text":…}` on the events channel, the way a stage verdict travels), not a failure: the agent starts and asks for login, which is the truthful state.
- The **sync loop** (`agentSyncInterval = 2s`): stat the allowlisted files; on any change, read them (`O_NOFOLLOW`; a symlink is skipped with a note), cap the set at `agentSetMaxBytes = 64 << 10` (over → note, no put), and if the bytes differ from the last put, `put_agent_credentials`. A failed put retries with backoff 2 s → 30 s and never blocks the agent. On shutdown, one final put bounded by the RPC timeout.
- The **revoke handler**, registered beside the file handlers: remove the allowlisted files, reset the baseline to "nothing", answer `{}`.
- **A1 false:** an entry may carry `"home_var": "HOME"`; `sessiond` then sets that variable to `dir + "/home"` in the *agent's* environment only (`chainArgv`'s env, not the container's).

- [ ] **Step 1: Write the failing tests** (the `rpc_test` fake host answers fetch/put; every case asserts no credential byte reaches the log)

```go
func TestAgentsStageWritesFetchedFilesReadOnlyToOwner(t *testing.T)   // 0600, contents equal, baseline recorded
func TestAgentsStageWithNoCredentialStartsTheAgentAnyway(t *testing.T) // version 0 → empty dir, no note
func TestAgentsStageRefusalIsANoteNotAFailure(t *testing.T)            // {"error"} → agent_note event, agent starts
func TestMissingMountFailsTheStageWithTheSentence(t *testing.T)
func TestSyncPutsExactlyOnceOnChange(t *testing.T)                     // a write → one put with the new bytes; a rewrite of the same bytes → none
func TestSyncRetriesAndNeverBlocks(t *testing.T)
func TestExitPerformsAFinalPut(t *testing.T)
func TestRevokeEmptiesAndResetsTheBaseline(t *testing.T)               // and a later write is a new put
func TestOversizedAndSymlinkedFilesAreNotSent(t *testing.T)
func TestNoCredentialByteReachesTheLog(t *testing.T)
```

- [ ] **Step 2: Run them** — FAIL. **Step 3: Implement.** **Step 4: PASS.**
- [ ] **Step 5: Gates** — the Go set; `cmd/sessiond` under `-race`.

---

### Task 4: Custody — the service, the port, the contract suite, the self-hosted store, the RPC answers

**Files:**
- Modify: `controlapp/agents.go` (the service), `controlapp/repotest/` (new `agentcreds.go`)
- Modify: `internal/controld/store.go`, `memstore.go`, `pgstore/pgstore.go`; Create: `pgstore/migrations/0010_agent_credentials.sql`, `internal/controld/agentvault.go`
- Modify: `internal/controld/srpc.go`, `internal/controld/srpc_test.go`

**Interfaces:**

```go
// controlapp
type AgentCredentialSet struct{ Version uint64; Files map[string][]byte }
type AgentCredentialStatus struct{ Provider string; Version uint64; UpdatedAt time.Time }
type AgentCredentialStore interface {
	FetchAgentCredentials(ctx, user control.ActorID, provider string) (AgentCredentialSet, error) // version 0, no files when none
	PutAgentCredentials(ctx, user control.ActorID, provider string, files map[string][]byte) (uint64, error)
	RevokeAgentCredentials(ctx, user control.ActorID, provider string) error                     // idempotent
	ListAgentCredentials(ctx, user control.ActorID) ([]AgentCredentialStatus, error)
}
type AgentCredentialService struct{ /* store, control.Authorizer, *AttachmentService (for the downward revoke), SessionRepository */ }
func (s *AgentCredentialService) AnswerFetch(ctx, row control.Session, provider string) (AgentCredentialSet, error)
func (s *AgentCredentialService) AnswerPut(ctx, row control.Session, provider string, files map[string][]byte) (uint64, error)
func (s *AgentCredentialService) Logout(ctx, sc control.Scope, provider string) error           // revoke + downward to every live session of the user
func (s *AgentCredentialService) Withdraw(ctx, ws control.WorkspaceID, user control.ActorID) error // downward to the user's live sessions in ws; custody untouched
func (s *AgentCredentialService) List(ctx, sc control.Scope) ([]AgentCredentialStatus, error)
```

The service owns the authorization, once, for both hosts: `AnswerFetch`/`AnswerPut` require the row to have a creator, the provider to be in the table, and **membership** — asked of the host's `control.Authorizer` as `ActionAttach` on the session resource in the creator's own scope, because "the creator may still attach to their own session in this workspace" is exactly "the creator is a current member of this workspace", and it needs no new action in the frozen contract. The host's `SessionRequest` guard has already established that the asking runner is the row's runner (`srpc.go` does; the hosted gateway does). `Logout` requires `sc.Actor` to be the user (an owner or admin of any workspace gets `control.ErrForbidden`: the credential is not the workspace's). The downward revoke is `AttachmentService.sessionRPC(row, MethodRevokeAgentCredentials, …)` to each live session, best effort, logged by session id only.

The sealed blob is JSON `{"files": {name: base64}}`; the self-hosted `agentvault.go` seals it with `seal.go` under `RAINIER_SECRETS_KEY` with `user + "\x00" + provider + "\x00" + version` bound as additional authenticated data, so a row copied to another user, provider, or version does not open. `0010_agent_credentials.sql`: `agent_credentials(user_id text REFERENCES users ON DELETE CASCADE, provider text, ciphertext bytea, nonce bytea, version bigint NOT NULL DEFAULT 1, updated_at timestamptz, PRIMARY KEY (user_id, provider))`.

- [ ] **Step 1: Write the failing tests**

```go
// controlapp/repotest — run by memstore, pgstore, and (later) the hosted store
func RunAgentCredentialStore(t *testing.T, open func(t *testing.T) controlapp.AgentCredentialStore) {
	// fetch before any put → version 0, no files; put → 1; put again → 2 and the new bytes;
	// revoke → fetch answers 0; revoke twice is fine; list returns versions and no bytes;
	// two users never see each other's set.
}

// controlapp/agents_test.go — the service over a fake store, authorizer, and transport
func TestAnswerFetchIsForTheCreatorOnly(t *testing.T)          // row without creator → refused; provider unknown → refused
func TestAnswerFetchReChecksMembership(t *testing.T)           // authorizer denies attach → refused, nothing read
func TestAnswerPutSealsBeforeAnswering(t *testing.T)           // store failure → error, no version returned
func TestLogoutRevokesEverywhere(t *testing.T)                 // store revoke + one downward revoke per live session of the user, in every workspace
func TestWithdrawTouchesOnlyThatWorkspace(t *testing.T)        // downward to sessions in ws only; store untouched
func TestLogoutRefusesARoleWithoutTheAccount(t *testing.T)     // workspace owner ≠ the user → ErrForbidden

// internal/controld
func TestAgentVaultBindsUserProviderVersion(t *testing.T)      // sealed for (a, claude, 1) does not open as (b, claude, 1), (a, codex, 1), (a, claude, 2)
func TestSessionRequestAnswersFetchAndPut(t *testing.T)        // through srpc.go, in the helper's shapes; the eight refusals of the mint apply
func TestNoCredentialByteReachesALogOrError(t *testing.T)
```

- [ ] **Step 2: Run them** — FAIL. **Step 3: Implement.** **Step 4: PASS**, `repotest` green on both stores.
- [ ] **Step 5: Gates** — the Go set; `go test ./internal/e2e/` (the chaos suite) unchanged.

---

### Task 5: `/v0/agents` and the CLI

**Files:**
- Create: `v0wire/agents.go`; Modify: `v0wire/wire_test.go`
- Modify: `internal/controld/api.go`, `internal/controld/controld.go` (routes; the membership hook calls `Withdraw` where a role is removed)
- Modify: `cmd/rainier/main.go`, `cmd/rainier/main_test.go`

**Interfaces:**
- `GET /v0/agents` → `{"agents": [{"provider", "status": "logged_in"|"none", "since", "version", "workspaces": [ids the caller is a member of], "note"}]}`; `DELETE /v0/agents/{provider}` → 204, and 403 with the `v0wire` error body when the caller is not the account. Both `requireUser`; neither carries a byte of a credential.
- `rainier agent login <provider> [--env NAME]`: `POST /v0/sessions` with name `agent-login-<provider>-<4 hex>`, no repositories, `cmd` = the provider's `LoginCmd`, the environment named or the context's default; attach as `runNew` does; on exit or detach, `DELETE` the session, `GET /v0/agents`, and print `logged in as of <since> (v<version>)` — or, if the version did not move, `login did not complete: <the last note, or "the agent wrote no credential">`. The provider list in the usage text comes from `controlapp.AgentProviders()`; an unknown provider is refused before any request.
- `rainier agent ls`: `PROVIDER  STATUS  SINCE  WORKSPACES`. `rainier agent logout <provider>`: prints `this logs <provider> out of every workspace you are in; a running agent keeps what it holds until it exits` before the DELETE (`--yes` skips the prompt for scripts).

- [ ] **Step 1: Write the failing tests** — `v0wire` golden JSON for the envelope; `internal/controld` route tests for 200/204/403 and the absence of any credential substring; `cmd/rainier` tests against the fake server: `agent login` creates with the right `cmd` and no repos, removes afterwards, reports the version moving or not; `agent ls` renders; `agent logout` calls DELETE and prints the caveat.
- [ ] **Step 2: Run them** — FAIL. **Step 3: Implement.** **Step 4: PASS.**
- [ ] **Step 5: Gates** — the Go set.

---

### Task 6: Docs, the live-fleet proof, and `v0.0.3` (reviewer)

- `README.md` ("Hosted login and contexts" gains "Agent login": the three verbs, where the home is, what a provider row is); `docs/deploy-gce.md` (the mount, the `test` provider, what the operator sees on first login).
- `scripts/e2e-fleet.sh` gains a phase **"agent credential home"** run with `RAINIER_E2E_TEST_AGENT=1` on the e2e controld: `rainier agent login test --env <the stock env>` (the `test` row's `LoginCmd` writes `credential_example` into its file and exits) → `agent ls` shows `logged_in v1`; a second session **on the other runner** boots and its file equals `credential_example`; a write of `credential_example2` inside that session is `v2` on `agent ls` within five seconds; the environment's snapshot contains nothing under `/rainier/agents`; `agent logout test --yes` → the file in the live session is gone within five seconds and `agent ls` shows `none`; `docker inspect` of the session shows the mount and `docker volume ls` shows one `rainier-agents-*` per (creator, workspace); grep of controld's and runnerd's logs for `credential_example` is empty.
- Then, by the operator only: a real `rainier agent login claude` on the fleet VM, a second session that starts authenticated, `agent logout`. Recorded as pass/fail. No transcript.
- Tag `v0.0.3` with release notes naming the mount, the three RPC methods, the three verbs, and the `Spec.Home` field; the external-import proof (`GOPROXY=direct`, no `replace`) extends to `controlapp.AgentProviders` and `runner.HomeMount`.

## Acceptance

- A create carries the home mount and the providers' variables; the driver mounts a chowned volume per (creator, workspace); `destroy` leaves it and a snapshot excludes it.
- `sessiond` fetches at boot, puts on change, empties on revoke, and never blocks or fails the agent over custody; an old runner fails the stage with the named sentence.
- Custody is sealed per (user, provider, version) and the contract suite passes on both stores; the service refuses a session without a creator, a non-member, an unknown provider, and a logout by anyone but the account.
- `rainier agent login|ls|logout` work against self-hosted controld; `/v0/agents` never returns a credential byte.
- The live-fleet e2e phase passes on the fleet VM, and one real Claude Code login round-trips to a second session.
- No provider name outside `controlapp/agents.go`; no credential byte in any log, error, fixture, or test output; `make verify` green; `v0.0.3` importable with no `replace`.

## Not in this plan

- The hosted cell's store, gateway methods, internal routes, outbox revoke, and edge forwarding (rainier-cloud, consuming `v0.0.3`).
- Carrying the non-credential part of the home in a portable checkpoint (the checkpoint plan; the allowlist is what makes the exclusion mechanical).
- A per-workspace login override, a selected-workspaces mode, provider-side revocation, Gemini CLI, and the `CLAUDE_CODE_OAUTH_TOKEN` / API-key route through workspace secrets.

## Honest sizing

Five worker tasks after a half-day of probes. Task 1 first (everything reads its types); Tasks 2 and 3 in parallel after it; Task 4 after 1; Task 5 after 4. Tasks 3 and 4 are substantial; 1, 2, and 5 are moderate. About three days at three workers with the reviewer gating, plus the live phase and the tag.

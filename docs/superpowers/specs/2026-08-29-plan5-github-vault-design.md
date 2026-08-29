# GitHub Connector + Credential Vault — Design (Rainier v0, Plan 5)

**Date:** 2026-08-29 · **Status:** Draft for Josh's review · **Author:** Jace
**Parent specs:** `2026-08-27-rainier-design.md` §8 (git provider, credentials), `2026-08-29-plan4-environments-design.md` §4.2 (connector vocabulary)
**Predecessors:** Plans 1–4 merged; Plan 3 and Plan 4 acceptance passed on the GCE fleet.

## 1. Problem statement

Sessions have toolchains (Plan 4) but no code and no credentials: the `github`
connector is stored vocabulary with no behavior, `login` verifies the GitHub
token and throws it away, and nothing in the system can clone, push, or tell
you a credential has gone stale. Plan 5 makes environments deliver **code with
working git**, under the compliance constraints the parent spec locks in:
unmodified tools, the user's own GitHub identity, tokens never persisted in
the sandbox.

**Success criteria (measurable):**

1. An env declaring `github{repo, base_branch}` + `rainier new --env` boots a
   session with the repo cloned at `/workspace/<name>` on branch
   `rainier/<session-name>`, and `git push` inside the session works with no
   token visible in `.git/config`, any file, or the process environment.
2. Commits made in-session attribute to the human: `user.name` = GitHub
   login, `user.email` = the GitHub noreply address
   (`<github_id>+<login>@users.noreply.github.com`).
3. A revoked/expired GitHub credential surfaces as a clear, named action
   within one failed operation: the in-session git error and the API error
   both say `rainier login --refresh github`; `rainier creds` shows
   per-provider status.
4. Session `repos` overrides beat env connector defaults; explicit `[]`
   means no clone (scratch semantics preserved); multi-repo clones land as
   sibling directories.
5. The cacheable/per-session split holds: `setup` (pre-clone, cached) and
   `init` (post-clone, every session, never cached) both stream to an
   attached viewer; a cache-hit session runs `init` but not `setup`.
6. `rainier push <dir> <session>:<path>` and `pull` round-trip a directory
   laptop↔session (bounded size, v0).
7. `GET /v1/sessions/{id}/diff` returns per-repo `--stat` vs the merge-base
   with the base branch.
8. Riders land: a container crash preserves the workspace volume (salvage
   until `rm`); `child_exited` is visible in `ls`; `attach --since 0`
   full-history replay actually reaches the viewer; `/v1/me` exposes the
   user id (and the CLI's owner-preference uses it).
9. All of the above pass in the e2e suite and in a live fleet-rehearsal
   phase (real clone/push against a throwaway GitHub repo when `gh` is
   available); GCE acceptance recorded in the runbook.

## 2. Goals and non-goals

**In scope:** the session RPC primitive (bidirectional control channel);
credential vault (schema, lifecycle, refresh UX, login scope upgrade);
GitHub connector execution (clone-at-boot, session branches, credential
helper, attribution); `init` hook; `push`/`pull`; diff endpoint; the four
riders; docs + rehearsal + acceptance.

**Non-goals (explicit):**

- Shadow-branch auto-push — **Plan 6** (decided with Josh, 2026-08-30);
  this plan builds the credential machinery it needs.
- GitHub App mode (installation tokens, org hardening) — the vault schema
  accommodates it (`provider` + refresh fields), not built.
- GitLab/Bitbucket — the git-provider seam stays a vocabulary question;
  one provider is a hypothetical seam and we do not build the abstraction
  until a second provider exists.
- PR creation API/CLI (ruled out in Plan 3-era scoping; `gh` works
  in-session once a token is mintable — document that instead).
- Streaming/continuous file sync; per-repo RBAC; token-scoped-per-repo
  minting (user token mirrors the user's own GitHub access, per spec §8).

## 3. Relevant context and constraints

- **Control channel is one-way today.** Plan 4's `FrameControl` carries
  sessiond→runnerd events only; `relay.ControlEvent{Kind,RC,Tail}` is the
  shape, `NewHubWithControl` the hook. Credential minting, diff, and
  push/pull all need round trips — the central new machinery.
- **Writer discipline:** sessiond-side writes go through Plan 4's shared
  `connWriter`; hub-side concurrent writes are safe (coder/websocket
  serializes writers) — the RPC layer must respect both.
- **Rootfs is read-only on cache-hit/scratch sessions** (Plan 4 amendment):
  `~/.gitconfig` is not writable there. Git configuration must live in the
  workspace and reach git via environment (`GIT_CONFIG_GLOBAL`).
- **Vault crypto rides `seal.go`** (AES-256-GCM under `RAINIER_SECRETS_KEY`)
  — same key, same helpers, no second mechanism. *Why not HashiCorp
  Vault/OpenBao/Infisical:* each is another stateful service, which the
  state rule exists to prevent; the application-level-encryption pattern is
  what Coder/Grafana/GitLab ship; the upgrade ladder (instance key → KMS
  envelope → external vault) is additive and documented, not built.
- **Login today:** `POST /v1/auth/github` verifies via `GET /user` with
  scope `read:user` and discards the token. Git needs scope `repo`.
  `gh auth token` (the `--from-gh` path) normally carries `repo` already;
  the device flow must request `repo read:user`, and the exchange endpoint
  must check the `X-OAuth-Scopes` response header and warn (not fail) when
  `repo` is absent — login-for-identity must keep working for members who
  never touch git.
- **Attach is owner-or-admin (Plan 4):** minted credentials are always the
  **session owner's**. An admin attached to another user's session
  therefore pushes as the owner — documented trust-your-team note, matching
  the existing posture.
- **Assumption:** GitHub's noreply email convention
  (`<id>+<login>@users.noreply.github.com`) is acceptable attribution
  without requesting the `user:email` scope. (Standard; used by gh itself.)

## 4. Proposed design

### 4.1 Session RPC — the primitive (one mechanism, three consumers)

`relay.ControlEvent` grows into a tagged envelope:

```
ControlEvent { Kind string; ID uint64; ... }        // existing event kinds keep ID 0
  requests  (runnerd→sessiond): kind "req:<method>", ID > 0, Payload json
  responses (sessiond→runnerd): kind "resp", same ID, OK bool, Payload json
```

- **Downstream path** (new): `Hub.SendControl(payload)` writes a
  FrameControl toward the session; `ServeSession`'s read loop dispatches
  inbound FrameControl to a handler registered via
  `ServeSessionWithControl`'s new handler argument (constructor wiring,
  like `NewHubWithControl` — no post-start mutation).
- **Correlation** lives at each initiator: controld's session-RPC caller
  keeps a pending map (same shape as runner dispatch: id → chan, timeout,
  conn-death fail-fast); sessiond answers inline (its handlers are quick:
  run a git command, read a socket).
- **rwire** grows `ToRunner{Type:"session_rpc", Session, Ref→n/a, RPC
  {Method, Payload}}` and results carry the response payload — runnerd is a
  pure forwarder between the runner conn and the hub's control channel,
  with per-hop timeouts.
- **In-sandbox origin** (credential helper): sessiond listens on a unix
  socket `/workspace/.rainier/agent.sock` (0700, owned by the session
  user). The helper is the sessiond binary itself re-invoked as
  `sessiond git-credential-helper` (git's helper protocol on stdin/stdout;
  static binary already in every image — no new artifact). Helper → unix
  socket → sessiond → **upstream** RPC (sessiond-initiated requests use the
  same envelope with its own ID space; runnerd forwards to controld as
  `FromRunner{Type:"session_req"}`; controld answers via `session_rpc`
  response routing). Methods v0: `mint_git_credential`, plus
  controld-initiated `diff`, `push_files`, `pull_files`.

*Alternative considered — separate socket/port per concern (a credentials
port, a diff exec path):* rejected; every new path would re-solve
correlation, timeouts, and single-writer discipline that the control channel
already solves once. *Alternative — reuse the attach/dial-back plane for
RPCs:* rejected for control-sized payloads (a full pairing + dial-back per
mint is heavy); the attach plane remains the answer if push/pull ever needs
true streaming (see §4.5).

### 4.2 Credential vault

```
credentials (migration 0004):
  user_id text, provider text        -- PK (user_id, provider); "github" only in v0
  ciphertext bytea, nonce bytea      -- sealed access token (seal.go)
  refresh_ciphertext bytea NULL, refresh_nonce bytea NULL   -- for expiring-token providers; unused by classic GitHub OAuth
  status text                        -- valid | needs_refresh
  scopes text                        -- last observed X-OAuth-Scopes, informational
  obtained_at, expires_at NULL, last_verified_at, last_used_at, updated_at
```

- **Login stores instead of discarding:** `POST /v1/auth/github` upserts the
  credential (sealed), records scopes, sets `valid`. Device flow requests
  `repo read:user`; missing `repo` → stored anyway + response carries a
  warning field the CLI prints ("git operations will prompt for refresh").
- **Mint path** (`mint_git_credential`): load owner's credential → status
  `needs_refresh` → refuse with the named-action error. Optimistic use: the
  token is returned without a per-mint GitHub round-trip; a failed git
  operation whose stderr smells like auth (401/403 from GitHub) triggers
  sessiond to report `credential_rejected` upstream, which flips status to
  `needs_refresh` — the *next* operation gets the clear refusal, and
  `rainier creds` shows it immediately. (*Alternative — verify against
  GitHub on every mint:* rejected; it adds a GitHub call per git operation
  and still races revocation. Verification happens on login, on refresh,
  and lazily on observed failure.)
- **Refresh UX:** `rainier login --refresh github` re-runs the token
  acquisition (`--from-gh`/`--token`/device flow) and upserts; `rainier
  creds` lists provider/status/scopes/last_verified/last_used. API:
  `GET /v1/credentials` returns the CALLER's rows only (metadata, never
  values — same write-only discipline as secrets).
- **TokenSource semantics** are internal shape, not a new dependency:
  the vault read + status check is our TokenSource; refresh-token flow
  slots into the nullable columns when a provider with expiring tokens
  (GitHub App mode) arrives.

### 4.3 GitHub connector execution

- **Resolution (controld):** createSpec expands the env's `github`
  connectors (session `repos` overrides win; explicit `[]` = none) into
  `rwire.Spec.Repos []RepoSpec{Owner, Name, BaseBranch, SessionBranch,
  Dir}`; `SessionBranch = rainier/<session-name>` (id suffix when unnamed);
  `Dir` = repo name, deduped. Create fails loudly (409, named action) when
  the owner has no `valid`-or-`needs_refresh` github credential at all.
  Attribution fields ride the Spec: `GitAuthorName`, `GitAuthorEmail`
  (noreply form, from users.github_id + login).
- **Boot order (sessiond wrapper chain):** workspace init → `setup` (only
  when dispatched; cacheable, pre-clone — MUST NOT depend on repo content,
  documented) → **clone stage** → `init` (every session, post-clone) →
  exec agent. Each stage streams on the PTY; each failure reports a staged
  control event — the existing `setup_failed` generalizes to
  `stage_failed{stage: setup|clone|init, rc, tail}` (wire-compatible:
  `setup_failed` remains an alias runnerd still accepts).
- **Clone mechanics:** `git clone --branch <base> -- https://github.com/
  <owner>/<name>.git <dir>` then `git checkout -b <session-branch>`, with
  `GIT_CONFIG_GLOBAL=/workspace/.rainier/gitconfig` exported to the whole
  child chain. sessiond writes that gitconfig at boot: `credential.helper =
  /usr/local/bin/sessiond git-credential-helper`, `user.name`,
  `user.email`, `push.default = current`. Tokens therefore never appear in
  any file; git asks the helper per operation, the helper asks controld,
  and the token lives for the duration of the git process.
- **Egress:** resolution appends `github.com` + `codeload.github.com` +
  `objects.githubusercontent.com` to the session's egress allowlist when
  repos are present (documented, visible in the session row).

### 4.4 The `init` hook

`environments.init` (+ `init_timeout_sec`) — migration 0004 alongside the
vault. NOT part of `setup_hash` (cache identity unchanged). Rides
`rwire.Spec.Init/InitTimeoutSec`; driver injects `RAINIER_INIT_B64` +
timeout the same way setup rides today; wrapper runs it after clones,
streamed; `stage_failed{init}` → session `failed`. Cold resume re-runs
`init` like it re-runs setup — same idempotency note in docs.

### 4.5 push/pull (one-shot, bounded)

`rainier push <local-dir> <session>:<path>` tars locally (cap 256MiB
compressed, refuse over), streams as chunked session-RPC requests
(`push_files{Seq, Data(1MiB), Done}`) → sessiond untars under `/workspace`
only (path must resolve inside it; reject `..`). `pull` mirrors. Progress
line client-side. *Alternative — attach-plane streaming (pairing +
dial-back):* the right shape for unbounded transfer, deferred until the cap
hurts; the RPC chunking is 200 lines, the streaming plane is a task-week.

### 4.6 Diff endpoint

`GET /v1/sessions/{id}/diff` (auth: team-visible like other reads) →
controld session-RPC `diff` → sessiond runs, per repo,
`git -C <dir> fetch -q origin <base> && git -C <dir> diff --stat
origin/<base>...HEAD` (bounded 30s, output capped 64KB) → aggregated
`{repos: [{repo, base_branch, session_branch, stat}]}`. Scratch/no-repo
sessions → `{repos: []}`. No fleet-chip work in v0 (adapters plan).

### 4.7 Riders

- **Crash preserves workspace:** runnerd's hub-death crash path calls a new
  `driver.DestroyContainer` (container only); the volume survives, orphaned
  until the session's `rm` — controld's destroy dispatch for a dead-but-
  placed session instructs `driver.RemoveWorkspace(sessionID)` (new method;
  derives the volume name) after container removal. Reconcile's
  terminal-orphan destroy keeps FULL destroy (those are duplicates, not
  crashes).
- **child_exited:** sessiond already knows (`s.Exited()`); on child exit it
  sends control event `child_exited{code}`; controld stores
  `sessions.child_exit_code` (nullable, migration 0004); `ls` renders state
  `running` with exit annotation (`running (exited 0)`) — display only, no
  state-machine change; the row stays attachable (scrollback) per the
  attach-on-failed philosophy.
- **`--since` replay fix:** diagnose and fix the client-side forwarding
  (found in acceptance: server log intact, viewer receives screen only);
  regression test = e2e attach with since=0 asserting pre-restart history.
- **`/v1/me` id:** additive `id` field; CLI owner-preference reads it at
  login instead of the `new`-response cache.

## 5. Edge cases and failure handling

- Owner has no github credential and the env has a github connector →
  create refused (409) naming `rainier login` — before any row is created
  (same pre-insert discipline as missing secrets).
- Credential revoked mid-clone → clone fails; `stage_failed{clone}` carries
  git's stderr tail; session `failed`; sessiond also emits
  `credential_rejected` → vault flips `needs_refresh` → `rainier creds` and
  the next create both name the fix.
- Repo the user cannot access → same path; the error is GitHub's own text.
- Clone timeout: per-repo bound (default 10m, env-overridable later if
  needed); timeout → `stage_failed{clone}`.
- Helper called with a non-github host → helper answers nothing (git falls
  through to its other helpers/anonymous); we only ever answer for
  `github.com`.
- Admin attached to another's session runs `git push` → owner's identity;
  documented (§3).
- push/pull path escapes (`..`, absolute outside /workspace) → refused
  in-sandbox; size cap enforced client-side AND sessiond-side.
- RPC hop failures (conn death mid-mint) → timeout at controld's pending
  table; git sees a helper failure; retrying the git command is safe and
  the natural user action.
- Session RPC vs a busy control channel: chunked push competes with
  terminal frames on one conn — the shared writer keeps frames intact;
  chunk pacing (ack every N) keeps a push from starving the PTY stream.

## 6. Scalability and extensibility notes

- The session RPC envelope is the extension point Plans 6–7 ride (tunnel
  setup handshakes, broker file ingest). Methods are a closed set per
  build; unknown methods answer a typed error (fail closed).
- The vault's `(user_id, provider)` key + refresh columns are the GitLab /
  GitHub-App slots; `git-provider` stays un-abstracted until a second
  provider forces the seam (deletion test).
- Chunked push/pull is deliberately narrow (cap + one-shot); the streaming
  upgrade path (attach-plane pairing) is named, not built.
- Egress additions are per-session and visible; a future org proxy/registry
  story doesn't change the shape.

## 7. Verification strategy

Seams: the session RPC (both directions) with scripted peers; the vault via
storetest extensions; the helper protocol as pure functions + a socket
round-trip test; clone/init via the wrapper chain composition tests
(sessiond) + fleet rehearsal for real git.

1. Unit: vault lifecycle (store→mint→reject→refresh), RPC correlation +
   timeout + conn-death, helper stdin/stdout protocol, gitconfig
   composition, RepoSpec expansion incl. overrides and `[]`.
2. e2e (fake driver, scripted sessiond): create-with-connector carries
   Repos+attribution; mint round-trip end-to-end over real websockets;
   credential_rejected flips status and the next create 409s with the named
   action; diff RPC; push/pull chunk round-trip; child_exited annotation;
   crash-preserves-volume (fake driver records).
3. Fleet rehearsal (gated on `gh`): create a throwaway private repo via
   `gh repo create`, env with github connector + init hook, full cycle —
   clone on session branch, commit, push (attribution asserted via
   `git log --format='%an %ae'`), diff endpoint, push/pull a directory,
   `rainier creds`, teardown incl. repo deletion. Skips loudly without gh.
4. GCE acceptance: criteria 1–9 recorded in the runbook.

## 8. Open questions

None blocking — all scope calls resolved with Josh 2026-08-30 (auto-push →
Plan 6; `init` hook in; auto session branches in). One watch item, not a
question: if the `repo` scope warning (§4.2) turns out to fire for
`--from-gh` users in practice, the login UX may need a `--scopes` hint —
decide on evidence from dogfooding.

## 9. Decisions log

2026-08-30, with Josh: shadow-branch auto-push deferred to Plan 6 (this plan
builds its prerequisites); `init` hook included (the npm-install slot;
per-session, post-clone, never cached); automatic session branches
(`rainier/<session-name>`) adopted. Earlier session decisions carried in:
vault = lifecycle-aware, stdlib AES-GCM in Postgres, OAuth-refresh prompts,
no external vault service (ladder documented); durability direction makes
the crash-preserves-volume and child_exited riders first-class.
Jace: session RPC as one bidirectional primitive on the existing control
channel (three consumers now, broker later); optimistic mint with lazy
revocation detection; helper = sessiond subcommand over a workspace unix
socket; GIT_CONFIG_GLOBAL in the workspace (read-only rootfs constraint);
noreply attribution without extra scopes; chunked bounded push/pull with the
streaming upgrade path named; `stage_failed` generalization of setup_failed.

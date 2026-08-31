# Environments — Design (Rainier v0, Plan 4)

**Date:** 2026-08-29 · **Status:** Draft for Josh's review · **Author:** Jace
**Parent spec:** `2026-08-27-rainier-design.md` §9 (environments), §8 (credentials — deferred to Plan 5)
**Predecessors:** Plans 1–3 merged and dogfooding on GCE (criteria 1–3, 5–7 passed; 4 in progress).

## 1. Problem statement

Sessions today run one stock image (bash + git + curl). Real work needs real
toolchains, repeatably: "a session on my project" should mean the right image,
the right tools installed, the right egress allowlist, and the project's
secrets — without paying setup time on every create. The spec's answer is the
**environment** (§9); this plan builds it, reorganized around a decision made
with Josh (2026-08-29): **the environment is the primary reusable object, and
external systems plug into it as typed connectors** — GitHub first (Plan 5),
with the laptop-as-resource-provider family (files, tunnels, browser) designed
into the vocabulary now and built in Plans 6–7.

**Success criteria (measurable):**

1. `rainier env create myapp --image node:22 --setup ./setup.sh --egress registry.npmjs.org,github.com` then `rainier new --env myapp` boots a session with the toolchain present.
2. The FIRST session on an environment runs setup live, streamed to the attached terminal; a snapshot is cached; the SECOND session boots from cache with no setup run — measurably faster (record both times on rainier-1).
3. Session work in `/workspace` survives warm suspend AND cold park (stop + resume) — the session volume exists and persists.
4. Env-declared secrets are injected as env vars into sessions of that env, stored encrypted in Postgres, never readable via the API after write.
5. An environment with `placement: rainier-1` always places there; placement to a non-existent runner queues with a visible reason.
6. Editing an environment never mutates a running session (sessions pin their resolved image at create).
7. Everything green in CI; acceptance run on the GCE fleet recorded in the runbook.

## 2. Goals and non-goals

**In scope:**

- Environment object: name, base image, setup script, egress allowlist,
  secret refs, connector list (vocabulary only, see §4.2), placement hint.
- Env CRUD: REST (`/v0/environments`), store schema, CLI (`rainier env
  create/ls/show/update/rm`).
- Setup pipeline: first-session setup streamed to the terminal → driver
  `Snapshot()` caches the env image → later sessions boot the cache; cache
  invalidation by content hash of (image, setup script).
- Session volume: `/workspace` as a real docker volume with create/destroy
  lifecycle in the driver (work must survive cold park).
- Team secrets: encrypted-at-rest in Postgres (controld-held key), injected
  as env vars (spec's honest v0: agent-readable, documented).
- Placement hint honored by the scheduler (the hardware/local-runner story).
- Image pre-pull on runners when a snapshot is cached (spec §9 fast path).
- devcontainer.json: `image` field read as a hint at `env create --from-devcontainer` (full spec compat explicitly later).

**Non-goals (explicit, all discussed with Josh 2026-08-29):**

- GitHub connector implementation — credential path, clone-at-boot, branch
  conventions, `rainier push/pull`, diff endpoint (Plan 5).
- The local broker: laptop file ingest at create, hardware tunnels, browser
  control tiers (Plans 6–7; vocabulary reserved here).
- Continuous two-way file sync (skipped indefinitely; one-shot push/pull is
  the model — if it ever returns, embed mutagen, don't write one).
- Warm pools, memory snapshots, K8s driver, per-env RBAC.

## 3. Context and constraints

- **Model decision (Josh, 2026-08-29):** connectors (including repos, later)
  are **defaults declared on the environment, overridable at session
  create**. Sessions stay the unit of attach/lifecycle; environments are the
  reusable template. `rainier new --env myapp` is the daily driver; scratch
  and cross-repo sessions remain first-class. (Extends spec §9's
  session-carries-repos model; §9 amended, not fought.)
- **Sessions are free terminals** (Josh): nothing here restricts what a user
  or agent does inside; environments only provision.
- Existing seams this rides on: `driver.Spec.Image` (per-session image already
  honored), `driver.Snapshot()` (exists; ref-collision minor from the final
  review gets fixed here), the scheduler's `freeCapacity`/`pickRunner`
  (placement hint slots in), `rwire.Spec` (grows env fields), the reconcile
  loop (setup state rides the existing creating→running machinery).
- Postgres-only state rule holds: secrets encrypted in PG (AES-GCM via a key
  from controld config — `RAINIER_SECRETS_KEY`, generated at install), no
  vault dependency in v0.
- Laptop-family connectors must be **degradable attachments**: a session
  never depends on the laptop being awake (criterion-4 soul constraint).
  This shapes the vocabulary now even though brokers come later.

**Assumptions:** docker `commit`-based snapshots are acceptable env caches in
v0 (they capture setup-installed toolchains; `/workspace` volume content is
deliberately NOT in the snapshot). Setup scripts are trusted team content
(they run as the session user inside the sandbox, egress-limited).

**Amendment (acceptance-run outcomes, 2026-08-29):** the build measured three
interactions the design missed, all fixed and test-pinned:

1. A `--read-only` rootfs (Plan 2 hardening) means `docker commit` captures
   NOTHING — so sessions dispatched **with** a setup script run with a
   writable rootfs (the first build per env edit runs at the script's own
   trust level); cache-booted and scratch sessions keep full hardening.
2. `docker commit` bakes the container's env block into the image config —
   which would re-trigger setup on cache boots and, far worse, publish
   **decrypted secret values** in the cached image. The driver's Snapshot now
   strips a per-session key list via `--change 'ENV K='`: the secret env
   keys, the setup vars, and the driver-injected identity (RAINIER_SESSION,
   RAINIER_DIAL, and both cases of the proxy vars, whose URLs carry the
   session id as userinfo). Runners record env KEYS (never values) for this.
3. Cacheable install paths are `$HOME` and the stock image's dedicated
   writable prefix `/opt/rainier-env` (on PATH). `/usr/local/bin` stays
   root-owned even during writable builds so a prompt-injected agent in a
   first-build session cannot tamper the sessiond binary into the team's
   cache (threat model §10) — pinned by a test proving uid 1000 cannot write
   it. User-level cache poisoning remains inherent to any shared build cache,
   the same class as a malicious package.

## 4. Proposed design

### 4.1 The environment object

```
environments (Postgres):
  id           text pk            -- env_<16hex>
  name         text unique        -- CLI handle
  image        text               -- base OCI ref
  setup        text               -- shell script, may be empty
  setup_hash   text               -- sha256(image + "\0" + setup); cache key
  egress_allow jsonb              -- default allowlist for sessions
  secret_refs  jsonb              -- [name, ...] referencing secrets table
  connectors   jsonb              -- typed list, §4.2
  placement    text nullable      -- runner name hint; null = scheduler's choice
  snapshot_ref text nullable      -- cached env image (per-fleet, v0 single-arch)
  created_at / updated_at

secrets (Postgres):
  name text pk, ciphertext bytea, nonce bytea, created_at
  -- AES-256-GCM, key = RAINIER_SECRETS_KEY (32B, controld config).
  -- API: PUT /v0/secrets/{name} (admin), names listable, values never returned.
```

Sessions gain `environment_id` (nullable — scratch sessions keep working) and
pin `resolved_image` at create (env edits never touch running sessions;
success criterion 6).

### 4.2 Connector vocabulary (reserved now, built across Plans 5–7)

`connectors` is a list of discriminated objects. Plan 4 validates shape and
stores them; only Plan 5+ gives them behavior. Defined types:

| type | fields (v0 sketch) | built in | notes |
|---|---|---|---|
| `github` | repo, base_branch | Plan 5 | clone-at-boot + credential helper; env-declared defaults, session `--repo` overrides |
| `files` | paths[] | Plan 6 | one-shot ingest from the user's broker at create; degradable |
| `tunnel` | name, target_host, target_port | Plan 6 | laptop/broker port bridged to session localhost; hardware (MAVLink, ROS, serial-over-TCP) |
| `browser` | tier: dedicated \| extension | Plans 6–7 | Tier A: CDP tunnel to a DEDICATED profile. Tier B: extension-mediated daily-profile actions (per-site grants, visible actions — the Gusto case). Raw CDP against a daily profile is never offered. |

Unknown connector types are rejected at the API (fail closed) so old servers
never silently ignore a connector a client relied on.

### 4.3 Setup pipeline and snapshot cache

Create flow for a session with `environment_id`:

1. controld resolves the env. If `snapshot_ref` is set AND `setup_hash`
   matches the hash the snapshot was built from → `resolved_image =
   snapshot_ref`, ordinary create, no setup.
2. Otherwise `resolved_image = env.image` and the create dispatch carries
   `setup` + a `setup:true` marker. sessiond (already PID 1, already owns the
   PTY) writes the script to `/workspace/.rainier/setup.sh` and runs it as
   the session's first child, **streamed like any other output** — an
   attached viewer literally watches provisioning (spec §9). Exit 0 → sessiond
   starts the normal shell/agent child and reports `setup_done` up the
   existing event path; controld asks the runner for `Snapshot()` and stores
   `snapshot_ref` + the hash. Non-zero exit → session state `failed` with the
   tail of the setup log in `error` (the full log is in the event log —
   attach `--since 0` shows everything).
3. Concurrent first-creates on one env: both run setup (wasteful, harmless);
   last snapshot wins — a guarded store update (`WHERE setup_hash = $expected`)
   prevents a stale snapshot overwriting a newer env edit. No locks, no
   coordination; team-scale.
4. Snapshot ref naming fixes the final-review minor: refs become
   `rainier-env:<env_id>-<setup_hash[:12]>` — content-addressed, collision-free
   across runnerd restarts, and stale ones are prunable by prefix.
5. Pre-pull: when controld stores a new `snapshot_ref`, it sends a
   fire-and-forget `prepull` command to every connected runner (new rwire
   message; runners `docker pull` in the background). Session creates never
   wait on pre-pull — it's a warm-path optimization only.

### 4.4 Session volume

The driver's `Create` gains a named volume `rainier-ws-<session_id>` mounted
at `/workspace` (rw; root stays read-only, tmpfs `/tmp` stays). `Destroy`
removes it; cold park (stop) preserves it — that's the point. `List`/`Recover`
are unaffected (volume follows the container's label). The contract suite
gains: write file → cold suspend → resume → file present; destroy → volume
gone.

### 4.5 API and CLI

REST (all following the Plan 3 envelope/authZ conventions — mutations
admin-only for envs and secrets, reads team-visible):

- `POST/GET /v0/environments`, `GET/PATCH/DELETE /v0/environments/{id}`
  (DELETE refuses while non-terminal sessions reference it — 409).
- `PUT /v0/secrets/{name}` (admin; value write-only), `GET /v0/secrets`
  (names + timestamps only), `DELETE /v0/secrets/{name}`.
- `POST /v0/sessions` gains `environment` (name or id) — resolved server-side;
  explicit `image`/`egress_allow` on the session still win (overrides).

CLI: `rainier env create|ls|show|update|rm`, `rainier secret set|ls|rm`,
`rainier new --env <name>` (plus existing flags as overrides).
`rainier env create --from-devcontainer [path]` reads `image` as a hint and
says exactly what it ignored.

### 4.6 Placement hint

`environments.placement` (runner name). Scheduler: if set, candidate set is
that runner only — full → session queues with reason `waiting for runner
<name>` (visible in `ls`); disconnected → queues the same way. This is the
hardware story's first half: a runnerd next to the drone bench + an env
pinned to it = sessions run at the hardware, today, with Plan 3 machinery.

## 5. Edge cases and failure handling

- Setup script hangs: bounded by a per-env `setup_timeout` (default 15m);
  sessiond kills the child, session `failed` with a clear error.
- Image pull failure / bad image ref: create fails via the existing
  `result{ok:false}` path → `failed` with detail.
- Env edited between snapshot and session create: hash mismatch → setup
  re-runs (correct, slower); guarded snapshot update prevents stale caching.
- Secrets key rotation: v0 = documented manual re-encrypt procedure (admin
  re-`PUT`s values); losing `RAINIER_SECRETS_KEY` loses secret values only,
  nothing else (runbook note).
- Env deleted while sessions reference it: refused (409). Sessions hold
  `resolved_image` anyway, so even a race leaves them bootable.
- Runner without the cached snapshot (joined after caching): create on it
  falls back to `docker pull` of the snapshot ref — wait: `docker commit`
  snapshots are LOCAL to the runner that made them (no registry in v0).
  Ruling for v0: `snapshot_ref` is per-runner — the cache lives where it was
  built; placement prefers snapshot-holding runners when free (a scheduler
  tiebreak), other runners rebuild from setup (correct, slower, logged).
  A shared registry is the v1 upgrade and is noted in the doc, not built.
- Secrets in `docker inspect`: env vars are visible to anyone with docker
  access on the VM — already true of the whole v0 threat model (§10),
  restated in docs.

## 6. Scalability and extensibility

- Per-runner snapshot cache + placement tiebreak keeps the common case (one
  active runner, or env pinned) fast without a registry; the registry
  upgrade path is additive (`snapshot_ref` becomes pullable, tiebreak
  becomes unnecessary).
- Connector vocabulary is the extension point for Plans 5–7; adding a type
  is additive (schema validates unknown types closed at the API but the
  storage is jsonb — a migration-free vocabulary).
- Placement hint is deliberately a single runner name in v0 — labels/affinity
  arrive when a real need does.

## 7. Verification

- Store contract suite extensions (env + secrets CRUD, guarded snapshot
  update); driver contract: volume lifecycle + cold-park persistence.
- Setup pipeline e2e (in-process, fake driver): first-create streams setup
  and snapshots; second-create skips setup; env-edit invalidates; setup
  failure lands `failed` with log tail.
- Secrets: encrypt/decrypt round-trip, API never returns values, injected
  env visible in-session (e2e).
- Placement-hint scheduler tests (pinned runner full/disconnected → queued
  with reason).
- GCE acceptance: criteria 1–7 of §1 run on rainier-1 and recorded in
  `docs/deploy-gce.md`'s notes (setup-vs-cache timings included).

## 8. Open questions

None — all three resolved by Josh 2026-08-29 (see §9): flat team-wide
secrets; setup as the session user with OS packages baked into the base
image; roadmap P5→P7 confirmed, with Plan 5's credential storage upgraded to
a **lifecycle-aware credential vault**: per-user, provider-typed credentials
(GitHub first), encrypted at rest with `expires_at`/`last_verified`/`status`
metadata; a stale or revoked credential is surfaced explicitly (CLI +
in-session error naming `rainier login --refresh <provider>`) and refreshed
by re-running the provider's OAuth flow, never by silent failure. Connectors
declare which vault credential they need, making the vault the credential
backend for the whole connector family (GitLab etc. additive later).

## 9. Decisions log

2026-08-29, with Josh: environments are the primary object; connectors
(github/files/tunnel/browser) are env-declared defaults with session
overrides; sessions are unrestricted terminals — we ship conveniences, not
restrictions; continuous two-way sync skipped (one-shot push/pull model);
hardware = first-class ergonomics via tunnels AND local-runner placement;
browser control is two-tier — dedicated-profile CDP tunnel (A) and
extension-mediated daily-profile actions with per-site grants (B); raw CDP
against a daily profile is never offered. Plan 4 = environments core (this
doc); GitHub connector and the credential path move to Plan 5.
2026-08-29, with Josh (round 2): secrets are one flat team-wide namespace in
v0; setup runs as the session user (OS packages belong in the base image —
hardening kept); roadmap P5 (GitHub connector + credential VAULT + push/pull
+ diff) → P6 (local broker: files ingest, tunnels/hardware, browser Tier A)
→ P7 (browser Tier B extension) confirmed; the vault prompts OAuth-driven
refresh when credentials expire or are revoked.
Jace: per-runner snapshot cache with placement tiebreak (no registry in v0);
content-addressed snapshot refs; secrets AES-GCM in PG under a
controld-config key; env edits never touch running sessions.

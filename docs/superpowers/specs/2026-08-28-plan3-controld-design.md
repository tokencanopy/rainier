# controld + rainier CLI — Design (Rainier v0, Plan 3 of 5)

**Date:** 2026-08-28 · **Status:** Draft for Josh's review · **Author:** Jace
**Parent spec:** `2026-08-27-rainier-design.md` (§3 controld, §7 control plane, §10 failure model)
**Predecessors:** Plan 1 (sessiond core), Plan 2 (runnerd fleet) — both merged.

## 1. Problem statement

The fleet today is one VM driven by dev tools: runnerd serves a localhost HTTP API
(`runnerctl`), sessions register over the relay, `rattach` connects through runnerd
directly. There is no identity, no durable state (ids die with the runnerd process),
no placement across machines, and no way to reach the fleet from off the box.

Plan 3 turns this into a product-shaped system: a control plane (**controld**) that
makes N runner VMs one fleet behind one authenticated API, with Postgres as the only
durable store, and a real **`rainier` CLI** replacing `runnerctl`/`rattach` — while
preserving the spec's failure invariant: **sessions outlive everything else**.

**Success criteria (measurable):**

1. `rainier login && rainier new && rainier attach` works from Josh's laptop against
   a GCE e2-medium over Tailscale, from a cold VM in under an hour of ops.
2. Kill controld mid-attach; restart it; `rainier attach` reconnects to the same
   session with full scrollback. The agent process never noticed.
3. Kill runnerd on a VM with live sessions; restart it; sessions re-register and are
   attachable. No container is destroyed.
4. A session survives the laptop sleeping overnight; reattach next morning shows the
   live TUI.
5. Burst 10 creates against a fleet with 4 free slots: 4 run, 6 sit visibly `queued`,
   and the queue drains as capacity frees — no failed creates, no lost sessions.
6. A fresh VM running runnerd with the join token appears in the fleet and receives
   placements with zero controld config changes or restarts.
7. Egress R4 closed: a session reaches an allowlisted host through egressd and
   cannot reach anything else (verified by an acceptance script).

## 2. Goals and non-goals

**In scope (decided with Josh, 2026-08-28):**

- controld: versioned REST API, Postgres, session registry + state machine,
  least-loaded placement, runner join protocol, terminal-plane relay.
- runnerd inversion: dials controld outbound; local HTTP surface remains as the
  dev/debug path (same pattern as sessiond's Plan 1 listener).
- Identity: GitHub device-flow login, config-file allowlist, bearer tokens,
  admin/member roles. (Git *credential minting* stays in Plan 4.)
- Egress R4: session network `internal: true` + proxy env wiring, with enforcement
  acceptance tests.
- Carried hardening (invariant violations found in the Plan 2 code):
  sessiond redial-with-backoff; runnerd registry rebuild from container labels.
- `rainier` CLI: `login`, `new`, `ls`, `attach`, `suspend`, `resume`, `rm`,
  `snapshot`; absorbs runnerctl + rattach.
- Deploy: compose for dev/CI; scripted single-VM GCE setup (e2-medium + Tailscale)
  as the dogfood target.

**Non-goals (explicit):**

- Event-plane WebSocket (deferred to the dashboard/adapters plan — REST polling
  only; decided 2026-08-28).
- Repos/git checkout, git credential helper, GitHub App mode (Plan 4).
- Adapters, transcripts, permissions API, web dashboard, mobile (Plan 5+).
- Multi-replica controld deployment, autoscaling runners, cross-runner session
  mobility, queued *resumes* (all v1 — but see §6 for what this design does now so
  they stay bounded projects).
- Terraform module; TLS/domain/LB setup (Tailscale is the blessed v0 exposure).

## 3. Context and constraints

**Interfaces preserved from Plans 1–2** (unchanged unless a task says otherwise):
`driver.Driver`/`Spec`/`Handle`/`Snapshot`; `relay.Hub`/`ServeSession`/`Conn`/
`Frame`; `wire.ClientMsg`/`ServerMsg` (client wire protocol — the CLI speaks
exactly what rattach speaks today); `session.*`. The runner-API contract suite
keeps passing untouched.

**Spec rules in force:** rule 3 (all reachability outbound: sessiond → runnerd →
controld; clients talk only to controld); state rule (ephemeral state in process
memory reconstructed from reconnection, durable state in Postgres, no second
store); single-team install (no tenant column).

**Assumptions (confirmed):**

- Docker driver labels every container `<label>=<session_id>` and can list by
  label — registry rebuild is feasible without new driver surface (add `List` +
  label→id recovery, or derive from `Inspect`; plan decides the minimal shape).
- Sessions on an `internal: true` Docker network can still reach the host gateway
  IP (runnerd's register listener and egressd bind there). This is how Docker
  internal networks behave (no masquerade/default route; the bridge itself
  remains) — **verified by an early spike task in the plan before anything is
  built on it.**

  **Amendment (spike outcome, 2026-08-28):** holds on native Linux dockerd (the
  GCE production target — the bridge gateway is on-subnet) and FAILS on
  VM-backed docker (Docker Desktop/colima: the host is reachable only via
  off-subnet NAT, which `internal: true` removes — breaking the register dial
  too). Ruling: **platform-split posture** — `internal: true` is the canonical
  compose shape, enforced and verified on Linux (CI + the GCE acceptance);
  VM-backed-docker dev machines get an explicit, auto-detected `internal: false`
  override — the same posture v0 already had, now loud instead of silent.
  Dev/prod enforcement parity (dual-homed egressd + containerized runnerd) is
  ledgered for v1. Secondary findings adopted: the session image carries curl
  (BusyBox wget ignores `https_proxy`); egressd accepts
  `Proxy-Authorization: Basic` (URL-userinfo `http://<session-id>:@…`)
  alongside Bearer — how env-var proxy flows carry session identity.
- A shared GitHub OAuth app (device flow enabled) will exist with its client ID
  baked into the OSS binary, gh-style. Creating it is Josh's action (§8).

**New assumption this design rests on (Josh, 2026-08-28):** a small team with
bursty session spawning is the design load. Concretely: tens of sessions, a
handful of runners, sub-second control-plane QPS — but create bursts that exceed
momentary capacity must degrade to a visible queue, and adding capacity must be
a data-plane-only operation.

## 4. Proposed design

### 4.1 Component and module map

```
laptop                          team's cloud
──────                          ────────────────────────────────────────────
rainier CLI ── HTTPS/WSS ──▶ controld ◀── outbound WSS ── runnerd (per VM)
                                │  ▲                        │ │
                             Postgres│                      │ └─ egressd
                                     └─ attach dial-back ───┘
                                        (outbound from runnerd)
sessiond → runnerd register/relay: unchanged from Plan 2.
```

New modules (codebase-design: each is a deep module; the interface is the test
surface):

- **`internal/controld`** — the control plane. Interface:
  `New(st Store, cfg Config) *Server` exposing `Handler() http.Handler` (client
  API + runner endpoints on one mux). Everything else — scheduler, runner
  connection registry, attach pairing, reconciler — is implementation, exercised
  through the HTTP/WS surface plus a fake runner in tests.
- **`controld.Store`** (interface, defined by the consumer) —
  users/tokens/sessions/runners persistence. Two adapters = a real seam:
  `internal/controld/pgstore` (pgx, embedded migrations) and an in-memory fake
  for tests. Mirrors the `driver.Driver`+fake pattern already in the repo.
- **`internal/rwire`** — runnerd↔controld message types + framing. Sibling of
  `internal/wire` (client↔session). Pure types, versioned (§4.3).
- **runnerd dial mode** — in `internal/runnerd`: `RunAgent(ctx, controldURL,
  token, ...)` drives the *same* core create/op/registry/hub code the HTTP
  handlers drive today. Two fronts over one core (HTTP dev surface + controld
  protocol) — a real seam, no logic duplicated.
- **`cmd/rainier`** + **`internal/cli`** — command dispatch, config file, REST
  client. rattach's raw-mode/ws attach loop is extracted to a shared package
  (`internal/attachio`) used by both `cmd/rainier attach` and `cmd/rattach`
  (kept as the dev tool).

Deleted/absorbed: `runnerctl` stays as a dev tool against runnerd's local
surface; nothing user-facing depends on it after this plan.

### 4.2 Runner protocol: control connection + dial-back attach

runnerd maintains **one outbound WSS control connection** to
`/v1/runners/connect` (runner token auth), with reconnect + jittered backoff.
On (re)connect it **announces**: runner name, capacity, and every live session
(id, state) — rebuilt from container labels if runnerd itself just restarted.

Over the control connection (JSON messages, `internal/rwire`):

- controld → runnerd: `create{session_id, spec}` · `destroy{session_id}` ·
  `suspend{session_id, warm}` · `resume{session_id}` · `snapshot{session_id}` ·
  `dial_attach{attach_id, session_id, since, cols, rows, target_url}`
- runnerd → controld: `announce{...}` (on connect) · `result{req_id, ok, detail}` ·
  `event{session_id, state}` (registered/died/suspended/resumed) ·
  `capacity{used, total}` (on change) · heartbeat.

**Attach path:** client opens WSS `/v1/sessions/{id}/attach?since=` at controld →
controld parks the socket under a fresh `attach_id`, sends `dial_attach` down the
owning runner's control conn → runnerd dials `target_url`
(`/v1/runners/attach-back?attach_id=`) outbound and feeds that socket straight
into its existing `hub.AttachClient` (the ws is a `relay.Conn`; since/cols/rows
come from the message) → controld splices the two sockets as a dumb byte pipe.
The client speaks `wire.ClientMsg/ServerMsg` end to end — identical bytes to
attaching to runnerd directly, so `internal/attachio` needs no relay awareness.

**Alternative considered — single multiplexed connection** (all attach traffic
framed inside the control conn): rejected. It would double-mux terminal bytes
(relay frames inside another frame layer), put bulk output on the same TCP
stream as control messages (a slow viewer or bulk paste head-of-line blocks
`create` dispatch), and require new flow-control code. Dial-back reuses
`relay.Hub` verbatim, isolates each viewer's backpressure onto its own
connection, and its explicit `target_url` is precisely what makes multi-replica
controld possible later (§6). Cost: one extra outbound socket per active viewer —
negligible at team scale.

**Alternative considered — controld dials runnerd:** violates spec rule 3
(runners must be reachable only outbound; teams shouldn't open inbound ports on
runner VMs). Not viable.

### 4.3 Wire/versioning stance

- Client REST/WS surface: `/v1/...`, additive-only evolution; pre-GA status noted
  in the API doc (licenses field additions without ceremony, not renames).
- `rwire` carries `proto: 1` in the announce; controld rejects unknown majors
  with a close reason naming both versions. runnerd and controld ship from one
  repo, but a fleet mid-upgrade will briefly run mixed versions — the reject
  must be legible in `runnerd`'s log, not a silent reconnect loop.

### 4.4 REST API (client-facing, bearer auth)

Error envelope everywhere: `{"error": {"code", "message"}}` with branchable
codes (`invalid_request`, `unauthenticated`, `forbidden`, `not_found`,
`conflict`, `no_capacity`, `session_not_ready`, `runner_unreachable`,
`internal`). JSON in/out, `Content-Type` explicit, `X-Request-Id` accepted/
generated/returned and attached to logs. `Cache-Control: no-store` on all GETs
(volatile, private). Unknown body fields rejected on writes.

| Endpoint | Notes |
|---|---|
| `POST /v1/auth/github` | Unauthenticated (the explicit exception). Body `{access_token}` → controld calls GitHub `/user`, checks allowlist, mints opaque bearer `rnr_…` (hash stored). Returns `{token, user}`. |
| `GET /v1/me` | Identity + role. |
| `POST /v1/sessions` | `{name?, image?, cmd?, egress_allow?}` → **202** `{session}` + `Location`. Accepts `Idempotency-Key` (CLI always sends one; unique index makes retries safe). 202, not 201: creation is asynchronous by design — the row is `queued`/`creating`; readiness arrives via state. |
| `GET /v1/sessions` | Filters `state=`, `runner=`; cursor pagination (`limit` capped at 100), stable sort `created_at desc, id`. Team-visible (spec: trust-your-team). Terminal states hidden unless `all=true`. |
| `GET /v1/sessions/{id}` | Full row incl. `state`, `runner`, `reachable`, `last_event_at`. |
| `DELETE /v1/sessions/{id}` | Destroy; on `queued` it cancels before dispatch. 204. Owner or admin. |
| `POST /v1/sessions/{id}/suspend` | `{warm}` (default true). 409 `conflict` while `creating`. |
| `POST /v1/sessions/{id}/resume` | 409 `no_capacity` if the pinned runner is full (§4.7). |
| `POST /v1/sessions/{id}/snapshot` | Synchronous, bounded timeout → `{ref}`. |
| `GET /v1/runners` | Fleet capacity view: name, connected, used/total, last_seen. |
| `WS /v1/sessions/{id}/attach?since=` | Terminal plane. Bearer in header. |
| `GET /healthz` | Unauthenticated liveness, no internals. |

Session ids are opaque and non-enumerable: `sess_<128-bit random hex>`, minted
by controld, persisted before dispatch. (Replaces runnerd's guessable `sess-N`,
which Plan 2's ledger already called out; runnerd's dev surface keeps its own
counter, prod path always receives controld's id in `create`.)

Authorization is object-level: reads are team-wide by design (named exception,
per spec §7); mutations check owner-or-admin in the handler. Names are unique
per owner among non-terminal sessions (409 otherwise) so `rainier attach <name>`
is unambiguous; ids remain canonical.

### 4.5 Identity and tokens

- **User login:** CLI runs GitHub's device flow itself against the baked-in
  client ID (`--client-id` override for teams with their own app) — the GitHub
  token goes GitHub → laptop → *the team's controld*, transiting nothing we
  operate (spec compliance). controld verifies via `GET /user`, checks the
  allowlist, mints its bearer, and **discards the GitHub token** (v0; Plan 4
  revisits storage when git minting needs it). `rainier login --from-gh` shells
  out to `gh auth token` for the solo/dogfood path; `--token` covers headless.
- **Allowlist, not first-come-admin:** controld config lists
  `admins: [github logins]`, `members: [...]`. No magic first-login promotion —
  fail closed; an empty allowlist means nobody can log in and controld says so
  at startup.
- **CLI token storage:** `~/.config/rainier/config.json`, `0600`
  (`{server_url, token}`). OS keychain is a ledgered nicety, not v0.
- **Runner join:** one shared runner token in controld config (env/file), sent
  by runnerd on connect, compared by hash. Per-runner minted tokens and
  revocation are v1 (single-team VPC; the token gates fleet membership, not
  user data). Rotation = config change + restart, documented.

### 4.6 State: Postgres schema and the session state machine

Tables (embedded SQL migrations, version-tracked):

- `users(id, github_id unique, login, role, created_at)`
- `api_tokens(id, user_id, token_hash unique, created_at, last_used_at)`
- `sessions(id, owner_id, name, image, cmd jsonb, egress_allow jsonb,
  state, runner_id nullable, idempotency_key unique nullable, error text,
  created_at, updated_at, last_event_at)`
- `runners(id, name unique, capacity_used, capacity_total, connected bool,
  last_seen_at)` — a *cache* of announced fact for `GET /v1/runners`; truth is
  the live connection set, reconciled on every (dis)connect.

Lifecycle state machine (single `state` column; every transition is one guarded
UPDATE … WHERE state = expected, so a racing op loses cleanly with `conflict`):

```
queued ──▶ creating ──▶ running ⇄ suspended_warm
   │           │           │  ⇄ suspended_cold
   ▼           ▼           ▼
canceled    failed       dead ──(rows kept; hidden from default ls)
                           ▲
running/suspended ──destroy──▶ destroyed
```

**Reachability is not a state.** `reachable` is derived per-read from the owning
runner's connectedness — a disconnected runner makes its sessions
`running, reachable:false`, not some ninth state. This keeps the machine small
and honest: controld genuinely doesn't know more than "last event said running;
runner currently away."

**Create is write-ahead durable (invariant).** `POST /v1/sessions` commits the
row (state `queued`) before the 202 is written and before any dispatch. A
successful create therefore always exists in Postgres, whatever happens next:
controld dies pre-dispatch ⇒ the scheduler re-picks `queued` rows on startup;
the dispatch message is lost ⇒ reconciliation re-dispatches (§4.8, idempotent
by id); and the inverse hole is closed structurally — a runner can only ever
run sessions whose controld-minted id was persisted first, so an announced id
Postgres doesn't know is by definition an orphan.

### 4.7 Placement, capacity, and burst behavior

- **Slot accounting:** `running` + `suspended_warm` occupy slots (paused
  containers hold memory); `suspended_cold` occupies none but **pins its
  runner** — the volume lives there and session mobility is v1. Resume targets
  the pinned runner; if it's full, `409 no_capacity` with a message naming the
  runner (queued resume is ledgered v1).
- **Placement:** connected runners with free slots, pick max-free, tie-break by
  runner name (deterministic for tests). No affinity/labels in v0 — deliberately
  narrow; environments (Plan 4) is where placement constraints would grow.
- **Queue:** creates with no capacity sit in `queued` (visible in `ls` with a
  reason). One scheduler goroutine wakes on capacity events (announce, session
  terminal transition, runner connect) plus a safety tick; dispatches oldest
  queued first. Fairness beyond FIFO is deliberately out — team scale.
- **No message queue (decided with Josh, 2026-08-28).** Postgres *is* the
  durable queue: `queued` rows are the backlog; delivery to runners rides the
  control connection with at-least-once semantics via reconcile-on-reannounce,
  made safe by id-idempotent dispatch. A broker (Pub/Sub/NATS/Redis streams)
  would add no guarantee this pair doesn't already provide, while breaking the
  spec's state rule and adding an operational dependency for every self-hosting
  team. **River** (the Postgres-native job queue) was considered separately
  since it respects the state rule (asked by Josh, 2026-08-28): rejected for
  the scheduler because dispatch is not a background job — it is a placement
  decision over live connection state that must execute on the process holding
  the runner's WebSocket, which anonymous SKIP-LOCKED workers cannot express —
  and because River's job rows would be a second source of truth beside
  `sessions.state`, retrying in parallel with reconciliation. River is the
  named candidate for v1's genuinely background jobs (event-log streaming,
  snapshot checkpoints, TTL sweeps); adopting it then is purely additive. If
  dispatch ever measures as a bottleneck, the seam is the scheduler's dispatch
  function — swapping its transport touches no API or schema.
- **Burst by adding metal:** a fresh VM + runnerd + join token = capacity, no
  controld touch (success criterion 6). This is the horizontal-scaling story for
  the data plane, and a future autoscaler is just automation of this join path.

### 4.8 Reconciliation (the failure invariant, mechanized)

On runner announce, controld reconciles Postgres against announced reality:

| Postgres says | Announce says | Action |
|---|---|---|
| running/suspended on R | present, state agrees | touch `last_event_at` |
| running/suspended on R | present, state differs | adopt announced state (runnerd is truth for liveness) |
| running/suspended on R | **absent** | mark `dead`, record error `lost at announce` |
| creating on R | absent | re-dispatch `create` (id-idempotent, see below) |
| queued | — | scheduler dispatches normally |
| — (unknown id) | present | orphan: log loudly, instruct destroy (Postgres is the source of desired state) |

- **Idempotent dispatch:** `create` carries controld's `session_id`; runnerd
  checks its registry/labels first and answers with current state instead of
  double-creating. Safe across control-conn flaps and controld restarts.
- **controld restart:** runners redial (backoff+jitter), announce, reconcile.
  Live attaches die and clients re-run attach (spec accepts attach downtime).
  Nothing else moves.
- **runnerd restart (carried hardening):** rebuild the registry from
  `docker ps --filter label` + inspect before connecting to controld, so the
  announce is truthful. Sessions' sessionds redial `/register` with backoff.
- **sessiond redial (carried hardening):** replace dial-once-`log.Fatalf` with
  a retry loop (boot and steady-state). Consequence: **hub death no longer
  implies container death**, so runnerd's hub-death handler changes from
  "crash ⇒ destroy" to: inspect the container via the driver — still running ⇒
  keep the entry, await re-register; gone ⇒ today's dead path. This is the one
  deliberate semantic change to Plan 2 behavior, and it is what makes success
  criterion 3 true.

### 4.9 Egress R4

Flip the session network to `internal: true`; verify (spike, §3 assumption)
that sessions still reach the host gateway for runnerd's register listener and
egressd; ensure the driver injects `HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY` from
the spec on every create. Acceptance script: allowlisted host fetch succeeds via
egressd (audit line appears), non-allowlisted host fails, register dial works.
Kernel-level enforcement stays a later driver iteration (spec §3), but after R4
the documented posture is finally true instead of aspirational.

### 4.10 CLI and deployment

CLI UX (the call sites we're designing for):

```
rainier login                      # device flow; prints code + URL
rainier new --image ghcr.io/x -- claude   # 202; prints id, starts attach by default
rainier ls [--all]                 # table: id, name, state, runner, reachable, age
rainier attach <id|name>           # raw-mode TUI, since=0 replay then live
rainier suspend|resume|rm|snapshot <id|name>
```

`new` attaches immediately by default (spec §9: attach and stream everything —
the user watches boot output; `--detach` opts out). Exit code and `Ctrl-]`
semantics match rattach.

Deployment: dev/CI = compose (controld + postgres + fleet-up runnerd, as
today). Dogfood = `scripts/gce-up.sh`: one e2-medium (decided 2026-08-28),
Docker + Tailscale, controld+postgres via compose, runnerd+egressd host-mode,
CLI pointed at the tailnet name. No LB, no domain, no cert management in v0.

## 5. Edge cases and failure handling

- **Create burst races:** id minted + row inserted before dispatch; the Plan 2
  "starting-state 409" pattern generalizes — ops on `queued`/`creating` return
  409 except DELETE-as-cancel, which flips `queued → canceled` atomically
  (guarded UPDATE; if dispatch won the race it proceeds as a destroy).
- **Attach to a not-yet-running session:** controld waits bounded (10 s, mirrors
  runnerd today) for `running` + registered, else `503 session_not_ready`. CLI
  retries with a spinner during `new`'s auto-attach.
- **Client socket dies while runnerd is dialing back:** pairing entries have a
  TTL; whichever side arrives second finds the pair gone and closes. Both
  sockets always die together (splice ends on either EOF).
- **Runner conn flaps mid-op:** `result` never arrives → op times out →
  surfaced as 504-equivalent `runner_unreachable`; reconciliation on
  re-announce trues up state. All ops are id-idempotent, so retry is safe.
- **Two controld-side writes racing (suspend vs destroy):** guarded UPDATEs;
  loser gets `conflict`. No advisory locks needed at this scale.
- **Postgres down:** API returns 503; runner conns stay up but dispatch pauses;
  sessions unaffected (mirrors the controld-down story; state rule keeps
  Postgres the only thing that can be down *besides* controld).
- **GitHub down:** login fails closed with a clear message; existing bearers
  keep working (no per-request GitHub calls).
- **Fail-closed defaults:** empty allowlist ⇒ no logins; missing runner token ⇒
  runnerd refuses to start in dial mode; egress default-deny unchanged.

## 6. Scalability and extensibility

**Designed load (Josh, 2026-08-28): small team, bursty spawns.** Numbers:
control QPS ≈ interactive (<10/s worst case), terminal relay KB/s per viewer,
tens of sessions, single-digit runners. One controld replica on the e2-medium
is comfortably an order of magnitude above this.

**Horizontal scaling posture — scale the data plane now, keep the control
plane replica-safe for later:**

- Data plane scales by adding runner VMs (dynamic join, §4.7). This is the axis
  bursty load actually stresses, and it ships in this plan.
- controld is written to the **replica-safety rules**: every durable fact in
  Postgres; every in-memory structure (runner conns, attach pairings, scheduler
  queue) rebuildable from reconnection/DB; `dial_attach.target_url` explicit
  per message rather than derived from global config, so a specific replica can
  name itself; attach pairing state strictly local to the replica holding the
  client socket (never needs sharing). Going multi-replica later is then an LB
  + runner-conn-routing project (a `runner→replica` table and a forward hop),
  not a protocol change. Deliberately **not built now** — one adapter would be
  a hypothetical seam.
- Postgres as the scaling backstop is per the spec's state rule; event
  summaries and log streaming (v1 durability work) are the first things that
  would pressure it, and they arrive with their own plan.

**Extensibility seams this design leaves clean:** `Store` (pg/fake), `Driver`
(unchanged — K8s driver lands under runnerd with zero controld changes),
`rwire` versioned proto, `/v1` additive REST, placement isolated in one
function (environment/affinity constraints slot in at Plan 4+), CLI client
package reusable by the dashboard's dev proxy later.

## 7. Verification strategy

Seams under test (fewer, at real caller crossings): the REST/WS surface with a
fake runner behind it; the runner protocol with a real runnerd against a fake
controld and vice versa; `Store` against both adapters; the driver contract
suite untouched.

1. **Unit:** placement (table-driven), state-machine guarded transitions,
   reconciliation table (§4.8 — each row is a test), auth middleware, device
   verification against a fake GitHub.
2. **Contract (api-design bar):** per endpoint — happy path, validation
   rejection, authZ denial, response-shape regression pin. Error envelope and
   pagination shape pinned once, shared.
3. **Protocol:** announce/reconcile/dispatch/dial-back over real websockets
   with fake driver; kill each side mid-flight and assert idempotent recovery.
4. **e2e (compose, CI):** controld + postgres + two runnerds (fake + docker
   driver): burst-over-capacity queue-and-drain; kill-controld-reattach;
   kill-runnerd-rebuild-reannounce; sessiond redial after network flap;
   egress R4 acceptance script.
5. **Cloud acceptance (manual, the milestone):** success criteria 1–7 executed
   on the e2-medium from the laptop; overnight-sleep reattach.

Most likely regressions: Plan 2's hub-death semantics (deliberately changed —
§4.8's inspect-before-destroy needs its own test forcing both branches), and
the resize-first attach contract through the new double relay (pin with an
end-to-end golden attach test reusing Plan 1 fixtures).

## 8. Open questions

1. ~~GitHub OAuth app~~ **Resolved (Josh, 2026-08-28):** dogfood via
   `rainier login --from-gh` (reuses the `gh` CLI token) for now; creating the
   shared device-flow OAuth app is deferred until a first external user needs
   it. Success criterion 1 reads `login --from-gh` accordingly; the device-flow
   code path still ships (it's the same exchange endpoint), just without a
   baked-in client ID until the app exists.
2. ~~GCP project~~ **Resolved (Josh, 2026-08-28):** a new GCP project named
   **`rainier`** hosts the fleet (e2-medium first). `gce-up.sh` targets it.
3. Deferred-not-open (ledgered): OS keychain for CLI token; per-runner join
   tokens + revocation; queued resume; scheduler fairness beyond FIFO;
   `config-ssh` (spec lists it CLI-v0, but it has no design yet and no Plan 3
   dependency — proposing it rides with Plan 4's repo/SSH-adjacent work).

No open questions remain that block writing the implementation plan.

## 9. Decisions log

2026-08-28, with Josh: identity auth **in** Plan 3 (git minting Plan 4); event
plane **deferred** to first real consumer; egress R4 **in**; milestone = one
GCE **e2-medium** + Tailscale, laptop CLI over the internet; architecture must
keep **horizontal scaling** first-class — resolved as: data plane scales by
runner join now, controld follows replica-safety rules but ships single-replica;
**no message queue** — Postgres rows + reconciliation are the durable queue
(§4.7), create is write-ahead durable (§4.6); dogfood auth via **`--from-gh`**
(shared OAuth app deferred); fleet lives in a new GCP project **`rainier`**.
Jace (charter: technical design): dial-back attach transport over single-mux
(§4.2); allowlist-config auth over first-login-admin (§4.5); reachability as
derived fact, not state (§4.6); cold sessions pin runners, resume 409s when
full (§4.7); sessiond redial + inspect-before-destroy semantic change (§4.8).

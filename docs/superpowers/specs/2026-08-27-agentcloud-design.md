# agentcloud — Design Spec

**Date:** 2026-08-27 · **Status:** Draft for review · **Author:** Jace (with Josh)
**Working name:** `agentcloud` (placeholder; final name TBD by Josh)
**Research basis:** four-track landscape research, 2026-08-27 (report: claude.ai artifact `2cc53a9a`)

## 1. Overview

agentcloud is self-hostable infrastructure that runs a developer's coding agents
(Claude Code, Codex, Gemini CLI, Goose, later OpenCode/pi) in the cloud while
feeling like local terminal sessions. A small team installs it in their own cloud
(GCP, AWS, Azure, or any Linux VMs); each agent runs in an isolated sandbox that
survives laptop sleep, network changes, and device switches. Users keep their own
AI subscriptions and their own GitHub identity.

### Goals (v1)

- Sessions survive any client disconnect; reattach is instant from any machine.
- Attach shows the agent's real TUI, byte-for-byte (terminal-first).
- Small-team install: one compose/Terraform setup in their cloud; Postgres is the
  only stateful service.
- Onboarding in minutes: GitHub device-flow login; agents authenticate with the
  user's own subscription through the vendor's own flow.
- Fleet visibility: glanceable status for 15+ concurrent sessions.
- Runtime is agent-agnostic and compute-agnostic by construction (runner API,
  adapter layer).

### Non-goals (v1)

Mobile client; K8s/gVisor driver; warm pools and memory snapshots; egress
token-injection and two-phase networking; OpenCode/pi adapters; GitLab/Bitbucket;
webhook-triggered sessions; hosted (SaaS) control plane; Cloudflare driver;
predictive local echo; session recordings / hash-chained audit; session sharing;
multi-tenant control plane. See §12 for the v2 line.

## 2. Locked decisions

| Decision | Choice |
|---|---|
| Repo model | Standalone repo, designed for small-team adoption (including Josh) |
| Deploy model | All-in-their-cloud: control plane + data plane in the team's cloud; single-tenant per install; VPC = tenant boundary |
| v1 substrate | VMs + Docker sessions behind the runner API; K8s + gVisor is a v2 driver |
| Agent layer | Agent-agnostic runtime; ACP as internal event vocabulary; pluggable adapters; universal PTY plane; per-session mode (TUI / ACP) |
| v1 adapters | Claude Code (TUI mode), generic ACP (structured mode), screen-diff fallback |
| Clients | Terminal-first: CLI + minimal web fleet dashboard in v1; mobile (responsive web + push) is the first fast-follow |
| Stack | Go for controld/runnerd/sessiond/CLI; TS/React for web |
| Auth | GitHub device flow (shared OAuth app client ID) as default; per-install GitHub App as opt-in org hardening; git-provider interface for future GitLab/Bitbucket |
| State rule | Ephemeral state in process memory, reconstructed from reconnections; durable state in Postgres; no Redis/second store until a measured bottleneck |

### Portability rules (hard constraints)

1. **sessiond runs inside the sandbox** (PID 1), dialing outward. A session is
   "any OCI container that runs sessiond." No host-side reach-in (`docker exec`,
   bind-mount assumptions).
2. **Snapshot is defined at the runner-API level as OCI image / workspace tar**,
   never as a Docker-specific mechanism.
3. **All reachability flows through outbound connections** (sessiond → runnerd →
   controld). Nothing above the driver may assume a session has a routable
   address. No `attach`/`exec` in the runner API.

These keep the future K8s+gVisor driver a bounded project (~2–4 weeks) with zero
client-visible changes.

## 3. Components

- **controld** (Go): control plane. HTTP API + three planes (§7), auth, session
  registry, GitHub token minting, Postgres. Stateless except Postgres; restarts
  recover by reconnection-and-reannounce from runnerd/sessiond.
- **runnerd** (Go): one per VM. Dials outbound to controld; pulls create/destroy
  jobs; manages Docker containers, session volumes, repo mirror cache, egressd;
  reports capacity/health.
- **sessiond** (Go, small static binary): PID 1 in every sandbox. Owns the PTY,
  runs the headless VT emulator, appends the event log, hosts the adapter,
  multiplexes everything over one outbound WebSocket.
- **egressd**: per-VM CONNECT proxy. Default-deny, per-session allowlist,
  destination audit log. Per-container iptables redirect.
- **CLI** (Go): `login`, `new`, `ls`, `attach`, `config-ssh`, device-flow auth.
- **Web dashboard** (TS/React): fleet view, read-only terminal, permission
  approvals, transcript view for ACP sessions.

Data flow: clients talk only to controld; controld talks only over connections
runnerd/sessiond opened outward. The team exposes controld's HTTPS port however
they like (LB+TLS, Tailscale, Cloudflare Tunnel).

**Session definition:** one agent process + its PTY + its sandbox + its own git
worktree/branch. A user runs many sessions; two sessions on one repo get
independent clones and branches. The session is the unit of attach, permissions,
diff, and lifecycle.

## 4. Runner API and the v1 Docker driver

Contract (controld → runnerd over the runner's outbound connection):

- `create_session(spec) → session_id` — spec: environment image (OCI ref), repo +
  base branch, resources, egress policy, adapter + mode, secret references.
- `suspend(id)` / `resume(id)` — semantics defined at the API level:
  **warm suspend** (processes in RAM, CPU freed) and **cold park** (processes
  stopped, volume retained; resume uses the agent's native session-resume).
- `snapshot(id) → OCI image / workspace tar` (portability rule 2).
- `destroy(id)`; capacity/health reporting upward.
- No attach/exec (portability rule 3).

Docker driver: one container per session — environment image, sessiond as PID 1,
non-root, `no-new-privileges`, default seccomp, CPU/memory limits, read-only root
except the session volume (worktree + agent home). Only network route is egressd.
Placement: capacity bin-packing across the team's VMs. Suspend mapping: warm =
`docker pause`; cold = stop + keep volume. Idle policy (configurable): warm
suspend ~10 min, cold park after a few hours, destroy only explicit/TTL.

## 5. sessiond — the session host

1. **Owns the PTY**: allocates it, spawns the agent CLI onto it, holds the
   controlling side forever; propagates resize (smallest-active-viewer wins in
   v1), delivers signals, reaps children. The agent cannot tell it isn't local —
   which also keeps the unmodified-CLI compliance property.
2. **Terminal state**: headless VT emulator maintains the current screen grid
   (incl. alternate screen) + ~10k-line scrollback ring. Attach = snapshot + live
   deltas; scrollback streams lazily. Never a byte-log replay.
3. **Event log**: every event (output frames, adapter events, lifecycle) appended
   with a monotonic sequence number on the session volume. Clients reconnect with
   `attach(session, since_seq)`; missed events replay from the log, then live.
   Survives cold park.
4. **Hosts the adapter** (§6) and multiplexes terminal + events + control over
   one outbound WebSocket to runnerd → controld; reconnects with backoff.

**Decision note — no tmux.** sessiond adopts tmux's process model (server-side
PTY owner, ephemeral viewers) but implements it directly: we need terminal state
as data (snapshot-on-attach, screen-diff status, permission-menu detection) and
a sequence-numbered event log, neither of which tmux exposes except via fragile
capture-pane/control-mode scraping; tmux also adds chrome/key interception that
risks TUI fidelity, and a C dependency in every sandbox image (portability rule
1). Users may still run tmux inside a session. Implementation basis:
`creack/pty` (MIT, de facto standard; handle Linux EIO-on-child-exit as EOF and
drive resize via Setsize) for PTY ownership, plus an embedded Go VT emulator —
leading candidate `charmbracelet/x/vt` (MIT, active, alternate screen +
scrollback; pinned version since it lives in Charm's experimental repo), with
`hinshun/vt10x` as minimal fallback. The §11 golden-fixture suite (real Claude
Code captures) is the acceptance gate for the emulator choice. Emulator
imperfections degrade snapshots/status detection only — live viewers always
receive the raw byte stream.

## 6. Adapter layer

Adapters normalize one agent's signals into **ACP vocabulary** (`message.delta`,
`tool.call`, `plan.updated`, `permission.requested`) plus fleet extensions
(`status.changed` over `working / awaiting-permission / awaiting-input / idle /
error / disconnected`, `diff.updated` from git vs session base, `cost.updated`),
and accept follow-up prompts + permission decisions. Compiled into sessiond,
selected in the session spec.

**Session modes** (chosen at create):

- **TUI mode**: agent runs interactively on the PTY (primary surface = the real
  TUI); events derived from side channels. The "feels local" mode.
- **ACP mode**: agent runs headless over ACP stdio; conversation is
  structured-first (CLI renders transcript + input line; web likewise). PTY plane
  still exists: attach drops into a shell in the session workspace.

Both modes ship in v1; usage decides emphasis.

**v1 adapters:**

1. **Claude Code (TUI)** — unmodified CLI on the PTY (the compliant subscription
   path). Transcript: tail the session JSONL. Status/notifications: hooks
   (`Stop`, `Notification`, `PreToolUse`, …). Cost: JSONL. Remote permission
   approval: sessiond recognizes the known prompt menu and injects the matching
   keystroke (bounded, documented scraping).
2. **Generic ACP (structured)** — spawn agent with its ACP flag; near-pure
   passthrough. Covers Codex, Gemini CLI, Goose, plus registry growth.
3. **Fallback (TUI)** — screen-stability diffing only: any CLI runs day one with
   working/idle status, no transcript.

Compliance note (load-bearing): Anthropic permits hosting the unmodified Claude
Code CLI where each user completes Anthropic's own login; it bans intermediating
claude.ai credentials, and subscription OAuth via the Agent SDK is not permitted.
Therefore Claude runs in TUI mode on subscriptions; the ACP bridge
(claude-code-acp, SDK-based) is only offered for API-key users.

## 7. Control plane

**Three planes:**

1. **Control (REST, versioned JSON):** `POST /sessions`, `GET /sessions` (fleet:
   status, repo/branch, pending-permission, cost, last activity),
   `POST /sessions/:id/messages`, `POST /permissions/:id/decision`,
   `GET /sessions/:id/transcript?since=`, `GET /sessions/:id/diff`, plus
   environments, repos, users.
2. **Event plane:** one WebSocket per client device carrying all the user's
   sessions' events, tagged by session. The dashboard is a pure consumer; the
   future mobile client is the same consumer with a different renderer.
3. **Terminal plane:** one WebSocket per attach — snapshot then frames; stdin +
   resize upstream. controld is a dumb relay pairing viewer connections with the
   session's sessiond connection.

**Auth:** default is GitHub **device flow** against a shared OAuth app client ID
baked into the OSS (gh-style). Tokens go directly GitHub → the team's install
(never transit anything the vendor operates) and are stored encrypted in the
install / CLI keychain. Web login uses the same OAuth app (browser flow).
`login --from-gh` reuses an existing `gh` token for solo/dogfood mode. Opt-in
hardening: a per-install **GitHub App** (manifest flow) for selected-repo
scoping and for system-initiated work (§8). Roles: admin (fleet, environments,
egress policy, GitHub config) and member (own sessions). Fleet view is
team-visible by default (trust-your-team v1).

**State:** Postgres holds users, sessions (spec + lifecycle + placement),
environments, permission decisions, and event summaries for the dashboard. Full
event logs live on session volumes. One install = one team; no tenant column.
No Redis (see state rule, §2).

## 8. Git provider, credentials, secrets

**Git provider interface** (auth flow, token minting, PR creation, webhooks);
GitHub is the v1 implementation; GitLab/Bitbucket are future providers.

**GitHub:**

- Default: user access tokens from device flow; the in-sandbox **git credential
  helper** requests tokens on demand from sessiond → controld. Tokens touch
  sandbox memory briefly, are never persisted, never in `.git/config`. Commits
  and PRs attribute to the actual human; authorization mirrors the user's real
  GitHub access.
- App mode (opt-in): per-install GitHub App via manifest flow; installation
  tokens (1 h, down-scoped to repo + `contents:write` + `pull_requests:write`)
  for system-initiated work (background PR creation; later webhook-triggered
  sessions). Bot-authored commits carry `Co-authored-by:` the human.
- Sessions push only their own branch by convention + audit in v1
  (proxy-enforced later); branch protections respected; PRs via API.

**Subscription login:** per user, per agent, a persistent **credential volume**
mounted into all their sessions. First session: user attaches and completes the
vendor's own login in the TUI (Claude OAuth; Codex device code). Credentials
never transit controld, never touch Postgres. API-key users use team secrets.

**Secrets & egress (v1 honesty):** egressd = CONNECT proxy, default-deny,
per-environment allowlist, destination logging (no payload MITM). Team secrets
encrypted in Postgres, injected as env vars at session start — agent-readable in
v1, documented as such. Token injection at the proxy + two-phase network are v2.

## 9. Environments, lifecycle, fast start

**Environment** (per repo, shareable): base OCI image + setup script + env/secret
refs + egress allowlist. First session runs setup live (streamed to the attached
terminal), then `snapshot()` caches the environment image; later sessions boot
from it. `devcontainer.json` `image` field read as a hint; full spec compat later.

**v1 fast path:** environment images pre-pulled to VMs on snapshot creation;
per-VM bare-repo mirror (worktree checkout in ~hundreds of ms, no network
clone); container start ~100 ms; agent CLI boot is the honest long pole (~few s).
**Attach immediately and stream everything** — user watches the session come up
within ~1 s even when total readiness is ~5 s. Instrument create-to-attach
latency from day one; warm pools/memory snapshots wait for measurements.

**Lifecycle:** idle → warm suspend (~10 min) → cold park (few hours) → destroy
only explicit/TTL. Warm resume near-instant; cold resume = agent native
`--resume` + event log, so context survives.

## 10. Security model & failure handling

**Threat model (v1):** defend against a misbehaving or prompt-injected agent —
not hostile co-tenants (single-team install; VPC is the tenant boundary).
Controls: hardened containers (§4), egressd default-deny, per-user credential
volumes, short-lived git tokens, authenticated client traffic. Known v1 gaps
(documented): env-var secrets agent-readable; no payload inspection; push-branch
discipline by convention.

**Failure invariant: sessions outlive everything else.**

- controld down → sessions unaffected; reconnect-and-reannounce on return;
  attach unavailable during outage (accepted).
- sessiond/container crash → runnerd restarts via cold-resume path.
- VM reboot → cold resume.
- VM loss → v1 durability is git: session branches push early and often
  (adapter-nudged). Unpushed work on a dead VM is lost in v1 (see §12).

## 11. Testing strategy

1. **VT emulator** golden tests against escape-sequence fixtures, including real
   Claude Code output captures (alternate screen, mouse, OSC).
2. **Adapters** against recorded JSONL/ACP fixtures.
3. **Runner-API contract suite that any driver must pass** (Docker now, K8s
   later) — makes the portability rules enforceable.
4. Compose-based e2e with a scripted fake agent: create → attach → disconnect →
   reattach → suspend → resume → snapshot.
5. Chaos runs: kill controld / kill sessiond / network flap; assert resume
   invariants. Real-agent smoke tests behind API keys.

## 12. v2 roadmap

- **Durability** (headline): (a) shadow-branch auto-push to
  `refs/sessions/<id>` every few minutes / per turn (restore = fetch);
  (b) event-log streaming through controld to Postgres/object storage
  (conversation survives total VM loss); (c) scheduled incremental
  `snapshot()` checkpoints to object storage + automatic rescheduling of
  sessions from last checkpoint on VM failure ("recovered, resumed from 3
  minutes ago") — also yields session mobility across VMs/backends. Process-state
  durability arrives only with the microVM memory-snapshot tier.
- Mobile: responsive web + web push (permission approvals, turn-complete).
- K8s + gVisor driver (agent-sandbox CRDs, warm pools); Firecracker snapshot
  tier; Cloudflare driver.
- Egress token injection, two-phase setup/agent network split.
- OpenCode + pi adapters; GitLab/Bitbucket providers; webhook triggers (CI-fix
  loops); predictive local echo; session recordings + hash-chained audit;
  session sharing; hosted control plane.

## 13. Open questions (Josh's call)

- Final product/repo name (placeholder: `agentcloud`).
- Open-source licensing posture (Apache-everything vs AGPL-core vs BSL) and
  pricing shape (orchestration seats — decided direction: never token margin).
- First wedge emphasis: solo prosumers vs small teams (design serves both; GTM
  ordering open).
- Relationship to agent-os: share the runner-API/WorkerBackend abstraction
  vs. keep them parallel initially.

## 14. References

- Research report (sources inline): claude.ai artifact `2cc53a9a-4815-4403-8d6e-6f97a1a07749`
- ACP: agentclientprotocol.com · Claude Code hooks/legal: code.claude.com/docs
- Prior art: Coder (architecture), Happy (mobile/E2E + MCP permission pattern),
  sshx (predictive echo), Zellij (web terminal), AgentAPI (screen-diff fallback),
  kubernetes-sigs/agent-sandbox (future K8s driver), Anthropic sandbox-runtime
  (egress patterns).

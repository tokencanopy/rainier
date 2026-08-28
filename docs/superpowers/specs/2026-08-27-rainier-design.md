# rainier — Design Spec

**Date:** 2026-08-27 · **Status:** Draft for review · **Author:** Jace (with Josh)
**Name:** Rainier (decided by Josh, 2026-08-27) · **Version:** this spec covers v0
**License:** Apache-2.0 · **Standalone:** separate from agent-os initially
**Research basis:** four-track landscape research, 2026-08-27 (report: claude.ai artifact `2cc53a9a`)

## 1. Overview

rainier is self-hostable infrastructure that runs a developer's coding agents
(Claude Code, Codex, Gemini CLI, Goose, later OpenCode/pi) in the cloud while
feeling like local terminal sessions. A small team installs it in their own cloud
(GCP, AWS, Azure, or any Linux VMs); each agent runs in an isolated sandbox that
survives laptop sleep, network changes, and device switches. Users keep their own
AI subscriptions and their own GitHub identity.

### Goals (v0)

- Sessions survive any client disconnect; reattach is instant from any machine.
- Attach shows the agent's real TUI, byte-for-byte (terminal-first).
- Small-team install: one compose/Terraform setup in their cloud; Postgres is the
  only stateful service.
- Onboarding in minutes: GitHub device-flow login; agents authenticate with the
  user's own subscription through the vendor's own flow.
- Fleet visibility: glanceable status for 15+ concurrent sessions.
- Runtime is agent-agnostic and compute-agnostic by construction (runner API,
  adapter layer).

### Non-goals (v0)

Mobile client; K8s/gVisor driver; warm pools and memory snapshots; egress
token-injection and two-phase networking; OpenCode/pi adapters; GitLab/Bitbucket;
webhook-triggered sessions; hosted (SaaS) control plane; Cloudflare driver;
predictive local echo; session recordings / hash-chained audit; session sharing;
multi-tenant control plane. See §12 for the v1 line.

## 2. Locked decisions

| Decision | Choice |
|---|---|
| Repo model | Standalone repo, designed for small-team adoption (including Josh) |
| Deploy model | All-in-their-cloud: control plane + data plane in the team's cloud; single-tenant per install; VPC = tenant boundary |
| v0 substrate | VMs + Docker sessions behind the runner API; K8s + gVisor is a v1 driver |
| Agent layer | Agent-agnostic runtime; ACP as internal event vocabulary; pluggable adapters; universal PTY plane; per-session mode (TUI / ACP) |
| v0 adapters | Claude Code (TUI mode), generic ACP (structured mode), screen-diff fallback |
| Clients | Terminal-first: CLI + minimal web fleet dashboard in v0; mobile (responsive web + push) is the first fast-follow |
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
  destination audit log. v0 mechanism (amended 2026-08-27): session containers
  live on an internal Docker network (no default route); egressd bridges it to
  the outside and sessions carry standard proxy env vars — the pattern proven
  in agent-os, portable to the K8s driver. Kernel-level enforcement (iptables /
  NetworkPolicy) hardens this in a later driver iteration.
- **CLI** (Go): `login`, `new`, `ls`, `attach`, `config-ssh`, device-flow auth.
- **Web dashboard** (TS/React): fleet view, read-only terminal, permission
  approvals, transcript view for ACP sessions.

Data flow: clients talk only to controld; controld talks only over connections
runnerd/sessiond opened outward. The team exposes controld's HTTPS port however
they like (LB+TLS, Tailscale, Cloudflare Tunnel).

**Session definition:** one agent process + its PTY + **its own sandboxed
filesystem** (the session volume). The volume may contain zero or more repo
checkouts, declared in the spec as `repos: [{repo, base_branch}, ...]` —
zero-repo scratch sessions and multi-repo (cross-repo task) sessions are
first-class; the common case is one repo. Each checkout is an independent clone
with a session branch per repo; two sessions on one repo never share files. A
session's identity is "an FS born from environment X with repos Y checked out."
The session is the unit of attach, permissions, diff, and lifecycle.

## 4. Runner API and the v0 Docker driver

Contract (controld → runnerd over the runner's outbound connection):

- `create_session(spec) → session_id` — spec: environment image (OCI ref),
  `repos: [{repo, base_branch}, ...]` (0..n), resources, egress policy,
  adapter + mode, secret references.
- `suspend(id)` / `resume(id)` — semantics defined at the API level:
  **warm suspend** (processes in RAM, CPU freed) and **cold park** (processes
  stopped, volume retained; resume uses the agent's native session-resume).
- `snapshot(id) → OCI image / workspace tar` (portability rule 2).
- `destroy(id)`; capacity/health reporting upward.
- No attach/exec (portability rule 3).

Docker driver: one container per session — environment image, sessiond as PID 1,
non-root, `no-new-privileges`, default seccomp, CPU/memory limits, read-only root
except the session volume (session FS incl. repo checkouts + agent home). Only
network route is egressd.
Placement: capacity bin-packing across the team's VMs. Suspend mapping: warm =
`docker pause`; cold = stop + keep volume. Idle policy (configurable): warm
suspend ~10 min, cold park after a few hours, destroy only explicit/TTL.

## 5. sessiond — the session host

1. **Owns the PTY**: allocates it, spawns the agent CLI onto it, holds the
   controlling side forever; propagates resize (smallest-active-viewer wins in
   v0), delivers signals, reaps children. The agent cannot tell it isn't local —
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
error / disconnected`, `diff.updated` per repo checkout (git vs its base
branch; the fleet view aggregates), `cost.updated`),
and accept follow-up prompts + permission decisions. Compiled into sessiond,
selected in the session spec.

**Session modes** (chosen at create):

- **TUI mode**: agent runs interactively on the PTY (primary surface = the real
  TUI); events derived from side channels. The "feels local" mode.
- **ACP mode**: agent runs headless over ACP stdio; conversation is
  structured-first (CLI renders transcript + input line; web likewise). PTY plane
  still exists: attach drops into a shell in the session workspace.

Both modes ship in v0; usage decides emphasis.

**v0 adapters:**

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
   status, repo/branch chips per session ("scratch" when none),
   pending-permission, cost, last activity),
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
team-visible by default (trust-your-team v0).

**State:** Postgres holds users, sessions (spec + lifecycle + placement),
environments, permission decisions, and event summaries for the dashboard. Full
event logs live on session volumes. One install = one team; no tenant column.
No Redis (see state rule, §2).

## 8. Git provider, credentials, secrets

**Git provider interface** (auth flow, token minting, PR creation, webhooks);
GitHub is the v0 implementation; GitLab/Bitbucket are future providers.

**GitHub:**

- Default: user access tokens from device flow; the in-sandbox **git credential
  helper** requests tokens on demand from sessiond → controld. Tokens touch
  sandbox memory briefly, are never persisted, never in `.git/config`. Commits
  and PRs attribute to the actual human; authorization mirrors the user's real
  GitHub access.
- App mode (opt-in): per-install GitHub App via manifest flow; installation
  tokens (1 h, down-scoped to the session's repo set + `contents:write` +
  `pull_requests:write`)
  for system-initiated work (background PR creation; later webhook-triggered
  sessions). Bot-authored commits carry `Co-authored-by:` the human.
- Sessions push only their own branch by convention + audit in v0
  (proxy-enforced later); branch protections respected; PRs via API.

**Subscription login:** each user gets one persistent **credential volume**
holding their agent home directories (`~/.claude`, `~/.codex`, …), mounted into
every session they own and nobody else's. Log in once per agent; all current
and future sessions are authenticated. Credentials never transit controld,
never touch Postgres. API-key users use team or per-user secrets instead.

**Agent login flows** (`rainier agent-login <agent>` starts a throwaway session
and attaches the user for a guided first login):

- **Claude Code:** `/login` in the real TUI prints Anthropic's OAuth URL —
  clickable in the user's local terminal because attach is their real terminal;
  authenticate in the local browser, paste the code back into the TUI. On
  headless Linux the credential lands in `~/.claude/.credentials.json` on the
  user's volume (supported no-keychain path). Alternative: user runs
  `claude setup-token` on their laptop and stores the long-lived token as a
  per-user secret (`CLAUDE_CODE_OAUTH_TOKEN`) — Anthropic's own headless
  mechanism.
- **Codex:** `codex login --device-auth` (code + URL, approve on laptop;
  credential → `~/.codex/auth.json`). Document the caveat: ChatGPT workspace
  admins must enable "Allow device code login". Additionally, the CLI supports
  **login port-forwarding**: forwarding an agent's localhost OAuth-callback
  port from the user's laptop into the sandbox during login, so ordinary
  browser-callback flows complete as if local — generalizes to any agent whose
  login assumes localhost.
- **Others:** Gemini CLI/Goose via device-style flows or API keys; OpenCode via
  per-provider API keys (its Anthropic subscription auth was disabled
  server-side by Anthropic — Claude subscriptions run through Claude Code on
  Rainier). Universal fallback: API keys as secrets.

Compliance recap: unmodified binaries; every login completes through the
vendor's own flow with the user's own account; device/paste-back codes pass
through the PTY as ordinary keystrokes; tokens live only on the user's volume
in the team's infrastructure. Ops notes for docs: concurrent sessions share one
agent home exactly as N local instances do; N parallel sessions on one Pro/Max
plan will hit vendor "ordinary individual usage" throttles — the dashboard
surfaces rate-limit state so it doesn't present as a Rainier failure.

**Secrets & egress (v0 honesty):** egressd = CONNECT proxy, default-deny,
per-environment allowlist, destination logging (no payload MITM). Team secrets
encrypted in Postgres, injected as env vars at session start — agent-readable in
v0, documented as such. Token injection at the proxy + two-phase network are v1.

## 9. Environments, lifecycle, fast start

**Environment** (commonly one per repo, shareable; defines image/setup/egress
while the session spec's `repos` list defines initial contents): base OCI image
+ setup script + env/secret
refs + egress allowlist. First session runs setup live (streamed to the attached
terminal), then `snapshot()` caches the environment image; later sessions boot
from it. `devcontainer.json` `image` field read as a hint; full spec compat later.

**v0 fast path:** environment images pre-pulled to VMs on snapshot creation;
per-VM bare-repo mirror (local clone with hardlinked objects in ~hundreds of
ms, no network clone); container start ~100 ms; agent CLI boot is the honest
long pole (~few s).
**Attach immediately and stream everything** — user watches the session come up
within ~1 s even when total readiness is ~5 s. Instrument create-to-attach
latency from day one; warm pools/memory snapshots wait for measurements.

**Lifecycle:** idle → warm suspend (~10 min) → cold park (few hours) → destroy
only explicit/TTL. Warm resume near-instant; cold resume = agent native
`--resume` + event log, so context survives.

## 10. Security model & failure handling

**Threat model (v0):** defend against a misbehaving or prompt-injected agent —
not hostile co-tenants (single-team install; VPC is the tenant boundary).
Controls: hardened containers (§4), egressd default-deny, per-user credential
volumes, short-lived git tokens, authenticated client traffic. Known v0 gaps
(documented): env-var secrets agent-readable; no payload inspection; push-branch
discipline by convention.

**Failure invariant: sessions outlive everything else.**

- controld down → sessions unaffected; reconnect-and-reannounce on return;
  attach unavailable during outage (accepted).
- sessiond/container crash → runnerd restarts via cold-resume path.
- VM reboot → cold resume.
- VM loss → v0 durability is git: session branches push early and often
  (adapter-nudged). Unpushed work on a dead VM is lost in v0 (see §12).

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

## 12. v1 roadmap

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

## 13. Decisions log & open questions

Decided (Josh, 2026-08-27): name **Rainier**; this scope is **v0** (roadmap in
§12 is v1); license **Apache-2.0**; Rainier stays a **standalone project**,
separate from agent-os initially. Pricing direction: orchestration seats, never
token margin.

Open (Josh's call): first wedge emphasis — solo prosumers vs small teams (the
design serves both; go-to-market ordering open). Exact pricing shape.

## 14. References

- Research report (sources inline): claude.ai artifact `2cc53a9a-4815-4403-8d6e-6f97a1a07749`
- ACP: agentclientprotocol.com · Claude Code hooks/legal: code.claude.com/docs
- Prior art: Coder (architecture), Happy (mobile/E2E + MCP permission pattern),
  sshx (predictive echo), Zellij (web terminal), AgentAPI (screen-diff fallback),
  kubernetes-sigs/agent-sandbox (future K8s driver), Anthropic sandbox-runtime
  (egress patterns).

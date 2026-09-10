# Rainier CLI v0 command contract

Status: accepted for hosted v0. This document is the authority for what
`rainier` offers, what each command promises, and which Cloud APIs each
promise depends on. The parser follows this file; where the two disagree,
this file is the bug report.

## 1. What the CLI is for

Hosted v0 serves one developer, one workspace, one paid Dedicated compute
plan, and several persistent coding sessions. The CLI exists for four jobs:

1. signing in,
2. checking readiness,
3. authenticating Claude Code or Codex,
4. creating and managing coding sessions.

Everything else — choosing a plan, paying, provisioning compute, creating or
renaming workspaces, connecting GitHub, administering environments and
secrets — belongs to the web application. The CLI never selects a plan,
starts a checkout, or provisions compute. When an action belongs to the web,
the CLI prints the **server-provided** destination and stops.

A new developer must be able to read the whole product surface off one help
screen. That is the design constraint that decides what `rainier --help`
contains; it is not a stylistic preference.

## 2. Public surface

`rainier --help` lists exactly this, and nothing else:

```
rainier login
rainier logout
rainier status [--verbose]

rainier new [--name NAME] [--agent claude|codex] [--detach] [-- CMD ARGS...]
rainier ls [--all] [--verbose] [--json]
rainier info <session> [--json]
rainier attach <session> [--view | --take]
rainier stop <session>
rainier delete <session> [--yes]

rainier agent login <claude|codex>
rainier agent status [--json]
rainier agent logout <claude|codex> [--yes]

rainier help [command]
rainier version
```

The happy path:

```
rainier login
rainier status
rainier agent login claude
rainier new --name foundation --agent claude
# Ctrl-] detaches; the session keeps running
rainier ls
rainier attach foundation
rainier stop foundation
rainier delete foundation
```

### 2.1 Compatibility aliases (hidden; documented under `rainier help all`)

| Alias | Canonical | Note |
| --- | --- | --- |
| `doctor` | `status --verbose` | temporary; scripts and runbooks still call it |
| `suspend` | `stop` | temporary; `--cold` accepted and ignored, `--warm` refused |
| `rm` | `delete` | temporary; keeps its existing non-interactive behavior — no prompt, no `--yes` needed |
| `agent ls` | `agent status` | temporary |

An alias prints a one-line deprecation note on stderr and then does exactly
what the canonical command does. `rm` is the one alias whose behavior is not
identical to its canonical command, and deliberately so: it has always
deleted without prompting, and scripts depend on that.

### 2.2 Advanced (dispatched, documented only under `rainier help all`)

`resume`, `snapshot`, `push`, `pull`, `creds`, `connection`, `secret`, `env`,
`context`, `workspace`, and the self-hosted `login` flags. These exist for
automation, self-hosted deployments, and administration. They are labeled
Advanced in `rainier help all` and never appear in the default help.

### 2.3 Removed completely

`diff`. It is gone from dispatch, help, docs, examples, tests, and
implementation. It is not replaced: Git is the source of truth for repository
and worktree state, and a developer or an agent runs Git inside the session.
Rainier grows no repository-diff abstraction.

The server route `GET /v0/sessions/{id}/diff` is out of scope for this
change. It is a published `/v0/` route with self-hosted consumers; retiring it
is an API decision, not a CLI one.

## 3. Sessions

### 3.1 Three facts, kept apart

A session has **three independent facts**, and the CLI never collapses them.
The control plane reports them separately because they are separate, and each
one fails on its own:

| Dimension | API field | What it answers |
| --- | --- | --- |
| **Session** | `state` | Is the sandbox coming up, up, stopped, or broken? |
| **Process** | `child_exit_code` | Has the command the session runs finished, and with what status? |
| **Connection** | `reachable` | Can the control plane talk to the runner holding it right now? |

Collapsing them loses information the API already has and the user needs. A
session whose agent exited is still a **running** sandbox: it holds its slot,
its filesystem is intact, and it is still attachable — calling it "finished"
tells somebody their session is over when it is not. A runner that dropped its
connection makes a session **unreachable**, not failed; the entitlement and the
work are still there, and treating the two alike would have somebody delete a
session over a network blip.

**Session lifecycle.**

| API `state` | Displayed |
| --- | --- |
| `queued`, `creating` | Starting |
| `running` | Running — **regardless of `child_exit_code`** |
| `suspended_warm`, `suspended_cold` | Stopped |
| `failed`, `dead` | Failed |
| `canceled` | Canceled |
| `destroyed` | Deleted |
| anything else | `Unknown (<the server's own word>)` |

`canceled` and `destroyed` are terminal records. They are absent from the
default list and appear under `--all`; when one is inspected by name it is
labelled accurately — a session somebody cancelled and one somebody deleted
are different events with different causes, and neither is "finished".

**Process.** `Exited (N)` when `child_exit_code` is present; `Running` when it
is absent and the session is running; `Paused` for warm suspension and `-`
for cold suspension or a sandbox that has not started. `Running` here means the child
process **exists** — it does not mean it is doing anything. An agent sitting at
a prompt and an agent mid-compile are the same fact, and the CLI does not imply
otherwise.

**Connection.** `Available` / `Unavailable`, from `reachable`. Unavailable is
never used as a lifecycle state.

**Unknown future states** stay unknown and carry the server's own word through.
They are never translated into another dimension's vocabulary: an unrecognized
lifecycle says nothing about the connection, and a reachable session in an
unknown state is still reachable.

### 3.1.1 Action eligibility

Eligibility is derived from the **raw API state's own transition rules**, never
from the display label. Two states that share a label can differ at the
endpoint, which is exactly why the label cannot decide this.

Read off the control plane:

| Action | Raw states the API accepts | Source |
| --- | --- | --- |
| attach | `running`; `failed` **only while its runner is connected** | `controlapp/attachments.go` `attachable` |
| resume | `suspended_warm`, `suspended_cold` | `controlapp` `ResumeSession` |
| suspend (stop) | **`running` only** | `controlapp` `SuspendSession` |
| delete | everything except `creating` (409 conflict) and `destroyed` | `controlapp` `DeleteSession` |

So:

- **Stop is not offered for a Starting session.** `queued` and `creating` share
  the label Starting and `SuspendSession` refuses both; claiming Stop because
  the label looked right would promise an operation the server rejects.
- **A running session whose child exited remains attachable and stoppable.**
  The server's rules are about the sandbox, not the process inside it.
- **`attach` is offered for `queued`/`creating`** because the CLI waits, and for
  the two suspended states because the CLI resumes first — each corresponds to
  a real API path the CLI drives.
- **A failed session's attach depends on `reachable`**, and that is the one
  place a connection fact legitimately decides an action, because the endpoint
  itself says so.
- **An unknown state yields `unknown`**, not a guess. The CLI makes no claim,
  refuses nothing locally, and lets the server be the authority.

### 3.2 Selectors

Every session command accepts three forms:

1. an exact `sess_...` id, used verbatim;
2. an unambiguous session name;
3. the literal `current`.

`current` resolves to the **opaque id** of the last session this CLI
successfully created or attached, recorded per server context in
`~/.config/rainier/config.json` as `current_session`. The id is persisted, never
the name, so renaming or recreating a name cannot silently retarget it.

- `current` is a keyword and is matched before any name lookup, so a real
  session named `current` is never reachable by that word. It stays
  addressable through its `sess_...` id, and `ls --verbose` prints the ids.
- An unset `current` fails with "no current session".
- A stale `current` (the id no longer exists) fails clearly and says to run
  `rainier ls`.
- An ambiguous name fails and lists each match's id and owner id. The CLI
  never picks silently. The one deterministic disambiguation it does apply is
  ownership: among same-name matches, the caller's own rows win over
  teammates'. That is a rule, not a guess, and it is unchanged from v0.0.x.

### 3.3 `rainier new`

```
rainier new [--name NAME] [--agent claude|codex] [--detach] [-- CMD ARGS...]
```

- With no `--env`, the session starts from the **workspace's default
  environment** (§5.2). A workspace with no resolvable default still creates a
  scratch session, as before, so self-hosted deployments are unaffected. An
  explicit `--image` opts out: it says "run exactly this image", and folding an
  environment's setup script in underneath one is how a session fails at boot.
- An idempotency key is generated automatically for every invocation. If create
  is not confirmed, the error gives that key for a retry with the same arguments
  and `--idempotency-key KEY`; mutations are not automatically retried after
  transport failures. Retry deletes by opaque ID, not a reusable name.
- `-- CMD ARGS...` runs an arbitrary command, unchanged.
- `--agent claude|codex` is resolved only from the server's launch catalog
  (§5.3). The CLI does not contain `claude` or `codex` command lines.
- `--agent` together with an explicit `-- CMD` is an invalid invocation
  (exit 2). Two answers to "what does this session run" is a mistake, not a
  precedence puzzle.
- A `workspace_not_ready` error creates no session. The CLI prints one
  actionable message and the server-provided onboarding destination (§5.1).
- `--detach` returns as soon as creation is accepted. Without it, `new` waits
  until the session is attachable and attaches.

### 3.4 `rainier ls`

Default columns: `NAME  STATE  PROCESS  CONNECTION  AGE` — one column per
dimension (§3.1). The exit code is never folded into the state cell; that is
what produced `running (exited -1)`.

- Default lists sessions the developer can still act on: every non-terminal
  row, including `stopped` ones and a `running` one whose child has exited.
- `--all` adds the terminal records — Canceled and Deleted.
- `--verbose` adds `ID`, `ENV`, the server's own `API STATE`, `RUNNER`, and any
  queue reason.
- Ordering is the server's, preserved across pages.

### 3.5 `rainier info <session>`

One authoritative view: the three dimensions on labelled lines, the server's
own `API state` beside them, timestamps, environment, and whether the session
can currently be attached, stopped or deleted — each with the reason when the
answer is no, so a person can tell "not yet" from "never".

Failure presence is included with fixed diagnostic guidance. Raw failure prose
is omitted because it can contain secrets unknown to the local credential store. Raw provider errors, terminal contents,
credentials, and internal database details are never shown.

A `Controller:` line reports who may type: `this device`, `another device`, or
`none`. It has three answers and no fourth. The API says whether somebody
holds control, never who — that is a fact about another person's session — so
"this device" is derived locally, from the generation this CLI was last
granted: nobody else can hold a generation without advancing past it, so a
live lease still at that number is this device's.

### 3.6 `rainier attach <session>`

Dispatched on the **raw** state, because the endpoint's rules are stated in raw
states and a display word groups states the endpoint treats differently.

- `running`: attach — including when its child has exited.
- `queued`, `creating`: attach waits, exactly as `new` does after a create.
- `suspended_warm`, `suspended_cold`: resume, then attach.
- `failed`: attach while its runner is still connected, which is what preserves
  setup-failure diagnosis. Otherwise a **connection** error saying so — the
  session failed and its runner has since gone are two facts, and the message
  keeps them apart because the second can come back.
- `dead`, `canceled`, `destroyed`: a clear result naming the accurate
  lifecycle.
- an unknown state: attempted; the server decides.
- Ctrl-] detaches locally and leaves the remote session running, and releases
  control as it goes, so the next attach claims it with no key press.
- `--since` remains the diagnostic replay and overrides every refusal above.

**Who may type.** At most one attached device controls a session at any
moment; every other attach is a viewer that receives the screen and the output
and whose input is discarded. This is invisible on one laptop and is the whole
point on a laptop and a phone.

- A plain `attach` claims control when nobody holds it — which is every
  single-device attach — and attaches as a viewer when somebody does, printing
  one line naming that another device has control and the key that takes it.
- `--view` never claims, and never types: it is held on the client from the
  first byte rather than from the server's answer, so it means the same thing
  against a server that does not implement conditional ownership — where a
  plain attach would take control unconditionally. Ctrl-\ still works; the
  flag is about what happens without one.
- `--take` takes control on attach, once, even from a live holder, and its one
  claim is spent by the first answer whatever that answer said: an attach that
  opened holding control does not take it back later, on its own, when
  somebody else takes it. `--view` and `--take` ask for opposite things and
  are refused together.
- **Ctrl-\** takes control inside a live attach and tells the device that had
  it, which drops to viewer and keeps showing output. Either side may take it
  back the same way. There is no confirmation prompt: the change is one key
  press away from being undone and the other side is told.
- Ctrl-\ is intercepted ONLY in an attach the server answered. Against a
  server that does not implement conditional ownership it is forwarded to the
  remote application as an ordinary byte, and the attach behaves exactly as it
  did before this existed.
- A viewer's resize is ignored. The terminal size follows the controller, so a
  phone watching does not squeeze a laptop's terminal to phone width.
- **Reconnect is conditional.** A controller that reconnects within its lease
  presents the generation it held and resumes control only while nobody took
  it; if somebody did, it comes back as a viewer and says so. A connection
  that merely dropped resumes: the attach released on its way out, so nobody
  holds control, and control nobody holds is claimed by whoever asks. A viewer
  stays a viewer. The CLI never claims control on its own — not on reconnect,
  and not in answer to a refusal.
- Control is a 30-second lease renewed every 5 seconds while the attach is
  live. A device that crashes without releasing holds control for at most the
  lease, after which the next attach claims it.
- A successful attach records `current` when it returns; a refused connection
  leaves it unchanged. The original context is retained across context switches.

### 3.7 `rainier stop <session>`

Stops a running session. This is the persisted (cold) stop; the warm/cold
distinction is not part of the normal interface.

- **Only a running session can be stopped** (`SuspendSession` accepts
  `control.StateRunning` and nothing else). The CLI says so before spending a
  round trip on the refusal.
- A running session whose child has exited is still stoppable.
- Idempotent: a session already in `suspended_warm` or `suspended_cold` reports
  that and exits 0. A warm suspension explicitly says capacity remains reserved;
  only a confirmed cold suspension claims resources were released.
- The CLI waits for the server's authoritative post-stop state and reports
  failure unless the server says the session is stopped. It never reports
  success on an unobserved or wrong final state, and never discards session
  state silently.
- **Billing.** Stop preserves the session and releases the session resources it
  was holding — the runner slot, its memory — for other sessions to use. It
  does **not** change what the developer pays: the Dedicated subscription stays
  active and continues to bill monthly whether sessions are running or stopped.
  Cancelling the subscription is a separate action, on the web. The help says
  this in as many words.

### 3.8 `rainier delete <session> [--yes]`

Permanently destroys the session. The message says so before it happens.

- On an interactive terminal, it asks for confirmation.
- Without a terminal it requires `--yes` and exits 2 otherwise. It never
  blocks waiting for input that cannot arrive.
- A 204 is "deleted"; a 202 is reported explicitly as accepted and still in
  progress.
- An already-removed session is reported as already gone, not as a failure.
- Terminal contents and other session data are never echoed.

## 4. Account and readiness

### 4.1 `rainier login`

- With no arguments and an existing context, it re-authenticates against that
  context's own server. Re-running it is always safe.
- With no arguments and no context, it uses the hosted default server if this
  build has one. The default is a build/configuration seam
  (`-ldflags -X main.defaultServer=...`, overridable by `RAINIER_SERVER`) and
  is **empty in source builds**: no unconfirmed production hostname is
  compiled in. With no default and no context, login exits 2 and says how to
  name a server.
- Self-hosted flags (`--from-gh`, `--token`, `--client-id`, `--server`,
  `--refresh`, `--context`, `--cloud`) are unchanged and documented under
  `rainier help login` and `rainier help all`.
- After authentication succeeds, login reports the **authoritative** workspace
  state and, when the workspace needs plan selection, payment, or
  provisioning, the server-provided web destination. Successful authentication
  is never reported as a ready workspace.

### 4.2 `rainier logout`

Removes the current context's local access and refresh credentials. It does
not delete remote sessions, does not revoke Claude or Codex credentials, and
does not disconnect GitHub. It keeps the context entry itself — server,
workspace, owner id — so a later bare `rainier login` knows where to sign in.

It is idempotent, and it names the context and server host it signed out of.
It prints no credential material.

### 4.3 `rainier status`

Default output:

```
Signed in: yes
Workspace: Personal
Compute: ready
Default environment: ready
GitHub: connected
Claude: ready
Codex: ready
```

Before compute is ready:

```
Compute: setup required (no plan selected)
Continue: https://example.test/app/workspace
```

The Compute row is read from the workspace's compute enrollment (§5.1) and
needs **both** its fields: `status` is what the workspace is entitled to,
`health` is whether that entitlement is reachable. Entitled-but-unreachable
gets its own sentence, because the recovery is nothing like selecting a plan:

```
Compute: entitled, but not reachable right now (health unavailable)
```

The `Continue:` line is printed only when the **server** supplied a
destination, and only on the row that is not ready. The CLI never composes one,
drops any value that is not an absolute `https` URL, and never infers payment
or readiness from a URL parameter or from local configuration. Every row is
derived from an API answer (§5).

- Required rows: `Signed in`, `Workspace`, `Compute`. Hosted contexts also
  require `Default environment`; on self-hosted servers it is advisory because
  scratch sessions and explicit `--env` do not require a default.
  If any is not ready — including a check that could not be run — the overall
  result is not ready and the exit code is 1.
- Advisory rows: `GitHub`, `Claude`, `Codex`. They are reported honestly and
  do not by themselves fail the command; none of them blocks `rainier new`.
- `--verbose` adds the diagnostics `doctor` prints today.
- Normal credential rotation during a status check may update local
  credentials. That is the token refresh working, not a state change.
- Errors carry the server's stable code and, when the server returns one, the
  request id.
- Credentials, cookies, login codes, and authorization headers are never
  printed, on any path.

### 4.4 `rainier agent`

```
rainier agent login <claude|codex>
rainier agent status [--json]
rainier agent logout <claude|codex> [--yes]
```

- `agent login` runs the provider's own supported interactive flow in a
  throwaway session. Nothing is pasted through the CLI. An incomplete login
  that writes no credential exits 1.
- It uses the server-selected default environment (§5.2); an ordinary hosted
  user never types `--env`. `--env` survives as an advanced override.
- If no compatible environment exists, that is reported as a readiness
  problem — "your workspace has no environment carrying that agent" — not as
  a missing flag.
- `agent status` reports exactly three states per provider:
  `not configured`, `ready`, `needs attention`. `ready` means a stored
  credential; Rainier does not verify it against the provider, and the output
  says so. Credential presence is never described as verified provider access.
- `agent logout` is explicit, warns that it reaches every workspace, and
  requires `--yes` when there is no terminal (exit 2 rather than a silent
  cancel or a hang).

### 4.5 GitHub

GitHub connection and workspace sharing are web actions in hosted v0.
`rainier status` reports GitHub readiness and the relevant web destination.
No `rainier github` command group is added here.

`creds` (self-hosted vault) and `connection` (hosted connection sharing)
remain under Advanced. **Removal dependency, exactly:** they can be deleted
once (a) the hosted web application performs connection sharing and
reconnection, so `connection share|unshare|reconnect` has no caller, and
(b) self-hosted `controld` deployments no longer need `creds` to inspect
`GET /v0/credentials`. Until both hold, deleting them breaks live flows.

## 5. Cloud API dependencies

Verified against `rainier-cloud` `origin/main` at
`05fbe23562292c4b69a12b0fdab2574911cd7dca`.

### 5.1 Workspace compute — **live**

`GET /v0/workspaces/{id}/compute` is served by the cell and forwarded to bearer
clients by the edge (`internal/edge/proxy/proxy.go` `CellPatterns`). It needs
only `view_workspace`, and it answers **200 with `status:"needs_plan"`** for a
workspace that has never enrolled — so there is no 404 ambiguity about the
state itself.

```json
{"status": "ready", "health": "available", "revision": "7"}
```

The CLI reads two fields and acts on both:

- `status` ∈ `needs_plan | awaiting_payment | provisioning | ready | failed |
  cancelling | cancelled` — the workspace's **entitlement**.
- `health` ∈ `available | unavailable | unknown` — whether that entitlement can
  be **reached** right now.

**`ready` + `health:"unavailable"` is not ready.** It is capacity that exists
and cannot be reached; the entitlement is intact and a session will not start,
so reporting it as ready would send somebody to `rainier new` to be refused.
This is the same separation §3.1 draws for a session: what a thing is entitled
to and whether it is reachable are two facts.

A 404 or 405 means the server publishes no compute route — an older cell, a
self-hosted `controld`, or a cell composed to sell no compute
(`computeUnavailable` answers 404 by design). A hosted CLI cannot distinguish
these from a missing route: it reports unknown readiness and exits 1. A
self-hosted context checks runner connectivity and capacity instead.

### 5.2 The onboarding destination — **still missing**

The destination exists server-side only as `browserhttp.onboardingFor()`, which
returns a **relative path** on the cookie-authenticated `/v0/web/bootstrap`.
Nothing bearer-reachable publishes it.

Proposed, `GET /v0/web/onboarding` — an **edge** route, because the console
origin is edge deployment configuration and the status→page routing is the web
application's. It carries no tenant state:

```json
{
  "console_url": "https://app.example",
  "destinations": {
    "needs_plan":       "https://app.example/app/workspace",
    "awaiting_payment": "https://app.example/app/workspace",
    "provisioning":     "https://app.example/app/workspace",
    "cancelling":       "https://app.example/app/workspace",
    "ready":            "https://app.example/app/sessions",
    "failed":           "https://app.example/app/workspace",
    "cancelled":        "https://app.example/app/workspace"
  }
}
```

Keys are exactly the §5.1 status vocabulary, so the CLI performs a **lookup** —
in a map the server supplied, keyed by a status the server supplied — and never
a policy decision. Both `onboardingFor` and this route must be generated from
one shared table, or the browser and the CLI will come to disagree about where
to send a person.

Until it ships, `status` prints **no `Continue:` line**. The CLI also drops any
value that is not an absolute `https` URL: the browser's own bootstrap serves a
relative path today, and a relative path in a "go here" message is worse than
no message.

### 5.3 Default environment

Resolved, in order:

1. an environment whose row carries `"default": true` — the server saying which
   one it is. **No server sends this field yet**; the CLI decodes it
   forward-compatibly;
2. the single environment in `GET /v0/environments`, when there is exactly one —
   an unambiguous read of the server's own catalog;
3. otherwise none, and `new` creates a scratch session exactly as before, so
   self-hosted deployments are unaffected.

An explicit `--image` opts out: it says "run exactly this image", and folding an
environment's setup script in underneath one is how a session fails at boot.

The CLI holds no environment catalog, no image names, and no setup scripts.

### 5.4 Agent launch catalog — **still missing**

`--agent claude|codex` needs the argv that *starts* an agent. That is a property
of the **environment's image**, which is regional, tenant-scoped state — so it
belongs on the environment, not on `/v0/agents`.

Proposed, `GET /v0/environments/{id}/agents` (a cell route; the existing
`/v0/environments/{rest...}` forwarding pattern already covers it):

```json
{
  "catalog_version": "1",
  "agents": [
    {"id": "claude", "display_name": "Claude Code", "argv": ["claude"], "requires_login": true}
  ]
}
```

- `argv` is a **structured array from a closed server catalog**, never a shell
  string. It becomes the session's command verbatim; an array cannot be
  re-split by a shell and the CLI adds nothing to it.
- No image tag, digest, package name, version, or provider infrastructure id.
- `requires_login` is the only join to custody, and it is a property of the
  *agent* rather than of the caller.

**`/v0/agents` stays credential custody** — account-scoped, keyed by
`(user, provider)`, with nowhere to put a credential and nothing to say about
an image. Launch capability has a different scope, a different owner, and a
different lifetime; putting it there would be the same collapse §3.1 exists to
prevent.

Until the route ships, `rainier new --agent X` fails with one message naming it
and showing the way round (`rainier new --name NAME -- claude`). Three refusals
are kept distinct because their recoveries differ: no catalog published, an
environment whose image carries nothing, and an agent that environment does not
carry.

### 5.5 `workspace_not_ready` — **live**

`internal/cell/api/workspace_compute.go` emits 409 `workspace_not_ready` from
**both** session create and session resume, for an enrolled workspace that is
not ready. A workspace with no enrollment is admitted unchanged, which is what
makes compute a migration rather than a cutover. The CLI branches on the code
and never on message text.

### 5.6 Session repository metadata

`info`'s repository display metadata needs a repository field on
`v0wire.SessionView`. There is none, so `info` omits the row rather than
reconstructing it from an environment's connectors.

## 6. Output contract

### 6.1 Streams and exit codes

- Human output → stdout. This includes help that was explicitly requested.
- Diagnostics, warnings, errors, and usage printed because an invocation was
  wrong → stderr.
- Exit 0 success, 1 operational or server failure, 2 invalid invocation.
- Output format never changes because stdout is a pipe. `--json` is the only
  way to get JSON.

### 6.2 `--json`

Supported by `status`, `ls`, `info`, `agent status`, and the asynchronous
mutation results (`new --detach`, `stop`, `resume`, `delete`). Every document carries:

```json
{"schema": "rainier.v0.<kind>", "version": 1, ...}
```

`schema` names the document; `version` is bumped only for an incompatible
change to it. Fields are added, not repurposed. Timestamps are RFC 3339 UTC,
as the API sends them. `--json` writes one document to stdout and nothing
else; failures still exit non-zero and explain themselves on stderr.

**A session document carries the canonical API facts verbatim.** `state` is the
server's own string and is **never** replaced by a derived word; `reachable` is
the boolean and `child_exit_code` the nullable integer the API sent, with the
key always present so a consumer cannot mistake "absent" for "older server". A
client that wants exactly what the control plane said reads those three and
ignores the rest.

The API's session view carries one further object, additively — nothing else
in it changed:

```json
"controller": {"generation": "3", "held": true}
```

`generation` is the controller generation currently in force, as a DECIMAL
STRING because it is a `uint64` and a JSON number past 2^53 is silently wrong
in a browser. `held` is whether anybody holds it right now. It names nobody: a
client learns that somebody has control, never who or on what device. Both
keys are always present; a server that predates them sends neither, which
reads as "nobody has control" and is the truth there, because nothing was ever
conditional.

The derived fields are **additive and separately named** — `lifecycle`,
`process`, `connection`, and an `actions` object — so nothing overwrites a fact
with an interpretation of it. `actions` is three-valued
(`yes` | `no` | `unknown`), because a state this build does not know admits no
honest boolean.

`ls --json` and `info --json` emit the same entry shape, from the same builder,
so a script cannot find one shape in a list and another in a detail read.

### 6.3 Errors

Errors branch on machine-readable server codes (`cli.APIError.Code`), never on
message text. When the server returns a request id, it is included so a
support conversation can start from an identifier.

Every output and error path runs through the redactor. It removes control and
bidi characters — server prose is untrusted, and a terminal escape in it can
rewrite lines the reader has already seen — and replaces every credential this
machine holds with `[redacted]`. Three properties are load-bearing:

- Newlines and tabs survive. Several errors are deliberately two or three
  lines, one of which is the `Continue:` address or the command to run next.
- Substitution is a **single pass**, longest match first. A loop of
  replacements can match a later credential inside the marker an earlier one
  wrote, which nests markers instead of redacting.
- A stored value shorter than 8 characters is not substituted. Nothing these
  servers issue is that short, and replacing a one-character value turns every
  occurrence of a common letter into a marker, destroying the message while
  protecting nothing.

## 7. Help structure

`rainier --help` fits one ordinary terminal screen (24 lines) and explains the
primary journey. `rainier help <command>` gives one command's detail.
`rainier help all` is the manual: compatibility aliases, self-hosted login,
administration, environments and secrets, contexts and workspaces, and the
low-level transfer and snapshot operations — each under an **Advanced**
heading. Advanced material never appears in the first-run experience.

### 6.4 Additional canonical facts

Session documents include `runner` and `last_event_at`; `info` labels the last
server event separately from creation time and process state. A last event is
not proof that an agent is actively working. Status compute checks include
`facts.status` and `facts.health` verbatim. Missing required facts (including
an unresolved default environment or a failed workspace lookup) fail readiness.

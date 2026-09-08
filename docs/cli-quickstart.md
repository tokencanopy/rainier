# Start a session with the CLI

These commands assume `rainier` is on your PATH. For a source build, run
`make build` and substitute `bin/rainier`. Check `rainier version` and
`rainier --help`. This guide describes the CLI source in this branch; packaged
releases expose these commands only once they include these changes.

## Prerequisites and login

Your administrator must provide a reachable server URL, account/workspace access,
and a connected runner with capacity. A coding agent additionally needs an
environment whose image contains its CLI, the egress access that agent needs,
and the agent's own login. The CLI does not provision servers or runners.

Use the appropriate login method. Both URLs below are placeholders; replace them
with the URL supplied by your administrator.

```bash
# Self-hosted server: use your existing GitHub CLI login.
rainier login --from-gh --server https://rainier.example.invalid

# Or hosted service: finish the browser sign-in.
rainier login --cloud https://edge.example.invalid
```

Use `rainier help login` for token and device-flow alternatives. If hosted login
lists several workspaces, select the actual id it displayed:

```bash
rainier workspace use ws_example
rainier context current
rainier status
```

On the hosted service a bare `rainier login` is enough after the first time: it
signs in again to the server this machine already uses, and it is safe to
rerun. `rainier logout` removes those credentials and nothing else — remote
sessions keep running, coding-agent logins stay in the workspace, and the
GitHub connection stays authorized.

`rainier status` answers one question in seven lines: signed in, workspace,
compute, default environment, GitHub, and each coding agent. Every line comes
from the server, never from local configuration, and when something has to
happen on the web the server names the address to continue at. Exit 0 means
everything required is ready; exit 1 means it is not; invalid usage exits 2.
`--json` prints one document; `--verbose` adds the full diagnostics that
`rainier doctor` used to print, and `doctor` remains as an alias for
`status --verbose`.

The verbose report takes at most about 15 seconds of network work, including
normal token refresh. It reports PASS/WARN/FAIL with a next action. It creates
no sessions, environments, or infrastructure; a normal hosted token refresh can
save rotated credentials to the current context.

Authentication and observable runner capacity are required. Unknown or unavailable
runner readiness fails the check. Missing environments or agent logins warn that
coding-agent readiness is incomplete, while a basic shell session can still work.
A stored agent login does not prove that its provider will accept it, or that an
environment contains the CLI. Capacity is an observation, not a reservation, and
environment placement/capability requirements can still prevent scheduling.

An unavailable optional endpoint means compatibility is not established. A 404
alone is not evidence that an upgrade is required. The report does not guess a
backend version; consult your administrator if an endpoint is missing or forbidden.

## Give your sessions GitHub access

How this works depends on which server you logged in to, and the two are not
interchangeable.

On a **self-hosted** server, `login` vaults a GitHub token for you. `rainier
creds` shows what is stored — provider, status, scopes, verification and use
times, never a value — and `rainier login --refresh github` replaces it when
the status is `needs_refresh` or the scopes lack `repo`.

On the **hosted** service there is no vault. You authorize GitHub in the
browser, on the last step of `login --cloud`, and the service brokers a
credential into each session; `rainier creds` says so rather than reporting an
empty vault. A connection starts shared with no workspace, so nothing can
clone until you share it with one:

```bash
rainier connection ls               # provider, GitHub login, access mode, workspaces
rainier connection share github     # let your current workspace use it
rainier connection reconnect github # authorize again, preserving access after success
```

`share` and `unshare` change only the workspace you name — your current one,
or `--workspace <id>` — keeping the other workspaces the connection reached
when the command read it. Neither changes the access
mode, so neither can widen the connection to all your workspaces. Neither
prints a GitHub credential: on this side the CLI never has one. Sharing requires
current membership; unsharing can remove a saved grant after you leave its
workspace. Use `rainier help connection` for the rest.

Use `connection reconnect` when Rainier requests new GitHub permissions or the
authorization itself needs replacing. It revokes the old connection before the
browser flow because the hosted API allows one live connection per account,
then restores the prior access mode and workspace selection after the new
connection appears. Do not interrupt it: abandoning the browser leaves GitHub
disconnected, and the CLI prints the saved workspace IDs and recovery commands.

The API replaces the connection's whole workspace list rather than adding to
it, and offers no conditional write, so the CLI reads the list and sends back
an edited copy. A change another client makes in between is overwritten,
including restoring a grant they removed. Avoid concurrent edits of the same connection. The CLI
checks the returned access mode and selection before reporting success, but
that response is still a snapshot. If it does not confirm the requested change,
the command fails and asks you to inspect `rainier connection ls`.

## Choose an environment and sign in to your coding agent

```bash
rainier agent status
rainier agent login codex
rainier status
```

On the hosted service that is the whole of it: `agent login` runs in your
workspace's default environment, whose image contains the agent. On a
self-hosted server that publishes several environments, choose one and name it:

```bash
rainier env ls                   # advanced: rainier help all
rainier env show dev             # replace dev with an available environment
rainier agent login codex --env dev
```

Finish the provider's login flow before exiting that login session. If there is
no suitable environment, `agent login` says so as a readiness problem rather
than asking for a flag; ask your administrator to create one with the needed
image, setup, and egress. Rainier does not install an agent merely because
`agent login` names it.

`agent status` reports `not configured`, `ready`, or `needs attention`.
`ready` means a credential is stored; Rainier does not verify it with the
provider.

## Start, detach, and return

```bash
rainier new --name box1
# In the terminal, Ctrl-] detaches and leaves the session running.
rainier ls
rainier attach box1
```

With no `--env`, `new` starts from your workspace's default environment. Pass a
command after `--` to run something specific, for example
`rainier new --name box2 -- codex`. Use `--detach` to create without opening the
terminal.

`rainier ls` shows name, lifecycle, child process, runner connection, and age.
A running sandbox stays Running after its child exits; the Process column
shows the exit code. `--all` adds Canceled and Deleted records. `--verbose`
adds ids, environments, runners, and the API state. `rainier info <session>`
also shows the last server event time, separately from process activity.

Any session command takes a name, a `sess_` id, or the word `current`, which is
the session you last created or attached.

`attach` resumes a stopped session when necessary and restores the current
screen, including after its child has exited; `rainier attach box1 --since 0` replays all
recorded output and is the way to read a failed session's log.

If initial attach keeps receiving 503 responses, the CLI waits about a minute,
then makes bounded readiness observations and prints a reattach command. It keeps
the session; do not repeatedly create replacements. Ctrl-C stops waiting without
removing the session. Run `rainier status --verbose`, follow its guidance, and reattach with
the printed session id. A queue reason can identify a runner/capability constraint;
otherwise the CLI reports only observed capacity or says the cause is unknown.
Transient disconnects after a successful attach continue to reconnect from the
last rendered sequence. Leave the terminal open when your laptop sleeps: the
remote process continues, and the CLI reconnects and replays missed output when
connectivity returns. The hosted gateway also periodically renews terminal
authorization; this uses the same reconnect path, not a new coding session.

On an expired hosted access token, the CLI adopts credentials already rotated by
another CLI or refreshes the saved pair, then retries the upgrade. It keeps the
original session, server, and workspace. Revoked login or denied workspace access
still stops recovery. If refresh cannot complete, reconnect with
`rainier attach box1 --since 0`; follow login guidance if required. An ambiguous
failed refresh exchange is not retried automatically because refresh tokens are
single-use. Terminal input modes are cleaned up on final return to the shell,
without clearing local scrollback.

`rainier stop <session>` stops a session and releases its compute; attaching
brings it back where you left it. `rainier delete <session>` destroys one
permanently — it asks first on a terminal and requires `--yes` in a script.
`rainier suspend` and `rainier rm` remain as compatibility aliases for scripts
that already call them.

Git inside the session is the source of truth for repository state: attach and
run `git status`, `git diff` or `git log` there. Rainier has no repository-diff
command.

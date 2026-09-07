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
rainier doctor
```

`doctor` takes at most about 15 seconds of network work, including normal token
refresh. It reports PASS/WARN/FAIL with a next action. Exit 0 means the required
basic-session checks passed; exit 1 means one failed; invalid usage exits 2.
It does not create sessions, environments, or infrastructure. A normal hosted
token refresh can save rotated credentials to the current context.

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
```

`share` and `unshare` change only the workspace you name — your current one,
or `--workspace <id>` for another you belong to — and leave every other
workspace's access as it was. Neither changes the access mode, so neither can
widen the connection to all your workspaces. Neither prints a credential: on
this side the CLI never has one. Use `rainier help connection` for the rest.

## Choose an environment and sign in to your coding agent

```bash
rainier env ls
rainier env show dev             # replace dev with an available environment
rainier agent ls
rainier agent login codex --env dev
rainier doctor
```

The `codex` example requires that the selected environment already contains Codex.
Use `rainier agent --help` to see supported providers, and finish the provider's
login flow before exiting that login session. If there is no suitable environment,
ask your administrator to create one with the needed image, setup, and egress;
`rainier env --help` documents the existing environment commands. Rainier does not
install an agent merely because `agent login` names it.

## Start, detach, and return

```bash
rainier new --env dev --name box1
# In the terminal, Ctrl-] detaches and leaves the session running.
rainier ls
rainier attach box1
```

Pass a command after `--` if your environment does not already start the desired
agent, for example `rainier new --env dev --name box2 -- codex`.
Use `--detach` on `new` to create without opening the terminal. `attach` resumes a
suspended session when necessary and restores the current screen. To replay all
recorded output, use `rainier attach box1 --since 0`.

If initial attach keeps receiving 503 responses, the CLI waits about a minute,
then makes bounded readiness observations and prints a reattach command. It keeps
the session; do not repeatedly create replacements. Ctrl-C stops waiting without
removing the session. Run `rainier doctor`, follow its guidance, and reattach with
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

`rainier rm <id|name>` destroys a session when you deliberately want to remove it.

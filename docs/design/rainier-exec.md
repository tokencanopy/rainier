# `rainier exec` — running one command inside a live session

`rainier exec <session> [--tty] [--cwd DIR] [--env K=V]... [--detach --log PATH] -- <command> [args...]`
runs a command inside an existing session's sandbox, as the session's own
user, streams its stdout and stderr (or a PTY) back to the caller, forwards
stdin when the caller has one, and exits with the command's exit code.

**Status: implemented.** It builds on conditional controller ownership (#84,
merged) and is written against that shape, not against the older attach path.
The corrections an independent review asked for after it was built are
designed in
[`rainier-exec-review-fixes.md`](rainier-exec-review-fixes.md); this document
carries their outcomes inline rather than as an appendix.

Four things changed between this document being approved and being built, and
each is written into the section it belongs to rather than left as an
addendum. They are listed here so a reader of the original can find them:

1. **`--detach` is in scope for v1**, in the minimal shape described under
   [Scope](#scope) and [Detached exec](#detached-exec). The motivating use
   case — `rainier exec s -- claude --continue …` to resume an interrupted
   unattended run — needs the process to outlive the caller, and the reasons
   this document gave for deferring it turn out to be answerable in one
   paragraph each rather than in a design of their own.
2. **`argv[0]` resolves on the SESSION's PATH**, the one sessiond composed
   for the agent, never sessiond's own.
3. **End of input is an explicit frame**, `exec_stdin_eof`, because a socket
   that is still open cannot express "no more input" and `cat` with a pipe on
   the other end has to terminate. (The message table below said `exec_eof`;
   the built name is `exec_stdin_eof`, which says which stream it ends.)
4. **A session that is deleted or suspended mid-exec is 125**, the same as a
   sandbox that died, with the caller's message naming which.

## Problem

A session is reachable today through exactly one door: `rainier attach`, which
splices a person's terminal to the agent's pty. That is the right door for a
person and the wrong one for everything else.

- An **unattended run** cannot be resumed or inspected without a human at a
  keyboard. `claude --continue` is a command; the only way to run it in an
  existing sandbox is to attach and type it, which means a terminal, a person,
  and a controller lease taken away from whoever had it.
- A **gate** cannot be run from outside. `go test ./...`, `make verify`, a
  lint pass — every one of them already runs inside the sandbox when an agent
  types it, and none of them can be driven by a script, a CI job, or a
  supervising agent.
- A **question** cannot be asked cheaply. `git status`, `cat` a build log, `ls
  /workspace` — all of them require an interactive attach whose whole cost is
  paid to read three lines.

The two fallbacks operators actually use are both bad. Attach-and-type means a
human, a displaced controller, and output that lands in the session's
scrollback where it is indistinguishable from the agent's own. Rebuilding the
session means losing the thing that made it worth keeping.

Everything needed to fix this already exists and is already load-bearing: the
attach plane parks a client socket and has the runner dial back to it
(`attachplane`), the relay multiplexes many attachments over one socket
(`internal/relay`), and the sandbox already spawns a process on a pty
(`internal/session.StartProc`). `exec` is a second **kind** of attachment over
that same machinery, not a second machinery.

## Scope

In: one command, one sandbox, streamed both ways, exit code returned, `--tty`,
`--cwd`, `--env`, `--json`, and a minimal `--detach`. The self-hosted route,
the CLI, and what Cloud has to add to route the same call.

Out: see [Non-goals](#non-goals). In particular there is no timeout the server
imposes.

## Model

An exec is an **attachment**, opened over the same dial-back plane a terminal
attach uses, carrying the same frames, bounded by the same budgets, and
reconnecting under the same rules — with three differences, each of which is
the whole reason it is a second kind rather than a flag on the first.

| | terminal attachment | exec attachment |
|---|---|---|
| what is on the far end | the session's one pty, shared by every viewer | a process this attachment created and owns |
| who may write | the controller, fenced by the generation | its own caller, unconditionally |
| where the output goes | the emulator and the event log, and therefore every viewer's screen and every later `--since` replay | this caller's socket, and nowhere else |

That third row is not a detail. An exec's output must never reach the
emulator, the event log, or another viewer: `rainier exec s -- cat huge.log`
would otherwise scribble a megabyte into the scrollback of a session somebody
is watching, and `rainier attach --since 0` would replay it forever. The exec
process is spawned beside `session.Session`, never inside it, and
`session.Session` is not modified by this design at all.

The second row is how exec composes with #84. **An exec is not the terminal.**
It does not take the controller lease, does not advance the controller
generation, is not displaced by a take-over, and cannot itself displace
anybody. Its frames carry no generation and the plane does not stamp them. In
`internal/session` terms it never reaches `Stdin`, `SetSize`, `Bind`, or
`mayWriteLocked` — the pty fence governs the pty, and an exec has its own.

### Authority

Not needing the lease is not the same as needing no authority. An exec writes
into the sandbox and runs arbitrary code there, which is exactly what a
**controller** does, so that is the question that is asked:

```go
policy.AuthorizeAttachment(ctx, scope, resource, control.AttachmentController)
```

— `controlapp.AttachmentPolicy`, asked with `control.AttachmentController`,
once per exec, **live** at the moment the exec is requested and never from an
answer cached at some earlier attach. A host that grants viewing without
granting driving refuses exec, and refuses it the same way and for the same
reason it refuses the take-control key.

The generic `control.Authorizer` is asked `ActionAttach` on
`ResourceSession`, deliberately **not** a new action. Two reasons. It is true:
exec grants nothing a controller attach does not already grant (see
[Security](#security)), so a host that has decided who may drive this session
has already decided who may exec into it. And it is safe across a rollout: a
new `Action` constant reaches every existing authorizer adapter as a verb it
has never seen, and a correctly written adapter fails closed on it — which
would mean exec refusing every caller on every self-hosted installation until
its adapter learned the word. The cost is that a host cannot permit attach and
refuse exec; that is [open question 1](#open-questions).

## Transport

### The route

```
WS GET /v0/sessions/{id}/exec
```

A **route**, not `attach?kind=exec`. A query parameter is the pattern #84
itself uses for ownership negotiation, and it is right there for exactly the
reason it is wrong here: a plane that predates the parameter *ignores* it. An
old plane handed `attach?kind=exec` would open an ordinary terminal
attachment — an unnegotiated controller attach, which #84 admits
unconditionally as a take-over — and the caller's `-- command` would never
run while the human at the other device silently lost control of their
session. Degrading is acceptable for a negotiation about who may type. It is
not acceptable for "run this". An old plane answers `404` on a route it does
not have, which is the honest answer and the one the CLI can act on.

Underneath the route everything is shared: `attachplane`'s pairing table, the
`dial_attach` the runner comes back on, the splice, `attachReadLimit`
(16 MiB), `defaultPairTTL` (15s), `attachFirstMsgTimeout` (15s), and the
client write budget (`defaultClientWriteBase` + `defaultClientWriteRate`).
There is one new socket type and no new plane.

### The request travels in the first message

The attach protocol's rule is that the first client message must be a resize.
The exec protocol's rule is that the first client message must be an
`exec_start`, carrying argv, cwd, env, tty, and the initial size; the same
`attachFirstMsgTimeout` bounds it, and a client that sends anything else is
closed with the same policy-violation close the attach path already uses.

Argv does **not** ride the query string, and that is a security decision
rather than an aesthetic one. A URL is written to the access log of every
proxy between the caller and the cell, so an argv in the URL is an argument in
a log file — precisely what [Security](#security) says is never recorded. It
also has no length to speak of, and arguments are arbitrary bytes.

### Budgets and flood

Sizes and rates are the plane's existing ones, reused unchanged. What is new
is the **overflow policy**, and it is the opposite of the terminal's.

`session.trySend` force-detaches a viewer whose channel is full. That is
correct there: the session belongs to the agent and to every other viewer, and
one stalled phone must not hold it hostage. An exec's process has exactly one
consumer — the caller who started it — so dropping its output would silently
corrupt the one answer the caller asked for. So an exec applies
**backpressure**: the sandbox stops reading the process's stdout/stderr pipes
while the socket is not draining, the process blocks on `write(2)`, and
nothing is dropped. A caller that has genuinely stopped reading is still
closed, by the client write budget it shares with attach, and its process is
killed by the disconnect rule below. Under `--tty` the throttle is the pty's
own kernel buffer, which is smaller and does the same job.

### Caller disconnect

**The process is killed.** `SIGTERM` to the process *group*, five seconds'
grace, then `SIGKILL` to the group. The group matters for the reason
`cmd/sessiond/files.go` already documents about `git fetch`: the child spawns
grandchildren that inherit the pipes, and signalling the leader alone leaves
them holding the far end.

Killing rather than orphaning is the answer for an ATTACHED exec, which is
every exec that did not ask otherwise. An exec's lifetime is its caller's,
which is a rule a script can reason about.

### Detached exec

`--detach` is the exception, and it is in scope for v1 because the case that
motivates this whole design needs it: `rainier exec s -- claude --continue …`
resumes an interrupted unattended run, and an unattended run whose supervisor
has to sit on a socket for six hours is not unattended.

This document originally deferred it on the grounds that "a process that
survives its caller needs somewhere for its output to go, a way to be listed,
a way to be killed, and a way to be reaped when the session is suspended".
Three of those four have one-line answers, and the fourth turns out not to be
needed:

- **Somewhere for its output to go** — `--log PATH`, named by the caller,
  required with `--detach`, and resolved inside `/workspace` exactly as
  `--cwd` is. A detached process writing outside the tree its session owns is
  the same escape `--cwd` refuses, by a slower route.
- **A way to be killed** — `rainier exec s -- kill <pid>`. The sandbox answers
  `exec_started{pid}` and closes the attachment; the CLI prints the pid on
  stdout and exits 0. There is no `rainier exec --kill`, because a sandbox
  already has a perfectly good one and it is audited like any other command.
- **A way to be reaped** — the session. A detached process is killed when the
  session is suspended, stopped or destroyed. It is never killed by its caller
  disconnecting; that is the entire difference the flag makes.

  Two paths, because a session can end in two ways. A cold stop and a destroy
  deliver `sessiond` a SIGTERM, and its handler ends every exec. The DEFAULT
  `rainier stop` is a warm suspend — `docker pause` — which delivers nothing at
  all: the freezer cgroup stops the process where it stands, so a detached run
  would be frozen and resumed rather than reaped, and an in-flight exec's
  caller would block until somebody resumed the session.

  So runnerd sends the sandbox a `suspending` control event before it pauses the
  container, and the sandbox answers **twice**: `suspend_ack` at once, and
  `suspend_ready` when the processes are actually gone. Three properties fall
  out of that shape, and each of them is the answer to a way the one-answer
  version was wrong:

  - **Two answers, so the two waits can be different lengths.** runnerd cannot
    tell a sandbox that is working from one that predates the notice entirely,
    and a session keeps the `sessiond` it booted with for life — so with one
    answer, every session created before exec shipped would make every warm
    stop wait out the whole "are they gone" budget, forever. The ack is cheap
    and immediate; hearing none means "this sandbox is old, carry on" after two
    seconds rather than twelve.
  - **A nonce on all three, echoed by the sandbox.** Sessiond's answer can
    legitimately outlast runnerd's budget, so without one, a straggler from a
    suspend that already gave up releases the NEXT suspend — freezing a
    container while its sandbox is mid-kill and leaving a signal pending in the
    freezer cgroup, which is the exact failure this handshake exists to prevent.
  - **The wait is for the PROCESSES, not the signals.** `KillAll` signals on a
    goroutine per exec, so answering as soon as it returns would report a
    suspend ready with the SIGTERMs still in flight. `KillAllAndWait` waits for
    the slots to come back, and the sandbox's budget is longer than its own kill
    grace so the SIGTERM→SIGKILL escalation completes before the clocks stop.

  A sessiond that predates the notice logs an unknown kind and drops it; the ack
  wait expires and the pause proceeds, which is the behaviour a fleet has today.
  A conn that has died and a dispatch whose context is already cancelled both
  end the wait at once rather than spending it on an answer that cannot come.

  One thing the sandbox does NOT do is keep accepting commands while it is
  quiescing. The relay conn stays up for the whole of that budget, so an exec
  arriving mid-sweep would be spawned into a container that is about to be
  frozen and then frozen alive — so the runner refuses new execs once the sweep
  has begun, with `exec_error{session_ending}`.
- **A way to be listed** — not needed in v1, and deliberately not built. The
  caller has the pid, the sandbox has `ps`, and a listing API would be a
  second source of truth about processes the sandbox already knows about.

The process is spawned in its own process group with stdin on `/dev/null`,
and it counts against the same eight-exec cap an attached one does, because
it is a process in the same container.

It also counts against a **smaller cap of its own: at most four of the eight
may be detached**, refused as `exec_error{too_many_detached}`. A detached
process holds its slot for as long as it runs, which can be hours, and the
only way to stop one is `rainier exec s -- kill <pid>` — which is itself an
exec and needs a slot. A session that filled all eight with detached work would
have no way left to stop any of it, with no listing and no kill API to fall
back on. The word is its own because reusing `too_many_execs` told the fifth
caller "this session is already running as many commands as it may" with half
the slots free.

`--detach` and `--tty` are refused together: a detached command has no
terminal to attach one to.

## Protocol changes

Every change below is additive and every new field is `omitempty`, so a peer
that sets none of them writes the bytes it writes today. Three wire hops
change.

### 1. `protocol/runner` — the operation

`runner.Attach` (the `dial_attach` block) gains the kind and the request:

```go
// Kind is which sort of attachment this dial-back opens: "" (or
// KindTerminal) is the session's pty, KindExec is a process this
// attachment creates and owns. Absent is the terminal, which is what
// every control plane older than this field sends.
Kind string    `json:"kind,omitempty"`
Exec *ExecSpec `json:"exec,omitempty"`
```

```go
// ExecSpec is one command to run inside the sandbox. It is resolved by
// the CALLER and validated by the sandbox; the control plane carries it
// and checks only its shape.
type ExecSpec struct {
    Argv []string          `json:"argv"`           // never empty; argv[0] is exec'd directly
    Cwd  string            `json:"cwd,omitempty"`  // absent means workspace.WorkspaceRoot
    Env  map[string]string `json:"env,omitempty"`  // caller additions; see the env rule
    TTY  bool              `json:"tty,omitempty"`
    Cols int               `json:"cols,omitempty"` // TTY only
    Rows int               `json:"rows,omitempty"` // TTY only
}
```

Values in `Env` are secrets as often as not and are never logged verbatim —
the same sentence `runner.Spec.Env` already carries, for the same reason.

`runner.FromRunner.Capabilities` gains the token `exec.v1`, announced by a
runner whose `runnerd` can forward an exec open. It is a cheap pre-check, not
the fence: see the matrix below for why the sandbox still has to confirm.

### 2. `internal/relay` — the frame

`relay.Frame` gains one field on a `FrameOpen`:

```go
Kind string    `json:"k,omitempty"`
Exec *ExecSpec `json:"x,omitempty"`
```

`serveSession`'s `FrameOpen` case branches on `Kind`: empty goes to
`s.Attach(...)` exactly as today, `"exec"` goes to the new exec runner and
**never touches `session.Session`**. `FrameClient` and `FrameClose` route by
the id's registered kind. A `FrameOpen` with `Kind == "exec"` on a build that
does not know the field falls into today's terminal branch — which is why the
handshake in §3 exists.

### 3. `protocol/terminal` — the messages

Exec reuses `ClientMessage` and `ServerMessage` rather than introducing a
third pair: the plane forwards whole messages of these two types and adding a
type would mean a third decode at every hop. New type words only.

| Direction | Type | Carries |
|---|---|---|
| client → server | `exec_start` | `Exec` — the spec above; must be the first message |
| client → server | `stdin` | `Data`; no `gen`, ever |
| client → server | `exec_stdin_eof` | — (close the process's stdin) |
| client → server | `resize` | `Cols`, `Rows`; `--tty` only, ignored otherwise |
| client → server | `exec_signal` | `Signal` — `"TERM"` or `"INT"`, nothing else |
| server → client | `exec_started` | — the process exists; **the handshake** |
| server → client | `exec_stdout` | `Data` |
| server → client | `exec_stderr` | `Data` |
| server → client | `exec_exit` | `ExitCode`, or `Signal` when it was killed |
| server → client | `exec_error` | `Reason` — a closed vocabulary, never free prose |

`exec_error`'s vocabulary is `unsupported`, `not_found`, `not_executable`,
`cwd_refused`, `env_refused`, `log_refused`, `too_many_execs`,
`too_many_detached`, `no_answer` and `stdin_overrun`. It is closed because the
CLI maps it to an exit code and a sentence, and because a free-form string from
inside a sandbox is a string a user's terminal renders — and it is published
here as `terminal.ExecReasons()` rather than as prose, so
`TestExecReasonsAreClosedBothWays` fails when a word is added to one and not
the other. The last three arrived after this list was first written and drifted
out of it for exactly that reason:

- `too_many_detached` — the DETACHED sub-cap below, which is a smaller number
  than the eight, and therefore a different sentence and a different remedy.
- `no_answer` — the sandbox said nothing in time. Kept apart from
  `unsupported`, which means "this session was created before exec shipped" and
  is permanent.
- `stdin_overrun` — the caller sent more input than the sandbox will hold for a
  command that is not reading it. The one reason that can arrive AFTER
  `exec_started`, because it is about the input rather than the command.

`ClientMessage` gains `Exec *ExecSpec` and `Signal string`; `ServerMessage`
gains `Signal string` and `Reason string`. `ExitCode` is reused as-is,
including its wire tag `exitCode`, which is contract and does not change.

The ownership vocabulary and the exec vocabulary never mix. #84's splice
already drops `control`/`control_ack` coming from a client and
`attached`/`stale`/`control_changed` coming from a sandbox; this design adds
the mirror rule — **a plane never stamps an exec frame with a generation, and
a sandbox never reads one off an exec frame** — and pins it with a test. An
exec client that sends `claim` is dropped exactly as a client's `control` is:
there is nothing for it to claim.

### 4. `cmd/sessiond` — spawning

The exec runner lives beside `session.Session`, not inside it, and takes the
same seam `session.New` already takes — a `start func(spec, onStdout,
onStderr) (Proc, error)` — so the whole thing is testable with no pty and no
container.

**User.** None is chosen, and that is the point. The container already runs as
`--user 1000:1000` (`internal/driver`), `sessiond` runs as that user, and a
plain fork/exec inherits it. There is no `--user` flag, no `setuid`, no `su`,
and no code path in this design that can produce a process running as anybody
but the session's own user.

**Env.** The child starts from the environment `sessiond` composed for the
agent — `HOME`, `PATH`, `GIT_CONFIG_GLOBAL`, the agent-home mounts, the
session's own `Env` — because `claude --continue` and `git status` are
worthless without it, then `TERM=xterm-256color` under `--tty` (matching
`StartProc`), then the caller's `--env`. The rule on the caller's additions is
a rule about **names**, not a table of them, because a user's own build needs
arbitrary names:

- the name matches `^[A-Za-z_][A-Za-z0-9_]*$`, and neither name nor value
  contains a NUL;
- the name is not in the reserved namespace `RAINIER_*`, which is how the
  boot chain, the credential helper and the agent sync address each other;
- the name is not one of `HOME`, `PATH`, `SHELL`, `IFS`, `LD_PRELOAD`,
  `LD_LIBRARY_PATH`, `GIT_CONFIG_GLOBAL`, `GIT_SSH_COMMAND`,
  `GIT_ASKPASS` — each of which turns "run this command" into "run
  something else", which is a different request.

A refusal is `exec_error{env_refused}` naming **the variable's name and
nothing else**. Values are never logged, never audited, never quoted in an
error.

The list is longer than three names in the built version, and its PURPOSE is
easy to misread, so: it is **not a privilege boundary**. A caller who may exec
at all may run `sh -c` and do whatever the session's user can do, which is the
whole of [Security](#security) below. What it protects is the audit record's
meaning and the caller's own expectation — `rainier exec s -- make` should run
make. So it covers every channel that turns a named binary into a different
program: the `LD_*` namespace as a PREFIX (because `LD_AUDIT` is `LD_PRELOAD`
by another name and the loader's list grows), git's `GIT_CONFIG_COUNT` /
`GIT_CONFIG_KEY_n` / `GIT_CONFIG_VALUE_n` config injection and its
`*_COMMAND` / `*_EDITOR` / `*_PAGER` hooks, the shell startup variables
(`BASH_ENV`, `ENV`, `SHELLOPTS`, `PS4`), and the interpreter option channels
(`NODE_OPTIONS`, `PERL5OPT`, `RUBYOPT`, `PYTHONSTARTUP`) that are `--eval`
under another name. It stays a list rather than a prefix wherever a prefix
would take something real away — `GIT_AUTHOR_NAME` and `PYTHONPATH` are how
ordinary builds work.

**Cwd.** Default `workspace.WorkspaceRoot` (`/workspace`). `--cwd` is
resolved with `workspace.Resolve(workspace.WorkspaceRoot, dir)` — the exact
function push and pull already use, including its symlink-escape check — so
"refuse a cwd outside the workspace" is not a new rule, it is the existing one
applied at a new door, and it is applied **in the sandbox**, which is the only
place the filesystem can be asked. The control plane applies
`workspace.ValidatePath` first, for the same reason it does on a transfer: the
last hop before a syscall trusts nobody, and the hop before it refuses early.

The agent home is deliberately not reachable: it is outside `/workspace`, so
`--cwd` into it is refused. A command may still *read* it — that is what makes
`claude --continue` work — because inheriting the environment is not the same
as being able to `cd` anywhere.

**No shell.** `argv[0]` is exec'd directly. No `sh -c`, no glob expansion, no
`$VAR` substitution, no `&&`. `--tty` does not change this: a pty is a
terminal, not a shell. A caller who wants a shell types one — `rainier exec s
-- sh -c 'cd src && make'` — which is visible in what they typed and in what
is audited.

**The PATH is the session's.** `argv[0]` is resolved against the `PATH` of the
environment composed above — the agent's — and never against `sessiond`'s own
ambient one. `os/exec`'s `LookPath` reads the ambient one, and the two are the
same string only by coincidence; the day they differ is the day a session's
environment adds a toolchain, and `rainier exec s -- make` has to find the
`make` the agent would have found. A name containing a slash is a path and is
never searched for, exactly as a shell treats it, and a relative one resolves
against the exec's own cwd. A candidate that exists but is not executable does
not end the search — it is remembered, so that a stray non-executable `make`
early on the path does not shadow the real one, and it is what turns the
answer from `not_found` (127) into `not_executable` (126) when nothing
runnable is found.

**Pty.** Under `--tty`, `pty.StartWithSize` with the caller's size, exactly
`StartProc`'s mechanism. A pty has one stream, so `--tty` merges stdout and
stderr into `exec_stdout` — which is why it is a flag and not the default, and
the CLI says so in help.

**Reaping.** `sessiond` installs a `SIGCHLD` reaper at boot and is the single
authoritative waiter on Linux. An exec's wait therefore goes through
`reap.AwaitStatus(pid, mark)` and then a best-effort `cmd.Wait()`, the way
`session/proc.go` does. `cmd.Wait()` alone would race the reaper and report
`ECHILD` instead of a status.

Two things about that wait are exec's doing, because exec is what makes it
happen more than once. Before this, exactly one pid — the agent's — was ever
awaited, once, at boot; now every exec awaits by pid for the life of a
session. So: the reaped-outcome table is bounded (an unclaimed orphan entry is
the ordinary shape of `git fetch` spawning `git-remote-https`, and a session
outliving everything else means an unbounded map is a leak measured in weeks),
an EVICTED entry leaves a tombstone so its waiter is told "no status here" and
falls back to `cmd.Wait` rather than parking forever, and every record carries
a monotonic sequence that a caller takes a `reap.Mark()` of immediately before
forking — `pid_max` is 32768, so a long-lived session wraps, and an outcome
recorded before this child existed cannot be this child's.

**Concurrency.** At most 8 concurrent execs per session; the ninth is refused
`exec_error{too_many_execs}` before anything is spawned. Eight is slack rather
than a working limit — it exists so a loop in a script cannot fork-bomb a
sandbox through a socket.

### The three-party compatibility matrix

Client, plane, and sandbox roll on different days, and a session keeps the
`sessiond` it booted with for as long as it lives — a session image rolls on
*create*, not on deploy — so "new plane, old sandbox" is not a mixed-version
week, it is permanent for every session that predates the roll. All four
pairings:

| Pairing | Behaviour |
|---|---|
| **new client + old plane** | `404` on `/v0/sessions/{id}/exec`. The CLI prints `this Rainier does not support exec (the control plane is older than the CLI)` and exits 1. Nothing degrades into an attach, which is the whole reason exec is a route. |
| **old client + new plane** | Nothing. An old CLI never calls the route; every existing route is byte-identical. |
| **new plane + old sandbox** | The plane refuses early when the runner announces no `exec.v1`, with `501 exec_unsupported`. When the runner is new but the *session's* `sessiond` is old — the permanent case above — the old `serveSession` reads a `FrameOpen` whose `Kind` it does not know and opens a **terminal** attachment, answering with a `snapshot`. So the plane requires a positive `exec_started` as the first server message and waits `attachplane.Options.ExecHandshakeTimeout` (10s by default) for it — deliberately not the acknowledgement timeout, because that one acknowledges a binding already installed and this one waits for a SPAWN. Anything else, including a `snapshot`, closes the socket with `exec_unsupported`; a sandbox that says NOTHING in that budget gets `no_answer`, which is a different word for a different fact. The handshake, not the capability, is the fence — the capability only saves the round trip. |
| **new sandbox + old plane** | Nothing. The new `serveSession` branches on `Kind`, an old plane never sets it, and every frame it does send takes the terminal branch it always took. |

The rule that makes the third row safe is worth stating on its own: **a new
plane accepts an exec only from a sandbox that said `exec_started`.** No byte
of the caller's stdin is forwarded before it, so a sandbox that answered a
snapshot has received nothing.

Each row is pinned by a test, in both directions:

| Pairing | Pinned by |
|---|---|
| **new client + old plane** | `cmd/rainier`'s `TestExecDialFailureSentences` (a 404 with no envelope is the version sentence) and `TestExecURLIsItsOwnRoute` (it is not an attach URL). `internal/controld`'s `TestExecRouteIsNotAnAttachParameter` is the negative half: `attach?kind=exec` opens a TERMINAL attachment and means nothing. |
| **old client + new plane** | `protocol/{runner,terminal}`'s `TestExecIsAdditiveOnTheDialAttach`, `TestExecMessagesAreAdditive`, and `internal/relay`'s `TestTerminalFrameWireShape` — every existing message and frame is byte-identical, so an old client's traffic is unchanged. |
| **new plane + old sandbox** | `attachplane`'s `TestExecRequiresExecStarted` (a snapshot, an output, an exit, an attached, and stdout-before-the-handshake are each refused, with no stdin forwarded) and `TestExecRefusesASilentSandbox`; `internal/controld`'s `TestExecAgainstAnOldSandboxIsRefusedCleanly`; `internal/e2e`'s `TestExecAgainstASandboxThatCannotExec`. |
| **new sandbox + old plane** | `internal/relay`'s `TestAnExecFrameOnAnOldDecoderIsATerminalOpen` (an exec frame decodes on a build that predates the field as an ordinary terminal open) and `TestFrameOpenRoutesByKind` (an absent kind reaches `session.Attach` exactly as today). |

## API and CLI contract

Additive to [`docs/cli-v0-contract.md`](../cli-v0-contract.md), with exactly
one exception, stated here because "additive" was not literally true: §6.1's
exit-code sentence ("0 success, 1 operational or server failure, 2 invalid
invocation") was universal and is now carved out for `rainier exec`, whose
status is the command's. The bytes of the sentence are unchanged; its scope is
not, and §6.1 now says so.

### Route

`WS GET /v0/sessions/{id}/exec`, authenticated as every `/v0/` route is,
upgraded only after every refusable thing has been refused — because a status
code has nowhere to go once the socket is a websocket.

| Condition | Status | Code |
|---|---|---|
| session not found (or not visible to the caller) | `404` | `not_found` |
| policy refuses the controller question | `403` | `forbidden` |
| session is not `running` | `409` | `session_not_running`, body carries `"state"` |
| runner not connected | `503` | `runner_unreachable` |
| runner has no `exec.v1`, or this host composed no exec plane | `501` | `exec_unsupported` |

A sandbox that never confirms is deliberately **not** in that table. It cannot
be: the dial-back and the handshake both happen after the upgrade, so there is
no status code left to send. The caller is told in the protocol instead — an
`exec_error{unsupported}` or `exec_error{no_answer}` and then a close — which
is the field a CLI switches on rather than a string it has to pattern-match.
Every row that IS in the table is answered pre-upgrade, including the
no-exec-plane 501: `controlapp` answers `ErrUnsupported` only once the socket
is a websocket, so the route asks `AttachmentService.ExecSupported` before it
upgrades.

Two of these differ from `attach` deliberately.

`attach` **waits** up to `AttachWait` for a session to reach `running`,
because a person who just typed `rainier new` is legitimately a few seconds
early and a friendly wait is better than a retry loop in every client. `exec`
does not wait at all: its caller is a script that wants an answer now, and
holding a request open changes a fast failure into a slow one. So "not
running" is a conflict with the resource's current state — `409`, with the
state named, so the caller can decide — rather than `attach`'s `503
session_not_ready`.

`attach` answers `502 runner_unreachable`. `exec` answers `503`, because the
runner is a dependency that is expected back and `503` is the code a client
retries on; a gateway that turns `502` into a retry is guessing.

The response body is the same `{"code": ..., "message": ...}` envelope every
other route writes, with `state` added on the `409`.

### Exit codes

The command's status **is** the CLI's status. Rainier's own failures use codes
the shell vocabulary already reserves for a wrapper, so a script can always
tell "the command failed" from "rainier failed":

| Outcome | `rainier exec` exits |
|---|---|
| the command exited *n* | *n* (0–255, verbatim) |
| the command was killed by signal *N* | 128+*N* |
| the command was never accepted (404/403/409/503/501, bad flags) | 2 for an invalid invocation, 1 for everything else — §6.1 unchanged |
| accepted, but no exit status ever arrived (socket died mid-run) | **125** |
| the command could not be executed (`not_executable`, `cwd_refused`, `env_refused`) | **126** |
| the command was not found | **127** |

125/126/127 are `env(1)` and shell convention, so they need no learning, and
they keep 1 meaning what §6.1 already says it means. The overlap is real and
unavoidable — a command may itself exit 126 — and it is the right trade:
a caller who needs certainty reads `--json`, where the three facts are
separate fields.

### Streams

- the command's **stdout** → rainier's stdout, byte for byte, unbuffered,
  nothing added, no trailing newline invented;
- the command's **stderr** → rainier's stderr, byte for byte;
- everything **rainier** says — waiting, refusals, the state in a `409`, the
  version sentence for a `404` — → stderr, always, so that
  `rainier exec s -- cat f > out` produces exactly `f`.

The command's bytes do **not** pass through §6.3's redactor. The redactor
exists to make untrusted server *prose* safe to print, and running it over a
byte stream would corrupt tarballs, JSON and anything else a caller pipes.
This is the same treatment `rainier attach` already gives pty bytes, and it is
safe for the same reason: a caller who asked to run a command in their own
sandbox asked for its output. Rainier's own sentences are redacted as always.

### `--json`

`--json` writes exactly one document to stdout and nothing else, as §6.2
requires. It therefore **moves the command's own bytes**: with `--json`, the
command's stdout and stderr both go to rainier's stderr as they arrive, and
stdout carries the document after the command exits.

```json
{
  "schema": "rainier.v0.exec",
  "version": 1,
  "session": "sess_example",
  "exit_code": 7,
  "signal": null,
  "started_at": "2026-09-11T10:00:00Z",
  "exited_at": "2026-09-11T10:00:04Z",
  "duration_ms": 4021,
  "queued_ms": 118,
  "tty": false
}
```

`queued_ms` is the request-to-`exec_started` time — the dial-back and the
spawn — kept apart from `duration_ms` so a slow cell is not read as a slow
build. `exit_code` and `signal` are both present and exactly one is null.
`--json` never carries output: bounding it would truncate, and not bounding it
would put a build log in a JSON string.

A caller who wants the command's stdout *and* machine-readable timings runs
without `--json` and reads the exit code, which is the machine-readable fact
that matters.

### Help and surface

`exec` is **Advanced** (§2.2): dispatched, documented under `rainier help
all`, absent from the twenty-line default help. It is automation's command,
and the first-run journey is `new` → `attach` → `stop`.

## Hosted path

For the same command to work against a hosted workspace, Cloud adds the route
and nothing else conceptual. **cell-api** registers `WS GET
/v0/sessions/{id}/exec` onto the same `controlapp` service the self-hosted
route uses, with the same `control.Scope` assertion every other session route
makes — workspace, actor and placement resolved from authenticated state and
never from a client-supplied field — and maps its collaboration-grant policy
onto the `AttachmentController` question, which is the one new thing its
policy adapter is asked. **cell-gateway** must route the upgrade to the cell
that holds the session and must keep the dial-back's `target_url` naming that
cell's own replica, which is a property the attach plane already requires and
exec inherits unchanged; it must also allow the new path through whatever
allowlist fronts the websocket upgrade. The **edge proxy** must not buffer the
stream (a build's output is useless in 64 KiB blocks), must not impose an idle
timeout shorter than the session keepalive attach already sends — a compiling
command is legitimately silent for minutes — and must preserve the upgrade
headers on a path it has not seen before. None of Cloud's internals are
designed here; the point is that exec asks Cloud for exactly what attach
already asks for, on one more path.

## Security

**Exec adds no authority beyond what attach-with-control already grants.** A
controller types bytes into a shell running as uid 1000 inside the sandbox,
with the sandbox's filesystem, the sandbox's egress allowlist, and the
sandbox's credential helper one `git` away. Every one of those is reachable by
typing. Exec differs in ergonomics — it is scriptable, non-interactive, and
concurrent — and ergonomics is not authority. Concretely, exec introduces no
mechanism for:

- **choosing a user** — there is no `--user` and no setuid path; the process
  is `sessiond`'s own uid, which is the container's;
- **leaving the workspace** — `--cwd` is `workspace.Resolve`d, symlinks
  included, and refused otherwise;
- **reading the agent home by path** — it is outside `/workspace`, so it is
  not a legal `--cwd`; a command inherits the environment that makes an agent
  work, which is what a typed command already inherits;
- **changing egress** — same container, same netns, same allowlist;
- **escaping the session's lifetime** — an attached command dies with its
  caller, and every command dies with the SESSION. `--detach` (see [Detached
  exec](#detached-exec)) is the one exception to the first half and no
  exception at all to the second: it outlives its caller, which is its whole
  point, and it is killed when the session is suspended, stopped or destroyed,
  including on the default warm stop. That bound is what keeps `--detach` from
  being a way to leave a process running in somebody's account indefinitely.

The one thing it adds that typing does not is **concurrency**, which is
bounded at 8 per session.

**Audit.** One `control.Event` per accepted exec, written through the same
`EventRecorder` and `UnitOfWork` an attach uses: actor, workspace, session,
placement generation, timestamp, and **the command name only** — `path.Base
(argv[0])`, capped at 64 bytes, refused to the event (recorded as `"?"`) if it
is not printable ASCII. It is what was ASKED FOR rather than what ran —
`-- ./git` is recorded as `git` — because the name is settled at the plane and
argv[0] is resolved a hop later, in the sandbox; see open question 6. Never arguments. Never the environment — not values,
not names. Never the cwd. Never a byte, or a length of a byte, of input or
output. "Somebody ran `git` in this session at this time" is the whole of what
the record says, and it is enough to answer the question an audit log is for
without turning the log into a transcript. The event is written on
**acceptance**, so an exec that crashes the sandbox still leaves a record; the
exit code is deliberately not in it, because adding it means a second write
and v0 has one.

Nothing about an exec is logged by the plane: the splice already logs no
message, no byte and no length, and exec inherits that rule verbatim.

## Failure modes and edge cases

**Session suspended.** `409 session_not_running`, carrying the state. Not a
resume. Resuming is a state change that costs minutes, can fail, and changes
what the caller is billed for; a command that quietly resumed a cold session
would make `rainier exec s -- git status` an expensive surprise. A script that
means it writes `rainier resume s && rainier exec s -- git status`, which is
two facts in the order the caller chose.

**The command never exits.** No server-side timeout, in v1 or in the
foreseeable one: a build legitimately runs for an hour and a wrong number is
worse than none. The caller bounds it — Ctrl-C, or `timeout 600 rainier exec
…` — and Ctrl-C is forwarded as `exec_signal{INT}` first, with a second press
detaching and killing. What actually protects the sandbox is the concurrency
cap, not a clock.

**Output flood.** Backpressure, never drops: the sandbox stops reading the
pipes, the process blocks on `write(2)`, and the caller receives every byte
eventually or is closed by the write budget it already has. This is the
deliberate opposite of `session.trySend`'s force-detach, and the reason is the
consumer count: a stalled terminal viewer is one of many and must not hold the
session, while a stalled exec caller is the only reader its process will ever
have.

What this design did **not** originally account for is where that backpressure
lands. Every attachment on a session shares one relay conn and one writer on
it (`relay.connWriter`), so an exec caller that has stopped reading eventually
backs that writer up — and while it is backed up, the agent's terminal output
and the session RPC wait behind it.

Two bounds, and they answer different failures.

At the PLANE, an exec caller gets a shorter write budget than a viewer (twenty
seconds of taking *nothing*, against a viewer's minute, plus a byte allowance
at 64 KiB/s): an exec caller is a script rather than a person watching a
screen, and it loses nothing by being disconnected and re-run, while a viewer
disconnected mid-scrollback loses their session. That budget drops a caller
draining below about 2.8 KB/s within roughly twenty seconds, and dropping the
caller is what unwedges this hop.

At the SANDBOX, `connWriter.writeWithin` bounds the exec forwarder twice, and
the two numbers are different because only one of them is free. **Acquiring**
the writer is bounded at thirty seconds — nothing has been written when that
expires, so the conn is untouched and the cost is this exec alone, and the
budget is deliberately longer than the plane's own so a healthy exec is not
dropped merely because some other peer is in the process of being dropped. The
**write itself** is bounded at sixty seconds, and that bound is not free: a
WebSocket frame cannot be abandoned half-written, so the transport's answer to
an expired write context is to close the conn. It is set an order of magnitude
above anything a merely slow peer can reach (one readChunk is 58,320 bytes on
the wire after two base64 hops, so sixty seconds is under a kilobyte a second)
precisely so it fires only for a conn that is not moving at all — where the
plane's budget has already come and gone, nothing will ever make
`ServeSession`'s `Read` fail, and closing it is what makes `sessiond` redial.

A dropped exec is always TOLD: the forwarder sends its `FrameClose` before it
returns, so the caller sees a connection that ended with no exit status — 125,
with a sentence — rather than a stream that simply stops.

The cure for the underlying shape is still a writer per attachment rather than
one per conn, which is a change to the relay; it is
[open question 4](#open-questions).

**Stdin flood.** The opposite hop has the opposite answer, and for the same
reason turned around. A caller's stdin arrives on the relay's *demux* — the
single goroutine that reads every frame for every attachment on the session,
the terminal's keystrokes and the session RPC included — so blocking there to
apply backpressure would freeze the session, and would freeze it in a way with
no way out: the `FrameClose` from a disconnect and the `exec_signal` from a
Ctrl-C are both frames on the blocked demux.

So stdin is buffered, bounded, and an overrun is a **named refusal**
(`exec_error{stdin_overrun}`) rather than a silent truncation or a bare
disconnect. The bound is eight megabytes per exec, which is several seconds of
a fast link against a command that has stopped reading altogether and nothing
at all against one that is merely slower than the network for a moment. Real
flow control — the sandbox telling the plane to stop reading the caller's
socket — would remove the bound, and is [open question 5](#open-questions).

**PTY resize.** A `resize` on an exec attachment resizes **that exec's own
pty** and nothing else. It never reaches `session.SetSize`, so it cannot move
the agent's terminal, cannot participate in `applySizeLocked`'s
controllers-only rule, and cannot be used by a viewer to squeeze a laptop's
screen. Without `--tty` a resize is ignored — there is no terminal to size.
The CLI forwards `SIGWINCH` while it has a tty of its own.

**Concurrent execs.** Independent attachments, independent processes,
independent sockets, no ordering between them. They share one filesystem,
which is the caller's problem in exactly the way two open terminals are.

**Exec during a take-over on the terminal.** Nothing happens, in both
directions, and that is a property this design pins rather than hopes for. A
take-over advances the controller generation and re-binds every terminal
attachment; an exec attachment holds no binding, so `displace`'s fan-out does
not reach it, `Bind` is never called on it, and its frames — which carry no
generation — are not fenced by `mayWriteLocked`. Conversely an exec cannot
advance a generation: it never calls `Claim`, `Renew`, `Release`, or
`NextControllerGeneration`. A test asserts that running an exec through a real
plane leaves `controller_generation` and the lease holder untouched, and
another asserts that a take-over mid-exec does not interrupt it.

**The session's child exits mid-exec.** The exec keeps running.
`session.Session` deliberately outlives its agent so viewers can read the
scrollback, and an exec is not the agent — a `git push` finishing after the
agent has exited is the normal shape of cleaning up an unattended run.

**The sandbox dies mid-exec.** The relay conn dies, the plane closes the
client, the CLI exits **125** with `the connection to the session ended before
the command reported an exit status`. The command's effects on the workspace
are whatever they were; nothing pretends otherwise.

**The session is deleted or suspended mid-exec.** The same 125, and the same
reason: from the caller's side these are one event — the connection ended
without a status — and inventing a distinction the CLI cannot actually observe
would be worse than admitting the one it can. What it does do is NAME which,
by re-reading the session once, briefly and best-effort, on its way out: "the
session was stopped before the command reported an exit status", or "deleted",
or "the session's sandbox ended". A person reading a build log can act on the
difference; a script reading the exit code does not have to learn a fourth
reserved number to get it.

## Verification

One seam per hop, so that nothing needs a container to be tested except the
thing that is actually about containers.

| Seam | What is tested, and how |
|---|---|
| **sessiond spawn** | The exec runner takes a `start` func the way `session.New` does. A fake starter returns a scripted proc: exit codes, signals, an argv that does not exist, a cwd refusal, every env-rule rejection, the 8-exec cap, and the kill-the-group path (asserted by a fake that records the pid it was signalled with and whether the signal was negative). One test uses a **real pty** for `--tty`, asserting the size reaches `Setsize` and that stdout and stderr arrive merged. |
| **relay framing** | Table test over `FrameOpen` kinds: `""` reaches `session.Attach`, `"exec"` reaches the exec runner, and a fake `session.Session` **fails the test if `Attach` or `Bind` is called** on an exec id. A wire-shape test pins that a terminal frame's bytes are byte-identical to today's (the `omitempty`s), which is the whole compatibility claim. |
| **ownership isolation** | Through the real `attachplane`: an exec running while a terminal take-over happens, asserting the generation moves for the terminal and the exec is neither stamped nor interrupted; and an exec attach against a fake repository that **fails on any call** to `CompareAndAdvanceControllerGeneration` or `RenewControllerLease`. Reverting the "do not stamp exec frames" line must fail the first. |
| **controlapp policy** | Table over (policy grants control / grants view only / grants neither) asserting `AuthorizeAttachment` is asked with `AttachmentController` **exactly once** per exec, that a refusal is `control.ErrDenied` → `403`, and that a view-only principal is refused rather than silently downgraded — the attach path's "admit as a viewer" rule has no meaning here, because there is no reduced exec. |
| **route** | The status table above, each row pinned pre-upgrade: `404`, `403`, `409` *with the state in the body*, `503`, `501`. Plus: a sandbox whose first server message is a `snapshot` closes `exec_unsupported` and forwards no stdin. |
| **CLI exit mapping** | `exitCodeFor(result)` as a pure function, table-tested over exit 0/1/7/255, signal TERM/KILL, no-status, not-found, not-executable, cwd-refused — and a test that rainier's own sentences never reach stdout. |
| **the CLI's own loop** | `internal/execio` is its own package, and its tests drive it over a real websocket against a scripted plane: the spec in the first message, the streams apart, stdin and its explicit EOF, a signal rather than a code, a connection that dies mid-run, and every refused upgrade's envelope. |
| **e2e** | In `internal/e2e`, through a real relay into a real container: `-- sh -c 'echo out; echo err >&2; exit 7'` asserting stream separation and exit 7; `--tty` asserting merged output and a size; a flood (`yes | head -c 50M`) asserting every byte arrives and no viewer's scrollback changed; and a disconnect asserting the process is gone. |

## Rollout

Three parties, and the order is forced by who must be able to **refuse**
before anybody asks.

1. **`sessiond`, in the session image.** It teaches the sandbox the exec kind
   and the `exec_started` handshake. Nothing calls it yet. A session created
   after this roll can be exec'd into; every session created before it keeps
   its old `sessiond` for life and answers `exec_unsupported` forever — which
   is why the handshake exists and why this step is first rather than
   simultaneous.
2. **The plane** — `controld` and, separately, Cloud's cell-api/gateway. The
   route appears and answers `501` on runners without `exec.v1` and
   `exec_unsupported` on sandboxes that do not confirm. Nothing calls it yet.
3. **The CLI**, tagged last, for the same reason #84 tags it last: a client
   that can ask for something no deployed plane can answer produces support
   traffic, and a client that ships after the plane produces none.

Each step is separately revertable, and no step requires the one after it.

## Non-goals

- **A listing of running execs, or reattaching to one.** `--detach` ships
  without either (see [Detached exec](#detached-exec)): the caller has the
  pid, the sandbox has `ps`, and a reattach would need the output durability
  a log file already provides.
- **File copy.** `rainier push` and `rainier pull` exist and are bounded,
  checked and resumable. `exec … -- tar` is not a supported way to move files
  and gets no help from this design.
- **A browser UI.** Exec is a CLI and an API. The web attach surface is
  unchanged.
- **An implicit shell.** Argv is exec'd; a shell is something a caller names.
- **`--user`.** There is one identity in a sandbox and exec does not add a
  second.
- **A server-imposed timeout**, a per-exec resource limit, or port
  forwarding.
- **Exec on a suspended session.** It is a `409`, not a feature waiting to be
  built.

## Open questions

1. **Who owns the policy question.** This design asks the generic
   `Authorizer` for `ActionAttach` and `AttachmentPolicy` for
   `AttachmentController`, which means a host cannot permit attach and refuse
   exec. The alternative is a new `control.ActionExec`, which every existing
   authorizer adapter would see as an unknown verb and (correctly) fail closed
   on, breaking exec on every self-hosted installation until its adapter
   learns the word. **Recommendation: ship the reuse**, since the authority is
   genuinely identical, and add the action only when a host asks to separate
   them — at which point the fail-closed week is a deliberate, announced cost
   rather than a side effect of a rollout.
2. **Exec while another device holds the terminal controller lease.** This
   design allows it: the lease governs one pty, an exec is not that pty, and
   refusing would disable exec exactly when it is most wanted — a headless run
   somebody is watching. The counter-argument is real: a `git checkout` from
   an exec, under a human's live session, is a surprise with no visible cause.
   A notice in the session's terminal is **not** the mitigation, because
   writing into the pty is the one thing this design promises never to do; the
   mitigation is the audit event. **Recommendation: allow it**, and revisit if
   the audit event proves too slow a way to find out.
3. **Whether `--json` may move the command's bytes to stderr.** §6.2 says a
   `--json` document is the only thing on stdout, and honouring it literally
   is what produces the rule above. The alternative is exempting exec from
   §6.2 so both can share stdout with a framing the caller has to parse, which
   is worse for every consumer except the one that wants both at once.
   **Recommendation: keep the rule**, and treat "stream *and* structured
   result" as a request for two invocations.
4. **One writer per conn, or one per attachment.** Every attachment on a
   session shares `relay.connWriter`, so a peer that stops reading backs up
   the agent's terminal and the session RPC behind it. Exec makes this easier
   to reach than attach did — an exec caller legitimately stops reading, where
   a person watching a screen does not — and the mitigation in this version is
   a plane-side budget that drops the slow caller plus a sandbox-side bound on
   acquiring the shared writer. Neither takes the writer away from a peer that
   is holding it inside `conn.Write`, because a WebSocket frame cannot be
   abandoned half-written; only a writer per attachment can.
   **Recommendation: give each attachment its own writer**, as a change to
   `internal/relay` rather than to this design, and revisit the budgets
   afterwards.
5. **Stdin flow control.** Because the demux cannot block, a caller's stdin is
   buffered to a bound and an overrun ends the exec with
   `exec_error{stdin_overrun}`. That is honest and bounded, and it is still a
   bound where the output direction has none.
   **Recommendation: add a credit to the exec protocol** — the sandbox
   reporting how much more stdin it will take — when a real workload hits the
   bound, and not before: a mechanism added against an imagined workload is a
   mechanism nobody can tune.
6. **What an audit record's command name is worth.** It is
   `path.Base(argv[0])` as the CALLER typed it, so `rainier exec s -- ./git`
   runs a file the caller planted and is recorded as `git`. Recording the
   RESOLVED path's base would make it a fact, but the resolution happens in
   the sandbox and the event is written at the plane, one hop earlier.
   **Recommendation: read the field as "what was asked for" rather than "what
   ran"**, say so where it is defined, and move the record to the sandbox's
   answer only if an audit reader ever needs the stronger claim.

# Terminal Happy Path — Design (Rainier v0, Plan 6)

**Date:** 2026-08-30 · **Status:** Approved for implementation · **Author:** Codex (with Josh)
**Parent spec:** `2026-08-27-rainier-design.md` §§1, 5, 7, 9
**Predecessor:** Plan 5 merged; live GCE latency baseline recorded 2026-08-30

## 1. Problem statement

Rainier can create and attach to a running session quickly, but the terminal
client still exposes lifecycle details. `rainier attach <session>` fails when
the session is warm- or cold-suspended, so the user must run `resume` and then
`attach`. A live attach also exits on a transient WebSocket interruption even
though sessiond, its PTY, and its sequence-numbered log survive it.

The v0 terminal contract should be simpler: `new` enters a new session and
`attach` enters an existing one. The CLI, not the user, handles the session's
current lifecycle state and brief control-plane or network interruptions.

Live measurements on a warmed GCE runner establish the starting point:

| Interaction | Median | p95 |
|---|---:|---:|
| New session to usable Bash | 1,005 ms | 1,105 ms |
| Attach by id to first frame | 99 ms | 108 ms |
| Attach by name to first frame | 163 ms | 170 ms |
| Terminal input to shell response | 29–31 ms | 36–54 ms |
| Warm resume to usable | 301 ms | 329 ms |
| Cold resume to usable | 589 ms | 640 ms |

These are sequential, warmed, same-region dogfood measurements. They are not
claims about cold image pulls, environment setup, agent CLI boot, burst load,
or other geographies.

## 2. Goals and non-goals

### Goals

1. `rainier attach <id|name>` attaches directly when the session is running.
2. The same command resumes a warm- or cold-suspended session, waits for it to
   become attachable, and opens the terminal.
3. A connected `rainier attach` automatically reconnects after a transient
   transport or gateway failure, resuming after the last sequence it rendered.
4. Ctrl-] remains an immediate, explicit detach and an `exit` frame remains a
   terminal outcome; neither triggers reconnection.
5. The existing `--since` contract is preserved: omitted means current screen,
   `--since 0` means the full log, and `--since N` means entries after N.
6. A checked-in live-fleet benchmark can reproduce the v0 latency metrics with
   synthetic session names and machine-readable output.

Warmed-fleet regression targets are p95 <1.5 s for create-to-usable, <150 ms
for attach by id, <225 ms for attach by name, and <75 ms for terminal RTT.
They are release targets reported by the benchmark, not timing assertions in
unit tests: CI scheduling noise is not a product latency measurement.

### Non-goals

- Infrastructure checkpoint, snapshot, restore, or VM-loss recovery.
- Agent installation, per-user Claude/Codex credential homes, or
  `rainier agent-login`; those are the next Plan 6 slice after terminal entry.
- Predictive local echo.
- Changing suspend policy or optimizing the measured 5.35 s cold-suspend path.
- A new control-plane endpoint, session state, or persisted field.

## 3. Relevant context and constraints

- The control plane already exposes `GET /v1/sessions/{id}` and
  `POST /v1/sessions/{id}/resume`. The CLI should compose them rather than add
  a convenience endpoint whose only consumer is the CLI.
- Attach authorization remains owner-or-admin and is enforced by controld.
  The state read is team-visible under the existing v0 trust model; it does
  not widen attach authorization.
- Controld already waits briefly for a creating session and answers 503
  `session_not_ready` when its bounded wait expires. `new` has a 60-second
  retry loop for precisely that response.
- `attachio.Run` currently treats disconnect, detach, and process exit as the
  same nil return. Its stdin goroutine can remain blocked after a disconnect;
  looping around the function without first fixing that ownership would create
  multiple readers racing for the local terminal.
- Session snapshots and output frames carry the server's sequence number. A
  reconnect can therefore request only entries after the last rendered frame.
- All checked-in benchmark identities and session names must be synthetic.

Assumption: once an attach has successfully connected, retrying transport
failures until the user detaches is preferable to returning them to a shell.
During a retry the local terminal returns to cooked mode, so Ctrl-C can stop a
long outage; input is never buffered and replayed into the remote PTY later.

## 4. Proposed design

### 4.1 State-aware attach in `cmd/rainier`

The existing session reference resolution remains the only name-resolution
path. After it produces an id, `runAttach` reads that session once:

- `running`, `creating`, or `queued`: enter the existing bounded attach retry.
- `suspended_warm` or `suspended_cold`: POST `resume`, then enter the same
  bounded attach retry.
- `failed`: attempt attach, preserving Plan 5's failed-setup diagnosis path.
- other terminal states: return a direct error naming the state.

If a concurrent client wins the resume race, the losing client's POST can
return an error even though the desired state was reached. On a resume error,
the CLI re-reads the session: if it is now running or creating, it continues;
otherwise it returns the original error. This avoids parsing error strings and
does not hide a failed resume.

`new` and `attach` share one `attachWithRetry` entry point. The first WebSocket
dial retains the existing 60-second readiness budget; permanent HTTP failures
(authentication, authorization, not found) still fail immediately.

### 4.2 Explicit attach outcomes

`internal/attachio.Run` returns an `Outcome` plus an error. Its reason is one
of `detached`, `disconnected`, or `exited`; it also carries the last rendered
sequence and the exit code when applicable. Dial and local-terminal setup
failures remain errors.

The stdin pump becomes lifecycle-bounded. It polls the stdin descriptor with a
short timeout, checks the attempt's done signal, and exits before `Run` returns.
That makes repeated calls safe: at most one goroutine owns stdin, stdout is no
longer touched after return, and a reconnect never inherits stale input.

The ideal caller is deliberately small:

```go
cursor := requestedCursor
for {
    outcome, err := attachio.Run(ctx, url, header, cursor)
    if err != nil { /* bounded retry if retryable */ }
    if outcome.Reason != attachio.Disconnected { return nil }
    cursor = outcome.LastSeq
}
```

`rattach` consumes the richer result but stays one-shot; automatic recovery is
a product behavior of `rainier`, not a requirement of the single-runner dev
tool.

### 4.3 Reconnect policy

After the first successful attach, `rainier` retries:

- pure transport failures;
- HTTP 429, 502, 503, and 504 responses.

Other HTTP responses are permanent and return immediately. Backoff starts at
100 ms and doubles to a 2 s cap. The CLI prints one connection-lost line, a
short retry status, and a reconnected line. It does not replay or log terminal
contents. The reconnect cursor is the last sequence actually written to local
stdout, so the user sees neither a gap nor a deliberate full-log replay.

### 4.4 Latency benchmark

A developer tool under `cmd/rainier-latency` drives the real configured API,
the built CLI process, WebSocket attach, and a command/expected-output probe.
It measures create acknowledgement, running state, usable terminal, attach by
id/name first frame and terminal RTT, and warm/cold resume-to-usable. Flags
control sample counts and whether destructive cold-suspend samples run.

Output is JSON Lines: one record per observation plus one summary per metric
(n, mean, p50, p95, min, max, and the applicable warmed-fleet target). Session
names use a `latency-test-` prefix plus random synthetic suffix. Every created
session is removed in deferred cleanup, and cleanup failures make the tool
exit non-zero. No session output, repo contents, tokens, user identity, runner
names, or production-derived identifiers are emitted.

Alternatives considered:

- **Server-side `POST /sessions/{id}/enter`:** rejected because resume and
  attach have different transports and lifetimes; composing existing APIs in
  the CLI keeps the control plane honest and reusable.
- **Reconnect by recursively calling today's `Run`:** rejected because its
  blocked stdin goroutine outlives a disconnected attempt and would race the
  next reader.
- **Timing assertions in ordinary tests:** rejected because scheduler and
  localhost noise make them flaky and they do not measure the deployed path.

## 5. Edge cases and failure handling

- A session deleted between state read and resume/attach returns the existing
  not-found error.
- A cold resume with no capacity returns the existing named 409; attach does
  not conceal it behind retries.
- Two simultaneous attaches to a suspended session converge when one resumes
  it; the other re-reads and continues if the session advanced.
- A session that exits while disconnected eventually sends an exit frame after
  reconnection; the CLI stops instead of reconnecting again.
- A cursor outside retained history falls back to sessiond's current-screen
  snapshot, preserving the existing server contract.
- A disconnect before the first frame reconnects from the original cursor,
  not from an invented zero cursor.
- Bytes typed while disconnected are not queued. This prevents a command typed
  against an outage message from executing unexpectedly after recovery.
- Context cancellation and Ctrl-] stop retries and restore the terminal.
- Benchmark interruption still attempts removal of every synthetic session;
  it never deletes a session it did not create.

## 6. Scalability and extensibility

Reconnect is per attached client and holds no server-side durable state beyond
the existing attach table. Exponential backoff prevents a control-plane outage
from becoming a tight reconnect loop. The explicit `Outcome` makes a future
configurable retry budget possible without inferring causes from printed text.

State-aware attach deliberately understands only existing lifecycle states.
If infrastructure-native restoring introduces a `restoring` state later, it
can join the waitable set without changing the terminal transport.

The latency tool reports raw observations so later burst and geographic runs
can use an external analysis system without changing the user CLI.

## 7. Verification strategy

1. `cmd/rainier` tests with real httptest HTTP handlers pin running attach,
   warm/cold automatic resume, concurrent resume convergence, terminal-state
   refusal, and permanent resume errors.
2. `internal/attachio` tests pin distinct outcomes, last-sequence tracking,
   disconnect before first frame, no post-return stdin/stdout access under the
   race detector, and cursor propagation.
3. A CLI integration test runs a scripted sessiond through controld, drops the
   first attach connection, and proves the next connection resumes after the
   last observed sequence.
4. `go test -p 1 ./...` and focused `-race` suites gate the branch.
5. `make e2e` drives a real local Postgres/controld/runnerd/session container:
   create, detach, warm suspend, one-command attach, forced connection drop,
   resumed output, cold suspend, one-command attach, and cleanup.
6. The live benchmark is run against the warmed GCE dogfood fleet. Its summary
   is compared with the targets above; the benchmark itself does not make CI
   timing-sensitive.

## 8. Open questions

None for this slice. The next design decision is how per-user agent credential
homes are named, mounted, backed up, and garbage-collected; this design does
not pre-empt it.

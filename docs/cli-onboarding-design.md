# CLI readiness and onboarding

## Problem and goals

The top-level manual obscures common commands, builds lack a version command,
and an initial attach can end with an unexplained 503. Make help fit within 26
nonblank lines, expose honest build metadata, and explain observed readiness
without changing session lifecycle or adding provisioning.

## Context and design

Keep stdlib flags and existing CLI/client boundaries. Add command help and version
dispatch before any config or network access; rehome the existing manual details.
`make build` uses `scripts/build.sh` to stamp the actual source worktree's revision
and dirty status. It disables automatic Go VCS stamping: on the supported local
toolchain that discovery can identify the enclosing checkout instead of a nested
worktree. Direct unstamped builds and source archives report plain `dev`. Explicit
linker-injected release versions take precedence. A synthetic nested-worktree
build regression checks clean/dirty attribution and the source-archive fallback.

`doctor` loads only the active config, then uses `cli.NewClient` and `DoContext`
against existing `/v0/me`, `/v0/runners`, `/v0/environments`, and `/v0/agents`.
One 15-second context bounds the whole report including normal token refresh;
individual requests have shorter bounds. Basic readiness requires a usable
config, authentication, workspace scope when hosted, and observed connected
runner capacity. Environments and agent credentials are optional for a basic
session and warnings for coding-agent readiness. Stop dependent probes after
authentication fails. Backend version remains unknown without advertised evidence.

Reports use PASS/WARN/FAIL, safe bounded terminal text, and concrete next actions.
Do not print raw errors, bodies, config, credentials, or URL secrets. Classify
HTTP/transport failures; preserve Retry-After without broad client changes. A 404
means unavailable/compatibility unestablished, never an inferred upgrade demand.

After initial attach retry exhaustion only, bounded GET observations explain
queue state, disconnected runners, evidenced capacity pressure, or uncertainty.
Show a shell-safe reattach command and doctor guidance. Do not delete, recreate,
provision, or change config. Established-stream reconnect behavior is unchanged.

## Alternatives and limits

An interactive backend wizard would cross provisioning/credential boundaries;
a new CLI framework would expand risk without helping this bounded task. Neither
is included. No JSON report, public API schema, npm artifact, release or tag changes.
Readiness is a point-in-time observation, not a promise that placement will succeed.

## Slices and verification

1. Compact and command-specific help, version metadata, build documentation;
   subprocess tests prove exit codes and operation without login/network.
2. Doctor and narrow HTTP error metadata; local HTTP tests cover missing/corrupt
   config, auth, status/transport errors, timeouts, malformed responses, runner
   capacity, optional endpoints, refresh, and safe output.
3. Initial attach guidance and onboarding docs; local WebSocket regression tests
   prove session retention, honest diagnosis, and preserved reconnect behavior.

Watch new behavior tests fail before implementation; run focused tests per slice,
then `make verify`, built-CLI local HTTP/WebSocket acceptance, and independent and
adversarial read-only reviews. Live infrastructure and credentials are out of scope.

## Open questions

No product choices remain. Existing routes must expose enough observation; missing
optional evidence is reported honestly rather than expanding backend scope.

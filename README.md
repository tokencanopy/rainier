# Rainier

<p align="center">
  <img src="assets/project-rainier-lockup.png" alt="Project Rainier logo: snowcapped mountain and pine trees" width="512">
</p>

Self-hostable infrastructure for running your coding agents — Claude Code,
Codex, Gemini CLI, and friends — in your own cloud, while they feel like local
terminal sessions. Sessions survive laptop sleep and network changes; attach
shows the agent's real TUI from any machine; your subscriptions and your GitHub
identity stay yours.

Status: **v0 Plan 4 — control plane + CLI + environments.** One `controld`
(Postgres-backed REST + WebSocket API, GitHub identity, least-loaded
placement) fronts N `runnerd` VMs that dial it outbound, and the `rainier` CLI
drives the whole fleet. Deploying it:
[`docs/deploy-gce.md`](docs/deploy-gce.md). Design:
[`docs/superpowers/specs/2026-08-27-rainier-design.md`](docs/superpowers/specs/2026-08-27-rainier-design.md),
[`docs/superpowers/specs/2026-08-28-plan3-controld-design.md`](docs/superpowers/specs/2026-08-28-plan3-controld-design.md)
and
[`docs/superpowers/specs/2026-08-29-plan4-environments-design.md`](docs/superpowers/specs/2026-08-29-plan4-environments-design.md).

## Quickstart

```bash
make build

# Point the CLI at your controld and log in with your GitHub identity.
# --from-gh borrows the token from the `gh` CLI; --token <t> and
# --client-id <id> (device flow) are the alternatives.
bin/rainier login --from-gh --server http://rainier-1:9090

bin/rainier new --name box1 --image rainier-session:latest   # creates, then attaches
bin/rainier ls                                               # id, name, env, state, runner, reachable, age
bin/rainier attach box1                                      # Ctrl-] detaches; the session keeps running
bin/rainier suspend box1 && bin/rainier resume box1
bin/rainier rm box1
```

`rainier new` attaches immediately by default so you watch the agent boot;
`--detach` opts out. Names are per-owner, so `<id|name>` takes either.

An **environment** is the reusable template a session starts from — a base
image, a setup script, an egress allowlist, and the team secrets its sessions
get as environment variables:

```bash
printf %s "$GH_PAT" | bin/rainier secret set GH_TOKEN   # write-only; stdin keeps it out of your history
bin/rainier env create dev --image node:22 \
  --setup-file ./setup.sh --secret-ref GH_TOKEN \
  --egress registry.npmjs.org,github.com
bin/rainier env ls                                      # name, id, image, cached
bin/rainier new --name box2 --env dev                   # ...starts from it
```

The first session on an environment runs the setup script live — attach and
you watch it — and its image is then snapshot-cached for the next one;
editing the image or the script invalidates that cache without touching any
running session. `--image`/`--egress` on `new` override the environment for
that one session. Setup scripts should install into image-visible paths —
`$HOME`, or `/opt/rainier-env` on the stock session image, which is already on
`PATH` — because that is what the snapshot carries forward, while `/workspace`
is a per-session volume the snapshot excludes. Details, including the one
hardening flag a first build trades away and why `/usr/local` is not the
install prefix, in [`docs/deploy-gce.md`](docs/deploy-gce.md) §7.

Locally, `make e2e` brings the whole stack up on your own machine (Postgres in
docker; controld, egressd and a dial-mode runnerd on the host, with runnerd
driving real containers) and drives that same CLI flow end to end, secrets and
environments included — the dress rehearsal for a real deploy.

## What runs where

| Component | Where | Talks to |
|---|---|---|
| `rainier` | your laptop | controld (HTTPS/WSS) |
| `controld` | one VM | Postgres; accepts runner and client connections |
| `runnerd` | each runner VM | dials controld outbound; drives docker |
| `egressd` | each runner VM | the session allowlist proxy (egress R4) |
| `sessiond` | inside each session container | dials its runnerd outbound |

Reachability is outbound-only in one direction (spec rule 3): sessiond →
runnerd → controld, and clients talk only to controld. Nothing dials into a
runner.

`runnerctl` and `rattach` are **dev tools**, not the product surface: they
drive one runnerd's local HTTP API directly, bypassing the control plane
(no identity, no placement, no durable state). They stay in-tree for
debugging a single box — `rainier` is the CLI to use.

## Tests

```bash
go test ./...                    # unit + contract suites (no services needed)
go test ./internal/e2e/ -race    # in-process end-to-end scenes: chaos, and environments
make e2e                         # full stack on docker, driven by the real CLI
./scripts/egress-check.sh        # egress R4 acceptance (exit 3 = skipped on VM-backed docker)
```

The store contract suite runs against memstore by default and against
Postgres when docker is available; the e2e scenes take
`RAINIER_TEST_PG_DSN=postgres://…` to run the same scenarios against pgstore.

License: [Apache-2.0](LICENSE)

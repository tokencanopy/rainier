# Deploying Rainier on GCE (the v0 dogfood target)

One `e2-medium` in GCP project `rainier-cloud`, reached over Tailscale. No load
balancer, no domain, no certificates, no systemd units — v0 is honest about
being one box you can rebuild in twenty minutes. Steps 1–6 are the Plan 3
design's §4.10 deployment story written out as commands; step 7 is Plan 4's
environments on top of it.

**What you end up with**

```
laptop                                  rainier-1 (GCE e2-medium, Debian 12)
──────                                  ─────────────────────────────────────
rainier CLI ──── tailnet ─────────────▶ controld :9090  ──┐  Postgres (docker)
                (MagicDNS name)                           │
                                        runnerd :8080 ◀───┘  (outbound WS)
                                        egressd :3128
                                        session containers on rainier-internal
```

Nothing listens on a public address. The VM's only inbound path is the
tailnet; the only inbound path for a session container is the runnerd
register listener on the docker bridge gateway.

---

## 1. Provision the VM

From your laptop, with `gcloud` authenticated and project `rainier-cloud` created
and billed:

```bash
./scripts/gce-up.sh                 # PROJECT/ZONE/VM/MACHINE_TYPE override the defaults
```

It creates `rainier-1` if it isn't there, installs Docker, Tailscale, git and
Go, and stops. Re-running it is safe and cheap.

**Go comes from the official tarball into `/usr/local/go`, not from apt.**
Debian bookworm's `golang-go` is 1.19 and this repo's `go.mod` requires
1.25.0, so an apt Go fails `make build` outright with `go.mod requires go >=
1.25.0`. `gce-up.sh` installs `GO_VERSION` (default 1.25.0) and appends
`/usr/local/go/bin` to `~/.profile`; bump `GO_VERSION` and re-run to upgrade.

Then join the tailnet, on the VM:

```bash
gcloud compute ssh rainier-1 --project rainier-cloud --zone us-west1-b
sudo tailscale up                   # authenticate in the browser it prints
tailscale status                    # note the MagicDNS name — "rainier-1" below
exit && gcloud compute ssh rainier-1 --project rainier-cloud --zone us-west1-b
go version                          # → go1.25.0 (from ~/.profile's PATH)
```

The second ssh is not superstition: `usermod -aG docker` and the new PATH both
only take effect on a fresh login, and every step below needs both. If
`go version` prints 1.19 or nothing, your shell found apt's Go (or none) —
`export PATH=$PATH:/usr/local/go/bin` and check `~/.profile`.

From your laptop, confirm the tailnet actually resolves the VM — everything
after this depends on it:

```bash
tailscale status | grep rainier-1
ping -c1 rainier-1
```

## 2. Build

On the VM:

```bash
git clone https://github.com/tokencanopy/rainier.git
cd rainier
make build                          # → bin/{controld,runnerd,egressd,rainier,sessiond,...}
docker build -t rainier-session:latest .   # the session image (also done by fleet-up.sh)
```

## 3. Postgres

Postgres is the only durable store (spec state rule: no second store). One
container, bound to loopback — nothing outside the VM ever talks to it:

```bash
docker run -d --name rainier-pg --restart unless-stopped \
  -e POSTGRES_PASSWORD="$(openssl rand -hex 16)" -e POSTGRES_DB=rainier \
  -v rainier-pgdata:/var/lib/postgresql/data \
  -p 127.0.0.1:5432:5432 postgres:16-alpine
```

Keep the password: it goes in the DSN below. (Save it somewhere real — the
named volume outlives the container, so a lost password means a lost fleet
history, not just a restart.)

```bash
export RAINIER_DB="postgres://postgres:<the password>@127.0.0.1:5432/rainier?sslmode=disable"
```

controld runs its own migrations on startup; there is no separate migrate
step.

## 4. controld

The fleet token is the shared secret every runnerd presents. Generate it
once and keep it for step 5 — controld and every runner must agree on it:

```bash
export RAINIER_RUNNER_TOKEN="rnr-$(openssl rand -hex 24)"
echo "$RAINIER_RUNNER_TOKEN" > ~/.rainier-runner-token && chmod 600 ~/.rainier-runner-token
```

controld also requires a secrets key: 32 bytes of hex that team secrets are
AES-256-GCM-encrypted with at rest. Generate it once and keep it — losing it
loses the stored secret **values** (and nothing else in the database); the
recovery is an admin re-running `rainier secret set` for each name.

```bash
export RAINIER_SECRETS_KEY="$(openssl rand -hex 32)"
echo "$RAINIER_SECRETS_KEY" > ~/.rainier-secrets-key && chmod 600 ~/.rainier-secrets-key
```

`--external-url` must be the **tailnet MagicDNS name**, not `localhost`: it is
what controld hands runners as the attach dial-back target, and a runner
refuses a dial-back whose origin isn't its own controld. On this single-VM
deployment the runner is on the same host, so it dials the same name back —
which is exactly what makes the laptop and the runner agree on one identity
for this replica.

```bash
export RAINIER_ADMINS="<your-github-login>"      # fail-closed: empty means nobody can log in
nohup ./bin/controld \
  --listen :9090 \
  --db "$RAINIER_DB" \
  --runner-token "$RAINIER_RUNNER_TOKEN" \
  --secrets-key "$RAINIER_SECRETS_KEY" \
  --external-url http://rainier-1:9090 \
  --admins "$RAINIER_ADMINS" \
  > /tmp/controld.log 2>&1 &
curl -sf http://rainier-1:9090/healthz && echo   # → ok
```

`--listen :9090` (dual-stack, not `0.0.0.0:9090`) matters: MagicDNS resolves
`rainier-1` to the tailnet IPv6 address first on the VM itself, and an
IPv4-only bind fails its own health check. No firewall rule is opened,
deliberately: GCE's default ingress denies 9090 from the internet, and the
tailnet interface is where the traffic actually arrives. Add `--members` for teammates who should
get the non-admin role. Allowlist entries are matched case-insensitively, as
GitHub logins are — `Alice` in `--admins` admits the account GitHub reports as
`alice`.

Every flag also reads a `RAINIER_*` environment variable
(`RAINIER_LISTEN`, `RAINIER_DB`, `RAINIER_RUNNER_TOKEN`, `RAINIER_SECRETS_KEY`,
`RAINIER_ADMINS`, `RAINIER_MEMBERS`, `RAINIER_EXTERNAL_URL`, `RAINIER_GITHUB_API`), so the
command above shrinks to `./bin/controld` once those are exported — an
explicit flag always wins over the environment.

## 5. The runner fleet

`fleet-up.sh` builds the session image, starts egressd, probes whether this
platform can enforce R4 (on Linux dockerd it can — expect
`R4 egress enforcement: ON`), creates the `rainier-internal` network, and
starts runnerd. Setting `CONTROLD_URL` is what puts runnerd in dial mode:

```bash
CONTROLD_URL=ws://rainier-1:9090 \
RAINIER_RUNNER_TOKEN="$(cat ~/.rainier-runner-token)" \
RUNNER_NAME=rainier-1 \
  ./scripts/fleet-up.sh

grep 'connected' /tmp/controld.log     # → controld: runner rainier-1 connected (used 0/16, ...)
```

Adding a second VM later is exactly steps 1, 2 and 5 with a different
`RUNNER_NAME` and `CONTROLD_URL=ws://rainier-1:9090` — no controld config
change, no restart (success criterion 6).

To stop the fleet: `./scripts/fleet-down.sh`. controld is a plain background
process: `kill` it. Sessions survive both (that is the whole point).

## 6. Log in from the laptop

```bash
make build                                          # local bin/rainier
./bin/rainier login --from-gh --server http://rainier-1:9090
./bin/rainier new --name box1 --image rainier-session:latest
# → prints the id, then attaches: you are in the container's shell
#   Ctrl-] detaches; the session keeps running
./bin/rainier ls
./bin/rainier attach box1
```

`login --from-gh` shells out to `gh auth token`. Without the `gh` CLI, use
`--token <github-token>`, or `--client-id <oauth-app-client-id>` to run the
device flow.

## 7. Environments, secrets, and the setup cache

An **environment** is the reusable template a session starts from: a base
image, a setup script, an egress allowlist, and the team secrets its sessions
get as environment variables. `rainier new --env <name>` is the daily driver;
scratch sessions (`--image`) keep working exactly as before.

```bash
# A team secret. Piped on stdin, never passed as an argument — that keeps the
# value out of your shell history and out of the process table. Values are
# write-only: nothing in this CLI or the API reads one back, so replace a
# secret you have lost rather than looking it up.
printf %s "$GH_PAT" | ./bin/rainier secret set GH_TOKEN
./bin/rainier secret ls                    # names and timestamps only

cat > setup.sh <<'EOF'
#!/bin/sh
set -eu
# Cached: the snapshot keeps the container's own filesystem. $HOME is the
# portable choice (node:22 gives uid 1000 a real one); the stock
# rainier-session image also offers /opt/rainier-env, already on PATH.
npm config set prefix "$HOME/.npm-global"
npm install -g typescript
# Per-session: /workspace is a volume, and the snapshot excludes volumes.
npm ci --prefix /workspace --cache /workspace/.npm
EOF

./bin/rainier env create web \
  --image node:22 \
  --setup-file ./setup.sh \
  --secret-ref GH_TOKEN \
  --egress registry.npmjs.org,github.com

./bin/rainier env ls                       # NAME ID IMAGE CACHED
./bin/rainier new --env web --name web1    # creates, then attaches
```

The first session on an environment runs the setup script live — attach and
you watch it happen, because it is the session's own first child and its
output is the session's output. When it exits 0, controld asks the runner to
commit the container as `rainier-env:<env id>-<first 12 hex of the setup
hash>`, records that ref, and tells every other runner to pre-pull it. `env
ls` then reads `CACHED yes`, and later sessions boot that image with no setup
step. Editing the image or the script moves the hash, `CACHED` drops back to
`no`, and the next session rebuilds; sessions already running are never
touched, because each pinned its resolved image at create.

**Install into image-visible paths, not `/workspace`.** `docker commit`
excludes volumes, so anything a setup script writes under `/workspace` is
per-session: the session that ran the script has it, and every session booted
from the cache starts with an empty workspace. Only writes to the container's
own filesystem end up in the snapshot. So `npm ci --prefix /workspace` gives
you a fresh install every time, while a toolchain installed into an
image-visible path is cached once and reused. Use `/workspace` for the
per-session preparation you actually want repeated.

Those paths have to be writable by the **session user (uid 1000)**, which the
image decides. On the stock `rainier-session:latest` they are **`$HOME` and
`/opt/rainier-env`** — the image creates the user, gives it that prefix, and
puts `/opt/rainier-env/bin` on `PATH`, so binaries a setup script installs
there are simply found. `/usr/local` is deliberately NOT one of them (see
below). Other images set their own policy: `node:22` and friends ship a
uid-1000 user with a real `$HOME` but a root-owned `/usr/local`, so a bare
`npm install -g` fails there; a custom image may chown whatever prefix it
likes. Only `/opt/rainier-env/bin` is on `PATH` for free — any other prefix
needs the image or the agent's shell init to add it. An image with no uid-1000
user at all gives it `HOME=/` on a root-owned rootfs, and a setup script there
can write nothing the cache could keep.

**The first build per environment edit runs with a writable rootfs.** That is
the one hardening flag rainier trades away, and only on the container that has
a setup script to run: it has to be able to install, and a read-only rootfs
makes that impossible. Every session afterwards boots the cached image with
`--read-only` back on, which is the population that matters. Nothing else
differs between the two: same unprivileged user, same `no-new-privileges`, same
tmpfs, same workspace volume, same egress wiring.

That window is why the install prefix is `/opt/rainier-env` and not
`/usr/local`. `/usr/local/bin` holds `sessiond` — the session's PID 1 — and
stays root-owned, so the session user cannot rewrite it even while the rootfs
is writable. A setup script is not trusted code: an agent runs inside these
containers and may be prompt-injected (design §10), and a PID 1 it could
replace would be baked into the cached image every later session of that
environment boots. Cache poisoning of **user-level** binaries under the install
prefix is still possible and is inherent to any shared build cache — the same
class of trust a malicious npm package already has. The platform's own agent is
what must stay out of reach, and it does.

**Secrets and the setup channel never reach the cached image.** The commit
strips every variable the create injected — an environment's resolved
`secret_ref` values — along with the setup script, the session's own identity,
and the egress proxy URLs (which embed that session id), so `docker image
inspect rainier-env:…` shows none of them. A session booted from the cache gets
all of it injected fresh at create (secrets are resolved per session, not baked
per image) and runs no setup script, which is the entire point of the cache.

**Where a secret is in plaintext.** Encrypted at rest in Postgres, decrypted by
controld at dispatch, and then sent to the runner **in the clear over the
controld→runnerd websocket** — that hop is not separately encrypted, and the
value lands as an ordinary environment variable inside the container, readable
by the agent (the spec's honest v0). The **tailnet (or VPC) is the transport
boundary that makes that acceptable**: nothing in this deployment listens on a
public address, and controld's own `--listen` is reachable only over it. A
deployment that exposes controld beyond a private network needs TLS on that hop
before it needs anything else here.

**Debugging a setup that failed.** A failed setup leaves the session `failed`
with the last 2KB the script printed in its `error` — enough to see, not always
enough to diagnose. The container is still there and sessiond is still serving
viewers, so **attach to it and read the whole log**:

```bash
./bin/rainier ls --all              # the failed session, with its runner
./bin/rainier attach --since 0 <id> # replays everything the setup printed
./bin/rainier rm <id>               # frees the slot
```

Attach works on a `failed` session for exactly as long as its runner holds its
current control connection. **`--since 0` is the flag that matters here:** a
plain `attach` opens on the current screen (the last ~24 rows), while
`--since 0` replays the whole event log — the full setup output, not the tail.
`--since N` resumes after sequence number N, which is what the disconnect line
prints when an attach drops.

**A failed session keeps its slot until you remove it** —
deliberate, so the evidence outlives the failure, but a forgotten one costs the
fleet a slot. **Read the log before restarting runnerd:** a restart re-announces
the container, reconciliation sees a session the store has already finished, and
collects it as an orphan (§4.8) — the row stays, the log is gone.

**Pre-pull failures on other runners are expected.** When an environment caches
a snapshot, controld tells every other connected runner to pull it. v0 has no
registry, so that ref names an image only the runner that built it has, and
those pulls fail by construction — you will see one informative line per runner
in `/tmp/runnerd.log`. Nothing is broken: a session placed on a non-holder
runner is dispatched with the setup script and rebuilds the environment there,
exactly as it would have with no pre-pull at all. The scheduler prefers the
holder anyway, so this is rare in practice, and it goes away with the
registry-backed distribution the design defers to §6.

**Setup scripts must be idempotent.** A cold-parked session that is resumed
starts its container again, and sessiond re-runs the setup wrapper on that
boot — the script runs a second time, over a `/workspace` that already has
the first run's output in it. Harmless for the cache, not harmless for a
script that appends to a file or assumes a clean directory.

**The session image must provide `sh` and `chown`.** Before the session
container starts, the driver initializes its fresh workspace volume with a
one-shot `sh -c` container built from that same image (a new named volume's
root is `root:root`, and the session runs as uid 1000). A distroless or
`FROM scratch` image therefore fails `Create` loudly — `initialize workspace
volume rainier-ws-…: the image must provide sh and chown` — rather than
producing a session with an unwritable workspace. It should also carry a
uid-1000 user, for the reason above.

**`--from-devcontainer` reads plain JSON only.** It is a one-field hint: it
takes `image` and prints every key it ignored. devcontainer.json is officially
JSONC, and a file with `//` comments or trailing commas is rejected with a
message that says so and points at `--image`.

`--placement <runner>` pins an environment's sessions to one runner (the
GPU-box or hardware-attached case). A pin the fleet cannot honor — the runner
is full, or has not joined — leaves the session `queued` with a visible
`waiting for runner <name>` in `rainier ls`, rather than silently placing it
somewhere else. `env rm` refuses while non-terminal sessions still reference
the environment, and says how many.

## 8. Rehearse locally first

`./scripts/e2e-fleet.sh` (or `make e2e`) runs this entire flow — postgres,
controld, dial-mode runnerd, and the real CLI through login/new/ls/attach/
suspend/resume/rm, then the environments phase (secret, setup script,
snapshot cache, second session from the cache), plus the egress acceptance —
on your own machine. It is the same wiring with `127.0.0.1` substituted for
the tailnet name, and it fails faster than a VM does. Run it before you run
this document.

It exits 0 when every check passed, and prints `FINDING:` lines for defects it
proved in the product rather than in itself; the summary counts them. A run
with findings is not a clean run — the findings are the rows marked ✗ in the
Plan 4 acceptance table below.

---

## Acceptance checklist — Plan 3 (control plane), success criteria 1–7

Copied verbatim from
[`docs/superpowers/specs/2026-08-28-plan3-controld-design.md`](superpowers/specs/2026-08-28-plan3-controld-design.md)
§1. Criterion 5 is already covered by the automated suite; the rest are what
this deployment exists to prove. Record what actually happened in the Result
column — including the date and anything surprising — rather than only
ticking the box.

| # | Criterion | How to run it | Status | Result |
|---|---|---|---|---|
| 1 | `rainier login && rainier new && rainier attach` works from Josh's laptop against a GCE e2-medium over Tailscale, from a cold VM in under an hour of ops. | Steps 1–6 above, timed from `gce-up.sh` to a live shell. | ☑ | 2026-08-29: cold VM → live shell ≈25 min including Tailscale install on the laptop; session running+reachable 3s after `new`; 167ms RTT. |
| 2 | Kill controld mid-attach; restart it; `rainier attach` reconnects to the same session with full scrollback. The agent process never noticed. | Attach, type something, `kill $(pgrep controld)` on the VM, restart it with the step-4 command, `rainier attach <id>` again. | ☑ | 2026-08-29: mid-kill viewer disconnected at seq 19 (accepted); runner auto-reconnected; `--since 0` replay carried the pre-kill scrollback; state `running`, same runner, empty error. |
| 3 | Kill runnerd on a VM with live sessions; restart it; sessions re-register and are attachable. No container is destroyed. | `docker ps` before, `pkill -x runnerd` (NOT `fleet-down.sh` — that is the teardown and removes session containers by design), re-run step 5, `docker ps` after, then attach. | ☑ | 2026-08-29: container id identical before/after; re-announce `used 1/16, 1 announced sessions`; sessiond re-registered; attach echoed live. |
| 4 | A session survives the laptop sleeping overnight; reattach next morning shows the live TUI. | Attach, start something long-running, close the laptop. Reattach in the morning. **Spans a day — start it the evening before and note the start time.** | ☑ | 2026-08-29 19:05Z, 12h after start: container never restarted (StartedAt 07:05:36Z), PID-1 sessiond and the original shell both at 12h00 elapsed, event log contiguous seq 1→756 spanning the whole day, live ticks on attach. Survived 3 runnerd restarts and a full-stack Plan 4 redeploy along the way. Finding: the CLI's `--since 0` full replay does not reach the viewer (server log intact; client forwarding bug) — fixed in Plan 5 T10: the CLI dialed with no cursor at all, and `--since 0` had no spelling on the wire that meant "the whole log". |
| 5 | Burst 10 creates against a fleet with 4 free slots: 4 run, 6 sit visibly `queued`, and the queue drains as capacity frees — no failed creates, no lost sessions. | Covered by `go test ./internal/e2e/ -run TestBurstQueuesAndDrains` (two 2-slot runners, real HTTP, real websockets). To re-run it here: `./scripts/fleet-down.sh`, bring the fleet back with `SLOTS=4` added to the step-5 environment, then `for i in $(seq 10); do rainier new --detach --name burst-$i; done` and watch `rainier ls`. | ☑ | Automated: green under `-race -count=5` (memstore and pgstore). |
| 6 | A fresh VM running runnerd with the join token appears in the fleet and receives placements with zero controld config changes or restarts. | Provision a second VM (step 1), build (step 2), run step 5 with `RUNNER_NAME=rainier-2`. Watch `rainier ls` place new sessions on it. | ☑ | 2026-08-29: throwaway e2-small joined over the VPC (`/etc/hosts` alias to rainier-1's internal IP keeps the dial-back origin check exact) with zero controld changes; placement landed on it once rainier-1 was capped full; cross-VM attach round-tripped; VM deleted after. Surfaced the first-ssh key-propagation race, now retried in `gce-up.sh`. |
| 7 | Egress R4 closed: a session reaches an allowlisted host through egressd and cannot reach anything else (verified by an acceptance script). | `./scripts/egress-check.sh` on the VM. On Linux dockerd it must exit **0** — exit 3 (SKIPPED) means the network came up non-internal and something is wrong with the platform probe. | ☑ | 2026-08-29: exit 0 — direct egress blocked, allowlisted allowed, non-allowlisted denied, both audit lines present. First live run of the enforced path; probe said `R4 egress enforcement: ON`. |

**Where each criterion can go wrong, so you know what you're looking at:**

- **1, before you even get there** — `make build` fails with `go.mod requires
  go >= 1.25.0`: your shell is finding Debian's apt Go (1.19), not
  `/usr/local/go/bin/go`. Log out and back in, or export the PATH by hand (see
  step 1). `docker: permission denied`: same cause, the `docker` group needs a
  fresh login too.
- **1** — `login` 403s: your GitHub login isn't in `--admins`. `new` sits
  `queued`: no runner joined (check `/tmp/controld.log` and `/tmp/runnerd.log`).
  `attach` says `runner_unreachable`: `--external-url` doesn't match what the
  runner dialed. A session that reaches `creating` and stays there means its
  sessiond can't reach runnerd's register listener — check the container's own
  log (`docker logs $(docker ps -q --filter label=rainier.session)`); a `405`
  there means the dial is being sent through egressd.
- **2** — Attach downtime is expected and accepted (design §4.8); state loss is
  not. After the restart the session must still be `running` on the same
  runner, with an empty `error`.
- **3** — The failure mode to watch for is a container being destroyed on
  restart, or a session marked `dead` at re-announce. Either means the
  registry rebuild (`Recover`) didn't see the container.
- **4** — Nothing on the VM should notice at all; the laptop's attach dies and
  a fresh `attach --since 0` replays the whole log.
- **7** — Read the audit lines in `/tmp/egressd.log`: an `allow` for the
  allowlisted host and a `deny` for the other, both carrying this session's id.

**Notes / follow-ups from the run:**

- 2026-08-29 (evening) Plan 4 acceptance on rainier-1, redeployed from merged
  main (PR #9; migrations 0002/0003 applied on controld start; `gce-1`
  survived the full-stack upgrade):
  - C1 ✓ `secret set` + `env create acc-env` (+placement rainier-1) +
    `new --env` → running in **3s** including the streamed setup build.
  - C2 ✓ snapshot content-addressed (`rainier-env:<id>-a8c79ba91acd`),
    cached; second create → running in **2s**, zero setup output in its full
    history, toolchain (`acc-tool`) present from the cached image.
  - C3 ✓ `/workspace` marker survived warm suspend AND cold park + resume.
  - C4 ✓ secret injected (len 21 read in-session); raw `/v1/secrets` listing
    carries names+timestamps only — no value anywhere.
  - C5 ✓ env pinned to a nonexistent runner → `queued (waiting for runner
    ghost-runner)` visible in ls and `queue_reason` in the API.
  - C6 ✓ env setup edited while acc-2 ran → its `image` stayed the original
    cached ref (resolved_image pinned at create).
  - C7 ✓ `egress-check.sh` exit 0 on the Linux fleet post-Plan-4 driver
    changes (allow+deny audit lines present).

- 2026-08-29 acceptance run (criteria 1,2,3,7 passed; 5 automated; 4 in
  progress; 6 pending a second VM):
  - `--listen 0.0.0.0:9090` failed its own `curl http://rainier-1:9090/healthz`
    because MagicDNS resolves the VM's own name to tailnet IPv6 first — fixed
    to dual-stack `:9090` in step 4 above.
  - Criterion 3's original recipe used `fleet-down.sh`, which removes session
    containers by design (it is the teardown) — recipe corrected to
    `pkill -x runnerd`.
  - Session create→running on the real VM: ~3s. Attach RTT over tailnet:
    ~167ms from the laptop.

---

## Acceptance checklist — Plan 4 (environments), success criteria 1–7

Copied verbatim from
[`docs/superpowers/specs/2026-08-29-plan4-environments-design.md`](superpowers/specs/2026-08-29-plan4-environments-design.md)
§1. Criterion 5 is covered by the automated suite; criteria 3 and 6 are too.
The rest are what the GCE run exists to prove — and what
`./scripts/e2e-fleet.sh`'s environments phase rehearses first, against real
containers on a laptop. Record what actually happened in the Result column.

| # | Criterion | How to run it | Status | Result |
|---|---|---|---|---|
| 1 | `rainier env create myapp --image node:22 --setup ./setup.sh --egress registry.npmjs.org,github.com` then `rainier new --env myapp` boots a session with the toolchain present. | Step 7 above, on the VM. | ◐ | 2026-08-29 local rehearsal: `env create` → `new --env` → attach works, and the setup script's install into `/opt/rainier-env` is present in the session AND in every session booted from the cache after it. GCE run pending. |
| 2 | The FIRST session on an environment runs setup live, streamed to the attached terminal; a snapshot is cached; the SECOND session boots from cache with no setup run — measurably faster (record both times on rainier-1). | Step 7 above: `rainier new --env web` twice, timing each, with `rainier env ls` in between. | ◐ | 2026-08-29 local rehearsal, after the fix round below: setup runs live with its output in the scrollback; the snapshot is committed content-addressed (`rainier-env:<env id>-<hash12>`); the second create dispatches no setup, boots that ref, still has the setup's `/opt/rainier-env` install, and did NOT re-run the script. Timings on this laptop are meaningless (trivial script, warm image) — **record real ones on rainier-1**, which is what the ◐ is for. |
| 3 | Session work in `/workspace` survives warm suspend AND cold park (stop + resume) — the session volume exists and persists. | Covered by the driver contract suite (`go test ./internal/driver/`), which runs the docker driver whenever a daemon is available: write → cold suspend → resume → file present; destroy → volume gone. | ☑ | Automated: green, docker and fake drivers. |
| 4 | Env-declared secrets are injected as env vars into sessions of that env, stored encrypted in Postgres, never readable via the API after write. | `go test ./internal/e2e/ -run TestSecretsReachSpec`; on the VM, an environment whose setup script echoes the secret's LENGTH. | ☑ | Injection and encryption: automated, plus the local rehearsal's setup script read the value. "Never readable": the API is write-only, and after the fix round below the cached image's config carries no value either (`docker image inspect rainier-env:…`, asserted by the rehearsal). |
| 5 | An environment with `placement: rainier-1` always places there; placement to a non-existent runner queues with a visible reason. | Covered by `go test ./internal/e2e/ -run TestPlacementPinQueuesWithReason` (a pinned session is passed over while a runner with two free slots sits there, then places the moment its own runner joins). | ☑ | Automated: green under `-race -count=5`. |
| 6 | Editing an environment never mutates a running session (sessions pin their resolved image at create). | Covered by `go test ./internal/e2e/ -run TestEnvEditInvalidatesCache` and controld's `SetSessionSetupHash` tests. | ☑ | Automated: the edit moves `setup_hash`, leaves the running session and the stale snapshot columns untouched, and only the NEXT create rebuilds. |
| 7 | Everything green in CI; acceptance run on the GCE fleet recorded in the runbook. | `go test ./... -race`, then this table filled in from the VM. | ◐ | CI green 2026-08-29. Local rehearsal recorded here; the GCE run is what remains. |

**Notes / follow-ups from the 2026-08-29 environments rehearsal**
(`RUNNERD_PORT=8081 PG_PORT=5434 ./scripts/e2e-fleet.sh`)

The first rehearsal exited 0 on the flow and recorded three findings — the
environment plumbing (API, CLI, resolution, placement, secrets-at-rest,
invalidation, events) was doing exactly what it should, and everything the
cache is *for* was broken. All three were one root cause, what `docker commit`
captures, and all three are fixed; the rehearsal now asserts each of them
rather than reporting it.

- **The snapshot cache was empty by construction.** Every session ran with
  `--read-only`, so a setup script could write only `/workspace` (a volume
  `docker commit` excludes) and `/tmp` — `mkdir /usr/local/...` failed with
  `Read-only file system`, and the committed image was the base image under a
  new tag. Design §4.4 ("root stays read-only") and §4.3 (setup provisions a
  cacheable image) contradicted each other, and §4.4 won. **Fixed:** a create
  carrying a setup script drops that one flag; cache-booted and scratch
  sessions keep it. The session image also needed a uid-1000 user and an
  install prefix it owns — a writable rootfs it has no permission to write is
  no better than a read-only one. That prefix is `/opt/rainier-env`, not
  `/usr/local`: sessiond is the session's PID 1, and a setup script that could
  replace it would have that baked into the cached image (design §10).
- **A cache-booted session re-ran its setup script.** `docker commit`
  snapshots the container's *config*, environment block included, so
  `RAINIER_SETUP_B64` rode along inside `rainier-env:…` and sessiond ran the
  script even though controld correctly dispatched none. **Fixed:** the
  snapshot names the setup channel for stripping, always.
- **The cached image carried the environment's decrypted secrets.** Same
  cause: every `-e` the driver passed, each resolved `secret_ref` value
  included, was in the committed config for anyone with a docker socket to
  read — and a registry-backed distribution (design §6) would have published
  them. **Fixed:** runnerd records each create's env KEYS (never values) and
  names them for stripping at snapshot time, alongside the setup channel.
- Unrelated repair found by the same run: `e2e-fleet.sh` read `rainier ls`
  columns by awk field number, and the ENV column that sessions grew in Plan 4
  shifted every field after it — `session_state` returned `-` forever, so the
  script's very first `rainier new` would have timed out. Columns are now read
  by their header's character offset (`cell`).

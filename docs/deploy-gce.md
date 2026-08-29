# Deploying Rainier on GCE (the v0 dogfood target)

One `e2-medium` in GCP project `rainier-cloud`, reached over Tailscale. No load
balancer, no domain, no certificates, no systemd units — v0 is honest about
being one box you can rebuild in twenty minutes. Everything here is the
Plan 3 design's §4.10 deployment story, written out as commands.

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
(`RAINIER_LISTEN`, `RAINIER_DB`, `RAINIER_RUNNER_TOKEN`, `RAINIER_ADMINS`,
`RAINIER_MEMBERS`, `RAINIER_EXTERNAL_URL`, `RAINIER_GITHUB_API`), so the
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

## 7. Rehearse locally first

`./scripts/e2e-fleet.sh` (or `make e2e`) runs this entire flow — postgres,
controld, dial-mode runnerd, and the real CLI through login/new/ls/attach/
suspend/resume/rm plus the egress acceptance — on your own machine. It is the
same wiring with `127.0.0.1` substituted for the tailnet name, and it fails
faster than a VM does. Run it before you run this document.

---

## Acceptance checklist — the design's success criteria 1–7

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
| 4 | A session survives the laptop sleeping overnight; reattach next morning shows the live TUI. | Attach, start something long-running, close the laptop. Reattach in the morning. **Spans a day — start it the evening before and note the start time.** | ◐ | Started 2026-08-29 07:10Z: minute-ticker loop live in `gce-1`; verify next morning. |
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
  a fresh one replays scrollback from `since=0`.
- **7** — Read the audit lines in `/tmp/egressd.log`: an `allow` for the
  allowlisted host and a `deny` for the other, both carrying this session's id.

**Notes / follow-ups from the run:**

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

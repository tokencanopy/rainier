# The session bootstrap token and vsock: configuring a microVM without a secret on the host

Status: proposed. No code in this change.
Implements rainier-cloud `docs/architecture/adr-0003-serverless-microvm-architecture.md`
§2.7 items 1 and 2, which §9 of that ADR names as needing its own design note here.
Contrast: the driver spike, [#95](https://github.com/tokencanopy/rainier/pull/95)
(branch `feat/microvm-driver-spike`), whose config delivery this replaces.

The two decisions this note implements, quoted:

> 1. **Credentials never enter `Spec.Env` for microVM sessions.** `runnerd` hands the
> guest only a short-lived, session-scoped bootstrap token. `sessiond` uses it to
> request credentials from `cell-gateway` at boot and again after every resume, which
> is what §2.2 and §4.3 already require. Nothing secret is written on the host, and the
> OSS `Resume(id)` signature, which carries no spec, stays as it is because a cold
> resume needs nothing from the host except the token. The Docker driver keeps its
> existing env-injection behavior; this rule is specific to the microVM driver.
>
> 2. **virtio-vsock is the single host-to-guest control channel.** It carries the boot
> configuration (session id, proxy address, egress allowlist, command, bootstrap
> token), the `sessiond` to `runnerd` connection that today rides a WebSocket through
> the TAP device, host-initiated lifecycle calls (flush before cold suspend, unmount
> the agent home, readiness), and the credential fetch. It works before the guest
> network is up, is invisible to guest routing and to the TAP firewall, and leaves the
> virtual NIC for egress through `egressd` only. Firecracker's MMDS is not used: it is
> readable by any guest process, its version 2 token exchange is not implemented in
> `sessiond`, and it answers at the metadata address the host is required to deny.

Item 1 is already truer than it reads, and this design is smaller because of it.

## 1. Problem

### What `Spec.Env` actually carries

`runner.Spec.Env` (`protocol/runner/messages.go:315`) says of itself: *"Values are
secrets as often as not, so this field is never logged verbatim."* It is filled in one
place — `controlapp.FleetService.createSpec` (`controlapp/scheduler.go:372`), line 405,
`spec.Env = cloneMap(material.Environment)` — from `launchMaterial.ResolveLaunchMaterial`
(`internal/controld/adapt_launch.go:38`), whose `secretEnvironment` (line 79) decrypts
each name in `control.Environment.SecretRefs` with `Open(l.key, …)`. `createSpec` then
merges that under `controlapp.AgentsEnv` (`controlapp/agents.go:156`).

So the secret set in `Spec.Env` is **an environment's decrypted `secret_refs`, and
nothing else.** Everything else is a typed field on `runner.Spec` that becomes an
environment variable only in `(*Docker).runArgs` (`internal/driver/docker.go:241`), and
each is plain configuration: `RAINIER_SETUP_B64` and `RAINIER_INIT_B64` with their
timeouts (`:353`, `:375`), `RAINIER_REPOS_B64` (`:367`, owner/name/branch/dir),
`RAINIER_GIT_AUTHOR_NAME` and `_EMAIL` (`:380`, whose `Spec` comment reads *"never a
credential: the token stays in the vault and reaches git through the in-sandbox
helper"*), `RAINIER_DIAL` and `RAINIER_SESSION` (`createWithID`,
`internal/runnerd/runnerd.go:384-385`), the four proxy variables and `NO_PROXY` (`:338`,
`:445`), and `RAINIER_AGENTS_B64` (paths and file allowlists). One caveat:
`RAINIER_SESSION` is identity, not configuration — `withSessionUserinfo` (`:484`) embeds
it in the proxy URL as egressd's authenticated identity, and `runnerd.go:579` calls a
leaked session id "a credential-shaped value".

And the credentials the ADR worries about most are **already not in `Spec.Env`**. They
are pulled upward over the session RPC: `mint_git_credential` (`internal/controld/srpc.go:81`,
answered by `answerMintGitCredential`, line 107) hands out one GitHub token per git
operation; `fetch_agent_credentials` / `put_agent_credentials`
(`protocol/runner/messages.go:42,46`, answered at `srpc.go:218` and `:254`) carry a
person's coding-agent login set, fetched at boot by `(*agentSync).fetchOne`
(`cmd/sessiond/agents.go:538`). That path is authorized by placement:
`authorizeSessionRequest` (`srpc.go:36`) refuses any request whose session the store does
not place on the asking runner. This design therefore invents no upward credential
channel; it reuses that one and adds a method to it.

### Why Docker's behaviour is acceptable and the spike's is not

`runArgs` appends `-e K=V` to a `docker run` argv; `dockerRun` is `execDocker`
(`internal/driver/docker_exec.go:31`), a plain `exec.CommandContext`. There is no
`--env-file` and nothing reaches the host filesystem: the value's lifetime is the CLI
process plus the container config, and `(*Server).stripEnvFor` (`runnerd.go:613`) names
both those keys and `driverEnvKeys` (line 592) on every snapshot so none of it bakes into
a cached image. On a Dedicated host — one workspace's own machine — that is a trusted
process handing a value to a trusted daemon.

A Serverless microVM host is a shared regional pool (ADR §3.1, §4.5) whose blast radius
is that ADR's named primary risk, and the spike shows what `Spec.Env` becomes when a
driver cannot use argv. `buildGuestEnv` copies `spec.Env` in wholesale;
`stageGuestSessionConfig` writes it to `<StateDir>/instances/<id>/session.json` at mode
`0600` inside a `0755` directory (under `/tmp/rainier-microvm` when `StateDir` is unset);
`saveInstanceRecord` writes the same map to `instance.json` at mode **`0644`**. Plaintext
workspace secrets, at rest, on a shared host, world-readable in one of the two. That is
the failure §2.7 was written against.

### Why MMDS is rejected

**Any guest process can read it:** MMDS answers unauthenticated HTTP at
`169.254.169.254`, and the spike PUTs `{"session_id", "dial_url", "env": cfg.Env, "cmd"}`
into `/mmds`, so every workspace secret is one `curl` away from anything the agent spawns.
**V2 is not implemented:** the spike's `PUT /mmds/config` sends only
`{"ipv4_address": "169.254.169.254"}` with no `version` field, and Firecracker's
documented default for a missing `version` is V1, which upstream marks deprecated;
matching that, `loadGuestConfig` in `cmd/sessiond/main.go` on that branch does a bare
`GET http://169.254.169.254/mmds` with no `X-metadata-token` and no
`PUT /latest/api/token`. **It answers at an address the host must deny:** tenancy §10.2
requires that workloads *"cannot reach cloud metadata"*, §16 lists metadata denial under
"Serverless sandbox escape", and ADR §4.3 has host `nftables` drop `169.254.169.254` on
every TAP interface — a config channel that needs the one address the firewall exists to
block is at war with its own rule.

Two further spike facts shaped the alternative: both MMDS PUTs discard their error
(`_ = fcClient.putJSON(...)`) and `/mmds/config` omits `network_interfaces`, so a
rejected setup yields a silently config-less guest; and the file half of
`loadGuestConfig`, `/workspace/.rainier/session.json`, is a dead channel — nothing on
that branch ever places that file inside the guest.

## 2. Goals and non-goals

**Goals.** One host-to-guest channel, available before the guest network is; no secret
written to host disk or held in a host process longer than the hop it is forwarded on;
the existing upward session RPC reused rather than duplicated; every wire change
additive; the Docker path byte-identical.

**Non-goals.** Any change to Docker driver behaviour. The jailer, network slots and
`nftables` rules (ADR §4.5, §4.3 — separate PRs in this sequence). Environment image
build and reflink copy (§2.7 item 3). Workspace disks and the portable checkpoint (§2.3).
Warm-pool claiming. Anything in `rainier-cloud` beyond the pointer in §5.

## 3. The bootstrap token

| | |
|---|---|
| **What it is** | 32 bytes from `crypto/rand`, base64url, opaque to every hop that carries it. Not a JWT and carrying no claims: the control plane holds the state. |
| **Who mints it** | `createSpec` — the one function that builds a `runner.Spec` — when the placement's runner announced `microvm.v1`. The plane stores `sha256(token)` against the session row with the placement generation and an expiry, and puts the plaintext on the wire once. `createSpec` is also where `spec.Env` is set, so it is the natural place for "either the secrets or the token, never both". |
| **What it buys** | Exactly `material.Environment`, and nothing else. Not a GitHub token and not the agent credential set: those already have their own upward methods, unchanged here, which is why the scope is this narrow. |
| **What it cannot do** | Name another session — the answer is derived from the row the placement guard read, never from the request. Mint a git credential. Put or revoke agent credentials. Open an attachment or an exec. Be used twice. |
| **Lifetime and fencing** | Single-use, 120 seconds from dispatch. Three independent refusals, all in the control plane: the hash does not match; the row's `PlacementGeneration` has moved past the one it was minted under; it is already consumed. A cold resume needs a fresh one, which is what keeps `Driver.Resume(ctx, id string) (restarted bool, err error)` free of a spec — runnerd asks for it upward rather than being handed it downward. |

**What runnerd sees.** The token, transiently, in memory: it arrives on `Spec`, goes to
the driver, is written into the guest's boot configuration over the vsock socket, and is
dropped. Never a file, never the registry entry — `createWithID` (`runnerd.go:370`)
already stores env **keys only** (`envKeys: envKeys(spec.Env)`), from the same instinct.
The **secrets** never reach runnerd at all: the exchange is an `RPCEnvelope` whose
`Payload` runnerd is documented as forwarding without parsing
(`protocol/runner/messages.go:99`; `routeControl` and `sendSessionRPC`, `runnerd.go:1076`
and `:1334`). That is the honest boundary — runnerd boots the VM and owns its vsock
socket, so a compromised runnerd has other doors; what this removes is the secret at rest
and the secret in a process that outlives the exchange.

### Message shapes

Additive fields on existing types. No new protocol file.

```go
// protocol/runner/messages.go — two fields on Spec.
type Spec struct {
	// ... existing fields unchanged ...

	// BootstrapToken is the single-use capability a microVM session exchanges
	// for its environment's decrypted secrets, which for such a session are
	// deliberately absent from Env. Minted per create and per cold resume,
	// fenced by the placement generation, never written to host disk. Absent
	// on every Docker create and from every older control plane, which is why
	// Env keeps its meaning.
	BootstrapToken string `json:"bootstrap_token,omitempty"`

	// SecretNames are the NAMES the token will deliver — a name is not a value,
	// as adapt_launch.go already says — so the guest can tell "no secrets
	// declared" from "declared and never arrived".
	SecretNames []string `json:"secret_names,omitempty"`
}

const (
	// Sandbox → control plane, once per boot:
	// {"protocol": 1, "token": "<opaque>"} → {"env": {"NAME": "value"}}.
	// Refused as the usual {"error": sentence} on ok:false when the token is
	// spent, expired, or fenced by a placement generation that has moved.
	MethodFetchSessionSecrets = "fetch_session_secrets"
	// Runner → control plane, on a cold resume:
	// {"protocol": 1} → {"token": "<opaque>", "expires_in_sec": 120}. No
	// session id: it is FromRunner.Session, which the guard has checked.
	MethodMintSessionBootstrap = "mint_session_bootstrap"
)

// Announced by a runnerd built with the microVM driver. The plane withholds
// Spec.Env's secret values only from a runner that announced it.
const CapabilityMicrovmV1 = "microvm.v1"
```

`RPCEnvelope`, `ToRunner` and `FromRunner` need **no change at all**. Both methods ride
shapes that exist: up as `FromRunner{Type: "session_req", Session: id, RPC: &env}`
(`internal/runnerd/agent.go:326`), down as `ToRunner{Type: "session_rpc", RPC: &ans}`
(`runnerplane.(*Plane).answerSessionRequest`, `runnerplane/conn.go:379`), through
`relay.ControlEvent{Kind: "req:<method>", ID: n}` on `FrameControl`
(`internal/relay/frame.go:27`). `mint_session_bootstrap` is the one new thing in the shape
of the traffic: it is originated by **runnerd**, not a sandbox, on a session it holds.
`authorizeSessionRequest` already answers that question — *"the runner token is
fleet-wide, so a `session_req` proves only that SOME runner sent it"* — so the guard is
unchanged; only the switch in `handleSessionRequest` (`srpc.go:63`) grows.

### Compatibility

**controld rolls before runners**, the order the fleet already uses.

| Pairing | Behaviour |
|---|---|
| new controld + old runner | No `microvm.v1` announced, so `createSpec` mints nothing, withholds nothing, dispatches today's `Spec` exactly. |
| old controld + new runner | The token is absent and `Spec.Env` carries the secrets. The microVM driver refuses to boot a session with a non-empty `Spec.Env` and an empty token, naming the control-plane version — a refusal, not a quiet write to `session.json`. |
| new sandbox + old controld | `handleSessionRequest`'s `default` arm answers `rpcRefusal(env.ID, "unknown method …")`, which its own comment names as *"what a newer sandbox talking to an older controld gets: a clear answer rather than a hang"*. The guest then applies the row below. |
| new sandbox, no token | On the Docker path, every session: the guest reads its environment block as today and never asks. On the microVM path, an empty `SecretNames` is a clean boot; a non-empty `SecretNames` with no token, or a refused exchange, fails the boot chain as `stage_failed` (the existing kind) naming the count of undelivered names and none of their values — closed on the secret, loud on the session, the bargain `secretEnvironment` already makes. |

## 4. vsock as the control channel

### The device

One additive call in `Create`, before `InstanceStart`, on the Firecracker API socket.
Facts below are from `firecracker-microvm/firecracker` `docs/vsock.md` at `main`; the
runbook pins **v1.17.0**, which is also the current latest release.

```
PUT /vsock
{ "guest_cid": 3, "uds_path": "<jail>/run/v.sock" }
```

Guest CID is 3 (the doc's value); the host is `HOST_CID`, integer 2. A guest-initiated
connection to host port *N* is forwarded to an `AF_UNIX` socket at `<uds_path>_N` —
*"a guest connection to port 52 will get forwarded to `./v.sock_52`"*. A host-initiated
connection goes the other way: connect to `<uds_path>`, send `CONNECT <port>\n`, and
Firecracker answers `OK <assigned_hostside_port>\n` if the guest is listening. Guest
kernels need `CONFIG_VIRTIO_VSOCKETS=y` and `/dev/vsock`; the host needs
`CONFIG_VHOST_VSOCK=m`. The doc warns that one `uds_path` cannot be multiplexed across
VMs, which is why the path is per-session inside the jail and a resume gets a fresh one.

### The port plan: one port

`<uds_path>_1024`, and nothing else. The host never sends `CONNECT`.

The temptation is three ports — config, stream, lifecycle — and the reason not to is that
`internal/relay` already multiplexes all three over one socket: *"Package relay
multiplexes many client attachments over the single outbound WebSocket a session opens to
runnerd"* (`internal/relay/frame.go:1`). `FrameControl` with `AttachID 0` carries events,
requests (`Kind: "req:<method>"`) and responses in **both** directions —
`Hub.SendControl` down, `ControlSender.Send` up. Host-initiated lifecycle calls are
therefore frames on the connection the guest opened, which is exactly what the suspend
handshake already is: `KindSuspending` down, `KindSuspendAck` and `KindSuspendReady` up,
all nonced in `ControlEvent.ID`. The boot configuration rides that conn as the first
`FrameControl` runnerd sends after accepting, so nothing has to predate the stream.

One property falls out of the socket path: **the session id stops being something the
guest asserts.** Today `sessiond` dials `dial + "?session=" + sessionID`
(`cmd/sessiond/main.go`, `dialLoop`) and `(*Server).register` (`runnerd.go:922`) believes
the query parameter, with no authentication on that hop at all. With vsock the listening
socket is inside one VM's jail directory, so the connection's identity is its path.

### Boot, step by step

1. `createSpec` sees `microvm.v1`, mints the token, sets `BootstrapToken` and
   `SecretNames`, and leaves the secret values out of `Spec.Env`. The agent-home path
   vars and `RAINIER_AGENTS_B64` stay — they are configuration.
2. `createWithID` claims the id, pushes egress, sets `SessionID` and `ProxyURL` as today.
   `DialURL` is left **empty**: there is no URL to dial, so `noProxyFor` has no dial host
   to exempt and `NO_PROXY` is just `noProxyBase`.
3. `MicrovmDriver.Create` reflink-copies the rootfs, allocates the slot, `PUT /vsock`,
   listens on `<uds_path>_1024`, `PUT /actions {"action_type":"InstanceStart"}`.
4. The guest boots. `sessiond` opens `AF_VSOCK` to (2, 1024).
5. Firecracker forwards it to `<uds_path>_1024`; runnerd accepts and builds the relay hub
   with `relay.NewHubWithControl`, as `register` does today, keyed by the session the
   socket path names.
6. runnerd's first frame is `FrameControl{Kind: "boot_config"}`: session id, command,
   proxy URL, egress allowlist, agent manifest, git author, repos, setup and init scripts
   with their bounds, `SecretNames`, and the token.
7. `sessiond` sends `req:fetch_session_secrets` with the token. runnerd forwards it up
   without parsing the payload; the plane answers `{"env": {…}}` back down.
8. `sessiond` applies the variables **in its own process**, composes the child's
   environment as it does today, runs the boot chain, execs the agent. Registered.

### Cold suspend, and resume

Cold suspend reuses the handshake that exists, with one additive field: `ControlEvent`
gains ``Cold bool `json:"cold,omitempty"` `` on `KindSuspending`, meaning "this is not a
freeze — flush, unmount `/rainier/agents`, forget every delivered secret". `sessiond`
answers `KindSuspendAck` at once and `KindSuspendReady` when done, under the budgets that
already exist (2s and 12s). runnerd then terminates the microVM — no memory image, per
ADR §2.2 — detaches the disk, drops the token, and produces the checkpoint behind the
§4.4 barrier. A sessiond predating `Cold` reads a plain `suspending`, quiesces its execs,
and the host-side unmount still happens; the guest simply did not help.

Resume is the boot sequence again with a new VM, a new `uds_path`, and a new token that
runnerd fetches with `req:mint_session_bootstrap` before step 6. `Resume` returns
`restarted=true` so runnerd's idle logic treats the agent as a new process. Throughout,
**the guest NIC is used only for egress through the proxy** — no register dial, no
credential fetch, no lifecycle call traverses it, so the TAP firewall has no
Rainier-shaped hole to keep open.

## 5. Changes by component

| Component | Change |
|---|---|
| `protocol/runner` | Two `omitempty` fields on `Spec`, two method constants, `CapabilityMicrovmV1`. Nothing on `ToRunner`, `FromRunner`, `RPCEnvelope`. |
| `internal/relay` | One `omitempty` bool on `ControlEvent` (`Cold`) and a `boot_config` kind. `Frame` untouched, so `TestTerminalFrameWireShape` and `TestControlEventWireShape` keep their meaning. |
| `controlapp` | `createSpec` mints, sets `SecretNames`, withholds `material.Environment` when the runner announced `microvm.v1`. A store seam for `(session, token hash, placement generation, expiry, consumed)`. |
| `internal/controld` | Two arms on `handleSessionRequest`'s switch and their answer functions, to the hygiene the neighbours keep: the value appears only in the payload, never in a log line, an error, or a refusal. `authorizeSessionRequest` unchanged. |
| `internal/runnerd` | Relay the exchange (`routeControl` and `sendSessionRPC` are already generic). Originate `mint_session_bootstrap` on a cold resume. Strip `Spec.Env`'s secret values **for the microvm driver only**, refusing when values are present and a token is not. Serve the vsock listener where `register` serves the WebSocket. |
| `internal/driver/microvm.go` | `PUT /vsock`; the boot config over it; **no `session.json`, no env in `instance.json`, no MMDS.** `buildGuestEnv` and `stageGuestSessionConfig` as the spike writes them do not survive. |
| `cmd/sessiond` | A transport seam behind `dialLoop`: Docker keeps `websocket.Dial(dial+"?session="+id)`, microVM dials `AF_VSOCK` to (2, 1024). Everything above it — `relay.ServeSessionWithExec`, `rpcDispatcher`, `agentSync` — is unchanged, because it already takes a `relay.Conn`. Config from `boot_config`; the exchange at boot and after resume. |
| rainier-cloud `cell-gateway` | Pointer only. It answers the two new methods with the same placement-generation fence its `SessionRequest` already applies to the three existing ones, and mints against tenancy §8.2's tuple. Note: the ADR's *"request credentials from `cell-gateway`"* cannot mean a guest-originated HTTP call — the guest has no route to the cell, by design — so the exchange rides the runner plane, as every other credential already does. |

## 6. Security review against the tenancy specification

Against `rainier-cloud docs/security/hosted-tenancy-and-security.md`:

- **§16 "Secret enters workspace or artifact"** — the secret touches no host disk, rootfs,
  workspace disk or checkpoint; it lives in guest RAM, never serialized (ADR §2.2).
- **§16 "Credential confused deputy"** — answered from the row `authorizeSessionRequest`
  read, never a request value, and bound to one session and one placement generation; a
  runner holding session A cannot exchange for B, because the id is `FromRunner.Session`.
- **§16 "Serverless sandbox escape" / §10.2** — the config channel no longer needs
  `169.254.169.254`, so metadata denial is unconditional, and vsock is invisible to guest
  routing, so no `nftables` exception exists for Rainier's own traffic.
- **§16 "DNS rebinding or SSRF reaches metadata"** — an SSRF in the guest reaches a
  metadata service that holds nothing.
- **§8.2** — delivery still requires the full tuple; this adds no delivery path, it
  narrows one, and the agent home stays its own encrypted per-user-per-workspace store.
- **§11** — the token carries no context and is not encrypted material, so it cannot be
  copied into another context and opened; only its hash is stored.
- **§18 item 34** — no credential in environment variables, images, checkpoints or
  exports; for a microVM session that becomes literally true of the environment block.
- **§18 item 38 (snapshot exclusion)** — `Snapshot` copies a rootfs that never held a
  secret, and workspace and agent home are separate devices excluded by construction (ADR
  §4.1). The exclusion test must still be written here; Docker gets it from `docker
  commit` skipping volumes.
- **§18 item 53** — the session id is proved by the vsock socket path rather than asserted
  in a query parameter, which is stronger than the current `register` hop.
- **§15.1** — no new field enters a log: not the token, not the secret map; a refusal names
  a count and never a value.

## 7. Test plan

- **Minting and single-use**, in `controlapp`: a table over (fresh, replayed, expired,
  superseded placement generation, another session's) asserting one success and four
  refusals with distinct reasons; and that `createSpec` sets secret values *or* a token,
  never both, over a matrix of announced capabilities.
- **The exchange**, in `internal/controld`: the two methods answered, an unknown one
  refused by name, and a test that greps its own log output for the fixture secret.
- **A fake vsock transport** for `cmd/sessiond`: the seam takes a
  `func(context.Context) (relay.Conn, error)`, so the microVM path is tested over an
  `net.Pipe`-backed fake with no VM and no KVM — boot config applied, exchange sent once,
  a refusal failing the boot chain as `stage_failed`, a resume re-exchanging.
- **`RunContract` implications** (`internal/driver/contract.go:92`): subtest 5, *"snapshot
  strips the named environment keys"*, creates with
  `Env: {"CONTRACT_SECRET": "must-not-survive"}` and calls `assertStrippedFromImage`. A
  driver that never accepts secret values in `Spec.Env` passes it vacuously, which is the
  wrong kind of green. The contract needs a twelfth subtest — *"a create carrying secrets
  and no token is refused"* — and `assertStrippedFromImage` needs a microVM arm reading the
  published ext4's configuration, so item 38's proof is a test and not a claim.
- **What only a real host can test:** that Firecracker forwards `<uds_path>_1024`; that the
  guest kernel has `/dev/vsock`; that `PUT /vsock` is accepted in the version and jailer
  configuration we ship; that the TAP firewall drops `169.254.169.254` while the session
  still boots; and the cold-suspend unmount. Phase 1's host harness, not `go test`.

## 8. Rollout and backward compatibility

Four steps, ordered by who must be able to refuse before anybody asks. Each is separately
revertable and none requires the one after it.

1. **`protocol/runner` and `internal/relay`** — the additive fields and method words.
   Nothing sets them; every existing message is byte-identical.
2. **The control plane** — `controlapp` mints and answers, `internal/controld` grows the
   two arms. Nothing asks yet, because no runner announces `microvm.v1`. This is the step
   that makes "controld rolls before runners" true.
3. **`runnerd` and the driver** — the capability, the strip, the vsock listener. The first
   runner to announce `microvm.v1` is the first session whose secrets are withheld, and by
   then the plane can answer.
4. **`sessiond`, in the session image.** A session keeps the `sessiond` it booted with for
   life, so this is last for the microVM path and irrelevant for the Docker path, which is
   why that path is the compatibility floor and stays as it is.

## 9. Open questions

1. **Is 120 seconds right?** It must cover a base-microVM claim, a guest boot and a round
   trip. Phase 1 measures it; until then it is a constant in one place.
2. **Should the token be spendable more than once within its TTL?** Single-use is safer and
   costs a `mint_session_bootstrap` round trip on every cold resume. If a sessiond
   crash-and-restart inside a live VM proves real, the answer is a second exchange within
   the TTL rather than a longer-lived token.
3. **Does the plane need the boot epoch?** runnerd has one (`registry.currentBoot`,
   `runnerd.go:938`) and the plane does not. Today the mint is the fence; if the two ever
   disagree, the epoch is the thing to put on the wire.
4. **Where does `boot_config`'s schema live?** Probably `protocol/runner` beside `Spec`
   rather than `internal/relay`, but it is not a runner-plane message, so this is unclear.
5. **Is withholding keyed on the right thing?** `microvm.v1` means a runner withholds for
   every session on it, including a Docker one if such a hybrid ever exists. Per-session
   keying is more precise and needs the plane to know the driver, which it does not.
6. **The ADR says "from `cell-gateway`".** This note routes the exchange over the runner
   plane, because a guest has no route to the cell. A direct guest-to-gateway call needs an
   egress hole and should be argued on its own rather than assumed from a phrase.

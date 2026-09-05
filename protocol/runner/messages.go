// Package runner defines the JSON messages exchanged between runnerd and
// controld over runnerd's single outbound control WebSocket. One struct per
// direction, same idiom as protocol/terminal. ProtocolVersion gates major
// changes: controld rejects an announce whose ProtocolVersion it doesn't
// speak, with a close reason naming both versions (design §4.3).
package runner

import "encoding/json"

// ProtocolVersion is the wire version a runner announces and controld checks
// before it accepts any command from that connection. It is 1: capability
// negotiation rides on additive fields of this version — a runner announces
// its capabilities and controld answers with an accept naming the generation
// and the capabilities it took — so a runner that sends none is judged
// exactly as before, and a rolling-version window is still deferred to the
// compatibility ADR. An announce whose value controld does not speak is
// fatal to the connection: controld closes it with a close reason naming
// both the announced and the expected version.
const ProtocolVersion = 1

// The three session-RPC methods that keep a coding agent's credential set
// equal to the control plane's sealed copy. They are wire words: a sandbox
// registers handlers under exactly these names and a control plane answers
// them, so they are declared once here rather than spelled at each end.
//
// None of them names a provider in its shape — a provider is a string the
// control plane's table defines and both ends merely carry — and none of them
// puts a credential anywhere a forwarder reads: files travel base64 inside the
// opaque payload, and a refusal is the usual {"error": sentence} on ok:false.
const (
	// MethodFetchAgentCredentials is sandbox → control plane, at boot:
	// {"provider": "..."} → {"version": n, "files": {name: base64}}. Version
	// 0 with no files is the truthful answer for a person who has not logged
	// that agent in; it is an answer, not a refusal, and the agent starts
	// anyway and asks them to log in.
	MethodFetchAgentCredentials = "fetch_agent_credentials"
	// MethodPutAgentCredentials is sandbox → control plane, whenever the
	// allowlisted files change: {"provider": "...", "files": {name: base64},
	// "version": n} → {"version": n+1}. The version the sandbox sends is the
	// one it last saw, so custody can tell a fresh login from a replay of a
	// set that has since been revoked.
	MethodPutAgentCredentials = "put_agent_credentials"
	// MethodRevokeAgentCredentials is control plane → sandbox, on a logout or
	// a membership that went away: {"provider": "..."} → {}. The sandbox
	// removes that provider's allowlisted files and forgets its baseline, so
	// a later login inside the same session is a new set rather than a re-put
	// of the revoked one.
	MethodRevokeAgentCredentials = "revoke_agent_credentials"
)

// HomeMount is the agent home a create mounts into a sandbox: one writable
// volume per (creator, workspace), landing at Path, inside which each coding
// agent gets its own subdirectory. It is what makes "log in once" true across
// sessions — a credential set lives in the volume, never in the image, the
// workspace, a checkpoint, or the environment — and it is the only writable
// place a session has outside its workspace.
//
// Volume is opaque on purpose. A volume name is visible to anyone with a
// shell on the runner, and an account identifier is not something to print
// there, so the control plane hands down a hash (controlapp.AgentHomeVolume)
// and the runner treats it as a name to mount, never as something to parse.
type HomeMount struct {
	Volume string `json:"volume"`
	Path   string `json:"path"`
}

// RPCEnvelope is one message of the session RPC — the bidirectional
// request/response channel that reaches all the way into a sandbox. It rides
// a ToRunner "session_rpc" going down and a FromRunner "session_req" coming
// up, and runnerd is a pure forwarder of it: it matches the envelope to a
// session, hands it to (or takes it from) that session's relay control
// channel, and never looks inside Payload.
//
// ID correlates a request with its one response and is assigned by whichever
// end originated the request, so the two directions have independent id
// spaces. Method names the operation on a request ("mint_git_credential",
// "diff", "push_files", "pull_files") and is the literal "resp" on a
// response, whose ID echoes the request being answered.
type RPCEnvelope struct {
	ID     uint64 `json:"id"`
	Method string `json:"method"`
	// OK is a response's verdict, meaningful only when Method is "resp". It
	// mirrors relay.ControlEvent.OK field for field, and that is the whole
	// reason it exists here: runnerd rebuilds one message from the other at
	// each hop, so a verdict this envelope could not carry would have to be
	// dug out of Payload — exactly the parse the forwarder is defined not to
	// do. False is the zero value and therefore absent from the wire, which is
	// the safe direction: a peer that fails to decode it reads a failure,
	// never a spurious success. The failure's detail lives in Payload, by
	// convention as {"error": "..."}.
	OK bool `json:"ok,omitempty"`
	// Payload is the method-specific body, opaque to runnerd. RawMessage so
	// it forwards without being parsed and re-encoded, and so it lands as
	// nested JSON rather than a base64 string.
	Payload json.RawMessage `json:"payload,omitempty"`
}

// FromRunner: runnerd → controld. Used/Total (capacity) piggyback on every
// message type so controld's runner view is always current without a separate
// capacity message.
//
// The event States split in three: "running" | "dead" report the container's
// lifecycle; "setup_done" | "setup_failed" report the outcome of an
// environment's setup script inside an already-running container (Plan 4
// design §4.3, the setup pipeline) — a setup_failed carries the tail of the
// script's output in Detail, the same field a result uses for its error text;
// and "child_exited" reports that the AGENT process inside a running session
// ended, carrying its exit status in Detail as a bare decimal string ("0" for
// a clean exit). That last one moves no state machine: the container stays up
// for viewers, so it is an observation controld records against the session.
type FromRunner struct {
	Type     string        `json:"type"`               // "announce" | "result" | "event" | "session_req"
	Proto    int           `json:"proto,omitempty"`    // announce
	Runner   string        `json:"runner,omitempty"`   // announce
	Sessions []SessionInfo `json:"sessions,omitempty"` // announce
	Used     int           `json:"used"`
	Total    int           `json:"total"`
	ReqID    uint64        `json:"req_id,omitempty"`  // result: correlates ToRunner.ReqID
	OK       bool          `json:"ok,omitempty"`      // result
	Detail   string        `json:"detail,omitempty"`  // result: error text or snapshot ref; event: setup_failed tail
	Session  string        `json:"session,omitempty"` // event, session_req
	State    string        `json:"state,omitempty"`   // event: "running" | "dead" | "setup_done" | "setup_failed" | "child_exited"
	// RPC carries a session-RPC message the sandbox originated ("session_req")
	// — a credential mint, say — which controld answers with a "session_rpc"
	// back down. Session names which sandbox it came from; without it a
	// response has nowhere to be routed.
	RPC *RPCEnvelope `json:"rpc,omitempty"`
	// Capabilities are the portable runtime capabilities this runner claims
	// on an announce: lowercase tokens such as "gpu" or "docker.rootless".
	// Absent means none — an old runner is a runner with no capabilities,
	// and every environment that requires one simply never lands on it.
	Capabilities []string `json:"capabilities,omitempty"` // announce
	// Generation is the runner generation controld granted in its accept,
	// echoed on later events and results so a report from a superseded
	// connection can be fenced by the store rather than by the socket it
	// arrived on. Zero means "the connection's" (an old runner).
	Generation uint64 `json:"generation,omitempty"` // event, result
	// PlacementGeneration echoes, on an event about a session, the value the
	// create that started its sandbox carried. Zero for an old runner or a
	// session created before the runner learned it.
	PlacementGeneration uint64 `json:"placement_generation,omitempty"` // event
}

// SessionInfo is one session's line in a FromRunner "announce": the stable
// session ID every later command addresses, plus the runner's current
// lifecycle state for it. It carries no payload and no history — the point
// of an announce is to reconcile controld's view of the runner after a
// (re)connect, so the list is complete in one message and a session still
// "starting" is omitted rather than given a provisional state. State is one
// of "running", "suspended_warm", or "suspended_cold".
type SessionInfo struct {
	ID    string `json:"id"`
	State string `json:"state"` // "running"|"suspended_warm"|"suspended_cold"
}

// ToRunner: controld → runnerd.
type ToRunner struct {
	// "destroy" is the whole teardown (container + workspace);
	// "remove_workspace" takes only the volume, for a session whose container
	// the crash path already removed and whose workspace it deliberately kept.
	//
	// "accept" is controld's answer to an announce, sent before any command:
	// the generation this connection acts under and the announced
	// capabilities controld will schedule on.
	Type    string  `json:"type"` // "accept"|"create"|"destroy"|"remove_workspace"|"suspend"|"resume"|"snapshot"|"prepull"|"dial_attach"|"session_rpc"
	ReqID   uint64  `json:"req_id,omitempty"`
	Session string  `json:"session,omitempty"`
	Spec    *Spec   `json:"spec,omitempty"`   // create
	Warm    bool    `json:"warm,omitempty"`   // suspend
	Attach  *Attach `json:"attach,omitempty"` // dial_attach
	// RPC carries a session-RPC message down to the sandbox named by Session
	// ("session_rpc"): either a controld-originated request (diff, push_files,
	// pull_files) or the response to a "session_req" that sandbox sent up.
	// Unlike every other ToRunner type this one is not a command runnerd
	// executes and answers with a "result" — ReqID stays zero and correlation
	// lives entirely in the envelope's own ID, because the response comes from
	// the sandbox, not from the runner.
	RPC *RPCEnvelope `json:"rpc,omitempty"`
	// Ref names an image: the tag a "snapshot" must produce, or the one a
	// "prepull" should fetch ahead of a create landing on this runner. It is
	// content-addressed by controld (rainier-env:<envID>-<setupHash>) so the
	// same environment resolves to the same ref on every runner.
	Ref string `json:"ref,omitempty"`
	// PlacementGeneration is the session's placement generation on a create;
	// the runner keeps it with the sandbox and echoes it on every event about
	// that session.
	PlacementGeneration uint64 `json:"placement_generation,omitempty"` // create
	// Generation is the runner generation controld grants this connection in
	// its accept. The runner stamps it on every result and event it sends
	// afterwards.
	Generation uint64 `json:"generation,omitempty"` // accept
	// Capabilities are the announced capabilities controld accepted and will
	// schedule on — the host's own spellings are not echoed, since they are
	// not claims the runner made.
	Capabilities []string `json:"capabilities,omitempty"` // accept
}

// RepoSpec is one repository a session clones at boot, fully resolved by
// controld: the sandbox neither parses an "owner/name" string nor invents a
// branch or a directory. Every field is a decision the control plane made
// (from an environment's github connector or the session's own `repos`), so
// what the session cloned is answerable from the dispatched command alone.
//
// SessionBranch is the branch the clone checks out after fetching BaseBranch:
// rainier/<session-name>, or rainier/<last 12 of the session id> when the
// session is unnamed. Dir is the directory under /workspace the repository
// lands in — the repository's own name, unless two of them share it, in which
// case the later ones are qualified by owner.
type RepoSpec struct {
	Owner         string `json:"owner"`
	Name          string `json:"name"`
	BaseBranch    string `json:"base_branch"`
	SessionBranch string `json:"session_branch"`
	Dir           string `json:"dir"`
}

// Spec is the create block of a ToRunner "create": everything controld has
// resolved about a session's environment before the runner may start it. The
// runner trusts it whole and invents none of it — the image to boot, the
// command to run, the egress allow-list, the repositories to clone with
// their fully-resolved branches and directories, the environment setup
// script and per-boot init hook with their timeouts, the git author
// identity, and the env map. Every field is omitempty because a create
// passes only the pieces that apply; Env values are secrets as often as not
// and never logged verbatim.
type Spec struct {
	Name        string   `json:"name,omitempty"`
	Image       string   `json:"image,omitempty"`
	Cmd         []string `json:"cmd,omitempty"`
	EgressAllow []string `json:"egress_allow,omitempty"`
	// Setup is the environment's setup script, run once inside the fresh
	// container; the runner reports its outcome as a "setup_done" /
	// "setup_failed" event. SetupTimeoutSec bounds that run (0 = the
	// runner's default). Both are absent on a create whose environment was
	// already snapshot-cached — the cached image IS the finished setup.
	Setup           string `json:"setup,omitempty"`
	SetupTimeoutSec int    `json:"setup_timeout_sec,omitempty"`
	// Repos are the repositories this session clones at boot, in the order
	// they are cloned. Empty is a session that clones nothing — a scratch
	// session, or one whose `repos` was an explicit empty list.
	Repos []RepoSpec `json:"repos,omitempty"`
	// Init is the environment's per-boot hook, run AFTER the clones and
	// before the agent, on every create including the ones that boot a cached
	// snapshot. That is the whole difference between it and Setup: setup
	// builds the image and is baked into the cache, init runs against the
	// code that was just cloned and therefore cannot be. InitTimeoutSec
	// bounds it (0 = the runner's default), the same carried-not-policed
	// contract Setup's timeout has.
	Init           string `json:"init,omitempty"`
	InitTimeoutSec int    `json:"init_timeout_sec,omitempty"`
	// GitAuthorName and GitAuthorEmail are the identity commits made inside
	// the session are attributed to: the owner's GitHub login and their
	// noreply address (<github_id>+<login>@users.noreply.github.com). Present
	// only when the session clones something — there is nothing to attribute
	// otherwise — and never a credential: the token stays in the vault and
	// reaches git through the in-sandbox helper, one operation at a time.
	GitAuthorName  string `json:"git_author_name,omitempty"`
	GitAuthorEmail string `json:"git_author_email,omitempty"`
	// Env is injected into the container's environment. Values are secrets
	// as often as not, so this field is never logged verbatim.
	Env map[string]string `json:"env,omitempty"`
	// Home is the agent home this session mounts: the (creator, workspace)
	// volume every coding agent keeps its own configuration and credential
	// set under. Absent on a create for a session with no creator, and on
	// every create a control plane older than this field ever sent — which is
	// why it is additive at ProtocolVersion 1 and omitempty: a runner that
	// does not know the field mounts nothing and the session's agents simply
	// ask for a login, which is the truthful state, not a failure.
	Home *HomeMount `json:"home,omitempty"`
}

// Attach is the dial_attach block of a ToRunner: controld tells the runner
// how to take over a terminal viewer it has parked. AttachID names the
// parked client pairing (16 hex characters from crypto/rand); Since is the
// attach cursor the viewer asked for, interpreted by the session exactly as
// in a relay FrameOpen — 0 for a snapshot of the current screen, the maximum
// uint64 for the whole log, otherwise the seq to replay from; Cols and Rows
// seed the terminal size; and TargetURL is this controld replica's
// attach-back WebSocket URL, which the runner must verify names its own
// controld origin before dialing it, because that dial carries the fleet
// runner token.
type Attach struct {
	AttachID  string `json:"attach_id"`
	Since     uint64 `json:"since"`
	Cols      int    `json:"cols"`
	Rows      int    `json:"rows"`
	TargetURL string `json:"target_url"` // ws(s) URL of THIS controld replica's attach-back endpoint
}

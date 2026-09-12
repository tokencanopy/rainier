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

// AgentCredentialProtocolVersion is the independently negotiated version of
// the credential-sync RPCs. It lives in the session manifest and every upward
// request, so a new control plane rejects an old sessiond rather than letting
// pre-logout writes bypass the revoke fence during a rolling deployment.
const AgentCredentialProtocolVersion = 1

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
	// {"protocol": 1, "provider": "..."} → {"version": n, "files": {name: base64}}. Version
	// 0 with no files is the truthful answer for a person who has not logged
	// that agent in; it is an answer, not a refusal, and the agent starts
	// anyway and asks them to log in.
	MethodFetchAgentCredentials = "fetch_agent_credentials"
	// MethodPutAgentCredentials is sandbox → control plane, whenever the
	// allowlisted files change: {"protocol": 1, "provider": "...", "files": {name: base64},
	// "version": n} → {"version": n+1}. The version the sandbox sends is the
	// one it last saw, so custody can tell a fresh login from a replay of a
	// set that has since been revoked.
	MethodPutAgentCredentials = "put_agent_credentials"
	// MethodRevokeAgentCredentials is control plane → sandbox, on a logout or
	// a membership that went away: {"provider": "...", "version": n} → {}.
	// Logout includes its tombstone version; membership withdrawal may omit it.
	// The sandbox removes that provider's allowlisted files and adopts a
	// supplied logout baseline. A withdrawal preserves its known custody
	// baseline while deleting local files unconditionally.
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
	// Active and IdleExited split Used by what the sandbox is actually doing:
	// Active counts sandboxes that are up with their child process still
	// running, IdleExited those that are up with the child gone. Both are
	// additive to Used/Total and ride every message beside them, so a control
	// plane can say "16 slots, 3 active, 13 idle" rather than "no free
	// capacity".
	//
	// A runner that predates them sends neither, and they read as zero — which
	// a consumer cannot tell apart from a current runner that is simply
	// holding nothing. That is a deliberate limit of an additive int, not an
	// oversight: the pair is only ever meaningful ALONGSIDE Used, and
	// "active 0, idle_exited 0, used 12" is already legible as "this runner is
	// not telling me", since twelve slots cannot be held by no sandboxes.
	//
	// Active+IdleExited is at most Used and usually less: a warm-suspended
	// sandbox and one still being created each hold a slot and are in neither
	// count, because neither has a child this runner can speak for. So is a
	// session a restarted runnerd rebuilt from its labelled container: what
	// its child is doing lived only in the memory of the process that died,
	// and a runner that has just come back reports "used 16, active 0,
	// idle_exited 0" rather than claiming sixteen working agents on a box
	// where every one of them may have finished hours ago.
	Active     int    `json:"active"`
	IdleExited int    `json:"idle_exited"`
	ReqID      uint64 `json:"req_id,omitempty"` // result: correlates ToRunner.ReqID
	OK         bool   `json:"ok,omitempty"`     // result
	// Conflict qualifies a result that is NOT ok: the runner received the
	// command and understood it, and refused it because it conflicts with
	// work the runner is already doing — a resume that overtook a cold
	// suspend the runner had already claimed, a command for a sandbox that
	// is still being created. It is the runner's "not yet", where a bare
	// OK:false is its "this failed", and only the first is worth retrying.
	//
	// The same distinction the runner's own HTTP front has always drawn
	// (409 rather than 500) reaching the control connection, which could not
	// see it before: a control plane that cannot tell the two apart reports
	// a healthy runner mid-stop as an internal error, and a client's
	// retry-on-conflict logic never runs.
	//
	// Additive both ways, like every other field added here: an old control
	// plane ignores it, and a new one reading false from an old runner —
	// which never sets it — gets exactly the behaviour it has today.
	// Meaningless on anything but a result, and omitted when false.
	Conflict bool   `json:"conflict,omitempty"` // result
	Detail   string `json:"detail,omitempty"`   // result: error text or snapshot ref; event: setup_failed tail
	Session  string `json:"session,omitempty"`  // event, session_req
	// State is the event's subject. "suspended_cold" is the runner reporting
	// a park it decided on itself — today only idle auto-stop, which stops a
	// sandbox whose child has exited and that nobody has attached to for the
	// configured timeout, exactly as an operator's stop would. It is the same
	// word the announce vocabulary uses for the same condition.
	State string `json:"state,omitempty"` // event: "running" | "dead" | "setup_done" | "setup_failed" | "child_exited" | "suspended_cold"
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
	// Mode and Generation are the controller binding the control plane
	// granted this attach: terminal.ModeControl or terminal.ModeView, and the
	// controller generation it holds. They travel here, on the command that
	// opens the attachment, so that the sandbox installs the binding in the
	// same step it creates the attachment — before a single byte of screen
	// has been queued, and with no acknowledgement round trip to order
	// against.
	//
	// Both are omitempty and both are absent from an older control plane's
	// command. A sandbox that receives no binding treats the attachment as
	// today's unconditional one, because under the old message set only one
	// client could be sending.
	Mode       string `json:"mode,omitempty"`
	Generation uint64 `json:"controller_generation,omitempty"`
	// Kind is which sort of attachment this dial-back opens: KindTerminal
	// (absent) is the session's pty, KindExec is a process this attachment
	// creates and owns. Absent is the terminal, which is what every control
	// plane older than this field sends and what a sandbox older than it
	// reads any dial_attach as.
	Kind string `json:"kind,omitempty"`
	// Exec is the command a KindExec attachment runs. It travels here, on
	// the command that opens the attachment, and never on a URL: a URL is
	// written to the access log of every proxy between the caller and the
	// cell, and an argv in a URL is an argument in a log file.
	Exec *ExecSpec `json:"exec,omitempty"`
}

// The two kinds of attachment a dial_attach can open. KindTerminal is the
// session's one pty, shared by every viewer; KindExec is a process the
// attachment itself creates and owns. The terminal kind is spelled as the
// EMPTY string because that is what every control plane older than this
// field sends, and because a runner that has never heard of the field
// forwards an absent value exactly as it forwards today's dial_attach.
const (
	KindTerminal = ""
	KindExec     = "exec"
)

// CapabilityExecV1 is the capability token a runnerd announces when its build
// can forward an exec attachment. It is a fact about the BUILD, not a claim
// an operator makes, so runnerd appends it to whatever --capability it was
// given rather than waiting to be told.
//
// It is a pre-check and not the fence. A runner forwards the dial_attach, but
// the sandbox is what actually runs the command, and a session keeps the
// sessiond it booted with for as long as it lives — so a new runner can be
// holding a session whose sandbox has never heard of exec. The authoritative
// fence is the sandbox's own `exec_started`; this token only saves the round
// trip when the answer is already known.
const CapabilityExecV1 = "exec.v1"

// ExecSpec is one command to run inside the sandbox. It is composed by the
// CALLER and validated by the sandbox; the control plane carries it and
// checks only its shape, because the filesystem the cwd and the log path
// name is the sandbox's and nobody else can ask it anything.
//
// Argv is never empty and argv[0] is exec'd directly: no shell, no glob, no
// $VAR, no `&&`. A caller who wants a shell names one.
//
// Env values are secrets as often as not and are never logged verbatim — the
// same sentence Spec.Env already carries, for the same reason.
type ExecSpec struct {
	Argv []string          `json:"argv"`          // never empty; argv[0] is exec'd directly
	Cwd  string            `json:"cwd,omitempty"` // absent means workspace.WorkspaceRoot
	Env  map[string]string `json:"env,omitempty"` // caller additions; see the env rule
	TTY  bool              `json:"tty,omitempty"`
	Cols int               `json:"cols,omitempty"` // TTY only
	Rows int               `json:"rows,omitempty"` // TTY only
	// Detach asks the sandbox to spawn the process in its own process group
	// with its output redirected to LogPath, answer `exec_started` with the
	// pid, and close the attachment. A detached process outlives its caller
	// and is bounded only by the session: it is killed when the session is
	// suspended, stopped or destroyed, never by a caller disconnecting.
	Detach bool `json:"detach,omitempty"`
	// LogPath is where a detached process's stdout and stderr go. It is
	// required with Detach and refused without it, and it is resolved inside
	// the workspace exactly as Cwd is — a detached process may not write its
	// output outside the tree its session owns.
	LogPath string `json:"log_path,omitempty"`
}

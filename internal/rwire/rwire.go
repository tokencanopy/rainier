// Package rwire defines the JSON messages exchanged between runnerd and
// controld over runnerd's single outbound control WebSocket. One struct per
// direction, same idiom as internal/wire. Proto gates major changes: controld
// rejects an announce whose Proto it doesn't speak, with a close reason
// naming both versions (design §4.3).
package rwire

import "encoding/json"

const Proto = 1

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
	// Payload is the method-specific body, opaque to runnerd. RawMessage so
	// it forwards without being parsed and re-encoded, and so it lands as
	// nested JSON rather than a base64 string.
	Payload json.RawMessage `json:"payload,omitempty"`
}

// FromRunner: runnerd → controld. Used/Total (capacity) piggyback on every
// message type so controld's runner view is always current without a separate
// capacity message.
//
// The event States split in two: "running" | "dead" report the container's
// lifecycle, while "setup_done" | "setup_failed" report the outcome of an
// environment's setup script inside an already-running container (Plan 4
// design §4.3, the setup pipeline) — a setup_failed carries the tail of the
// script's output in Detail, the same field a result uses for its error text.
type FromRunner struct {
	Type     string        `json:"type"` // "announce" | "result" | "event" | "session_req"
	Proto    int           `json:"proto,omitempty"`    // announce
	Runner   string        `json:"runner,omitempty"`   // announce
	Sessions []SessionInfo `json:"sessions,omitempty"` // announce
	Used     int           `json:"used"`
	Total    int           `json:"total"`
	ReqID    uint64        `json:"req_id,omitempty"` // result: correlates ToRunner.ReqID
	OK       bool          `json:"ok,omitempty"`     // result
	Detail   string        `json:"detail,omitempty"` // result: error text or snapshot ref; event: setup_failed tail
	Session  string        `json:"session,omitempty"` // event, session_req
	State    string        `json:"state,omitempty"`   // event: "running" | "dead" | "setup_done" | "setup_failed"
	// RPC carries a session-RPC message the sandbox originated ("session_req")
	// — a credential mint, say — which controld answers with a "session_rpc"
	// back down. Session names which sandbox it came from; without it a
	// response has nowhere to be routed.
	RPC *RPCEnvelope `json:"rpc,omitempty"`
}

type SessionInfo struct {
	ID    string `json:"id"`
	State string `json:"state"` // "running"|"suspended_warm"|"suspended_cold"
}

// ToRunner: controld → runnerd.
type ToRunner struct {
	Type    string  `json:"type"` // "create"|"destroy"|"suspend"|"resume"|"snapshot"|"prepull"|"dial_attach"|"session_rpc"
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
}

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
	// Env is injected into the container's environment. Values are secrets
	// as often as not, so this field is never logged verbatim.
	Env map[string]string `json:"env,omitempty"`
}

type Attach struct {
	AttachID  string `json:"attach_id"`
	Since     uint64 `json:"since"`
	Cols      int    `json:"cols"`
	Rows      int    `json:"rows"`
	TargetURL string `json:"target_url"` // ws(s) URL of THIS controld replica's attach-back endpoint
}

// Package rwire defines the JSON messages exchanged between runnerd and
// controld over runnerd's single outbound control WebSocket. One struct per
// direction, same idiom as internal/wire. Proto gates major changes: controld
// rejects an announce whose Proto it doesn't speak, with a close reason
// naming both versions (design §4.3).
package rwire

const Proto = 1

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
	Type     string        `json:"type"` // "announce" | "result" | "event"
	Proto    int           `json:"proto,omitempty"`    // announce
	Runner   string        `json:"runner,omitempty"`   // announce
	Sessions []SessionInfo `json:"sessions,omitempty"` // announce
	Used     int           `json:"used"`
	Total    int           `json:"total"`
	ReqID    uint64        `json:"req_id,omitempty"` // result: correlates ToRunner.ReqID
	OK       bool          `json:"ok,omitempty"`     // result
	Detail   string        `json:"detail,omitempty"` // result: error text or snapshot ref; event: setup_failed tail
	Session  string        `json:"session,omitempty"` // event
	State    string        `json:"state,omitempty"`   // event: "running" | "dead" | "setup_done" | "setup_failed"
}

type SessionInfo struct {
	ID    string `json:"id"`
	State string `json:"state"` // "running"|"suspended_warm"|"suspended_cold"
}

// ToRunner: controld → runnerd.
type ToRunner struct {
	Type    string  `json:"type"` // "create"|"destroy"|"suspend"|"resume"|"snapshot"|"prepull"|"dial_attach"
	ReqID   uint64  `json:"req_id,omitempty"`
	Session string  `json:"session,omitempty"`
	Spec    *Spec   `json:"spec,omitempty"`   // create
	Warm    bool    `json:"warm,omitempty"`   // suspend
	Attach  *Attach `json:"attach,omitempty"` // dial_attach
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

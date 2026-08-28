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
type FromRunner struct {
	Type     string        `json:"type"` // "announce" | "result" | "event"
	Proto    int           `json:"proto,omitempty"`    // announce
	Runner   string        `json:"runner,omitempty"`   // announce
	Sessions []SessionInfo `json:"sessions,omitempty"` // announce
	Used     int           `json:"used"`
	Total    int           `json:"total"`
	ReqID    uint64        `json:"req_id,omitempty"` // result: correlates ToRunner.ReqID
	OK       bool          `json:"ok,omitempty"`     // result
	Detail   string        `json:"detail,omitempty"` // result: error text, or snapshot ref
	Session  string        `json:"session,omitempty"` // event
	State    string        `json:"state,omitempty"`   // event: "running" | "dead"
}

type SessionInfo struct {
	ID    string `json:"id"`
	State string `json:"state"` // "running"|"suspended_warm"|"suspended_cold"
}

// ToRunner: controld → runnerd.
type ToRunner struct {
	Type    string  `json:"type"` // "create"|"destroy"|"suspend"|"resume"|"snapshot"|"dial_attach"
	ReqID   uint64  `json:"req_id,omitempty"`
	Session string  `json:"session,omitempty"`
	Spec    *Spec   `json:"spec,omitempty"`   // create
	Warm    bool    `json:"warm,omitempty"`   // suspend
	Attach  *Attach `json:"attach,omitempty"` // dial_attach
}

type Spec struct {
	Name        string   `json:"name,omitempty"`
	Image       string   `json:"image,omitempty"`
	Cmd         []string `json:"cmd,omitempty"`
	EgressAllow []string `json:"egress_allow,omitempty"`
}

type Attach struct {
	AttachID  string `json:"attach_id"`
	Since     uint64 `json:"since"`
	Cols      int    `json:"cols"`
	Rows      int    `json:"rows"`
	TargetURL string `json:"target_url"` // ws(s) URL of THIS controld replica's attach-back endpoint
}

// Package relay multiplexes many client attachments over the single outbound
// WebSocket a session opens to runnerd (spec rule 3). One Frame = one event
// for one attachment, tagged by AttachID.
package relay

import "encoding/json"

type FrameType uint8

const (
	FrameOpen FrameType = iota
	FrameClose
	FrameClient
	FrameServer
	// FrameControl carries a sessiond-originated control event — a setup
	// outcome today, a credential request later — over the same conn as the
	// terminal mux, tagged with AttachID 0 because no attachment owns it.
	// Its value is spelled out instead of left to iota because it is
	// wire-visible: the two ends of a live conn can be different builds, so
	// the numbers above must never renumber underneath it.
	FrameControl FrameType = 4
)

type Frame struct {
	Type     FrameType `json:"t"`
	AttachID uint64    `json:"a"`
	Since    uint64    `json:"s,omitempty"`
	Cols     int       `json:"c,omitempty"`
	Rows     int       `json:"r,omitempty"`
	Payload  []byte    `json:"p,omitempty"`
}

// ControlEvent is the JSON payload a FrameControl carries: sessiond
// reporting on something that belongs to the session as a whole rather than
// to any one viewer. Today the only kinds are the two setup outcomes —
// "setup_done" and "setup_failed" — which runnerd turns into rwire events
// for controld's setup orchestration.
//
// It lives here, beside the frame that carries it, so the two ends of that
// hop cannot drift: cmd/sessiond marshals this type and internal/runnerd
// unmarshals it. relay does not interpret it — a payload is opaque bytes to
// Send and to the hub's control handler alike — but owning the shape is what
// keeps a field rename from silently becoming a dropped event.
type ControlEvent struct {
	Kind string `json:"kind"`
	// RC is the setup script's exit status on a "setup_failed", or -1 when
	// the script was still running at its timeout and was terminated.
	RC int `json:"rc,omitempty"`
	// Tail is the last few KB of the session's own output on a
	// "setup_failed" — what the script printed before it gave up — or the
	// timeout message for RC -1. It is the only diagnostic that reaches a
	// user whose session never came up, so it travels with the event rather
	// than being left in a log inside a container that is about to go away.
	Tail string `json:"tail,omitempty"`
}

func Encode(f Frame) ([]byte, error) { return json.Marshal(f) }

func Decode(b []byte) (Frame, error) {
	var f Frame
	err := json.Unmarshal(b, &f)
	return f, err
}

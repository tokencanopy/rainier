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

func Encode(f Frame) ([]byte, error) { return json.Marshal(f) }

func Decode(b []byte) (Frame, error) {
	var f Frame
	err := json.Unmarshal(b, &f)
	return f, err
}

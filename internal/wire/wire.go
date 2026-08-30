// Package wire defines the JSON messages exchanged between sessiond and
// viewers. One message type per direction keeps the protocol greppable.
package wire

// SinceAll is the attach cursor that asks for the WHOLE event log, first
// entry onward — what `rainier attach --since 0` (and `new`'s auto-attach)
// requests, and what the runbook's "read the full setup output" flow needs.
//
// It cannot be spelled 0. Since Plan 1 an attach cursor of 0 has meant "I
// hold no cursor at all — paint me a screen", which is what every plain
// attach sends and what internal/server's tests pin; and relay.Frame.Since
// is `json:"s,omitempty"`, so an explicit 0 is literally indistinguishable
// from an absent one by the time the request reaches sessiond. Those two
// requests are different and always were, so the second one gets its own
// value rather than a second meaning bolted onto the first.
//
// A reserved maximum is the one value in the domain no real cursor can ever
// be (a viewer resuming after 2^64-1 frames has other problems), so it needs
// no new field on any hop: it rides the existing uint64 through the attach
// query string, rwire.Attach, and relay.Frame exactly like any other cursor,
// and only the two ends — the CLI that spells it and session.Attach that
// reads it — know it is special.
const SinceAll uint64 = ^uint64(0)

type ClientMsg struct {
	Type string `json:"type"`           // "stdin" | "resize"
	Data []byte `json:"data,omitempty"` // stdin bytes
	Cols int    `json:"cols,omitempty"`
	Rows int    `json:"rows,omitempty"`
}

type ServerMsg struct {
	Type     string `json:"type"` // "snapshot" | "output" | "exit"
	Seq      uint64 `json:"seq,omitempty"`
	Data     []byte `json:"data,omitempty"`
	Cols     int    `json:"cols,omitempty"`
	Rows     int    `json:"rows,omitempty"`
	ExitCode int    `json:"exitCode,omitempty"`
}

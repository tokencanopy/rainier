// Package terminal defines the JSON messages a terminal attach exchanges
// between a viewer and a session: ClientMessage carries stdin and resize
// into the pty, ServerMessage carries snapshot, output, and exit back out.
// One message type per direction keeps the protocol greppable. These types
// are the single source of truth for the attach wire bytes shared by
// cmd/rainier, cmd/sessiond, and controld's relay; no copy of them lives
// anywhere else.
package terminal

// SinceAll is the attach cursor that asks for the WHOLE event log, first
// entry onward — what `rainier attach --since 0` (and `new`'s auto-attach)
// requests, and what the runbook's "read the full setup output" flow needs.
//
// It cannot be spelled 0. Since Plan 1 an attach cursor of 0 has meant "I
// hold no cursor at all — paint me a screen", which is what every plain
// attach sends and what the server's tests pin; and the relay frame's
// Since field is `json:"s,omitempty"`, so an explicit 0 is literally
// indistinguishable from an absent one by the time the request reaches
// sessiond. Those two requests are different and always were, so the second
// one gets its own value rather than a second meaning bolted onto the first.
//
// A reserved maximum is the one value in the domain no real cursor can ever
// be (a viewer resuming after 2^64-1 frames has other problems), so it needs
// no new field on any hop: it rides the existing uint64 through the attach
// query string, runner.Attach, and the relay frame exactly like any other
// cursor, and only the two ends — the CLI that spells it and session.Attach
// that reads it — know it is special.
const SinceAll uint64 = ^uint64(0)

// ClientMessage is one message a viewer sends into an attached session. Type
// is "stdin" (Data carries the bytes to feed the pty) or "resize" (Cols and
// Rows carry the new terminal size). Every optional field is omitempty, so a
// resize emits only type, cols, and rows, and a stdin emits only type and
// data (base64-encoded by encoding/json, as []byte always is).
type ClientMessage struct {
	Type string `json:"type"`           // "stdin" | "resize"
	Data []byte `json:"data,omitempty"` // stdin bytes
	Cols int    `json:"cols,omitempty"`
	Rows int    `json:"rows,omitempty"`
}

// ServerMessage is one message a session sends out to a viewer. Type is
// "snapshot" (a screen paint: Data carries the serialized screen, Cols/Rows
// its size, Seq the sequence the log last committed), "output" (Seq names
// the event and Data carries the raw bytes), or "exit" (ExitCode carries the
// session's terminal status). The exit tag is the camel-case "exitCode" on
// the wire, which is part of the contract and must not change. Seq, Data,
// Cols, Rows, and ExitCode are omitempty, so each type emits exactly the
// fields that apply to it.
type ServerMessage struct {
	Type     string `json:"type"` // "snapshot" | "output" | "exit"
	Seq      uint64 `json:"seq,omitempty"`
	Data     []byte `json:"data,omitempty"`
	Cols     int    `json:"cols,omitempty"`
	Rows     int    `json:"rows,omitempty"`
	ExitCode int    `json:"exitCode,omitempty"`
}

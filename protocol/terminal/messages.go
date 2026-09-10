// Package terminal defines the JSON messages a terminal attach exchanges
// between a viewer and a session: ClientMessage carries stdin and resize
// into the pty, ServerMessage carries snapshot, output, and exit back out.
// One message type per direction keeps the protocol greppable. These types
// are the single source of truth for the attach wire bytes shared by
// cmd/rainier, cmd/sessiond, and controld's relay; no copy of them lives
// anywhere else.
package terminal

import "strconv"

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
	Type string `json:"type"`           // "stdin" | "resize" | "claim" | "release" | "control"
	Data []byte `json:"data,omitempty"` // stdin bytes
	Cols int    `json:"cols,omitempty"`
	Rows int    `json:"rows,omitempty"`
	// Mode is the mode a plane is installing on a "control" message into a
	// sandbox: ModeControl or ModeView.
	Mode string `json:"mode,omitempty"`
	// Expected is the generation a claim advances from — the one the client
	// last saw. A claim from a superseded generation is answered "stale"
	// rather than granted, which is what makes a two-device race have exactly
	// one winner.
	Expected Gen `json:"expected,omitempty"`
	// Generation is the generation this frame was sent under. Input and
	// resize carry it so that a frame from a controller that has since been
	// displaced is discarded where it would execute, however far along the
	// path it already was. Absent means "no negotiated generation": from a
	// client, an old client; from a plane, an old plane, whose input is
	// treated as the current controller's because under the old message set
	// only one client could have sent it.
	//
	// A plane REPLACES this field on every frame it forwards, with the
	// generation it granted that attach — it stamps a legacy client's frames
	// for the same reason. A sandbox therefore reads the plane's view of who
	// was typing and never the client's own claim about it, which is what
	// keeps the fence out of reach of the peer it is fencing.
	Generation Gen `json:"gen,omitempty"`
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
	Type     string `json:"type"` // "snapshot" | "output" | "exit" | "attached" | "stale" | "control_changed" | "control_ack"
	Seq      uint64 `json:"seq,omitempty"`
	Data     []byte `json:"data,omitempty"`
	Cols     int    `json:"cols,omitempty"`
	Rows     int    `json:"rows,omitempty"`
	ExitCode int    `json:"exitCode,omitempty"`
	// Mode is this attach's mode on an "attached" or "control_changed".
	Mode string `json:"mode,omitempty"`
	// Generation is the generation this attach now holds on an "attached" or
	// "control_changed", the current generation on a "stale" (so a client
	// that lost a claim can decide whether to claim again from it), and the
	// generation being confirmed on a "control_ack".
	Generation Gen `json:"gen,omitempty"`
}

// ---------------------------------------------------------------------------
// conditional controller ownership
// ---------------------------------------------------------------------------

// The attach request's conditional-ownership parameters. They travel on the
// attach URL's query string, beside `since`, rather than in the first
// message: the negotiation is part of the request, so a plane can settle it
// before it upgrades the socket, and a plane that predates this protocol
// ignores three unknown parameters exactly as it ignores any other.
//
//	ParamControl   the capability advertisement; CapabilityControl means "I
//	               understand conditional ownership; tell me my mode and my
//	               generation". A client that omits it receives today's
//	               message set, byte for byte.
//	ParamMode      ModeControl (claim control when it is free) or ModeView
//	               (never claim). Omitted reads as ModeControl.
//	ParamExpected  a decimal generation: claim ONLY from this one. It is what
//	               a reconnecting controller presents, so that a device that
//	               was superseded while it was away comes back as a viewer
//	               instead of taking control from whoever has it.
const (
	ParamControl      = "control"
	ParamMode         = "mode"
	ParamExpected     = "expected"
	CapabilityControl = "v1"
)

// The two modes an attach can hold. A controller may write to the pty; a
// viewer receives the screen and the output and nothing it sends is executed.
// They are spelled "control" and "view" on the wire — the client's request and
// the server's answer use one vocabulary, so a reader of a packet capture does
// not have to know which direction it was going.
const (
	ModeControl = "control"
	ModeView    = "view"
)

// The message types conditional ownership adds. Every one of them is sent
// only to, or accepted only from, a peer that advertised CapabilityControl.
//
// Client → server:
//
//	TypeClaim   ask to become the controller, advancing from Expected
//	TypeRelease give up control (and, with it, the generation)
//
// Server → client:
//
//	TypeAttached       the mode and generation this attach now holds; a
//	                   negotiated client receives it BEFORE its first
//	                   snapshot or output byte
//	TypeStale          the claim lost: Generation is the current value, so
//	                   the client can decide whether to claim again from it
//	TypeControlChanged this attach is no longer what it was — a displaced
//	                   controller learns here that it is now a viewer
//
// Plane → sandbox and back, over the frames an attachment already carries:
//
//	TypeControl    installs an attachment's mode and generation at the pty
//	TypeControlAck the sandbox confirming it has installed them, which is
//	               what lets a plane refuse to tell a taker it has control
//	               until the fence that protects it is actually in place
const (
	TypeClaim          = "claim"
	TypeRelease        = "release"
	TypeAttached       = "attached"
	TypeStale          = "stale"
	TypeControlChanged = "control_changed"
	TypeControl        = "control"
	TypeControlAck     = "control_ack"
)

// Gen is a controller generation on the wire: a DECIMAL STRING, not a number.
// A generation is a uint64 and JSON numbers are IEEE 754 doubles in the
// browser that renders the next consumer of this protocol, so a large one
// would arrive silently wrong there rather than loudly broken. The empty
// string is generation zero — "no generation" — which is what omitempty
// leaves out of a message that has none.
type Gen string

// GenOf renders v as a wire generation. Zero renders as the empty string, so
// it disappears from the JSON entirely and an old peer sees the same bytes it
// has always seen.
func GenOf(v uint64) Gen {
	if v == 0 {
		return ""
	}
	return Gen(strconv.FormatUint(v, 10))
}

// Value parses g. Anything that is not a decimal uint64 — including the empty
// string — is zero, which is the safe reading: an unparseable generation
// fences exactly as a missing one does, and there is no error a peer holding
// a malformed frame could act on.
func (g Gen) Value() uint64 {
	v, err := strconv.ParseUint(string(g), 10, 64)
	if err != nil {
		return 0
	}
	return v
}

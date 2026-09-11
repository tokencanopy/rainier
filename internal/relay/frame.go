// Package relay multiplexes many client attachments over the single outbound
// WebSocket a session opens to runnerd (spec rule 3). One Frame = one event
// for one attachment, tagged by AttachID.
package relay

import (
	"encoding/json"

	"github.com/tokencanopy/rainier/protocol/runner"
)

type FrameType uint8

const (
	FrameOpen FrameType = iota
	FrameClose
	FrameClient
	FrameServer
	// FrameControl carries a control event — a setup outcome, a session-RPC
	// request, its response — over the same conn as the terminal mux, tagged
	// with AttachID 0 because no attachment owns it. It travels in BOTH
	// directions: up from sessiond through ControlSender.Send, down from
	// runnerd through Hub.SendControl.
	// Its value is spelled out instead of left to iota because it is
	// wire-visible: the two ends of a live conn can be different builds, so
	// the numbers above must never renumber underneath it.
	FrameControl FrameType = 4
)

type Frame struct {
	Type     FrameType `json:"t"`
	AttachID uint64    `json:"a"`
	// Since is a FrameOpen's attach cursor, interpreted by session.Attach: 0
	// asks for a snapshot of the current screen, terminal.SinceAll for the whole
	// event log, anything else resumes after that sequence number. The
	// omitempty is why "the whole log" has a value of its own instead of
	// being spelled 0 — a zero Since is absent from these bytes entirely, so
	// this hop cannot tell an explicit 0 from a Frame that never set it.
	Since uint64 `json:"s,omitempty"`
	Cols  int    `json:"c,omitempty"`
	Rows  int    `json:"r,omitempty"`
	// Mode and Gen are a FrameOpen's controller binding: which of
	// terminal.ModeControl / terminal.ModeView the attachment is, and the
	// controller generation it holds. They are the last hop of the binding
	// the control plane granted, and they ride the frame that opens the
	// attachment so the session installs them before it queues a screen.
	//
	// Absent — an older control plane, or an older runner — means the
	// attachment is unbound, which the session reads as today's
	// unconditional attachment. Both are omitempty for that reason: a peer
	// that sets neither writes the bytes it has always written.
	Mode string `json:"m,omitempty"`
	Gen  uint64 `json:"g,omitempty"`
	// Kind and Exec are a FrameOpen's attachment KIND: absent (runner.KindTerminal)
	// opens the session's pty exactly as it always has, runner.KindExec opens a
	// process this attachment creates and owns, with Exec naming it.
	//
	// Both are omitempty, so a terminal FrameOpen is byte-identical to the one
	// this hop has always written (pinned by TestTerminalFrameWireShape). A
	// FrameOpen carrying KindExec on a sessiond that predates the field falls
	// into today's terminal branch and answers a snapshot — which is exactly
	// why the plane requires a positive `exec_started` before it forwards
	// anything, and why that handshake, not this field, is the fence.
	Kind    string           `json:"k,omitempty"`
	Exec    *runner.ExecSpec `json:"x,omitempty"`
	Payload []byte           `json:"p,omitempty"`
}

// ControlEvent is the JSON payload a FrameControl carries: a message about
// the session as a whole rather than about any one viewer. It has three
// shapes, distinguished by Kind and ID:
//
//   - An EVENT — "setup_done", "setup_failed"/"stage_failed", "child_exited",
//     "suspending"/"suspend_ready" — is fire-and-forget and carries ID 0. Most
//     travel upward only (sessiond → runnerd), where runnerd turns them into
//     rwire events for controld; the suspend pair is the one that travels both
//     ways and stays between runnerd and the sandbox.
//   - A REQUEST is Kind "req:<method>" with an ID greater than zero. Either
//     end may originate one: the sandbox asking controld to mint a git
//     credential goes up, a diff or a push/pull goes down.
//   - A RESPONSE is Kind "resp" echoing that request's ID, with OK as its
//     verdict and Payload as its body — exactly one per request.
//
// The event shape predates the other two and its bytes must not change: every
// field added for the RPC shapes is omitempty, so a Plan 4 peer keeps seeing
// the same `{"kind":"setup_done"}` it has always seen (pinned by
// TestControlEventWireShape).
//
// It lives here, beside the frame that carries it, so the two ends of that
// hop cannot drift: cmd/sessiond marshals this type and internal/runnerd
// unmarshals it. relay does not interpret it — a payload is opaque bytes to
// ControlSender.Send, Hub.SendControl, and both control handlers alike — but
// owning the shape is what keeps a field rename from silently becoming a
// dropped event.
// The two kinds of the suspend handshake. They are constants rather than
// literals because both ends of the hop are in different repositories'
// worth of build lineage — runnerd runs on the host and sessiond ships inside
// the session image, so a typo on one side would be a silently ignored frame
// rather than a compile error.
const (
	// KindSuspending is runnerd telling a sandbox it is about to be FROZEN.
	// It is sent before `docker pause`, which sessiond never sees as a signal:
	// a paused sessiond receives no SIGTERM, so without this the exec runner's
	// KillAll — the one bound on a detached exec's life — would never run on
	// the default `rainier stop`.
	//
	// A sessiond that predates it logs an unknown kind and drops it, and
	// runnerd's acknowledgement wait expires and pauses anyway, which is
	// exactly the behaviour the fleet has today.
	KindSuspending = "suspending"
	// KindSuspendReady is the sandbox saying its execs are gone (or that it
	// gave up waiting for one). It carries nothing: runnerd is waiting for the
	// FACT, and a count would be a number nobody acts on.
	KindSuspendReady = "suspend_ready"
)

type ControlEvent struct {
	Kind string `json:"kind"`
	// ID correlates a request with its one response. It is per-direction and
	// assigned by whichever end originated the request, so the same number can
	// be in flight both ways at once without collision; each end matches a
	// response only against the requests it sent. Zero means "not an RPC" —
	// the fire-and-forget event shape — which is why it is omitempty rather
	// than a pointer: an event has no id to omit ambiguously.
	ID uint64 `json:"id,omitempty"`
	// OK is a response's verdict, meaningful only on Kind "resp". False is the
	// zero value and therefore absent from the wire, which is the safe
	// direction: a peer that fails to decode it reads a failure, never a
	// spurious success. The failure's detail lives in Payload.
	OK bool `json:"ok,omitempty"`
	// Payload is the method-specific body of a request or response, opaque to
	// relay and to runnerd's forwarding alike — only the two ends that speak
	// the method interpret it. RawMessage rather than []byte so it rides as
	// nested JSON instead of a base64 string, and so a forwarder can pass it
	// through without parsing it.
	Payload json.RawMessage `json:"payload,omitempty"`
	// Stage names which stage of the session's boot chain failed on a
	// "stage_failed": "setup", "clone", "agents", or "init". Sessiond does not send it
	// yet — the boot chain that has stages is a later task — but the field is
	// defined here, with the rest of the vocabulary, so that the sender and
	// the runnerd that decodes it are agreeing on one shape rather than two
	// that happen to match.
	Stage string `json:"stage,omitempty"`
	// RC is the failing stage's exit status, or -1 when the stage was still
	// running at its timeout and was terminated.
	RC int `json:"rc,omitempty"`
	// Tail is the last few KB of the session's own output on a stage failure —
	// what the script printed before it gave up — or the timeout message for
	// RC -1. It is the only diagnostic that reaches a user whose session never
	// came up, so it travels with the event rather than being left in a log
	// inside a container that is about to go away.
	Tail string `json:"tail,omitempty"`
}

func Encode(f Frame) ([]byte, error) { return json.Marshal(f) }

func Decode(b []byte) (Frame, error) {
	var f Frame
	err := json.Unmarshal(b, &f)
	return f, err
}

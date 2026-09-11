// Package terminal defines the JSON messages a terminal attach exchanges
// between a viewer and a session: ClientMessage carries stdin and resize
// into the pty, ServerMessage carries snapshot, output, and exit back out.
// One message type per direction keeps the protocol greppable. These types
// are the single source of truth for the attach wire bytes shared by
// cmd/rainier, cmd/sessiond, and controld's relay; no copy of them lives
// anywhere else.
package terminal

import (
	"strconv"

	"github.com/tokencanopy/rainier/protocol/runner"
)

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
	// Exec is the command an "exec_start" opens with — the required first
	// message of an exec attachment, and the only message that ever carries
	// it. It is the runner protocol's ExecSpec rather than a second copy of
	// it, so the spec a caller composed is the spec the sandbox validates,
	// with no re-encoding between them.
	Exec *runner.ExecSpec `json:"exec,omitempty"`
	// Signal is the signal an "exec_signal" asks the sandbox to deliver to
	// the exec's process group: SignalTERM or SignalINT and nothing else. A
	// sandbox refuses every other word rather than translating it.
	Signal string `json:"signal,omitempty"`
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
	// Signal is the signal that killed an exec's process on an "exec_exit",
	// spelled as a name ("TERM", "KILL", "SEGV"). Exactly one of ExitCode
	// and Signal is meaningful on an exec_exit, and Signal being non-empty
	// is what says which: a process killed by a signal has no exit code, and
	// reporting 0 for one would tell a script the command succeeded.
	Signal string `json:"signal,omitempty"`
	// Reason is an "exec_error"'s machine-readable cause, from the closed
	// vocabulary below. It is closed because the CLI maps it to an exit code
	// and a sentence, and because free-form prose from inside a sandbox is a
	// string somebody's terminal renders.
	Reason string `json:"reason,omitempty"`
	// PID is the process id an "exec_started" reports for a DETACHED exec —
	// the one fact its caller needs, since `rainier exec s -- kill <pid>` is
	// how a detached process is stopped. It is absent on an attached exec,
	// whose lifetime is its caller's and which therefore has nothing to
	// address later.
	PID int `json:"pid,omitempty"`
}

// ---------------------------------------------------------------------------
// exec
// ---------------------------------------------------------------------------

// The message types an exec attachment adds. Exec reuses ClientMessage and
// ServerMessage rather than introducing a third pair: the plane forwards
// whole messages of these two types, and a third would mean a third decode at
// every hop. Only the type words are new.
//
// Client → server:
//
//	TypeExecStart    the spec; MUST be the first message on an exec
//	                 attachment, and is the only one that carries Exec
//	TypeExecStdinEOF the caller's stdin reached EOF; close the child's
//	                 stdin. It is an explicit frame because a socket that is
//	                 still open cannot express "no more input", and `cat`
//	                 with a pipe on the other end must terminate
//	TypeExecSignal   deliver Signal to the exec's process GROUP
//
// "stdin" and "resize" are reused as-is, and neither ever carries a
// generation on an exec attachment: a plane does not stamp an exec frame and
// a sandbox does not read one off it.
//
// Server → client:
//
//	TypeExecStarted  the process exists. This is the HANDSHAKE: a plane
//	                 accepts an exec only from a sandbox that sent it, and
//	                 not one byte of the caller's stdin is forwarded before
//	                 it. A sandbox that predates exec answers a snapshot
//	                 instead, which is what makes an old sandbox a clean
//	                 refusal rather than a terminal attachment nobody asked
//	                 for
//	TypeExecStdout   the command's stdout (and, under --tty, everything,
//	                 because a pty has one stream)
//	TypeExecStderr   the command's stderr
//	TypeExecExit     ExitCode, or Signal when a signal killed it
//	TypeExecError    Reason, from the closed vocabulary below
const (
	TypeExecStart    = "exec_start"
	TypeExecStdinEOF = "exec_stdin_eof"
	TypeExecSignal   = "exec_signal"
	TypeExecStarted  = "exec_started"
	TypeExecStdout   = "exec_stdout"
	TypeExecStderr   = "exec_stderr"
	TypeExecExit     = "exec_exit"
	TypeExecError    = "exec_error"
)

// ExitSignalNumber is the number behind the name an `exec_exit` carries, so a
// caller can report the shell's 128+N for a command a signal killed.
//
// The names are SHORT ("TERM", not "SIGTERM" and not "terminated") because
// they are wire vocabulary: a Go runtime's own spelling of a signal is an
// implementation detail of the sandbox, and a client that had to recognise it
// would be coupled to the sandbox's language. An unknown name may also be a
// bare decimal, which is what a sandbox sends for a signal this table does
// not have — so a new signal reports a correct 128+N instead of a wrong one.
//
// The numbers are Linux's, which is what a sandbox is. They are written out
// rather than taken from syscall so that this package stays free of a
// platform: it is the wire, and the browser that renders the next consumer of
// it has no syscall package at all.
func ExitSignalNumber(name string) (int, bool) {
	if n, ok := exitSignalNumbers[name]; ok {
		return n, true
	}
	if n, err := strconv.Atoi(name); err == nil && n > 0 && n < 128 {
		return n, true
	}
	return 0, false
}

var exitSignalNumbers = map[string]int{
	"HUP": 1, "INT": 2, "QUIT": 3, "ILL": 4, "TRAP": 5, "ABRT": 6, "BUS": 7,
	"FPE": 8, "KILL": 9, "USR1": 10, "SEGV": 11, "USR2": 12, "PIPE": 13,
	"ALRM": 14, "TERM": 15, "STKFLT": 16, "CHLD": 17, "CONT": 18, "STOP": 19,
	"TSTP": 20, "TTIN": 21, "TTOU": 22, "URG": 23, "XCPU": 24, "XFSZ": 25,
	"VTALRM": 26, "PROF": 27, "WINCH": 28, "IO": 29, "PWR": 30, "SYS": 31,
}

// The two signals an exec attachment may ask for, and the only two. They are
// the ones a caller's own Ctrl-C and its process's termination map onto;
// anything else is a request to do something to a process inside a sandbox
// that the caller can express by running `kill` in that sandbox, where it is
// audited like any other command.
const (
	SignalTERM = "TERM"
	SignalINT  = "INT"
)

// ExecError's closed vocabulary. Every one of them means the command did not
// run, which is why the CLI maps the whole set to 126 except ReasonNotFound
// (127) and ReasonUnsupported (1, with the version sentence).
//
//	ReasonUnsupported  this sandbox does not know what an exec is — an old
//	                   sessiond, detected by the missing exec_started
//	ReasonNotFound     argv[0] was not found on the SESSION's PATH
//	ReasonNotExecutable argv[0] was found and is not executable
//	ReasonCwdRefused   --cwd is not inside the workspace, or leaves it
//	                   through a symbolic link
//	ReasonEnvRefused   an --env name the env rule refuses; the message names
//	                   the VARIABLE'S NAME and nothing else, never its value
//	ReasonLogRefused   --log is missing, is not inside the workspace, or
//	                   could not be opened
//	ReasonTooManyExecs this session already has the maximum number of
//	                   concurrent execs
//	ReasonNoAnswer     the sandbox did not answer in time. It is kept apart
//	                   from ReasonUnsupported deliberately: "this session was
//	                   created before exec shipped" is permanent and sends a
//	                   caller to make a new session, and saying it about a
//	                   sandbox that was merely slow would be a false and
//	                   actionable statement
//	ReasonStdinOverrun the caller sent more input than the sandbox may hold
//	                   for a command that is not reading it. It is the one
//	                   reason that can arrive AFTER exec_started, because it
//	                   is about the input rather than about the command
const (
	ReasonUnsupported   = "unsupported"
	ReasonNotFound      = "not_found"
	ReasonNotExecutable = "not_executable"
	ReasonCwdRefused    = "cwd_refused"
	ReasonEnvRefused    = "env_refused"
	ReasonLogRefused    = "log_refused"
	ReasonTooManyExecs  = "too_many_execs"
	ReasonNoAnswer      = "no_answer"
	ReasonStdinOverrun  = "stdin_overrun"
)

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

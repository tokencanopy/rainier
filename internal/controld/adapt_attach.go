package controld

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// This file is the attach plane behind two control ports: the dial-back
// pairing as control.AttachmentBroker, and the client's websocket as
// control.TerminalStream. Everything above them — who may attach, in which
// mode, and whether the session is attachable at all — is the attachment
// service's; everything below is this replica's sockets. Neither side reads a
// terminal message for anything but forwarding it, and no message, no byte of
// it, and no length of it is ever logged.

// ---------------------------------------------------------------------------
// the close vocabulary
// ---------------------------------------------------------------------------

// The four reasons a client socket is closed with, as sentinels rather than
// sentences, because the sentence is wsTerminalStream.Close's to pick: a
// close reason is a fixed, 123-byte-bounded string on the wire, and none of
// these may ever quote what the client or the runner said.
var (
	// errAttachFirstMessage is a client that did not open with the resize the
	// protocol requires (or did not open at all before the first-message
	// timeout). It is the client's own protocol error, so it closes with a
	// policy violation rather than an invitation to retry.
	errAttachFirstMessage = errors.New("controld: the first attach message must be a resize")
	// errAttachNoDialBack is the pairing TTL: the runner took the dial_attach
	// and never came back for the client.
	errAttachNoDialBack = errors.New("controld: no dial-back within the pairing TTL")
	// errAttachIDCollision is the refusal to overwrite another client's
	// parked pairing (see attachTable.park).
	errAttachIDCollision = errors.New("controld: attach id collision")
	// errAttachEnded is an attach that ran and is over — one side of the
	// splice stopped, and the other is being closed after it.
	errAttachEnded = errors.New("controld: the attach ended")
)

// ---------------------------------------------------------------------------
// control.TerminalStream over the client socket
// ---------------------------------------------------------------------------

// wsTerminalStream is the typed adapter between the client's websocket and
// control.TerminalStream. relay.Conn is raw frames; the port carries whole
// terminal.ClientMessage/ServerMessage values, and protocol/terminal IS the
// wire format, so the decode/encode across this boundary is lossless.
//
// close is the socket's one closing step, held by a sync.Once because two
// owners can reach it: the broker (a protocol error, or the pairing TTL) and
// the attachment service or the handler above it (any refusal after the
// upgrade). Whichever arrives first names the reason; a second close would
// otherwise fail on an already-closed socket and log a line saying nothing.
type wsTerminalStream struct {
	c    *websocket.Conn
	once *sync.Once
}

var _ control.TerminalStream = wsTerminalStream{}

func newWSTerminalStream(c *websocket.Conn) wsTerminalStream {
	return wsTerminalStream{c: c, once: &sync.Once{}}
}

// Receive reads one client message. A read failure is the caller's to
// interpret: it is a client that hung up as often as it is a malformed frame,
// and neither is worth a log line.
func (s wsTerminalStream) Receive(ctx context.Context) (terminal.ClientMessage, error) {
	var m terminal.ClientMessage
	if err := wsjson.Read(ctx, s.c, &m); err != nil {
		return terminal.ClientMessage{}, err
	}
	return m, nil
}

// Send writes one server message to the client.
func (s wsTerminalStream) Send(ctx context.Context, m terminal.ServerMessage) error {
	return wsjson.Write(ctx, s.c, m)
}

// Close ends the socket with the one close code and fixed reason its error
// maps to. Everything the service reports — and every failure the broker has
// no specific word for — is "try again later": the client's own next attach
// is the remedy, and the reason says which dependency to blame without
// quoting anybody.
func (s wsTerminalStream) Close(err error) error {
	code, reason := attachCloseReason(err)
	s.once.Do(func() { closeAttach(s.c, code, reason) })
	return nil
}

// attachCloseReason is the whole mapping, in one place: a client that broke
// the protocol or may not be here is a policy violation, and everything else
// is a retryable close naming the dependency.
func attachCloseReason(err error) (websocket.StatusCode, string) {
	switch {
	case errors.Is(err, errAttachFirstMessage):
		return websocket.StatusPolicyViolation, "first attach message must be resize"
	case errors.Is(err, control.ErrDenied):
		return websocket.StatusPolicyViolation, "not authorized to attach to this session"
	case errors.Is(err, errAttachIDCollision):
		return websocket.StatusInternalError, "attach id collision"
	case errors.Is(err, errAttachNoDialBack):
		return websocket.StatusTryAgainLater, "runner did not dial back"
	case errors.Is(err, control.ErrConflict):
		return websocket.StatusTryAgainLater, "session not ready"
	case errors.Is(err, errAttachEnded):
		return websocket.StatusTryAgainLater, "the attach ended"
	default:
		return websocket.StatusTryAgainLater, "runner unreachable"
	}
}

// ---------------------------------------------------------------------------
// the attach cursor, adapter-internal
// ---------------------------------------------------------------------------

// attachSinceKey carries the client's `since` from the HTTP handler to the
// broker. control.AttachTarget names the session, the placement and the two
// generations — not the cursor — and control is frozen, so the cursor travels
// beside the command rather than in it. It is adapter-internal on both ends:
// nothing outside this package sets or reads it.
type attachSinceKey struct{}

func withAttachSince(ctx context.Context, since uint64) context.Context {
	return context.WithValue(ctx, attachSinceKey{}, since)
}

// attachSince is the cursor the handler parsed, or 0 — the same answer a
// malformed `since` gets on the wire, which costs a full replay and never an
// error the client could act on.
func attachSince(ctx context.Context) uint64 {
	since, _ := ctx.Value(attachSinceKey{}).(uint64)
	return since
}

// ---------------------------------------------------------------------------
// control.AttachmentBroker over the dial-back pairing
// ---------------------------------------------------------------------------

// attachBroker is the dial-back pairing behind control.AttachmentBroker. By
// the time it is called the attachment service has settled every question of
// authority and readiness; what is left is this replica's half of design
// §4.2 — read the opening resize, park the client under a fresh attach id,
// ask the session's runner to dial back, and hold the client until the splice
// that claims it is over.
type attachBroker struct{ srv *Server }

var _ control.AttachmentBroker = attachBroker{}

// Attach performs the pairing for one authorized attach. Every exit closes
// the client stream with a reason it can read: the port's contract is that a
// broker either splices the stream or ends it, never both and never neither.
func (b attachBroker) Attach(ctx context.Context, target control.AttachTarget, stream control.TerminalStream) error {
	first, err := attachFirstResize(ctx, stream)
	if err != nil {
		_ = stream.Close(err)
		return err
	}

	attachID := randHex(8) // 16 hex characters, crypto/rand
	pa := &pendingAttach{stream: stream, done: make(chan struct{})}
	// Park before sending: the runner can dial back the instant it reads the
	// command, and an entry that isn't there yet would be refused.
	if !b.srv.attaches.park(attachID, pa) {
		log.Printf("controld: attach %s: attach id %s is already parked; refusing rather than "+
			"overwriting another client's pairing", target.SessionID, attachID)
		_ = stream.Close(errAttachIDCollision)
		return control.ErrUnavailable
	}

	dial := runner.ToRunner{Type: "dial_attach", Session: string(target.SessionID), Attach: &runner.Attach{
		AttachID:  attachID,
		Since:     attachSince(ctx),
		Cols:      first.Cols,
		Rows:      first.Rows,
		TargetURL: b.srv.attachBackURL(attachID),
	}}
	if err := b.srv.sendToRunner(string(target.RunnerID), dial); err != nil {
		// The command never left this process, so no runner can ever claim
		// this entry: take it back and let the caller close the client. The
		// 502 this would have been is moot post-upgrade — a close reason is
		// all the client can still be told.
		b.srv.attaches.claim(attachID)
		log.Printf("controld: attach %s: %v", target.SessionID, err)
		return control.ErrUnavailable
	}

	// Nobody may hold a parked socket forever. If the dial-back never comes
	// (the runner died between reading the command and dialing, the command
	// was lost with a flapping conn), the TTL is what closes the client
	// rather than leaving it waiting on a terminal that will never speak.
	expired := make(chan struct{})
	ttl := time.AfterFunc(b.srv.cfg.AttachPairTTL, func() {
		if _, ok := b.srv.attaches.claim(attachID); !ok {
			return // the dial-back got here first; it owns the socket now
		}
		log.Printf("controld: attach %s: no dial-back from %s within %s; closing the client",
			target.SessionID, target.RunnerID, b.srv.cfg.AttachPairTTL)
		close(expired)
		_ = stream.Close(errAttachNoDialBack)
		close(pa.done)
	})
	defer ttl.Stop()

	// Hold the attach open for its whole life: the socket now belongs to
	// whoever claims the pairing, and returning would let the handler above
	// run its deferred close on a socket the splice is still using.
	<-pa.done
	select {
	case <-expired:
		return control.ErrUnavailable
	default:
		return nil
	}
}

// attachFirstResize reads the one message a client must open with. Its
// cols/rows size the dial_attach — and so the session's FrameOpen — which is
// why it is consumed here and deliberately not forwarded into the splice;
// every later resize travels as ordinary client traffic. The bound is the
// same one an upgraded-but-silent client has always had: without it, a client
// that connects and says nothing parks a goroutine and a file descriptor
// indefinitely.
func attachFirstResize(ctx context.Context, stream control.TerminalStream) (terminal.ClientMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, attachFirstMsgTimeout)
	defer cancel()
	m, err := stream.Receive(ctx)
	if err != nil {
		// The client's own error text says nothing this end may relay, and
		// the outcome is the same either way: it did not open the way the
		// protocol requires.
		return terminal.ClientMessage{}, errAttachFirstMessage
	}
	if m.Type != "resize" {
		return terminal.ClientMessage{}, errAttachFirstMessage
	}
	return m, nil
}

package attachplane

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/coder/websocket"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

const (
	// attachReadLimit matches runnerd's and sessiond's own read limits: a
	// snapshot replaying a large scrollback is the biggest frame either
	// direction of this splice ever carries.
	attachReadLimit = 16 << 20
	// attachFirstMsgTimeout bounds how long an upgraded-but-silent client
	// may hold a socket before sending the resize the protocol requires
	// first. Without it a client that connects and says nothing parks a
	// goroutine and a file descriptor indefinitely.
	attachFirstMsgTimeout = 15 * time.Second
	// defaultPairTTL bounds how long a parked client socket waits for its
	// runner to dial back before the plane closes it (design §5).
	defaultPairTTL = 15 * time.Second
)

// Host is what a plane needs from its host: the three things it cannot know
// on its own. Every method is one dependency.
type Host interface {
	// IdentifyRunner authenticates a runner's dial-back and names the pool
	// and runner it is. Any error refuses the dial-back before the socket is
	// upgraded and before any pairing is claimed.
	IdentifyRunner(ctx context.Context, r *http.Request) (control.PoolID, control.RunnerID, error)
	// Send delivers a command to a runner — the dial_attach, in this plane's
	// case — reporting only whether it was queued for delivery.
	Send(pool control.PoolID, id control.RunnerID, m runner.ToRunner) error
	// BackURL is this replica's attach-back WebSocket URL for an attach id.
	// It must carry the id as the `attach_id` query parameter, which is what
	// BackHandler reads it back out of, and it must route to THIS replica: a
	// host with a gateway in front of it names the gateway's public URL and
	// routes the dial-back home itself.
	BackURL(attachID string) string
}

// Options configures a plane. The zero value is usable: the pairing TTL
// defaults and log lines go to the standard logger.
type Options struct {
	// PairTTL bounds how long a parked client socket waits for its runner's
	// dial-back before the plane closes it. Zero means 15s.
	PairTTL time.Duration
	// Logf receives the plane's operational log lines — a refused pairing, a
	// dial-back that never came. It never receives a terminal message, a byte
	// of one, or a length of one. Zero means log.Printf.
	Logf func(string, ...any)
}

// Plane is one replica's attach plane: the pairings it is waiting on, the
// dial-back endpoint they are claimed through, and the broker that mints
// them. Its zero value is not usable — construct it with New.
type Plane struct {
	host     Host
	ttl      time.Duration
	logf     func(string, ...any)
	attaches *attachTable
}

// New returns a plane over h. It panics on a nil host: a plane without one
// cannot reach a single runner, and failing at composition is better than
// failing at the first attach.
func New(h Host, o Options) *Plane {
	if h == nil {
		panic("attachplane: a Host is required")
	}
	if o.PairTTL <= 0 {
		o.PairTTL = defaultPairTTL
	}
	if o.Logf == nil {
		o.Logf = log.Printf
	}
	return &Plane{host: h, ttl: o.PairTTL, logf: o.Logf, attaches: newAttachTable()}
}

// Broker returns the plane behind control.AttachmentBroker, for the
// application's attachment service to hand authorized streams to.
func (p *Plane) Broker() control.AttachmentBroker { return broker{p} }

// BackHandler returns the runner's dial-back endpoint. Mount it at the path
// BackURL names.
func (p *Plane) BackHandler() http.Handler { return http.HandlerFunc(p.handleAttachBack) }

// ---------------------------------------------------------------------------
// control.AttachmentBroker over the dial-back pairing
// ---------------------------------------------------------------------------

// broker is the dial-back pairing behind control.AttachmentBroker. By the
// time it is called the attachment service has settled every question of
// authority and readiness; what is left is this replica's half of design
// §4.2 — read the opening resize, park the client under a fresh attach id,
// ask the session's runner to dial back, and hold the client until the splice
// that claims it is over.
type broker struct{ p *Plane }

var _ control.AttachmentBroker = broker{}

// Attach performs the pairing for one authorized attach. Every exit closes
// the client stream with a reason it can read: the port's contract is that a
// broker either splices the stream or ends it, never both and never neither.
func (b broker) Attach(ctx context.Context, target control.AttachTarget, stream control.TerminalStream) error {
	p := b.p
	first, err := attachFirstResize(ctx, stream)
	if err != nil {
		_ = stream.Close(err)
		return err
	}

	attachID := randHex(8) // 16 hex characters, crypto/rand
	pa := &pendingAttach{stream: stream, done: make(chan struct{})}
	// Park before sending: the runner can dial back the instant it reads the
	// command, and an entry that isn't there yet would be refused.
	if !p.attaches.park(attachID, pa) {
		p.logf("controld: attach %s: attach id %s is already parked; refusing rather than "+
			"overwriting another client's pairing", target.SessionID, attachID)
		_ = stream.Close(errAttachIDCollision)
		return control.ErrUnavailable
	}

	dial := runner.ToRunner{Type: "dial_attach", Session: string(target.SessionID), Attach: &runner.Attach{
		AttachID:  attachID,
		Since:     Since(ctx),
		Cols:      first.Cols,
		Rows:      first.Rows,
		TargetURL: p.host.BackURL(attachID),
	}}
	if err := p.host.Send(target.PoolID, target.RunnerID, dial); err != nil {
		// The command never left this process, so no runner can ever claim
		// this entry: take it back and let the caller close the client. The
		// 502 this would have been is moot post-upgrade — a close reason is
		// all the client can still be told.
		p.attaches.claim(attachID)
		p.logf("controld: attach %s: %v", target.SessionID, err)
		return control.ErrUnavailable
	}

	// Nobody may hold a parked socket forever. If the dial-back never comes
	// (the runner died between reading the command and dialing, the command
	// was lost with a flapping conn), the TTL is what closes the client
	// rather than leaving it waiting on a terminal that will never speak.
	expired := make(chan struct{})
	ttl := time.AfterFunc(p.ttl, func() {
		if _, ok := p.attaches.claim(attachID); !ok {
			return // the dial-back got here first; it owns the socket now
		}
		p.logf("controld: attach %s: no dial-back from %s within %s; closing the client",
			target.SessionID, target.RunnerID, p.ttl)
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

// ---------------------------------------------------------------------------
// WS GET <BackURL path>?attach_id=
// ---------------------------------------------------------------------------

// handleAttachBack serves the runner half of the pairing: runnerd dials this
// outbound (spec rule 3 — nothing dials into a runner) carrying the attach_id
// the plane handed it, and the plane splices that socket onto the client
// waiting under it. Authentication is the host's, the same check it makes on
// a runner's control connection.
//
// The identity the host returns is deliberately not matched against the
// pairing: an attach id is 8 crypto/rand bytes handed to exactly one runner,
// so the id IS the claim, and a host that wants to fence on the runner too
// can refuse the dial-back in IdentifyRunner.
func (p *Plane) handleAttachBack(w http.ResponseWriter, r *http.Request) {
	if _, _, err := p.host.IdentifyRunner(r.Context(), r); err != nil {
		writeErr(w, http.StatusUnauthorized, "unauthenticated", "invalid runner token")
		return
	}
	attachID := r.URL.Query().Get("attach_id")
	// Answer an expired or unknown pairing as plain HTTP, before upgrading:
	// a runner that dialed back too late gets a status code it can log
	// rather than a websocket it must decode a close reason from. This is
	// only a hint — claim below is what actually takes ownership.
	if !p.attaches.has(attachID) {
		writeErr(w, http.StatusNotFound, "not_found", "unknown attach id")
		return
	}

	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return // Accept has already answered the request
	}
	defer c.CloseNow()
	c.SetReadLimit(attachReadLimit)

	pa, ok := p.attaches.claim(attachID)
	if !ok {
		// The TTL fired between the check above and here: the client socket
		// is already closed and gone. Not a protocol violation on the
		// runner's part — it did exactly what it was told, just too late —
		// so it gets "try again later", the same code the expired client got.
		closeAttach(c, websocket.StatusTryAgainLater, "attach pairing expired")
		return
	}
	// Release the client handler once the splice is over, whatever ends it.
	defer close(pa.done)

	splice(r.Context(), pa.stream, wsRunnerConn{c})
}

// ---------------------------------------------------------------------------
// the error envelope, and the ids
// ---------------------------------------------------------------------------

// errorEnvelope is the JSON shape of the two errors this plane answers before
// an upgrade: {"error":{"code":..., "message":...}} — the same envelope the
// rest of the /v0/ surface writes, so a runner decodes one shape whatever
// refused it. code is machine-readable; message never carries internal
// detail.
type errorEnvelope struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(errorEnvelope{Error: errorBody{Code: code, Message: msg}}); err != nil {
		log.Printf("controld: writing JSON response: %v", err)
	}
}

// randHex returns n random bytes, hex-encoded (2n hex characters), sourced
// from crypto/rand.
func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand failing means the OS entropy source is broken; there
		// is no sane recovery, and an attach id that is not unguessable is
		// not an attach id.
		panic("attachplane: crypto/rand: " + err.Error())
	}
	return hex.EncodeToString(b)
}

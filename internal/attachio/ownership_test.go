package attachio

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/creack/pty"

	"github.com/tokencanopy/rainier/protocol/terminal"
)

// fakePlane is a server on the other end of one attach: it records the query
// string the client dialed with and every frame it sent, and it says whatever
// the test tells it to say. A plane that predates conditional ownership is a
// script with no ownership message in it — which is faithful, because that is
// exactly and only what an old plane is on the wire.
type fakePlane struct {
	script []terminal.ServerMessage // sent, in order, after the opening resize

	mu       sync.Mutex
	query    url.Values
	got      []terminal.ClientMessage
	extra    chan terminal.ServerMessage
	connOnce sync.Once
	ready    chan struct{}
}

func newFakePlane(script ...terminal.ServerMessage) *fakePlane {
	return &fakePlane{script: script,
		extra: make(chan terminal.ServerMessage, 8), ready: make(chan struct{})}
}

func (p *fakePlane) serve(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer c.CloseNow()
	p.mu.Lock()
	p.query = r.URL.Query()
	p.mu.Unlock()

	ctx := r.Context()
	var first terminal.ClientMessage
	if wsjson.Read(ctx, c, &first) != nil {
		return
	}
	p.record(first)
	for _, m := range p.script {
		if wsjson.Write(ctx, c, m) != nil {
			return
		}
	}
	p.connOnce.Do(func() { close(p.ready) })

	go func() {
		for {
			select {
			case m := <-p.extra:
				if wsjson.Write(ctx, c, m) != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	for {
		var m terminal.ClientMessage
		if wsjson.Read(ctx, c, &m) != nil {
			return
		}
		p.record(m)
	}
}

func (p *fakePlane) record(m terminal.ClientMessage) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.got = append(p.got, m)
}

func (p *fakePlane) received() []terminal.ClientMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]terminal.ClientMessage(nil), p.got...)
}

func (p *fakePlane) dialedWith(key string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.query.Get(key)
}

// awaitFrame waits for a client frame this test cares about.
func (p *fakePlane) awaitFrame(t *testing.T, match func(terminal.ClientMessage) bool) terminal.ClientMessage {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, m := range p.received() {
			if match(m) {
				return m
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("the expected client frame never arrived")
	return terminal.ClientMessage{}
}

// runAgainst drives one attach against p with stdin a test writes into, and
// returns the outcome and everything printed locally.
type attachRun struct {
	plane  *fakePlane
	stdin  *os.File
	stdout *bytes.Buffer
	mu     sync.Mutex
	done   chan struct {
		out Outcome
		err error
	}
}

func (r *attachRun) printed() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stdout.String()
}

func runAgainst(t *testing.T, p *fakePlane, o Options) *attachRun {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(p.serve))
	t.Cleanup(ts.Close)
	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stdinW.Close(); stdinR.Close() })

	run := &attachRun{plane: p, stdin: stdinW, stdout: &bytes.Buffer{}}
	run.done = make(chan struct {
		out Outcome
		err error
	}, 1)
	// The buffer is written from Run's goroutine and read from the test's, so
	// it needs the same lock printed() takes.
	out := &lockedWriter{mu: &run.mu, w: run.stdout}
	go func() {
		o, err := runWithIO(context.Background(),
			"ws"+strings.TrimPrefix(ts.URL, "http")+"/attach", nil, 0, o, stdinR, out)
		run.done <- struct {
			out Outcome
			err error
		}{o, err}
	}()
	<-p.ready
	return run
}

type lockedWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(b []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(b)
}

func (r *attachRun) detach(t *testing.T) Outcome {
	t.Helper()
	if _, err := r.stdin.Write([]byte{detachKey}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-r.done:
		if got.err != nil {
			t.Fatalf("attach: %v", got.err)
		}
		return got.out
	case <-time.After(5 * time.Second):
		t.Fatal("the attach never ended")
		return Outcome{}
	}
}

func (r *attachRun) awaitPrinted(t *testing.T, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(r.printed(), want) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("never printed %q; printed %q", want, r.printed())
}

// TestTheAttachURLCarriesTheRequest pins how a client asks. The negotiation
// is part of the request rather than the first message, so a plane settles it
// before it upgrades — and a plane that predates it ignores three unknown
// parameters exactly as it ignores any other.
func TestTheAttachURLCarriesTheRequest(t *testing.T) {
	p := newFakePlane(terminal.ServerMessage{
		Type: terminal.TypeAttached, Mode: terminal.ModeControl, Generation: terminal.GenOf(4)})
	run := runAgainst(t, p, Options{Control: true, Mode: terminal.ModeControl, Expected: 3})
	defer run.detach(t)

	if got := p.dialedWith(terminal.ParamControl); got != terminal.CapabilityControl {
		t.Errorf("control = %q, want %q", got, terminal.CapabilityControl)
	}
	if got := p.dialedWith(terminal.ParamExpected); got != "3" {
		t.Errorf("expected = %q, want 3", got)
	}
	if got := p.dialedWith(terminal.ParamMode); got != "" {
		t.Errorf("mode = %q; control is the default and need not be spelled", got)
	}
}

// TestAnAttachThatAsksForNothingDialsExactlyAsBefore is the promise to every
// caller that never heard of conditional ownership, cmd/rattach included.
func TestAnAttachThatAsksForNothingDialsExactlyAsBefore(t *testing.T) {
	p := newFakePlane(terminal.ServerMessage{Type: "snapshot", Seq: 1, Data: []byte("screen")})
	run := runAgainst(t, p, Options{})
	defer run.detach(t)

	for _, key := range []string{terminal.ParamControl, terminal.ParamMode, terminal.ParamExpected} {
		if got := p.dialedWith(key); got != "" {
			t.Errorf("an unasked attach dialed with %s=%q", key, got)
		}
	}
}

// TestJourney2AViewerIsToldAndTypesNothing is the phone's experience: one
// line naming the situation and the key that changes it, and keystrokes that
// do not leave the machine.
func TestJourney2AViewerIsToldAndTypesNothing(t *testing.T) {
	p := newFakePlane(
		terminal.ServerMessage{Type: terminal.TypeAttached, Mode: terminal.ModeView, Generation: terminal.GenOf(2)},
		terminal.ServerMessage{Type: "snapshot", Seq: 1, Data: []byte("screen")})
	run := runAgainst(t, p, Options{Control: true, Mode: terminal.ModeControl})
	run.awaitPrinted(t, NoticeViewing)

	if _, err := run.stdin.Write([]byte("rm -rf /\r")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	for _, m := range p.received() {
		if m.Type == "stdin" {
			t.Fatalf("a viewer sent stdin: %q", m.Data)
		}
	}
	out := run.detach(t)
	if out.Mode != terminal.ModeView || out.Generation != 2 {
		t.Fatalf("outcome = %s at %d, want view at 2", out.Mode, out.Generation)
	}
}

// TestTheTakeKeyClaimsFromTheGenerationItWasTold is the one key binding: it
// asks, from the generation this device actually saw, and it asks once.
func TestTheTakeKeyClaimsFromTheGenerationItWasTold(t *testing.T) {
	p := newFakePlane(
		terminal.ServerMessage{Type: terminal.TypeAttached, Mode: terminal.ModeView, Generation: terminal.GenOf(5)},
		terminal.ServerMessage{Type: "snapshot", Seq: 1, Data: []byte("screen")})
	run := runAgainst(t, p, Options{Control: true, Mode: terminal.ModeControl})
	run.awaitPrinted(t, NoticeViewing)

	if _, err := run.stdin.Write([]byte{takeKey}); err != nil {
		t.Fatal(err)
	}
	claim := p.awaitFrame(t, func(m terminal.ClientMessage) bool { return m.Type == terminal.TypeClaim })
	if claim.Expected.Value() != 5 {
		t.Fatalf("claimed from %q, want the 5 it was told about", claim.Expected)
	}

	// The plane grants it, and from here what this device types goes out
	// stamped with the generation it holds.
	p.extra <- terminal.ServerMessage{
		Type: terminal.TypeAttached, Mode: terminal.ModeControl, Generation: terminal.GenOf(6)}
	run.awaitPrinted(t, NoticeHaveControl)
	if _, err := run.stdin.Write([]byte("ls\r")); err != nil {
		t.Fatal(err)
	}
	stdin := p.awaitFrame(t, func(m terminal.ClientMessage) bool { return m.Type == "stdin" })
	if stdin.Generation.Value() != 6 {
		t.Fatalf("stdin was stamped %q, want 6", stdin.Generation)
	}
	out := run.detach(t)
	if out.Mode != terminal.ModeControl || out.Generation != 6 {
		t.Fatalf("outcome = %s at %d, want control at 6", out.Mode, out.Generation)
	}
}

// TestJourney4TheDisplacedControllerIsToldAndStopsTyping is the laptop's side
// of a take-over: one line, and it keeps showing output while sending none.
func TestJourney4TheDisplacedControllerIsToldAndStopsTyping(t *testing.T) {
	p := newFakePlane(
		terminal.ServerMessage{Type: terminal.TypeAttached, Mode: terminal.ModeControl, Generation: terminal.GenOf(1)},
		terminal.ServerMessage{Type: "snapshot", Seq: 1, Data: []byte("screen")})
	run := runAgainst(t, p, Options{Control: true, Mode: terminal.ModeControl})

	p.extra <- terminal.ServerMessage{
		Type: terminal.TypeControlChanged, Mode: terminal.ModeView, Generation: terminal.GenOf(2)}
	run.awaitPrinted(t, NoticeTaken)

	before := len(p.received())
	if _, err := run.stdin.Write([]byte("y\r")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	for _, m := range p.received()[before:] {
		if m.Type == "stdin" {
			t.Fatal("a displaced controller kept typing")
		}
	}
	// Output still arrives: it dropped to viewer, it did not detach.
	p.extra <- terminal.ServerMessage{Type: "output", Seq: 2, Data: []byte("still watching")}
	run.awaitPrinted(t, "still watching")
	run.detach(t)
}

// TestALostClaimSaysSoAndDoesNotRetry pins the absence of the loop. The
// client is told it lost, it says so once, and it sends nothing further
// unless a person presses the key again.
func TestALostClaimSaysSoAndDoesNotRetry(t *testing.T) {
	p := newFakePlane(
		terminal.ServerMessage{Type: terminal.TypeAttached, Mode: terminal.ModeView, Generation: terminal.GenOf(3)},
		terminal.ServerMessage{Type: "snapshot", Seq: 1, Data: []byte("screen")})
	run := runAgainst(t, p, Options{Control: true, Mode: terminal.ModeControl, Take: true})
	// --take spends its one claim on its own.
	p.awaitFrame(t, func(m terminal.ClientMessage) bool { return m.Type == terminal.TypeClaim })

	p.extra <- terminal.ServerMessage{Type: terminal.TypeStale, Generation: terminal.GenOf(4)}
	run.awaitPrinted(t, NoticeStale)

	time.Sleep(200 * time.Millisecond)
	claims := 0
	for _, m := range p.received() {
		if m.Type == terminal.TypeClaim {
			claims++
		}
	}
	if claims != 1 {
		t.Fatalf("%d claims were sent; --take asks once and never in answer to a refusal", claims)
	}
	out := run.detach(t)
	if out.Generation != 4 {
		t.Fatalf("outcome generation = %d, want the 4 the refusal named", out.Generation)
	}
}

// TestNewClientOldPlane is the compatibility pairing that matters most to an
// installed CLI: it asks, nothing answers, and it behaves exactly as it
// always has — it types, it stamps nothing, and Ctrl-\ is a byte for the
// remote application rather than a key this client eats.
func TestNewClientOldPlane(t *testing.T) {
	p := newFakePlane(terminal.ServerMessage{Type: "snapshot", Seq: 1, Data: []byte("screen")})
	run := runAgainst(t, p, Options{Control: true, Mode: terminal.ModeControl})

	if _, err := run.stdin.Write([]byte{takeKey}); err != nil {
		t.Fatal(err)
	}
	if _, err := run.stdin.Write([]byte("ls\r")); err != nil {
		t.Fatal(err)
	}
	var typed []byte
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		typed = nil
		for _, m := range p.received() {
			if m.Type == "stdin" {
				if m.Generation != "" {
					t.Fatalf("an unnegotiated attach stamped a generation: %q", m.Generation)
				}
				typed = append(typed, m.Data...)
			}
			if m.Type == terminal.TypeClaim {
				t.Fatal("a client that was never answered claimed control anyway")
			}
		}
		if bytes.Contains(typed, []byte{takeKey}) && bytes.Contains(typed, []byte("ls\r")) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !bytes.Contains(typed, []byte{takeKey}) {
		t.Fatalf("Ctrl-\\ was swallowed against a plane that never negotiated; the remote application never saw it (got %q)", typed)
	}
	// The screen it painted, and not one word about ownership: there is
	// nothing to say when nothing was negotiated.
	for _, notice := range []string{NoticeViewing, NoticeTaken, NoticeHaveControl, NoticeStale} {
		if strings.Contains(run.printed(), notice) {
			t.Fatalf("an unnegotiated attach printed %q", notice)
		}
	}
	out := run.detach(t)
	if out.Mode != "" || out.Generation != 0 {
		t.Fatalf("outcome = %s at %d, want an empty mode: nothing was negotiated", out.Mode, out.Generation)
	}
}

// TestScanKeysReadsTheTwoKeysThisPackageOwns keeps the pure part pure: which
// byte in a chunk this package interprets, and which it forwards.
func TestScanKeysReadsTheTwoKeysThisPackageOwns(t *testing.T) {
	for _, tc := range []struct {
		name     string
		buf      []byte
		active   bool
		wantAt   int
		wantKind controlKey
	}{
		{"ordinary bytes", []byte("ls -la\r"), true, -1, keyNone},
		{"detach", []byte{'a', detachKey, 'b'}, true, 1, keyDetach},
		{"take", []byte{'a', takeKey}, true, 1, keyTake},
		{"take is a plain byte when nothing negotiated", []byte{'a', takeKey}, false, -1, keyNone},
		{"detach wins when it comes first", []byte{detachKey, takeKey}, true, 0, keyDetach},
		{"take wins when it comes first", []byte{takeKey, detachKey}, true, 0, keyTake},
	} {
		t.Run(tc.name, func(t *testing.T) {
			at, kind := scanKeys(tc.buf, tc.active)
			if at != tc.wantAt || kind != tc.wantKind {
				t.Fatalf("scanKeys = (%d, %v), want (%d, %v)", at, kind, tc.wantAt, tc.wantKind)
			}
		})
	}
}

// TestViewNeverTypesEvenWhenNothingAnswers is --view against a plane that
// predates conditional ownership — the exact pairing the compatibility matrix
// is about, and the one where the flag is load-bearing. That plane admits
// every attach as an unconditional controller and takes control from whoever
// had it, so a client that waited for permission to be a viewer would type
// into somebody else's shell after asking not to.
//
// It is also the pre-answer window against a NEW plane: --view is the user's
// instruction, and it holds from the first byte rather than from the first
// answer.
func TestViewNeverTypesEvenWhenNothingAnswers(t *testing.T) {
	p := newFakePlane(terminal.ServerMessage{Type: "snapshot", Seq: 1, Data: []byte("screen")})
	run := runAgainst(t, p, Options{Control: true, Mode: terminal.ModeView})
	if _, err := run.stdin.Write([]byte("rm -rf /\r")); err != nil {
		t.Fatal(err)
	}
	// Give the keystroke every chance to arrive before concluding it did not.
	time.Sleep(200 * time.Millisecond)
	for _, m := range p.received() {
		if m.Type == "stdin" {
			t.Fatalf("--view typed %q into a session it asked only to watch", m.Data)
		}
	}
	out := run.detach(t)
	if out.Mode != terminal.ModeView {
		t.Fatalf("a --view attach ended in mode %q, want view", out.Mode)
	}
}

// TestTakeIsSpentByTheFirstAnswer pins --take as a flag on one attach rather
// than a standing instruction. An attach that opened holding control has
// already had what it asked for; a claim left unspent would fire minutes
// later, when somebody else takes control, as a snatch-back nobody pressed a
// key for.
func TestTakeIsSpentByTheFirstAnswer(t *testing.T) {
	p := newFakePlane(terminal.ServerMessage{
		Type: terminal.TypeAttached, Mode: terminal.ModeControl, Generation: terminal.GenOf(1)})
	run := runAgainst(t, p, Options{Control: true, Mode: terminal.ModeControl, Take: true})

	// Somebody else takes control. This client is told, and must not answer.
	p.extra <- terminal.ServerMessage{
		Type: terminal.TypeControlChanged, Mode: terminal.ModeView, Generation: terminal.GenOf(2)}
	run.awaitPrinted(t, NoticeTaken)
	time.Sleep(200 * time.Millisecond)
	for _, m := range p.received() {
		if m.Type == terminal.TypeClaim {
			t.Fatalf("--take claimed control back on its own, from generation %q", m.Expected)
		}
	}
	run.detach(t)
}

// TestGainingControlSaysHowBigThisTerminalIs is the other half of "the pty
// follows the controller". A viewer's resizes are suppressed on the way out,
// so the size the session holds for this attachment is the one it had when it
// attached — possibly several window changes ago. Taking control has to say
// what this terminal actually is, or the take-over snaps the pty to a stale
// size.
//
// It drives a real pty, because the size only exists when there is a terminal
// to measure and a pipe has none.
func TestGainingControlSaysHowBigThisTerminalIs(t *testing.T) {
	master, slave, err := pty.Open()
	if err != nil {
		t.Fatalf("pty.Open: %v", err)
	}
	defer master.Close()
	defer slave.Close()

	p := newFakePlane(terminal.ServerMessage{
		Type: terminal.TypeAttached, Mode: terminal.ModeView, Generation: terminal.GenOf(1)})
	ts := httptest.NewServer(http.HandlerFunc(p.serve))
	defer ts.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		runWithIO(context.Background(), "ws"+strings.TrimPrefix(ts.URL, "http")+"/attach",
			nil, 0, Options{Control: true, Mode: terminal.ModeControl}, slave, io.Discard)
	}()
	<-p.ready
	// Everything the attach has said while it was only watching.
	p.awaitFrame(t, func(m terminal.ClientMessage) bool { return m.Type == "resize" })
	before := len(p.received())

	p.extra <- terminal.ServerMessage{
		Type: terminal.TypeAttached, Mode: terminal.ModeControl, Generation: terminal.GenOf(2)}

	deadline := time.Now().Add(5 * time.Second)
	var sized bool
	for time.Now().Before(deadline) && !sized {
		for _, m := range p.received()[before:] {
			if m.Type == "resize" {
				sized = true
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	master.Write([]byte{detachKey})
	<-done
	if !sized {
		t.Fatal("taking control sent no size, so the pty keeps whatever this attachment had when it was watching")
	}
}

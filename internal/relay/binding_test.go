package relay

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/internal/session"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// newRelayedSession wires one session to a hub over an in-memory pair, the
// way runnerd and sessiond are wired over the real outbound conn.
func newRelayedSession(t *testing.T) (*Hub, context.Context) {
	t.Helper()
	s, err := session.New(
		session.Config{Argv: []string{"sh", "-i"}, Cols: 80, Rows: 24, LogPath: filepath.Join(t.TempDir(), "s.log")},
		session.StartProc,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Stop)
	sessConn, runConn := newPipe()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go ServeSession(ctx, sessConn, s)
	hub := NewHub(ctx, runConn)
	t.Cleanup(hub.Close)
	return hub, ctx
}

// awaitEcho reads until the session echoes want, or reports that it never did
// within a bound. It is how these tests tell "the keystroke executed" from
// "the keystroke was fenced" without reaching inside the session.
func awaitEcho(t *testing.T, c *pipeConn, want string, within time.Duration) bool {
	t.Helper()
	deadline := time.After(within)
	for {
		select {
		case <-deadline:
			return false
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), within)
		raw, err := c.Read(ctx)
		cancel()
		if err != nil {
			return false
		}
		var m terminal.ServerMessage
		if json.Unmarshal(raw, &m) != nil {
			continue
		}
		if (m.Type == "output" || m.Type == "snapshot") && contains(m.Data, want) {
			return true
		}
	}
}

// TestABoundViewersInputIsFencedThroughTheRelay walks the whole last hop: the
// binding rides the frame that opens the attachment, and a viewer's stdin is
// dropped at the process on the other side of it. The plane drops these too;
// this is the fence that still holds when one gets past it.
func TestABoundViewersInputIsFencedThroughTheRelay(t *testing.T) {
	hub, ctx := newRelayedSession(t)

	controller, hubController := newPipe()
	go hub.AttachClient(ctx, hubController, Open{Cols: 80, Rows: 24, Mode: terminal.ModeControl, Generation: 1})
	if m := readServerMsg(t, controller); m.Type != "snapshot" {
		t.Fatalf("first msg = %s, want snapshot", m.Type)
	}

	viewer, hubViewer := newPipe()
	go hub.AttachClient(ctx, hubViewer, Open{Cols: 80, Rows: 24, Mode: terminal.ModeView, Generation: 1})
	if m := readServerMsg(t, viewer); m.Type != "snapshot" {
		t.Fatalf("viewer's first msg = %s, want snapshot", m.Type)
	}

	writeClientMsg(t, viewer, terminal.ClientMessage{
		Type: "stdin", Data: []byte("echo viewer-marker\n"), Generation: terminal.GenOf(1)})
	if awaitEcho(t, viewer, "viewer-marker\r\n", time.Second) {
		t.Fatal("a viewer's keystroke was executed on the other side of the relay")
	}

	writeClientMsg(t, controller, terminal.ClientMessage{
		Type: "stdin", Data: []byte("echo controller-marker\n"), Generation: terminal.GenOf(1)})
	if !awaitEcho(t, controller, "controller-marker", 5*time.Second) {
		t.Fatal("the controller's keystroke never executed")
	}
}

// TestAMidAttachHandoffIsAcknowledged pins the acknowledgement the plane
// waits on: installing a binding on a live attachment answers with a
// control_ack naming the generation, which is what lets a plane refuse to
// tell a taker it has control until the fence protecting it is in place.
func TestAMidAttachHandoffIsAcknowledged(t *testing.T) {
	hub, ctx := newRelayedSession(t)

	laptop, hubLaptop := newPipe()
	go hub.AttachClient(ctx, hubLaptop, Open{Cols: 80, Rows: 24, Mode: terminal.ModeControl, Generation: 1})
	if m := readServerMsg(t, laptop); m.Type != "snapshot" {
		t.Fatalf("first msg = %s, want snapshot", m.Type)
	}

	phone, hubPhone := newPipe()
	go hub.AttachClient(ctx, hubPhone, Open{Cols: 80, Rows: 24, Mode: terminal.ModeView, Generation: 1})
	if m := readServerMsg(t, phone); m.Type != "snapshot" {
		t.Fatalf("viewer's first msg = %s, want snapshot", m.Type)
	}

	writeClientMsg(t, phone, terminal.ClientMessage{
		Type: terminal.TypeControl, Mode: terminal.ModeControl, Generation: terminal.GenOf(2)})
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("the handoff was never acknowledged")
		default:
		}
		m := readServerMsg(t, phone)
		if m.Type != terminal.TypeControlAck {
			continue
		}
		if m.Generation.Value() != 2 || m.Mode != terminal.ModeControl {
			t.Fatalf("ack = %s at %q, want control at 2", m.Mode, m.Generation)
		}
		break
	}

	// The laptop's next keystroke was sent under the generation it still
	// believes it holds, and is discarded at the process.
	writeClientMsg(t, laptop, terminal.ClientMessage{
		Type: "stdin", Data: []byte("echo displaced-marker\n"), Generation: terminal.GenOf(1)})
	if awaitEcho(t, laptop, "displaced-marker\r\n", time.Second) {
		t.Fatal("a displaced controller's keystroke executed")
	}
}

// TestAnOldPlanesAttachmentIsUnconditional is the new-sandbox + old-plane
// pairing over the real hop: an opening frame with no binding, and stdin with
// no generation on it, because that is all an older plane can send.
func TestAnOldPlanesAttachmentIsUnconditional(t *testing.T) {
	hub, ctx := newRelayedSession(t)

	client, hubClient := newPipe()
	go hub.AttachClient(ctx, hubClient, Open{Cols: 80, Rows: 24})
	if m := readServerMsg(t, client); m.Type != "snapshot" {
		t.Fatalf("first msg = %s, want snapshot", m.Type)
	}
	writeClientMsg(t, client, terminal.ClientMessage{Type: "stdin", Data: []byte("echo legacy-marker\n")})
	if !awaitEcho(t, client, "legacy-marker", 5*time.Second) {
		t.Fatal("an old plane's input was fenced; it carries no generation and never will")
	}
}

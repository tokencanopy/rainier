package relay

import (
	"context"
	"encoding/json"
	"testing"
)

// The bulk stream a cold suspend carries: its frame's shape on the wire, and
// the one discipline that makes it safe — it goes through the same writer as
// every control frame, so the end marker cannot overtake the chunk it ends.

// TestStreamFrameWireShape pins the frame a workspace chunk travels in. Both
// ends of this hop come from different build lineages — sessiond ships inside
// a session image, the hub runs on the host — so the type number and the field
// names are a contract, not an implementation detail.
func TestStreamFrameWireShape(t *testing.T) {
	if FrameStream != 5 {
		t.Fatalf("FrameStream is %d; renumbering it silently reroutes a tenant's workspace", FrameStream)
	}
	b, err := Encode(Frame{Type: FrameStream, AttachID: 91, Payload: []byte("abc")})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"t":5,"a":91,"p":"YWJj"}` {
		t.Fatalf("a stream frame on the wire = %s", b)
	}
	f, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if f.Type != FrameStream || f.AttachID != 91 || string(f.Payload) != "abc" {
		t.Fatalf("a stream frame decoded as %+v", f)
	}
}

// TestWorkspaceEndCarriesCountsAndNoNames. The end marker is the only thing
// that tells the host a stream is COMPLETE rather than truncated, and the only
// thing it may carry about a workspace is how much of one there was: a path is
// session content (tenancy §15.1) and this event reaches an operator's log.
func TestWorkspaceEndCarriesCountsAndNoNames(t *testing.T) {
	b, err := json.Marshal(ControlEvent{Kind: KindWorkspaceEnd, ID: 7, OK: true, Entries: 12, Bytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"kind":"workspace_end","id":7,"ok":true,"entries":12,"bytes":4096}` {
		t.Fatalf("the end marker on the wire = %s", b)
	}
	var ev ControlEvent
	if err := json.Unmarshal(b, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Entries != 12 || ev.Bytes != 4096 {
		t.Fatalf("the end marker decoded as %+v", ev)
	}
}

// TestSendStreamSharesTheControlWriter is the ordering guarantee the handshake
// rests on. A chunk and the end marker that follows it are written through one
// writer, so what the host reads is what the guest wrote, in that order.
func TestSendStreamSharesTheControlWriter(t *testing.T) {
	a, b := newPipe()
	defer a.Close()
	defer b.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sender := &ControlSender{w: newConnWriter(ctx, a)}

	end, err := json.Marshal(ControlEvent{Kind: KindWorkspaceEnd, ID: 4, OK: true, Entries: 1, Bytes: 3})
	if err != nil {
		t.Fatal(err)
	}
	if err := sender.SendStream(4, []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := sender.Send(end); err != nil {
		t.Fatal(err)
	}

	first := readFrame(t, b)
	if first.Type != FrameStream || first.AttachID != 4 || string(first.Payload) != "abc" {
		t.Fatalf("the first frame is %+v, want the chunk", first)
	}
	second := readFrame(t, b)
	if second.Type != FrameControl || second.AttachID != 0 {
		t.Fatalf("the second frame is %+v, want the end marker on the control channel", second)
	}
	var ev ControlEvent
	if err := json.Unmarshal(second.Payload, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Kind != KindWorkspaceEnd || ev.ID != 4 {
		t.Fatalf("the second frame carries %+v, want the workspace end marker for suspend 4", ev)
	}
}

func readFrame(t *testing.T, c Conn) Frame {
	t.Helper()
	raw, err := c.Read(context.Background())
	if err != nil {
		t.Fatalf("reading a frame: %v", err)
	}
	f, err := Decode(raw)
	if err != nil {
		t.Fatalf("decoding a frame: %v", err)
	}
	return f
}

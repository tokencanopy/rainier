package relay

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/tokencanopy/rainier/protocol/runner"
)

func TestFrameRoundTrip(t *testing.T) {
	cases := []Frame{
		{Type: FrameOpen, AttachID: 7, Since: 3, Cols: 80, Rows: 24},
		{Type: FrameClient, AttachID: 7, Payload: []byte(`{"type":"stdin","data":"aGk="}`)},
		{Type: FrameServer, AttachID: 7, Payload: []byte(`{"type":"output","seq":9}`)},
		{Type: FrameClose, AttachID: 7},
	}
	for _, in := range cases {
		b, err := Encode(in)
		if err != nil {
			t.Fatal(err)
		}
		out, err := Decode(b)
		if err != nil {
			t.Fatal(err)
		}
		if out.Type != in.Type || out.AttachID != in.AttachID || out.Since != in.Since ||
			out.Cols != in.Cols || out.Rows != in.Rows || !bytes.Equal(out.Payload, in.Payload) {
			t.Fatalf("round trip: got %+v want %+v", out, in)
		}
	}
}

// TestControlEventWireShape pins the exact JSON of the three shapes that now
// share ControlEvent — the fire-and-forget event, the request, the response —
// because they are what a sessiond and a runnerd of different builds agree on
// across a live conn. The omitempty half is the load-bearing one: a Plan 4
// peer must keep seeing byte-identical setup events after the RPC fields were
// added, so an event must never start carrying an empty id/ok/payload/stage.
func TestControlEventWireShape(t *testing.T) {
	ev, err := json.Marshal(ControlEvent{Kind: "setup_done"})
	if err != nil {
		t.Fatal(err)
	}
	if string(ev) != `{"kind":"setup_done"}` {
		t.Fatalf("plain event on the wire = %s, want {\"kind\":\"setup_done\"}", ev)
	}
	for _, tag := range []string{"id", "ok", "payload", "stage", "rc", "tail", "live", "seq"} {
		if strings.Contains(string(ev), `"`+tag+`"`) {
			t.Fatalf("empty event leaked %q: %s", tag, ev)
		}
	}

	req, err := json.Marshal(ControlEvent{Kind: "req:mint_git_credential", ID: 3, Payload: json.RawMessage(`{"host":"github.com"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if string(req) != `{"kind":"req:mint_git_credential","id":3,"payload":{"host":"github.com"}}` {
		t.Fatalf("request on the wire = %s", req)
	}

	resp, err := json.Marshal(ControlEvent{Kind: "resp", ID: 3, OK: true, Payload: json.RawMessage(`{"token":"x"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if string(resp) != `{"kind":"resp","id":3,"ok":true,"payload":{"token":"x"}}` {
		t.Fatalf("response on the wire = %s", resp)
	}

	// Stage rides the same struct (T7's stage_failed); pin its tag now so the
	// field it is spelled with cannot drift before the task that sends it.
	fail, err := json.Marshal(ControlEvent{Kind: "stage_failed", Stage: "clone", RC: 128, Tail: "fatal: repo not found"})
	if err != nil {
		t.Fatal(err)
	}
	if string(fail) != `{"kind":"stage_failed","stage":"clone","rc":128,"tail":"fatal: repo not found"}` {
		t.Fatalf("stage failure on the wire = %s", fail)
	}

	// The live-exec count, whose ZERO is its most important value: it is what
	// starts the idle clock when the last command ends. `live` is omitempty,
	// so that report puts no `live` on the wire at all and decodes back to the
	// 0 that was meant — the same trick, and the same one-line justification,
	// as a clean child exit's rc.
	busy, err := json.Marshal(ControlEvent{Kind: KindExecCount, Live: 2, Seq: 7})
	if err != nil {
		t.Fatal(err)
	}
	if string(busy) != `{"kind":"exec_count","live":2,"seq":7}` {
		t.Fatalf("exec count on the wire = %s", busy)
	}
	idle, err := json.Marshal(ControlEvent{Kind: KindExecCount, Live: 0, Seq: 8})
	if err != nil {
		t.Fatal(err)
	}
	if string(idle) != `{"kind":"exec_count","seq":8}` {
		t.Fatalf("an empty exec count on the wire = %s", idle)
	}
	var lastOne ControlEvent
	if err := json.Unmarshal(idle, &lastOne); err != nil {
		t.Fatal(err)
	}
	if lastOne.Kind != KindExecCount || lastOne.Live != 0 || lastOne.Seq != 8 {
		t.Fatalf("the last exec ending round-tripped to %+v", lastOne)
	}

	var back ControlEvent
	if err := json.Unmarshal(resp, &back); err != nil {
		t.Fatal(err)
	}
	if back.Kind != "resp" || back.ID != 3 || !back.OK || string(back.Payload) != `{"token":"x"}` {
		t.Fatalf("response round trip mangled: %+v", back)
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	if _, err := Decode([]byte("not json")); err == nil {
		t.Fatal("expected error decoding garbage")
	}
}

// TestAnUnboundFrameIsTheBytesItAlwaysWas is the cross-version promise on
// this hop, which is the one between runnerd and sessiond — two halves that
// ship in different artifacts and roll on different days. The binding is
// additive, so a peer that sets neither field writes exactly the bytes it
// wrote before the field existed, and an older peer reading a newer one's
// frame sees only members it already knows.
func TestAnUnboundFrameIsTheBytesItAlwaysWas(t *testing.T) {
	raw, err := Encode(Frame{Type: FrameOpen, AttachID: 7, Since: 3, Cols: 80, Rows: 24})
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"t":0,"a":7,"s":3,"c":80,"r":24}`
	if string(raw) != want {
		t.Fatalf("an unbound open frame = %s, want %s", raw, want)
	}

	// And the binding round-trips when it IS set, which is the other half:
	// an omitempty that never carried its value would fence nobody.
	bound, err := Encode(Frame{Type: FrameOpen, AttachID: 7, Cols: 80, Rows: 24,
		Mode: "control", Gen: 4})
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(bound)
	if err != nil {
		t.Fatal(err)
	}
	if got.Mode != "control" || got.Gen != 4 {
		t.Fatalf("decoded binding = %q at %d, want control at 4", got.Mode, got.Gen)
	}
}

// TestTerminalFrameWireShape is the exec half of the promise above. sessiond
// ships in the session image and a session keeps the one it booted with for
// life, so a TERMINAL frame's bytes are read by builds that predate exec by
// months: they must not change, and the two fields exec adds are omitempty
// exactly so they do not.
func TestTerminalFrameWireShape(t *testing.T) {
	for _, f := range []Frame{
		{Type: FrameOpen, AttachID: 7, Since: 3, Cols: 80, Rows: 24},
		{Type: FrameOpen, AttachID: 7, Cols: 80, Rows: 24, Mode: "control", Gen: 4},
		{Type: FrameClient, AttachID: 7, Payload: []byte(`{"type":"stdin"}`)},
		{Type: FrameServer, AttachID: 7, Payload: []byte(`{"type":"output"}`)},
		{Type: FrameClose, AttachID: 7},
		{Type: FrameControl, Payload: []byte(`{"kind":"setup_done"}`)},
	} {
		raw, err := Encode(f)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), `"k"`) || strings.Contains(string(raw), `"x"`) {
			t.Fatalf("a terminal frame leaked an exec field: %s", raw)
		}
	}
}

// TestExecFrameWireShape pins the bytes an exec open actually writes, and
// that the spec round-trips through the hop unchanged — the frame is the last
// place the command could be mangled before the sandbox validates it.
func TestExecFrameWireShape(t *testing.T) {
	raw, err := Encode(Frame{Type: FrameOpen, AttachID: 7, Cols: 80, Rows: 24,
		Kind: runner.KindExec, Exec: &runner.ExecSpec{Argv: []string{"git", "status"}, TTY: true}})
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"t":0,"a":7,"c":80,"r":24,"k":"exec","x":{"argv":["git","status"],"tty":true}}`
	if string(raw) != want {
		t.Fatalf("an exec open frame = %s\nwant %s", raw, want)
	}

	got, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != runner.KindExec || got.Exec == nil ||
		len(got.Exec.Argv) != 2 || got.Exec.Argv[1] != "status" || !got.Exec.TTY {
		t.Fatalf("decoded exec open = %+v", got)
	}
}

// TestAnExecFrameOnAnOldDecoderIsATerminalOpen is the third row of the
// compatibility matrix, at the hop where it happens: a sessiond that predates
// the Kind field decodes an exec FrameOpen into a frame with no kind — an
// ordinary terminal open — because unknown JSON keys are dropped. That is
// exactly why the plane requires a positive exec_started before it forwards a
// byte, and this test exists so the claim is checked rather than asserted.
func TestAnExecFrameOnAnOldDecoderIsATerminalOpen(t *testing.T) {
	raw, err := Encode(Frame{Type: FrameOpen, AttachID: 7, Cols: 80, Rows: 24,
		Kind: runner.KindExec, Exec: &runner.ExecSpec{Argv: []string{"git"}}})
	if err != nil {
		t.Fatal(err)
	}
	// oldFrame is the struct as it was before exec: the same tags, without
	// the two this change added.
	var old struct {
		Type     FrameType `json:"t"`
		AttachID uint64    `json:"a"`
		Since    uint64    `json:"s,omitempty"`
		Cols     int       `json:"c,omitempty"`
		Rows     int       `json:"r,omitempty"`
		Mode     string    `json:"m,omitempty"`
		Gen      uint64    `json:"g,omitempty"`
		Payload  []byte    `json:"p,omitempty"`
	}
	if err := json.Unmarshal(raw, &old); err != nil {
		t.Fatalf("an old build could not decode an exec frame at all: %v", err)
	}
	if old.Type != FrameOpen || old.AttachID != 7 || old.Cols != 80 || old.Rows != 24 {
		t.Fatalf("an old build decoded an exec open as %+v", old)
	}
}

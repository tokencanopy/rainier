package terminal_test

import (
	"encoding/json"
	"testing"

	"github.com/tokencanopy/rainier/protocol/terminal"
)

// TestSinceAllIsReservedMaximum pins the cursor sentinel: the whole-log
// attach cursor is the maximum uint64, never 0, because 0 already means "no
// cursor, paint me a screen" and cannot carry a second meaning across the
// relay's `s,omitempty` frame.
func TestSinceAllIsReservedMaximum(t *testing.T) {
	if terminal.SinceAll != ^uint64(0) {
		t.Fatalf("SinceAll = %d, want max uint64", terminal.SinceAll)
	}
}

// TestResizeWireShape pins the exact bytes of a resize: no trailing data
// field, only the three fields a resize carries.
func TestResizeWireShape(t *testing.T) {
	b, err := json.Marshal(terminal.ClientMessage{Type: "resize", Cols: 80, Rows: 24})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"type":"resize","cols":80,"rows":24}` {
		t.Fatalf("resize JSON = %s", b)
	}
}

// TestStdinWireShape pins the exact bytes of a stdin frame, including the
// base64 encoding of the data field ([]byte always encodes as base64).
func TestStdinWireShape(t *testing.T) {
	b, err := json.Marshal(terminal.ClientMessage{Type: "stdin", Data: []byte("echo hello\n")})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"type":"stdin","data":"ZWNobyBoZWxsbwo="}` {
		t.Fatalf("stdin JSON = %s", b)
	}
}

// TestSnapshotWireShape pins the exact bytes of a snapshot: seq, the base64
// data field, and the screen size cols/rows.
func TestSnapshotWireShape(t *testing.T) {
	b, err := json.Marshal(terminal.ServerMessage{
		Type: "snapshot", Seq: 1, Data: []byte("abc"), Cols: 80, Rows: 24,
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"type":"snapshot","seq":1,"data":"YWJj","cols":80,"rows":24}` {
		t.Fatalf("snapshot JSON = %s", b)
	}
}

// TestOutputWireShape pins the exact bytes of an output frame: seq and the
// base64 data field, with cols/rows omitted because output carries no size.
func TestOutputWireShape(t *testing.T) {
	b, err := json.Marshal(terminal.ServerMessage{Type: "output", Seq: 17, Data: []byte("xyz")})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"type":"output","seq":17,"data":"eHl6"}` {
		t.Fatalf("output JSON = %s", b)
	}
}

// TestExitWireShape pins the exact bytes of an exit frame, including the
// camel-case exitCode tag that is part of the wire contract.
func TestExitWireShape(t *testing.T) {
	b, err := json.Marshal(terminal.ServerMessage{Type: "exit", ExitCode: 7})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"type":"exit","exitCode":7}` {
		t.Fatalf("exit JSON = %s", b)
	}
}

// TestServerMessageRoundTrip pins that a message survives a marshal/unmarshal
// cycle with its binary data intact.
func TestServerMessageRoundTrip(t *testing.T) {
	in := terminal.ServerMessage{Type: "output", Seq: 7, Data: []byte{0x1b, '[', 'H'}}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out terminal.ServerMessage
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Type != "output" || out.Seq != 7 || string(out.Data) != "\x1b[H" {
		t.Fatalf("round trip = %+v", out)
	}
}

// TestUnknownFieldsTolerated pins forward compatibility: an older side must
// not choke on fields a newer side adds.
func TestUnknownFieldsTolerated(t *testing.T) {
	var m terminal.ServerMessage
	if err := json.Unmarshal([]byte(`{"type":"output","seq":1,"data":"eA==","future_field":1}`), &m); err != nil {
		t.Fatalf("unknown field should be ignored: %v", err)
	}
	if m.Seq != 1 || string(m.Data) != "x" {
		t.Fatalf("message mangled: %+v", m)
	}
}

// TestUnnegotiatedMessagesAreByteIdentical is the compatibility promise in
// its most direct form: a peer that uses none of the conditional-ownership
// fields must put exactly the same bytes on the wire it always has. Every
// field this task added is omitempty for this reason, and a future one that
// forgets fails here rather than in somebody's terminal.
func TestUnnegotiatedMessagesAreByteIdentical(t *testing.T) {
	for _, tc := range []struct {
		name string
		msg  any
		want string
	}{
		{"stdin", terminal.ClientMessage{Type: "stdin", Data: []byte("hi")}, `{"type":"stdin","data":"aGk="}`},
		{"resize", terminal.ClientMessage{Type: "resize", Cols: 80, Rows: 24}, `{"type":"resize","cols":80,"rows":24}`},
		{"snapshot", terminal.ServerMessage{Type: "snapshot", Seq: 3, Data: []byte("s"), Cols: 80, Rows: 24},
			`{"type":"snapshot","seq":3,"data":"cw==","cols":80,"rows":24}`},
		{"output", terminal.ServerMessage{Type: "output", Seq: 4, Data: []byte("o")}, `{"type":"output","seq":4,"data":"bw=="}`},
		{"exit", terminal.ServerMessage{Type: "exit", ExitCode: 2}, `{"type":"exit","exitCode":2}`},
	} {
		b, err := json.Marshal(tc.msg)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if string(b) != tc.want {
			t.Errorf("%s = %s, want %s", tc.name, b, tc.want)
		}
	}
}

// TestGenIsADecimalString pins the one thing about generations that a browser
// would otherwise get silently wrong: they are decimal strings, exact past
// 2^53, and anything unparseable reads as zero rather than as an error nobody
// could act on.
func TestGenIsADecimalString(t *testing.T) {
	if got := terminal.GenOf(0); got != "" {
		t.Errorf("GenOf(0) = %q, want the empty string so it leaves the wire entirely", got)
	}
	const big = uint64(1) << 60
	g := terminal.GenOf(big)
	if g != "1152921504606846976" {
		t.Errorf("GenOf(2^60) = %q", g)
	}
	if got := g.Value(); got != big {
		t.Errorf("round trip = %d, want %d", got, big)
	}
	for _, bad := range []terminal.Gen{"", "x", "-1", "1.0", " 1"} {
		if got := bad.Value(); got != 0 {
			t.Errorf("Gen(%q).Value() = %d, want 0", bad, got)
		}
	}
	b, err := json.Marshal(terminal.ServerMessage{Type: terminal.TypeAttached, Mode: terminal.ModeControl, Generation: terminal.GenOf(3)})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"type":"attached","mode":"control","gen":"3"}`; string(b) != want {
		t.Errorf("attached = %s, want %s", b, want)
	}
}

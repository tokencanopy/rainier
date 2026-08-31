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

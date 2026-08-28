package wire

import (
	"encoding/json"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	in := ServerMsg{Type: "output", Seq: 7, Data: []byte{0x1b, '[', 'H'}}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out ServerMsg
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Type != "output" || out.Seq != 7 || string(out.Data) != "\x1b[H" {
		t.Fatalf("round trip = %+v", out)
	}
}

func TestClientMsgOmitsEmpty(t *testing.T) {
	b, _ := json.Marshal(ClientMsg{Type: "resize", Cols: 80, Rows: 24})
	s := string(b)
	if s != `{"type":"resize","cols":80,"rows":24}` {
		t.Fatalf("marshal = %s", s)
	}
}

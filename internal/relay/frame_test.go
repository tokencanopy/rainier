package relay

import (
	"bytes"
	"testing"
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

func TestDecodeRejectsGarbage(t *testing.T) {
	if _, err := Decode([]byte("not json")); err == nil {
		t.Fatal("expected error decoding garbage")
	}
}

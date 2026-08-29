package relay

import (
	"bytes"
	"encoding/json"
	"strings"
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

// TestControlEventWireShape pins the exact JSON of the three shapes that now
// share ControlEvent — the fire-and-forget event, the request, the response —
// because they are what a sessiond and a runnerd of different builds agree on
// across a live conn. The omitempty half is the load-bearing one: a Plan 4
// peer must keep seeing byte-identical setup events after the RPC fields were
// added, so an event must never start carrying an empty id/ok/payload/stage.
func TestControlEventWireShape(t *testing.T) {
	ev, err := json.Marshal(ControlEvent{Kind: "setup_done"})
	if err != nil { t.Fatal(err) }
	if string(ev) != `{"kind":"setup_done"}` {
		t.Fatalf("plain event on the wire = %s, want {\"kind\":\"setup_done\"}", ev)
	}
	for _, tag := range []string{"id", "ok", "payload", "stage", "rc", "tail"} {
		if strings.Contains(string(ev), `"`+tag+`"`) { t.Fatalf("empty event leaked %q: %s", tag, ev) }
	}

	req, err := json.Marshal(ControlEvent{Kind: "req:mint_git_credential", ID: 3, Payload: json.RawMessage(`{"host":"github.com"}`)})
	if err != nil { t.Fatal(err) }
	if string(req) != `{"kind":"req:mint_git_credential","id":3,"payload":{"host":"github.com"}}` {
		t.Fatalf("request on the wire = %s", req)
	}

	resp, err := json.Marshal(ControlEvent{Kind: "resp", ID: 3, OK: true, Payload: json.RawMessage(`{"token":"x"}`)})
	if err != nil { t.Fatal(err) }
	if string(resp) != `{"kind":"resp","id":3,"ok":true,"payload":{"token":"x"}}` {
		t.Fatalf("response on the wire = %s", resp)
	}

	// Stage rides the same struct (T7's stage_failed); pin its tag now so the
	// field it is spelled with cannot drift before the task that sends it.
	fail, err := json.Marshal(ControlEvent{Kind: "stage_failed", Stage: "clone", RC: 128, Tail: "fatal: repo not found"})
	if err != nil { t.Fatal(err) }
	if string(fail) != `{"kind":"stage_failed","stage":"clone","rc":128,"tail":"fatal: repo not found"}` {
		t.Fatalf("stage failure on the wire = %s", fail)
	}

	var back ControlEvent
	if err := json.Unmarshal(resp, &back); err != nil { t.Fatal(err) }
	if back.Kind != "resp" || back.ID != 3 || !back.OK || string(back.Payload) != `{"token":"x"}` {
		t.Fatalf("response round trip mangled: %+v", back)
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	if _, err := Decode([]byte("not json")); err == nil {
		t.Fatal("expected error decoding garbage")
	}
}

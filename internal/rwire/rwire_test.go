package rwire

import (
	"encoding/json"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	in := ToRunner{Type: "create", ReqID: 7, Session: "sess_ab12",
		Spec: &Spec{Image: "img", Cmd: []string{"bash"}, EgressAllow: []string{"example.com"}}}
	b, err := json.Marshal(in)
	if err != nil { t.Fatal(err) }
	var out ToRunner
	if err := json.Unmarshal(b, &out); err != nil { t.Fatal(err) }
	if out.ReqID != 7 || out.Spec == nil || out.Spec.Image != "img" {
		t.Fatalf("round trip mangled: %+v", out)
	}
}

func TestUnknownFieldsTolerated(t *testing.T) {
	// Forward compatibility: an older side must not choke on new fields.
	var m FromRunner
	if err := json.Unmarshal([]byte(`{"type":"event","session":"s","state":"running","future_field":1}`), &m); err != nil {
		t.Fatalf("unknown field should be ignored: %v", err)
	}
	if m.State != "running" { t.Fatalf("state lost: %+v", m) }
}

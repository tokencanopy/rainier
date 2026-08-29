package rwire

import (
	"encoding/json"
	"strings"
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

// TestEnvironmentSpecRoundTrip covers the environment vocabulary: the setup
// script and its timeout, the env map, and the content-addressed Ref that
// snapshot/prepull commands carry. The literal JSON assertion pins the wire
// tag names, which both ends (controld, runnerd) and any future non-Go peer
// depend on being stable.
func TestEnvironmentSpecRoundTrip(t *testing.T) {
	in := ToRunner{Type: "create", ReqID: 11, Session: "sess_cd34",
		Spec: &Spec{Image: "img", Setup: "apt-get install -y jq", SetupTimeoutSec: 900,
			Env: map[string]string{"FOO": "bar"}}}
	b, err := json.Marshal(in)
	if err != nil { t.Fatal(err) }
	if got := string(b); !strings.Contains(got, `"setup":"apt-get install -y jq"`) ||
		!strings.Contains(got, `"setup_timeout_sec":900`) || !strings.Contains(got, `"env":{"FOO":"bar"}`) {
		t.Fatalf("spec tags wrong on the wire: %s", got)
	}
	var out ToRunner
	if err := json.Unmarshal(b, &out); err != nil { t.Fatal(err) }
	if out.Spec == nil || out.Spec.Setup != in.Spec.Setup || out.Spec.SetupTimeoutSec != 900 ||
		out.Spec.Env["FOO"] != "bar" {
		t.Fatalf("round trip mangled: %+v", out.Spec)
	}

	pre := ToRunner{Type: "prepull", Ref: "rainier-env:env_ab12-0123456789ab"}
	pb, err := json.Marshal(pre)
	if err != nil { t.Fatal(err) }
	if !strings.Contains(string(pb), `"ref":"rainier-env:env_ab12-0123456789ab"`) {
		t.Fatalf("ref tag wrong on the wire: %s", pb)
	}
	var pout ToRunner
	if err := json.Unmarshal(pb, &pout); err != nil { t.Fatal(err) }
	if pout.Type != "prepull" || pout.Ref != pre.Ref { t.Fatalf("round trip mangled: %+v", pout) }

	// An empty Spec must not start emitting the new fields: omitempty keeps
	// a plain scratch create byte-identical to what Plan 1-3 peers expect.
	eb, err := json.Marshal(ToRunner{Type: "create", Spec: &Spec{Image: "img"}})
	if err != nil { t.Fatal(err) }
	for _, tag := range []string{"setup", "setup_timeout_sec", "env", "ref"} {
		if strings.Contains(string(eb), `"`+tag+`"`) { t.Fatalf("empty spec leaked %q: %s", tag, eb) }
	}
}

// TestSetupEventDecodes pins the event vocabulary the setup pipeline adds:
// runnerd reports setup outcomes as ordinary FromRunner events, with the
// failure tail in Detail.
func TestSetupEventDecodes(t *testing.T) {
	var m FromRunner
	raw := `{"type":"event","session":"sess_ef56","state":"setup_failed","detail":"exit 1: no such package"}`
	if err := json.Unmarshal([]byte(raw), &m); err != nil { t.Fatal(err) }
	if m.State != "setup_failed" || m.Detail != "exit 1: no such package" {
		t.Fatalf("setup event mangled: %+v", m)
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

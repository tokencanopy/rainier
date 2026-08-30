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
	for _, tag := range []string{"setup", "setup_timeout_sec", "env", "ref", "rpc"} {
		if strings.Contains(string(eb), `"`+tag+`"`) { t.Fatalf("empty spec leaked %q: %s", tag, eb) }
	}
}

// TestSessionRPCRoundTrip pins the session-RPC vocabulary runnerd forwards in
// both directions: a controld-initiated "session_rpc" going down, a
// sessiond-initiated "session_req" coming up, and the response to that one
// going back down as another "session_rpc" whose Method is "resp". The
// literal JSON assertions are the point — runnerd is a pure forwarder here, so
// a renamed tag would not fail any forwarding test, it would just quietly
// deliver an RPC with no method to the far end.
func TestSessionRPCRoundTrip(t *testing.T) {
	down := ToRunner{Type: "session_rpc", Session: "sess_ab12",
		RPC: &RPCEnvelope{ID: 42, Method: "diff", Payload: json.RawMessage(`{"repo":"api"}`)}}
	db, err := json.Marshal(down)
	if err != nil { t.Fatal(err) }
	if got := string(db); !strings.Contains(got, `"type":"session_rpc"`) ||
		!strings.Contains(got, `"rpc":{"id":42,"method":"diff","payload":{"repo":"api"}}`) {
		t.Fatalf("session_rpc wrong on the wire: %s", got)
	}
	var dout ToRunner
	if err := json.Unmarshal(db, &dout); err != nil { t.Fatal(err) }
	if dout.Type != "session_rpc" || dout.Session != "sess_ab12" || dout.RPC == nil ||
		dout.RPC.ID != 42 || dout.RPC.Method != "diff" || string(dout.RPC.Payload) != `{"repo":"api"}` {
		t.Fatalf("round trip mangled: %+v", dout.RPC)
	}

	// Upward: the sandbox asks for a credential. No payload — a method whose
	// arguments are all implied by the session it came from must still put an
	// id and a method on the wire, since the id is what the response is
	// correlated against.
	up := FromRunner{Type: "session_req", Session: "sess_ab12",
		RPC: &RPCEnvelope{ID: 7, Method: "mint_git_credential"}}
	ub, err := json.Marshal(up)
	if err != nil { t.Fatal(err) }
	if got := string(ub); !strings.Contains(got, `"type":"session_req"`) ||
		!strings.Contains(got, `"rpc":{"id":7,"method":"mint_git_credential"}`) {
		t.Fatalf("session_req wrong on the wire: %s", got)
	}
	var uout FromRunner
	if err := json.Unmarshal(ub, &uout); err != nil { t.Fatal(err) }
	if uout.Type != "session_req" || uout.RPC == nil || uout.RPC.ID != 7 ||
		uout.RPC.Method != "mint_git_credential" || uout.RPC.Payload != nil {
		t.Fatalf("round trip mangled: %+v", uout.RPC)
	}

	// The answer to that request travels back as a session_rpc whose Method is
	// "resp", echoing the request's id.
	answer := ToRunner{Type: "session_rpc", Session: "sess_ab12",
		RPC: &RPCEnvelope{ID: 7, Method: "resp", Payload: json.RawMessage(`{"ok":true}`)}}
	ab, err := json.Marshal(answer)
	if err != nil { t.Fatal(err) }
	if !strings.Contains(string(ab), `"rpc":{"id":7,"method":"resp","payload":{"ok":true}}`) {
		t.Fatalf("session_rpc response wrong on the wire: %s", ab)
	}

	// A response's verdict rides the envelope, in both directions, because
	// runnerd reproduces the relay ControlEvent at the far end from the
	// envelope alone — it never opens Payload to find out how a call went.
	// False is the zero value and stays off the wire (the safe direction: a
	// peer that fails to decode it reads a failure, never a spurious success),
	// so only an ok:true response carries the tag.
	okAnswer, err := json.Marshal(FromRunner{Type: "session_req", Session: "sess_ab12",
		RPC: &RPCEnvelope{ID: 7, Method: "resp", OK: true, Payload: json.RawMessage(`{"token":"x"}`)}})
	if err != nil { t.Fatal(err) }
	if !strings.Contains(string(okAnswer), `"rpc":{"id":7,"method":"resp","ok":true,"payload":{"token":"x"}}`) {
		t.Fatalf("ok response wrong on the wire: %s", okAnswer)
	}
	var okOut FromRunner
	if err := json.Unmarshal(okAnswer, &okOut); err != nil { t.Fatal(err) }
	if okOut.RPC == nil || !okOut.RPC.OK { t.Fatalf("round trip lost the verdict: %+v", okOut.RPC) }
	failed, err := json.Marshal(ToRunner{Type: "session_rpc", Session: "sess_ab12",
		RPC: &RPCEnvelope{ID: 7, Method: "resp"}})
	if err != nil { t.Fatal(err) }
	if strings.Contains(string(failed), `"ok"`) {
		t.Fatalf("a failed response leaked an ok tag: %s", failed)
	}

	// A message with no RPC must not start carrying an empty envelope: every
	// Plan 1-4 message type keeps its exact bytes.
	for _, m := range []any{ToRunner{Type: "destroy", Session: "s"}, FromRunner{Type: "event", Session: "s", State: "running"}} {
		b, err := json.Marshal(m)
		if err != nil { t.Fatal(err) }
		if strings.Contains(string(b), `"rpc"`) { t.Fatalf("non-RPC message leaked rpc: %s", b) }
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

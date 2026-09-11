package runner_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/tokencanopy/rainier/protocol/runner"
)

func TestPublicRunnerWireShapes(t *testing.T) {
	create := runner.ToRunner{
		Type: "create", ReqID: 7, Session: "sess_example",
		Spec: &runner.Spec{Image: "example.invalid/agent@sha256:0000", Cmd: []string{"bash"}},
	}
	got, err := json.Marshal(create)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"type":"create","req_id":7,"session":"sess_example","spec":{"image":"example.invalid/agent@sha256:0000","cmd":["bash"]}}`
	if string(got) != want {
		t.Fatalf("create JSON = %s, want %s", got, want)
	}
	if runner.ProtocolVersion != 1 {
		t.Fatalf("protocol version = %d, want 1", runner.ProtocolVersion)
	}
	if runner.AgentCredentialProtocolVersion != 1 {
		t.Fatalf("agent credential protocol version = %d, want 1", runner.AgentCredentialProtocolVersion)
	}
}

func TestRoundTrip(t *testing.T) {
	in := runner.ToRunner{Type: "create", ReqID: 7, Session: "sess_ab12",
		Spec: &runner.Spec{Image: "img", Cmd: []string{"bash"}, EgressAllow: []string{"example.com"}}}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out runner.ToRunner
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
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
	in := runner.ToRunner{Type: "create", ReqID: 11, Session: "sess_cd34",
		Spec: &runner.Spec{Image: "img", Setup: "apt-get install -y jq", SetupTimeoutSec: 900,
			Env: map[string]string{"FOO": "bar"}}}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(b); !strings.Contains(got, `"setup":"apt-get install -y jq"`) ||
		!strings.Contains(got, `"setup_timeout_sec":900`) || !strings.Contains(got, `"env":{"FOO":"bar"}`) {
		t.Fatalf("spec tags wrong on the wire: %s", got)
	}
	var out runner.ToRunner
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Spec == nil || out.Spec.Setup != in.Spec.Setup || out.Spec.SetupTimeoutSec != 900 ||
		out.Spec.Env["FOO"] != "bar" {
		t.Fatalf("round trip mangled: %+v", out.Spec)
	}

	pre := runner.ToRunner{Type: "prepull", Ref: "rainier-env:env_ab12-0123456789ab"}
	pb, err := json.Marshal(pre)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(pb), `"ref":"rainier-env:env_ab12-0123456789ab"`) {
		t.Fatalf("ref tag wrong on the wire: %s", pb)
	}
	var pout runner.ToRunner
	if err := json.Unmarshal(pb, &pout); err != nil {
		t.Fatal(err)
	}
	if pout.Type != "prepull" || pout.Ref != pre.Ref {
		t.Fatalf("round trip mangled: %+v", pout)
	}

	// An empty Spec must not start emitting the new fields: omitempty keeps
	// a plain scratch create byte-identical to what Plan 1-3 peers expect.
	eb, err := json.Marshal(runner.ToRunner{Type: "create", Spec: &runner.Spec{Image: "img"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tag := range []string{"setup", "setup_timeout_sec", "env", "ref", "rpc",
		"repos", "init", "init_timeout_sec", "git_author_name", "git_author_email"} {
		if strings.Contains(string(eb), `"`+tag+`"`) {
			t.Fatalf("empty spec leaked %q: %s", tag, eb)
		}
	}
}

// TestRepoAndInitSpecRoundTrip pins the Plan 5 create vocabulary: the
// repositories a session clones (owner, name, and the three names the clone
// resolves to — base branch, session branch, and the directory it lands in),
// the per-boot init hook with its bound, and the git identity commits made
// inside the session are attributed to.
//
// The literal JSON is the assertion that matters. sessiond decodes these
// bytes out of an environment variable the driver base64s them into, so a
// renamed tag would not fail any dispatch test — it would produce a container
// that clones nothing and says nothing about why.
func TestRepoAndInitSpecRoundTrip(t *testing.T) {
	in := runner.ToRunner{Type: "create", ReqID: 12, Session: "sess_ef56", Spec: &runner.Spec{
		Image: "img",
		Repos: []runner.RepoSpec{
			{Owner: "acme", Name: "app", BaseBranch: "main", SessionBranch: "rainier/work", Dir: "app"},
			{Owner: "other", Name: "app", BaseBranch: "dev", SessionBranch: "rainier/work", Dir: "other__app"},
		},
		Init: "make dev-server", InitTimeoutSec: 900,
		GitAuthorName: "alice", GitAuthorEmail: "42+alice@users.noreply.github.com",
	}}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"repos":[{"owner":"acme","name":"app","base_branch":"main","session_branch":"rainier/work","dir":"app"},`,
		`{"owner":"other","name":"app","base_branch":"dev","session_branch":"rainier/work","dir":"other__app"}]`,
		`"init":"make dev-server"`,
		`"init_timeout_sec":900`,
		`"git_author_name":"alice"`,
		`"git_author_email":"42+alice@users.noreply.github.com"`,
	} {
		if !strings.Contains(string(b), want) {
			t.Fatalf("spec tags wrong on the wire: missing %s in\n%s", want, b)
		}
	}

	var out runner.ToRunner
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Spec == nil || len(out.Spec.Repos) != 2 {
		t.Fatalf("round trip mangled: %+v", out.Spec)
	}
	if got := out.Spec.Repos[0]; got != in.Spec.Repos[0] {
		t.Fatalf("repo[0] = %+v, want %+v", got, in.Spec.Repos[0])
	}
	if out.Spec.Init != in.Spec.Init || out.Spec.InitTimeoutSec != 900 ||
		out.Spec.GitAuthorName != "alice" || out.Spec.GitAuthorEmail != in.Spec.GitAuthorEmail {
		t.Fatalf("round trip mangled: %+v", out.Spec)
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
	down := runner.ToRunner{Type: "session_rpc", Session: "sess_ab12",
		RPC: &runner.RPCEnvelope{ID: 42, Method: "diff", Payload: json.RawMessage(`{"repo":"api"}`)}}
	db, err := json.Marshal(down)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(db); !strings.Contains(got, `"type":"session_rpc"`) ||
		!strings.Contains(got, `"rpc":{"id":42,"method":"diff","payload":{"repo":"api"}}`) {
		t.Fatalf("session_rpc wrong on the wire: %s", got)
	}
	var dout runner.ToRunner
	if err := json.Unmarshal(db, &dout); err != nil {
		t.Fatal(err)
	}
	if dout.Type != "session_rpc" || dout.Session != "sess_ab12" || dout.RPC == nil ||
		dout.RPC.ID != 42 || dout.RPC.Method != "diff" || string(dout.RPC.Payload) != `{"repo":"api"}` {
		t.Fatalf("round trip mangled: %+v", dout.RPC)
	}

	// Upward: the sandbox asks for a credential. No payload — a method whose
	// arguments are all implied by the session it came from must still put an
	// id and a method on the wire, since the id is what the response is
	// correlated against.
	up := runner.FromRunner{Type: "session_req", Session: "sess_ab12",
		RPC: &runner.RPCEnvelope{ID: 7, Method: "mint_git_credential"}}
	ub, err := json.Marshal(up)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(ub); !strings.Contains(got, `"type":"session_req"`) ||
		!strings.Contains(got, `"rpc":{"id":7,"method":"mint_git_credential"}`) {
		t.Fatalf("session_req wrong on the wire: %s", got)
	}
	var uout runner.FromRunner
	if err := json.Unmarshal(ub, &uout); err != nil {
		t.Fatal(err)
	}
	if uout.Type != "session_req" || uout.RPC == nil || uout.RPC.ID != 7 ||
		uout.RPC.Method != "mint_git_credential" || uout.RPC.Payload != nil {
		t.Fatalf("round trip mangled: %+v", uout.RPC)
	}

	// The answer to that request travels back as a session_rpc whose Method is
	// "resp", echoing the request's id.
	answer := runner.ToRunner{Type: "session_rpc", Session: "sess_ab12",
		RPC: &runner.RPCEnvelope{ID: 7, Method: "resp", Payload: json.RawMessage(`{"ok":true}`)}}
	ab, err := json.Marshal(answer)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(ab), `"rpc":{"id":7,"method":"resp","payload":{"ok":true}}`) {
		t.Fatalf("session_rpc response wrong on the wire: %s", ab)
	}

	// A response's verdict rides the envelope, in both directions, because
	// runnerd reproduces the relay ControlEvent at the far end from the
	// envelope alone — it never opens Payload to find out how a call went.
	// False is the zero value and stays off the wire (the safe direction: a
	// peer that fails to decode it reads a failure, never a spurious success),
	// so only an ok:true response carries the tag.
	okAnswer, err := json.Marshal(runner.FromRunner{Type: "session_req", Session: "sess_ab12",
		RPC: &runner.RPCEnvelope{ID: 7, Method: "resp", OK: true, Payload: json.RawMessage(`{"token":"x"}`)}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(okAnswer), `"rpc":{"id":7,"method":"resp","ok":true,"payload":{"token":"x"}}`) {
		t.Fatalf("ok response wrong on the wire: %s", okAnswer)
	}
	var okOut runner.FromRunner
	if err := json.Unmarshal(okAnswer, &okOut); err != nil {
		t.Fatal(err)
	}
	if okOut.RPC == nil || !okOut.RPC.OK {
		t.Fatalf("round trip lost the verdict: %+v", okOut.RPC)
	}
	failed, err := json.Marshal(runner.ToRunner{Type: "session_rpc", Session: "sess_ab12",
		RPC: &runner.RPCEnvelope{ID: 7, Method: "resp"}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(failed), `"ok"`) {
		t.Fatalf("a failed response leaked an ok tag: %s", failed)
	}

	// A message with no RPC must not start carrying an empty envelope: every
	// Plan 1-4 message type keeps its exact bytes.
	for _, m := range []any{runner.ToRunner{Type: "destroy", Session: "s"}, runner.FromRunner{Type: "event", Session: "s", State: "running"}} {
		b, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(b), `"rpc"`) {
			t.Fatalf("non-RPC message leaked rpc: %s", b)
		}
	}
}

// TestSetupEventDecodes pins the event vocabulary the setup pipeline adds:
// runnerd reports setup outcomes as ordinary FromRunner events, with the
// failure tail in Detail.
func TestSetupEventDecodes(t *testing.T) {
	var m runner.FromRunner
	raw := `{"type":"event","session":"sess_ef56","state":"setup_failed","detail":"exit 1: no such package"}`
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatal(err)
	}
	if m.State != "setup_failed" || m.Detail != "exit 1: no such package" {
		t.Fatalf("setup event mangled: %+v", m)
	}
}

func TestUnknownFieldsTolerated(t *testing.T) {
	// Forward compatibility: an older side must not choke on new fields.
	var m runner.FromRunner
	if err := json.Unmarshal([]byte(`{"type":"event","session":"s","state":"running","future_field":1}`), &m); err != nil {
		t.Fatalf("unknown field should be ignored: %v", err)
	}
	if m.State != "running" {
		t.Fatalf("state lost: %+v", m)
	}
}

// TestHomeMountRoundTrip pins the agent home's wire tags and, just as
// importantly, its absence: the field is additive at ProtocolVersion 1, so a
// create that mounts no home has to marshal to exactly the bytes a Plan 1-5
// peer already expects. It also pins the three agent-credential method names,
// which are wire words a sandbox and a control plane agree on by string.
func TestHomeMountRoundTrip(t *testing.T) {
	in := runner.ToRunner{Type: "create", ReqID: 21, Session: "sess_example",
		Spec: &runner.Spec{Image: "img", Home: &runner.HomeMount{
			Volume: "rainier-agents-0123456789abcdef", Path: "/rainier/agents"}}}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	const wantHome = `"home":{"volume":"rainier-agents-0123456789abcdef","path":"/rainier/agents"}`
	if !strings.Contains(string(b), wantHome) {
		t.Fatalf("home tags wrong on the wire: %s", b)
	}
	var out runner.ToRunner
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Spec == nil || out.Spec.Home == nil ||
		out.Spec.Home.Volume != in.Spec.Home.Volume || out.Spec.Home.Path != in.Spec.Home.Path {
		t.Fatalf("round trip mangled the home: %+v", out.Spec)
	}

	// No home, no key: the golden create of TestPublicRunnerWireShapes stays
	// byte-for-byte what it was.
	nb, err := json.Marshal(runner.Spec{Image: "img"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(nb), `"home"`) {
		t.Fatalf("a spec with no home leaked a home key: %s", nb)
	}

	if runner.MethodFetchAgentCredentials != "fetch_agent_credentials" ||
		runner.MethodPutAgentCredentials != "put_agent_credentials" ||
		runner.MethodRevokeAgentCredentials != "revoke_agent_credentials" {
		t.Fatalf("agent credential method names moved: %q %q %q",
			runner.MethodFetchAgentCredentials, runner.MethodPutAgentCredentials,
			runner.MethodRevokeAgentCredentials)
	}
}

// TestAnUnboundDialAttachIsTheBytesItAlwaysWas is the same additive promise
// one hop earlier, on the command a control plane sends a runner. controld
// and runnerd are separately deployed, so a dial_attach that grants no
// binding has to be byte-identical to the one every runner already knows.
func TestAnUnboundDialAttachIsTheBytesItAlwaysWas(t *testing.T) {
	raw, err := json.Marshal(runner.Attach{
		AttachID: "att_example", Since: 3, Cols: 80, Rows: 24,
		TargetURL: "wss://rainier.example.invalid/v0/attach-back/att_example"})
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"attach_id":"att_example","since":3,"cols":80,"rows":24,` +
		`"target_url":"wss://rainier.example.invalid/v0/attach-back/att_example"}`
	if string(raw) != want {
		t.Fatalf("an unbound dial_attach = %s\nwant %s", raw, want)
	}

	bound, err := json.Marshal(runner.Attach{AttachID: "att_example", Mode: "view", Generation: 9})
	if err != nil {
		t.Fatal(err)
	}
	var back runner.Attach
	if err := json.Unmarshal(bound, &back); err != nil {
		t.Fatal(err)
	}
	if back.Mode != "view" || back.Generation != 9 {
		t.Fatalf("decoded binding = %q at %d, want view at 9", back.Mode, back.Generation)
	}
}

// TestCapacityCountsOnTheWire pins the tag names of the two counts that split
// Used by what a sandbox is doing. Both ends read them off the wire by name,
// and a Go-side test that goes through the struct would go green against a
// typo that silently delivered zeros to a real control plane — which is the
// same "unknown" an old runner sends, so nothing downstream would complain.
//
// They are NOT omitempty, deliberately: a runner with nothing idle reports
// idle_exited 0, and a field that vanished at zero would be indistinguishable
// on the wire from one that was never sent.
func TestCapacityCountsOnTheWire(t *testing.T) {
	b, err := json.Marshal(runner.FromRunner{Type: "announce", Used: 4, Total: 16, Active: 3, IdleExited: 1})
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	for _, want := range []string{`"used":4`, `"total":16`, `"active":3`, `"idle_exited":1`} {
		if !strings.Contains(got, want) {
			t.Fatalf("capacity tags wrong on the wire: want %s in %s", want, got)
		}
	}
	zero, err := json.Marshal(runner.FromRunner{Type: "announce", Total: 16})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"active":0`, `"idle_exited":0`} {
		if !strings.Contains(string(zero), want) {
			t.Fatalf("a zero count must still ride the wire: want %s in %s", want, zero)
		}
	}
	var out runner.FromRunner
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Active != 3 || out.IdleExited != 1 {
		t.Fatalf("round trip mangled the counts: %+v", out)
	}
}

// TestConflictRefusalOnTheWire pins the tag of the bit that tells a refusal
// the runner chose ("not yet — I am already stopping this sandbox") from one
// it suffered ("this failed"). Both ends read it off the wire by name, and a
// control plane that cannot see it can only report the first as the second:
// a 500 internal error for a healthy runner mid-stop.
//
// omitempty, deliberately and unlike the capacity counts: `false` is the
// answer every result that is not a conflict has always given, so a result
// that omits the key and one that sends false mean the same thing, and
// omitting it keeps an ordinary result byte-for-byte what it was.
func TestConflictRefusalOnTheWire(t *testing.T) {
	b, err := json.Marshal(runner.FromRunner{Type: "result", ReqID: 7, Detail: "session is being suspended", Conflict: true})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"conflict":true`) {
		t.Fatalf("conflict tag wrong on the wire: %s", b)
	}
	plain, err := json.Marshal(runner.FromRunner{Type: "result", ReqID: 7, Detail: "no such session"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(plain), `"conflict"`) {
		t.Fatalf("an ordinary refusal grew a conflict key: %s", plain)
	}
	var out runner.FromRunner
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if !out.Conflict {
		t.Fatalf("round trip lost the conflict bit: %+v", out)
	}
	// An old runner sends no key at all, which must decode as "not a
	// conflict" — today's behaviour, which is what makes the field additive.
	var old runner.FromRunner
	if err := json.Unmarshal([]byte(`{"type":"result","req_id":7,"detail":"boom"}`), &old); err != nil {
		t.Fatal(err)
	}
	if old.Conflict {
		t.Fatal("a result from a runner that predates the field decoded as a conflict")
	}
}

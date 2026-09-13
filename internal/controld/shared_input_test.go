package controld

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/coder/websocket"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// The self-hosted composition's half of the shared attachment policy: the
// default this binary ships, the flag that selects the other one, and the two
// things the composition does with the answer — grant by it, and report it.

// sharedInput is the attach fixture under the policy this binary DEFAULTS to.
// The fixture names exclusive because the tests built on it pin take-overs; a
// test whose subject is the default has to say so.
func sharedInput(c *Config) { c.InputPolicy = control.PolicyShared }

// TestTheDefaultInputPolicyIsShared pins the product default at the
// composition root: a Config that names no policy grants shared input, and the
// attachment service is composed with the same answer the view reports.
func TestTheDefaultInputPolicyIsShared(t *testing.T) {
	s, err := New(NewMemStore(), Config{
		RunnerToken: "t", ExternalURL: "http://x:9090", SecretsKey: testSecretsKey,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := s.inputPolicy(); got != control.PolicyShared {
		t.Fatalf("the default input policy is %q, want shared", got)
	}

	exclusive, err := New(NewMemStore(), Config{
		RunnerToken: "t", ExternalURL: "http://x:9090", SecretsKey: testSecretsKey,
		InputPolicy: control.PolicyExclusive,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := exclusive.inputPolicy(); got != control.PolicyExclusive {
		t.Fatalf("the selected input policy is %q, want exclusive", got)
	}
}

// TestTheSessionViewReportsThePolicyAndTheTyperCount is what `rainier info`
// reads its input row out of, and the only place a client learns the policy.
func TestTheSessionViewReportsThePolicyAndTheTyperCount(t *testing.T) {
	fx := newAttachFixture(t, sharedInput)

	if got := sessionInput(t, fx); got.Policy != "shared" || got.Attached != 0 {
		t.Fatalf("with nobody attached the view says %+v, want shared with 0", got)
	}

	laptop, _, err := dialAttach(t, fx.ts, fx.id, "?control=v1", fx.tok)
	if err != nil {
		t.Fatalf("dial attach: %v", err)
	}
	defer laptop.CloseNow()
	writeClient(t, laptop, terminal.ClientMessage{Type: "resize", Cols: 120, Rows: 40})
	if m := readServer(t, laptop); m.Type != terminal.TypeAttached || m.Mode != terminal.ModeControl {
		t.Fatalf("first server msg = %+v, want attached as control", m)
	}
	if got := sessionInput(t, fx); got.Policy != "shared" || got.Attached != 1 {
		t.Fatalf("with one typer the view says %+v, want shared with 1", got)
	}

	browser, _, err := dialAttach(t, fx.ts, fx.id, "?control=v1", fx.tok)
	if err != nil {
		t.Fatalf("dial second attach: %v", err)
	}
	defer browser.CloseNow()
	writeClient(t, browser, terminal.ClientMessage{Type: "resize", Cols: 80, Rows: 24})
	if m := readServer(t, browser); m.Type != terminal.TypeAttached || m.Mode != terminal.ModeControl {
		t.Fatalf("the second attach's first server msg = %+v, want attached as control", m)
	}
	if got := sessionInput(t, fx); got.Policy != "shared" || got.Attached != 2 {
		t.Fatalf("with two typers the view says %+v, want shared with 2", got)
	}

	// A viewer is not a typer, whatever else it is.
	phone, _, err := dialAttach(t, fx.ts, fx.id, "?control=v1&mode=view", fx.tok)
	if err != nil {
		t.Fatalf("dial view attach: %v", err)
	}
	defer phone.CloseNow()
	writeClient(t, phone, terminal.ClientMessage{Type: "resize", Cols: 40, Rows: 20})
	if m := readServer(t, phone); m.Type != terminal.TypeAttached || m.Mode != terminal.ModeView {
		t.Fatalf("the viewer's first server msg = %+v, want attached as view", m)
	}
	if got := sessionInput(t, fx); got.Attached != 2 {
		t.Fatalf("a viewer counted as a typer: %+v", got)
	}
}

// TestTwoNegotiatedAttachesTypeUnderOneGeneration is the whole change seen from
// the outside, over real HTTP through the real composition: both terminals are
// told `control` at the SAME generation, the stored generation never moves, and
// neither client is told it lost anything.
func TestTwoNegotiatedAttachesTypeUnderOneGeneration(t *testing.T) {
	fx := newAttachFixture(t, sharedInput)
	before := storedControllerGeneration(t, fx)

	laptop, _, err := dialAttach(t, fx.ts, fx.id, "?control=v1", fx.tok)
	if err != nil {
		t.Fatalf("dial attach: %v", err)
	}
	defer laptop.CloseNow()
	writeClient(t, laptop, terminal.ClientMessage{Type: "resize", Cols: 120, Rows: 40})
	first := readServer(t, laptop)
	if first.Type != terminal.TypeAttached || first.Mode != terminal.ModeControl {
		t.Fatalf("first server msg = %+v, want attached as control", first)
	}
	// The binding rode the frame that opens the attachment, at the generation
	// the row already carried rather than a new one.
	if open := fx.sd.nextOpen(t); open.Mode != terminal.ModeControl || open.Gen != before {
		t.Fatalf("FrameOpen carried mode %q at %d, want control at %d", open.Mode, open.Gen, before)
	}

	browser, _, err := dialAttach(t, fx.ts, fx.id, "?control=v1", fx.tok)
	if err != nil {
		t.Fatalf("dial second attach: %v", err)
	}
	defer browser.CloseNow()
	writeClient(t, browser, terminal.ClientMessage{Type: "resize", Cols: 80, Rows: 24})
	second := readServer(t, browser)
	if second.Type != terminal.TypeAttached || second.Mode != terminal.ModeControl {
		t.Fatalf("the second attach's first server msg = %+v, want attached as control", second)
	}
	if second.Generation != first.Generation {
		t.Fatalf("the two typers hold %q and %q, want one generation",
			first.Generation, second.Generation)
	}
	if open := fx.sd.nextOpen(t); open.Mode != terminal.ModeControl || open.Gen != before {
		t.Fatalf("the second FrameOpen carried mode %q at %d, want control at %d",
			open.Mode, open.Gen, before)
	}
	if got := storedControllerGeneration(t, fx); got != before {
		t.Fatalf("the stored generation moved from %d to %d under a shared policy", before, got)
	}

	// Both type, and the fake sessiond echoes what it executed back to its
	// sender — which is the round trip that proves the plane carried it.
	writeClient(t, laptop, terminal.ClientMessage{Type: "stdin", Data: []byte("a")})
	if got := awaitOutput(t, laptop); got != "a" {
		t.Fatalf("the laptop's keystroke came back %q", got)
	}
	writeClient(t, browser, terminal.ClientMessage{Type: "stdin", Data: []byte("b")})
	if got := awaitOutput(t, browser); got != "b" {
		t.Fatalf("the browser's keystroke came back %q", got)
	}
}

// awaitOutput reads this client's stream until an `output` arrives and returns
// its bytes. The opening snapshot is in front of it, and under this policy an
// ownership message may not be (nothing displaces anybody), so the read has to
// skip what it is not asking about rather than assume a position.
func awaitOutput(t *testing.T, c *websocket.Conn) string {
	t.Helper()
	for i := 0; i < 8; i++ {
		if m := readServer(t, c); m.Type == "output" {
			return string(m.Data)
		}
	}
	t.Fatal("no output reached the client")
	return ""
}

// sessionInput reads the input object off GET /v0/sessions/{id}.
func sessionInput(t *testing.T, fx *attachFixture) struct {
	Policy   string `json:"policy"`
	Attached int    `json:"attached"`
} {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
		fx.ts.URL+"/v0/sessions/"+fx.id, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+fx.tok)
	resp, err := fx.ts.Client().Do(req)
	if err != nil {
		t.Fatalf("GET the session: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET the session: %s", resp.Status)
	}
	var body struct {
		Session struct {
			Input struct {
				Policy   string `json:"policy"`
				Attached int    `json:"attached"`
			} `json:"input"`
		} `json:"session"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decoding the session view: %v", err)
	}
	return struct {
		Policy   string `json:"policy"`
		Attached int    `json:"attached"`
	}{body.Session.Input.Policy, body.Session.Input.Attached}
}

// storedControllerGeneration reads the generation off the row, which is the
// value nothing under a shared policy may move.
func storedControllerGeneration(t *testing.T, fx *attachFixture) uint64 {
	t.Helper()
	row, err := fx.st.Sessions().GetSession(context.Background(), installWorkspace, control.SessionID(fx.id))
	if err != nil {
		t.Fatalf("reading the session row: %v", err)
	}
	return row.ControllerGeneration
}

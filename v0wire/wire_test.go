// v0wire/wire_test.go
package v0wire_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/controlapp"
	"github.com/tokencanopy/rainier/v0wire"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// at is a fixed synthetic instant, so every golden below is byte-stable.
func at(sec int) time.Time { return time.Date(2026, 1, 2, 3, 4, sec, 0, time.UTC) }

// golden marshals v and compares the bytes exactly. The /v0/ wire is a
// contract: a renamed key, a reordered field, or a nil rendered as null
// instead of [] is a breaking change, and this is where it is caught.
func golden(t *testing.T, v any, want string) {
	t.Helper()
	got, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(got) != want {
		t.Fatalf("JSON mismatch\n got: %s\nwant: %s", got, want)
	}
}

// keySet fails unless the object's key set is exactly want. Views carry no
// omitempty on purpose: a key that appears only sometimes cannot be told
// apart from an older server that never had it.
func keySet(t *testing.T, v any, want ...string) {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("decode for key-set check: %v; body=%s", err, raw)
	}
	got := make([]string, 0, len(m))
	for k := range m {
		got = append(got, k)
	}
	slices.Sort(got)
	wantSorted := slices.Clone(want)
	slices.Sort(wantSorted)
	if !slices.Equal(got, wantSorted) {
		t.Fatalf("keys = %v, want %v", got, wantSorted)
	}
}

// fullSession is one fully populated session row, synthetic throughout.
func fullSession() control.Session {
	code := 0
	return control.Session{
		ID:        "sess_example",
		CreatorID: "usr_example",
		Name:      "demo",
		Spec: control.PortableSpec{
			Image:       "images.example.test/base:1",
			Cmd:         []string{"bash", "-lc", "echo ok"},
			EgressAllow: []string{"example.test", "203.0.113.7"},
		},
		State:         control.StateRunning,
		RunnerID:      "runner-example",
		Error:         "the setup script failed",
		ChildExitCode: &code,
		CreatedAt:     at(5),
		UpdatedAt:     at(6),
		LastEventAt:   at(7),
	}
}

const fullSessionJSON = `{"id":"sess_example","owner_id":"usr_example","name":"demo",` +
	`"image":"images.example.test/base:1","cmd":["bash","-lc","echo ok"],` +
	`"egress_allow":["example.test","203.0.113.7"],"state":"running","runner":"runner-example",` +
	`"reachable":true,"error":"the setup script failed","environment":"dev",` +
	`"queue_reason":"waiting for runner runner-example","child_exit_code":0,` +
	`"created_at":"2026-01-02T03:04:05Z","updated_at":"2026-01-02T03:04:06Z",` +
	`"last_event_at":"2026-01-02T03:04:07Z"}`

func fullDerived() v0wire.SessionDerived {
	return v0wire.SessionDerived{
		Reachable:   true,
		Environment: "dev",
		QueueReason: "waiting for runner runner-example",
	}
}

// fullEnvironment is one fully populated environment row.
func fullEnvironment() control.Environment {
	return control.Environment{
		ID:             "env_example",
		Name:           "dev",
		Image:          "images.example.test/base:1",
		Setup:          "echo setup",
		SetupHash:      "sha256:0000000000000001",
		Init:           "echo init",
		InitTimeoutSec: 60,
		EgressAllow:    []string{"example.test"},
		SecretRefs:     []string{"API_TOKEN"},
		Connectors: []control.Connector{
			{Type: "github", Raw: json.RawMessage(`{"type":"github","repo":"acme/widgets"}`)},
		},
		Requirements:    control.Requirements{Capabilities: []string{"placement:runner-example", "gpu", "docker.rootless"}},
		SetupTimeoutSec: 900,
		Snapshot:        control.Checkpoint{Ref: "snap:dev@0000000000000001"},
		SnapshotHash:    "sha256:0000000000000001",
		CreatedAt:       at(5),
		UpdatedAt:       at(6),
	}
}

const fullEnvironmentJSON = `{"id":"env_example","name":"dev","image":"images.example.test/base:1",` +
	`"setup":"echo setup","setup_hash":"sha256:0000000000000001","init":"echo init",` +
	`"init_timeout_sec":60,"egress_allow":["example.test"],"secret_refs":["API_TOKEN"],` +
	`"connectors":[{"type":"github","repo":"acme/widgets"}],"placement":"runner-example",` +
	`"capabilities":["gpu","docker.rootless"],"setup_timeout_sec":900,` +
	`"snapshot_ref":"snap:dev@0000000000000001","snapshot_runner":"runner-example",` +
	`"snapshot_hash":"sha256:0000000000000001","created_at":"2026-01-02T03:04:05Z",` +
	`"updated_at":"2026-01-02T03:04:06Z"}`

func fullRunner() control.Runner {
	return control.Runner{
		ID:            "runner-example",
		PoolID:        "pool_example",
		CapacityUsed:  2,
		CapacityTotal: 4,
		Connected:     true,
		Generation:    7,
		Capabilities:  []string{"gpu"},
		LastSeenAt:    at(5),
	}
}

const fullRunnerJSON = `{"name":"runner-example","connected":true,"capacity_used":2,` +
	`"capacity_total":4,"last_seen_at":"2026-01-02T03:04:05Z"}`

// ---------------------------------------------------------------------------
// the views
// ---------------------------------------------------------------------------

func TestSessionViewGoldenJSON(t *testing.T) {
	golden(t, v0wire.RenderSession(fullSession(), fullDerived()), fullSessionJSON)
}

func TestSessionViewKeySet(t *testing.T) {
	keySet(t, v0wire.RenderSession(control.Session{}, v0wire.SessionDerived{}),
		"id", "owner_id", "name", "image", "cmd", "egress_allow", "state", "runner",
		"reachable", "error", "environment", "queue_reason", "child_exit_code",
		"created_at", "updated_at", "last_event_at")
	// The key set does not depend on how much of the row is populated.
	keySet(t, v0wire.RenderSession(fullSession(), fullDerived()),
		"id", "owner_id", "name", "image", "cmd", "egress_allow", "state", "runner",
		"reachable", "error", "environment", "queue_reason", "child_exit_code",
		"created_at", "updated_at", "last_event_at")
}

func TestSessionViewNormalizesEmptyAndNull(t *testing.T) {
	// A zero row: nil Cmd and EgressAllow render as [] rather than null (the
	// memstore-vs-pgstore difference never reaches the wire), and a session
	// whose agent has not exited renders child_exit_code as null.
	golden(t, v0wire.RenderSession(control.Session{}, v0wire.SessionDerived{}),
		`{"id":"","owner_id":"","name":"","image":"","cmd":[],"egress_allow":[],`+
			`"state":"","runner":"","reachable":false,"error":"","environment":"",`+
			`"queue_reason":"","child_exit_code":null,"created_at":"0001-01-01T00:00:00Z",`+
			`"updated_at":"0001-01-01T00:00:00Z","last_event_at":"0001-01-01T00:00:00Z"}`)
}

func TestSessionViewRendersTimestampsAsUTC(t *testing.T) {
	row := fullSession()
	east := time.FixedZone("TEST", 2*60*60)
	row.CreatedAt = row.CreatedAt.In(east)
	row.UpdatedAt = row.UpdatedAt.In(east)
	row.LastEventAt = row.LastEventAt.In(east)
	golden(t, v0wire.RenderSession(row, fullDerived()), fullSessionJSON)
}

func TestSessionViewNeverAliasesTheRow(t *testing.T) {
	row := fullSession()
	view := v0wire.RenderSession(row, v0wire.SessionDerived{})
	*view.ChildExitCode = 42
	if *row.ChildExitCode != 0 {
		t.Fatalf("rendering aliased the row's exit code: %d", *row.ChildExitCode)
	}
}

func TestSessionEnvelopesGoldenJSON(t *testing.T) {
	view := v0wire.RenderSession(fullSession(), fullDerived())
	golden(t, v0wire.SessionEnvelope{Session: view}, `{"session":`+fullSessionJSON+`}`)
	keySet(t, v0wire.SessionEnvelope{Session: view}, "session")

	page := v0wire.SessionsEnvelope{Sessions: []v0wire.SessionView{view}, NextCursor: "cursor_example"}
	golden(t, page, `{"sessions":[`+fullSessionJSON+`],"next_cursor":"cursor_example"}`)
	keySet(t, page, "sessions", "next_cursor")
}

func TestEnvironmentViewGoldenJSON(t *testing.T) {
	golden(t, v0wire.RenderEnvironment(fullEnvironment(), "runner-example"), fullEnvironmentJSON)
}

func TestEnvironmentViewKeySet(t *testing.T) {
	want := []string{"id", "name", "image", "setup", "setup_hash", "init", "init_timeout_sec",
		"egress_allow", "secret_refs", "connectors", "placement", "capabilities",
		"setup_timeout_sec", "snapshot_ref", "snapshot_runner", "snapshot_hash",
		"created_at", "updated_at"}
	keySet(t, v0wire.RenderEnvironment(control.Environment{}, ""), want...)
	keySet(t, v0wire.RenderEnvironment(fullEnvironment(), "runner-example"), want...)
}

func TestEnvironmentViewNormalizesEmptyLists(t *testing.T) {
	golden(t, v0wire.RenderEnvironment(control.Environment{}, ""),
		`{"id":"","name":"","image":"","setup":"","setup_hash":"","init":"","init_timeout_sec":0,`+
			`"egress_allow":[],"secret_refs":[],"connectors":[],"placement":"","capabilities":[],`+
			`"setup_timeout_sec":0,"snapshot_ref":"","snapshot_runner":"","snapshot_hash":"",`+
			`"created_at":"0001-01-01T00:00:00Z","updated_at":"0001-01-01T00:00:00Z"}`)
}

func TestEnvironmentViewRendersAConnectorWithNoRaw(t *testing.T) {
	// Unreachable through ValidateConnectors, which always keeps the caller's
	// bytes — but a row from anywhere else must not encode as invalid JSON and
	// truncate the whole response.
	e := control.Environment{Connectors: []control.Connector{{Type: "github"}}}
	view := v0wire.RenderEnvironment(e, "")
	if got, want := string(view.Connectors[0]), `{"type":"github"}`; got != want {
		t.Fatalf("connector = %s, want %s", got, want)
	}
}

func TestEnvironmentEnvelopesGoldenJSON(t *testing.T) {
	view := v0wire.RenderEnvironment(fullEnvironment(), "runner-example")
	golden(t, v0wire.EnvironmentEnvelope{Environment: view}, `{"environment":`+fullEnvironmentJSON+`}`)
	keySet(t, v0wire.EnvironmentEnvelope{Environment: view}, "environment")

	list := v0wire.EnvironmentsEnvelope{Environments: []v0wire.EnvironmentView{view}}
	golden(t, list, `{"environments":[`+fullEnvironmentJSON+`]}`)
	keySet(t, list, "environments")
}

func TestRunnerViewGoldenJSON(t *testing.T) {
	golden(t, v0wire.RenderRunner(fullRunner(), true), fullRunnerJSON)
	keySet(t, v0wire.RenderRunner(fullRunner(), true),
		"name", "connected", "capacity_used", "capacity_total", "last_seen_at")

	// connected is the caller's answer, not the row's: a host that knows the
	// connection is gone renders false over a row that still says true.
	if v0wire.RenderRunner(fullRunner(), false).Connected {
		t.Fatal("connected=false was not honored")
	}
}

func TestRunnersEnvelopeGoldenJSON(t *testing.T) {
	env := v0wire.RunnersEnvelope{Runners: []v0wire.RunnerView{v0wire.RenderRunner(fullRunner(), true)}}
	golden(t, env, `{"runners":[`+fullRunnerJSON+`]}`)
	keySet(t, env, "runners")
}

func TestErrorBodyGoldenJSON(t *testing.T) {
	golden(t, v0wire.ErrorBody{Code: "not_found", Message: "not found"},
		`{"code":"not_found","message":"not found"}`)
	keySet(t, v0wire.ErrorBody{}, "code", "message")
}

// ---------------------------------------------------------------------------
// the sentinel -> status table (moved from internal/controld/adapt_http_test.go)
// ---------------------------------------------------------------------------

// TestStatusForTable pins every row of the sentinel mapping. The wire learns
// about the closed error set here and nowhere else.
func TestStatusForTable(t *testing.T) {
	cases := []struct {
		err    error
		status int
		code   string
	}{
		{control.ErrInvalid, http.StatusBadRequest, "invalid_request"},
		{control.ErrDenied, http.StatusForbidden, "forbidden"},
		{control.ErrNotFound, http.StatusNotFound, "not_found"},
		{control.ErrConflict, http.StatusConflict, "conflict"},
		{control.ErrStale, http.StatusConflict, "conflict"},
		{control.ErrUnavailable, http.StatusInternalServerError, "internal"},
		{control.ErrUnsupported, http.StatusNotImplemented, "unsupported"},
		{context.Canceled, 0, ""},
		{context.DeadlineExceeded, 0, ""},
		{errors.New("pq: relation does not exist (host=db.internal.invalid)"), http.StatusInternalServerError, "internal"},
	}
	for _, tc := range cases {
		status, code, msg := v0wire.StatusFor(tc.err)
		if status != tc.status || code != tc.code {
			t.Errorf("StatusFor(%v) = %d %q, want %d %q", tc.err, status, code, tc.status, tc.code)
		}
		if status != 0 && msg == "" {
			t.Errorf("StatusFor(%v) has no message", tc.err)
		}
		if status != 0 && (msg == tc.err.Error()) {
			t.Errorf("StatusFor(%v) relayed the error text", tc.err)
		}
	}
	// A wrapped sentinel maps the same as the bare one.
	if status, _, _ := v0wire.StatusFor(errors.Join(errors.New("adapter detail"), control.ErrNotFound)); status != http.StatusNotFound {
		t.Fatalf("wrapped ErrNotFound = %d", status)
	}
}

func TestWriteControlErrorWritesNothingForACallerThatWentAway(t *testing.T) {
	rec := httptest.NewRecorder()
	v0wire.WriteControlError(rec, context.Canceled)
	if rec.Code != http.StatusOK || rec.Body.Len() != 0 {
		t.Fatalf("wrote %d %q for a canceled caller", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	v0wire.WriteControlError(rec, control.ErrNotFound)
	if rec.Code != http.StatusNotFound || rec.Header().Get("Content-Type") == "" {
		t.Fatalf("got %d %q", rec.Code, rec.Body.String())
	}
}

func TestWriteErrorEnvelope(t *testing.T) {
	rec := httptest.NewRecorder()
	v0wire.WriteError(rec, http.StatusBadRequest, "invalid_request", "malformed request body")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", rec.Code)
	}
	if got, want := rec.Header().Get("Content-Type"), "application/json; charset=utf-8"; got != want {
		t.Fatalf("content-type = %q, want %q", got, want)
	}
	if got, want := strings.TrimSpace(rec.Body.String()),
		`{"error":{"code":"invalid_request","message":"malformed request body"}}`; got != want {
		t.Fatalf("body = %s, want %s", got, want)
	}
}

// ---------------------------------------------------------------------------
// DecodeJSON
// ---------------------------------------------------------------------------

func TestDecodeJSON(t *testing.T) {
	type body struct {
		Name string `json:"name,omitempty"`
	}
	decode := func(raw string) (body, *httptest.ResponseRecorder, bool) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v0/sessions", strings.NewReader(raw))
		var v body
		ok := v0wire.DecodeJSON(rec, req, &v, 64<<10)
		return v, rec, ok
	}

	t.Run("an empty body is the zero value", func(t *testing.T) {
		v, rec, ok := decode("")
		if !ok || v.Name != "" || rec.Body.Len() != 0 {
			t.Fatalf("got %+v ok=%v body=%q", v, ok, rec.Body.String())
		}
	})
	t.Run("an object decodes", func(t *testing.T) {
		v, _, ok := decode(`{"name":"demo"}`)
		if !ok || v.Name != "demo" {
			t.Fatalf("got %+v ok=%v", v, ok)
		}
	})
	t.Run("an unknown field is a 400", func(t *testing.T) {
		_, rec, ok := decode(`{"nmae":"demo"}`)
		if ok || rec.Code != http.StatusBadRequest {
			t.Fatalf("ok=%v status=%d", ok, rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "malformed request body") {
			t.Fatalf("body = %s", rec.Body.String())
		}
	})
	t.Run("a second JSON value is a 400", func(t *testing.T) {
		_, rec, ok := decode(`{"name":"a"}{"name":"b"}`)
		if ok || rec.Code != http.StatusBadRequest {
			t.Fatalf("ok=%v status=%d", ok, rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "single JSON object") {
			t.Fatalf("body = %s", rec.Body.String())
		}
	})
	t.Run("a body past the limit is a 400", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v0/sessions",
			strings.NewReader(`{"name":"`+strings.Repeat("x", 512)+`"}`))
		var v body
		if ok := v0wire.DecodeJSON(rec, req, &v, 64); ok || rec.Code != http.StatusBadRequest {
			t.Fatalf("ok=%v status=%d", ok, rec.Code)
		}
	})
}

// ---------------------------------------------------------------------------
// the create-session body
// ---------------------------------------------------------------------------

func TestDecodeCreateSession(t *testing.T) {
	t.Run("a scratch body becomes the command", func(t *testing.T) {
		cmd, msg := v0wire.DecodeCreateSession(v0wire.CreateSessionRequest{
			Name:        "demo",
			Image:       "images.example.test/base:1",
			Cmd:         []string{"bash", "-lc", "echo ok"},
			EgressAllow: []string{"example.test"},
		})
		if msg != "" {
			t.Fatalf("msg = %q, want none", msg)
		}
		if cmd.Name != "demo" || cmd.Spec.Image != "images.example.test/base:1" {
			t.Fatalf("cmd = %+v", cmd)
		}
		if !slices.Equal(cmd.Spec.Cmd, []string{"bash", "-lc", "echo ok"}) ||
			!slices.Equal(cmd.Spec.EgressAllow, []string{"example.test"}) {
			t.Fatalf("spec = %+v", cmd.Spec)
		}
		if cmd.Repos != nil {
			t.Fatalf("repos = %+v, want nil (inherit the environment's)", cmd.Repos)
		}
	})

	t.Run("nil and empty repos are different requests", func(t *testing.T) {
		cmd, msg := v0wire.DecodeCreateSession(v0wire.CreateSessionRequest{Repos: []v0wire.RepoRequest{}})
		if msg != "" {
			t.Fatalf("msg = %q", msg)
		}
		if cmd.Repos == nil || len(cmd.Repos) != 0 {
			t.Fatalf("repos = %#v, want a non-nil empty slice (clone nothing)", cmd.Repos)
		}
	})

	t.Run("an explicit base_branch travels and an absent one stays empty", func(t *testing.T) {
		branch := "trunk"
		cmd, msg := v0wire.DecodeCreateSession(v0wire.CreateSessionRequest{Repos: []v0wire.RepoRequest{
			{Repo: "acme/widgets", BaseBranch: &branch},
			{Repo: "acme/.github"},
		}})
		if msg != "" {
			t.Fatalf("msg = %q", msg)
		}
		want := []control.RepoRef{{Repo: "acme/widgets", BaseBranch: "trunk"}, {Repo: "acme/.github"}}
		if !slices.Equal(cmd.Repos, want) {
			t.Fatalf("repos = %+v, want %+v", cmd.Repos, want)
		}
	})

	t.Run("rejections name the offending entry", func(t *testing.T) {
		empty := ""
		cases := []struct {
			name string
			in   []v0wire.RepoRequest
			want string
		}{
			{"no owner", []v0wire.RepoRequest{{Repo: "widgets"}}, "repos[0].repo"},
			{"a path segment too many", []v0wire.RepoRequest{{Repo: "acme/widgets/deep"}}, "repos[0].repo"},
			{"the parent directory", []v0wire.RepoRequest{{Repo: "acme/.."}}, "repos[0].repo"},
			{"an option-looking name", []v0wire.RepoRequest{{Repo: "acme/-widgets"}}, "repos[0].repo"},
			{"a later entry is named by index", []v0wire.RepoRequest{{Repo: "acme/widgets"}, {Repo: "nope"}}, "repos[1].repo"},
			{"an explicitly empty base_branch", []v0wire.RepoRequest{{Repo: "acme/widgets", BaseBranch: &empty}}, "repos[0].base_branch"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				cmd, msg := v0wire.DecodeCreateSession(v0wire.CreateSessionRequest{Repos: tc.in})
				if msg == "" {
					t.Fatalf("accepted %+v as %+v", tc.in, cmd)
				}
				if !strings.Contains(msg, tc.want) {
					t.Fatalf("message = %q, want it to mention %q", msg, tc.want)
				}
			})
		}
	})
}

func TestSuspendRequestDistinguishesAbsentFromFalse(t *testing.T) {
	var req v0wire.SuspendRequest
	if err := json.Unmarshal([]byte(`{}`), &req); err != nil || req.Warm != nil {
		t.Fatalf("absent warm = %v (%v)", req.Warm, err)
	}
	if err := json.Unmarshal([]byte(`{"warm":false}`), &req); err != nil || req.Warm == nil || *req.Warm {
		t.Fatalf("explicit false = %v (%v)", req.Warm, err)
	}
}

// ---------------------------------------------------------------------------
// connector vocabulary (moved from internal/controld/api_test.go)
// ---------------------------------------------------------------------------

const (
	ghConnJSON      = `{"type":"github","repo":"acme/widgets","base_branch":"trunk"}`
	filesConnJSON   = `{"type":"files","paths":["/etc/hosts","notes.md"]}`
	tunnelConnJSON  = `{"type":"tunnel","name":"mav","target_host":"127.0.0.1","target_port":14550}`
	browserConnJSON = `{"type":"browser","tier":"dedicated"}`
)

// connectorArray renders elems as the JSON array "connectors" accepts.
func connectorArray(elems ...string) json.RawMessage {
	return json.RawMessage("[" + strings.Join(elems, ",") + "]")
}

func TestValidateConnectors(t *testing.T) {
	t.Run("absent and empty both mean no connectors", func(t *testing.T) {
		got, err := v0wire.ValidateConnectors(nil)
		if err != nil || len(got) != 0 {
			t.Fatalf("ValidateConnectors(nil) = %+v, %v; want none, nil", got, err)
		}
		got, err = v0wire.ValidateConnectors(json.RawMessage(`[]`))
		if err != nil || len(got) != 0 {
			t.Fatalf("ValidateConnectors([]) = %+v, %v; want none, nil", got, err)
		}
	})

	t.Run("one of each type is accepted, type decoded and bytes kept verbatim", func(t *testing.T) {
		want := []string{ghConnJSON, filesConnJSON, tunnelConnJSON, browserConnJSON}
		wantTypes := []string{"github", "files", "tunnel", "browser"}

		got, err := v0wire.ValidateConnectors(connectorArray(want...))
		if err != nil {
			t.Fatalf("ValidateConnectors: %v", err)
		}
		if len(got) != len(want) {
			t.Fatalf("got %d connectors, want %d", len(got), len(want))
		}
		for i := range got {
			if got[i].Type != wantTypes[i] {
				t.Errorf("connector %d type = %q, want %q", i, got[i].Type, wantTypes[i])
			}
			// Raw is never empty and never re-rendered: the stores diverge on
			// how they persist an empty Raw, so the API must keep that case
			// out of reachable space entirely.
			if string(got[i].Raw) != want[i] {
				t.Errorf("connector %d raw = %s, want %s", i, got[i].Raw, want[i])
			}
		}
	})

	t.Run("a github connector may omit base_branch (it defaults to main)", func(t *testing.T) {
		const in = `{"type":"github","repo":"acme/widgets"}`
		got, err := v0wire.ValidateConnectors(connectorArray(in))
		if err != nil {
			t.Fatalf("ValidateConnectors: %v", err)
		}
		// The default is a decode-time value, not a stored one: the bytes
		// stay exactly as the client wrote them.
		if len(got) != 1 || string(got[0].Raw) != in {
			t.Fatalf("got %+v, want the original bytes kept verbatim", got)
		}
		gh, err := v0wire.DecodeGitHubConnector(json.RawMessage(in))
		if err != nil {
			t.Fatalf("DecodeGitHubConnector: %v", err)
		}
		if gh.BaseBranch == nil || *gh.BaseBranch != v0wire.DefaultBaseBranch {
			t.Errorf("base_branch = %v, want the default main filled in", gh.BaseBranch)
		}
	})

	t.Run("a dot-leading repository name is still a repository name", func(t *testing.T) {
		// The path specials are refused, but `.github` is a real and extremely
		// common repository, and a dotted directory under /workspace is still
		// under /workspace. Refusing it would close nothing.
		for _, in := range []string{
			`{"type":"github","repo":"acme/.github"}`,
			`{"type":"github","repo":"acme/dot.name"}`,
			`{"type":"github","repo":"acme/with-dash"}`,
			`{"type":"github","repo":"acme/_under"}`,
		} {
			if _, err := v0wire.ValidateConnectors(connectorArray(in)); err != nil {
				t.Errorf("ValidateConnectors(%s) = %v, want it accepted", in, err)
			}
		}
	})

	t.Run("rejections name what was wrong", func(t *testing.T) {
		cases := []struct {
			name, in, want string
		}{
			{"not an array", `{"type":"browser","tier":"dedicated"}`, "array"},
			{"element is not an object", `["github"]`, "connectors[0]"},
			{"missing type", `[{"repo":"acme/widgets"}]`, "type"},
			{"unknown type", `[{"type":"gitlab","repo":"acme/widgets"}]`, "gitlab"},
			{"unknown type is named even in a later element", `[` + ghConnJSON + `,{"type":"gitlab"}]`, "gitlab"},
			{"unknown field on github", `[{"type":"github","repo":"x/y","extra":1}]`, "extra"},
			{"unknown field on files", `[{"type":"files","paths":["a"],"recursive":true}]`, "recursive"},
			{"unknown field on tunnel", `[{"type":"tunnel","name":"n","target_host":"h","target_port":1,"proto":"tcp"}]`, "proto"},
			{"unknown field on browser", `[{"type":"browser","tier":"dedicated","profile":"x"}]`, "profile"},
			{"repo without an owner", `[{"type":"github","repo":"widgets"}]`, "repo"},
			{"repo with a space", `[{"type":"github","repo":"acme/wid gets"}]`, "repo"},
			{"repo with a path segment too many", `[{"type":"github","repo":"acme/widgets/deep"}]`, "repo"},
			// The name becomes a directory component under /workspace, so the
			// two path specials are refused HERE rather than left to git's
			// accident of declining a non-empty clone destination.
			{"repo named ..", `[{"type":"github","repo":"acme/.."}]`, "repo"},
			{"repo named .", `[{"type":"github","repo":"acme/."}]`, "repo"},
			{"owner named ..", `[{"type":"github","repo":"../widgets"}]`, "repo"},
			{"repo starting with a dash", `[{"type":"github","repo":"acme/-widgets"}]`, "repo"},
			{"owner starting with a dash", `[{"type":"github","repo":"-acme/widgets"}]`, "repo"},
			{"explicitly empty base_branch", `[{"type":"github","repo":"a/b","base_branch":""}]`, "base_branch"},
			{"files with no paths", `[{"type":"files","paths":[]}]`, "paths"},
			{"files with a missing paths key", `[{"type":"files"}]`, "paths"},
			{"files with an empty path", `[{"type":"files","paths":["ok",""]}]`, "paths"},
			{"tunnel without a name", `[{"type":"tunnel","target_host":"h","target_port":22}]`, "name"},
			{"tunnel without a host", `[{"type":"tunnel","name":"n","target_port":22}]`, "target_host"},
			{"tunnel port 0", `[{"type":"tunnel","name":"n","target_host":"h","target_port":0}]`, "target_port"},
			{"tunnel port 65536", `[{"type":"tunnel","name":"n","target_host":"h","target_port":65536}]`, "target_port"},
			{"tunnel port negative", `[{"type":"tunnel","name":"n","target_host":"h","target_port":-1}]`, "target_port"},
			{"browser with an unknown tier", `[{"type":"browser","tier":"daily"}]`, "tier"},
			{"browser with no tier", `[{"type":"browser"}]`, "tier"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				got, err := v0wire.ValidateConnectors(json.RawMessage(tc.in))
				if err == nil {
					t.Fatalf("ValidateConnectors(%s) = %+v, want an error", tc.in, got)
				}
				if !strings.Contains(err.Error(), tc.want) {
					t.Errorf("error = %q, want it to mention %q", err, tc.want)
				}
			})
		}
	})

	t.Run("boundary ports are accepted", func(t *testing.T) {
		for _, port := range []int{1, 65535} {
			in := fmt.Sprintf(`{"type":"tunnel","name":"n","target_host":"h","target_port":%d}`, port)
			if _, err := v0wire.ValidateConnectors(connectorArray(in)); err != nil {
				t.Errorf("port %d: %v", port, err)
			}
		}
	})

	t.Run("both browser tiers are accepted", func(t *testing.T) {
		for _, tier := range []string{"dedicated", "extension"} {
			in := fmt.Sprintf(`{"type":"browser","tier":%q}`, tier)
			if _, err := v0wire.ValidateConnectors(connectorArray(in)); err != nil {
				t.Errorf("tier %s: %v", tier, err)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// the environment validators
// ---------------------------------------------------------------------------

func TestValidateEnvironmentBasics(t *testing.T) {
	t.Run("accepts", func(t *testing.T) {
		for _, name := range []string{"dev", "a", "a-b-c", "gpu-env-1", strings.Repeat("a", 64)} {
			if bad := v0wire.ValidateEnvironmentBasics(name, "img:1", 0, 0); bad != "" {
				t.Errorf("name %q: %s", name, bad)
			}
		}
		if bad := v0wire.ValidateEnvironmentBasics("dev", "img:1", 900, 900); bad != "" {
			t.Errorf("timeouts: %s", bad)
		}
	})

	t.Run("rejects", func(t *testing.T) {
		cases := []struct {
			name         string
			envName      string
			image        string
			setup, initT int
			want         string
		}{
			{"an empty name", "", "img:1", 0, 0, "name"},
			{"an uppercase name", "Dev", "img:1", 0, 0, "name"},
			{"an underscore (which is how an id is told from a name)", "dev_1", "img:1", 0, 0, "name"},
			{"a name past 64 characters", strings.Repeat("a", 65), "img:1", 0, 0, "name"},
			{"no image", "dev", "", 0, 0, "image"},
			{"a negative setup timeout", "dev", "img:1", -1, 0, "setup_timeout_sec"},
			{"a negative init timeout", "dev", "img:1", 0, -1, "init_timeout_sec"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				bad := v0wire.ValidateEnvironmentBasics(tc.envName, tc.image, tc.setup, tc.initT)
				if bad == "" {
					t.Fatalf("accepted %+v", tc)
				}
				if !strings.Contains(bad, tc.want) {
					t.Fatalf("message = %q, want it to mention %q", bad, tc.want)
				}
			})
		}
	})
}

func TestValidateCapabilities(t *testing.T) {
	t.Run("accepts", func(t *testing.T) {
		for _, caps := range [][]string{
			nil,
			{},
			{"gpu"},
			{"gpu", "docker.rootless", "arm64", "cuda-12", "a"},
			{strings.Repeat("a", 64)},
		} {
			if err := v0wire.ValidateCapabilities("capabilities", caps); err != nil {
				t.Errorf("%v: %v", caps, err)
			}
		}
	})

	t.Run("rejects", func(t *testing.T) {
		cases := []struct {
			name string
			caps []string
			want string
		}{
			{"uppercase", []string{"GPU"}, "capabilities"},
			{"whitespace", []string{"has space"}, "capabilities"},
			{"empty token", []string{""}, "capabilities"},
			{"a leading dash", []string{"-gpu"}, "capabilities"},
			{"past 64 characters", []string{strings.Repeat("a", 65)}, "capabilities"},
			{"a host prefix", []string{"placement:runner-example"}, "host prefix"},
			{"any colon at all", []string{"snapshot:env_example"}, "host prefix"},
			{"a duplicate", []string{"gpu", "gpu"}, "twice"},
			{"more than the maximum", make([]string, v0wire.MaxCapabilities+1), "at most"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				err := v0wire.ValidateCapabilities("capabilities", tc.caps)
				if err == nil {
					t.Fatalf("accepted %v", tc.caps)
				}
				if !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("error = %q, want it to mention %q", err, tc.want)
				}
			})
		}
	})

	t.Run("the field is named by the caller", func(t *testing.T) {
		err := v0wire.ValidateCapabilities("announced capabilities", []string{"GPU"})
		if err == nil || !strings.Contains(err.Error(), "announced capabilities") {
			t.Fatalf("error = %v, want it to name the caller's field", err)
		}
	})

	t.Run("an over-long token is clipped out of the message", func(t *testing.T) {
		err := v0wire.ValidateCapabilities("capabilities", []string{strings.Repeat("z", 300)})
		if err == nil {
			t.Fatal("accepted a 300-character capability")
		}
		if len(err.Error()) > 200 {
			t.Fatalf("message is %d bytes; the offending token is not clipped", len(err.Error()))
		}
	})
}

func TestEnvironmentRequirementsRoundTrip(t *testing.T) {
	t.Run("the pin comes first and reads back", func(t *testing.T) {
		reqs := v0wire.EnvironmentRequirements("runner-example", []string{"gpu", "docker.rootless"})
		want := []string{"placement:runner-example", "gpu", "docker.rootless"}
		if !slices.Equal(reqs.Capabilities, want) {
			t.Fatalf("capabilities = %v, want %v", reqs.Capabilities, want)
		}
		if got := v0wire.PlacementOf(reqs); got != "runner-example" {
			t.Fatalf("PlacementOf = %q", got)
		}
		if got := v0wire.RenderEnvironment(control.Environment{Requirements: reqs}, "").Capabilities; !slices.Equal(got, []string{"gpu", "docker.rootless"}) {
			t.Fatalf("rendered capabilities = %v", got)
		}
	})

	t.Run("neither half is no requirements at all", func(t *testing.T) {
		reqs := v0wire.EnvironmentRequirements("", nil)
		if reqs.Capabilities != nil {
			t.Fatalf("capabilities = %#v, want nil", reqs.Capabilities)
		}
		if got := v0wire.PlacementOf(reqs); got != "" {
			t.Fatalf("PlacementOf = %q, want empty", got)
		}
	})

	t.Run("a pin with no capabilities, and capabilities with no pin", func(t *testing.T) {
		if got := v0wire.EnvironmentRequirements("runner-example", nil).Capabilities; !slices.Equal(got, []string{"placement:runner-example"}) {
			t.Fatalf("pin only = %v", got)
		}
		reqs := v0wire.EnvironmentRequirements("", []string{"gpu"})
		if !slices.Equal(reqs.Capabilities, []string{"gpu"}) {
			t.Fatalf("capabilities only = %v", reqs.Capabilities)
		}
		if got := v0wire.PlacementOf(reqs); got != "" {
			t.Fatalf("PlacementOf = %q, want empty", got)
		}
	})
}

// ---------------------------------------------------------------------------
// the agent view: GET /v0/agents
// ---------------------------------------------------------------------------

// agentsGolden builds the expected envelope from the PROVIDER TABLE rather
// than from names spelled here. The plan's rule is that a provider is named
// in exactly one place in this repository (controlapp/agents.go), and a
// golden that spelled two of them would be a second copy of the table that
// nobody would remember to update. What is pinned is the shape: the key set,
// the key order, the values, and the fact that a row with no credential still
// carries a "since" key, rendered null.
//
// logged is the index of the one provider the fixture has logged in; every
// other row is "none".
func agentsGolden(t *testing.T, logged int, since string, version uint64, workspaces []string) string {
	t.Helper()
	ws, err := json.Marshal(workspaces)
	if err != nil {
		t.Fatalf("marshal workspaces: %v", err)
	}
	if workspaces == nil {
		ws = []byte("[]")
	}
	rows := make([]string, 0, 4)
	for i, p := range controlapp.AgentProviders() {
		if i == logged {
			rows = append(rows, fmt.Sprintf(
				`{"provider":%q,"status":"logged_in","since":%q,"version":%d,"workspaces":%s}`,
				p.Name, since, version, ws))
			continue
		}
		rows = append(rows, fmt.Sprintf(
			`{"provider":%q,"status":"none","since":null,"version":0,"workspaces":%s}`, p.Name, ws))
	}
	return `{"agents":[` + strings.Join(rows, ",") + `]}`
}

// TestAgentsEnvelopeGoldenJSON pins both statuses in one body: the first
// provider in the table is logged in and carries a version and an RFC 3339
// "since"; every other provider is present, says "none", and renders its
// "since" as null. Every provider in the table appears — the listing answers
// "what could you log in to, and have you", not "what have you logged in to".
func TestAgentsEnvelopeGoldenJSON(t *testing.T) {
	rows := controlapp.AgentProviders()
	if len(rows) < 2 {
		t.Fatalf("the provider table has %d rows; this golden needs a logged-in one and a none one", len(rows))
	}
	statuses := []controlapp.AgentCredentialStatus{
		{Provider: rows[0].Name, Version: 3, UpdatedAt: at(5)},
	}
	env := v0wire.RenderAgents(statuses, []control.WorkspaceID{"ws_example"})
	golden(t, env, agentsGolden(t, 0, "2026-01-02T03:04:05Z", 3, []string{"ws_example"}))
	keySet(t, env, "agents")
	// The key set is identical on both rows, populated or not — the rule
	// every view in this package holds (doc.go).
	keySet(t, env.Agents[0], "provider", "status", "since", "version", "workspaces")
	keySet(t, env.Agents[1], "provider", "status", "since", "version", "workspaces")
}

// A caller with no credential at all still gets every provider, and every
// row's "since" is null — the empty case the CLI's table renders as "-".
func TestAgentsEnvelopeWithNothingLoggedIn(t *testing.T) {
	env := v0wire.RenderAgents(nil, []control.WorkspaceID{"ws_example"})
	golden(t, env, agentsGolden(t, -1, "", 0, []string{"ws_example"}))
	if len(env.Agents) != len(controlapp.AgentProviders()) {
		t.Fatalf("agents = %d rows, want one per provider", len(env.Agents))
	}
}

// A nil workspace list renders as [] and never as null, the rule every list
// on this wire follows.
func TestAgentsEnvelopeNormalizesTheWorkspaceList(t *testing.T) {
	golden(t, v0wire.RenderAgents(nil, nil), agentsGolden(t, -1, "", 0, nil))
}

// Timestamps are UTC RFC 3339 like every other timestamp on this wire, and
// sub-second precision is dropped rather than leaking a store's resolution
// into a rendered body.
func TestAgentsEnvelopeRendersSinceAsUTCSeconds(t *testing.T) {
	rows := controlapp.AgentProviders()
	east := time.FixedZone("TEST", 2*60*60)
	statuses := []controlapp.AgentCredentialStatus{
		{Provider: rows[0].Name, Version: 3, UpdatedAt: at(5).Add(500 * time.Millisecond).In(east)},
	}
	golden(t, v0wire.RenderAgents(statuses, []control.WorkspaceID{"ws_example"}),
		agentsGolden(t, 0, "2026-01-02T03:04:05Z", 3, []string{"ws_example"}))
}

// A status for a provider this build does not have a row for is dropped
// rather than rendered: the table is what the listing enumerates, and a
// stored set for a retired provider is not something a client can act on.
func TestAgentsEnvelopeIgnoresAProviderOutsideTheTable(t *testing.T) {
	env := v0wire.RenderAgents([]controlapp.AgentCredentialStatus{
		{Provider: "provider_example", Version: 9, UpdatedAt: at(5)},
	}, []control.WorkspaceID{"ws_example"})
	golden(t, env, agentsGolden(t, -1, "", 0, []string{"ws_example"}))
}

// The view has nowhere to put a credential, and no row of it aliases another
// row's workspace list.
func TestAgentViewCarriesNoCredentialAndNoSharedSlice(t *testing.T) {
	env := v0wire.RenderAgents(nil, []control.WorkspaceID{"ws_example"})
	if len(env.Agents) < 2 {
		t.Fatalf("agents = %d rows, want at least two", len(env.Agents))
	}
	env.Agents[0].Workspaces[0] = "credential_example"
	if env.Agents[1].Workspaces[0] != "ws_example" {
		t.Fatalf("rows share one workspace slice: %v", env.Agents[1].Workspaces)
	}
	raw, err := json.Marshal(v0wire.RenderAgents([]controlapp.AgentCredentialStatus{
		{Provider: controlapp.AgentProviders()[0].Name, Version: 1, UpdatedAt: at(5)},
	}, []control.WorkspaceID{"ws_example"}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(raw), "credential_example") {
		t.Fatalf("the rendered envelope carries credential bytes: %s", raw)
	}
}

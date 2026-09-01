package control_test

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/terminal"
	"github.com/tokencanopy/rainier/protocol/workspace"
)

// Compile-time assertions: Application embeds the four operation interfaces,
// and each port has a concrete external fake proving a separate module (this
// package — control_test is external, exactly as a Rainier Cloud module would
// be) can implement it without any internal/ import.
var (
	_ control.Sessions     = (control.Application)(nil)
	_ control.Environments = (control.Application)(nil)
	_ control.Fleet        = (control.Application)(nil)
	_ control.Attachments  = (control.Application)(nil)
	_ control.Application  = (*fakeApplication)(nil)

	_ control.Authorizer            = (*fakeAuthorizer)(nil)
	_ control.SessionRepository     = (*fakeSessionRepository)(nil)
	_ control.EnvironmentRepository = (*fakeEnvironmentRepository)(nil)
	_ control.FleetRepository       = (*fakeFleetRepository)(nil)
	_ control.PoolResolver          = (*fakePoolResolver)(nil)
	_ control.RunnerTransport       = (*fakeRunnerTransport)(nil)
	_ control.AttachmentBroker      = (*fakeAttachmentBroker)(nil)
	_ control.EventRecorder         = (*fakeEventRecorder)(nil)
	_ control.Clock                 = fakeClock(nil)
	_ control.IDGenerator           = fakeIDGenerator{}
	_ control.TerminalStream        = (*fakeTerminalStream)(nil)
)

// TestConstructVocabulary constructs every ID, actor kind, execution mode, and
// scope from outside the package. Actors and placement are authoritative
// adapter output: an external caller builds them as values, never by decoding
// a client JSON field.
func TestConstructVocabulary(t *testing.T) {
	_ = control.WorkspaceID("ws_example")
	_ = control.ActorID("act_example")
	_ = control.SessionID("sess_example")
	_ = control.EnvironmentID("env_example")
	_ = control.PoolID("pool_example")
	_ = control.RunnerID("runner_example")
	_ = control.EventID("evt_example")

	actor := control.Actor{ID: "act_example", Kind: control.ActorUser}
	_ = control.Actor{ID: "act_example", Kind: control.ActorService}
	_ = actor

	for _, mode := range []control.ExecutionMode{
		control.ExecutionSelfHosted,
		control.ExecutionDedicated,
		control.ExecutionServerless,
	} {
		_ = mode
	}

	scope := control.Scope{
		WorkspaceID: "ws_example",
		Actor:       actor,
		Placement: control.PlacementScope{
			ProductRegion: "us",
			HomeCell:      "cell-1",
			Mode:          control.ExecutionDedicated,
		},
	}
	_ = scope
}

// TestScopeValidate pins zero and unknown scope validation to ErrInvalid
// without ever asserting free-form error text.
func TestScopeValidate(t *testing.T) {
	valid := func(mode control.ExecutionMode) control.Scope {
		return control.Scope{
			WorkspaceID: "ws_example",
			Actor:       control.Actor{ID: "act_example", Kind: control.ActorUser},
			Placement: control.PlacementScope{
				ProductRegion: "us",
				HomeCell:      "cell-1",
				Mode:          mode,
			},
		}
	}

	// A zero Scope is invalid.
	if err := (control.Scope{}).Validate(); !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("zero scope: got %v, want ErrInvalid", err)
	}

	// A fully populated scope validates; empty workspace and actor IDs do not.
	if err := valid(control.ExecutionDedicated).Validate(); err != nil {
		t.Fatalf("valid scope rejected: %v", err)
	}
	noWS := valid(control.ExecutionDedicated)
	noWS.WorkspaceID = ""
	if err := noWS.Validate(); !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("empty workspace: got %v, want ErrInvalid", err)
	}
	noActor := valid(control.ExecutionDedicated)
	noActor.Actor.ID = ""
	if err := noActor.Validate(); !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("empty actor id: got %v, want ErrInvalid", err)
	}

	// Unknown actor kinds and execution modes.
	badKind := valid(control.ExecutionDedicated)
	badKind.Actor.Kind = control.ActorKind("admin")
	if err := badKind.Validate(); !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("unknown actor kind: got %v, want ErrInvalid", err)
	}
	badMode := valid(control.ExecutionDedicated)
	badMode.Placement.Mode = control.ExecutionMode("hybrid")
	if err := badMode.Validate(); !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("unknown mode: got %v, want ErrInvalid", err)
	}

	// Missing hosted region or cell.
	noRegion := valid(control.ExecutionDedicated)
	noRegion.Placement.ProductRegion = ""
	if err := noRegion.Validate(); !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("missing region: got %v, want ErrInvalid", err)
	}
	noCell := valid(control.ExecutionDedicated)
	noCell.Placement.HomeCell = ""
	if err := noCell.Validate(); !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("missing cell: got %v, want ErrInvalid", err)
	}

	// Self-hosted uses documented installation-local region/cell values and
	// still validates.
	selfHosted := valid(control.ExecutionSelfHosted)
	selfHosted.Placement.ProductRegion = "self-hosted"
	selfHosted.Placement.HomeCell = "default"
	if err := selfHosted.Validate(); err != nil {
		t.Fatalf("self-hosted scope rejected: %v", err)
	}
	for _, mode := range []control.ExecutionMode{control.ExecutionDedicated, control.ExecutionServerless} {
		if err := valid(mode).Validate(); err != nil {
			t.Fatalf("mode %q rejected: %v", mode, err)
		}
	}
}

// TestSentinels pins the seven sentinel identities with errors.Is, so wrapped
// errors branch on them without asserting message text.
func TestSentinels(t *testing.T) {
	for _, sentinel := range []error{
		control.ErrInvalid,
		control.ErrDenied,
		control.ErrNotFound,
		control.ErrConflict,
		control.ErrStale,
		control.ErrUnavailable,
		control.ErrUnsupported,
	} {
		if !errors.Is(sentinel, sentinel) {
			t.Fatalf("%v must match itself", sentinel)
		}
		wrapped := errors.Join(sentinel, errors.New("safe context only"))
		if !errors.Is(wrapped, sentinel) {
			t.Fatalf("wrapped %v must match via errors.Is", sentinel)
		}
	}
}

// TestSessionStateVocabulary proves the session-state vocabulary and its pure
// predicates carry today's semantics byte-for-byte, so the extracted
// application service cannot drift.
func TestSessionStateVocabulary(t *testing.T) {
	values := map[control.SessionState]string{
		control.StateQueued:        "queued",
		control.StateCreating:      "creating",
		control.StateRunning:       "running",
		control.StateSuspendedWarm: "suspended_warm",
		control.StateSuspendedCold: "suspended_cold",
		control.StateCanceled:      "canceled",
		control.StateFailed:        "failed",
		control.StateDead:          "dead",
		control.StateDestroyed:     "destroyed",
	}
	for state, want := range values {
		if string(state) != want {
			t.Errorf("state %q has string value %q, want %q", state, string(state), want)
		}
	}

	terminalStates := map[control.SessionState]bool{
		control.StateQueued: false, control.StateCreating: false, control.StateRunning: false,
		control.StateSuspendedWarm: false, control.StateSuspendedCold: false,
		control.StateCanceled: true, control.StateFailed: true,
		control.StateDead: true, control.StateDestroyed: true,
	}
	for state, want := range terminalStates {
		if got := state.Terminal(); got != want {
			t.Errorf("%s.Terminal() = %v, want %v", state, got, want)
		}
	}

	slotStates := map[control.SessionState]bool{
		control.StateQueued:   false,
		control.StateCreating: true, control.StateRunning: true,
		control.StateSuspendedWarm: true, control.StateSuspendedCold: false,
		control.StateCanceled: false, control.StateFailed: false,
		control.StateDead: false, control.StateDestroyed: false,
	}
	for state, want := range slotStates {
		if got := state.OccupiesSlot(); got != want {
			t.Errorf("%s.OccupiesSlot() = %v, want %v", state, got, want)
		}
	}

	wantOrder := []control.SessionState{
		control.StateQueued, control.StateCreating, control.StateRunning,
		control.StateSuspendedWarm, control.StateSuspendedCold,
	}
	if len(control.NonTerminal) != len(wantOrder) {
		t.Fatalf("NonTerminal has %d states, want %d", len(control.NonTerminal), len(wantOrder))
	}
	for i, want := range wantOrder {
		if control.NonTerminal[i] != want {
			t.Errorf("NonTerminal[%d] = %s, want %s", i, control.NonTerminal[i], want)
		}
	}
}

// TestCreateSessionOptionality pins the contradictory-scratch/environment
// rejection and the nil-vs-empty repository override.
func TestCreateSessionOptionality(t *testing.T) {
	both := control.CreateSession{EnvironmentID: "env_example", Spec: control.PortableSpec{Image: "img"}}
	if err := both.Validate(); !errors.Is(err, control.ErrInvalid) {
		t.Fatalf("environment + scratch spec: got %v, want ErrInvalid", err)
	}
	if err := (control.CreateSession{EnvironmentID: "env_example"}).Validate(); err != nil {
		t.Fatalf("environment-only create rejected: %v", err)
	}
	scratch := control.CreateSession{Spec: control.PortableSpec{Image: "img"}}
	if err := scratch.Validate(); err != nil {
		t.Fatalf("scratch-only create rejected: %v", err)
	}
	// A scratch spec with only a command is not "empty".
	withCmd := control.CreateSession{Spec: control.PortableSpec{Cmd: []string{"make"}}}
	if withCmd.Validate() != nil {
		t.Fatal("scratch spec with only a command must validate")
	}

	// The nil-vs-empty repository override is representable: nil inherits,
	// empty-but-non-nil clones nothing, populated clones exactly these.
	var _ []control.RepoRef = nil // nil: inherit
	_ = []control.RepoRef{}       // empty: clone nothing
	_ = []control.RepoRef{{Repo: "acme/app", BaseBranch: "main"}}
}

// TestConstructPortableModels builds the remaining portable models an
// external consumer constructs with real values: checkpoint metadata,
// requirements, a pool and runner, and the provider-neutral event/usage
// facts.
func TestConstructPortableModels(t *testing.T) {
	_ = control.Checkpoint{Ref: "ckpt_example", Format: "rainier-workspace-v0", Capabilities: []string{"portable"}}
	_ = control.Requirements{Capabilities: []string{"gpu"}, MinCPU: 2, MinMemoryBytes: 1 << 30, MinDiskBytes: 10 << 30}
	_ = control.Pool{ID: "pool_example", Capabilities: []string{"gpu"}, CapacityUsed: 1, CapacityTotal: 4}
	_ = control.Runner{ID: "runner_example", PoolID: "pool_example", Generation: 1, CapacityTotal: 4, Connected: true}
	_ = control.Event{
		ID: "evt_example", WorkspaceID: "ws_example", ActorID: "act_example",
		Action: control.ActionCreate,
		Resource: control.Resource{
			Kind: control.ResourceSession, WorkspaceID: "ws_example",
			ID: "sess_example", CreatorID: "act_example",
		},
		At:    time.Now(),
		Usage: control.Usage{CPUTimeSeconds: 1.5, AgentTokenCount: 128},
	}
}

// create is the ideal external call-site shape this contract exists to make
// possible: a consumer builds a Scope from its own authenticated regional
// state and calls straight through the Application interface.
func create(ctx context.Context, app control.Application, scope control.Scope) (control.Session, error) {
	return app.CreateSession(ctx, scope, control.CreateSession{
		Name:           "investigate",
		EnvironmentID:  "env_example",
		IdempotencyKey: "idem_example",
	})
}

func TestIdealCallSite(t *testing.T) {
	app := &fakeApplication{}
	scope := control.Scope{
		WorkspaceID: "ws_example",
		Actor:       control.Actor{ID: "act_example", Kind: control.ActorUser},
		Placement:   control.PlacementScope{ProductRegion: "us", HomeCell: "cell-1", Mode: control.ExecutionDedicated},
	}
	got, err := create(context.Background(), app, scope)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if got.ID != "sess_example" || got.WorkspaceID != "ws_example" {
		t.Fatalf("unexpected session: %+v", got)
	}
	if app.created.Name != "investigate" || app.created.EnvironmentID != "env_example" || app.created.IdempotencyKey != "idem_example" {
		t.Fatalf("fake received %+v", app.created)
	}
	if app.lastScope != scope {
		t.Fatalf("fake received scope %+v, want %+v", app.lastScope, scope)
	}
}

// ---------------------------------------------------------------------------
// compile-only external fakes — one per port, proving a separate module can
// implement every port with no internal/ import.
// ---------------------------------------------------------------------------

type fakeApplication struct {
	created   control.CreateSession
	lastScope control.Scope
}

func (f *fakeApplication) CreateSession(_ context.Context, sc control.Scope, c control.CreateSession) (control.Session, error) {
	f.created, f.lastScope = c, sc
	return control.Session{ID: "sess_example", WorkspaceID: sc.WorkspaceID}, nil
}
func (f *fakeApplication) GetSession(context.Context, control.Scope, control.SessionID) (control.Session, error) {
	return control.Session{}, nil
}
func (f *fakeApplication) ListSessions(context.Context, control.Scope, control.SessionQuery) (control.SessionPage, error) {
	return control.SessionPage{}, nil
}
func (f *fakeApplication) DeleteSession(context.Context, control.Scope, control.DeleteSession) error {
	return nil
}
func (f *fakeApplication) SuspendSession(context.Context, control.Scope, control.SuspendSession) (control.Session, error) {
	return control.Session{}, nil
}
func (f *fakeApplication) ResumeSession(context.Context, control.Scope, control.ResumeSession) (control.Session, error) {
	return control.Session{}, nil
}
func (f *fakeApplication) SnapshotSession(context.Context, control.Scope, control.SnapshotSession) (control.Checkpoint, error) {
	return control.Checkpoint{}, nil
}
func (f *fakeApplication) CreateEnvironment(context.Context, control.Scope, control.CreateEnvironment) (control.Environment, error) {
	return control.Environment{}, nil
}
func (f *fakeApplication) GetEnvironment(context.Context, control.Scope, control.EnvironmentID) (control.Environment, error) {
	return control.Environment{}, nil
}
func (f *fakeApplication) ListEnvironments(context.Context, control.Scope, control.EnvironmentQuery) (control.EnvironmentPage, error) {
	return control.EnvironmentPage{}, nil
}
func (f *fakeApplication) UpdateEnvironment(context.Context, control.Scope, control.UpdateEnvironment) (control.Environment, error) {
	return control.Environment{}, nil
}
func (f *fakeApplication) DeleteEnvironment(context.Context, control.Scope, control.DeleteEnvironment) error {
	return nil
}
func (f *fakeApplication) RegisterRunner(context.Context, control.RunnerRegistration) (control.RunnerRegistrationResult, error) {
	return control.RunnerRegistrationResult{}, nil
}
func (f *fakeApplication) ReconcileRunner(context.Context, control.RunnerSnapshot) (control.ReconcileResult, error) {
	return control.ReconcileResult{}, nil
}
func (f *fakeApplication) ApplyRunnerEvent(context.Context, control.RunnerEvent) error { return nil }
func (f *fakeApplication) ListRunners(context.Context, control.Scope, control.RunnerQuery) (control.RunnerPage, error) {
	return control.RunnerPage{}, nil
}
func (f *fakeApplication) AttachTerminal(context.Context, control.Scope, control.AttachTerminal, control.TerminalStream) error {
	return nil
}
func (f *fakeApplication) WorkspaceDiff(context.Context, control.Scope, control.WorkspaceDiff) (workspace.DiffAnswer, error) {
	return workspace.DiffAnswer{}, nil
}
func (f *fakeApplication) PushWorkspace(context.Context, control.Scope, control.PushWorkspace) error {
	return nil
}
func (f *fakeApplication) PullWorkspace(context.Context, control.Scope, control.PullWorkspace) error {
	return nil
}

type fakeAuthorizer struct{}

func (fakeAuthorizer) Authorize(context.Context, control.Scope, control.Action, control.Resource) error {
	return nil
}

type fakeSessionRepository struct{}

func (fakeSessionRepository) CreateSession(context.Context, control.WorkspaceID, control.Session) (control.Session, error) {
	return control.Session{}, nil
}
func (fakeSessionRepository) GetSession(context.Context, control.WorkspaceID, control.SessionID) (control.Session, error) {
	return control.Session{}, nil
}
func (fakeSessionRepository) SessionByIDem(context.Context, control.WorkspaceID, control.ActorID, string) (control.Session, error) {
	return control.Session{}, nil
}
func (fakeSessionRepository) ListSessions(context.Context, control.WorkspaceID, control.SessionQuery) ([]control.Session, string, error) {
	return nil, "", nil
}
func (fakeSessionRepository) Transition(context.Context, control.WorkspaceID, control.SessionID, []control.SessionState, control.SessionState, control.TransitionOpts) error {
	return nil
}
func (fakeSessionRepository) SetSessionSetupHash(context.Context, control.WorkspaceID, control.SessionID, string) error {
	return nil
}
func (fakeSessionRepository) SetChildExitCode(context.Context, control.WorkspaceID, control.SessionID, int) error {
	return nil
}

type fakeEnvironmentRepository struct{}

func (fakeEnvironmentRepository) CreateEnvironment(context.Context, control.WorkspaceID, control.Environment) (control.Environment, error) {
	return control.Environment{}, nil
}
func (fakeEnvironmentRepository) GetEnvironment(context.Context, control.WorkspaceID, control.EnvironmentID) (control.Environment, error) {
	return control.Environment{}, nil
}
func (fakeEnvironmentRepository) ListEnvironments(context.Context, control.WorkspaceID, control.EnvironmentQuery) ([]control.Environment, string, error) {
	return nil, "", nil
}
func (fakeEnvironmentRepository) UpdateEnvironment(context.Context, control.WorkspaceID, control.Environment) (control.Environment, error) {
	return control.Environment{}, nil
}
func (fakeEnvironmentRepository) DeleteEnvironment(context.Context, control.WorkspaceID, control.EnvironmentID) error {
	return nil
}
func (fakeEnvironmentRepository) CountSessionsByEnvironment(context.Context, control.WorkspaceID, control.EnvironmentID, []control.SessionState) (int, error) {
	return 0, nil
}
func (fakeEnvironmentRepository) SetEnvironmentSnapshot(context.Context, control.WorkspaceID, control.EnvironmentID, string, string, control.RunnerID) error {
	return nil
}

type fakeFleetRepository struct{}

func (fakeFleetRepository) UpsertRunner(context.Context, control.PoolID, control.Runner) error {
	return nil
}
func (fakeFleetRepository) SetRunnerConnected(context.Context, control.PoolID, control.RunnerID, bool) error {
	return nil
}
func (fakeFleetRepository) ListRunners(context.Context, control.PoolID) ([]control.Runner, error) {
	return nil, nil
}
func (fakeFleetRepository) SessionsOnRunner(context.Context, control.PoolID, control.RunnerID, []control.SessionState) ([]control.Session, error) {
	return nil, nil
}
func (fakeFleetRepository) OldestQueued(context.Context, control.PoolID) ([]control.Session, error) {
	return nil, nil
}

type fakePoolResolver struct{}

func (fakePoolResolver) EligiblePools(context.Context, control.Scope, control.Requirements) ([]control.Pool, error) {
	return nil, nil
}

type fakeRunnerTransport struct{}

func (fakeRunnerTransport) Dispatch(_ context.Context, _ control.PoolID, _ control.RunnerID, m runner.ToRunner) (runner.FromRunner, error) {
	return runner.FromRunner{}, nil
}
func (fakeRunnerTransport) Connected(control.PoolID, control.RunnerID) bool { return true }

type fakeAttachmentBroker struct{}

func (fakeAttachmentBroker) Attach(context.Context, control.AttachTarget, control.TerminalStream) error {
	return nil
}

type fakeEventRecorder struct{}

func (fakeEventRecorder) Record(context.Context, control.Event) error { return nil }

type fakeClock func() time.Time

func (f fakeClock) Now() time.Time { return f() }

type fakeIDGenerator struct{}

func (fakeIDGenerator) NewSessionID() control.SessionID         { return "sess_example" }
func (fakeIDGenerator) NewEnvironmentID() control.EnvironmentID { return "env_example" }
func (fakeIDGenerator) NewEventID() control.EventID             { return "evt_example" }

type fakeTerminalStream struct{}

func (fakeTerminalStream) Receive(context.Context) (terminal.ClientMessage, error) {
	return terminal.ClientMessage{}, nil
}
func (fakeTerminalStream) Send(context.Context, terminal.ServerMessage) error { return nil }
func (fakeTerminalStream) Close(error) error                                  { return nil }

// ---------------------------------------------------------------------------
// AST guards: provider and sensitive-host vocabulary, and import hygiene.
// ---------------------------------------------------------------------------

func controlSourceDir(t *testing.T) string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Dir(file)
}

// parsedControl holds the imports and exported names of the control package,
// read from its source so the guards run against the real surface rather than
// a hand-maintained list.
type parsedControl struct {
	imports []string
	names   []string
}

func parseControl(t *testing.T) parsedControl {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(controlSourceDir(t), "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	imports := map[string]bool{}
	names := map[string]bool{}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", file, err)
		}
		for _, imp := range f.Imports {
			imports[strings.Trim(imp.Path.Value, `"`)] = true
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.TypeSpec:
				if ast.IsExported(x.Name.Name) {
					names[x.Name.Name] = true
				}
				switch t := x.Type.(type) {
				case *ast.StructType:
					for _, field := range t.Fields.List {
						for _, name := range field.Names {
							if ast.IsExported(name.Name) {
								names[name.Name] = true
							}
						}
					}
				case *ast.InterfaceType:
					for _, m := range t.Methods.List {
						for _, name := range m.Names {
							if ast.IsExported(name.Name) {
								names[name.Name] = true
							}
						}
					}
				}
			case *ast.FuncDecl:
				if x.Recv == nil && ast.IsExported(x.Name.Name) {
					names[x.Name.Name] = true
				}
			case *ast.ValueSpec:
				for _, name := range x.Names {
					if ast.IsExported(name.Name) {
						names[name.Name] = true
					}
				}
			}
			return true
		})
	}
	return parsedControl{imports: sortedKeys(imports), names: sortedKeys(names)}
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// normalize strips everything but lowercase letters and digits, so an
// identifier like "DiskID" is compared against the phrase "disk ID" the same
// way a hypothetical "DiskID" field would be spelled.
func normalize(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// forbiddenPhrases is the provider and sensitive-host vocabulary the public
// control surface must not contain. Provider terms name GCP projects, AWS
// accounts, Azure subscriptions, OCI compartments, machine types, clusters,
// native zones, and disk IDs. Sensitive terms name hosted concepts — email,
// roles, tokens, secret values, prices, charges — that belong to adapters.
var forbiddenPhrases = []string{
	// provider vocabulary
	"gcp", "aws", "azure", "oracle", "hetzner", "netcup",
	"project", "instancetype", "machinetype", "cluster", "nativezone", "diskid",
	// sensitive-host vocabulary
	"email", "role", "token", "secret", "price", "charge",
}

// allowedNames are the documented exceptions: SecretRefs is a secret
// reference (a name, never a value); AgentTokenCount is a provider-neutral
// usage fact (an LLM token count, not a credential); and the region/mode
// identifiers are Rainier product concepts, not provider or hosted ones.
var allowedNames = map[string]string{
	"SecretRefs":          "portable secret reference, never a value",
	"Checkpoint":          "opaque portable snapshot reference",
	"AgentTokenCount":     "provider-neutral usage fact, not a credential",
	"ProductRegion":       "Rainier product region, not a provider-native zone",
	"HomeCell":            "Rainier home cell",
	"PlacementScope":      "Rainier placement context",
	"ExecutionMode":       "Rainier execution mode",
	"ExecutionSelfHosted": "Rainier execution mode",
	"ExecutionDedicated":  "Rainier execution mode",
	"ExecutionServerless": "Rainier execution mode",
}

func matchesForbidden(name string) (string, bool) {
	n := normalize(name)
	for _, phrase := range forbiddenPhrases {
		if strings.Contains(n, phrase) {
			return phrase, true
		}
	}
	return "", false
}

// TestControlHasNoProviderOrSensitiveVocabulary rejects any exported name —
// type, const, var, func, struct field, or interface method — containing
// provider or sensitive-host vocabulary, outside the documented allowlist.
func TestControlHasNoProviderOrSensitiveVocabulary(t *testing.T) {
	for _, name := range parseControl(t).names {
		if reason, ok := allowedNames[name]; ok {
			t.Logf("allowed: %s (%s)", name, reason)
			continue
		}
		if phrase, bad := matchesForbidden(name); bad {
			t.Errorf("exported name %q contains forbidden vocabulary %q", name, phrase)
		}
	}
}

// TestVocabularyGuardIsLive proves the allowlist exists for names that would
// otherwise trip the guard: SecretRefs carries the word "secret", and
// AgentTokenCount the word "token".
func TestVocabularyGuardIsLive(t *testing.T) {
	for name, want := range map[string]bool{
		"SecretRefs":      true,
		"AgentTokenCount": true,
		"Checkpoint":      false,
		"ProductRegion":   false,
	} {
		_, matches := matchesForbidden(name)
		if matches != want {
			t.Errorf("%s: matchesForbidden = %v, want %v", name, matches, want)
		}
	}
}

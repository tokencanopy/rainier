package controlapp

import (
	"encoding/base64"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/tokencanopy/rainier/control"
)

// agentEnvEntry mirrors, in the test's own words, the shape AgentsEnv encodes
// into RAINIER_AGENTS_B64. It is deliberately a second declaration rather than
// a reference to the production one: sessiond decodes these bytes with a type
// of its own, and this test is the place where the two spellings are proven to
// agree.
type agentEnvEntry struct {
	Provider string   `json:"provider"`
	Dir      string   `json:"dir"`
	Files    []string `json:"files"`
}

// withTestAgentProvider turns the synthetic provider on for one test and off
// again afterwards. The toggle is a package-level variable a host sets once at
// startup, so a test that flips it must put it back.
func withTestAgentProvider(t *testing.T) {
	t.Helper()
	prev := EnableTestAgentProvider
	EnableTestAgentProvider = true
	t.Cleanup(func() { EnableTestAgentProvider = prev })
}

// TestAgentProvidersNeverSpellASecretPath guards the one property that makes
// the provider table safe to treat as data: a Files entry names a file inside
// the provider's own directory and nothing else, so no row can ever point the
// sync at a path outside the agent home. It also pins that the synthetic
// provider is invisible unless a host asks for it, and that the table is a
// fresh slice on every call — a caller that sorted or appended to it must not
// be able to change what the next caller sees.
func TestAgentProvidersNeverSpellASecretPath(t *testing.T) {
	seenEnv := map[string]string{}
	seenName := map[string]bool{}
	for _, p := range AgentProviders() {
		if p.Name == "" {
			t.Fatalf("a provider row has no name: %+v", p)
		}
		if seenName[p.Name] {
			t.Fatalf("duplicate provider name %q", p.Name)
		}
		seenName[p.Name] = true
		if p.HomeEnv == "" {
			t.Fatalf("provider %q has no home variable", p.Name)
		}
		if other, dup := seenEnv[p.HomeEnv]; dup {
			t.Fatalf("providers %q and %q share the variable %q", other, p.Name, p.HomeEnv)
		}
		seenEnv[p.HomeEnv] = p.Name
		if len(p.Files) == 0 {
			t.Fatalf("provider %q allowlists no file", p.Name)
		}
		for _, f := range p.Files {
			if f == "" || strings.ContainsAny(f, `/\`) || strings.Contains(f, "..") {
				t.Fatalf("provider %q names a path, not a file: %q", p.Name, f)
			}
		}
		if len(p.LoginCmd) == 0 {
			t.Fatalf("provider %q has no login command", p.Name)
		}
		for _, h := range p.Egress {
			if h == "" || strings.Contains(h, "/") {
				t.Fatalf("provider %q names a URL, not a host: %q", p.Name, h)
			}
		}
	}

	if slices.ContainsFunc(AgentProviders(), func(p AgentProvider) bool { return p.Name == "test" }) {
		t.Fatal("the synthetic provider is present without a host enabling it")
	}
	withTestAgentProvider(t)
	if !slices.ContainsFunc(AgentProviders(), func(p AgentProvider) bool { return p.Name == "test" }) {
		t.Fatal("the synthetic provider is absent after the host enabled it")
	}

	// The table is data the caller owns a copy of, not a package variable it
	// can reach into.
	first := AgentProviders()
	first[0].Name = "mutated"
	first[0].Files[0] = "../escape"
	if got := AgentProviders(); got[0].Name == "mutated" || got[0].Files[0] == "../escape" {
		t.Fatalf("a caller mutated the table: %+v", got[0])
	}
}

// TestAgentHomeVolumeIsOpaqueAndStable pins why the volume is hashed at all: a
// docker volume name is visible to anyone with a shell on the runner, so it
// must not print an account or a workspace, and it must still be the same name
// every time the same person boots a session in the same workspace — that
// sameness IS the "log in once" promise.
func TestAgentHomeVolumeIsOpaqueAndStable(t *testing.T) {
	const ws, creator = control.WorkspaceID("ws_example"), control.ActorID("user_example")
	name := AgentHomeVolume(ws, creator)
	if name != AgentHomeVolume(ws, creator) {
		t.Fatal("the same workspace and creator produced two names")
	}
	if !strings.HasPrefix(name, "rainier-agents-") {
		t.Fatalf("volume name = %q, want the rainier-agents- prefix", name)
	}
	if got := len(strings.TrimPrefix(name, "rainier-agents-")); got != 16 {
		t.Fatalf("volume key is %d characters, want 16", got)
	}
	if strings.Contains(name, string(ws)) || strings.Contains(name, string(creator)) {
		t.Fatalf("volume name %q spells an identifier", name)
	}
	if same := AgentHomeVolume(ws, "user_other"); same == name {
		t.Fatal("two creators share one home in the same workspace")
	}
	if same := AgentHomeVolume("ws_other", creator); same == name {
		t.Fatal("one creator's two workspaces share one home")
	}
	// The separator matters: without it (ws_a, bc) and (ws_ab, c) would hash
	// to one volume and two people would share a credential set.
	if AgentHomeVolume("ws_a", "bc") == AgentHomeVolume("ws_ab", "c") {
		t.Fatal("the workspace and creator are concatenated without a separator")
	}
}

// TestCreateSpecCarriesTheHome is the create half of the feature: every
// session a person starts gets their home mounted, every provider is pointed
// at its own subdirectory of it, the manifest sessiond reads names the same
// directories and files, and the hosts an agent's login and inference reach
// are on the egress list. The last case pins the precedence rule the
// scheduler already documents: the resolver's environment is applied last, so
// a host that wants a different directory for one provider still wins.
func TestCreateSpecCarriesTheHome(t *testing.T) {
	withTestAgentProvider(t)
	fx := newFleetFixture(t)
	row := control.Session{ID: "sess_example", WorkspaceID: "ws_example", CreatorID: "user_example",
		Spec: control.PortableSpec{Image: "registry.example.invalid/base@sha256:0000"}}

	spec, fail := fx.service.createSpec(fleetCtx, row, nil)
	if fail != "" {
		t.Fatalf("createSpec failed: %s", fail)
	}
	if spec.Home == nil {
		t.Fatal("the create carries no agent home")
	}
	if want := AgentHomeVolume("ws_example", "user_example"); spec.Home.Volume != want {
		t.Fatalf("home volume = %q, want %q", spec.Home.Volume, want)
	}
	if spec.Home.Path != HomeMountPath {
		t.Fatalf("home path = %q, want %q", spec.Home.Path, HomeMountPath)
	}

	providers := AgentProviders()
	for _, p := range providers {
		want := HomeMountPath + "/" + p.Name
		if got := spec.Env[p.HomeEnv]; got != want {
			t.Fatalf("env %s = %q, want %q", p.HomeEnv, got, want)
		}
		for _, h := range p.Egress {
			if n := slices.Index(spec.EgressAllow, h); n < 0 {
				t.Fatalf("egress list is missing %q: %v", h, spec.EgressAllow)
			}
			if n := countHost(spec.EgressAllow, h); n != 1 {
				t.Fatalf("egress list names %q %d times: %v", h, n, spec.EgressAllow)
			}
		}
	}

	raw, err := base64.StdEncoding.DecodeString(spec.Env["RAINIER_AGENTS_B64"])
	if err != nil {
		t.Fatalf("RAINIER_AGENTS_B64 is not base64: %v", err)
	}
	var entries []agentEnvEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		t.Fatalf("RAINIER_AGENTS_B64 is not the manifest: %v", err)
	}
	if len(entries) != len(providers) {
		t.Fatalf("manifest has %d entries, want %d", len(entries), len(providers))
	}
	for i, p := range providers {
		if entries[i].Provider != p.Name {
			t.Fatalf("manifest entry %d is %q, want %q", i, entries[i].Provider, p.Name)
		}
		if want := HomeMountPath + "/" + p.Name; entries[i].Dir != want {
			t.Fatalf("manifest dir for %q = %q, want %q", p.Name, entries[i].Dir, want)
		}
		if !slices.Equal(entries[i].Files, p.Files) {
			t.Fatalf("manifest files for %q = %v, want %v", p.Name, entries[i].Files, p.Files)
		}
	}

	// A session nobody created — there is no such thing today, but the field
	// is optional in the contract — gets none of it: no volume, no variables
	// pointing at a directory that was never mounted, and no egress hole for
	// a login that cannot happen.
	anon := row
	anon.CreatorID = ""
	spec, fail = fx.service.createSpec(fleetCtx, anon, nil)
	if fail != "" {
		t.Fatalf("createSpec failed: %s", fail)
	}
	if spec.Home != nil {
		t.Fatalf("a create with no creator carries a home: %+v", spec.Home)
	}
	if _, ok := spec.Env[agentsEnvVar]; ok {
		t.Fatalf("a create with no creator carries the agent manifest: %v", spec.Env)
	}
	for _, p := range providers {
		if _, ok := spec.Env[p.HomeEnv]; ok {
			t.Fatalf("a create with no creator sets %s", p.HomeEnv)
		}
		for _, h := range p.Egress {
			if slices.Contains(spec.EgressAllow, h) {
				t.Fatalf("a create with no creator opened %q: %v", h, spec.EgressAllow)
			}
		}
	}

	// Last wins: the resolver's environment is merged over the table's.
	override := "/rainier/agents/elsewhere"
	first := providers[0]
	fx = newFleetFixtureWithResolver(t, &fleetFakeResolver{material: LaunchMaterial{
		Environment: map[string]string{first.HomeEnv: override}}})
	spec, fail = fx.service.createSpec(fleetCtx, row, nil)
	if fail != "" {
		t.Fatalf("createSpec failed: %s", fail)
	}
	if got := spec.Env[first.HomeEnv]; got != override {
		t.Fatalf("env %s = %q, want the resolver's %q", first.HomeEnv, got, override)
	}
}

func countHost(hosts []string, want string) int {
	n := 0
	for _, h := range hosts {
		if h == want {
			n++
		}
	}
	return n
}

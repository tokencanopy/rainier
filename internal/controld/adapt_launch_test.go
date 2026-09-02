package controld

import (
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/controlapp"
)

// launchTestEnv is the control-shaped environment the resolver is handed at
// dispatch. It is built here rather than converted from a store row so this
// test depends on nothing but the control model and the adapter under test.
func launchTestEnv(secretRefs []string, connectors ...control.Connector) control.Environment {
	return control.Environment{
		ID:          "env_example",
		WorkspaceID: installWorkspace,
		Name:        "example",
		SecretRefs:  secretRefs,
		Connectors:  connectors,
	}
}

func githubConnectorRaw(t *testing.T, repo, baseBranch string) control.Connector {
	t.Helper()
	raw, err := json.Marshal(map[string]string{"type": "github", "repo": repo, "base_branch": baseBranch})
	if err != nil {
		t.Fatalf("marshal connector: %v", err)
	}
	return control.Connector{Type: "github", Raw: raw}
}

func TestLaunchMaterialResolvesReposAttributionAndSecrets(t *testing.T) {
	ctx := context.Background()
	st := NewMemStore()
	u, err := st.UpsertUser(ctx, 12345, "octo-example", "member")
	if err != nil {
		t.Fatalf("UpsertUser: %v", err)
	}
	key := testSecretsKey
	ciphertext, nonce, err := Seal(key, []byte("s3cr3t-value"))
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := st.PutSecret(ctx, "API_TOKEN", ciphertext, nonce); err != nil {
		t.Fatalf("PutSecret: %v", err)
	}
	env := launchTestEnv([]string{"API_TOKEN"}, githubConnectorRaw(t, "acme/app", "main"))
	row := control.Session{
		ID: "sess_example", WorkspaceID: installWorkspace, CreatorID: control.ActorID(u.ID),
		Name: "investigate", EnvironmentID: "env_example",
	}

	m, err := (launchMaterial{st: st, key: key}).ResolveLaunchMaterial(ctx, row, &env)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Repos) != 1 || m.Repos[0].Owner != "acme" || m.Repos[0].Name != "app" ||
		m.Repos[0].BaseBranch != "main" || m.Repos[0].SessionBranch != "rainier/investigate" || m.Repos[0].Dir != "app" {
		t.Fatalf("repos = %+v", m.Repos)
	}
	if m.GitAuthorName != "octo-example" || m.GitAuthorEmail != "12345+octo-example@users.noreply.github.com" {
		t.Fatalf("attribution = %q <%s>", m.GitAuthorName, m.GitAuthorEmail)
	}
	if !reflect.DeepEqual(m.EgressAllow, gitEgressHosts) {
		t.Fatalf("egress = %v", m.EgressAllow)
	}
	if m.Environment["API_TOKEN"] != "s3cr3t-value" {
		t.Fatal("secret not opened")
	}
	// The material must not hand the package's own list out for mutation.
	m.EgressAllow[0] = "mutated.invalid"
	if slices.Contains(gitEgressHosts, "mutated.invalid") {
		t.Fatal("material aliased gitEgressHosts")
	}
}

// TestLaunchMaterialSessionReposOverrideConnectors pins the nil-vs-empty rule
// the whole clone path turns on: a session that stored an explicit empty repo
// list clones nothing, and therefore needs neither attribution nor git egress,
// no matter what its environment's connectors say.
func TestLaunchMaterialSessionReposOverrideConnectors(t *testing.T) {
	ctx := context.Background()
	st := NewMemStore()
	u, err := st.UpsertUser(ctx, 12345, "octo-example", "member")
	if err != nil {
		t.Fatalf("UpsertUser: %v", err)
	}
	env := launchTestEnv(nil, githubConnectorRaw(t, "acme/app", "main"))
	row := control.Session{
		ID: "sess_example", WorkspaceID: installWorkspace, CreatorID: control.ActorID(u.ID),
		Name: "investigate", EnvironmentID: "env_example",
		Spec: control.PortableSpec{Repos: []control.RepoRef{}},
	}

	m, err := (launchMaterial{st: st, key: testSecretsKey}).ResolveLaunchMaterial(ctx, row, &env)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Repos) != 0 {
		t.Fatalf("repos = %+v, want none", m.Repos)
	}
	if m.GitAuthorName != "" || m.GitAuthorEmail != "" {
		t.Fatalf("attribution = %q <%s>, want none", m.GitAuthorName, m.GitAuthorEmail)
	}
	if m.EgressAllow != nil {
		t.Fatalf("egress = %v, want none", m.EgressAllow)
	}

	// The other half of the rule: a non-empty override beats the connectors.
	row.Spec.Repos = []control.RepoRef{{Repo: "acme/other", BaseBranch: "trunk"}}
	m, err = (launchMaterial{st: st, key: testSecretsKey}).ResolveLaunchMaterial(ctx, row, &env)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Repos) != 1 || m.Repos[0].Name != "other" || m.Repos[0].BaseBranch != "trunk" {
		t.Fatalf("repos = %+v, want the session's own override", m.Repos)
	}
}

// TestLaunchMaterialMissingSecretNamesTheRefNotTheValue pins the fail-closed
// rule and the one thing the error may say: the reference's NAME. No value can
// appear, because no value was ever loaded.
func TestLaunchMaterialMissingSecretNamesTheRefNotTheValue(t *testing.T) {
	ctx := context.Background()
	st := NewMemStore()
	u, err := st.UpsertUser(ctx, 12345, "octo-example", "member")
	if err != nil {
		t.Fatalf("UpsertUser: %v", err)
	}
	ciphertext, nonce, err := Seal(testSecretsKey, []byte("s3cr3t-value"))
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := st.PutSecret(ctx, "PRESENT_TOKEN", ciphertext, nonce); err != nil {
		t.Fatalf("PutSecret: %v", err)
	}
	env := launchTestEnv([]string{"PRESENT_TOKEN", "API_TOKEN"})
	row := control.Session{
		ID: "sess_example", WorkspaceID: installWorkspace, CreatorID: control.ActorID(u.ID),
		Name: "investigate", EnvironmentID: "env_example",
	}

	m, err := (launchMaterial{st: st, key: testSecretsKey}).ResolveLaunchMaterial(ctx, row, &env)
	if err == nil {
		t.Fatalf("want an error for a dangling secret ref, got material %+v", m)
	}
	if m.Environment != nil {
		t.Fatalf("a failed resolve must return no material, got %d variables", len(m.Environment))
	}
	if !strings.Contains(err.Error(), "API_TOKEN") {
		t.Fatalf("error must name the reference: %q", err)
	}
	// Deliberately not printed: an error that failed this check is one that
	// carries the material, and repeating it in the test log would leak it a
	// second time.
	if strings.Contains(err.Error(), "s3cr3t-value") || strings.Contains(err.Error(), "PRESENT_TOKEN") {
		t.Fatal("the error carried a secret value or an unrelated reference name")
	}
}

// TestLaunchMaterialScratchSessionHasNoMaterial pins the case that must cost
// nothing: a scratch session has no environment, so there is no connector to
// decode, no owner to read, and no secret to open.
func TestLaunchMaterialScratchSessionHasNoMaterial(t *testing.T) {
	ctx := context.Background()
	st := NewMemStore()
	row := control.Session{
		ID: "sess_example", WorkspaceID: installWorkspace, CreatorID: "usr_absent",
		Name: "investigate",
	}

	m, err := (launchMaterial{st: st, key: testSecretsKey}).ResolveLaunchMaterial(ctx, row, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(m, controlapp.LaunchMaterial{}) {
		t.Fatalf("material = %+v, want the zero value", m)
	}
}

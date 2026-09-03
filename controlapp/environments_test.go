package controlapp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"testing"

	"github.com/tokencanopy/rainier/control"
)

var _ control.Environments = (*EnvironmentService)(nil)

// expectedSetupHash mirrors the frozen setup identity: sha256(image+"\x00"+setup).
func expectedSetupHash(image, setup string) string {
	h := sha256.Sum256([]byte(image + "\x00" + setup))
	return hex.EncodeToString(h[:])
}

type environmentFixture struct {
	svc    *EnvironmentService
	repo   *sessionStubEnvironmentRepo
	auth   *sessionStubAuthorizer
	events *sessionStubEventRecorder
	log    *sessionCallLog
}

func newEnvironmentFixture(t *testing.T) *environmentFixture {
	t.Helper()
	log := &sessionCallLog{}
	repo := newSessionStubEnvironmentRepo(log)
	auth := &sessionStubAuthorizer{log: log}
	events := &sessionStubEventRecorder{log: log}
	svc, err := NewEnvironmentService(EnvironmentOptions{
		Authorizer:   auth,
		Environments: repo,
		Events:       events,
		Clock:        sessionStubClock{now: sessionFixedNow},
		IDs:          &sessionStubIDs{log: log, sessionID: "sess_example", envID: "env_example", eventID: "evt_example"},
		UnitOfWork:   directUOW{},
	})
	if err != nil {
		t.Fatalf("NewEnvironmentService: %v", err)
	}
	return &environmentFixture{svc: svc, repo: repo, auth: auth, events: events, log: log}
}

func validEnvironmentOptions() EnvironmentOptions {
	return EnvironmentOptions{
		Authorizer:   &sessionStubAuthorizer{},
		Environments: newSessionStubEnvironmentRepo(nil),
		Events:       &sessionStubEventRecorder{},
		Clock:        sessionStubClock{now: sessionFixedNow},
		IDs:          &sessionStubIDs{sessionID: "sess_example", envID: "env_example", eventID: "evt_example"},
		UnitOfWork:   directUOW{},
	}
}

func TestNewEnvironmentServiceRequiresEveryDependency(t *testing.T) {
	if _, err := NewEnvironmentService(validEnvironmentOptions()); err != nil {
		t.Fatalf("NewEnvironmentService(valid): %v", err)
	}
	tests := []struct {
		name string
		mut  func(*EnvironmentOptions)
	}{
		{"authorizer", func(o *EnvironmentOptions) { o.Authorizer = nil }},
		{"environments", func(o *EnvironmentOptions) { o.Environments = nil }},
		{"events", func(o *EnvironmentOptions) { o.Events = nil }},
		{"clock", func(o *EnvironmentOptions) { o.Clock = nil }},
		{"ids", func(o *EnvironmentOptions) { o.IDs = nil }},
		{"unit of work", func(o *EnvironmentOptions) { o.UnitOfWork = nil }},
	}
	for _, tt := range tests {
		o := validEnvironmentOptions()
		tt.mut(&o)
		if _, err := NewEnvironmentService(o); !errors.Is(err, control.ErrInvalid) {
			t.Fatalf("missing %s: got %v, want ErrInvalid", tt.name, err)
		}
	}
}

func TestCreateEnvironmentPinsSetupHash(t *testing.T) {
	f := newEnvironmentFixture(t)
	got, err := f.svc.CreateEnvironment(context.Background(), sessionTestScope(), control.CreateEnvironment{
		Name: "standard", Image: "registry.example.invalid/rainier@sha256:0000",
		Setup: "make bootstrap", EgressAllow: []string{"example.com"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "env_example" || got.WorkspaceID != "ws_example" || got.SetupHash == "" {
		t.Fatalf("environment = %+v", got)
	}
	if want := expectedSetupHash("registry.example.invalid/rainier@sha256:0000", "make bootstrap"); got.SetupHash != want {
		t.Fatalf("setup hash = %q, want %q", got.SetupHash, want)
	}
	if !slices.Equal(got.EgressAllow, []string{"example.com"}) {
		t.Fatalf("egress = %#v", got.EgressAllow)
	}
	if !got.CreatedAt.Equal(sessionFixedNow) || !got.UpdatedAt.Equal(sessionFixedNow) {
		t.Fatalf("timestamps = %v/%v", got.CreatedAt, got.UpdatedAt)
	}
	if len(f.events.events) != 1 || f.events.events[0].Action != control.ActionCreate {
		t.Fatalf("events = %+v", f.events.events)
	}
}

func TestCreateEnvironmentInvalidInput(t *testing.T) {
	t.Run("invalid scope touches no port", func(t *testing.T) {
		f := newEnvironmentFixture(t)
		if _, err := f.svc.CreateEnvironment(context.Background(), control.Scope{}, control.CreateEnvironment{Name: "x", Image: "i"}); !errors.Is(err, control.ErrInvalid) {
			t.Fatalf("got %v, want ErrInvalid", err)
		}
		if len(f.log.snapshot()) != 0 {
			t.Fatalf("invalid scope touched ports: %v", f.log.snapshot())
		}
	})

	tests := []struct {
		name string
		mut  func(*control.CreateEnvironment)
	}{
		{"empty name", func(c *control.CreateEnvironment) { c.Name = "" }},
		{"empty image", func(c *control.CreateEnvironment) { c.Image = "" }},
		{"negative setup timeout", func(c *control.CreateEnvironment) { c.SetupTimeoutSec = -1 }},
		{"negative init timeout", func(c *control.CreateEnvironment) { c.InitTimeoutSec = -1 }},
		{"negative min cpu", func(c *control.CreateEnvironment) { c.Requirements.MinCPU = -1 }},
		{"negative min memory", func(c *control.CreateEnvironment) { c.Requirements.MinMemoryBytes = -1 }},
		{"negative min disk", func(c *control.CreateEnvironment) { c.Requirements.MinDiskBytes = -1 }},
		{"empty capability", func(c *control.CreateEnvironment) { c.Requirements.Capabilities = []string{""} }},
		{"empty secret ref", func(c *control.CreateEnvironment) { c.SecretRefs = []string{""} }},
		{"empty connector type", func(c *control.CreateEnvironment) {
			c.Connectors = []control.Connector{{Type: "", Raw: json.RawMessage(`{}`)}}
		}},
		{"invalid connector raw", func(c *control.CreateEnvironment) {
			c.Connectors = []control.Connector{{Type: "github", Raw: json.RawMessage(`{`)}}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newEnvironmentFixture(t)
			cmd := control.CreateEnvironment{Name: "standard", Image: "registry.example.invalid/rainier@sha256:0000"}
			tt.mut(&cmd)
			if _, err := f.svc.CreateEnvironment(context.Background(), sessionTestScope(), cmd); !errors.Is(err, control.ErrInvalid) {
				t.Fatalf("got %v, want ErrInvalid", err)
			}
			if len(f.repo.rows) != 0 {
				t.Fatalf("invalid create stored a row: %+v", f.repo.rows)
			}
		})
	}
}

func TestCreateEnvironmentAuthorizesBeforeStorage(t *testing.T) {
	f := newEnvironmentFixture(t)
	f.auth.err = control.ErrDenied
	if _, err := f.svc.CreateEnvironment(context.Background(), sessionTestScope(), control.CreateEnvironment{Name: "standard", Image: "registry.example.invalid/rainier@sha256:0000"}); !errors.Is(err, control.ErrDenied) {
		t.Fatalf("got %v, want ErrDenied", err)
	}
	if len(f.repo.rows) != 0 || f.log.has("environments:create") {
		t.Fatalf("denied create stored a row or reached the repository: %v", f.log.snapshot())
	}
}

func TestCreateEnvironmentDuplicateNameConflict(t *testing.T) {
	f := newEnvironmentFixture(t)
	f.repo.createErr = control.ErrConflict
	if _, err := f.svc.CreateEnvironment(context.Background(), sessionTestScope(), control.CreateEnvironment{Name: "standard", Image: "registry.example.invalid/rainier@sha256:0000"}); !errors.Is(err, control.ErrConflict) {
		t.Fatalf("got %v, want ErrConflict", err)
	}
}

func TestGetListEnvironmentWorkspaceScopedAndCopied(t *testing.T) {
	f := newEnvironmentFixture(t)
	env := control.Environment{
		ID: "env_example", WorkspaceID: "ws_example", Name: "standard",
		Image: "registry.example.invalid/rainier@sha256:0000", Setup: "make bootstrap",
		SetupHash:   expectedSetupHash("registry.example.invalid/rainier@sha256:0000", "make bootstrap"),
		EgressAllow: []string{"example.com"}, SecretRefs: []string{"token"},
		Requirements: control.Requirements{Capabilities: []string{"gpu"}},
		Connectors:   []control.Connector{{Type: "github", Raw: json.RawMessage(`{"repo":"acme/app"}`)}},
	}
	f.repo.put(env)

	got, err := f.svc.GetEnvironment(context.Background(), sessionTestScope(), "env_example")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "env_example" || got.WorkspaceID != "ws_example" {
		t.Fatalf("environment = %+v", got)
	}
	// Returned copies do not alias the stored row.
	got.EgressAllow[0] = "evil.example"
	got.SecretRefs[0] = "evil"
	got.Requirements.Capabilities[0] = "evil"
	got.Connectors[0].Raw = json.RawMessage(`{"repo":"evil/thing"}`)
	stored := f.repo.rows["env_example"]
	if stored.EgressAllow[0] != "example.com" || stored.SecretRefs[0] != "token" ||
		stored.Requirements.Capabilities[0] != "gpu" || string(stored.Connectors[0].Raw) != `{"repo":"acme/app"}` {
		t.Fatalf("returned environment aliased the stored row: %+v", stored)
	}

	// Cross-workspace get is ErrNotFound.
	other := sessionTestScope()
	other.WorkspaceID = "ws_other"
	if _, err := f.svc.GetEnvironment(context.Background(), other, "env_example"); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("cross-workspace get: got %v, want ErrNotFound", err)
	}

	// List is workspace-scoped and an empty page is a non-nil empty slice.
	page, err := f.svc.ListEnvironments(context.Background(), sessionTestScope(), control.EnvironmentQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Environments) != 1 || page.Environments[0].ID != "env_example" {
		t.Fatalf("page = %+v", page)
	}
	empty, err := f.svc.ListEnvironments(context.Background(), other, control.EnvironmentQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if empty.Environments == nil || len(empty.Environments) != 0 {
		t.Fatalf("cross-workspace page = %#v, want non-nil empty", empty.Environments)
	}
}

func TestUpdateEnvironmentOptionalityAndStaleSnapshot(t *testing.T) {
	f := newEnvironmentFixture(t)
	image := "registry.example.invalid/rainier@sha256:0000"
	oldHash := expectedSetupHash(image, "make bootstrap")
	env := control.Environment{
		ID: "env_example", WorkspaceID: "ws_example", Name: "standard",
		Image: image, Setup: "make bootstrap", SetupHash: oldHash,
		EgressAllow:  []string{"example.com"},
		Snapshot:     control.Checkpoint{Ref: "snap_ref_example", Format: "rainier-runner-v0", Capabilities: []string{"workspace"}},
		SnapshotHash: oldHash,
	}
	f.repo.put(env)

	// An explicit empty slice clears the list; nil leaves it alone.
	empty := []string{}
	updated, err := f.svc.UpdateEnvironment(context.Background(), sessionTestScope(), control.UpdateEnvironment{
		ID: "env_example", EgressAllow: &empty,
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated.EgressAllow == nil || len(updated.EgressAllow) != 0 {
		t.Fatalf("explicit clear lost: %#v", updated.EgressAllow)
	}

	// A setup edit changes the hash and leaves the old snapshot visibly stale.
	newSetup := "make bootstrap v2"
	updated, err = f.svc.UpdateEnvironment(context.Background(), sessionTestScope(), control.UpdateEnvironment{
		ID: "env_example", Setup: &newSetup,
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated.SetupHash == oldHash {
		t.Fatalf("setup hash unchanged after setup edit")
	}
	if updated.SnapshotHash != oldHash {
		t.Fatalf("snapshot hash = %q, want preserved %q (stale, not erased)", updated.SnapshotHash, oldHash)
	}
	if updated.Snapshot.Ref != "snap_ref_example" {
		t.Fatalf("snapshot ref = %q, want preserved", updated.Snapshot.Ref)
	}
}

func TestDeleteEnvironmentGuard(t *testing.T) {
	f := newEnvironmentFixture(t)
	env := control.Environment{
		ID: "env_example", WorkspaceID: "ws_example", Name: "standard",
		Image:     "registry.example.invalid/rainier@sha256:0000",
		SetupHash: expectedSetupHash("registry.example.invalid/rainier@sha256:0000", ""),
	}
	f.repo.put(env)

	// A live non-terminal session refuses the delete.
	f.repo.liveSessionCount = 1
	if err := f.svc.DeleteEnvironment(context.Background(), sessionTestScope(), control.DeleteEnvironment{ID: "env_example"}); !errors.Is(err, control.ErrConflict) {
		t.Fatalf("delete with live session: got %v, want ErrConflict", err)
	}
	if f.log.has("environments:delete") {
		t.Fatalf("delete reached the repository despite a live session: %v", f.log.snapshot())
	}

	// No live session deletes and records an event.
	f.repo.liveSessionCount = 0
	if err := f.svc.DeleteEnvironment(context.Background(), sessionTestScope(), control.DeleteEnvironment{ID: "env_example"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.repo.rows["env_example"]; ok {
		t.Fatalf("environment still present after delete")
	}
	if len(f.events.events) != 1 || f.events.events[0].Action != control.ActionDelete {
		t.Fatalf("events = %+v", f.events.events)
	}
}

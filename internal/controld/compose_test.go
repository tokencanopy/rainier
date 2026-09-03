package controld

import (
	"context"
	"errors"
	"testing"

	"github.com/tokencanopy/rainier/control"
)

// TestNewComposesTheFourServices pins the composition root: a Server built
// over the in-memory store carries every application service, wired to the
// adapters, before any handler is rewired to use them.
func TestNewComposesTheFourServices(t *testing.T) {
	s, err := New(NewMemStore(), Config{
		RunnerToken: "rnr_example", ExternalURL: "http://controld.agents.localhost",
		SecretsKey: testSecretsKey,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if s.sessions == nil || s.environments == nil || s.fleet == nil || s.attachments == nil {
		t.Fatalf("services not composed: %+v", s)
	}
	if s.transport == nil || s.broker == nil {
		t.Fatal("adapter fields not composed")
	}
	// The transport is real from the start: with no runner registered it
	// reports nothing connected.
	if s.transport.Connected(installPool, "runner-a") {
		t.Fatal("transport reported a connection with no runner registered")
	}
}

// noWorkspaceStore refuses to provision a workspace, standing in for a store
// that cannot be written at startup at all.
type noWorkspaceStore struct {
	MemStore
	err error
}

func (n noWorkspaceStore) EnsureWorkspace(ctx context.Context, ws control.WorkspaceID) error {
	return n.err
}

// TestNewFailsWhenTheWorkspaceCannotBeProvisioned pins the fail-closed half of
// the composition root: every tenant row is keyed by the installation
// workspace, so a controld that could not assert it would answer every
// request "not found" and look exactly like data loss.
func TestNewFailsWhenTheWorkspaceCannotBeProvisioned(t *testing.T) {
	down := errors.New("store down")
	_, err := New(noWorkspaceStore{MemStore: NewMemStore(), err: down}, Config{
		RunnerToken: "rnr_example", ExternalURL: "http://controld.agents.localhost",
		SecretsKey: testSecretsKey,
	})
	if !errors.Is(err, down) {
		t.Fatalf("New = %v, want it to fail closed on the workspace write", err)
	}
}

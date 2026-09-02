package controld

import (
	"testing"
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
	if s.gens == nil || s.transport == nil || s.broker == nil {
		t.Fatal("adapter fields not composed")
	}
	// Until Task 5, nothing is connected and nothing is dispatchable.
	if s.transport.Connected(installPool, "runner-a") {
		t.Fatal("placeholder transport reported a connection")
	}
}

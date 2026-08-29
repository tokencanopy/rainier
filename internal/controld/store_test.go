package controld_test

import (
	"regexp"
	"testing"

	"rainier/internal/controld"
)

// TestSetupHash pins the wire-visible definition of an environment's build
// identity. Every store recomputes it, snapshot refs are named after its
// first 12 characters, and the guarded snapshot update compares it across
// processes — so the algorithm (sha256 of image + NUL + setup, hex-encoded)
// can't drift without invalidating every cached snapshot in a deployment.
func TestSetupHash(t *testing.T) {
	const want = "4ff6c53fe610903df30946ce1edb212986ea51b0ebd9488337fe2d37d060adae"
	if got := controld.SetupHash("ubuntu:24.04", "apt-get install -y make"); got != want {
		t.Fatalf("SetupHash: got %q, want %q", got, want)
	}

	// The NUL separator is what keeps the pair unambiguous: these two
	// image/setup splits concatenate to the same string and must not hash
	// alike.
	if a, b := controld.SetupHash("img", "setup"), controld.SetupHash("imgsetup", ""); a == b {
		t.Fatalf("image/setup boundary must be part of the hash: both %q", a)
	}
	if controld.SetupHash("img", "") == controld.SetupHash("img", " ") {
		t.Fatal("setup content must be part of the hash")
	}
}

// TestNewEnvironmentID pins the id shape callers and the CLI match on.
func TestNewEnvironmentID(t *testing.T) {
	shape := regexp.MustCompile(`^env_[0-9a-f]{32}$`)
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		id := controld.NewEnvironmentID()
		if !shape.MatchString(id) {
			t.Fatalf("environment id %q must be env_ plus 32 hex chars", id)
		}
		if seen[id] {
			t.Fatalf("environment id repeated: %q", id)
		}
		seen[id] = true
	}
}

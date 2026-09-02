package controlapp

import "testing"

// TestSetupHashPinsExactDigest pins the wire-visible output of setupHash for
// a fixed synthetic input. The value is load-bearing across two writers: the
// scheduler pins a session's SetupHash at dispatch and the environment
// service writes the environment's SetupHash, and a cached snapshot is only
// reused when the two agree. Both call this one function, so any change to
// the digest, the NUL separator, or the hex encoding fails here rather than
// silently disabling snapshot reuse in production.
func TestSetupHashPinsExactDigest(t *testing.T) {
	const (
		image = "registry.example.invalid/rainier@sha256:0000"
		setup = "apt-get install -y build-essential"
		want  = "b8f29fb43aedb4ae1cc21abc8598cd1fe418b146c6ee3dbf23853eff7d9606cd"
	)
	if got := setupHash(image, setup); got != want {
		t.Fatalf("setupHash(%q, %q) = %q, want %q", image, setup, got, want)
	}
}

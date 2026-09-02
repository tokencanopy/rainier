package controlapp

import (
	"crypto/sha256"
	"encoding/hex"
)

// setupHash returns the identity of a build's inputs: the hex-encoded
// SHA-256 of image, a NUL separator, and setup. Init, egress, connectors,
// secrets, requirements, and timeouts are deliberately excluded: init runs
// per boot and the snapshot caches only image plus setup.
//
// The equality is load-bearing across two writers. A session's pinned
// SetupHash (written by the scheduler at dispatch) must equal the
// environment's SetupHash (written by the environment service) or a cached
// snapshot is never reused, so both call this one function and
// TestSetupHashPinsExactDigest pins its exact output.
func setupHash(image, setup string) string {
	sum := sha256.Sum256([]byte(image + "\x00" + setup))
	return hex.EncodeToString(sum[:])
}

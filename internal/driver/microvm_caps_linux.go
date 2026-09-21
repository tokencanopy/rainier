//go:build linux

// internal/driver/microvm_caps_linux.go
package driver

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// capStatusPath is where the kernel publishes this process's capability sets.
// It is a variable so the tests can point it at a fixture rather than at a
// process whose privileges they would have to arrange.
var capStatusPath = "/proc/self/status"

// effectiveCapabilities reads the EFFECTIVE set — the one the kernel actually
// consults — as a bitmask.
//
// Effective and not permitted: a process can hold a capability in its
// permitted set and not have raised it, and a check against permitted would
// pass on a runner that cannot in fact create a TAP device. Go's runtime does
// not raise capabilities, so for this program the two are the same in every
// ordinary case; reading the one the kernel enforces is still the right
// question to ask.
func effectiveCapabilities() (uint64, error) {
	f, err := os.Open(capStatusPath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		rest, ok := strings.CutPrefix(scanner.Text(), "CapEff:")
		if !ok {
			continue
		}
		mask, err := strconv.ParseUint(strings.TrimSpace(rest), 16, 64)
		if err != nil {
			return 0, fmt.Errorf("parse CapEff %q from %s: %w", strings.TrimSpace(rest), capStatusPath, err)
		}
		return mask, nil
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	return 0, fmt.Errorf("%s has no CapEff line", capStatusPath)
}

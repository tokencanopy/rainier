//go:build !linux

// internal/driver/microvm_caps_other.go
package driver

import (
	"errors"
	"fmt"
	"runtime"
)

// capStatusPath exists on every platform so the package's tests do not need
// build tags of their own. Off Linux nothing reads it.
var capStatusPath = ""

// effectiveCapabilities always fails off Linux, because there is nothing here
// that could run a microVM anyway — no /dev/kvm, no network namespaces, no
// jailer. Answering "you have everything" would be the one shape of this
// function that could let a runner start and then execute nothing.
func effectiveCapabilities() (uint64, error) {
	return 0, fmt.Errorf("microvm: capabilities are a Linux concept and this is %s: %w", runtime.GOOS, errors.ErrUnsupported)
}

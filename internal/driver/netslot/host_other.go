//go:build !linux

// internal/driver/netslot/host_other.go
//
// There is no non-Linux implementation and there is deliberately no fake in
// its place. A microVM host is a Linux host with KVM; a developer machine
// that is not one has no business building network namespaces, and a
// constructor that quietly returned something that pretended to would give
// the driver above it a slot whose firewall does not exist.
package netslot

import (
	"errors"
	"fmt"
	"runtime"
)

// LinuxHost is declared on every platform so that callers do not need build
// tags of their own. Off Linux it can only be nil.
type LinuxHost struct{}

// NewLinuxHost always fails off Linux.
func NewLinuxHost(string) (*LinuxHost, error) {
	return nil, fmt.Errorf("netslot: network slots need Linux network namespaces, and this is %s: %w", runtime.GOOS, errors.ErrUnsupported)
}

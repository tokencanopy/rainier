//go:build !linux

package main

import (
	"context"
	"errors"
	"net"
)

// dialVsock on anything but Linux. A microVM guest is Linux by construction
// — Firecracker boots a Linux kernel — so this exists only so the package
// builds on a developer's machine, and it refuses rather than pretending.
func dialVsock(context.Context, uint32, uint32) (net.Conn, error) {
	return nil, errors.New("vsock is a Linux facility; this sessiond was built for another platform")
}

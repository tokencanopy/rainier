//go:build !linux

// internal/driver/netslot/host_other.go
//
// There is no non-Linux implementation and there is deliberately no fake in
// its place. A microVM host is a Linux host with KVM; a developer machine
// that is not one has no business building network namespaces, and a
// constructor that quietly returned something that pretended to would give
// the driver above it a slot whose firewall does not exist.
//
// The type is still declared, and still satisfies Host, so that callers need
// no build tags of their own. Every method is unreachable: NewLinuxHost is
// the only thing that produces one and it always fails.
package netslot

import (
	"context"
	"errors"
	"fmt"
	"runtime"
)

// LinuxHost is declared on every platform. Off Linux it can only be nil.
type LinuxHost struct{}

var _ Host = (*LinuxHost)(nil)

// NewLinuxHost always fails off Linux.
func NewLinuxHost(string) (*LinuxHost, error) { return nil, unsupported() }

func unsupported() error {
	return fmt.Errorf("netslot: network slots need Linux network namespaces, and this is %s: %w", runtime.GOOS, errors.ErrUnsupported)
}

func (*LinuxHost) AddNetns(context.Context, string) error                  { return unsupported() }
func (*LinuxHost) DelNetns(context.Context, string) error                  { return unsupported() }
func (*LinuxHost) ListNetns(context.Context) ([]string, error)             { return nil, unsupported() }
func (*LinuxHost) AddVeth(context.Context, string, string, string) error   { return unsupported() }
func (*LinuxHost) DelLink(context.Context, string, string) error           { return unsupported() }
func (*LinuxHost) AddTap(context.Context, string, string, string) error    { return unsupported() }
func (*LinuxHost) AddAddr(context.Context, string, string, string) error   { return unsupported() }
func (*LinuxHost) LinkUp(context.Context, string, string) error            { return unsupported() }
func (*LinuxHost) AddRoute(context.Context, string, string, string) error  { return unsupported() }
func (*LinuxHost) DelRoute(context.Context, string, string, string) error  { return unsupported() }
func (*LinuxHost) SetSysctl(context.Context, string, string, string) error { return unsupported() }
func (*LinuxHost) ApplyNft(context.Context, string, string) error          { return unsupported() }
func (*LinuxHost) DeleteNftTable(context.Context, string, string) error    { return unsupported() }

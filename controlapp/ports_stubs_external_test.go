package controlapp_test

import (
	"context"

	"github.com/tokencanopy/rainier/control"
)

// The external mirror of ports_stubs_test.go: controlapp_test is a separate
// package (exactly as a Rainier Cloud module would be), so it supplies its
// own unit of work and checkpoint locator rather than borrowing the internal
// ones — which is itself the proof that both ports are satisfiable from
// outside the module.
var (
	_ control.UnitOfWork        = extDirectUOW{}
	_ control.CheckpointLocator = extLocatorStub{}
)

// extDirectUOW is control.UnitOfWork for a host with no transactions.
type extDirectUOW struct{}

func (extDirectUOW) Run(ctx context.Context, fn func(context.Context) error) error { return fn(ctx) }

// extLocatorStub answers "nowhere", the self-hosted answer for a checkpoint
// with no holder.
type extLocatorStub struct{ location control.CheckpointLocation }

func (l extLocatorStub) LocateCheckpoint(context.Context, control.WorkspaceID, control.Checkpoint) (control.CheckpointLocation, error) {
	return l.location, nil
}

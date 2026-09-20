package controlapp_test

import (
	"context"
	"time"

	"github.com/tokencanopy/rainier/control"
)

// extBootstrapStub is the external mirror of bootstrapStub: controlapp_test
// is a separate package, exactly as a Rainier Cloud module would be, so it
// supplies its own bootstrap store rather than borrowing the internal one —
// which is itself the proof that the port is satisfiable from outside.
var _ control.SessionBootstrapStore = extBootstrapStub{}

type extBootstrapStub struct{}

func (extBootstrapStub) PutSessionBootstrap(context.Context, control.WorkspaceID, control.SessionID, control.SessionBootstrap) error {
	return nil
}

func (extBootstrapStub) ConsumeSessionBootstrap(context.Context, control.WorkspaceID, control.SessionID, string, uint64, time.Time) error {
	return control.ErrBootstrapUnknown
}

package controlapp

import (
	"context"

	"github.com/tokencanopy/rainier/control"
)

// The two host ports every service in this package now requires, stubbed once
// for every fixture in the package's internal tests. They are deliberately
// the simplest thing the contract allows — a host without transactions runs
// the closure directly, and a locator answers whatever it was told to — so a
// test that is not about atomicity or placement reads exactly as it did
// before the ports existed. The tests that ARE about them supply their own.
var (
	_ control.UnitOfWork        = directUOW{}
	_ control.CheckpointLocator = locatorStub{}
)

// directUOW is control.UnitOfWork for a host with no transactions: Run calls
// fn with the context it was handed and returns fn's error unchanged.
type directUOW struct{}

func (directUOW) Run(ctx context.Context, fn func(context.Context) error) error { return fn(ctx) }

// locatorStub is control.CheckpointLocator with a configurable answer: the
// zero value says "nowhere", which is what every self-hosted checkpoint said
// before a locator existed.
type locatorStub struct {
	location control.CheckpointLocation
	err      error
}

func (l locatorStub) LocateCheckpoint(context.Context, control.WorkspaceID, control.Checkpoint) (control.CheckpointLocation, error) {
	if l.err != nil {
		return control.CheckpointLocation{}, l.err
	}
	return l.location, nil
}

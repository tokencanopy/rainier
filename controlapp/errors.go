package controlapp

import (
	"context"
	"errors"

	"github.com/tokencanopy/rainier/control"
)

// portError normalizes a port error to the closed control sentinel
// vocabulary, so an adapter's SQL, connection string, or provider text can
// never leave a service through a control interface. Context cancellation and
// deadline propagation are preserved (the caller went away, which is not a
// dependency failure); a sentinel the port already reported passes through
// unchanged; every other adapter failure is ErrUnavailable.
//
// Every service in this package normalizes at its own port boundary with this
// one helper, so a given adapter failure surfaces identically no matter which
// control interface the caller reached it through.
func portError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	switch {
	case errors.Is(err, control.ErrInvalid):
		return control.ErrInvalid
	case errors.Is(err, control.ErrDenied):
		return control.ErrDenied
	case errors.Is(err, control.ErrNotFound):
		return control.ErrNotFound
	case errors.Is(err, control.ErrConflict):
		return control.ErrConflict
	case errors.Is(err, control.ErrStale):
		return control.ErrStale
	case errors.Is(err, control.ErrUnavailable):
		return control.ErrUnavailable
	case errors.Is(err, control.ErrUnsupported):
		return control.ErrUnsupported
	default:
		return control.ErrUnavailable
	}
}

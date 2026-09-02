package controld

import (
	"context"
	"errors"
	"net/http"

	"github.com/tokencanopy/rainier/control"
)

// controlStatus maps a control sentinel to today's status, slug, and a fixed
// message. It is the one place the wire learns about the closed error set;
// handlers that already validated a request pass their own specific message
// for ErrInvalid and ErrConflict, and never invent a second mapping.
//
// A caller that went away gets nothing written: the caller is the one
// component that cannot read the answer, and there is no dependency to
// report. writeControlErr honors that by writing nothing for those two.
func controlStatus(err error) (status int, code, msg string) {
	switch {
	case errors.Is(err, control.ErrInvalid):
		return http.StatusBadRequest, "invalid_request", "invalid request"
	case errors.Is(err, control.ErrDenied):
		return http.StatusForbidden, "forbidden", "not authorized"
	case errors.Is(err, control.ErrNotFound):
		return http.StatusNotFound, "not_found", "not found"
	case errors.Is(err, control.ErrConflict):
		return http.StatusConflict, "conflict", "conflict"
	case errors.Is(err, control.ErrStale):
		return http.StatusConflict, "conflict", "stale"
	case errors.Is(err, control.ErrUnavailable):
		return http.StatusInternalServerError, "internal", "internal error"
	case errors.Is(err, control.ErrUnsupported):
		return http.StatusNotImplemented, "unsupported", "unsupported"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return 0, "", ""
	default:
		return http.StatusInternalServerError, "internal", "internal error"
	}
}

// unavailableStatus refines ErrUnavailable the way today's handlers split it:
// for a session placed on a runner, the dependency a lifecycle command fails
// on is the runner — it has no control connection here, or it has one and did
// not answer within OpTimeout — and both have always been 502
// runner_unreachable (ErrDispatchTimeout wraps ErrRunnerUnreachable). The
// message says which. A session placed nowhere has no runner to blame, so its
// ErrUnavailable is the store's and stays 500.
func (s *Server) unavailableStatus(row control.Session) (int, string, string) {
	if row.RunnerID == "" {
		return http.StatusInternalServerError, "internal", "internal error"
	}
	if s.transport == nil || !s.transport.Connected(installPool, row.RunnerID) {
		return http.StatusBadGateway, "runner_unreachable", "runner is not connected"
	}
	return http.StatusBadGateway, "runner_unreachable", "runner did not respond"
}

// writeControlErr answers a service error with controlStatus's fixed
// mapping. Handlers that need a specific message or the unavailable
// refinement compute the triple themselves and call writeErr.
func writeControlErr(w http.ResponseWriter, err error) {
	status, code, msg := controlStatus(err)
	if status == 0 {
		return
	}
	writeErr(w, status, code, msg)
}

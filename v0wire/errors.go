// v0wire/errors.go
package v0wire

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/tokencanopy/rainier/control"
)

// ErrorBody is the "error" object inside every error response this wire
// returns. Code is machine-readable and branchable; Message is for humans and
// never carries internal detail (upstream bodies, stack traces, SQL).
type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ErrorEnvelope is the JSON shape of every error response:
// {"error":{"code":..., "message":...}}.
type ErrorEnvelope struct {
	Error ErrorBody `json:"error"`
}

// StatusFor maps a control sentinel to its status, slug, and a fixed message.
// It is the one place the wire learns about the closed error set; handlers
// that already validated a request pass their own specific message for
// ErrInvalid and ErrConflict, and never invent a second mapping.
//
// A caller that went away gets a zero status and nothing to write: the caller
// is the one component that cannot read the answer, and there is no dependency
// to report. WriteControlError honors that by writing nothing for those two.
func StatusFor(err error) (status int, code, msg string) {
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

// WriteError writes status with a JSON error envelope. msg must never contain
// anything a caller shouldn't see — log the detail separately instead.
func WriteError(w http.ResponseWriter, status int, code, msg string) {
	WriteJSON(w, status, ErrorEnvelope{Error: ErrorBody{Code: code, Message: msg}})
}

// WriteControlError answers a service error with StatusFor's fixed mapping.
// Hosts that need a specific message, or a refinement of their own, compute
// the triple with StatusFor and call WriteError.
func WriteControlError(w http.ResponseWriter, err error) {
	status, code, msg := StatusFor(err)
	if status == 0 {
		return
	}
	WriteError(w, status, code, msg)
}

// WriteJSON writes v as the response body with the given status and this
// wire's content type. A body that fails to encode has already had its status
// line sent, so there is nothing left to tell the caller; a host that wants
// that failure in its log wraps the writer.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

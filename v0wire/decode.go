// v0wire/decode.go
package v0wire

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

// DecodeJSON decodes r's body (capped at limit bytes) into v, rejecting
// unknown fields and a body holding more than one JSON value. An empty body
// decodes to v's zero value: every field in every body this wire accepts is
// optional, so "no body at all" and "{}" are the same request.
//
// It writes a 400 invalid_request response and returns false on any failure;
// callers should return immediately when it does.
func DecodeJSON(w http.ResponseWriter, r *http.Request, v any, limit int64) bool {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil && !errors.Is(err, io.EOF) {
		WriteError(w, http.StatusBadRequest, "invalid_request", "malformed request body")
		return false
	}
	if dec.More() {
		WriteError(w, http.StatusBadRequest, "invalid_request", "request body must contain a single JSON object")
		return false
	}
	return true
}

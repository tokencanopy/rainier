package controld

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// TestControlStatusTable pins every row of the sentinel mapping. The wire
// learns about the closed error set here and nowhere else.
func TestControlStatusTable(t *testing.T) {
	cases := []struct {
		err    error
		status int
		code   string
	}{
		{control.ErrInvalid, http.StatusBadRequest, "invalid_request"},
		{control.ErrDenied, http.StatusForbidden, "forbidden"},
		{control.ErrNotFound, http.StatusNotFound, "not_found"},
		{control.ErrConflict, http.StatusConflict, "conflict"},
		{control.ErrStale, http.StatusConflict, "conflict"},
		{control.ErrUnavailable, http.StatusInternalServerError, "internal"},
		{control.ErrUnsupported, http.StatusNotImplemented, "unsupported"},
		{context.Canceled, 0, ""},
		{context.DeadlineExceeded, 0, ""},
		{errors.New("pq: relation does not exist (host=db.internal.invalid)"), http.StatusInternalServerError, "internal"},
	}
	for _, tc := range cases {
		status, code, msg := controlStatus(tc.err)
		if status != tc.status || code != tc.code {
			t.Errorf("controlStatus(%v) = %d %q, want %d %q", tc.err, status, code, tc.status, tc.code)
		}
		if status != 0 && msg == "" {
			t.Errorf("controlStatus(%v) has no message", tc.err)
		}
		if status != 0 && (msg == tc.err.Error()) {
			t.Errorf("controlStatus(%v) relayed the error text", tc.err)
		}
	}
	// A wrapped sentinel maps the same as the bare one.
	if status, _, _ := controlStatus(errors.Join(errors.New("adapter detail"), control.ErrNotFound)); status != http.StatusNotFound {
		t.Fatalf("wrapped ErrNotFound = %d", status)
	}
}

func TestWriteControlErrWritesNothingForACallerThatWentAway(t *testing.T) {
	rec := httptest.NewRecorder()
	writeControlErr(rec, context.Canceled)
	if rec.Code != http.StatusOK || rec.Body.Len() != 0 {
		t.Fatalf("wrote %d %q for a canceled caller", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	writeControlErr(rec, control.ErrNotFound)
	if rec.Code != http.StatusNotFound || rec.Header().Get("Content-Type") == "" {
		t.Fatalf("got %d %q", rec.Code, rec.Body.String())
	}
}

// connectedMap is the smallest control.RunnerTransport a status test needs.
type connectedMap map[control.RunnerID]bool

func (m connectedMap) Connected(_ control.PoolID, id control.RunnerID) bool { return m[id] }
func (connectedMap) Dispatch(context.Context, control.PoolID, control.RunnerID, runner.ToRunner) (runner.FromRunner, error) {
	return runner.FromRunner{}, control.ErrUnavailable
}

func TestUnavailableStatusRefinesByRunnerConnectivity(t *testing.T) {
	s := &Server{transport: connectedMap{"runner-a": true}}
	placedOnDisconnected := control.Session{ID: "sess_example", RunnerID: "runner-b"}
	if status, code, _ := s.unavailableStatus(placedOnDisconnected); status != http.StatusBadGateway || code != "runner_unreachable" {
		t.Fatalf("disconnected runner = %d %s", status, code)
	}
	placedOnConnected := control.Session{ID: "sess_example", RunnerID: "runner-a"}
	if status, code, _ := s.unavailableStatus(placedOnConnected); status != http.StatusInternalServerError || code != "internal" {
		t.Fatalf("connected runner = %d %s", status, code)
	}
	if status, _, _ := s.unavailableStatus(control.Session{ID: "sess_example"}); status != http.StatusInternalServerError {
		t.Fatalf("unplaced = %d", status)
	}
	if status, _, _ := (&Server{}).unavailableStatus(placedOnDisconnected); status != http.StatusInternalServerError {
		t.Fatalf("no transport composed yet = %d", status)
	}
}

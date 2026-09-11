package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tokencanopy/rainier/internal/cli"
)

// conflictingResumeServer answers every resume with the 409 a control plane
// writes when the runner holding the session is already stopping it — idle
// auto-stop's cold suspend, claimed and not yet landed — and serves the
// session as stopped throughout, which is what the row reads while that stop
// is in flight.
func conflictingResumeServer(t *testing.T, posts, gets *int) *httptest.Server {
	t.Helper()
	row := session{ID: "sess_example", Name: "investigate", OwnerID: "usr_example", State: "suspended_cold"}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			*posts++
			w.WriteHeader(http.StatusConflict)
			io.WriteString(w, `{"error":{"code":"conflict","message":"session cannot be resumed right now"}}`)
			return
		}
		*gets++
		if strings.HasPrefix(r.URL.Path, "/v0/sessions/") {
			json.NewEncoder(w).Encode(sessionEnvelope{Session: row})
			return
		}
		json.NewEncoder(w).Encode(sessionsEnvelope{Sessions: []session{row}})
	}))
	t.Cleanup(ts.Close)
	return ts
}

// TestARefusedResumeIsAConflictOnTheCommandLine is the far end of the path a
// runner's new refusal travels: the runner says "I am already stopping this
// sandbox", controlapp turns that into control.ErrConflict, controld writes
// 409 `conflict`, and this is what the person holding the terminal gets.
//
// Exit 1 — every rainier failure that is not a usage error is 1, and saying so
// is the point: what the fix changes is the class, the code and the sentence,
// not the exit status. Before it, the same refusal was a 500 `internal`,
// "could not resume session", about a runner that is healthy and answering.
func TestARefusedResumeIsAConflictOnTheCommandLine(t *testing.T) {
	posts, gets := 0, 0
	ts := conflictingResumeServer(t, &posts, &gets)
	cfg := hostedConfig(t, ts.URL)

	err := runResume([]string{"investigate"})
	if err == nil {
		t.Fatal("the refused resume succeeded")
	}
	if got := err.Error(); got != "conflict: session cannot be resumed right now" {
		t.Errorf("error = %q, want the server's own sentence under its own code", got)
	}
	if got := exitCodeFor(err); got != 1 {
		t.Errorf("exit code = %d, want 1", got)
	}
	var sb strings.Builder
	reportError(cfg, &sb, err)
	if !strings.Contains(sb.String(), "code conflict") {
		t.Errorf("stderr = %q, does not carry the stable code a person can act on", sb.String())
	}
	if posts != 1 {
		t.Errorf("resume posted %d time(s), want 1 — `rainier resume` does not retry on its own", posts)
	}
}

// TestAttachRetriesARefusedResumeRatherThanFailingAtOnce is the behaviour the
// conflict code buys, and the reason the class matters beyond the wording:
// resumeForAttach has always had a bounded convergence loop for a resume that
// lost a race, and it keys on `code == "conflict"`. A refusal dressed as a 500
// never reached it, so `rainier attach` gave up instantly on a condition that
// clears itself in the time it takes `docker stop` to return.
//
// Fails without the conflict class: with the old 500 the loop is skipped and
// no further GET is made.
func TestAttachRetriesARefusedResumeRatherThanFailingAtOnce(t *testing.T) {
	posts, gets := 0, 0
	ts := conflictingResumeServer(t, &posts, &gets)

	c := &cli.Client{Base: ts.URL}
	before := gets
	err := resumeForAttach(c, "sess_example")
	if err == nil {
		t.Fatal("the refused resume succeeded")
	}
	if !strings.Contains(err.Error(), "conflict: session cannot be resumed right now") {
		t.Errorf("error = %v, want the original refusal preserved", err)
	}
	if gets <= before {
		t.Error("attach gave up without re-reading the row: the refusal never reached the convergence loop, " +
			"which is what a conflict code is for")
	}
}

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

// the two answers a refused resume can carry: the 409 a control plane writes
// once the runner can say "I am already stopping this sandbox", and the 500
// it wrote before it could.
const (
	resumeConflictBody = `{"error":{"code":"conflict","message":"session cannot be resumed right now"}}`
	resumeInternalBody = `{"error":{"code":"internal","message":"could not resume session"}}`
)

// refusingResumeServer answers every resume with status/body and serves the
// session as stopped throughout, which is what the row reads while a stop is
// in flight against it.
func refusingResumeServer(t *testing.T, status int, body string, posts, gets *int) *httptest.Server {
	t.Helper()
	row := session{ID: "sess_example", Name: "investigate", OwnerID: "usr_example", State: "suspended_cold"}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			*posts++
			w.WriteHeader(status)
			io.WriteString(w, body)
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
	ts := refusingResumeServer(t, http.StatusConflict, resumeConflictBody, &posts, &gets)
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

// TestAttachTreatsTheTwoRefusalsDifferently is the behaviour the conflict
// CLASS buys, stated as the contrast that makes it worth having.
//
// These are characterization tests of the far end: no CLI production code
// changes in this round, and both halves below pass on either side of it. The
// seam that decides WHICH of the two answers the server sends is pinned where
// it lives — `controlapp`'s dispatch, `internal/controld`'s handler, and the
// `internal/e2e` scene that drives the whole path. What this test records is
// why that decision matters once it reaches a person's machine.
//
// `resumeForAttach` has always had a bounded convergence loop for a resume
// that lost a race, and it keys on `code == "conflict"`. A refusal dressed as
// a 500 never reaches it, so `rainier attach` gives up instantly on a
// condition that clears itself in the time `docker stop` takes to return.
func TestAttachTreatsTheTwoRefusalsDifferently(t *testing.T) {
	t.Run("a conflict is converged on", func(t *testing.T) {
		// Costs the loop's full 2s budget: the row never reaches a live
		// state, because a refused resume moves nothing. That is the point
		// being asserted, not an accident of the fixture.
		posts, gets := 0, 0
		ts := refusingResumeServer(t, http.StatusConflict, resumeConflictBody, &posts, &gets)

		err := resumeForAttach(&cli.Client{Base: ts.URL}, "sess_example")
		if err == nil {
			t.Fatal("the refused resume succeeded")
		}
		if !strings.Contains(err.Error(), "conflict: session cannot be resumed right now") {
			t.Errorf("error = %v, want the original refusal preserved", err)
		}
		if gets == 0 {
			t.Error("attach gave up without re-reading the row: the refusal never reached the " +
				"convergence loop, which is what a conflict code is for")
		}
	})

	t.Run("an internal error is not", func(t *testing.T) {
		posts, gets := 0, 0
		ts := refusingResumeServer(t, http.StatusInternalServerError, resumeInternalBody, &posts, &gets)

		err := resumeForAttach(&cli.Client{Base: ts.URL}, "sess_example")
		if err == nil {
			t.Fatal("the refused resume succeeded")
		}
		if gets != 0 {
			t.Errorf("attach re-read the row %d time(s) for a 500: only a conflict is worth "+
				"converging on, and a server error is not", gets)
		}
	})
}

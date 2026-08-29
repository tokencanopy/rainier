package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"rainier/internal/cli"
)

// ---------------------------------------------------------------------------
// resolveSessionID — review round 1, finding 3: names are unique only
// per-owner, but GET /v1/sessions is team-visible, so two teammates can
// share a name.
// ---------------------------------------------------------------------------

// pagedSessions serves GET /v1/sessions from a fixed set of pages, keyed by
// the "cursor" query param ("" for the first page). Every test in this file
// uses it so the fixture stays in one place.
func pagedSessions(t *testing.T, pages map[string]sessionsEnvelope) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page, ok := pages[r.URL.Query().Get("cursor")]
		if !ok {
			t.Fatalf("unexpected cursor %q", r.URL.Query().Get("cursor"))
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(page)
	}))
	t.Cleanup(ts.Close)
	return ts
}

// TestResolveSessionIDAmbiguousNameAcrossOwners is the finding's mandated
// test: two owners ("alice", "bob") each have a session named "dev-box".
// Without a cached owner id the CLI must refuse rather than silently acting
// on whichever paginated first; with one cached, it must prefer the
// caller's own.
func TestResolveSessionIDAmbiguousNameAcrossOwners(t *testing.T) {
	ts := pagedSessions(t, map[string]sessionsEnvelope{
		"": {Sessions: []session{
			{ID: "sess_alice1", Name: "dev-box", OwnerID: "usr_alice", State: "running"},
			{ID: "sess_bob1", Name: "dev-box", OwnerID: "usr_bob", State: "running"},
		}},
	})
	c := &cli.Client{Base: ts.URL}

	t.Run("no cached owner id: ambiguous, both matches listed", func(t *testing.T) {
		id, err := resolveSessionID(c, "", "dev-box")
		if err == nil {
			t.Fatalf("resolveSessionID = (%q, nil), want an ambiguous-name error", id)
		}
		msg := err.Error()
		if !strings.Contains(msg, "ambiguous name \"dev-box\"") {
			t.Errorf("error = %q, want it to name the ambiguous ref", msg)
		}
		if !strings.Contains(msg, "sess_alice1") || !strings.Contains(msg, "usr_alice") {
			t.Errorf("error = %q, want it to list sess_alice1 (owner usr_alice)", msg)
		}
		if !strings.Contains(msg, "sess_bob1") || !strings.Contains(msg, "usr_bob") {
			t.Errorf("error = %q, want it to list sess_bob1 (owner usr_bob)", msg)
		}
		if !strings.Contains(msg, "use the session id") {
			t.Errorf("error = %q, want it to tell the caller to use the session id", msg)
		}
	})

	t.Run("owner preference resolves it when exactly one match is the caller's own", func(t *testing.T) {
		id, err := resolveSessionID(c, "usr_bob", "dev-box")
		if err != nil {
			t.Fatalf("resolveSessionID: %v", err)
		}
		if id != "sess_bob1" {
			t.Fatalf("resolveSessionID = %q, want sess_bob1 (bob's own)", id)
		}
	})

	t.Run("a cached owner id that owns none of the matches is still ambiguous", func(t *testing.T) {
		_, err := resolveSessionID(c, "usr_carol", "dev-box")
		if err == nil {
			t.Fatal("resolveSessionID: want an ambiguous-name error, got nil")
		}
		if !strings.Contains(err.Error(), "ambiguous name") {
			t.Fatalf("error = %q, want an ambiguous-name error", err.Error())
		}
	})

	t.Run("sess_ prefix is used verbatim, no lookup performed", func(t *testing.T) {
		id, err := resolveSessionID(c, "", "sess_untouched")
		if err != nil || id != "sess_untouched" {
			t.Fatalf("resolveSessionID(sess_untouched) = (%q, %v), want (sess_untouched, nil)", id, err)
		}
	})
}

// TestResolveSessionIDCollectsMatchesAcrossPages asserts a match on page 1
// does not short-circuit the search — a second match on a later page must
// still be found and folded into the ambiguity decision. This is the
// regression the old "return on first match" implementation missed.
func TestResolveSessionIDCollectsMatchesAcrossPages(t *testing.T) {
	ts := pagedSessions(t, map[string]sessionsEnvelope{
		"": {
			Sessions:   []session{{ID: "sess_alice1", Name: "dev-box", OwnerID: "usr_alice", State: "running"}},
			NextCursor: "page2",
		},
		"page2": {
			Sessions: []session{{ID: "sess_bob1", Name: "dev-box", OwnerID: "usr_bob", State: "running"}},
		},
	})
	c := &cli.Client{Base: ts.URL}

	_, err := resolveSessionID(c, "", "dev-box")
	if err == nil {
		t.Fatal("resolveSessionID: want an ambiguous-name error spanning both pages, got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "sess_alice1") || !strings.Contains(msg, "sess_bob1") {
		t.Fatalf("error = %q, want it to list matches from both page 1 and page 2", msg)
	}
}

// TestResolveSessionIDUniqueNameNoAmbiguity is the ordinary case: one match,
// no ambiguity machinery involved.
func TestResolveSessionIDUniqueNameNoAmbiguity(t *testing.T) {
	ts := pagedSessions(t, map[string]sessionsEnvelope{
		"": {Sessions: []session{{ID: "sess_only", Name: "solo", OwnerID: "usr_alice", State: "running"}}},
	})
	c := &cli.Client{Base: ts.URL}

	id, err := resolveSessionID(c, "", "solo")
	if err != nil || id != "sess_only" {
		t.Fatalf("resolveSessionID = (%q, %v), want (sess_only, nil)", id, err)
	}
}

// TestResolveSessionIDNotFound asserts a name matching nothing is a plain
// not-found error, not an ambiguity error.
func TestResolveSessionIDNotFound(t *testing.T) {
	ts := pagedSessions(t, map[string]sessionsEnvelope{"": {}})
	c := &cli.Client{Base: ts.URL}

	_, err := resolveSessionID(c, "", "nope")
	if err == nil {
		t.Fatal("resolveSessionID: want a not-found error, got nil")
	}
	if !strings.Contains(err.Error(), `no session named "nope" found`) {
		t.Fatalf("error = %q, want the not-found message", err.Error())
	}
}

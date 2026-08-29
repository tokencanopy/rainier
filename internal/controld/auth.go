// internal/controld/auth.go
package controld

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

const (
	// githubExchangeBodyLimit caps the /v1/auth/github request body; the
	// only field it carries is a GitHub access token, so 4KB is generous.
	githubExchangeBodyLimit = 4 << 10
	// githubUserBodyLimit caps how much of GitHub's /user response we will
	// ever read — it's untrusted input, and we need at most an id and a
	// login.
	githubUserBodyLimit = 1 << 20
	// githubCallTimeout bounds the one outbound call the exchange handler
	// makes.
	githubCallTimeout = 10 * time.Second
)

// ---------------------------------------------------------------------------
// shared response plumbing (Task 10 moves/reuses these verbatim for the rest
// of the client API)
// ---------------------------------------------------------------------------

// errorBody is the "error" object inside errorEnvelope.
type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// errorEnvelope is the JSON shape of every error response this API returns:
// {"error":{"code":..., "message":...}}. code is machine-readable and
// branchable; message is for humans and never carries internal detail
// (upstream bodies, stack traces, SQL).
type errorEnvelope struct {
	Error errorBody `json:"error"`
}

// writeErr writes status with a JSON error envelope. msg must never contain
// anything a caller shouldn't see — log the detail separately instead.
func writeErr(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, errorEnvelope{Error: errorBody{Code: code, Message: msg}})
}

// writeJSON writes v as the response body with the given status and the
// standard JSON content type.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("controld: writing JSON response: %v", err)
	}
}

// ---------------------------------------------------------------------------
// user view + role
// ---------------------------------------------------------------------------

// userView is the client-facing rendering of a User: login and role only,
// never the internal id or GitHub id.
type userView struct {
	Login string `json:"login"`
	Role  string `json:"role"`
}

// userJSON renders u as its client-facing view.
func userJSON(u User) userView {
	return userView{Login: u.Login, Role: u.Role}
}

// authResponse is the body of a successful POST /v1/auth/github. Token is
// controld's own opaque bearer — never the GitHub token, which this API
// takes in and never gives back.
//
// Scopes is what GitHub said the presented token can do, echoed so the CLI
// can show it; Warning is present only when something about the token will
// bite later (v0: no `repo` scope, so git operations can't work yet).
type authResponse struct {
	Token   string   `json:"token"`
	User    userView `json:"user"`
	Scopes  string   `json:"scopes"`
	Warning string   `json:"warning,omitempty"`
}

// meResponse is the body of a successful GET /v1/me.
type meResponse struct {
	User userView `json:"user"`
}

// roleFor reports the role a GitHub login is allowed to log in as, per the
// configured allowlists: admin beats member when a login (unusually)
// appears in both. A login in neither list cannot log in at all — ok is
// false — which also means an empty Admins and Members together fail
// closed: nobody can log in.
//
// Matching is case-insensitive (EqualFold), because GitHub logins are: the
// same account answers to "Alice" and "alice", and GitHub returns whatever
// casing the account was registered with, not whatever the operator typed
// into RAINIER_ADMINS. A case-only mismatch would otherwise lock out exactly
// the person the allowlist was written for, with a 403 that looks like a
// policy decision rather than a typo.
func (s *Server) roleFor(login string) (role string, ok bool) {
	for _, a := range s.cfg.Admins {
		if strings.EqualFold(a, login) {
			return "admin", true
		}
	}
	for _, m := range s.cfg.Members {
		if strings.EqualFold(m, login) {
			return "member", true
		}
	}
	return "", false
}

// ---------------------------------------------------------------------------
// POST /v1/auth/github
// ---------------------------------------------------------------------------

// githubAuthRequest is the decoded body of POST /v1/auth/github.
type githubAuthRequest struct {
	AccessToken string `json:"access_token"`
}

// githubUser is the subset of GitHub's GET /user response this handler
// needs. It is untrusted third-party input and is validated before use.
type githubUser struct {
	ID    int64  `json:"id"`
	Login string `json:"login"`
}

// errGitHubUnauthorized is returned by fetchGitHubUser when GitHub itself
// rejects the presented access token (its 401), as distinct from any other
// upstream failure.
var errGitHubUnauthorized = errors.New("controld: github rejected the access token")

// githubScopesHeader is the response header GitHub puts a classic OAuth
// token's granted scopes in, on every authenticated API response — which is
// why the exchange can read it off the /user call it already makes instead of
// spending a second round-trip on it.
const githubScopesHeader = "X-OAuth-Scopes"

// gitScope is the GitHub scope a token needs before rainier can clone, pull,
// or push on the user's behalf. `public_repo` is deliberately not accepted as
// a substitute: it cannot read a private repository, and a login that
// silently passed with it would fail at the first private clone instead of
// here.
const gitScope = "repo"

// githubRepoScopeWarning is the exact text POST /v1/auth/github returns (and
// the CLI prints) when the presented token has no `repo` scope. The login
// still succeeds and the credential is still stored: the token is a perfectly
// good identity, it just can't do git yet, and saying so at login beats an
// unexplainable clone failure later.
const githubRepoScopeWarning = "token lacks repo scope; git operations will require rainier login --refresh github"

// hasGitScope reports whether the X-OAuth-Scopes value scopes grants
// gitScope. GitHub renders the header as a comma-separated list with spaces
// ("repo, read:user"), so each entry is trimmed and compared whole — a
// substring test would accept `repo:status` and `public_repo` alike.
//
// An empty header (a fine-grained token, or a provider that reports no
// scopes) reads as "not granted", which is the fail-safe direction: it
// produces a warning nobody has to act on rather than silence in front of a
// git setup that won't work.
func hasGitScope(scopes string) bool {
	for _, s := range strings.Split(scopes, ",") {
		if strings.TrimSpace(s) == gitScope {
			return true
		}
	}
	return false
}

// handleGitHubAuth serves POST /v1/auth/github: the one unauthenticated
// endpoint on this API by design. It exchanges a caller-supplied GitHub
// access token for controld's own opaque bearer token, gated by the
// configured admin/member allowlists.
//
// Since Plan 5 the GitHub token is also SEALED INTO THE VAULT rather than
// discarded — that is what lets a session's git mint from it later (spec
// §4.2) — but it is still never logged and never returned: the response
// carries controld's own token, the user's login and role, and the scopes
// GitHub reported. `rainier login --refresh github` is this same request
// again; the upsert is a whole-row replace, so a refresh is all it takes to
// clear a needs_refresh row.
func (s *Server) handleGitHubAuth(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, githubExchangeBodyLimit)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	var req githubAuthRequest
	if err := dec.Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_request", "malformed request body")
		return
	}
	if dec.More() {
		writeErr(w, http.StatusBadRequest, "invalid_request", "request body must contain a single JSON object")
		return
	}
	if req.AccessToken == "" {
		writeErr(w, http.StatusBadRequest, "invalid_request", "access_token is required")
		return
	}

	gu, scopes, err := s.fetchGitHubUser(r.Context(), req.AccessToken)
	switch {
	case errors.Is(err, errGitHubUnauthorized):
		writeErr(w, http.StatusUnauthorized, "unauthenticated", "invalid GitHub access token")
		return
	case err != nil:
		// The detail (upstream status, body, decode error) is exactly what
		// the api-design bar says never crosses this boundary; it goes to
		// the log instead.
		log.Printf("controld: github token exchange: %v", err)
		writeErr(w, http.StatusInternalServerError, "internal", "github token exchange failed")
		return
	}

	role, ok := s.roleFor(gu.Login)
	if !ok {
		writeErr(w, http.StatusForbidden, "forbidden", "not authorized")
		return
	}

	u, err := s.st.UpsertUser(r.Context(), gu.ID, gu.Login, role)
	if err != nil {
		log.Printf("controld: upsert user %q: %v", gu.Login, err)
		writeErr(w, http.StatusInternalServerError, "internal", "could not complete login")
		return
	}

	// The credential is stored BEFORE the bearer token is minted, so a login
	// that answers 200 is always a login whose git works: the failure mode
	// this ordering rules out is a caller holding a perfectly good token over
	// a vault with nothing in it, which would surface much later as a clone
	// that can't authenticate. A failure here loses nothing but this attempt
	// — logging in again is the whole retry.
	if err := s.storeGitHubCredential(r.Context(), u.ID, req.AccessToken, scopes); err != nil {
		// The error is from Seal or the store; neither carries the token,
		// and neither does this line.
		log.Printf("controld: storing github credential for user %s: %v", u.ID, err)
		writeErr(w, http.StatusInternalServerError, "internal", "could not complete login")
		return
	}

	tok, hash := NewToken()
	if err := s.st.InsertToken(r.Context(), u.ID, hash); err != nil {
		log.Printf("controld: insert token for user %s: %v", u.ID, err)
		writeErr(w, http.StatusInternalServerError, "internal", "could not complete login")
		return
	}

	resp := authResponse{Token: tok, User: userJSON(u), Scopes: scopes}
	if !hasGitScope(scopes) {
		// A warning, never a failure: the login is valid and the credential
		// is stored: only git is out of reach until the user re-logs in with
		// a broader token.
		resp.Warning = githubRepoScopeWarning
	}
	writeJSON(w, http.StatusOK, resp)
}

// fetchGitHubUser calls {GitHubAPIBase}/user with token as a bearer and
// validates the response shape — a third-party response is untrusted input
// exactly like a client request. It returns the user, the X-OAuth-Scopes
// header from that SAME response (GitHub reports a classic token's scopes on
// every authenticated response, so this costs no extra round-trip), and
// errGitHubUnauthorized for GitHub's own 401 or a wrapped error (detail for
// the log, never the caller) for anything else that goes wrong: a non-200/401
// status, unparseable JSON, or a shape that fails validation (id <= 0 or an
// empty login).
//
// Scopes are returned as GitHub spelled them, whitespace and all: they are
// stored and displayed verbatim, and only hasGitScope ever parses them.
func (s *Server) fetchGitHubUser(ctx context.Context, token string) (githubUser, string, error) {
	ctx, cancel := context.WithTimeout(ctx, githubCallTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.cfg.GitHubAPIBase+"/user", nil)
	if err != nil {
		return githubUser{}, "", fmt.Errorf("building github /user request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/vnd.github+json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return githubUser{}, "", fmt.Errorf("calling github /user: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return githubUser{}, "", errGitHubUnauthorized
	}
	if resp.StatusCode != http.StatusOK {
		return githubUser{}, "", fmt.Errorf("github /user: unexpected status %d", resp.StatusCode)
	}

	var gu githubUser
	if err := json.NewDecoder(io.LimitReader(resp.Body, githubUserBodyLimit)).Decode(&gu); err != nil {
		return githubUser{}, "", fmt.Errorf("decoding github /user response: %w", err)
	}
	if gu.ID <= 0 || gu.Login == "" {
		return githubUser{}, "", fmt.Errorf("github /user: invalid shape (id=%d, login=%q)", gu.ID, gu.Login)
	}
	return gu, resp.Header.Get(githubScopesHeader), nil
}

// ---------------------------------------------------------------------------
// bearer middleware + GET /v1/me
// ---------------------------------------------------------------------------

// requireUser wraps next behind bearer authentication: it parses
// "Authorization: Bearer rnr_...", hashes the token, and looks it up via
// the store. Any failure — missing header, wrong scheme, empty token, or a
// hash the store doesn't recognize — is the same 401 unauthenticated;
// requireUser never distinguishes "malformed" from "unknown" in its
// response, and it never logs the token itself.
func (s *Server) requireUser(next func(http.ResponseWriter, *http.Request, User)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		const prefix = "Bearer "
		authz := r.Header.Get("Authorization")
		if !strings.HasPrefix(authz, prefix) {
			writeErr(w, http.StatusUnauthorized, "unauthenticated", "missing bearer token")
			return
		}
		tok := strings.TrimPrefix(authz, prefix)
		if tok == "" {
			writeErr(w, http.StatusUnauthorized, "unauthenticated", "missing bearer token")
			return
		}

		u, err := s.st.UserByToken(r.Context(), HashToken(tok))
		if err != nil {
			if !errors.Is(err, ErrNotFound) {
				log.Printf("controld: user by token: %v", err)
			}
			writeErr(w, http.StatusUnauthorized, "unauthenticated", "invalid or expired token")
			return
		}
		next(w, r, u)
	}
}

// requireAdmin is requireUser plus the role check every fleet-wide mutation
// needs: team secrets and environments belong to the whole team, so unlike a
// session (owner-or-admin, authorizeOwnerOrAdmin) they have no owner to fall
// back on — only an admin may write them (design §4.5).
//
// The order matters and matches requireUser's own contract: an
// unauthenticated caller is 401 and learns nothing about what the route
// would have required; an authenticated non-admin is 403 forbidden.
func (s *Server) requireAdmin(next func(http.ResponseWriter, *http.Request, User)) http.HandlerFunc {
	return s.requireUser(func(w http.ResponseWriter, r *http.Request, u User) {
		if u.Role != "admin" {
			writeErr(w, http.StatusForbidden, "forbidden", "admin role required")
			return
		}
		next(w, r, u)
	})
}

// handleMe serves GET /v1/me: the caller's own identity and role.
func (s *Server) handleMe(w http.ResponseWriter, r *http.Request, u User) {
	writeJSON(w, http.StatusOK, meResponse{User: userJSON(u)})
}

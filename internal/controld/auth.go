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

// authResponse is the body of a successful POST /v1/auth/github.
type authResponse struct {
	Token string   `json:"token"`
	User  userView `json:"user"`
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
func (s *Server) roleFor(login string) (role string, ok bool) {
	for _, a := range s.cfg.Admins {
		if a == login {
			return "admin", true
		}
	}
	for _, m := range s.cfg.Members {
		if m == login {
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

// handleGitHubAuth serves POST /v1/auth/github: the one unauthenticated
// endpoint on this API by design. It exchanges a caller-supplied GitHub
// access token for controld's own opaque bearer token, gated by the
// configured admin/member allowlists. The GitHub token is used for exactly
// one upstream call and then discarded — it is never stored and never
// logged.
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

	gu, err := s.fetchGitHubUser(r.Context(), req.AccessToken)
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
	tok, hash := NewToken()
	if err := s.st.InsertToken(r.Context(), u.ID, hash); err != nil {
		log.Printf("controld: insert token for user %s: %v", u.ID, err)
		writeErr(w, http.StatusInternalServerError, "internal", "could not complete login")
		return
	}

	writeJSON(w, http.StatusOK, authResponse{Token: tok, User: userJSON(u)})
}

// fetchGitHubUser calls {GitHubAPIBase}/user with token as a bearer and
// validates the response shape — a third-party response is untrusted input
// exactly like a client request. It returns errGitHubUnauthorized for
// GitHub's own 401 and a wrapped error (detail for the log, never the
// caller) for anything else that goes wrong: a non-200/401 status,
// unparseable JSON, or a shape that fails validation (id <= 0 or an empty
// login).
func (s *Server) fetchGitHubUser(ctx context.Context, token string) (githubUser, error) {
	ctx, cancel := context.WithTimeout(ctx, githubCallTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.cfg.GitHubAPIBase+"/user", nil)
	if err != nil {
		return githubUser{}, fmt.Errorf("building github /user request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/vnd.github+json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return githubUser{}, fmt.Errorf("calling github /user: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return githubUser{}, errGitHubUnauthorized
	}
	if resp.StatusCode != http.StatusOK {
		return githubUser{}, fmt.Errorf("github /user: unexpected status %d", resp.StatusCode)
	}

	var gu githubUser
	if err := json.NewDecoder(io.LimitReader(resp.Body, githubUserBodyLimit)).Decode(&gu); err != nil {
		return githubUser{}, fmt.Errorf("decoding github /user response: %w", err)
	}
	if gu.ID <= 0 || gu.Login == "" {
		return githubUser{}, fmt.Errorf("github /user: invalid shape (id=%d, login=%q)", gu.ID, gu.Login)
	}
	return gu, nil
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

// handleMe serves GET /v1/me: the caller's own identity and role.
func (s *Server) handleMe(w http.ResponseWriter, r *http.Request, u User) {
	writeJSON(w, http.StatusOK, meResponse{User: userJSON(u)})
}

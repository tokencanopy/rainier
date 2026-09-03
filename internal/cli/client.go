package cli

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
)

// requestIDBytes/idempotencyKeyBytes both render as 16 hex characters —
// what the brief and controld's own randHex(8) convention both use for
// these headers.
const headerIDBytes = 8

// errBodyLimit caps how much of a non-2xx response body Do will ever read
// while trying to decode the error envelope. It's untrusted input (some of
// it, on a 5xx, may not even be controld's own text) and doesn't need to be
// large — the envelope's code+message are always small.
const errBodyLimit = 64 << 10

// Client is the rainier CLI's HTTP client: a controld base URL, the bearer
// token to authenticate with, and (optionally) a caller-supplied
// *http.Client — nil means http.DefaultClient.
//
// The remaining fields are what a hosted context adds, and a zero Client
// (every self-hosted caller, every test that builds one by hand) behaves
// exactly as it always did without them:
//
//   - Workspace, when set, is sent as Rainier-Workspace on every request:
//     the hosted edge scopes the call to that workspace.
//   - RefreshToken, when set, lets one 401 be answered by refreshing the
//     hosted token pair and retrying the request once, instead of surfacing
//     as an error the user cannot act on. SaveTokens persists the rotated
//     pair; it is nil for a client nobody expects to outlive the process.
type Client struct {
	Base  string
	Token string
	HTTP  *http.Client

	Workspace    string
	RefreshToken string
	SaveTokens   func(TokenPair) error

	// mu guards Token and RefreshToken across a refresh: a rotated pair
	// replaces both at once, and a request in flight must not be sent with
	// half of it.
	mu sync.Mutex
}

// NewClient builds the client for cfg's current context, carrying its
// workspace scope and — for a hosted context — the refresh token and the
// writer that saves a rotated pair back into that same context.
func NewClient(cfg Config) *Client {
	name := cfg.ActiveName()
	ctx, ok := cfg.Active()
	if !ok {
		// A Config assembled in memory from the legacy accessors alone (a
		// test, a tool) still names a server; honor it.
		ctx = Context{Server: cfg.ServerURL, Token: cfg.Token, OwnerID: cfg.OwnerID}
	}
	c := &Client{Base: ctx.Server, Token: ctx.Token, Workspace: ctx.Workspace, RefreshToken: ctx.RefreshToken}
	if ctx.Hosted() {
		c.SaveTokens = func(p TokenPair) error { return saveRotated(name, p) }
	}
	return c
}

// TokenPair is the hosted edge's token response — the body POST
// /v0/auth/login-attempts/{id}/exchange and POST /v0/auth/refresh both
// answer with. The refresh token is single-use: every refresh rotates it,
// and replaying a spent one revokes the chain.
type TokenPair struct {
	TokenType        string `json:"token_type"`
	AccessToken      string `json:"access_token"`
	AccessExpiresAt  string `json:"access_expires_at"`
	RefreshToken     string `json:"refresh_token"`
	RefreshExpiresAt string `json:"refresh_expires_at"`
}

// ErrLoginAgain is the one thing the CLI can say when a hosted context's
// credentials are past saving: the refresh was refused (a replayed,
// expired, or revoked token), or the refreshed pair was refused too. It
// carries no token material and no server detail — there is exactly one
// action left, and this sentence is it.
var ErrLoginAgain = errors.New("session expired: log in again with `rainier login --cloud`")

// saveRotated writes a rotated hosted pair back into the named context,
// leaving every other context — and which one is current — alone.
func saveRotated(name string, p TokenPair) error {
	cfg, err := Load()
	if err != nil {
		return err
	}
	ctx := cfg.Contexts[name]
	ctx.Token, ctx.RefreshToken, ctx.AccessExpiresAt = p.AccessToken, p.RefreshToken, p.AccessExpiresAt
	cfg.UpdateContext(name, ctx)
	return Save(cfg)
}

// Option adjusts one Do call's request before it's sent — the
// "path-independent option" mechanism for headers that aren't part of
// every request, namely Idempotency-Key.
type Option func(*http.Request)

// IdempotencyKey sets the Idempotency-Key header for one Do call. Callers
// that need one (POST /v0/sessions) generate a fresh key per invocation —
// Do itself never invents one, since only the caller knows which calls are
// retries of the same intent and which are new.
func IdempotencyKey(key string) Option {
	return func(r *http.Request) { r.Header.Set("Idempotency-Key", key) }
}

// errorEnvelope mirrors controld's {"error":{"code","message"}} — the one
// error shape every non-2xx response on that API returns.
type errorEnvelope struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// APIError is controld's stable non-2xx error envelope. Error preserves the
// CLI's existing "code: message" text while errors.As lets callers make
// decisions from Code instead of parsing prose.
type APIError struct {
	Code    string
	Message string
	// Status is the HTTP status the envelope arrived with. It is what tells
	// an expired credential (401) from an ordinary refusal that happens to
	// share a code, and it is not part of Error's text.
	Status int
}

func (e *APIError) Error() string { return e.Code + ": " + e.Message }

// RandHex returns n random bytes rendered as 2n lowercase hex characters —
// used for X-Request-Id (below) and by cmd/rainier for a fresh
// Idempotency-Key per `rainier new` invocation.
func RandHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand.Read failing means the OS's CSRF is unusable; nothing
		// this process does from here on is trustworthy either. This
		// mirrors controld's own randHex, which panics for the same reason.
		panic("cli: crypto/rand unavailable: " + err.Error())
	}
	return hex.EncodeToString(b)
}

// Do issues method against path (appended verbatim to c.Base — callers own
// their own query strings), marshaling in as the JSON body (skipped when
// in is nil) and decoding a 2xx response into out (skipped when out is
// nil). It always sets Authorization (when c.Token is set) and a fresh
// X-Request-Id; opts may add more headers (IdempotencyKey).
//
// Errors are send's, below — including the error-envelope decoding every
// non-2xx response on this API gets.
func (c *Client) Do(method, path string, in, out any, opts ...Option) error {
	return c.DoContext(context.Background(), method, path, in, out, opts...)
}

// DoContext is Do with the request bound to ctx. Use it for polling and
// other bounded operations so cancellation can interrupt a stalled transport.
func (c *Client) DoContext(ctx context.Context, method, path string, in, out any, opts ...Option) error {
	resp, err := c.send(ctx, method, path, in, opts...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return err
		}
	}
	return nil
}

// Open is Do for a response that is NOT JSON: it returns the successful
// response's body for the caller to read and close. The one such response on
// this API is a pull's archive (GET /v0/sessions/{id}/files), which is
// streamed and may be hundreds of megabytes — decoding it into memory the way
// Do does would defeat the point of streaming it.
//
// Failures are identical to Do's: a non-2xx is read as the error envelope and
// returned as an error, with the body already closed, so a caller only ever
// holds a body it is going to read.
func (c *Client) Open(method, path string, opts ...Option) (io.ReadCloser, error) {
	resp, err := c.send(context.Background(), method, path, nil, opts...)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// send performs one request and returns the response with its status already
// judged: a 2xx comes back with the body open and unread, anything else comes
// back as an error with the body closed.
//
// For a hosted context (one carrying a refresh token) a 401 is answered by
// refreshing the token pair once and retrying the request once — the whole
// point of a short-lived access token is that its expiry is invisible. One
// retry, never a loop: a second 401, or a refusal to refresh at all, is
// ErrLoginAgain.
func (c *Client) send(ctx context.Context, method, path string, in any, opts ...Option) (*http.Response, error) {
	resp, err := c.attempt(ctx, method, path, in, opts...)
	if err == nil || !unauthorized(err) {
		return resp, err
	}
	c.mu.Lock()
	canRefresh := c.RefreshToken != ""
	c.mu.Unlock()
	if !canRefresh {
		return nil, err
	}
	if rerr := c.refresh(ctx); rerr != nil {
		return nil, rerr
	}
	resp, err = c.attempt(ctx, method, path, in, opts...)
	if err != nil && unauthorized(err) {
		return nil, ErrLoginAgain
	}
	return resp, err
}

// unauthorized reports whether err is the API's own 401 — the only status a
// refresh can do anything about.
func unauthorized(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.Status == http.StatusUnauthorized
}

// refresh exchanges the stored refresh token for a new pair and installs it.
// The edge rotates the refresh token on every use, so the new one is saved
// before the retry: losing it would strand the context on a token that is
// already spent. A 401 here — credential_replayed, expired, revoked — is
// terminal.
func (c *Client) refresh(ctx context.Context) error {
	c.mu.Lock()
	rt := c.RefreshToken
	c.mu.Unlock()

	body, err := json.Marshal(map[string]string{"refresh_token": rt})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.Base+"/v0/auth/refresh", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Request-Id", RandHex(headerIDBytes))

	httpClient := c.HTTP
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return ErrLoginAgain
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("refreshing the session: unexpected response: %d", resp.StatusCode)
	}
	var pair TokenPair
	if err := json.NewDecoder(io.LimitReader(resp.Body, errBodyLimit)).Decode(&pair); err != nil {
		return fmt.Errorf("refreshing the session: %w", err)
	}
	if pair.AccessToken == "" {
		return ErrLoginAgain
	}

	c.mu.Lock()
	c.Token, c.RefreshToken = pair.AccessToken, pair.RefreshToken
	save := c.SaveTokens
	c.mu.Unlock()
	if save != nil {
		if err := save(pair); err != nil {
			return fmt.Errorf("saving the refreshed session: %w", err)
		}
	}
	return nil
}

// attempt is one request — send without the refresh.
//
// A non-2xx response is decoded as controld's error envelope and returned as
// fmt.Errorf("%s: %s", code, message); a body that isn't that shape at all
// (an upstream proxy's plain-text 502, a truncated response) falls back to a
// generic error carrying the status and a clipped body rather than panicking
// on the failed decode. A transport failure (DNS, connection refused, timeout)
// is returned exactly as http.Client.Do returned it — no wrapping — so callers
// and tests can match on the underlying error type.
func (c *Client) attempt(ctx context.Context, method, path string, in any, opts ...Option) (*http.Response, error) {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.Base+path, body)
	if err != nil {
		return nil, err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	c.mu.Lock()
	token := c.Token
	c.mu.Unlock()
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if c.Workspace != "" {
		req.Header.Set("Rainier-Workspace", c.Workspace)
	}
	req.Header.Set("X-Request-Id", RandHex(headerIDBytes))
	for _, o := range opts {
		o(req)
	}

	httpClient := c.HTTP
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		data, _ := io.ReadAll(io.LimitReader(resp.Body, errBodyLimit))
		var env errorEnvelope
		if err := json.Unmarshal(data, &env); err != nil || env.Error.Code == "" {
			const clip = 200
			text := string(data)
			if len(text) > clip {
				text = text[:clip]
			}
			return nil, fmt.Errorf("unexpected response: %d %s", resp.StatusCode, text)
		}
		return nil, &APIError{Code: env.Error.Code, Message: env.Error.Message, Status: resp.StatusCode}
	}
	return resp, nil
}

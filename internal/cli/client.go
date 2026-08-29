package cli

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
type Client struct {
	Base  string
	Token string
	HTTP  *http.Client
}

// Option adjusts one Do call's request before it's sent — the
// "path-independent option" mechanism for headers that aren't part of
// every request, namely Idempotency-Key.
type Option func(*http.Request)

// IdempotencyKey sets the Idempotency-Key header for one Do call. Callers
// that need one (POST /v1/sessions) generate a fresh key per invocation —
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
// A non-2xx response is decoded as controld's error envelope and returned
// as fmt.Errorf("%s: %s", code, message); a body that isn't that shape at
// all (an upstream proxy's plain-text 502, a truncated response) falls back
// to a generic error carrying the status and a clipped body rather than
// panicking on the failed decode. A transport failure (DNS, connection
// refused, timeout) is returned exactly as http.Client.Do returned it — no
// wrapping — so callers and tests can match on the underlying error type.
func (c *Client) Do(method, path string, in, out any, opts ...Option) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}

	req, err := http.NewRequest(method, c.Base+path, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
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
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, errBodyLimit))
		var env errorEnvelope
		if err := json.Unmarshal(data, &env); err != nil || env.Error.Code == "" {
			const clip = 200
			text := string(data)
			if len(text) > clip {
				text = text[:clip]
			}
			return fmt.Errorf("unexpected response: %d %s", resp.StatusCode, text)
		}
		return fmt.Errorf("%s: %s", env.Error.Code, env.Error.Message)
	}

	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return err
		}
	}
	return nil
}

package cli

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPErrorMetadataSurvivesNonEnvelope(t *testing.T) {
	for _, body := range []string{`{"error":{"code":"limited","message":"wait"}}`, "upstream unavailable"} {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Retry-After", "12")
			w.WriteHeader(429)
			fmt.Fprint(w, body)
		}))
		c := Client{Base: ts.URL}
		err := c.DoContext(context.Background(), http.MethodGet, "/v0/me", nil, nil)
		ts.Close()
		var api *APIError
		if !errors.As(err, &api) || api.Status != 429 || api.RetryAfter != "12" {
			t.Fatalf("metadata lost: %v", err)
		}
	}
}

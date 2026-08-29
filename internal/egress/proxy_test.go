package egress

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// startOrigin is a plain TCP echo server the proxy will tunnel to when allowed.
func startOrigin(t *testing.T) (host string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { io.Copy(c, c); c.Close() }()
		}
	}()
	return ln.Addr().String(), func() { ln.Close() }
}

func connectThrough(t *testing.T, proxyURL, target, session string) (*http.Response, net.Conn) {
	t.Helper()
	return connectWithAuth(t, proxyURL, target, "Bearer "+session)
}

// connectWithAuth is connectThrough's more general form: it sends whatever
// literal Proxy-Authorization value the caller supplies (or none at all —
// pass "" to omit the header), so tests can drive the Basic-auth and
// malformed-header paths directly instead of only the Bearer form.
func connectWithAuth(t *testing.T, proxyURL, target, authValue string) (*http.Response, net.Conn) {
	t.Helper()
	u := strings.TrimPrefix(proxyURL, "http://")
	conn, err := net.Dial("tcp", u)
	if err != nil {
		t.Fatal(err)
	}
	authHeader := ""
	if authValue != "" {
		authHeader = "Proxy-Authorization: " + authValue + "\r\n"
	}
	req := fmt.Sprintf("CONNECT %s HTTP/1.1\r\nHost: %s\r\n%s\r\n", target, target, authHeader)
	conn.Write([]byte(req))
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	return resp, conn
}

func TestDefaultDenyAndAllow(t *testing.T) {
	origin, stop := startOrigin(t)
	defer stop()
	var audit bytes.Buffer
	p := New(&audit)
	srv := httptest.NewServer(p.Handler())
	defer srv.Close()

	// No allow entry → deny.
	resp, _ := connectThrough(t, srv.URL, origin, "sessA")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 for unlisted session, got %d", resp.StatusCode)
	}

	// Allow the origin's host for sessA → tunnel succeeds and echoes.
	host, _, _ := net.SplitHostPort(origin)
	p.SetAllow("sessA", []string{host})
	resp2, conn := connectThrough(t, srv.URL, origin, "sessA")
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 after allow, got %d", resp2.StatusCode)
	}
	conn.Write([]byte("ping"))
	buf := make([]byte, 4)
	io.ReadFull(conn, buf)
	if string(buf) != "ping" {
		t.Fatalf("echo through tunnel = %q", buf)
	}
	conn.Close()

	if !strings.Contains(audit.String(), `"decision":"deny"`) ||
		!strings.Contains(audit.String(), `"decision":"allow"`) {
		t.Fatalf("audit missing decisions: %s", audit.String())
	}
}

// TestWildcardBoundary regression-tests the *.suffix matcher against the
// bypass-prone cases: a real subdomain must be permitted, but a look-alike
// hostname that merely contains the pattern (no dot boundary, or a wrong
// tail) or the bare apex domain itself must not be.
func TestWildcardBoundary(t *testing.T) {
	var audit bytes.Buffer
	p := New(&audit)
	srv := httptest.NewServer(p.Handler())
	defer srv.Close()

	p.SetAllow("sessB", []string{"*.example.com"})

	// Permitted: a genuine subdomain under the wildcard suffix. The upstream
	// dial for this synthetic hostname will typically fail (no such host),
	// but that's irrelevant here — we only assert the allow gate passed,
	// i.e. the proxy never answered 403 for it.
	resp, conn := connectThrough(t, srv.URL, "a.example.com:443", "sessB")
	if resp.StatusCode == http.StatusForbidden {
		t.Fatalf("expected a.example.com to be permitted by *.example.com, got 403")
	}
	conn.Close()

	denyCases := []string{
		"evil-example.com:443",         // shares the suffix text but no dot boundary
		"example.com.attacker.net:443", // contains the pattern, wrong tail
		"example.com:443",              // bare apex domain, not a subdomain
	}
	for _, target := range denyCases {
		resp, _ := connectThrough(t, srv.URL, target, "sessB")
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("expected %s to be denied (403), got %d", target, resp.StatusCode)
		}
	}

	if !strings.Contains(audit.String(), `"host":"a.example.com"`) ||
		!strings.Contains(audit.String(), `"decision":"allow"`) {
		t.Fatalf("audit missing allow line for wildcard subdomain: %s", audit.String())
	}
}

// TestMissingAuthHeaderDenied regression-tests that a CONNECT with no
// Proxy-Authorization header at all resolves to the empty-string session,
// which (per default-deny) must never be implicitly allowed.
func TestMissingAuthHeaderDenied(t *testing.T) {
	var audit bytes.Buffer
	p := New(&audit)
	srv := httptest.NewServer(p.Handler())
	defer srv.Close()

	u := strings.TrimPrefix(srv.URL, "http://")
	conn, err := net.Dial("tcp", u)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	target := "example.com:443"
	req := fmt.Sprintf("CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, target)
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 for CONNECT with no Proxy-Authorization header, got %d", resp.StatusCode)
	}
}

// TestBasicAuthSessionIdentity is the env-var proxy flow (Task 13, R4):
// curl/wget emit `Proxy-Authorization: Basic base64(session-id:)` — never
// Bearer — when the proxy URL carries the session id as URL userinfo
// (http://<session-id>:@host:port), which is the only way a plain
// HTTP_PROXY env var can carry identity at all (there is no way to set a
// literal header from an env var). egressd must decode this and extract the
// same session identity Bearer would have carried, not the whole "Basic
// ..." string verbatim (that would never match any real allow entry and
// every such request would silently default-deny).
func TestBasicAuthSessionIdentity(t *testing.T) {
	cases := []struct {
		name  string
		creds string // "<username>:<password>" before base64 encoding
	}{
		{
			// Matches exactly what curl sends for
			// https_proxy="http://sess-42:@host:port" (verified against real
			// curl during the Task 13 spike): username "sess-42", empty
			// password, still colon-terminated.
			"empty password (the driver's actual form)", "sess-42:",
		},
		{
			// Review round 1, finding 3: a non-empty password must not
			// leak into or corrupt the extracted session id — only the
			// username half (before the first colon) is the identity,
			// regardless of what follows it. Guards against a future
			// change accidentally using the whole decoded string as the
			// session id instead of cutting on the colon.
			"non-empty password must still yield just the username", "sess-42:somepassword",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			origin, stop := startOrigin(t)
			defer stop()
			var audit bytes.Buffer
			p := New(&audit)
			srv := httptest.NewServer(p.Handler())
			defer srv.Close()

			host, _, _ := net.SplitHostPort(origin)
			p.SetAllow("sess-42", []string{host})

			creds := base64.StdEncoding.EncodeToString([]byte(c.creds))
			resp, conn := connectWithAuth(t, srv.URL, origin, "Basic "+creds)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("expected 200 for allowlisted session via Basic auth, got %d", resp.StatusCode)
			}
			conn.Close()

			if !strings.Contains(audit.String(), `"session":"sess-42"`) {
				t.Fatalf("audit does not show the decoded session id sess-42: %s", audit.String())
			}
			if strings.Contains(audit.String(), "Basic ") {
				t.Fatalf("audit leaked the raw Basic header instead of the decoded session id: %s", audit.String())
			}
		})
	}
}

// TestBasicAuthDeniedWhenNotAllowlisted mirrors TestDefaultDenyAndAllow but
// for the Basic-auth path: a session correctly identified via Basic auth
// still goes through the same allowlist check as Bearer — decoding the
// header correctly must not accidentally bypass the allow gate.
func TestBasicAuthDeniedWhenNotAllowlisted(t *testing.T) {
	var audit bytes.Buffer
	p := New(&audit)
	srv := httptest.NewServer(p.Handler())
	defer srv.Close()

	creds := base64.StdEncoding.EncodeToString([]byte("sess-99:"))
	resp, _ := connectWithAuth(t, srv.URL, "example.com:443", "Basic "+creds)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 for a Basic-identified session with no allow entry, got %d", resp.StatusCode)
	}
}

// TestMalformedProxyAuthDenied regression-tests every way a
// Proxy-Authorization header can fail to parse cleanly — invalid base64, a
// decoded value with no colon, and an unrecognized scheme entirely — all of
// which must fall through to the empty-string session (default-deny), never
// panic or crash the handler goroutine.
func TestMalformedProxyAuthDenied(t *testing.T) {
	var audit bytes.Buffer
	p := New(&audit)
	srv := httptest.NewServer(p.Handler())
	defer srv.Close()
	// An empty-session allow entry must not exist, so if malformed parsing
	// ever produced "" as a session id, this proves it's still denied too —
	// but the real assertion below is simply "still 403", not this rule.

	cases := []struct {
		name string
		auth string
	}{
		{"invalid base64", "Basic not-valid-base64!!!"},
		{"decoded with no colon at all", "Basic " + base64.StdEncoding.EncodeToString([]byte("sess-1"))},
		{"unrecognized scheme", "Digest sess-1"},
		{"empty Basic payload", "Basic "},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resp, _ := connectWithAuth(t, srv.URL, "example.com:443", c.auth)
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("expected 403 for malformed auth %q, got %d", c.auth, resp.StatusCode)
			}
		})
	}
}

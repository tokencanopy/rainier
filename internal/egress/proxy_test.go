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

// TestMissingAuthHeaderChallenged is the regression pin for the bug Plan 5's
// first live GitHub rehearsal found: a CONNECT with no Proxy-Authorization
// header must be CHALLENGED (407 + Proxy-Authenticate), not refused (403).
//
// The old 403 was invisible for as long as every egress test used curl, which
// sends the proxy URL's userinfo preemptively and so never needs a challenge.
// git does not: it sets CURLAUTH_ANY, sends its first CONNECT bare, and takes
// a 403 as final — so on any fleet where egressd enforces, a session with a
// github connector could not clone. TestGitThroughProxy (docker-gated) proves
// the client half; this proves the wire answer it depends on.
//
// The 407 grants nothing. It opens no tunnel, and the retry it invites is
// checked by the same allowlist as everything else (TestDefaultDenyAndAllow,
// TestBasicAuthDeniedWhenNotAllowlisted).
func TestMissingAuthHeaderChallenged(t *testing.T) {
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
	if resp.StatusCode != http.StatusProxyAuthRequired {
		t.Fatalf("expected 407 for CONNECT with no Proxy-Authorization header, got %d", resp.StatusCode)
	}
	// The header is the half that does the work: a 407 with no
	// Proxy-Authenticate names no scheme, and a CURLAUTH_ANY client still has
	// nothing it is allowed to send.
	if got := resp.Header.Get("Proxy-Authenticate"); got != `Basic realm="rainier"` {
		t.Fatalf("Proxy-Authenticate = %q, want `Basic realm=\"rainier\"`", got)
	}
	// The audit says "challenge", not "deny": the two are different events and
	// reading one as the other is what made the live failure look like a
	// policy denial for an empty session id.
	if !strings.Contains(audit.String(), `"decision":"challenge"`) {
		t.Fatalf("audit does not record the challenge: %s", audit.String())
	}
	if strings.Contains(audit.String(), `"decision":"deny"`) {
		t.Fatalf("audit logged a denial for a request that was only challenged: %s", audit.String())
	}
}

// TestChallengedClientRetryIsAllowlisted is the two-step exchange a
// challenge-response client actually performs, end to end against the real
// handler: bare CONNECT → 407, then the same CONNECT carrying the credentials
// the challenge asked for → tunnel. It is the unit-level shape of what
// TestGitThroughProxy proves with a real git.
func TestChallengedClientRetryIsAllowlisted(t *testing.T) {
	origin, stop := startOrigin(t)
	defer stop()
	var audit bytes.Buffer
	p := New(&audit)
	srv := httptest.NewServer(p.Handler())
	defer srv.Close()

	host, _, _ := net.SplitHostPort(origin)
	p.SetAllow("sess-retry", []string{host})

	resp, conn := connectWithAuth(t, srv.URL, origin, "")
	if resp.StatusCode != http.StatusProxyAuthRequired {
		t.Fatalf("first CONNECT: got %d, want 407", resp.StatusCode)
	}
	conn.Close()

	creds := base64.StdEncoding.EncodeToString([]byte("sess-retry:"))
	resp2, conn2 := connectWithAuth(t, srv.URL, origin, "Basic "+creds)
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("retry with credentials: got %d, want 200", resp2.StatusCode)
	}
	defer conn2.Close()
	conn2.Write([]byte("ping"))
	buf := make([]byte, 4)
	io.ReadFull(conn2, buf)
	if string(buf) != "ping" {
		t.Fatalf("echo through the tunnel the retry opened = %q", buf)
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

// TestMalformedProxyAuthNeverTunnels regression-tests every way a
// Proxy-Authorization header can fail to parse cleanly — invalid base64, a
// decoded value with no colon, and an unrecognized scheme entirely — none of
// which may ever open a tunnel, panic, or crash the handler goroutine.
//
// A header that did not parse asserts no identity, so it gets the same 407
// challenge a missing one gets: from the proxy's side "you sent me nothing
// usable" and "you sent me nothing" are the same fact, and asking again is the
// answer that lets a client whose credential was mangled recover. The
// exception is the one case that DOES parse into an identity — a decoded value
// with no colon at all is read as the whole session id (see
// sessionFromProxyAuth) — and an identified session with no allow entry is a
// denial, 403, exactly as before.
//
// Neither answer tunnels, which is the property this test actually holds.
func TestMalformedProxyAuthNeverTunnels(t *testing.T) {
	var audit bytes.Buffer
	p := New(&audit)
	srv := httptest.NewServer(p.Handler())
	defer srv.Close()
	// No allow entry exists for any session here, so nothing below could be
	// permitted even if it were identified.

	cases := []struct {
		name string
		auth string
		want int
	}{
		{"invalid base64", "Basic not-valid-base64!!!", http.StatusProxyAuthRequired},
		{"unrecognized scheme", "Digest sess-1", http.StatusProxyAuthRequired},
		{"empty Basic payload", "Basic ", http.StatusProxyAuthRequired},
		{"no header at all", "", http.StatusProxyAuthRequired},
		{
			// Parses: the whole decoded value is the session id, so this is an
			// identified client asking for a host it has no allow entry for.
			"decoded with no colon at all is an identity, so a denial",
			"Basic " + base64.StdEncoding.EncodeToString([]byte("sess-1")),
			http.StatusForbidden,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resp, conn := connectWithAuth(t, srv.URL, "example.com:443", c.auth)
			defer conn.Close()
			if resp.StatusCode != c.want {
				t.Fatalf("auth %q: got %d, want %d", c.auth, resp.StatusCode, c.want)
			}
			if resp.StatusCode == http.StatusOK {
				t.Fatalf("auth %q opened a tunnel", c.auth)
			}
		})
	}
}

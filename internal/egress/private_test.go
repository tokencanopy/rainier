package egress

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
)

// The allowlist matches the NAME a client wrote into its CONNECT line. What
// that name resolves to is a nameserver's answer, and a default egress list
// that carries a dozen public package hosts makes "an allowlisted name that
// resolves somewhere private" a considerably more interesting thing to try
// than it was when the list was whatever one operator typed.
//
// These tests state the resolver's answer directly rather than standing up a
// DNS server, because what is under test is the proxy's response to an answer,
// and a test that had to control real resolution would prove less and skip
// more.

// fixedResolver answers every lookup with addrs.
func fixedResolver(addrs ...string) func(context.Context, string) ([]netip.Addr, error) {
	return func(context.Context, string) ([]netip.Addr, error) {
		out := make([]netip.Addr, 0, len(addrs))
		for _, a := range addrs {
			out = append(out, netip.MustParseAddr(a))
		}
		return out, nil
	}
}

// connectTo opens a CONNECT for target through the proxy at proxyURL as
// session, and returns the response.
func connectTo(t *testing.T, proxyURL, session, target string) *http.Response {
	t.Helper()
	conn, err := net.Dial("tcp", strings.TrimPrefix(proxyURL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\nProxy-Authorization: Bearer %s\r\n\r\n", target, target, session)
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// TestAllowlistedNameResolvingPrivateIsDenied is the rebinding case: the name
// is on the session's list, and the answer is the cloud metadata address that
// hands out the RUNNER's own instance credentials. The allowlist says yes and
// the destination is the one place a sandbox must never reach.
func TestAllowlistedNameResolvingPrivateIsDenied(t *testing.T) {
	for name, addr := range map[string]string{
		"cloud metadata":    "169.254.169.254",
		"RFC1918":           "10.1.2.3",
		"loopback":          "127.0.0.1",
		"carrier-grade NAT": "100.64.0.1",
		"IPv6 unique-local": "fd00::1",
		"IPv6 link-local":   "fe80::1",
		"4-in-6 metadata":   "::ffff:169.254.169.254",
		"unspecified":       "0.0.0.0",
	} {
		t.Run(name, func(t *testing.T) {
			var audit bytes.Buffer
			p := New(&audit)
			p.resolve = fixedResolver(addr)
			p.SetAllow("sess-a", []string{"registry.example.test"})
			srv := httptest.NewServer(p.Handler())
			defer srv.Close()

			resp := connectTo(t, srv.URL, "sess-a", "registry.example.test:443")
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("CONNECT to an allowlisted name resolving to %s = %d, want 403", addr, resp.StatusCode)
			}
			if !strings.Contains(audit.String(), `"decision":"deny_private"`) {
				t.Errorf("audit does not record the refusal as a private-destination one:\n%s", audit.String())
			}
		})
	}
}

// TestVettedAddrsKeepsOnlyThePublicAnswers: a name answering with both a
// public and a private address is not refused — a dual-stack or split-horizon
// answer is ordinary — but only the public addresses survive to be dialed,
// and dialing the vetted ADDRESS rather than re-resolving the name is what
// closes the window between the check and the connection.
func TestVettedAddrsKeepsOnlyThePublicAnswers(t *testing.T) {
	p := New(nil)
	p.resolve = fixedResolver("10.0.0.7", "93.184.216.34", "169.254.169.254", "2606:4700::1111")

	got, err := p.vettedAddrs(context.Background(), "registry.example.test")
	if err != nil {
		t.Fatalf("vettedAddrs: %v", err)
	}
	want := []string{"93.184.216.34", "2606:4700::1111"}
	if len(got) != len(want) {
		t.Fatalf("vettedAddrs = %v, want %v", got, want)
	}
	for i, addr := range got {
		if addr.String() != want[i] {
			t.Fatalf("vettedAddrs = %v, want %v", got, want)
		}
	}

	// An answer with nothing public left is the refusal, not an empty allow.
	p.resolve = fixedResolver("10.0.0.7", "169.254.169.254")
	if _, err := p.vettedAddrs(context.Background(), "registry.example.test"); !errors.Is(err, errPrivateDestination) {
		t.Fatalf("vettedAddrs error = %v, want errPrivateDestination", err)
	}
}

// TestLiteralPrivateAddressIsDenied: a client that skips DNS entirely and
// CONNECTs to an address gets the same answer, provided the address is
// somehow on its list.
func TestLiteralPrivateAddressIsDenied(t *testing.T) {
	var audit bytes.Buffer
	p := New(&audit)
	p.SetAllow("sess-a", []string{"169.254.169.254"})
	srv := httptest.NewServer(p.Handler())
	defer srv.Close()

	resp := connectTo(t, srv.URL, "sess-a", "169.254.169.254:80")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("CONNECT to a literal metadata address = %d, want 403", resp.StatusCode)
	}
	if !strings.Contains(audit.String(), `"decision":"deny_private"`) {
		t.Errorf("audit does not record the refusal as a private-destination one:\n%s", audit.String())
	}
}

// TestAllowPrivateDestinationsLiftsTheGuard, because a local fleet's origins
// and this package's own fixtures live on loopback and must still be
// reachable when a host says so.
func TestAllowPrivateDestinationsLiftsTheGuard(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer origin.Close()
	host, port, err := net.SplitHostPort(strings.TrimPrefix(origin.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}

	var audit bytes.Buffer
	p := New(&audit, AllowPrivateDestinations())
	p.SetAllow("sess-a", []string{host})
	srv := httptest.NewServer(p.Handler())
	defer srv.Close()

	resp := connectTo(t, srv.URL, "sess-a", net.JoinHostPort(host, port))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT with the guard lifted = %d, want 200", resp.StatusCode)
	}
}

// TestRedirectTargetIsCheckedSeparately. A redirect is not a proxy concern at
// all — the client follows it — and that is exactly the property worth
// pinning: following one means a NEW CONNECT, for the new host, through the
// same allowlist. A permitted host cannot bounce a session onto a host the
// session was never granted, and it certainly cannot bounce it onto a private
// address, which is the "redirect broadens access" shape.
func TestRedirectTargetIsCheckedSeparately(t *testing.T) {
	var audit bytes.Buffer
	p := New(&audit)
	p.resolve = fixedResolver("93.184.216.34") // a public answer for anything
	p.SetAllow("sess-a", []string{"registry.example.test"})
	srv := httptest.NewServer(p.Handler())
	defer srv.Close()

	// The host the redirect pointed at is not on the list.
	if resp := connectTo(t, srv.URL, "sess-a", "cdn.example.invalid:443"); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("CONNECT to a redirect target that is not allowlisted = %d, want 403", resp.StatusCode)
	}
	// And a redirect that pointed at a private address is refused by the
	// guard even when its host IS allowlisted.
	p.resolve = fixedResolver("169.254.169.254")
	if resp := connectTo(t, srv.URL, "sess-a", "registry.example.test:443"); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("CONNECT to an allowlisted redirect target resolving privately = %d, want 403", resp.StatusCode)
	}
}

// TestUnresolvableHostIsStillAnAllow keeps the audit log's vocabulary honest:
// a name the resolver cannot answer for is not a policy refusal, it is an
// allowed request whose dial failed. Muddling the two would put a "denied"
// line in the log on every transient DNS blip.
func TestUnresolvableHostIsStillAnAllow(t *testing.T) {
	var audit bytes.Buffer
	p := New(&audit)
	p.resolve = func(context.Context, string) ([]netip.Addr, error) {
		return nil, errors.New("no such host")
	}
	p.SetAllow("sess-a", []string{"registry.example.test"})
	srv := httptest.NewServer(p.Handler())
	defer srv.Close()

	resp := connectTo(t, srv.URL, "sess-a", "registry.example.test:443")
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("CONNECT to an unresolvable allowlisted name = %d, want 502", resp.StatusCode)
	}
	if !strings.Contains(audit.String(), `"decision":"allow"`) {
		t.Errorf("audit should record the allowlist's own verdict:\n%s", audit.String())
	}
}

// TestIsPrivateAddrClassification is the table the guard turns on, stated
// directly so a range that stops being refused fails here and not in a
// postmortem.
func TestIsPrivateAddrClassification(t *testing.T) {
	private := []string{
		"0.0.0.0", "127.0.0.1", "10.0.0.1", "172.16.0.1", "192.168.1.1",
		"169.254.169.254", "100.64.0.1", "100.100.100.200", "192.0.0.1",
		"198.18.0.1", "240.0.0.1", "224.0.0.1",
		"::1", "fd00::1", "fe80::1", "::", "ff02::1", "::ffff:10.0.0.1",
	}
	for _, s := range private {
		if !isPrivateAddr(netip.MustParseAddr(s)) {
			t.Errorf("isPrivateAddr(%s) = false, want true", s)
		}
	}
	public := []string{"1.1.1.1", "8.8.8.8", "93.184.216.34", "104.16.0.1", "2606:4700::1111"}
	for _, s := range public {
		if isPrivateAddr(netip.MustParseAddr(s)) {
			t.Errorf("isPrivateAddr(%s) = true, want false", s)
		}
	}
}

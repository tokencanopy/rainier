package egress

import (
	"bufio"
	"bytes"
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
	if err != nil { t.Fatal(err) }
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil { return }
			go func() { io.Copy(c, c); c.Close() }()
		}
	}()
	return ln.Addr().String(), func() { ln.Close() }
}

func connectThrough(t *testing.T, proxyURL, target, session string) (*http.Response, net.Conn) {
	t.Helper()
	u := strings.TrimPrefix(proxyURL, "http://")
	conn, err := net.Dial("tcp", u)
	if err != nil { t.Fatal(err) }
	req := fmt.Sprintf("CONNECT %s HTTP/1.1\r\nHost: %s\r\nProxy-Authorization: Bearer %s\r\n\r\n", target, target, session)
	conn.Write([]byte(req))
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil { t.Fatal(err) }
	return resp, conn
}

func TestDefaultDenyAndAllow(t *testing.T) {
	origin, stop := startOrigin(t); defer stop()
	var audit bytes.Buffer
	p := New(&audit)
	srv := httptest.NewServer(p.Handler()); defer srv.Close()

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
	if string(buf) != "ping" { t.Fatalf("echo through tunnel = %q", buf) }
	conn.Close()

	if !strings.Contains(audit.String(), `"decision":"deny"`) ||
		!strings.Contains(audit.String(), `"decision":"allow"`) {
		t.Fatalf("audit missing decisions: %s", audit.String())
	}
}

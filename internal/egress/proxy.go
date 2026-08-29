// Package egress is a per-VM CONNECT proxy: default-deny, per-session allowlist,
// audit log. It is the only path out for session containers on the internal
// network (spec §8). The session is identified by its Proxy-Authorization
// header, either a literal Bearer token (the session id) or — the form a
// plain HTTP_PROXY/HTTPS_PROXY env var actually produces, since curl/wget
// have no way to set a literal header from an env var — HTTP Basic auth
// decoded from the proxy URL's userinfo (http://<session-id>:@host:port),
// added for egress R4's env-var proxy flow (Task 13; see
// sessionFromProxyAuth).
package egress

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Rule struct {
	Session string
	Allow   []string
}

type Proxy struct {
	mu    sync.RWMutex
	allow map[string][]string
	audit io.Writer
	now   func() time.Time
}

func New(audit io.Writer) *Proxy {
	return &Proxy{allow: map[string][]string{}, audit: audit, now: time.Now}
}

func (p *Proxy) SetAllow(session string, hosts []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.allow[session] = append([]string(nil), hosts...)
}

func (p *Proxy) permitted(session, host string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, pat := range p.allow[session] {
		if pat == host {
			return true
		}
		if strings.HasPrefix(pat, "*.") && strings.HasSuffix(host, pat[1:]) {
			return true
		}
	}
	return false
}

func (p *Proxy) logDecision(session, host, port, decision string) {
	if p.audit == nil {
		return
	}
	line, _ := json.Marshal(map[string]string{
		"session": session, "host": host, "port": port,
		"decision": decision, "ts": p.now().UTC().Format(time.RFC3339),
	})
	fmt.Fprintln(p.audit, string(line))
}

// sessionFromProxyAuth extracts the session id a client asserted via its
// Proxy-Authorization header, in either of two forms:
//
//   - "Bearer <session-id>" — sent by anything that can set a literal
//     header (e.g. a client speaking CONNECT directly).
//   - "Basic base64(<session-id>:)" — what curl and wget actually send, with
//     no way to ask for anything else, when their proxy URL carries the
//     session id as URL userinfo (http://<session-id>:@host:port). A plain
//     HTTP_PROXY/HTTPS_PROXY env var has no way to carry an arbitrary header
//     at all — URL userinfo is the only channel curl-family tools expose,
//     and they always encode it as HTTP Basic auth on the CONNECT request,
//     never Bearer. This is how egress R4's env-var proxy flow carries
//     session identity at all (Task 13).
//
// Anything else — no header, an unrecognized scheme, invalid base64, empty
// Basic payload — returns "", which permitted() never matches against any
// real allow entry, so it falls through to default-deny rather than ever
// panicking or silently trusting an unparseable header.
func sessionFromProxyAuth(header string) string {
	if s, ok := strings.CutPrefix(header, "Bearer "); ok {
		return s
	}
	b64, ok := strings.CutPrefix(header, "Basic ")
	if !ok || b64 == "" {
		return ""
	}
	decoded, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return ""
	}
	// curl/wget always encode "<session-id>:" (empty password, colon kept) —
	// split on the first colon and take the username half. A decoded value
	// with no colon at all is treated as the whole session id rather than
	// rejected outright: it still has to match a real SetAllow entry to be
	// permitted, so being lenient here costs nothing and only helps a
	// hand-built client that omitted the trailing colon.
	if user, _, found := strings.Cut(string(decoded), ":"); found {
		return user
	}
	return string(decoded)
}

func (p *Proxy) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "only CONNECT supported", http.StatusMethodNotAllowed)
			return
		}
		session := sessionFromProxyAuth(r.Header.Get("Proxy-Authorization"))
		host, port, err := net.SplitHostPort(r.Host)
		if err != nil {
			host, port = r.Host, "443"
		}
		if !p.permitted(session, host) {
			p.logDecision(session, host, port, "deny")
			http.Error(w, "egress denied", http.StatusForbidden)
			return
		}
		p.logDecision(session, host, port, "allow")

		upstream, err := net.DialTimeout("tcp", net.JoinHostPort(host, port), 10*time.Second)
		if err != nil {
			http.Error(w, "upstream dial failed", http.StatusBadGateway)
			return
		}
		defer upstream.Close()

		hj, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "no hijack", http.StatusInternalServerError)
			return
		}
		client, _, err := hj.Hijack()
		if err != nil {
			return
		}
		defer client.Close()
		client.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))

		done := make(chan struct{}, 2)
		go func() { io.Copy(upstream, client); done <- struct{}{} }()
		go func() { io.Copy(client, upstream); done <- struct{}{} }()
		<-done
	})
}

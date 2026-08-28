// Package egress is a per-VM CONNECT proxy: default-deny, per-session allowlist,
// audit log. It is the only path out for session containers on the internal
// network (spec §8). The session is identified by the Proxy-Authorization
// bearer token = its session id.
package egress

import (
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
	p.mu.Lock(); defer p.mu.Unlock()
	p.allow[session] = append([]string(nil), hosts...)
}

func (p *Proxy) permitted(session, host string) bool {
	p.mu.RLock(); defer p.mu.RUnlock()
	for _, pat := range p.allow[session] {
		if pat == host { return true }
		if strings.HasPrefix(pat, "*.") && strings.HasSuffix(host, pat[1:]) { return true }
	}
	return false
}

func (p *Proxy) logDecision(session, host, port, decision string) {
	if p.audit == nil { return }
	line, _ := json.Marshal(map[string]string{
		"session": session, "host": host, "port": port,
		"decision": decision, "ts": p.now().UTC().Format(time.RFC3339),
	})
	fmt.Fprintln(p.audit, string(line))
}

func (p *Proxy) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "only CONNECT supported", http.StatusMethodNotAllowed)
			return
		}
		session := strings.TrimPrefix(r.Header.Get("Proxy-Authorization"), "Bearer ")
		host, port, err := net.SplitHostPort(r.Host)
		if err != nil { host, port = r.Host, "443" }
		if !p.permitted(session, host) {
			p.logDecision(session, host, port, "deny")
			http.Error(w, "egress denied", http.StatusForbidden)
			return
		}
		p.logDecision(session, host, port, "allow")

		upstream, err := net.DialTimeout("tcp", net.JoinHostPort(host, port), 10*time.Second)
		if err != nil { http.Error(w, "upstream dial failed", http.StatusBadGateway); return }
		defer upstream.Close()

		hj, ok := w.(http.Hijacker)
		if !ok { http.Error(w, "no hijack", http.StatusInternalServerError); return }
		client, _, err := hj.Hijack()
		if err != nil { return }
		defer client.Close()
		client.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))

		done := make(chan struct{}, 2)
		go func() { io.Copy(upstream, client); done <- struct{}{} }()
		go func() { io.Copy(client, upstream); done <- struct{}{} }()
		<-done
	})
}

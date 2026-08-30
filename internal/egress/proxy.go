// Package egress is a per-VM CONNECT proxy: default-deny, per-session allowlist,
// audit log. It is the only path out for session containers on the internal
// network (spec §8). The session is identified by its Proxy-Authorization
// header, either a literal Bearer token (the session id) or — the form a
// plain HTTP_PROXY/HTTPS_PROXY env var actually produces, since curl/wget
// have no way to set a literal header from an env var — HTTP Basic auth
// decoded from the proxy URL's userinfo (http://<session-id>:@host:port),
// added for egress R4's env-var proxy flow (Task 13; see
// sessionFromProxyAuth). A CONNECT that carries no usable identity is answered
// with a 407 challenge rather than a refusal, because a client that waits to be
// asked (git does; curl does not) would otherwise never send the credential it
// already holds — see challengeRealm.
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
// Basic payload — returns "", meaning "this request asserted no identity I can
// use". The handler answers that with a 407 challenge, never with a tunnel: the
// empty session is not a session, it matches no allow entry, and the client
// gets asked to authenticate instead of being trusted or silently refused.
// Lumping a malformed header in with a missing one is deliberate — from the
// proxy's side they are the same fact, and a client whose credential did not
// survive the wire is exactly the one that should be asked to send it again.
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

// challengeRealm names this proxy in the `Proxy-Authenticate: Basic realm=...`
// header that answers an unidentified CONNECT. It is cosmetic to the protocol
// — clients key their stored credential off the proxy's host:port, not the
// realm — but it is what a human sees in a prompt or a trace, so it says which
// proxy is asking.
//
// THE CHALLENGE IS THE WHOLE POINT, and its absence was a production blocker
// (found by Plan 5's first live GitHub rehearsal). HTTP proxy auth is
// challenge-RESPONSE: a client MAY send credentials preemptively, but it is
// only obliged to send them after a 407 that names a scheme. Which of the two
// a tool does is its own choice, and the two tools that matter here disagree:
//
//   - curl, run directly, defaults CURLOPT_PROXYAUTH to Basic and therefore
//     sends `Proxy-Authorization: Basic ...` from the proxy URL's userinfo on
//     the very first CONNECT. Every egress test up to Plan 5 was a curl test,
//     so every one of them passed.
//   - git sets CURLOPT_PROXYAUTH to CURLAUTH_ANY, which cannot pick a scheme
//     without being told one. Its first CONNECT deliberately carries NO
//     credentials, and it waits for the 407 to learn what to send.
//
// So a proxy that answers an unidentified CONNECT with 403 is, to git, a proxy
// that has simply refused: it never retries, and the user sees
// "fatal: unable to access '...': CONNECT tunnel failed, response 403" with
// its session id — sitting right there in the proxy URL — never once offered.
// On a fleet where egressd enforces, that is a session with a github connector
// that cannot clone at all.
//
// The 407 is NOT a relaxation of default-deny. It grants nothing: no tunnel is
// opened, no bytes reach any upstream, and the retry that follows goes through
// exactly the same allowlist check as any other identified request. The two
// answers now say two different things, which is also what the audit log
// needed — "challenge" is the proxy asking who you are, "deny" is the proxy
// telling an identified session that this host is not on its list.
const challengeRealm = "rainier"

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
		// No usable identity yet: CHALLENGE, don't refuse. See challengeRealm
		// for why this is a 407 and not the 403 it used to be.
		if session == "" {
			p.logDecision(session, host, port, "challenge")
			w.Header().Set("Proxy-Authenticate", `Basic realm="`+challengeRealm+`"`)
			http.Error(w, "proxy authentication required", http.StatusProxyAuthRequired)
			return
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

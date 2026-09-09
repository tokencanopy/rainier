// Package egress is a per-VM CONNECT proxy: default-deny, per-session allowlist,
// audit log. It is the only path out for session containers on the internal
// network (spec §8). The session is identified by its Proxy-Authorization
// header, either a literal Bearer token (the session id) or — the form a
// plain HTTP_PROXY/HTTPS_PROXY env var actually produces, since curl/wget
// have no way to set a literal header from an env var — HTTP Basic auth
// decoded from the proxy URL's userinfo (http://<session-id>:<ignored>@host:port),
// added for egress R4's env-var proxy flow (Task 13; see
// sessionFromProxyAuth). A CONNECT that carries no usable identity is answered
// with a 407 challenge rather than a refusal, because a client that waits to be
// asked (git does; curl does not) would otherwise never send the credential it
// already holds — see challengeRealm.
package egress

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
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

	// allowPrivate lifts the private-destination guard. See
	// AllowPrivateDestinations.
	allowPrivate bool
	// resolve is the name lookup the guard uses, overridable so a test can
	// state what a name resolves to instead of needing a DNS server that
	// answers with a private address.
	resolve func(ctx context.Context, host string) ([]netip.Addr, error)
}

// Option configures a Proxy at construction. There is exactly one, and it
// exists because the guard it lifts is otherwise unconditional.
type Option func(*Proxy)

// AllowPrivateDestinations lets this proxy tunnel to loopback, RFC1918,
// link-local and the other non-public ranges vettedAddrs refuses.
//
// It is for a proxy whose origins are FIXTURES — this package's own tests
// serve a git repository on localhost and allowlist "localhost" — and for a
// local fleet where the thing on the other side is a container on the same
// machine. It is not a production setting: egressd does not offer a flag for
// it, because on a runner the private addresses on the other side of that
// name are the metadata service, the other tenants' sandboxes, and the
// runner's own control plane.
func AllowPrivateDestinations() Option { return func(p *Proxy) { p.allowPrivate = true } }

func New(audit io.Writer, opts ...Option) *Proxy {
	p := &Proxy{allow: map[string][]string{}, audit: audit, now: time.Now, resolve: lookupAddrs}
	for _, opt := range opts {
		opt(p)
	}
	return p
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

// errPrivateDestination reports a host that resolved only to addresses this
// proxy will not tunnel to.
var errPrivateDestination = errors.New("destination is not a public address")

// vettedAddrs resolves host and returns the public addresses it answers with,
// or errPrivateDestination when it answers with none.
//
// WHY THE PROXY RESOLVES AT ALL, when net.Dial would have done it a line
// later: the allowlist is a set of NAMES, matched against the string a client
// wrote into its CONNECT request line, and the mapping from that string to an
// address belongs to a nameserver nobody here controls. Two things follow.
//
// The first is DNS rebinding. Nothing stops an answer for an allowlisted name
// from being 169.254.169.254 — the cloud metadata endpoint that hands out the
// runner's own instance credentials — or a 10.0.0.0/8 address on the runner's
// network, or 127.0.0.1, which from the proxy's side is the runner itself. The
// name matched, so the allowlist said yes; without this the tunnel opens. The
// session container's own network cannot reach any of those (it is
// `internal: true` and has no route at all), which is exactly why the proxy is
// the interesting place to attack: it is the one process that has both a route
// to the private network and an instruction to dial where a session points it.
//
// The second is that resolving here and DIALING WHAT WAS RESOLVED closes the
// window between the two. A guard that resolved, approved, and then handed the
// NAME to net.Dial would be checking one answer and connecting to another; the
// second lookup can differ from the first, and a short TTL is all it takes. So
// the vetted addresses, not the name, are what dialFirst connects to.
//
// This is defence in depth and not the allowlist's replacement: the baseline
// (controlapp) is what decides a name may be reached at all, and every host on
// it is a public package or source host. The guard is what keeps that decision
// from being silently re-answered by a DNS reply.
func (p *Proxy) vettedAddrs(ctx context.Context, host string) ([]netip.Addr, error) {
	if p.allowPrivate {
		// The caller has said the private side is where its origins live, so
		// there is nothing to resolve here: dial the name as before.
		return nil, nil
	}
	// A literal address needs no lookup and gets the same verdict.
	if addr, err := netip.ParseAddr(host); err == nil {
		if isPrivateAddr(addr) {
			return nil, errPrivateDestination
		}
		return []netip.Addr{addr}, nil
	}
	addrs, err := p.resolve(ctx, host)
	if err != nil {
		return nil, err
	}
	public := make([]netip.Addr, 0, len(addrs))
	for _, addr := range addrs {
		if !isPrivateAddr(addr) {
			public = append(public, addr)
		}
	}
	if len(public) == 0 {
		return nil, errPrivateDestination
	}
	return public, nil
}

// dialTimeout bounds the WHOLE outbound attempt for one CONNECT — every
// address tried, not each one — and resolveTimeout bounds the lookup in front
// of it. Both are explicit because the guard moved work that net.DialTimeout
// used to bound on its own: a name with four A records would otherwise be four
// ten-second attempts, and a nameserver that never answers would hold the
// handler open for as long as the client was willing to wait.
const (
	dialTimeout    = 10 * time.Second
	resolveTimeout = 5 * time.Second
)

func lookupAddrs(ctx context.Context, host string) ([]netip.Addr, error) {
	ctx, cancel := context.WithTimeout(ctx, resolveTimeout)
	defer cancel()
	return net.DefaultResolver.LookupNetIP(ctx, "ip", host)
}

// isPrivateAddr reports whether addr is somewhere a session has no business
// reaching THROUGH THIS PROXY. Everything not routable on the public internet
// is refused, rather than an enumeration of the endpoints known to be
// sensitive: 169.254.169.254 is the metadata address on three clouds and
// 100.100.100.200 is a fourth's, and a list of the ones somebody remembered is
// a list that is wrong the next time a provider picks an address.
//
// The 4-in-6 form is unmapped first, because ::ffff:169.254.169.254 is the
// same destination written differently and the v6 predicates do not see it.
func isPrivateAddr(addr netip.Addr) bool {
	addr = addr.Unmap()
	switch {
	case !addr.IsValid(), addr.IsUnspecified(), addr.IsLoopback(),
		addr.IsPrivate(),          // RFC1918 and the v6 unique-local fc00::/7
		addr.IsLinkLocalUnicast(), // 169.254.0.0/16 — cloud metadata — and fe80::/10
		addr.IsLinkLocalMulticast(),
		addr.IsInterfaceLocalMulticast(),
		addr.IsMulticast():
		return true
	}
	// The IPv4 ranges Go has no predicate for and a session still has no
	// reason to reach: carrier-grade NAT (where more than one cloud puts an
	// internal service), IETF protocol assignments, benchmarking, and the
	// reserved 240/4.
	for _, prefix := range reservedV4 {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}

var reservedV4 = []netip.Prefix{
	netip.MustParsePrefix("100.64.0.0/10"), // RFC6598 carrier-grade NAT
	netip.MustParsePrefix("192.0.0.0/24"),  // RFC6890 IETF protocol assignments
	netip.MustParsePrefix("198.18.0.0/15"), // RFC2544 benchmarking
	netip.MustParsePrefix("240.0.0.0/4"),   // RFC1112 reserved
}

// dialFirst connects to the first of addrs that answers, or to host by name
// when addrs is nil — which is what AllowPrivateDestinations leaves behind.
// Trying each in turn matters for a real registry: a name with an A and a AAAA
// record on a runner with no IPv6 route would otherwise fail half the time
// depending on resolver order.
//
// The deadline is shared across the whole loop, not per address, so a name
// with several dead records costs one dialTimeout rather than one each.
func dialFirst(ctx context.Context, host string, addrs []netip.Addr, port string) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	var d net.Dialer
	if len(addrs) == 0 {
		return d.DialContext(ctx, "tcp", net.JoinHostPort(host, port))
	}
	var err error
	for _, addr := range addrs {
		var conn net.Conn
		if conn, err = d.DialContext(ctx, "tcp", net.JoinHostPort(addr.String(), port)); err == nil {
			return conn, nil
		}
		if ctx.Err() != nil {
			break
		}
	}
	return nil, err
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
		// An allowlisted NAME is not yet an allowlisted DESTINATION. See
		// vettedAddrs: the allowlist is matched on the string the client
		// wrote, and what that string resolves to is the answer of a
		// nameserver this proxy does not control.
		addrs, err := p.vettedAddrs(r.Context(), host)
		if errors.Is(err, errPrivateDestination) {
			p.logDecision(session, host, port, "deny_private")
			http.Error(w, "egress denied: host resolves to a non-public address", http.StatusForbidden)
			return
		}
		// Anything else the lookup could not answer is NOT a policy decision:
		// the allowlist said yes and a nameserver did not answer, which is the
		// same "allow, then the dial failed" this proxy has always logged and
		// answered 502 to. Keeping it that way keeps the audit log's three
		// words meaning exactly what they meant — a fourth would appear on
		// every transient DNS failure and read like a refusal.
		p.logDecision(session, host, port, "allow")
		if err != nil {
			http.Error(w, "upstream dial failed", http.StatusBadGateway)
			return
		}

		upstream, err := dialFirst(r.Context(), host, addrs, port)
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

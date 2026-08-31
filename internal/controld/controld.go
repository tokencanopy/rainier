// internal/controld/controld.go
package controld

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"rainier/internal/xfer"
)

const (
	// defaultOpTimeout bounds one dispatch round-trip to a runner. Docker
	// pulls are the slow case; a minute is generous without letting a client
	// request hang indefinitely on a wedged runner.
	defaultOpTimeout = 60 * time.Second
	// defaultGitHubAPIBase is the real GitHub API; tests point Config at a
	// fake instead (Task 9).
	defaultGitHubAPIBase = "https://api.github.com"
	// defaultAttachWait bounds how long an attach holds its request open
	// waiting for the session to reach `running` — 10s, mirroring runnerd's
	// own hub wait (design §5).
	defaultAttachWait = 10 * time.Second
	// defaultAttachPairTTL bounds how long a parked client socket waits for
	// its runner to dial back before controld closes it (design §5).
	defaultAttachPairTTL = 15 * time.Second
)

// Config is controld's startup configuration. RunnerToken, ExternalURL, and
// SecretsKey are required; the rest default.
type Config struct {
	// RunnerToken is the fleet-wide bearer every runnerd presents on
	// /v0/runners/connect (and, later, on the attach-back dial).
	RunnerToken string
	// SecretsKey is the AES-256 key team secrets are sealed under at rest
	// (seal.go), parsed from RAINIER_SECRETS_KEY by ParseSecretsKey. It is
	// required and has no default: a zero key means "unconfigured", and New
	// refuses to start rather than seal every team secret under a key of
	// zeros. Losing it loses secret values and nothing else.
	SecretsKey [32]byte
	// Admins and Members are GitHub logins; a login in neither list cannot
	// log in at all (fail closed — an empty allowlist admits nobody).
	Admins  []string
	Members []string
	// GitHubAPIBase is the base URL of GitHub's API, overridden in tests.
	GitHubAPIBase string
	// ExternalURL is the http(s) base URL that both clients and runners
	// reach this replica at. The attach plane derives the runner's
	// dial-back ws URL from it, so it must be absolute and routable from
	// the fleet — not "localhost" on a multi-host deployment.
	ExternalURL string
	// OpTimeout is the budget for one dispatch round-trip to a runner.
	OpTimeout time.Duration
	// AttachWait bounds how long WS /v0/sessions/{id}/attach waits for the
	// session to reach `running` before answering 503 session_not_ready.
	// Zero means defaultAttachWait; tests shorten it.
	AttachWait time.Duration
	// AttachPairTTL bounds how long a parked client socket waits for its
	// runner's dial-back before controld closes it. Zero means
	// defaultAttachPairTTL; tests shorten it.
	AttachPairTTL time.Duration
}

// Server is controld: the HTTP/WebSocket surface, the runner plane, and (as
// later tasks land) the scheduler and attach plane. Its zero value is not
// usable — construct it with New.
type Server struct {
	st  Store
	cfg Config

	// mu guards runners and runnerLocks. It is held only for map reads and
	// writes, never across a store call or a socket write, so a slow runner
	// can't stall registration for the rest of the fleet.
	mu      sync.Mutex
	runners map[string]*runnerConn
	// runnerLocks serializes the store writes that describe one runner
	// (connected flag and capacity) — see nameLock. Keyed by runner name,
	// never held while mu is.
	runnerLocks map[string]*sync.Mutex

	// attaches holds the attach pairings this replica is waiting on, keyed
	// by attach_id — client sockets parked between the dial_attach sent to
	// their runner and the dial-back that claims them (see attach.go). It
	// has its own lock: pairing is per-socket and must never contend with
	// the fleet-wide runner map.
	attaches *attachTable

	// xferMax is the most this replica will relay in ONE file transfer, in
	// either direction — xfer.MaxBytes in production, lowered by tests. It is
	// a field rather than the constant used inline because the pull path is
	// where a sandbox's own bound stops being enough: a compromised one that
	// never says "done" is answering an endless stream, and something on this
	// side has to be the thing that stops reading it.
	xferMax int64

	// schedWake carries capacity news to the scheduler loop (Task 8). It is
	// buffered by one and written non-blockingly: the loop only needs to
	// know that *something* changed, so a pending wake absorbs any number
	// of further ones.
	schedWake chan struct{}
}

// New validates cfg, applies defaults, and returns a Server over st.
func New(st Store, cfg Config) (*Server, error) {
	if st == nil {
		return nil, errors.New("controld: store is required")
	}
	if cfg.RunnerToken == "" {
		return nil, errors.New("controld: RunnerToken is required")
	}
	if cfg.ExternalURL == "" {
		return nil, errors.New("controld: ExternalURL is required")
	}
	if cfg.SecretsKey == ([32]byte{}) {
		// Fail closed, like the runner token: a controld that came up
		// without a key would either refuse every secret at runtime (a
		// puzzling half-broken fleet) or, worse, seal them all under zeros.
		return nil, errors.New("controld: SecretsKey is required (set RAINIER_SECRETS_KEY to 64 hex characters; generate one with: openssl rand -hex 32)")
	}
	u, err := url.Parse(cfg.ExternalURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("controld: ExternalURL must be an absolute http(s) URL, got %q", cfg.ExternalURL)
	}
	cfg.ExternalURL = strings.TrimRight(cfg.ExternalURL, "/")
	if cfg.OpTimeout <= 0 {
		cfg.OpTimeout = defaultOpTimeout
	}
	if cfg.AttachWait <= 0 {
		cfg.AttachWait = defaultAttachWait
	}
	if cfg.AttachPairTTL <= 0 {
		cfg.AttachPairTTL = defaultAttachPairTTL
	}
	if cfg.GitHubAPIBase == "" {
		cfg.GitHubAPIBase = defaultGitHubAPIBase
	}
	return &Server{
		st:          st,
		cfg:         cfg,
		runners:     map[string]*runnerConn{},
		runnerLocks: map[string]*sync.Mutex{},
		attaches:    newAttachTable(),
		xferMax:     xfer.MaxBytes,
		schedWake:   make(chan struct{}, 1),
	}, nil
}

// Handler returns controld's full HTTP surface: the runner control endpoint
// and attach dial-back, the auth exchange, the sessions/runners client API,
// and the client attach plane, all wrapped in the shared middleware chain
// (request id, nosniff, no-store on GET — see withMiddleware). Paths no route
// claims 404.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v0/runners/connect", s.handleRunnerConnect)
	mux.HandleFunc("GET /v0/runners/attach-back", s.handleAttachBack)
	mux.HandleFunc("POST /v0/auth/github", s.handleGitHubAuth)
	mux.HandleFunc("GET /v0/me", s.requireUser(s.handleMe))

	mux.HandleFunc("POST /v0/sessions", s.requireUser(s.handleCreateSession))
	mux.HandleFunc("GET /v0/sessions", s.requireUser(s.handleListSessions))
	mux.HandleFunc("GET /v0/sessions/{id}", s.requireUser(s.handleGetSession))
	mux.HandleFunc("DELETE /v0/sessions/{id}", s.requireUser(s.handleDeleteSession))
	mux.HandleFunc("POST /v0/sessions/{id}/suspend", s.requireUser(s.handleSuspendSession))
	mux.HandleFunc("POST /v0/sessions/{id}/resume", s.requireUser(s.handleResumeSession))
	mux.HandleFunc("POST /v0/sessions/{id}/snapshot", s.requireUser(s.handleSnapshotSession))
	mux.HandleFunc("GET /v0/sessions/{id}/attach", s.requireUser(s.handleClientAttach))

	// Workspace inspection — the session's working tree, read and written
	// from outside. The diff is team-visible like the other session reads
	// (design §4.6): `--stat` is metadata, file paths and churn counts. The
	// two file routes are owner-or-admin like attach, because they carry the
	// tree itself — bytes out, and writes in (§4.4; api.go, sessionForRPC).
	mux.HandleFunc("GET /v0/sessions/{id}/diff", s.requireUser(s.handleSessionDiff))
	mux.HandleFunc("POST /v0/sessions/{id}/files", s.requireUser(s.handlePushFiles))
	mux.HandleFunc("GET /v0/sessions/{id}/files", s.requireUser(s.handlePullFiles))

	mux.HandleFunc("GET /v0/runners", s.requireUser(s.handleListRunners))

	// Secrets: writes are admin-only, the listing is team-visible (names and
	// timestamps only — values are write-only at this API, design §4.5).
	mux.HandleFunc("PUT /v0/secrets/{name}", s.requireAdmin(s.handlePutSecret))
	mux.HandleFunc("GET /v0/secrets", s.requireUser(s.handleListSecrets))
	mux.HandleFunc("DELETE /v0/secrets/{name}", s.requireAdmin(s.handleDeleteSecret))

	// Credentials are per-user, not per-team: the listing is the caller's
	// own rows and nobody else's (not even an admin's view of them), and
	// there is no write route at all — a credential is stored by logging in.
	mux.HandleFunc("GET /v0/credentials", s.requireUser(s.handleListCredentials))

	// Environments belong to the whole team, so like secrets they have no
	// owner to fall back on: mutations are admin-only, reads team-visible
	// (design §4.5). The {id} routes take an id or a name.
	mux.HandleFunc("POST /v0/environments", s.requireAdmin(s.handleCreateEnvironment))
	mux.HandleFunc("GET /v0/environments", s.requireUser(s.handleListEnvironments))
	mux.HandleFunc("GET /v0/environments/{id}", s.requireUser(s.handleGetEnvironment))
	mux.HandleFunc("PATCH /v0/environments/{id}", s.requireAdmin(s.handleUpdateEnvironment))
	mux.HandleFunc("DELETE /v0/environments/{id}", s.requireAdmin(s.handleDeleteEnvironment))

	mux.HandleFunc("GET /healthz", s.handleHealthz)

	return withMiddleware(mux)
}

// Run hosts controld's background loops and blocks until ctx is done.
// schedulerLoop already returns on ctx.Done(), so Run needs nothing further
// of its own to block on.
func (s *Server) Run(ctx context.Context) {
	s.schedulerLoop(ctx)
}

// wakeScheduler tells the scheduler loop that capacity or the queue may have
// changed. It never blocks: a wake already pending means the loop hasn't run
// yet and will see this change too.
func (s *Server) wakeScheduler() {
	select {
	case s.schedWake <- struct{}{}:
	default:
	}
}

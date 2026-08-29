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
)

const (
	// defaultOpTimeout bounds one dispatch round-trip to a runner. Docker
	// pulls are the slow case; a minute is generous without letting a client
	// request hang indefinitely on a wedged runner.
	defaultOpTimeout = 60 * time.Second
	// defaultGitHubAPIBase is the real GitHub API; tests point Config at a
	// fake instead (Task 9).
	defaultGitHubAPIBase = "https://api.github.com"
)

// Config is controld's startup configuration. RunnerToken and ExternalURL
// are required; the rest default.
type Config struct {
	// RunnerToken is the fleet-wide bearer every runnerd presents on
	// /v1/runners/connect (and, later, on the attach-back dial).
	RunnerToken string
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
	u, err := url.Parse(cfg.ExternalURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("controld: ExternalURL must be an absolute http(s) URL, got %q", cfg.ExternalURL)
	}
	cfg.ExternalURL = strings.TrimRight(cfg.ExternalURL, "/")
	if cfg.OpTimeout <= 0 {
		cfg.OpTimeout = defaultOpTimeout
	}
	if cfg.GitHubAPIBase == "" {
		cfg.GitHubAPIBase = defaultGitHubAPIBase
	}
	return &Server{
		st:          st,
		cfg:         cfg,
		runners:     map[string]*runnerConn{},
		runnerLocks: map[string]*sync.Mutex{},
		schedWake:   make(chan struct{}, 1),
	}, nil
}

// Handler returns controld's full HTTP surface: the runner control endpoint
// today, plus the client API and attach plane as later tasks register them.
// Paths no task has claimed yet 404.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/runners/connect", s.handleRunnerConnect)
	return mux
}

// Run hosts controld's background loops and blocks until ctx is done. The
// scheduler loop lands here in Task 8; callers already start it as
// `go srv.Run(ctx)` so that arrival changes nothing at the call site.
func (s *Server) Run(ctx context.Context) {
	<-ctx.Done()
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

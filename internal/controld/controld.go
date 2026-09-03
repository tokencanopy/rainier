// Package controld is the self-hosted Rainier control plane: the HTTP and
// WebSocket surface, GitHub login, the secrets vault, and the runner plane,
// composed over the portable application services in
// github.com/tokencanopy/rainier/controlapp behind the frozen public
// github.com/tokencanopy/rainier/control contract.
//
// The split is deliberate and the tests hold it: session lifecycle, in-pool
// placement, runner reconciliation and event application, and attach and
// workspace orchestration live once, in controlapp, and this package reaches
// them only through the four services New composes. What this package owns is
// the host side of each port (adapt_*.go) — the pool resolver over the one
// installation pool, the GitHub-role rule as the authorizer and attachment
// policy, the vault and
// connectors as launch material, the runner plane
// (github.com/tokencanopy/rainier/runnerplane) as the transport, the
// dial-back pairing as the attach broker — while the three repository ports
// are the store's own, implemented natively over its workspace-keyed rows and
// read off it by compose(). Plus everything with no portable
// counterpart: request decoding and JSON rendering, GitHub login and tokens,
// secrets and credentials, the sandbox's upward credential-mint RPC, and the
// setup_done snapshot arm.
//
// The store is read directly, never for a decision, in a handful of places:
// an environment referenced by name, the missing-secret and
// missing-credential preflights at create, a pinned runner's free slots for
// a session's queue_reason, an environment's snapshot runner for its view,
// the count behind an in-use refusal, the session behind a runner's
// setup_done or credential_rejected, and the owner check and readiness wait
// that precede a terminal's WebSocket upgrade. The direct writes that remain
// are the runner heartbeat and disconnect flags, the environment snapshot
// after setup_done, credential status, secrets, users, and tokens —
// transport bookkeeping and host policy, never lifecycle.
package controld

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/tokencanopy/rainier/attachplane"
	"github.com/tokencanopy/rainier/control"
	"github.com/tokencanopy/rainier/controlapp"
	"github.com/tokencanopy/rainier/runnerplane"
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
	// ensureWorkspaceTimeout bounds New's one store write. It is startup, not
	// a request, so it is generous — but bounded, so a store that will never
	// answer produces an error a process manager can act on rather than a
	// controld wedged before it ever listened.
	ensureWorkspaceTimeout = 10 * time.Second
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
	// MaxTransferBytes is the most this replica relays in one file transfer,
	// either direction; zero means workspace.MaxBytes; tests lower it.
	MaxTransferBytes int64
}

// Server is controld: the HTTP/WebSocket surface, the runner plane, and (as
// later tasks land) the scheduler and attach plane. Its zero value is not
// usable — construct it with New.
type Server struct {
	st  Store
	cfg Config

	// plane is the runner plane: the runner WebSocket endpoint, the
	// connection registry behind it, the generation mint and its fences,
	// reconciliation, event translation, and the dispatch correlation. It is
	// composed over runnerHost (runners.go), which answers the handful of
	// questions the plane deliberately leaves to its host.
	plane *runnerplane.Plane

	// attach is the dial-back attach plane: the pairings this replica is
	// waiting on — client sockets parked between the dial_attach sent to
	// their runner and the dial-back that claims them — behind the broker
	// and the dial-back handler composed below. Its state is its own:
	// pairing is per-socket and must never contend with the fleet-wide
	// runner map.
	attach *attachplane.Plane

	// pushes holds the file uploads this replica is relaying, keyed by the
	// client's own transfer id — one io.Pipe and one PushWorkspace call per
	// transfer, spanning the many HTTP requests one upload arrives as (see
	// api.go). Like the pairing table it has its own lock and its own
	// lifetime, and its zero value is usable.
	pushes pushTable

	// The four application services controld is composed from, built by New
	// over the adapters in adapt_*.go. Handlers reach the store, the runner
	// plane, and the attach plane only through these (Tasks 4–6 of the
	// recomposition plan rewire them one surface at a time).
	sessions     *controlapp.SessionService
	environments *controlapp.EnvironmentService
	fleet        *controlapp.FleetService
	attachments  *controlapp.AttachmentService

	// transport is the runner plane behind the control.RunnerTransport port
	// (plane.Transport()) and broker the dial-back attach pairing behind
	// control.AttachmentBroker (adapt_attach.go, over the pairing table
	// above).
	transport control.RunnerTransport
	broker    control.AttachmentBroker
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
	// The installation workspace has to exist before anything reads or
	// writes a row keyed by it, and a store from any source — a fresh
	// memstore, a database that predates the scope columns, one restored from
	// a dump — may not carry it yet. Asserting it here fails closed: a
	// controld that came up over a store it cannot scope would answer every
	// request with "not found" and look like data loss.
	//
	// Its own context, because New takes none and a wedged store must not
	// hang startup indefinitely.
	ensureCtx, cancelEnsure := context.WithTimeout(context.Background(), ensureWorkspaceTimeout)
	defer cancelEnsure()
	if err := st.EnsureWorkspace(ensureCtx, installWorkspace); err != nil {
		return nil, fmt.Errorf("controld: provisioning the installation workspace: %w", err)
	}

	s := &Server{
		st:  st,
		cfg: cfg,
	}
	// The runner plane is built before the services because it IS the
	// transport they are composed over; its host reaches back through s for
	// the fleet service, which compose() sets a moment later and nothing
	// calls before. The attach plane likewise reaches the runner plane
	// through s.sendToRunner.
	s.plane = runnerplane.New(runnerHost{s}, runnerplane.Options{
		OpTimeout: cfg.OpTimeout,
		Logf:      func(format string, a ...any) { log.Printf("controld: "+format, a...) },
	})
	s.transport = s.plane.Transport()
	s.attach = attachplane.New(attachHost{s}, attachplane.Options{
		PairTTL: cfg.AttachPairTTL, Logf: log.Printf,
	})
	s.broker = s.attach.Broker()
	if err := s.compose(); err != nil {
		return nil, err
	}
	return s, nil
}

// fleetSafetyInterval is how often the fleet service re-drains every known
// pool even when no wake arrived — the same 10s safety tick today's scheduler
// loop runs on, so a wake that was coalesced away costs at most that long.
const fleetSafetyInterval = 10 * time.Second

// compose builds the four application services over one adapter set. The
// three repository ports are the store's own accessors — the sessions
// Sessions() creates are the sessions Fleet() places — and every other port
// is one of the adapters in adapt_*.go; nothing here is a second
// implementation of any behavior the services own. It is the composition
// root the program plan keeps reviewer-owned.
func (s *Server) compose() error {
	var (
		auth     = ownerOrAdmin{}
		sessions = s.st.Sessions()
		envs     = s.st.Environments()
		fleet    = s.st.Fleet()
		pools    = installationPools{st: s.st}
		clock    = systemClock{}
		ids      = idGenerator{}
		ckpts    = pinnedCheckpoints{st: s.st}
		// The store is both the host's atomicity and its event record: an
		// event lands in the same transaction as the mutation it describes,
		// which is what makes it a fact rather than a hope. Passing anything
		// else here would put the two writes in different units.
		events control.EventRecorder = s.st
		uow    control.UnitOfWork    = s.st
	)
	fleetSvc, err := controlapp.NewFleetService(controlapp.FleetOptions{
		Authorizer: auth, Sessions: sessions, Environments: envs, Fleet: fleet, Pools: pools,
		Transport: s.transport, Events: events, Clock: clock, IDs: ids,
		SafetyInterval: fleetSafetyInterval,
		LaunchMaterial: launchMaterial{st: s.st, key: s.cfg.SecretsKey},
		// The self-hosted bounds for a hook whose environment declares none,
		// exactly as the old scheduler applied them (api.go).
		DefaultSetupTimeoutSec: defaultSetupTimeoutSec,
		DefaultInitTimeoutSec:  defaultInitTimeoutSec,
		UnitOfWork:             uow,
		Checkpoints:            ckpts,
	})
	if err != nil {
		return fmt.Errorf("controld: composing the fleet service: %w", err)
	}
	sessionSvc, err := controlapp.NewSessionService(controlapp.SessionOptions{
		Authorizer: auth, Sessions: sessions, Environments: envs, Pools: pools,
		Events: events, Clock: clock, IDs: ids, Wake: fleetSvc.Wake,
		Fleet: fleet, Transport: s.transport, UnitOfWork: uow,
	})
	if err != nil {
		return fmt.Errorf("controld: composing the session service: %w", err)
	}
	envSvc, err := controlapp.NewEnvironmentService(controlapp.EnvironmentOptions{
		Authorizer: auth, Environments: envs, Events: events, Clock: clock, IDs: ids,
		UnitOfWork: uow,
	})
	if err != nil {
		return fmt.Errorf("controld: composing the environment service: %w", err)
	}
	attachSvc, err := controlapp.NewAttachmentService(controlapp.AttachmentOptions{
		Authorizer: auth, Policy: auth, Sessions: sessions, Transport: s.transport,
		Broker: s.broker, Events: events, Clock: clock, IDs: ids, UnitOfWork: uow,
		MaxTransferBytes: s.cfg.MaxTransferBytes,
	})
	if err != nil {
		return fmt.Errorf("controld: composing the attachment service: %w", err)
	}
	s.fleet, s.sessions, s.environments, s.attachments = fleetSvc, sessionSvc, envSvc, attachSvc
	return nil
}

// Handler returns controld's full HTTP surface: the runner control endpoint
// and attach dial-back, the auth exchange, the sessions/runners client API,
// and the client attach plane, all wrapped in the shared middleware chain
// (request id, nosniff, no-store on GET — see withMiddleware). Paths no route
// claims 404.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /v0/runners/connect", s.plane.Handler())
	mux.Handle("GET /v0/runners/attach-back", s.attach.BackHandler())
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

// Run hosts controld's background loops and blocks until ctx is done. The
// scheduler is the fleet service's own loop, which returns ctx.Err() when the
// context ends — a shutdown, not a failure, so it is deliberately discarded.
func (s *Server) Run(ctx context.Context) {
	_ = s.fleet.Run(ctx)
}

// wakeScheduler tells the fleet service that capacity or the queue may have
// changed in the installation pool. It never blocks: the service coalesces
// wakes and re-drains every known pool on its safety tick regardless.
func (s *Server) wakeScheduler() {
	s.fleet.Wake(installPool)
}

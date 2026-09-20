package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// The microVM boot: what a guest reads off its vsock connection before it is
// a session at all, and the one exchange that turns a capability into the
// environment secrets its create deliberately did not carry.
//
// See docs/design/2026-09-20-microvm-bootstrap-token-and-vsock.md §4. On the
// Docker path none of this runs: the container's environment block IS the
// configuration, sessiond reads it with bootEnvFromOS as it always has, and
// no secret ever has to be asked for.

const (
	// bootConfigWait bounds how long a guest waits for its configuration.
	// The host writes it as the first frame the instant the guest connects,
	// so this is a bound on a host that is not going to answer rather than
	// on anything that legitimately takes time.
	bootConfigWait = 30 * time.Second
	// secretsExchangeWait bounds the round trip to the control plane. It
	// sits under the bootstrap token's own 120-second lifetime, so a refusal
	// is an answer rather than an expiry.
	secretsExchangeWait = 30 * time.Second
	// bootDialAttempts and bootDialBackoff bound the retry on the very first
	// dial. Everything else in this process redials with backoff for the
	// life of the session, and the one dial that could not was the one whose
	// failure is fatal — a guest whose kernel brought /dev/vsock up a moment
	// after sessiond started, or a host whose accept loop was one scheduling
	// quantum behind, would have taken the session down for a condition that
	// resolves itself. The bound exists because a guest that genuinely has
	// no channel must fail rather than sit in a loop nobody can see.
	bootDialAttempts = 10
	bootDialBackoff  = 500 * time.Millisecond
	// secretsRequestID is the id this end assigns its boot-time exchange.
	//
	// The request is made before the RPC dispatcher is serving — there is no
	// session to serve for yet — so it is numbered by hand, and it is
	// numbered OUT OF the dispatcher's space rather than at the bottom of
	// it. The dispatcher counts from 1, so a boot exchange numbered 1 whose
	// answer arrived late would be matched to the first call the dispatcher
	// made afterwards (an agent credential fetch, typically) and answered
	// with a body that method cannot read.
	//
	// It stays clear of runnerd's own reserved space too: that is the high
	// BIT (1<<63), and a request carrying it is refused at the runner as a
	// sandbox using an id that is not its to use.
	secretsRequestID = 1 << 62
)

// bootstrapper is the guest side of the boot: it reads a configuration off
// each connection and, when that configuration brings a token it has not
// already spent, exchanges it for the session's secrets.
//
// It is stateful for one reason, and it is the reason a redial is not a
// resume: a bootstrap token is SINGLE-USE. A sessiond that re-exchanged on
// every connection would be refused on the first redial inside a live VM and
// would keep being refused forever. What distinguishes a resume from a
// redial is that a resume brings a NEW token — runnerd mints one before it
// boots the new VM — so "exchange when the token is one I have not spent" is
// exactly "at boot and again after every resume", with no second signal
// needed.
type bootstrapper struct {
	mu sync.Mutex
	// attempted is the last token this session sent a request for, whatever
	// the answer was.
	//
	// ATTEMPTED and not "spent", which is the difference between a session
	// that recovers and one that does not. A token is consumed by the
	// control plane BEFORE the secrets are resolved, so a refused or
	// timed-out exchange has spent it just as surely as a successful one;
	// retrying it can only ever be refused. A sessiond that keyed on success
	// would re-exchange a dead token on every redial, fail the preamble
	// every time, and take a session that was merely missing its secrets —
	// and had already said so, loudly, as a failed boot stage — off the air
	// entirely.
	attempted string
	// delivered are the names this session has been given values for, kept
	// so a cold suspend can forget them by name. NAMES, never values: what
	// was delivered lives in the process environment and nowhere else.
	delivered []string
}

// bootFailure is a boot that cannot proceed, rendered as the sentence its
// failing stage prints.
//
// It names a COUNT and never a name's value — and never the names either,
// beyond how many there were, because a refusal that listed them would put
// an environment's shape in a session's error text where a workspace's
// members can read it.
type bootFailure struct{ reason string }

func (f *bootFailure) Error() string { return f.reason }

// undeliveredSecrets is the sentence a boot fails with when the values a
// create promised did not arrive. why is the control plane's own refusal, or
// this end's description of what was missing; neither carries a value.
func undeliveredSecrets(n int, why string) *bootFailure {
	return &bootFailure{reason: fmt.Sprintf(
		"rainier: this session's environment declares %d secret(s) that could not be delivered: %s. "+
			"The agent has not been started, because an environment that promised a credential and does not have it "+
			"is not the environment this session was created from.", n, why)}
}

// readBootConfig waits for the first control frame on a fresh connection and
// requires it to be the boot configuration.
//
// Anything else on the way is dropped rather than refused: this conn is the
// session's whole channel, and a host that sent something before the
// configuration is a host this guest should keep listening to.
func readBootConfig(ctx context.Context, conn relay.Conn) (runner.BootConfig, error) {
	ctx, cancel := context.WithTimeout(ctx, bootConfigWait)
	defer cancel()
	for {
		raw, err := conn.Read(ctx)
		if err != nil {
			return runner.BootConfig{}, fmt.Errorf("waiting for the boot configuration: %w", err)
		}
		f, err := relay.Decode(raw)
		if err != nil || f.Type != relay.FrameControl {
			continue
		}
		var ev relay.ControlEvent
		if err := json.Unmarshal(f.Payload, &ev); err != nil || ev.Kind != relay.KindBootConfig {
			continue
		}
		var cfg runner.BootConfig
		if err := json.Unmarshal(ev.Payload, &cfg); err != nil {
			return runner.BootConfig{}, fmt.Errorf("the boot configuration could not be decoded: %w", err)
		}
		if cfg.Protocol != runner.SessionBootstrapProtocolVersion {
			return runner.BootConfig{}, fmt.Errorf(
				"this host speaks boot-configuration protocol %d and this session image speaks %d; the session image must be replaced",
				cfg.Protocol, runner.SessionBootstrapProtocolVersion)
		}
		return cfg, nil
	}
}

// exchange turns cfg's token into the session's secrets, or says why it
// could not.
//
// It returns (nil, nil) for the two cases that are not failures: a
// configuration declaring no secrets at all, and a token this session has
// already spent — which is every redial inside one live VM.
//
// The request is written straight onto the conn rather than through the RPC
// dispatcher because the dispatcher is not serving yet: the relay it rides on
// is started later, by dialLoop, once there is a session to serve.
func (b *bootstrapper) exchange(ctx context.Context, conn relay.Conn, cfg runner.BootConfig) (map[string]string, error) {
	if len(cfg.SecretNames) == 0 {
		// A clean boot. "No secrets declared" and "declared and never
		// arrived" are different facts, and this is the first.
		return nil, nil
	}
	b.mu.Lock()
	already := cfg.BootstrapToken != "" && cfg.BootstrapToken == b.attempted
	if !already {
		// Recorded BEFORE the request goes out, not after it succeeds: the
		// control plane spends the token when it receives it, so a request
		// that was sent has spent it whatever comes back.
		b.attempted = cfg.BootstrapToken
	}
	b.mu.Unlock()
	if already {
		return nil, nil
	}
	if cfg.BootstrapToken == "" {
		return nil, undeliveredSecrets(len(cfg.SecretNames),
			"this session's create carried no bootstrap token, which means a control plane that does not withhold them")
	}

	body, err := json.Marshal(struct {
		Protocol int    `json:"protocol"`
		Token    string `json:"token"`
	}{runner.SessionBootstrapProtocolVersion, cfg.BootstrapToken})
	if err != nil {
		return nil, undeliveredSecrets(len(cfg.SecretNames), "the request could not be encoded")
	}
	payload, err := json.Marshal(relay.ControlEvent{
		Kind: "req:" + runner.MethodFetchSessionSecrets, ID: secretsRequestID, Payload: body})
	if err != nil {
		return nil, undeliveredSecrets(len(cfg.SecretNames), "the request could not be encoded")
	}
	frame, err := relay.Encode(relay.Frame{Type: relay.FrameControl, Payload: payload})
	if err != nil {
		return nil, undeliveredSecrets(len(cfg.SecretNames), "the request could not be encoded")
	}

	ctx, cancel := context.WithTimeout(ctx, secretsExchangeWait)
	defer cancel()
	if err := conn.Write(ctx, frame); err != nil {
		return nil, undeliveredSecrets(len(cfg.SecretNames), "the request could not be sent to the runner")
	}

	for {
		raw, err := conn.Read(ctx)
		if err != nil {
			return nil, undeliveredSecrets(len(cfg.SecretNames), "the runner did not answer")
		}
		f, derr := relay.Decode(raw)
		if derr != nil || f.Type != relay.FrameControl {
			continue
		}
		var ev relay.ControlEvent
		if json.Unmarshal(f.Payload, &ev) != nil || ev.Kind != "resp" || ev.ID != secretsRequestID {
			continue
		}
		if !ev.OK {
			// The control plane's own sentence, relayed: it names which of
			// the four conditions holds — spent, expired, fenced, unknown —
			// and never a value.
			return nil, undeliveredSecrets(len(cfg.SecretNames), rpcErrorText(ev.Payload))
		}
		var answer struct {
			Env map[string]string `json:"env"`
		}
		if json.Unmarshal(ev.Payload, &answer) != nil {
			// Logged without the error, and reported without it: a json
			// message quotes what it choked on, and that is the secret.
			return nil, undeliveredSecrets(len(cfg.SecretNames), "the answer could not be decoded")
		}
		return answer.Env, nil
	}
}

// remember records which names this session has been given values for, so a
// cold suspend can unset exactly those and nothing else.
func (b *bootstrapper) remember(names []string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, n := range names {
		if !slices.Contains(b.delivered, n) {
			b.delivered = append(b.delivered, n)
		}
	}
}

// forget unsets every delivered secret from this process's environment and
// reports how many there were.
//
// It is what a cold suspend asks for. The VM is about to be terminated with
// no memory image written anywhere, so this is belt and braces rather than
// the guarantee — but it is the half this process can make, and it is free:
// a sessiond that is frozen instead of terminated (a host that sent `cold`
// and then changed its mind) has no secret left in its environment for
// whatever reads it next.
//
// It cannot reach a child that already inherited them. That is stated rather
// than papered over: the agent has the values, by design, and the thing that
// takes them away is the VM ending.
func (b *bootstrapper) forget() int {
	b.mu.Lock()
	names := b.delivered
	b.delivered = nil
	// `attempted` is deliberately NOT cleared. The token this session was
	// handed is spent whether or not this process still holds its values,
	// and a resume brings a new one — so forgetting which token was already
	// presented would only make the next connection re-present a dead one.
	b.mu.Unlock()
	for _, n := range names {
		_ = os.Unsetenv(n)
	}
	return len(names)
}

// applyBootConfig puts a boot configuration into this process's environment,
// in the shape the boot chain already reads (bootEnvFromOS).
//
// Translating into the environment block rather than threading the struct
// through is deliberate: everything downstream — prepareBoot, the agent
// sync, the exec runner's inherited environment, the child the agent becomes
// — already reads exactly these variables on the Docker path, and a second
// way of saying the same thing is a second way for the two paths to diverge.
// The scripts arrive as plain strings on the wire and are base64-encoded
// here for the same reason they are on the other path: that is what the
// reader expects.
//
// RAINIER_DIAL is deliberately NOT set. There is nothing to dial.
func applyBootConfig(cfg runner.BootConfig, secrets map[string]string) error {
	set := func(k, v string) error {
		if v == "" {
			return nil
		}
		return os.Setenv(k, v)
	}
	b64 := func(s string) string {
		if s == "" {
			return ""
		}
		return base64.StdEncoding.EncodeToString([]byte(s))
	}

	// The secrets go FIRST, so that the configuration below wins over them.
	// That is the same precedence the control plane already applies on the
	// Docker path, where the agent-home keys are launch invariants a
	// workspace's own configuration may not replace; a delivered value
	// spelled like one of them must not redirect credential custody here
	// either. (The plane also drops such a name from SecretNames, so this is
	// the second of two fences rather than the only one.)
	for k, v := range secrets {
		if err := os.Setenv(k, v); err != nil {
			return fmt.Errorf("applying this session's environment: %w", err)
		}
	}

	if err := set("RAINIER_SESSION", cfg.SessionID); err != nil {
		return err
	}
	if cfg.ProxyURL != "" {
		for _, k := range []string{"HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy"} {
			if err := set(k, cfg.ProxyURL); err != nil {
				return err
			}
		}
		for _, k := range []string{"NO_PROXY", "no_proxy"} {
			if err := set(k, cfg.NoProxy); err != nil {
				return err
			}
		}
	}
	if cfg.Setup != "" {
		if err := set("RAINIER_SETUP_B64", b64(cfg.Setup)); err != nil {
			return err
		}
		if err := set("RAINIER_SETUP_TIMEOUT", strconv.Itoa(cfg.SetupTimeoutSec)); err != nil {
			return err
		}
	}
	if cfg.Init != "" {
		if err := set("RAINIER_INIT_B64", b64(cfg.Init)); err != nil {
			return err
		}
		if err := set("RAINIER_INIT_TIMEOUT", strconv.Itoa(cfg.InitTimeoutSec)); err != nil {
			return err
		}
	}
	if len(cfg.Repos) > 0 {
		blob, err := json.Marshal(cfg.Repos)
		if err != nil {
			return fmt.Errorf("encoding this session's repository list: %w", err)
		}
		if err := set("RAINIER_REPOS_B64", base64.StdEncoding.EncodeToString(blob)); err != nil {
			return err
		}
	}
	if err := set("RAINIER_GIT_AUTHOR_NAME", cfg.GitAuthorName); err != nil {
		return err
	}
	if err := set("RAINIER_GIT_AUTHOR_EMAIL", cfg.GitAuthorEmail); err != nil {
		return err
	}
	for k, v := range cfg.Env {
		if err := os.Setenv(k, v); err != nil {
			return fmt.Errorf("applying this session's configuration: %w", err)
		}
	}
	return nil
}

// bootOverVsock is the whole microVM boot preamble, run once before this
// process has a session at all: dial, read the configuration, exchange the
// token, apply both.
//
// It returns the connection it bootstrapped on, which dialLoop then serves
// its relay over — the SAME conn, because the boot configuration and the
// terminal stream and the session RPC all ride one channel by design, and
// because a second dial would be a second guest for a socket that serves one.
//
// A failed exchange is returned as a *bootFailure and NOT as a dial error:
// the connection is good, the session is real, and what has to happen is
// that the boot chain fails loudly enough for a person to see why. The
// caller turns it into a failing stage.
func bootOverVsock(ctx context.Context, dial dialSession, b *bootstrapper) (relay.Conn, runner.BootConfig, *bootFailure, error) {
	conn, err := dialBoot(ctx, dial)
	if err != nil {
		return nil, runner.BootConfig{}, nil, err
	}
	cfg, err := readBootConfig(ctx, conn)
	if err != nil {
		_ = conn.Close()
		return nil, runner.BootConfig{}, nil, err
	}
	secrets, xerr := b.exchange(ctx, conn, cfg)
	var failure *bootFailure
	if xerr != nil {
		bf, ok := xerr.(*bootFailure)
		if !ok {
			_ = conn.Close()
			return nil, cfg, nil, xerr
		}
		failure = bf
		// The conn is NOT handed back on a failed exchange. It may be
		// perfectly good (a refusal) or it may not (a timeout, which ends
		// the read by poisoning the conn's deadline), and the boot has no
		// way to tell — so it is closed and dialLoop dials a fresh one. The
		// cost is one extra connection on a session that is about to fail
		// its boot chain anyway; the alternative is serving a conn that may
		// already be dead and discovering it one frame later.
		//
		// The re-dial is safe precisely because the token is recorded as
		// ATTEMPTED: the preamble on the new connection reads the
		// configuration again and asks for nothing.
		_ = conn.Close()
		conn = nil
	}
	if err := applyBootConfig(cfg, secrets); err != nil {
		if conn != nil {
			_ = conn.Close()
		}
		return nil, cfg, nil, err
	}
	b.remember(namesOf(secrets))
	log.Printf("sessiond booted as %s over vsock with %d declared secret(s), %d delivered",
		cfg.SessionID, len(cfg.SecretNames), len(secrets))
	return conn, cfg, failure, nil
}

// reBootstrap is the preamble on every LATER connection: read the
// configuration again, and exchange again when it brings a token this
// session has not spent — which is what a cold resume brings and a redial
// does not.
//
// The two failures it can meet are answered differently, and the difference
// is the difference between a session that recovers and one that vanishes.
//
// A configuration that never arrived means this conn is not usable, so the
// error is returned and dialLoop backs off and dials another.
//
// A refused EXCHANGE does not: the session is already running, its agent
// already has whatever it was given, and it has already reported a failed
// boot chain if it was given nothing. Tearing the conn down there would take
// a session that was merely missing its secrets off the air completely — no
// terminal, no attach, no events — for as long as the refusal persisted,
// which for a spent token is forever. So it is logged and the connection is
// served.
func reBootstrap(ctx context.Context, conn relay.Conn, b *bootstrapper) error {
	cfg, err := readBootConfig(ctx, conn)
	if err != nil {
		return err
	}
	secrets, xerr := b.exchange(ctx, conn, cfg)
	if xerr != nil {
		log.Printf("this session's secrets were not re-delivered on a new connection (%v); "+
			"serving it anyway — the session is running and has already reported what it was given", xerr)
		return nil
	}
	if len(secrets) == 0 {
		return nil
	}
	if err := applyBootConfig(cfg, secrets); err != nil {
		return err
	}
	b.remember(namesOf(secrets))
	log.Printf("sessiond re-applied %d environment secret(s) after a resume", len(secrets))
	return nil
}

// dialBoot is the first dial, with the retry every later one already has.
//
// A failure here is fatal to the session, which is why it is the one dial
// that must not give up on the first refusal: a guest whose /dev/vsock came
// up a moment after this process did, or a host whose accept loop was a
// scheduling quantum behind, is a condition that resolves itself in
// milliseconds. A guest that genuinely has no channel still fails, and still
// fails quickly.
func dialBoot(ctx context.Context, dial dialSession) (relay.Conn, error) {
	var err error
	for attempt := 1; ; attempt++ {
		var conn relay.Conn
		conn, err = dial(ctx)
		if err == nil {
			return conn, nil
		}
		if attempt >= bootDialAttempts || ctx.Err() != nil {
			return nil, fmt.Errorf("after %d attempt(s): %w", attempt, err)
		}
		log.Printf("the host control channel is not answering yet (%v); retrying", err)
		select {
		case <-time.After(bootDialBackoff):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// namesOf returns a map's keys. Names, which are not values — the same
// distinction the launch-material resolver and the create both make.
func namesOf(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

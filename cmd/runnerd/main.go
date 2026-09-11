// cmd/runnerd/main.go
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/tokencanopy/rainier/internal/driver"
	"github.com/tokencanopy/rainier/internal/runnerd"
)

func main() {
	listen := flag.String("listen", "0.0.0.0:8080", "control + relay listen address")
	dialBase := flag.String("dial-base", "ws://runnerd:8080", "URL sessiond containers dial to register")
	image := flag.String("image", "rainier-session:latest", "default session image")
	network := flag.String("network", "rainier-internal", "internal docker network for sessions")
	egressAdmin := flag.String("egress-admin", "http://egressd:3129", "egressd admin URL")
	slots := flag.Int("slots", 16, "how many sandboxes this runner may hold at once, running or warm-suspended; a cold-stopped session keeps its files but no slot")
	idleStop := flag.Duration("idle-stop", 30*time.Minute,
		"stop a session whose agent process has exited and that has had no attachment for this `duration`, exactly as rainier stop does: the container stops, the files are kept, the slot comes back, and an attach resumes it. A session whose agent is still running is never stopped, however long nobody has watched it. 0 disables it")
	controld := flag.String("controld", "", "controld URL to dial (ws://host:port); enables agent (dial) mode when set")
	runnerToken := flag.String("runner-token", envDefault("RAINIER_RUNNER_TOKEN", ""),
		"bearer token for the controld dial (required when --controld is set; or set RAINIER_RUNNER_TOKEN, which keeps it out of the process list)")
	hostname, _ := os.Hostname()
	runnerName := flag.String("runner-name", hostname, "name this runner announces to controld")
	proxyURL := flag.String("proxy-url", "", "egress proxy URL injected into every session (forwarded to controld dial mode)")
	seccompProfile := flag.String("session-seccomp-profile", "", "host path to an optional seccomp profile for session containers")
	appArmorProfile := flag.String("session-apparmor-profile", "", "name of an optional loaded AppArmor profile for session containers")
	var capabilities capabilityFlag
	flag.Var(&capabilities, "capability",
		"a portable capability this runner claims (e.g. gpu); repeatable, or set RAINIER_RUNNER_CAPABILITIES to a comma-separated list")
	flag.Parse()
	// The flag wins whole: an operator who passed --capability is naming the
	// complete set, so the environment's list is a DEFAULT, not something the
	// flag adds to. Same rule as every other flag here.
	if len(capabilities) == 0 {
		capabilities = splitCapabilities(os.Getenv("RAINIER_RUNNER_CAPABILITIES"))
	}

	drv := driver.NewDocker(driver.DockerOpts{
		Image:                  *image,
		Network:                *network,
		TotalSlots:             *slots,
		SessionSeccompProfile:  *seccompProfile,
		SessionAppArmorProfile: *appArmorProfile,
	})
	// *proxyURL reaches New directly now (Task 13) so the local HTTP-only
	// surface — today's default, and every dev/CI compose run — injects it
	// into every driver.Spec too, not just agent (dial) mode below.
	s := runnerd.New(drv, *dialBase, *egressAdmin, *proxyURL)

	// Rebuild the registry from the driver's labeled containers before
	// serving, so a restart is truthful about sessions that outlived it
	// instead of forgetting them outright. Always runs first, in both HTTP
	// and dial mode.
	if err := s.Recover(context.Background()); err != nil {
		log.Fatalf("recover: %v", err)
	}

	// Started after Recover so the sweep cannot race its registry writes, and
	// before either serving mode takes the foreground.
	//
	// Recovered sessions are never auto-stopped until they report a child
	// exit — the exit that would make them candidates lived only in the memory
	// of the process that just died. That is the safe direction and it is also
	// a real gap: a runner restarted onto a box full of finished sessions
	// reclaims none of them until each is removed by hand. A durable activity
	// record is #85's job.
	go s.RunIdleStop(context.Background(), *idleStop)

	log.Printf("runnerd on %s (dial-base %s)", *listen, *dialBase)
	if *controld == "" {
		// HTTP-only mode (today's default): the local surface is the whole
		// program, so it owns the foreground.
		log.Fatal(http.ListenAndServe(*listen, s.Handler()))
	}

	// Fail closed: dialing controld with no bearer token would let anyone
	// impersonate this runner on any network path that can reach controld.
	if *runnerToken == "" {
		log.Fatal("--runner-token is required when --controld is set")
	}
	// The local HTTP surface stays up as the dev/debug path, unchanged, but
	// now runs in the background — the agent dial owns the foreground.
	go func() {
		log.Fatal(http.ListenAndServe(*listen, s.Handler()))
	}()
	log.Printf("dialing controld at %s as %q", *controld, *runnerName)
	if err := s.RunAgent(context.Background(), runnerd.AgentConfig{
		ControldURL:  *controld,
		Token:        *runnerToken,
		RunnerName:   *runnerName,
		ProxyURL:     *proxyURL,
		Capabilities: capabilities,
	}); err != nil {
		log.Fatalf("agent: %v", err)
	}
}

// capabilityFlag collects a repeatable --capability into the list runnerd
// announces, in the order they were passed. Nothing is validated here: a
// capability is a claim about this runner, and controld is the one that
// decides whether it will accept and schedule on it — refusing a token
// locally would only move the same error to a place the operator cannot see
// the fleet's answer.
type capabilityFlag []string

func (c *capabilityFlag) String() string { return strings.Join(*c, ",") }

func (c *capabilityFlag) Set(v string) error {
	*c = append(*c, v)
	return nil
}

// splitCapabilities reads RAINIER_RUNNER_CAPABILITIES: a comma-separated
// list, trimmed, with empty entries dropped so a trailing comma (or an unset
// variable) announces nothing rather than a nameless capability.
func splitCapabilities(v string) []string {
	var out []string
	for _, part := range strings.Split(v, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// envDefault returns env's value when it is set and non-empty, else def — the
// same rule cmd/controld applies to every one of its flags, so a secret can be
// handed to runnerd through the environment instead of the command line,
// where every user on the host could read it with ps.
func envDefault(env, def string) string {
	if v := os.Getenv(env); v != "" {
		return v
	}
	return def
}

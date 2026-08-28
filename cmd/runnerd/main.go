// cmd/runnerd/main.go
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"

	"rainier/internal/driver"
	"rainier/internal/runnerd"
)

func main() {
	listen := flag.String("listen", "0.0.0.0:8080", "control + relay listen address")
	dialBase := flag.String("dial-base", "ws://runnerd:8080", "URL sessiond containers dial to register")
	image := flag.String("image", "rainier-session:latest", "default session image")
	network := flag.String("network", "rainier-internal", "internal docker network for sessions")
	egressAdmin := flag.String("egress-admin", "http://egressd:3129", "egressd admin URL")
	slots := flag.Int("slots", 16, "capacity")
	controld := flag.String("controld", "", "controld URL to dial (ws://host:port); enables agent (dial) mode when set")
	runnerToken := flag.String("runner-token", "", "bearer token for the controld dial (required when --controld is set)")
	hostname, _ := os.Hostname()
	runnerName := flag.String("runner-name", hostname, "name this runner announces to controld")
	proxyURL := flag.String("proxy-url", "", "egress proxy URL injected into every session (forwarded to controld dial mode)")
	flag.Parse()

	drv := driver.NewDocker(driver.DockerOpts{Image: *image, Network: *network, TotalSlots: *slots})
	s := runnerd.New(drv, *dialBase, *egressAdmin)

	// Rebuild the registry from the driver's labeled containers before
	// serving, so a restart is truthful about sessions that outlived it
	// instead of forgetting them outright. Always runs first, in both HTTP
	// and dial mode.
	if err := s.Recover(context.Background()); err != nil {
		log.Fatalf("recover: %v", err)
	}

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
		ControldURL: *controld,
		Token:       *runnerToken,
		RunnerName:  *runnerName,
		ProxyURL:    *proxyURL,
	}); err != nil {
		log.Fatalf("agent: %v", err)
	}
}

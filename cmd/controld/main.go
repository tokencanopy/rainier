// cmd/controld/main.go
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"strings"

	"rainier/internal/controld"
	"rainier/internal/controld/pgstore"
)

func main() {
	listen := flag.String("listen", envDefault("RAINIER_LISTEN", ":9090"), "client + runner listen address")
	db := flag.String("db", envDefault("RAINIER_DB", ""), "Postgres DSN (required)")
	runnerToken := flag.String("runner-token", envDefault("RAINIER_RUNNER_TOKEN", ""),
		"fleet-wide bearer every runnerd presents on connect (required; or set RAINIER_RUNNER_TOKEN)")
	admins := flag.String("admins", envDefault("RAINIER_ADMINS", ""), "comma-separated GitHub logins allowed to log in as admin")
	members := flag.String("members", envDefault("RAINIER_MEMBERS", ""), "comma-separated GitHub logins allowed to log in as member")
	externalURL := flag.String("external-url", envDefault("RAINIER_EXTERNAL_URL", ""),
		"http(s) base URL clients and runners reach this replica at (required)")
	githubAPI := flag.String("github-api", envDefault("RAINIER_GITHUB_API", "https://api.github.com"), "GitHub API base URL")
	flag.Parse()

	// Every flag above accepts its RAINIER_* env var as a default (flag.String's
	// default itself, via envDefault) — an explicit flag on the command line
	// always wins over the environment, and the environment always wins over
	// the hardcoded default.
	if *db == "" {
		log.Fatal("controld: --db is required (Postgres DSN)")
	}
	if *runnerToken == "" {
		log.Fatal("controld: --runner-token is required (or set RAINIER_RUNNER_TOKEN)")
	}
	if *externalURL == "" {
		log.Fatal("controld: --external-url is required")
	}

	adminLogins := splitLogins(*admins)
	memberLogins := splitLogins(*members)

	ctx := context.Background()

	st, err := pgstore.Open(ctx, *db)
	if err != nil {
		log.Fatalf("controld: opening store: %v", err)
	}
	defer st.Close()

	srv, err := controld.New(st, controld.Config{
		RunnerToken:   *runnerToken,
		Admins:        adminLogins,
		Members:       memberLogins,
		GitHubAPIBase: *githubAPI,
		ExternalURL:   *externalURL,
	})
	if err != nil {
		log.Fatalf("controld: %v", err)
	}

	log.Printf("controld: external url %s", *externalURL)
	log.Printf("controld: %d admin(s), %d member(s) allowlisted", len(adminLogins), len(memberLogins))
	if len(adminLogins) == 0 && len(memberLogins) == 0 {
		// Fail closed is the right default (no magic first-login promotion),
		// but a fleet operator who forgot to configure either list would
		// otherwise have no clue why every login attempt gets 403 — say so
		// loudly, at startup, where they're looking.
		log.Printf("controld: WARNING — both --admins and --members are empty; nobody can log in")
	}

	go srv.Run(ctx)

	log.Printf("controld listening on %s", *listen)
	log.Fatal(http.ListenAndServe(*listen, srv.Handler()))
}

// envDefault returns the value of the named environment variable, or def if
// it is unset or empty. Used as a flag's default so an explicit command-line
// flag always overrides the environment, which always overrides the
// hardcoded default.
func envDefault(env, def string) string {
	if v := os.Getenv(env); v != "" {
		return v
	}
	return def
}

// splitLogins parses a comma-separated login list, trimming whitespace and
// dropping empty entries (so "alice, ,bob," and "alice,bob" parse the same).
// An empty string yields a nil slice, not a one-element slice holding "".
func splitLogins(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

package main

import (
	"flag"
	"fmt"
	"net/url"
	"os"

	"github.com/tokencanopy/rainier/internal/cli"
)

// runLogout removes this machine's credentials for the current server
// (docs/cli-v0-contract.md §4.2).
//
// The boundaries matter more than the mechanism, because "log out" is a word
// people expect to mean several different things:
//
//   - It removes the access and refresh tokens for ONE context. Nothing else.
//   - It does not delete remote sessions. Signing out of a laptop is not a
//     reason to destroy work running in the cloud, and there is no way to get
//     it back.
//   - It does not revoke Claude or Codex credentials. Those live in the
//     workspace's agent custody, are shared with every device, and have their
//     own explicit command: `rainier agent logout`.
//   - It does not disconnect GitHub. That is an account-level authorization
//     managed in the browser.
//
// It keeps the context itself — server, workspace, owner id — so that a later
// bare `rainier login` knows where to sign back in. What is removed is
// exactly the material that authenticates, which is the only thing "signed
// out" can honestly mean here.
func runLogout(args []string) error {
	fs := flag.NewFlagSet("logout", flag.ExitOnError)
	contextName := fs.String("context", "", "sign out of this `context` instead of the current one")
	fs.Parse(reorderArgs(fs, args))
	if fs.NArg() != 0 {
		return usagef("usage: rainier logout [--context NAME]")
	}

	cfg, err := cli.Load()
	if err != nil {
		return fmt.Errorf("cannot read the rainier config: %w", err)
	}
	name := *contextName
	if name == "" {
		name = cfg.ActiveName()
	}
	ctx, ok := cfg.Contexts[name]
	if !ok {
		// Idempotent, and not an error: "sign me out" is satisfied by being
		// signed out, however that came to be true.
		fmt.Printf("not signed in (no context named %s)\n", name)
		return nil
	}
	if !ctx.SignedIn() {
		fmt.Printf("already signed out of %s\n", describeContext(name, ctx))
		return nil
	}

	if err := cli.UpdateConfig(func(latest *cli.Config) error {
		stored, ok := latest.Contexts[name]
		if !ok {
			return nil
		}
		// Record how this context authenticates BEFORE removing the evidence.
		// A config written before Kind existed says "hosted" only by carrying
		// a refresh token, and this is the one operation that deletes it — so
		// without this line a hosted user who upgrades, logs out and logs back
		// in is told there is no server configured.
		if stored.Hosted() {
			stored.Kind = cli.KindHosted
		}
		stored.Token, stored.RefreshToken, stored.AccessExpiresAt = "", "", ""
		latest.UpdateContext(name, stored)
		return nil
	}); err != nil {
		return fmt.Errorf("saving config: %w", err)
	}
	// The host and never the whole URL, and never a token: a server URL can
	// carry a path or userinfo, and this line ends up in scrollback.
	fmt.Printf("signed out of %s\n", describeContext(name, ctx))
	fmt.Fprintln(os.Stderr, "remote sessions, coding-agent logins and your GitHub connection are unchanged")
	return nil
}

// describeContext names a context by the two things that identify it to a
// person: what they called it and which server it is. The URL is reduced to
// its scheme and host, since everything else in one can be a credential.
func describeContext(name string, ctx cli.Context) string {
	host := "unknown server"
	if u, err := url.Parse(ctx.Server); err == nil && u.Host != "" {
		host = u.Host
	}
	if name == host {
		return name
	}
	return name + " (" + host + ")"
}

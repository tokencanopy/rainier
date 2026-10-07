package main

import (
	"context"
	"slices"

	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// guestBoot is the launch state consumed by main before preparing the boot chain.
type guestBoot struct {
	conn    relay.Conn
	config  runner.BootConfig
	argv    []string
	failure *bootFailure
}

func bootGuest(ctx context.Context, dial dialSession, boots *bootstrapper, imageCommand []string) (guestBoot, error) {
	conn, cfg, failure, err := bootOverVsock(ctx, dial, boots)
	if err != nil {
		return guestBoot{}, err
	}
	// The image command only supplies a fallback. A microVM's per-session
	// command arrives on the authenticated boot channel, not in PID 1's argv.
	command := imageCommand
	if len(cfg.Cmd) > 0 {
		command = cfg.Cmd
	}
	return guestBoot{conn: conn, config: cfg, argv: slices.Clone(command), failure: failure}, nil
}

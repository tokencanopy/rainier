package main

import (
	"context"
	"os/exec"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/protocol/runner"
)

func TestGuestBootLaunchesRequestedCommand(t *testing.T) {
	for _, requested := range []bool{true, false} {
		name := "image_fallback"
		if requested {
			name = "requested_command"
		}
		t.Run(name, func(t *testing.T) {
			cleanEnv(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			dial, hosts := fakeTransport(t)
			type result struct {
				boot guestBoot
				err  error
			}
			done := make(chan result, 1)
			go func() {
				boot, err := bootGuest(ctx, dial, &bootstrapper{}, []string{"/bin/sh", "-c", "printf image_default"})
				done <- result{boot, err}
			}()
			host := <-hosts
			cfg := runner.BootConfig{Protocol: runner.SessionBootstrapProtocolVersion, SessionID: "session_command_test"}
			want := "image_default"
			if requested {
				// Shell-looking text must remain one literal argument, not be re-parsed.
				want = "requested ; $(false) ' quoted"
				cfg.Cmd = []string{"/bin/sh", "-c", `printf '%s' "$1"`, "command-test", want}
			}
			host.sendBootConfig(cfg)
			got := <-done
			if got.err != nil || got.boot.failure != nil {
				t.Fatalf("guest boot failed: %v %v", got.err, got.boot.failure)
			}
			defer got.boot.conn.Close()
			output, err := exec.CommandContext(ctx, got.boot.argv[0], got.boot.argv[1:]...).Output()
			if err != nil {
				t.Fatal(err)
			}
			if string(output) != want {
				t.Fatalf("launched output %q, want %q", output, want)
			}
		})
	}
}

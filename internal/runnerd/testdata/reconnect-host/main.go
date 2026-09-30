// A built execution probe for the optional host callback, not a shipping command.
// The driver and guest proof provider are synthetic; RunAgent and the host callback
// are real. No capability or local RPC endpoint is enabled by this probe.
package main

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"github.com/tokencanopy/rainier/internal/driver"
	"github.com/tokencanopy/rainier/internal/runnerd"
	"github.com/tokencanopy/rainier/protocol/runner"
	"os"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := runnerd.New(driver.NewFake(4), "", "", "")
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = s.RunAgent(ctx, runnerd.AgentConfig{ControldURL: os.Args[1], Token: "testtoken", RunnerName: "runner_process_test"})
	}()
	private := ed25519.NewKeyFromSeed(make([]byte, 32))
	input := bufio.NewScanner(os.Stdin)
	for input.Scan() {
		if input.Text() != "authorize" {
			break
		}
		out, err := s.AuthorizeGuestReconnect(ctx, "session_test", func(ctx context.Context, c runner.GuestReconnectChallenge) (string, error) {
			message, err := c.SigningMessage()
			if err != nil {
				return "", err
			}
			return base64.RawURLEncoding.EncodeToString(ed25519.Sign(private, message)), nil
		})
		if err != nil {
			fmt.Println(err.Error())
		} else if out.Epoch == 2 && out.ExpiresInSec == 120 {
			fmt.Println("accepted")
		} else {
			fmt.Println("unexpected")
		}
	}
	cancel()
	<-done
}

// A built execution probe for the optional host callback, not a shipping command.
// The driver and signing guest are synthetic. RunAgent, host authorization and
// bounded guest stream transport run over real WebSocket and TCP connections.
// No capability or shipping listener is enabled by this probe.
package main

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/tokencanopy/rainier/internal/driver"
	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/internal/runnerd"
	"github.com/tokencanopy/rainier/protocol/runner"
	"net"
	"os"
	"strings"
	"time"
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
	input := bufio.NewScanner(os.Stdin)
	for input.Scan() {
		mode := input.Text()
		if mode != "authorize" && mode != "wrong_attempt" && mode != "oversize" {
			break
		}
		out, err := authorizeStream(ctx, s, mode)
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

func authorizeStream(parent context.Context, s *runnerd.Server, mode string) (runner.GuestReconnectAcceptResponse, error) {
	var zero runner.GuestReconnectAcceptResponse
	ctx, cancel := context.WithTimeout(parent, 6*time.Second)
	defer cancel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return zero, errors.New("unavailable")
	}
	defer listener.Close()
	peer, err := (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
	if err != nil {
		return zero, errors.New("unavailable")
	}
	defer peer.Close()
	host, err := listener.Accept()
	if err != nil {
		return zero, errors.New("unavailable")
	}
	defer host.Close()
	guestDone := make(chan error, 1)
	go func() {
		conn := relay.NetConn(peer)
		event, err := relay.ReadGuestReconnectFrame(ctx, conn)
		if err != nil {
			guestDone <- nil
			return
		} // authority refused before a challenge
		if event.Kind != relay.KindGuestReconnectChallenge {
			guestDone <- errors.New("unexpected")
			return
		}
		challenge, err := runner.DecodeGuestReconnectChallenge(event.Payload)
		if err != nil {
			guestDone <- errors.New("unexpected")
			return
		}
		if mode == "oversize" {
			_, _ = peer.Write([]byte(strings.Repeat("x", 4097)))
		} else {
			message, _ := challenge.SigningMessage()
			private := ed25519.NewKeyFromSeed(make([]byte, 32))
			proof := runner.GuestReconnectAcceptRequest{Protocol: 1, AttemptID: challenge.AttemptID, Signature: base64.RawURLEncoding.EncodeToString(ed25519.Sign(private, message))}
			if mode == "wrong_attempt" {
				proof.AttemptID = "other_test"
			}
			body, _ := json.Marshal(proof)
			_ = relay.WriteGuestReconnectFrame(ctx, conn, relay.KindGuestReconnectProof, body)
		}
		// Neither acceptance nor configuration belongs to this transport helper.
		if raw, err := conn.Read(ctx); err == nil || len(raw) > 0 {
			guestDone <- errors.New("unexpected")
			return
		}
		guestDone <- nil
	}()
	accepted, err := driver.AuthorizeGuestConnection(ctx, s, "session_test", relay.NetConn(host))
	_ = host.Close()
	if guestErr := <-guestDone; guestErr != nil {
		return zero, guestErr
	}
	return accepted, err
}

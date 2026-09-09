package egress

import (
	"context"
	"net"
	"net/netip"
	"testing"
	"time"
)

func TestDialAddressesFallsBackAfterBlackhole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	var targets []string
	conn, err := dialAddresses(ctx, []netip.Addr{netip.MustParseAddr("192.0.2.1"), netip.MustParseAddr("192.0.2.2")}, "443", func(attempt context.Context, network, address string) (net.Conn, error) {
		targets = append(targets, address)
		if network != "tcp" {
			t.Fatalf("network = %q", network)
		}
		if len(targets) == 1 {
			<-attempt.Done()
			return nil, attempt.Err()
		}
		if err := attempt.Err(); err != nil {
			return nil, err
		}
		return client, nil
	})
	if err != nil || conn != client {
		t.Fatalf("healthy fallback not reached: %v; targets=%v", err, targets)
	}
	if len(targets) != 2 || targets[0] != "192.0.2.1:443" || targets[1] != "192.0.2.2:443" {
		t.Fatalf("did not dial vetted IPs: %v", targets)
	}
}

package driver

import (
	"net"
	"testing"
)

func TestGuestReconnectAdmissionAllowsOnePendingPeer(t *testing.T) {
	g := &guestChannel{reconnect: true}
	first, peer := net.Pipe()
	defer peer.Close()
	defer first.Close()
	defer g.close()
	boot, ok := g.admit(first)
	if !ok || !boot {
		t.Fatal("initial boot refused")
	}
	next, other := net.Pipe()
	defer next.Close()
	defer other.Close()
	if _, ok := g.admit(next); ok {
		t.Fatal("peer admitted while initial delivery pending")
	}
	g.finishAdmission()
	boot, ok = g.admit(next)
	if !ok || boot {
		t.Fatal("reconnect reused initial boot path")
	}
	excess, last := net.Pipe()
	defer excess.Close()
	defer last.Close()
	if _, ok := g.admit(excess); ok {
		t.Fatal("unbounded pending reconnects")
	}
	g.finishAdmission()
	g.drop(next)
	boot, ok = g.admit(excess)
	if !ok || boot {
		t.Fatal("failed attempt prevented fresh proof")
	}
}

func TestGuestReconnectTrackedCloseReleasesConnection(t *testing.T) {
	g := &guestChannel{reconnect: true}
	a, b := net.Pipe()
	defer b.Close()
	g.admit(a)
	tracked := &guestTrackedConn{Conn: a, channel: g}
	if err := tracked.Close(); err != nil {
		t.Fatal(err)
	}
	g.mu.Lock()
	count := len(g.conns)
	g.mu.Unlock()
	if count != 0 {
		t.Fatal("closed connections accumulate in listener")
	}
}

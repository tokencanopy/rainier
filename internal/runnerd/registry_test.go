// internal/runnerd/registry_test.go
package runnerd

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/internal/relay"
)

func TestRegistryTracksAndWaits(t *testing.T) {
	r := newRegistry()
	if _, ok := r.get("s1"); ok {
		t.Fatal("empty registry returned a hub")
	}
	r.put("s1", &sessionEntry{id: "s1", state: "running"})
	e, ok := r.get("s1")
	if !ok || e.id != "s1" {
		t.Fatalf("get = %+v, %v", e, ok)
	}
	r.remove("s1")
	if _, ok := r.get("s1"); ok {
		t.Fatal("removed entry still present")
	}
}

// TestSetHubReportsTheHubItDisplacesSoItCanBeRetired is review round 2,
// finding 2.
//
// setHub used to overwrite the field and drop the previous hub on the floor,
// naming nobody. The ordinary re-register is a hub whose conn already died,
// so nothing was visibly wrong — but a SECOND live connection claiming one
// session left the first hub running with nothing able to reach it: its
// readLoop never errored, and its conn, its fd and its attachments were held
// for the life of the process, because the peer that decides when that conn
// dies is the sandbox.
//
// It is reported rather than closed here, because a displaced hub still has
// frames to deliver — see retireDisplacedHub, which is what ends it.
func TestSetHubReportsTheHubItDisplacesSoItCanBeRetired(t *testing.T) {
	r := newRegistry()
	r.put("s1", &sessionEntry{id: "s1", state: "running"})

	firstGuest, firstHost := net.Pipe()
	defer firstGuest.Close()
	defer firstHost.Close()
	first := relay.NewHub(context.Background(), relay.NetConn(firstHost))
	if displaced, ok := r.setHub("s1", first); !ok || displaced != nil {
		t.Fatalf("the first setHub displaced %p (ok=%v); there was nothing to displace", displaced, ok)
	}

	secondGuest, secondHost := net.Pipe()
	defer secondGuest.Close()
	defer secondHost.Close()
	second := relay.NewHub(context.Background(), relay.NetConn(secondHost))
	displaced, ok := r.setHub("s1", second)
	if !ok {
		t.Fatal("the entry did not take the second hub")
	}
	if displaced != first {
		t.Fatalf("setHub reported %p as displaced, want the first hub %p — an unreported hub is a leaked one", displaced, first)
	}
	got, ok := r.hub("s1")
	if !ok || got != second {
		t.Fatalf("the registry holds %p, want the second hub %p", got, second)
	}

	// Setting the SAME hub again displaces nothing: a caller told to retire
	// it would be closing the session's own live connection.
	if displaced, ok := r.setHub("s1", second); !ok || displaced != nil {
		t.Fatalf("re-setting a session's own hub reported it as displaced (%p, ok=%v)", displaced, ok)
	}

	// And an entry that vanished reports neither.
	r.remove("s1")
	if displaced, ok := r.setHub("s1", second); ok || displaced != nil {
		t.Fatalf("setHub on a removed entry = (%p, %v), want (nil, false)", displaced, ok)
	}
}

// TestRetireDisplacedHubWaitsAndThenCloses pins both halves of the grace.
//
// A hub that is going to drain what it already read gets to: the child_exited
// on a conn a redial replaced can be the only copy of it
// (TestAChildExitAcrossARedialIsRecordedEndToEnd). A hub whose peer simply
// never hangs up does not get to hold a conn, an fd and its attachments
// forever.
func TestRetireDisplacedHubWaitsAndThenCloses(t *testing.T) {
	guest, host := net.Pipe()
	defer guest.Close()
	defer host.Close()
	h := relay.NewHub(context.Background(), relay.NetConn(host))

	// Within the grace it is left alone, live conn and all.
	go retireDisplacedHub("s1", h, time.Hour)
	select {
	case <-h.Done():
		t.Fatal("a displaced hub was closed inside its grace, dropping what it had already read")
	case <-time.After(100 * time.Millisecond):
	}

	// And when the grace is up it goes, peer or no peer.
	go retireDisplacedHub("s1", h, time.Millisecond)
	select {
	case <-h.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("a displaced hub outlived its grace; its conn, its fd and its attachments are leaked")
	}
}

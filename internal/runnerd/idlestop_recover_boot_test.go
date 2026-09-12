package runnerd

import (
	"context"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/internal/driver"
)

// TestRecoveredSessionsAreMintedABootEpochToo pins the half of the boot-epoch
// fix that the delete-and-recreate test cannot reach: entries rebuilt by
// Recover go through put(), not putIfAbsent(), and a put() that forgot to mint
// would leave every recovered session on epoch zero — the value a control
// frame from a container this runner never watched start would also carry.
// Mutating mintBoot out of put() left the suite green; this is the test that
// makes it fail.
func TestRecoveredSessionsAreMintedABootEpochToo(t *testing.T) {
	ctx := context.Background()
	fd := driver.NewFake(4)
	for _, id := range []string{"sess-recovered-a", "sess-recovered-b"} {
		if _, err := fd.Create(ctx, driver.Spec{SessionID: id, Image: "img.invalid"}); err != nil {
			t.Fatalf("seed the container: %v", err)
		}
	}
	clk := newFakeClock()
	rd := New(fd, "", "", "")
	rd.now = clk.now
	if err := rd.Recover(ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}

	a, b := rd.reg.currentBoot("sess-recovered-a"), rd.reg.currentBoot("sess-recovered-b")
	if a == 0 || b == 0 {
		t.Fatalf("recovered entries carry boot epochs %d and %d; zero is the epoch nothing may live on", a, b)
	}
	if a == b {
		t.Fatalf("two recovered entries share boot epoch %d; the counter is registry-wide so an id reused later cannot collide", a)
	}

	// A frame on the epoch nothing lives on is refused by the registry: the
	// recovered session is not marked exited, and so is never auto-stopped.
	rd.routeControl("sess-recovered-a", 0, 1, []byte(`{"kind":"child_exited","rc":0}`))
	if e, ok := rd.reg.get("sess-recovered-a"); !ok || !e.childExitedAt.IsZero() {
		t.Fatal("a child_exited carrying epoch zero was recorded against a recovered session")
	}
	clk.set(time.Hour)
	if stops := rd.sweepIdle(ctx, 30*time.Minute); len(stops) != 0 {
		t.Fatalf("stopped %v on the strength of a frame from no boot", stops)
	}

	// Its own epoch still works, so recovery has not fenced the session off
	// from the exit its real child will report.
	rd.routeControl("sess-recovered-a", a, 1, []byte(`{"kind":"child_exited","rc":0}`))
	if e, ok := rd.reg.get("sess-recovered-a"); !ok || e.childExitedAt.IsZero() {
		t.Fatal("a child_exited on the recovered session's own epoch was not recorded")
	}
}

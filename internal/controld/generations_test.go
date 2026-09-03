// internal/controld/generations_test.go
package controld

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/protocol/runner"
)

// TestRunnerGenerationContinuesAcrossRestart pins that the generation is the
// store's, not the process's: a second Server over the same store registers
// the runner at the next generation, not at 1.
func TestRunnerGenerationContinuesAcrossRestart(t *testing.T) {
	st := NewMemStore()
	s1, ts1 := newTestControldOver(t, st)
	joinRunner(t, s1, ts1, runnerScript{Name: "vm1", Total: 4})
	ts1.Close()

	s2, ts2 := newTestControldOver(t, st)
	joinRunner(t, s2, ts2, runnerScript{Name: "vm1", Total: 4})

	rows, err := st.Fleet().ListRunners(context.Background(), installPool)
	if err != nil || len(rows) != 1 || rows[0].Generation != 2 {
		t.Fatalf("after restart: %+v, %v; want vm1 at generation 2", rows, err)
	}
}

// TestSupersededConnectionIsFencedOnHeartbeat: once the store holds a newer
// generation for a runner (another replica registered it), this replica's
// connection is refused at its next heartbeat and ends.
func TestSupersededConnectionIsFencedOnHeartbeat(t *testing.T) {
	s, st, ts := newTestControld(t)
	f := joinRunner(t, s, ts, runnerScript{Name: "vm1", Total: 4})

	if _, err := st.NextRunnerGeneration(context.Background(), installPool, "vm1"); err != nil {
		t.Fatal(err)
	}

	// Any message heartbeats, and the fake stamps its capacity onto whatever
	// it writes — so a heartbeat that got through would be visible as a used
	// slot on the row.
	f.setCapacity(1, 4)
	f.write(t, runner.FromRunner{Type: "event", Session: "sess_nobody", State: "running"})

	eventually(t, 2*time.Second, func() error {
		if s.runnerConnected("vm1") {
			return errors.New("superseded connection still registered")
		}
		return nil
	})
	rows, _ := st.Fleet().ListRunners(context.Background(), installPool)
	if rows[0].Generation != 2 || rows[0].CapacityUsed != 0 {
		t.Fatalf("the stale heartbeat wrote through: %+v", rows[0])
	}
}

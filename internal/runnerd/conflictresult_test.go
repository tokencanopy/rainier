package runnerd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/protocol/runner"
)

// nextResult reads up the agent's connection until the result for reqID
// arrives, skipping the events (a stop's own "suspended_cold", say) that share
// the socket.
func nextResult(t *testing.T, conn *fakeConn, reqID uint64) runner.FromRunner {
	t.Helper()
	for {
		m := conn.readMsg(t)
		if m.Type == "result" && m.ReqID == reqID {
			return m
		}
	}
}

// TestTheTwoSurfacesAgreeOnWhichRefusalsAreConflicts is the anti-drift pin,
// and it exists because the drift itself is the defect this test's siblings
// fix: `errSuspendInFlight` was born a 409 on the runner's local HTTP front
// and a plain not-ok on the control connection, which the control plane could
// only report as an internal error.
//
// So the rule is stated once, over every sentinel the op surfaces can return:
// mapOpErr answers 409 for exactly the errors opConflict calls conflicts. Add
// a sentinel to one side only and this fails, whichever side you forgot.
func TestTheTwoSurfacesAgreeOnWhichRefusalsAreConflicts(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"no such session", errNoSuchSession},
		{"still starting", errSessionStarting},
		{"unknown op", errUnknownOp},
		{"suspend in flight", errSuspendInFlight},
		{"session exists", errSessionExists},
		{"wrapped suspend in flight", fmt.Errorf("resume: %w", errSuspendInFlight)},
		{"an unclassified driver failure", errors.New("docker daemon is not running")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			mapOpErr(rec, tc.err, func() { t.Fatal("onOK ran for an error") })
			http409 := rec.Code == http.StatusConflict
			if wire := opConflict(tc.err); wire != http409 {
				t.Fatalf("%v: the local front says %d, the control connection says conflict=%v; "+
					"one refusal, two answers", tc.err, rec.Code, wire)
			}
		})
	}
	// And nil is neither, which is what keeps a SUCCESSFUL result from
	// claiming to be a conflict.
	if opConflict(nil) {
		t.Fatal("a nil error reads as a conflict")
	}
	// The one refusal whose two surfaces disagree on purpose. The create
	// route answers errSessionExists 409 without going through mapOpErr,
	// because on the control path the agent reports that id collision as OK —
	// the desired state is reached — and `ok: true, conflict: true` would be
	// two answers to one question. Stated here so the exemption is a decision
	// on the record rather than a gap someone finds later.
	if opConflict(errSessionExists) {
		t.Fatal("errSessionExists is flagged as a conflict, but the agent reports it as a SUCCESS: " +
			"the result would claim to be both")
	}
}

// TestARefusedResumeReachesControldAsAConflict drives the refusal up the real
// control connection, which is the seam the previous round left open: the
// runner refuses a resume that overtook a stop it had already claimed, and
// the only thing controld could see was `ok: false` — indistinguishable from
// a command that genuinely failed, and therefore reported to the person as an
// internal error rather than as the transient conflict it is.
//
// Fails without FromRunner.Conflict being set on the agent's result arm: the
// field reads false and the refusal is a plain failure again.
func TestARefusedResumeReachesControldAsAConflict(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	sd := newStallingStopDriver()
	rd := New(sd, "", "", "")
	rd.now = clk.now // before anything serves
	h := &idleHarness{t: t, clk: clk, rd: rd, fd: sd.Fake, id: "sess-idle-1", boot: map[string]uint64{}}
	h.create(h.id)
	h.run(30*time.Minute, idleStep{at: 0, act: childExits})
	h.clk.set(30 * time.Minute)

	fc := newFakeControld(t, testToken)
	agentCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	go rd.RunAgent(agentCtx, AgentConfig{ControldURL: fc.wsURL(), Token: testToken, RunnerName: "vm1"})
	conn := fc.nextConn(t)
	conn.readAnnounce(t)

	swept := make(chan []string, 1)
	go func() { swept <- rd.sweepIdle(ctx, 30*time.Minute) }()
	<-sd.entered

	// The command controld sends when a person types `rainier attach` on a
	// row that reads stopped — which is what the row reads while this stop is
	// in flight, because announceState renders a "suspending" entry as
	// suspended_cold.
	conn.send(t, runner.ToRunner{Type: "resume", Session: h.id, ReqID: 7})
	res := nextResult(t, conn, 7)
	if res.OK {
		t.Fatal("the resume was accepted while a stop was in flight")
	}
	if !res.Conflict {
		t.Error("the refusal reached controld as a plain failure: nothing on the wire says it is a " +
			"conflict, so the control plane can only report a healthy runner mid-stop as an internal error")
	}
	if res.Detail != errSuspendInFlight.Error() {
		t.Errorf("result detail = %q, want %q", res.Detail, errSuspendInFlight.Error())
	}

	// The stop lands, and the same command from the same control plane now
	// succeeds — the conflict was "not yet", which is exactly what the bit
	// claims and what makes retrying it right.
	close(sd.release)
	if stops := <-swept; len(stops) != 1 || stops[0] != h.id {
		t.Fatalf("sweep stopped %v, want the session", stops)
	}
	conn.send(t, runner.ToRunner{Type: "resume", Session: h.id, ReqID: 8})
	ok := nextResult(t, conn, 8)
	if !ok.OK || ok.Conflict {
		t.Fatalf("resume after the stop landed = ok %v conflict %v, want ok and no conflict", ok.OK, ok.Conflict)
	}
}

// TestAnOrdinaryFailureIsNotAConflict keeps the bit narrow: a command the
// runner could not carry out is still a plain failure, because a control plane
// that retried those would retry a broken docker daemon forever.
func TestAnOrdinaryFailureIsNotAConflict(t *testing.T) {
	rd, _, conn := runnerWithControld(t)
	conn.send(t, runner.ToRunner{Type: "resume", Session: "sess-nobody.invalid", ReqID: 3})
	res := nextResult(t, conn, 3)
	if res.OK {
		t.Fatal("a resume for a session this runner does not hold was accepted")
	}
	if res.Conflict {
		t.Error("a session that does not exist was reported as a conflict; retrying it can never help")
	}
	_ = rd
}

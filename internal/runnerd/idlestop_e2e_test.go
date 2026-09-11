package runnerd

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/tokencanopy/rainier/internal/driver"
	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/runner"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

// The unit table next door drives the rule; this file drives the PATH — a real
// websocket standing in for the container's sessiond, a real websocket client
// on /attach, the real agent connection to a fake controld, and the fake
// driver underneath. Nothing here needs docker: internal/driver.Fake plus this
// package's in-process /register sandbox is the whole stack below the runner.

// attachmentsOn reads how many attachments the registry currently counts for
// id. In-package, and only a test reads it: the production code reads it under
// the same lock inside the idle predicate.
func attachmentsOn(rd *Server, id string) int {
	for _, e := range rd.reg.list() {
		if e.id == id {
			return e.attachments
		}
	}
	return -1
}

// waitForAttachments polls until the registry counts want attachments for id.
// A detach is asynchronous — the count drops when AttachClient's pump returns,
// which is after the client's conn actually dies — so the alternative to
// polling is a sleep long enough to be flaky.
func waitForAttachments(t *testing.T, rd *Server, id string, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if attachmentsOn(rd, id) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("session %s has %d attachment(s), want %d", id, attachmentsOn(rd, id), want)
}

// nextEventOfState reads up the agent's connection until an event with the
// given state arrives, skipping whatever else shares the socket.
func nextEventOfState(t *testing.T, conn *fakeConn, state string) runner.FromRunner {
	t.Helper()
	for {
		m := conn.readMsg(t)
		if m.Type == "event" && m.State == state {
			return m
		}
	}
}

// attachOnce opens a real attachment on the local /attach front and returns
// it. The resize is the first frame the front requires.
func attachOnce(t *testing.T, ctx context.Context, srvURL, id string) *websocket.Conn {
	t.Helper()
	base := strings.Replace(srvURL, "http", "ws", 1)
	c, _, err := websocket.Dial(ctx, base+"/attach?session="+id, nil)
	if err != nil {
		t.Fatalf("dial /attach: %v", err)
	}
	c.SetReadLimit(16 << 20)
	if err := wsjson.Write(ctx, c, terminal.ClientMessage{Type: "resize", Cols: 80, Rows: 24}); err != nil {
		t.Fatalf("first resize: %v", err)
	}
	return c
}

// TestIdleStopEndToEnd is the whole feature through the real path: a
// registered sandbox reports its child exited over the control channel, a
// viewer holds it open past the timeout, and only once that viewer leaves and
// the timeout passes does the runner stop it — releasing the slot, keeping the
// workspace, landing on the state an operator's stop lands on, and telling
// controld in the vocabulary the announce already uses.
func TestIdleStopEndToEnd(t *testing.T) {
	const idle = 30 * time.Minute
	clk := newFakeClock()
	rd, srv, conn := runnerWithControld(t, func(s *Server) { s.now = clk.now })
	fd := rd.drv.(*driver.Fake)
	ctx := context.Background()

	id, sb := dialSandbox(t, rd, srv)
	// The registration's own event, so the assertions below read the socket
	// from a known point.
	if m := nextEventOfState(t, conn, "running"); m.Session != id {
		t.Fatalf("running event for %q, want %q", m.Session, id)
	}

	// A viewer arrives, then the agent finishes underneath them.
	client := attachOnce(t, ctx, srv.URL, id)
	waitForAttachments(t, rd, id, 1)
	sb.send(t, relay.ControlEvent{Kind: "child_exited", RC: 0})
	if m := nextEventOfState(t, conn, "child_exited"); m.Detail != "0" {
		t.Fatalf("child_exited detail = %q, want %q", m.Detail, "0")
	}

	// Ten hours of somebody reading the scrollback is not idleness.
	clk.set(10 * time.Hour)
	if stops := rd.sweepIdle(ctx, idle); len(stops) != 0 {
		t.Fatalf("stopped %v with a viewer attached", stops)
	}
	if used, _, _ := fd.Capacity(ctx); used != 1 {
		t.Fatalf("slots used with a viewer attached = %d, want 1", used)
	}

	// The viewer leaves. The timer starts HERE, not at the exit ten hours ago.
	client.CloseNow()
	waitForAttachments(t, rd, id, 0)
	clk.set(10*time.Hour + 29*time.Minute)
	if stops := rd.sweepIdle(ctx, idle); len(stops) != 0 {
		t.Fatalf("stopped %v 29 minutes after the detach", stops)
	}

	clk.set(10*time.Hour + 30*time.Minute)
	if stops := rd.sweepIdle(ctx, idle); len(stops) != 1 || stops[0] != id {
		t.Fatalf("sweep stopped %v, want [%s]", stops, id)
	}

	// What the operator's stop would have left: a stopped container, the
	// workspace untouched, and the slot back.
	handle, state, ok := rd.reg.opTarget(id)
	if !ok || state != "suspended" {
		t.Fatalf("registry state = %q (present: %v), want %q", state, ok, "suspended")
	}
	h, err := fd.Inspect(ctx, handle)
	if err != nil || h.State != driver.StateSuspended {
		t.Fatalf("container state = %+v (%v), want suspended", h, err)
	}
	if used, _, _ := fd.Capacity(ctx); used != 0 {
		t.Fatalf("slots used after the idle stop = %d, want 0", used)
	}
	if vols := fd.Volumes(); len(vols) != 1 {
		t.Fatalf("workspace volumes = %v, want the session's still there", vols)
	}

	// And what controld hears: the park, with the capacity split beside it.
	ev := nextEventOfState(t, conn, "suspended_cold")
	if ev.Session != id {
		t.Fatalf("suspended_cold event for %q, want %q", ev.Session, id)
	}
	if !strings.Contains(ev.Detail, "idle for") {
		t.Fatalf("suspended_cold detail = %q, want it to say how long it was idle", ev.Detail)
	}
	if ev.Total == 0 {
		t.Fatalf("event carried total = 0; capacity must ride every message")
	}
	if ev.Active != 0 || ev.IdleExited != 0 {
		t.Fatalf("counts after the only session stopped = (%d active, %d idle-exited), want (0, 0)",
			ev.Active, ev.IdleExited)
	}

	// `docker stop` kills the container's sessiond, so its conn dies too —
	// the one socket-level event this runner must NOT read as a crash. The
	// entry survives it, and the announce says the session is cold-parked.
	sb.c.CloseNow()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, hubUp := rd.reg.hub(id); !hubUp {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the hub outlived the sandbox conn")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, ok := rd.reg.get(id); !ok {
		t.Fatal("the session was removed when its stopped container's conn died")
	}
	ann := rd.Announce()
	if len(ann) != 1 || ann[0].ID != id || ann[0].State != "suspended_cold" {
		t.Fatalf("announce = %+v, want one suspended_cold for %s", ann, id)
	}
	if vols := fd.Volumes(); len(vols) != 1 {
		t.Fatalf("workspace volumes = %v, want the session's still there", vols)
	}
}

// TestIdleStopCountsRideEveryMessage: the two additive counts are on the
// announce as well as on events, since a control plane's first sight of a
// runner is its announce.
func TestIdleStopCountsRideEveryMessage(t *testing.T) {
	clk := newFakeClock()
	rd, srv, conn := runnerWithControld(t, func(s *Server) { s.now = clk.now })

	id, sb := dialSandbox(t, rd, srv)
	m := nextEventOfState(t, conn, "running")
	if m.Active != 1 || m.IdleExited != 0 {
		t.Fatalf("counts with one working agent = (%d, %d), want (1, 0)", m.Active, m.IdleExited)
	}

	sb.send(t, relay.ControlEvent{Kind: "child_exited", RC: 7})
	ev := nextEventOfState(t, conn, "child_exited")
	if ev.Session != id {
		t.Fatalf("child_exited for %q, want %q", ev.Session, id)
	}
	// The count is read when the message is sent, which is after the exit was
	// recorded — the same message therefore reports the sandbox as idle.
	if ev.Active != 0 || ev.IdleExited != 1 {
		t.Fatalf("counts after the child exited = (%d, %d), want (0, 1)", ev.Active, ev.IdleExited)
	}
	if ev.Used != 1 || ev.Total == 0 {
		t.Fatalf("used/total = %d/%d, want the slot still held by the up-but-finished sandbox", ev.Used, ev.Total)
	}
}

// TestIdleStopCountsTheControldSideAttachToo is the dial_attach half of the
// same rule: a viewer who arrived through controld's attach plane holds the
// session open exactly as a local one does, and leaving starts the timer
// exactly as leaving locally does. The counting is deliberately around both
// fronts, so a regression in either is a stopped session with somebody
// watching it.
func TestIdleStopCountsTheControldSideAttachToo(t *testing.T) {
	clk := newFakeClock()
	rd := New(driver.NewFake(4), "", "", "")
	rd.now = clk.now
	srv := httptest.NewServer(rd.Handler())
	defer srv.Close()
	ctx := context.Background()

	id, sb := dialSandbox(t, rd, srv)
	sb.send(t, relay.ControlEvent{Kind: "child_exited", RC: 0})
	waitForChildExit(t, rd, id)

	// controld's end of the attach-back: it accepts the dial and holds the
	// conn open, which is all a viewer is as far as the runner is concerned.
	viewerGone := make(chan struct{})
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		<-viewerGone
	}))
	defer target.Close()
	targetWS := strings.Replace(target.URL, "http", "ws", 1)

	// Driven directly rather than through a controld connection: the origin
	// guard only permits a target that IS this runner's controld, so the
	// config names the same server the attachment lands on.
	attachDone := make(chan struct{})
	go func() {
		defer close(attachDone)
		rd.dialAttachBack(ctx, runner.ToRunner{Type: "dial_attach", Session: id,
			Attach: &runner.Attach{AttachID: "0123456789abcdef", Cols: 80, Rows: 24, TargetURL: targetWS}},
			AgentConfig{ControldURL: targetWS, Token: testToken})
	}()
	waitForAttachments(t, rd, id, 1)

	clk.set(10 * time.Hour)
	if stops := rd.sweepIdle(ctx, 30*time.Minute); len(stops) != 0 {
		t.Fatalf("stopped %v with a controld-side viewer attached", stops)
	}

	// The viewer leaves: the timer starts from here, on this front too.
	close(viewerGone)
	waitForAttachments(t, rd, id, 0)
	<-attachDone
	clk.set(10*time.Hour + 29*time.Minute)
	if stops := rd.sweepIdle(ctx, 30*time.Minute); len(stops) != 0 {
		t.Fatalf("stopped %v 29 minutes after the controld-side viewer left", stops)
	}
	clk.set(10*time.Hour + 30*time.Minute)
	if stops := rd.sweepIdle(ctx, 30*time.Minute); len(stops) != 1 {
		t.Fatalf("sweep stopped %v, want the session 30 minutes after the detach", stops)
	}
}

// waitForChildExit polls until the registry has recorded id's child exit. The
// report travels over a websocket and is routed on a goroutine of its own, so
// a test that acted on it immediately would be racing the frame.
func waitForChildExit(t *testing.T, rd *Server, id string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, e := range rd.reg.list() {
			if e.id == id && !e.childExitedAt.IsZero() {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("session %s never recorded a child exit", id)
}

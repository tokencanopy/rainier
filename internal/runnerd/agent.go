// internal/runnerd/agent.go
package runnerd

import (
	"context"
	"errors"
	"log"
	mrand "math/rand"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"rainier/internal/driver"
	"rainier/internal/rwire"
)

// AgentConfig configures RunAgent's outbound dial to controld.
type AgentConfig struct {
	ControldURL string // e.g. ws://host:9090 — /v1/runners/connect appended
	Token       string
	RunnerName  string
	ProxyURL    string // forwarded into every driver.Spec (egress R4)
}

// jitter returns a random duration in [0, d/2) — timing spread, not security.
func jitter(d time.Duration) time.Duration { return time.Duration(mrand.Int63n(int64(d / 2))) }

// nextBackoff doubles d and clamps the RESULT to a 30s cap. Mirrors
// cmd/sessiond's nextBackoff (package main there, so not importable): that
// function's doc comment explains why clamping the doubled value — not
// guarding the pre-doubled one — is what actually holds a 1s..30s cap
// instead of drifting to 32s and freezing there.
func nextBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

// RunAgent dials controld and serves its commands until ctx is canceled,
// redialing with jittered backoff (1s..30s cap) whenever the connection
// ends. It only returns once ctx is done — any other agentSession error is
// logged and retried, since a runner with no control conn is still useful
// on its local HTTP surface but should keep trying to phone home.
func (s *Server) RunAgent(ctx context.Context, cfg AgentConfig) error {
	s.proxyURL = cfg.ProxyURL
	backoff := time.Second
	for {
		err := s.agentSession(ctx, cfg)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Printf("controld conn ended: %v; redialing in %s", err, backoff)
		select {
		case <-time.After(backoff + jitter(backoff)):
		case <-ctx.Done():
			return ctx.Err()
		}
		backoff = nextBackoff(backoff)
	}
}

// agentSession dials controld once, sends the announce as the FIRST message
// on the conn, then serves rwire.ToRunner commands until the conn ends.
func (s *Server) agentSession(ctx context.Context, cfg AgentConfig) error {
	hdr := http.Header{"Authorization": {"Bearer " + cfg.Token}}
	c, _, err := websocket.Dial(ctx, cfg.ControldURL+"/v1/runners/connect", &websocket.DialOptions{HTTPHeader: hdr})
	if err != nil {
		return err
	}
	defer c.CloseNow()
	c.SetReadLimit(16 << 20)

	out := make(chan rwire.FromRunner, 64)
	send := func(m rwire.FromRunner) {
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		m.Used, m.Total, _ = s.drv.Capacity(cctx) // best-effort; piggybacked on every message
		cancel()
		select {
		case out <- m:
		default: // drop under absurd backlog; a later announce restores truth
		}
	}
	// OnEvent must be swapped atomically per connection and cleared on exit,
	// or a dead conn's closure keeps receiving events (and writing to an out
	// channel nothing drains any more).
	s.OnEvent = func(id, state string) { send(rwire.FromRunner{Type: "event", Session: id, State: state}) }
	defer func() { s.OnEvent = nil }()

	used, total, _ := s.drv.Capacity(ctx)
	ann := rwire.FromRunner{Type: "announce", Proto: rwire.Proto, Runner: cfg.RunnerName,
		Sessions: s.Announce(), Used: used, Total: total}
	if err := wsjson.Write(ctx, c, ann); err != nil {
		return err
	}

	writeDone := make(chan error, 1)
	go func() { // single writer: every FromRunner goes out over this one goroutine
		for m := range out {
			if err := wsjson.Write(ctx, c, m); err != nil {
				writeDone <- err
				return
			}
		}
	}()

	for {
		var m rwire.ToRunner
		if err := wsjson.Read(ctx, c, &m); err != nil {
			return err
		}
		go s.execute(ctx, m, send, cfg) // ops are slow (docker); never block the reader
	}
}

// execute runs one ToRunner command and reports its result via send. Called
// in its own goroutine per inbound message (agentSession's read loop) so a
// slow docker op never blocks the next command from being read.
func (s *Server) execute(ctx context.Context, m rwire.ToRunner, send func(rwire.FromRunner), cfg AgentConfig) {
	switch m.Type {
	case "create":
		// Idempotency first: an id already in the registry means a prior
		// create already landed (or is mid-flight — CreateWithID's own
		// "starting" entry counts). controld may resend a create it's
		// unsure reached us (e.g. after its own reconnect); re-running
		// drv.Create against an id that already has a container would be
		// wrong, not merely redundant.
		if _, ok := s.reg.get(m.Session); ok {
			send(rwire.FromRunner{Type: "result", ReqID: m.ReqID, OK: true})
			return
		}
		var spec driver.Spec
		var allow []string
		if m.Spec != nil {
			spec = driver.Spec{Name: m.Spec.Name, Image: m.Spec.Image, Cmd: m.Spec.Cmd, EgressAllow: m.Spec.EgressAllow}
			allow = m.Spec.EgressAllow
		}
		err := s.CreateWithID(ctx, m.Session, spec, allow)
		send(rwire.FromRunner{Type: "result", ReqID: m.ReqID, OK: err == nil, Detail: errText(err)})
	case "suspend", "resume", "snapshot":
		ref, err := s.Op(ctx, m.Session, m.Type, m.Warm)
		detail := ref
		if err != nil {
			detail = err.Error()
		}
		send(rwire.FromRunner{Type: "result", ReqID: m.ReqID, OK: err == nil, Detail: detail})
	case "destroy":
		err := s.Delete(ctx, m.Session)
		// A missing session is still ok: the desired end state (no session)
		// is already reached, whether we just deleted it or this destroy
		// simply arrived after some other path already had.
		ok := err == nil || errors.Is(err, errNoSuchSession)
		send(rwire.FromRunner{Type: "result", ReqID: m.ReqID, OK: ok, Detail: errTextUnless(err, errNoSuchSession)})
	case "dial_attach":
		// Implemented in a later task; log-and-ignore rather than a silent
		// drop that would look identical to lost network to someone
		// debugging a stuck attach.
		log.Printf("agent: dial_attach not yet implemented (session %s); ignoring", m.Session)
	default:
		log.Printf("agent: unknown command type %q", m.Type)
	}
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// errTextUnless is errText, but also suppresses the message for an error
// matching sentinel (used by destroy: errNoSuchSession is a success case,
// not a failure detail worth surfacing).
func errTextUnless(err, sentinel error) string {
	if err == nil || errors.Is(err, sentinel) {
		return ""
	}
	return err.Error()
}

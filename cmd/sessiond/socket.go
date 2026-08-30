package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"
)

// The agent socket is how a process INSIDE the sandbox reaches sessiond: the
// git credential helper is invoked by git itself, with no relay conn of its
// own, and needs an answer only controld can give. It dials this socket,
// sessiond forwards the request upstream as a session RPC, and the answer
// comes back the same way.
//
// The protocol is deliberately the smallest thing that works: one JSON request
// object per connection, one JSON response, then close. No framing, no
// multiplexing, no state — a helper runs once per git operation and exits, so
// a connection that outlives one exchange has nothing left to do.
const (
	// agentSocketPath is inside the session's own workspace volume, the one
	// writable persistent path a session has. Task 7's helper hard-codes the
	// same path; it is part of the in-sandbox contract, not a preference.
	agentSocketPath = "/workspace/.rainier/agent.sock"
	// agentSocketDeadline bounds each end of one exchange: how long a
	// connected client may take to send its request, and how long the write of
	// the response may take. Nothing here is long-lived — the helper writes
	// immediately or it is broken — and an unbounded read would let a stuck
	// client hold a goroutine for the life of the session.
	agentSocketDeadline = 5 * time.Second
	// agentSocketCallTimeout bounds the upstream RPC one request turns into.
	// It matches the helper's own timeout (Task 7): the helper is the only
	// caller, and a bound longer than its would just leave sessiond talking to
	// a client that has already given up.
	agentSocketCallTimeout = 20 * time.Second
)

// socketRequest is one call from inside the sandbox. Method names the session
// RPC to make; Payload is its body, forwarded verbatim.
type socketRequest struct {
	Method  string          `json:"method"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// socketResponse is its answer. OK is the verdict and Error carries the
// upstream refusal's own words when it is false — the credential helper prints
// that on stderr, where git shows it to the user.
type socketResponse struct {
	OK      bool            `json:"ok"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Error   string          `json:"error,omitempty"`
}

// agentSocket serves the protocol above. call performs one request (in
// production: the upstream session RPC).
type agentSocket struct {
	call     func(method string, payload json.RawMessage) (json.RawMessage, error)
	deadline time.Duration
}

// listenAgentSocket binds the unix socket at path, replacing whatever is
// already there.
//
// Removing the stale one is not housekeeping: /workspace is a persistent
// volume, so a cold-parked session that starts again finds the PREVIOUS boot's
// socket file sitting in it, and bind would fail with "address already in use"
// — leaving the credential helper with nothing to talk to for the whole life
// of the session.
//
// The socket is chmod'ed 0700 after bind: only the session's own user asks for
// its credentials. There is a sliver between bind and chmod where the mode is
// the umask's; the directory above it is the session's own workspace, and
// everything in the container runs as the same user, so the window is closed
// by the surroundings rather than by racing them.
func listenAgentSocket(path string) (net.Listener, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o700); err != nil {
		ln.Close()
		return nil, err
	}
	return ln, nil
}

// serve accepts connections until ctx ends or the listener fails. Each
// connection is served on its own goroutine: they are independent one-shot
// exchanges, and the upstream call inside one takes as long as controld does.
func (a *agentSocket) serve(ctx context.Context, ln net.Listener) {
	go func() {
		<-ctx.Done()
		ln.Close() // unblocks Accept below
	}()
	for {
		c, err := ln.Accept()
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("agent socket: accept: %v", err)
			}
			return
		}
		go a.serveConn(c)
	}
}

// serveConn runs one exchange. Every failure — a request that never arrives, a
// malformed one, a refusal upstream — ends with the connection closed, and
// wherever there is something to say, it is said in the response first: a
// helper that gets a closed socket can only report "sessiond is broken", while
// one that gets a message can print the actual reason.
func (a *agentSocket) serveConn(c net.Conn) {
	defer c.Close()

	// The read deadline covers a client that connects and then says nothing.
	// It is cleared before the upstream call, which carries its own (longer)
	// bound: a mint that legitimately takes ten seconds must not be cut off by
	// the deadline that exists to catch a stalled client.
	c.SetReadDeadline(time.Now().Add(a.deadline))
	var req socketRequest
	if err := json.NewDecoder(c).Decode(&req); err != nil {
		log.Printf("agent socket: reading a request: %v", err)
		return
	}
	c.SetReadDeadline(time.Time{})

	resp := socketResponse{}
	switch {
	case req.Method == "":
		resp.Error = "the request named no method"
	default:
		out, err := a.call(req.Method, req.Payload)
		if err != nil {
			resp.Error = err.Error()
		} else {
			resp.OK, resp.Payload = true, out
		}
	}

	c.SetWriteDeadline(time.Now().Add(a.deadline))
	if err := json.NewEncoder(c).Encode(resp); err != nil {
		log.Printf("agent socket: answering %s: %v", req.Method, err)
	}
}

// startAgentSocket opens the in-sandbox socket and serves it in the
// background, forwarding every request upstream as a session RPC.
//
// A failure to open it is logged, not fatal. Plenty of sessions never need it
// (a scratch session with no repos never runs a git helper), and killing a
// session that is otherwise perfectly serviceable over a socket nobody may ask
// for is the wrong trade — the sessions that DO need it fail loudly at their
// first git operation, with this log line waiting in the terminal.
func startAgentSocket(ctx context.Context, path string, d *rpcDispatcher) {
	ln, err := listenAgentSocket(path)
	if err != nil {
		log.Printf("agent socket: %v; in-sandbox RPC is unavailable for this boot", err)
		return
	}
	log.Printf("agent socket listening on %s", path)
	a := &agentSocket{
		deadline: agentSocketDeadline,
		call: func(method string, payload json.RawMessage) (json.RawMessage, error) {
			return d.Call(method, payload, agentSocketCallTimeout)
		},
	}
	go a.serve(ctx, ln)
}

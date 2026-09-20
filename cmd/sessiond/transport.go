package main

import (
	"context"
	"fmt"
	"os"

	"github.com/coder/websocket"

	"github.com/tokencanopy/rainier/internal/relay"
)

// The transport seam: how this sessiond reaches the runner holding it.
//
// There are two, and everything above them is identical. A Docker session
// dials a WebSocket over the container network and asserts its session id in
// a query parameter; a microVM session opens an AF_VSOCK connection to the
// host, which Firecracker forwards to a socket inside that VM's own
// directory — so the id is the host's statement rather than the guest's
// claim, and the channel works before the guest's network does.
//
// relay.ServeSessionWithExec, the RPC dispatcher and the agent sync are
// untouched by the difference, because every one of them already takes a
// relay.Conn.

const (
	// transportWebSocket is the default and the compatibility floor: every
	// session that exists today, and every Docker session there will ever be.
	transportWebSocket = "websocket"
	// transportVsock is the microVM path. It is a flag (or RAINIER_TRANSPORT)
	// rather than something inferred, and it comes from the session IMAGE
	// rather than from a create: a microVM guest receives no environment at
	// all until its boot configuration arrives, so there is nothing per-session
	// for it to be inferred from. Which transport a guest has is a property of
	// the rootfs it booted, which is exactly where the flag is set.
	transportVsock = "vsock"
)

// dialSession opens one connection to the runner. It is the whole seam: a
// function of a context, because that is all either transport needs.
type dialSession func(context.Context) (relay.Conn, error)

// websocketTransport is the Docker path, byte for byte what dialLoop did
// before the seam existed: the same URL, the same nil options, the same read
// limit. A session keeps the sessiond it booted with for life, so this hop
// is a contract with images months old and is not a place to tidy anything.
func websocketTransport(dial, sessionID string) dialSession {
	return func(ctx context.Context) (relay.Conn, error) {
		c, _, err := websocket.Dial(ctx, dial+"?session="+sessionID, nil)
		if err != nil {
			return nil, err
		}
		c.SetReadLimit(16 << 20)
		return relay.WSConn(c), nil
	}
}

// vsockTransport is the microVM path: AF_VSOCK to (HOST_CID, 1024).
//
// One port, because internal/relay already multiplexes the configuration,
// the terminal stream, the session RPC and the lifecycle handshake over one
// conn. The host never dials in; the guest opens this and everything rides
// it.
func vsockTransport() dialSession {
	return func(ctx context.Context) (relay.Conn, error) {
		c, err := dialVsock(ctx, vsockHostCID, vsockControlPort)
		if err != nil {
			return nil, fmt.Errorf("dial the host control channel over vsock: %w", err)
		}
		return relay.NetConn(c), nil
	}
}

// envOr reads a variable, falling back to def when it is unset or empty.
// It is how --transport takes its default from the session IMAGE: a microVM
// guest has no per-session environment to be configured from, but it does
// have the rootfs it booted, and that is the right place for a fact about
// which transport this guest has.
func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

const (
	// vsockHostCID is HOST_CID: the host's context id is always 2.
	vsockHostCID = 2
	// vsockControlPort is the host port the runner listens on. It matches
	// the driver's guestControlPort, and the two are the same wire fact
	// spelled in two binaries that ship in different artifacts.
	vsockControlPort = 1024
)

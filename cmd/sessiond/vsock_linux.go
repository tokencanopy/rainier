//go:build linux

package main

import (
	"context"
	"fmt"
	"net"

	"github.com/mdlayher/socket"
	"golang.org/x/sys/unix"
)

// dialVsock opens an AF_VSOCK stream to (cid, port). net.FileConn cannot
// adopt this address family. socket.Conn registers the descriptor with Go's
// poller without net's address-family conversion and supports cancellation
// during Connect as well as the I/O deadlines relay.NetConn requires.
func dialVsock(ctx context.Context, cid, port uint32) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c, err := socket.Socket(unix.AF_VSOCK, unix.SOCK_STREAM, 0, "vsock", nil)
	if err != nil {
		return nil, fmt.Errorf("vsock socket: %w", err)
	}
	ok := false
	defer func() {
		if !ok {
			_ = c.Close()
		}
	}()
	remote := vsockAddr{cid, port}
	if _, err := c.Connect(ctx, &unix.SockaddrVM{CID: cid, Port: port}); err != nil {
		return nil, fmt.Errorf("vsock connect to (%d,%d): %w", cid, port, err)
	}
	sa, err := c.Getsockname()
	if err != nil {
		return nil, fmt.Errorf("vsock local address: %w", err)
	}
	local, valid := sa.(*unix.SockaddrVM)
	if !valid {
		return nil, fmt.Errorf("vsock local address has unexpected type %T", sa)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ok = true
	return &vsockConn{Conn: c, local: vsockAddr{local.CID, local.Port}, remote: remote}, nil
}

type vsockAddr struct{ cid, port uint32 }

func (a vsockAddr) Network() string { return "vsock" }
func (a vsockAddr) String() string  { return fmt.Sprintf("%d:%d", a.cid, a.port) }

// Only address reporting is ours; the socket library owns descriptor lifetime,
// concurrent I/O, EOF handling and deadlines.
type vsockConn struct {
	*socket.Conn
	local, remote vsockAddr
}

func (c *vsockConn) LocalAddr() net.Addr  { return c.local }
func (c *vsockConn) RemoteAddr() net.Addr { return c.remote }

var _ net.Conn = (*vsockConn)(nil)

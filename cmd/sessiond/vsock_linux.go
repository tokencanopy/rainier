//go:build linux

package main

import (
	"context"
	"fmt"
	"net"
	"os"

	"golang.org/x/sys/unix"
)

// dialVsock opens an AF_VSOCK stream to (cid, port).
//
// There is no net.Dial("vsock", …): the address family is not in the
// standard library, so the socket is made by hand and handed to net.FileConn,
// which gives back an ordinary net.Conn with the runtime's poller behind it
// — deadlines, cancellation and all, which relay.NetConn relies on.
//
// The guest needs CONFIG_VIRTIO_VSOCKETS and /dev/vsock for this to work at
// all, and whether the session image's kernel has them is one of the things
// only a real host can answer (design note §7).
func dialVsock(ctx context.Context, cid, port uint32) (net.Conn, error) {
	fd, err := unix.Socket(unix.AF_VSOCK, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("vsock socket: %w", err)
	}
	// From here on the fd is owned by this function until os.NewFile takes
	// it: every failure below closes it, or a guest that retries its dial
	// leaks one per attempt for the life of the session.
	if err := unix.Connect(fd, &unix.SockaddrVM{CID: cid, Port: port}); err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("vsock connect to (%d,%d): %w", cid, port, err)
	}
	f := os.NewFile(uintptr(fd), fmt.Sprintf("vsock:%d:%d", cid, port))
	if f == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("vsock: fd %d is not a file", fd)
	}
	// FileConn DUPLICATES the descriptor, so the original is closed here
	// whether it succeeded or not.
	conn, err := net.FileConn(f)
	_ = f.Close()
	if err != nil {
		return nil, fmt.Errorf("vsock: adopting the connection: %w", err)
	}
	// A context that is already done must not leave a live connection
	// behind; there is nothing to cancel mid-connect, because unix.Connect
	// on a blocking socket does not take one.
	if err := ctx.Err(); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

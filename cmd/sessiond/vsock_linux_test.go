//go:build linux

package main

import (
	"context"
	"errors"
	"github.com/mdlayher/socket"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"testing"
	"time"
)

func TestDialVsockCanceledBeforeSocket(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	conn, err := dialVsock(ctx, 2, 1024)
	if conn != nil {
		conn.Close()
		t.Fatal("canceled dial returned a connection")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled dial = %v, want context.Canceled", err)
	}
}

// An actual socket pair exercises the descriptor and poller, without requiring
// a hypervisor. The opt-in KVM lifecycle test covers the AF_VSOCK dial itself.
func TestVsockConnStreamAndDeadlines(t *testing.T) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK, 0)
	if err != nil {
		t.Fatal(err)
	}
	left, err := socket.New(fds[0], "vsock-test-left")
	if err != nil {
		unix.Close(fds[1])
		t.Fatal(err)
	}
	defer left.Close()
	right, err := socket.New(fds[1], "vsock-test-right")
	if err != nil {
		t.Fatal(err)
	}
	defer right.Close()
	c := &vsockConn{Conn: left, local: vsockAddr{3, 10000}, remote: vsockAddr{2, 1024}}
	if c.LocalAddr().String() != "3:10000" || c.RemoteAddr().String() != "2:1024" || c.RemoteAddr().Network() != "vsock" {
		t.Fatal("incorrect address")
	}
	if err := c.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 4)
	if _, err := io.ReadFull(right, b); err != nil || string(b) != "ping" {
		t.Fatalf("read=%q err=%v", b, err)
	}
	if _, err := right.Write([]byte("pong")); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(c, b); err != nil || string(b) != "pong" {
		t.Fatalf("reply=%q err=%v", b, err)
	}
	if err := c.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Read(b); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("deadline read=%v", err)
	}
	if err := c.SetDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	right.Close()
	if _, err := c.Read(b); !errors.Is(err, io.EOF) {
		t.Fatalf("peer close=%v", err)
	}
	c.Close()
	if _, err := c.Write(b); err == nil {
		t.Fatal("write after close succeeded")
	}
}

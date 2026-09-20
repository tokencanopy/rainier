// internal/relay/netconn.go
package relay

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// NetConn adapts a byte STREAM to Conn, which is a message interface.
//
// It exists for virtio-vsock, the single host-to-guest control channel a
// microVM session has (ADR-0003 §2.7 item 2): a vsock connection is an
// ordinary AF_VSOCK stream on the guest side and an AF_UNIX stream on the
// host side, with no framing of its own, where the WebSocket this package
// grew up on delivered whole messages. Everything above Conn — the hub, the
// session side, the control channel, the RPC dispatcher — is unchanged by
// this, which is the whole reason the seam is at Conn.
//
// The framing is one JSON value per LINE. It is chosen rather than a length
// prefix because every message on this conn is already a JSON object produced
// by encoding/json, which never emits a bare newline inside one: a framing
// that needs no escaping and no second encoder cannot disagree with the
// encoder above it. maxFrameBytes bounds a line, so a peer cannot make this
// end allocate without limit by never sending one.
//
// Writes are serialized. That is not belt-and-braces: a Hub writes from the
// goroutine of every attached client AND from SendControl, and two concurrent
// Write calls on a stream interleave the bytes of two frames and corrupt it
// permanently for the reader. A *websocket.Conn serializes internally and a
// net.Conn does not, so the discipline has to be here.
func NetConn(c net.Conn) Conn {
	return &netConn{
		c: c,
		// The same 16 MiB bound both WebSocket ends set with SetReadLimit, so
		// a frame that crosses this transport and a frame that crosses that
		// one are the same size of thing.
		r: bufio.NewReaderSize(c, netConnReadBuffer),
	}
}

const (
	// netConnReadBuffer is the reader's initial buffer. A control frame is
	// a few hundred bytes and a terminal frame a few KiB; the buffer grows
	// for the rare large one rather than being sized for it.
	netConnReadBuffer = 64 << 10
	// maxFrameBytes is the largest single message this transport will
	// assemble, matching the WebSocket ends' SetReadLimit.
	maxFrameBytes = 16 << 20
)

type netConn struct {
	c net.Conn
	r *bufio.Reader

	// wmu serializes writers — see NetConn's doc comment.
	wmu sync.Mutex
}

// ErrFrameTooLarge is returned when a peer sends a line longer than this
// transport will assemble. It ends the connection rather than skipping the
// frame: a reader that resynchronized mid-stream would hand the decoder the
// tail of one message as though it were a whole one.
var ErrFrameTooLarge = errors.New("relay: frame over the transport's size limit")

func (n *netConn) Read(ctx context.Context) ([]byte, error) {
	// A cancelled ctx unblocks the read by poisoning the conn's deadline,
	// which is the only thing that can interrupt a blocking stream read. It
	// is one-shot by design: a cancelled context means this conn is over, and
	// AfterFunc costs nothing on the path where it never fires.
	if ctx.Done() != nil {
		stop := context.AfterFunc(ctx, func() { _ = n.c.SetReadDeadline(time.Now()) })
		defer stop()
	}
	var line []byte
	for {
		chunk, err := n.r.ReadSlice('\n')
		if len(line)+len(chunk) > maxFrameBytes {
			return nil, ErrFrameTooLarge
		}
		line = append(line, chunk...)
		if err == nil {
			break
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if len(line) > 0 && errors.Is(err, net.ErrClosed) {
			// A partial line at a closed conn is not a message.
			return nil, err
		}
		return nil, err
	}
	// A JSON value never contains a bare newline or carriage return, so
	// trimming them is free and makes the framing tolerant of a peer that
	// writes CRLF.
	return bytes.TrimRight(line[:len(line)-1], "\r"), nil
}

func (n *netConn) Write(ctx context.Context, b []byte) error {
	if len(b) > maxFrameBytes {
		return fmt.Errorf("%w: %d bytes", ErrFrameTooLarge, len(b))
	}
	if bytes.ContainsRune(b, '\n') {
		// Unreachable for anything encoding/json produced, and a hard error
		// rather than an escape: a message with a newline in it would frame
		// as two, and the second would be garbage the peer could not decode
		// and could not resynchronize from.
		return errors.New("relay: a frame containing a newline cannot be framed by line")
	}
	if ctx.Done() != nil {
		stop := context.AfterFunc(ctx, func() { _ = n.c.SetWriteDeadline(time.Now()) })
		defer stop()
	}
	n.wmu.Lock()
	defer n.wmu.Unlock()
	// One Write call, not two: a frame and its terminator must not be
	// separable by a concurrent writer or by a short write in between.
	out := make([]byte, 0, len(b)+1)
	out = append(out, b...)
	out = append(out, '\n')
	_, err := n.c.Write(out)
	return err
}

func (n *netConn) Close() error { return n.c.Close() }

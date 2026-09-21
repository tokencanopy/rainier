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
	// netConnReadBuffer is the reader's buffer, and it is FIXED —
	// bufio.Reader never grows one. A frame larger than it is assembled a
	// bufferful at a time by the ErrBufferFull loop in Read, which is what
	// keeps a 64 KiB buffer from being a 64 KiB frame limit. A control frame
	// is a few hundred bytes and a terminal frame a few KiB, so the common
	// case takes one pass.
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
	// A cancelled context CLOSES the conn, which is the only thing that can
	// interrupt a blocking stream read — and it is deliberately the same
	// answer *websocket.Conn gives, rather than merely poisoning a deadline.
	//
	// The difference matters at one caller. connWriter.writeWithin bounds an
	// exec frame's write and documents the transport's answer to an expired
	// write context as closing the conn, "because closing it is what makes
	// sessiond redial". A transport that instead left the conn readable and
	// unwritable would leave a session whose output and RPC were silently
	// dead upward while both ends still believed the conn was live — which
	// is worse than either failing or working.
	//
	// It follows that no caller may bound a read or a write on a conn it
	// wants to keep. cmd/sessiond's boot preamble is the one that tries, and
	// it closes and re-dials rather than handing on a conn it may have
	// broken.
	if ctx.Done() != nil {
		stop := context.AfterFunc(ctx, func() { _ = n.c.Close() })
		defer stop()
	}
	var line []byte
	for {
		chunk, err := n.r.ReadSlice('\n')
		// The terminator is not part of the message, so a frame of exactly
		// maxFrameBytes is readable: the budget is the message's, and the
		// one byte of framing is this transport's own.
		if len(line)+len(chunk) > maxFrameBytes+1 {
			return nil, ErrFrameTooLarge
		}
		line = append(line, chunk...)
		if err == nil {
			break
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		// Anything else ends the read, whatever was accumulated: a line with
		// no terminator is not a message, and handing half of one to the
		// decoder would be worse than reporting the conn's own error.
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
	n.wmu.Lock()
	defer n.wmu.Unlock()
	// Same answer as Read's, and for the same reason: a cancelled write
	// context closes the conn, which is what *websocket.Conn does and what
	// connWriter.writeWithin's own doc comment relies on.
	if ctx.Done() != nil {
		stop := context.AfterFunc(ctx, func() { _ = n.c.Close() })
		defer stop()
	}
	// One Write call, not two: a frame and its terminator must not be
	// separable by a concurrent writer or by a short write in between.
	out := make([]byte, 0, len(b)+1)
	out = append(out, b...)
	out = append(out, '\n')
	_, err := n.c.Write(out)
	return err
}

func (n *netConn) Close() error { return n.c.Close() }

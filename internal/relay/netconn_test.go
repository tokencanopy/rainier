package relay

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
)

// TestNetConnRoundTripsFrames pins that a stream transport delivers the same
// MESSAGES a WebSocket one does: what goes in as one Write comes out as one
// Read, whatever the stream does with the boundaries in between.
func TestNetConnRoundTripsFrames(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	left, right := NetConn(a), NetConn(b)
	ctx := context.Background()

	frames := [][]byte{
		[]byte(`{"t":4,"a":0,"p":"eyJraW5kIjoiYm9vdF9jb25maWcifQ=="}`),
		[]byte(`{"t":3,"a":7,"p":"` + strings.Repeat("A", 200<<10) + `"}`),
		[]byte(`{"t":1,"a":7}`),
	}
	go func() {
		for _, f := range frames {
			if err := left.Write(ctx, f); err != nil {
				return
			}
		}
	}()
	for i, want := range frames {
		got, err := right.Read(ctx)
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("frame %d: got %d bytes, want %d", i, len(got), len(want))
		}
	}
}

// TestNetConnSerializesConcurrentWrites is the property a raw stream needs
// and a WebSocket supplies for free. A Hub writes from the goroutine of every
// attached client and from SendControl; two interleaved writes would corrupt
// the stream permanently for the reader, so every frame must arrive whole.
func TestNetConnSerializesConcurrentWrites(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	left, right := NetConn(a), NetConn(b)
	ctx := context.Background()

	const writers, each = 8, 16
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			payload := []byte(strings.Repeat(string(rune('a'+w)), 32<<10))
			for i := 0; i < each; i++ {
				if err := left.Write(ctx, payload); err != nil {
					return
				}
			}
		}(w)
	}
	go func() { wg.Wait(); a.Close() }()

	for n := 0; n < writers*each; n++ {
		got, err := right.Read(ctx)
		if err != nil {
			t.Fatalf("frame %d: %v", n, err)
		}
		if len(got) != 32<<10 {
			t.Fatalf("frame %d is %d bytes: two writes interleaved", n, len(got))
		}
		if bytes.Count(got, got[:1]) != len(got) {
			t.Fatalf("frame %d mixes two writers' bytes", n)
		}
	}
}

// TestNetConnRefusesAnUnframeableWrite pins the one shape this framing cannot
// carry. It is unreachable for anything encoding/json produces, and it is an
// error rather than an escape because a silently split frame would leave the
// peer decoding garbage it could never resynchronize from.
func TestNetConnRefusesAnUnframeableWrite(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	if err := NetConn(a).Write(context.Background(), []byte("one\ntwo")); err == nil {
		t.Fatal("a frame containing a newline was accepted")
	}
	_ = b
}

// TestNetConnReadEndsOnACancelledContext pins that a blocking stream read is
// interruptible, which is what a Hub whose context ends relies on.
func TestNetConnReadEndsOnACancelledContext(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := NetConn(a).Read(ctx)
		done <- err
	}()
	cancel()
	if err := <-done; err == nil {
		t.Fatal("a read on a cancelled context returned no error")
	}
}

// TestNetConnRefusesAnOversizedFrame pins the bound: a peer cannot make this
// end assemble an unbounded line.
func TestNetConnRefusesAnOversizedFrame(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	go func() {
		// Written straight to the stream, bypassing the writer's own bound,
		// because it is the READER's bound this pins.
		chunk := bytes.Repeat([]byte("x"), 1<<20)
		for i := 0; i < 20; i++ {
			if _, err := a.Write(chunk); err != nil {
				return
			}
		}
	}()
	if _, err := NetConn(b).Read(context.Background()); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("an oversized frame gave %v, want ErrFrameTooLarge", err)
	}
}

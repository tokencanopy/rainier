// Command rattach is the dev attach client: it dials sessiond's /attach
// websocket, puts the local terminal in raw mode, and pipes stdin/stdout
// through. Ctrl-] detaches without touching the remote session.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"golang.org/x/term"

	"rainier/internal/wire"
)

const detachKey = 0x1d // Ctrl-]

func main() {
	url := flag.String("url", "ws://127.0.0.1:7070", "sessiond base URL")
	since := flag.Uint64("since", 0, "resume from sequence number")
	flag.Parse()

	ctx := context.Background()
	c, _, err := websocket.Dial(ctx, fmt.Sprintf("%s/attach?since=%d", *url, *since), nil)
	if err != nil {
		log.Fatal(err)
	}
	defer c.CloseNow()

	fd := int(os.Stdin.Fd())
	isTTY := term.IsTerminal(fd)

	// restore is a no-op unless stdin is a real tty. Raw mode, and the
	// SIGWINCH-driven size reporting that depends on term.GetSize working,
	// are both skipped for non-tty stdin (e.g. a script piping input in) —
	// there is no terminal state to save or restore in that case.
	restore := func() {}
	if isTTY {
		oldState, err := term.MakeRaw(fd)
		if err != nil {
			log.Fatal(err)
		}
		restore = func() { term.Restore(fd, oldState) }
	}
	defer restore()

	if isTTY {
		sendSize := func() {
			w, h, err := term.GetSize(fd)
			if err == nil {
				wsjson.Write(ctx, c, wire.ClientMsg{Type: "resize", Cols: w, Rows: h})
			}
		}
		sendSize() // required first message

		winch := make(chan os.Signal, 1)
		signal.Notify(winch, syscall.SIGWINCH)
		go func() {
			for range winch {
				sendSize()
			}
		}()
	} else {
		// No tty to size from: announce a fixed default so the server's
		// resize-first contract is still satisfied.
		wsjson.Write(ctx, c, wire.ClientMsg{Type: "resize", Cols: 80, Rows: 24})
	}

	var lastSeq uint64
	go func() {
		for {
			var m wire.ServerMsg
			if err := wsjson.Read(ctx, c, &m); err != nil {
				restore()
				fmt.Printf("\r\n[disconnected at seq %d; rattach --since %d to resume]\r\n", lastSeq, lastSeq)
				os.Exit(0)
			}
			switch m.Type {
			case "snapshot", "output":
				os.Stdout.Write(m.Data)
				if m.Seq > 0 {
					lastSeq = m.Seq
				}
			case "exit":
				restore()
				fmt.Printf("\r\n[session process exited: %d]\r\n", m.ExitCode)
				os.Exit(0)
			}
		}
	}()

	buf := make([]byte, 1024)
	for {
		n, err := os.Stdin.Read(buf)
		if err != nil {
			// Stdin closed (EOF on a pipe, or a read error on a real tty).
			// There's nothing left to forward, but the session may still
			// have output in flight, so block here instead of returning:
			// the goroutine above is what ends the process, on disconnect
			// or session exit — or the process gets killed externally.
			select {}
		}
		for i := 0; i < n; i++ {
			if buf[i] == detachKey {
				restore()
				fmt.Printf("\r\n[detached at seq %d; session still running]\r\n", lastSeq)
				return
			}
		}
		wsjson.Write(ctx, c, wire.ClientMsg{Type: "stdin", Data: append([]byte(nil), buf[:n]...)})
	}
}

// Command vtcap records a program's raw PTY output to a fixture file. Run real TUIs
// (claude, htop, vim) and press keys yourself; Ctrl-C stops recording.
package main

import (
	"flag"
	"log"
	"os"
	"sync"

	"golang.org/x/term"

	"rainier/internal/session"
)

func main() {
	out := flag.String("out", "", "output fixture path")
	flag.Parse()
	if *out == "" || len(flag.Args()) == 0 {
		log.Fatal("usage: vtcap --out testdata/vt/name.input -- <command...>")
	}
	f, err := os.Create(*out)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// Raw mode only makes sense when stdin is a real tty (interactive keys
	// need to reach the child unbuffered and un-echoed); when stdin is a
	// pipe or file (headless/scripted recording), leave it alone — there is
	// no terminal state to save or restore. Same guard as cmd/rattach.
	fd := int(os.Stdin.Fd())
	restore := func() {}
	if term.IsTerminal(fd) {
		oldState, err := term.MakeRaw(fd)
		if err != nil {
			log.Fatal(err)
		}
		restore = func() { term.Restore(fd, oldState) }
	}
	defer restore()

	var mu sync.Mutex
	p, err := session.StartProc(flag.Args(), 120, 32, func(b []byte) {
		mu.Lock()
		f.Write(b)
		os.Stdout.Write(b) // mirror so the operator sees the TUI
		mu.Unlock()
	})
	if err != nil {
		restore()
		log.Fatal(err)
	}
	go func() {
		buf := make([]byte, 1024)
		for {
			n, err := os.Stdin.Read(buf)
			if err != nil {
				return
			}
			p.Write(buf[:n])
		}
	}()
	code := p.Wait()
	restore()
	log.Printf("recorded to %s (exit %d)", *out, code)
}

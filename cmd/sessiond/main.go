package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"rainier/internal/reap"
	"rainier/internal/server"
	"rainier/internal/session"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:7070", "dev listener address")
	logPath := flag.String("log", "/tmp/session.log", "event log path")
	cols := flag.Int("cols", 120, "initial cols")
	rows := flag.Int("rows", 32, "initial rows")
	flag.Parse()
	argv := flag.Args()
	if len(argv) == 0 {
		log.Fatal("usage: sessiond [flags] -- <command> [args...]")
	}
	s, err := session.New(session.Config{Argv: argv, Cols: *cols, Rows: *rows, LogPath: *logPath}, session.StartProc)
	if err != nil { log.Fatal(err) }
	go func() {
		<-s.Exited()
		log.Printf("child exited with code %d; sessiond stays up for viewers", s.ExitCode())
	}()

	reap.Start() // single authoritative waiter on Linux; agent exit code flows back through Proc.Wait via Session

	term := make(chan os.Signal, 1)
	signal.Notify(term, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-term
		// Graceful: ask the agent to exit; the exit path closes viewers and the
		// process ends when the child is reaped. Give it a moment, then hard-exit.
		s.Stop()
		select {
		case <-s.Exited():
		case <-time.After(5 * time.Second):
		}
		os.Exit(0)
	}()
	log.Printf("sessiond listening on %s", *listen)
	if err := http.ListenAndServe(*listen, server.New(s)); err != nil {
		log.Fatal(err)
	}
	_ = os.Stdout
}

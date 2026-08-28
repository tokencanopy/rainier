// internal/session/session.go
// Session wires a PTY process to the emulator, the event log, and viewer
// fan-out. The session never depends on any viewer connection.
package session

import (
	"sync"

	"rainier/internal/eventlog"
	"rainier/internal/term"
	"rainier/internal/wire"
)

type Config struct {
	Argv       []string
	Cols, Rows int
	LogPath    string
}

type viewer struct {
	id   int
	ch   chan wire.ServerMsg
	size Size
}

type Session struct {
	mu      sync.Mutex
	emu     term.Emulator
	log     *eventlog.Log
	proc    Proc
	viewers map[int]*viewer
	nextID  int
	size    Size
	exited  chan struct{}
	exitC   int
}

func New(cfg Config, start func(argv []string, cols, rows int, onOutput func([]byte)) (Proc, error)) (*Session, error) {
	lg, err := eventlog.Open(cfg.LogPath)
	if err != nil { return nil, err }
	s := &Session{
		emu:     term.NewEmulator(cfg.Cols, cfg.Rows),
		log:     lg,
		viewers: map[int]*viewer{},
		size:    Size{cfg.Cols, cfg.Rows},
		exited:  make(chan struct{}),
	}
	p, err := start(cfg.Argv, cfg.Cols, cfg.Rows, s.onOutput)
	if err != nil { lg.Close(); return nil, err }
	s.proc = p
	go func() {
		code := p.Wait()
		s.mu.Lock()
		s.exitC = code
		for _, v := range s.viewers { s.trySend(v, wire.ServerMsg{Type: "exit", ExitCode: code}) }
		s.mu.Unlock()
		close(s.exited)
	}()
	return s, nil
}

func (s *Session) onOutput(b []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.emu.Feed(b)
	seq, _ := s.log.Append("output", b)
	msg := wire.ServerMsg{Type: "output", Seq: seq, Data: append([]byte(nil), b...)}
	for _, v := range s.viewers { s.trySend(v, msg) }
}

// trySend enforces the slow-consumer policy: overflow force-detaches.
func (s *Session) trySend(v *viewer, m wire.ServerMsg) {
	select {
	case v.ch <- m:
	default:
		delete(s.viewers, v.id)
		close(v.ch)
	}
}

type Attachment struct {
	ID   int
	Msgs <-chan wire.ServerMsg
}

func (s *Session) Attach(since uint64, size Size) (*Attachment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := &viewer{id: s.nextID, ch: make(chan wire.ServerMsg, 256), size: size}
	s.nextID++
	s.viewers[v.id] = v
	if since > 0 && since <= s.log.LastSeq() {
		entries, err := s.log.Since(since)
		if err == nil {
			for _, e := range entries {
				v.ch <- wire.ServerMsg{Type: "output", Seq: e.Seq, Data: e.Data}
			}
		}
	} else {
		scr := s.emu.Screen()
		v.ch <- wire.ServerMsg{
			Type: "snapshot", Seq: s.log.LastSeq(),
			Data: term.Serialize(scr), Cols: scr.Cols, Rows: scr.Rows,
		}
	}
	s.applySizeLocked()
	return &Attachment{ID: v.id, Msgs: v.ch}, nil
}

func (s *Session) Detach(id int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.viewers[id]; ok {
		delete(s.viewers, id)
		close(v.ch)
		s.applySizeLocked()
	}
}

func (s *Session) Stdin(p []byte) { s.proc.Write(p) }

func (s *Session) SetSize(id int, size Size) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.viewers[id]; ok {
		v.size = size
		s.applySizeLocked()
	}
}

func (s *Session) applySizeLocked() {
	var sizes []Size
	for _, v := range s.viewers { sizes = append(sizes, v.size) }
	eff, ok := EffectiveSize(sizes)
	if !ok || eff == s.size { return }
	s.size = eff
	s.emu.Resize(eff.Cols, eff.Rows)
	s.proc.Resize(eff.Cols, eff.Rows)
}

func (s *Session) Exited() <-chan struct{} { return s.exited }
func (s *Session) ExitCode() int           { return s.exitC }

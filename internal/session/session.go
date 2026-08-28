// internal/session/session.go
// Session wires a PTY process to the emulator, the event log, and viewer
// fan-out. The session never depends on any viewer connection.
package session

import (
	"log"
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
		// Attachment.Msgs is documented "closed on detach/exit": finish the
		// lifecycle for every viewer still attached at exit time.
		for _, v := range s.viewers { close(v.ch) }
		s.viewers = map[int]*viewer{}
		// Close s.exited before releasing s.mu: any Attach that acquires
		// s.mu after this point is guaranteed (via the mutex) to observe it
		// closed, so its non-blocking `select` on s.exited can never take
		// the default branch for a session that has, in fact, already
		// exited — which would otherwise strand that viewer with no future
		// exit notice or channel close.
		close(s.exited)
		s.mu.Unlock()
	}()
	return s, nil
}

func (s *Session) onOutput(b []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.emu.Feed(b)
	seq, err := s.log.Append("output", b)
	if err != nil {
		// Seq 0 marks an unlogged frame: clients ignore Seq 0 as a resume
		// cursor, so a viewer that later resumes across this point simply
		// misses it. A persistent log failure therefore degrades resume
		// correctness only for the frames it drops; accepted for v0.
		log.Printf("event log append failed: %v", err)
	}
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

	replay := since > 0 && since <= s.log.LastSeq()
	var entries []eventlog.Entry
	if replay {
		var err error
		entries, err = s.log.Since(since)
		if err != nil { replay = false }
	}

	// The replay path stages len(entries) sends into the viewer's channel
	// before any caller is reading it (Attach still holds s.mu the whole
	// time). Size the channel so the entire backlog fits without blocking;
	// a fresh snapshot-only attach only ever needs the steady-state 256.
	chCap := 256
	if replay { chCap = len(entries) + 256 }
	v := &viewer{id: s.nextID, ch: make(chan wire.ServerMsg, chCap), size: size}
	s.nextID++
	s.viewers[v.id] = v

	if replay {
		for _, e := range entries {
			v.ch <- wire.ServerMsg{Type: "output", Seq: e.Seq, Data: append([]byte(nil), e.Data...)}
		}
	} else {
		scr := s.emu.Screen()
		v.ch <- wire.ServerMsg{
			Type: "snapshot", Seq: s.log.LastSeq(),
			Data: term.Serialize(scr), Cols: scr.Cols, Rows: scr.Rows,
		}
	}

	// Late attach after the child has already exited: the exit goroutine's
	// fan-out only reaches viewers that existed at exit time, so tell this
	// one directly and close out its lifecycle immediately, matching the
	// "closed on detach/exit" contract for every attach path.
	select {
	case <-s.exited:
		v.ch <- wire.ServerMsg{Type: "exit", ExitCode: s.exitC}
		delete(s.viewers, v.id)
		close(v.ch)
	default:
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

// Stop signals the agent process to terminate (SIGTERM). The normal exit
// path (close viewers, close exited) then runs. Safe to call more than once.
func (s *Session) Stop() { s.proc.Stop() }

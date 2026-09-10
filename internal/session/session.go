// internal/session/session.go
// Session wires a PTY process to the emulator, the event log, and viewer
// fan-out. The session never depends on any viewer connection.
package session

import (
	"log"
	"sync"

	"github.com/tokencanopy/rainier/internal/eventlog"
	"github.com/tokencanopy/rainier/internal/term"
	"github.com/tokencanopy/rainier/protocol/terminal"
)

type Config struct {
	Argv       []string
	Cols, Rows int
	LogPath    string
}

type viewer struct {
	id   int
	ch   chan terminal.ServerMessage
	size Size
	bind Binding
}

// Binding is what the control plane says one attachment is: whether it may
// write to the pty, and under which controller generation. It arrives with
// the attachment (a relay FrameOpen, a dial_attach) or on a later "control"
// message, and it is the only thing that decides whether a byte from that
// attachment reaches the process.
//
// The zero value is UNBOUND, and unbound means "no control plane told me
// anything", which is today's attachment: it may write. That is the
// compatibility rule for an older plane, and it is safe for the reason it is
// written down — under the old message set only one client could be sending,
// because the old plane had no way to admit a second one as anything else.
type Binding struct {
	Bound      bool
	Mode       string
	Generation uint64
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
	// controllerGen is the highest controller generation any binding has
	// told this session about. It is the session's own fence: a bound
	// attachment writes only while its binding still matches it, so a frame
	// from a controller that has since been displaced is discarded HERE,
	// where it would otherwise be typed into somebody's shell, however far
	// along the path it already was when the handoff happened.
	//
	// It only ever goes up. A late frame carrying a superseded binding
	// cannot walk it backwards.
	controllerGen uint64
}

func New(cfg Config, start func(argv []string, cols, rows int, onOutput func([]byte)) (Proc, error)) (*Session, error) {
	lg, err := eventlog.Open(cfg.LogPath)
	if err != nil {
		return nil, err
	}
	s := &Session{
		emu:     term.NewEmulator(cfg.Cols, cfg.Rows),
		log:     lg,
		viewers: map[int]*viewer{},
		size:    Size{cfg.Cols, cfg.Rows},
		exited:  make(chan struct{}),
	}
	p, err := start(cfg.Argv, cfg.Cols, cfg.Rows, s.onOutput)
	if err != nil {
		lg.Close()
		return nil, err
	}
	s.proc = p
	go func() {
		code := p.Wait()
		s.mu.Lock()
		s.exitC = code
		for _, v := range s.viewers {
			s.trySend(v, terminal.ServerMessage{Type: "exit", ExitCode: code})
		}
		// Attachment.Msgs is documented "closed on detach/exit": finish the
		// lifecycle for every viewer still attached at exit time.
		for _, v := range s.viewers {
			close(v.ch)
		}
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
	msg := terminal.ServerMessage{Type: "output", Seq: seq, Data: append([]byte(nil), b...)}
	for _, v := range s.viewers {
		s.trySend(v, msg)
	}
}

// trySend enforces the slow-consumer policy: overflow force-detaches.
func (s *Session) trySend(v *viewer, m terminal.ServerMessage) {
	select {
	case v.ch <- m:
	default:
		delete(s.viewers, v.id)
		close(v.ch)
	}
}

type Attachment struct {
	ID   int
	Msgs <-chan terminal.ServerMessage
}

// Attach adds a viewer and decides what it opens with, from its cursor:
//
//   - 0 — "I hold no cursor": a snapshot of the current screen, then live
//     output. Every plain attach, and the only shape a fresh viewer should
//     ever cost the log (spec §5: never repaint a screen by replaying raw
//     bytes).
//   - terminal.SinceAll — "the whole log": every entry from the first, then
//     live. What `rainier attach --since 0` asks for, and the only way to
//     read output that has already scrolled off the screen — a failed
//     setup's full log, an overnight session's history.
//   - anything else — a resume cursor: the entries after it, then live.
//
// The last two both fall back to the snapshot when the log cannot answer
// them (an empty log, or a cursor already past its end): a viewer must
// never open on silence with no screen and no size.
func (s *Session) Attach(since uint64, size Size, bind Binding) (*Attachment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.observeLocked(bind)

	from, all := since, since == terminal.SinceAll
	if all {
		from = 0 // every entry, since sequence numbers start at 1
	}
	replay := s.log.LastSeq() > 0 && (all || (since > 0 && since <= s.log.LastSeq()))
	var entries []eventlog.Entry
	if replay {
		var err error
		entries, err = s.log.Since(from)
		if err != nil {
			replay = false
		}
	}

	// The replay path stages len(entries) sends into the viewer's channel
	// before any caller is reading it (Attach still holds s.mu the whole
	// time). Size the channel so the entire backlog fits without blocking;
	// a fresh snapshot-only attach only ever needs the steady-state 256.
	chCap := 256
	if replay {
		chCap = len(entries) + 256
	}
	v := &viewer{id: s.nextID, ch: make(chan terminal.ServerMessage, chCap), size: size, bind: bind}
	s.nextID++
	s.viewers[v.id] = v

	if replay {
		for _, e := range entries {
			v.ch <- terminal.ServerMessage{Type: "output", Seq: e.Seq, Data: append([]byte(nil), e.Data...)}
		}
	} else {
		scr := s.emu.Screen()
		v.ch <- terminal.ServerMessage{
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
		v.ch <- terminal.ServerMessage{Type: "exit", ExitCode: s.exitC}
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

// Stdin writes p into the pty on behalf of attachment id, under the
// generation the frame was sent with, and reports whether it was executed.
//
// This is the fence, and it is here — at the process — rather than only at
// the plane, because the plane is not where input executes: a keystroke
// accepted from a controller that was displaced a moment later can already be
// past it. Everything a terminal sends goes through this one door, including
// the bytes a terminal writes back in answer to a query, so none of it needs
// a rule of its own.
func (s *Session) Stdin(id int, gen uint64, p []byte) bool {
	s.mu.Lock()
	v, ok := s.viewers[id]
	allowed := ok && s.mayWriteLocked(v, gen)
	s.mu.Unlock()
	if !allowed {
		return false
	}
	s.proc.Write(p)
	return true
}

// mayWriteLocked is the whole execution rule, in one place:
//
//   - an UNBOUND attachment writes. No control plane told this session
//     anything about it, which means an older plane, and under the old
//     message set only one client could have been sending.
//   - a BOUND attachment writes only while it is the controller AND its
//     generation is still the session's current one AND the frame was sent
//     under that same generation. A viewer never writes; a displaced
//     controller never writes again; a frame stamped with a superseded
//     generation is discarded however long it was in flight.
func (s *Session) mayWriteLocked(v *viewer, gen uint64) bool {
	if !v.bind.Bound {
		return true
	}
	return v.bind.Mode == terminal.ModeControl &&
		v.bind.Generation == s.controllerGen &&
		gen == s.controllerGen
}

// Bind installs a new binding on a live attachment — a mid-attach handoff —
// and reports whether the attachment still exists. The session's own fence
// moves with it, so a take-over installed here fences every other attachment
// before this call returns.
func (s *Session) Bind(id int, bind Binding) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.viewers[id]
	if !ok {
		return false
	}
	s.observeLocked(bind)
	v.bind = bind
	// The controller may have changed, and the pty follows the controller.
	s.applySizeLocked()
	return true
}

// observeLocked walks the session's fence up to a binding's generation. It
// never walks it back down: a frame carrying a superseded binding must not be
// able to un-displace the controller that superseded it.
func (s *Session) observeLocked(bind Binding) {
	if bind.Bound && bind.Generation > s.controllerGen {
		s.controllerGen = bind.Generation
	}
}

// SetSize records attachment id's terminal size and, if that attachment is
// entitled to move the pty, applies it. A viewer's size is remembered — it
// becomes load-bearing the moment that viewer takes control — and ignored:
// the pty follows the controller, so a phone watching a laptop's session does
// not squeeze the laptop's terminal down to phone width.
func (s *Session) SetSize(id int, gen uint64, size Size) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.viewers[id]
	if !ok {
		return false
	}
	v.size = size
	if !s.mayWriteLocked(v, gen) {
		return false
	}
	s.applySizeLocked()
	return true
}

// applySizeLocked resizes the pty to what its CONTROLLERS ask for. With one
// attachment that is the same rule it has always been. With several it is the
// rule that makes a viewer harmless: a session whose controllers have all
// gone keeps the size it had rather than snapping to whoever is watching.
func (s *Session) applySizeLocked() {
	var sizes []Size
	for _, v := range s.viewers {
		if !s.mayWriteLocked(v, v.bind.Generation) {
			continue
		}
		sizes = append(sizes, v.size)
	}
	eff, ok := EffectiveSize(sizes)
	if !ok || eff == s.size {
		return
	}
	s.size = eff
	s.emu.Resize(eff.Cols, eff.Rows)
	s.proc.Resize(eff.Cols, eff.Rows)
}

func (s *Session) Exited() <-chan struct{} { return s.exited }
func (s *Session) ExitCode() int           { return s.exitC }

// Stop signals the agent process to terminate (SIGTERM). The normal exit
// path (close viewers, close exited) then runs. Safe to call more than once.
func (s *Session) Stop() { s.proc.Stop() }

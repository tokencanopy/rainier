package sandboxexec

import (
	"testing"

	"github.com/tokencanopy/rainier/protocol/terminal"
)

// TestAClosingAttachmentEmitsNothingHoweverMuchRoomTheOutboxHas pins the check
// emit makes BEFORE its send: once the attachment is closing, nothing more
// goes out, even though the outbox still has room for it.
//
// The guarantee is worth a test of its own because it is what separates two
// answers a script reads differently. An exec whose session was stopped under
// it must report 125 with a sentence naming which end it was — not the 143
// that a SIGTERM-shaped exec_exit would produce, which a script reads as the
// command's own answer. run() calls emit for that exit message on its way out,
// and the kill that ended the process has already closed `closing`.
//
// Without the first select, BOTH arms of the second one are ready — the outbox
// has room and `closing` is shut — and Go picks between ready arms uniformly
// at random. One call cannot tell the two implementations apart; that fair
// coin IS the defect, which is why deleting the check survives this whole
// package's suite and is caught only 4 times in 10 at e2e.
//
// So the assertion is over a run of calls. At 200 the version without the
// check survives with probability 2^-200, which is deterministic in every
// sense that matters, and the test is still microseconds.
func TestAClosingAttachmentEmitsNothingHoweverMuchRoomTheOutboxHas(t *testing.T) {
	start := newFakeStarter()
	r, _ := testRunner(t, start.start)
	a := newAttachment(r)

	// Exactly the state a killed exec is in when run() reaches its exit
	// message: `closing` is shut, the outbox is still open, and it is empty.
	a.closeOnce.Do(func() { close(a.closing) })
	if a.outClosed {
		t.Fatal("the outbox was closed before the test could use it")
	}
	if cap(a.msgs) < 1 {
		t.Fatal("the outbox has no room, so this test cannot tell the two implementations apart")
	}

	const tries = 200
	for i := 0; i < tries; i++ {
		if err := a.emit(terminal.ServerMessage{
			Type: terminal.TypeExecExit, Signal: terminal.SignalTERM}); err != errAttachmentClosed {
			t.Fatalf("emit %d on a closing attachment returned %v, want %v — "+
				"the SIGTERM that ended this exec would be reported as the command's own "+
				"exit status, which a script reads as a 143 it should act on",
				i, err, errAttachmentClosed)
		}
	}
	if n := len(a.msgs); n != 0 {
		t.Fatalf("%d of %d messages reached a closing attachment's outbox, want none", n, tries)
	}
}

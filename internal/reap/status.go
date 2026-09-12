package reap

import "syscall"

// Status is one reaped child's whole outcome: the exit code it chose, or the
// signal that killed it. Exactly one of the two is meaningful, and Signal is
// what says which — a process killed by a signal has no exit status at all,
// and a command may legitimately exit 137 on its own, so a single integer
// with reserved values could not tell the two apart.
//
// It is a type rather than a second return value because `rainier exec` has
// to report the difference to its caller: a signalled command exits 128+N and
// an ordinary one exits its own code.
type Status struct {
	// Code is the exit status, meaningful only while Signal is zero.
	Code int
	// Signal is the signal that killed the process, zero when it exited on
	// its own.
	Signal syscall.Signal
}

// ExitCode is the outcome as os/exec reports it: the exit status, or -1 for a
// process a signal killed. It exists so AwaitExit keeps the exact answer it
// has always given, cmd.Wait's.
func (s Status) ExitCode() int {
	if s.Signal != 0 {
		return -1
	}
	return s.Code
}

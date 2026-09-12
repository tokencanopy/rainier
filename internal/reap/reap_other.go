//go:build !linux

package reap

// Start is a no-op off Linux so host (macOS) builds and tests still compile.
func Start() {}

// Mark is always zero off Linux: there is no table for a mark to be relative
// to, and every AwaitStatus falls back to the caller's own wait anyway.
func Mark() uint64 { return 0 }

// AwaitStatus always reports "not reaped" off Linux; the caller falls back to
// its own cmd.Wait for the outcome.
func AwaitStatus(pid int, since uint64) (Status, bool) { return Status{}, false }

// AwaitExit always reports "not reaped" off Linux; the caller falls back to
// its own cmd.Wait for the exit code.
func AwaitExit(pid int, since uint64) (int, bool) { return 0, false }

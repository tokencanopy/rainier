//go:build !linux

package reap

// Start is a no-op off Linux so host (macOS) builds and tests still compile.
func Start() {}

// AwaitStatus always reports "not reaped" off Linux; the caller falls back to
// its own cmd.Wait for the outcome.
func AwaitStatus(pid int) (Status, bool) { return Status{}, false }

// AwaitExit always reports "not reaped" off Linux; the caller falls back to
// its own cmd.Wait for the exit code.
func AwaitExit(pid int) (int, bool) { return 0, false }

//go:build windows

package cli

// withConfigLock has no cross-process lock on Windows yet; the in-process
// mutex still serializes one client's refreshes, and the reload under it
// still adopts a pair another process rotated first.
func withConfigLock(fn func() error) error { return fn() }

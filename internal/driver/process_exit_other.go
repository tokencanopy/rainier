//go:build !linux

package driver

// No terminal-state evidence is implemented for non-production host kernels.
func processExited(int, uint64) bool { return false }

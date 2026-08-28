//go:build !linux

package reap

// Reap is a no-op off Linux so host (macOS) builds and tests still compile.
func Reap(directChild int) <-chan int { return make(chan int) }

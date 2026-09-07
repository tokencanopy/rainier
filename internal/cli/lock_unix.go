//go:build !windows

package cli

import (
	"context"
	"errors"
	"os"
	"syscall"
	"time"
)

// withConfigLock runs fn while holding an advisory lock beside the config
// file, so two rainier processes sharing one config never refresh the same
// pair at the same moment. The edge rotates the refresh token on every use
// and treats a second use as a replay that revokes the whole family — which
// is right for an attacker and wrong for `rainier ls` in a second terminal.
// The lock is per config path, taken for the refresh alone, and released
// when fn returns; a process that dies holding it releases it with its file
// descriptor.
func withConfigLock(ctx context.Context, fn func() error) error {
	path, err := configPath()
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	// Nonblocking acquisition keeps the refresh deadline meaningful even
	// while a sibling process is waiting on its own network exchange.
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			break
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return fn()
}

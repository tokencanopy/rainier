//go:build windows

package cli

import "context"

// withConfigLock has no cross-process lock on Windows yet; the in-process
// mutex still serializes one client's refreshes, and the reload under it
// still adopts a pair another process rotated first.
func withConfigLock(ctx context.Context, fn func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return fn()
}

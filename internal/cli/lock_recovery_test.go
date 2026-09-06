//go:build !windows

package cli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestRefreshDeadlineIncludesSiblingConfigLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	t.Setenv("RAINIER_CONFIG", path)
	f, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	c := &Client{RefreshToken: "synthetic_refresh", LoadTokens: func() (TokenPair, bool) { return TokenPair{}, false }}
	go func() { done <- c.RefreshAfterUnauthorized(ctx) }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("refresh = %v, want context canceled", err)
		}
		syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	case <-time.After(time.Second):
		syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		<-done
		t.Fatal("refresh ignored cancellation while waiting for sibling's config lock")
	}
}

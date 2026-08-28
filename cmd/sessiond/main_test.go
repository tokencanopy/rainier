// cmd/sessiond/main_test.go
package main

import (
	"testing"
	"time"
)

// TestNextBackoff pins dialLoop's backoff step: double, then clamp to the
// design's 30s cap (spec: "jittered exponential backoff 1s..30s cap"). This
// is the regression test for the review-round-1 finding that the old inline
// `if backoff < 30*time.Second { backoff *= 2 }` overshoots to 32s and
// freezes there (16s < 30s passes the guard, then doubles to 32s, and every
// later step also fails the < 30s guard, freezing forever above the stated
// cap) instead of clamping at 30s.
func TestNextBackoff(t *testing.T) {
	cases := []struct {
		in, want time.Duration
	}{
		{time.Second, 2 * time.Second},
		{2 * time.Second, 4 * time.Second},
		{4 * time.Second, 8 * time.Second},
		{8 * time.Second, 16 * time.Second},
		{16 * time.Second, 30 * time.Second}, // clamped: 16*2=32 > 30 cap
		{30 * time.Second, 30 * time.Second}, // already at cap: stays put
	}
	for _, c := range cases {
		if got := nextBackoff(c.in); got != c.want {
			t.Errorf("nextBackoff(%s) = %s, want %s", c.in, got, c.want)
		}
	}
}

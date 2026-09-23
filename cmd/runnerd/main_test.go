// cmd/runnerd/main_test.go
package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The two flags a microVM runner cannot start without, and what each refusal
// has to say. They are Fatals in main for the reason the kernel and the rootfs
// are: a runner that started without a checkpoint store would accept placements
// and then fail every cold suspend, with a tenant's work still inside a VM the
// durability barrier will not let it terminate.

func keyFile(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "checkpoint.key")
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	if err := os.WriteFile(p, key, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestCheckpointConfigBuildsTheDriversPorts(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "checkpoints")
	opts, err := checkpointConfig(dir, keyFile(t), 1000, 1000)
	if err != nil {
		t.Fatalf("checkpointConfig: %v", err)
	}
	if opts.Store == nil || opts.Keys == nil {
		t.Fatalf("the configuration is %+v, want a store and a key wrapper", opts)
	}
	if opts.KeyRef == "" {
		t.Error("the configuration names no key reference; a manifest has to record one")
	}
	if opts.OwnerUID != 1000 || opts.OwnerGID != 1000 {
		t.Errorf("the restored workspace would be given to %d:%d, want 1000:1000", opts.OwnerUID, opts.OwnerGID)
	}
	// The store directory is created, because a runner that started without one
	// would discover it at the worst possible moment.
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("the store directory was not created: %v", err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Errorf("the store directory is mode %v, want 0700", info.Mode().Perm())
	}
}

func TestCheckpointConfigRefusesWhatARunnerCannotStartWith(t *testing.T) {
	good := keyFile(t)
	loose := filepath.Join(t.TempDir(), "loose.key")
	if err := os.WriteFile(loose, make([]byte, 32), 0o644); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name  string
		dir   string
		key   string
		uid   int
		gid   int
		wants string
	}{
		{"no store directory", "", good, 1000, 1000, "--checkpoint-store-dir is required"},
		{"no key file", t.TempDir(), "", 1000, 1000, "--checkpoint-key-file is required"},
		{"a key file that is not there", t.TempDir(), filepath.Join(t.TempDir(), "nope"), 1000, 1000, "checkpoint key file"},
		{"a key anyone can read", t.TempDir(), loose, 1000, 1000, "readable by group or other"},
		{"a negative uid", t.TempDir(), good, -1, 1000, "must not be negative"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := checkpointConfig(tc.dir, tc.key, tc.uid, tc.gid)
			if err == nil {
				t.Fatal("the configuration was accepted")
			}
			if !strings.Contains(err.Error(), tc.wants) {
				t.Errorf("the refusal is %q, want it to name %q", err, tc.wants)
			}
		})
	}
}

// TestCheckpointConfigSaysWhyRatherThanWhat. These lines are what an operator
// reads at three in the morning, so each one names the consequence and not just
// the missing argument.
func TestCheckpointConfigSaysWhyRatherThanWhat(t *testing.T) {
	_, err := checkpointConfig("", keyFile(t), 1000, 1000)
	if err == nil {
		t.Fatal("an empty store directory was accepted")
	}
	for _, want := range []string{"portable checkpoint", "no default"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal %q does not explain %q", err, want)
		}
	}
	if _, err = checkpointConfig(t.TempDir(), "", 1000, 1000); err == nil {
		t.Fatal("an empty key file was accepted")
	}
	if !strings.Contains(err.Error(), "in the clear") {
		t.Errorf("the refusal %q does not explain what an unencrypted checkpoint would be", err)
	}
}

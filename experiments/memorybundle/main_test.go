package main

import (
	"bytes"
	"context"
	"github.com/tokencanopy/rainier/checkpoint"
	"github.com/tokencanopy/rainier/internal/driver"
	"os"
	"path/filepath"
	"testing"
)

var artifacts = []string{"memory", "vmstate", "rootfs", "workspace", "home", "metadata.json"}

func fixture(t *testing.T) (string, string, string) {
	t.Helper()
	root := t.TempDir()
	source := filepath.Join(root, "source")
	os.Mkdir(source, 0700)
	for _, n := range artifacts {
		if err := os.WriteFile(filepath.Join(source, n), []byte("synthetic "+n), 0600); err != nil {
			t.Fatal(err)
		}
	}
	key := filepath.Join(root, "key")
	os.WriteFile(key, bytes.Repeat([]byte{42}, 32), 0600)
	return source, filepath.Join(root, "store"), key
}
func TestRoundTrip(t *testing.T) {
	source, store, key := fixture(t)
	if err := bundle("seal", store, key, source, 1); err != nil {
		t.Fatal(err)
	}
	if err := bundle("verify", store, key, "", 1); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "restore")
	if err := bundle("open", store, key, target, 1); err != nil {
		t.Fatal(err)
	}
	for _, n := range artifacts {
		b, e := os.ReadFile(filepath.Join(target, n))
		if e != nil || string(b) != "synthetic "+n {
			t.Fatalf("artifact %s mismatch", n)
		}
	}
	if err := bundle("open", store, key, target, 1); err == nil {
		t.Fatal("overwrote existing destination")
	}
}
func TestRejectInvalidSource(t *testing.T) {
	for _, kind := range []string{"missing", "extra", "symlink", "directory"} {
		t.Run(kind, func(t *testing.T) {
			source, store, key := fixture(t)
			switch kind {
			case "missing":
				os.Remove(filepath.Join(source, "home"))
			case "extra":
				os.WriteFile(filepath.Join(source, "extra"), []byte("x"), 0600)
			case "symlink":
				os.Remove(filepath.Join(source, "home"))
				os.Symlink(key, filepath.Join(source, "home"))
			case "directory":
				os.Remove(filepath.Join(source, "home"))
				os.Mkdir(filepath.Join(source, "home"), 0700)
			}
			if err := bundle("seal", store, key, source, 1); err == nil {
				t.Fatal("accepted invalid source")
			}
		})
	}
}
func TestRejectDamageWithoutPublishing(t *testing.T) {
	for _, kind := range []string{"wrong-key", "tamper", "truncate", "wrong-generation"} {
		t.Run(kind, func(t *testing.T) {
			source, store, key := fixture(t)
			if err := bundle("seal", store, key, source, 1); err != nil {
				t.Fatal(err)
			}
			generation := uint64(1)
			if kind == "wrong-key" {
				os.WriteFile(key, bytes.Repeat([]byte{43}, 32), 0600)
			} else if kind == "wrong-generation" {
				generation = 2
			} else {
				var largest string
				var size int64
				filepath.Walk(store, func(p string, i os.FileInfo, e error) error {
					if e == nil && i.Mode().IsRegular() && i.Size() > size {
						largest = p
						size = i.Size()
					}
					return e
				})
				b, e := os.ReadFile(largest)
				if e != nil {
					t.Fatal(e)
				}
				if kind == "tamper" {
					b[len(b)/2] ^= 1
				} else {
					b = b[:len(b)/2]
				}
				os.WriteFile(largest, b, 0600)
			}
			target := filepath.Join(t.TempDir(), "restore")
			if err := bundle("open", store, key, target, generation); err == nil {
				t.Fatal("accepted damaged bundle")
			}
			if _, err := os.Lstat(target); !os.IsNotExist(err) {
				t.Fatal("published failed restore")
			}
		})
	}
}

func TestRestoreRejectsAuthenticatedIncompleteBundle(t *testing.T) {
	source, storePath, keyPath := fixture(t)
	os.Remove(filepath.Join(source, "home"))
	key, err := driver.LoadCheckpointKey(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	store, err := driver.NewDirBlobStore(storePath)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := checkpoint.NewStaticKeyWrapper("memory-experiment-key", key)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := checkpoint.NewWriter(store, keys, checkpoint.WriterOptions{KeyRef: "memory-experiment-key"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = writer.Write(context.Background(), checkpoint.Context{Workspace: "memory-experiment", Session: "synthetic-session", Generation: 1}, checkpoint.DirSource(source))
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "restore")
	if err := bundle("open", storePath, keyPath, target, 1); err == nil {
		t.Fatal("published incomplete but authenticated bundle")
	}
	if _, err := os.Lstat(target); !os.IsNotExist(err) {
		t.Fatal("failed restore was published")
	}
}

// memorybundle is an operator-only experiment; it is not a production resume API.
// All paths must be on a private, operator-owned filesystem with no other writers.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/tokencanopy/rainier/checkpoint"
	"github.com/tokencanopy/rainier/internal/driver"
)

var required = map[string]bool{"memory": true, "vmstate": true, "rootfs": true, "workspace": true, "home": true, "metadata.json": true}

func validate(dir string) error {
	info, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return errors.New("artifact directory must be private")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	if len(entries) != len(required) {
		return errors.New("incorrect artifact count")
	}
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			return err
		}
		if !required[e.Name()] || !info.Mode().IsRegular() || info.Size() == 0 {
			return errors.New("unexpected or empty artifact")
		}
	}
	return nil
}
func bundle(action, storePath, keyPath, dir string, generation uint64) error {
	if action != "seal" && action != "verify" && action != "open" {
		return errors.New("unknown action")
	}
	c := checkpoint.Context{Workspace: "memory-experiment", Session: "synthetic-session", Generation: generation}
	if err := c.Validate(); err != nil {
		return err
	}
	key, err := driver.LoadCheckpointKey(keyPath)
	if err != nil {
		return err
	}
	store, err := driver.NewDirBlobStore(storePath)
	if err != nil {
		return err
	}
	keys, err := checkpoint.NewStaticKeyWrapper("memory-experiment-key", key)
	if err != nil {
		return err
	}
	ctx := context.Background()
	// The local operator owns this experimental store and key. This is NOT a
	// control-plane authorization policy and must never be wired into a service.
	reader, err := checkpoint.NewReader(store, keys, checkpoint.ReaderOptions{Authorize: func(context.Context, checkpoint.Context, checkpoint.Manifest) error { return nil }})
	if err != nil {
		return err
	}
	switch action {
	case "seal":
		if err := validate(dir); err != nil {
			return err
		}
		writer, err := checkpoint.NewWriter(store, keys, checkpoint.WriterOptions{KeyRef: "memory-experiment-key"})
		if err != nil {
			return err
		}
		if _, err = writer.Write(ctx, c, checkpoint.DirSource(dir)); err != nil {
			return err
		}
		_, err = reader.Verify(ctx, c)
		return err
	case "verify":
		_, err = reader.Verify(ctx, c)
		return err
	case "open":
		if dir == "" {
			return errors.New("destination required")
		}
		absolute, err := filepath.Abs(dir)
		if err != nil {
			return err
		}
		lock, err := os.OpenFile(absolute+".lock", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		lock.Close()
		defer os.Remove(absolute + ".lock")
		if _, err = os.Lstat(absolute); !os.IsNotExist(err) {
			return errors.New("destination already exists or cannot be checked")
		}
		stage, err := os.MkdirTemp(filepath.Dir(absolute), ".memory-restore-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(stage)
		if _, err = reader.Restore(ctx, c, stage); err != nil {
			return err
		}
		if err = validate(stage); err != nil {
			return err
		}
		return os.Rename(stage, absolute)
	}
	panic("unreachable")
}
func main() {
	action := flag.String("action", "", "seal, verify, or open")
	store := flag.String("store", "", "private experimental blob directory")
	key := flag.String("key", "", "private 32-byte key file, outside artifact directory")
	dir := flag.String("dir", "", "source or new destination directory")
	generation := flag.Uint64("generation", 0, "explicit snapshot generation")
	flag.Parse()
	if *store == "" || *key == "" || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "store and key required; no positional arguments")
		os.Exit(2)
	}
	if err := bundle(*action, *store, *key, *dir, *generation); err != nil {
		fmt.Fprintln(os.Stderr, "memory bundle operation failed:", err)
		os.Exit(1)
	}
	fmt.Println("memory bundle operation verified")
}

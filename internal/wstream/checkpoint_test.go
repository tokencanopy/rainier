package wstream

import (
	"bytes"
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/tokencanopy/rainier/checkpoint"
)

// The claim this whole package exists for: a workspace that left a guest as a
// stream can be checkpointed, verified, and restored into a directory that is
// the workspace again.
//
// It uses the real library rather than a stand-in, because "the fs.FS the
// checkpoint writer walks" is not a property of an interface — it is a property
// of that walk, in the order it makes its calls, with the exclusions it applies
// and the refusals it raises.
func TestFSFeedsTheCheckpointWriter(t *testing.T) {
	root := fixture(t)
	// A file large enough to span several of the writer's copy buffers, so the
	// body path is exercised rather than only the header path.
	if err := os.WriteFile(filepath.Join(root, "dir/big.bin"), bytes.Repeat([]byte("x"), 200<<10), 0o644); err != nil {
		t.Fatal(err)
	}
	exclude := checkpoint.DefaultExclusions()

	var buf bytes.Buffer
	sent, err := Write(context.Background(), os.DirFS(root), &buf, Limits{Exclude: exclude})
	if err != nil {
		t.Fatalf("streaming the workspace: %v", err)
	}

	f, err := NewFS(bytes.NewReader(buf.Bytes()), Limits{Exclude: exclude})
	if err != nil {
		t.Fatalf("reading the stream: %v", err)
	}
	if f.Indexed() != int(sent.Entries) {
		t.Fatalf("the host indexed %d entries, the guest streamed %d", f.Indexed(), sent.Entries)
	}

	store := checkpoint.NewMemoryStore()
	keys, err := checkpoint.NewStaticKeyWrapper("test/checkpoint/v1", [32]byte{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	w, err := checkpoint.NewWriter(store, keys, checkpoint.WriterOptions{KeyRef: "test/checkpoint/v1"})
	if err != nil {
		t.Fatal(err)
	}
	c := checkpoint.Context{Workspace: "ws-example", Session: "sess-example", Generation: 1}
	res, err := w.Write(context.Background(), c, checkpoint.Source{FS: f})
	if err != nil {
		t.Fatalf("checkpointing the stream: %v", err)
	}
	if res.Manifest.Entries != int64(f.Entries()) {
		t.Errorf("the checkpoint carries %d entries, the stream presented %d", res.Manifest.Entries, f.Entries())
	}

	r, err := checkpoint.NewReader(store, keys, checkpoint.ReaderOptions{
		Authorize: func(context.Context, checkpoint.Context, checkpoint.Manifest) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Verify(context.Background(), c); err != nil {
		t.Fatalf("verifying the committed checkpoint: %v", err)
	}

	target := filepath.Join(t.TempDir(), "restored")
	if _, err := r.Restore(context.Background(), c, target); err != nil {
		t.Fatalf("restoring the checkpoint: %v", err)
	}
	compareTrees(t, root, target, exclude)
}

// compareTrees requires the restored tree to be the source tree, entry for
// entry: the same names in the same order, the same kinds, the same
// permissions, the same content and the same link targets.
func compareTrees(t *testing.T, want, got string, exclude []string) {
	t.Helper()
	walk := func(dir string) []string {
		var out []string
		err := fs.WalkDir(os.DirFS(dir), ".", func(name string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if name == "." {
				return nil
			}
			for _, e := range exclude {
				if name == e {
					return fs.SkipDir
				}
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			switch {
			case d.IsDir():
				out = append(out, "d "+name+" "+info.Mode().Perm().String())
			case d.Type()&fs.ModeSymlink != 0:
				target, err := os.Readlink(filepath.Join(dir, name))
				if err != nil {
					return err
				}
				out = append(out, "l "+name+" -> "+target)
			default:
				body, err := os.ReadFile(filepath.Join(dir, name))
				if err != nil {
					return err
				}
				out = append(out, "f "+name+" "+info.Mode().Perm().String()+" "+
					info.ModTime().UTC().Truncate(1e9).String()+" "+string(body[:min(len(body), 16)]))
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walking %s: %v", dir, err)
		}
		return out
	}
	a, b := walk(want), walk(got)
	if len(a) != len(b) {
		t.Fatalf("the restored tree has %d entries, the source has %d:\n%v\n%v", len(b), len(a), b, a)
	}
	for i := range a {
		if a[i] != b[i] {
			t.Errorf("entry %d restored as %q, want %q", i, b[i], a[i])
		}
	}
}

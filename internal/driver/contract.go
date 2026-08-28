// internal/driver/contract.go
package driver

import (
	"context"
	"testing"
)

func RunContract(t *testing.T, newDriver func(t *testing.T) (Driver, func())) {
	t.Run("create-inspect-destroy", func(t *testing.T) {
		d, cleanup := newDriver(t); defer cleanup()
		ctx := context.Background()
		h, err := d.Create(ctx, Spec{Name: "t1", Image: "", SessionID: "s1", DialURL: "ws://x"})
		if err != nil { t.Fatal(err) }
		if h.ID == "" { t.Fatal("empty handle id") }
		got, err := d.Inspect(ctx, h.ID)
		if err != nil || got.State != StateRunning { t.Fatalf("inspect = %+v, %v", got, err) }
		if err := d.Destroy(ctx, h.ID); err != nil { t.Fatal(err) }
		if g, _ := d.Inspect(ctx, h.ID); g.State != StateGone { t.Fatalf("post-destroy state = %s", g.State) }
	})

	t.Run("suspend-resume", func(t *testing.T) {
		d, cleanup := newDriver(t); defer cleanup()
		ctx := context.Background()
		h, _ := d.Create(ctx, Spec{Name: "t2", Image: "", SessionID: "s2", DialURL: "ws://x"})
		defer d.Destroy(ctx, h.ID)
		if err := d.Suspend(ctx, h.ID, true); err != nil { t.Fatal(err) }
		if g, _ := d.Inspect(ctx, h.ID); g.State != StateSuspended { t.Fatalf("warm state = %s", g.State) }
		if err := d.Resume(ctx, h.ID); err != nil { t.Fatal(err) }
		if g, _ := d.Inspect(ctx, h.ID); g.State != StateRunning { t.Fatalf("resumed state = %s", g.State) }
		if err := d.Suspend(ctx, h.ID, false); err != nil { t.Fatal(err) } // cold
		if err := d.Resume(ctx, h.ID); err != nil { t.Fatal(err) }
	})

	t.Run("snapshot", func(t *testing.T) {
		d, cleanup := newDriver(t); defer cleanup()
		ctx := context.Background()
		h, _ := d.Create(ctx, Spec{Name: "t3", Image: "", SessionID: "s3", DialURL: "ws://x"})
		defer d.Destroy(ctx, h.ID)
		snap, err := d.Snapshot(ctx, h.ID)
		if err != nil || snap.Ref == "" { t.Fatalf("snapshot = %+v, %v", snap, err) }
		// Two snapshots of the SAME handle must get distinct refs: a fixed
		// per-container suffix (e.g. derived only from the container id's
		// length) makes every snapshot of that container collide on the same
		// ref, so a second `docker commit` silently overwrites the first
		// snapshot under the same tag instead of producing a new one.
		snap2, err := d.Snapshot(ctx, h.ID)
		if err != nil || snap2.Ref == "" { t.Fatalf("second snapshot = %+v, %v", snap2, err) }
		if snap2.Ref == snap.Ref { t.Fatalf("two snapshots of the same handle got the same ref: %q", snap.Ref) }
	})

	t.Run("capacity", func(t *testing.T) {
		d, cleanup := newDriver(t); defer cleanup()
		ctx := context.Background()
		used0, total, _ := d.Capacity(ctx)
		if total <= 0 { t.Fatalf("total capacity must be positive, got %d", total) }
		h, _ := d.Create(ctx, Spec{Name: "t4", Image: "", SessionID: "s4", DialURL: "ws://x"})
		defer d.Destroy(ctx, h.ID)
		used1, _, _ := d.Capacity(ctx)
		if used1 != used0+1 { t.Fatalf("used should rise by 1: %d → %d", used0, used1) }
	})
}

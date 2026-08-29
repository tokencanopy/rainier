// internal/driver/contract.go
package driver

import (
	"context"
	"testing"
)

func RunContract(t *testing.T, newDriver func(t *testing.T) (Driver, func())) {
	t.Run("create-inspect-destroy", func(t *testing.T) {
		d, cleanup := newDriver(t)
		defer cleanup()
		ctx := context.Background()
		h, err := d.Create(ctx, Spec{Name: "t1", Image: "", SessionID: "s1", DialURL: "ws://x"})
		if err != nil {
			t.Fatal(err)
		}
		if h.ID == "" {
			t.Fatal("empty handle id")
		}
		got, err := d.Inspect(ctx, h.ID)
		if err != nil || got.State != StateRunning {
			t.Fatalf("inspect = %+v, %v", got, err)
		}
		if err := d.Destroy(ctx, h.ID); err != nil {
			t.Fatal(err)
		}
		if g, _ := d.Inspect(ctx, h.ID); g.State != StateGone {
			t.Fatalf("post-destroy state = %s", g.State)
		}
	})

	t.Run("suspend-resume", func(t *testing.T) {
		d, cleanup := newDriver(t)
		defer cleanup()
		ctx := context.Background()
		h, _ := d.Create(ctx, Spec{Name: "t2", Image: "", SessionID: "s2", DialURL: "ws://x"})
		defer d.Destroy(ctx, h.ID)
		if err := d.Suspend(ctx, h.ID, true); err != nil {
			t.Fatal(err)
		}
		if g, _ := d.Inspect(ctx, h.ID); g.State != StateSuspended {
			t.Fatalf("warm state = %s", g.State)
		}
		if err := d.Resume(ctx, h.ID); err != nil {
			t.Fatal(err)
		}
		if g, _ := d.Inspect(ctx, h.ID); g.State != StateRunning {
			t.Fatalf("resumed state = %s", g.State)
		}
		if err := d.Suspend(ctx, h.ID, false); err != nil {
			t.Fatal(err)
		} // cold
		if err := d.Resume(ctx, h.ID); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("snapshot", func(t *testing.T) {
		d, cleanup := newDriver(t)
		defer cleanup()
		ctx := context.Background()
		h, _ := d.Create(ctx, Spec{Name: "t3", Image: "", SessionID: "s3", DialURL: "ws://x"})
		defer d.Destroy(ctx, h.ID)
		snap, err := d.Snapshot(ctx, h.ID)
		if err != nil || snap.Ref == "" {
			t.Fatalf("snapshot = %+v, %v", snap, err)
		}
		// Two snapshots of the SAME handle must get distinct refs: a fixed
		// per-container suffix (e.g. derived only from the container id's
		// length) makes every snapshot of that container collide on the same
		// ref, so a second `docker commit` silently overwrites the first
		// snapshot under the same tag instead of producing a new one.
		snap2, err := d.Snapshot(ctx, h.ID)
		if err != nil || snap2.Ref == "" {
			t.Fatalf("second snapshot = %+v, %v", snap2, err)
		}
		if snap2.Ref == snap.Ref {
			t.Fatalf("two snapshots of the same handle got the same ref: %q", snap.Ref)
		}
	})

	t.Run("capacity", func(t *testing.T) {
		d, cleanup := newDriver(t)
		defer cleanup()
		ctx := context.Background()
		used0, total, _ := d.Capacity(ctx)
		if total <= 0 {
			t.Fatalf("total capacity must be positive, got %d", total)
		}
		h, _ := d.Create(ctx, Spec{Name: "t4", Image: "", SessionID: "s4", DialURL: "ws://x"})
		defer d.Destroy(ctx, h.ID)
		used1, _, _ := d.Capacity(ctx)
		if used1 != used0+1 {
			t.Fatalf("used should rise by 1: %d → %d", used0, used1)
		}
	})

	t.Run("list reflects create and destroy", func(t *testing.T) {
		d, cleanup := newDriver(t)
		defer cleanup()
		ctx := context.Background()
		h, err := d.Create(ctx, Spec{Name: "t5", Image: "", SessionID: "s5", DialURL: "ws://x"})
		if err != nil {
			t.Fatal(err)
		}

		listed, err := d.List(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var found *Listed
		for i := range listed {
			if listed[i].SessionID == "s5" {
				found = &listed[i]
			}
		}
		if found == nil {
			t.Fatalf("List after create does not contain session s5: %+v", listed)
		}
		if found.Handle.ID != h.ID {
			t.Fatalf("listed handle id = %q, want %q", found.Handle.ID, h.ID)
		}
		if found.Handle.State != StateRunning {
			t.Fatalf("listed state = %s, want %s", found.Handle.State, StateRunning)
		}

		if err := d.Destroy(ctx, h.ID); err != nil {
			t.Fatal(err)
		}
		listed, err = d.List(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, l := range listed {
			if l.SessionID == "s5" {
				t.Fatalf("List after destroy still contains session s5: %+v", listed)
			}
		}
	})

	t.Run("workspace survives cold park", func(t *testing.T) {
		// Cold park is stop, not destroy: the container AND the /workspace
		// volume behind it have to still be there afterwards, or a resumed
		// session comes back to a blank workspace — which is the whole reason
		// the volume is named per session instead of anonymous.
		//
		// What every driver owes here is the surface: after a cold
		// suspend/resume round trip the session is running again, still
		// listed under the same session id, and still on the same handle.
		// Proving the FILES themselves survived needs to look inside the
		// container, which only the docker driver can do — that half lives in
		// docker_test.go's TestDockerWorkspaceSurvivesColdPark.
		d, cleanup := newDriver(t)
		defer cleanup()
		ctx := context.Background()
		h, err := d.Create(ctx, Spec{Name: "t7", Image: "", SessionID: "s7", DialURL: "ws://x"})
		if err != nil {
			t.Fatal(err)
		}
		defer d.Destroy(ctx, h.ID)

		if err := d.Suspend(ctx, h.ID, false); err != nil {
			t.Fatal(err)
		} // cold
		if err := d.Resume(ctx, h.ID); err != nil {
			t.Fatal(err)
		}
		if g, err := d.Inspect(ctx, h.ID); err != nil || g.State != StateRunning {
			t.Fatalf("inspect after cold park + resume = %+v, %v; want %s", g, err, StateRunning)
		}

		listed, err := d.List(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var found *Listed
		for i := range listed {
			if listed[i].SessionID == "s7" {
				found = &listed[i]
			}
		}
		if found == nil {
			t.Fatalf("List after cold park + resume dropped session s7: %+v", listed)
		}
		if found.Handle.ID != h.ID {
			t.Fatalf("handle id after cold park + resume = %q, want %q (a new handle means the session was recreated, not resumed)", found.Handle.ID, h.ID)
		}
	})

	t.Run("capacity ignores cold-parked", func(t *testing.T) {
		d, cleanup := newDriver(t)
		defer cleanup()
		ctx := context.Background()
		used0, _, _ := d.Capacity(ctx)
		h, err := d.Create(ctx, Spec{Name: "t6", Image: "", SessionID: "s6", DialURL: "ws://x"})
		if err != nil {
			t.Fatal(err)
		}
		defer d.Destroy(ctx, h.ID)
		used1, _, _ := d.Capacity(ctx)
		if used1 != used0+1 {
			t.Fatalf("used after create = %d, want %d", used1, used0+1)
		}

		if err := d.Suspend(ctx, h.ID, false); err != nil {
			t.Fatal(err)
		} // cold
		used2, _, _ := d.Capacity(ctx)
		if used2 != used0 {
			t.Fatalf("used after cold suspend = %d, want %d (cold-parked containers must not occupy a slot)", used2, used0)
		}

		listed, err := d.List(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var found *Listed
		for i := range listed {
			if listed[i].SessionID == "s6" {
				found = &listed[i]
			}
		}
		if found == nil {
			t.Fatalf("List after cold suspend does not contain session s6: %+v", listed)
		}
		if found.Handle.State != StateSuspended {
			t.Fatalf("listed state after cold suspend = %s, want %s", found.Handle.State, StateSuspended)
		}
	})
}

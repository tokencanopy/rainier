// internal/driver/contract.go
package driver

import (
	"context"
	"slices"
	"strings"
	"testing"
)

// cleanupSnapshotRef best-effort removes an image a snapshot subtest just
// committed. Only the docker driver puts a real image on the host — every
// other driver's ref is bookkeeping it drops with the driver itself — so this
// is a no-op for them, and its failure is never worth failing a test over:
// leaving a stray tag behind is untidy, not incorrect.
//
// It lives here rather than in each driver's own tests because the refs it has
// to clean up are the ones the shared subtests above name, and the alternative
// (the docker contract run silently accumulating one `rainier-*` image per
// invocation on the developer's machine) is what the pre-Task-6 suite did.
func cleanupSnapshotRef(d Driver, ref string) {
	if _, ok := d.(*Docker); !ok || ref == "" {
		return
	}
	dockerRun(context.Background(), "image", "rm", "-f", ref)
}

// assertStrippedFromImage checks a committed image's own configuration for the
// values a snapshot was told to strip. Only the docker driver has one to read
// — every other driver's snapshot is bookkeeping with no config behind it — so
// this is a no-op for them, and the fake's half of the same guarantee is
// asserted through Fake.Strips instead.
//
// It asserts on the whole rendered env block: `value` must not appear anywhere
// in it (a stripped key whose value merely moved to another key is still a
// leak), and each stripped key must be present-but-empty rather than carrying
// anything at all.
func assertStrippedFromImage(t *testing.T, d Driver, ref, value string, stripped []string) {
	t.Helper()
	if _, ok := d.(*Docker); !ok {
		return
	}
	out, err := dockerRun(context.Background(), "image", "inspect",
		"-f", "{{range .Config.Env}}{{println .}}{{end}}", ref)
	if err != nil {
		t.Fatalf("docker image inspect %s: %v", ref, err)
	}
	if strings.Contains(out, value) {
		t.Fatalf("the committed image's config still carries a stripped value:\n%s", out)
	}
	for _, line := range strings.Split(out, "\n") {
		for _, k := range stripped {
			if v, ok := strings.CutPrefix(line, k+"="); ok && v != "" {
				t.Fatalf("stripped key %s carries %q in the committed image's config:\n%s", k, v, out)
			}
		}
	}
}

// workspaceExists reports whether sessionID's workspace volume is present
// right now. It is a type switch for the same reason assertStrippedFromImage
// is one: the two drivers keep their volumes in completely different places (a
// docker daemon; a map in the fake), and the crash-vs-rm subtest below has to
// be able to ask both of them the same question — otherwise the one guarantee
// that matters ("a crash keeps the workspace") could only ever be pinned
// against one driver, and the other would be free to drift.
func workspaceExists(t *testing.T, d Driver, sessionID string) bool {
	t.Helper()
	name := workspaceVolume(sessionID)
	switch dd := d.(type) {
	case *Docker:
		// `docker volume ls --filter name=` matches on substring, so the names
		// are compared exactly rather than trusting the filter to be anchored.
		out, err := dockerRun(context.Background(), "volume", "ls", "-q", "--filter", "name="+name)
		if err != nil {
			t.Fatalf("docker volume ls: %v", err)
		}
		for _, line := range strings.Split(out, "\n") {
			if strings.TrimSpace(line) == name {
				return true
			}
		}
		return false
	case *Fake:
		return slices.Contains(dd.Volumes(), name)
	default:
		t.Fatalf("workspaceExists: no volume view for driver %T", d)
		return false
	}
}

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

	t.Run("snapshot with an empty ref generates unique refs", func(t *testing.T) {
		// An empty ref is the dev surface's case (POST
		// /sessions/{id}/snapshot has no environment to name), and the driver
		// mints the tag itself. The Plan 2 behavior, re-pinned through the
		// two-argument signature.
		d, cleanup := newDriver(t)
		defer cleanup()
		ctx := context.Background()
		h, _ := d.Create(ctx, Spec{Name: "t3", Image: "", SessionID: "s3", DialURL: "ws://x"})
		defer d.Destroy(ctx, h.ID)
		snap, err := d.Snapshot(ctx, h.ID, "", nil)
		if err != nil || snap.Ref == "" {
			t.Fatalf("snapshot = %+v, %v", snap, err)
		}
		defer cleanupSnapshotRef(d, snap.Ref)
		// Two snapshots of the SAME handle must get distinct refs: a fixed
		// per-container suffix (e.g. derived only from the container id's
		// length) makes every snapshot of that container collide on the same
		// ref, so a second `docker commit` silently overwrites the first
		// snapshot under the same tag instead of producing a new one.
		snap2, err := d.Snapshot(ctx, h.ID, "", nil)
		if err != nil || snap2.Ref == "" {
			t.Fatalf("second snapshot = %+v, %v", snap2, err)
		}
		defer cleanupSnapshotRef(d, snap2.Ref)
		if snap2.Ref == snap.Ref {
			t.Fatalf("two snapshots of the same handle got the same ref: %q", snap.Ref)
		}
	})

	t.Run("snapshot honors an explicit ref", func(t *testing.T) {
		// Plan 4 content-addresses an environment's cached image in CONTROLD
		// (rainier-env:<envID>-<setupHash>) and hands the runner that exact
		// tag, so the same environment resolves to the same ref on every
		// runner in the fleet. The driver treats it as opaque and must return
		// it verbatim — a driver that minted its own name here, or decorated
		// the caller's, would break that addressing outright: controld would
		// record one ref and later creates would look for an image nobody
		// ever tagged.
		d, cleanup := newDriver(t)
		defer cleanup()
		ctx := context.Background()
		h, _ := d.Create(ctx, Spec{Name: "t8", Image: "", SessionID: "s8", DialURL: "ws://x"})
		defer d.Destroy(ctx, h.ID)
		const ref = "rainier-env:contract-abc123"
		defer cleanupSnapshotRef(d, ref)
		snap, err := d.Snapshot(ctx, h.ID, ref, nil)
		if err != nil {
			t.Fatalf("snapshot to %q: %v", ref, err)
		}
		if snap.Ref != ref {
			t.Fatalf("snapshot ref = %q, want %q verbatim", snap.Ref, ref)
		}
	})

	t.Run("snapshot strips the named environment keys", func(t *testing.T) {
		// The security half of Plan 4's cache. A commit captures the
		// container's config, environment block and all, so an environment's
		// decrypted secrets and the setup channel that triggers a re-run would
		// otherwise be baked into an image every later session boots. Every
		// driver owes the same guarantee: what a caller names in stripEnv is
		// not in the committed image.
		//
		// Only the docker driver has a config to inspect; the fake's contract
		// is that it accepts and records the list (asserted in fake_test.go),
		// so this subtest asserts the shared surface — the call succeeds and
		// still returns the caller's ref verbatim — and then the real config
		// where there is one.
		d, cleanup := newDriver(t)
		defer cleanup()
		ctx := context.Background()
		h, err := d.Create(ctx, Spec{
			Name: "t9", Image: "", SessionID: "s9", DialURL: "ws://x",
			Setup: "true",
			Env:   map[string]string{"CONTRACT_SECRET": "must-not-survive"},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer d.Destroy(ctx, h.ID)

		const ref = "rainier-env:contract-strip"
		defer cleanupSnapshotRef(d, ref)
		strip := []string{"CONTRACT_SECRET", "RAINIER_SETUP_B64", "RAINIER_SETUP_TIMEOUT"}
		snap, err := d.Snapshot(ctx, h.ID, ref, strip)
		if err != nil {
			t.Fatalf("snapshot with a strip list: %v", err)
		}
		if snap.Ref != ref {
			t.Fatalf("snapshot ref = %q, want %q verbatim", snap.Ref, ref)
		}
		assertStrippedFromImage(t, d, ref, "must-not-survive", strip)
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

	t.Run("destroy takes the container and the workspace together", func(t *testing.T) {
		// The explicit-rm path, re-pinned unchanged as the crash path is
		// carved out beside it: whatever else the split does, `rainier rm`
		// must still leave nothing behind. A volume with no container left to
		// name it is a workspace nobody will ever find again, one per session
		// ever created.
		d, cleanup := newDriver(t)
		defer cleanup()
		ctx := context.Background()
		h, err := d.Create(ctx, Spec{Name: "t-rm", Image: "", SessionID: "s-rm", DialURL: "ws://x"})
		if err != nil {
			t.Fatal(err)
		}
		if !workspaceExists(t, d, "s-rm") {
			t.Fatal("Create left no workspace volume to remove")
		}
		if err := d.Destroy(ctx, h.ID); err != nil {
			t.Fatal(err)
		}
		if g, _ := d.Inspect(ctx, h.ID); g.State != StateGone {
			t.Fatalf("post-destroy state = %s, want gone", g.State)
		}
		if workspaceExists(t, d, "s-rm") {
			t.Fatal("Destroy removed the container but left the workspace volume behind")
		}
	})

	t.Run("a crash keeps the workspace; removing it is a separate act", func(t *testing.T) {
		// The two halves of the split, in the order the crash path runs them.
		// DestroyContainer reclaims the slot and NOTHING else: a session whose
		// sessiond died still holds every hour of work under /workspace, and
		// the container's death is not the user asking for that to be thrown
		// away. RemoveWorkspace is the second act, and only an explicit rm
		// ever performs it.
		d, cleanup := newDriver(t)
		defer cleanup()
		ctx := context.Background()
		h, err := d.Create(ctx, Spec{Name: "t-crash", Image: "", SessionID: "s-crash", DialURL: "ws://x"})
		if err != nil {
			t.Fatal(err)
		}
		if !workspaceExists(t, d, "s-crash") {
			t.Fatal("Create left no workspace volume to keep")
		}

		if err := d.DestroyContainer(ctx, h.ID); err != nil {
			t.Fatal(err)
		}
		if g, _ := d.Inspect(ctx, h.ID); g.State != StateGone {
			t.Fatalf("post-DestroyContainer state = %s, want gone", g.State)
		}
		if !workspaceExists(t, d, "s-crash") {
			t.Fatal("DestroyContainer removed the workspace volume; a crash must keep it")
		}

		if err := d.RemoveWorkspace(ctx, "s-crash"); err != nil {
			t.Fatal(err)
		}
		if workspaceExists(t, d, "s-crash") {
			t.Fatal("RemoveWorkspace left the volume behind")
		}

		// Tolerating an absent volume is not politeness: every caller is a
		// teardown path that may be running second (a full Destroy already
		// took it, a reconcile got there first), and an error there would turn
		// a completed teardown into a reported failure.
		if err := d.RemoveWorkspace(ctx, "s-crash"); err != nil {
			t.Fatalf("RemoveWorkspace of an already-removed volume = %v, want nil", err)
		}
		if err := d.RemoveWorkspace(ctx, "s-never-existed"); err != nil {
			t.Fatalf("RemoveWorkspace of a volume that never existed = %v, want nil", err)
		}
		// An empty session id names no workspace. `rainier-ws-` on its own is
		// a real volume name a driver must never be talked into removing, so
		// the id-less case is a no-op rather than a prefix-only `volume rm`.
		if err := d.RemoveWorkspace(ctx, ""); err != nil {
			t.Fatalf("RemoveWorkspace(\"\") = %v, want nil", err)
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

// internal/driver/microvm_envimage_test.go
//
// Booting a session from a cached environment: a ref controld minted
// (rainier-env:<envID>-<setupHash>) resolves through this host's manifest to a
// digest, and the session boots a copy-on-write copy of THAT image rather than
// of the runner's base rootfs. It is the whole point of the cache, and the
// reason a cache-hit create carries an init and no setup script.
package driver

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestMicrovmCreateBootsFromASnapshotRef is the round trip: snapshot a session
// under an environment ref, then create from that ref and land on the
// published digest.
func TestMicrovmCreateBootsFromASnapshotRef(t *testing.T) {
	m, sim, cloner := testMicrovmCloning(t, MicrovmOpts{TotalSlots: 4})
	m.SetHost(&stubMicrovmHost{})
	ctx := context.Background()

	// The session that builds the cache.
	builder, err := m.Create(ctx, Spec{
		SessionID: "sess-builder",
		Setup:     "install the toolchain",
	})
	if err != nil {
		t.Fatal(err)
	}
	rootfs, err := m.sessionRootfsPath(builder.ID)
	if err != nil {
		t.Fatal(err)
	}
	const built = "the environment after its setup script ran"
	if err := os.WriteFile(rootfs, []byte(built), microvmFileMode); err != nil {
		t.Fatal(err)
	}

	// controld's content-addressed ref for the environment.
	const ref = "rainier-env:env7-9f2c1a0b4d5e"
	if _, err := m.Snapshot(ctx, builder.ID, ref, []string{"RAINIER_SETUP_B64", "RAINIER_SETUP_TIMEOUT"}); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	manifest, ok := m.images.manifest(ref)
	if !ok {
		t.Fatal("the snapshot published no manifest")
	}
	blob, ok := m.images.have(manifest.Digest)
	if !ok {
		t.Fatal("the published digest is not in the store")
	}

	// A later session, dispatched with the cached ref and no setup script —
	// the image IS the finished setup.
	cached, err := m.Create(ctx, Spec{SessionID: "sess-cached", Image: ref})
	if err != nil {
		t.Fatalf("create from a cached environment ref: %v", err)
	}
	cfg, ok := sim.Config(cached.ID)
	if !ok {
		t.Fatal("the cached session was never launched")
	}
	if cfg.BaseImageDigest != manifest.Digest {
		t.Errorf("the cached session booted %s, want the published %s", cfg.BaseImageDigest, manifest.Digest)
	}
	if cfg.BaseImagePath != blob {
		t.Errorf("the cached session booted %q, want the store's %q", cfg.BaseImagePath, blob)
	}
	if cfg.BaseImagePath == m.opts.BaseRootfs {
		t.Fatal("the cached session booted the runner's base rootfs; the environment cache did nothing")
	}

	// And it really is a copy of the published image, not of the base.
	copies := cloner.copies()
	last := copies[len(copies)-1]
	if last.src != blob {
		t.Errorf("the cached session's rootfs was cloned from %q, want %q", last.src, blob)
	}
	data, err := os.ReadFile(cfg.RootfsPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != built {
		t.Errorf("the cached session's root filesystem is %q, want the cached environment %q", data, built)
	}

	// The two sessions have root filesystems of their own: the cache is
	// copy-on-write, not shared.
	builderRootfs, err := m.sessionRootfsPath(builder.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RootfsPath == builderRootfs || cfg.RootfsPath == blob {
		t.Fatalf("the cached session boots %q, which is another session's rootfs or the image itself", cfg.RootfsPath)
	}
}

// TestMicrovmCreateRefusesAnImageRefThatNamesAPath is the containment rule,
// and it is a cross-tenant one.
//
// Spec.Image is carried from an environment somebody DECLARED — the wire
// checks only that it is non-empty — so a path branch that accepted any
// readable file would let a create name another session's workspace disk or
// another creator's agent home. Since this change a session's rootfs is
// writable and Snapshot publishes it by digest, so the sequence would be:
// boot a copy of somebody else's workspace, read it, and publish it under an
// environment ref that every later session of that environment boots.
// "Excluded by construction" (ADR-0003 §2.7 item 3) is only true while nothing
// can ask for them by name.
func TestMicrovmCreateRefusesAnImageRefThatNamesAPath(t *testing.T) {
	m, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 4})
	ctx := context.Background()

	victim, err := m.Create(ctx, Spec{
		SessionID: "sess-victim",
		Home:      &HomeMount{Volume: "home-victim", Path: "/rainier/agents"},
	})
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := m.workspaceDiskPath("sess-victim")
	if err != nil {
		t.Fatal(err)
	}
	home, err := m.homeDiskPath("home-victim")
	if err != nil {
		t.Fatal(err)
	}
	victimRootfs, err := m.sessionRootfsPath(victim.ID)
	if err != nil {
		t.Fatal(err)
	}
	elsewhere := filepath.Join(t.TempDir(), "somebody-elses.ext4")
	if err := os.WriteFile(elsewhere, []byte("not an environment image"), 0o600); err != nil {
		t.Fatal(err)
	}

	for _, ref := range []string{
		workspace,
		home,
		victimRootfs,
		elsewhere,
		filepath.Join(m.opts.StateDir, "instances", victim.ID, "instance.json"),
		"../../etc/passwd",
		"rainier-env:x/../../y",
	} {
		h, err := m.Create(ctx, Spec{SessionID: "sess-attacker", Image: ref})
		if err == nil {
			t.Errorf("a create booted %q, which is not an environment image", ref)
			_ = m.Destroy(ctx, h.ID)
			continue
		}
	}
	// The one path a create may name is the runner's own base rootfs, which is
	// what an empty Spec.Image resolves to — so the rule above cannot have
	// broken the ordinary create.
	if _, err := m.Create(ctx, Spec{SessionID: "sess-ordinary", Image: m.opts.BaseRootfs}); err != nil {
		t.Errorf("a create naming this runner's own --rootfs was refused: %v", err)
	}
}

// TestMicrovmPrepullThenCreateBootsTheFetchedImage is the path a fleet
// actually takes: controld dispatches a prepull ahead of the create, the
// create resolves the same ref, and nothing is fetched twice.
func TestMicrovmPrepullThenCreateBootsTheFetchedImage(t *testing.T) {
	const ref = "rainier-env:env7-cached"
	const content = "an environment image built in CI"
	src := newDirSource(t, map[string][]byte{ref: []byte(content)})
	m, sim, cloner := testMicrovmCloning(t, MicrovmOpts{TotalSlots: 4, ImageSource: src})
	ctx := context.Background()

	if err := m.Prepull(ctx, ref); err != nil {
		t.Fatalf("Prepull: %v", err)
	}
	manifest, ok := m.images.manifest(ref)
	if !ok {
		t.Fatal("the prepull published no manifest")
	}

	h, err := m.Create(ctx, Spec{SessionID: "sess-prepulled", Image: ref})
	if err != nil {
		t.Fatalf("create from a prepulled ref: %v", err)
	}
	cfg, _ := sim.Config(h.ID)
	if cfg.BaseImageDigest != manifest.Digest {
		t.Errorf("the session booted %s, want the prepulled %s", cfg.BaseImageDigest, manifest.Digest)
	}
	data, err := os.ReadFile(cfg.RootfsPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != content {
		t.Errorf("the session's rootfs is %q, want the fetched image %q", data, content)
	}
	if got := cloner.copies()[0].src; got != cfg.BaseImagePath {
		t.Errorf("cloned from %q, want the fetched image %q", got, cfg.BaseImagePath)
	}
}

// TestMicrovmCreateRefusesARefWhoseImageIsGone: a manifest is a promise about
// bytes this host has, and a ref whose image an operator pruned resolves to
// nothing rather than to the runner's base rootfs. Falling back would boot a
// session into an environment without its setup, silently — the one failure
// the cache exists to make impossible.
func TestMicrovmCreateRefusesARefWhoseImageIsGone(t *testing.T) {
	m, _ := testMicrovm(t, MicrovmOpts{TotalSlots: 4})
	m.SetHost(&stubMicrovmHost{})
	ctx := context.Background()

	h, err := m.Create(ctx, Spec{SessionID: "sess-pruned"})
	if err != nil {
		t.Fatal(err)
	}
	const ref = "rainier-env:pruned"
	if _, err := m.Snapshot(ctx, h.ID, ref, nil); err != nil {
		t.Fatal(err)
	}
	manifest, ok := m.images.manifest(ref)
	if !ok {
		t.Fatal("no manifest")
	}
	blob, _ := m.images.have(manifest.Digest)
	if err := os.Remove(blob); err != nil {
		t.Fatal(err)
	}

	_, err = m.Create(ctx, Spec{SessionID: "sess-after-prune", Image: ref})
	if err == nil {
		t.Fatal("a create resolved a ref whose image is not on this host")
	}
	if !strings.Contains(err.Error(), ref) {
		t.Errorf("error = %q, want it to name the ref", err)
	}
	if used, _, _ := m.Capacity(ctx); used != 1 {
		t.Errorf("used = %d, want only the first session's slot", used)
	}
}

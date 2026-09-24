// internal/driver/microvm_sim_test.go
//
// The microVM driver's test seam: a hypervisor and a disk formatter with no
// host behind either. The third member of the seam, the network host, is
// netslot.FakeHost — it lives in the netslot package because the pool's own
// tests need it too.
//
// They live in a _test.go file rather than beside the driver so that the
// question "can a production path reach the simulated engine?" is answered by
// the compiler instead of by review. MicrovmOpts requires all three to be
// injected together or not at all; this file is the only place that has them
// to inject.
//
// None of it is a MODEL of Firecracker. SimulatedEngine reports a stopped VM
// as VMMStateStopped where the real engine reports VMMStateGone, because a
// terminated Firecracker leaves no process to ask; a test that turns on that
// difference wraps this engine and corrects it (see goneWhenStoppedEngine).
package driver

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// SimulatedDiskFormatter is the formatting half of the seam.
//
// FormatFromDir writes the tree it was given into the image file as a TAR,
// which is not an ext4 and does not pretend to be one — the point is that a
// test can read back what a restore put in, and that a fixture which never
// looked at the directory would be a fixture that passed while the restore was
// empty. What a real host does is mkfs.ext4 -d (Ext4Formatter), and only a real
// host can evidence that.
type SimulatedDiskFormatter struct{}

func (SimulatedDiskFormatter) Format(string) error { return nil }

func (SimulatedDiskFormatter) FormatFromDir(path, dir string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	tw := tar.NewWriter(f)
	err = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || p == dir {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		link := ""
		if info.Mode()&fs.ModeSymlink != 0 {
			if link, err = os.Readlink(p); err != nil {
				return err
			}
		}
		hdr, err := tar.FileInfoHeader(info, link)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		hdr.Name = filepath.ToSlash(rel)
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		src, err := os.Open(p)
		if err != nil {
			return err
		}
		defer src.Close()
		_, err = io.Copy(tw, src)
		return err
	})
	if err != nil {
		return err
	}
	return tw.Close()
}

// ---------------------------------------------------------------------------
// Simulated Engine (test seam)
// ---------------------------------------------------------------------------

// SimulatedEngine is the hypervisor half of the test seam: a state machine
// with no VM behind it. It is never constructed by production code; see
// MicrovmOpts. It exists so the shared driver contract (RunContract) can be
// run against this driver's own lifecycle bookkeeping on a machine with no
// KVM, which is every developer machine and all of CI.
//
// It is NOT a model of Firecracker. Most importantly it reports a stopped VM
// as VMMStateStopped, where the real engine reports VMMStateGone because a
// terminated Firecracker leaves no process to ask. A test that cares about
// that difference injects an engine answering the way the real one does.
type SimulatedEngine struct {
	mu         sync.Mutex
	stateDir   string
	states     map[string]VMMState
	configs    map[string]VMMConfig
	failOnStop map[string]error
	failLaunch error
}

func NewSimulatedEngine() *SimulatedEngine {
	return NewSimulatedEngineWithDir("")
}

func NewSimulatedEngineWithDir(stateDir string) *SimulatedEngine {
	s := &SimulatedEngine{
		stateDir:   stateDir,
		states:     make(map[string]VMMState),
		configs:    make(map[string]VMMConfig),
		failOnStop: make(map[string]error),
	}
	if stateDir != "" {
		s.recoverStates()
	}
	return s
}

func (s *SimulatedEngine) stateFilePath(id string) string {
	if s.stateDir == "" {
		return ""
	}
	return filepath.Join(s.stateDir, "instances", id, "vmm_sim_state.txt")
}

func (s *SimulatedEngine) recoverStates() {
	entries, err := os.ReadDir(filepath.Join(s.stateDir, "instances"))
	if err != nil {
		return
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		id := entry.Name()
		if data, err := os.ReadFile(s.stateFilePath(id)); err == nil {
			s.states[id] = VMMState(strings.TrimSpace(string(data)))
		}
	}
}

func (s *SimulatedEngine) saveState(id string, st VMMState) {
	if s.stateDir == "" {
		return
	}
	path := s.stateFilePath(id)
	_ = os.MkdirAll(filepath.Dir(path), microvmDirMode)
	_ = os.WriteFile(path, []byte(string(st)), microvmFileMode)
}

// Config returns the VMMConfig this engine was last launched with for id.
func (s *SimulatedEngine) Config(id string) (VMMConfig, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg, ok := s.configs[id]
	return cfg, ok
}

// SetState forces the simulated hypervisor's view of id, standing in for an
// event the driver did not cause — a VMM that crashed, say.
func (s *SimulatedEngine) SetState(id string, st VMMState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[id] = st
	s.saveState(id, st)
}

// FailOnStop makes every Stop of id fail with err, standing in for a VMM that
// will not die. A nil err clears it.
func (s *SimulatedEngine) FailOnStop(id string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err == nil {
		delete(s.failOnStop, id)
		return
	}
	s.failOnStop[id] = err
}

// FailLaunch makes every Launch fail with err, standing in for a VMM that
// will not start — the one case that leaves a half-configured boot behind.
// A nil err clears it.
func (s *SimulatedEngine) FailLaunch(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failLaunch = err
}

func (s *SimulatedEngine) Launch(_ context.Context, cfg VMMConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failLaunch != nil {
		return s.failLaunch
	}
	s.states[cfg.ID] = VMMStateRunning
	s.configs[cfg.ID] = cfg
	s.saveState(cfg.ID, VMMStateRunning)
	return nil
}

func (s *SimulatedEngine) Pause(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[id] = VMMStatePaused
	s.saveState(id, VMMStatePaused)
	return nil
}

// Resume is strict about what it will unpause, because Firecracker is: PATCH
// /vm {"state": "Resumed"} against a VM that was never paused answers 400.
// Modelling that here is what lets the shared contract's "resuming an
// already-running container restarts nothing" subtest actually fail — a
// driver that called through unconditionally would get an error from the real
// engine and a shrug from a lenient fake.
func (s *SimulatedEngine) Resume(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch s.states[id] {
	case VMMStatePaused:
		s.states[id] = VMMStateRunning
		s.saveState(id, VMMStateRunning)
		return nil
	case VMMStateRunning:
		return fmt.Errorf("simulated firecracker: PATCH /vm Resumed on %s, which is not paused", id)
	default:
		return fmt.Errorf("simulated firecracker: no live vm %s to resume", id)
	}
}

func (s *SimulatedEngine) Stop(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err, ok := s.failOnStop[id]; ok {
		return err
	}
	s.states[id] = VMMStateStopped
	s.saveState(id, VMMStateStopped)
	return nil
}

// There is no Snapshot here either, and for the same reason the real engine
// has none: publishing an environment image is a copy of a file the DRIVER
// owns, so the contract's snapshot subtests run against the real image store
// with this engine merely pausing and resuming underneath them.

func (s *SimulatedEngine) State(_ context.Context, id string) (VMMState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.states[id]
	if !ok {
		return VMMStateGone, nil
	}
	return st, nil
}

func (s *SimulatedEngine) PID(_ string) int {
	return 0
}

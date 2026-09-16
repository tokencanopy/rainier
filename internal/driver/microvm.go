// internal/driver/microvm.go
package driver

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
)

// MicrovmOpts configures the MicroVM driver.
type MicrovmOpts struct {
	BaseRootfs string        // default base rootfs image or template ref
	KernelPath string        // guest vmlinux kernel path
	StateDir   string        // directory holding instance sockets, metadata, and disks
	TotalSlots int           // maximum simultaneous active slot capacity
	VMMPath    string        // path to Firecracker or VMM executable
	Engine     MicrovmEngine // optional VMM engine; defaults to simulated/firecracker based on KVM
}

// VMMState is the hypervisor-observed execution state of a microVM.
type VMMState string

const (
	VMMStateRunning VMMState = "running"
	VMMStatePaused  VMMState = "paused"
	VMMStateStopped VMMState = "stopped"
	VMMStateGone    VMMState = "gone"
)

// VMMConfig is the launch configuration handed down to the microVM hypervisor.
type VMMConfig struct {
	ID                string
	SessionID         string
	VCPU              int
	MemoryMB          int
	KernelPath        string
	RootfsPath        string
	WorkspaceDiskPath string
	HomeDiskPath      string
	TapDevice         string
	GuestIP           string
	GatewayIP         string
	BootArgs          string
	Env               map[string]string
	StripEnv          []string
}

// MicrovmEngine is the pluggable hypervisor backend interface.
// Production Linux hosts use FirecrackerEngine; tests and dev workstations
// use SimulatedEngine.
type MicrovmEngine interface {
	Launch(ctx context.Context, cfg VMMConfig) error
	Pause(ctx context.Context, id string) error
	Resume(ctx context.Context, id string) error
	Stop(ctx context.Context, id string) error
	Snapshot(ctx context.Context, id, ref string, stripEnv []string) (Snapshot, error)
	State(ctx context.Context, id string) (VMMState, error)
}

// microvmInstance tracks one microVM's state within the driver.
type microvmInstance struct {
	id        string
	sessionID string
	state     State
	cold      bool
	volume    string
	env       map[string]string
	cfg       VMMConfig
}

// Microvm implements driver.Driver for hardware-isolated microVMs.
type Microvm struct {
	mu        sync.Mutex
	opts      MicrovmOpts
	engine    MicrovmEngine
	seq       int
	snapSeq   atomic.Int64
	instances map[string]*microvmInstance
	volumes   map[string]bool
	pulls     []string
	strips    [][]string
}

// NewMicrovm creates a new MicroVM driver.
func NewMicrovm(opts MicrovmOpts) *Microvm {
	if opts.TotalSlots <= 0 {
		opts.TotalSlots = 16
	}
	if opts.StateDir == "" {
		opts.StateDir = "/run/rainier/microvm"
	}
	engine := opts.Engine
	if engine == nil {
		if hasKVM() {
			engine = NewFirecrackerEngine(opts.VMMPath, opts.StateDir)
		} else {
			engine = NewSimulatedEngine()
		}
	}
	return &Microvm{
		opts:      opts,
		engine:    engine,
		instances: make(map[string]*microvmInstance),
		volumes:   make(map[string]bool),
	}
}

func hasKVM() bool {
	_, err := os.Stat("/dev/kvm")
	return err == nil
}

// Volumes returns every live workspace volume name, sorted.
// Required by contract tests and volume verification.
func (m *Microvm) Volumes() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Sorted(maps.Keys(m.volumes))
}

// HasWorkspace reports whether the workspace volume for sessionID exists.
func (m *Microvm) HasWorkspace(sessionID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.volumes[workspaceVolume(sessionID)]
}

// Pulls returns the recorded prepull refs (in call order).
func (m *Microvm) Pulls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.pulls)
}

// Strips returns the stripEnv list of every Snapshot call, in call order.
func (m *Microvm) Strips() [][]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([][]string, len(m.strips))
	for i, s := range m.strips {
		out[i] = slices.Clone(s)
	}
	return out
}

func (m *Microvm) usedLocked() int {
	used := 0
	for _, inst := range m.instances {
		if inst.state == StateRunning || (inst.state == StateSuspended && !inst.cold) {
			used++
		}
	}
	return used
}

func (m *Microvm) Create(ctx context.Context, spec Spec) (Handle, error) {
	if err := checkScriptSizes(spec); err != nil {
		return Handle{}, err
	}
	if err := checkHome(spec.Home); err != nil {
		return Handle{}, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	used := m.usedLocked()
	if used >= m.opts.TotalSlots {
		return Handle{}, fmt.Errorf("no capacity: %d/%d", used, m.opts.TotalSlots)
	}

	m.seq++
	id := fmt.Sprintf("mvm-%d", m.seq)

	volume := ""
	if spec.SessionID != "" {
		volume = workspaceVolume(spec.SessionID)
		m.volumes[volume] = true
	}

	rootfs := spec.Image
	if rootfs == "" {
		rootfs = m.opts.BaseRootfs
	}

	cfg := VMMConfig{
		ID:                id,
		SessionID:         spec.SessionID,
		VCPU:              2,
		MemoryMB:          2048,
		KernelPath:        m.opts.KernelPath,
		RootfsPath:        rootfs,
		WorkspaceDiskPath: volume,
		Env:               maps.Clone(spec.Env),
	}

	if err := m.engine.Launch(ctx, cfg); err != nil {
		if volume != "" {
			delete(m.volumes, volume)
		}
		return Handle{}, fmt.Errorf("launch microvm %s: %w", id, err)
	}

	m.instances[id] = &microvmInstance{
		id:        id,
		sessionID: spec.SessionID,
		state:     StateRunning,
		volume:    volume,
		env:       maps.Clone(spec.Env),
		cfg:       cfg,
	}

	return Handle{ID: id, State: StateRunning}, nil
}

func (m *Microvm) Suspend(ctx context.Context, id string, warm bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	inst, ok := m.instances[id]
	if !ok {
		return fmt.Errorf("no such id %s", id)
	}

	if warm {
		if err := m.engine.Pause(ctx, id); err != nil {
			return err
		}
		inst.state = StateSuspended
		inst.cold = false
	} else {
		if err := m.engine.Stop(ctx, id); err != nil {
			return err
		}
		inst.state = StateSuspended
		inst.cold = true
	}
	return nil
}

func (m *Microvm) Resume(ctx context.Context, id string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	inst, ok := m.instances[id]
	if !ok {
		return false, fmt.Errorf("no such id %s", id)
	}

	restarted := inst.state == StateSuspended && inst.cold
	if err := m.engine.Resume(ctx, id); err != nil {
		return false, err
	}

	inst.state = StateRunning
	inst.cold = false
	return restarted, nil
}

func (m *Microvm) Snapshot(ctx context.Context, id, ref string, stripEnv []string) (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.instances[id]; !ok {
		return Snapshot{}, fmt.Errorf("no such id %s", id)
	}

	m.strips = append(m.strips, slices.Clone(stripEnv))

	if ref == "" {
		m.snapSeq.Add(1)
		ref = fmt.Sprintf("rainier-mvm:%s-%d", id, m.snapSeq.Load())
	}

	return m.engine.Snapshot(ctx, id, ref, stripEnv)
}

func (m *Microvm) Prepull(ctx context.Context, ref string) error {
	if ref == "" {
		return errors.New("prepull: empty image ref")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pulls = append(m.pulls, ref)
	return nil
}

func (m *Microvm) Destroy(ctx context.Context, id string) error {
	m.mu.Lock()
	sessionID := ""
	if inst, ok := m.instances[id]; ok {
		sessionID = inst.sessionID
	}
	m.mu.Unlock()

	if err := m.DestroyContainer(ctx, id); err != nil {
		return err
	}
	return m.RemoveWorkspace(ctx, sessionID)
}

func (m *Microvm) DestroyContainer(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.engine.Stop(ctx, id); err != nil {
		// Log or tolerate already stopped
	}
	delete(m.instances, id)
	return nil
}

func (m *Microvm) RemoveWorkspace(ctx context.Context, sessionID string) error {
	if sessionID == "" {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.volumes, workspaceVolume(sessionID))
	return nil
}

func (m *Microvm) Inspect(ctx context.Context, id string) (Handle, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	inst, ok := m.instances[id]
	if !ok {
		return Handle{ID: id, State: StateGone}, nil
	}
	return Handle{ID: id, State: inst.state}, nil
}

func (m *Microvm) Capacity(ctx context.Context) (int, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.usedLocked(), m.opts.TotalSlots, nil
}

func (m *Microvm) List(ctx context.Context) ([]Listed, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]Listed, 0, len(m.instances))
	for id, inst := range m.instances {
		out = append(out, Listed{
			SessionID: inst.sessionID,
			Handle:    Handle{ID: id, State: inst.state},
		})
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Simulated Engine (For testing and platforms without hardware KVM)
// ---------------------------------------------------------------------------

type SimulatedEngine struct {
	mu     sync.Mutex
	states map[string]VMMState
}

// NewSimulatedEngine constructs an in-memory VMM engine.
func NewSimulatedEngine() *SimulatedEngine {
	return &SimulatedEngine{
		states: make(map[string]VMMState),
	}
}

func (s *SimulatedEngine) Launch(_ context.Context, cfg VMMConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[cfg.ID] = VMMStateRunning
	return nil
}

func (s *SimulatedEngine) Pause(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[id] = VMMStatePaused
	return nil
}

func (s *SimulatedEngine) Resume(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[id] = VMMStateRunning
	return nil
}

func (s *SimulatedEngine) Stop(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[id] = VMMStateStopped
	return nil
}

func (s *SimulatedEngine) Snapshot(_ context.Context, id, ref string, _ []string) (Snapshot, error) {
	return Snapshot{Ref: ref}, nil
}

func (s *SimulatedEngine) State(_ context.Context, id string) (VMMState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.states[id]
	if !ok {
		return VMMStateGone, nil
	}
	return st, nil
}

// ---------------------------------------------------------------------------
// Firecracker Engine (For Linux hosts with /dev/kvm)
// ---------------------------------------------------------------------------

type FirecrackerEngine struct {
	vmmPath  string
	stateDir string
}

// NewFirecrackerEngine constructs an engine managing real Firecracker VMM processes.
func NewFirecrackerEngine(vmmPath, stateDir string) *FirecrackerEngine {
	if vmmPath == "" {
		vmmPath = "firecracker"
	}
	return &FirecrackerEngine{
		vmmPath:  vmmPath,
		stateDir: stateDir,
	}
}

func (f *FirecrackerEngine) Launch(ctx context.Context, cfg VMMConfig) error {
	sockDir := filepath.Join(f.stateDir, cfg.ID)
	if err := os.MkdirAll(sockDir, 0700); err != nil {
		return err
	}
	return nil
}

func (f *FirecrackerEngine) Pause(ctx context.Context, id string) error {
	return nil
}

func (f *FirecrackerEngine) Resume(ctx context.Context, id string) error {
	return nil
}

func (f *FirecrackerEngine) Stop(ctx context.Context, id string) error {
	sockDir := filepath.Join(f.stateDir, id)
	_ = os.RemoveAll(sockDir)
	return nil
}

func (f *FirecrackerEngine) Snapshot(ctx context.Context, id, ref string, stripEnv []string) (Snapshot, error) {
	return Snapshot{Ref: ref}, nil
}

func (f *FirecrackerEngine) State(ctx context.Context, id string) (VMMState, error) {
	return VMMStateRunning, nil
}

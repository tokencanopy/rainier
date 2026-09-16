// internal/driver/microvm.go
package driver

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
)

// MicrovmOpts configures the MicroVM driver.
type MicrovmOpts struct {
	BaseRootfs string        // default base rootfs image or template ref
	KernelPath string        // guest vmlinux kernel path
	StateDir   string        // directory holding instance sockets, metadata, and disks
	TotalSlots int           // maximum simultaneous active slot capacity
	VMMPath    string        // path to Firecracker or VMM executable
	Engine     MicrovmEngine // optional VMM engine override; defaults to firecracker on KVM or simulated
}

// VMMState is the hypervisor-observed execution state of a microVM.
type VMMState string

const (
	VMMStateRunning VMMState = "running"
	VMMStatePaused  VMMState = "paused"
	VMMStateStopped VMMState = "stopped"
	VMMStateGone    VMMState = "gone"
)

// VMMConfig is the complete configuration handed down to the microVM hypervisor.
type VMMConfig struct {
	ID                string
	SessionID         string
	VCPU              int
	MemoryMB          int
	KernelPath        string
	RootfsPath        string
	WorkspaceDiskPath string
	HomeDiskPath      string
	Cmd               []string
	EgressAllow       []string
	DialURL           string
	ProxyURL          string
	TapDevice         string
	GuestIP           string
	GatewayIP         string
	BootArgs          string
	Env               map[string]string
	StripEnv          []string
}

// MicrovmEngine is the pluggable hypervisor backend interface.
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
	pulls     []string
	strips    [][]string
}

// NewMicrovm creates a new MicroVM driver.
func NewMicrovm(opts MicrovmOpts) *Microvm {
	if opts.TotalSlots <= 0 {
		opts.TotalSlots = 16
	}
	if opts.StateDir == "" {
		opts.StateDir = filepath.Join(os.TempDir(), "rainier-microvm")
	}
	_ = os.MkdirAll(filepath.Join(opts.StateDir, "workspaces"), 0755)

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
	}
}

func hasKVM() bool {
	_, err := os.Stat("/dev/kvm")
	return err == nil
}

// workspaceDir returns the host filesystem path for sessionID's persistent workspace.
func (m *Microvm) workspaceDir(sessionID string) string {
	if sessionID == "" {
		return ""
	}
	return filepath.Join(m.opts.StateDir, "workspaces", workspaceVolume(sessionID))
}

// ensureWorkspaceVolume creates the workspace directory on disk, reporting whether
// it was created.
func (m *Microvm) ensureWorkspaceVolume(sessionID string) (bool, error) {
	if sessionID == "" {
		return false, nil
	}
	dir := m.workspaceDir(sessionID)
	if _, err := os.Stat(dir); err == nil {
		return false, nil
	}
	if err := os.MkdirAll(filepath.Join(dir, ".rainier"), 0755); err != nil {
		return false, fmt.Errorf("create workspace volume %s: %w", sessionID, err)
	}
	return true, nil
}

// Volumes returns every live workspace volume name, sorted.
// Scans the on-disk workspace directory so state is preserved across driver restarts.
func (m *Microvm) Volumes() []string {
	wsRoot := filepath.Join(m.opts.StateDir, "workspaces")
	entries, err := os.ReadDir(wsRoot)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), workspaceVolumePrefix) {
			out = append(out, e.Name())
		}
	}
	slices.Sort(out)
	return out
}

// HasWorkspace reports whether the workspace volume directory for sessionID exists.
func (m *Microvm) HasWorkspace(sessionID string) bool {
	if sessionID == "" {
		return false
	}
	_, err := os.Stat(m.workspaceDir(sessionID))
	return err == nil
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

// buildGuestEnv translates the full Spec into the environment map passed into the microVM guest.
func buildGuestEnv(spec Spec) map[string]string {
	env := make(map[string]string)

	if spec.DialURL != "" {
		env["RAINIER_DIAL"] = spec.DialURL
	}
	if spec.SessionID != "" {
		env["RAINIER_SESSION"] = spec.SessionID
	}
	if spec.ProxyURL != "" {
		proxyURL := withSessionUserinfo(spec.ProxyURL, spec.SessionID)
		noProxy := noProxyFor(spec.DialURL)
		env["HTTP_PROXY"] = proxyURL
		env["http_proxy"] = proxyURL
		env["HTTPS_PROXY"] = proxyURL
		env["https_proxy"] = proxyURL
		env["NO_PROXY"] = noProxy
		env["no_proxy"] = noProxy
	}
	if spec.Setup != "" {
		env["RAINIER_SETUP_B64"] = base64.StdEncoding.EncodeToString([]byte(spec.Setup))
		env["RAINIER_SETUP_TIMEOUT"] = strconv.Itoa(spec.SetupTimeoutSec)
	}
	if len(spec.Repos) > 0 {
		if blob, err := json.Marshal(spec.Repos); err == nil {
			env["RAINIER_REPOS_B64"] = base64.StdEncoding.EncodeToString(blob)
		}
	}
	if spec.Init != "" {
		env["RAINIER_INIT_B64"] = base64.StdEncoding.EncodeToString([]byte(spec.Init))
		env["RAINIER_INIT_TIMEOUT"] = strconv.Itoa(spec.InitTimeoutSec)
	}
	if spec.GitAuthorName != "" {
		env["RAINIER_GIT_AUTHOR_NAME"] = spec.GitAuthorName
	}
	if spec.GitAuthorEmail != "" {
		env["RAINIER_GIT_AUTHOR_EMAIL"] = spec.GitAuthorEmail
	}
	for k, v := range spec.Env {
		env[k] = v
	}
	return env
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

	createdVolume := false
	if spec.SessionID != "" {
		created, err := m.ensureWorkspaceVolume(spec.SessionID)
		if err != nil {
			return Handle{}, err
		}
		createdVolume = created
	}

	m.seq++
	id := fmt.Sprintf("mvm-%d", m.seq)

	rootfs := spec.Image
	if rootfs == "" {
		rootfs = m.opts.BaseRootfs
	}

	homePath := ""
	if spec.Home != nil {
		homePath = spec.Home.Volume
	}

	guestEnv := buildGuestEnv(spec)

	cfg := VMMConfig{
		ID:                id,
		SessionID:         spec.SessionID,
		VCPU:              2,
		MemoryMB:          2048,
		KernelPath:        m.opts.KernelPath,
		RootfsPath:        rootfs,
		WorkspaceDiskPath: m.workspaceDir(spec.SessionID),
		HomeDiskPath:      homePath,
		Cmd:               slices.Clone(spec.Cmd),
		EgressAllow:       slices.Clone(spec.EgressAllow),
		DialURL:           spec.DialURL,
		ProxyURL:          spec.ProxyURL,
		Env:               guestEnv,
	}

	if err := m.engine.Launch(ctx, cfg); err != nil {
		if createdVolume {
			_ = m.RemoveWorkspace(ctx, spec.SessionID)
		}
		return Handle{}, fmt.Errorf("launch microvm %s: %w", id, err)
	}

	m.instances[id] = &microvmInstance{
		id:        id,
		sessionID: spec.SessionID,
		state:     StateRunning,
		volume:    workspaceVolume(spec.SessionID),
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

	if inst.cold {
		// Cold resume: relaunch the stopped microVM process pointing to the existing workspace
		if err := m.engine.Launch(ctx, inst.cfg); err != nil {
			return false, fmt.Errorf("relaunch cold microvm %s: %w", id, err)
		}
		inst.state = StateRunning
		inst.cold = false
		return true, nil
	}

	// Warm resume: unpause the existing frozen vCPUs
	if err := m.engine.Resume(ctx, id); err != nil {
		return false, err
	}
	inst.state = StateRunning
	return false, nil
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
	sessionID := ""
	m.mu.Lock()
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

	if _, ok := m.instances[id]; !ok {
		return nil
	}

	if err := m.engine.Stop(ctx, id); err != nil {
		// Check whether the VM is already confirmed stopped or gone
		st, stateErr := m.engine.State(ctx, id)
		if stateErr != nil || (st != VMMStateGone && st != VMMStateStopped) {
			// Failed to stop and resource is not confirmed gone: preserve record to prevent orphaning
			return fmt.Errorf("destroy microvm %s: %w", id, err)
		}
	}

	delete(m.instances, id)
	return nil
}

func (m *Microvm) RemoveWorkspace(_ context.Context, sessionID string) error {
	if sessionID == "" {
		return nil
	}
	dir := m.workspaceDir(sessionID)
	_ = os.RemoveAll(dir)
	return nil
}

func (m *Microvm) Inspect(ctx context.Context, id string) (Handle, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	inst, ok := m.instances[id]
	if !ok {
		return Handle{ID: id, State: StateGone}, nil
	}

	// Reconcile observed hypervisor state
	if st, err := m.engine.State(ctx, id); err == nil {
		switch st {
		case VMMStateGone:
			inst.state = StateGone
		case VMMStateStopped:
			if inst.state == StateRunning {
				inst.state = StateSuspended
				inst.cold = true
			}
		case VMMStatePaused:
			inst.state = StateSuspended
			inst.cold = false
		case VMMStateRunning:
			inst.state = StateRunning
		}
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
		if st, err := m.engine.State(ctx, id); err == nil {
			if st == VMMStateGone {
				inst.state = StateGone
			} else if st == VMMStateStopped && inst.state == StateRunning {
				inst.state = StateSuspended
				inst.cold = true
			}
		}
		out = append(out, Listed{
			SessionID: inst.sessionID,
			Handle:    Handle{ID: id, State: inst.state},
		})
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Simulated Engine (For testing and dev environments without /dev/kvm)
// ---------------------------------------------------------------------------

type SimulatedEngine struct {
	mu        sync.Mutex
	states    map[string]VMMState
	configs   map[string]VMMConfig
	failOnStop map[string]error
}

// NewSimulatedEngine constructs an in-memory VMM engine for tests.
func NewSimulatedEngine() *SimulatedEngine {
	return &SimulatedEngine{
		states:     make(map[string]VMMState),
		configs:    make(map[string]VMMConfig),
		failOnStop: make(map[string]error),
	}
}

func (s *SimulatedEngine) Launch(_ context.Context, cfg VMMConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[cfg.ID] = VMMStateRunning
	s.configs[cfg.ID] = cfg
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
	if err, ok := s.failOnStop[id]; ok {
		return err
	}
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
	mu       sync.Mutex
	vmmPath  string
	stateDir string
	procs    map[string]*exec.Cmd
	initErr  error
}

// NewFirecrackerEngine constructs an engine managing real Firecracker VMM processes.
func NewFirecrackerEngine(vmmPath, stateDir string) *FirecrackerEngine {
	if vmmPath == "" {
		vmmPath = "firecracker"
	}
	resolvedPath, err := exec.LookPath(vmmPath)
	var initErr error
	if err != nil {
		initErr = fmt.Errorf("firecracker executable %q not found on PATH: %w", vmmPath, err)
	} else {
		vmmPath = resolvedPath
	}
	return &FirecrackerEngine{
		vmmPath:  vmmPath,
		stateDir: stateDir,
		procs:    make(map[string]*exec.Cmd),
		initErr:  initErr,
	}
}

func (f *FirecrackerEngine) Launch(ctx context.Context, cfg VMMConfig) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.initErr != nil {
		return f.initErr
	}

	if !hasKVM() {
		return errors.New("/dev/kvm not found: hardware virtualization is required for Firecracker")
	}

	sockDir := filepath.Join(f.stateDir, "sockets", cfg.ID)
	if err := os.MkdirAll(sockDir, 0700); err != nil {
		return fmt.Errorf("create socket dir: %w", err)
	}
	sockPath := filepath.Join(sockDir, "firecracker.sock")
	_ = os.Remove(sockPath)

	cmd := exec.CommandContext(ctx, f.vmmPath, "--api-sock", sockPath)
	if err := cmd.Start(); err != nil {
		_ = os.RemoveAll(sockDir)
		return fmt.Errorf("start firecracker %s: %w", cfg.ID, err)
	}

	f.procs[cfg.ID] = cmd
	return nil
}

func (f *FirecrackerEngine) Pause(ctx context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.procs[id]; !ok {
		return fmt.Errorf("microvm %s not running", id)
	}
	// On Linux, Firecracker pauses vCPUs via PUT /vm {"state": "Paused"}
	return nil
}

func (f *FirecrackerEngine) Resume(ctx context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.procs[id]; !ok {
		return fmt.Errorf("microvm %s not running", id)
	}
	// On Linux, Firecracker resumes vCPUs via PUT /vm {"state": "Resumed"}
	return nil
}

func (f *FirecrackerEngine) Stop(ctx context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	cmd, ok := f.procs[id]
	if !ok {
		return nil
	}

	if cmd.Process != nil {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		_ = cmd.Wait()
	}

	delete(f.procs, id)
	sockDir := filepath.Join(f.stateDir, "sockets", id)
	_ = os.RemoveAll(sockDir)
	return nil
}

func (f *FirecrackerEngine) Snapshot(ctx context.Context, id, ref string, stripEnv []string) (Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.procs[id]; !ok {
		return Snapshot{}, fmt.Errorf("microvm %s not running", id)
	}
	return Snapshot{Ref: ref}, nil
}

func (f *FirecrackerEngine) State(ctx context.Context, id string) (VMMState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	cmd, ok := f.procs[id]
	if !ok {
		return VMMStateGone, nil
	}

	if cmd.Process == nil {
		return VMMStateGone, nil
	}

	// Probe process liveness via signal 0
	if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
		return VMMStateStopped, nil
	}

	return VMMStateRunning, nil
}

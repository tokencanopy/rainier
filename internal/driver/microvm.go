// internal/driver/microvm.go
package driver

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// MicrovmOpts configures the MicroVM driver.
type MicrovmOpts struct {
	BaseRootfs string        // default base ext4 rootfs image path
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
	ID                string            `json:"id"`
	SessionID         string            `json:"session_id"`
	VCPU              int               `json:"vcpu"`
	MemoryMB          int               `json:"memory_mb"`
	KernelPath        string            `json:"kernel_path"`
	RootfsPath        string            `json:"rootfs_path"`
	WorkspaceDiskPath string            `json:"workspace_disk_path"`
	HomeDiskPath      string            `json:"home_disk_path"`
	Cmd               []string          `json:"cmd"`
	EgressAllow       []string          `json:"egress_allow"`
	DialURL           string            `json:"dial_url"`
	ProxyURL          string            `json:"proxy_url"`
	TapDevice         string            `json:"tap_device"`
	GuestIP           string            `json:"guest_ip"`
	GatewayIP         string            `json:"gateway_ip"`
	BootArgs          string            `json:"boot_args"`
	Env               map[string]string `json:"env"`
	StripEnv          []string          `json:"strip_env"`
}

// MicrovmEngine is the pluggable hypervisor backend interface.
type MicrovmEngine interface {
	Launch(ctx context.Context, cfg VMMConfig) error
	Pause(ctx context.Context, id string) error
	Resume(ctx context.Context, id string) error
	Stop(ctx context.Context, id string) error
	Snapshot(ctx context.Context, id, ref string, stripEnv []string) (Snapshot, error)
	State(ctx context.Context, id string) (VMMState, error)
	PID(id string) int
}

// instanceRecord is the persistent metadata stored on disk for each microVM instance.
type instanceRecord struct {
	ID        string    `json:"id"`
	SessionID string    `json:"session_id"`
	State     State     `json:"state"`
	Cold      bool      `json:"cold"`
	Volume    string    `json:"volume"`
	PID       int       `json:"pid"`
	Cfg       VMMConfig `json:"cfg"`
}

// snapshotManifest associates a snapshot ref with its on-disk artifacts and stripped config.
type snapshotManifest struct {
	Ref          string            `json:"ref"`
	InstanceID   string            `json:"instance_id"`
	SnapshotPath string            `json:"snapshot_path"`
	MemFilePath  string            `json:"mem_file_path"`
	Env          map[string]string `json:"env"`
	CreatedAt    time.Time         `json:"created_at"`
}

// Microvm implements driver.Driver for hardware-isolated microVMs.
type Microvm struct {
	mu        sync.Mutex
	opts      MicrovmOpts
	engine    MicrovmEngine
	seq       int
	snapSeq   atomic.Int64
	instances map[string]*instanceRecord
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
	_ = os.MkdirAll(filepath.Join(opts.StateDir, "instances"), 0755)
	_ = os.MkdirAll(filepath.Join(opts.StateDir, "snapshots"), 0755)

	engine := opts.Engine
	if engine == nil {
		if hasKVM() {
			engine = NewFirecrackerEngine(opts.VMMPath, opts.StateDir)
		} else {
			engine = NewSimulatedEngineWithDir(opts.StateDir)
		}
	}

	m := &Microvm{
		opts:      opts,
		engine:    engine,
		instances: make(map[string]*instanceRecord),
	}
	_ = m.recoverDiskInstances()
	return m
}

func hasKVM() bool {
	_, err := os.Stat("/dev/kvm")
	return err == nil
}

func (m *Microvm) instanceMetaPath(id string) string {
	return filepath.Join(m.opts.StateDir, "instances", id, "instance.json")
}

func (m *Microvm) saveInstanceRecord(rec *instanceRecord) error {
	dir := filepath.Join(m.opts.StateDir, "instances", rec.ID)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create instance dir: %w", err)
	}
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal instance record: %w", err)
	}
	if err := os.WriteFile(m.instanceMetaPath(rec.ID), data, 0644); err != nil {
		return fmt.Errorf("write instance record: %w", err)
	}
	return nil
}

func (m *Microvm) deleteInstanceRecord(id string) {
	dir := filepath.Join(m.opts.StateDir, "instances", id)
	_ = os.RemoveAll(dir)
}

// recoverDiskInstances restores instances recorded on disk after a runner restart.
func (m *Microvm) recoverDiskInstances() error {
	instancesDir := filepath.Join(m.opts.StateDir, "instances")
	entries, err := os.ReadDir(instancesDir)
	if err != nil {
		return nil
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		id := entry.Name()
		data, err := os.ReadFile(m.instanceMetaPath(id))
		if err != nil {
			continue
		}
		var rec instanceRecord
		if err := json.Unmarshal(data, &rec); err != nil {
			continue
		}

		// Reconcile observed hypervisor state
		st, stateErr := m.engine.State(context.Background(), id)
		if stateErr == nil {
			switch st {
			case VMMStateRunning:
				rec.State = StateRunning
				rec.Cold = false
			case VMMStatePaused:
				rec.State = StateSuspended
				rec.Cold = false
			case VMMStateStopped:
				rec.State = StateSuspended
				rec.Cold = true
			case VMMStateGone:
				if rec.Cold {
					rec.State = StateSuspended
				} else {
					rec.State = StateGone
				}
			}
		}

		m.instances[id] = &rec
		if n, _ := strconv.Atoi(strings.TrimPrefix(id, "mvm-")); n > m.seq {
			m.seq = n
		}
	}
	return nil
}

// workspaceDir returns the host filesystem path for sessionID's persistent workspace.
func (m *Microvm) workspaceDir(sessionID string) string {
	if sessionID == "" {
		return ""
	}
	return filepath.Join(m.opts.StateDir, "workspaces", workspaceVolume(sessionID))
}

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

// SnapshotEnv returns the recorded environment map for a committed snapshot ref.
func (m *Microvm) SnapshotEnv(ref string) map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	manifestPath := filepath.Join(m.opts.StateDir, "snapshots", "refs", sanitizeRef(ref), "manifest.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil
	}
	var sm snapshotManifest
	if err := json.Unmarshal(data, &sm); err != nil {
		return nil
	}
	return sm.Env
}

func sanitizeRef(ref string) string {
	return url.PathEscape(strings.ReplaceAll(ref, ":", "_"))
}

func (m *Microvm) usedLocked() int {
	used := 0
	for _, inst := range m.instances {
		if inst.State == StateRunning || (inst.State == StateSuspended && !inst.Cold) {
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

// writeGuestBootstrapFiles stages the session environment and guest launch script into the workspace.
func (m *Microvm) writeGuestBootstrapFiles(sessionID string, env map[string]string, cmd []string) error {
	wsDir := m.workspaceDir(sessionID)
	if wsDir == "" {
		return nil
	}
	rainierDir := filepath.Join(wsDir, ".rainier")
	if err := os.MkdirAll(rainierDir, 0755); err != nil {
		return err
	}

	var envBuf bytes.Buffer
	for k, v := range env {
		fmt.Fprintf(&envBuf, "%s=%s\n", k, v)
	}
	if err := os.WriteFile(filepath.Join(rainierDir, "session.env"), envBuf.Bytes(), 0600); err != nil {
		return err
	}

	// Write guest bootstrap script that exports the session environment and launches sessiond
	var script bytes.Buffer
	script.WriteString("#!/bin/sh\nset -a\n")
	script.WriteString(". /workspace/.rainier/session.env\nset +a\n")
	script.WriteString("exec /usr/local/bin/sessiond")
	if env["RAINIER_DIAL"] != "" {
		fmt.Fprintf(&script, " --dial %q", env["RAINIER_DIAL"])
	}
	if env["RAINIER_SESSION"] != "" {
		fmt.Fprintf(&script, " --session %q", env["RAINIER_SESSION"])
	}
	script.WriteString(" --")
	if len(cmd) == 0 {
		script.WriteString(" /bin/bash\n")
	} else {
		for _, c := range cmd {
			fmt.Fprintf(&script, " %q", c)
		}
		script.WriteString("\n")
	}

	return os.WriteFile(filepath.Join(rainierDir, "bootstrap.sh"), script.Bytes(), 0755)
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

	cmd := spec.Cmd
	if len(cmd) == 0 {
		cmd = []string{"/bin/bash"}
	}

	guestEnv := buildGuestEnv(spec)
	if spec.SessionID != "" {
		if err := m.writeGuestBootstrapFiles(spec.SessionID, guestEnv, cmd); err != nil {
			if createdVolume {
				_ = m.RemoveWorkspace(ctx, spec.SessionID)
			}
			return Handle{}, fmt.Errorf("write guest bootstrap: %w", err)
		}
	}

	tapDevice := fmt.Sprintf("tap-%s", id)
	if spec.SessionID != "" {
		tapDevice = fmt.Sprintf("tap-%s", spec.SessionID)
	}

	cfg := VMMConfig{
		ID:                id,
		SessionID:         spec.SessionID,
		VCPU:              2,
		MemoryMB:          2048,
		KernelPath:        m.opts.KernelPath,
		RootfsPath:        rootfs,
		WorkspaceDiskPath: m.workspaceDir(spec.SessionID),
		HomeDiskPath:      homePath,
		Cmd:               slices.Clone(cmd),
		EgressAllow:       slices.Clone(spec.EgressAllow),
		DialURL:           spec.DialURL,
		ProxyURL:          spec.ProxyURL,
		TapDevice:         tapDevice,
		GuestIP:           "172.18.0.2",
		GatewayIP:         "172.18.0.1",
		Env:               guestEnv,
	}

	if err := m.engine.Launch(ctx, cfg); err != nil {
		if createdVolume {
			_ = m.RemoveWorkspace(ctx, spec.SessionID)
		}
		return Handle{}, fmt.Errorf("launch microvm %s: %w", id, err)
	}

	rec := &instanceRecord{
		ID:        id,
		SessionID: spec.SessionID,
		State:     StateRunning,
		Volume:    workspaceVolume(spec.SessionID),
		PID:       m.engine.PID(id),
		Cfg:       cfg,
	}
	if err := m.saveInstanceRecord(rec); err != nil {
		_ = m.engine.Stop(ctx, id)
		if createdVolume {
			_ = m.RemoveWorkspace(ctx, spec.SessionID)
		}
		return Handle{}, fmt.Errorf("save instance metadata %s: %w", id, err)
	}
	m.instances[id] = rec

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
		inst.State = StateSuspended
		inst.Cold = false
	} else {
		if err := m.engine.Stop(ctx, id); err != nil {
			return err
		}
		inst.State = StateSuspended
		inst.Cold = true
		inst.PID = 0
	}
	if err := m.saveInstanceRecord(inst); err != nil {
		return fmt.Errorf("save suspend metadata %s: %w", id, err)
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

	if inst.Cold {
		// Cold resume: relaunch the stopped microVM process pointing to the existing workspace
		if err := m.engine.Launch(ctx, inst.Cfg); err != nil {
			return false, fmt.Errorf("relaunch cold microvm %s: %w", id, err)
		}
		inst.State = StateRunning
		inst.Cold = false
		inst.PID = m.engine.PID(id)
		if err := m.saveInstanceRecord(inst); err != nil {
			return false, fmt.Errorf("save resume metadata %s: %w", id, err)
		}
		return true, nil
	}

	// Warm resume: unpause the existing frozen vCPUs
	if err := m.engine.Resume(ctx, id); err != nil {
		return false, err
	}
	inst.State = StateRunning
	if err := m.saveInstanceRecord(inst); err != nil {
		return false, fmt.Errorf("save resume metadata %s: %w", id, err)
	}
	return false, nil
}

func (m *Microvm) Snapshot(ctx context.Context, id, ref string, stripEnv []string) (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	inst, ok := m.instances[id]
	if !ok {
		return Snapshot{}, fmt.Errorf("no such id %s", id)
	}

	m.strips = append(m.strips, slices.Clone(stripEnv))

	if ref == "" {
		m.snapSeq.Add(1)
		ref = fmt.Sprintf("rainier-mvm:%s-%d", id, m.snapSeq.Load())
	}

	snap, err := m.engine.Snapshot(ctx, id, ref, stripEnv)
	if err != nil {
		return Snapshot{}, err
	}

	// Persist snapshot manifest stripped of sensitive keys
	strippedEnv := make(map[string]string)
	for k, v := range inst.Cfg.Env {
		if !slices.Contains(stripEnv, k) {
			strippedEnv[k] = v
		}
	}

	refDir := filepath.Join(m.opts.StateDir, "snapshots", "refs", sanitizeRef(ref))
	if err := os.MkdirAll(refDir, 0755); err != nil {
		return Snapshot{}, fmt.Errorf("create snapshot ref dir: %w", err)
	}

	sm := snapshotManifest{
		Ref:        ref,
		InstanceID: id,
		Env:        strippedEnv,
		CreatedAt:  time.Now(),
	}
	smData, err := json.MarshalIndent(sm, "", "  ")
	if err != nil {
		return Snapshot{}, fmt.Errorf("marshal snapshot manifest: %w", err)
	}
	if err := os.WriteFile(filepath.Join(refDir, "manifest.json"), smData, 0644); err != nil {
		return Snapshot{}, fmt.Errorf("write snapshot manifest: %w", err)
	}

	return snap, nil
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
		sessionID = inst.SessionID
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
		st, stateErr := m.engine.State(ctx, id)
		if stateErr != nil || (st != VMMStateGone && st != VMMStateStopped) {
			return fmt.Errorf("destroy microvm %s: %w", id, err)
		}
	}

	delete(m.instances, id)
	m.deleteInstanceRecord(id)
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

	if st, err := m.engine.State(ctx, id); err == nil {
		switch st {
		case VMMStateGone:
			inst.State = StateGone
		case VMMStateStopped:
			if inst.State == StateRunning {
				inst.State = StateSuspended
				inst.Cold = true
			}
		case VMMStatePaused:
			inst.State = StateSuspended
			inst.Cold = false
		case VMMStateRunning:
			inst.State = StateRunning
		}
	}

	return Handle{ID: id, State: inst.State}, nil
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
				inst.State = StateGone
			} else if st == VMMStateStopped && inst.State == StateRunning {
				inst.State = StateSuspended
				inst.Cold = true
			}
		}
		out = append(out, Listed{
			SessionID: inst.SessionID,
			Handle:    Handle{ID: id, State: inst.State},
		})
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Simulated Engine (For testing and dev environments without /dev/kvm)
// ---------------------------------------------------------------------------

type SimulatedEngine struct {
	mu         sync.Mutex
	stateDir   string
	states     map[string]VMMState
	configs    map[string]VMMConfig
	failOnStop map[string]error
}

// NewSimulatedEngine constructs an in-memory VMM engine for tests.
func NewSimulatedEngine() *SimulatedEngine {
	return NewSimulatedEngineWithDir("")
}

// NewSimulatedEngineWithDir constructs a simulated engine that can persist state across driver restarts.
func NewSimulatedEngineWithDir(stateDir string) *SimulatedEngine {
	s := &SimulatedEngine{
		stateDir:   stateDir,
		states:     make(map[string]VMMState),
		configs:    make(map[string]VMMConfig),
		failOnStop: make(map[string]error),
	}
	if stateDir != "" {
		_ = s.recoverStates()
	}
	return s
}

func (s *SimulatedEngine) stateFilePath(id string) string {
	if s.stateDir == "" {
		return ""
	}
	return filepath.Join(s.stateDir, "instances", id, "vmm_sim_state.txt")
}

func (s *SimulatedEngine) recoverStates() error {
	instancesDir := filepath.Join(s.stateDir, "instances")
	entries, err := os.ReadDir(instancesDir)
	if err != nil {
		return nil
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		id := entry.Name()
		data, err := os.ReadFile(s.stateFilePath(id))
		if err == nil {
			s.states[id] = VMMState(strings.TrimSpace(string(data)))
		}
	}
	return nil
}

func (s *SimulatedEngine) saveState(id string, st VMMState) {
	if s.stateDir == "" {
		return
	}
	path := s.stateFilePath(id)
	_ = os.MkdirAll(filepath.Dir(path), 0755)
	_ = os.WriteFile(path, []byte(string(st)), 0644)
}

func (s *SimulatedEngine) Launch(_ context.Context, cfg VMMConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
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

func (s *SimulatedEngine) Resume(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[id] = VMMStateRunning
	s.saveState(id, VMMStateRunning)
	return nil
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

func (s *SimulatedEngine) PID(_ string) int {
	return 0
}

// ---------------------------------------------------------------------------
// Firecracker Engine (Production Linux hosts with /dev/kvm)
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

type firecrackerClient struct {
	client *http.Client
}

func newFirecrackerClient(sockPath string) *firecrackerClient {
	return &firecrackerClient{
		client: &http.Client{
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					var d net.Dialer
					return d.DialContext(ctx, "unix", sockPath)
				},
			},
			Timeout: 5 * time.Second,
		},
	}
}

func (c *firecrackerClient) putJSON(ctx context.Context, endpoint string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, "http://localhost"+endpoint, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("firecracker %s failed (%d): %s", endpoint, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func (c *firecrackerClient) patchJSON(ctx context.Context, endpoint string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, "http://localhost"+endpoint, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("firecracker %s failed (%d): %s", endpoint, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func (f *FirecrackerEngine) socketPath(id string) string {
	return filepath.Join(f.stateDir, "sockets", id, "firecracker.sock")
}

func (f *FirecrackerEngine) pidFilePath(id string) string {
	return filepath.Join(f.stateDir, "instances", id, "pid")
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

	if cfg.KernelPath == "" {
		return errors.New("kernel image path is required for Firecracker launch")
	}
	if cfg.RootfsPath == "" {
		return errors.New("rootfs image path is required for Firecracker launch")
	}

	sockDir := filepath.Join(f.stateDir, "sockets", cfg.ID)
	if err := os.MkdirAll(sockDir, 0700); err != nil {
		return fmt.Errorf("create socket dir: %w", err)
	}
	sockPath := f.socketPath(cfg.ID)
	_ = os.Remove(sockPath)

	// Launch Firecracker process detached from request context so it outlives the create HTTP request
	cmd := exec.Command(f.vmmPath, "--api-sock", sockPath)
	if err := cmd.Start(); err != nil {
		_ = os.RemoveAll(sockDir)
		return fmt.Errorf("start firecracker %s: %w", cfg.ID, err)
	}

	// Persist PID to disk
	if cmd.Process != nil {
		pidDir := filepath.Join(f.stateDir, "instances", cfg.ID)
		_ = os.MkdirAll(pidDir, 0755)
		_ = os.WriteFile(f.pidFilePath(cfg.ID), []byte(strconv.Itoa(cmd.Process.Pid)), 0644)
	}

	var initSuccess bool
	defer func() {
		if !initSuccess && cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			_ = os.RemoveAll(sockDir)
		}
	}()

	fcClient := newFirecrackerClient(sockPath)
	if err := waitForSocket(ctx, sockPath); err != nil {
		return fmt.Errorf("wait for firecracker socket %s: %w", cfg.ID, err)
	}

	// 1. Machine configuration
	if err := fcClient.putJSON(ctx, "/machine-config", map[string]any{
		"vcpu_count":   cfg.VCPU,
		"mem_size_mib": cfg.MemoryMB,
		"smt":          false,
	}); err != nil {
		return fmt.Errorf("set machine config: %w", err)
	}

	// 2. Boot source
	bootArgs := cfg.BootArgs
	if bootArgs == "" {
		bootArgs = "console=ttyS0 reboot=k panic=1 pci=off init=/workspace/.rainier/bootstrap.sh"
	}
	if err := fcClient.putJSON(ctx, "/boot-source", map[string]any{
		"kernel_image_path": cfg.KernelPath,
		"boot_args":         bootArgs,
	}); err != nil {
		return fmt.Errorf("set boot source: %w", err)
	}

	// 3. Rootfs drive
	if err := fcClient.putJSON(ctx, "/drives/rootfs", map[string]any{
		"drive_id":       "rootfs",
		"path_on_host":   cfg.RootfsPath,
		"is_root_device": true,
		"is_read_only":   true,
	}); err != nil {
		return fmt.Errorf("set rootfs drive: %w", err)
	}

	// 4. Workspace drive
	if cfg.WorkspaceDiskPath != "" {
		if err := fcClient.putJSON(ctx, "/drives/workspace", map[string]any{
			"drive_id":       "workspace",
			"path_on_host":   cfg.WorkspaceDiskPath,
			"is_root_device": false,
			"is_read_only":   false,
		}); err != nil {
			return fmt.Errorf("set workspace drive: %w", err)
		}
	}

	// 5. Network interface
	if cfg.TapDevice != "" {
		if err := fcClient.putJSON(ctx, "/network-interfaces/eth0", map[string]any{
			"iface_id":      "eth0",
			"host_dev_name": cfg.TapDevice,
			"guest_mac":     "AA:FC:00:00:00:01",
		}); err != nil {
			return fmt.Errorf("set network interface: %w", err)
		}
	}

	// 6. MMDS metadata
	_ = fcClient.putJSON(ctx, "/mmds/config", map[string]any{
		"ipv4_address": "169.254.169.254",
	})
	_ = fcClient.putJSON(ctx, "/mmds", map[string]any{
		"session_id": cfg.SessionID,
		"dial_url":   cfg.DialURL,
		"env":        cfg.Env,
		"cmd":        cfg.Cmd,
	})

	// 7. Start Instance
	if err := fcClient.putJSON(ctx, "/actions", map[string]any{
		"action_type": "InstanceStart",
	}); err != nil {
		return fmt.Errorf("start instance %s: %w", cfg.ID, err)
	}

	f.procs[cfg.ID] = cmd
	initSuccess = true
	return nil
}

func waitForSocket(ctx context.Context, sockPath string) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			conn, err := net.Dial("unix", sockPath)
			if err == nil {
				_ = conn.Close()
				return nil
			}
		}
	}
}

func (f *FirecrackerEngine) Pause(ctx context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	fcClient := newFirecrackerClient(f.socketPath(id))
	return fcClient.patchJSON(ctx, "/vm", map[string]any{"state": "Paused"})
}

func (f *FirecrackerEngine) Resume(ctx context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	fcClient := newFirecrackerClient(f.socketPath(id))
	return fcClient.patchJSON(ctx, "/vm", map[string]any{"state": "Resumed"})
}

func (f *FirecrackerEngine) Stop(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	var pid int
	if cmd, ok := f.procs[id]; ok && cmd.Process != nil {
		pid = cmd.Process.Pid
	} else {
		// Read PID from disk if engine was restarted
		if data, err := os.ReadFile(f.pidFilePath(id)); err == nil {
			pid, _ = strconv.Atoi(strings.TrimSpace(string(data)))
		}
	}

	var stopErr error
	if pid > 0 {
		if err := syscall.Kill(pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
			stopErr = fmt.Errorf("sigterm pid %d: %w", pid, err)
		} else {
			// Poll up to 3 seconds for clean exit
			exited := false
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				if err := syscall.Kill(pid, 0); err != nil {
					exited = true
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			if !exited {
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
		}
	}

	delete(f.procs, id)
	sockDir := filepath.Join(f.stateDir, "sockets", id)
	if err := os.RemoveAll(sockDir); err != nil && stopErr == nil {
		stopErr = fmt.Errorf("remove socket dir: %w", err)
	}
	return stopErr
}

func (f *FirecrackerEngine) Snapshot(ctx context.Context, id, ref string, _ []string) (Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	snapDir := filepath.Join(f.stateDir, "snapshots", id)
	if err := os.MkdirAll(snapDir, 0755); err != nil {
		return Snapshot{}, err
	}

	fcClient := newFirecrackerClient(f.socketPath(id))
	if err := fcClient.putJSON(ctx, "/snapshot/create", map[string]any{
		"snapshot_type": "Diff",
		"snapshot_path": filepath.Join(snapDir, "snapshot.json"),
		"mem_file_path": filepath.Join(snapDir, "memfile"),
	}); err != nil {
		return Snapshot{}, fmt.Errorf("firecracker snapshot: %w", err)
	}
	return Snapshot{Ref: ref}, nil
}

func (f *FirecrackerEngine) State(_ context.Context, id string) (VMMState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var pid int
	if cmd, ok := f.procs[id]; ok && cmd.Process != nil {
		pid = cmd.Process.Pid
	} else {
		// Read PID from disk if engine was restarted
		if data, err := os.ReadFile(f.pidFilePath(id)); err == nil {
			pid, _ = strconv.Atoi(strings.TrimSpace(string(data)))
		}
	}

	if pid <= 0 {
		return VMMStateGone, nil
	}

	// Probe process liveness via signal 0
	if err := syscall.Kill(pid, 0); err != nil {
		return VMMStateStopped, nil
	}

	return VMMStateRunning, nil
}

func (f *FirecrackerEngine) PID(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	if cmd, ok := f.procs[id]; ok && cmd.Process != nil {
		return cmd.Process.Pid
	}
	if data, err := os.ReadFile(f.pidFilePath(id)); err == nil {
		p, _ := strconv.Atoi(strings.TrimSpace(string(data)))
		return p
	}
	return 0
}

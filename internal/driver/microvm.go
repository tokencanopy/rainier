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
	Network    string        // network bridge name (default "rainier-internal")
	Engine     MicrovmEngine // optional VMM engine override; defaults to firecracker on KVM or simulated
	Tap        TapManager    // optional TAP manager override
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

// TapManager manages creation, bridge attachment, and teardown of host TAP network devices.
type TapManager interface {
	Allocate(tapName, bridgeName string) error
	Release(tapName string) error
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
	DiskPath     string            `json:"disk_path"`
	Env          map[string]string `json:"env"`
	Cmd          []string          `json:"cmd"`
	StrippedKeys []string          `json:"stripped_keys"`
	CreatedAt    time.Time         `json:"created_at"`
}

// guestSessionConfig is the structured JSON configuration written into the guest workspace.
type guestSessionConfig struct {
	SessionID   string            `json:"session_id"`
	DialURL     string            `json:"dial_url"`
	ProxyURL    string            `json:"proxy_url"`
	Env         map[string]string `json:"env"`
	Cmd         []string          `json:"cmd"`
	EgressAllow []string          `json:"egress_allow"`
}

// Microvm implements driver.Driver for hardware-isolated microVMs.
type Microvm struct {
	mu        sync.Mutex
	opts      MicrovmOpts
	engine    MicrovmEngine
	tap       TapManager
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
	if opts.Network == "" {
		opts.Network = "rainier-internal"
	}
	_ = os.MkdirAll(filepath.Join(opts.StateDir, "workspaces"), 0755)
	_ = os.MkdirAll(filepath.Join(opts.StateDir, "instances"), 0755)
	_ = os.MkdirAll(filepath.Join(opts.StateDir, "snapshots", "refs"), 0755)
	_ = os.MkdirAll(filepath.Join(opts.StateDir, "rootfs"), 0755)

	engine := opts.Engine
	if engine == nil {
		if hasKVM() {
			engine = NewFirecrackerEngine(opts.VMMPath, opts.StateDir)
		} else {
			engine = NewSimulatedEngineWithDir(opts.StateDir)
		}
	}

	tap := opts.Tap
	if tap == nil {
		if hasKVM() && canManageTap() {
			tap = NewLinuxTapManager(opts.Network)
		} else {
			tap = NewSimulatedTapManager()
		}
	}

	m := &Microvm{
		opts:      opts,
		engine:    engine,
		tap:       tap,
		instances: make(map[string]*instanceRecord),
	}
	_ = m.recoverDiskInstances()
	return m
}

func canManageTap() bool {
	return os.Geteuid() == 0
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

// workspaceDiskPath returns the host block-device/disk-image path for sessionID's workspace.
func (m *Microvm) workspaceDiskPath(sessionID string) string {
	if sessionID == "" {
		return ""
	}
	return filepath.Join(m.opts.StateDir, "workspaces", workspaceVolume(sessionID)+".ext4")
}

func (m *Microvm) ensureWorkspaceDisk(sessionID string) (string, bool, error) {
	if sessionID == "" {
		return "", false, nil
	}
	diskPath := m.workspaceDiskPath(sessionID)
	if _, err := os.Stat(diskPath); err == nil {
		return diskPath, false, nil
	}

	_ = os.MkdirAll(filepath.Dir(diskPath), 0755)
	f, err := os.OpenFile(diskPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return "", false, fmt.Errorf("create workspace disk %s: %w", sessionID, err)
	}
	if err := f.Truncate(10 * 1024 * 1024 * 1024); err != nil {
		f.Close()
		_ = os.Remove(diskPath)
		return "", false, fmt.Errorf("truncate workspace disk %s: %w", sessionID, err)
	}
	f.Close()

	if mkfs, err := exec.LookPath("mkfs.ext4"); err == nil {
		cmd := exec.Command(mkfs, "-F", "-q", "-m", "0", diskPath)
		if out, err := cmd.CombinedOutput(); err != nil {
			_ = os.Remove(diskPath)
			return "", false, fmt.Errorf("mkfs.ext4 %s: %w: %s", diskPath, err, strings.TrimSpace(string(out)))
		}
	}

	return diskPath, true, nil
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
		name := e.Name()
		if strings.HasPrefix(name, workspaceVolumePrefix) {
			trimmed := strings.TrimSuffix(name, ".ext4")
			if !slices.Contains(out, trimmed) {
				out = append(out, trimmed)
			}
		}
	}
	slices.Sort(out)
	return out
}

// HasWorkspace reports whether the workspace disk image for sessionID exists.
func (m *Microvm) HasWorkspace(sessionID string) bool {
	if sessionID == "" {
		return false
	}
	_, err := os.Stat(m.workspaceDiskPath(sessionID))
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

// resolveRootfs determines the host ext4 rootfs disk image file for rootfsRef.
func (m *Microvm) resolveRootfs(ctx context.Context, rootfsRef string) (string, error) {
	if rootfsRef == "" {
		rootfsRef = m.opts.BaseRootfs
	}
	if rootfsRef == "" {
		rootfsRef = "rainier-rootfs:latest"
	}

	// 1. Direct filesystem path
	if _, err := os.Stat(rootfsRef); err == nil {
		return rootfsRef, nil
	}

	// 2. Check snapshot artifacts
	snapDisk := filepath.Join(m.opts.StateDir, "snapshots", "refs", sanitizeRef(rootfsRef), "workspace.ext4")
	if _, err := os.Stat(snapDisk); err == nil {
		return snapDisk, nil
	}

	// 3. Check unpacked rootfs cache
	cachedPath := filepath.Join(m.opts.StateDir, "rootfs", sanitizeRef(rootfsRef)+".ext4")
	if _, err := os.Stat(cachedPath); err == nil {
		return cachedPath, nil
	}

	// 4. Prepull / unpack OCI tag into cached ext4 file
	if err := m.prepullLocked(rootfsRef); err != nil {
		return "", fmt.Errorf("prepull rootfs %s: %w", rootfsRef, err)
	}

	return cachedPath, nil
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

// stageGuestSessionConfig writes structured JSON configuration for sessiond.
func (m *Microvm) stageGuestSessionConfig(id string, cfg guestSessionConfig) error {
	dir := filepath.Join(m.opts.StateDir, "instances", id)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "session.json"), data, 0600)
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

	var workspaceDisk string
	createdDisk := false
	if spec.SessionID != "" {
		p, created, err := m.ensureWorkspaceDisk(spec.SessionID)
		if err != nil {
			return Handle{}, err
		}
		workspaceDisk = p
		createdDisk = created
	}

	m.seq++
	id := fmt.Sprintf("mvm-%d", m.seq)

	rootfsPath, err := m.resolveRootfs(ctx, spec.Image)
	if err != nil {
		if createdDisk {
			_ = m.RemoveWorkspace(ctx, spec.SessionID)
		}
		return Handle{}, fmt.Errorf("resolve rootfs: %w", err)
	}

	homeDiskPath := ""
	if spec.Home != nil && spec.Home.Volume != "" {
		homeDiskPath = filepath.Join(m.opts.StateDir, "workspaces", spec.Home.Volume+".ext4")
	}

	cmd := spec.Cmd
	if len(cmd) == 0 {
		cmd = []string{"/bin/bash"}
	}

	guestEnv := buildGuestEnv(spec)
	guestConfig := guestSessionConfig{
		SessionID:   spec.SessionID,
		DialURL:     spec.DialURL,
		ProxyURL:    spec.ProxyURL,
		Env:         guestEnv,
		Cmd:         slices.Clone(cmd),
		EgressAllow: slices.Clone(spec.EgressAllow),
	}

	if err := m.stageGuestSessionConfig(id, guestConfig); err != nil {
		if createdDisk {
			_ = m.RemoveWorkspace(ctx, spec.SessionID)
		}
		return Handle{}, fmt.Errorf("stage guest session config: %w", err)
	}

	tapDevice := fmt.Sprintf("tap-%s", id)
	if err := m.tap.Allocate(tapDevice, m.opts.Network); err != nil {
		if createdDisk {
			_ = m.RemoveWorkspace(ctx, spec.SessionID)
		}
		return Handle{}, fmt.Errorf("allocate tap device %s: %w", tapDevice, err)
	}

	cfg := VMMConfig{
		ID:                id,
		SessionID:         spec.SessionID,
		VCPU:              2,
		MemoryMB:          2048,
		KernelPath:        m.opts.KernelPath,
		RootfsPath:        rootfsPath,
		WorkspaceDiskPath: workspaceDisk,
		HomeDiskPath:      homeDiskPath,
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
		_ = m.tap.Release(tapDevice)
		if createdDisk {
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
		_ = m.tap.Release(tapDevice)
		if createdDisk {
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

	// Copy workspace disk to snapshot artifact preserving sparseness
	snapDiskPath := filepath.Join(refDir, "workspace.ext4")
	if inst.Cfg.WorkspaceDiskPath != "" {
		_ = copySparseFile(snapDiskPath, inst.Cfg.WorkspaceDiskPath)
	}

	sm := snapshotManifest{
		Ref:          ref,
		InstanceID:   id,
		DiskPath:     snapDiskPath,
		Env:          strippedEnv,
		Cmd:          slices.Clone(inst.Cfg.Cmd),
		StrippedKeys: slices.Clone(stripEnv),
		CreatedAt:    time.Now(),
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

func (m *Microvm) prepullLocked(ref string) error {
	if ref == "" {
		return errors.New("prepull: empty image ref")
	}
	m.pulls = append(m.pulls, ref)

	cachedPath := filepath.Join(m.opts.StateDir, "rootfs", sanitizeRef(ref)+".ext4")
	if _, err := os.Stat(cachedPath); err == nil {
		return nil
	}

	_ = os.MkdirAll(filepath.Join(m.opts.StateDir, "rootfs"), 0755)

	f, err := os.Create(cachedPath)
	if err != nil {
		return err
	}
	_ = f.Truncate(1024 * 1024)
	f.Close()
	return nil
}

func (m *Microvm) Prepull(_ context.Context, ref string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.prepullLocked(ref)
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

	inst, ok := m.instances[id]
	if !ok {
		return nil
	}

	if err := m.engine.Stop(ctx, id); err != nil {
		st, stateErr := m.engine.State(ctx, id)
		if stateErr != nil || (st != VMMStateGone && st != VMMStateStopped) {
			return fmt.Errorf("destroy microvm %s: %w", id, err)
		}
	}

	if inst.Cfg.TapDevice != "" {
		_ = m.tap.Release(inst.Cfg.TapDevice)
	}

	delete(m.instances, id)
	m.deleteInstanceRecord(id)
	return nil
}

func (m *Microvm) RemoveWorkspace(_ context.Context, sessionID string) error {
	if sessionID == "" {
		return nil
	}
	diskPath := m.workspaceDiskPath(sessionID)
	_ = os.Remove(diskPath)
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
// Linux TAP Manager (Production Linux hosts)
// ---------------------------------------------------------------------------

type LinuxTapManager struct {
	bridgeName string
}

func NewLinuxTapManager(bridgeName string) *LinuxTapManager {
	if bridgeName == "" {
		bridgeName = "rainier-internal"
	}
	return &LinuxTapManager{bridgeName: bridgeName}
}

func (l *LinuxTapManager) Allocate(tapName, bridgeName string) error {
	if bridgeName == "" {
		bridgeName = l.bridgeName
	}
	cmd := exec.Command("ip", "tuntap", "add", "dev", tapName, "mode", "tap")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("ip tuntap add %s: %w: %s", tapName, err, strings.TrimSpace(string(out)))
	}
	if bridgeName != "" {
		cmdBridge := exec.Command("ip", "link", "set", "dev", tapName, "master", bridgeName)
		_ = cmdBridge.Run()
	}
	cmdUp := exec.Command("ip", "link", "set", "dev", tapName, "up")
	if out, err := cmdUp.CombinedOutput(); err != nil {
		_ = l.Release(tapName)
		return fmt.Errorf("ip link set %s up: %w: %s", tapName, err, strings.TrimSpace(string(out)))
	}
	return nil
}

func (l *LinuxTapManager) Release(tapName string) error {
	cmd := exec.Command("ip", "link", "delete", "dev", tapName)
	return cmd.Run()
}

// ---------------------------------------------------------------------------
// Simulated TAP Manager (For testing and dev environments)
// ---------------------------------------------------------------------------

type SimulatedTapManager struct {
	mu        sync.Mutex
	allocated map[string]bool
}

func NewSimulatedTapManager() *SimulatedTapManager {
	return &SimulatedTapManager{allocated: make(map[string]bool)}
}

func (s *SimulatedTapManager) Allocate(tapName, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.allocated[tapName] = true
	return nil
}

func (s *SimulatedTapManager) Release(tapName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.allocated, tapName)
	return nil
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

	cmd := exec.Command(f.vmmPath, "--api-sock", sockPath)
	if err := cmd.Start(); err != nil {
		_ = os.RemoveAll(sockDir)
		return fmt.Errorf("start firecracker %s: %w", cfg.ID, err)
	}

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
		bootArgs = "console=ttyS0 reboot=k panic=1 pci=off ip=172.18.0.2::172.18.0.1:255.255.0.0:guest:eth0:off init=/init"
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

	// 4b. Home drive (if mounted)
	if cfg.HomeDiskPath != "" {
		if err := fcClient.putJSON(ctx, "/drives/home", map[string]any{
			"drive_id":       "home",
			"path_on_host":   cfg.HomeDiskPath,
			"is_root_device": false,
			"is_read_only":   false,
		}); err != nil {
			return fmt.Errorf("set home drive: %w", err)
		}
	}

	// 5. Network interface (TAP device)
	if cfg.TapDevice != "" {
		if err := fcClient.putJSON(ctx, "/network-interfaces/eth0", map[string]any{
			"iface_id":      "eth0",
			"host_dev_name": cfg.TapDevice,
			"guest_mac":     "AA:FC:00:00:00:01",
		}); err != nil {
			return fmt.Errorf("set network interface: %w", err)
		}
	}

	// 6. Deliver session parameters via MMDS
	_ = fcClient.putJSON(ctx, "/mmds/config", map[string]any{
		"ipv4_address": "169.254.169.254",
	})
	_ = fcClient.putJSON(ctx, "/mmds", map[string]any{
		"session_id": cfg.SessionID,
		"dial_url":   cfg.DialURL,
		"env":        cfg.Env,
		"cmd":        cfg.Cmd,
	})

	// 7. Start MicroVM Instance
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
		if data, err := os.ReadFile(f.pidFilePath(id)); err == nil {
			pid, _ = strconv.Atoi(strings.TrimSpace(string(data)))
		}
	}

	var stopErr error
	sockPath := f.socketPath(id)
	if pid > 0 && isFirecrackerPID(pid, sockPath) {
		if err := syscall.Kill(pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
			stopErr = fmt.Errorf("sigterm pid %d: %w", pid, err)
		} else {
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

func (f *FirecrackerEngine) State(ctx context.Context, id string) (VMMState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var pid int
	if cmd, ok := f.procs[id]; ok && cmd.Process != nil {
		pid = cmd.Process.Pid
	} else {
		if data, err := os.ReadFile(f.pidFilePath(id)); err == nil {
			pid, _ = strconv.Atoi(strings.TrimSpace(string(data)))
		}
	}

	if pid <= 0 {
		return VMMStateGone, nil
	}

	sockPath := f.socketPath(id)
	if !isFirecrackerPID(pid, sockPath) {
		return VMMStateGone, nil
	}

	// Query /vm to distinguish between Running and Paused states
	fcClient := newFirecrackerClient(sockPath)
	type vmDesc struct {
		State string `json:"state"`
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost/vm", nil)
	if err == nil {
		if resp, err := fcClient.client.Do(req); err == nil {
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				var desc vmDesc
				if json.NewDecoder(resp.Body).Decode(&desc) == nil {
					if strings.EqualFold(desc.State, "Paused") {
						return VMMStatePaused, nil
					}
					if strings.EqualFold(desc.State, "Resumed") || strings.EqualFold(desc.State, "Running") {
						return VMMStateRunning, nil
					}
				}
			}
		}
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

func isAllZeroes(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

func copySparseFile(dstPath, srcPath string) error {
	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer dst.Close()

	buf := make([]byte, 1024*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if isAllZeroes(buf[:n]) {
				if _, err := dst.Seek(int64(n), io.SeekCurrent); err != nil {
					return err
				}
			} else {
				if _, err := dst.Write(buf[:n]); err != nil {
					return err
				}
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
	}
	if st, err := src.Stat(); err == nil {
		_ = dst.Truncate(st.Size())
	}
	return nil
}

func isFirecrackerPID(pid int, expectedSock string) bool {
	if pid <= 0 {
		return false
	}
	if err := syscall.Kill(pid, 0); err != nil {
		return false
	}

	cmdlinePath := fmt.Sprintf("/proc/%d/cmdline", pid)
	if data, err := os.ReadFile(cmdlinePath); err == nil {
		cmdline := string(data)
		return strings.Contains(cmdline, "firecracker") && strings.Contains(cmdline, expectedSock)
	}

	cmd := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "command=")
	if out, err := cmd.Output(); err == nil {
		s := string(out)
		return strings.Contains(s, "firecracker") && (expectedSock == "" || strings.Contains(s, expectedSock))
	}

	return false
}

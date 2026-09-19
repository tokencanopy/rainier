// internal/driver/microvm.go
//
// The microVM driver: one Firecracker microVM per session, in place of the
// docker driver's one container per session.
//
// This is the second step of that driver, and it is deliberately not a
// working end-to-end path yet. What it establishes is the seam and the
// invariants: a production `--driver=microvm` either has hardware
// virtualization, a kernel, a rootfs, a formatter and the Firecracker binary,
// or it refuses to start; nothing decrypted is written to host disk; a
// cold-parked session reads as suspended rather than gone; and the simulated
// engine behind the tests is reachable only by explicitly injecting it.
// Guest configuration delivery (vsock), the jailer, per-VM network slots,
// nftables, metering, and real image and snapshot work by digest each land in
// their own change — see the TODO markers below and ADR-0003.
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

const (
	// defaultMicrovmVCPU and defaultMicrovmMemoryMiB are ADR-0003 §5.1's
	// per-session floor (4 vCPU, 8 GiB). They are a FLOOR, not a suggestion:
	// the host's admission envelope (4 to 6 sessions on an n2-standard-16) is
	// derived from them, so a driver that quietly handed out less would make
	// the fleet's slot accounting describe a machine nobody is running.
	defaultMicrovmVCPU      = 4
	defaultMicrovmMemoryMiB = 8192

	// microvmDirMode and microvmFileMode are the only modes this driver ever
	// writes with. Everything under the state directory — instance records,
	// the staged guest config, snapshot manifests, workspace and agent-home
	// disk images, pid files — describes or contains one tenant's session, on
	// a host that also runs other tenants' sessions.
	microvmDirMode  os.FileMode = 0o700
	microvmFileMode os.FileMode = 0o600

	// workspaceDiskBytes is the sparse size of a session's workspace image:
	// the file is created at this length and consumes only what the guest
	// actually writes.
	workspaceDiskBytes = 10 << 30

	// firecrackerSocketTimeout bounds the wait for a freshly started
	// Firecracker to open its API socket. It is the VMM's own deadline rather
	// than the caller's: a Create whose context has no deadline must not be
	// able to wait forever on a VMM that will never answer.
	firecrackerSocketTimeout = 10 * time.Second
)

// MicrovmOpts configures the microVM driver.
//
// Engine, Tap and Format are one seam, not three: they are the three places
// this driver touches the host, and they are injected TOGETHER by tests or
// not at all. Production passes none of them and gets the Firecracker engine,
// the real TAP manager and mkfs.ext4 — after NewMicrovm has checked that this
// host can actually provide all three. There is deliberately no
// "simulate whatever is missing" path: a driver that silently substituted a
// simulated engine reports every session as running while nothing executes,
// which is the single worst failure mode this component has.
type MicrovmOpts struct {
	BaseRootfs string // default base ext4 rootfs image path (required in production)
	KernelPath string // guest vmlinux kernel path (required in production)
	StateDir   string // directory holding instance sockets, metadata, and disks (always required)
	TotalSlots int    // maximum simultaneous active slot capacity
	VCPU       int    // vCPUs per session; 0 means defaultMicrovmVCPU
	MemoryMiB  int    // memory per session in MiB; 0 means defaultMicrovmMemoryMiB
	VMMPath    string // path to the Firecracker executable; empty means "firecracker" on PATH
	Network    string // network bridge name (default "rainier-internal")

	Engine MicrovmEngine // test seam; nil in production
	Tap    TapManager    // test seam; nil in production
	Format DiskFormatter // test seam; nil in production
}

// VMMState is the hypervisor-observed execution state of a microVM.
type VMMState string

const (
	VMMStateRunning VMMState = "running"
	VMMStatePaused  VMMState = "paused"
	VMMStateStopped VMMState = "stopped"
	VMMStateGone    VMMState = "gone"
)

// VMMConfig is the complete configuration handed down to the microVM
// hypervisor.
//
// Env is the one field that is never serialized. It carries the session's
// resolved environment, which for a hosted session is where an environment's
// DECRYPTED secrets live, and ADR-0003 §2.2 and §2.7 both forbid that landing
// on host disk: a file that outlives the session, the cold park and a runnerd
// restart is a class of exposure the docker driver does not have, since it
// puts Spec.Env only in a `docker run` argv. The `json:"-"` is the
// enforcement, not a convention — every structure this driver persists either
// embeds VMMConfig or is derived from it.
type VMMConfig struct {
	ID                string            `json:"id"`
	SessionID         string            `json:"session_id"`
	VCPU              int               `json:"vcpu"`
	MemoryMiB         int               `json:"memory_mib"`
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
	Env               map[string]string `json:"-"`
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

// TapManager manages creation, bridge attachment, and teardown of host TAP
// network devices.
type TapManager interface {
	Allocate(tapName, bridgeName string) error
	Release(tapName string) error
}

// DiskFormatter puts a filesystem on a freshly created disk image, before any
// guest is handed it as a block device.
//
// It is an interface for the same reason MicrovmEngine is: formatting is a
// host capability this driver cannot fake, and the alternative to naming it
// here was the previous behavior — skip mkfs.ext4 when it is not on PATH and
// attach a 10 GiB file of zeroes to the guest as though it were a filesystem.
type DiskFormatter interface {
	Format(path string) error
}

// instanceRecord is the persistent metadata stored on disk for each microVM
// instance.
type instanceRecord struct {
	ID        string    `json:"id"`
	SessionID string    `json:"session_id"`
	State     State     `json:"state"`
	Cold      bool      `json:"cold"`
	Volume    string    `json:"volume"`
	PID       int       `json:"pid"`
	Cfg       VMMConfig `json:"cfg"`

	// envLive marks a record whose Cfg.Env is the live environment THIS
	// process built in Create. It is unexported and therefore never
	// serialized, which is the point: a record recovered from disk after a
	// runnerd restart has no environment behind it, and a cold resume must
	// say so rather than boot a guest with a silently empty one.
	envLive bool
}

// snapshotManifest records what a snapshot committed.
//
// EnvKeys is KEYS, never values. The manifest is a file on a shared host that
// outlives the session, so it is under the same rule as the instance record
// (see VMMConfig.Env): an environment's decrypted values have no business in
// it. Keys are enough for what the manifest is for — proving that what a
// caller named in stripEnv did not survive the commit.
type snapshotManifest struct {
	Ref          string    `json:"ref"`
	InstanceID   string    `json:"instance_id"`
	EnvKeys      []string  `json:"env_keys"`
	Cmd          []string  `json:"cmd"`
	StrippedKeys []string  `json:"stripped_keys"`
	CreatedAt    time.Time `json:"created_at"`
}

// guestSessionConfig is the structured configuration staged on the host for
// sessiond.
//
// It carries no environment. What a session needs in order to identify itself
// and reach the relay is not secret; what an environment resolved out of its
// secret refs is, and ADR-0003 §2.7 item 1 has those arriving in the guest
// from cell-gateway against a short-lived bootstrap token, never from a file
// on the host.
//
// TODO(PR 2): this file is staged on the host and nothing copies it into the
// guest. The delivery channel is virtio-vsock (ADR-0003 §2.7 item 2).
type guestSessionConfig struct {
	SessionID   string   `json:"session_id"`
	DialURL     string   `json:"dial_url"`
	ProxyURL    string   `json:"proxy_url"`
	Cmd         []string `json:"cmd"`
	EgressAllow []string `json:"egress_allow"`
}

// Microvm implements driver.Driver for hardware-isolated microVMs.
type Microvm struct {
	mu        sync.Mutex
	opts      MicrovmOpts
	engine    MicrovmEngine
	tap       TapManager
	format    DiskFormatter
	seq       int
	pending   int // slots reserved by an in-flight Create, counted as used
	snapSeq   atomic.Int64
	instances map[string]*instanceRecord
	pulls     []string
	strips    [][]string
}

// NewMicrovm creates a new microVM driver, or fails.
//
// It returns an error rather than a degraded driver on purpose. Every
// condition checked here — a state directory, a kernel, a rootfs, /dev/kvm,
// mkfs.ext4, the Firecracker binary — is something a microVM session cannot
// run without, and a runner that started anyway would accept sessions, report
// them running, and execute nothing.
func NewMicrovm(opts MicrovmOpts) (*Microvm, error) {
	if opts.StateDir == "" {
		return nil, errors.New("microvm: a state directory is required (--microvm-state-dir / RAINIER_MICROVM_STATE_DIR): it holds every session's workspace and agent-home disk image, and a temp-directory default puts a tenant's files somewhere the host reaps")
	}
	if opts.TotalSlots <= 0 {
		opts.TotalSlots = 16
	}
	if opts.VCPU <= 0 {
		opts.VCPU = defaultMicrovmVCPU
	}
	if opts.MemoryMiB <= 0 {
		opts.MemoryMiB = defaultMicrovmMemoryMiB
	}
	if opts.Network == "" {
		opts.Network = "rainier-internal"
	}

	engine, tap, format := opts.Engine, opts.Tap, opts.Format
	switch {
	case engine == nil && tap == nil && format == nil:
		// The production path. Everything below must be true of this host.
		if err := checkMicrovmHost(opts); err != nil {
			return nil, err
		}
		ext4, err := NewExt4Formatter()
		if err != nil {
			return nil, err
		}
		engine = NewFirecrackerEngine(opts.VMMPath, opts.StateDir)
		tap = NewLinuxTapManager(opts.Network)
		format = ext4
	case engine != nil && tap != nil && format != nil:
		// The test seam, injected whole.
	default:
		return nil, errors.New("microvm: MicrovmOpts.Engine, .Tap and .Format are one seam and must be injected together or not at all; injecting some of them leaves a production component talking to a simulated one")
	}

	for _, dir := range []string{"workspaces", "homes", "instances", filepath.Join("snapshots", "refs"), "rootfs"} {
		if err := os.MkdirAll(filepath.Join(opts.StateDir, dir), microvmDirMode); err != nil {
			return nil, fmt.Errorf("microvm: create state directory: %w", err)
		}
	}

	m := &Microvm{
		opts:      opts,
		engine:    engine,
		tap:       tap,
		format:    format,
		instances: make(map[string]*instanceRecord),
	}
	m.recoverDiskInstances()
	return m, nil
}

// checkMicrovmHost is the fail-closed preflight for a production microVM
// runner. The order is deliberate: configuration the operator got wrong is
// reported before facts about the machine they cannot change with a flag.
func checkMicrovmHost(opts MicrovmOpts) error {
	if err := readableFile("guest kernel (--kernel / RAINIER_KERNEL_PATH)", opts.KernelPath); err != nil {
		return err
	}
	if err := readableFile("base rootfs image (--rootfs / RAINIER_ROOTFS_PATH)", opts.BaseRootfs); err != nil {
		return err
	}
	kvm, err := os.OpenFile("/dev/kvm", os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("microvm: /dev/kvm is not usable by this process: a microVM session is a hardware-isolated VM and there is no software fallback: %w", err)
	}
	_ = kvm.Close()
	vmm := opts.VMMPath
	if vmm == "" {
		vmm = "firecracker"
	}
	if _, err := exec.LookPath(vmm); err != nil {
		return fmt.Errorf("microvm: firecracker executable %q not found: %w", vmm, err)
	}
	return nil
}

func readableFile(what, path string) error {
	if path == "" {
		return fmt.Errorf("microvm: %s is required", what)
	}
	fi, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("microvm: %s %q: %w", what, path, err)
	}
	if !fi.Mode().IsRegular() {
		return fmt.Errorf("microvm: %s %q is not a regular file", what, path)
	}
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("microvm: %s %q is not readable: %w", what, path, err)
	}
	return f.Close()
}

func (m *Microvm) instanceDir(id string) string {
	return filepath.Join(m.opts.StateDir, "instances", id)
}

func (m *Microvm) instanceMetaPath(id string) string {
	return filepath.Join(m.instanceDir(id), "instance.json")
}

// persistable copies a record for writing to disk. The copy drops Cfg.Env —
// which `json:"-"` would drop anyway — so that a record handed to a goroutine
// outside the driver mutex carries no reference to the live map either.
func persistable(rec *instanceRecord) instanceRecord {
	cp := *rec
	cp.Cfg.Env = nil
	cp.envLive = false
	return cp
}

// saveRecord writes a record copy. It takes a value rather than the pointer
// the map holds because it is always called with the driver mutex released.
func (m *Microvm) saveRecord(rec instanceRecord) error {
	dir := m.instanceDir(rec.ID)
	if err := os.MkdirAll(dir, microvmDirMode); err != nil {
		return fmt.Errorf("create instance dir: %w", err)
	}
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal instance record: %w", err)
	}
	if err := os.WriteFile(m.instanceMetaPath(rec.ID), data, microvmFileMode); err != nil {
		return fmt.Errorf("write instance record: %w", err)
	}
	return nil
}

func (m *Microvm) deleteInstanceRecord(id string) {
	_ = os.RemoveAll(m.instanceDir(id))
}

// recoverDiskInstances restores instances recorded on disk after a runner
// restart. Records recovered here have no live environment (see
// instanceRecord.envLive), which a cold Resume reports rather than papers
// over.
func (m *Microvm) recoverDiskInstances() {
	entries, err := os.ReadDir(filepath.Join(m.opts.StateDir, "instances"))
	if err != nil {
		return
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
		if st, err := m.engine.State(context.Background(), id); err == nil {
			reconcileState(&rec, st)
		}
		m.instances[id] = &rec
		if n, _ := strconv.Atoi(strings.TrimPrefix(id, "mvm-")); n > m.seq {
			m.seq = n
		}
	}
}

// reconcileState folds the hypervisor's observed state into the driver's own
// record — with one deliberate asymmetry.
//
// A session this driver cold-parked has no VM process BY CONSTRUCTION: cold
// suspend terminates it, because ADR-0003 §2.2 forbids keeping its memory
// anywhere. The real FirecrackerEngine.State therefore answers VMMStateGone
// for exactly the sessions that are healthily parked, and reading that as
// StateGone deletes a live session from every view runnerd has: Inspect calls
// it dead and Recover drops it. So the driver's own record is consulted
// first, and "gone" means gone only for a session the driver did not park.
func reconcileState(rec *instanceRecord, st VMMState) {
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

// workspaceDiskPath returns the host disk-image path for sessionID's
// workspace.
func (m *Microvm) workspaceDiskPath(sessionID string) string {
	if sessionID == "" {
		return ""
	}
	return filepath.Join(m.opts.StateDir, "workspaces", workspaceVolume(sessionID)+".ext4")
}

// homeDiskPath returns the host disk-image path for an agent home volume.
//
// Homes live in their own directory rather than beside the workspaces. They
// belong to a (creator, workspace) and not to a session, they outlive every
// session that mounts them, and ADR-0003 §2.3 keeps them out of the workspace
// disk and its checkpoint — a shared directory is one Volumes() scan or one
// teardown glob away from treating the two as the same thing.
func (m *Microvm) homeDiskPath(volume string) string {
	if volume == "" {
		return ""
	}
	return filepath.Join(m.opts.StateDir, "homes", volume+".ext4")
}

// ensureDisk creates and formats a sparse disk image at path if it is not
// already there, and reports whether this call is the one that created it.
func (m *Microvm) ensureDisk(path string) (string, bool, error) {
	if path == "" {
		return "", false, nil
	}
	if fi, err := os.Stat(path); err == nil {
		if !fi.Mode().IsRegular() {
			return "", false, fmt.Errorf("disk image %s is not a regular file", path)
		}
		return path, false, nil
	}

	if err := os.MkdirAll(filepath.Dir(path), microvmDirMode); err != nil {
		return "", false, fmt.Errorf("create disk directory for %s: %w", path, err)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, microvmFileMode)
	if err != nil {
		return "", false, fmt.Errorf("create disk image %s: %w", path, err)
	}
	if err := f.Truncate(workspaceDiskBytes); err != nil {
		f.Close()
		_ = os.Remove(path)
		return "", false, fmt.Errorf("size disk image %s: %w", path, err)
	}
	f.Close()

	// A half-made disk is removed rather than left for the next create to
	// find with Stat and hand to a guest as a filesystem.
	if err := m.format.Format(path); err != nil {
		_ = os.Remove(path)
		return "", false, err
	}
	return path, true, nil
}

// Volumes returns every live workspace volume name, sorted.
func (m *Microvm) Volumes() []string {
	entries, err := os.ReadDir(filepath.Join(m.opts.StateDir, "workspaces"))
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

func (m *Microvm) snapshotRefDir(ref string) string {
	return filepath.Join(m.opts.StateDir, "snapshots", "refs", sanitizeRef(ref))
}

// snapshotManifestBytes returns the raw manifest a commit wrote, or nil.
func (m *Microvm) snapshotManifestBytes(ref string) []byte {
	data, err := os.ReadFile(filepath.Join(m.snapshotRefDir(ref), "manifest.json"))
	if err != nil {
		return nil
	}
	return data
}

// SnapshotEnvKeys returns the environment keys a committed snapshot recorded,
// sorted, and whether there is a manifest at all. It is how the shared
// contract's strip subtest proves, for this driver, that what a caller named
// in stripEnv did not survive the commit.
//
// Keys, not values: see snapshotManifest.
func (m *Microvm) SnapshotEnvKeys(ref string) ([]string, bool) {
	data := m.snapshotManifestBytes(ref)
	if data == nil {
		return nil, false
	}
	var sm snapshotManifest
	if err := json.Unmarshal(data, &sm); err != nil {
		return nil, false
	}
	return sm.EnvKeys, true
}

func sanitizeRef(ref string) string {
	return url.PathEscape(strings.ReplaceAll(ref, ":", "_"))
}

func (m *Microvm) usedLocked() int {
	used := m.pending
	for _, inst := range m.instances {
		if inst.State == StateRunning || (inst.State == StateSuspended && !inst.Cold) {
			used++
		}
	}
	return used
}

// locateRootfs finds the host ext4 image for rootfsRef, or says why it
// cannot.
//
// There are exactly two answers: a path on this host, or an image this host
// has already cached. There is deliberately no third branch that MAKES one.
// The previous behavior truncated a 1 MiB empty file and called the ref
// satisfied, so a create for an image nobody had ever fetched succeeded and
// booted a guest off a megabyte of zeroes.
//
// TODO(PR 4): fetching a missing ref belongs here — an ext4 image by digest,
// reflink-copied per session (ADR-0003 §2.7 item 3). Until that exists, a ref
// this host does not have is an error, not a placeholder.
func (m *Microvm) locateRootfs(rootfsRef string) (string, error) {
	if rootfsRef == "" {
		return "", errors.New("microvm: empty rootfs ref")
	}
	if fi, err := os.Stat(rootfsRef); err == nil && fi.Mode().IsRegular() {
		return rootfsRef, nil
	}
	cached := filepath.Join(m.opts.StateDir, "rootfs", sanitizeRef(rootfsRef)+".ext4")
	if fi, err := os.Stat(cached); err == nil && fi.Mode().IsRegular() {
		return cached, nil
	}
	return "", fmt.Errorf("microvm: rootfs %q is not on this host, neither as a path nor as a cached image at %s; fetching an environment image by digest lands with the image work (ADR-0003 §2.7 item 3)", rootfsRef, cached)
}

// resolveRootfs picks the rootfs for a create: the spec's image, else the
// runner's configured base image.
func (m *Microvm) resolveRootfs(rootfsRef string) (string, error) {
	if rootfsRef == "" {
		rootfsRef = m.opts.BaseRootfs
	}
	if rootfsRef == "" {
		return "", errors.New("microvm: no rootfs image: the spec names none and this runner has no --rootfs")
	}
	return m.locateRootfs(rootfsRef)
}

// buildGuestEnv translates the full Spec into the environment map passed into
// the microVM guest. The result is held in memory for the lifetime of this
// process and never written to disk; see VMMConfig.Env.
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

// stageGuestSessionConfig writes the non-secret half of a session's
// configuration for sessiond.
func (m *Microvm) stageGuestSessionConfig(id string, cfg guestSessionConfig) error {
	dir := m.instanceDir(id)
	if err := os.MkdirAll(dir, microvmDirMode); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "session.json"), data, microvmFileMode)
}

// reserveSlot takes the capacity decision and an id under the mutex, so that
// the rest of Create — disks, mkfs, the rootfs lookup, the TAP, the launch —
// runs with the mutex RELEASED. Holding it across engine.Launch froze
// Inspect, List, Capacity and Destroy for every session on the host behind
// one Firecracker that had not yet opened its socket.
func (m *Microvm) reserveSlot() (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if used := m.usedLocked(); used >= m.opts.TotalSlots {
		return "", fmt.Errorf("no capacity: %d/%d", used, m.opts.TotalSlots)
	}
	m.pending++
	m.seq++
	return fmt.Sprintf("mvm-%d", m.seq), nil
}

// commitSlot turns the reservation into a live record, or drops it when rec
// is nil.
func (m *Microvm) commitSlot(rec *instanceRecord) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pending--
	if rec != nil {
		m.instances[rec.ID] = rec
	}
}

func (m *Microvm) Create(ctx context.Context, spec Spec) (Handle, error) {
	if err := checkScriptSizes(spec); err != nil {
		return Handle{}, err
	}
	if err := checkHome(spec.Home); err != nil {
		return Handle{}, err
	}

	id, err := m.reserveSlot()
	if err != nil {
		return Handle{}, err
	}
	rec, err := m.launch(ctx, id, spec)
	m.commitSlot(rec)
	if err != nil {
		return Handle{}, err
	}
	return Handle{ID: rec.ID, State: StateRunning}, nil
}

// launch is Create's side-effecting half, and runs with the driver mutex
// released throughout.
func (m *Microvm) launch(ctx context.Context, id string, spec Spec) (*instanceRecord, error) {
	var undo []func()
	done := false
	defer func() {
		if done {
			return
		}
		for i := len(undo) - 1; i >= 0; i-- {
			undo[i]()
		}
	}()

	workspaceDisk, created, err := m.ensureDisk(m.workspaceDiskPath(spec.SessionID))
	if err != nil {
		return nil, err
	}
	if created {
		undo = append(undo, func() { _ = os.Remove(workspaceDisk) })
	}

	// The agent home is a disk image like the workspace, created and
	// formatted the same way and BEFORE launch: Firecracker rejects a
	// /drives/home whose path_on_host does not exist, so a session carrying
	// Spec.Home previously could not boot at all. It is not rolled back on
	// failure — a home belongs to the (creator, workspace) and a concurrent
	// session of the same person may already be mounted on it; an empty
	// formatted image is harmless, removing a live one is not.
	homeDisk := ""
	if spec.Home != nil {
		if homeDisk, _, err = m.ensureDisk(m.homeDiskPath(spec.Home.Volume)); err != nil {
			return nil, err
		}
	}

	rootfsPath, err := m.resolveRootfs(spec.Image)
	if err != nil {
		return nil, fmt.Errorf("resolve rootfs: %w", err)
	}

	cmd := spec.Cmd
	if len(cmd) == 0 {
		cmd = []string{"/bin/bash"}
	}

	if err := m.stageGuestSessionConfig(id, guestSessionConfig{
		SessionID:   spec.SessionID,
		DialURL:     spec.DialURL,
		ProxyURL:    spec.ProxyURL,
		Cmd:         slices.Clone(cmd),
		EgressAllow: slices.Clone(spec.EgressAllow),
	}); err != nil {
		return nil, fmt.Errorf("stage guest session config: %w", err)
	}
	undo = append(undo, func() { m.deleteInstanceRecord(id) })

	tapDevice := "tap-" + id
	if err := m.tap.Allocate(tapDevice, m.opts.Network); err != nil {
		return nil, fmt.Errorf("allocate tap device %s: %w", tapDevice, err)
	}
	undo = append(undo, func() { _ = m.tap.Release(tapDevice) })

	cfg := VMMConfig{
		ID:                id,
		SessionID:         spec.SessionID,
		VCPU:              m.opts.VCPU,
		MemoryMiB:         m.opts.MemoryMiB,
		KernelPath:        m.opts.KernelPath,
		RootfsPath:        rootfsPath,
		WorkspaceDiskPath: workspaceDisk,
		HomeDiskPath:      homeDisk,
		Cmd:               slices.Clone(cmd),
		EgressAllow:       slices.Clone(spec.EgressAllow),
		DialURL:           spec.DialURL,
		ProxyURL:          spec.ProxyURL,
		TapDevice:         tapDevice,
		// TODO(PR 3): one address for every VM on the bridge. A per-host slot
		// allocator (TAP, IP, MAC, netns) lands with the network work, along
		// with the nftables drops ADR-0003 §4.3 requires.
		GuestIP:   "172.18.0.2",
		GatewayIP: "172.18.0.1",
		Env:       buildGuestEnv(spec),
	}

	if err := m.engine.Launch(ctx, cfg); err != nil {
		return nil, fmt.Errorf("launch microvm %s: %w", id, err)
	}
	undo = append(undo, func() { _ = m.engine.Stop(context.WithoutCancel(ctx), id) })

	rec := &instanceRecord{
		ID:        id,
		SessionID: spec.SessionID,
		State:     StateRunning,
		Volume:    workspaceVolume(spec.SessionID),
		PID:       m.engine.PID(id),
		Cfg:       cfg,
		envLive:   true,
	}
	if err := m.saveRecord(persistable(rec)); err != nil {
		return nil, fmt.Errorf("save instance metadata %s: %w", id, err)
	}

	done = true
	return rec, nil
}

func (m *Microvm) Suspend(ctx context.Context, id string, warm bool) error {
	m.mu.Lock()
	_, ok := m.instances[id]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("no such id %s", id)
	}

	if warm {
		if err := m.engine.Pause(ctx, id); err != nil {
			return err
		}
	} else {
		// Cold park terminates the VM, and no memory image is written
		// anywhere: ADR-0003 §2.2 treats a guest memory image as a
		// secret-bearing artifact, which is the same reason the Firecracker
		// engine's Snapshot refuses.
		//
		// TODO(PR 3): the TAP device stays allocated for the whole dormant
		// window and is not re-created after a restart.
		if err := m.engine.Stop(ctx, id); err != nil {
			return err
		}
	}

	m.mu.Lock()
	inst, ok := m.instances[id]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("no such id %s", id)
	}
	inst.State = StateSuspended
	inst.Cold = !warm
	if !warm {
		inst.PID = 0
	}
	rec := persistable(inst)
	m.mu.Unlock()

	if err := m.saveRecord(rec); err != nil {
		return fmt.Errorf("save suspend metadata %s: %w", id, err)
	}
	return nil
}

func (m *Microvm) Resume(ctx context.Context, id string) (bool, error) {
	m.mu.Lock()
	inst, ok := m.instances[id]
	if !ok {
		m.mu.Unlock()
		return false, fmt.Errorf("no such id %s", id)
	}
	cold, envLive, cfg := inst.Cold, inst.envLive, inst.Cfg
	m.mu.Unlock()

	restarted := false
	if cold {
		// A cold resume is a fresh boot, and a fresh boot needs the session's
		// environment. This driver holds that in memory only (ADR-0003 §2.7
		// item 1), so a record recovered from disk after a runnerd restart
		// has none — including the case where the original session carried no
		// Spec.Env at all, which the driver deliberately cannot tell apart,
		// having refused to write the evidence down.
		//
		// The honest answer is to refuse. Relaunching would boot a guest with
		// a silently empty environment: no relay dial, no proxy, no
		// credentials, and an agent that reports itself healthy.
		//
		// TODO(PR 2): the fix is not to persist the environment. It is the
		// bootstrap token over vsock — sessiond asks cell-gateway for fresh
		// short-lived credentials at boot and after every resume (ADR-0003
		// §2.7 item 1, §4.3) — after which a cold resume needs nothing from
		// the host but the token.
		if !envLive {
			return false, fmt.Errorf("cold resume of %s: this session's guest environment was held in memory only and did not survive a runnerd restart; a clean relaunch needs the bootstrap token over vsock (ADR-0003 §2.7 item 1), which is not implemented yet", id)
		}
		if err := m.engine.Launch(ctx, cfg); err != nil {
			return false, fmt.Errorf("relaunch cold microvm %s: %w", id, err)
		}
		restarted = true
	} else if err := m.engine.Resume(ctx, id); err != nil {
		return false, err
	}

	m.mu.Lock()
	inst, ok = m.instances[id]
	if !ok {
		m.mu.Unlock()
		return restarted, fmt.Errorf("no such id %s", id)
	}
	inst.State = StateRunning
	inst.Cold = false
	if restarted {
		inst.PID = m.engine.PID(id)
	}
	rec := persistable(inst)
	m.mu.Unlock()

	if err := m.saveRecord(rec); err != nil {
		return restarted, fmt.Errorf("save resume metadata %s: %w", id, err)
	}
	return restarted, nil
}

// Snapshot commits the session's environment image.
//
// For the Firecracker engine it does not, yet, and says so: see
// FirecrackerEngine.Snapshot. What this method owns either way is the ref
// (minted here when the caller names none, returned verbatim when they do)
// and the manifest recording what the commit would carry, with the caller's
// stripEnv keys removed from it.
func (m *Microvm) Snapshot(ctx context.Context, id, ref string, stripEnv []string) (Snapshot, error) {
	m.mu.Lock()
	inst, ok := m.instances[id]
	if !ok {
		m.mu.Unlock()
		return Snapshot{}, fmt.Errorf("no such id %s", id)
	}
	m.strips = append(m.strips, slices.Clone(stripEnv))
	if ref == "" {
		m.snapSeq.Add(1)
		ref = fmt.Sprintf("rainier-mvm:%s-%d", id, m.snapSeq.Load())
	}
	survivingKeys := make([]string, 0, len(inst.Cfg.Env))
	for k := range inst.Cfg.Env {
		if !slices.Contains(stripEnv, k) {
			survivingKeys = append(survivingKeys, k)
		}
	}
	slices.Sort(survivingKeys)
	cmd := slices.Clone(inst.Cfg.Cmd)
	m.mu.Unlock()

	snap, err := m.engine.Snapshot(ctx, id, ref, stripEnv)
	if err != nil {
		return Snapshot{}, err
	}

	refDir := m.snapshotRefDir(ref)
	if err := os.MkdirAll(refDir, microvmDirMode); err != nil {
		return Snapshot{}, fmt.Errorf("create snapshot ref dir: %w", err)
	}
	smData, err := json.MarshalIndent(snapshotManifest{
		Ref:          ref,
		InstanceID:   id,
		EnvKeys:      survivingKeys,
		Cmd:          cmd,
		StrippedKeys: slices.Clone(stripEnv),
		CreatedAt:    time.Now(),
	}, "", "  ")
	if err != nil {
		return Snapshot{}, fmt.Errorf("marshal snapshot manifest: %w", err)
	}
	if err := os.WriteFile(filepath.Join(refDir, "manifest.json"), smData, microvmFileMode); err != nil {
		return Snapshot{}, fmt.Errorf("write snapshot manifest: %w", err)
	}

	return snap, nil
}

// Prepull reports whether this host can already boot ref, and records the
// call. It never fabricates an image: see locateRootfs.
func (m *Microvm) Prepull(_ context.Context, ref string) error {
	if ref == "" {
		return errors.New("prepull: empty image ref")
	}
	if _, err := m.locateRootfs(ref); err != nil {
		return err
	}
	m.mu.Lock()
	m.pulls = append(m.pulls, ref)
	m.mu.Unlock()
	return nil
}

func (m *Microvm) Destroy(ctx context.Context, id string) error {
	m.mu.Lock()
	sessionID := ""
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
	inst, ok := m.instances[id]
	if !ok {
		m.mu.Unlock()
		return nil
	}
	tapDevice := inst.Cfg.TapDevice
	m.mu.Unlock()

	if err := m.engine.Stop(ctx, id); err != nil {
		st, stateErr := m.engine.State(ctx, id)
		if stateErr != nil || (st != VMMStateGone && st != VMMStateStopped) {
			return fmt.Errorf("destroy microvm %s: %w", id, err)
		}
	}
	if tapDevice != "" {
		_ = m.tap.Release(tapDevice)
	}

	m.mu.Lock()
	delete(m.instances, id)
	m.mu.Unlock()

	m.deleteInstanceRecord(id)
	return nil
}

func (m *Microvm) RemoveWorkspace(_ context.Context, sessionID string) error {
	if sessionID == "" {
		return nil
	}
	if err := os.Remove(m.workspaceDiskPath(sessionID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove workspace disk for %s: %w", sessionID, err)
	}
	return nil
}

func (m *Microvm) Inspect(ctx context.Context, id string) (Handle, error) {
	m.mu.Lock()
	_, ok := m.instances[id]
	m.mu.Unlock()
	if !ok {
		return Handle{ID: id, State: StateGone}, nil
	}

	// engine.State is I/O against a VMM socket, so it does not run under the
	// driver mutex.
	st, stErr := m.engine.State(ctx, id)

	m.mu.Lock()
	defer m.mu.Unlock()
	inst, ok := m.instances[id]
	if !ok {
		return Handle{ID: id, State: StateGone}, nil
	}
	if stErr == nil {
		reconcileState(inst, st)
	}
	return Handle{ID: id, State: inst.State}, nil
}

func (m *Microvm) Capacity(_ context.Context) (int, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// TODO(PR 3): slots only. ADR-0003 §4.6 wants the microVM's cgroup
	// (cpu.stat, memory.current) measured from outside the guest.
	return m.usedLocked(), m.opts.TotalSlots, nil
}

func (m *Microvm) List(ctx context.Context) ([]Listed, error) {
	m.mu.Lock()
	ids := make([]string, 0, len(m.instances))
	for id := range m.instances {
		ids = append(ids, id)
	}
	m.mu.Unlock()

	observed := make(map[string]VMMState, len(ids))
	for _, id := range ids {
		if st, err := m.engine.State(ctx, id); err == nil {
			observed[id] = st
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Listed, 0, len(m.instances))
	for id, inst := range m.instances {
		if st, ok := observed[id]; ok {
			reconcileState(inst, st)
		}
		out = append(out, Listed{
			SessionID: inst.SessionID,
			Handle:    Handle{ID: id, State: inst.State},
		})
	}
	slices.SortFunc(out, func(a, b Listed) int { return strings.Compare(a.Handle.ID, b.Handle.ID) })
	return out, nil
}

// ---------------------------------------------------------------------------
// Disk formatters
// ---------------------------------------------------------------------------

// Ext4Formatter formats disk images with mkfs.ext4.
type Ext4Formatter struct{ mkfs string }

// NewExt4Formatter resolves mkfs.ext4, or fails.
//
// Fail closed: the alternative, which this driver used to take, was to skip
// the format when the tool was missing and attach a 10 GiB file of zeroes to
// the guest as its workspace.
func NewExt4Formatter() (*Ext4Formatter, error) {
	p, err := exec.LookPath("mkfs.ext4")
	if err != nil {
		return nil, fmt.Errorf("microvm: mkfs.ext4 is not on PATH: session workspace and agent-home images must be formatted before a guest is handed them as block devices: %w", err)
	}
	return &Ext4Formatter{mkfs: p}, nil
}

func (e *Ext4Formatter) Format(path string) error {
	out, err := exec.Command(e.mkfs, "-F", "-q", "-m", "0", path).CombinedOutput()
	if err != nil {
		return fmt.Errorf("mkfs.ext4 %s: %w: %s", path, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// SimulatedDiskFormatter is the formatting half of the test seam. It is never
// constructed by production code; see MicrovmOpts.
type SimulatedDiskFormatter struct{}

func (SimulatedDiskFormatter) Format(string) error { return nil }

// ---------------------------------------------------------------------------
// Linux TAP Manager (production Linux hosts)
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
// Simulated TAP Manager (test seam)
// ---------------------------------------------------------------------------

// SimulatedTapManager is the networking half of the test seam. It is never
// constructed by production code; see MicrovmOpts.
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

// Snapshot records the commit and nothing else. The simulated engine has no
// filesystem to commit, so what it returns is the ref; the driver's manifest
// beside it is what the contract's strip subtest reads back.
func (s *SimulatedEngine) Snapshot(_ context.Context, _, ref string, _ []string) (Snapshot, error) {
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
// Firecracker Engine (production Linux hosts with /dev/kvm)
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

func (c *firecrackerClient) do(ctx context.Context, method, endpoint string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://localhost"+endpoint, bytes.NewReader(data))
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

func (c *firecrackerClient) putJSON(ctx context.Context, endpoint string, payload any) error {
	return c.do(ctx, http.MethodPut, endpoint, payload)
}

func (c *firecrackerClient) patchJSON(ctx context.Context, endpoint string, payload any) error {
	return c.do(ctx, http.MethodPatch, endpoint, payload)
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
	if err := os.MkdirAll(sockDir, microvmDirMode); err != nil {
		return fmt.Errorf("create socket dir: %w", err)
	}
	sockPath := f.socketPath(cfg.ID)
	_ = os.Remove(sockPath)

	// TODO(PR 3): this execs firecracker directly. ADR-0003 §4.5 requires the
	// jailer: per-VM uid and gid, its own cgroup, its own netns, a per-session
	// chroot, and seccomp on the VMM process.
	cmd := exec.Command(f.vmmPath, "--api-sock", sockPath)
	if err := cmd.Start(); err != nil {
		_ = os.RemoveAll(sockDir)
		return fmt.Errorf("start firecracker %s: %w", cfg.ID, err)
	}

	if cmd.Process != nil {
		pidDir := filepath.Join(f.stateDir, "instances", cfg.ID)
		_ = os.MkdirAll(pidDir, microvmDirMode)
		_ = os.WriteFile(f.pidFilePath(cfg.ID), []byte(strconv.Itoa(cmd.Process.Pid)), microvmFileMode)
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
	if err := waitForSocket(ctx, sockPath, firecrackerSocketTimeout); err != nil {
		return fmt.Errorf("wait for firecracker socket %s: %w", cfg.ID, err)
	}

	// 1. Machine configuration
	if err := fcClient.putJSON(ctx, "/machine-config", map[string]any{
		"vcpu_count":   cfg.VCPU,
		"mem_size_mib": cfg.MemoryMiB,
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

	// 4b. Agent home drive. The driver creates and formats this image before
	// launch, so path_on_host always names a real file.
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

	// There is deliberately no MMDS configuration here. MMDS answers at
	// 169.254.169.254 — the exact address ADR-0003 §4.3 requires the host to
	// drop on every TAP — so the config channel and the metadata-denial rule
	// could never both hold. The two PUTs also discarded their errors, and
	// /mmds/config without network_interfaces is rejected outright, so what
	// the guest actually received was nothing.
	//
	// TODO(PR 2): virtio-vsock is the single host-to-guest control channel
	// (ADR-0003 §2.7 item 2), carrying the boot configuration and the
	// bootstrap token. Until it lands, a guest booted by this engine receives
	// no session configuration at all.

	// 6. Start the microVM instance
	if err := fcClient.putJSON(ctx, "/actions", map[string]any{
		"action_type": "InstanceStart",
	}); err != nil {
		return fmt.Errorf("start instance %s: %w", cfg.ID, err)
	}

	f.procs[cfg.ID] = cmd
	initSuccess = true
	return nil
}

// waitForSocket waits for a freshly started VMM to open its API socket, under
// its OWN deadline as well as the caller's: a Create whose context carries no
// deadline must still give up on a VMM that never answers.
func waitForSocket(ctx context.Context, sockPath string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("socket %s did not open within %s: %w", sockPath, timeout, ctx.Err())
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
	fcClient := newFirecrackerClient(f.socketPath(id))
	return fcClient.patchJSON(ctx, "/vm", map[string]any{"state": "Paused"})
}

func (f *FirecrackerEngine) Resume(ctx context.Context, id string) error {
	fcClient := newFirecrackerClient(f.socketPath(id))
	return fcClient.patchJSON(ctx, "/vm", map[string]any{"state": "Resumed"})
}

func (f *FirecrackerEngine) Stop(_ context.Context, id string) error {
	f.mu.Lock()
	cmd, tracked := f.procs[id]
	delete(f.procs, id)
	f.mu.Unlock()

	var pid int
	if tracked && cmd.Process != nil {
		pid = cmd.Process.Pid
	} else if data, err := os.ReadFile(f.pidFilePath(id)); err == nil {
		pid, _ = strconv.Atoi(strings.TrimSpace(string(data)))
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

	sockDir := filepath.Join(f.stateDir, "sockets", id)
	if err := os.RemoveAll(sockDir); err != nil && stopErr == nil {
		stopErr = fmt.Errorf("remove socket dir: %w", err)
	}
	return stopErr
}

// Snapshot refuses, for two separate reasons that both have to hold.
//
// The guest-memory half is an invariant: ADR-0003 §2.2 forbids serializing an
// authenticated session's RAM to durable storage, because an untrusted agent
// process may have copied a decrypted credential anywhere in its heap. The
// previous implementation PUT /snapshot/create with a mem_file_path and wrote
// exactly that artifact.
//
// The filesystem half is unbuilt: committing an environment image means
// committing the session's writable ROOT filesystem (ADR-0003 §4.1, §2.7 item
// 3), and there is no writable upper layer to commit — the rootfs drive is
// read-only with no scratch device above it. Committing the WORKSPACE
// instead, which is what used to happen, inverts the definition: it publishes
// one tenant's files under an environment ref that later sessions boot as
// their root.
//
// TODO(PR 4): an ext4 image published by digest, reflink-copied per session.
func (f *FirecrackerEngine) Snapshot(_ context.Context, _, _ string, _ []string) (Snapshot, error) {
	return Snapshot{}, errors.New("microvm snapshot: committing an environment image for a microVM session is not implemented; it lands with the image work (ADR-0003 §2.7 item 3). Guest memory is never serialized to host disk (ADR-0003 §2.2), and the session workspace is not an environment image")
}

func (f *FirecrackerEngine) State(ctx context.Context, id string) (VMMState, error) {
	f.mu.Lock()
	cmd, tracked := f.procs[id]
	f.mu.Unlock()

	var pid int
	if tracked && cmd.Process != nil {
		pid = cmd.Process.Pid
	} else if data, err := os.ReadFile(f.pidFilePath(id)); err == nil {
		pid, _ = strconv.Atoi(strings.TrimSpace(string(data)))
	}

	// A terminated Firecracker leaves no process to ask, so this engine never
	// answers VMMStateStopped: a cold-parked session reads as Gone here, and
	// it is the DRIVER's own record that tells parked from vanished. See
	// reconcileState.
	if pid <= 0 {
		return VMMStateGone, nil
	}
	sockPath := f.socketPath(id)
	if !isFirecrackerPID(pid, sockPath) {
		return VMMStateGone, nil
	}

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
	cmd, tracked := f.procs[id]
	f.mu.Unlock()
	if tracked && cmd.Process != nil {
		return cmd.Process.Pid
	}
	if data, err := os.ReadFile(f.pidFilePath(id)); err == nil {
		p, _ := strconv.Atoi(strings.TrimSpace(string(data)))
		return p
	}
	return 0
}

func hasKVM() bool {
	_, err := os.Stat("/dev/kvm")
	return err == nil
}

// isFirecrackerPID reports whether pid is the Firecracker serving
// expectedSock, so that a recycled pid is never signalled in a VM's name.
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

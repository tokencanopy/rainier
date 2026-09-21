// internal/driver/microvm.go
//
// The microVM driver: one Firecracker microVM per session, in place of the
// docker driver's one container per session.
//
// This is the third step of that driver, and it is deliberately not a
// working end-to-end path yet. What the second step established is the seam
// and the invariants: a production `--driver=microvm` either has hardware
// virtualization, a kernel, a rootfs, a formatter and the Firecracker binary,
// or it refuses to start; nothing decrypted is written to host disk; a
// cold-parked session reads as suspended rather than gone; and the simulated
// engine behind the tests is reachable only by explicitly injecting it.
//
// What this step adds is the host-to-guest channel those invariants were
// waiting for: virtio-vsock, carrying a session's whole configuration and
// its bootstrap token, in place of a staged file and in place of MMDS. See
// microvm_vsock.go, and docs/design/2026-09-20-microvm-bootstrap-token-and-vsock.md.
//
// The jailer, per-VM network slots, nftables, metering, and real image and
// snapshot work by digest each land in their own change — see the TODO
// markers below and ADR-0003.
package driver

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/tokencanopy/rainier/internal/driver/netslot"
	"github.com/tokencanopy/rainier/protocol/runner"
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

	// firecrackerTermTimeout is how long a VMM gets to exit on SIGTERM before
	// SIGKILL; firecrackerKillTimeout is how long the reap after SIGKILL may
	// take before Stop reports that the process outlived it.
	firecrackerTermTimeout = 3 * time.Second
	firecrackerKillTimeout = 2 * time.Second
)

// MicrovmOpts configures the microVM driver.
//
// Engine, Net and Format are one seam, not three: they are the three places
// this driver touches the host, and they are injected TOGETHER by tests or
// not at all. Production passes none of them and gets the Firecracker engine,
// the real netslot host and mkfs.ext4 — after NewMicrovm has checked that
// this host can actually provide all three. There is deliberately no
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

	// SlotGuestCIDR and SlotUplinkCIDR are the two host-local ranges a
	// session's addresses are carved from, one /30 per slot (ADR-0003 §5.2).
	// Empty means netslot's defaults.
	SlotGuestCIDR  string
	SlotUplinkCIDR string
	// SlotNamePrefix prefixes the namespace, veth and TAP names this driver
	// creates, and is what reclaim recognises as its own. Empty means
	// netslot's default.
	SlotNamePrefix string
	// NetnsDir is where named network namespaces live. Empty means netslot's
	// default, which is where `ip netns` puts them.
	NetnsDir string

	// EgressProxyAddr and EgressProxyPort are the ONE host-side destination
	// a guest may reach through its slot's firewall (ADR-0003 §4.3): the
	// egress proxy this runner was started with. It is an ADDRESS and not a
	// URL because a rule that named a host would be a rule a guest could
	// move by answering a DNS query.
	//
	// Empty means the guest gets no host-side exception at all, which is a
	// legal configuration (a runner with no proxy) and never a reason to
	// leave the firewall off.
	EgressProxyAddr string
	EgressProxyPort int

	// ControlPlaneCIDRs are the regional control-plane ranges a guest must
	// not be able to reach. They are deployment configuration, not a
	// constant: a host given none denies nothing extra rather than guessing
	// at somebody's network.
	ControlPlaneCIDRs []string

	// Jail is the jailer envelope every microVM runs inside (ADR-0003 §4.5):
	// the per-VM uid range, where the per-VM cgroups go, and whether seccomp
	// is on. Zero values are the defaults.
	Jail JailOpts

	// CgroupRoot is the cgroup v2 mount point. Empty means
	// defaultCgroupRoot. It is configurable because the tests point it at a
	// fixture filesystem, and because a host that mounts it elsewhere is a
	// host, not a bug.
	CgroupRoot string

	Engine MicrovmEngine // test seam; nil in production
	Net    netslot.Host  // test seam; nil in production
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
	ID                string   `json:"id"`
	SessionID         string   `json:"session_id"`
	VCPU              int      `json:"vcpu"`
	MemoryMiB         int      `json:"memory_mib"`
	KernelPath        string   `json:"kernel_path"`
	RootfsPath        string   `json:"rootfs_path"`
	WorkspaceDiskPath string   `json:"workspace_disk_path"`
	HomeDiskPath      string   `json:"home_disk_path"`
	Cmd               []string `json:"cmd"`
	DialURL           string   `json:"dial_url"`
	ProxyURL          string   `json:"proxy_url"`

	// The session's network slot (ADR-0003 §5.2). SlotIndex is the one value
	// the rest can be re-derived from, and is what a record recovered after a
	// runnerd restart re-associates with (see recoverDiskInstances); the
	// others are recorded beside it so a host-side investigation does not
	// need the allocator to read a session's address off disk.
	//
	// They replace the single hard-coded 172.18.0.2/172.18.0.1/AA:FC:… every
	// microVM used to boot with, which collided the moment a host ran the two
	// concurrent sessions ADR-0003 §5.1 sizes for.
	SlotIndex    int    `json:"slot_index"`
	Netns        string `json:"netns"`
	TapDevice    string `json:"tap_device"`
	GuestIP      string `json:"guest_ip"`
	GatewayIP    string `json:"gateway_ip"`
	GuestNetmask string `json:"guest_netmask"`
	GuestMAC     string `json:"guest_mac"`
	// VsockUDSPath is the path Firecracker is told to serve this VM's
	// virtio-vsock device on: the guest's connections to host port N are
	// forwarded to "<VsockUDSPath>_N", and 1024 is the only port this design
	// uses. It is a path and not a value — recorded like every other path
	// here, and per BOOT rather than per instance, because the documentation
	// warns that one uds_path cannot be multiplexed across VMs and a cold
	// resume is a new VM.
	//
	// It is named from INSIDE the VM's chroot ("/v1.sock"), because the VMM
	// is jailed and that is the only name it can use. The driver's own end
	// of the same file is the host path (*Microvm).vsockPaths returns.
	VsockUDSPath string            `json:"vsock_uds_path"`
	Env          map[string]string `json:"-"`

	// EgressAllow is the session's per-destination allowlist, which egressd
	// enforces at the proxy. It is NOT what stops a guest reaching the host's
	// metadata service or a neighbour: that is the per-slot nftables ruleset
	// the network slot carries (ADR-0003 §4.3), which is applied on allocate
	// and holds whatever the guest does with its own routes or proxy
	// variables.
	EgressAllow []string `json:"egress_allow"`
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

	// boot is the configuration the guest is handed over vsock, and channel
	// is the host end of that conn. Both are unexported and therefore never
	// serialized, which is the point: the configuration carries the session's
	// bootstrap token and ADR-0003 §2.7 item 1 keeps every part of it off a
	// shared host's disk.
	//
	// bootLive says the pair is this process's own. A record recovered from
	// disk after a runnerd restart has no configuration behind it, and a cold
	// resume must say so rather than boot a guest that will never be told
	// what it is. (The bootstrap token would survive such a restart — the
	// runner can mint a fresh one — but the rest of the configuration would
	// not, and persisting it is precisely what this design does not do.)
	boot     runner.BootConfig
	channel  *guestChannel
	bootLive bool

	// slot is the network slot this instance holds, or nil when it holds
	// none — a cold-parked session, which has no VM and therefore no reason
	// to keep a /30 and a namespace off the pool for the whole dormant
	// window. It is unexported because the POOL owns it: what survives on
	// disk is Cfg.SlotIndex, and a record recovered after a restart is
	// re-associated through it (see recoverDiskInstances).
	slot *netslot.Slot

	// boots counts launches of this instance, so each one gets a vsock socket
	// path of its own. A cold resume is a new VM and Firecracker's own
	// documentation warns that one uds_path cannot be multiplexed across two.
	//
	// It is read AND advanced under the driver mutex by the one resume that
	// claimed the instance (see resuming), so no two boots of one instance
	// can ever be handed the same path.
	boots int

	// resuming is the claim one Resume holds over this instance while it is
	// in flight. A second Resume for the same id is refused rather than run
	// beside it: see Resume for what two concurrent cold ones would do to
	// each other's socket.
	resuming bool

	// epoch counts mutations of this record, and exists because Inspect and
	// List ask the hypervisor with the driver mutex RELEASED (its answer is
	// socket I/O, and holding the mutex across it froze every other session
	// on the host). That window is long enough for a Suspend to complete, and
	// stamping a stale "running" over the record afterwards would leak a slot
	// and send the next Resume down the warm path into a VM that is no longer
	// there. A reconcile therefore reads the epoch before it asks, and drops
	// its answer on the floor if the record moved while it was asking.
	epoch uint64
}

// bump marks a record as changed. Every mutation of a live record goes
// through it, under the driver mutex.
func (rec *instanceRecord) bump() { rec.epoch++ }

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

// A session's configuration is no longer staged on the host at all.
//
// It used to be written to "<StateDir>/instances/<id>/session.json", against
// the day something would carry it into the guest. That day is this change,
// and what carries it is virtio-vsock: the configuration is composed in
// memory (bootConfigFor), handed to the guest as the first control frame on
// the conn it opens, and never written anywhere. A file is not needed, and a
// file on a shared host that describes one tenant's session — and, once the
// bootstrap token exists, carries a capability — is exactly what ADR-0003
// §2.7 asks for there not to be.
//
// It was also never delivered through the session's own workspace, for a
// reason that still holds and is worth keeping written down: a file inside a
// volume the agent can write is a file the agent can rewrite, and a sessiond
// reading its session id or its proxy back out of one would let a session
// re-register as another or point its egress somewhere else. Over vsock the
// configuration arrives on a socket inside one VM's own directory, which the
// guest cannot write and cannot forge.

// Microvm implements driver.Driver for hardware-isolated microVMs.
type Microvm struct {
	mu        sync.Mutex
	opts      MicrovmOpts
	engine    MicrovmEngine
	slots     *netslot.Pool
	format    DiskFormatter
	seq       int
	pending   int // slots reserved by an in-flight Create, counted as used
	snapSeq   atomic.Int64
	instances map[string]*instanceRecord
	pulls     []string
	strips    [][]string

	// host is the runner above this driver: where a guest's control
	// connection goes, and who asks the control plane for a fresh bootstrap
	// token on a cold resume. nil is a real state — see MicrovmHost.
	host MicrovmHost
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
	engine, net, format := opts.Engine, opts.Net, opts.Format
	switch {
	case engine == nil && net == nil && format == nil:
		// The production path. Everything below must be true of this host.
		if err := checkMicrovmHost(opts); err != nil {
			return nil, err
		}
		ext4, err := NewExt4Formatter()
		if err != nil {
			return nil, err
		}
		linux, err := netslot.NewLinuxHost(opts.NetnsDir)
		if err != nil {
			return nil, err
		}
		engine = NewFirecrackerEngine(FirecrackerOpts{
			VMMPath:  opts.VMMPath,
			StateDir: opts.StateDir,
			NetnsDir: opts.NetnsDir,
			Jail:     opts.Jail,
		})
		net = linux
		format = ext4
	case engine != nil && net != nil && format != nil:
		// The test seam, injected whole.
	default:
		return nil, errors.New("microvm: MicrovmOpts.Engine, .Net and .Format are one seam and must be injected together or not at all; injecting some of them leaves a production component talking to a simulated one")
	}

	slots, err := netslot.New(opts.slotConfig(), net)
	if err != nil {
		return nil, err
	}

	for _, dir := range []string{"workspaces", "homes", "instances", filepath.Join("snapshots", "refs"), "rootfs"} {
		if err := os.MkdirAll(filepath.Join(opts.StateDir, dir), microvmDirMode); err != nil {
			return nil, fmt.Errorf("microvm: create state directory: %w", err)
		}
	}

	m := &Microvm{
		opts:      opts,
		engine:    engine,
		slots:     slots,
		format:    format,
		instances: make(map[string]*instanceRecord),
	}
	// Records first, leftovers second, and never the other way round: a
	// session that outlived its runnerd still holds its slot, and a reclaim
	// that ran first would tear the network out from under a live guest.
	m.recoverDiskInstances()
	m.reclaimNetworkSlots()
	return m, nil
}

// slotConfig is the network envelope this runner hands the slot allocator.
func (o MicrovmOpts) slotConfig() netslot.Config {
	return netslot.Config{
		GuestCIDR:         o.SlotGuestCIDR,
		UplinkCIDR:        o.SlotUplinkCIDR,
		Slots:             o.TotalSlots,
		NamePrefix:        o.SlotNamePrefix,
		NetnsDir:          o.NetnsDir,
		ProxyAddr:         o.EgressProxyAddr,
		ProxyPort:         o.EgressProxyPort,
		ControlPlaneCIDRs: slices.Clone(o.ControlPlaneCIDRs),
	}
}

// reclaimNetworkSlots tears down slots left on this host by a previous
// runnerd, after recoverDiskInstances has re-associated the ones whose
// sessions are still here.
//
// A failure is logged and not fatal. The alternative — refusing to start
// because one leftover namespace would not go away — takes a whole host's
// sessions offline over one slot that the pool already knows not to hand out.
func (m *Microvm) reclaimNetworkSlots() {
	n, err := m.slots.Reclaim(context.Background())
	if err != nil {
		log.Printf("microvm: reclaiming leftover network slots: %v", err)
	}
	if n > 0 {
		log.Printf("microvm: reclaimed %d network slot(s) left by a previous run", n)
	}
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
	vmm := opts.VMMPath
	if vmm == "" {
		vmm = jailExecName
	}
	if _, err := exec.LookPath(vmm); err != nil {
		return fmt.Errorf("microvm: firecracker executable %q not found: %w", vmm, err)
	}
	// The jailer, and it is not optional: ADR-0003 §4.5 requires every
	// microVM to run under it, and this driver has no unjailed launch path.
	jailer := opts.Jail.JailerPath
	if jailer == "" {
		jailer = "jailer"
	}
	if _, err := exec.LookPath(jailer); err != nil {
		return fmt.Errorf("microvm: firecracker's jailer %q not found: every microVM runs under it (per-VM uid and gid, its own cgroup, its own netns, a chroot, seccomp) and there is deliberately no unjailed fallback: %w", jailer, err)
	}
	// A jailed VM runs as neither the owner of the shared images nor a
	// member of the runner's group, so it can only read them through the
	// "other" bit. Checked here, at startup, rather than at the first create:
	// it is a property of the operator's configuration and they can fix it
	// before a session ever lands.
	for _, shared := range []string{opts.KernelPath, opts.BaseRootfs} {
		if err := checkSharedImageReadable(shared); err != nil {
			return err
		}
	}

	// Machine facts, last, because an operator cannot change them with a
	// flag and the configuration above is what they came to fix.
	kvm, err := os.OpenFile("/dev/kvm", os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("microvm: /dev/kvm is not usable by this process: a microVM session is a hardware-isolated VM and there is no software fallback: %w.\n\n%s", err, MicrovmHostRequirements())
	}
	_ = kvm.Close()
	return checkMicrovmPrivileges(opts)
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

// pathSegment is the character set this driver will put in a host path.
//
// Every name it joins onto StateDir — a session id, an agent home volume, an
// instance id — arrives from the control plane or off the disk, and
// filepath.Join CLEANS its result, so a session id of "../../etc" does not
// produce a silly path, it produces a real one outside the state directory.
// RemoveWorkspace would then delete that file. Nothing legitimate needs a
// separator or a dot segment, so the answer is to refuse rather than to
// sanitize: a name this rejects is a bug or an attack, and neither is served
// by guessing what it meant.
var pathSegment = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

func checkPathSegment(what, name string) error {
	if name == "" {
		return fmt.Errorf("microvm: %s is empty", what)
	}
	if !pathSegment.MatchString(name) {
		return fmt.Errorf("microvm: %s %q is not a legal name: only letters, digits, %q and %q, so that it can never name anything outside the state directory", what, name, "_", "-")
	}
	return nil
}

func (m *Microvm) instanceDir(id string) string {
	return filepath.Join(m.opts.StateDir, "instances", id)
}

func (m *Microvm) instanceMetaPath(id string) string {
	return filepath.Join(m.instanceDir(id), "instance.json")
}

// persistable copies a record for writing to disk. The copy drops Cfg.Env and
// the guest's boot configuration — which being unexported, or `json:"-"`,
// would drop anyway — so that a record handed to a goroutine outside the
// driver mutex carries no reference to the live map, the live configuration,
// or the bootstrap token in it either.
func persistable(rec *instanceRecord) instanceRecord {
	cp := *rec
	cp.Cfg.Env = nil
	cp.boot = runner.BootConfig{}
	cp.channel = nil
	cp.bootLive = false
	cp.slot = nil
	return cp
}

// applySlot stamps a network slot's addressing onto a VM configuration.
// Everything here is derived from the slot index, and is recorded so that a
// host-side investigation can read a session's address off disk without the
// allocator.
func applySlot(cfg *VMMConfig, slot *netslot.Slot) {
	if slot == nil {
		cfg.SlotIndex, cfg.Netns, cfg.TapDevice = 0, "", ""
		cfg.GuestIP, cfg.GatewayIP, cfg.GuestNetmask, cfg.GuestMAC = "", "", "", ""
		return
	}
	cfg.SlotIndex = slot.Index
	cfg.Netns = slot.Netns
	cfg.TapDevice = slot.Tap
	cfg.GuestIP = slot.GuestIP.String()
	cfg.GatewayIP = slot.GatewayIP.String()
	cfg.GuestNetmask = slot.GuestNetmask()
	cfg.GuestMAC = slot.MAC
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
		if checkPathSegment("instance id", id) != nil {
			continue
		}
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
		m.reassociateSlot(&rec)
		m.instances[id] = &rec
		if n, _ := strconv.Atoi(strings.TrimPrefix(id, "mvm-")); n > m.seq {
			m.seq = n
		}
	}
}

// reassociateSlot puts a recovered record back in touch with its network
// slot, or clears the record's claim on one.
//
// Two cases, and the difference is what the record says about itself rather
// than what the host says:
//
//   - A session that is still running (or warm-paused) is using its slot
//     right now: its guest is on that address and its TAP is in that
//     namespace. The slot is re-claimed so the pool will not hand the index
//     to anyone else, and reclaim leaves it alone.
//   - A cold-parked session has no VM. Its slot was released at suspend and
//     the namespace is already gone; the record keeps the index only as
//     history, and the cold resume allocates a fresh one.
//
// A claim that cannot be honoured — two records naming one index, an index
// outside this runner's envelope after the operator shrank --slots — is
// logged and dropped rather than fatal: the session keeps its files and
// reads as suspended, which is recoverable, where refusing to start takes the
// whole host down.
func (m *Microvm) reassociateSlot(rec *instanceRecord) {
	if rec.Cfg.SlotIndex == 0 {
		return
	}
	if rec.State != StateRunning && !(rec.State == StateSuspended && !rec.Cold) {
		applySlot(&rec.Cfg, nil)
		return
	}
	slot, err := m.slots.Adopt(rec.Cfg.SlotIndex, rec.ID)
	if err != nil {
		log.Printf("microvm: %s claims network slot %d and cannot have it (%v); the session keeps its files and its slot is left to reclaim", rec.ID, rec.Cfg.SlotIndex, err)
		applySlot(&rec.Cfg, nil)
		return
	}
	rec.slot = slot
	applySlot(&rec.Cfg, slot)
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
// Callers hold the driver mutex, and must have checked that the record has
// not moved since they asked the hypervisor (see instanceRecord.epoch).
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
	rec.bump()
}

// workspaceDiskPath returns the host disk-image path for sessionID's
// workspace, or an error if sessionID is not a name that may become one.
func (m *Microvm) workspaceDiskPath(sessionID string) (string, error) {
	if err := checkPathSegment("session id", sessionID); err != nil {
		return "", err
	}
	return filepath.Join(m.opts.StateDir, "workspaces", workspaceVolume(sessionID)+".ext4"), nil
}

// homeDiskPath returns the host disk-image path for an agent home volume.
//
// Homes live in their own directory rather than beside the workspaces. They
// belong to a (creator, workspace) and not to a session, they outlive every
// session that mounts them, and ADR-0003 §2.3 keeps them out of the workspace
// disk and its checkpoint — a shared directory is one Volumes() scan or one
// teardown glob away from treating the two as the same thing.
func (m *Microvm) homeDiskPath(volume string) (string, error) {
	if err := checkPathSegment("agent home volume", volume); err != nil {
		return "", err
	}
	return filepath.Join(m.opts.StateDir, "homes", volume+".ext4"), nil
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

func (m *Microvm) snapshotRefDir(ref string) (string, error) {
	seg, err := sanitizeRef(ref)
	if err != nil {
		return "", err
	}
	return filepath.Join(m.opts.StateDir, "snapshots", "refs", seg), nil
}

// snapshotManifestBytes returns the raw manifest a commit wrote, or nil.
func (m *Microvm) snapshotManifestBytes(ref string) []byte {
	dir, err := m.snapshotRefDir(ref)
	if err != nil {
		return nil
	}
	data, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
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

// sanitizeRef turns an opaque image ref into one path segment, or refuses.
//
// Escaping alone is not enough. url.PathEscape leaves "." and ".." exactly as
// they are, so a ref of ".." used to name the PARENT of the refs directory
// and a commit wrote its manifest one level up — over whatever was there. The
// dot segments are therefore rejected outright, and the escaped result is
// re-checked for a separator rather than trusted to have none.
func sanitizeRef(ref string) (string, error) {
	if ref == "" {
		return "", errors.New("microvm: empty snapshot ref")
	}
	seg := url.PathEscape(strings.ReplaceAll(ref, ":", "_"))
	switch seg {
	case "", ".", "..":
		return "", fmt.Errorf("microvm: snapshot ref %q does not name anything this host can store", ref)
	}
	if strings.ContainsAny(seg, `/\`) || seg != filepath.Base(seg) {
		return "", fmt.Errorf("microvm: snapshot ref %q would name a path, not an entry", ref)
	}
	return seg, nil
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
	seg, err := sanitizeRef(rootfsRef)
	if err != nil {
		return "", err
	}
	cached := filepath.Join(m.opts.StateDir, "rootfs", seg+".ext4")
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

// buildGuestEnv is the driver's own record of the CONFIGURATION variables a
// session was created with. It is held in memory for the lifetime of this
// process and never written to disk; see VMMConfig.Env.
//
// It no longer copies spec.Env, and that deletion is the point of this
// change. Spec.Env is where an environment's decrypted secret_refs live on
// the Docker path, and on this one they are not dispatched at all — the
// create carries their names and a token instead, and the values reach the
// guest from the control plane over the exchange the boot configuration
// starts. What a guest actually runs on arrives over vsock
// (bootConfigFor), not from this map, which is why nothing here is a
// delivery channel any more: it is what a snapshot's manifest names as
// having been configured, and nothing else reads it.
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
	return env
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
	// Before anything with a side effect, and before a slot is even
	// reserved: a create this host must not perform is refused rather than
	// half-performed. See refuseUnwithheldEnv.
	if err := refuseUnwithheldEnv(spec); err != nil {
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

	workspaceDisk := ""
	if spec.SessionID != "" {
		path, err := m.workspaceDiskPath(spec.SessionID)
		if err != nil {
			return nil, err
		}
		disk, created, err := m.ensureDisk(path)
		if err != nil {
			return nil, err
		}
		workspaceDisk = disk
		if created {
			undo = append(undo, func() { _ = os.Remove(disk) })
		}
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
		path, err := m.homeDiskPath(spec.Home.Volume)
		if err != nil {
			return nil, err
		}
		if homeDisk, _, err = m.ensureDisk(path); err != nil {
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

	undo = append(undo, func() { m.deleteInstanceRecord(id) })

	// The guest's control channel, opened BEFORE the VM starts: the guest
	// dials host port 1024 as soon as its sessiond is up, and a listener
	// created after InstanceStart is a race whose loser is a session that
	// boots and is never configured.
	bootCfg := bootConfigFor(spec)
	udsPath, listenPath, err := m.vsockPaths(id, 1)
	if err != nil {
		return nil, err
	}
	channel, err := m.openGuestChannel(spec.SessionID, udsPath, listenPath, bootCfg)
	if err != nil {
		return nil, err
	}
	undo = append(undo, channel.close)

	slot, err := m.slots.Allocate(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("allocate network slot for %s: %w", id, err)
	}
	undo = append(undo, func() { _ = m.slots.Release(context.WithoutCancel(ctx), slot) })

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
		VsockUDSPath:      vsockGuestPath(1),
		Env:               buildGuestEnv(spec),
	}
	applySlot(&cfg, slot)

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
		slot:      slot,
		boot:      bootCfg,
		channel:   channel,
		bootLive:  true,
		boots:     1,
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
	var released *netslot.Slot
	if !warm {
		inst.PID = 0
		// The VM is gone, so its vsock socket is a path nothing serves. It
		// is closed and removed here rather than left for the resume to
		// overwrite: a stale socket on a shared host is one more file
		// describing a session that is not running, and the resume gets a
		// path of its own anyway.
		if inst.channel != nil {
			inst.channel.close()
			inst.channel = nil
		}
		// The slot goes back to the pool for the same reason the VM goes
		// away: a cold-parked session is not occupying this host, and a /30,
		// a namespace and a firewall held for a dormant window nobody
		// bounded is capacity ADR-0003 §5.1 counted on having. A cold resume
		// allocates a fresh one.
		released, inst.slot = inst.slot, nil
		applySlot(&inst.Cfg, nil)
	}
	inst.bump()
	rec := persistable(inst)
	m.mu.Unlock()

	if released != nil {
		if err := m.slots.Release(ctx, released); err != nil {
			// Not fatal to the suspend: the session IS parked, its files are
			// intact, and the pool has already recorded the index as one
			// that needs finishing before reuse.
			log.Printf("microvm: releasing network slot %d for cold-parked %s: %v", released.Index, id, err)
		}
	}

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
	running := inst.State == StateRunning
	cold, bootLive, cfg := inst.Cold, inst.bootLive, inst.Cfg
	bootCfg, sessionID := inst.boot, inst.SessionID

	// Resuming a session that is already running restarts nothing, and says
	// so without touching the hypervisor (driver.go's Resume contract). The
	// call is not merely redundant: Firecracker answers PATCH /vm {"state":
	// "Resumed"} with 400 on a VM that was never paused, so an unconditional
	// resume turned "nothing to do" into a failed Resume — and runnerd reads
	// a failed Resume as a session it could not bring back.
	if running {
		m.mu.Unlock()
		return false, nil
	}

	// One resume at a time per instance, claimed under the same lock that
	// reads the state it decided on.
	//
	// Two concurrent cold resumes would otherwise both see the same state,
	// both compute the same next boot number, and both open a channel on the
	// same socket path — and openGuestChannel UNLINKS before it listens, so
	// the second would remove the first's live socket out from under a VM
	// that had already been told about it. That session then boots and is
	// never configured, which is the one outcome this whole path exists to
	// prevent. The pair of them would also mint two tokens and launch twice.
	if inst.resuming {
		m.mu.Unlock()
		return false, fmt.Errorf("resume of %s: a resume is already in flight for this instance", id)
	}
	inst.resuming = true
	// The boot number is taken HERE, under the same lock, and it advances
	// even for an attempt that fails: a failed launch may have left a socket
	// behind at that path, and an attempt that reuses a number is an attempt
	// that inherits it. The counter is only ever a source of distinct paths.
	boots := inst.boots
	if cold {
		inst.boots++
		boots = inst.boots
	}
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		if e, ok := m.instances[id]; ok {
			e.resuming = false
		}
		m.mu.Unlock()
	}()

	restarted := false
	var (
		channel *guestChannel
		slot    *netslot.Slot
	)
	// A cold resume builds a whole new VM, so anything it allocated has to be
	// given back when it does not get there.
	defer func() {
		if slot != nil {
			_ = m.slots.Release(context.WithoutCancel(ctx), slot)
		}
	}()
	if cold {
		// A cold resume is a fresh boot, and a fresh boot needs the session's
		// whole configuration. This driver holds that in memory only
		// (ADR-0003 §2.7 item 1), so a record recovered from disk after a
		// runnerd restart has none — including the case where the original
		// session carried nothing secret at all, which the driver
		// deliberately cannot tell apart, having refused to write the
		// evidence down.
		//
		// The honest answer is to refuse. Relaunching would boot a guest that
		// is never told what it is: no session id, no proxy, no boot chain,
		// no secrets, and an agent that reports itself healthy.
		//
		// The bootstrap token is not what is missing here — the runner can
		// mint a fresh one, and does, three lines below. What is missing is
		// everything else, and persisting THAT is what this design rules out.
		// Rebuilding it from the control plane on a resume is the follow-up
		// (a create-shaped resume, ADR-0003 §2.3's portable checkpoint).
		if !bootLive {
			return false, fmt.Errorf("cold resume of %s: this session's guest configuration was held in memory only (ADR-0003 §2.7 item 1) and did not survive a runnerd restart; a clean relaunch needs the control plane to re-resolve it, which is the portable-checkpoint work and not this change", id)
		}
		// A new VM gets a new token and a new socket. The token because the
		// old one is single-use and fenced by a placement generation the
		// plane may have moved past; the socket because Firecracker's own
		// documentation warns that one uds_path cannot be multiplexed across
		// two VMs.
		host := m.currentHost()
		if host == nil {
			return false, fmt.Errorf("cold resume of %s: this driver has no runner above it to mint a bootstrap token", id)
		}
		token, err := host.MintSessionBootstrap(ctx, sessionID)
		if err != nil {
			return false, fmt.Errorf("cold resume of %s: minting a bootstrap token: %w", id, err)
		}
		bootCfg.BootstrapToken = token
		udsPath, listenPath, err := m.vsockPaths(id, boots)
		if err != nil {
			return false, err
		}
		if channel, err = m.openGuestChannel(sessionID, udsPath, listenPath, bootCfg); err != nil {
			return false, err
		}
		cfg.VsockUDSPath = vsockGuestPath(boots)
		// A new VM gets a new network slot too: the one this session had was
		// returned to the pool when it was parked, and may be another
		// session's by now.
		if slot, err = m.slots.Allocate(ctx, id); err != nil {
			channel.close()
			return false, fmt.Errorf("cold resume of %s: allocating a network slot: %w", id, err)
		}
		applySlot(&cfg, slot)
		if err := m.engine.Launch(ctx, cfg); err != nil {
			channel.close()
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
		if channel != nil {
			channel.close()
		}
		return restarted, fmt.Errorf("no such id %s", id)
	}
	inst.State = StateRunning
	inst.Cold = false
	if restarted {
		inst.PID = m.engine.PID(id)
		if inst.channel != nil {
			inst.channel.close()
		}
		inst.channel = channel
		inst.boot = bootCfg
		inst.Cfg.VsockUDSPath = cfg.VsockUDSPath
		inst.slot = slot
		applySlot(&inst.Cfg, slot)
		// The deferred release must not take back the slot the record now
		// holds, so the handover is recorded by clearing the local.
		slot = nil
		// inst.boots was advanced when this resume claimed the instance, so
		// that the path it opened could not collide with a concurrent one.
	}
	inst.bump()
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
	// What this session was CONFIGURED with, from both places a key can come
	// from: the driver's own injection (buildGuestEnv) and the configuration
	// block the guest was handed over vsock. Both, because the strip list is
	// a promise about the committed image and a key that survived through
	// the channel the driver does not happen to be looking at is a key that
	// survived.
	//
	// Keys, never values: see snapshotManifest.
	surviving := map[string]struct{}{}
	for k := range inst.Cfg.Env {
		surviving[k] = struct{}{}
	}
	for k := range inst.boot.Env {
		surviving[k] = struct{}{}
	}
	survivingKeys := make([]string, 0, len(surviving))
	for k := range surviving {
		if !slices.Contains(stripEnv, k) {
			survivingKeys = append(survivingKeys, k)
		}
	}
	slices.Sort(survivingKeys)
	cmd := slices.Clone(inst.Cfg.Cmd)
	m.mu.Unlock()

	// The ref becomes a directory name, so it is checked before anything with
	// a side effect runs rather than on the way to writing the manifest.
	refDir, err := m.snapshotRefDir(ref)
	if err != nil {
		return Snapshot{}, err
	}

	snap, err := m.engine.Snapshot(ctx, id, ref, stripEnv)
	if err != nil {
		return Snapshot{}, err
	}

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

	// TODO(PR 4): an id this driver does not know resolves to an empty
	// session id, so RemoveWorkspace below is a no-op and the disk image
	// stays on the host with nothing left to name it — one leaked workspace
	// per Destroy that arrives after the record is gone (a reconcile that
	// raced a crash, a retried rm). Fixing it means finding the workspace
	// from the disk rather than from the record, which is the same lookup the
	// image work needs.
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
	slot := inst.slot
	inst.slot = nil
	channel := inst.channel
	inst.channel = nil
	m.mu.Unlock()
	// Closed before the VM is signalled, so nothing can dial a control
	// channel for a session that is being torn down.
	if channel != nil {
		channel.close()
	}

	if err := m.engine.Stop(ctx, id); err != nil {
		st, stateErr := m.engine.State(ctx, id)
		if stateErr != nil || (st != VMMStateGone && st != VMMStateStopped) {
			// The slot is deliberately NOT released here. The VM may still
			// be alive on that TAP, and taking its namespace away would
			// leave a running guest with a half-torn-down network while the
			// index went back to the pool for the next session to build on
			// top of. The record keeps the slot; a later Destroy, or the
			// next runnerd's reclaim, finishes it.
			m.mu.Lock()
			if inst, ok := m.instances[id]; ok && inst.slot == nil {
				inst.slot = slot
			}
			m.mu.Unlock()
			return fmt.Errorf("destroy microvm %s: %w", id, err)
		}
	}
	if slot != nil {
		if err := m.slots.Release(ctx, slot); err != nil {
			// The VM is gone and the record is going with it, so this is not
			// a failed teardown of the SESSION. The pool has recorded the
			// index as one that needs finishing before reuse.
			log.Printf("microvm: releasing network slot %d for destroyed %s: %v", slot.Index, id, err)
		}
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
	// A teardown is the one path where an unchecked id does real damage:
	// os.Remove of whatever "../../something" resolved to. An id that cannot
	// name a workspace is an error, not a removal.
	path, err := m.workspaceDiskPath(sessionID)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove workspace disk for %s: %w", sessionID, err)
	}
	return nil
}

func (m *Microvm) Inspect(ctx context.Context, id string) (Handle, error) {
	m.mu.Lock()
	inst, ok := m.instances[id]
	if !ok {
		m.mu.Unlock()
		return Handle{ID: id, State: StateGone}, nil
	}
	epoch := inst.epoch
	m.mu.Unlock()

	// engine.State is I/O against a VMM socket, so it does not run under the
	// driver mutex — and the record may therefore have moved underneath it.
	st, stErr := m.engine.State(ctx, id)

	m.mu.Lock()
	defer m.mu.Unlock()
	inst, ok = m.instances[id]
	if !ok {
		return Handle{ID: id, State: StateGone}, nil
	}
	if stErr == nil && inst.epoch == epoch {
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
	epochs := make(map[string]uint64, len(m.instances))
	for id, inst := range m.instances {
		epochs[id] = inst.epoch
	}
	m.mu.Unlock()

	// Every engine.State here is I/O against a VMM socket, so none of it runs
	// under the driver mutex, and each answer is only good for the record as
	// it stood when the epoch above was read.
	observed := make(map[string]VMMState, len(epochs))
	for id := range epochs {
		if st, err := m.engine.State(ctx, id); err == nil {
			observed[id] = st
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Listed, 0, len(m.instances))
	for id, inst := range m.instances {
		if st, ok := observed[id]; ok && inst.epoch == epochs[id] {
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

// ---------------------------------------------------------------------------
// Firecracker Engine (production Linux hosts with /dev/kvm)
// ---------------------------------------------------------------------------

type FirecrackerEngine struct {
	mu         sync.Mutex
	vmmPath    string
	jailerPath string
	stateDir   string
	netnsDir   string
	jail       JailOpts
	uids       *uidAllocator
	starter    processStarter
	procs      map[string]vmmProcess
	initErr    error
	// kvm is the "can this host run a VM at all" check, as a field so the
	// jail tests can run on a machine without /dev/kvm. Production never
	// replaces it, and NewMicrovm has already refused on a host where it
	// answers false, so this is the second of two checks rather than the
	// only one.
	kvm func() bool
}

// JailOpts is the jail envelope: the uid range, where the per-VM cgroups go,
// and whether seccomp is on. Zero values mean the defaults in
// microvm_jailer.go, and Seccomp is expressed as "off" so that no
// configuration mistake can leave the filter off by omission.
type JailOpts struct {
	JailerPath   string
	UIDFirst     int
	UIDCount     int
	CgroupParent string
	SeccompOff   bool
	// RunnerGID is the group given to every jail directory and per-session
	// image, so runnerd can still create the next boot's control socket and
	// tear the jail down without being the VM's user. Zero means this
	// process's own gid, which is what a runner wants in every case that is
	// not a test.
	RunnerGID int
}

// FirecrackerOpts is what the production engine is built from.
type FirecrackerOpts struct {
	VMMPath  string
	StateDir string
	NetnsDir string
	Jail     JailOpts

	// Starter is the test seam that reads back the argv a host would be
	// asked to run. nil is production.
	Starter processStarter
}

// NewFirecrackerEngine builds the production engine, or one that refuses
// every launch and says why.
//
// It refuses without the JAILER as firmly as without Firecracker itself.
// ADR-0003 §4.5 requires every microVM to run under it, and an engine that
// fell back to exec'ing Firecracker directly would put a tenant's VMM in
// runnerd's own user, mount namespace and file descriptor table — which is
// the blast radius the bakeoff names as this substrate's primary risk. There
// is no flag that produces that launch and no code path that reaches it.
func NewFirecrackerEngine(opts FirecrackerOpts) *FirecrackerEngine {
	vmmPath := opts.VMMPath
	if vmmPath == "" {
		vmmPath = jailExecName
	}
	jailerPath := opts.Jail.JailerPath
	if jailerPath == "" {
		jailerPath = "jailer"
	}
	jail := opts.Jail
	if jail.CgroupParent == "" {
		jail.CgroupParent = defaultJailCgroupParent
	}
	if jail.RunnerGID <= 0 {
		jail.RunnerGID = os.Getgid()
	}

	var initErr error
	if resolved, err := exec.LookPath(vmmPath); err != nil {
		initErr = fmt.Errorf("firecracker executable %q not found on PATH: %w", vmmPath, err)
	} else {
		vmmPath = resolved
	}
	if resolved, err := exec.LookPath(jailerPath); err != nil {
		if initErr == nil {
			initErr = fmt.Errorf("firecracker's jailer %q not found on PATH: ADR-0003 §4.5 requires every microVM to run under it — a dedicated uid and gid, its own cgroup, its own netns, a chroot and seccomp — and there is deliberately no unjailed launch path: %w", jailerPath, err)
		}
	} else {
		jailerPath = resolved
	}

	uids, err := newUIDAllocator(jail.UIDFirst, jail.UIDCount)
	if err != nil && initErr == nil {
		initErr = err
	}

	starter := opts.Starter
	if starter == nil {
		starter = execStarter{}
	}

	netnsDir := opts.NetnsDir
	if netnsDir == "" {
		netnsDir = netslot.DefaultNetnsDir
	}

	return &FirecrackerEngine{
		vmmPath:    vmmPath,
		jailerPath: jailerPath,
		stateDir:   opts.StateDir,
		netnsDir:   netnsDir,
		jail:       jail,
		uids:       uids,
		starter:    starter,
		procs:      make(map[string]vmmProcess),
		initErr:    initErr,
		kvm:        hasKVM,
	}
}

// jailSpecFor resolves one instance's jail.
func (f *FirecrackerEngine) jailSpecFor(cfg VMMConfig) (jailSpec, error) {
	uid, err := f.uids.claim(cfg.ID)
	if err != nil {
		return jailSpec{}, err
	}
	netns := ""
	if cfg.Netns != "" {
		netns = filepath.Join(f.netnsDir, cfg.Netns)
	}
	return jailSpec{
		ID:        cfg.ID,
		ExecFile:  f.vmmPath,
		Base:      jailBaseDir(f.stateDir),
		Root:      jailRootDir(f.stateDir, cfg.ID),
		UID:       uid,
		GID:       uid,
		RunnerGID: f.jail.RunnerGID,
		Netns:     netns,
		Cgroup:    f.jail.CgroupParent,
		Seccomp:   !f.jail.SeccompOff,
	}, nil
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

// socketPath is where the VMM's API socket lands ON THE HOST.
//
// It is inside the jail, because a chrooted Firecracker can only create it
// there. What Firecracker itself is told is jailAPISocketPath, the same file
// named from inside the chroot; this engine's HTTP client dials the host
// name. The two are one file and there is exactly one place each spelling is
// produced.
func (f *FirecrackerEngine) socketPath(id string) string {
	return filepath.Join(jailRootDir(f.stateDir, id), jailAPISocketPath)
}

func (f *FirecrackerEngine) pidFilePath(id string) string {
	return filepath.Join(f.stateDir, "instances", id, "pid")
}

// microvmBootArgsBase is the part of the guest kernel command line that is
// the same for every session.
const microvmBootArgsBase = "console=ttyS0 reboot=k panic=1 pci=off"

// bootArgs is the guest kernel command line for one VM.
//
// The addressing comes from the session's network slot. It used to be a
// constant — one address, one gateway, one /16 netmask for every microVM on
// the host — which collided the moment a host ran the two concurrent sessions
// ADR-0003 §5.1 sizes it for. `ip=` is the kernel's own built-in
// configuration, which is why the netmask is dotted rather than a prefix
// length: it predates CIDR notation and will not parse "/30".
//
// A configuration with no slot gets no `ip=` at all rather than a default.
// A guest that boots with the wrong address is a guest on someone else's /30.
func bootArgs(cfg VMMConfig) string {
	if cfg.GuestIP == "" || cfg.GatewayIP == "" {
		return microvmBootArgsBase + " init=/init"
	}
	return fmt.Sprintf("%s ip=%s::%s:%s:guest:eth0:off init=/init",
		microvmBootArgsBase, cfg.GuestIP, cfg.GatewayIP, cfg.GuestNetmask)
}

// Launch starts one VMM and configures it.
//
// f.mu guards the process map and NOTHING else. Waiting for a socket and
// PUTting seven endpoints is seconds of I/O, and State, PID and Stop take the
// same mutex — holding it across a boot froze Inspect and List for every
// other session on the host behind whichever VM was slowest to come up.
func (f *FirecrackerEngine) Launch(ctx context.Context, cfg VMMConfig) error {
	// initErr, vmmPath and stateDir are set once in the constructor and never
	// written again, so they need no lock.
	if f.initErr != nil {
		return f.initErr
	}
	if !f.kvm() {
		return errors.New("/dev/kvm not found: hardware virtualization is required for Firecracker")
	}
	if cfg.KernelPath == "" {
		return errors.New("kernel image path is required for Firecracker launch")
	}
	if cfg.RootfsPath == "" {
		return errors.New("rootfs image path is required for Firecracker launch")
	}

	// The jail, built before anything is started: a chrooted Firecracker can
	// only see what is already inside it.
	spec, err := f.jailSpecFor(cfg)
	if err != nil {
		return err
	}
	if err := f.prepareJail(spec, cfg); err != nil {
		_ = f.removeJail(cfg.ID)
		f.uids.release(cfg.ID)
		return fmt.Errorf("prepare the jail for %s: %w", cfg.ID, err)
	}

	sockPath := f.socketPath(cfg.ID)
	_ = os.Remove(sockPath)

	// The jailer does the whole of ADR-0003 §4.5's host hardening and then
	// execve's into Firecracker, so the process this engine tracks IS the
	// VMM: Stop's reap and its identity check both still work on it. It also
	// enters the session's network namespace (`--netns`), which is where the
	// slot's TAP device is — the device does not exist in the host's
	// namespace at all.
	proc, err := f.starter.Start(f.jailerPath, jailerArgs(spec))
	if err != nil {
		_ = f.removeJail(cfg.ID)
		f.uids.release(cfg.ID)
		return fmt.Errorf("start jailed firecracker %s: %w", cfg.ID, err)
	}

	if pid := proc.Pid(); pid > 0 {
		pidDir := filepath.Join(f.stateDir, "instances", cfg.ID)
		_ = os.MkdirAll(pidDir, microvmDirMode)
		_ = os.WriteFile(f.pidFilePath(cfg.ID), []byte(strconv.Itoa(pid)), microvmFileMode)
	}

	var initSuccess bool
	defer func() {
		if initSuccess {
			return
		}
		// A launch that got part-way leaves a live VMM, a jail full of hard
		// links and a uid nobody else may use. All three go, in that order:
		// the process first, because removing the jail under a running
		// Firecracker is how a VMM ends up writing into a directory that has
		// been unlinked.
		_ = proc.Kill()
		_ = proc.Wait()
		_ = f.removeJail(cfg.ID)
		f.uids.release(cfg.ID)
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

	// 2. Boot source.
	//
	// Every path from here on is named from INSIDE the chroot, because that
	// is the only filesystem the VMM can still see. prepareJail put a hard
	// link to each of them there; the host names are in cfg and are what the
	// links point at.
	if err := fcClient.putJSON(ctx, "/boot-source", map[string]any{
		"kernel_image_path": jailKernelPath,
		"boot_args":         bootArgs(cfg),
	}); err != nil {
		return fmt.Errorf("set boot source: %w", err)
	}

	// 3. Rootfs drive
	if err := fcClient.putJSON(ctx, "/drives/rootfs", map[string]any{
		"drive_id":       "rootfs",
		"path_on_host":   jailRootfsPath,
		"is_root_device": true,
		"is_read_only":   true,
	}); err != nil {
		return fmt.Errorf("set rootfs drive: %w", err)
	}

	// 4. Workspace drive
	if cfg.WorkspaceDiskPath != "" {
		if err := fcClient.putJSON(ctx, "/drives/workspace", map[string]any{
			"drive_id":       "workspace",
			"path_on_host":   jailWorkspacePath,
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
			"path_on_host":   jailHomePath,
			"is_root_device": false,
			"is_read_only":   false,
		}); err != nil {
			return fmt.Errorf("set home drive: %w", err)
		}
	}

	// 5. Network interface: the slot's TAP device, inside the slot's network
	// namespace, with the slot's MAC. The jailer put the VMM in that
	// namespace (`--netns`), which is the only reason it can open a device
	// that does not exist in the host's.
	if cfg.TapDevice != "" {
		if err := fcClient.putJSON(ctx, "/network-interfaces/eth0", map[string]any{
			"iface_id":      "eth0",
			"host_dev_name": cfg.TapDevice,
			"guest_mac":     cfg.GuestMAC,
		}); err != nil {
			return fmt.Errorf("set network interface: %w", err)
		}
	}

	// 5b. virtio-vsock: the single host-to-guest control channel (ADR-0003
	// §2.7 item 2). It carries the boot configuration, the bootstrap token,
	// the sessiond-to-runnerd stream that used to ride a WebSocket through
	// the TAP device, the lifecycle handshake, and the credential fetch — all
	// over one conn, because internal/relay already multiplexes exactly those
	// things over one conn.
	//
	// The guest's connections to host port N arrive on "<uds_path>_N", and
	// the driver is already listening on _1024 by the time this runs. The
	// host never sends CONNECT, so there is no host-initiated direction to
	// configure.
	//
	// There is deliberately still no MMDS configuration. MMDS answers
	// unauthenticated HTTP at 169.254.169.254 — the exact address ADR-0003
	// §4.3 requires the host to drop on every TAP — so a configuration
	// channel that needed it would be at war with the rule that exists to
	// block it. vsock is invisible to guest routing and to the TAP firewall
	// alike, which is why the metadata denial can stay unconditional.
	if cfg.VsockUDSPath != "" {
		if err := fcClient.putJSON(ctx, "/vsock", map[string]any{
			"guest_cid": guestCID,
			"uds_path":  cfg.VsockUDSPath,
		}); err != nil {
			// Not a discarded error, unlike the two MMDS PUTs this replaces:
			// a guest with no control channel is a guest that can never be
			// configured, and a silently config-less VM reporting itself
			// healthy is the failure that made this whole design necessary.
			return fmt.Errorf("configure the guest control channel (vsock): %w", err)
		}
	}

	// 6. Start the microVM instance
	if err := fcClient.putJSON(ctx, "/actions", map[string]any{
		"action_type": "InstanceStart",
	}); err != nil {
		return fmt.Errorf("start instance %s: %w", cfg.ID, err)
	}

	f.mu.Lock()
	f.procs[cfg.ID] = proc
	f.mu.Unlock()

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

func (f *FirecrackerEngine) Stop(ctx context.Context, id string) error {
	f.mu.Lock()
	proc, tracked := f.procs[id]
	delete(f.procs, id)
	f.mu.Unlock()

	var pid int
	if tracked {
		pid = proc.Pid()
	} else if data, err := os.ReadFile(f.pidFilePath(id)); err == nil {
		pid, _ = strconv.Atoi(strings.TrimSpace(string(data)))
	}

	// waited is the channel this process's own Wait reports on. A VMM this
	// engine started is a CHILD: signalling it is not enough, because an
	// un-Waited child stays in the process table as a zombie, and a runner
	// that parks and resumes sessions all day accumulates one per stopped VM
	// until it runs out of process slots. Only a tracked process can be
	// Waited — a pid recovered from the pid file after a runnerd restart
	// belongs to no child of this process, and for that one the kernel has
	// already reparented it to init, which reaps it.
	var waited chan error
	if tracked && pid > 0 {
		waited = make(chan error, 1)
		go func() { waited <- proc.Wait() }()
	}

	var stopErr error
	switch {
	case pid > 0 && isFirecrackerPID(pid, id):
		// The signal goes to the VMM's process GROUP when it leads one,
		// which is what the jailed launch arranges (execStarter sets
		// Setpgid). The jailer execve's into Firecracker so the leader IS
		// the VMM, and the group is what catches anything a jailed VMM left
		// beside itself. killProcessTree reads the group back from the
		// kernel rather than assuming it, so a pid recovered across a
		// runnerd restart — whose group this process knows nothing about —
		// is only ever signalled on its own.
		if err := killProcessTree(pid, syscall.SIGTERM); err != nil {
			stopErr = fmt.Errorf("sigterm pid %d: %w", pid, err)
		} else if !awaitExit(ctx, waited, pid, firecrackerTermTimeout) {
			_ = killProcessTree(pid, syscall.SIGKILL)
			if !awaitExit(ctx, waited, pid, firecrackerKillTimeout) && stopErr == nil {
				stopErr = notExitedErr(ctx, pid)
			}
		}
	case waited != nil:
		// Nothing was signalled. Either the process is already gone, or the
		// pid no longer identifies as this VM's VMM and must not be signalled
		// in its name — that identity check is the whole reason a recycled
		// pid is never killed here.
		//
		// The child still has to be reaped either way, but reaping is not
		// allowed to block teardown. A bare receive here was unbounded: a
		// live process that failed the identity check will never exit on its
		// own, so Stop — and with it Destroy, Suspend and every caller
		// holding a session's teardown — waited forever.
		if !awaitExit(ctx, waited, pid, firecrackerKillTimeout) {
			stopErr = fmt.Errorf("firecracker %s: pid %d is still running but does not identify as this VM's VMM, so it was left alone", id, pid)
		}
	}

	// The jail goes with the VM, and only AFTER it: removing a chroot out
	// from under a live Firecracker is how a VMM ends up writing into
	// unlinked files. What is removed is the directory and the hard links in
	// it, never the images they point at — a session's workspace lives under
	// the state directory's workspaces/ and survives this untouched.
	//
	// It is also what removes the API socket and the guest control socket,
	// which used to live in their own directory outside the jail and now
	// cannot: a chrooted Firecracker can neither create nor connect to
	// anything outside its root.
	if err := f.removeJail(id); err != nil && stopErr == nil {
		stopErr = err
	}
	// The uid comes back only once the jail that used it is gone. Releasing
	// it earlier would let the next VM take a number that still owns files
	// on this host.
	if stopErr == nil {
		f.uids.release(id)
	}
	return stopErr
}

func notExitedErr(ctx context.Context, pid int) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("firecracker pid %d: waiting for it to exit was cut short: %w", pid, err)
	}
	return fmt.Errorf("firecracker pid %d did not exit after SIGKILL", pid)
}

// awaitExit waits for a VMM to be gone and reports whether it is, giving up
// at timeout or when ctx is done — never later than one of the two. For a
// child of this process that means Wait returning (which also reaps it); for
// a pid inherited across a restart, polling signal 0 is all there is, since
// the kernel reparented it to init and init does the reaping.
//
// A false answer leaves the Wait goroutine in place. That is deliberate: it
// is one goroutine that ends when the process finally does, and the
// alternative — abandoning the child unreaped — is the zombie this exists to
// prevent.
func awaitExit(ctx context.Context, waited chan error, pid int, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	if waited != nil {
		select {
		case <-waited:
			return true
		case <-timer.C:
			return false
		case <-ctx.Done():
			return false
		}
	}

	poll := time.NewTicker(50 * time.Millisecond)
	defer poll.Stop()
	for {
		if err := syscall.Kill(pid, 0); err != nil {
			return true
		}
		select {
		case <-poll.C:
		case <-timer.C:
			return false
		case <-ctx.Done():
			return false
		}
	}
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
	proc, tracked := f.procs[id]
	f.mu.Unlock()

	var pid int
	if tracked {
		pid = proc.Pid()
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
	if !isFirecrackerPID(pid, id) {
		return VMMStateGone, nil
	}

	return instanceState(ctx, newFirecrackerClient(f.socketPath(id)))
}

// instanceState reads a running VMM's execution state.
//
// The endpoint is GET / (InstanceInfo). There is no GET /vm in Firecracker's
// API — /vm takes a PATCH and nothing else — so the previous code asked for a
// path that 404s, fell through its own error handling, and returned Running
// unconditionally. A warm-paused VM therefore reconciled as running, which is
// precisely the state Inspect exists to tell apart.
//
// Anything this cannot read is an error rather than a guess. State's caller
// leaves the driver's own record alone when State errors, which is the right
// outcome for a VMM that answered something unexpected: the driver keeps what
// it knows instead of overwriting it with a default.
func instanceState(ctx context.Context, c *firecrackerClient) (VMMState, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost/", nil)
	if err != nil {
		return "", err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("firecracker GET /: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("firecracker GET / failed (%d): %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var info struct {
		State string `json:"state"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return "", fmt.Errorf("firecracker GET /: %w", err)
	}
	switch {
	case strings.EqualFold(info.State, "Running"):
		return VMMStateRunning, nil
	case strings.EqualFold(info.State, "Paused"):
		return VMMStatePaused, nil
	default:
		// "Not started" lands here: the VMM process is up but InstanceStart
		// was never issued or never took. Launch does not return until it
		// has, so this is a VM in a state the driver did not put it in, and
		// naming it beats mapping it onto one of the driver's own.
		return "", fmt.Errorf("firecracker instance state %q is not one this driver put it in", info.State)
	}
}

func (f *FirecrackerEngine) PID(id string) int {
	f.mu.Lock()
	proc, tracked := f.procs[id]
	f.mu.Unlock()
	if tracked && proc.Pid() > 0 {
		return proc.Pid()
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

// isFirecrackerPID reports whether pid is THIS VM's Firecracker, so that a
// recycled pid is never signalled in a VM's name.
//
// marker is what tells one VM's VMM from another's on the same host. Under
// the jailer it is the instance id, and it is on the command line twice over:
// the jailer passes its own `--id` through to Firecracker, and the binary it
// execs lives at <chroot base>/firecracker/<id>/root/firecracker, so the
// argv carries the id whichever way it is read. It used to be the API socket
// path, which no longer distinguishes anything — every jailed VMM serves
// /run/firecracker.socket, because every one of them has a root of its own.
func isFirecrackerPID(pid int, marker string) bool {
	if pid <= 0 {
		return false
	}
	if err := syscall.Kill(pid, 0); err != nil {
		return false
	}

	cmdlinePath := fmt.Sprintf("/proc/%d/cmdline", pid)
	if data, err := os.ReadFile(cmdlinePath); err == nil {
		// The cmdline is NUL-separated, so a substring search over it can
		// match across argument boundaries. That is harmless here: both
		// needles are whole arguments or parts of one path, and the check is
		// "is this plausibly the VMM we started" rather than a parser.
		cmdline := string(data)
		return strings.Contains(cmdline, jailExecName) && strings.Contains(cmdline, marker)
	}

	cmd := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "command=")
	if out, err := cmd.Output(); err == nil {
		s := string(out)
		return strings.Contains(s, jailExecName) && (marker == "" || strings.Contains(s, marker))
	}

	return false
}

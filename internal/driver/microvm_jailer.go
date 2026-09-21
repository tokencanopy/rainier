// internal/driver/microvm_jailer.go
//
// Firecracker's jailer, which is how this driver starts a VMM (ADR-0003
// §4.5): a dedicated uid and gid per VM, its own cgroup, its own network
// namespace, a chroot into a per-session directory, and a seccomp filter on
// the Firecracker process.
//
// The jailer is not an optimisation and it is not optional. The bakeoff names
// host-compromise blast radius as this substrate's primary risk, and a
// Firecracker that this runner exec'd directly would run as runnerd's own
// user, with runnerd's file descriptors, in runnerd's mount namespace, next
// to every other tenant's disk image on the host. There is therefore no
// "unjailed" launch path and no flag that produces one: an engine without a
// jailer binary refuses to construct.
//
// It is also, usefully, the privileged helper this design would otherwise
// have to write. The jailer is the component that does the chroot, the
// cgroup, the device nodes and the uid drop; runnerd hands it a directory and
// a number and never performs any of those itself. What runnerd still does in
// its own right — a directory, some hard links, an ownership change — is in
// this file and nowhere else, so the list of things a future privileged
// helper would take over is exactly the list of functions here.
package driver

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
)

const (
	// jailBaseName is the chroot base, under the state directory. It is one
	// letter because every byte of it is on the vsock socket's path budget:
	// the guest control socket lives INSIDE the jail (a chrooted Firecracker
	// cannot reach a socket outside it), and sun_path is 108 bytes. See
	// (*Microvm).vsockPaths.
	jailBaseName = "j"

	// jailExecName is the name the jailer knows the VMM binary by, and the
	// directory component it composes into the chroot path:
	// <base>/<exec name>/<id>/root. It is Firecracker's own convention, not
	// a choice.
	jailExecName = "firecracker"

	// The in-jail paths. Everything the VMM is told about is named relative
	// to its chroot, because after the jailer has done its work that is the
	// only filesystem it can see.
	jailAPISocketPath = "/run/firecracker.socket"
	jailKernelPath    = "/vmlinux"
	jailRootfsPath    = "/rootfs.ext4"
	jailWorkspacePath = "/workspace.ext4"
	jailHomePath      = "/home.ext4"

	// jailDirMode and jailFileMode are owner-and-group, never other. The
	// owner is the VM's own uid; the group is the RUNNER's, which is what
	// lets runnerd still create the next boot's socket and tear the jail
	// down afterwards without being the VM's user. Another VM is neither,
	// because its uid and its primary gid are both its own.
	jailDirMode  os.FileMode = 0o770
	jailFileMode os.FileMode = 0o660

	// defaultJailUIDFirst is the bottom of the per-VM uid and gid range. It
	// is well above the system range and above the 65534 (nobody) boundary,
	// so a jailed VM can never land on a real account on the host.
	defaultJailUIDFirst = 200000
	// defaultJailUIDCount bounds it. It is much larger than any host's slot
	// count so that a uid is never recycled while the previous VM's files
	// might still exist, without being so large that a misconfiguration
	// silently walks into another subsystem's range.
	defaultJailUIDCount = 4096

	// defaultJailCgroupParent is where per-VM cgroups are created:
	// /sys/fs/cgroup/<parent>/<vm id> on a cgroup v2 host. Metering reads
	// cpu.stat and memory.current from there (ADR-0003 §4.6).
	defaultJailCgroupParent = "rainier"
)

// jailBaseDir is the jailer's --chroot-base-dir for a state directory.
func jailBaseDir(stateDir string) string { return filepath.Join(stateDir, jailBaseName) }

// jailRootDir is the directory that BECOMES the VM's root filesystem view.
//
// The layout is the jailer's: <base>/firecracker/<id>/root. runnerd creates
// it, fills it, and hands it over; the jailer chroots into it.
func jailRootDir(stateDir, id string) string {
	return filepath.Join(jailBaseDir(stateDir), jailExecName, id, "root")
}

// jailInstanceDir is the directory jailRootDir sits in, and what a teardown
// removes: removing only `root` would leave an empty `<id>` behind for every
// session this host has ever run.
func jailInstanceDir(stateDir, id string) string {
	return filepath.Dir(jailRootDir(stateDir, id))
}

// jailCgroupPath is where the jailer puts this VM's cgroup, and therefore
// where its usage is read from.
func jailCgroupPath(cgroupRoot, parent, id string) string {
	return filepath.Join(cgroupRoot, parent, id)
}

// ---------------------------------------------------------------------------
// uid and gid allocation
// ---------------------------------------------------------------------------

// uidAllocator hands out one (uid, gid) pair per live VM from a configured
// range.
//
// Per-VM rather than per-host is the whole point: two sessions sharing a uid
// share every file-permission decision the kernel makes about them, so one
// tenant's Firecracker could open the other's disk image, ptrace its VMM, and
// signal its process. The pair is the same number for both, so a VM's files
// are readable by its own primary group and nobody else's.
type uidAllocator struct {
	mu    sync.Mutex
	first int
	count int
	held  map[string]int // instance id -> uid
	used  map[int]bool
}

func newUIDAllocator(first, count int) (*uidAllocator, error) {
	if first <= 0 {
		first = defaultJailUIDFirst
	}
	if count <= 0 {
		count = defaultJailUIDCount
	}
	if first < 1000 {
		return nil, fmt.Errorf("microvm: the per-VM uid range starts at %d, inside the system range; a jailed VM must never run as a real account on this host", first)
	}
	return &uidAllocator{first: first, count: count, held: map[string]int{}, used: map[int]bool{}}, nil
}

// claim returns id's uid, allocating one if it has none. It is idempotent so
// that a relaunch of the same instance keeps the ownership its jail already
// carries.
func (a *uidAllocator) claim(id string) (int, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if uid, ok := a.held[id]; ok {
		return uid, nil
	}
	for i := range a.count {
		uid := a.first + i
		if a.used[uid] {
			continue
		}
		a.used[uid] = true
		a.held[id] = uid
		return uid, nil
	}
	return 0, fmt.Errorf("microvm: no free per-VM uid in [%d, %d); every jailed VM needs one of its own", a.first, a.first+a.count)
}

// release returns id's uid to the range.
func (a *uidAllocator) release(id string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if uid, ok := a.held[id]; ok {
		delete(a.used, uid)
		delete(a.held, id)
	}
}

// ---------------------------------------------------------------------------
// The jail
// ---------------------------------------------------------------------------

// jailSpec is everything one jailed launch needs, resolved.
type jailSpec struct {
	ID       string // the jailer's --id: the chroot name AND the cgroup name
	ExecFile string // the Firecracker binary the jailer execs
	Base     string // --chroot-base-dir
	Root     string // the chroot itself, on the host: <Base>/firecracker/<ID>/root

	// UID and GID are what the VMM runs as, and they are the SAME number
	// and per VM. Sharing either across VMs would share every
	// file-permission decision the kernel makes about them.
	UID int
	GID int
	// RunnerGID is the group the jail's contents are given to. It is
	// runnerd's own, and it is deliberately NOT the VM's: it is what lets
	// runnerd create the next boot's control socket here and remove the jail
	// afterwards, while the VM next door — neither owner nor in this
	// group — has no access at all.
	RunnerGID int

	Netns   string // path to the slot's network namespace, or ""
	Cgroup  string // --parent-cgroup
	Seccomp bool
}

// jailerArgs is the jailer command line.
//
// Written as one function with no branches beyond the two optional flags, so
// that the argv a host is asked to run is a thing a test can read back
// whole — which is what microvm_jailer_test.go does. Everything after "--" is
// Firecracker's own, and the only thing this driver passes it is the API
// socket, named relative to the chroot because that is where it will be.
func jailerArgs(spec jailSpec) []string {
	args := []string{
		"--id", spec.ID,
		"--exec-file", spec.ExecFile,
		"--uid", strconv.Itoa(spec.UID),
		"--gid", strconv.Itoa(spec.GID),
		"--chroot-base-dir", spec.Base,
		"--cgroup-version", "2",
		"--parent-cgroup", spec.Cgroup,
	}
	if spec.Netns != "" {
		args = append(args, "--netns", spec.Netns)
	}
	// Seccomp is ON by default and stays on unless it is explicitly turned
	// off, which is why the flag here is the negative one: there is no
	// configuration mistake that can leave the filter off by omission.
	if !spec.Seccomp {
		args = append(args, "--no-seccomp")
	}
	// Deliberately NOT --daemonize and NOT --new-pid-ns. Both put the VMM
	// somewhere this engine can no longer Wait on it: the first detaches it,
	// the second makes it a grandchild in a namespace of its own. The jailer
	// as used here execve's into Firecracker, so the process this engine
	// tracks IS the VMM, which is what Stop's reap and its identity check
	// both depend on.
	return append(args, "--", "--api-sock", jailAPISocketPath)
}

// prepareJail builds the chroot the jailer will hand to Firecracker.
//
// Everything the VM needs must be INSIDE it, because after the chroot there
// is no outside. Images are hard-linked rather than copied so that a 10 GiB
// workspace costs an inode; they fall back to a copy only across filesystems,
// which is a configuration an operator chose and which the error paths name.
//
// The ownership rule is the one in jailDirMode: owner is the VM, group is the
// runner. The VM needs to read and write its own files; the runner needs to
// create the next boot's control socket in this directory and remove the
// whole thing afterwards, and it is not the VM's user. Nothing is readable by
// "other", so the VM next door — a different uid, a different primary gid —
// has no access at all.
func (f *FirecrackerEngine) prepareJail(spec jailSpec, cfg VMMConfig) error {
	// The control socket may already be in here: the driver creates its
	// listener BEFORE the VM starts (see openGuestChannel), because a guest
	// dials as soon as it boots. So this makes the directory if it is not
	// there and never wipes it.
	for _, dir := range []string{spec.Root, filepath.Join(spec.Root, "run")} {
		if err := os.MkdirAll(dir, jailDirMode); err != nil {
			return fmt.Errorf("create jail directory %s: %w", dir, err)
		}
		if err := chownJailPath(dir, spec.UID, spec.RunnerGID, jailDirMode); err != nil {
			return err
		}
	}

	// Read-only images shared with every other session on the host: the
	// guest kernel and the base rootfs. They are hard-linked, which shares
	// the INODE — so their ownership cannot be changed for one VM without
	// changing it for every other VM holding the same image. They therefore
	// have to be readable as they are.
	for _, img := range []struct{ host, jail string }{
		{cfg.KernelPath, jailKernelPath},
		{cfg.RootfsPath, jailRootfsPath},
	} {
		if img.host == "" {
			continue
		}
		if err := checkSharedImageReadable(img.host); err != nil {
			return err
		}
		if err := linkOrCopy(img.host, filepath.Join(spec.Root, img.jail)); err != nil {
			return err
		}
	}

	// Per-session writable images: exactly one VM owns each, so each is
	// chowned to that VM.
	for _, img := range []struct{ host, jail string }{
		{cfg.WorkspaceDiskPath, jailWorkspacePath},
		{cfg.HomeDiskPath, jailHomePath},
	} {
		if img.host == "" {
			continue
		}
		if err := linkOrCopy(img.host, filepath.Join(spec.Root, img.jail)); err != nil {
			return err
		}
		// The hard link and its original are one inode, so this changes the
		// image's ownership on the host too. That is the intent for a
		// workspace, which belongs to one session.
		//
		// For the AGENT HOME it is a known rough edge: ADR-0003 §2.3 lets
		// two concurrent sessions of the same creator mount one home device
		// on one host, and under per-VM uids the second boot's chown wins.
		// Both VMs keep working — permissions are checked when Firecracker
		// opens the drive, and it has already opened it — but the second
		// session's uid is now the owner of the first's home image. Which of
		// the two homes materialisations ADR-0003 §9 picks decides the real
		// answer; until then this is written down rather than papered over.
		if err := chownJailPath(img.host, spec.UID, spec.RunnerGID, jailFileMode); err != nil {
			return err
		}
	}

	// The guest control socket. A chrooted Firecracker connects to it by its
	// in-jail path, as the VM's uid, so the VM's uid has to be able to.
	if cfg.VsockUDSPath != "" {
		sock := filepath.Join(spec.Root, cfg.VsockUDSPath+"_"+strconv.Itoa(guestControlPort))
		if _, err := os.Stat(sock); err == nil {
			if err := chownJailPath(sock, spec.UID, spec.RunnerGID, jailFileMode); err != nil {
				return err
			}
		}
	}
	return nil
}

// removeJail takes the whole chroot away, hard links and all.
//
// Removing a hard link is not removing the file it points at: a session's
// workspace image lives under the state directory's workspaces/ and survives
// this untouched, which is the entire reason the images are linked in rather
// than moved.
func (f *FirecrackerEngine) removeJail(id string) error {
	dir := jailInstanceDir(f.stateDir, id)
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("remove the jail directory %s: %w", dir, err)
	}
	return nil
}

// checkSharedImageReadable refuses an image the jailed VM could not open.
//
// A jailed Firecracker is neither the owner of the host's kernel image nor in
// the runner's group, so "other" read is the only bit that can let it in —
// and the image cannot simply be chowned, because a hard link shares its
// inode with every other jail holding the same image. Refusing here, with the
// path and the fix in the message, beats a VM that boots to a kernel it
// cannot read.
func checkSharedImageReadable(path string) error {
	fi, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("microvm: jail image %s: %w", path, err)
	}
	if fi.Mode().Perm()&0o004 == 0 {
		return fmt.Errorf("microvm: %s is mode %#o and a jailed VM runs as neither its owner nor a member of its group, so it could not read it. The guest kernel and the base rootfs are shared read-only images and are hard-linked into every jail, so their ownership cannot be changed per VM; make them world-readable (chmod o+r %s)", path, fi.Mode().Perm(), path)
	}
	return nil
}

// linkOrCopy hard-links src to dst, falling back to a copy across
// filesystems.
//
// The fallback is real but it is not free: a base rootfs on a different
// filesystem from --microvm-state-dir is copied on every boot. That is a
// configuration an operator chose and can undo, which is why it is a slow
// path rather than an error.
func linkOrCopy(src, dst string) error {
	if _, err := os.Stat(dst); err == nil {
		// A relaunch into a jail that was not fully torn down. The link is
		// removed and remade rather than trusted: the previous boot's image
		// may not be this boot's.
		if err := os.Remove(dst); err != nil {
			return fmt.Errorf("replace stale jail image %s: %w", dst, err)
		}
	}
	err := os.Link(src, dst)
	if err == nil {
		return nil
	}
	if !errors.Is(err, syscall.EXDEV) {
		return fmt.Errorf("link %s into the jail at %s: %w", src, dst, err)
	}
	return copyFileInto(src, dst)
}

func copyFileInto(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open %s to copy into the jail: %w", src, err)
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, microvmFileMode)
	if err != nil {
		return fmt.Errorf("create %s in the jail: %w", dst, err)
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		_ = os.Remove(dst)
		return fmt.Errorf("copy %s into the jail: %w", src, err)
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(dst)
		return fmt.Errorf("copy %s into the jail: %w", src, err)
	}
	return nil
}

// chownJailPath is the one privileged filesystem operation this driver
// performs in its own right: giving a file to the VM's uid.
//
// It needs CAP_CHOWN. It is a named function, called from one place, for the
// reason the file header gives: if a deployment ever decides runnerd may not
// hold CAP_CHOWN, this is the call a privileged helper takes over, and there
// is exactly one of it.
//
// Changing ownership to our own uid is unprivileged, which is what lets the
// tests exercise the whole jail layout on an ordinary machine.
func chownJailPath(path string, uid, gid int, mode os.FileMode) error {
	if err := os.Chown(path, uid, gid); err != nil {
		return fmt.Errorf("microvm: give %s to the jailed VM's uid %d (this needs CAP_CHOWN): %w", path, uid, err)
	}
	if err := os.Chmod(path, mode); err != nil {
		return fmt.Errorf("microvm: set the mode of %s: %w", path, err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// The process seam
// ---------------------------------------------------------------------------

// vmmProcess is a started VMM, as much of it as this engine ever touches.
type vmmProcess interface {
	// Pid is the process id, or 0 if it never started.
	Pid() int
	// Wait reaps the child. A VMM this engine started is a child, and an
	// un-Waited child is a zombie for as long as the runner lives.
	Wait() error
	// Kill ends the process and anything it left in its process group.
	Kill() error
}

// processStarter starts a VMM. It is the seam the jailer tests use to read
// back the exact argv a host would be asked to run, without a host.
type processStarter interface {
	Start(name string, args []string) (vmmProcess, error)
}

// execStarter is the production starter.
type execStarter struct{}

func (execStarter) Start(name string, args []string) (vmmProcess, error) {
	cmd := exec.Command(name, args...)
	// Its own process group. The jailer execve's into Firecracker so the
	// tracked pid is the VMM itself, but a jailed VMM may still leave
	// something of its own behind, and a group is what lets Stop end the
	// tree rather than the one process it happens to know about. It also
	// detaches the VMM from runnerd's terminal, so a Ctrl-C in a dev session
	// does not signal every tenant's guest.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &execProcess{cmd: cmd}, nil
}

type execProcess struct{ cmd *exec.Cmd }

func (p *execProcess) Pid() int {
	if p.cmd.Process == nil {
		return 0
	}
	return p.cmd.Process.Pid
}

func (p *execProcess) Wait() error { return p.cmd.Wait() }

func (p *execProcess) Kill() error {
	pid := p.Pid()
	if pid <= 0 {
		return nil
	}
	return killProcessTree(pid, syscall.SIGKILL)
}

// killProcessTree signals a VMM and whatever it left in its process group.
//
// The group is only signalled when the process leads it. Reading that back
// from the kernel rather than assuming it is what makes this safe for a pid
// recovered from a pid file after a runnerd restart: such a process was
// started by a runner that is gone, this one has no idea what group it is in,
// and signalling a group it does not lead could reach anything on the host —
// including, if the pgid happened to be runnerd's own, every session.
func killProcessTree(pid int, sig syscall.Signal) error {
	if pid <= 0 {
		return nil
	}
	if pgid, err := syscall.Getpgid(pid); err == nil && pgid == pid {
		if err := syscall.Kill(-pgid, sig); err != nil && !errors.Is(err, syscall.ESRCH) {
			return err
		}
		return nil
	}
	if err := syscall.Kill(pid, sig); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}

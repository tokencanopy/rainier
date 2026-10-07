package driver

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// guestHostIdentity is non-secret host metadata, never a guest credential.
// StartTime alone is insufficient across host reboots or namespace replacement.
type guestHostIdentity struct {
	BootID          string `json:"boot_id"`
	StartTime       uint64 `json:"start_time"`
	NamespaceDevice uint64 `json:"namespace_device"`
	NamespaceInode  uint64 `json:"namespace_inode"`
}

func readGuestHostIdentity(procRoot, bootPath, namespace string, pid int, id string, uid, gid int) (guestHostIdentity, error) {
	var zero guestHostIdentity
	fail := errors.New("microvm: surviving guest identity unavailable")
	if pid <= 0 || checkPathSegment("instance", id) != nil {
		return zero, fail
	}
	dir := filepath.Join(procRoot, strconv.Itoa(pid))
	stat, err := os.ReadFile(filepath.Join(dir, "stat"))
	if err != nil {
		return zero, fail
	}
	start, err := guestProcessStart(stat, pid)
	if err != nil {
		return zero, fail
	}
	args, err := os.ReadFile(filepath.Join(dir, "cmdline"))
	if err != nil || !guestProcessArguments(args, id) {
		return zero, fail
	}
	status, err := os.ReadFile(filepath.Join(dir, "status"))
	if err != nil {
		return zero, fail
	}
	owners := map[string]int{"Uid:": uid, "Gid:": gid}
	for _, line := range strings.Split(string(status), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		want, ok := owners[fields[0]]
		if !ok {
			continue
		}
		if want < 0 || len(fields) != 5 {
			return zero, fail
		}
		for _, value := range fields[1:] {
			if value != strconv.Itoa(want) {
				return zero, fail
			}
		}
		owners[fields[0]] = -1
	}
	if owners["Uid:"] != -1 || owners["Gid:"] != -1 {
		return zero, fail
	}
	boot, err := os.ReadFile(bootPath)
	if err != nil {
		return zero, fail
	}
	bootID := strings.TrimSpace(string(boot))
	if len(bootID) != 36 {
		return zero, fail
	}
	for i, c := range bootID {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return zero, fail
			}
			continue
		}
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return zero, fail
		}
	}
	var ns unix.Stat_t
	if unix.Lstat(namespace, &ns) != nil || ns.Mode&unix.S_IFMT != unix.S_IFREG || ns.Ino == 0 {
		return zero, fail
	}
	// Reject PID reuse during the multi-file inspection, too.
	stat, err = os.ReadFile(filepath.Join(dir, "stat"))
	if err != nil {
		return zero, fail
	}
	after, err := guestProcessStart(stat, pid)
	if err != nil || after != start {
		return zero, fail
	}
	return guestHostIdentity{BootID: bootID, StartTime: start, NamespaceDevice: uint64(ns.Dev), NamespaceInode: uint64(ns.Ino)}, nil
}

func guestRecoveryBoot(path string) (int, error) {
	boot, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(path, "/v"), ".sock"))
	if err != nil || boot < 1 || path != vsockGuestPath(boot) {
		return 0, errors.New("microvm: invalid recovered guest socket path")
	}
	return boot, nil
}

// guestIdentityVerifier is deliberately optional: reconnect must fail closed
// on an engine that cannot prove local process ownership.
type guestIdentityVerifier interface {
	guestIdentity(VMMConfig, int) (guestHostIdentity, error)
}

func (f *FirecrackerEngine) guestIdentity(cfg VMMConfig, pid int) (guestHostIdentity, error) {
	var zero guestHostIdentity
	fail := errors.New("microvm: surviving guest ownership unavailable")
	if checkPathSegment("instance", cfg.ID) != nil || checkPathSegment("namespace", cfg.Netns) != nil || pid <= 0 || f.PID(cfg.ID) != pid {
		return zero, fail
	}
	uid, err := f.uids.forSlot(cfg.SlotIndex)
	if err != nil {
		return zero, fail
	}
	boot, err := guestRecoveryBoot(cfg.VsockUDSPath)
	if err != nil {
		return zero, fail
	}
	root := jailRootDir(f.stateDir, cfg.ID)
	// The named jail and both VMM-owned sockets must still belong to this VM.
	paths := []struct {
		path string
		kind uint32
	}{
		{root, unix.S_IFDIR}, {f.socketPath(cfg.ID), unix.S_IFSOCK}, {filepath.Join(root, vsockSocketName(boot)), unix.S_IFSOCK},
	}
	for _, p := range paths {
		var st unix.Stat_t
		if unix.Lstat(p.path, &st) != nil || uint32(st.Mode)&unix.S_IFMT != p.kind || st.Uid != uint32(uid) {
			return zero, fail
		}
		if p.kind == unix.S_IFDIR && (st.Gid != uint32(f.jail.RunnerGID) || os.FileMode(st.Mode&0777) != jailDirMode) {
			return zero, fail
		}
	}
	members, err := os.ReadFile(filepath.Join(cfg.CgroupPath, "cgroup.procs"))
	if err != nil {
		return zero, fail
	}
	found := false
	for _, p := range strings.Fields(string(members)) {
		if p == strconv.Itoa(pid) {
			found = true
		}
	}
	if !found {
		return zero, fail
	}
	return readGuestHostIdentity("/proc", "/proc/sys/kernel/random/boot_id", filepath.Join(f.netnsDir, cfg.Netns), pid, cfg.ID, uid, uid)
}

package driver

import (
	"context"
	"errors"
	"net"
	"os"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// bindRecoveredGuestListener requires the state-directory ownership lock and
// verified VM/jail ownership from its caller. It never unlinks an active socket
// and touches only the host's control listener, never the surviving VMM's UDS.
func bindRecoveredGuestListener(path string, uid int) (*net.UnixListener, error) {
	fail := errors.New("microvm: recovered guest listener is not exclusively owned")
	before, err := os.Lstat(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fail
	}
	if err == nil {
		var st unix.Stat_t
		if !before.Mode().IsRegular() && before.Mode()&os.ModeSocket == 0 {
			return nil, fail
		}
		if unix.Lstat(path, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFSOCK || st.Uid != uint32(uid) {
			return nil, fail
		}
		c, dialErr := net.DialTimeout("unix", path, 100*time.Millisecond)
		if dialErr == nil {
			c.Close()
			return nil, fail
		}
		if !errors.Is(dialErr, syscall.ECONNREFUSED) {
			return nil, fail
		}
		after, err := os.Lstat(path)
		if err != nil || !os.SameFile(before, after) {
			return nil, fail
		}
		if err := os.Remove(path); err != nil {
			return nil, fail
		}
	}
	return net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
}

// RecoverGuest binds only after runner registration has reauthorized the exact
// session placement. It never reconstructs boot configuration from disk.
func (m *Microvm) RecoverGuest(ctx context.Context, id, sessionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	fail := errors.New("microvm: guest recovery fenced")
	rec, ok := m.instances[id]
	if !ok || !m.opts.GuestReconnect || m.stateLock == nil || !rec.Reconnect || rec.SessionID != sessionID || rec.State != StateRunning || rec.Cold || rec.resuming || rec.slot == nil || rec.slot.Key != id || ctx.Err() != nil {
		return fail
	}
	if rec.channel != nil {
		return nil
	}
	if rec.Cfg.ID != id || rec.Cfg.SessionID != sessionID || rec.Cfg.CgroupPath != m.cgroupPathFor(id) {
		return fail
	}
	expected := rec.Cfg
	applySlot(&expected, rec.slot)
	cfg := rec.Cfg
	if cfg.SlotIndex != expected.SlotIndex || cfg.Netns != expected.Netns || cfg.TapDevice != expected.TapDevice || cfg.GuestIP != expected.GuestIP || cfg.GatewayIP != expected.GatewayIP || cfg.GuestNetmask != expected.GuestNetmask || cfg.GuestMAC != expected.GuestMAC {
		return fail
	}
	f, ok := m.engine.(*FirecrackerEngine)
	if !ok {
		return fail
	}
	identity, err := f.guestIdentity(cfg, rec.PID)
	if err != nil || identity != rec.Identity || identity.StartTime == 0 {
		return fail
	}
	boot, err := guestRecoveryBoot(cfg.VsockUDSPath)
	if err != nil {
		return fail
	}
	uds, path, err := m.vsockPaths(id, boot)
	if err != nil {
		return fail
	}
	uid, err := f.uids.forSlot(cfg.SlotIndex)
	if err != nil {
		return fail
	}
	listener, err := bindRecoveredGuestListener(path, uid)
	if err != nil {
		return err
	}
	if err := f.chownJailPath(path, uid, f.jail.RunnerGID, jailFileMode); err != nil {
		listener.Close()
		return fail
	}
	// Verify again after binding and before publication. A failed recovery closes
	// only its new listener, never Firecracker's device socket.
	current, err := f.guestIdentity(cfg, rec.PID)
	if err != nil || current != identity || ctx.Err() != nil {
		listener.Close()
		return fail
	}
	g := &guestChannel{listener: listener, listenPath: path, udsPath: uds, served: true, reconnect: true}
	rec.channel = g
	rec.boots = boot
	go m.acceptGuests(sessionID, g)
	return nil
}

package driver

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// lockGuestState excludes another current-version microVM runner before discovery or
// cleanup can touch surviving VMs. Keep the file in place: unlinking it would
// permit a second owner to lock a different inode at the same path.
func lockGuestState(dir string) (*os.File, error) {
	fd, err := unix.Open(filepath.Join(dir, "guest-reconnect.lock"), unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0600)
	if err != nil {
		return nil, fmt.Errorf("microvm: open recovery ownership: %w", err)
	}
	f := os.NewFile(uintptr(fd), "guest-reconnect.lock")
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		f.Close()
		return nil, fmt.Errorf("microvm: inspect recovery ownership: %w", err)
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG || st.Uid != uint32(os.Geteuid()) || st.Mode&0777 != 0600 || st.Nlink != 1 {
		f.Close()
		return nil, fmt.Errorf("microvm: unsafe recovery ownership file")
	}
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("microvm: recovery state already owned: %w", err)
	}
	return f, nil
}

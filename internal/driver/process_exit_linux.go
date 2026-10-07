package driver

import "golang.org/x/sys/unix"

// processExited requires whole-thread-group exit, not just a zombie leader.
// A process pidfd becomes readable only after its last thread exits. Recheck
// birth after opening it so a PID reused during inspection cannot grant cleanup.
func processExited(pid int, birth uint64) bool {
	fd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		return false
	}
	defer unix.Close(fd)
	start, err := processStartTime(pid)
	if err != nil || start != birth {
		return false
	}
	fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
	n, err := unix.Poll(fds, 0)
	return err == nil && n == 1 && fds[0].Revents&unix.POLLIN != 0
}

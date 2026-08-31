//go:build darwin

package attachio

import "golang.org/x/sys/unix"

func flushTTYInput(fd int) error {
	return unix.IoctlSetPointerInt(fd, unix.TIOCFLUSH, unix.TCIFLUSH)
}

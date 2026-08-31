//go:build linux

package attachio

import "golang.org/x/sys/unix"

func flushTTYInput(fd int) error {
	return unix.IoctlSetInt(fd, unix.TCFLSH, unix.TCIFLUSH)
}

package driver

import (
	"bytes"
	"encoding/binary"
	"errors"
	"golang.org/x/sys/unix"
)

// Preserve argv boundaries. Flattened ps output is not process identity.
func processArguments(pid int) ([]byte, error) {
	raw, err := unix.SysctlRaw("kern.procargs2", pid)
	if err != nil {
		return nil, err
	}
	return darwinProcessArguments(raw)
}
func darwinProcessArguments(raw []byte) ([]byte, error) {
	invalid := errors.New("microvm: unavailable process arguments")
	if len(raw) < 5 {
		return nil, invalid
	}
	argc := int(binary.NativeEndian.Uint32(raw[:4]))
	if argc < 1 || argc > 4096 {
		return nil, invalid
	}
	raw = raw[4:]
	end := bytes.IndexByte(raw, 0)
	if end < 0 {
		return nil, invalid
	}
	raw = raw[end+1:]
	for len(raw) > 0 && raw[0] == 0 {
		raw = raw[1:]
	}
	var out []byte
	for i := 0; i < argc; i++ {
		end = bytes.IndexByte(raw, 0)
		if end < 0 {
			return nil, invalid
		}
		out = append(out, raw[:end+1]...)
		raw = raw[end+1:]
	}
	return out, nil
}

func processStartTime(pid int) (uint64, error) {
	info, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return 0, err
	}
	t := info.Proc.P_starttime
	if t.Sec <= 0 || t.Usec < 0 {
		return 0, errors.New("microvm: unavailable process start time")
	}
	return uint64(t.Sec)*1000000 + uint64(t.Usec), nil
}

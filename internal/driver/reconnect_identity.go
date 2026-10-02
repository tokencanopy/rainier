package driver

import (
	"bytes"
	"errors"
	"path/filepath"
	"strconv"
	"strings"
)

// guestProcessStart extracts Linux stat field 22 without treating spaces or
// parentheses in comm as field separators. A missing identity fails closed.
func guestProcessStart(data []byte, pid int) (uint64, error) {
	s := string(data)
	open, end := strings.Index(s, " ("), strings.LastIndex(s, ") ")
	if open < 1 || end <= open || s[:open] != strconv.Itoa(pid) || pid <= 0 {
		return 0, errors.New("invalid guest process stat")
	}
	fields := strings.Fields(s[end+2:])
	if len(fields) < 20 {
		return 0, errors.New("short guest process stat")
	}
	start, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || start == 0 {
		return 0, errors.New("invalid guest process start time")
	}
	return start, nil
}

func guestProcessArguments(data []byte, id string) bool {
	args := bytes.Split(data, []byte{0})
	if len(args) < 3 || filepath.Base(string(args[0])) != jailExecName {
		return false
	}
	found := false
	for i := 1; i < len(args); i++ {
		if string(args[i]) != "--id" {
			continue
		}
		if found || i+1 >= len(args) || string(args[i+1]) != id {
			return false
		}
		found = true
		i++
	}
	return found
}

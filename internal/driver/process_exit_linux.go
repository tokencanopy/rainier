package driver

import (
	"fmt"
	"os"
	"strings"
)

// processExited binds a terminal kernel state to the recorded process birth.
// Zombies cannot execute or retain the VM's resources, but still answer kill(0).
func processExited(pid int, birth uint64) bool {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return false
	}
	start, err := guestProcessStart(data, pid)
	if err != nil || start != birth {
		return false
	}
	fields := strings.Fields(string(data)[strings.LastIndex(string(data), ") ")+2:])
	return fields[0] == "Z" || fields[0] == "X"
}

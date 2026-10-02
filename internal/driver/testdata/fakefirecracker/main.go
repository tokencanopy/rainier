// A native child for process-identity and reaping tests. It accepts the test's
// argv unchanged and exits on TERM; it does not emulate a VM or API socket.
package main

import (
	"os"
	"os/signal"
	"syscall"
)

func main() { c := make(chan os.Signal, 1); signal.Notify(c, syscall.SIGTERM); <-c }

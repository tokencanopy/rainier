package session

import (
	"errors"
	"io"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"sync"
	"syscall"

	"github.com/creack/pty"
)

type proc struct {
	cmd  *exec.Cmd
	ptmx *os.File
	mu   sync.Mutex
	done chan struct{}
	code int
}

type Proc interface {
	Write(p []byte) (int, error)
	Resize(cols, rows int) error
	Wait() int
	Stop()
}

// StartProc spawns argv on a new PTY. onOutput is called from a single
// goroutine with each chunk read from the PTY until child exit. The byte slice
// passed to onOutput is only valid for the duration of the call; consumers that
// retain the bytes must copy them.
func StartProc(argv []string, cols, rows int, onOutput func([]byte)) (Proc, error) {
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = append(os.Environ(), "TERM=xterm-256color")
	ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{Cols: uint16(cols), Rows: uint16(rows)})
	if err != nil { return nil, err }
	p := &proc{cmd: cmd, ptmx: ptmx, done: make(chan struct{})}
	go func() {
		buf := make([]byte, 32*1024)
		for {
			n, err := ptmx.Read(buf)
			if n > 0 { onOutput(buf[:n]) }
			if err != nil {
				// EOF (macOS) and EIO (Linux) are the PTY's normal end-of-stream
				// signals when the child exits; anything else is diagnostic-worthy.
				if !errors.Is(err, io.EOF) && !isEIO(err) {
					log.Printf("pty read error: %v", err)
				}
				break
			}
		}
		p.code = waitCode(cmd)
		ptmx.Close()
		close(p.done)
	}()
	return p, nil
}

func isEIO(err error) bool {
	var pe *fs.PathError
	return errors.As(err, &pe) && errors.Is(pe.Err, syscall.EIO)
}

func waitCode(cmd *exec.Cmd) int {
	if err := cmd.Wait(); err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) { return ee.ExitCode() }
		return -1
	}
	return 0
}

func (p *proc) Write(b []byte) (int, error) { return p.ptmx.Write(b) }
func (p *proc) Resize(cols, rows int) error {
	return pty.Setsize(p.ptmx, &pty.Winsize{Cols: uint16(cols), Rows: uint16(rows)})
}
func (p *proc) Wait() int { <-p.done; return p.code }
func (p *proc) Stop()     { p.cmd.Process.Signal(syscall.SIGTERM) }

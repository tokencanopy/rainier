// internal/e2e/census_test.go
package e2e

import (
	"context"
	"net/http"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/tokencanopy/rainier/internal/execio"
)

// This file is the census: what `rainier exec` costs a runner that is left
// running, measured rather than reasoned about.
//
// It exists because a leak of one descriptor per command is invisible to every
// other kind of test. The relay's client-read-error exit wrote its FrameClose,
// deleted its map entry and returned WITHOUT closing the client socket, so a
// websocket sat in CLOSE_WAIT for runnerd's whole life — once per human detach
// before exec, and once per COMMAND after it, which walks a CI loop to EMFILE.
// Unit tests catch the missing line with a fake; only a census catches "and it
// really is fd-for-fd clean end to end".

// openFDs is how many descriptors this process holds. Linux only — /proc is
// where the number lives.
func openFDs(t *testing.T) int {
	t.Helper()
	d, err := os.Open("/proc/self/fd")
	if err != nil {
		t.Skip("no /proc: this census needs a Linux to count descriptors on")
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		t.Fatal(err)
	}
	return len(names) - 1 // less the handle this function is holding
}

// settleForCensus gives finalizers, close handshakes and reaped process groups
// time to actually land, so a delta is a leak rather than a race with cleanup.
func settleForCensus() {
	for i := 0; i < 8; i++ {
		runtime.GC()
		time.Sleep(250 * time.Millisecond)
	}
}

// TestExecLeaksNoDescriptorsOrGoroutines runs fifty exec cycles of each shape
// that ends an exec differently, and counts.
//
// Both shapes matter and they take different exits. A command that finishes is
// closed by the SANDBOX — a FrameClose down the relay, which the hub has always
// answered by closing the client. A caller that HANGS UP is closed by the hub
// noticing its read fail, which is the exit that leaked: reverting that one line
// makes this test report +50 descriptors over the fifty hang-up cycles.
func TestExecLeaksNoDescriptorsOrGoroutines(t *testing.T) {
	if _, err := os.Stat("/bin/sh"); err != nil {
		t.Skip("this machine has no /bin/sh")
	}
	f, id, _ := execScene(t, "census")

	// Warm up: the first cycles build connection pools and caches, and those
	// are not leaks.
	for i := 0; i < 5; i++ {
		if _, _, _, err := f.exec(id, shellSpec("echo warm"), nil); err != nil {
			t.Fatal(err)
		}
	}
	settleForCensus()
	fd0, g0 := openFDs(t), runtime.NumGoroutine()

	const cycles = 50
	for i := 0; i < cycles; i++ {
		res, out, _, err := f.exec(id, shellSpec("echo hello; echo err >&2; exit 3"), nil)
		if err != nil {
			t.Fatalf("cycle %d: %v", i, err)
		}
		if res.ExitCode == nil || *res.ExitCode != 3 || out != "hello\n" {
			t.Fatalf("cycle %d answered %+v with stdout %q", i, res, out)
		}
	}
	for i := 0; i < cycles; i++ {
		hangUpMidExec(t, f, id)
	}
	// The killed process groups have to be reaped before the slots come back,
	// and a held slot is a goroutine and a pipe pair.
	deadline := time.Now().Add(60 * time.Second)
	for f.sessiond(id).execRunner().LiveCount() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d exec(s) are still live after every caller hung up",
				f.sessiond(id).execRunner().LiveCount())
		}
		time.Sleep(50 * time.Millisecond)
	}

	settleForCensus()
	fd1, g1 := openFDs(t), runtime.NumGoroutine()
	t.Logf("%d clean + %d hang-up cycles: fds %d -> %d (%+d), goroutines %d -> %d (%+d)",
		cycles, cycles, fd0, fd1, fd1-fd0, g0, g1, g1-g0)

	// A leak is per-cycle; a handful either way is pooling. The threshold is a
	// fifth of the cycle count so a genuine one-per-command leak (which is
	// +100 here) cannot hide under it.
	if fd1-fd0 > cycles/5 {
		t.Fatalf("%+d descriptors over %d cycles. `rainier exec` in a CI loop walks "+
			"this runner to EMFILE.%s", fd1-fd0, 2*cycles, stacksForCensus())
	}
	if g1-g0 > cycles/5 {
		t.Fatalf("%+d goroutines over %d cycles.%s", g1-g0, 2*cycles, stacksForCensus())
	}
}

// hangUpMidExec runs a command and walks away while it is still running, which
// is the exit that leaked.
func hangUpMidExec(t *testing.T, f *fleet, id string) {
	t.Helper()
	ctx, cancel := context.WithCancel(f.ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = execio.Run(ctx, f.wsBase()+"/v0/sessions/"+id+"/exec", execio.Options{
			Spec:   shellSpec("echo up; sleep 120"),
			Header: http.Header{"Authorization": {"Bearer " + f.token}},
		})
	}()
	time.Sleep(60 * time.Millisecond) // it is running
	cancel()
	<-done
}

// stacksForCensus is every goroutine's stack, which is the only useful thing to
// print when a census fails: the number says there is a leak and the stacks say
// whose.
func stacksForCensus() string {
	buf := make([]byte, 1<<20)
	return "\n\n" + string(buf[:runtime.Stack(buf, true)])
}

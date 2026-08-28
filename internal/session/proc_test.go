package session

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func TestProcEchoAndCleanExit(t *testing.T) {
	var mu sync.Mutex
	var out strings.Builder
	p, err := StartProc([]string{"sh", "-c", "echo hello-pty"}, 80, 24, func(b []byte) {
		mu.Lock(); out.Write(b); mu.Unlock()
	})
	if err != nil { t.Fatal(err) }
	code := p.Wait() // must return despite Linux EIO-on-exit behavior
	if code != 0 { t.Fatalf("exit = %d", code) }
	mu.Lock(); defer mu.Unlock()
	if !strings.Contains(out.String(), "hello-pty") { t.Fatalf("output = %q", out.String()) }
}

func TestProcStdinReachesChild(t *testing.T) {
	var mu sync.Mutex
	var out strings.Builder
	p, err := StartProc([]string{"cat"}, 80, 24, func(b []byte) {
		mu.Lock(); out.Write(b); mu.Unlock()
	})
	if err != nil { t.Fatal(err) }
	p.Write([]byte("ping\n"))
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock(); s := out.String(); mu.Unlock()
		if strings.Contains(s, "ping") { p.Stop(); p.Wait(); return }
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("stdin never echoed; output = %q", out.String())
}

func TestProcExitCode(t *testing.T) {
	p, err := StartProc([]string{"sh", "-c", "exit 3"}, 80, 24, func([]byte) {})
	if err != nil { t.Fatal(err) }
	if code := p.Wait(); code != 3 { t.Fatalf("exit = %d, want 3", code) }
}

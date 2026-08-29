package attachio

import (
	"strings"
	"testing"
)

// TestAttachURL is moved verbatim from cmd/rattach/main_test.go's
// TestAttachURL (unexported attachURL there) — same cases, now against the
// exported AttachURL.
func TestAttachURL(t *testing.T) {
	t.Run("session present", func(t *testing.T) {
		got := AttachURL("ws://127.0.0.1:8080", 42, "sess-7")
		if !strings.Contains(got, "since=42") {
			t.Errorf("AttachURL(...) = %q, want it to contain since=42", got)
		}
		if !strings.Contains(got, "session=sess-7") {
			t.Errorf("AttachURL(...) = %q, want it to contain session=sess-7", got)
		}
	})

	t.Run("session present with characters needing escaping", func(t *testing.T) {
		got := AttachURL("ws://127.0.0.1:8080", 0, "sess a/b")
		if !strings.Contains(got, "session=sess+a%2Fb") {
			t.Errorf("AttachURL(...) = %q, want an escaped session param", got)
		}
	})

	t.Run("session empty", func(t *testing.T) {
		got := AttachURL("ws://127.0.0.1:7070", 0, "")
		if !strings.Contains(got, "since=0") {
			t.Errorf("AttachURL(...) = %q, want it to contain since=0", got)
		}
		if strings.Contains(got, "session=") {
			t.Errorf("AttachURL(...) = %q, want no session param when session is empty", got)
		}
	})
}

// TestScanDetach pins the three cases the brief calls out: mid-buffer,
// absent, and first byte.
func TestScanDetach(t *testing.T) {
	t.Run("mid-buffer", func(t *testing.T) {
		buf := []byte("hello\x1dworld")
		if got := ScanDetach(buf); got != 5 {
			t.Errorf("ScanDetach(%q) = %d, want 5", buf, got)
		}
	})

	t.Run("absent", func(t *testing.T) {
		buf := []byte("hello world")
		if got := ScanDetach(buf); got != -1 {
			t.Errorf("ScanDetach(%q) = %d, want -1", buf, got)
		}
	})

	t.Run("first byte", func(t *testing.T) {
		buf := []byte("\x1dhello")
		if got := ScanDetach(buf); got != 0 {
			t.Errorf("ScanDetach(%q) = %d, want 0", buf, got)
		}
	})

	t.Run("empty buffer", func(t *testing.T) {
		if got := ScanDetach(nil); got != -1 {
			t.Errorf("ScanDetach(nil) = %d, want -1", got)
		}
	})
}

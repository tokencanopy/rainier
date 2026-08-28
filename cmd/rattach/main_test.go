package main

import (
	"strings"
	"testing"
)

func TestAttachURL(t *testing.T) {
	t.Run("session present", func(t *testing.T) {
		got := attachURL("ws://127.0.0.1:8080", 42, "sess-7")
		if !strings.Contains(got, "since=42") {
			t.Errorf("attachURL(...) = %q, want it to contain since=42", got)
		}
		if !strings.Contains(got, "session=sess-7") {
			t.Errorf("attachURL(...) = %q, want it to contain session=sess-7", got)
		}
	})

	t.Run("session present with characters needing escaping", func(t *testing.T) {
		got := attachURL("ws://127.0.0.1:8080", 0, "sess a/b")
		if !strings.Contains(got, "session=sess+a%2Fb") {
			t.Errorf("attachURL(...) = %q, want an escaped session param", got)
		}
	})

	t.Run("session empty", func(t *testing.T) {
		got := attachURL("ws://127.0.0.1:7070", 0, "")
		if !strings.Contains(got, "since=0") {
			t.Errorf("attachURL(...) = %q, want it to contain since=0", got)
		}
		if strings.Contains(got, "session=") {
			t.Errorf("attachURL(...) = %q, want no session param when session is empty", got)
		}
	})
}

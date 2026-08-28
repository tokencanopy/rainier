package eventlog

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestAppendSinceRoundTrip(t *testing.T) {
	p := filepath.Join(t.TempDir(), "s.log")
	l, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	s1, _ := l.Append("output", []byte("hello"))
	s2, _ := l.Append("output", []byte("world"))
	if s1 != 1 || s2 != 2 {
		t.Fatalf("seqs = %d,%d, want 1,2", s1, s2)
	}
	got, err := l.Since(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Seq != 2 || !bytes.Equal(got[0].Data, []byte("world")) {
		t.Fatalf("Since(1) = %+v", got)
	}
	if l.LastSeq() != 2 {
		t.Fatalf("LastSeq = %d", l.LastSeq())
	}
}

func TestReloadResumesSeq(t *testing.T) {
	p := filepath.Join(t.TempDir(), "s.log")
	l, _ := Open(p)
	l.Append("output", []byte("a"))
	l.Close()
	l2, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	s, _ := l2.Append("output", []byte("b"))
	if s != 2 {
		t.Fatalf("seq after reload = %d, want 2", s)
	}
	got, _ := l2.Since(0)
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
}

func TestOpenTruncatesCorruptTail(t *testing.T) {
	p := filepath.Join(t.TempDir(), "s.log")
	l, _ := Open(p)
	l.Append("output", []byte("a"))
	l.Append("output", []byte("b"))
	l.Close()
	f, _ := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0o600)
	f.WriteString(`{"seq":3,"t":"outp`) // torn write, no newline
	f.Close()
	l2, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if l2.LastSeq() != 2 {
		t.Fatalf("LastSeq = %d, want 2", l2.LastSeq())
	}
	s, _ := l2.Append("output", []byte("c"))
	if s != 3 {
		t.Fatalf("seq = %d, want 3", s)
	}
	entries, _ := l2.Since(0)
	if len(entries) != 3 {
		t.Fatalf("len = %d, want 3", len(entries))
	}
	// file must be clean JSONL again: every line parses
	raw, _ := os.ReadFile(p)
	for i, line := range bytes.Split(bytes.TrimRight(raw, "\n"), []byte("\n")) {
		var e Entry
		if json.Unmarshal(line, &e) != nil {
			t.Fatalf("line %d unparseable: %q", i, line)
		}
	}
}

// Package eventlog is an append-only, sequence-numbered event log backed by a
// JSONL file. It is the resume backbone: viewers reconnect with a sequence
// cursor and replay exactly what they missed.
package eventlog

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
	"time"
)

type Entry struct {
	Seq  uint64 `json:"seq"`
	Type string `json:"t"`
	Data []byte `json:"d"` // std json base64-encodes []byte
	TS   int64  `json:"ts"`
}

type Log struct {
	mu      sync.Mutex
	f       *os.File
	w       *bufio.Writer
	entries []Entry // in-memory copy; v0 keeps all entries (bounded later by rotation)
	last    uint64
}

func Open(path string) (*Log, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	l := &Log{f: f}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for sc.Scan() {
		var e Entry
		if json.Unmarshal(sc.Bytes(), &e) == nil {
			l.entries = append(l.entries, e)
			l.last = e.Seq
		}
	}
	if err := sc.Err(); err != nil {
		f.Close()
		return nil, err
	}
	l.w = bufio.NewWriter(f)
	return l, nil
}

func (l *Log) Append(typ string, data []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.last++
	e := Entry{Seq: l.last, Type: typ, Data: append([]byte(nil), data...), TS: time.Now().UnixMilli()}
	b, err := json.Marshal(e)
	if err != nil {
		return 0, err
	}
	if _, err := l.w.Write(append(b, '\n')); err != nil {
		return 0, err
	}
	if err := l.w.Flush(); err != nil {
		return 0, err
	}
	l.entries = append(l.entries, e)
	return e.Seq, nil
}

func (l *Log) Since(seq uint64) ([]Entry, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []Entry
	for _, e := range l.entries {
		if e.Seq > seq {
			out = append(out, e)
		}
	}
	return out, nil
}

func (l *Log) LastSeq() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.last
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.w.Flush()
	return l.f.Close()
}

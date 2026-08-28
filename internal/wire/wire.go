// Package wire defines the JSON messages exchanged between sessiond and
// viewers. One message type per direction keeps the protocol greppable.
package wire

type ClientMsg struct {
	Type string `json:"type"`           // "stdin" | "resize"
	Data []byte `json:"data,omitempty"` // stdin bytes
	Cols int    `json:"cols,omitempty"`
	Rows int    `json:"rows,omitempty"`
}

type ServerMsg struct {
	Type     string `json:"type"` // "snapshot" | "output" | "exit"
	Seq      uint64 `json:"seq,omitempty"`
	Data     []byte `json:"data,omitempty"`
	Cols     int    `json:"cols,omitempty"`
	Rows     int    `json:"rows,omitempty"`
	ExitCode int    `json:"exitCode,omitempty"`
}

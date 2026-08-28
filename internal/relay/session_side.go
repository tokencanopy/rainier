// internal/relay/session_side.go
package relay

import (
	"context"
	"encoding/json"
	"sync"

	"rainier/internal/session"
	"rainier/internal/wire"
)

// ServeSession runs on the sessiond side: it reads Frames off conn (an
// already-established outbound conn to runnerd) and demultiplexes them onto
// s by AttachID — FrameOpen calls s.Attach and starts a per-attachment
// goroutine pumping s's ServerMsgs back as FrameServer; FrameClient carries a
// raw wire.ClientMsg into s.Stdin/s.SetSize; FrameClose calls s.Detach.
// Returns when conn.Read errors (conn closed).
func ServeSession(ctx context.Context, conn Conn, s *session.Session) error {
	var mu sync.Mutex
	atts := map[uint64]*session.Attachment{}
	write := func(f Frame) { b, _ := Encode(f); conn.Write(ctx, b) }

	for {
		raw, err := conn.Read(ctx)
		if err != nil { return err }
		f, err := Decode(raw)
		if err != nil { continue }
		switch f.Type {
		case FrameOpen:
			att, err := s.Attach(f.Since, session.Size{Cols: f.Cols, Rows: f.Rows})
			if err != nil { write(Frame{Type: FrameClose, AttachID: f.AttachID}); continue }
			mu.Lock(); atts[f.AttachID] = att; mu.Unlock()
			go func(id uint64, a *session.Attachment) {
				for msg := range a.Msgs {
					p, _ := json.Marshal(msg)
					write(Frame{Type: FrameServer, AttachID: id, Payload: p})
				}
				write(Frame{Type: FrameClose, AttachID: id})
				mu.Lock(); delete(atts, id); mu.Unlock()
			}(f.AttachID, att)
		case FrameClient:
			var cm wire.ClientMsg
			if json.Unmarshal(f.Payload, &cm) != nil { continue }
			mu.Lock(); att := atts[f.AttachID]; mu.Unlock()
			if att == nil { continue }
			switch cm.Type {
			case "stdin": s.Stdin(cm.Data)
			case "resize": s.SetSize(att.ID, session.Size{Cols: cm.Cols, Rows: cm.Rows})
			}
		case FrameClose:
			mu.Lock(); att := atts[f.AttachID]; delete(atts, f.AttachID); mu.Unlock()
			if att != nil { s.Detach(att.ID) }
		}
	}
}

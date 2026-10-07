package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
)

// Reconnect control kinds run before the ordinary relay is online. They use
// the existing FrameControl envelope with only kind and payload fields.
const (
	KindGuestReconnectChallenge = "guest_reconnect_challenge"
	KindGuestReconnectProof     = "guest_reconnect_proof"
	KindGuestReconnectAccepted  = "guest_reconnect_accepted"
	KindGuestReconnectRefused   = "guest_reconnect_refused"
	KindGuestReconnectReady     = "guest_reconnect_ready"
	KindGuestReconnectReadyAck  = "guest_reconnect_ready_ack"
	GuestReconnectFrameLimit    = 4096
)

var errReconnectFrame = errors.New("invalid guest reconnect frame")

// ReadGuestReconnectFrame requires a transport with pre-allocation read bounds
// (NetConn today). It rejects unknown, duplicate, null and extra envelope fields,
// non-control/attachment frames and unknown kinds. Payloads need their shared
// protocol decoder as well. The caller owns its handshake context and cleanup.
func ReadGuestReconnectFrame(ctx context.Context, c Conn) (ControlEvent, error) {
	var zero ControlEvent
	reader, ok := c.(interface {
		ReadLimited(context.Context, int) ([]byte, error)
	})
	if !ok {
		return zero, errReconnectFrame
	}
	raw, err := reader.ReadLimited(ctx, GuestReconnectFrameLimit)
	if err != nil {
		return zero, errReconnectFrame
	}
	var frame Frame
	if strictReconnectObject(raw, &frame, "t", "a", "p") != nil || frame.Type != FrameControl || frame.AttachID != 0 {
		return zero, errReconnectFrame
	}
	var event ControlEvent
	if strictReconnectObject(frame.Payload, &event, "kind", "payload") != nil || !reconnectKind(event.Kind) {
		return zero, errReconnectFrame
	}
	return event, nil
}

// WriteGuestReconnectFrame writes one bounded handshake control frame. The
// caller must supply a deadline; no configuration or relay traffic belongs here.
func WriteGuestReconnectFrame(ctx context.Context, c Conn, kind string, payload []byte) error {
	if !reconnectKind(kind) {
		return errReconnectFrame
	}
	body, err := json.Marshal(ControlEvent{Kind: kind, Payload: payload})
	if err != nil {
		return errReconnectFrame
	}
	raw, err := Encode(Frame{Type: FrameControl, Payload: body})
	if err != nil || len(raw) > GuestReconnectFrameLimit {
		return errReconnectFrame
	}
	if err := c.Write(ctx, raw); err != nil {
		return errReconnectFrame
	}
	return nil
}
func reconnectKind(kind string) bool {
	switch kind {
	case KindGuestReconnectChallenge, KindGuestReconnectProof, KindGuestReconnectAccepted, KindGuestReconnectRefused, KindGuestReconnectReady, KindGuestReconnectReadyAck:
		return true
	}
	return false
}
func strictReconnectObject(raw []byte, out any, keys ...string) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return errReconnectFrame
	}
	allowed := make(map[string]bool, len(keys))
	for _, k := range keys {
		allowed[k] = true
	}
	seen := make(map[string]bool, len(keys))
	for d.More() {
		key, err := d.Token()
		name, ok := key.(string)
		if err != nil || !ok || !allowed[name] || seen[name] {
			return errReconnectFrame
		}
		var value json.RawMessage
		if d.Decode(&value) != nil || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return errReconnectFrame
		}
		seen[name] = true
	}
	if token, err = d.Token(); err != nil || token != json.Delim('}') || len(seen) != len(keys) {
		return errReconnectFrame
	}
	if _, err = d.Token(); err != io.EOF {
		return errReconnectFrame
	}
	if json.Unmarshal(raw, out) != nil {
		return errReconnectFrame
	}
	return nil
}

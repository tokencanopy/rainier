package runnerd

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/tokencanopy/rainier/internal/relay"
	"github.com/tokencanopy/rainier/protocol/runner"
)

// Exact spellings and one occurrence per field, including the outer envelope.
// Typed JSON decoding alone accepts case aliases and duplicate last values.
func guestPreambleObject(raw []byte, keys ...string) (map[string]json.RawMessage, error) {
	if len(raw) > runner.GuestReconnectPayloadLimit {
		return nil, errReconnectInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, errReconnectInvalid
	}
	fields := make(map[string]json.RawMessage, len(keys))
	allowed := make(map[string]bool, len(keys))
	for _, key := range keys {
		allowed[key] = true
	}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, errReconnectInvalid
		}
		key, ok := token.(string)
		if !ok || !allowed[key] || fields[key] != nil {
			return nil, errReconnectInvalid
		}
		var value json.RawMessage
		if decoder.Decode(&value) != nil || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return nil, errReconnectInvalid
		}
		fields[key] = value
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') || len(fields) != len(keys) {
		return nil, errReconnectInvalid
	}
	if _, err = decoder.Token(); err != io.EOF {
		return nil, errReconnectInvalid
	}
	return fields, nil
}
func decodeGuestBootstrapEvent(raw []byte, method string) (relay.ControlEvent, error) {
	fields, err := guestPreambleObject(raw, "kind", "id", "payload")
	var event relay.ControlEvent
	if err != nil || json.Unmarshal(fields["kind"], &event.Kind) != nil || json.Unmarshal(fields["id"], &event.ID) != nil || event.Kind != "req:"+method || event.ID == 0 || isRunnerOriginated(event.ID) {
		return relay.ControlEvent{}, errReconnectInvalid
	}
	event.Payload = fields["payload"]
	return event, nil
}

type guestRedemption struct {
	Protocol uint64 `json:"protocol"`
	Token    string `json:"token"`
}

func decodeGuestRedemption(raw []byte) (guestRedemption, error) {
	fields, err := guestPreambleObject(raw, "protocol", "token")
	var req guestRedemption
	if err != nil || json.Unmarshal(fields["protocol"], &req.Protocol) != nil || json.Unmarshal(fields["token"], &req.Token) != nil || req.Protocol != runner.GuestReconnectProtocol || req.Token == "" {
		return guestRedemption{}, errReconnectInvalid
	}
	return req, nil
}

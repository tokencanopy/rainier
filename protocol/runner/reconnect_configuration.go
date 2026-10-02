package runner

import (
	"bytes"
	"encoding/json"
	"io"
)

// MethodGuestReconnectConfiguration is runner-only. A protocol-only request
// resolves current non-secret launch configuration under the authenticated
// session placement and current policy. It neither proves a guest nor mints a
// token. Use DecodeGuestReconnectBeginRequest for its protocol-only request.
const MethodGuestReconnectConfiguration = "guest_reconnect_configuration"

// GuestReconnectConfigurationLimit bounds an authorized configuration response,
// which may contain setup/init scripts. Requests and proof frames retain 4 KiB.
const GuestReconnectConfigurationLimit = 4 << 20

// GuestReconnectConfiguration carries fresh launch material with no bootstrap
// token or secret values. The host checks session/placement against its retained
// ownership, then supplies only the token returned by this stream's proof.
// It must never replay this response on a later attempt or persist it to disk.
type GuestReconnectConfiguration struct {
	Protocol            uint64 `json:"protocol"`
	SessionID           string `json:"session_id"`
	PlacementGeneration uint64 `json:"placement_generation"`
	Spec                *Spec  `json:"spec"`
}

// DecodeGuestReconnectConfiguration bounds the whole payload, rejects duplicate,
// null, unknown or trailing values, and returns zero configuration on failure.
// Nested launch fields keep Spec's existing encoding/json field semantics.
func DecodeGuestReconnectConfiguration(payload []byte) (GuestReconnectConfiguration, error) {
	var v GuestReconnectConfiguration
	if decodeReconnectObjectLimit(payload, &v, GuestReconnectConfigurationLimit, "protocol", "session_id", "placement_generation", "spec") != nil || v.Protocol != GuestReconnectProtocol || !reconnectID(v.SessionID) || v.PlacementGeneration == 0 || v.Spec == nil || v.Spec.BootstrapToken != "" {
		return GuestReconnectConfiguration{}, errGuestReconnectMessage
	}
	d := json.NewDecoder(bytes.NewReader(payload))
	d.UseNumber()
	if !uniqueConfigurationValue(d, 0) {
		return GuestReconnectConfiguration{}, errGuestReconnectMessage
	}
	if _, err := d.Token(); err != io.EOF {
		return GuestReconnectConfiguration{}, errGuestReconnectMessage
	}
	d = json.NewDecoder(bytes.NewReader(payload))
	d.DisallowUnknownFields()
	if d.Decode(&v) != nil {
		return GuestReconnectConfiguration{}, errGuestReconnectMessage
	}
	return v, nil
}

// Bound recursion independently of the byte limit; tenant environment maps
// have arbitrary keys but must not hide duplicates from typed JSON decoding.
func uniqueConfigurationValue(d *json.Decoder, depth int) bool {
	if depth > 32 {
		return false
	}
	token, err := d.Token()
	if err != nil || token == nil {
		return false
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return true
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for d.More() {
			token, err := d.Token()
			key, ok := token.(string)
			if err != nil || !ok || seen[key] {
				return false
			}
			seen[key] = true
			if !uniqueConfigurationValue(d, depth+1) {
				return false
			}
		}
		token, err = d.Token()
		return err == nil && token == json.Delim('}')
	case '[':
		for d.More() {
			if !uniqueConfigurationValue(d, depth+1) {
				return false
			}
		}
		token, err = d.Token()
		return err == nil && token == json.Delim(']')
	default:
		return false
	}
}

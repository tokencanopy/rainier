package runner

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"io"
)

// Reconnect methods use the existing session-RPC envelope. These names alone do
// not enable a handler or advertise support. Enrollment is the fresh guest's
// single-use bootstrap exchange; begin and accept are runner-originated for a
// surviving guest. Hosts must never forward arbitrary guest-originated begin or
// accept calls, and must derive scope from the original authenticated socket and
// stored placement, never a payload. Authorization is still control-plane work.
const (
	MethodEnrollGuestReconnect = "enroll_guest_reconnect"
	MethodBeginGuestReconnect  = "begin_guest_reconnect"
	MethodAcceptGuestReconnect = "accept_guest_reconnect"
	// GuestReconnectPayloadLimit bounds requests and cryptographic responses,
	// including JSON whitespace. The authorized enrollment environment response
	// retains the existing bootstrap response contract and is not subject to it.
	// Transport frame limits remain independently required.
	GuestReconnectPayloadLimit = 4 << 10
)

// GuestReconnectEnrollRequest spends the initial bootstrap token and pins the
// public key and boot epoch atomically. It contains no selectable workload scope.
// Tokens and keys are canonical unpadded base64url of 32 bytes. Use the bounded
// DecodeGuestReconnectEnrollRequest at untrusted boundaries, not json.Unmarshal.
type GuestReconnectEnrollRequest struct {
	Protocol  uint64 `json:"protocol"`
	Token     string `json:"token"`
	BootEpoch string `json:"boot_epoch"`
	PublicKey string `json:"public_key"`
}

// GuestReconnectBeginRequest asks the control plane to create a fresh pending
// challenge. It neither replaces a token nor authorizes relay takeover.
type GuestReconnectBeginRequest struct {
	Protocol uint64 `json:"protocol"`
}

// GuestReconnectAcceptRequest presents a signature over the stored challenge.
// The caller cannot select a key, challenge, session, placement or connection epoch.
// A structurally valid proof still requires signature and durable authority checks.
type GuestReconnectAcceptRequest struct {
	Protocol  uint64 `json:"protocol"`
	AttemptID string `json:"attempt_id"`
	Signature string `json:"signature"`
}

// GuestReconnectEnrollResponse returns the freshly resolved environment after
// atomic enrollment/spend succeeds, matching the existing bootstrap exchange.
// The control plane must resolve secrets only after authorization; a resolution
// failure does not undo the spend. This secret-bearing response is deliberately
// outside the 4 KiB cryptographic-message limit, like the existing exchange.
// No response may be cached, logged or delivered before enrollment commits.
type GuestReconnectEnrollResponse struct {
	Env map[string]string `json:"env"`
}

// GuestReconnectAcceptResponse carries newly committed connection authority.
// Epoch is per enrolled boot, not a placement or controller generation. A lost
// response requires a fresh challenge; this bearer response must never be cached
// for replay. ExpiresInSec describes the token TTL, not the challenge lifetime.
type GuestReconnectAcceptResponse struct {
	Epoch        uint64 `json:"epoch"`
	Token        string `json:"token"`
	ExpiresInSec uint32 `json:"expires_in_sec"`
}

// GuestReconnectErrorResponse is the entire refusal payload. Error is one of
// invalid, expired, fenced or unavailable; no peer, provider or database text may
// be substituted. In session-RPC it accompanies an envelope with OK=false.
type GuestReconnectErrorResponse struct {
	Error string `json:"error"`
}

var errGuestReconnectMessage = errors.New("runner: invalid guest reconnect message")

// DecodeGuestReconnectEnrollRequest strictly decodes one bounded version-1
// enrollment. All reconnect decoders reject missing, unknown, duplicate,
// case-aliased, null, trailing or oversized data and malformed field values. On
// failure they return a zero value and a fixed error containing no input bytes.
func DecodeGuestReconnectEnrollRequest(payload []byte) (GuestReconnectEnrollRequest, error) {
	var v GuestReconnectEnrollRequest
	if decodeReconnectObject(payload, &v, "protocol", "token", "boot_epoch", "public_key") != nil || v.Protocol != GuestReconnectProtocol || !reconnectID(v.BootEpoch) {
		return GuestReconnectEnrollRequest{}, errGuestReconnectMessage
	}
	if _, ok := reconnectBytes(v.Token, 32); !ok {
		return GuestReconnectEnrollRequest{}, errGuestReconnectMessage
	}
	if _, ok := reconnectBytes(v.PublicKey, ed25519.PublicKeySize); !ok {
		return GuestReconnectEnrollRequest{}, errGuestReconnectMessage
	}
	return v, nil
}

// DecodeGuestReconnectBeginRequest applies the strict bounded reconnect wire
// rules. No challenge, identity or scope fields are accepted from the requester.
func DecodeGuestReconnectBeginRequest(payload []byte) (GuestReconnectBeginRequest, error) {
	var v GuestReconnectBeginRequest
	if decodeReconnectObject(payload, &v, "protocol") != nil || v.Protocol != GuestReconnectProtocol {
		return GuestReconnectBeginRequest{}, errGuestReconnectMessage
	}
	return v, nil
}

// DecodeGuestReconnectAcceptRequest checks shape and canonical signature
// encoding only. The control plane must verify proof, expiry and current authority.
func DecodeGuestReconnectAcceptRequest(payload []byte) (GuestReconnectAcceptRequest, error) {
	var v GuestReconnectAcceptRequest
	if decodeReconnectObject(payload, &v, "protocol", "attempt_id", "signature") != nil || v.Protocol != GuestReconnectProtocol || !reconnectID(v.AttemptID) {
		return GuestReconnectAcceptRequest{}, errGuestReconnectMessage
	}
	if _, ok := reconnectBytes(v.Signature, ed25519.SignatureSize); !ok {
		return GuestReconnectAcceptRequest{}, errGuestReconnectMessage
	}
	return v, nil
}

// DecodeGuestReconnectChallenge applies strict wire and signed-transcript
// validation. The receiving host must also compare session, placement and host
// incarnation with its expected authority before presenting a challenge to a guest.
func DecodeGuestReconnectChallenge(payload []byte) (GuestReconnectChallenge, error) {
	var v GuestReconnectChallenge
	if decodeReconnectObject(payload, &v, "protocol", "session_id", "boot_epoch", "host_incarnation", "attempt_id", "placement_generation", "challenge") != nil {
		return GuestReconnectChallenge{}, errGuestReconnectMessage
	}
	if _, err := v.SigningMessage(); err != nil {
		return GuestReconnectChallenge{}, errGuestReconnectMessage
	}
	return v, nil
}

// DecodeGuestReconnectAcceptResponse checks the fresh token's canonical encoding
// and positive epoch/TTL. It does not validate store authority or lease freshness.
func DecodeGuestReconnectAcceptResponse(payload []byte) (GuestReconnectAcceptResponse, error) {
	var v GuestReconnectAcceptResponse
	if decodeReconnectObject(payload, &v, "epoch", "token", "expires_in_sec") != nil || v.Epoch == 0 || v.ExpiresInSec == 0 {
		return GuestReconnectAcceptResponse{}, errGuestReconnectMessage
	}
	if _, ok := reconnectBytes(v.Token, 32); !ok {
		return GuestReconnectAcceptResponse{}, errGuestReconnectMessage
	}
	return v, nil
}

// DecodeGuestReconnectErrorResponse accepts only the four fixed reconnect
// refusal codes, with the same size and exact-field checks as successful messages.
func DecodeGuestReconnectErrorResponse(payload []byte) (GuestReconnectErrorResponse, error) {
	var v GuestReconnectErrorResponse
	if decodeReconnectObject(payload, &v, "error") != nil {
		return GuestReconnectErrorResponse{}, errGuestReconnectMessage
	}
	switch v.Error {
	case "invalid", "expired", "fenced", "unavailable":
		return v, nil
	}
	return GuestReconnectErrorResponse{}, errGuestReconnectMessage
}

func reconnectID(id string) bool {
	if len(id) == 0 || len(id) > 256 {
		return false
	}
	for i := range id {
		if id[i] < 0x21 || id[i] > 0x7e {
			return false
		}
	}
	return true
}

// Check the flat object before typed decoding: encoding/json alone folds field
// case and accepts duplicate keys. No raw decoder error may escape this boundary.
func decodeReconnectObject(payload []byte, out any, names ...string) error {
	if len(payload) > GuestReconnectPayloadLimit {
		return errGuestReconnectMessage
	}
	d := json.NewDecoder(bytes.NewReader(payload))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return errGuestReconnectMessage
	}
	seen := make(map[string]bool, len(names))
	for d.More() {
		token, err = d.Token()
		name, ok := token.(string)
		if err != nil || !ok || seen[name] {
			return errGuestReconnectMessage
		}
		known := false
		for _, want := range names {
			if name == want {
				known = true
				break
			}
		}
		if !known {
			return errGuestReconnectMessage
		}
		var raw json.RawMessage
		if d.Decode(&raw) != nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return errGuestReconnectMessage
		}
		seen[name] = true
	}
	if token, err = d.Token(); err != nil || token != json.Delim('}') || len(seen) != len(names) {
		return errGuestReconnectMessage
	}
	if _, err = d.Token(); err != io.EOF {
		return errGuestReconnectMessage
	}
	if json.Unmarshal(payload, out) != nil {
		return errGuestReconnectMessage
	}
	return nil
}

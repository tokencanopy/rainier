package runner

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/binary"
	"errors"
)

// GuestReconnectProtocol is negotiated independently of the boot configuration.
// Its presence does not enable memory persistence or permit legacy token replay.
const GuestReconnectProtocol uint64 = 1

// GuestReconnectChallenge is the control-plane-issued context signed by a
// surviving guest. SessionID and placement are derived from the authenticated
// runner binding, never selected by a proof request. Challenges expire and are
// consumed in the authoritative store; signature verification alone is not an
// authorization decision. HostIncarnation identifies the runner connection's
// control-plane generation, not a guest-supplied host name.
type GuestReconnectChallenge struct {
	Protocol            uint64 `json:"protocol"`
	SessionID           string `json:"session_id"`
	BootEpoch           string `json:"boot_epoch"`
	HostIncarnation     string `json:"host_incarnation"`
	AttemptID           string `json:"attempt_id"`
	PlacementGeneration uint64 `json:"placement_generation"`
	Challenge           string `json:"challenge"`
}

var errGuestReconnect = errors.New("runner: invalid guest reconnect proof")

// SigningMessage returns the versioned, domain-separated binary transcript.
// Identifiers are 1..256 printable ASCII bytes; nonce encoding is canonical
// unpadded base64url of exactly 32 bytes. Invalid input returns a fixed error
// that never contains peer data. Signatures must not be made over JSON encoding.
func (c GuestReconnectChallenge) SigningMessage() ([]byte, error) {
	if c.Protocol != GuestReconnectProtocol || c.PlacementGeneration == 0 {
		return nil, errGuestReconnect
	}
	nonce, ok := reconnectBytes(c.Challenge, 32)
	if !ok {
		return nil, errGuestReconnect
	}
	message := []byte("rainier-guest-reconnect-v1\x00")
	for _, id := range []string{c.SessionID, c.BootEpoch, c.HostIncarnation, c.AttemptID} {
		if len(id) == 0 || len(id) > 256 {
			return nil, errGuestReconnect
		}
		for i := 0; i < len(id); i++ {
			if id[i] < 0x21 || id[i] > 0x7e {
				return nil, errGuestReconnect
			}
		}
		message = binary.BigEndian.AppendUint32(message, uint32(len(id)))
		message = append(message, id...)
	}
	message = binary.BigEndian.AppendUint64(message, c.PlacementGeneration)
	return append(message, nonce...), nil
}

// VerifyProof verifies a canonical base64url Ed25519 public key and signature
// over this exact challenge. It does NOT check expiry, enrollment, placement or
// single use; the authoritative store must atomically enforce those conditions
// before any configuration or bootstrap capability is returned.
func (c GuestReconnectChallenge) VerifyProof(publicKey, signature string) error {
	message, err := c.SigningMessage()
	if err != nil {
		return err
	}
	key, ok := reconnectBytes(publicKey, ed25519.PublicKeySize)
	if !ok {
		return errGuestReconnect
	}
	sig, ok := reconnectBytes(signature, ed25519.SignatureSize)
	if !ok {
		return errGuestReconnect
	}
	if !ed25519.Verify(ed25519.PublicKey(key), message, sig) {
		return errGuestReconnect
	}
	return nil
}

func reconnectBytes(s string, size int) ([]byte, bool) {
	if len(s) != base64.RawURLEncoding.EncodedLen(size) {
		return nil, false
	}
	b, err := base64.RawURLEncoding.Strict().DecodeString(s)
	return b, err == nil && len(b) == size && base64.RawURLEncoding.EncodeToString(b) == s
}

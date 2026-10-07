package runner

// SessionBootstrapRedeemRequest is the exact one-use secret redemption body.
// The same decoder is used for legacy boot and proof-issued host redemption.
type SessionBootstrapRedeemRequest struct {
	Protocol uint64 `json:"protocol"`
	Token    string `json:"token"`
}

func DecodeSessionBootstrapRedeemRequest(payload []byte) (SessionBootstrapRedeemRequest, error) {
	var v SessionBootstrapRedeemRequest
	if decodeReconnectObject(payload, &v, "protocol", "token") != nil || v.Protocol != SessionBootstrapProtocolVersion {
		return SessionBootstrapRedeemRequest{}, errGuestReconnectMessage
	}
	if _, ok := reconnectBytes(v.Token, 32); !ok {
		return SessionBootstrapRedeemRequest{}, errGuestReconnectMessage
	}
	return v, nil
}

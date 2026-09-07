package attachio

import (
	"testing"

	"github.com/coder/websocket"
)

func TestLeaseExpiryAllowsReauthorizationButPolicyDenialsDoNot(t *testing.T) {
	for _, tc := range []struct {
		reason string
		want   bool
	}{
		{"attach lease expired; reattach", true},
		{"access revoked", false},
		{"attach lease expired; reattach: access revoked", false},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			err := websocket.CloseError{Code: websocket.StatusPolicyViolation, Reason: tc.reason}
			if got := retryableWebSocketReadError(err); got != tc.want {
				t.Fatalf("retryable = %v, want %v", got, tc.want)
			}
		})
	}
}

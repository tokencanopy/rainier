package runner

import (
	"encoding/base64"
	"testing"
)

func TestBootstrapRedemptionExactWire(t *testing.T) {
	token := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	valid := `{"protocol":1,"token":"` + token + `"}`
	if _, err := DecodeSessionBootstrapRedeemRequest([]byte(valid)); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{`{"protocol":1}`, `{"protocol":1,"token":null}`, valid + ` {}`, `{"Protocol":1,"token":"` + token + `"}`, `{"protocol":1,"protocol":1,"token":"` + token + `"}`, `{"protocol":1,"token":"not-canonical"}`, `{"protocol":1,"token":"` + token + `","extra":1}`} {
		if _, err := DecodeSessionBootstrapRedeemRequest([]byte(raw)); err == nil {
			t.Fatal("malformed redemption accepted")
		}
	}
}

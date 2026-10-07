package runnerd

import (
	"github.com/tokencanopy/rainier/protocol/runner"
	"testing"
)

func TestGuestBootstrapPreambleRejectsAmbiguousFields(t *testing.T) {
	for _, raw := range []string{
		`{"kind":"req:fetch_session_secrets","id":1,"id":2,"payload":{"protocol":1,"token":"test"}}`,
		`{"Kind":"req:fetch_session_secrets","id":1,"payload":{}}`,
		`{"kind":"req:fetch_session_secrets","id":1,"extra":true,"payload":{}}`,
		`{"kind":"req:fetch_session_secrets","id":1,"payload":{}} {}`,
	} {
		if _, err := decodeGuestBootstrapEvent([]byte(raw), runner.MethodFetchSessionSecrets); err == nil {
			t.Fatal("accepted ambiguous envelope")
		}
	}
	for _, raw := range []string{
		`{"protocol":1,"token":"first","token":"second"}`,
		`{"protocol":1,"Token":"test"}`,
		`{"protocol":1,"token":"test","session":"another_test"}`,
		`{"protocol":1,"token":null}`,
	} {
		if _, err := decodeGuestRedemption([]byte(raw)); err == nil {
			t.Fatal("accepted ambiguous redemption")
		}
	}
	event, err := decodeGuestBootstrapEvent([]byte(`{"kind":"req:fetch_session_secrets","id":9,"payload":{"protocol":1,"token":"test"}}`), runner.MethodFetchSessionSecrets)
	if err != nil || event.ID != 9 {
		t.Fatal("valid envelope rejected")
	}
	req, err := decodeGuestRedemption(event.Payload)
	if err != nil || req.Token != "test" {
		t.Fatal("valid redemption rejected")
	}
}

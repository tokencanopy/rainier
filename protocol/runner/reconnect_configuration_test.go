package runner_test

import (
	"encoding/json"
	"github.com/tokencanopy/rainier/protocol/runner"
	"strings"
	"testing"
)

func TestReconnectConfigurationContract(t *testing.T) {
	valid := `{"protocol":1,"session_id":"session_test","placement_generation":3,"spec":{"image":"example.invalid/test","env":{"CONFIG_TEST":"test"}}}`
	got, err := runner.DecodeGuestReconnectConfiguration([]byte(valid))
	if err != nil || got.SessionID != "session_test" || got.Spec.Env["CONFIG_TEST"] != "test" {
		t.Fatal("valid configuration refused")
	}
	for _, bad := range []string{
		strings.Replace(valid, `"protocol":1`, `"protocol":2`, 1),
		strings.Replace(valid, `"placement_generation":3`, `"placement_generation":0`, 1),
		strings.Replace(valid, `"session_test"`, `""`, 1),
		strings.Replace(valid, `"spec":{`, `"spec":{"bootstrap_token":"token_test",`, 1),
		strings.Replace(valid, `"spec":{`, `"spec":{"unknown":true,`, 1),
		strings.Replace(valid, `"spec":{`, `"spec":{"image":"duplicate_test",`, 1),
		strings.Replace(valid, `"env":{`, `"env":{"CONFIG_TEST":"duplicate_test",`, 1),
		strings.Replace(valid, `"protocol":1`, `"protocol":1,"protocol":1`, 1),
		strings.Replace(valid, `"protocol":1`, `"Protocol":1`, 1),
		valid + ` true`, `null`, strings.Repeat(" ", runner.GuestReconnectConfigurationLimit+1),
	} {
		rejected, err := runner.DecodeGuestReconnectConfiguration([]byte(bad))
		if err == nil || rejected.Spec != nil || rejected.SessionID != "" {
			t.Fatal("invalid configuration accepted or retained data")
		}
	}
	large := runner.GuestReconnectConfiguration{Protocol: 1, SessionID: "session_test", PlacementGeneration: 3, Spec: &runner.Spec{Setup: strings.Repeat("x", 8192)}}
	raw, _ := json.Marshal(large)
	if _, err := runner.DecodeGuestReconnectConfiguration(raw); err != nil {
		t.Fatal("configuration incorrectly uses proof limit")
	}
}

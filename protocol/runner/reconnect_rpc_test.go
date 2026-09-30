package runner_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/tokencanopy/rainier/protocol/runner"
	"reflect"
	"strings"
	"testing"
)

func TestReconnectChallengeRejectsUnknownWireAuthority(t *testing.T) {
	payload := `{"protocol":1,"session_id":"session_test","boot_epoch":"boot_test","host_incarnation":"7","attempt_id":"attempt_test","placement_generation":3,"challenge":"` + strings.Repeat("A", 43) + `","creator":"attacker_test"}`
	if _, err := runner.DecodeGuestReconnectChallenge([]byte(payload)); err == nil {
		t.Fatal("accepted unknown authority field in reconnect challenge")
	}
}

func TestReconnectRPCWireContract(t *testing.T) {
	if runner.MethodEnrollGuestReconnect != "enroll_guest_reconnect" || runner.MethodBeginGuestReconnect != "begin_guest_reconnect" || runner.MethodAcceptGuestReconnect != "accept_guest_reconnect" || runner.GuestReconnectPayloadLimit != 4096 {
		t.Fatal("reconnect wire constants changed")
	}
	for _, tc := range reconnectMessages() {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.decode([]byte(tc.wire))
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(got)
			if err != nil || string(encoded) != tc.wire {
				t.Fatalf("wire changed: %s (%v)", encoded, err)
			}
			// Exactly 4096 bytes is permitted; the next whitespace byte must refuse.
			bounded := tc.wire + strings.Repeat(" ", 4096-len(tc.wire))
			if _, err := tc.decode([]byte(bounded)); err != nil {
				t.Fatal("boundary refused", err)
			}
			if _, err := tc.decode([]byte(bounded + " ")); err == nil {
				t.Fatal("oversized whitespace bypassed bound")
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal([]byte(tc.wire), &fields); err != nil {
				t.Fatal(err)
			}
			first := ""
			for key := range fields {
				first = key
				break
			}
			invalid := map[string]string{
				"unknown":           strings.TrimSuffix(tc.wire, "}") + `,"workspace":"attacker_test"}`,
				"duplicate":         strings.TrimSuffix(tc.wire, "}") + `,"` + first + `":` + string(fields[first]) + `}`,
				"escaped_duplicate": strings.TrimSuffix(tc.wire, "}") + `,"\u` + fmt.Sprintf("%04x", first[0]) + first[1:] + `":` + string(fields[first]) + `}`,
				"case":              strings.Replace(tc.wire, `"`+first+`":`, `"`+strings.ToUpper(first)+`":`, 1),
				"trailing":          tc.wire + ` {"sensitive":"payload_test"}`,
				"trailing_scalar":   tc.wire + ` true`,
				"array":             "[" + tc.wire + "]", "null": "null", "empty": "", "truncated": tc.wire[:len(tc.wire)-1],
			}
			for key := range fields {
				original := fields[key]
				delete(fields, key)
				value, _ := json.Marshal(fields)
				invalid["missing_"+key] = string(value)
				fields[key] = json.RawMessage("null")
				value, _ = json.Marshal(fields)
				invalid["null_"+key] = string(value)
				fields[key] = json.RawMessage("[]")
				value, _ = json.Marshal(fields)
				invalid["type_"+key] = string(value)
				fields[key] = original
			}
			for name, payload := range invalid {
				t.Run(name, func(t *testing.T) {
					got, err := tc.decode([]byte(payload))
					if err == nil {
						t.Fatal("accepted malformed message")
					}
					if err.Error() != "runner: invalid guest reconnect message" {
						t.Fatal("non-fixed error")
					}
					if !reflect.ValueOf(got).IsZero() {
						t.Fatal("failed decode retained authority")
					}
				})
			}
		})
	}
}

type reconnectMessageCase struct {
	name, wire string
	decode     func([]byte) (any, error)
}

func reconnectMessages() []reconnectMessageCase {
	token := strings.Repeat("A", 43)
	signature := strings.Repeat("A", 86)
	return []reconnectMessageCase{
		{"enroll", `{"protocol":1,"token":"` + token + `","boot_epoch":"boot_test","public_key":"` + token + `"}`, func(b []byte) (any, error) { return runner.DecodeGuestReconnectEnrollRequest(b) }},
		{"begin", `{"protocol":1}`, func(b []byte) (any, error) { return runner.DecodeGuestReconnectBeginRequest(b) }},
		{"accept", `{"protocol":1,"attempt_id":"attempt_test","signature":"` + signature + `"}`, func(b []byte) (any, error) { return runner.DecodeGuestReconnectAcceptRequest(b) }},

		{"challenge", `{"protocol":1,"session_id":"session_test","boot_epoch":"boot_test","host_incarnation":"7","attempt_id":"attempt_test","placement_generation":3,"challenge":"` + token + `"}`, func(b []byte) (any, error) { return runner.DecodeGuestReconnectChallenge(b) }},
		{"accepted", `{"epoch":2,"token":"` + token + `","expires_in_sec":120}`, func(b []byte) (any, error) { return runner.DecodeGuestReconnectAcceptResponse(b) }},
		{"refused", `{"error":"fenced"}`, func(b []byte) (any, error) { return runner.DecodeGuestReconnectErrorResponse(b) }},
	}
}

func TestReconnectRPCRejectsInvalidValues(t *testing.T) {
	for _, tc := range reconnectMessages() {
		var fields map[string]json.RawMessage
		_ = json.Unmarshal([]byte(tc.wire), &fields)
		mutations := map[string][]string{
			"protocol":             {"0", "2", "-1", "1.0", "1e0", "18446744073709551616"},
			"epoch":                {"0", "-1", "18446744073709551616"},
			"placement_generation": {"0", "-1"},
			"expires_in_sec":       {"0", "-1", "4294967296"}, "error": {`"internal-secret_test"`, `""`, `"FENCED"`},
		}
		for _, key := range []string{"token", "signature", "public_key", "challenge"} {
			if raw, ok := fields[key]; ok {
				var original string
				_ = json.Unmarshal(raw, &original)
				// Nonzero unused base64 bits, padding, short/long and forbidden alphabets.
				for _, bad := range []string{"", original + "=", original[:len(original)-1], original + "A", strings.Repeat("+", len(original)), original[:len(original)-1] + "B"} {
					b, _ := json.Marshal(bad)
					mutations[key] = append(mutations[key], string(b))
				}
			}
		}
		for _, key := range []string{"attempt_id", "boot_epoch", "session_id", "host_incarnation"} {
			for _, bad := range []string{"", strings.Repeat("a", 257), "white space", "line\nbreak", "unicode_é"} {
				b, _ := json.Marshal(bad)
				mutations[key] = append(mutations[key], string(b))
			}
		}
		for key, values := range mutations {
			original, exists := fields[key]
			if !exists {
				continue
			}
			for i, value := range values {
				t.Run(fmt.Sprintf("%s/%s/%d", tc.name, key, i), func(t *testing.T) {
					fields[key] = json.RawMessage(value)
					payload, _ := json.Marshal(fields)
					if got, err := tc.decode(payload); err == nil || !reflect.ValueOf(got).IsZero() {
						t.Fatal("invalid field value accepted or retained")
					}
				})
			}
			fields[key] = original
		}
	}
	for _, code := range []string{"invalid", "expired", "fenced", "unavailable"} {
		v, err := runner.DecodeGuestReconnectErrorResponse([]byte(`{"error":"` + code + `"}`))
		if err != nil || v.Error != code {
			t.Fatal("known refusal rejected")
		}
	}
}

// Exercise the public codec and signed transcript as real consumers do. Schema
// validity and cryptographic proof remain distinct from durable authorization.
func ExampleDecodeGuestReconnectAcceptRequest() {
	public, private, _ := ed25519.GenerateKey(rand.Reader)
	challenge := runner.GuestReconnectChallenge{Protocol: 1, SessionID: "session_test", BootEpoch: "boot_test", HostIncarnation: "7", AttemptID: "attempt_test", PlacementGeneration: 3, Challenge: base64.RawURLEncoding.EncodeToString(make([]byte, 32))}
	wire, _ := json.Marshal(challenge)
	decoded, err := runner.DecodeGuestReconnectChallenge(wire)
	if err != nil {
		panic(err)
	}
	transcript, _ := decoded.SigningMessage()
	proof := runner.GuestReconnectAcceptRequest{Protocol: 1, AttemptID: decoded.AttemptID, Signature: base64.RawURLEncoding.EncodeToString(ed25519.Sign(private, transcript))}
	wire, _ = json.Marshal(proof)
	request, err := runner.DecodeGuestReconnectAcceptRequest(wire)
	if err != nil {
		panic(err)
	}
	fmt.Println("signature verified:", decoded.VerifyProof(base64.RawURLEncoding.EncodeToString(public), request.Signature) == nil)
	// Output: signature verified: true
}

func FuzzReconnectRPCDecoders(f *testing.F) {
	cases := reconnectMessages()
	for _, tc := range cases {
		f.Add(tc.wire)
	}
	f.Add(`{"protocol":1,"protocol":1}`)
	f.Fuzz(func(t *testing.T, payload string) {
		for _, tc := range cases {
			value, err := tc.decode([]byte(payload))
			if err != nil {
				if !reflect.ValueOf(value).IsZero() || err.Error() != "runner: invalid guest reconnect message" {
					t.Fatal("unsafe error result")
				}
				continue
			}
			if len(payload) > 4096 {
				t.Fatal("size bound bypassed")
			}
			canonical, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			again, err := tc.decode(canonical)
			if err != nil || !reflect.DeepEqual(value, again) {
				t.Fatal("successful decoding is not stable")
			}
		}
	})
}

func TestReconnectEnrollmentKeepsBootstrapEnvironmentShape(t *testing.T) {
	response := runner.GuestReconnectEnrollResponse{Env: map[string]string{"SYNTHETIC_VALUE": "fixture_test"}}
	body, err := json.Marshal(response)
	if err != nil || string(body) != `{"env":{"SYNTHETIC_VALUE":"fixture_test"}}` {
		t.Fatal("enrollment changed the bootstrap environment contract")
	}
}

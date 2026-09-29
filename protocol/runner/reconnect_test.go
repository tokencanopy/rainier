package runner

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"testing"
)

func reconnectChallenge() GuestReconnectChallenge {
	return GuestReconnectChallenge{Protocol: 1, SessionID: "session.test", BootEpoch: "boot.test", HostIncarnation: "host.test", AttemptID: "attempt.test", PlacementGeneration: 7, Challenge: base64.RawURLEncoding.EncodeToString(make([]byte, 32))}
}

func TestGuestReconnectTranscript(t *testing.T) {
	c := reconnectChallenge()
	got, err := c.SigningMessage()
	if err != nil {
		t.Fatal(err)
	}
	// Fixed wire vector, independent of the implementation's encoder.
	want := "7261696e6965722d67756573742d7265636f6e6e6563742d7631000000000c73657373696f6e2e7465737400000009626f6f742e7465737400000009686f73742e746573740000000c617474656d70742e746573740000000000000007" + strings.Repeat("00", 32)
	if hex.EncodeToString(got) != want {
		t.Fatalf("wire transcript differs: %x", got)
	}
}

func TestGuestReconnectProofBindsEveryField(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	original := reconnectChallenge()
	message, err := original.SigningMessage()
	if err != nil {
		t.Fatal(err)
	}
	signature := base64.RawURLEncoding.EncodeToString(ed25519.Sign(private, message))
	key := base64.RawURLEncoding.EncodeToString(public)
	if err := original.VerifyProof(key, signature); err != nil {
		t.Fatal(err)
	}
	mutations := map[string]func(*GuestReconnectChallenge){
		"session":   func(c *GuestReconnectChallenge) { c.SessionID = "another.test" },
		"boot":      func(c *GuestReconnectChallenge) { c.BootEpoch = "another.test" },
		"host":      func(c *GuestReconnectChallenge) { c.HostIncarnation = "another.test" },
		"attempt":   func(c *GuestReconnectChallenge) { c.AttemptID = "another.test" },
		"placement": func(c *GuestReconnectChallenge) { c.PlacementGeneration++ },
		"challenge": func(c *GuestReconnectChallenge) {
			c.Challenge = base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("x", 32)))
		},
		"protocol": func(c *GuestReconnectChallenge) { c.Protocol = 2 },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			c := original
			mutate(&c)
			if c.VerifyProof(key, signature) == nil {
				t.Fatal("accepted a proof for another context")
			}
		})
	}
	_, other, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if original.VerifyProof(base64.RawURLEncoding.EncodeToString(other.Public().(ed25519.PublicKey)), signature) == nil {
		t.Fatal("accepted another guest key")
	}
}

func TestGuestReconnectRejectsMalformedInputs(t *testing.T) {
	for name, mutate := range map[string]func(*GuestReconnectChallenge){
		"empty session":          func(c *GuestReconnectChallenge) { c.SessionID = "" },
		"oversized id":           func(c *GuestReconnectChallenge) { c.AttemptID = strings.Repeat("x", 257) },
		"control character":      func(c *GuestReconnectChallenge) { c.BootEpoch = "boot\nsecret" },
		"zero placement":         func(c *GuestReconnectChallenge) { c.PlacementGeneration = 0 },
		"short challenge":        func(c *GuestReconnectChallenge) { c.Challenge = "AAAA" },
		"noncanonical challenge": func(c *GuestReconnectChallenge) { c.Challenge += "=" },
	} {
		t.Run(name, func(t *testing.T) {
			c := reconnectChallenge()
			mutate(&c)
			if _, err := c.SigningMessage(); err == nil {
				t.Fatal("accepted malformed context")
			}
		})
	}
	c := reconnectChallenge()
	for _, key := range []string{"", "secret.test", strings.Repeat("A", 43)} {
		for _, sig := range []string{"", "secret.test", strings.Repeat("A", 86)} {
			if err := c.VerifyProof(key, sig); err == nil {
				t.Fatal("accepted invalid proof")
			} else if strings.Contains(err.Error(), "secret.test") {
				t.Fatal("error discloses input")
			}
		}
	}
}

// internal/controld/seal_test.go
package controld

import (
	"bytes"
	"strings"
	"testing"
)

// testSecretsKeyHex is the fixed 32-byte key every controld test's Config
// carries. New() requires a non-zero SecretsKey (fail closed), so every
// Config construction in this package's tests — and in the cli and e2e
// suites, which have their own copies — has to supply one.
const testSecretsKeyHex = "0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0"

// testSecretsKey is testSecretsKeyHex parsed once, for test Configs.
var testSecretsKey = mustSecretsKey(testSecretsKeyHex)

func mustSecretsKey(hexKey string) [32]byte {
	k, err := ParseSecretsKey(hexKey)
	if err != nil {
		panic("controld test: bad test secrets key: " + err.Error())
	}
	return k
}

// ---------------------------------------------------------------------------
// ParseSecretsKey
// ---------------------------------------------------------------------------

func TestParseSecretsKey(t *testing.T) {
	t.Run("64 hex characters parse to the 32 bytes they encode", func(t *testing.T) {
		key, err := ParseSecretsKey(testSecretsKeyHex)
		if err != nil {
			t.Fatalf("ParseSecretsKey: %v", err)
		}
		want := [32]byte{0x0f, 0x1e, 0x2d, 0x3c, 0x4b, 0x5a, 0x69, 0x78, 0x87, 0x96, 0xa5, 0xb4, 0xc3, 0xd2, 0xe1, 0xf0,
			0x0f, 0x1e, 0x2d, 0x3c, 0x4b, 0x5a, 0x69, 0x78, 0x87, 0x96, 0xa5, 0xb4, 0xc3, 0xd2, 0xe1, 0xf0}
		if key != want {
			t.Fatalf("key = %x, want %x", key, want)
		}
	})

	t.Run("uppercase hex is accepted", func(t *testing.T) {
		lower, err := ParseSecretsKey(testSecretsKeyHex)
		if err != nil {
			t.Fatalf("ParseSecretsKey(lower): %v", err)
		}
		upper, err := ParseSecretsKey(strings.ToUpper(testSecretsKeyHex))
		if err != nil {
			t.Fatalf("ParseSecretsKey(upper): %v", err)
		}
		if lower != upper {
			t.Fatalf("upper-case hex parsed differently: %x vs %x", upper, lower)
		}
	})

	t.Run("rejects anything that isn't exactly 64 hex characters", func(t *testing.T) {
		cases := map[string]string{
			"empty":                 "",
			"short (62 chars)":      testSecretsKeyHex[:62],
			"long (66 chars)":       testSecretsKeyHex + "ab",
			"odd length (63)":       testSecretsKeyHex[:63],
			"non-hex at 64 chars":   strings.Repeat("z", 64),
			"0x prefix":             "0x" + testSecretsKeyHex[2:],
			"trailing whitespace":   testSecretsKeyHex[:63] + " ",
			"all zeros (unset key)": strings.Repeat("0", 64),
		}
		for name, in := range cases {
			if _, err := ParseSecretsKey(in); err == nil {
				t.Errorf("ParseSecretsKey(%s): want error, got nil", name)
			}
		}
	})

	// The key is the one thing that must never reach a log line or an
	// operator's terminal scrollback, and ParseSecretsKey's errors are
	// printed verbatim at startup.
	t.Run("the error never echoes the key material", func(t *testing.T) {
		almost := testSecretsKeyHex + "ff" // valid hex, wrong length
		_, err := ParseSecretsKey(almost)
		if err == nil {
			t.Fatal("want error for an over-long key")
		}
		if strings.Contains(err.Error(), testSecretsKeyHex[:16]) {
			t.Fatalf("error echoed key material: %v", err)
		}
	})

	t.Run("the error names RAINIER_SECRETS_KEY so an operator knows what to fix", func(t *testing.T) {
		_, err := ParseSecretsKey("nope")
		if err == nil {
			t.Fatal("want error")
		}
		if !strings.Contains(err.Error(), "RAINIER_SECRETS_KEY") {
			t.Errorf("error = %q, want it to name RAINIER_SECRETS_KEY", err)
		}
	})
}

// ---------------------------------------------------------------------------
// Seal / Open
// ---------------------------------------------------------------------------

func TestSealOpenRoundTrip(t *testing.T) {
	plaintexts := map[string][]byte{
		"ascii":         []byte("ghp_averyrealisticlookingtokenvalue"),
		"empty":         {},
		"binary":        {0x00, 0xff, 0x7f, 0x80, 0x00},
		"multi-line":    []byte("-----BEGIN KEY-----\nabc\ndef\n-----END KEY-----\n"),
		"64KB (at cap)": bytes.Repeat([]byte("x"), 64<<10),
	}
	for name, pt := range plaintexts {
		ct, nonce, err := Seal(testSecretsKey, pt)
		if err != nil {
			t.Fatalf("Seal(%s): %v", name, err)
		}
		if len(nonce) != 12 {
			t.Errorf("Seal(%s): nonce is %d bytes, want 12", name, len(nonce))
		}
		if bytes.Equal(ct, pt) && len(pt) > 0 {
			t.Errorf("Seal(%s): ciphertext equals plaintext", name)
		}
		if len(ct) <= len(pt) {
			t.Errorf("Seal(%s): ciphertext (%d) is not longer than plaintext (%d) — no GCM tag?", name, len(ct), len(pt))
		}

		got, err := Open(testSecretsKey, ct, nonce)
		if err != nil {
			t.Fatalf("Open(%s): %v", name, err)
		}
		if !bytes.Equal(got, pt) {
			t.Errorf("Open(%s) = %q, want %q", name, got, pt)
		}
	}
}

func TestSealUsesAFreshNonceEveryCall(t *testing.T) {
	const pt = "the same plaintext, twice"
	ct1, n1, err := Seal(testSecretsKey, []byte(pt))
	if err != nil {
		t.Fatalf("Seal (1): %v", err)
	}
	ct2, n2, err := Seal(testSecretsKey, []byte(pt))
	if err != nil {
		t.Fatalf("Seal (2): %v", err)
	}
	// A repeated nonce under one AES-GCM key is catastrophic (it leaks the
	// XOR of the plaintexts and the authentication subkey), so this is the
	// property that matters, not merely that the ciphertexts differ.
	if bytes.Equal(n1, n2) {
		t.Fatalf("two Seals reused nonce %x", n1)
	}
	if bytes.Equal(ct1, ct2) {
		t.Fatalf("two Seals of the same plaintext produced identical ciphertext %x", ct1)
	}
	if len(n1) != 12 || len(n2) != 12 {
		t.Fatalf("nonce lengths = %d, %d, want 12", len(n1), len(n2))
	}
}

func TestOpenRejectsTamperedCiphertext(t *testing.T) {
	ct, nonce, err := Seal(testSecretsKey, []byte("original value"))
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	t.Run("flipped ciphertext bit", func(t *testing.T) {
		bad := bytes.Clone(ct)
		bad[0] ^= 0x01
		if got, err := Open(testSecretsKey, bad, nonce); err == nil {
			t.Fatalf("Open of tampered ciphertext = %q, want an authentication error", got)
		}
	})

	t.Run("flipped tag bit", func(t *testing.T) {
		bad := bytes.Clone(ct)
		bad[len(bad)-1] ^= 0x01
		if got, err := Open(testSecretsKey, bad, nonce); err == nil {
			t.Fatalf("Open of tampered tag = %q, want an authentication error", got)
		}
	})

	t.Run("truncated ciphertext", func(t *testing.T) {
		if got, err := Open(testSecretsKey, ct[:len(ct)-1], nonce); err == nil {
			t.Fatalf("Open of truncated ciphertext = %q, want an authentication error", got)
		}
	})
}

func TestOpenRejectsTamperedNonce(t *testing.T) {
	ct, nonce, err := Seal(testSecretsKey, []byte("original value"))
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	t.Run("flipped nonce bit", func(t *testing.T) {
		bad := bytes.Clone(nonce)
		bad[0] ^= 0x01
		if got, err := Open(testSecretsKey, ct, bad); err == nil {
			t.Fatalf("Open with a tampered nonce = %q, want an authentication error", got)
		}
	})

	t.Run("wrong-length nonce", func(t *testing.T) {
		if got, err := Open(testSecretsKey, ct, nonce[:11]); err == nil {
			t.Fatalf("Open with an 11-byte nonce = %q, want an error", got)
		}
		if got, err := Open(testSecretsKey, ct, append(bytes.Clone(nonce), 0x00)); err == nil {
			t.Fatalf("Open with a 13-byte nonce = %q, want an error", got)
		}
	})
}

func TestOpenRejectsTheWrongKey(t *testing.T) {
	ct, nonce, err := Seal(testSecretsKey, []byte("original value"))
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	other := testSecretsKey
	other[31] ^= 0x01

	got, err := Open(other, ct, nonce)
	if err == nil {
		t.Fatalf("Open under a different key = %q, want an authentication error", got)
	}
	// The failure is reported without saying anything about the value or the
	// key — this error text reaches logs.
	if strings.Contains(err.Error(), "original value") {
		t.Fatalf("Open error leaked plaintext: %v", err)
	}
}

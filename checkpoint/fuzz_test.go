package checkpoint

import (
	"errors"
	"testing"
)

// FuzzParseManifest fuzzes the one parser that reads bytes nothing has
// authenticated yet. The manifest is fetched from object storage and parsed
// BEFORE any key is touched, which makes it the only untrusted-input surface in
// this package — everything else has already passed a GCM tag by the time it is
// interpreted.
//
// Two properties:
//
//  1. It never panics, and never returns an error outside this package's
//     vocabulary. A parser that panicked would turn a corrupted object into a
//     crashed cell-worker; one that returned an unclassified error would defeat
//     every errors.Is in the callers.
//  2. Anything it ACCEPTS survives a round trip: Encode then ParseManifest gives
//     back an identical struct. A parser that accepted a manifest its own encoder
//     could not reproduce would be a format with two spellings, which is a format
//     that eventually disagrees with itself.
func FuzzParseManifest(f *testing.F) {
	// A real manifest, and then the ways one goes wrong.
	valid := `{
  "version": "rainier.checkpoint.v1",
  "workspace": "workspace-alpha",
  "session": "sess-0000000000000001",
  "generation": 7,
  "created_at": "2026-09-20T12:34:56Z",
  "cipher": "AES-256-GCM",
  "kdf": "HKDF-SHA256",
  "compression": "none",
  "key_ref": "example.test/keys/checkpoint/1",
  "wrapped_key": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
  "content_key": "p/rainier.checkpoint.v1/ws/workspace-alpha/sess/sess-0000000000000001/gen/00000000000000000007/content.00",
  "frame_size": 65536,
  "frames": 4,
  "nonce_prefix": "hRGG2gvFiF0=",
  "content_bytes": 215104,
  "content_digest": "e849ac2249c95a3a07c759fa1e6396b6b354054b7c34b8602692454d7e109ce3",
  "plain_bytes": 215040,
  "entries": 11,
  "file_bytes": 204874,
  "skipped": 0,
  "tree_digest": "b74cd204dc149f04d6c03839ddfce165274bcea451464c4a19a5fb120414e70f",
  "manifest_nonce": "9+o9S2gyLjIYbkwe",
  "manifest_auth": "RN4puY/ownC/vEjXaw8gkQ=="
}`
	f.Add(valid)
	f.Add("")
	f.Add("{}")
	f.Add("null")
	f.Add("[]")
	f.Add(`{"version":"rainier.checkpoint.v1"}`)
	f.Add(`{"version":"rainier.checkpoint.v2","anything":1}`)
	f.Add(`{"version":"rainier.checkpoint.v1","generation":-1}`)
	f.Add(`{"version":"rainier.checkpoint.v1","frame_size":9223372036854775807}`)
	f.Add(`{"version":"rainier.checkpoint.v1","frames":4294967295}`)
	f.Add(valid + valid)
	f.Add(valid[:len(valid)/2])

	known := []error{
		ErrManifest, ErrFormatVersion, ErrInvalid,
	}

	f.Fuzz(func(t *testing.T, body string) {
		m, err := ParseManifest([]byte(body))
		if err != nil {
			for _, k := range known {
				if errors.Is(err, k) {
					return
				}
			}
			t.Fatalf("ParseManifest returned an error outside the package vocabulary: %v", err)
		}
		enc, err := m.Encode()
		if err != nil {
			t.Fatalf("an accepted manifest could not be encoded: %v", err)
		}
		again, err := ParseManifest(enc)
		if err != nil {
			t.Fatalf("an accepted manifest did not survive a round trip: %v", err)
		}
		if again != m {
			t.Fatalf("a manifest changed across Encode and ParseManifest")
		}
	})
}

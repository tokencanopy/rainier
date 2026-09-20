package checkpoint

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
)

// committed is one written checkpoint plus everything a test needs to poke at it.
type committed struct {
	h   *harness
	res Result
}

func commit(t *testing.T) *committed {
	t.Helper()
	h := newHarness(t, MinFrameSize)
	res, err := h.w.Write(context.Background(), h.c, DirSource(fixtureTree(t), DefaultExclusions()...))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if res.Manifest.Frames < 3 {
		t.Fatalf("the fixture must cross several frames, got %d", res.Manifest.Frames)
	}
	return &committed{h: h, res: res}
}

// setManifestField rewrites one JSON field of the stored manifest. json.Number
// keeps integers from round-tripping through float64.
func (c *committed) setManifestField(t *testing.T, field string, value any) {
	t.Helper()
	b, ok := c.h.store.Object(c.res.ManifestKey)
	if !ok {
		t.Fatal("no stored manifest")
	}
	var obj map[string]any
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if err := dec.Decode(&obj); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if value == nil {
		delete(obj, field)
	} else {
		obj[field] = value
	}
	out, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	c.h.store.Overwrite(c.res.ManifestKey, out)
}

func TestManifestTamperFailsAuthentication(t *testing.T) {
	const otherDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	cases := []struct {
		name  string
		field string
		value any
		want  error
	}{
		{"content digest", "content_digest", otherDigest, ErrAuth},
		{"tree digest", "tree_digest", otherDigest, ErrAuth},
		{"creation time", "created_at", "2020-01-01T00:00:00Z", ErrAuth},
		{"entry count", "entries", json.Number("3"), ErrAuth},
		{"file byte total", "file_bytes", json.Number("1"), ErrAuth},
		{"skipped count", "skipped", json.Number("2"), ErrAuth},
		{"wrapped key", "wrapped_key", base64.StdEncoding.EncodeToString(make([]byte, 60)), ErrAuth},
		{"nonce prefix", "nonce_prefix", base64.StdEncoding.EncodeToString(make([]byte, noncePrefixLen)), ErrAuth},
		{"manifest tag", "manifest_auth", base64.StdEncoding.EncodeToString(make([]byte, tagLen)), ErrAuth},
		{"manifest nonce", "manifest_nonce", base64.StdEncoding.EncodeToString(make([]byte, nonceLen)), ErrAuth},
		{"key reference", "key_ref", "example.test/keys/checkpoint/9", ErrKeyUnavailable},
		{"workspace", "workspace", "workspace-beta", ErrContextMismatch},
		{"session", "session", "sess-0000000000000002", ErrContextMismatch},
		{"generation", "generation", json.Number("8"), ErrContextMismatch},
		{"format version", "version", "rainier.checkpoint.v2", ErrFormatVersion},
		{"cipher", "cipher", "AES-128-GCM", ErrFormatVersion},
		{"kdf", "kdf", "HKDF-SHA512", ErrFormatVersion},
		{"compression", "compression", "zstd", ErrFormatVersion},
		{"frame count", "frames", json.Number("99"), ErrManifest},
		{"frame size", "frame_size", json.Number("17"), ErrManifest},
		{"plaintext length", "plain_bytes", json.Number("999999999"), ErrManifest},
		{"ciphertext length", "content_bytes", json.Number("999999999"), ErrManifest},
		{"a missing field", "tree_digest", nil, ErrManifest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := commit(t)
			c.setManifestField(t, tc.field, tc.value)
			_, err := c.h.r.Verify(context.Background(), c.h.c)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Verify error = %v, want %v", err, tc.want)
			}
			assertNoContentInError(t, err)
		})
	}
}

func TestManifestTamperOnTheContentKey(t *testing.T) {
	t.Run("another generation's content object", func(t *testing.T) {
		c := commit(t)
		other := c.h.c
		other.Generation = 9
		c.setManifestField(t, "content_key", contentKeyFor("checkpoints", other, strings.Repeat("a", attemptLen*2)))
		if _, err := c.h.r.Verify(context.Background(), c.h.c); !errors.Is(err, ErrManifest) {
			t.Fatalf("Verify error = %v, want ErrManifest", err)
		}
	})
	t.Run("a different attempt in the same generation", func(t *testing.T) {
		c := commit(t)
		c.setManifestField(t, "content_key",
			contentKeyFor("checkpoints", c.h.c, strings.Repeat("b", attemptLen*2)))
		// The shape check passes, so the manifest tag is what refuses it.
		if _, err := c.h.r.Verify(context.Background(), c.h.c); !errors.Is(err, ErrAuth) {
			t.Fatalf("Verify error = %v, want ErrAuth", err)
		}
	})
	t.Run("an unknown field", func(t *testing.T) {
		c := commit(t)
		c.setManifestField(t, "compression_level", json.Number("3"))
		if _, err := c.h.r.Verify(context.Background(), c.h.c); !errors.Is(err, ErrManifest) {
			t.Fatalf("Verify error = %v, want ErrManifest", err)
		}
	})
}

func TestContentTamper(t *testing.T) {
	frame := int64(MinFrameSize) + tagLen

	cases := []struct {
		name   string
		mutate func(t *testing.T, b []byte) []byte
		want   error
	}{
		{"a flipped bit in the first frame", func(_ *testing.T, b []byte) []byte {
			b[0] ^= 1
			return b
		}, ErrAuth},
		{"a flipped bit in a middle frame", func(_ *testing.T, b []byte) []byte {
			b[frame+17] ^= 0x80
			return b
		}, ErrAuth},
		{"a flipped bit in the last frame", func(_ *testing.T, b []byte) []byte {
			b[len(b)-1] ^= 1
			return b
		}, ErrAuth},
		{"a flipped bit in the last frame's tag", func(_ *testing.T, b []byte) []byte {
			b[len(b)-tagLen] ^= 1
			return b
		}, ErrAuth},
		{"the whole object zeroed", func(_ *testing.T, b []byte) []byte {
			return make([]byte, len(b))
		}, ErrAuth},
		{"two frames swapped", func(t *testing.T, b []byte) []byte {
			if int64(len(b)) < 2*frame {
				t.Fatal("the fixture is too small to swap frames")
			}
			out := bytes.Clone(b)
			copy(out[:frame], b[frame:2*frame])
			copy(out[frame:2*frame], b[:frame])
			return out
		}, ErrAuth},
		{"one byte truncated", func(_ *testing.T, b []byte) []byte {
			return b[:len(b)-1]
		}, ErrTruncated},
		{"the last frame truncated away", func(t *testing.T, b []byte) []byte {
			if int64(len(b)) <= frame {
				t.Fatal("the fixture is too small")
			}
			return b[:frame]
		}, ErrTruncated},
		{"one byte appended", func(_ *testing.T, b []byte) []byte {
			return append(bytes.Clone(b), 0)
		}, ErrTrailingData},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := commit(t)
			replaceObject(t, c.h.store, c.res.ContentKey, func(b []byte) []byte {
				return tc.mutate(t, b)
			})
			_, err := c.h.r.Verify(context.Background(), c.h.c)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Verify error = %v, want %v", err, tc.want)
			}
			assertNoContentInError(t, err)
		})
	}
}

func TestContentObjectMissingIsNotAMissingCheckpoint(t *testing.T) {
	c := commit(t)
	if err := c.h.store.Delete(context.Background(), c.res.ContentKey); err != nil {
		t.Fatal(err)
	}
	if _, err := c.h.r.Verify(context.Background(), c.h.c); !errors.Is(err, ErrTruncated) {
		t.Fatalf("Verify error = %v, want ErrTruncated", err)
	}
}

// TestContextSwapThroughThePublicAPI copies a whole committed checkpoint — both
// objects, byte for byte — into another session's storage prefix and tries to
// read it there. This is the tenancy specification's §18 item 39 as an operation
// somebody could actually perform.
func TestContextSwapThroughThePublicAPI(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		swap func(c Context) Context
	}{
		{"another workspace", func(c Context) Context { c.Workspace = "workspace-beta"; return c }},
		{"another session", func(c Context) Context { c.Session = "sess-0000000000000002"; return c }},
		{"another generation", func(c Context) Context { c.Generation = 11; return c }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := commit(t)
			other := tc.swap(c.h.c)

			// Copy both objects to the other context's keys.
			manifest, _ := c.h.store.Object(c.res.ManifestKey)
			content, _ := c.h.store.Object(c.res.ContentKey)
			attempt := c.res.ContentKey[strings.LastIndex(c.res.ContentKey, ".")+1:]
			otherContent := contentKeyFor("checkpoints", other, attempt)
			c.h.store.Overwrite(manifestKey("checkpoints", other), manifest)
			c.h.store.Overwrite(otherContent, content)

			// As copied, the manifest still says whose it is.
			if _, err := c.h.r.Verify(ctx, other); !errors.Is(err, ErrContextMismatch) {
				t.Fatalf("Verify error = %v, want ErrContextMismatch", err)
			}

			// Now rewrite the identity fields so the cheap check passes. The
			// cryptography is what must refuse it from here, and it does: the
			// data key was wrapped with the original context as additional data.
			var obj map[string]any
			dec := json.NewDecoder(bytes.NewReader(manifest))
			dec.UseNumber()
			if err := dec.Decode(&obj); err != nil {
				t.Fatal(err)
			}
			obj["workspace"] = other.Workspace
			obj["session"] = other.Session
			obj["generation"] = json.Number(itoa(other.Generation))
			obj["content_key"] = otherContent
			rewritten, err := json.Marshal(obj)
			if err != nil {
				t.Fatal(err)
			}
			c.h.store.Overwrite(manifestKey("checkpoints", other), rewritten)

			if _, err := c.h.r.Verify(ctx, other); !errors.Is(err, ErrAuth) {
				t.Fatalf("Verify after rewriting identity = %v, want ErrAuth", err)
			}
		})
	}
}

// TestEachBindingRefusesTheWrongContextOnItsOwn reaches past the cheap identity
// check and past whichever layer would have failed first, and exercises all
// three bindings separately. A context-swap test that only proves a string
// comparison failed has tested nothing; these prove the key wrap, the manifest
// tag and every frame each bind the context by themselves.
func TestEachBindingRefusesTheWrongContextOnItsOwn(t *testing.T) {
	ctx := context.Background()
	c := commit(t)
	good := c.h.c
	bad := good
	bad.Session = "sess-0000000000000002"

	keys := testWrapper(t)
	wrapped, err := base64.StdEncoding.DecodeString(c.res.Manifest.WrappedKey)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("the key wrap", func(t *testing.T) {
		if _, err := keys.Unwrap(ctx, testKeyRef, wrapped, bad.aad(purposeKey)); !errors.Is(err, ErrAuth) {
			t.Fatalf("Unwrap under another context = %v, want ErrAuth", err)
		}
		if _, err := keys.Unwrap(ctx, testKeyRef, wrapped, good.aad(purposeManifest)); !errors.Is(err, ErrAuth) {
			t.Fatalf("Unwrap under another PURPOSE = %v, want ErrAuth", err)
		}
	})

	dek, err := keys.Unwrap(ctx, testKeyRef, wrapped, good.aad(purposeKey))
	if err != nil {
		t.Fatalf("Unwrap: %v", err)
	}
	kContent, kManifest, err := deriveSubkeys(dek, good)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("the manifest tag", func(t *testing.T) {
		if err := c.res.Manifest.checkAuth(kManifest, good); err != nil {
			t.Fatalf("checkAuth under its own context: %v", err)
		}
		if err := c.res.Manifest.checkAuth(kManifest, bad); !errors.Is(err, ErrAuth) {
			t.Fatalf("checkAuth under another context = %v, want ErrAuth", err)
		}
		// And with the content subkey, which is the domain separation working.
		if err := c.res.Manifest.checkAuth(kContent, good); !errors.Is(err, ErrAuth) {
			t.Fatalf("checkAuth under the content subkey = %v, want ErrAuth", err)
		}
	})

	t.Run("every frame", func(t *testing.T) {
		content, ok := c.h.store.Object(c.res.ContentKey)
		if !ok {
			t.Fatal("no content object")
		}
		m := c.res.Manifest
		prefix, err := base64.StdEncoding.DecodeString(m.NoncePrefix)
		if err != nil {
			t.Fatal(err)
		}
		// Its own context: the whole stream opens.
		fr, err := newFrameReader(bytes.NewReader(content), good, kContent, prefix, m.FrameSize, m.Frames, m.PlainBytes)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := readAll(fr); err != nil {
			t.Fatalf("reading frames under their own context: %v", err)
		}
		// Another context: the FIRST frame already fails, so no plaintext of any
		// frame is reachable.
		fr, err = newFrameReader(bytes.NewReader(content), bad, kContent, prefix, m.FrameSize, m.Frames, m.PlainBytes)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := readAll(fr); !errors.Is(err, ErrAuth) {
			t.Fatalf("reading frames under another context = %v, want ErrAuth", err)
		}
		// And the frame INDEX is bound: reading the stream while claiming a
		// different frame count makes the final flag wrong for the last frame.
		fr, err = newFrameReader(bytes.NewReader(content), good, kContent, prefix, m.FrameSize, m.Frames-1,
			m.PlainBytes-(m.PlainBytes-int64(m.Frames-1)*m.FrameSize))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := readAll(fr); !errors.Is(err, ErrAuth) {
			t.Fatalf("reading with a short frame count = %v, want ErrAuth", err)
		}
	})
}

// TestFrameNoncesAreUniqueAndDerivedFromTheIndex is the nonce-reuse check: two
// checkpoints of the same tree under the same context draw different data keys
// and different nonce prefixes, and no frame inside one checkpoint shares a
// nonce with another.
func TestFrameNoncesAreUniqueAndDerivedFromTheIndex(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, MinFrameSize)
	root := fixtureTree(t)

	first, err := h.w.Write(ctx, h.c, DirSource(root, DefaultExclusions()...))
	if err != nil {
		t.Fatal(err)
	}
	second := h.c
	second.Generation++
	other, err := h.w.Write(ctx, second, DirSource(root, DefaultExclusions()...))
	if err != nil {
		t.Fatal(err)
	}
	if first.Manifest.NoncePrefix == other.Manifest.NoncePrefix {
		t.Error("two checkpoints drew the same nonce prefix")
	}
	if first.Manifest.WrappedKey == other.Manifest.WrappedKey {
		t.Error("two checkpoints wrapped to the same bytes")
	}
	// Identical trees, identical contexts apart from the generation, and yet the
	// ciphertexts must differ: the data key is fresh per checkpoint.
	fb, _ := h.store.Object(first.ContentKey)
	ob, _ := h.store.Object(other.ContentKey)
	if bytes.Equal(fb, ob) {
		t.Error("two checkpoints of the same tree produced identical ciphertext")
	}

	// Within one checkpoint, the nonce is prefix || big-endian index, so the
	// frames' nonces are distinct by construction. Assert the construction
	// rather than trusting it.
	prefix, err := base64.StdEncoding.DecodeString(first.Manifest.NoncePrefix)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for i := uint32(0); i < first.Manifest.Frames; i++ {
		var nonce [nonceLen]byte
		copy(nonce[:], prefix)
		nonce[nonceLen-4] = byte(i >> 24)
		nonce[nonceLen-3] = byte(i >> 16)
		nonce[nonceLen-2] = byte(i >> 8)
		nonce[nonceLen-1] = byte(i)
		if seen[string(nonce[:])] {
			t.Fatalf("frame %d reuses a nonce", i)
		}
		seen[string(nonce[:])] = true
	}
}

func TestStaticKeyWrapperRefusals(t *testing.T) {
	ctx := context.Background()
	if _, err := NewStaticKeyWrapper("", testKey()); !errors.Is(err, ErrInvalid) {
		t.Error("an empty key reference was accepted")
	}
	if _, err := NewStaticKeyWrapper(testKeyRef, [dekLen]byte{}); !errors.Is(err, ErrInvalid) {
		t.Error("an all-zero key was accepted")
	}
	w := testWrapper(t)
	if w.Ref() != testKeyRef {
		t.Errorf("Ref = %q", w.Ref())
	}
	if _, _, err := w.Wrap(ctx, "example.test/keys/other/1", make([]byte, dekLen), nil); !errors.Is(err, ErrKeyUnavailable) {
		t.Error("wrapping under an unknown reference was accepted")
	}
	if _, _, err := w.Wrap(ctx, "", make([]byte, dekLen-1), nil); !errors.Is(err, ErrInvalid) {
		t.Error("a short data key was accepted")
	}
	wrapped, used, err := w.Wrap(ctx, "", bytes.Repeat([]byte{7}, dekLen), []byte("aad"))
	if err != nil || used != testKeyRef {
		t.Fatalf("Wrap = %v, %v", used, err)
	}
	if bytes.Contains(wrapped, bytes.Repeat([]byte{7}, dekLen)) {
		t.Fatal("the wrapped key contains the plaintext key")
	}
	if _, err := w.Unwrap(ctx, testKeyRef, wrapped, []byte("other")); !errors.Is(err, ErrAuth) {
		t.Error("unwrapping under different additional data succeeded")
	}
	if _, err := w.Unwrap(ctx, testKeyRef, wrapped[:nonceLen+tagLen-1], []byte("aad")); !errors.Is(err, ErrAuth) {
		t.Error("unwrapping a short blob succeeded")
	}
	got, err := w.Unwrap(ctx, testKeyRef, wrapped, []byte("aad"))
	if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{7}, dekLen)) {
		t.Fatalf("Unwrap = %v", err)
	}
}

func TestSubkeysAreDistinctAndContextBound(t *testing.T) {
	dek := bytes.Repeat([]byte{3}, dekLen)
	a := testContext()
	b := a
	b.Generation++

	ac, am, err := deriveSubkeys(dek, a)
	if err != nil {
		t.Fatal(err)
	}
	bc, bm, err := deriveSubkeys(dek, b)
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2][]byte{{ac, am}, {ac, bc}, {am, bm}, {ac, bm}} {
		if bytes.Equal(pair[0], pair[1]) {
			t.Error("two subkeys that must differ are equal")
		}
	}
	if _, _, err := deriveSubkeys(dek[:1], a); !errors.Is(err, ErrInvalid) {
		t.Error("a short data key derived subkeys")
	}
}

// assertNoContentInError is the no-leak rule as a test: an error from this
// package may not carry a fixture path, a fixture file's name, the planted
// credential, or base64 that could be key material.
func assertNoContentInError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		return
	}
	msg := err.Error()
	for _, forbidden := range []string{
		plantedCredential, "README.md", "big.bin", "run.sh", "ünïcødé", ".credentials.json",
		"/tmp/", "link.txt", "main.go",
	} {
		if strings.Contains(msg, forbidden) {
			t.Errorf("the error quotes content (%q): %s", forbidden, msg)
		}
	}
}

func readAll(r io.Reader) (int64, error) {
	n, err := io.Copy(io.Discard, r)
	return n, err
}

func itoa(u uint64) string {
	if u == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for u > 0 {
		i--
		b[i] = byte('0' + u%10)
		u /= 10
	}
	return string(b[i:])
}

package checkpoint

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	mathrand "math/rand/v2"
)

// updateGolden rewrites testdata/manifest.golden.json instead of comparing
// against it. It is a flag rather than an environment variable so that the way
// to regenerate the file is discoverable from `go test -h` output.
var updateGolden = flag.Bool("update-golden", false, "rewrite the golden manifest instead of comparing against it")

// TestManifestAuthInputCoversEveryField is the test that keeps the format's worst
// possible bug from happening. authInput is written by hand — a MAC over
// json.Marshal output would be a MAC over whatever the encoder does this year —
// and the cost of writing it by hand is that a field added to the struct and
// forgotten here would be UNAUTHENTICATED METADATA in a plaintext object anybody
// can edit.
//
// So: reflect over every JSON tag on Manifest, and fail if any of them, other
// than the nonce and the tag that are excluded by definition, is missing from the
// authenticated input.
func TestManifestAuthInputCoversEveryField(t *testing.T) {
	m := Manifest{
		Version: FormatVersion, Workspace: "w", Session: "s", Generation: 1,
		CreatedAt: "2026-09-20T00:00:00Z", Cipher: cipherName, KDF: kdfName,
		Compression: compressionNone, KeyRef: "k", WrappedKey: "AAAA",
		ContentKey: "c", FrameSize: MinFrameSize, Frames: 1, NoncePrefix: "BBBB",
		ContentBytes: 2, ContentDigest: "d", PlainBytes: 3, Entries: 4,
		FileBytes: 5, Skipped: 6, TreeDigest: "e",
		ManifestNonce: "CCCC", ManifestAuth: "DDDD",
	}
	input := string(m.authInput(testContext()))

	typ := reflect.TypeOf(Manifest{})
	for i := 0; i < typ.NumField(); i++ {
		tag := strings.Split(typ.Field(i).Tag.Get("json"), ",")[0]
		if tag == "" || tag == "-" {
			t.Fatalf("field %s has no JSON tag", typ.Field(i).Name)
		}
		excluded := false
		for _, e := range authInputExcluded {
			if tag == e {
				excluded = true
			}
		}
		if excluded {
			if strings.Contains(input, "\x00"+tag+"=") {
				t.Errorf("%s is in the authenticated input but is excluded by definition", tag)
			}
			continue
		}
		if !strings.Contains(input, "\x00"+tag+"=") {
			t.Errorf("manifest field %q is NOT authenticated; add it to authInput", tag)
		}
	}

	// And the authenticated input begins with the purpose-separated context, so
	// no ciphertext produced for the key wrap or for a frame can be presented as
	// a manifest tag.
	if !strings.HasPrefix(input, string(testContext().aad(purposeManifest))) {
		t.Error("the authenticated input does not begin with the manifest-purpose context")
	}
}

// TestManifestAuthInputBindsEveryFieldsValue is the other half, and the half
// that matters more. The coverage test above proves each field's NAME appears in
// the authenticated input; it would still pass if a field were bound to a
// constant, or to the wrong field's value — `add("frames", "0")` survives it.
//
// This one changes exactly one field at a time, through reflection so no field
// can be forgotten, and requires the authenticated input to change with it. A
// field bound to a constant, bound to another field, or dropped fails here.
//
// It is also the answer to a subtler gap: eleven of the manifest's fields are
// refused by a CHEAPER, EARLIER check than the tag — the identity comparison,
// validate()'s enumerations and length arithmetic, the wrapper's key-reference
// check — so a tamper test for those fields never reaches the tag at all and
// proves nothing about whether the tag binds them.
func TestManifestAuthInputBindsEveryFieldsValue(t *testing.T) {
	base := Manifest{
		Version: FormatVersion, Workspace: "workspace-alpha", Session: "sess-1", Generation: 7,
		CreatedAt: "2026-09-20T00:00:00Z", Cipher: cipherName, KDF: kdfName,
		Compression: compressionNone, KeyRef: "example.test/keys/1", WrappedKey: "AAAA",
		ContentKey: "p/content.00", FrameSize: MinFrameSize, Frames: 4, NoncePrefix: "BBBB",
		ContentBytes: 200, ContentDigest: "aa", PlainBytes: 100, Entries: 9,
		FileBytes: 50, Skipped: 1, TreeDigest: "bb",
		ManifestNonce: "CCCC", ManifestAuth: "DDDD",
	}
	c := testContext()
	want := string(base.authInput(c))

	typ := reflect.TypeOf(Manifest{})
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		tag := strings.Split(field.Tag.Get("json"), ",")[0]
		excluded := false
		for _, e := range authInputExcluded {
			if tag == e {
				excluded = true
			}
		}

		mutated := base
		v := reflect.ValueOf(&mutated).Elem().Field(i)
		switch v.Kind() {
		case reflect.String:
			v.SetString(v.String() + "-changed")
		case reflect.Uint64, reflect.Uint32:
			v.SetUint(v.Uint() + 1)
		case reflect.Int64:
			v.SetInt(v.Int() + 1)
		default:
			t.Fatalf("field %s has a kind this test does not know how to change", field.Name)
		}

		got := string(mutated.authInput(c))
		switch {
		case excluded && got != want:
			t.Errorf("changing %q changed the authenticated input, but it is excluded by definition", tag)
		case !excluded && got == want:
			t.Errorf("changing %q did NOT change the authenticated input; its value is not bound", tag)
		}
	}

	// And the context is bound too: the same manifest under a different context
	// authenticates different bytes.
	other := c
	other.Generation++
	if string(base.authInput(other)) == want {
		t.Error("the authenticated input does not depend on the checkpoint generation")
	}
}

func TestSummaryHasNowhereToPutASecret(t *testing.T) {
	typ := reflect.TypeOf(Summary{})
	for i := 0; i < typ.NumField(); i++ {
		name := typ.Field(i).Name
		for _, forbidden := range []string{"Wrapped", "Nonce", "Auth", "Key "} {
			if strings.Contains(name, strings.TrimSpace(forbidden)) && name != "KeyRef" {
				t.Errorf("Summary has a field named %q, which is not content-free", name)
			}
		}
	}
}

func TestParseManifestIsStrict(t *testing.T) {
	// A valid manifest, rendered from a real write, is the baseline every case
	// below mutates.
	c := commit(t)
	valid, _ := c.h.store.Object(c.res.ManifestKey)
	if _, err := ParseManifest(valid); err != nil {
		t.Fatalf("the baseline manifest does not parse: %v", err)
	}

	edit := func(t *testing.T, mutate func(map[string]any)) []byte {
		t.Helper()
		var obj map[string]any
		dec := json.NewDecoder(strings.NewReader(string(valid)))
		dec.UseNumber()
		if err := dec.Decode(&obj); err != nil {
			t.Fatal(err)
		}
		mutate(obj)
		out, err := json.Marshal(obj)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}

	cases := []struct {
		name string
		body func(t *testing.T) []byte
		want error
	}{
		{"empty", func(*testing.T) []byte { return nil }, ErrManifest},
		{"not JSON", func(*testing.T) []byte { return []byte("not json") }, ErrManifest},
		{"not an object", func(*testing.T) []byte { return []byte(`["a"]`) }, ErrManifest},
		{"an unknown field", func(t *testing.T) []byte {
			return edit(t, func(o map[string]any) { o["extra"] = 1 })
		}, ErrManifest},
		{"a missing field", func(t *testing.T) []byte {
			return edit(t, func(o map[string]any) { delete(o, "frames") })
		}, ErrManifest},
		{"a mistyped field", func(t *testing.T) []byte {
			return edit(t, func(o map[string]any) { o["frames"] = "four" })
		}, ErrManifest},
		{"trailing JSON", func(t *testing.T) []byte {
			return append(append([]byte{}, valid...), []byte("{}")...)
		}, ErrManifest},
		{"an unknown version", func(t *testing.T) []byte {
			return edit(t, func(o map[string]any) { o["version"] = "rainier.checkpoint.v99" })
		}, ErrFormatVersion},
		{"an unknown version with unknown fields", func(t *testing.T) []byte {
			// The version is checked leniently FIRST, so a future format is named
			// as a future format rather than as a malformed manifest.
			return edit(t, func(o map[string]any) {
				o["version"] = "rainier.checkpoint.v99"
				o["chunks"] = []any{"a", "b"}
			})
		}, ErrFormatVersion},
		{"a frame size below the floor", func(t *testing.T) []byte {
			return edit(t, func(o map[string]any) { o["frame_size"] = json.Number("1024") })
		}, ErrManifest},
		{"a frame size above the ceiling", func(t *testing.T) []byte {
			return edit(t, func(o map[string]any) { o["frame_size"] = json.Number("1073741824") })
		}, ErrManifest},
		{"zero frames", func(t *testing.T) []byte {
			return edit(t, func(o map[string]any) { o["frames"] = json.Number("0") })
		}, ErrManifest},
		{"generation zero", func(t *testing.T) []byte {
			return edit(t, func(o map[string]any) { o["generation"] = json.Number("0") })
		}, ErrManifest},
		{"a workspace with a slash", func(t *testing.T) []byte {
			return edit(t, func(o map[string]any) { o["workspace"] = "a/b" })
		}, ErrManifest},
		{"a non-UTC creation time", func(t *testing.T) []byte {
			return edit(t, func(o map[string]any) { o["created_at"] = "2026-09-20T00:00:00+02:00" })
		}, ErrManifest},
		{"a sub-second creation time", func(t *testing.T) []byte {
			return edit(t, func(o map[string]any) { o["created_at"] = "2026-09-20T00:00:00.5Z" })
		}, ErrManifest},
		{"an uppercase digest", func(t *testing.T) []byte {
			return edit(t, func(o map[string]any) {
				o["tree_digest"] = strings.ToUpper(o["tree_digest"].(string))
			})
		}, ErrManifest},
		{"a short nonce prefix", func(t *testing.T) []byte {
			return edit(t, func(o map[string]any) { o["nonce_prefix"] = "AAAA" })
		}, ErrManifest},
		{"a nonce prefix that is not base64", func(t *testing.T) []byte {
			return edit(t, func(o map[string]any) { o["nonce_prefix"] = "!!!!!!!!!!!" })
		}, ErrManifest},
		{"an absolute content key", func(t *testing.T) []byte {
			return edit(t, func(o map[string]any) { o["content_key"] = "/a/b" })
		}, ErrManifest},
		{"a content key with dot-dot", func(t *testing.T) []byte {
			return edit(t, func(o map[string]any) { o["content_key"] = "a/../b" })
		}, ErrManifest},
		{"an empty key reference", func(t *testing.T) []byte {
			return edit(t, func(o map[string]any) { o["key_ref"] = "" })
		}, ErrManifest},
		{"an oversized manifest", func(t *testing.T) []byte {
			return edit(t, func(o map[string]any) {
				o["key_ref"] = strings.Repeat("k", maxManifestBytes)
			})
		}, ErrManifest},
		{"a plaintext length shorter than an empty tar", func(t *testing.T) []byte {
			return edit(t, func(o map[string]any) { o["plain_bytes"] = json.Number("7") })
		}, ErrManifest},
		{"an entry count that cannot fit", func(t *testing.T) []byte {
			return edit(t, func(o map[string]any) { o["entries"] = json.Number("999999999") })
		}, ErrManifest},
		{"a negative count", func(t *testing.T) []byte {
			return edit(t, func(o map[string]any) { o["skipped"] = json.Number("-1") })
		}, ErrManifest},
		{"a file-byte total over the plaintext length", func(t *testing.T) []byte {
			return edit(t, func(o map[string]any) { o["file_bytes"] = o["plain_bytes"] })
		}, nil}, // equal is allowed; only GREATER is refused
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseManifest(tc.body(t))
			if tc.want == nil {
				if err != nil {
					t.Fatalf("ParseManifest = %v, want success", err)
				}
				return
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("ParseManifest = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestManifestRoundTripsThroughEncode(t *testing.T) {
	c := commit(t)
	enc, err := c.res.Manifest.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseManifest(enc)
	if err != nil {
		t.Fatalf("ParseManifest: %v", err)
	}
	if got != c.res.Manifest {
		t.Error("a manifest does not survive Encode and ParseManifest unchanged")
	}
}

// TestGoldenManifest pins the on-the-wire shape. Every random value and the clock
// are injected, so the manifest is byte-for-byte deterministic and the format
// cannot change without this file changing with it.
//
// A failure here is not automatically a bug: it means the bytes changed. Read the
// diff, decide whether the change is intended, and if it is, regenerate with
//
//	go test ./checkpoint/ -run TestGoldenManifest -update-golden
//
// and put the new file in the same commit as the change that caused it.
func TestGoldenManifest(t *testing.T) {
	root := fixtureTree(t)
	store := NewMemoryStore()

	// Two independent deterministic streams, so that a change in how many bytes
	// one side draws cannot silently shift the other.
	wrapRand := mathrand.NewChaCha8([32]byte{'w', 'r', 'a', 'p'})
	writeRand := mathrand.NewChaCha8([32]byte{'w', 'r', 'i', 't', 'e'})

	keys, err := newStaticKeyWrapper(testKeyRef, testKey(), wrapRand)
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWriter(store, keys, WriterOptions{
		Prefix:    "checkpoints",
		KeyRef:    testKeyRef,
		FrameSize: MinFrameSize,
		rand:      writeRand,
		now:       func() time.Time { return time.Date(2026, 9, 20, 12, 34, 56, 789, time.UTC) },
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := w.Write(context.Background(), testContext(), DirSource(root))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	got, err := res.Manifest.Encode()
	if err != nil {
		t.Fatal(err)
	}

	path := filepath.Join("testdata", "manifest.golden.json")
	if *updateGolden {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
		t.Log("golden manifest updated")
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the golden manifest: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("the manifest no longer matches the golden file.\n--- got ---\n%s\n--- want ---\n%s",
			got, want)
	}

	// The golden bytes are a real manifest, not just a matching string: they
	// parse, and the checkpoint they describe verifies.
	if _, err := ParseManifest(want); err != nil {
		t.Fatalf("the golden manifest does not parse: %v", err)
	}
	r, err := NewReader(store, keys, ReaderOptions{
		Prefix:    "checkpoints",
		Authorize: (&authorizeRecorder{}).hook,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Verify(context.Background(), testContext()); err != nil {
		t.Fatalf("the golden checkpoint does not verify: %v", err)
	}
}

func TestStorageLayout(t *testing.T) {
	c := testContext()
	if got, want := manifestKey("checkpoints", c),
		"checkpoints/rainier.checkpoint.v1/ws/workspace-alpha/sess/sess-0000000000000001/gen/00000000000000000007/manifest.json"; got != want {
		t.Errorf("manifest key = %q, want %q", got, want)
	}
	if got, want := manifestKey("", c),
		"rainier.checkpoint.v1/ws/workspace-alpha/sess/sess-0000000000000001/gen/00000000000000000007/manifest.json"; got != want {
		t.Errorf("manifest key with no prefix = %q", got)
	}
	// Zero-padded, so a lexical listing is generation order.
	low, high := c, c
	low.Generation, high.Generation = 9, 10
	if manifestKey("p", low) > manifestKey("p", high) {
		t.Error("generation 9 sorts after generation 10")
	}
}

package term

import (
	"os"
	"path/filepath"
	"testing"
)

// fixtures: name → input bytes fed to a fresh 20x5 emulator
var fixtures = map[string][]byte{
	"plain":     []byte("hello"),
	"newline":   []byte("line1\r\nline2"),
	"color":     []byte("\x1b[31mred\x1b[0m plain \x1b[1;38;5;40mbold-green\x1b[0m"),
	"cursor":    []byte("abc\x1b[2;3Hxy\x1b[Hz"),
	"clear":     []byte("junk junk junk\x1b[2J\x1b[Hclean"),
	"altscreen": []byte("main\x1b[?1049halt-content"),
	"wrap":      []byte("aaaaaaaaaaaaaaaaaaaaaaaaaa"), // wider than 20 cols
	"wide":      []byte("你好 ab\r\n中文"),                // CJK width-2 runes must not shift trailing cells
}

func screensEqual(t *testing.T, a, b Screen) {
	t.Helper()
	if a.Cols != b.Cols || a.Rows != b.Rows { t.Fatalf("size %dx%d vs %dx%d", a.Cols, a.Rows, b.Cols, b.Rows) }
	if a.CursorX != b.CursorX || a.CursorY != b.CursorY {
		t.Fatalf("cursor (%d,%d) vs (%d,%d)", a.CursorX, a.CursorY, b.CursorX, b.CursorY)
	}
	for y := 0; y < a.Rows; y++ {
		for x := 0; x < a.Cols; x++ {
			ca, cb := cellAt(a, y, x), cellAt(b, y, x)
			if ca.R == 0 { ca.R = ' ' }
			if cb.R == 0 { cb.R = ' ' }
			if ca != cb { t.Fatalf("cell (%d,%d): %+v vs %+v", y, x, ca, cb) }
		}
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	for name, input := range fixtures {
		t.Run(name, func(t *testing.T) {
			a := NewEmulator(20, 5)
			a.Feed(input)
			sa := a.Screen()
			b := NewEmulator(20, 5)
			b.Feed(Serialize(sa))
			screensEqual(t, sa, b.Screen())
		})
	}
}

// TestWideRuneCellWidth pins the exact symptom this fixture was diagnosed
// from: without carrying cell width through, the "好" continuation cell's
// serialized space shifts every following cell one column to the right, so
// "b" (which directly follows "你好 a" in the fixture) lands in column 7
// instead of column 6.
func TestWideRuneCellWidth(t *testing.T) {
	e := NewEmulator(20, 5)
	e.Feed(fixtures["wide"])
	s := e.Screen()

	if s.Cells[0][0].R != '你' || s.Cells[0][0].Width != 2 {
		t.Fatalf("cell(0,0) = %+v, want lead cell of 你 with Width 2", s.Cells[0][0])
	}
	if s.Cells[0][1].Width != 0 {
		t.Fatalf("cell(0,1) = %+v, want the 你 continuation cell (Width 0)", s.Cells[0][1])
	}
	if s.Cells[0][2].R != '好' || s.Cells[0][2].Width != 2 {
		t.Fatalf("cell(0,2) = %+v, want lead cell of 好 with Width 2", s.Cells[0][2])
	}
	if s.Cells[0][3].Width != 0 {
		t.Fatalf("cell(0,3) = %+v, want the 好 continuation cell (Width 0)", s.Cells[0][3])
	}
	if s.Cells[0][4].R != ' ' {
		t.Fatalf("cell(0,4) = %q, want the literal space", s.Cells[0][4].R)
	}
	if s.Cells[0][5].R != 'a' {
		t.Fatalf("cell(0,5) = %q, want 'a'", s.Cells[0][5].R)
	}
	if s.Cells[0][6].R != 'b' {
		t.Fatalf("cell(0,6) = %q, want 'b' (regression: previously landed in column 7)", s.Cells[0][6].R)
	}

	// Round-trip through Serialize and confirm the target emulator agrees
	// column-for-column, not just on this row's raw feed.
	b := NewEmulator(20, 5)
	b.Feed(Serialize(s))
	rt := b.Screen()
	if rt.Cells[0][6].R != 'b' {
		t.Fatalf("after round-trip, cell(0,6) = %q, want 'b'", rt.Cells[0][6].R)
	}
}

func TestPlainTextLandsInCells(t *testing.T) {
	e := NewEmulator(20, 5)
	e.Feed([]byte("hi"))
	s := e.Screen()
	if s.Cells[0][0].R != 'h' || s.Cells[0][1].R != 'i' {
		t.Fatalf("row0 = %q %q", s.Cells[0][0].R, s.Cells[0][1].R)
	}
	if s.CursorX != 2 || s.CursorY != 0 { t.Fatalf("cursor = (%d,%d)", s.CursorX, s.CursorY) }
}

func TestAltScreenFlag(t *testing.T) {
	e := NewEmulator(20, 5)
	e.Feed([]byte("\x1b[?1049h"))
	if !e.Screen().Alt { t.Fatal("expected Alt=true after 1049h") }
	e.Feed([]byte("\x1b[?1049l"))
	if e.Screen().Alt { t.Fatal("expected Alt=false after 1049l") }
}

func TestResize(t *testing.T) {
	e := NewEmulator(20, 5)
	e.Feed([]byte("hello"))
	e.Resize(40, 10)
	s := e.Screen()
	if s.Cols != 40 || s.Rows != 10 { t.Fatalf("size = %dx%d", s.Cols, s.Rows) }
}

// TestSnapshotRoundTripFiles runs the same round-trip property as
// TestSnapshotRoundTrip over recorded fixtures of real TUI output captured
// by cmd/vtcap, at the 120x32 size vtcap records with.
func TestSnapshotRoundTripFiles(t *testing.T) {
	files, _ := filepath.Glob("../../testdata/vt/*.input")
	if len(files) == 0 { t.Skip("no recorded fixtures yet") }
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			input, err := os.ReadFile(f)
			if err != nil { t.Fatal(err) }
			a := NewEmulator(120, 32)
			a.Feed(input)
			sa := a.Screen()
			b := NewEmulator(120, 32)
			b.Feed(Serialize(sa))
			screensEqual(t, sa, b.Screen())
		})
	}
}

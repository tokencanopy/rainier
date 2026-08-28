package term

import (
	"image/color"
	"io"
	"sync"

	uv "github.com/charmbracelet/ultraviolet"
	"github.com/charmbracelet/x/ansi"
	vt "github.com/charmbracelet/x/vt"
)

// emu wraps a github.com/charmbracelet/x/vt Emulator behind the term.Emulator
// interface. The vt.Emulator is not safe for concurrent use, so all access is
// serialized behind mu.
type emu struct {
	mu sync.Mutex
	t  *vt.Emulator

	// cursorHidden mirrors DECTCEM (mode 25) state. vt.Emulator does not
	// expose a CursorHidden() accessor, so we track it ourselves via the
	// CursorVisibility callback, which fires on every ESC[?25h/l and on
	// alt-screen switches.
	cursorHidden bool
}

// NewEmulator returns an Emulator backed by charmbracelet/x/vt.
func NewEmulator(cols, rows int) Emulator {
	t := vt.NewEmulator(cols, rows)
	e := &emu{t: t}
	e.t.SetCallbacks(vt.Callbacks{
		CursorVisibility: func(visible bool) {
			e.cursorHidden = !visible
		},
	})
	// vt.Emulator answers terminal queries (DA1/DA2, DSR, CPR, ...) by
	// writing the reply into an internal io.Pipe that Terminal.Read reads
	// back out. Nothing here is on the other end of a real PTY, so if we
	// never drain it, feeding a query sequence (e.g. "\x1b[6n") would block
	// that Write — and therefore Feed — forever. Drain and discard.
	go io.Copy(io.Discard, t) //nolint:errcheck
	return e
}

func (e *emu) Feed(p []byte) {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, _ = e.t.Write(p)
}

func (e *emu) Resize(cols, rows int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.t.Resize(cols, rows)
}

func (e *emu) Screen() Screen {
	e.mu.Lock()
	defer e.mu.Unlock()

	cols, rows := e.t.Width(), e.t.Height()
	s := Screen{Cols: cols, Rows: rows, Cells: make([][]Cell, rows)}
	for y := 0; y < rows; y++ {
		s.Cells[y] = make([]Cell, cols)
		for x := 0; x < cols; x++ {
			s.Cells[y][x] = convertCell(e.t.CellAt(x, y))
		}
	}
	pos := e.t.CursorPosition()
	s.CursorX, s.CursorY = pos.X, pos.Y
	s.CursorHidden = e.cursorHidden
	s.Alt = e.t.IsAltScreen()
	return s
}

// convertCell maps a *uv.Cell (the vt library's per-cell rune + style type)
// to term.Cell.
func convertCell(c *uv.Cell) Cell {
	if c == nil {
		return Cell{R: ' ', Width: 1}
	}
	if c.Content == "" {
		// The vt library represents the column(s) after a wide cell's lead
		// column as zero-value placeholder cells (empty Content, Width 0) —
		// see charmbracelet/ultraviolet's Line.Set. Carry that Width 0
		// through unchanged so Serialize knows to skip it.
		return Cell{Width: 0}
	}
	r := []rune(c.Content)[0]
	w := c.Width
	if w <= 0 {
		w = 1
	}
	return Cell{
		R:         r,
		Width:     w,
		FG:        convertColor(c.Style.Fg),
		BG:        convertColor(c.Style.Bg),
		Bold:      c.Style.Attrs&uv.AttrBold != 0,
		Underline: c.Style.Underline != uv.UnderlineNone,
		Reverse:   c.Style.Attrs&uv.AttrReverse != 0,
	}
}

// convertColor maps the vt/ansi library's color.Color representations
// (ansi.BasicColor and ansi.IndexedColor for 256-color, anything else for
// 24-bit true color) into term.Color{Mode 0/1/2}.
func convertColor(c color.Color) Color {
	switch v := c.(type) {
	case nil:
		return Color{}
	case ansi.BasicColor:
		return Color{Mode: 1, Idx: uint8(v)}
	case ansi.IndexedColor:
		return Color{Mode: 1, Idx: uint8(v)}
	default:
		r, g, b, _ := c.RGBA()
		return Color{Mode: 2, R: uint8(r >> 8), G: uint8(g >> 8), B: uint8(b >> 8)}
	}
}

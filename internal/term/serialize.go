package term

import (
	"fmt"
	"strings"
)

// Serialize renders a Screen as one repaint escape sequence: reset, clear,
// paint every row with minimal SGR changes, restore cursor. Feeding the
// result into a fresh emulator of the same size reproduces the screen.
func Serialize(s Screen) []byte {
	var b strings.Builder
	b.WriteString("\x1b[0m\x1b[2J\x1b[H")
	cur := styleKey{}
	for y := 0; y < s.Rows; y++ {
		b.WriteString(fmt.Sprintf("\x1b[%d;1H", y+1))
		for x := 0; x < s.Cols; x++ {
			c := cellAt(s, y, x)
			k := keyOf(c)
			if k != cur {
				b.WriteString("\x1b[0m")
				writeSGR(&b, c)
				cur = k
			}
			r := c.R
			if r == 0 { r = ' ' }
			b.WriteRune(r)
		}
	}
	b.WriteString("\x1b[0m")
	b.WriteString(fmt.Sprintf("\x1b[%d;%dH", s.CursorY+1, s.CursorX+1))
	if s.CursorHidden { b.WriteString("\x1b[?25l") } else { b.WriteString("\x1b[?25h") }
	return []byte(b.String())
}

func cellAt(s Screen, y, x int) Cell {
	if y < len(s.Cells) && x < len(s.Cells[y]) { return s.Cells[y][x] }
	return Cell{R: ' '}
}

type styleKey struct {
	fg, bg  Color
	b, u, r bool
}

func keyOf(c Cell) styleKey { return styleKey{c.FG, c.BG, c.Bold, c.Underline, c.Reverse} }

func writeSGR(b *strings.Builder, c Cell) {
	if c.Bold { b.WriteString("\x1b[1m") }
	if c.Underline { b.WriteString("\x1b[4m") }
	if c.Reverse { b.WriteString("\x1b[7m") }
	switch c.FG.Mode {
	case 1: fmt.Fprintf(b, "\x1b[38;5;%dm", c.FG.Idx)
	case 2: fmt.Fprintf(b, "\x1b[38;2;%d;%d;%dm", c.FG.R, c.FG.G, c.FG.B)
	}
	switch c.BG.Mode {
	case 1: fmt.Fprintf(b, "\x1b[48;5;%dm", c.BG.Idx)
	case 2: fmt.Fprintf(b, "\x1b[48;2;%d;%d;%dm", c.BG.R, c.BG.G, c.BG.B)
	}
}
